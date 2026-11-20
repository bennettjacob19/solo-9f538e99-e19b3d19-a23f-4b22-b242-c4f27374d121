"""本地 UDP 回显端（支持 UDP_GRO 聚合接收）。

- 普通数据报与内核聚合（GRO）的数据都从同一 IPv4 端口接收；
- 内核支持时启用 UDP_GRO：一次 recvmsg 拿到整组聚合数据后，按 UDP_GRO
  控制消息给出的分段长度还原原始数据报边界，逐包校验、判定与计数，
  较短的末包与其余各包同等处理；
- 整组使用**收取该组时**的回显配置快照判定组内全部报文（配置变更
  只影响之后到达的组）；
- 内核不支持 UDP_GRO 时自动回退为普通逐包回显，行为与旧版一致；
- 每次接收（一组）都记入观察日志，供试投接口按试投编号回查。

行为可通过 API 动态配置：
- ``drop_threshold``: UDP 负载超过该字节数即丢弃（模拟"大包不通"）；
- ``mode = "disconnect"``: 丢弃一切报文（模拟断联）。

每次成功变更配置都会使 ``epoch`` 单调递增。探测任务在开始与结束时比对
epoch：若探测期间配置发生变化，本次结论作废。
"""
from __future__ import annotations

import asyncio
import socket
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from .netutil import enable_udp_gro, parse_gro_cmsg, split_segments
from .packets import MAX_UDP_PAYLOAD, parse_packet

RECV_BUFSIZE = 256 * 1024  # 聚合数据可能远超单包上限（gso_size × 段数）
CMSG_BUFSIZE = socket.CMSG_SPACE(16)
GROUP_LOG_MAX = 100  # 内存中保留的接收分组条数上限


class EchoConfig(BaseModel):
    """回显端行为配置。校验失败时调用方不得改动现有状态。"""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["normal", "disconnect"] = Field(
        default="normal",
        description="normal=按阈值丢弃；disconnect=断联，丢弃一切报文",
    )
    drop_threshold: Optional[int] = Field(
        default=None,
        ge=0,
        le=MAX_UDP_PAYLOAD,
        description="UDP 负载超过该字节数即丢弃；null 表示不丢弃",
    )


class EchoState:
    """回显端共享状态（配置 + 世代号）。仅在事件循环内访问，替换是原子的。"""

    def __init__(self) -> None:
        self.config = EchoConfig()
        self.epoch = 0

    def apply(self, cfg: EchoConfig) -> int:
        """应用已通过校验的新配置，返回新 epoch。校验失败时不应调用本方法。"""
        self.config = cfg
        self.epoch += 1
        return self.epoch


# ---------------------------------------------------------------------------
# 接收分组观察记录
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ObservedPacket:
    """组内单个报文的观察记录。"""

    session_id: Optional[uuid.UUID]
    seq: Optional[int]
    size: int
    echoed: bool
    reason: Literal["echoed", "disconnect", "over_threshold", "invalid", "send_error"]


@dataclass(frozen=True)
class ObservedGroup:
    """一次接收（一次 recvmsg）观察到的一组数据报。"""

    group_id: int
    received_at: datetime
    gso_size: Optional[int]  # UDP_GRO 分段长度；None = 普通单包接收
    epoch: int  # 判定本组时使用的配置世代
    packets: tuple[ObservedPacket, ...]


class EchoGroupPacketOut(BaseModel):
    session_id: Optional[str]
    seq: Optional[int]
    size: int
    echoed: bool
    reason: str


class EchoGroupOut(BaseModel):
    group_id: int
    received_at: datetime
    gso_size: Optional[int] = Field(..., description="UDP_GRO 分段长度；null 表示普通单包接收")
    segment_sizes: List[int] = Field(..., description="组内各数据报长度（按接收顺序）")
    packets: List[EchoGroupPacketOut]
    config_epoch: int = Field(..., description="判定本组时使用的回显配置世代")


class EchoGroupsView(BaseModel):
    gro_enabled: bool = Field(..., description="回显端是否成功启用 UDP_GRO")
    groups: List[EchoGroupOut]


def echo_group_out(g: ObservedGroup) -> EchoGroupOut:
    """把内部观察记录序列化为 API 输出模型。"""
    return EchoGroupOut(
        group_id=g.group_id,
        received_at=g.received_at,
        gso_size=g.gso_size,
        segment_sizes=[p.size for p in g.packets],
        packets=[
            EchoGroupPacketOut(
                session_id=str(p.session_id) if p.session_id is not None else None,
                seq=p.seq,
                size=p.size,
                echoed=p.echoed,
                reason=p.reason,
            )
            for p in g.packets
        ],
        config_epoch=g.epoch,
    )


# ---------------------------------------------------------------------------
# 回显端
# ---------------------------------------------------------------------------


class EchoServer:
    """回显端：原始 socket + add_reader + recvmsg（可解析 UDP_GRO 聚合数据）。

    普通包与内核聚合的数据都从同一 IPv4 端口接收；内核不支持 UDP_GRO
    （或非 Linux 平台）时回退为普通逐包回显。
    """

    def __init__(self, state: EchoState, sock: socket.socket) -> None:
        self.state = state
        self.sock = sock
        self.gro_enabled = enable_udp_gro(sock)
        self.echoed = 0
        self.dropped = 0
        self.send_errors = 0
        self._groups: Deque[ObservedGroup] = deque(maxlen=GROUP_LOG_MAX)
        self._group_seq = 0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._transport: Optional[asyncio.DatagramTransport] = None  # 非 Linux 兜底

    async def start(self) -> None:
        """在事件循环上挂接接收回调。应在循环运行前（lifespan 内）调用。"""
        loop = asyncio.get_running_loop()
        self._loop = loop
        if hasattr(loop, "add_reader"):
            loop.add_reader(self.sock.fileno(), self._on_readable)
        else:  # pragma: no cover - 非 Unix 平台兜底
            self.gro_enabled = False
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _FallbackProtocol(self), sock=self.sock
            )
            self._transport = transport  # type: ignore[assignment]

    def close(self) -> None:
        """摘除接收回调并关闭 socket（可重复调用）。"""
        if self._loop is not None and hasattr(self._loop, "remove_reader"):
            try:
                self._loop.remove_reader(self.sock.fileno())
            except Exception:  # noqa: BLE001 - 关闭阶段不放大异常
                pass
        if self._transport is not None:
            self._transport.close()
        self.sock.close()

    # ------------------------------------------------------------ 接收

    def _on_readable(self) -> None:
        """排空内核接收队列，逐个处理到达的（可能聚合的）数据。"""
        while True:
            try:
                data, ancdata, _flags, addr = self.sock.recvmsg(
                    RECV_BUFSIZE, CMSG_BUFSIZE if self.gro_enabled else 0
                )
            except (BlockingIOError, InterruptedError):
                return
            except OSError:  # socket 已关闭
                return
            self.handle_batch(data, parse_gro_cmsg(ancdata), addr)

    def handle_batch(self, data: bytes, gso_size: Optional[int], addr) -> None:  # noqa: ANN001
        """处理一次接收到的数据（可能是一组聚合报文）。

        整组共用收取本组时的配置快照，按原始数据报边界逐包校验、
        判定、计数并回显；随后把本组记入观察日志。
        """
        cfg = self.state.config
        epoch = self.state.epoch
        records = [
            self._handle_one(seg, cfg, addr) for seg in split_segments(data, gso_size)
        ]
        self._group_seq += 1
        self._groups.append(
            ObservedGroup(
                group_id=self._group_seq,
                received_at=datetime.now(timezone.utc),
                gso_size=gso_size,
                epoch=epoch,
                packets=tuple(records),
            )
        )

    def _handle_one(self, seg: bytes, cfg: EchoConfig, addr) -> ObservedPacket:  # noqa: ANN001
        """按配置快照判定单个数据报：符合条件则原样回送。"""
        pkt = parse_packet(seg)
        session_id = pkt.session_id if pkt is not None else None
        seq = pkt.seq if pkt is not None else None
        if cfg.mode == "disconnect":
            self.dropped += 1
            return ObservedPacket(session_id, seq, len(seg), False, "disconnect")
        if cfg.drop_threshold is not None and len(seg) > cfg.drop_threshold:
            self.dropped += 1
            return ObservedPacket(session_id, seq, len(seg), False, "over_threshold")
        if pkt is None:
            self.dropped += 1
            return ObservedPacket(None, None, len(seg), False, "invalid")
        if self._send(seg, addr):
            self.echoed += 1
            return ObservedPacket(session_id, seq, len(seg), True, "echoed")
        self.send_errors += 1
        return ObservedPacket(session_id, seq, len(seg), False, "send_error")

    def _send(self, data: bytes, addr) -> bool:  # noqa: ANN001
        try:
            if self._transport is not None:
                self._transport.sendto(data, addr)
            else:
                self.sock.sendto(data, addr)
            return True
        except OSError:
            return False

    # ------------------------------------------------------------ 观察日志

    def recent_groups(self, limit: int = 20) -> List[ObservedGroup]:
        """最近接收的分组，新的在前。"""
        groups = list(self._groups)
        return groups[-limit:][::-1]

    def groups_for(self, session_id: uuid.UUID) -> List[ObservedGroup]:
        """只取包含该试投编号报文的分组（并发试投互不串扰），按接收顺序。"""
        return [
            g
            for g in self._groups
            if any(p.session_id == session_id for p in g.packets)
        ]


class _FallbackProtocol(asyncio.DatagramProtocol):
    """非 Linux 平台兜底：普通逐包回显（无 UDP_GRO）。"""

    def __init__(self, server: EchoServer) -> None:
        self.server = server

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.server.handle_batch(data, None, addr)

    def error_received(self, exc: Exception) -> None:  # pragma: no cover
        pass
