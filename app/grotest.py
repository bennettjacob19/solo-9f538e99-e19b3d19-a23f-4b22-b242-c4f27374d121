"""UDP_SEGMENT 试投：一次 sendmsg 发出整批，逐序号核对原样回包。

- 每次试投使用独立 socket（临时端口）与独立试投编号（UUID），
  并发试投、迟到回包互不影响，结果只含本批；
- 整批用一次 UDP_SEGMENT 发送（内核 GSO 分段），段长为各包长度
  （除末包可更短外必须等长，与内核切分边界一致）；
- 回显端观察到的接收分组按试投编号过滤后随结果返回；
- 内核不支持 UDP_SEGMENT（发送）或 UDP_GRO（接收）时在结果中分别说明。
"""
from __future__ import annotations

import asyncio
import errno
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Annotated, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .echo_server import EchoGroupOut, EchoServer, EchoState, echo_group_out
from .netutil import send_udp_segments, set_dont_fragment
from .packets import HEADER_SIZE, build_packet, parse_packet

MIN_SEGMENT = HEADER_SIZE  # 28：小于报文头的包无法通过校验
MAX_SEGMENT = 1400
MIN_PACKETS = 2
MAX_PACKETS = 16

# 内核拒绝 UDP_SEGMENT 时常见的 errno：通常表示该卸载不可用
_OFFLOAD_ERRNOS = {errno.EINVAL, errno.ENOPROTOOPT, errno.EOPNOTSUPP}


class GroTestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lengths: List[Annotated[int, Field(ge=MIN_SEGMENT, le=MAX_SEGMENT)]] = Field(
        ...,
        min_length=MIN_PACKETS,
        max_length=MAX_PACKETS,
        description="各报文长度（字节）：2~16 个，每个 28~1400；除末包可更短外必须等长",
    )
    timeout_ms: int = Field(500, ge=20, le=30000, description="等待整批回包的总超时（毫秒）")
    target_host: Optional[str] = Field(None, description="回显端地址，缺省为本地回显端")
    target_port: Optional[int] = Field(None, ge=1, le=65535, description="回显端端口，缺省为本地回显端")

    @model_validator(mode="after")
    def _check_layout(self) -> "GroTestRequest":
        head = self.lengths[:-1]
        if any(size != head[0] for size in head):
            raise ValueError("除末包外各包必须等长（UDP_SEGMENT 按统一段长切分）")
        if self.lengths[-1] > head[0]:
            raise ValueError("末包只能等长或更短，不能大于段长")
        return self


class GroPacketOut(BaseModel):
    seq: int
    size: int
    echoed: bool = Field(..., description="是否收到该序号的原样回包")
    rtt_ms: Optional[float] = None


class GroTestResult(BaseModel):
    test_id: str
    target: str
    lengths: List[int]
    gso_size: int = Field(..., description="本次 UDP_SEGMENT 段长（= 各包长度）")
    packet_count: int
    send_status: Literal["sent", "offload_unavailable", "send_error"] = Field(
        ...,
        description="sent=整批已发出；offload_unavailable=内核不支持 UDP_SEGMENT；send_error=其他发送失败",
    )
    send_detail: Optional[str] = None
    df_enabled: bool = Field(..., description="试投 socket 是否成功设置 DF（禁止 IP 分片）")
    echo_gro_enabled: bool = Field(..., description="回显端是否成功启用 UDP_GRO")
    aggregation_observed: bool = Field(..., description="回显端是否观察到聚合接收（单组多包）")
    note: Optional[str] = Field(None, description="未观察到聚合 / 卸载不可用等情况说明")
    packets: List[GroPacketOut] = Field(..., description="按序号排列的本批回包核对结果")
    observed_groups: List[EchoGroupOut] = Field(
        ..., description="回显端实际观察到的本批接收分组及组内序号"
    )
    stray_replies: int = Field(0, description="被丢弃的迟到/不对应回包数")
    started_at: datetime
    finished_at: datetime
    duration_ms: float


class GroTester:
    """UDP_SEGMENT 试投执行器。"""

    def __init__(
        self,
        state: EchoState,
        echo: EchoServer,
        default_target: tuple[str, int],
    ) -> None:
        self._state = state
        self._echo = echo
        self._default_target = default_target

    async def run(self, req: GroTestRequest) -> GroTestResult:
        test_id = uuid.uuid4()
        started_at = datetime.now(timezone.utc)
        t0 = time.monotonic()
        host, port = self._default_target
        if req.target_host is not None:
            host = req.target_host
        if req.target_port is not None:
            port = req.target_port
        target = f"{host}:{port}"

        gso_size = req.lengths[0]
        count = len(req.lengths)
        # 各包携带本次试投编号与序号；拼接顺序即内核切分顺序
        sent_packets = [
            build_packet(test_id, i + 1, size) for i, size in enumerate(req.lengths)
        ]
        payload = b"".join(sent_packets)

        # 本批专用 socket（独立临时端口），connect 到回显端：
        # 其他试投的回包在内核层就被过滤，不会到达本 socket。
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        df_ok = set_dont_fragment(sock)

        send_status: Literal["sent", "offload_unavailable", "send_error"] = "sent"
        send_detail: Optional[str] = None
        note: Optional[str] = None
        echoed_rtt: Dict[int, float] = {}
        stray = 0

        try:
            sock.connect((host, port))  # UDP connect 即时完成，只做对端过滤
            try:
                send_udp_segments(sock, payload, gso_size)
            except OSError as exc:
                if exc.errno in _OFFLOAD_ERRNOS:
                    send_status = "offload_unavailable"
                    send_detail = f"{type(exc).__name__}: {exc}"
                    note = (
                        f"内核不支持 UDP_SEGMENT 发送卸载（{send_detail}），整批未发出；"
                        "回显端普通回显不受影响"
                    )
                else:
                    send_status = "send_error"
                    send_detail = f"{type(exc).__name__}: {exc}"
                    note = f"UDP_SEGMENT 发送失败：{send_detail}"

            if send_status == "sent":
                t_send = time.monotonic()
                deadline = t_send + req.timeout_ms / 1000.0
                while len(echoed_rtt) < count:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        data = await asyncio.wait_for(
                            loop.sock_recv(sock, 65535), remaining
                        )
                    except asyncio.TimeoutError:
                        break
                    except OSError as exc:  # 连通型 UDP 的异步 ICMP 错误等
                        note = f"接收回包出错：{type(exc).__name__}: {exc}"
                        break
                    pkt = parse_packet(data)
                    if (
                        pkt is not None
                        and pkt.session_id == test_id
                        and 1 <= pkt.seq <= count
                        and pkt.seq not in echoed_rtt
                        and data == sent_packets[pkt.seq - 1]  # 原样回包才采信
                    ):
                        echoed_rtt[pkt.seq] = (time.monotonic() - t_send) * 1000.0
                    else:
                        # 迟到/不对应回包：丢弃不采信，继续等
                        stray += 1
        finally:
            sock.close()

        packets_out = [
            GroPacketOut(
                seq=i + 1,
                size=size,
                echoed=(i + 1) in echoed_rtt,
                rtt_ms=round(echoed_rtt[i + 1], 3) if (i + 1) in echoed_rtt else None,
            )
            for i, size in enumerate(req.lengths)
        ]

        # 只回查本批试投编号命中的分组，并发试投互不影响
        groups = [echo_group_out(g) for g in self._echo.groups_for(test_id)]
        aggregation = any(
            g.gso_size is not None and len(g.segment_sizes) > 1 for g in groups
        )

        if note is None:
            if not self._echo.gro_enabled:
                note = (
                    "回显端内核不支持 UDP_GRO，已回退普通回显：整批被逐包接收，"
                    "各包按普通数据报判定回显"
                )
            elif not aggregation:
                note = (
                    "回显端未观察到聚合：整批被逐包接收（内核未做 GRO 合并），"
                    "各包按普通数据报判定回显"
                )

        finished_at = datetime.now(timezone.utc)
        return GroTestResult(
            test_id=str(test_id),
            target=target,
            lengths=req.lengths,
            gso_size=gso_size,
            packet_count=count,
            send_status=send_status,
            send_detail=send_detail,
            df_enabled=df_ok,
            echo_gro_enabled=self._echo.gro_enabled,
            aggregation_observed=aggregation,
            note=note,
            packets=packets_out,
            observed_groups=groups,
            stray_replies=stray,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=round((time.monotonic() - t0) * 1000.0, 3),
        )
