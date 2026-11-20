"""socket 工具：DF 位设置与 UDP 卸载（UDP_GRO / UDP_SEGMENT）。"""
from __future__ import annotations

import socket
import struct
import sys
from typing import Optional

# Linux 常量兜底（某些 Python 构建未导出）
IP_MTU_DISCOVER = getattr(socket, "IP_MTU_DISCOVER", 10)
IP_PMTUDISC_DO = getattr(socket, "IP_PMTUDISC_DO", 2)

# UDP 卸载相关常量（Linux 通用值；Python 未导出时兜底）
UDP_SEGMENT = getattr(socket, "UDP_SEGMENT", 103)  # 发送：UDP GSO（内核 >= 4.18）
UDP_GRO = getattr(socket, "UDP_GRO", 104)          # 接收：UDP GRO（内核 >= 5.0）


def set_dont_fragment(sock) -> bool:  # noqa: ANN001, ANN201
    """对 IPv4 UDP socket 设置 PMTUDISC_DO（DF=1）。

    设置后内核不会对本 socket 发出的报文做 IP 分片；
    若报文超过路径 MTU，发送将直接失败（EMSGSIZE），由上层按发送失败处理。
    返回是否设置成功（非 Linux 平台可能不支持，调用方应如实记录）。

    接受真实 socket 或 asyncio 的 TransportSocket（鸭子类型）。
    """
    if getattr(sock, "family", None) != socket.AF_INET:
        return False
    try:
        sock.setsockopt(socket.IPPROTO_IP, IP_MTU_DISCOVER, IP_PMTUDISC_DO)
        return True
    except (OSError, AttributeError):
        return False


def enable_udp_gro(sock) -> bool:  # noqa: ANN001, ANN201
    """在 IPv4 UDP socket 上启用 UDP_GRO（聚合接收）。

    启用后，内核聚合（GRO/GSO）的数据报会整体交付给一次 recvmsg，
    并通过 UDP_GRO 控制消息携带分段长度；普通数据报不受影响。
    内核不支持（< 5.0）时返回 False，调用方按普通逐包接收继续工作。
    """
    if getattr(sock, "family", None) != socket.AF_INET:
        return False
    try:
        sock.setsockopt(socket.IPPROTO_UDP, UDP_GRO, 1)
        return True
    except (OSError, AttributeError):
        return False


def parse_gro_cmsg(ancdata) -> Optional[int]:  # noqa: ANN001, ANN201
    """从 recvmsg 的控制消息中解析 UDP_GRO 分段长度；无该控制消息时返回 None。

    内核交付的 UDP_GRO cmsg 负载为本机字节序的 int（4 字节），
    个别场景可能为 2 字节，均兼容解析。
    """
    for level, ctype, cdata in ancdata:
        if level == socket.IPPROTO_UDP and ctype == UDP_GRO:
            if len(cdata) >= 4:
                return int.from_bytes(cdata[:4], sys.byteorder)
            if len(cdata) >= 2:
                return int.from_bytes(cdata[:2], sys.byteorder)
    return None


def split_segments(data: bytes, gso_size: Optional[int]) -> list[bytes]:
    """按 UDP_GRO 分段长度还原原始数据报边界。

    - 无聚合（gso_size 为空或不合理）时，整体视为一个数据报；
    - 聚合时按 gso_size 逐段切分，末包允许更短（余数段），
      与各段等长时自然没有余数段。
    """
    if not gso_size or gso_size <= 0 or gso_size >= len(data):
        return [data]
    return [data[i:i + gso_size] for i in range(0, len(data), gso_size)]


def send_udp_segments(sock, payload: bytes, gso_size: int) -> int:  # noqa: ANN001, ANN201
    """用一次 sendmsg + UDP_SEGMENT 控制消息发出整批数据（UDP GSO）。

    内核按 gso_size 把 payload 切分为独立 UDP 数据报发出（末包可更短）。
    内核不支持（< 4.18）或参数被拒时抛 OSError，由调用方区分上报。
    """
    cmsg = (socket.IPPROTO_UDP, UDP_SEGMENT, struct.pack("=H", gso_size))
    return sock.sendmsg([payload], [cmsg])
