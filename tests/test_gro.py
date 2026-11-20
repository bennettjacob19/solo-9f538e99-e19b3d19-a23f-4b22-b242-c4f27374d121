"""UDP_SEGMENT 试投与 UDP_GRO 聚合接收的端到端测试。"""
from __future__ import annotations

import errno
import socket
import sys
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.netutil import UDP_GRO, parse_gro_cmsg, split_segments


@pytest.fixture()
def client():
    # 每个用例一套全新 lifespan：回显端状态、epoch、观察日志都重置
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def no_gro_client(monkeypatch):
    """模拟内核不支持 UDP_GRO：setsockopt 失败，回退普通回显。"""
    monkeypatch.setattr("app.echo_server.enable_udp_gro", lambda sock: False)
    with TestClient(app) as c:
        yield c


def gro_test(client, **kw):
    kw.setdefault("timeout_ms", 300)
    r = client.post("/echo/gro-test", json=kw)
    assert r.status_code == 200, r.text
    return r.json()


def own_packets(res):
    """结果观察分组中属于本批的报文。"""
    return [
        p
        for g in res["observed_groups"]
        for p in g["packets"]
        if p["session_id"] == res["test_id"]
    ]


# ---------------------------------------------------------------- 工具函数


class TestSegmentSplit:
    def test_no_gro_single_segment(self):
        assert split_segments(b"abc", None) == [b"abc"]
        assert split_segments(b"abc", 0) == [b"abc"]
        assert split_segments(b"abc", -1) == [b"abc"]

    def test_gso_not_smaller_than_data(self):
        assert split_segments(b"abc", 3) == [b"abc"]
        assert split_segments(b"abc", 100) == [b"abc"]

    def test_split_with_short_tail(self):
        assert split_segments(b"aaaabbbbc", 4) == [b"aaaa", b"bbbb", b"c"]

    def test_split_exact_multiple(self):
        assert split_segments(b"aabbcc", 2) == [b"aa", "bb".encode(), b"cc"]

    def test_parse_cmsg(self):
        assert parse_gro_cmsg([]) is None
        assert parse_gro_cmsg([(socket.IPPROTO_TCP, 999, b"\x00" * 4)]) is None
        raw4 = (1400).to_bytes(4, sys.byteorder)
        raw2 = (1400).to_bytes(2, sys.byteorder)
        assert parse_gro_cmsg([(socket.IPPROTO_UDP, UDP_GRO, raw4)]) == 1400
        assert parse_gro_cmsg([(socket.IPPROTO_UDP, UDP_GRO, raw2)]) == 1400


# ---------------------------------------------------------------- 入参校验


class TestGroValidation:
    def test_packet_count_bounds(self, client):
        assert client.post("/echo/gro-test", json={"lengths": [100]}).status_code == 422
        assert client.post("/echo/gro-test", json={"lengths": []}).status_code == 422
        assert client.post("/echo/gro-test", json={"lengths": [100] * 17}).status_code == 422
        assert client.post("/echo/gro-test", json={"lengths": [100] * 16}).status_code == 200

    def test_length_range(self, client):
        assert client.post("/echo/gro-test", json={"lengths": [27, 27]}).status_code == 422
        assert client.post("/echo/gro-test", json={"lengths": [1401, 1401]}).status_code == 422
        assert client.post("/echo/gro-test", json={"lengths": [28, 28]}).status_code == 200
        assert client.post("/echo/gro-test", json={"lengths": [1400, 1400]}).status_code == 200

    def test_equal_except_last(self, client):
        # 中间包不等长 → 422
        assert client.post("/echo/gro-test", json={"lengths": [100, 200, 100]}).status_code == 422
        # 末包更长 → 422
        assert client.post("/echo/gro-test", json={"lengths": [100, 100, 200]}).status_code == 422
        # 末包等长 / 更短 → 通过
        assert client.post("/echo/gro-test", json={"lengths": [100, 100, 100]}).status_code == 200
        assert client.post("/echo/gro-test", json={"lengths": [100, 100, 28]}).status_code == 200

    def test_extra_fields_rejected(self, client):
        r = client.post("/echo/gro-test", json={"lengths": [100, 100], "bogus": 1})
        assert r.status_code == 422


# ---------------------------------------------------------------- 试投


class TestGroRun:
    def test_all_echoed_with_short_tail(self, client):
        res = gro_test(client, lengths=[1400, 1400, 1400, 800])
        assert res["send_status"] == "sent", res["note"]
        assert res["gso_size"] == 1400
        assert res["packet_count"] == 4
        assert res["df_enabled"] is True
        # 逐序号核对：全部收到原样回包
        assert [p["seq"] for p in res["packets"]] == [1, 2, 3, 4]
        assert all(p["echoed"] for p in res["packets"])
        assert all(p["rtt_ms"] is not None for p in res["packets"])
        # 回显端观察到了本批分组，且组内序号完整
        groups = res["observed_groups"]
        assert groups, "回显端应观察到本批分组"
        seqs = sorted(p["seq"] for p in own_packets(res))
        assert seqs == [1, 2, 3, 4]
        # 观察到的各段长度与发送布局一致
        sizes = [s for g in groups for s in g["segment_sizes"]]
        assert sizes == [1400, 1400, 1400, 800]
        # 聚合标志与观察记录自洽；未观察到聚合时必须有说明
        aggregated = any(
            g["gso_size"] is not None and len(g["segment_sizes"]) > 1 for g in groups
        )
        assert res["aggregation_observed"] == aggregated
        if aggregated:
            g = next(g for g in groups if len(g["segment_sizes"]) > 1)
            assert g["gso_size"] == 1400
        else:
            assert res["note"]

    def test_max_batch(self, client):
        res = gro_test(client, lengths=[100] * 15 + [28])
        assert res["send_status"] == "sent"
        assert all(p["echoed"] for p in res["packets"])
        assert len(res["packets"]) == 16

    def test_drop_threshold_applies_per_packet(self, client):
        r = client.put("/echo/config", json={"mode": "normal", "drop_threshold": 1000})
        assert r.status_code == 200
        epoch = r.json()["epoch"]

        res = gro_test(client, lengths=[1400, 1400, 800])
        by_seq = {p["seq"]: p for p in res["packets"]}
        assert not by_seq[1]["echoed"] and not by_seq[2]["echoed"]
        # 较短的末包按自身长度判定，照常回显
        assert by_seq[3]["echoed"]

        # 观察记录：整组使用收取该组时的配置（epoch 即当前配置世代）
        groups = res["observed_groups"]
        assert groups
        assert all(g["config_epoch"] == epoch for g in groups)
        reasons = {p["seq"]: p["reason"] for p in own_packets(res)}
        assert reasons == {1: "over_threshold", 2: "over_threshold", 3: "echoed"}

    def test_disconnect_drops_all_but_still_observed(self, client):
        client.put("/echo/config", json={"mode": "disconnect"})
        res = gro_test(client, lengths=[100, 100], timeout_ms=200)
        assert res["send_status"] == "sent"
        assert not any(p["echoed"] for p in res["packets"])
        # 丢包也如实记录在观察分组里
        own = own_packets(res)
        assert len(own) == 2
        assert all(p["reason"] == "disconnect" for p in own)

    def test_concurrent_tests_isolated(self, client):
        out = {}

        def worker(name, lengths):
            out[name] = gro_test(client, lengths=lengths, timeout_ms=300)

        threads = [
            threading.Thread(target=worker, args=("a", [1400, 1400, 1400])),
            threading.Thread(target=worker, args=("b", [200, 200, 100])),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        a, b = out["a"], out["b"]
        assert a["test_id"] != b["test_id"]
        for res, sizes in ((a, [1400, 1400, 1400]), (b, [200, 200, 100])):
            assert res["send_status"] == "sent"
            assert all(p["echoed"] for p in res["packets"])
            assert res["stray_replies"] == 0
            # 只返回本批分组：组内报文全部属于本批，布局与发送一致
            assert res["observed_groups"]
            for g in res["observed_groups"]:
                for p in g["packets"]:
                    assert p["session_id"] == res["test_id"]
            own_sizes = [s for g in res["observed_groups"] for s in g["segment_sizes"]]
            assert own_sizes == sizes


# ---------------------------------------------------------------- 观察与配置接口


class TestObservability:
    def test_config_exposes_gro_flag(self, client):
        body = client.get("/echo/config").json()
        assert "gro_enabled" in body
        assert body["stats"] == {"echoed": 0, "dropped": 0, "send_errors": 0}

    def test_groups_endpoint(self, client):
        res = gro_test(client, lengths=[100, 100])
        r = client.get("/echo/groups")
        assert r.status_code == 200
        body = r.json()
        assert body["gro_enabled"] == res["echo_gro_enabled"]
        assert body["groups"], "试投后应有观察分组"
        g0 = body["groups"][0]
        assert {
            "group_id",
            "received_at",
            "gso_size",
            "segment_sizes",
            "packets",
            "config_epoch",
        } <= set(g0)
        # 最新的组就是刚才那次试投
        assert g0["packets"][0]["session_id"] == res["test_id"]

    def test_stats_count_per_packet(self, client):
        gro_test(client, lengths=[100, 100, 100])
        stats = client.get("/echo/config").json()["stats"]
        assert stats["echoed"] == 3
        assert stats["dropped"] == 0

    def test_index_lists_gro_endpoints(self, client):
        body = client.get("/").json()
        assert "POST /echo/gro-test" in body["endpoints"]["gro"]


# ---------------------------------------------------------------- 卸载不可用 / 回退


class TestFallback:
    def test_gro_unavailable_still_echoes_normally(self, no_gro_client):
        client = no_gro_client
        assert client.get("/echo/config").json()["gro_enabled"] is False

        res = gro_test(client, lengths=[500, 500, 300])
        # 普通回显照常提供：各包逐包接收、逐包回显
        assert res["send_status"] == "sent"
        assert res["echo_gro_enabled"] is False
        assert all(p["echoed"] for p in res["packets"])
        # 不可能观察到聚合，且必须在 note 中说明卸载不可用
        assert res["aggregation_observed"] is False
        assert "UDP_GRO" in res["note"]
        # 观察分组均为普通单包
        assert res["observed_groups"]
        assert all(g["gso_size"] is None for g in res["observed_groups"])
        sizes = [s for g in res["observed_groups"] for s in g["segment_sizes"]]
        assert sizes == [500, 500, 300]

    def test_udp_segment_unavailable(self, client, monkeypatch):
        def boom(sock, payload, gso_size):
            raise OSError(errno.EINVAL, "Invalid argument")

        monkeypatch.setattr("app.grotest.send_udp_segments", boom)
        res = gro_test(client, lengths=[500, 500])
        assert res["send_status"] == "offload_unavailable"
        assert "UDP_SEGMENT" in res["note"]
        # 整批未发出：无回包、无观察分组
        assert not any(p["echoed"] for p in res["packets"])
        assert res["observed_groups"] == []

    def test_udp_segment_send_error(self, client, monkeypatch):
        def boom(sock, payload, gso_size):
            raise OSError(errno.EMSGSIZE, "Message too long")

        monkeypatch.setattr("app.grotest.send_udp_segments", boom)
        res = gro_test(client, lengths=[500, 500])
        assert res["send_status"] == "send_error"
        assert "EMSGSIZE" in res["send_detail"] or "Message too long" in res["send_detail"]
        assert res["observed_groups"] == []
