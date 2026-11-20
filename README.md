# 遥测网关 UDP 最大负载探测

小包能到回显端、大包却超时？本服务为值班员确认遥测网关**可用的最大 UDP 负载**：
FastAPI + asyncio 实现的探测 API 与本地回显端，默认探测范围 **576～1400 字节**，
二分搜索收敛，当次返回最大确认长度、各次探测结果及无法判定原因。

回显端已启用 **UDP_GRO 聚合接收**（内核不支持时自动回退普通回显），并可用
**UDP_SEGMENT 一次发送整批报文**试投，逐序号核对回包、查看回显端实际观察到的聚合分组。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

### 本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## 值班流程（/docs 内即可操作）

1. **设置回显端行为**：`PUT /echo/config`
   - `{"mode": "normal", "drop_threshold": 1000}` — UDP 负载超过 1000 字节即丢弃；
   - `{"mode": "disconnect", "drop_threshold": null}` — 断联，丢弃一切报文；
   - 入参校验失败返回 422，**原配置保持不变**。
2. **发起探测**：`POST /probe`（Body 可全部缺省）

   ```json
   {"min_size": 576, "max_size": 1400, "timeout_ms": 500, "small_size": 64}
   ```
3. **读取结论**：

   | `status` | 含义 | `max_confirmed_size` |
   |---|---|---|
   | `ok` | 已确认最大可用负载 | 确认值（等于 `max_size` 时 `reached_range_max=true`，实际上限可能更高） |
   | `below_range` | 链路正常，但范围下限都确认不了 | `null` |
   | `undetermined` | 大包失败后小包复核也失败，**链路不确定** | 此前已确认的下界（无则 `null`），**绝不把故障当作尺寸上限** |
   | `invalidated` | 探测期间回显端配置被修改，**本次结论作废** | `null` |

   `attempts[]` 列出每次探测（`probe`）与小包复核（`link-verify`）的尺寸、结果与 RTT。
4. 历史结论：`GET /probes`、`GET /probe/{probe_id}`（内存保留最近 200 条）。

## UDP_SEGMENT 聚合试投（/docs 内即可操作）

`POST /echo/gro-test` 用**一次 UDP_SEGMENT 发送**把整批报文交给内核 GSO 分段发出，
验证回显链路的聚合收发行为：

```json
{"lengths": [1400, 1400, 1400, 800], "timeout_ms": 500}
```

- `lengths`：2～16 个报文长度，每个 28～1400 字节；**除末包可更短外各包必须等长**
  （与内核按统一段长切分的边界一致），段长即 `gso_size`；
- 结果中 `packets[]` **按序号**给出每个报文是否收到**原样回包**（逐字节比对）及 RTT；
- `observed_groups[]` 列出**回显端实际观察到的本批接收分组**：`gso_size`（null 表示
  普通单包接收）、组内各报文长度与**组内序号**、判定时使用的配置世代 `config_epoch`；
- 未观察到聚合（`aggregation_observed=false`）与卸载不可用
  （`send_status="offload_unavailable"` 或 `echo_gro_enabled=false`）会在 `note`
  中**分别说明**；
- 每次试投使用独立 socket 与独立试投编号（UUID），**并发试投只返回本批结果**，
  迟到/不对应回包计入 `stray_replies`。

回显端接收行为：

- 普通包与内核聚合的数据都从**同一 IPv4 端口**接收；启用 UDP_GRO 后，一次聚合接收
  按 UDP_GRO 控制消息给出的分段长度**还原原始数据报边界**，逐包校验、判定与计数，
  **较短的末包也照此处理**（按自身长度判定、原样回送）；
- 每组使用**收取该组时**的回显配置快照判定组内全部报文，配置变更只影响之后到达的组；
- 内核不支持 UDP_GRO 时**继续提供普通回显**（逐包接收），`GET /echo/config` 的
  `gro_enabled` 如实反映；
- `GET /echo/groups` 可查看回显端最近观察到的接收分组（含 /probe 流量，内存保留
  最近 100 组）。

> 内核要求：UDP_SEGMENT（发送 GSO）≥ 4.18，UDP_GRO（接收聚合）≥ 5.0。
> 容器与宿主机共享内核，宿主机满足即可；不满足时对应能力自动降级并在结果中说明。

## 判定规则与需求对应

- **禁止 IP 分片**：探测与回显 socket 均设置 `IP_MTU_DISCOVER/IP_PMTUDISC_DO`（DF=1），
  结果中 `df_enabled` 如实上报；超过路径 MTU 时发送直接失败（EMSGSIZE），按发送失败处理。
- **报文带本次探测编号**：每次运行生成 UUID，连同序号写入 28 字节报文头
  （`magic | UUID | seq | size | 填充`）；只有 magic/编号/序号/长度**全部对应**的回包才算成功。
- **大包失败小包复核**：大包超时或发送失败 → 立即用 `small_size`（默认 64B）复核链路；
  小包通 = 确为尺寸受限，继续二分；小包也不通 = `undetermined`，故障不记为尺寸上限。
- **配置改变结论作废**：回显端配置带单调递增的 `epoch`，探测开始/每步/结束比对，
  不一致即 `invalidated`。
- **并发与迟到回包隔离**：每次探测/试投运行使用独立临时端口 socket 并 connect 到回显端，
  其他运行的回包在内核层就被过滤；迟到的本次回包按序号拒收并计入 `stray_replies`。
- **配置失败保留原状态**：配置先经 Pydantic 校验，通过才原子替换并递增 epoch。
- **聚合接收逐包判定**：UDP_GRO 聚合数据按分段长度还原数据报边界，整组共用收取时的
  配置快照，逐包校验、计数并原样回送，末包更短也不例外。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `ECHO_HOST` | `0.0.0.0` | 回显端监听地址 |
| `ECHO_PORT` | `9999` | 回显端 UDP 端口 |
| `PROBE_TARGET_HOST` | `127.0.0.1` | 探测/试投默认目标（`POST /probe`、`POST /echo/gro-test` 可用 `target_host/target_port` 覆盖，指向真实网关） |

## 项目结构

```
app/
  main.py        # FastAPI 路由与生命周期
  echo_server.py # 本地回显端（UDP_GRO 聚合接收、阈值丢弃/断联、epoch、分组观察日志）
  prober.py      # 二分探测引擎（DF、探测编号、小包复核、作废判定）
  grotest.py     # UDP_SEGMENT 试投引擎（一次发整批、逐序号核对、分组回查）
  packets.py     # 探测报文编解码
  netutil.py     # DF 位设置、UDP_GRO/UDP_SEGMENT 卸载工具
tests/
  test_api.py    # 配置/探测/报文编解码测试
  test_gro.py    # UDP_SEGMENT 试投与 UDP_GRO 聚合接收测试
Dockerfile / docker-compose.yml
```
