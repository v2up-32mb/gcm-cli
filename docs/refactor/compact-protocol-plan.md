# 协议精简重构计划

> 分支: `refactor/compact-protocol`（基于 `main`）
> 创建时间: 2026-07-28
> 状态: **待审查**（审查通过后方可开始实施）

---

## 一、背景与动机

### 1.1 现有协议格式

当前 worker.js 和 Go 客户端使用 **5 字节定长头** 的二进制多路复用协议:

```
[WS_ID:3字节][STREAM_ID:1字节][TYPE:1字节][可选DATA]
```

| TYPE | 含义 | DATA 内容 |
|------|------|-----------|
| 0 CONNECT | 发起连接 | ASCII `host:port\|` |
| 1 CONNECTED | 连接成功 | 无 |
| 2 DATA | 数据传输 | 任意二进制 |
| 3 CLOSE | 关闭流 | 无 |

### 1.2 问题分析

1. **WS_ID (3字节) 完全冗余**
   - 免费版 CF Worker 每条 WS 连接对应一个实例，不存在跨连接复用场景
   - Worker 侧 `StreamManager` 从客户端首条消息"提取" WS_ID 后，仅在回包中原样抄回，从不用于路由/查找
   - 客户端侧 `ConnectionID` 作为内部连接标识用于日志/监控，与协议层的 WS_ID 是两个概念，不应绑定
   - 3 字节头开销在大量小包场景（SSH 交互、TCP keepalive）累积可观

2. **TYPE 半字节即可覆盖**
   - 当前仅 4 种消息类型（0–3），4 bit (0–15) 绰绰有余
   - 即便未来扩展 ERROR(4)、PING/PONG 等，4 bit 仍可容纳 16 种类型

3. **STREAM_ID 半字节 (16路) 偏紧**
   - 代理场景一个浏览器页面可能同时打开 10+ 连接
   - 当前 `maxStreamsPerConnection: 256`（1 字节全量）
   - 砍到 4 bit（16路）可能导致频繁拒绝连接，不建议

### 1.3 设计决策

**采用 2 字节头方案**（方案 B）:

```
[STREAM_ID:1字节][TYPE:1字节][可选DATA]
```

- 去掉 WS_ID，头从 5 字节降到 2 字节
- STREAM_ID 保留 1 字节（256 路并发，与现有 maxStreams 一致）
- TYPE 保留 1 字节（编码简单，字节对齐，未来扩展无压力）
- 相比把 TYPE 塞进半字节的 1 字节方案，2 字节方案编程更简单、错误率更低、扩展余量更充足

---

## 二、影响范围清单

### 2.1 协议层（核心变更）

| 文件 | 变更内容 |
|------|----------|
| `protocol/message.go` | `HeaderSize` 5→2；`Message` 结构体移除 `WSID` 字段；`Encode/Decode` 重写；`NewMessage/NewConnectMessage/NewDataMessage/NewCloseMessage` 签名移除 `wsID` 参数 |
| `protocol/message_test.go`（新建） | 补充新旧格式编解码单元测试 |

### 2.2 连接池层

| 文件 | 变更内容 |
|------|----------|
| `pool/connection.go` | `ConnItem.ConnectionID` 字段保留（内部标识用），但从协议编码路径中移除；`generateWSID()` 保留用于内部标识；所有构造协议消息的调用点移除 wsID 参数 |
| `pool/connection.go` | `formatConnID()` 保留不变（内部日志用） |
| `pool/proxy_transport.go` | `NewDataMessage` / `NewCloseMessage` 调用移除 `connItem.ConnectionID` 参数 |
| `pool/stream_manager.go` | 消息分发逻辑适配新 `Message` 结构体（无 WSID 字段） |
| `pool/stream_manager_test.go` | 适配新协议格式 |
| `pool/traffic_counter.go` | `ConnStats.WSID` 字段可保留（内部统计用，不涉及协议编码）或同步重命名为 `ConnID`，视审查意见决定 |

### 2.3 SOCKS5 层

| 文件 | 变更内容 |
|------|----------|
| `socks5/server.go` | `NewConnectMessage` 调用移除 wsID 参数；消息读取适配新 `Message` 结构体 |

### 2.4 监控层

| 文件 | 变更内容 |
|------|----------|
| `metrics/server.go` | `formatConnID()` 保留（与 pool 包重复定义，可考虑后续去重，但不在本次重构范围）；`ConnectionDetail` 结构体中 WSID 字段若仅用于展示则保留 |

### 2.5 中转层

| 文件 | 变更内容 |
|------|----------|
| `relay/manager.go` | 检查是否有协议消息构造/解码调用（初步看无直接依赖，待确认）|

### 2.6 服务端（worker.js）

| 文件 | 变更内容 |
|------|----------|
| `worker.js` | `HEADER_LEN` 5→2；解析逻辑 `slice(0,3)` 去掉、streamId 从 `[3]` 改为 `[0]`、type 从 `[4]` 改为 `[1]`；所有回包构造去掉 `wsId` 前缀；移除 `setWsId/getWsId` 逻辑 |

### 2.7 生成代码

| 文件 | 变更内容 |
|------|----------|
| `gen/gcm/v1/proto/gcm.pb.go` | protobuf 生成代码，含 `ws_id` 字段。本次重构是否同步更新 proto 定义和生成代码，待审查确认 |

---

## 三、实施步骤

### Phase 0: 准备与基线
- [ ] 确认当前测试全部通过：`go test ./...`
- [ ] 记录当前协议格式的行为基线（可选：抓包样本）

### Phase 1: Go 客户端协议层重构
- [ ] 修改 `protocol/message.go`
  - `HeaderSize` 改为 2
  - `Message` 结构体移除 `WSID` 字段
  - 重写 `Encode()`：`[StreamID][Type][Data]`
  - 重写 `Decode()`：从 `data[0]` 读 StreamID，`data[1]` 读 Type，`data[2:]` 读 Data
  - 移除 `NewMessage` 的 `wsID` 参数
  - 移除 `NewConnectMessage` / `NewDataMessage` / `NewCloseMessage` 的 `wsID` 参数
- [ ] 新建 `protocol/message_test.go`，覆盖编码/解码边界用例

### Phase 2: 连接池层适配
- [ ] 修改 `pool/proxy_transport.go`
  - `NewDataMessage(connItem.ConnectionID, streamID, b)` → `NewDataMessage(streamID, b)`
  - `NewCloseMessage(connItem.ConnectionID, streamID)` → `NewCloseMessage(streamID)`
  - 移除注释中的 wsID 相关说明
- [ ] 修改 `pool/connection.go`
  - 消息解码后的分发逻辑适配（`msg.StreamID` / `msg.Type` 不变，`msg.WSID` 引用移除）
  - `generateWSID()` 保留（内部标识用），但不再传入协议消息
- [ ] 修改 `pool/stream_manager.go`
  - 消息分发逻辑适配新 Message 结构体
- [ ] 运行 `pool/stream_manager_test.go` 确认通过

### Phase 3: SOCKS5 层适配
- [ ] 修改 `socks5/server.go`
  - `NewConnectMessage(connItem.ConnectionID, streamID, host, port)` → `NewConnectMessage(streamID, host, port)`
  - 所有 `msg.WSID` / `msg.StreamID` / `msg.Type` 引用适配
  - `wsID := connItem.ConnectionID` 相关行移除

### Phase 4: worker.js 同步更新
- [ ] 修改 `worker.js`
  - `HEADER_LEN = 5` → `HEADER_LEN = 2`
  - 消息解析：`streamId = uint8Array[0]`，`msgType = uint8Array[1]`，`payload = uint8Array.slice(2)`
  - 移除 `setWsId` / `getWsId` / `this.wsId` 逻辑
  - 所有回包构造（`sendConnected` / `sendData` / `sendClose`）去掉 `wsId` 前缀
  - `pumpRemoteToWebSocket` 签名移除 `wsId` 参数
  - `handleSession` 中移除 wsId 相关代码

### Phase 5: 辅助文件清理
- [ ] `pool/traffic_counter.go`：`ConnStats.WSID` → 重命名为 `ConnID`（纯内部，无协议影响）
- [ ] 确认 `relay/manager.go` 无协议依赖
- [ ] 确认 `metrics/server.go` 的 `formatConnID` 不受影响
- [ ] 确认 `gen/gcm/v1/proto/gcm.pb.go` 是否需要更新（待审查决定）

### Phase 6: 验证
- [ ] `go vet ./...`
- [ ] `go build ./...`
- [ ] `go test ./...`
- [ ] 手动端到端测试：Go 客户端 + 精简后的 worker.js 能正常代理
- [ ] 可选：抓包确认新协议格式为 2 字节头

---

## 四、不变更的内容

以下内容**不在本次重构范围内**，保持不变：

| 内容 | 原因 |
|------|------|
| `ConnItem.ConnectionID` 字段 | 内部连接标识，用于日志/监控/亲和性，与协议解耦后保留 |
| `generateWSID()` 方法 | 生成内部连接 ID，仍用于 `ConnItem.ConnectionID` 赋值 |
| `formatConnID()` 函数 | 日志/监控格式化，纯内部使用 |
| `maxStreamsPerConnection` 配置 | 仍为 256，与新协议的 1 字节 StreamID 一致 |
| CONNECT 消息的 DATA 格式 | 仍为 ASCII `host:port\|` |
| WebSocket 传输方式 | 仍为 BinaryMessage |
| 连接池/中转/DNS/ECH 等上层逻辑 | 不涉及协议编解码的逻辑不动 |
| `metrics/server.go` 重复的 `formatConnID` | 留到后续清理，不在本次范围 |

---

## 五、风险与回滚

### 5.1 风险

1. **协议不兼容**：新协议与旧版 worker.js / 旧版 Go 客户端完全不兼容。升级时需 client + worker 同步部署。
2. **遗漏调用点**：grep 覆盖了所有 `.go` 文件，但仍可能在非显式调用的动态路径中遗漏。
3. **protobuf 生成代码**：`gen/gcm/v1/proto/gcm.pb.go` 含 `ws_id` 字段，若不同步更新可能导致 `mux-on-grpc` 相关引用不一致——但该分支计划删除，影响可控。

### 5.2 回滚策略

- 所有变更仅在 `refactor/compact-protocol` 分支上
- 若验证失败，直接 `git checkout main` 即可回滚
- worker.js 变更也在同分支，旧的 worker.js 在 main 分支保留

---

## 六、预期收益

| 指标 | 旧协议 | 新协议 | 变化 |
|------|--------|--------|------|
| 协议头大小 | 5 字节 | 2 字节 | **-60%** |
| CONNECT 消息开销 | 5 + len(host:port\|) | 2 + len(host:port\|) | 每 CONNECT 省 3 字节 |
| DATA 消息开销 | 5 + len(data) | 2 + len(data) | 每 DATA 包省 3 字节 |
| CLOSE 消息开销 | 5 字节 | 2 字节 | 省 3 字节 |
| 代码复杂度 | Message 含 WSID + 处处传参 | Message 无 WSID + 参数简化 | **显著降低** |

在大量小包场景（SSH 交互、TCP keepalive、HTTP 长轮询）下，3 字节/包的节省在加密隧道中会叠加放大，对延迟敏感型代理有实际意义。

---

## 七、待审查确认项

1. **2 字节头方案确认**：`[STREAM_ID:1B][TYPE:1B]`，不采用更激进的 1 字节复合方案
2. **protobuf 生成代码**：`gen/gcm/v1/proto/gcm.pb.go` 是否纳入本次更新（该文件含 `ws_id` 字段）
3. `ConnStats.WSID` → `ConnID` 重命名是否纳入本次
4. `metrics/server.go` 中重复的 `formatConnID` 是否在本次合并去重
5. worker.js 的 fallback IP 逻辑是否保留（与协议头无关，但趁此机会确认）

---

*本计划仅为设计方案，审查通过后开始实施。*
