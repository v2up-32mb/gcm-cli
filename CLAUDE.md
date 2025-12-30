# GCM - Cloudflare Worker Proxy 客户端 (Go 版本)

## 项目概述

这是 GCM 代理客户端的 Go 语言实现版本，通过 SOCKS5 协议提供本地代理服务，流量经过中转节点转发到 Cloudflare Worker。

**优化目标**: 极致低延迟代理

**与 Node.js 版本的关系**: Go 版本是从 Node.js 版本 (`gcm.cjs`) 移植而来，功能对等，性能更优。

## 技术栈

- **运行环境**: Go 1.24+
- **核心依赖**: `github.com/gorilla/websocket` - WebSocket 客户端
- **包管理器**: Go modules

## 项目结构

```
/
├── main.go              # 主程序入口 (127 行)
├── config/
│   └── config.go        # 配置管理 (551 行)
├── logger/
│   └── logger.go        # 日志系统 (339 行)
├── dns/
│   ├── cache.go         # DNS 缓存管理 (243 行)
│   └── doh.go           # DoH 客户端 (178 行)
├── relay/
│   └── manager.go       # 中转节点管理 (508 行)
├── pool/
│   └── connection.go    # WebSocket 连接池 (1149 行)
├── socks5/
│   └── server.go        # SOCKS5 代理服务器 (395 行)
├── protocol/
│   └── message.go       # 协议编解码 (131 行)
├── metrics/
│   └── server.go        # Prometheus 监控端点 (231 行)
├── go.mod               # 模块依赖定义
├── go.sum               # 依赖锁定文件
└── CLAUDE.md            # 本文档
```

## 核心架构

### 1. 全局配置 (CONFIG)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| **基础配置** | | |
| `workerHost` | `gcm.ics.de5.net` | Cloudflare Worker 地址 |
| `localPort` | `1080` | 本地 SOCKS5 监听端口 |
| `proxyToken` | `""` | 访问令牌（可选） |
| **连接池配置** | | |
| `minPoolSize` | `3` | 最小连接池大小（Go 版本默认较小）|
| `maxPoolSize` | `15` | 最大并发连接数 |
| `connectionTTL` | `300000` | 连接最大存活时间 (ms, 5分钟) |
| `connectionTimeout` | `1000` | 连接超时时间 (ms, Go 版本默认 1 秒) |
| **中转节点配置** | | |
| `relayIPs` | 见配置 | 中转节点列表 (IP/域名:端口) |
| `relayMonitorInterval` | `30000` | 节点延迟监控间隔 (ms) |
| `relayMaxLatency` | `500` | 节点最大可接受延迟 (ms, Go 版本更严格) |
| `relayFailureThreshold` | `3` | 节点连续失败次数阈值 |
| **DNS 缓存配置** | | |
| `enableDoH` | `true` | 是否开启 DoH |
| `dohUrl` | `https://1.1.1.1/dns-query` | DoH 服务地址 |
| `dohThroughProxy` | `true` | DoH 是否通过代理 |
| `dnsCacheTTL` | `1800000` | DNS 缓存过期时间 (30分钟) |
| **心跳保活配置** | | |
| `heartbeatInterval` | `15000` | 心跳间隔 (15秒) |
| `heartbeatTimeout` | `3000` | 心跳响应超时 (3秒) |
| **多路复用配置** | | |
| `enableMultiplex` | `true` | 是否启用多路复用（Go 版本默认启用）|
| `maxStreamsPerConnection` | `5` | 每个连接最大并发流数 |
| **目标地址亲和性** | | |
| `enableAffinity` | `true` | 是否启用目标地址亲和性（Go 版本默认启用）|
| `affinityTTL` | `600000` | 亲和性缓存过期时间 (10分钟) |
| **监控指标配置** | | |
| `enableMetrics` | `false` | 是否启用监控指标 |
| `metricsPort` | `9090` | 监控指标暴露端口 |


### 2. 核心模块

#### Logger (logger/logger.go)
- **功能**: 分级日志系统（可采用成熟的日志库）
- **特性**:
  - 支持 DEBUG/INFO/WARN/ERROR 四级日志
  - 可选文件日志输出（支持轮转）
  - 按作用域分组的日志器

#### DNSCache (dns/cache.go)
- **功能**: DNS 解析结果缓存管理器
- **特性**:
  - LRU 风格缓存，支持 TTL 过期
  - 自动清理过期条目
  - 命中率统计
  - 预热常用域名功能（根据gfwlist）

#### DoHClient (dns/doh.go)
- **功能**: DNS over HTTPS 客户端
- **作用**: 安全解析域名，防止 DNS 污染
- **支持**: A 记录 (IPv4) 和 AAAA 记录 (IPv6)
- **超时**: 3 秒超时，快速失败

#### RelayManager (relay/manager.go)
- **功能**: 中转节点管理器（动态优选版）
- **特性**:
  - 支持多种格式: IP, IP:Port, Domain, Domain:Port
  - **最低延迟优先策略**
  - **持续监控**: 定期抽查节点
  - **失败惩罚**: 连续失败 3 次自动剔除节点
  - **评分机制**: 延迟 + 失败惩罚（每次失败 +500ms）
  - **强制重评**: 连接失败时触发节点重新测速
  - **优化到CDN节点的连接速度**：使用中转节点时，将在底层替换websocket.Dialer.NetDial函数为自定义函数，将中转节点的IP和端口作为参数传递给底层，底层将使用中转节点的IP和端口建立连接，从而优化到CDN节点的连接速度

#### ConnectionPool (pool/connection.go) **[核心模块]**
- **功能**: WebSocket 连接池
- **特性**:
  - 自动维护连接池大小
  - 空闲连接自动清理 (5分钟 TTL)
  - 请求队列机制（防止连接池耗尽报错）
  - **连接池预热**: 启动时并发创建连接
  - **断线重连**: 连接断开时自动补充
  - **动态调整**: 根据负载自动调整 minPoolSize
  - **心跳保活**: 每 15 秒 PING，超时自动剔除
  - **多路复用流管理**: 每个连接支持多个并发流, 每个流有独立的 ID, 每个连接有独立的StreamManager来管理stream流的创建、关闭和消息处理, 如果有成熟的多路复用库，可以直接使用

#### Socks5Server (socks5/server.go)
- **功能**: SOCKS5 代理服务器
- **支持**:
  - CONNECT 命令
  - IPv4 / 域名 / IPv6 地址类型
  - **智能 DNS 预解析（带缓存）**
  - IPv6 格式自动修正
  - **多路复用支持**：从连接池获取stream，对接tcp连接

#### MetricsServer (metrics/server.go)
- **功能**: Prometheus 格式监控指标暴露
- **端点**:
  - `GET /metrics` - Prometheus 格式指标
  - `GET /health` - 健康检查
  - `GET /monitor` - 可视化的，基于图表的metrics信息
- **指标**:
  - 连接池状态（空闲/活跃/建立中/排队）
  - DNS 缓存统计（大小/命中/未命中）
  - 中转节点状态（数量/平均延迟/最佳/最差）
  - 请求统计（成功率、响应时间、数据传输量）

## 协议格式

### Worker 通信协议（二进制格式）

**协议结构**: `[WS_ID:3字节][STREAM_ID:1字节][TYPE:1字节][DATA...]`

| 类型值 | 名称 | 说明 |
|--------|------|------|
| 0 | CONNECT | 发起连接请求 |
| 1 | CONNECTED | 连接建立成功 |
| 2 | DATA | 数据传输 |
| 3 | CLOSE | 关闭连接 |

**示例**:
- 连接请求: `[3字节WSID][1字节StreamID][0x00]host:port|`
- 连接成功: `[3字节WSID][1字节StreamID][0x01]`
- 数据传输: `[3字节WSID][1字节StreamID][0x02][二进制数据]`
- 关闭连接: `[3字节WSID][1字节StreamID][0x03]`

## 功能对比: Go 版本 vs Node.js 版本

| 功能 | Node.js 版本 | Go 版本 | 说明 |
|------|-------------|---------|------|
| **基础功能** |
| SOCKS5 代理 | ✅ | ✅ | 功能对等 |
| WebSocket 连接池 | ✅ | ✅ | Go 版本代码更简洁 |
| 中转节点优选 | ✅ | ✅ | Go 版本默认配置更严格 |
| DoH 防污染 | ✅ | ✅ | 功能对等 |
| IPv6 支持 | ✅ | ✅ | 功能对等 |
| **优化功能** |
| DNS 缓存 | ✅ | ✅ | 功能对等 |
| 动态节点优选 | ✅ | ✅ | Go 版本有强制重评机制 |
| 节点健康检查 | ✅ | ✅ | 功能对等 |
| 连接池扩容 | ✅ | ✅ | Go 版本默认较小 |
| 心跳保活 | ✅ | ✅ | Go 版本超时更短 (3s vs 5s) |
| HTTP Metrics | ✅ | ✅ | 功能对等 |
| TCP_NODELAY | ✅ | ✅ | 功能对等 |
| 连接池预热 | ✅ | ✅ | 功能对等 |
| 断线重连 | ✅ | ✅ | 功能对等 |
| 请求超时控制 | ✅ | ✅ | 功能对等 |
| 动态池调整 | ✅ | ✅ | 功能对等 |
| 日志文件输出 | ✅ | ✅ | 功能对等 |
| 连接池统计增强 | ✅ | ✅ | 功能对等 |
| 多路复用 | ✅ | ✅ | Go 版本默认启用 |
| 命令行参数 | ✅ | ✅ | Go 版本参数更丰富 |
| TLS ECH | ✅ | ❌ | Go 版本暂未实现 |

## 已知问题

### ✅ 预热失败导致服务无法启动 (已解决)

**问题描述**:
当连接池预热失败（例如网络不可达、中转节点不可用）时，SOCKS5 服务端口无法启动监听，程序一直阻塞。

**根本原因**:
`Warmup()` 函数在主线程中同步执行，即使有超时机制，底层的 `dialer.Dial()` 在某些网络场景（如 IPv6）下仍会阻塞超过配置的超时时间。

**修复方案**:
将连接池预热改为**异步执行**，使用 goroutine 在后台运行预热。这样即使预热过程阻塞或失败，SOCKS5 服务器也能立即启动，后续请求会触发按需创建连接。

**测试结果**: ✓ SOCKS5 服务始终能正常启动，预热失败不影响服务可用性

---

### ✅ 连接池负载不均问题 (已解决)

**问题描述**:
当启用多路复用时，所有请求集中在单个活跃连接上，导致 Stream ID 耗尽。

**根本原因**:
`GetConnection` 函数只检查 `streamHandlers`（活跃连接），从未考虑空闲连接池 `p.pool`。

**测试结果**: ✓ 并发请求成功分散到多个连接

---

### ✅ 隧道超时逻辑缺陷 (已解决)

**问题描述**:
`createTunnel` 函数中无条件阻塞等待 `TunnelTimeout`（60秒）。

**修复方案**:
使用 `select` + `done channel` 实现连接成功时立即返回。

**修改位置**: `/root/gcm/go/socks5/server.go`

## 运行方式

### 编译
```bash
cd /root/gcm/go
go build -o gcm-go main.go
```

### 基础运行
```bash
# 使用默认配置
./gcm-go

# 查看帮助
./gcm-go --help
```

### 命令行参数

| 参数 | 简写 | 说明 | 示例 |
|------|------|------|------|
| **配置文件** |
| `--config <file>` | `-c` | 配置文件路径 | `--config my.json` |
| **基本配置** |
| `--worker <host>` | `-w` | Worker 地址 | `--worker example.com` |
| `--port <number>` | `-p` | SOCKS5 监听端口 | `--port 1088` |
| `--token <token>` | `-t` | 代理访问令牌 | `--token abc123` |
| `--log-level <level>` | | 日志级别 | `--log-level DEBUG` |
| **DNS配置** |
| `--doh <url>` | `-d` | DoH 服务地址 | `--doh https://1.1.1.1/dns-query` |
| `--doh-through-proxy` | | 通过代理使用 DoH | `--doh-through-proxy` |
| `--no-doh` | | 禁用 DoH | `--no-doh` |
| **中转节点配置** |
| `--relay <ips>` | `-r` | 中转节点列表 | `--relay "1.1.1.1:443,2.2.2.2:443"` |
| `--min-pool <n>` | | 最小连接池大小 | `--min-pool 10` |
| `--max-pool <n>` | | 最大连接池大小 | `--max-pool 50` |
| **Metrics配置** |
| `--metrics` | | 启用 Metrics 端点 | `--metrics` |
| `--metrics-port <n>` | | Metrics 端口 | `--metrics-port 9090` |
| **连接池配置** |
| `--no-warmup` | | 禁用连接池预热 | `--no-warmup` |
| `--no-reconnect` | | 禁用断线自动重连 | `--no-reconnect` |
| `--no-dynamic-pool` | | 禁用动态池调整 | `--no-dynamic-pool` |
| **日志配置** |
| `--log-file <path>` | | 启用日志文件 | `--log-file ./gcm.log` |
| **隧道配置** |
| `--timeout <seconds>` | | 隧道超时时间 | `--timeout 60` |
| `--no-mux` | | 禁用多路复用 | `--no-mux` |

### 运行示例

```bash
# 自定义 Worker 和端口
./gcm-go -w example.com -p 1088

# 禁用 DoH，启用日志文件
./gcm-go --no-doh --log-file ./gcm.log

# 自定义中转节点和连接池大小
./gcm-go -r "1.1.1.1:443,2.2.2.2:443" --min-pool 20

# 启用 Metrics 端点
./gcm-go --metrics --metrics-port 9090

# DEBUG 级别日志
./gcm-go --log-level DEBUG
```

## Go 版本优势

1. **性能更好**: Go 的 goroutine 并发模型比 Node.js 的事件循环更适合高并发场景
2. **内存占用更低**: 编译后的二进制文件无需 Node.js 运行时
3. **部署更简单**: 单个可执行文件，无需依赖 node_modules
4. **类型安全**: 静态类型检查，减少运行时错误
5. **代码更清晰**: 约 3800 行代码，结构化模块设计

## 开发状态

| 模块 | 状态 | 说明 |
|------|------|------|
| main.go | ❌ 未完成 | 主入口和初始化流程 |
| config | ❌ 未完成 | 配置加载和命令行参数解析 |
| logger | ❌ 未完成 | 分级日志和文件日志 |
| dns | ❌ 未完成 | DoH 客户端和 DNS 缓存 |
| relay | ❌ 未完成 | 中转节点管理和优选 |
| pool | ❌ 未完成 | 连接池核心功能 |
| socks5 | ❌ 未完成 | SOCKS5 代理服务器 |
| protocol | ❌ 未完成 | 消息编解码 |
| metrics | ❌ 未完成 | Prometheus 监控端点 |

## 与 Node.js 版本的差异

### 配置默认值差异

| 配置项 | Node.js | Go | 说明 |
|--------|---------|-----|------|
| minPoolSize | 30 | 3 | Go 版本更保守 |
| maxPoolSize | 999 | 15 | Go 版本限制更严格 |
| relayMaxLatency | 1000ms | 500ms | Go 版本延迟要求更严格 |
| connectionTimeout | 5000ms | 1000ms | Go 版本超时更短 |
| heartbeatTimeout | 5000ms | 3000ms | Go 版本心跳超时更短 |
| enableMultiplex | false | true | Go 版本默认启用多路复用 |
| enableMetrics | true | false | Go 版本默认不启用 |

### 代码结构差异

| 方面 | Node.js | Go |
|------|---------|-----|
| 文件组织 | 单文件 (1900行) | 模块化 (10个文件) |
| 依赖管理 | npm/pnpm | Go modules |
| 并发模型 | 事件循环 + async/await | goroutine + channel |
| 错误处理 | try/catch | 多返回值 |

---

**文档版本**: 1.0
**最后更新**: 2025-12
**维护者**: GCM Team
