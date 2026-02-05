# GCM Go 版本 API 文档

## 项目概述

GCM (Cloudflare Worker Proxy 客户端) 的 Go 语言实现，通过 SOCKS5 协议提供本地代理服务，流量经过中转节点转发到 Cloudflare Worker。

**优化目标**: 极致低延迟代理

---

## 模块列表

### 核心模块

| 模块 | 文档 | 描述 |
|------|------|------|
| [main](api/main.md) | [文档](api/main.md) | 程序入口，负责初始化和启动所有模块 |
| [config](api/config.md) | [文档](api/config.md) | 配置管理（结构体、命令行参数、配置文件加载） |
| [logger](api/logger.md) | [文档](api/logger.md) | 分级日志系统（控制台+文件日志） |

### 网络模块

| 模块 | 文档 | 描述 |
|------|------|------|
| [socks5](api/socks5.md) | [文档](api/socks5.md) | SOCKS5 代理服务器实现 |
| [pool](api/pool.md) | [文档](api/pool.md) | WebSocket 连接池、Stream 多路复用、HTTP over WebSocket |
| [protocol](api/protocol.md) | [文档](api/protocol.md) | Worker 通信协议消息格式和编解码 |

### 支持模块

| 模块 | 文档 | 描述 |
|------|------|------|
| [dns](api/dns.md) | [文档](api/dns.md) | DoH 客户端和 DNS 缓存 |
| [relay](api/relay.md) | [文档](api/relay.md) | 中转节点管理（测速、优选、负载均衡） |
| [ech](api/ech.md) | [文档](api/ech.md) | TLS ECH 配置管理 |
| [metrics](api/metrics.md) | [文档](api/metrics.md) | Prometheus 格式监控指标暴露 |

---

## 项目结构

```
/root/projects/gcm/go/
├── main.go                 # 主程序入口
├── config/
│   ├── config.go           # 配置结构体定义
│   ├── flags.go            # 命令行参数定义
│   └── loader.go           # 配置加载逻辑
├── logger/
│   └── logger.go           # 日志系统
├── dns/
│   ├── cache.go            # DNS 缓存管理
│   ├── doh.go              # DoH 客户端
│   └── warmup_list.go      # DNS 预热域名列表
├── relay/
│   └── manager.go          # 中转节点管理
├── pool/
│   ├── connection.go       # WebSocket 连接池
│   ├── stream_manager.go   # Stream 多路复用管理
│   ├── proxy_transport.go  # HTTP over WebSocket
│   ├── traffic_counter.go  # 流量统计
│   ├── quality_monitor.go  # 连接质量监控
│   └── session_rotator.go  # 会话轮换
├── socks5/
│   └── server.go           # SOCKS5 代理服务器
├── protocol/
│   └── message.go          # 协议编解码
├── metrics/
│   └── server.go           # Prometheus 监控端点
├── ech/
│   └── manager.go          # ECH 配置管理
└── docs/
    └── api/                # API 文档（本目录）
```

---

## 核心架构

### 连接池 (pool)

```
ConnectionPool
    ├── 空闲池 (pool)
    ├── 活跃连接 (managerByConn)
    ├── Stream 多路复用
    ├── 连接质量监控
    └── 会话轮换
```

### 请求流程

```
客户端 → SOCK5 → DNS缓存 → 连接池获取连接 → 分配StreamID
    → WebSocket → 中转节点 → Cloudflare Worker → 目标服务器
```

### 消息协议

```
[WS_ID:3字节][STREAM_ID:1字节][TYPE:1字节][DATA...]
```

---

## 快速链接

- [配置参考](api/config.md)
- [连接池 API](api/pool.md)
- [协议格式](api/protocol.md)
- [Metrics 指标](api/metrics.md)

---

**文档版本**: 1.0
**最后更新**: 2025-02-05
