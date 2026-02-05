# socks5 包

## 概述

socks5 包实现 SOCKS5 代理服务器，支持 CONNECT 命令和 IPv4/域名/IPv6 地址类型。

---

## 类型定义

### Server

```go
type Server struct {
    cfg           *config.Config
    log           *logger.Logger
    pool          *pool.ConnectionPool
    dnsCache      *dns.DNSCache
    server        net.Listener
    activeTunnels int32
}
```

SOCKS5 代理服务器。

---

## 常量

```go
const (
    socks5Version = 0x05
    authNone      = 0x00
    cmdConnect    = 0x01
    atypIPv4      = 0x01
    atypDomain    = 0x03
    atypIPv6      = 0x04
)
```

SOCKS5 协议常量。

---

## 构造函数

### NewServer

```go
func NewServer(cfg *config.Config, p *pool.ConnectionPool, dc *dns.DNSCache) *Server
```

创建 SOCKS5 服务器。

---

## 主要方法

### Start

```go
func (s *Server) Start() error
```

启动 SOCKS5 服务器（开始监听）。

### Close

```go
func (s *Server) Close() error
```

关闭服务器。

---

## 内部方法

### handleConnection

```go
func (s *Server) handleConnection(clientConn net.Conn)
```

处理客户端连接：
1. 认证阶段
2. 请求阶段
3. 创建隧道

### handleAuth

```go
func (s *Server) handleAuth(conn net.Conn) error
```

处理 SOCKS5 认证（当前仅支持无认证方式）。

### handleRequest

```go
func (s *Server) handleRequest(conn net.Conn) (originalHost, resolvedHost string, port uint16, err error)
```

处理 SOCKS5 请求。

返回：`(原始主机名, 解析后的主机, 端口, 错误)`

支持的地址类型：
- IPv4 (atypIPv4)
- 域名 (atypDomain) - 支持 DNS 预解析
- IPv6 (atypIPv6)

### createTunnel

```go
func (s *Server) createTunnel(clientConn net.Conn, originalHost, resolvedHost string, port uint16)
```

创建代理隧道。

流程：
1. 从连接池获取连接并分配 Stream ID
2. 发送 CONNECT 消息到 Worker
3. 等待 CONNECTED 响应
4. 双向转发数据
5. 处理窗口流控
6. 关闭时发送 CLOSE 消息

---

## 协议交互流程

```
客户端 → SOCKS5 服务器 → WebSocket → Worker → 目标服务器

1. 认证阶段:
   客户端: [版本, 方法数量, 方法列表]
   服务器: [版本, 选择的方法]

2. 请求阶段:
   客户端: [版本, 命令, 保留, 地址类型, 地址..., 端口]
   服务器: [版本, 状态, 保留, 地址类型, 绑定地址..., 绑定端口]

3. 数据传输:
   客户端 ←→ SOCKS5 服务器 ←→ WebSocket ←→ Worker ←→ 目标服务器
```
