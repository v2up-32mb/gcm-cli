# pool 包

## 概述

pool 包提供 WebSocket 连接池管理、Stream 多路复用、HTTP over WebSocket 传输等功能。

## 文件结构

- `connection.go` - 连接池核心管理
- `stream_manager.go` - Stream 多路复用
- `proxy_transport.go` - HTTP over WebSocket
- `traffic_counter.go` - 流量统计
- `quality_monitor.go` - 连接质量监控
- `session_rotator.go` - 会话轮换

---

## connection.go

### 类型定义

#### ConnItem

```go
type ConnItem struct {
    WS              *websocket.Conn
    ConnectionID    [3]byte
    RelayAddr       string
    CreatedAt       time.Time
    Traffic         *TrafficCounter
    BaselineRTT     time.Duration
    QualityScore    int64
    IsDegraded      bool
    // ... 内部字段
}
```

WebSocket 连接项，代表池中的一个连接。

#### ConnectionPool

```go
type ConnectionPool struct {
    pool              []*ConnItem
    managerByConn     map[*ConnItem]*StreamManager
    relayManager      *relay.RelayManager
    echManager        *ech.EchManager
    // ... 配置和统计字段
}
```

WebSocket 连接池。

### 构造函数

#### NewConnectionPool

```go
func NewConnectionPool(cfg *config.Config, rm *relay.RelayManager, em *ech.EchManager) *ConnectionPool
```

创建新的连接池。

### 主要方法

#### Start

```go
func (p *ConnectionPool) Start()
```

启动连接池（启动维护循环）。

#### Close

```go
func (p *ConnectionPool) Close()
```

关闭连接池并清理资源。

#### GetConnection

```go
func (p *ConnectionPool) GetConnection(ctx context.Context) (*ConnItem, error)
```

从池中获取一个连接（阻塞等待）。

#### GetConnectionWithStream

```go
func (p *ConnectionPool) GetConnectionWithStream(ctx context.Context, targetAddr string) (*ConnItem, byte, error)
```

获取连接并分配 Stream ID，返回 `(连接, StreamID, 错误)`。

#### ReleaseConnection

```go
func (p *ConnectionPool) ReleaseConnection(conn *ConnItem)
```

将连接放回池中。

#### RegisterStreamHandler

```go
func (p *ConnectionPool) RegisterStreamHandler(conn *ConnItem, streamID byte, handler *StreamHandler, targetAddr string)
```

注册 Stream 消息处理器。

#### UnregisterStreamHandler

```go
func (p *ConnectionPool) UnregisterStreamHandler(conn *ConnItem, streamID byte) (targetAddr string, isEmpty bool)
```

注销 Stream 处理器，返回目标地址和连接是否为空。

#### Warmup

```go
func (p *ConnectionPool) Warmup() error
```

预热连接池（并发创建 minPoolSize 个连接）。

#### GetStats

```go
func (p *ConnectionPool) GetStats() PoolStats
```

获取连接池统计信息。

#### GetEnhancedStats

```go
func (p *ConnectionPool) GetEnhancedStats() EnhancedPoolStats
```

获取增强统计信息（包含请求成功率等）。

#### UpdateAffinityScore

```go
func (p *ConnectionPool) UpdateAffinityScore(conn *ConnItem, targetAddr string, success bool)
```

更新亲和性分数。

---

## stream_manager.go

### 类型定义

#### Stream

```go
type Stream struct {
    ID               byte
    TargetAddr       string
    CreatedAt        time.Time
    RecvWindow       int64
    SendWindow       int64
    MinWindow        int64
    MaxWindow        int64
    TotalBytesRecv   int64
    TotalBytesSent   int64
    TotalLossCount   int64
    TimeoutCount     int64
    SuccessCount     int64
    // ... 内部字段
}
```

Stream 表示多路复用中的一个流。

#### StreamHandler

```go
type StreamHandler struct {
    OnMessage func(msg *protocol.Message)
    OnClose   func()
    OnCleanup func()
}
```

Stream 消息处理器回调。

#### StreamManager

```go
type StreamManager struct {
    conn            *ConnItem
    streams         map[byte]*Stream
    streamHandlers  map[byte]*StreamHandler
    affinityMapping map[string]byte  // targetAddr -> streamID
    maxStreams      int
    // ... 内部字段
}
```

Stream 管理器，负责多路复用。

### 构造函数

#### NewStreamManager

```go
func NewStreamManager(conn *ConnItem, maxStreams int, defaultWindowSize, minWindowSize, maxWindowSize int64, windowTimeout time.Duration) *StreamManager
```

创建新的 Stream 管理器。

### 主要方法

#### RegisterStream

```go
func (sm *StreamManager) RegisterStream(streamID byte, targetAddr string) *Stream
```

注册新的 Stream。

#### UnregisterStream

```go
func (sm *StreamManager) UnregisterStream(streamID byte)
```

注销 Stream。

#### GetStream

```go
func (sm *StreamManager) GetStream(streamID byte) *Stream
```

获取指定 Stream。

#### GetStreamByAddr

```go
func (sm *StreamManager) GetStreamByAddr(targetAddr string) *Stream
```

根据目标地址获取 Stream（亲和性路由）。

#### AllocateStreamID

```go
func (sm *StreamManager) AllocateStreamID() (byte, error)
```

分配新的 Stream ID。

#### GetStreamCount

```go
func (sm *StreamManager) GetStreamCount() int
```

获取当前活跃 Stream 数量。

#### HandleMessage

```go
func (sm *StreamManager) HandleMessage(msg *protocol.Message)
```

处理接收到的消息。

---

## proxy_transport.go

### 类型定义

#### ProxyTransport

```go
type ProxyTransport struct {
    pool *ConnectionPool
}
```

实现 `http.RoundTripper` 接口，通过 WebSocket 隧道传输 HTTP 请求。

### 构造函数

#### NewProxyTransport

```go
func NewProxyTransport(pool *ConnectionPool) *ProxyTransport
```

创建新的 ProxyTransport。

### 方法

#### RoundTrip

```go
func (pt *ProxyTransport) RoundTrip(req *http.Request) (*http.Response, error)
```

实现 http.RoundTripper 接口，通过 WebSocket 隧道发送 HTTP 请求。

---

## traffic_counter.go

### 类型定义

#### TrafficCounter

```go
type TrafficCounter struct {
    bytesSent     int64
    bytesRecv     int64
    streamCount   int64   // 累计 Stream 总数
    activeStreams int64   // 当前活跃 Stream 数
    avgSendRate   float64 // 平均发送速率
    maxSendRate   float64 // 最大发送速率
    avgRecvRate   float64 // 平均接收速率
    maxRecvRate   float64 // 最大接收速率
    // ... 内部字段
}
```

原子流量计数器。

### 方法

#### AddSent

```go
func (c *TrafficCounter) AddSent(n int64)
```

增加发送字节数。

#### AddRecv

```go
func (c *TrafficCounter) AddRecv(n int64)
```

增加接收字节数。

#### IncStream

```go
func (c *TrafficCounter) IncStream()
```

增加 Stream 计数。

#### DecStream

```go
func (c *TrafficCounter) DecStream()
```

减少活跃 Stream 计数。

#### GetSnapshot

```go
func (c *TrafficCounter) GetSnapshot() (sent, recv, streams int64)
```

获取当前快照。

#### UpdateRates

```go
func (c *TrafficCounter) UpdateRates(now time.Time)
```

更新速率统计（应定期调用）。

#### GetRateSnapshot

```go
func (c *TrafficCounter) GetRateSnapshot() (avgSent, maxSent, avgRecv, maxRecv float64)
```

获取速率统计快照。

---

## quality_monitor.go

### 类型定义

#### ConnectionQualityMonitor

```go
type ConnectionQualityMonitor struct {
    pool             *ConnectionPool
    checkInterval    time.Duration
    degradeThreshold int64
    switchCooldown   time.Duration
    // ... 内部字段
}
```

连接质量监控器。

### 构造函数

#### NewConnectionQualityMonitor

```go
func NewConnectionQualityMonitor(pool *ConnectionPool, cfg *config.Config, log *logger.Logger) *ConnectionQualityMonitor
```

创建质量监控器。

### 方法

#### Start

```go
func (m *ConnectionQualityMonitor) Start()
```

启动质量监控循环。

#### Stop

```go
func (m *ConnectionQualityMonitor) Stop()
```

停止质量监控循环。

---

## session_rotator.go

### 类型定义

#### SessionRotator

```go
type SessionRotator struct {
    pool         *ConnectionPool
    maxLifetime  time.Duration
    drainTimeout time.Duration
    // ... 内部字段
}
```

会话轮换器，管理连接的自动轮换。

### 构造函数

#### NewSessionRotator

```go
func NewSessionRotator(pool *ConnectionPool, cfg *config.Config) *SessionRotator
```

创建会话轮换器。

### 方法

#### IsDraining

```go
func (sr *SessionRotator) IsDraining(conn *ConnItem) bool
```

检查连接是否正在排空。

#### ShouldUseConnection

```go
func (sr *SessionRotator) ShouldUseConnection(conn *ConnItem) bool
```

检查连接是否应该被使用（非排空状态）。

#### Stop

```go
func (sr *SessionRotator) Stop()
```

停止轮换器。

#### GetStats

```go
func (sr *SessionRotator) GetStats() map[string]interface{}
```

获取轮换器统计信息。
