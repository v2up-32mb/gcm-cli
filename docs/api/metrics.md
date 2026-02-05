# metrics 包

## 概述

metrics 包提供 Prometheus 格式的监控指标暴露端点。

---

## 类型定义

### Server

```go
type Server struct {
    cfg          *config.Config
    log          *logger.Logger
    pool         *pool.ConnectionPool
    relayManager *relay.RelayManager
    dnsCache     *dns.DNSCache
    server       *http.Server
    requestCount int64
}
```

Metrics HTTP 服务器。

---

## 构造函数

### NewServer

```go
func NewServer(cfg *config.Config, p *pool.ConnectionPool, rm *relay.RelayManager, dc *dns.DNSCache) *Server
```

创建 Metrics 服务器。

---

## 主要方法

### Start

```go
func (s *Server) Start() error
```

启动 Metrics 服务器。

监听端口：`cfg.MetricsPort`（默认 9090）

### Close

```go
func (s *Server) Close() error
```

关闭服务器。

---

## HTTP 端点

### GET /metrics

返回 Prometheus 格式的监控指标。

超时保护：10 秒

#### 指标列表

**连接池指标：**
- `gcm_pool_idle` - 空闲连接数
- `gcm_pool_active` - 活跃连接数
- `gcm_pool_pending` - 正在建立的连接数
- `gcm_pool_queued` - 等待中的请求数
- `gcm_pool_load_rate` - 连接池负载率(%)

**请求统计指标：**
- `gcm_requests_total` - 总请求数
- `gcm_requests_success_total` - 成功请求数
- `gcm_requests_failure_total` - 失败请求数
- `gcm_requests_timeout_total` - 超时请求数
- `gcm_requests_success_rate` - 请求成功率(%)
- `gcm_request_duration_min` - 最低请求延迟(毫秒)
- `gcm_request_duration_max` - 最高请求延迟(毫秒)
- `gcm_request_duration_avg` - 平均请求延迟(毫秒)

**Per-Connection 流量指标：**
- `gcm_conn_bytes_sent{ws_id,relay}` - 发送字节数
- `gcm_conn_bytes_received{ws_id,relay}` - 接收字节数
- `gcm_conn_stream_count{ws_id,relay}` - Stream 数量
- `gcm_conn_rtt{ws_id,relay}` - RTT 延迟(毫秒)

**DNS 缓存指标：**
- `gcm_dns_cache_size` - 缓存条目数
- `gcm_dns_cache_hits_total` - 命中次数
- `gcm_dns_cache_misses_total` - 未命中次数
- `gcm_dns_cache_hit_rate` - 命中率
- `gcm_dns_requests_total` - 总请求数

**全局流量指标：**
- `gcm_bytes_sent_total` - 总发送字节数
- `gcm_bytes_received_total` - 总接收字节数

**连接池创建/关闭指标：**
- `gcm_pool_created_total` - 创建连接总数
- `gcm_pool_closed_total` - 关闭连接总数

**速率统计指标：**
- `gcm_rate_send_avg` - 平均发送速率(字节/秒)
- `gcm_rate_send_max` - 最大发送速率(字节/秒)
- `gcm_rate_recv_avg` - 平均接收速率(字节/秒)
- `gcm_rate_recv_max` - 最大接收速率(字节/秒)

**中转节点指标：**
- `gcm_relay_nodes_total` - 节点总数
- `gcm_relay_nodes_optimal` - 有效节点数
- `gcm_relay_latency_avg` - 平均延迟(毫秒)
- `gcm_relay_latency_best` - 最低延迟(毫秒)
- `gcm_relay_latency_worst` - 最高延迟(毫秒)
- `gcm_relay_total_tests` - 总测速次数
- `gcm_relay_removed_nodes` - 移除节点总数

**负载均衡指标：**
- `gcm_relay_active_connections{relay}` - 节点当前活跃连接数
- `gcm_relay_total_connections{relay}` - 节点累计创建连接数
- `gcm_relay_quality_score{relay}` - 节点平均质量评分(0-100)
- `gcm_relay_weight{relay}` - 节点动态权重

**流控和拥塞控制指标：**
- `gcm_flow_window_avg` - 平均窗口大小(字节)
- `gcm_flow_window_min` - 最小窗口大小(字节)
- `gcm_flow_window_max` - 最大窗口大小(字节)
- `gcm_flow_rtt_avg` - 平均RTT(毫秒)
- `gcm_flow_loss_rate` - 平均丢包率
- `gcm_flow_stream_count` - 活跃Stream数量

**运行时间：**
- `gcm_uptime_seconds` - 运行时间(秒)

### GET /health

健康检查端点。返回 "OK\n"。

### GET /monitor

监控面板页面。返回 `statics/monitor.html`。

超时保护：5 秒

### /static/*

静态文件服务。提供 `statics/` 目录下的文件。
