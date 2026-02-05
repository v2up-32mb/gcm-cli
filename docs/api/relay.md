# relay 包

## 概述

relay 包提供中转节点管理功能，包括节点测速、动态优选、负载均衡、健康检查等。

---

## 类型定义

### RelayNode

```go
type RelayNode struct {
    IP                string
    Port              int
    Source            string        // 来源（原始配置字符串）
    Latency           time.Duration // 延迟
    FailCount         int           // 连续失败次数
    LastCheck         time.Time     // 上次检查时间
    Score             int           // 分数 = 延迟(ms) + 失败惩罚

    // 负载均衡字段
    ActiveConnections int32   // 当前活跃连接数（原子操作）
    TotalConnections  int64   // 累计创建连接数（原子操作）
    AvgQualityScore   float64 // 平均连接质量评分（0-100）
    Weight            float64 // 动态权重（用于加权轮询）
}
```

中转节点。

### RelayManager

```go
type RelayManager struct {
    mu                   sync.RWMutex
    rawRelays            []string         // 原始配置列表
    optimalRelays        []*RelayNode     // 优选节点列表
    allNodes             []*RelayNode     // 所有已配置节点
    isInitialized        bool
    totalTestCount       int
    totalRemovedCount    int
    lastForceRescoreTime time.Time
    cfg                  *config.Config
    dnsCache             *dns.DNSCache
    // ... 内部字段
}
```

中转节点管理器。

### RelayStats

```go
type RelayStats struct {
    TotalNodes   int           // 所有节点数
    OptimalNodes int           // 有效节点数（低延迟）
    TotalTests   int
    Removed      int
    AvgLatency   time.Duration
    BestLatency  time.Duration
    WorstLatency time.Duration
}
```

中转节点统计信息。

### DetailedNodeInfo

```go
type DetailedNodeInfo struct {
    IP                string
    Port              int
    ActiveConnections int32
    TotalConnections  int64
    AvgQualityScore   float64
    Weight            float64
}
```

节点详细信息（用于 Metrics）。

---

## 构造函数

### NewRelayManager

```go
func NewRelayManager(relayList []string, cfg *config.Config, dnsCache *dns.DNSCache) *RelayManager
```

创建中转节点管理器。

---

## 主要方法

### Init

```go
func (rm *RelayManager) Init() error
```

初始化中转节点：
1. 解析所有输入（IP 直接使用，域名解析并测速优选 Top 2）
2. 批量测速
3. 按延迟过滤（仅保留低于阈值的节点）
4. 启动后台定期重评循环

### GetNextRelay

```go
func (rm *RelayManager) GetNextRelay() *RelayNode
```

获取最优节点（最低延迟优先）。

### GetCurrentBest

```go
func (rm *RelayManager) GetCurrentBest() *RelayNode
```

获取当前最优节点（同 GetNextRelay）。

### GetBestRelayExcluding

```go
func (rm *RelayManager) GetBestRelayExcluding(excludeAddr string) *RelayNode
```

获取最优节点（排除指定地址）。

### GetNextRelayWithLoadBalance

```go
func (rm *RelayManager) GetNextRelayWithLoadBalance() *RelayNode
```

负载均衡选择节点（加权随机）。

权重计算公式：
```
权重 = 基础权重 × 负载因子 × 质量因子
```

其中：
- 基础权重 = 1000 / (延迟ms + 1)
- 负载因子 = 1.0 - (当前连接数 / 最大连接数) × 0.5
- 质量因子 = 平均质量评分 / 100

### ReportFailure

```go
func (rm *RelayManager) ReportFailure(ip string, port int)
```

记录节点失败。连续失败达到阈值后自动移除节点。

### ForceRescore

```go
func (rm *RelayManager) ForceRescore() bool
```

强制重新评分所有节点。带防抖保护（冷却期）。

### GetStats

```go
func (rm *RelayManager) GetStats() RelayStats
```

获取统计信息。

### GetDetailedNodes

```go
func (rm *RelayManager) GetDetailedNodes() []DetailedNodeInfo
```

获取所有节点的详细信息（用于 Metrics）。

### UpdateNodeLoad

```go
func (rm *RelayManager) UpdateNodeLoad(ip string, port int, delta int32)
```

更新节点负载信息（原子操作）。

### UpdateNodeQuality

```go
func (rm *RelayManager) UpdateNodeQuality(ip string, port int, score float64)
```

更新节点质量评分（使用 EMA 平滑）。

新评分 = 0.7 × 旧评分 + 0.3 × 新评分

### Close

```go
func (rm *RelayManager) Close()
```

关闭管理器（停止后台循环）。

---

## 工具函数

### ParseHostPort

```go
func ParseHostPort(input string) (host string, port int)
```

解析 "host:port" 或 "[ipv6]:port" 或 "host"。

默认端口：443
