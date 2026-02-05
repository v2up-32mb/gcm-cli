# ech 包

## 概述

ech 包提供 TLS ECH (Encrypted Client Hello) 配置管理功能，支持配置缓存和定时刷新。

---

## 类型定义

### EchManager

```go
type EchManager struct {
    mu              sync.RWMutex
    cache           map[string]*cacheEntry
    echDomain       string                       // ECH 查询域名
    dohFunc         func(string) ([]byte, error) // DoH 查询函数
    cacheTTL        time.Duration                // 缓存 TTL
    refreshInterval time.Duration                // 定时刷新间隔
    // ... 内部字段
}
```

ECH 配置管理器。

### cacheEntry

```go
type cacheEntry struct {
    echConfig []byte    // ECH 配置字节
    expiresAt time.Time // 过期时间
}
```

ECH 缓存条目。

### flight

```go
type flight struct {
    wg    sync.WaitGroup
    value []byte
    err   error
}
```

代表一个正在进行的 ECH 配置查询（用于 singleFlight 防止缓存击穿）。

---

## 构造函数

### NewEchManager

```go
func NewEchManager(dohClient *dns.DoHClient, echDomain string, cacheTTL time.Duration, refreshInterval time.Duration) *EchManager
```

创建 ECH 管理器。

参数：
- `dohClient` - DoH 客户端实例
- `echDomain` - ECH 查询域名
- `cacheTTL` - 缓存过期时间（默认 24 小时）
- `refreshInterval` - 定时刷新间隔（默认 12 小时，0 表示禁用）

---

## 主要方法

### GetTlsConfig

```go
func (em *EchManager) GetTlsConfig(domain string, useEch bool) (*tls.Config, error)
```

获取 TLS 配置。

参数：
- `domain` - 目标域名
- `useEch` - 是否启用 ECH

如果 ECH 获取失败，自动回退到标准 TLS（不返回错误）。

### Refresh

```go
func (em *EchManager) Refresh(domain string) error
```

强制刷新指定域名的 ECH 配置。

### ClearCache

```go
func (em *EchManager) ClearCache()
```

清空所有缓存。

### GetCacheStats

```go
func (em *EchManager) GetCacheStats() (total int, expired int)
```

获取缓存统计信息。返回：`(总条目数, 过期条目数)`。

### CleanupExpired

```go
func (em *EchManager) CleanupExpired() int
```

清理过期的缓存条目。返回：清理的条目数。

### StartAutoRefresh

```go
func (em *EchManager) StartAutoRefresh()
```

启动定时刷新任务（后台运行）。

### StopAutoRefresh

```go
func (em *EchManager) StopAutoRefresh()
```

停止定时刷新任务。

---

## 内部方法

### getECHConfig

```go
func (em *EchManager) getECHConfig(domain string) ([]byte, error)
```

获取 ECH 配置（带缓存和 singleFlight 防止击穿）。

### fetchAndCache

```go
func (em *EchManager) fetchAndCache(domain string) ([]byte, error)
```

从 DoH 查询 ECH 配置并缓存。

### autoRefreshLoop

```go
func (em *EchManager) autoRefreshLoop()
```

定时刷新循环（后台运行）。

### refreshAllCached

```go
func (em *EchManager) refreshAllCached()
```

刷新所有缓存的 ECH 配置。

---

## 特性

1. **缓存机制**：自动缓存 ECH 配置，避免重复查询
2. **SingleFlight**：防止缓存击穿，多个并发请求共享同一个查询
3. **定时刷新**：支持定时自动刷新 ECH 配置
4. **自动回退**：ECH 获取失败时自动回退到标准 TLS
