# dns 包

## 概述

dns 包提供 DNS over HTTPS (DoH) 客户端和 DNS 缓存功能。

## 文件结构

- `cache.go` - DNS 缓存管理
- `doh.go` - DoH 客户端实现
- `warmup_list.go` - DNS 预热域名列表

---

## cache.go

### 类型定义

#### CacheEntry

```go
type CacheEntry struct {
    IP          string       // A/AAAA 记录的 IP
    HTTPSRecord *HTTPSRecord // HTTPS 记录（可选）
    ExpiresAt   time.Time
}
```

DNS 缓存条目。

#### DNSCache

```go
type DNSCache struct {
    mu              sync.RWMutex
    cache           map[string]*CacheEntry  // key: "domain:type"
    stats           CacheStats
    lastCleanupTime time.Time
    ttl             time.Duration
    cleanupInterval time.Duration
    dohClient       *DoHClient
    // ... 内部字段
}
```

DNS 缓存管理器。

#### CacheStats

```go
type CacheStats struct {
    Hits   int64
    Misses int64
}
```

缓存统计（原子操作计数器）。

#### CacheStatsInfo

```go
type CacheStatsInfo struct {
    Size    int
    Hits    int64
    Misses  int64
    HitRate float64
}
```

缓存统计信息（快照）。

### 构造函数

#### NewDNSCache

```go
func NewDNSCache(cfg *config.Config, dohClient *DoHClient) *DNSCache
```

创建 DNS 缓存。自动启动后台清理循环。

### 主要方法

#### Get

```go
func (dc *DNSCache) Get(domain string, queryType string) (string, bool)
```

获取缓存的 IP。如果缓存过期会自动删除。

#### Set

```go
func (dc *DNSCache) Set(domain string, queryType string, ip string)
```

设置缓存条目。

#### ResolveCached

```go
func (dc *DNSCache) ResolveCached(domain string, queryType string) (string, error)
```

带缓存的解析方法。先查缓存，未命中则执行 DoH 查询。

#### ResolveA

```go
func (dc *DNSCache) ResolveA(domain string) (string, error)
```

解析 A 记录（IPv4），带缓存。

#### ResolveAAAA

```go
func (dc *DNSCache) ResolveAAAA(domain string) (string, error)
```

解析 AAAA 记录（IPv6），带缓存。

#### ResolveAny

```go
func (dc *DNSCache) ResolveAny(domain string) (string, string, error)
```

解析域名（优先 A 记录，失败则尝试 AAAA）。

返回：`(IP, 类型(A/AAAA), 错误)`。

#### SetHTTPS

```go
func (dc *DNSCache) SetHTTPS(domain string, record *HTTPSRecord)
```

缓存 HTTPS 记录。

#### GetHTTPS

```go
func (dc *DNSCache) GetHTTPS(domain string) (*HTTPSRecord, bool)
```

获取缓存的 HTTPS 记录。

#### ResolveHTTPS

```go
func (dc *DNSCache) ResolveHTTPS(domain string) (*HTTPSRecord, error)
```

解析 HTTPS 记录，带缓存。

#### GetECHConfig

```go
func (dc *DNSCache) GetECHConfig(domain string) ([]byte, error)
```

获取域名的 ECH 配置，带缓存。

#### GetStats

```go
func (dc *DNSCache) GetStats() CacheStatsInfo
```

获取缓存统计信息（大小、命中数、未命中数、命中率）。

#### Warmup

```go
func (dc *DNSCache) Warmup(domains []string)
```

预热缓存（异步解析常用域名）。合并默认列表和自定义列表。

#### Close

```go
func (dc *DNSCache) Close()
```

关闭 DNS 缓存（停止清理循环）。

---

## doh.go

### 类型定义

#### HTTPSRecord

```go
type HTTPSRecord struct {
    Priority int
    Target   string
    Params   map[string]string
    ECH      []byte  // ECH 配置信息（已解码）
    raw      []byte  // 原始记录数据
}
```

HTTPS DNS 记录（SVCB/HTTPS 类型）。

#### DoHClient

```go
type DoHClient struct {
    dohURL  string
    client  *http.Client
    enabled bool
    // ... 内部字段
}
```

DNS over HTTPS 客户端。

#### DoHResponse

```go
type DoHResponse struct {
    Status   int
    TC       bool
    RD       bool
    RA       bool
    AD       bool
    CD       bool
    Question []struct {
        Name string
        Type int
    }
    Answer []struct {
        Name string
        Type int
        Data string
    }
}
```

DoH JSON 响应格式。

### 常量

```go
const (
    RecordTypeA     = 1
    RecordTypeAAAA  = 28
    RecordTypeHTTPS = 65
)
```

DNS 记录类型常量。

### 构造函数

#### NewDoHClient

```go
func NewDoHClient(cfg *config.Config) *DoHClient
```

创建 DoH 客户端。默认超时 1 秒。

### 主要方法

#### Resolve

```go
func (d *DoHClient) Resolve(domain string, queryType string) (string, error)
```

解析域名（支持 A/AAAA/HTTPS 记录）。

优先尝试 RFC 8484（标准 DoH），失败时回退到 JSON API。

内置重试机制（最多 3 次）。

#### ResolveA

```go
func (d *DoHClient) ResolveA(domain string) (string, error)
```

解析 A 记录（IPv4）。

#### ResolveAAAA

```go
func (d *DoHClient) ResolveAAAA(domain string) (string, error)
```

解析 AAAA 记录（IPv6）。

#### ResolveHTTPS

```go
func (d *DoHClient) ResolveHTTPS(domain string) (*HTTPSRecord, error)
```

解析 HTTPS 记录。

#### GetECHConfig

```go
func (d *DoHClient) GetECHConfig(domain string) ([]byte, error)
```

获取域名的 ECH 配置。

#### EnableProxy

```go
func (d *DoHClient) EnableProxy(proxyTransport http.RoundTripper)
```

启用代理模式。`proxyTransport` 应该是 `pool.ProxyTransport` 实例。

### 工具函数

#### IsIPv6

```go
func IsIPv6(ip string) bool
```

检查是否为 IPv6 地址。

#### FormatIPv6

```go
func FormatIPv6(ip string) string
```

格式化 IPv6 地址（添加方括号，用于 URL）。

#### LookupIP

```go
func LookupIP(host string) ([]string, error)
```

标准 DNS 查询（本地 DNS 解析）。优先返回 IPv4。

---

## warmup_list.go

### 变量

#### DefaultWarmupDomains

```go
var DefaultWarmupDomains = []string{
    "www.google.com",
    "www.youtube.com",
    "twitter.com",
    "github.com",
    // ... 更多常用域名
}
```

默认预热域名列表（约 35 个域名）。
