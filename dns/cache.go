package dns

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gcm/gcm/config"
	"github.com/gcm/gcm/logger"
)

// CacheEntry 缓存条目
type CacheEntry struct {
	IP          string       // A/AAAA 记录的 IP
	HTTPSRecord *HTTPSRecord // HTTPS 记录（可选）
	ExpiresAt   time.Time
}

// DNSCache DNS 缓存管理器
type DNSCache struct {
	mu              sync.RWMutex
	cache           map[string]*CacheEntry // key: "domain:type"
	stats           CacheStats
	lastCleanupTime time.Time
	ttl             time.Duration
	cleanupInterval time.Duration
	dohClient       *DoHClient
	log             *logger.Logger
	stopCleanup     chan struct{}
}

// CacheStats 缓存统计
type CacheStats struct {
	Hits   int64
	Misses int64
}

// NewDNSCache 创建 DNS 缓存
func NewDNSCache(cfg *config.Config, dohClient *DoHClient) *DNSCache {
	cache := &DNSCache{
		cache:           make(map[string]*CacheEntry),
		ttl:             cfg.GetDNSCacheTTL(),
		cleanupInterval: cfg.GetDNSCacheCleanupInterval(),
		dohClient:       dohClient,
		log:             logger.GetLogger("DNSCache"),
		stopCleanup:     make(chan struct{}),
	}

	cache.log.Debug("DNS缓存已初始化 (TTL: %d秒, 清理间隔: %d秒)",
		int(cache.ttl.Seconds()), int(cache.cleanupInterval.Seconds()))

	// 启动定期清理
	go cache.cleanupLoop()

	return cache
}

// getKey 生成缓存键
func (dc *DNSCache) getKey(domain string, queryType string) string {
	return domain + ":" + queryType
}

// Get 获取缓存的 IP
func (dc *DNSCache) Get(domain string, queryType string) (string, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	key := dc.getKey(domain, queryType)
	entry, exists := dc.cache[key]
	if !exists {
		return "", false
	}

	// 检查是否过期
	if time.Now().After(entry.ExpiresAt) {
		delete(dc.cache, key)
		dc.log.Debug("缓存过期: %s (%s)", domain, queryType)
		return "", false
	}

	// 缓存命中，增加命中计数（原子操作）
	atomic.AddInt64(&dc.stats.Hits, 1)
	ttl := int(time.Until(entry.ExpiresAt).Seconds())
	dc.log.Debug("缓存命中: %s (%s) -> %s (TTL:%ds)", domain, queryType, entry.IP, ttl)
	return entry.IP, true
}

// Set 设置缓存
func (dc *DNSCache) Set(domain string, queryType string, ip string) {
	if ip == "" {
		return
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	key := dc.getKey(domain, queryType)
	dc.cache[key] = &CacheEntry{
		IP:        ip,
		ExpiresAt: time.Now().Add(dc.ttl),
	}

	dc.log.Debug("缓存添加: %s (%s) -> %s (TTL:%ds)", domain, queryType, ip, int(dc.ttl.Seconds()))
}

// ResolveCached 带缓存的解析方法
func (dc *DNSCache) ResolveCached(domain string, queryType string) (string, error) {
	// 1. 先查缓存
	if ip, found := dc.Get(domain, queryType); found {
		return ip, nil
	}

	// 2. 缓存未命中，执行 DoH 查询
	atomic.AddInt64(&dc.stats.Misses, 1)
	ip, err := dc.dohClient.Resolve(domain, queryType)
	if err != nil {
		return "", err
	}

	// 3. 缓存结果
	dc.Set(domain, queryType, ip)
	return ip, nil
}

// ResolveResolveA 解析 A 记录 (带缓存)
func (dc *DNSCache) ResolveA(domain string) (string, error) {
	return dc.ResolveCached(domain, "A")
}

// ResolveAAAA 解析 AAAA 记录 (带缓存)
func (dc *DNSCache) ResolveAAAA(domain string) (string, error) {
	return dc.ResolveCached(domain, "AAAA")
}

// SetHTTPS 缓存 HTTPS 记录
func (dc *DNSCache) SetHTTPS(domain string, record *HTTPSRecord) {
	if record == nil {
		return
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	key := dc.getKey(domain, "HTTPS")
	dc.cache[key] = &CacheEntry{
		HTTPSRecord: record,
		ExpiresAt:   time.Now().Add(dc.ttl),
	}

	dc.log.Debug("缓存添加: %s (HTTPS) -> Priority=%d, Target=%s (TTL:%ds)",
		domain, record.Priority, record.Target, int(dc.ttl.Seconds()))
}

// GetHTTPS 获取缓存的 HTTPS 记录
func (dc *DNSCache) GetHTTPS(domain string) (*HTTPSRecord, bool) {
	dc.mu.RLock()
	key := dc.getKey(domain, "HTTPS")
	entry, exists := dc.cache[key]
	if !exists || entry.HTTPSRecord == nil {
		dc.mu.RUnlock()
		return nil, false
	}

	// 检查是否过期
	if time.Now().After(entry.ExpiresAt) {
		dc.mu.RUnlock()
		// 升级为写锁删除过期条目
		dc.mu.Lock()
		// 双重检查：可能在锁升级期间被其他 goroutine 删除或更新
		if entry, exists := dc.cache[key]; exists && time.Now().After(entry.ExpiresAt) {
			delete(dc.cache, key)
			dc.log.Debug("缓存过期: %s (HTTPS)", domain)
		}
		dc.mu.Unlock()
		return nil, false
	}

	// 缓存命中
	atomic.AddInt64(&dc.stats.Hits, 1)
	result := entry.HTTPSRecord
	ttl := int(time.Until(entry.ExpiresAt).Seconds())
	dc.mu.RUnlock()

	dc.log.Debug("缓存命中: %s (HTTPS) -> Priority=%d (TTL:%ds)",
		domain, result.Priority, ttl)
	return result, true
}

// ResolveHTTPS 解析 HTTPS 记录（带缓存）
func (dc *DNSCache) ResolveHTTPS(domain string) (*HTTPSRecord, error) {
	// 1. 先查缓存
	if record, found := dc.GetHTTPS(domain); found {
		return record, nil
	}

	// 2. 缓存未命中，执行 DoH 查询
	atomic.AddInt64(&dc.stats.Misses, 1)
	record, err := dc.dohClient.ResolveHTTPS(domain)
	if err != nil {
		return nil, err
	}

	// 3. 缓存结果
	dc.SetHTTPS(domain, record)
	return record, nil
}

// GetECHConfig 获取 ECH 配置（带缓存）
func (dc *DNSCache) GetECHConfig(domain string) ([]byte, error) {
	// 1. 先查缓存
	if record, found := dc.GetHTTPS(domain); found {
		if len(record.ECH) > 0 {
			dc.log.Debug("从缓存获取 ECH 配置: %s (长度: %d 字节)", domain, len(record.ECH))
			return record.ECH, nil
		}
	}

	// 2. 缓存未命中，查询 HTTPS 记录
	record, err := dc.ResolveHTTPS(domain)
	if err != nil {
		return nil, fmt.Errorf("获取 HTTPS 记录失败: %w", err)
	}

	if len(record.ECH) == 0 {
		return nil, fmt.Errorf("HTTPS 记录中未找到 ECH 配置")
	}

	return record.ECH, nil
}

// ResolveAny 解析域名（优先 A 记录，失败则尝试 AAAA）
func (dc *DNSCache) ResolveAny(domain string) (string, string, error) {
	// 优先尝试 A 记录
	ip, err := dc.ResolveA(domain)
	if err == nil {
		return ip, "A", nil
	}

	// 尝试 AAAA
	ip, err = dc.ResolveAAAA(domain)
	if err == nil {
		return ip, "AAAA", nil
	}

	return "", "", err
}

// cleanupLoop 定期清理过期缓存
func (dc *DNSCache) cleanupLoop() {
	ticker := time.NewTicker(dc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dc.cleanup()
		case <-dc.stopCleanup:
			return
		}
	}
}

// cleanup 清理过期缓存
func (dc *DNSCache) cleanup() {
	startTime := time.Now()
	now := time.Now()
	cleaned := 0
	beforeSize := 0

	dc.mu.Lock()
	defer dc.mu.Unlock()

	beforeSize = len(dc.cache)

	for key, entry := range dc.cache {
		if now.After(entry.ExpiresAt) {
			delete(dc.cache, key)
			cleaned++
		}
	}

	if cleaned > 0 || beforeSize > 0 {
		elapsed := time.Since(startTime)
		dc.log.Debug("清理完成: 清除%d条过期缓存, 剩余%d条, 耗时%dms",
			cleaned, len(dc.cache), elapsed.Milliseconds())
	}

	dc.lastCleanupTime = now
}

// GetStats 获取缓存统计
func (dc *DNSCache) GetStats() CacheStatsInfo {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	// 使用原子操作读取统计计数器
	hits := atomic.LoadInt64(&dc.stats.Hits)
	misses := atomic.LoadInt64(&dc.stats.Misses)
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return CacheStatsInfo{
		Size:    len(dc.cache),
		Hits:    hits,
		Misses:  misses,
		HitRate: hitRate,
	}
}

// Warmup 预热缓存（常用域名）
func (dc *DNSCache) Warmup(domains []string) {
	// 合并默认列表和自定义列表
	allDomains := mergeUnique(DefaultWarmupDomains, domains)

	dc.log.Info("开始预热 %d 个域名...", len(allDomains))
	startTime := time.Now()
	successCount := 0

	for _, domain := range allDomains {
		if _, err := dc.ResolveA(domain); err == nil {
			successCount++
		}
		if _, err := dc.ResolveAAAA(domain); err == nil {
			successCount++
		}
	}

	elapsed := time.Since(startTime)
	dc.log.Info("预热完成: 成功%d条, 当前缓存%d条, 耗时%dms",
		successCount, len(dc.cache), elapsed.Milliseconds())
}

// mergeUnique 合并两个域名列表，去重
func mergeUnique(base, extra []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(base)+len(extra))

	// 添加基础列表
	for _, d := range base {
		if _, ok := seen[d]; !ok {
			seen[d] = struct{}{}
			result = append(result, d)
		}
	}

	// 添加额外列表
	for _, d := range extra {
		if _, ok := seen[d]; !ok {
			seen[d] = struct{}{}
			result = append(result, d)
		}
	}

	return result
}

// Close 关闭 DNS 缓存
func (dc *DNSCache) Close() {
	close(dc.stopCleanup)
}

// CacheStatsInfo 缓存统计信息
type CacheStatsInfo struct {
	Size    int
	Hits    int64
	Misses  int64
	HitRate float64
}
