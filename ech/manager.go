// Package ech 提供 TLS ECH (Encrypted Client Hello) 配置管理功能。
//
// 主要功能：
//   - 从 DoH 获取 ECH 配置
//   - 配置缓存（支持 TTL）
//   - 定时自动刷新
//   - SingleFlight 防止缓存击穿
package ech

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"gcm/dns"
	"gcm/logger"
)

// cacheEntry 表示 ECH 缓存条目。
type cacheEntry struct {
	// echConfig 是 ECH 配置的字节数据。
	echConfig []byte
	// expiresAt 是缓存过期时间。
	expiresAt time.Time
}

// EchManager 管理 ECH 配置的缓存和刷新。
type EchManager struct {
	mu              sync.RWMutex
	cache           map[string]*cacheEntry
	echDomain       string                       // ECH 查询域名
	dohFunc         func(string) ([]byte, error) // DoH 查询函数
	cacheTTL        time.Duration                // 缓存 TTL
	refreshInterval time.Duration                // 定时刷新间隔
	stopChan        chan struct{}                // 停止信号
	log             *logger.Logger

	// singleFlight 防止缓存击穿
	flightMu   sync.Mutex           // 保护 flightMap
	flightMap  map[string]*flight  // 正在进行的查询
}

// NewEchManager 创建 ECH 管理器。
//
// 参数：
//   - dohClient: DoH 客户端实例，用于查询 HTTPS 记录
//   - echDomain: ECH 查询域名，用于获取 HTTPS 记录中的 ECH 配置
//   - cacheTTL: 缓存过期时间，默认 24 小时
//   - refreshInterval: 定时刷新间隔，默认 12 小时（0 表示禁用定时刷新）
func NewEchManager(dohClient *dns.DoHClient, echDomain string, cacheTTL time.Duration, refreshInterval time.Duration) *EchManager {
	if cacheTTL == 0 {
		cacheTTL = 24 * time.Hour // 默认 24 小时
	}

	if refreshInterval == 0 {
		refreshInterval = 12 * time.Hour // 默认 12 小时
	}

	return &EchManager{
		cache:           make(map[string]*cacheEntry),
		echDomain:       echDomain,
		dohFunc:         func(domain string) ([]byte, error) {
			return dohClient.GetECHConfig(domain)
		},
		cacheTTL:        cacheTTL,
		refreshInterval: refreshInterval,
		stopChan:        make(chan struct{}),
		log:             logger.GetLogger("ECH"),
		flightMap:        make(map[string]*flight),
	}
}

// GetTlsConfig 获取 TLS 配置。
//
// 参数：
//   - domain: 目标域名
//   - useEch: 是否启用 ECH
//
// 如果 ECH 获取失败，自动回退到标准 TLS（不返回错误）。
func (em *EchManager) GetTlsConfig(domain string, useEch bool) (*tls.Config, error) {
	// 基础 TLS 配置
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: domain,
	}

	// 如果不启用 ECH，直接返回标准配置
	if !useEch {
		return tlsConfig, nil
	}

	// 获取 ECH 配置
	echConfig, err := em.getECHConfig(domain)
	if err != nil {
		em.log.Warn("获取 ECH 配置失败 (%s): %v，回退到标准 TLS", domain, err)
		return tlsConfig, nil // 回退到标准 TLS，不返回错误
	}

	// 设置 ECH 配置列表
	tlsConfig.EncryptedClientHelloConfigList = echConfig
	em.log.Debug("已为 %s 设置 ECH 配置 (长度: %d 字节)", domain, len(echConfig))

	return tlsConfig, nil
}

// getECHConfig 获取 ECH 配置（带缓存和 SingleFlight 防止击穿）。
//
// domain 参数用于日志输出，实际查询使用 em.echDomain。
func (em *EchManager) getECHConfig(domain string) ([]byte, error) {
	// 使用 echDomain 作为缓存键
	cacheKey := em.echDomain

	// 先尝试从缓存读取（快速路径，不加锁）
	em.mu.RLock()
	entry, exists := em.cache[cacheKey]
	em.mu.RUnlock()

	// 缓存命中且未过期
	if exists && time.Now().Before(entry.expiresAt) {
		// 静默返回，不输出日志（避免大量重复日志）
		return entry.echConfig, nil
	}

	// 缓存未命中或已过期，使用 singleFlight 模式
	em.flightMu.Lock()

	// 双重检查：可能在等待 flightMu 时缓存已被其他 goroutine 填充
	em.mu.RLock()
	entry, exists = em.cache[cacheKey]
	em.mu.RUnlock()

	if exists && time.Now().Before(entry.expiresAt) {
		em.flightMu.Unlock()
		return entry.echConfig, nil
	}

	// 检查是否已有查询在进行中
	if flight, inFlight := em.flightMap[cacheKey]; inFlight {
		em.flightMu.Unlock()
		em.log.Debug("等待其他 goroutine 获取 ECH 配置: %s", cacheKey)
		flight.wg.Wait()
		// 等待完成后，缓存应该已经被填充
		if flight.err != nil {
			return nil, flight.err
		}
		// 再次检查缓存
		em.mu.RLock()
		entry = em.cache[cacheKey]
		em.mu.RUnlock()
		if entry != nil && time.Now().Before(entry.expiresAt) {
			return entry.echConfig, nil
		}
		return nil, fmt.Errorf("ECH 配置获取失败: %v", flight.err)
	}

	// 创建新的 flight
	flight := &flight{}
	flight.wg.Add(1)
	em.flightMap[cacheKey] = flight
	em.flightMu.Unlock()

	// 执行查询（在 flightMu 外，允许并发查询不同的 key）
	result, err := em.fetchAndCache(cacheKey)

	// 通知等待的 goroutine
	flight.value = result
	flight.err = err
	flight.wg.Done()

	// 清理 flight
	em.flightMu.Lock()
	delete(em.flightMap, cacheKey)
	em.flightMu.Unlock()

	return result, err
}

// fetchAndCache 从 DoH 查询 ECH 配置并缓存。
func (em *EchManager) fetchAndCache(domain string) ([]byte, error) {
	// 调用 DoH 查询函数
	echConfig, err := em.dohFunc(domain)
	if err != nil {
		return nil, fmt.Errorf("DoH 查询失败: %w", err)
	}

	// 验证配置有效性
	if len(echConfig) == 0 {
		return nil, fmt.Errorf("ECH 配置为空")
	}

	// 写入缓存
	em.mu.Lock()
	em.cache[domain] = &cacheEntry{
		echConfig: echConfig,
		expiresAt: time.Now().Add(em.cacheTTL),
	}
	em.mu.Unlock()

	em.log.Info("成功获取并缓存 %s 的 ECH 配置 (长度: %d 字节, TTL: %v)",
		domain, len(echConfig), em.cacheTTL)

	return echConfig, nil
}

// Refresh 强制刷新指定域名的 ECH 配置。
//
// 此方法会立即从 DoH 查询最新配置并更新缓存。
func (em *EchManager) Refresh(domain string) error {
	em.log.Info("强制刷新 %s 的 ECH 配置", domain)

	// 先删除旧缓存
	em.mu.Lock()
	delete(em.cache, domain)
	em.mu.Unlock()

	// 重新查询并缓存
	_, err := em.fetchAndCache(domain)
	if err != nil {
		return fmt.Errorf("刷新 ECH 配置失败: %w", err)
	}

	return nil
}

// ClearCache 清空所有缓存。
func (em *EchManager) ClearCache() {
	em.mu.Lock()
	defer em.mu.Unlock()

	count := len(em.cache)
	em.cache = make(map[string]*cacheEntry)
	em.log.Info("已清空 ECH 缓存 (清除 %d 个条目)", count)
}

// GetCacheStats 获取缓存统计信息。
//
// 返回：总条目数、过期条目数。
func (em *EchManager) GetCacheStats() (total int, expired int) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	now := time.Now()
	total = len(em.cache)

	for _, entry := range em.cache {
		if now.After(entry.expiresAt) {
			expired++
		}
	}

	return total, expired
}

// CleanupExpired 清理过期的缓存条目。
//
// 返回：清理的条目数。
func (em *EchManager) CleanupExpired() int {
	em.mu.Lock()
	defer em.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for domain, entry := range em.cache {
		if now.After(entry.expiresAt) {
			delete(em.cache, domain)
			cleaned++
		}
	}

	if cleaned > 0 {
		em.log.Debug("清理了 %d 个过期的 ECH 缓存条目", cleaned)
	}

	return cleaned
}

// StartAutoRefresh 启动定时刷新任务。
//
// 会在后台定期刷新所有缓存的 ECH 配置。
func (em *EchManager) StartAutoRefresh() {
	em.log.Info("启动 ECH 定时刷新任务 (间隔: %v)", em.refreshInterval)

	go em.autoRefreshLoop()
}

// autoRefreshLoop 定时刷新循环（后台运行）。
func (em *EchManager) autoRefreshLoop() {
	ticker := time.NewTicker(em.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			em.refreshAllCached()
		case <-em.stopChan:
			em.log.Info("ECH 定时刷新任务已停止")
			return
		}
	}
}

// refreshAllCached 刷新 ECH 配置。
func (em *EchManager) refreshAllCached() {
	em.log.Info("开始定时刷新 ECH 配置: %s", em.echDomain)
	startTime := time.Now()

	// 刷新 echDomain
	if err := em.Refresh(em.echDomain); err != nil {
		em.log.Warn("刷新 %s 的 ECH 配置失败: %v", em.echDomain, err)
	} else {
		elapsed := time.Since(startTime)
		em.log.Info("ECH 定时刷新完成 (耗时: %v)", elapsed)
	}
}

// StopAutoRefresh 停止定时刷新任务。
func (em *EchManager) StopAutoRefresh() {
	close(em.stopChan)
}
