package pool

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gcm/config"
	"gcm/logger"
	"gcm/protocol"
	"gcm/relay"
	"github.com/gorilla/websocket"
)

// EchManagerInterface 表示 ECH (Encrypted Client Hello) 管理器接口。
//
// EchManagerInterface 定义了 ECH 配置管理的标准接口，用于获取 TLS 配置
// 和刷新 ECH 配置。实现此接口的类型可以提供 ECH 支持。
//
// 主要方法：
//   - GetTlsConfig: 获取包含 ECH 配置的 TLS 配置对象
//   - Refresh: 刷新指定域名的 ECH 配置
type EchManagerInterface interface {
	GetTlsConfig(domain string, useEch bool) (*tls.Config, error)
	Refresh(domain string) error
}

// ConnItem 表示单个 WebSocket 连接项。
//
// ConnItem 封装了一个 WebSocket 连接及其相关的状态信息，包括连接标识、
// 流量统计、质量监控、多路复用状态等。每个 ConnItem 对应一个到 Worker 的
// WebSocket 连接，可以承载多个并发的 Stream（多路复用模式）。
//
// 主要功能：
//   - 连接标识：3 字节的 WS ID，全局唯一
//   - 流量统计：发送/接收字节数、活跃 Stream 数量
//   - 质量监控：RTT、丢包率、心跳失败次数、质量评分（0-100）
//   - 多路复用：目标地址亲和性、Stream 数量管理
//   - 生命周期：创建时间、过期时间（带随机偏移）
//
// 并发安全：
//   - WS 写操作使用 writeMu 保护
//   - targets 映射使用 mu 保护
//   - 质量监控字段使用 qualityMu 保护
//   - 原子字段（RTT、Streams、QualityScore 等）使用 atomic 操作
type ConnItem struct {
	WS           *websocket.Conn
	ConnectionID []byte // 3 bytes WS ID
	RelayAddr    string // 中转节点地址
	CreatedAt    time.Time
	ExpiresAt    time.Time           // 过期时间（带随机偏移，用于错峰销毁）
	RTT          atomic.Int64        // 存储纳秒值
	Streams      atomic.Int32        // 当前活跃流数（原子操作）
	Traffic      *TrafficCounter     // 流量计数器
	mu           sync.Mutex          // 保护 targets
	writeMu      sync.Mutex          // 保护 WS 写操作
	targets      map[string]struct{} // 该连接服务的前往目标地址集合 (用于多路复用亲和性)
	closing      atomic.Bool         // 正在关闭标记（防止重复清理）
	inPool       atomic.Bool         // 是否在空闲池中（防止重复放入）

	// 质量监控字段
	QualityScore      int64             // 质量评分 (0-100)，原子操作
	BaselineRTT       time.Duration     // 基线 RTT（创建时的 RTT）
	RTTHistory        [10]time.Duration // RTT 历史（环形缓冲区）
	RTTIndex          int               // RTT 历史索引
	HeartbeatFailures int64             // 心跳失败次数（原子操作）
	RequestFailures   int64             // 请求失败次数（原子操作）
	RequestSuccesses  int64             // 请求成功次数（原子操作）
	LastQualityCheck  time.Time         // 上次质量检查时间
	IsDegraded        bool              // 是否已劣化
	DegradedSince     time.Time         // 劣化开始时间
	qualityMu         sync.Mutex        // 保护质量监控字段
}

// WriteMessage 线程安全的 WebSocket 写入方法。
//
// WriteMessage 使用互斥锁保护 WebSocket 的写操作，确保并发写入的安全性。
// gorilla/websocket 库要求同一时刻只能有一个 goroutine 执行写操作。
//
// 参数：
//   - messageType: WebSocket 消息类型（BinaryMessage 或 TextMessage）
//   - data: 要发送的数据
//
// 返回值：错误（如果有）。
func (c *ConnItem) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.WS.WriteMessage(messageType, data)
}

// AddTarget 添加目标地址到该连接的服务集合。
//
// AddTarget 用于多路复用模式下的目标地址亲和性管理。
// 记录该连接正在服务的目标地址，用于后续请求的连接选择优化。
//
// 参数：
//   - target: 目标地址（格式：host:port）
func (c *ConnItem) AddTarget(target string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets == nil {
		c.targets = make(map[string]struct{})
	}
	c.targets[target] = struct{}{}
}

// RemoveTarget 从该连接的服务集合中移除目标地址。
//
// RemoveTarget 在 Stream 关闭时调用，清理目标地址亲和性记录。
//
// 参数：
//   - target: 目标地址（格式：host:port）
func (c *ConnItem) RemoveTarget(target string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets != nil {
		delete(c.targets, target)
	}
}

// HasTarget 检查该连接是否服务于指定目标地址。
//
// HasTarget 用于多路复用模式下的连接选择优化，优先选择已经服务该目标的连接。
//
// 参数：
//   - target: 目标地址（格式：host:port）
//
// 返回值：如果该连接正在服务该目标返回 true，否则返回 false。
func (c *ConnItem) HasTarget(target string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets == nil {
		return false
	}
	_, exists := c.targets[target]
	return exists
}

// GetTargetCount 获取该连接服务的目标地址数量。
//
// GetTargetCount 返回该连接当前服务的不同目标地址数量，用于负载均衡和连接选择。
//
// 返回值：目标地址数量。
func (c *ConnItem) GetTargetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets == nil {
		return 0
	}
	return len(c.targets)
}

// LoadFactor 计算负载因子。
//
// LoadFactor 计算该连接的负载因子（0.0 - 1.0），表示连接的拥挤程度。
// 负载因子越高表示连接越拥挤，越不适合分配新的 Stream。
//
// 参数：
//   - maxStreams: 每个连接的最大 Stream 数量
//
// 返回值：负载因子（0.0 表示空闲，1.0 表示已满）。
func (c *ConnItem) LoadFactor(maxStreams int) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if maxStreams <= 0 {
		return 0
	}
	streams := c.Streams.Load()
	if streams >= int32(maxStreams) {
		return 1.0
	}
	return float64(streams) / float64(maxStreams)
}

// ============================================================================
// 质量监控方法
// ============================================================================

// CalculateQualityScore 计算连接质量评分。
//
// CalculateQualityScore 基于 RTT、丢包率和活跃流数计算连接的质量评分（0-100）。
// 评分越高表示连接质量越好，越适合分配新的请求。
//
// 评分算法：
//   - RTT 评分（权重 50%）：RTT 每增加 10ms 扣 1 分
//   - 丢包率评分（权重 30%）：丢包率越高扣分越多
//   - 流数评分（权重 20%）：活跃流数越多扣分越多
//
// 返回值：质量评分（0-100），同时缓存到 QualityScore 字段。
func (c *ConnItem) CalculateQualityScore() int64 {
	c.qualityMu.Lock()
	defer c.qualityMu.Unlock()

	// RTT 评分 (0-100，越低越好)
	rtt := time.Duration(c.RTT.Load())
	rttScore := int64(100)
	if rtt > 0 {
		// RTT 每增加 10ms，扣 1 分，最低 0 分
		rttMs := rtt.Milliseconds()
		score := 100 - rttMs/10
		if score < 0 {
			score = 0
		}
		rttScore = int64(score)
	}

	// 丢包率评分 (0-100)
	lossRate := c.GetLossRate()
	lossScore := int64(100)
	if lossRate > 0 {
		score := 100 - int64(lossRate*100)
		if score < 0 {
			score = 0
		}
		lossScore = score
	}

	// 流数评分 (0-100，越少越好)
	streamScore := int64(100 - c.Streams.Load()*5)
	if streamScore < 0 {
		streamScore = 0
	}

	// 加权平均: RTT 50% + 丢包 30% + 流数 20%
	score := (rttScore*50 + lossScore*30 + streamScore*20) / 100

	// 缓存评分
	atomic.StoreInt64(&c.QualityScore, score)

	return score
}

// RecordRTT 记录 RTT 样本。
//
// RecordRTT 将 RTT 样本添加到历史记录（环形缓冲区），并更新当前 RTT 值。
// 用于质量监控和连接选择优化。
//
// 参数：
//   - rtt: RTT 样本值
func (c *ConnItem) RecordRTT(rtt time.Duration) {
	c.qualityMu.Lock()
	defer c.qualityMu.Unlock()

	// 更新 RTT 历史（环形缓冲区）
	c.RTTHistory[c.RTTIndex] = rtt
	c.RTTIndex = (c.RTTIndex + 1) % len(c.RTTHistory)

	// 更新当前 RTT（存储纳秒值）
	c.RTT.Store(rtt.Nanoseconds())
}

// RecordSuccess 记录成功的请求。
//
// RecordSuccess 增加成功请求计数，用于计算丢包率和质量评分。
func (c *ConnItem) RecordSuccess() {
	atomic.AddInt64(&c.RequestSuccesses, 1)
}

// RecordFailure 记录失败的请求。
//
// RecordFailure 增加失败请求计数，用于计算丢包率和质量评分。
func (c *ConnItem) RecordFailure() {
	atomic.AddInt64(&c.RequestFailures, 1)
}

// GetAverageRTT 获取平均 RTT。
//
// GetAverageRTT 计算 RTT 历史记录中所有有效样本的平均值。
// 用于质量监控和连接选择优化。
//
// 返回值：平均 RTT，如果没有样本返回 0。
func (c *ConnItem) GetAverageRTT() time.Duration {
	c.qualityMu.Lock()
	defer c.qualityMu.Unlock()

	// 计算 RTT 历史的平均值
	var sum time.Duration
	count := 0
	for _, rtt := range c.RTTHistory {
		if rtt > 0 {
			sum += rtt
			count++
		}
	}

	if count == 0 {
		return time.Duration(c.RTT.Load()) // 如果没有历史数据，返回当前 RTT
	}
	return sum / time.Duration(count)
}

// GetLossRate 获取丢包率。
//
// GetLossRate 计算请求失败次数占总请求次数的比例。
// 用于质量监控和连接选择优化。
//
// 返回值：丢包率（0.0 - 1.0），如果没有请求记录返回 0。
func (c *ConnItem) GetLossRate() float64 {
	successes := atomic.LoadInt64(&c.RequestSuccesses)
	failures := atomic.LoadInt64(&c.RequestFailures)
	total := successes + failures

	if total == 0 {
		return 0
	}
	return float64(failures) / float64(total)
}

// StreamHandler 表示 Stream 消息处理器。
//
// StreamHandler 定义了 Stream 生命周期中的回调函数，用于处理消息、关闭、错误和清理事件。
// 在多路复用模式下，每个 Stream 都有一个独立的 StreamHandler。
//
// 回调函数：
//   - OnMessage: 接收到消息时调用（CONNECTED、DATA、CLOSE 消息）
//   - OnClose: 连接关闭时调用
//   - OnError: 发生错误时调用
//   - OnCleanup: 清理资源时调用（Stream 注销时）
type StreamHandler struct {
	OnMessage func(msg *protocol.Message)
	OnClose   func()
	OnError   func()
	OnCleanup func()
}

// ConnectionPool 表示 WebSocket 连接池。
//
// ConnectionPool 管理到 Cloudflare Worker 的 WebSocket 连接，提供连接复用、
// 多路复用、负载均衡、质量监控等功能。这是 GCM 代理客户端的核心组件。
//
// 主要功能：
//   - 连接管理：自动维护连接池大小（minPoolSize ~ maxPoolSize）
//   - 多路复用：单个连接支持多个并发 Stream（0-255）
//   - 负载均衡：基于质量评分和负载因子选择最优连接
//   - 质量监控：RTT、丢包率、心跳检测
//   - 请求队列：连接池耗尽时排队等待
//   - 自动维护：定期清理过期连接、补充连接、心跳保活
//   - 中转节点：支持通过中转节点连接 Worker
//   - ECH 支持：支持 TLS Encrypted Client Hello
//
// 工作流程：
//  1. 启动时预热连接池（可选）
//  2. 请求到达时从池中获取连接
//  3. 多路复用模式下分配 Stream ID
//  4. 发送 CONNECT 消息建立隧道
//  5. 双向数据转发
//  6. 连接空闲时归还到池中
//  7. 后台维护循环定期清理和补充
//
// 并发安全：所有公开方法都是并发安全的。
type ConnectionPool struct {
	mu           sync.RWMutex
	cfg          *config.Config
	log          *logger.Logger
	relayManager *relay.RelayManager
	echManager   EchManagerInterface // ECH 配置管理器

	pool               []*ConnItem
	activeConnections  int32
	pendingConnections int32
	requestQueue       chan *connRequest
	// StreamManager 集成: 每条连接对应一个 StreamManager
	managerByConn     map[*ConnItem]*StreamManager
	pendingHeartbeats map[string]time.Time

	// 目标地址亲和性映射 (用于多路复用优化)
	targetToConn map[string]*ConnItem // 目标地址 -> 当前服务的连接

	// 亲和性分数增强
	connAffinityScore map[*ConnItem]map[string]int64 // 连接 -> 目标的亲和分数
	affinityMu        sync.RWMutex

	lastRelayFetchTime time.Time
	currentMinPoolSize int32

	// ECH 降级控制
	echFailureCount    int32     // ECH 连续失败次数
	echDisabledUntil   time.Time // ECH 禁用截止时间
	echFallbackEnabled bool      // 是否已启用 ECH 降级

	// 会话轮换器
	sessionRotator *SessionRotator

	stats    PoolStats
	stopChan chan struct{}
}

// connRequest 连接请求
type connRequest struct {
	connCh chan *ConnItem
	errCh  chan error
}

// PoolStats 表示连接池统计信息。
//
// PoolStats 记录连接池的运行统计数据，包括请求统计、响应时间、流量统计和连接统计。
// 所有字段都使用原子操作更新，确保并发安全。
//
// 统计字段：
//   - Requests: 总请求数
//   - Successes: 成功请求数
//   - Failures: 失败请求数
//   - Timeouts: 超时请求数
//   - TotalResponseTime: 总响应时间（纳秒）
//   - MinResponseTime: 最小响应时间（纳秒）
//   - MaxResponseTime: 最大响应时间（纳秒）
//   - BytesReceived: 总接收字节数
//   - BytesSent: 总发送字节数
//   - StartTime: 连接池启动时间
//   - CreatedConnections: 创建的连接总数
//   - ClosedConnections: 关闭的连接总数
type PoolStats struct {
	Requests           int64
	Successes          int64
	Failures           int64
	Timeouts           int64
	TotalResponseTime  int64
	MinResponseTime    int64
	MaxResponseTime    int64
	BytesReceived      int64
	BytesSent          int64
	StartTime          time.Time
	CreatedConnections int64
	ClosedConnections  int64
}

// NewConnectionPool 创建并初始化连接池。
//
// NewConnectionPool 创建一个新的 WebSocket 连接池实例，初始化所有必要的数据结构，
// 并启动后台维护循环（连接维护、心跳保活、动态调整等）。
//
// 参数：
//   - cfg: 配置对象
//   - relayMgr: 中转节点管理器
//   - echMgr: ECH 配置管理器（可选，传 nil 表示不使用 ECH）
//
// 返回值：初始化完成的 ConnectionPool 实例。
//
// 注意：
//   - 返回的连接池已经启动了后台维护循环
//   - 调用方应该在程序退出时调用 Close 方法清理资源
func NewConnectionPool(cfg *config.Config, relayMgr *relay.RelayManager, echMgr EchManagerInterface) *ConnectionPool {
	p := &ConnectionPool{
		cfg:                cfg,
		log:                logger.GetLogger("Pool"),
		relayManager:       relayMgr,
		echManager:         echMgr,
		pool:               make([]*ConnItem, 0),
		requestQueue:       make(chan *connRequest, cfg.MaxPoolSize*2),
		managerByConn:      make(map[*ConnItem]*StreamManager),
		pendingHeartbeats:  make(map[string]time.Time),
		targetToConn:       make(map[string]*ConnItem),
		currentMinPoolSize: int32(cfg.MinPoolSize),
		stopChan:           make(chan struct{}),
		stats: PoolStats{
			StartTime:       time.Now(),
			MinResponseTime: -1,
		},
	}

	p.log.Debug("连接池已初始化 (Min:%d, Max:%d, 多路复用:%v)",
		cfg.MinPoolSize, cfg.MaxPoolSize, cfg.EnableMultiplex)

	// 启动后台维护
	go p.maintainLoop()
	go p.cullLoop()
	go p.statsLoop()
	go p.heartbeatLoop()
	go p.trafficReportLoop()
	go p.rateUpdateLoop()
	go p.congestionControlLoop() // 拥塞控制循环

	if cfg.EnableDynamicPool {
		go p.dynamicPoolLoop()
	}

	// 初始化会话轮换器（应对 CF 110 秒限制）
	if cfg.EnableSessionRotation {
		p.sessionRotator = NewSessionRotator(p, cfg)
		p.log.Info("会话轮换器已启动 (最大寿命: %v, 排空超时: %v)",
			cfg.GetMaxSessionLifetime(), cfg.GetSessionDrainTimeout())
	}

	return p
}

// Warmup 预热连接池。
//
// Warmup 在连接池启动时串行创建指定数量的连接，加速首次请求的响应速度。
// 连接创建间隔会根据 TTL 自动计算，避免连接同时过期。
//
// 工作流程：
//  1. 检查是否启用预热（EnablePoolWarmup）
//  2. 计算创建间隔（TTL / 目标数量，限制在 200ms-2s 之间）
//  3. 串行创建连接，每次创建后等待间隔时间
//  4. 连续失败 3 次则提前终止
//  5. 超过 WarmupTimeout 则提前终止
//
// 返回值：错误（如果有）。
//
// 注意：
//   - 预热失败不影响服务启动，后续请求会触发按需创建
//   - 预热过程在后台 goroutine 中执行，不阻塞主线程
func (p *ConnectionPool) Warmup() error {
	if !p.cfg.EnablePoolWarmup || p.currentMinPoolSize <= 0 {
		return nil
	}

	targetSize := int(p.currentMinPoolSize)
	p.log.Info("开始预热连接池，目标: %d 个连接（串行创建）...", targetSize)

	startTime := time.Now()
	created := 0
	failed := 0
	consecutiveFailures := 0

	// 计算创建间隔：将 TTL 分散到所有连接上
	// 例如 TTL=5min, 目标=10个连接，间隔=30s
	ttl := p.cfg.GetConnectionTTL()
	interval := ttl / time.Duration(targetSize+1)
	// 限制间隔范围：最小 200ms，最大 2s
	if interval < 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	if interval > 2*time.Second {
		interval = 2 * time.Second
	}
	p.log.Debug("预热间隔: %v", interval)

	// 串行创建连接
	for created < targetSize {
		// 检查超时
		if time.Since(startTime) > p.cfg.GetWarmupTimeout() {
			p.log.Warn("预热超时，已创建 %d/%d 个连接 (失败: %d)", created, targetSize, failed)
			break
		}

		// 创建单个连接
		success := p.createConnectionSync("预热")
		if success {
			created++
			consecutiveFailures = 0
			p.log.Debug("预热进度: %d/%d", created, targetSize)
		} else {
			failed++
			consecutiveFailures++
			if consecutiveFailures > 3 {
				p.log.Warn("预热连续失败 %d 次，跳过预热", consecutiveFailures)
				break
			}
		}

		// 添加间隔（最后一个不需要等待）
		if created < targetSize {
			time.Sleep(interval)
		}
	}

	elapsed := time.Since(startTime)
	p.log.Info("预热完成，创建 %d 个连接 (失败: %d)，耗时 %dms", created, failed, elapsed.Milliseconds())

	return nil
}

// generateWSID 生成 WebSocket ID (3字节)
func (p *ConnectionPool) generateWSID() []byte {
	buf := make([]byte, 3)
	rand.Read(buf)
	return buf
}

// calculateExpiresAt 计算带随机偏移的过期时间（用于错峰销毁）
// 随机偏移范围：±30% 的 TTL，确保连接不会同时过期
func (p *ConnectionPool) calculateExpiresAt(createdAt time.Time) time.Time {
	ttl := p.cfg.GetConnectionTTL()
	// 生成 -0.3 到 +0.3 的随机偏移系数
	var randomBytes [1]byte
	rand.Read(randomBytes[:])
	// 将 0-255 映射到 -0.3 到 +0.3
	offsetRatio := (float64(randomBytes[0])/255.0 - 0.5) * 0.6
	offset := time.Duration(float64(ttl) * offsetRatio)
	return createdAt.Add(ttl + offset)
}

// getTLSConfig 获取 TLS 配置（支持 ECH 和自动降级）
func (p *ConnectionPool) getTLSConfig() *tls.Config {
	// 检查 ECH 是否被临时禁用
	useECH := p.cfg.EnableECH
	if useECH && time.Now().Before(p.echDisabledUntil) {
		p.log.Debug("ECH 当前处于降级状态，使用普通 TLS")
		useECH = false
	}

	if p.echManager != nil {
		tlsConfig, err := p.echManager.GetTlsConfig(p.cfg.WorkerHost, useECH)
		if err != nil {
			p.log.Warn("获取 TLS 配置失败，使用默认配置: %v", err)
			return &tls.Config{
				MinVersion: tls.VersionTLS13,
				ServerName: p.cfg.WorkerHost,
			}
		}
		return tlsConfig
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: p.cfg.WorkerHost,
	}
}

// handleDialError 智能处理拨号错误
func (p *ConnectionPool) handleDialError(err error, relay *relay.RelayNode) {
	if err == nil {
		return
	}

	errStr := err.Error()

	// 1. 判断是否为 ECH 相关错误
	if p.cfg.EnableECH && p.echManager != nil {
		if strings.Contains(errStr, "ech") ||
			strings.Contains(errStr, "encrypted_client_hello") ||
			strings.Contains(errStr, "tls: handshake failure") {

			// 增加 ECH 失败计数
			failCount := atomic.AddInt32(&p.echFailureCount, 1)
			p.log.Warn("检测到 ECH 相关错误 (连续失败: %d 次)", failCount)

			// 如果连续失败 3 次，启用降级模式
			if failCount >= 3 {
				p.mu.Lock()
				if !p.echFallbackEnabled {
					p.echFallbackEnabled = true
					p.echDisabledUntil = time.Now().Add(5 * time.Minute) // 降级 5 分钟
					p.log.Warn("ECH 连续失败 %d 次，启用降级模式，将使用普通 TLS (持续 5 分钟)", failCount)
				}
				p.mu.Unlock()
			} else {
				// 失败次数未达到阈值，尝试刷新配置
				p.log.Info("尝试刷新 ECH 配置...")
				go func() {
					if err := p.echManager.Refresh(p.cfg.ECHDomain); err != nil {
						p.log.Error("刷新 ECH 配置失败: %v", err)
					} else {
						p.log.Info("ECH 配置已刷新")
					}
				}()
			}
			return
		}
	}

	// 2. 判断是否为中转节点连接失败
	if relay != nil && (strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "timeout")) {
		p.log.Warn("中转节点连接失败，触发节点重评")
		go p.handleConnectionFailure()
		return
	}

	// 3. 其他错误，仅记录日志
	p.log.Warn("拨号失败，等待重试: %v", err)
}

// createConnectionSync 同步创建连接（用于预热），返回成功/失败
func (p *ConnectionPool) createConnectionSync(reason string) bool {
	// 双重检查
	currentSize := int(len(p.pool)) + int(atomic.LoadInt32(&p.activeConnections)) +
		int(atomic.LoadInt32(&p.pendingConnections))
	if currentSize >= p.cfg.MaxPoolSize {
		p.log.Debug("连接池已满 (%d/%d)，跳过创建: %s", currentSize, p.cfg.MaxPoolSize, reason)
		return false
	}

	atomic.AddInt32(&p.pendingConnections, 1)
	defer func() {
		atomic.AddInt32(&p.pendingConnections, -1)
	}()

	atomic.AddInt64(&p.stats.CreatedConnections, 1)

	// 使用负载均衡选择节点
	relay := p.relayManager.GetNextRelayWithLoadBalance()
	// loadIncremented 仅在当前 goroutine 中使用，无需原子操作
	var loadIncremented bool
	if relay != nil {
		// 增加节点负载计数
		p.relayManager.UpdateNodeLoad(relay.IP, relay.Port, 1)
		loadIncremented = true
		defer func() {
			// 只有在连接失败时才减少负载计数
			// 成功时由连接关闭时处理
			if loadIncremented {
				p.relayManager.UpdateNodeLoad(relay.IP, relay.Port, -1)
			}
		}()
	}

	var url string
	var headers http.Header
	var customDial func(network, addr string) (net.Conn, error)

	if relay != nil {
		// 中转模式：URL 仍用原始 Worker，但通过 NetDial 将 TCP 连接到中转节点
		url = fmt.Sprintf("wss://%s/%s", p.cfg.WorkerHost, p.cfg.UserID)
		customDial = func(network, addr string) (net.Conn, error) {
			// addr 是 workerHost:443，替换为中转节点的 IP:PORT
			return net.DialTimeout(network, net.JoinHostPort(relay.IP, fmt.Sprintf("%d", relay.Port)), p.cfg.GetConnectionTimeout())
		}
		p.log.Debug("创建连接 (%s) -> 中转: %s:%d (TLS SNI: %s)", reason, relay.IP, relay.Port, p.cfg.WorkerHost)
	} else {
		// 直连模式：也需要设置 DialTimeout，否则会无限期等待
		url = fmt.Sprintf("wss://%s/%s", p.cfg.WorkerHost, p.cfg.UserID)
		customDial = func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, p.cfg.GetConnectionTimeout())
		}
		p.log.Debug("创建连接 (%s) -> 直连: %s", reason, p.cfg.WorkerHost)
	}

	headers = make(http.Header)
	headers.Set("Host", p.cfg.WorkerHost)
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.140")

	// 获取 TLS 配置（支持 ECH）
	tlsConfig := p.getTLSConfig()

	// 配置 WebSocket Dialer
	dialer := websocket.Dialer{
		HandshakeTimeout: p.cfg.GetConnectionTimeout(),
		NetDial:          customDial,
		TLSClientConfig:  tlsConfig,
	}

	startTime := time.Now()
	ws, resp, err := dialer.Dial(url, headers)
	if err != nil {
		atomic.AddInt64(&p.stats.Failures, 1)
		p.log.Warn("连接失败 (%s): %v (目标: %s)", reason, err, url)

		// 智能处理拨号错误
		p.handleDialError(err, relay)

		return false
	}
	defer resp.Body.Close()

	// 连接成功，重置 ECH 失败计数
	if p.cfg.EnableECH && atomic.LoadInt32(&p.echFailureCount) > 0 {
		atomic.StoreInt32(&p.echFailureCount, 0)
		p.log.Debug("连接成功，重置 ECH 失败计数")
	}

	latency := time.Since(startTime)
	connectionID := p.generateWSID()

	// 构建中转节点地址字符串
	relayAddr := p.cfg.WorkerHost // 直连模式使用 Worker 地址
	if relay != nil {
		relayAddr = fmt.Sprintf("%s:%d", relay.IP, relay.Port)
	}

	now := time.Now()
	item := &ConnItem{
		WS:           ws,
		ConnectionID: connectionID,
		RelayAddr:    relayAddr,
		CreatedAt:    now,
		ExpiresAt:    p.calculateExpiresAt(now),
		Traffic:      &TrafficCounter{},
		// 初始化质量监控字段
		QualityScore:      100, // 初始满分
		BaselineRTT:       latency,
		RTTHistory:        [10]time.Duration{},
		RTTIndex:          0,
		HeartbeatFailures: 0,
		RequestFailures:   0,
		RequestSuccesses:  0,
		LastQualityCheck:  now,
		IsDegraded:        false,
	}
	item.RTT.Store(latency.Nanoseconds())

	connIDStr := fmt.Sprintf("%02x%02x%02x", connectionID[0], connectionID[1], connectionID[2])
	p.log.Debug("新连接 [%s] 已就绪 (%s), 握手延迟: %dms", connIDStr, reason, latency.Milliseconds())

	// 设置 TCP NODELAY
	if p.cfg.EnableTcpNoDelay {
		// 获取底层连接
		if nc, ok := ws.UnderlyingConn().(interface{ SetNoDelay(bool) error }); ok {
			nc.SetNoDelay(true)
		}
	}

	// 初始化 StreamManager（messageLoop 需要它来分发消息）
	// 所有连接都必须在 managerByConn 中，无论是否有活跃的 stream
	p.mu.Lock()
	p.managerByConn[item] = NewStreamManager(
		item,
		int(p.cfg.MaxStreamsPerConnection),
		p.cfg.GetDefaultWindowSize(),
		p.cfg.GetMinWindowSize(),
		p.cfg.GetMaxWindowSize(),
		p.cfg.GetWindowTimeout(),
	)
	p.mu.Unlock()

	// 启动消息处理循环
	go p.messageLoop(item)

	// 将连接加入池（同步操作，确保加入成功后才返回）
	p.mu.Lock()
	item.inPool.Store(true)
	p.pool = append(p.pool, item)
	p.mu.Unlock()

	// 连接成功，取消 defer 的负载减 1（由连接关闭时处理）
	loadIncremented = false

	return true
}

// createConnection 创建新连接（异步版本，用于运行时）
func (p *ConnectionPool) createConnection(reason string) bool {
	// 双重检查
	currentSize := int(len(p.pool)) + int(atomic.LoadInt32(&p.activeConnections)) +
		int(atomic.LoadInt32(&p.pendingConnections))
	if currentSize >= p.cfg.MaxPoolSize {
		p.log.Debug("连接池已满 (%d/%d)，跳过创建: %s", currentSize, p.cfg.MaxPoolSize, reason)
		return false
	}

	atomic.AddInt32(&p.pendingConnections, 1)
	defer atomic.AddInt32(&p.pendingConnections, -1)

	atomic.AddInt64(&p.stats.CreatedConnections, 1)

	// 使用负载均衡选择节点
	relay := p.relayManager.GetNextRelayWithLoadBalance()
	if relay != nil {
		// 增加节点负载计数
		p.relayManager.UpdateNodeLoad(relay.IP, relay.Port, 1)
		defer p.relayManager.UpdateNodeLoad(relay.IP, relay.Port, -1)
	}

	var url string
	var headers http.Header
	var customDial func(network, addr string) (net.Conn, error)

	if relay != nil {
		// 中转模式：URL 仍用原始 Worker，但通过 NetDial 将 TCP 连接到中转节点
		url = fmt.Sprintf("wss://%s/%s", p.cfg.WorkerHost, p.cfg.UserID)
		customDial = func(network, addr string) (net.Conn, error) {
			// addr 是 workerHost:443，替换为中转节点的 IP:PORT
			return net.DialTimeout(network, net.JoinHostPort(relay.IP, fmt.Sprintf("%d", relay.Port)), p.cfg.GetConnectionTimeout())
		}
		p.log.Debug("创建连接 (%s) -> 中转: %s:%d (TLS SNI: %s)", reason, relay.IP, relay.Port, p.cfg.WorkerHost)
	} else {
		// 直连模式：也需要设置 DialTimeout，否则会无限期等待
		url = fmt.Sprintf("wss://%s/%s", p.cfg.WorkerHost, p.cfg.UserID)
		customDial = func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, p.cfg.GetConnectionTimeout())
		}
		p.log.Debug("创建连接 (%s) -> 直连: %s", reason, p.cfg.WorkerHost)
	}

	headers = make(http.Header)
	headers.Set("Host", p.cfg.WorkerHost)
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.140")

	// 获取 TLS 配置（支持 ECH）
	tlsConfig := p.getTLSConfig()

	// 配置 WebSocket Dialer
	dialer := websocket.Dialer{
		HandshakeTimeout: p.cfg.GetConnectionTimeout(),
		NetDial:          customDial,
		TLSClientConfig:  tlsConfig,
	}

	// 使用 channel 和 goroutine 实现可靠的超时保护
	type dialResult struct {
		ws   *websocket.Conn
		resp *http.Response
		err  error
	}
	resultChan := make(chan dialResult, 1)

	go func() {
		ws, resp, err := dialer.Dial(url, headers)
		if err != nil {
			p.log.Debug("Dial goroutine 返回错误: %v", err)
		}
		resultChan <- dialResult{ws, resp, err}
	}()

	startTime := time.Now()
	var ws *websocket.Conn
	var resp *http.Response
	var err error

	// 等待连接完成或超时
	select {
	case res := <-resultChan:
		ws, resp, err = res.ws, res.resp, res.err
		p.log.Debug("Dial 完成，耗时: %dms", time.Since(startTime).Milliseconds())
	case <-time.After(p.cfg.GetConnectionTimeout() * 2): // 外层超时保护（2倍 ConnectionTimeout）
		atomic.AddInt64(&p.stats.Failures, 1)
		p.log.Warn("连接失败 (%s): 总体超时 (目标: %s)", reason, url)
		return false
	}

	if err != nil {
		atomic.AddInt64(&p.stats.Failures, 1)
		p.log.Warn("连接失败 (%s): %v (目标: %s)", reason, err, url)

		// 智能处理拨号错误
		p.handleDialError(err, relay)

		return false
	}
	defer resp.Body.Close()

	// 连接成功，重置 ECH 失败计数
	if p.cfg.EnableECH && atomic.LoadInt32(&p.echFailureCount) > 0 {
		atomic.StoreInt32(&p.echFailureCount, 0)
		p.log.Debug("连接成功，重置 ECH 失败计数")
	}

	latency := time.Since(startTime)
	connectionID := p.generateWSID()

	// 构建中转节点地址字符串
	relayAddr := p.cfg.WorkerHost // 直连模式使用 Worker 地址
	if relay != nil {
		relayAddr = fmt.Sprintf("%s:%d", relay.IP, relay.Port)
	}

	now := time.Now()
	item := &ConnItem{
		WS:           ws,
		ConnectionID: connectionID,
		RelayAddr:    relayAddr,
		CreatedAt:    now,
		ExpiresAt:    p.calculateExpiresAt(now),
		Traffic:      &TrafficCounter{},
		// 初始化质量监控字段
		QualityScore:      100, // 初始满分
		BaselineRTT:       latency,
		RTTHistory:        [10]time.Duration{},
		RTTIndex:          0,
		HeartbeatFailures: 0,
		RequestFailures:   0,
		RequestSuccesses:  0,
		LastQualityCheck:  now,
		IsDegraded:        false,
	}
	item.RTT.Store(latency.Nanoseconds())

	connIDStr := fmt.Sprintf("%02x%02x%02x", connectionID[0], connectionID[1], connectionID[2])
	p.log.Debug("新连接 [%s] 已就绪 (%s), 握手延迟: %dms", connIDStr, reason, latency.Milliseconds())

	// 设置 TCP NODELAY
	if p.cfg.EnableTcpNoDelay {
		// 获取底层连接
		if nc, ok := ws.UnderlyingConn().(interface{ SetNoDelay(bool) error }); ok {
			nc.SetNoDelay(true)
		}
	}

	// 初始化 StreamManager（确保 messageLoop 能立即分发消息）
	p.mu.Lock()
	p.managerByConn[item] = NewStreamManager(
		item,
		int(p.cfg.MaxStreamsPerConnection),
		p.cfg.GetDefaultWindowSize(),
		p.cfg.GetMinWindowSize(),
		p.cfg.GetMaxWindowSize(),
		p.cfg.GetWindowTimeout(),
	)
	p.mu.Unlock()

	// 启动消息处理循环
	go p.messageLoop(item)

	// 检查是否有等待的请求
	select {
	case req := <-p.requestQueue:
		atomic.AddInt32(&p.activeConnections, 1)
		req.connCh <- item
	default:
		p.mu.Lock()
		item.inPool.Store(true)
		p.pool = append(p.pool, item)
		p.mu.Unlock()
	}

	return true
}

// createConnectionWithRelay 使用指定的中转节点创建连接
func (p *ConnectionPool) createConnectionWithRelay(relay *relay.RelayNode, reason string) bool {
	// 检查连接池是否已满
	currentSize := int(len(p.pool)) + int(atomic.LoadInt32(&p.activeConnections)) +
		int(atomic.LoadInt32(&p.pendingConnections))
	if currentSize >= p.cfg.MaxPoolSize {
		p.log.Debug("连接池已满 (%d/%d)，跳过创建: %s", currentSize, p.cfg.MaxPoolSize, reason)
		return false
	}

	atomic.AddInt32(&p.pendingConnections, 1)
	defer atomic.AddInt32(&p.pendingConnections, -1)

	atomic.AddInt64(&p.stats.CreatedConnections, 1)

	// 使用指定的中转节点
	url := fmt.Sprintf("wss://%s/%s", p.cfg.WorkerHost, p.cfg.UserID)
	customDial := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, net.JoinHostPort(relay.IP, fmt.Sprintf("%d", relay.Port)), p.cfg.GetConnectionTimeout())
	}
	p.log.Debug("创建连接 (%s) -> 中转: %s:%d (TLS SNI: %s)", reason, relay.IP, relay.Port, p.cfg.WorkerHost)

	headers := make(http.Header)
	headers.Set("Host", p.cfg.WorkerHost)
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.140")

	tlsConfig := p.getTLSConfig()

	dialer := websocket.Dialer{
		HandshakeTimeout: p.cfg.GetConnectionTimeout(),
		NetDial:          customDial,
		TLSClientConfig:  tlsConfig,
	}

	// 使用 channel 实现超时保护
	type dialResult struct {
		ws   *websocket.Conn
		resp *http.Response
		err  error
	}
	resultChan := make(chan dialResult, 1)

	go func() {
		ws, resp, err := dialer.Dial(url, headers)
		resultChan <- dialResult{ws, resp, err}
	}()

	startTime := time.Now()
	var ws *websocket.Conn
	var resp *http.Response
	var err error

	select {
	case res := <-resultChan:
		ws, resp, err = res.ws, res.resp, res.err
	case <-time.After(p.cfg.GetConnectionTimeout() * 2):
		atomic.AddInt64(&p.stats.Failures, 1)
		p.log.Warn("连接失败 (%s): 总体超时", reason)
		return false
	}

	if err != nil {
		atomic.AddInt64(&p.stats.Failures, 1)
		p.log.Warn("连接失败 (%s): %v", reason, err)
		return false
	}
	defer resp.Body.Close()

	latency := time.Since(startTime)
	connectionID := p.generateWSID()
	relayAddr := fmt.Sprintf("%s:%d", relay.IP, relay.Port)

	now := time.Now()
	item := &ConnItem{
		WS:           ws,
		ConnectionID: connectionID,
		RelayAddr:    relayAddr,
		CreatedAt:    now,
		ExpiresAt:    p.calculateExpiresAt(now),
		Traffic:      &TrafficCounter{},
		// 初始化质量监控字段
		QualityScore:      100,
		BaselineRTT:       latency,
		RTTHistory:        [10]time.Duration{},
		RTTIndex:          0,
		HeartbeatFailures: 0,
		RequestFailures:   0,
		RequestSuccesses:  0,
		LastQualityCheck:  now,
		IsDegraded:        false,
	}
	item.RTT.Store(latency.Nanoseconds())

	connIDStr := fmt.Sprintf("%02x%02x%02x", connectionID[0], connectionID[1], connectionID[2])
	p.log.Debug("新连接 [%s] 已就绪 (%s), 握手延迟: %dms", connIDStr, reason, latency.Milliseconds())

	// 设置 TCP NODELAY
	if p.cfg.EnableTcpNoDelay {
		if nc, ok := ws.UnderlyingConn().(interface{ SetNoDelay(bool) error }); ok {
			nc.SetNoDelay(true)
		}
	}

	// 初始化 StreamManager
	p.mu.Lock()
	p.managerByConn[item] = NewStreamManager(
		item,
		int(p.cfg.MaxStreamsPerConnection),
		p.cfg.GetDefaultWindowSize(),
		p.cfg.GetMinWindowSize(),
		p.cfg.GetMaxWindowSize(),
		p.cfg.GetWindowTimeout(),
	)
	p.mu.Unlock()

	// 启动消息处理循环
	go p.messageLoop(item)

	// 将连接加入池
	p.mu.Lock()
	item.inPool.Store(true)
	p.pool = append(p.pool, item)
	p.mu.Unlock()

	return true
}

// messageLoop 消息处理循环
func (p *ConnectionPool) messageLoop(item *ConnItem) {
	ws := item.WS
	connIDStr := fmt.Sprintf("%02x%02x%02x", item.ConnectionID[0], item.ConnectionID[1], item.ConnectionID[2])

	// 设置 Pong 处理器，处理心跳响应
	ws.SetPongHandler(func(appData string) error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// 计算 RTT 并更新连接信息
		if lastPing, ok := p.pendingHeartbeats[connIDStr]; ok {
			rtt := time.Since(lastPing)
			// 使用指数移动平均 (EMA) 更新 RTT，平滑波动
			// 新RTT = 0.7 * 旧RTT + 0.3 * 测量RTT
			oldRTT := item.RTT.Load()
			newRTT := (oldRTT*7/10 + rtt.Nanoseconds()*3/10)
			item.RTT.Store(newRTT)
			// 记录 RTT 到历史缓冲区
			item.RecordRTT(rtt)
		}

		delete(p.pendingHeartbeats, connIDStr)
		return nil
	})

	defer func() {
		// 连接关闭处理 - 记录流量统计
		sent, recv, streams := item.Traffic.GetSnapshot()
		connIDStr := formatConnID(item.ConnectionID)
		p.log.Info("WS[%s] 关闭 | %s | ↑ %s | ↓ %s | Streams: %d",
			connIDStr, item.RelayAddr,
			formatBytes(sent), formatBytes(recv), streams)

		// 修复死锁问题：先获取需要的数据，释放锁后再关闭 WebSocket
		var mgrToCleanup *StreamManager
		var wasInPool bool
		var wasActive bool

		p.mu.Lock()
		// 1. 尝试从空闲池中移除
		for i, ci := range p.pool {
			if ci == item {
				p.pool = append(p.pool[:i], p.pool[i+1:]...)
				wasInPool = true
				break
			}
		}

		// 2. 检查是否在活跃连接中（managerByConn）
		if mgr, exists := p.managerByConn[item]; exists {
			mgrToCleanup = mgr
			delete(p.managerByConn, item)
			// 如果不在空闲池中，说明是活跃连接，需要递减 activeConnections
			if !wasInPool {
				wasActive = true
			}
		}

		delete(p.pendingHeartbeats, connIDStr)
		// 清理亲和性分数，防止内存泄漏
		delete(p.connAffinityScore, item)
		// 清理会话轮换器中的排空记录，防止内存泄漏
		if p.sessionRotator != nil {
			p.sessionRotator.RemoveConnection(item)
		}
		atomic.AddInt64(&p.stats.ClosedConnections, 1)
		p.mu.Unlock()

		// 递减活跃连接计数（在锁外操作）
		if wasActive {
			atomic.AddInt32(&p.activeConnections, -1)
		}

		// 在锁外进行清理操作，避免阻塞其他操作
		if mgrToCleanup != nil {
			mgrToCleanup.HandleConnectionClose()
		}

		// 最后关闭 WebSocket（在锁外，避免阻塞）
		ws.Close() // nolint:errcheck
	}()

	for {
		// 检查是否正在关闭（防止处理已关闭的连接）
		if item.closing.Load() {
			p.log.Debug("连接 [%s] 正在关闭，退出消息循环", formatConnID(item.ConnectionID))
			return
		}

		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}

		msg, err := protocol.Decode(data)
		if err != nil {
			p.log.Debug("消息解码失败: %v", err)
			continue
		}

		// 通过 StreamManager 分发消息到对应的 stream
		p.mu.RLock()
		mgr, ok := p.managerByConn[item]
		p.mu.RUnlock()

		if ok {
			mgr.DispatchMessage(msg)
		}
	}
}

// handleConnectionFailure 处理连接失败
func (p *ConnectionPool) handleConnectionFailure() {
	// 触发节点重新评分
	if p.relayManager.ForceRescore() {
		p.log.Info("节点重新评分完成，后续连接将使用负载均衡选择")
	}
}

// GetConnectionWithStream 原子化地获取连接并分配 Stream ID。
//
// GetConnectionWithStream 是推荐使用的方法，它确保获取连接和分配 Stream 是原子操作，
// 避免了先获取连接再分配 Stream 时可能出现的阻塞问题。
//
// 工作流程：
//  1. 优先从空闲池获取连接（按质量评分排序）
//  2. 尝试从活跃连接中选择负载最低的连接
//  3. 如果所有连接都已满，创建新连接或排队等待
//  4. 分配 Stream ID（0-255）
//  5. 返回连接和 Stream ID
//
// 参数：
//   - ctx: 上下文对象（支持超时和取消）
//   - targetAddr: 目标地址（格式：host:port）
//
// 返回值：
//   - *ConnItem: 连接项
//   - byte: 分配的 Stream ID
//   - error: 错误（如果有）
func (p *ConnectionPool) GetConnectionWithStream(ctx context.Context, targetAddr string) (*ConnItem, byte, error) {
	maxStreams := int(p.cfg.MaxStreamsPerConnection)
	deadline, hasDeadline := ctx.Deadline()

	p.log.Debug("GetConnectionWithStream 开始 -> %s", targetAddr)

	retryCount := 0
	for {
		// 快速检查：先不持有锁，只持有读锁来检查是否有可用连接
		var bestConn *ConnItem
		var bestMgr *StreamManager

		p.mu.Lock()

		// 记录当前池状态
		idleCount := len(p.pool)
		activeCount := int(atomic.LoadInt32(&p.activeConnections))
		pendingCount := int(atomic.LoadInt32(&p.pendingConnections))

		// 1. 检查空闲池（从头部取，头部是最高质量/最低RTT）
		for len(p.pool) > 0 {
			item := p.pool[0]
			p.pool = p.pool[1:]

			// 标记连接已从空闲池取出
			item.inPool.Store(false)

			if item.WS == nil {
				continue
			}

			// 检查连接是否正在关闭（防止使用已关闭的连接）
			if item.closing.Load() {
				p.log.Debug("跳过正在关闭的连接: [%s]", formatConnID(item.ConnectionID))
				continue
			}

			// 检查连接是否正在排空（会话轮换）
			if p.sessionRotator != nil && !p.sessionRotator.ShouldUseConnection(item) {
				p.log.Debug("跳过排空状态的连接: [%s]", formatConnID(item.ConnectionID))
				continue
			}

			// 获取或创建 StreamManager
			mgr, ok := p.managerByConn[item]
			if !ok {
				mgr = NewStreamManager(
					item,
					maxStreams,
					p.cfg.GetDefaultWindowSize(),
					p.cfg.GetMinWindowSize(),
					p.cfg.GetMaxWindowSize(),
					p.cfg.GetWindowTimeout(),
				)
				p.managerByConn[item] = mgr
			}

			// 尝试立即分配流
			streamID, allocated := mgr.tryAllocateStream(targetAddr)
			if allocated {
				atomic.AddInt32(&p.activeConnections, 1)
				p.mu.Unlock()

				p.log.Debug("获取连接+流: [%s] Stream[%02x] -> %s (空闲连接, RTT:%dms)",
					formatConnID(item.ConnectionID), streamID, targetAddr,
					time.Duration(item.RTT.Load()).Milliseconds())
				return item, streamID, nil
			}
			// 分配失败，连接已满，放回池的末尾
			item.inPool.Store(true)
			p.pool = append(p.pool, item)
		}

		// 2. 检查亲和性连接
		if targetAddr != "" && p.cfg.EnableMultiplex {
			if affinityConn, exists := p.targetToConn[targetAddr]; exists {
				// 检查连接是否正在关闭
				if !affinityConn.closing.Load() {
					if mgr, ok := p.managerByConn[affinityConn]; ok && affinityConn.WS != nil {
						streamID, allocated := mgr.tryAllocateStream(targetAddr)
						if allocated {
							p.mu.Unlock()
							p.log.Debug("获取连接+流: [%s] Stream[%02x] -> %s (亲和连接)",
								formatConnID(affinityConn.ConnectionID), streamID, targetAddr)
							return affinityConn, streamID, nil
						}
					}
				}
			}
		}

		// 3. 检查活跃连接中流数最少的
		if p.cfg.EnableMultiplex {
			minStreams := maxStreams + 1
			for item, mgr := range p.managerByConn {
				if item.WS == nil || item.closing.Load() {
					continue
				}
				streamCount := mgr.GetStreamCount()
				if streamCount < minStreams {
					minStreams = streamCount
					bestConn = item
					bestMgr = mgr
				}
			}

			// 尝试使用流数最少的连接
			if bestConn != nil && bestMgr != nil && minStreams < maxStreams {
				streamID, allocated := bestMgr.tryAllocateStream(targetAddr)
				if allocated {
					p.mu.Unlock()
					p.log.Debug("获取连接+流: [%s] Stream[%02x] -> %s (活跃连接,流数:%d)",
						formatConnID(bestConn.ConnectionID), streamID, targetAddr, minStreams)
					return bestConn, streamID, nil
				}
			}
		}

		// 4. 所有连接都满载，需要创建新连接
		currentSize := idleCount + activeCount + pendingCount

		if currentSize >= p.cfg.MaxPoolSize {
			p.mu.Unlock()
			p.log.Warn("所有连接已满载（%d/%d），无法满足请求 -> %s", currentSize, p.cfg.MaxPoolSize, targetAddr)
			return nil, 0, fmt.Errorf("所有连接已满载（%d/%d）", currentSize, p.cfg.MaxPoolSize)
		}

		p.log.Debug("所有连接已满，触发新连接创建 (当前:%d/%d, 待创建:%d) -> %s",
			currentSize, p.cfg.MaxPoolSize, pendingCount, targetAddr)

		// 触发连接创建（如果还没有正在创建的连接）
		// 使用 CompareAndSwap 避免竞态条件：只有一个 goroutine 能成功从 0 -> 1
		if atomic.CompareAndSwapInt32(&p.pendingConnections, 0, 1) {
			p.mu.Unlock()
			go func() {
				success := p.createConnection("请求触发")
				if !success {
					// 创建失败，记录日志（计数器会在后面重置）
					p.log.Debug("新连接创建失败")
				}
				atomic.AddInt32(&p.pendingConnections, -1)
			}()
		} else {
			p.mu.Unlock()
		}

		retryCount++
		// 等待新连接可用
		select {
		case <-time.After(50 * time.Millisecond):
			// 继续循环重试
			p.log.Debug("重试 %d: 等待新连接 (空闲:%d, 活跃:%d, 待建:%d) -> %s",
				retryCount, idleCount, activeCount, pendingCount, targetAddr)
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}

		if !hasDeadline {
			// 没有 deadline，最多重试一定次数
			continue
		}

		// 检查是否超时
		if time.Until(deadline) <= 0 {
			return nil, 0, fmt.Errorf("获取连接超时")
		}
	}
}

// GetConnection 从连接池获取连接。
//
// GetConnection 使用智能选择算法从连接池中选择最优连接，支持多路复用模式。
// 这是传统的获取连接方法，不包含 Stream ID 分配（需要单独调用 AllocateStreamID）。
//
// 选择策略：
//  1. 优先从空闲池选择（按质量评分排序）
//  2. 检查目标地址亲和性（优先选择已服务该目标的连接）
//  3. 从活跃连接中选择负载最低的连接
//  4. 如果所有连接都已满，创建新连接或排队等待
//
// 评分算法：
//   - 负载因子（权重 60%）：活跃流数 / 最大流数
//   - RTT（权重 40%）：归一化 RTT（假设 2000ms 为最差情况）
//
// 参数：
//   - ctx: 上下文对象（支持超时和取消）
//   - targetAddr: 目标地址（格式：host:port）
//
// 返回值：
//   - *ConnItem: 连接项
//   - error: 错误（如果有）
//
// 注意：推荐使用 GetConnectionWithStream 方法，它提供原子化的连接获取和 Stream 分配。
func (p *ConnectionPool) GetConnection(ctx context.Context, targetAddr string) (*ConnItem, error) {
	var selectedItem *ConnItem
	var selectedReason string
	var selectedScore float64
	var isAffinity bool
	var isFromPool bool

	// 选择和移除必须是原子操作，全程持有锁
	p.mu.Lock()

	maxStreams := int(p.cfg.MaxStreamsPerConnection)

	// 辅助函数：计算连接评分
	calcScore := func(item *ConnItem, streams int) float64 {
		loadFactor := 0.0
		if maxStreams > 0 {
			loadFactor = float64(streams) / float64(maxStreams)
		}
		// RTT 归一化 (假设 2000ms 为最差情况)
		rttNorm := float64(item.RTT.Load()) / (2000.0 * 1e6)
		if rttNorm > 1.0 {
			rttNorm = 1.0
		}
		// 综合评分：负载 60% + RTT 40%（越低越好）
		return loadFactor*0.6 + rttNorm*0.4
	}

	// 1. 首先检查空闲池（优先使用空闲连接）
	// 空闲池已按质量评分排序（在 ReleaseConnection 和 QualityMonitor 中维护）
	var lowQualityConns []*ConnItem // 收集低质量连接，稍后关闭
	if len(p.pool) > 0 {
		// 直接从头部取连接（已排序，头部是最高质量）
		for len(p.pool) > 0 {
			item := p.pool[0]
			p.pool = p.pool[1:]

			if item.WS == nil {
				continue
			}

			// 检查连接是否正在排空（会话轮换）
			if p.sessionRotator != nil && !p.sessionRotator.ShouldUseConnection(item) {
				p.log.Debug("跳过排空状态的连接: [%s]", formatConnID(item.ConnectionID))
				continue
			}

			// 检查连接质量评分
			qualityScore := atomic.LoadInt64(&item.QualityScore)
			if qualityScore < 40 {
				// 质量过低，收集起来稍后关闭（避免持有锁时调用 Close）
				lowQualityConns = append(lowQualityConns, item)
				p.log.Warn("连接 [%s] 质量过低 (分数=%d)，跳过使用", formatConnID(item.ConnectionID), qualityScore)
				continue
			}

			selectedItem = item
			selectedReason = fmt.Sprintf("空闲连接(质量=%d)", qualityScore)
			selectedScore = calcScore(item, 0)
			isFromPool = true
			break
		}
	}

	// 2. 如果没有从空闲池选择到，检查亲和性连接和活跃连接
	if selectedItem == nil && p.cfg.EnableMultiplex {
		// 检查目标地址亲和性
		if targetAddr != "" {
			if affinityConn, exists := p.targetToConn[targetAddr]; exists {
				if mgr, ok := p.managerByConn[affinityConn]; ok && affinityConn.WS != nil {
					streamCount := mgr.GetStreamCount()
					hasSpace := streamCount < maxStreams

					if hasSpace && mgr.HasTarget(targetAddr) {
						selectedItem = affinityConn
						selectedReason = fmt.Sprintf("亲和连接(目标:%s, streams:%d/%d)", targetAddr, streamCount, maxStreams)
						selectedScore = calcScore(affinityConn, streamCount) - 0.5 // 亲和连接有加成
						isAffinity = true
					}
				}
			}
		}

		// 3. 如果没有亲和连接，从活跃连接中找最优的
		if selectedItem == nil {
			for item, mgr := range p.managerByConn {
				if item.WS == nil {
					continue
				}

				streamCount := mgr.GetStreamCount()
				hasSpace := streamCount < maxStreams
				if !hasSpace {
					continue
				}

				score := calcScore(item, streamCount)
				if selectedItem == nil || score < selectedScore {
					selectedItem = item
					selectedScore = score
					selectedReason = fmt.Sprintf("活跃连接(streams:%d/%d, rtt:%dms)",
						streamCount, maxStreams, time.Duration(item.RTT.Load()).Milliseconds())
				}
			}
		}
	}

	// 如果从空闲池选择了连接，增加活跃计数
	if isFromPool {
		atomic.AddInt32(&p.activeConnections, 1)
	}

	// 把低质量连接放回空闲池末尾（让 messageLoop defer 正确处理）
	for _, conn := range lowQualityConns {
		conn.inPool.Store(true)
		p.pool = append(p.pool, conn)
	}

	p.mu.Unlock()

	// 释放锁后，关闭低质量连接（会触发 messageLoop defer 清理）
	for _, conn := range lowQualityConns {
		conn.WS.Close()
	}

	// 如果找到了连接，返回它
	if selectedItem != nil {
		if isFromPool || isAffinity {
			p.log.Debug("选择连接: [%s] %s (score:%.3f, affinity:%v)",
				formatConnID(selectedItem.ConnectionID), selectedReason, selectedScore, isAffinity)
		}
		return selectedItem, nil
	}

	// 没有可用连接，检查是否能新建
	currentSize := len(p.pool) + int(atomic.LoadInt32(&p.activeConnections)) +
		int(atomic.LoadInt32(&p.pendingConnections))
	if currentSize < p.cfg.MaxPoolSize {
		go p.createConnection("请求触发")
	}

	// 加入等待队列
	req := &connRequest{
		connCh: make(chan *ConnItem, 1),
		errCh:  make(chan error, 1),
	}

	select {
	case p.requestQueue <- req:
		select {
		case item := <-req.connCh:
			return item, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// formatConnID 格式化连接 ID
func formatConnID(connID []byte) string {
	if len(connID) != 3 {
		return "??????"
	}
	return fmt.Sprintf("%02x%02x%02x", connID[0], connID[1], connID[2])
}

// ReleaseConnection 释放连接回连接池。
//
// ReleaseConnection 将使用完毕的连接归还到空闲池，供后续请求复用。
// 只有当连接没有活跃 Stream 时才会放回池中，否则保持在活跃状态。
//
// 工作流程：
//  1. 检查连接是否已经被关闭（通过 managerByConn 检查）
//  2. 获取当前 Stream 数量
//  3. 如果 Stream 数量为 0，使用 CAS 操作防止重复放入
//  4. 按质量评分降序插入到空闲池中（高质量连接在前）
//
// 参数：
//   - item: 要释放的连接项
//
// 注意：
//   - 使用 CAS 操作确保连接不会被重复放入空闲池
//   - 连接的 managerByConn 条目不会被删除，由 messageLoop 负责清理
//   - 多次调用此方法是安全的（CAS 操作保证幂等性）
func (p *ConnectionPool) ReleaseConnection(item *ConnItem) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查连接是否已经被关闭（managerByConn 已被 messageLoop defer 删除）
	mgr, hasManager := p.managerByConn[item]
	if !hasManager {
		// 连接已经被关闭，不需要处理
		return
	}

	streamCount := mgr.GetStreamCount()

	if streamCount == 0 {
		// 使用 CAS 操作防止重复放入空闲池
		// 如果 inPool 已经是 true，说明连接已经在空闲池中，直接返回
		if !item.inPool.CompareAndSwap(false, true) {
			return
		}

		// 没有活跃的 stream，放回池中以供重用
		// 注意：不删除 managerByConn 条目，因为 messageLoop 需要它来分发消息
		// 连接会在关闭时由 messageLoop 的 defer 函数清理

		// 有序插入：按质量评分降序插入
		score := atomic.LoadInt64(&item.QualityScore)
		insertPos := len(p.pool)
		for i := 0; i < len(p.pool); i++ {
			if atomic.LoadInt64(&p.pool[i].QualityScore) < score {
				insertPos = i
				break
			}
		}

		// 插入到正确位置
		p.pool = append(p.pool, nil)
		copy(p.pool[insertPos+1:], p.pool[insertPos:])
		p.pool[insertPos] = item

		atomic.AddInt32(&p.activeConnections, -1)
	}
	// 如果还有活跃的 stream，连接保持活跃状态，直到最后一个释放
}

// ============================================================================
// 亲和性分数管理
// ============================================================================

// UpdateAffinityScore 更新连接对目标地址的亲和分数。
//
// UpdateAffinityScore 根据请求成功或失败调整连接对特定目标地址的亲和分数，
// 用于负载均衡时优先选择已经服务过该目标的连接（缓存命中优化）。
//
// 评分规则：
//   - 成功：分数 +10
//   - 失败：分数 -50
//   - 负分数自动重置为 0
//
// 参数：
//   - conn: 连接项
//   - targetAddr: 目标地址（格式：host:port）
//   - success: 请求是否成功
//
// 并发安全：使用 affinityMu 互斥锁保护。
func (p *ConnectionPool) UpdateAffinityScore(conn *ConnItem, targetAddr string, success bool) {
	if targetAddr == "" {
		return
	}

	p.affinityMu.Lock()
	defer p.affinityMu.Unlock()

	if p.connAffinityScore == nil {
		p.connAffinityScore = make(map[*ConnItem]map[string]int64)
	}

	if p.connAffinityScore[conn] == nil {
		p.connAffinityScore[conn] = make(map[string]int64)
	}

	if success {
		p.connAffinityScore[conn][targetAddr] += 10
	} else {
		p.connAffinityScore[conn][targetAddr] -= 50
		// 负分数重置为 0
		if p.connAffinityScore[conn][targetAddr] < 0 {
			p.connAffinityScore[conn][targetAddr] = 0
		}
	}
}

// GetAffinityScore 获取连接对目标地址的亲和分数。
//
// GetAffinityScore 返回连接对特定目标地址的亲和分数，用于负载均衡时
// 优先选择已经服务过该目标的连接（缓存命中优化）。
//
// 参数：
//   - conn: 连接项
//   - targetAddr: 目标地址（格式：host:port）
//
// 返回值：亲和分数（int64），如果没有记录返回 0。
//
// 并发安全：使用 affinityMu 读锁保护。
func (p *ConnectionPool) GetAffinityScore(conn *ConnItem, targetAddr string) int64 {
	if targetAddr == "" {
		return 0
	}

	p.affinityMu.RLock()
	defer p.affinityMu.RUnlock()

	if p.connAffinityScore == nil {
		return 0
	}

	if p.connAffinityScore[conn] == nil {
		return 0
	}

	return p.connAffinityScore[conn][targetAddr]
}

// GetAllActiveConnections 获取所有活跃连接。
//
// GetAllActiveConnections 返回所有活跃的 WebSocket 连接，包括空闲池中的连接
// 和正在使用的连接。主要用于 Metrics 暴露和流量统计。
//
// 返回值：所有活跃连接的切片。
//
// 并发安全：使用 mu 读锁保护。
func (p *ConnectionPool) GetAllActiveConnections() []*ConnItem {
	p.mu.RLock()
	result := make([]*ConnItem, 0, len(p.pool)+len(p.managerByConn))
	result = append(result, p.pool...)

	for conn := range p.managerByConn {
		result = append(result, conn)
	}
	p.mu.RUnlock()

	// 在锁外进行去重和过滤（使用 map 来去重）
	seen := make(map[*ConnItem]struct{}, len(result))
	unique := make([]*ConnItem, 0, len(result))
	for _, conn := range result {
		if _, exists := seen[conn]; !exists {
			// 过滤掉已关闭的连接
			if conn.closing.Load() {
				continue
			}
			seen[conn] = struct{}{}
			unique = append(unique, conn)
		}
	}

	return unique
}

// RegisterStreamHandler 注册 Stream 的消息处理器。
//
// RegisterStreamHandler 为指定的 Stream 注册消息处理回调函数，用于处理该 Stream
// 接收到的消息（CONNECTED、DATA、CLOSE 等）。如果连接的 StreamManager 不存在，
// 会自动创建。在多路复用模式下，还会记录目标地址亲和性。
//
// 参数：
//   - item: 连接项
//   - streamID: Stream ID（0-255）
//   - handler: 消息处理器（包含 OnMessage、OnClose、OnCleanup 回调）
//   - targetAddr: 目标地址（格式：host:port）
//
// 并发安全：使用 mu 互斥锁保护。
func (p *ConnectionPool) RegisterStreamHandler(item *ConnItem, streamID byte, handler *StreamHandler, targetAddr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 获取或创建 StreamManager
	mgr, ok := p.managerByConn[item]
	if !ok {
		mgr = NewStreamManager(
			item,
			int(p.cfg.MaxStreamsPerConnection),
			p.cfg.GetDefaultWindowSize(),
			p.cfg.GetMinWindowSize(),
			p.cfg.GetMaxWindowSize(),
			p.cfg.GetWindowTimeout(),
		)
		p.managerByConn[item] = mgr
	}

	// 注册 handler
	mgr.RegisterHandler(streamID, handler)

	// 记录目标地址亲和性
	if targetAddr != "" && p.cfg.EnableMultiplex {
		// 添加目标到连接的目标集合
		item.AddTarget(targetAddr)
		// 建立目标到连接的映射
		p.targetToConn[targetAddr] = item

		p.log.Debug("记录目标亲和性: 目标=%s -> 连接=%s, Stream=%02x",
			targetAddr, formatConnID(item.ConnectionID), streamID)
	}
}

// AllocateStreamID 分配一个新的 Stream ID。
//
// AllocateStreamID 为指定连接分配一个空闲的 Stream ID（0-255），
// 如果所有 Stream ID 都已占用，会阻塞等待直到有空闲 Stream 或超时。
// 使用位图算法实现 O(1) 时间复杂度的 Stream ID 分配。
//
// 参数：
//   - item: 连接项
//   - targetAddr: 目标地址（用于日志和调试）
//   - timeout: 分配超时时间
//
// 返回值：
//   - byte: 分配的 Stream ID
//   - bool: 是否成功分配（超时返回 false）
func (p *ConnectionPool) AllocateStreamID(item *ConnItem, targetAddr string, timeout time.Duration) (byte, bool) {
	p.mu.Lock()

	// 获取或创建 StreamManager
	mgr, ok := p.managerByConn[item]
	if !ok {
		mgr = NewStreamManager(
			item,
			int(p.cfg.MaxStreamsPerConnection),
			p.cfg.GetDefaultWindowSize(),
			p.cfg.GetMinWindowSize(),
			p.cfg.GetMaxWindowSize(),
			p.cfg.GetWindowTimeout(),
		)
		p.managerByConn[item] = mgr
	}
	p.mu.Unlock()

	// 通过 StreamManager 分配 stream
	return mgr.AllocateStream(targetAddr, timeout)
}

// UnregisterStreamHandler 注销 Stream 的消息处理器。
//
// UnregisterStreamHandler 从 StreamManager 中移除指定的 Stream，释放 Stream ID，
// 并调用清理回调函数。同时更新连接的 Stream 计数和目标地址亲和性映射。
//
// 参数：
//   - item: 连接项
//   - streamID: 要注销的 Stream ID
//
// 返回值：
//   - targetAddr: 目标地址（用于清理亲和性映射）
//   - isEmpty: 该连接是否已无活跃 Stream
func (p *ConnectionPool) UnregisterStreamHandler(item *ConnItem, streamID byte) (targetAddr string, isEmpty bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	mgr, ok := p.managerByConn[item]
	if !ok {
		return "", true
	}

	targetAddr, isEmpty = mgr.UnregisterStream(streamID)

	// 如果该连接已无活跃 stream，清理亲和性映射
	if isEmpty && targetAddr != "" {
		delete(p.targetToConn, targetAddr)
		// 清理连接上的目标记录
		item.RemoveTarget(targetAddr)
	}

	return targetAddr, isEmpty
}

// maintainLoop 维护连接池大小
func (p *ConnectionPool) maintainLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.maintainPool()
		case <-p.stopChan:
			return
		}
	}
}

// maintainPool 维护连接池
func (p *ConnectionPool) maintainPool() {
	currentSize := len(p.pool) + int(atomic.LoadInt32(&p.activeConnections)) +
		int(atomic.LoadInt32(&p.pendingConnections))

	// 优先保证最小连接数（不管负载率如何）
	if currentSize < int(p.currentMinPoolSize) {
		p.createConnection("维护补给")
		return
	}

	// 多路复用模式下：根据实际负载率决定是否需要额外扩容
	if p.cfg.EnableMultiplex {
		p.mu.RLock()
		totalStreams := 0
		maxStreams := int(p.cfg.MaxStreamsPerConnection)

		for item, mgr := range p.managerByConn {
			if item.WS == nil {
				continue
			}
			totalStreams += mgr.GetStreamCount()
		}
		p.mu.RUnlock()

		// 计算当前负载率（基于总容量）
		totalCapacity := currentSize * maxStreams
		var currentLoad float64
		if totalCapacity > 0 {
			currentLoad = float64(totalStreams) / float64(totalCapacity)
		}

		// 负载率低于阈值时，不需要额外扩容
		lowThreshold := p.cfg.DynamicPoolLowThreshold // 默认 0.3
		if currentLoad < lowThreshold {
			return
		}
	}

	if currentSize >= p.cfg.MaxPoolSize {
		return
	}

	// 检查是否需要按需扩容
	needExpansion := false
	reason := ""

	// 1. 如果有等待队列，立即扩容
	if len(p.requestQueue) > 0 {
		needExpansion = true
		reason = fmt.Sprintf("等待队列(%d)", len(p.requestQueue))
	} else if p.cfg.EnableMultiplex && atomic.LoadInt32(&p.activeConnections) > 0 {
		// 2. 多路复用模式下：检查活跃连接的整体利用率
		p.mu.RLock()
		totalStreams := 0
		activeConnCount := 0
		maxStreams := int(p.cfg.MaxStreamsPerConnection)

		for item, mgr := range p.managerByConn {
			if item.WS == nil {
				continue
			}
			streamCount := mgr.GetStreamCount()
			totalStreams += streamCount
			activeConnCount++
		}
		p.mu.RUnlock()

		// 修复：计算正确的整体负载率
		// 负载率 = 总活跃流数 / (连接数 × 每连接最大流数)
		if activeConnCount > 0 {
			currentLoad := float64(totalStreams) / float64(activeConnCount*maxStreams)
			highThreshold := p.cfg.DynamicPoolHighThreshold // 默认 0.6

			if currentLoad > highThreshold {
				needExpansion = true
				reason = fmt.Sprintf("高负载(利用率%.1f%%: 流数=%d, 连接=%d, 最大=%d)",
					currentLoad*100, totalStreams, activeConnCount, maxStreams)
			}
		}
	}

	if needExpansion {
		p.log.Info("触发按需扩容: %s", reason)
		p.createConnection(fmt.Sprintf("按需扩容(%s)", reason))
	}

	// 每次维护时检查并清理劣质连接
	p.cullDegradedConnections()
}

// cullDegradedConnections 清理劣质连接
func (p *ConnectionPool) cullDegradedConnections() {
	connections := p.GetAllActiveConnections()
	culled := 0

	for _, conn := range connections {
		if conn.WS == nil {
			continue
		}

		// 检查是否已经标记为正在关闭（防止重复清理）
		if conn.closing.Load() {
			continue
		}

		// 检查是否正在排空（会话轮换中的连接不处理）
		if p.sessionRotator != nil && !p.sessionRotator.ShouldUseConnection(conn) {
			continue
		}

		score := conn.CalculateQualityScore()

		if score < 30 {
			connIDStr := formatConnID(conn.ConnectionID)
			p.log.Warn("连接 [%s] 质量过低 (%d)，主动关闭", connIDStr, score)

			// 标记为正在关闭
			conn.closing.Store(true)

			// 立即从所有数据结构中移除（防止重复使用）
			p.mu.Lock()
			delete(p.managerByConn, conn)
			// 从 p.pool 中移除
			for i, item := range p.pool {
				if item == conn {
					p.pool = append(p.pool[:i], p.pool[i+1:]...)
					break
				}
			}
			// 从 targetToConn 中移除相关映射
			for target, c := range p.targetToConn {
				if c == conn {
					delete(p.targetToConn, target)
				}
			}
			p.mu.Unlock()

			// 关闭 WebSocket（会触发 messageLoop 退出）
			conn.WS.Close()
			culled++
		}
	}

	if culled > 0 {
		p.log.Info("清理了 %d 个劣质连接", culled)
	}
}

// cullLoop 清理过期连接
func (p *ConnectionPool) cullLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.cullOldConnections()
		case <-p.stopChan:
			return
		}
	}
}

// cullOldConnections 清理过期连接（平滑销毁：每次只清理一个）
func (p *ConnectionPool) cullOldConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	beforeSize := len(p.pool)
	if beforeSize == 0 {
		return
	}

	now := time.Now()
	keepMin := min(p.cfg.MinPoolSize, int(p.currentMinPoolSize))

	// 平滑销毁：每次只清理一个过期连接，避免批量清理导致连接池突然变空
	for i, item := range p.pool {
		// 使用 ExpiresAt 判断是否过期（带随机偏移，实现错峰销毁）
		if beforeSize-1 >= keepMin && now.After(item.ExpiresAt) {
			// 从池中移除
			p.pool = append(p.pool[:i], p.pool[i+1:]...)
			item.WS.Close()
			atomic.AddInt64(&p.stats.ClosedConnections, 1)
			p.log.Debug("清理过期连接: [%s] 已过期 %v",
				formatConnID(item.ConnectionID), now.Sub(item.ExpiresAt))
			return // 每次只清理一个
		}
	}
}

// statsLoop 统计日志
func (p *ConnectionPool) statsLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.logStats()
		case <-p.stopChan:
			return
		}
	}
}

// logStats 输出统计日志
func (p *ConnectionPool) logStats() {
	total := len(p.pool) + int(atomic.LoadInt32(&p.activeConnections)) +
		int(atomic.LoadInt32(&p.pendingConnections))

	if total == 0 {
		return
	}

	idle := len(p.pool)
	active := int(atomic.LoadInt32(&p.activeConnections))
	pending := int(atomic.LoadInt32(&p.pendingConnections))
	queued := len(p.requestQueue)

	// 收集所有连接信息（包含 RTT 和 Stream 数）
	type connInfo struct {
		id      string
		rtt     time.Duration
		streams int
	}

	var allConns []connInfo

	// 从 managerByConn 获取所有连接
	p.mu.RLock()
	for item, mgr := range p.managerByConn {
		connIDStr := fmt.Sprintf("%02x%02x%02x", item.ConnectionID[0], item.ConnectionID[1], item.ConnectionID[2])
		allConns = append(allConns, connInfo{
			id:      connIDStr,
			rtt:     time.Duration(item.RTT.Load()),
			streams: mgr.GetStreamCount(),
		})
	}
	p.mu.RUnlock()

	if len(allConns) > 0 {
		// 按 RTT 排序
		sort.Slice(allConns, func(i, j int) bool {
			return allConns[i].rtt < allConns[j].rtt
		})

		var connParts []string
		for _, c := range allConns {
			status := "I"
			if c.streams > 0 {
				status = "A"
			}
			connParts = append(connParts, fmt.Sprintf("[%s:%dms:%ds:%s]",
				c.id, c.rtt.Milliseconds(), c.streams, status))
		}

		// 合并输出：连接池状态 + 所有连接 RTT 信息
		p.log.Debug("连接池: 空闲%d 活跃%d 建立中%d 等队列%d | 连接: %s",
			idle, active, pending, queued, strings.Join(connParts, " "))
	} else {
		p.log.Debug("连接池: 空闲%d 活跃%d 建立中%d 等队列%d",
			idle, active, pending, queued)
	}
}

// heartbeatLoop 心跳检测
func (p *ConnectionPool) heartbeatLoop() {
	ticker := time.NewTicker(p.cfg.GetHeartbeatInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.sendHeartbeat()
		case <-p.stopChan:
			return
		}
	}
}

// sendHeartbeat 发送心跳
func (p *ConnectionPool) sendHeartbeat() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	timeout := 0

	for _, item := range p.pool {
		if item.WS != nil {
			connIDStr := fmt.Sprintf("%02x%02x%02x", item.ConnectionID[0], item.ConnectionID[1], item.ConnectionID[2])

			// 检查是否有待响应的心跳
			if lastPing, ok := p.pendingHeartbeats[connIDStr]; ok {
				if now.Sub(lastPing) > p.cfg.GetHeartbeatTimeout() {
					p.log.Debug("连接 [%s] 心跳超时，移除", connIDStr)
					item.WS.Close()
					delete(p.pendingHeartbeats, connIDStr)
					timeout++
				}
			} else {
				// 发送新心跳
				if err := item.WriteMessage(websocket.PingMessage, nil); err == nil {
					p.pendingHeartbeats[connIDStr] = now
				} else {
					item.WS.Close()
				}
			}
		}
	}

	if timeout > 0 {
		p.log.Debug("心跳超时: %d 个连接", timeout)
	}
}

// trafficReportLoop 定期报告连接流量统计
func (p *ConnectionPool) trafficReportLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.logConnectionTraffic()
		case <-p.stopChan:
			return
		}
	}
}

// logConnectionTraffic 输出所有活跃连接的流量统计
func (p *ConnectionPool) logConnectionTraffic() {
	connData := p.GetConnectionsData()
	if len(connData) == 0 {
		return
	}

	p.log.Debug("========== 连接流量统计 ==========")
	for _, cd := range connData {
		connIDStr := fmt.Sprintf("%02x%02x%02x", cd.ConnectionID[0], cd.ConnectionID[1], cd.ConnectionID[2])
		p.log.Debug("WS[%s] → %s | ↑ %s | ↓ %s | Streams: %d",
			connIDStr, cd.RelayAddr,
			formatBytes(cd.Sent), formatBytes(cd.Recv), cd.StreamCount)
	}
	p.log.Debug("==================================")
}

// rateUpdateLoop 定期更新所有连接的速率统计
func (p *ConnectionPool) rateUpdateLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.UpdateAllRates()
		case <-p.stopChan:
			return
		}
	}
}

// dynamicPoolLoop 动态调整连接池大小
func (p *ConnectionPool) dynamicPoolLoop() {
	ticker := time.NewTicker(p.cfg.GetDynamicPoolInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.adjustPoolSize()
		case <-p.stopChan:
			return
		}
	}
}

// adjustPoolSize 动态调整连接池大小
func (p *ConnectionPool) adjustPoolSize() {
	p.mu.RLock()
	idle := len(p.pool)
	active := int(atomic.LoadInt32(&p.activeConnections))
	total := idle + active
	p.mu.RUnlock()

	if total == 0 {
		return
	}

	var utilizationRatio float64
	var activeRatio float64
	queued := len(p.requestQueue)

	// 多路复用模式下：计算流级别的利用率
	if p.cfg.EnableMultiplex {
		totalStreams := 0
		maxStreams := int(p.cfg.MaxStreamsPerConnection)

		p.mu.RLock()
		// 统计所有活跃连接的流数
		for conn := range p.managerByConn {
			conn.mu.Lock()
			totalStreams += int(conn.Streams.Load())
			conn.mu.Unlock()
		}
		p.mu.RUnlock()

		// 流级别利用率 = 总流数 / (总连接数 × 每连接最大流数)
		totalCapacity := total * maxStreams
		if totalCapacity > 0 {
			utilizationRatio = float64(totalStreams+queued) / float64(totalCapacity)
		}
		// 活跃率 = 有流的连接数 / 总连接数
		activeRatio = float64(active) / float64(total)
	} else {
		// 非多路复用模式：使用连接级别的利用率
		activeRatio = float64(active) / float64(total)
		utilizationRatio = float64(active+queued) / float64(total+queued)
	}

	newSize := int(p.currentMinPoolSize)
	var reason string

	if utilizationRatio > p.cfg.DynamicPoolHighThreshold {
		newSize = min(int(float64(p.currentMinPoolSize)*1.5), p.cfg.DynamicPoolMaxSize)
		reason = fmt.Sprintf("高负载 (利用率 %.1f%%)", utilizationRatio*100)
	} else if activeRatio < p.cfg.DynamicPoolLowThreshold && int(p.currentMinPoolSize) > p.cfg.DynamicPoolMinSize {
		newSize = max(int(float64(p.currentMinPoolSize)*0.7), p.cfg.DynamicPoolMinSize)
		reason = fmt.Sprintf("低负载 (活跃率 %.1f%%)", activeRatio*100)
	} else {
		return
	}

	if newSize != int(p.currentMinPoolSize) {
		p.log.Info("调整 minPoolSize: %d -> %d (%s)", p.currentMinPoolSize, newSize, reason)
		atomic.StoreInt32(&p.currentMinPoolSize, int32(newSize))

		// 扩容时创建新连接
		if newSize > total {
			p.log.Info("触发动态扩容: %d -> %d (%s)", total, newSize, reason)
			needed := newSize - total
			for i := 0; i < min(needed, 5); i++ {
				go p.createConnection("动态扩容")
			}
		}
	}
}

// GetEnhancedStats 获取增强的连接池统计信息。
//
// GetEnhancedStats 返回连接池的详细统计信息，包括请求统计、响应时间、
// 流量统计、连接数统计等。所有统计字段使用原子操作读取，确保并发安全。
//
// 统计信息包括：
//   - 请求统计：总请求数、成功数、失败数、超时数、成功率
//   - 响应时间：平均、最小、最大响应时间（毫秒）
//   - 流量统计：发送/接收字节数（包括已关闭连接和活跃连接）
//   - 连接统计：创建/关闭连接数、池大小、活跃连接数、排队请求数
//   - 运行时间：连接池启动以来的运行时间
//
// 返回值：PoolStatsInfo 结构体，包含所有统计信息。
//
// 并发安全：使用原子操作和读锁保护。
func (p *ConnectionPool) GetEnhancedStats() PoolStatsInfo {
	// 使用原子操作读取所有统计字段，避免数据竞争
	requests := atomic.LoadInt64(&p.stats.Requests)
	successes := atomic.LoadInt64(&p.stats.Successes)
	failures := atomic.LoadInt64(&p.stats.Failures)
	timeouts := atomic.LoadInt64(&p.stats.Timeouts)
	totalResponseTime := atomic.LoadInt64(&p.stats.TotalResponseTime)
	minResponseTime := atomic.LoadInt64(&p.stats.MinResponseTime)
	maxResponseTime := atomic.LoadInt64(&p.stats.MaxResponseTime)
	bytesSent := atomic.LoadInt64(&p.stats.BytesSent)
	bytesReceived := atomic.LoadInt64(&p.stats.BytesReceived)
	createdConnections := atomic.LoadInt64(&p.stats.CreatedConnections)
	closedConnections := atomic.LoadInt64(&p.stats.ClosedConnections)

	// 累加当前活跃连接的流量（总流量 = 已关闭连接流量 + 存活连接流量）
	p.mu.RLock()
	for conn := range p.managerByConn {
		sent, recv, _ := conn.Traffic.GetSnapshot()
		bytesSent += sent
		bytesReceived += recv
	}
	p.mu.RUnlock()

	uptime := time.Since(p.stats.StartTime)

	// 计算成功率（使用原子读取的值）
	successRate := 0.0
	if requests > 0 {
		successRate = float64(successes) / float64(requests) * 100
	}

	// 计算平均响应时间（使用原子读取的值）
	avgResponseTime := 0.0
	if successes > 0 {
		avgResponseTime = float64(totalResponseTime) / float64(successes)
	}

	return PoolStatsInfo{
		Requests:           requests,
		Successes:          successes,
		Failures:           failures,
		Timeouts:           timeouts,
		SuccessRate:        successRate,
		AvgResponseTime:    avgResponseTime,
		MinResponseTime:    float64(minResponseTime),
		MaxResponseTime:    float64(maxResponseTime),
		BytesSent:          bytesSent,
		BytesReceived:      bytesReceived,
		Uptime:             uptime,
		CreatedConnections: createdConnections,
		ClosedConnections:  closedConnections,
		PoolSize:           len(p.pool),
		ActiveConnections:  int(atomic.LoadInt32(&p.activeConnections)),
		PendingConnections: int(atomic.LoadInt32(&p.pendingConnections)),
		QueuedRequests:     len(p.requestQueue),
	}
}

// RecordRequestStart 记录请求开始。
//
// RecordRequestStart 增加请求计数，并返回当前时间戳（毫秒），
// 用于后续计算响应时间。
//
// 返回值：当前时间戳（毫秒）。
//
// 并发安全：使用原子操作。
func (p *ConnectionPool) RecordRequestStart() int64 {
	atomic.AddInt64(&p.stats.Requests, 1)
	return time.Now().UnixMilli()
}

// RecordRequestSuccess 记录请求成功。
//
// RecordRequestSuccess 增加成功计数，计算响应时间并累加到总响应时间，
// 同时更新最小和最大响应时间。使用 CAS 循环确保最小/最大值更新的原子性。
//
// 参数：
//   - startTime: 请求开始时间戳（毫秒），由 RecordRequestStart 返回
//
// 并发安全：使用原子操作和 CAS 循环。
func (p *ConnectionPool) RecordRequestSuccess(startTime int64) {
	atomic.AddInt64(&p.stats.Successes, 1)
	responseTime := time.Now().UnixMilli() - startTime
	atomic.AddInt64(&p.stats.TotalResponseTime, responseTime)

	// 更新最小/最大响应时间
	for {
		min := atomic.LoadInt64(&p.stats.MinResponseTime)
		if min == -1 || responseTime < min {
			if atomic.CompareAndSwapInt64(&p.stats.MinResponseTime, min, responseTime) {
				break
			}
		} else {
			break
		}
	}

	for {
		max := atomic.LoadInt64(&p.stats.MaxResponseTime)
		if responseTime > max {
			if atomic.CompareAndSwapInt64(&p.stats.MaxResponseTime, max, responseTime) {
				break
			}
		} else {
			break
		}
	}
}

// RecordRequestFailure 记录请求失败。
//
// RecordRequestFailure 增加失败计数，用于统计请求失败率。
//
// 并发安全：使用原子操作。
func (p *ConnectionPool) RecordRequestFailure() {
	atomic.AddInt64(&p.stats.Failures, 1)
}

// RecordRequestTimeout 记录请求超时。
//
// RecordRequestTimeout 增加超时计数，用于统计请求超时率。
//
// 并发安全：使用原子操作。
func (p *ConnectionPool) RecordRequestTimeout() {
	atomic.AddInt64(&p.stats.Timeouts, 1)
}

// RecordDataTransfer 记录数据传输。
//
// RecordDataTransfer 增加发送和接收的字节数统计，用于流量监控。
//
// 参数：
//   - sent: 发送的字节数
//   - received: 接收的字节数
//
// 并发安全：使用原子操作。
func (p *ConnectionPool) RecordDataTransfer(sent, received int64) {
	atomic.AddInt64(&p.stats.BytesSent, sent)
	atomic.AddInt64(&p.stats.BytesReceived, received)
}

// UpdateAllRates 更新所有连接的速率统计。
//
// UpdateAllRates 遍历所有活跃连接，调用其 Traffic.UpdateRates 方法更新
// 发送/接收速率统计。直接访问 managerByConn 映射，避免调用
// GetAllActiveConnections() 造成额外的内存分配开销。
//
// 并发安全：使用 mu 读锁保护连接列表的读取。
func (p *ConnectionPool) UpdateAllRates() {
	now := time.Now()

	p.mu.RLock()
	conns := make([]*ConnItem, 0, len(p.managerByConn))
	for conn := range p.managerByConn {
		conns = append(conns, conn)
	}
	p.mu.RUnlock()

	for _, conn := range conns {
		conn.Traffic.UpdateRates(now)
	}
}

// GetStats 获取统计信息指针。
//
// GetStats 返回连接池的统计信息结构体指针，用于原子操作访问统计字段。
// 调用方可以使用 atomic 包的函数读取或修改统计字段。
//
// 返回值：PoolStats 结构体指针。
func (p *ConnectionPool) GetStats() *PoolStats {
	return &p.stats
}

// Close 关闭连接池。
//
// Close 停止连接池的所有后台维护循环，关闭所有 WebSocket 连接，
// 并释放相关资源。调用此方法后，连接池实例不应再被使用。
//
// 清理步骤：
//  1. 关闭 stopChan 通道，停止后台维护循环
//  2. 停止会话轮换器（如果启用）
//  3. 关闭空闲池中的所有连接
//  4. 关闭活跃连接，并调用 StreamManager 的 HandleConnectionClose
//
// 并发安全：使用 mu 互斥锁保护。
func (p *ConnectionPool) Close() {
	close(p.stopChan)

	// 停止会话轮换器
	if p.sessionRotator != nil {
		p.sessionRotator.Stop()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, item := range p.pool {
		item.WS.Close()
	}
	for item, mgr := range p.managerByConn {
		mgr.HandleConnectionClose()
		item.WS.Close()
	}
	p.pool = nil
	p.managerByConn = nil
}

// PoolStatsInfo 表示连接池统计信息的快照。
//
// PoolStatsInfo 包含连接池的详细统计信息，由 GetEnhancedStats 方法返回。
// 所有字段都是快照值，反映调用时刻的状态。
//
// 统计字段：
//   - 请求统计：Requests（总数）、Successes（成功）、Failures（失败）、Timeouts（超时）、SuccessRate（成功率%）
//   - 响应时间：AvgResponseTime（平均）、MinResponseTime（最小）、MaxResponseTime（最大），单位毫秒
//   - 流量统计：BytesSent（发送）、BytesReceived（接收），单位字节
//   - 连接统计：CreatedConnections（创建）、ClosedConnections（关闭）、PoolSize（池大小）、ActiveConnections（活跃）、PendingConnections（建立中）
//   - 其他：Uptime（运行时间）、QueuedRequests（排队请求数）
type PoolStatsInfo struct {
	Requests           int64
	Successes          int64
	Failures           int64
	Timeouts           int64
	SuccessRate        float64
	AvgResponseTime    float64
	MinResponseTime    float64
	MaxResponseTime    float64
	BytesSent          int64
	BytesReceived      int64
	Uptime             time.Duration
	CreatedConnections int64
	ClosedConnections  int64
	PoolSize           int
	ActiveConnections  int
	PendingConnections int
	QueuedRequests     int
}

// ConnectionData 表示单个连接的详细数据。
//
// ConnectionData 包含单个 WebSocket 连接的详细信息，由 GetConnectionsData 方法返回。
// 主要用于 Metrics 暴露和监控，提供连接级别的统计数据。
//
// 字段说明：
//   - ConnectionID: 连接标识（3 字节）
//   - RelayAddr: 中转节点地址（格式：host:port）
//   - RTT: 往返时延
//   - Sent: 发送的字节数
//   - Recv: 接收的字节数
//   - StreamCount: 活跃 Stream 数量（空闲连接为 0）
//   - RateSnapshot: 速率快照数据
type ConnectionData struct {
	ConnectionID []byte
	RelayAddr    string
	RTT          time.Duration
	Sent         int64
	Recv         int64
	StreamCount  int          // 使用 StreamManager.GetStreamCount() 作为权威来源
	RateSnapshot RateSnapshot // 速率快照数据
}

// RateSnapshot 表示连接的速率快照数据。
//
// RateSnapshot 包含连接的发送和接收速率统计，嵌入在 ConnectionData 中。
// 速率单位为字节/秒，由 TrafficCounter.GetRateSnapshot 方法计算。
//
// 字段说明：
//   - AvgSent: 平均发送速率（字节/秒）
//   - MaxSent: 最大发送速率（字节/秒）
//   - AvgRecv: 平均接收速率（字节/秒）
//   - MaxRecv: 最大接收速率（字节/秒）
type RateSnapshot struct {
	AvgSent float64 // 平均发送速率 (字节/秒)
	MaxSent float64 // 最大发送速率 (字节/秒)
	AvgRecv float64 // 平均接收速率 (字节/秒)
	MaxRecv float64 // 最大接收速率 (字节/秒)
}

// GetConnectionsData 获取所有连接的详细数据。
//
// GetConnectionsData 返回所有活跃连接的详细信息，包括连接标识、中转地址、
// RTT、流量统计、Stream 数量和速率快照。主要用于 Metrics 暴露和监控。
//
// 数据来源：
//   - 空闲池中的连接（Stream 数量为 0）
//   - 活跃连接（从 StreamManager 获取 Stream 数量）
//   - 自动过滤掉 closing 状态的连接
//
// 返回值：ConnectionData 切片，包含所有连接的详细信息。
//
// 并发安全：使用 mu 读锁保护。
func (p *ConnectionPool) GetConnectionsData() []ConnectionData {
	p.mu.RLock()
	result := make([]ConnectionData, 0, len(p.pool)+len(p.managerByConn))

	// 从空闲池获取连接（过滤 closing 状态）
	for _, conn := range p.pool {
		if conn.closing.Load() {
			continue
		}
		sent, recv, _ := conn.Traffic.GetSnapshot()
		avgSent, maxSent, avgRecv, maxRecv := conn.Traffic.GetRateSnapshot()
		result = append(result, ConnectionData{
			ConnectionID: conn.ConnectionID,
			RelayAddr:    conn.RelayAddr,
			RTT:          time.Duration(conn.RTT.Load()),
			Sent:         sent,
			Recv:         recv,
			StreamCount:  0, // 空闲连接没有 stream
			RateSnapshot: RateSnapshot{
				AvgSent: avgSent,
				MaxSent: maxSent,
				AvgRecv: avgRecv,
				MaxRecv: maxRecv,
			},
		})
	}

	// 从 managerByConn 获取连接及其 stream 计数（过滤 closing 状态）
	for conn, mgr := range p.managerByConn {
		if conn.closing.Load() {
			continue
		}
		sent, recv, _ := conn.Traffic.GetSnapshot()
		avgSent, maxSent, avgRecv, maxRecv := conn.Traffic.GetRateSnapshot()
		result = append(result, ConnectionData{
			ConnectionID: conn.ConnectionID,
			RelayAddr:    conn.RelayAddr,
			RTT:          time.Duration(conn.RTT.Load()),
			Sent:         sent,
			Recv:         recv,
			StreamCount:  mgr.GetStreamCount(), // 使用 StreamManager.GetStreamCount() 作为权威来源
			RateSnapshot: RateSnapshot{
				AvgSent: avgSent,
				MaxSent: maxSent,
				AvgRecv: avgRecv,
				MaxRecv: maxRecv,
			},
		})
	}
	p.mu.RUnlock()

	// 去重（同一连接可能同时存在于 pool 和 managerByConn）
	seen := make(map[string]struct{})
	unique := make([]ConnectionData, 0, len(result))
	for _, cd := range result {
		connIDStr := fmt.Sprintf("%02x%02x%02x", cd.ConnectionID[0], cd.ConnectionID[1], cd.ConnectionID[2])
		if _, exists := seen[connIDStr]; !exists {
			seen[connIDStr] = struct{}{}
			unique = append(unique, cd)
		}
	}

	return unique
}

// min 返回最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max 返回最大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// GetStream 获取指定的 Stream 对象（用于流控）
func (p *ConnectionPool) GetStream(conn *ConnItem, streamID byte) *Stream {
	if conn == nil {
		return nil
	}

	p.mu.RLock()
	mgr, exists := p.managerByConn[conn]
	p.mu.RUnlock()

	if !exists || mgr == nil {
		return nil
	}

	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	return mgr.streams[streamID]
}

// congestionControlLoop 拥塞控制循环
func (p *ConnectionPool) congestionControlLoop() {
	ticker := time.NewTicker(p.cfg.GetCongestionControlInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.adjustAllStreamsWindow()
		case <-p.stopChan:
			return
		}
	}
}

// adjustAllStreamsWindow 调整所有活跃 Stream 的窗口大小
func (p *ConnectionPool) adjustAllStreamsWindow() {
	p.mu.RLock()
	managers := make([]*StreamManager, 0, len(p.managerByConn))
	for _, mgr := range p.managerByConn {
		managers = append(managers, mgr)
	}
	p.mu.RUnlock()

	adjustedCount := 0
	for _, mgr := range managers {
		mgr.mu.RLock()
		streams := make([]*Stream, 0, len(mgr.streams))
		for _, s := range mgr.streams {
			streams = append(streams, s)
		}
		mgr.mu.RUnlock()

		for _, stream := range streams {
			stream.AdjustWindowSize()
			adjustedCount++
		}
	}

	if adjustedCount > 0 {
		p.log.Debug("拥塞控制: 调整了 %d 个 Stream 的窗口大小", adjustedCount)
	}
}

// GetFlowControlStats 获取窗口流控和拥塞控制统计
func (p *ConnectionPool) GetFlowControlStats() (avgWindow, minWindow, maxWindow int64, avgRTT time.Duration, avgLossRate float64, streamCount int) {
	p.mu.RLock()
	managers := make([]*StreamManager, 0, len(p.managerByConn))
	for _, mgr := range p.managerByConn {
		managers = append(managers, mgr)
	}
	p.mu.RUnlock()

	var totalWindow int64
	var totalRTT time.Duration
	var totalLossRate float64
	minWindow = MaxWindowSize
	maxWindow = MinWindowSize

	for _, mgr := range managers {
		mgr.mu.RLock()
		streams := make([]*Stream, 0, len(mgr.streams))
		for _, s := range mgr.streams {
			streams = append(streams, s)
		}
		mgr.mu.RUnlock()

		for _, stream := range streams {
			windowSize := atomic.LoadInt64(&stream.windowSize)
			totalWindow += windowSize
			if windowSize < minWindow {
				minWindow = windowSize
			}
			if windowSize > maxWindow {
				maxWindow = windowSize
			}

			rtt := stream.GetAverageRTT()
			if rtt > 0 {
				totalRTT += rtt
			}

			totalLossRate += stream.GetLossRate()
			streamCount++
		}
	}

	if streamCount > 0 {
		avgWindow = totalWindow / int64(streamCount)
		avgRTT = totalRTT / time.Duration(streamCount)
		avgLossRate = totalLossRate / float64(streamCount)
	} else {
		minWindow = 0
		maxWindow = 0
	}

	return
}
