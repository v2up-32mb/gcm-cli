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

// EchManagerInterface ECH 管理器接口
type EchManagerInterface interface {
	GetTlsConfig(domain string, useEch bool) (*tls.Config, error)
}

// ConnItem 连接项
type ConnItem struct {
	WS           *websocket.Conn
	ConnectionID []byte // 3 bytes WS ID
	RelayAddr    string // 中转节点地址
	CreatedAt    time.Time
	RTT          time.Duration
	Streams      int                 // 当前活跃流数
	Traffic      *TrafficCounter     // 流量计数器
	mu           sync.Mutex          // 保护 Streams 和 targets
	writeMu      sync.Mutex          // 保护 WS 写操作
	targets      map[string]struct{} // 该连接服务的前往目标地址集合 (用于多路复用亲和性)
}

// WriteMessage 线程安全的 WebSocket 写入方法
func (c *ConnItem) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.WS.WriteMessage(messageType, data)
}

// AddTarget 添加目标地址到该连接的服务集合
func (c *ConnItem) AddTarget(target string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets == nil {
		c.targets = make(map[string]struct{})
	}
	c.targets[target] = struct{}{}
}

// RemoveTarget 从该连接的服务集合中移除目标地址
func (c *ConnItem) RemoveTarget(target string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets != nil {
		delete(c.targets, target)
	}
}

// HasTarget 检查该连接是否服务于指定目标地址
func (c *ConnItem) HasTarget(target string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets == nil {
		return false
	}
	_, exists := c.targets[target]
	return exists
}

// GetTargetCount 获取该连接服务的目标地址数量
func (c *ConnItem) GetTargetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.targets == nil {
		return 0
	}
	return len(c.targets)
}

// LoadFactor 计算负载因子 (0.0 - 1.0，越高越拥挤)
func (c *ConnItem) LoadFactor(maxStreams int) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if maxStreams <= 0 {
		return 0
	}
	if c.Streams >= maxStreams {
		return 1.0
	}
	return float64(c.Streams) / float64(maxStreams)
}

// StreamHandler 流处理器
type StreamHandler struct {
	OnMessage func(msg *protocol.Message)
	OnClose   func()
	OnError   func()
	OnCleanup func()
}

// ConnectionPool WebSocket 连接池
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

	currentRelay       *relay.RelayNode
	lastRelayFetchTime time.Time
	currentMinPoolSize int32

	stats    PoolStats
	stopChan chan struct{}
}

// connRequest 连接请求
type connRequest struct {
	connCh chan *ConnItem
	errCh  chan error
}

// PoolStats 连接池统计
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

// NewConnectionPool 创建连接池
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

	if cfg.EnableDynamicPool {
		go p.dynamicPoolLoop()
	}

	return p
}

// Warmup 连接池预热
func (p *ConnectionPool) Warmup() error {
	if !p.cfg.EnablePoolWarmup || p.currentMinPoolSize <= 0 {
		return nil
	}

	p.log.Info("开始预热连接池，目标: %d 个连接...", p.currentMinPoolSize)

	startTime := time.Now()
	targetSize := int(p.currentMinPoolSize)
	concurrency := p.cfg.WarmupConcurrency
	created := 0
	failed := 0
	consecutiveFailures := 0 // 连续失败计数

	// 分批次创建连接
	for created < targetSize {
		remaining := targetSize - created
		batchSize := min(remaining, concurrency)

		// 并发创建一批连接，使用 err channel 等待每个完成
		type result struct {
			success bool
		}
		results := make(chan result, batchSize)

		for i := 0; i < batchSize; i++ {
			go func() {
				// createConnection 内部会同步等待连接建立完成
				success := p.createConnectionSync("预热")
				results <- result{success: success}
			}()
		}

		// 等待所有连接创建完成
		batchFailed := 0
		for i := 0; i < batchSize; i++ {
			res := <-results
			if res.success {
				created++
				consecutiveFailures = 0 // 重置连续失败计数
			} else {
				failed++
				batchFailed++
			}
		}
		close(results)

		// 检查超时
		if time.Since(startTime) > p.cfg.GetWarmupTimeout() {
			p.log.Warn("预热超时，已创建 %d/%d 个连接 (失败: %d)", created, targetSize, failed)
			break
		}

		if created >= targetSize {
			break
		}

		// 如果这批全部失败，增加连续失败计数
		if batchFailed >= batchSize {
			consecutiveFailures++
			// 如果连续失败超过 3 次，快速退出（网络可能不可用）
			if consecutiveFailures > 3 {
				p.log.Warn("预热连续失败 %d 次，跳过预热", consecutiveFailures)
				break
			}
			p.log.Warn("本批连接全部失败，等待后重试...")
			time.Sleep(500 * time.Millisecond)
		} else {
			consecutiveFailures = 0
			time.Sleep(100 * time.Millisecond)
		}
	}

	elapsed := time.Since(startTime)
	p.log.Info("预热完成，创建 %d 个连接 (失败: %d)，耗时 %dms", created, failed, elapsed.Milliseconds())

	// 初始化中转节点
	p.initializeRelay()

	return nil
}

// initializeRelay 初始化当前使用的节点
func (p *ConnectionPool) initializeRelay() {
	p.currentRelay = p.relayManager.GetCurrentBest()
	p.lastRelayFetchTime = time.Now()

	if p.currentRelay != nil {
		p.log.Info("当前中转节点: %s:%d (%dms)",
			p.currentRelay.IP, p.currentRelay.Port, p.currentRelay.Latency.Milliseconds())
	} else {
		p.log.Warn("无可用的中转节点，将使用直连模式")
	}
}

// generateWSID 生成 WebSocket ID (3字节)
func (p *ConnectionPool) generateWSID() []byte {
	buf := make([]byte, 3)
	rand.Read(buf)
	return buf
}

// getTLSConfig 获取 TLS 配置（支持 ECH）
func (p *ConnectionPool) getTLSConfig() *tls.Config {
	if p.echManager != nil {
		tlsConfig, err := p.echManager.GetTlsConfig(p.cfg.WorkerHost, p.cfg.EnableECH)
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

	// 使用缓存的节点
	if p.currentRelay == nil {
		p.currentRelay = p.relayManager.GetCurrentBest()
	}
	relay := p.currentRelay

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

		// 连接失败都需要记录警告信息
		p.log.Warn("连接失败 (%s): %v (目标: %s)", reason, err, url)

		// 触发强制重评
		go p.handleConnectionFailure()

		return false
	}
	defer resp.Body.Close()

	latency := time.Since(startTime)
	connectionID := p.generateWSID()

	// 构建中转节点地址字符串
	relayAddr := p.cfg.WorkerHost // 直连模式使用 Worker 地址
	if relay != nil {
		relayAddr = fmt.Sprintf("%s:%d", relay.IP, relay.Port)
	}

	item := &ConnItem{
		WS:           ws,
		ConnectionID: connectionID,
		RelayAddr:    relayAddr,
		CreatedAt:    time.Now(),
		RTT:          latency,
		Streams:      0,
		Traffic:      &TrafficCounter{},
	}

	connIDStr := fmt.Sprintf("%06x", connectionID[0]<<16|connectionID[1]<<8|connectionID[2])
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
	p.managerByConn[item] = NewStreamManager(item, int(p.cfg.MaxStreamsPerConnection))
	p.mu.Unlock()

	// 启动消息处理循环
	go p.messageLoop(item)

	// 将连接加入池（同步操作，确保加入成功后才返回）
	p.mu.Lock()
	p.pool = append(p.pool, item)
	p.mu.Unlock()

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

	// 使用缓存的节点
	if p.currentRelay == nil {
		p.currentRelay = p.relayManager.GetCurrentBest()
	}
	relay := p.currentRelay

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

		// 连接失败都需要记录警告信息
		p.log.Warn("连接失败 (%s): %v (目标: %s)", reason, err, url)

		// 触发强制重评
		go p.handleConnectionFailure()

		return false
	}
	defer resp.Body.Close()

	latency := time.Since(startTime)
	connectionID := p.generateWSID()

	// 构建中转节点地址字符串
	relayAddr := p.cfg.WorkerHost // 直连模式使用 Worker 地址
	if relay != nil {
		relayAddr = fmt.Sprintf("%s:%d", relay.IP, relay.Port)
	}

	item := &ConnItem{
		WS:           ws,
		ConnectionID: connectionID,
		RelayAddr:    relayAddr,
		CreatedAt:    time.Now(),
		RTT:          latency,
		Streams:      0,
		Traffic:      &TrafficCounter{},
	}

	connIDStr := fmt.Sprintf("%06x", connectionID[0]<<16|connectionID[1]<<8|connectionID[2])
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
	p.managerByConn[item] = NewStreamManager(item, int(p.cfg.MaxStreamsPerConnection))
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
		p.pool = append(p.pool, item)
		p.mu.Unlock()
	}

	return true
}

// messageLoop 消息处理循环
func (p *ConnectionPool) messageLoop(item *ConnItem) {
	ws := item.WS
	connIDStr := fmt.Sprintf("%06x", item.ConnectionID[0]<<16|item.ConnectionID[1]<<8|item.ConnectionID[2])

	// 设置 Pong 处理器，处理心跳响应
	ws.SetPongHandler(func(appData string) error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// 计算 RTT 并更新连接信息
		if lastPing, ok := p.pendingHeartbeats[connIDStr]; ok {
			rtt := time.Since(lastPing)
			// 使用指数移动平均 (EMA) 更新 RTT，平滑波动
			// 新RTT = 0.7 * 旧RTT + 0.3 * 测量RTT
			item.RTT = time.Duration(int64(item.RTT)*7/10 + int64(rtt)*3/10)
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

		p.mu.Lock()
		// 从空闲池中移除
		for i, ci := range p.pool {
			if ci == item {
				p.pool = append(p.pool[:i], p.pool[i+1:]...)
				atomic.AddInt64(&p.stats.ClosedConnections, 1)
				break
			}
		}
		// 通知 StreamManager 清理所有 stream
		if mgr, ok := p.managerByConn[item]; ok {
			mgr.HandleConnectionClose()
			delete(p.managerByConn, item)
		}
		delete(p.pendingHeartbeats, connIDStr)
		p.mu.Unlock()

		// 静默关闭 WebSocket（忽略 "already closed" 错误）
		ws.Close() // nolint:errcheck
	}()

	for {
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
	if p.relayManager.ForceRescore() {
		newRelay := p.relayManager.GetCurrentBest()
		if newRelay != nil {
			p.mu.Lock()
			oldRelay := p.currentRelay
			p.currentRelay = newRelay
			p.mu.Unlock()

			if oldRelay == nil || oldRelay.IP != newRelay.IP || oldRelay.Port != newRelay.Port {
				p.log.Info("已切换中转节点: %s:%d -> %s:%d (%dms)",
					oldRelay.IP, oldRelay.Port, newRelay.IP, newRelay.Port, newRelay.Latency.Milliseconds())
			}
		}
	}
}

// GetConnectionWithStream 原子化地获取连接并分配流 ID
// 这是推荐使用的方法，它确保获取连接和分配流是原子操作，避免阻塞
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

		// 1. 检查空闲池
		for len(p.pool) > 0 {
			item := p.pool[len(p.pool)-1]
			p.pool = p.pool[:len(p.pool)-1]

			if item.WS != nil {
				// 获取或创建 StreamManager
				mgr, ok := p.managerByConn[item]
				if !ok {
					mgr = NewStreamManager(item, maxStreams)
					p.managerByConn[item] = mgr
				}

				// 尝试立即分配流
				streamID, allocated := mgr.tryAllocateStream(targetAddr)
				if allocated {
					atomic.AddInt32(&p.activeConnections, 1)
					p.mu.Unlock()

					p.log.Debug("获取连接+流: [%s] Stream[%02x] -> %s (空闲连接)",
						formatConnID(item.ConnectionID), streamID, targetAddr)
					return item, streamID, nil
				}
				// 分配失败，连接已满
				// 放回池的最前面（下次优先使用）
				p.pool = append([]*ConnItem{item}, p.pool...)
			}
		}

		// 2. 检查亲和性连接
		if targetAddr != "" && p.cfg.EnableMultiplex {
			if affinityConn, exists := p.targetToConn[targetAddr]; exists {
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

		// 3. 检查活跃连接中流数最少的
		if p.cfg.EnableMultiplex {
			minStreams := maxStreams + 1
			for item, mgr := range p.managerByConn {
				if item.WS == nil {
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

// GetConnection 获取连接（支持目标地址亲和性）
// 注意：此方法只获取连接，不分配流。调用者需要调用 AllocateStreamID 分配流。
// 推荐使用 GetConnectionWithStream 代替此方法。
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
		rttNorm := float64(item.RTT.Milliseconds()) / 2000.0
		if rttNorm > 1.0 {
			rttNorm = 1.0
		}
		// 综合评分：负载 60% + RTT 40%（越低越好）
		return loadFactor*0.6 + rttNorm*0.4
	}

	// 1. 首先检查空闲池（优先使用空闲连接）
	for len(p.pool) > 0 {
		item := p.pool[len(p.pool)-1]

		if item.WS != nil {
			selectedItem = item
			selectedReason = "空闲连接"
			selectedScore = calcScore(item, 0)
			// 移除并标记为来自池中
			p.pool = p.pool[:len(p.pool)-1]
			isFromPool = true
			break
		}
		// 如果 item.WS == nil，移除并继续检查下一个
		p.pool = p.pool[:len(p.pool)-1]
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
						streamCount, maxStreams, item.RTT.Milliseconds())
				}
			}
		}
	}

	// 如果从空闲池选择了连接，增加活跃计数
	if isFromPool {
		atomic.AddInt32(&p.activeConnections, 1)
	}

	p.mu.Unlock()

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
	return fmt.Sprintf("%06x", connID[0]<<16|connID[1]<<8|connID[2])
}

// ReleaseConnection 释放连接
func (p *ConnectionPool) ReleaseConnection(item *ConnItem) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 通过 StreamManager 检查活跃 stream 数量
	mgr, hasManager := p.managerByConn[item]
	streamCount := 0
	if hasManager {
		streamCount = mgr.GetStreamCount()
	}

	if streamCount == 0 {
		// 没有活跃的 stream，放回池中以供重用
		// 注意：不删除 managerByConn 条目，因为 messageLoop 需要它来分发消息
		// 连接会在关闭时由 messageLoop 的 defer 函数清理
		p.pool = append(p.pool, item)
		atomic.AddInt32(&p.activeConnections, -1)
	}
	// 如果还有活跃的 stream，连接保持活跃状态，直到最后一个释放
}

// GetAllActiveConnections 获取所有活跃连接（包括空闲和正在使用的）
// 用于 metrics 暴露和流量统计
func (p *ConnectionPool) GetAllActiveConnections() []*ConnItem {
	p.mu.RLock()
	result := make([]*ConnItem, 0, len(p.pool)+len(p.managerByConn))
	result = append(result, p.pool...)

	for conn := range p.managerByConn {
		result = append(result, conn)
	}
	p.mu.RUnlock()

	// 在锁外进行去重（使用 map 来去重）
	seen := make(map[*ConnItem]struct{}, len(result))
	unique := make([]*ConnItem, 0, len(result))
	for _, conn := range result {
		if _, exists := seen[conn]; !exists {
			seen[conn] = struct{}{}
			unique = append(unique, conn)
		}
	}

	return unique
}

// RegisterStreamHandler 注册流处理器（支持目标地址亲和性）
func (p *ConnectionPool) RegisterStreamHandler(item *ConnItem, streamID byte, handler *StreamHandler, targetAddr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 获取或创建 StreamManager
	mgr, ok := p.managerByConn[item]
	if !ok {
		mgr = NewStreamManager(item, int(p.cfg.MaxStreamsPerConnection))
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

// AllocateStreamID 分配一个新的 Stream ID（阻塞等待可用）
// 返回分配的 Stream ID 和是否成功（false 表示超时）
func (p *ConnectionPool) AllocateStreamID(item *ConnItem, targetAddr string, timeout time.Duration) (byte, bool) {
	p.mu.Lock()

	// 获取或创建 StreamManager
	mgr, ok := p.managerByConn[item]
	if !ok {
		mgr = NewStreamManager(item, int(p.cfg.MaxStreamsPerConnection))
		p.managerByConn[item] = mgr
	}
	p.mu.Unlock()

	// 通过 StreamManager 分配 stream
	return mgr.AllocateStream(targetAddr, timeout)
}

// UnregisterStreamHandler 注销流处理器
// 返回目标地址（用于清理亲和性映射）和是否该连接已无活跃 stream
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

	if currentSize < int(p.currentMinPoolSize) {
		p.createConnection("维护补给")
		return
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
		hasHighLoadConn := false // 是否有高负载连接

		// 流数阈值：maxStreams 的 60%，超过此值认为连接负载较高
		streamThreshold := max(int(float64(maxStreams)*0.6), 1)

		for item, mgr := range p.managerByConn {
			if item.WS == nil {
				continue
			}
			streamCount := mgr.GetStreamCount()
			totalStreams += streamCount
			activeConnCount++

			// 只要有一个连接达到或超过阈值，就标记为高负载
			if streamCount >= streamThreshold {
				hasHighLoadConn = true
			}
		}
		p.mu.RUnlock()

		// 当存在高负载连接时，提前扩容（避免等到 100% 才触发）
		if activeConnCount > 0 && hasHighLoadConn {
			needExpansion = true
			reason = fmt.Sprintf("连接高负载(活跃:%d,总流:%d,阈值:%d)",
				activeConnCount, totalStreams, streamThreshold)
		}
	}

	if needExpansion {
		p.log.Info("触发按需扩容: %s", reason)
		p.createConnection(fmt.Sprintf("按需扩容(%s)", reason))
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

// cullOldConnections 清理过期连接
func (p *ConnectionPool) cullOldConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	beforeSize := len(p.pool)
	if beforeSize == 0 {
		return
	}

	now := time.Now()
	keepMin := min(p.cfg.MinPoolSize, int(p.currentMinPoolSize))

	newPool := make([]*ConnItem, 0, beforeSize)
	removed := 0

	// 按创建时间排序
	for _, item := range p.pool {
		if removed < beforeSize-keepMin && now.Sub(item.CreatedAt) > p.cfg.GetConnectionTTL() {
			item.WS.Close()
			atomic.AddInt64(&p.stats.ClosedConnections, 1)
			removed++
		} else {
			newPool = append(newPool, item)
		}
	}

	p.pool = newPool
	if removed > 0 {
		p.log.Debug("清理过期连接: 清除%d个 (%d -> %d)", removed, beforeSize, len(p.pool))
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
		connIDStr := fmt.Sprintf("%06x", item.ConnectionID[0]<<16|item.ConnectionID[1]<<8|item.ConnectionID[2])
		allConns = append(allConns, connInfo{
			id:      connIDStr,
			rtt:     item.RTT,
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
			connIDStr := fmt.Sprintf("%06x", item.ConnectionID[0]<<16|item.ConnectionID[1]<<8|item.ConnectionID[2])

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

	activeRatio := float64(active) / float64(total)
	queued := len(p.requestQueue)
	utilizationRatio := float64(active+queued) / float64(total+queued)

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

// GetEnhancedStats 获取增强统计信息
func (p *ConnectionPool) GetEnhancedStats() PoolStatsInfo {
	uptime := time.Since(p.stats.StartTime)
	successRate := 0.0
	if p.stats.Requests > 0 {
		successRate = float64(p.stats.Successes) / float64(p.stats.Requests) * 100
	}
	avgResponseTime := 0.0
	if p.stats.Successes > 0 {
		avgResponseTime = float64(p.stats.TotalResponseTime) / float64(p.stats.Successes)
	}

	return PoolStatsInfo{
		Requests:           p.stats.Requests,
		Successes:          p.stats.Successes,
		Failures:           p.stats.Failures,
		Timeouts:           p.stats.Timeouts,
		SuccessRate:        successRate,
		AvgResponseTime:    avgResponseTime,
		MinResponseTime:    float64(p.stats.MinResponseTime),
		MaxResponseTime:    float64(p.stats.MaxResponseTime),
		BytesSent:          p.stats.BytesSent,
		BytesReceived:      p.stats.BytesReceived,
		Uptime:             uptime,
		CreatedConnections: p.stats.CreatedConnections,
		ClosedConnections:  p.stats.ClosedConnections,
		PoolSize:           len(p.pool),
		ActiveConnections:  int(atomic.LoadInt32(&p.activeConnections)),
		PendingConnections: int(atomic.LoadInt32(&p.pendingConnections)),
		QueuedRequests:     len(p.requestQueue),
	}
}

// RecordRequestStart 记录请求开始
func (p *ConnectionPool) RecordRequestStart() int64 {
	atomic.AddInt64(&p.stats.Requests, 1)
	return time.Now().UnixMilli()
}

// RecordRequestSuccess 记录请求成功
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

// RecordRequestFailure 记录请求失败
func (p *ConnectionPool) RecordRequestFailure() {
	atomic.AddInt64(&p.stats.Failures, 1)
}

// RecordRequestTimeout 记录请求超时
func (p *ConnectionPool) RecordRequestTimeout() {
	atomic.AddInt64(&p.stats.Timeouts, 1)
}

// RecordDataTransfer 记录数据传输
func (p *ConnectionPool) RecordDataTransfer(sent, received int64) {
	atomic.AddInt64(&p.stats.BytesSent, sent)
	atomic.AddInt64(&p.stats.BytesReceived, received)
}

// UpdateAllRates 更新所有连接的速率统计
// 直接访问 managerByConn，避免调用 GetAllActiveConnections() 造成额外开销
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

// GetStats 获取统计信息指针（用于原子操作访问）
func (p *ConnectionPool) GetStats() *PoolStats {
	return &p.stats
}

// Close 关闭连接池
func (p *ConnectionPool) Close() {
	close(p.stopChan)

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

// PoolStatsInfo 连接池统计信息
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

// ConnectionData 连接数据（用于 metrics 暴露）
type ConnectionData struct {
	ConnectionID []byte
	RelayAddr    string
	RTT          time.Duration
	Sent         int64
	Recv         int64
	StreamCount  int          // 使用 StreamManager.GetStreamCount() 作为权威来源
	RateSnapshot RateSnapshot // 速率快照数据
}

// RateSnapshot 速率快照数据
type RateSnapshot struct {
	AvgSent float64 // 平均发送速率 (字节/秒)
	MaxSent float64 // 最大发送速率 (字节/秒)
	AvgRecv float64 // 平均接收速率 (字节/秒)
	MaxRecv float64 // 最大接收速率 (字节/秒)
}

// GetConnectionsData 获取所有连接的数据（用于 metrics 暴露）
// 返回包含流量统计和 stream 计数的连接数据列表
func (p *ConnectionPool) GetConnectionsData() []ConnectionData {
	p.mu.RLock()
	result := make([]ConnectionData, 0, len(p.pool)+len(p.managerByConn))

	// 从空闲池获取连接
	for _, conn := range p.pool {
		sent, recv, _ := conn.Traffic.GetSnapshot()
		avgSent, maxSent, avgRecv, maxRecv := conn.Traffic.GetRateSnapshot()
		result = append(result, ConnectionData{
			ConnectionID: conn.ConnectionID,
			RelayAddr:    conn.RelayAddr,
			RTT:          conn.RTT,
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

	// 从 managerByConn 获取连接及其 stream 计数
	for conn, mgr := range p.managerByConn {
		sent, recv, _ := conn.Traffic.GetSnapshot()
		avgSent, maxSent, avgRecv, maxRecv := conn.Traffic.GetRateSnapshot()
		result = append(result, ConnectionData{
			ConnectionID: conn.ConnectionID,
			RelayAddr:    conn.RelayAddr,
			RTT:          conn.RTT,
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
