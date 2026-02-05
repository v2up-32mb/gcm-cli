package pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gcm/logger"
	"gcm/protocol"
)

// 窗口流控常量。
//
// 这些常量定义了 Stream 窗口流控的默认参数，
// 用于防止接收端过载和实现拥塞控制。
const (
	DefaultWindowSize = 256 * 1024      // 默认窗口大小 256KB
	MinWindowSize     = 32 * 1024       // 最小窗口大小 32KB
	MaxWindowSize     = 1024 * 1024     // 最大窗口大小 1MB
	WindowTimeout     = 5 * time.Second // 窗口等待超时
)

// StreamState 表示 Stream 的状态枚举。
//
// StreamState 定义了 Stream 的生命周期状态，
// 遵循类似 TCP 的状态机转换规则。
type StreamState int

const (
	StreamStateIdle        StreamState = iota // 空闲状态
	StreamStateSynSent                        // 已发送 CONNECT
	StreamStateEstablished                    // 已收到 CONNECTED
	StreamStateFinWait                        // 已发送/收到 CLOSE
	StreamStateClosed                         // 完全关闭
)

// Stream 表示单个多路复用流的状态。
//
// Stream 实现了类似 yamux/smux 的多路复用流，
// 支持窗口流控、拥塞控制和状态机管理。每个 Stream
// 对应一个 SOCKS5 隧道连接。
//
// 并发安全：所有公开方法都是并发安全的。
type Stream struct {
	ID           byte
	TargetAddr   string
	CreatedAt    time.Time
	Handler      *StreamHandler
	BytesSent    int64
	BytesRecv    int64
	LastActiveAt time.Time

	// 窗口流控 (借鉴 yamux 设计)
	sendWindow  int64         // 可发送字节数（原子操作）
	recvWindow  int64         // 可接收字节数（原子操作）
	windowSize  int64         // 窗口大小（默认 256KB）
	sendBlocked chan struct{} // 发送阻塞通知
	windowMu    sync.Mutex    // 保护窗口操作

	// 拥塞控制 (借鉴 smux 设计)
	rttHistory   [10]time.Duration // RTT 历史记录（环形缓冲区）
	rttIndex     int               // RTT 历史索引
	baselineRTT  time.Duration     // 基线 RTT（最小值）
	timeoutCount int64             // 超时次数（原子操作）
	successCount int64             // 成功次数（原子操作）

	// 状态机与优先级
	state    StreamState // 当前状态
	priority int         // 优先级（0=低，1=中，2=高）
	stateMu  sync.Mutex  // 保护状态转换

	// 流控配置
	minWindowSize int64         // 最小窗口大小
	maxWindowSize int64         // 最大窗口大小
	windowTimeout time.Duration // 窗口等待超时
}

// StreamManager 表示单个 WebSocket 连接的 Stream 管理器。
//
// StreamManager 负责管理单个 WebSocket 连接上的所有多路复用流，
// 包括 Stream ID 分配、消息分发、窗口流控和生命周期管理。
// 使用位图算法实现 O(1) 时间复杂度的 Stream ID 分配。
//
// 每条 WebSocket 连接对应一个 StreamManager 实例。
//
// 并发安全：所有公开方法都是并发安全的。
type StreamManager struct {
	conn    *ConnItem        // 所属的连接
	max     int              // 最大 stream 数量
	streams map[byte]*Stream // Stream ID -> Stream
	mu      sync.RWMutex     // 保护 streams 映射
	log     *logger.Logger   // 日志器

	// 位图分配优化 (借鉴 smux 设计)
	allocBitmap [4]uint64 // 256 bits = 4 x 64-bit words，跟踪 Stream ID 占用状态
	nextHint    byte      // 上次分配的 ID + 1，避免重复扫描

	// 窗口流控配置
	defaultWindowSize int64         // 默认窗口大小
	minWindowSize     int64         // 最小窗口大小
	maxWindowSize     int64         // 最大窗口大小
	windowTimeout     time.Duration // 窗口等待超时
}

// NewStreamManager 创建并初始化 StreamManager。
//
// 参数：
//   - conn: 所属的 WebSocket 连接
//   - maxStreams: 最大 Stream 数量（通常为 256）
//   - defaultWindowSize: 默认窗口大小（字节）
//   - minWindowSize: 最小窗口大小（字节）
//   - maxWindowSize: 最大窗口大小（字节）
//   - windowTimeout: 窗口等待超时时间
//
// 返回值：初始化完成的 StreamManager 实例。
func NewStreamManager(conn *ConnItem, maxStreams int, defaultWindowSize, minWindowSize, maxWindowSize int64, windowTimeout time.Duration) *StreamManager {
	return &StreamManager{
		conn:              conn,
		max:               maxStreams,
		streams:           make(map[byte]*Stream),
		log:               logger.GetLogger("StreamMgr"),
		nextHint:          0, // 从 0 开始分配
		defaultWindowSize: defaultWindowSize,
		minWindowSize:     minWindowSize,
		maxWindowSize:     maxWindowSize,
		windowTimeout:     windowTimeout,
	}
}

// ============================================================================
// 位图分配算法 (借鉴 smux 设计)
// ============================================================================

// setBit 标记指定 Stream ID 为已分配
func (sm *StreamManager) setBit(id byte) {
	word := id / 64
	bit := id % 64
	sm.allocBitmap[word] |= (1 << bit)
}

// clearBit 标记指定 Stream ID 为已释放
func (sm *StreamManager) clearBit(id byte) {
	word := id / 64
	bit := id % 64
	sm.allocBitmap[word] &^= (1 << bit)
}

// isBitSet 检查指定 Stream ID 是否已分配
func (sm *StreamManager) isBitSet(id byte) bool {
	word := id / 64
	bit := id % 64
	return (sm.allocBitmap[word] & (1 << bit)) != 0
}

// findFreeBit 查找空闲的 Stream ID (O(1) 平均时间复杂度)
// 从 nextHint 开始循环查找，返回 (streamID, found)
func (sm *StreamManager) findFreeBit() (byte, bool) {
	// 从 nextHint 开始循环扫描 256 个位置
	for offset := 0; offset < 256; offset++ {
		id := byte((int(sm.nextHint) + offset) % 256)
		word := id / 64
		bit := id % 64

		// 检查该位是否空闲
		if (sm.allocBitmap[word] & (1 << bit)) == 0 {
			// 更新 nextHint 为下一个位置
			sm.nextHint = byte((int(id) + 1) % 256)
			return id, true
		}
	}

	// 所有 256 个 ID 都已占用
	return 0, false
}

// tryAllocateStream 尝试立即分配一个 Stream ID（不阻塞）
// 返回 Stream ID 和是否成功（如果连接已满则立即返回 false）
func (sm *StreamManager) tryAllocateStream(targetAddr string) (byte, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 检查是否已达到最大数量
	if len(sm.streams) >= sm.max {
		return 0, false
	}

	connIDStr := formatConnID(sm.conn.ConnectionID)

	// 使用位图算法查找空闲 Stream ID (O(1) 平均复杂度)
	streamID, found := sm.findFreeBit()
	if !found {
		// 理论上不应该发生（len < max 但找不到空闲位）
		sm.log.Warn("连接 [%s] 位图分配失败，但 len=%d < max=%d",
			connIDStr, len(sm.streams), sm.max)
		return 0, false
	}

	// 标记位图为已占用
	sm.setBit(streamID)

	// 注册 Stream（初始化窗口流控）
	sm.streams[streamID] = &Stream{
		ID:            streamID,
		TargetAddr:    targetAddr,
		CreatedAt:     time.Now(),
		LastActiveAt:  time.Now(),
		sendWindow:    sm.defaultWindowSize,
		recvWindow:    sm.defaultWindowSize,
		windowSize:    sm.defaultWindowSize,
		sendBlocked:   make(chan struct{}, 1),
		minWindowSize: sm.minWindowSize,
		maxWindowSize: sm.maxWindowSize,
		windowTimeout: sm.windowTimeout,
	}

	sm.conn.mu.Lock()
	sm.conn.Streams.Add(1)
	sm.conn.Traffic.IncStream()
	sm.conn.mu.Unlock()

	sm.log.Debug("连接 [%s] 分配 Stream[%02x] -> %s (位图优化)",
		connIDStr, streamID, targetAddr)

	return streamID, true
}

// AllocateStream 分配一个新的 Stream ID。
//
// 使用位图算法查找空闲的 Stream ID，支持超时等待。
// 如果连接已满，会等待直到有空闲 Stream 或超时。
//
// 参数：
//   - targetAddr: 目标地址（用于日志和调试）
//   - timeout: 分配超时时间
//
// 返回值：
//   - byte: 分配的 Stream ID
//   - bool: 是否成功分配（超时返回 false）
func (sm *StreamManager) AllocateStream(targetAddr string, timeout time.Duration) (byte, bool) {
	deadline := time.Now().Add(timeout)
	connIDStr := formatConnID(sm.conn.ConnectionID)

	for time.Now().Before(deadline) {
		sm.mu.Lock()

		// 检查是否已达到最大数量
		if len(sm.streams) >= sm.max {
			sm.mu.Unlock()
			sm.log.Debug("连接 [%s] 已达到最大 stream 数量 (%d)，等待中...",
				connIDStr, sm.max)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// 使用位图算法查找空闲 Stream ID (O(1) 平均复杂度)
		streamID, found := sm.findFreeBit()
		if !found {
			// 理论上不应该发生（len < max 但找不到空闲位）
			sm.mu.Unlock()
			sm.log.Warn("连接 [%s] 位图分配失败，但 len=%d < max=%d",
				connIDStr, len(sm.streams), sm.max)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// 标记位图为已占用
		sm.setBit(streamID)

		// 注册 Stream
		sm.streams[streamID] = &Stream{
			ID:           streamID,
			TargetAddr:   targetAddr,
			CreatedAt:    time.Now(),
			LastActiveAt: time.Now(),
			sendWindow:   sm.defaultWindowSize,
			recvWindow:   sm.defaultWindowSize,
			windowSize:   sm.defaultWindowSize,
			sendBlocked:  make(chan struct{}, 1),
		}

		sm.conn.mu.Lock()
		sm.conn.Streams.Add(1)
		sm.conn.Traffic.IncStream()
		sm.conn.mu.Unlock()
		sm.mu.Unlock()

		sm.log.Debug("连接 [%s] 分配 Stream[%02x] -> %s (位图优化)",
			connIDStr, streamID, targetAddr)

		return streamID, true
	}

	return 0, false
}

// RegisterHandler 注册 Stream 的消息处理器。
//
// 为指定的 Stream 设置消息处理回调函数，
// 用于处理该 Stream 接收到的消息。
//
// 参数：
//   - streamID: Stream ID
//   - handler: 消息处理器（包含 OnMessage、OnClose、OnCleanup 回调）
func (sm *StreamManager) RegisterHandler(streamID byte, handler *StreamHandler) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if s, exists := sm.streams[streamID]; exists {
		s.Handler = handler
		s.LastActiveAt = time.Now()
		connIDStr := formatConnID(sm.conn.ConnectionID)
		sm.log.Debug("连接 [%s] Stream[%02x] 注册处理器", connIDStr, streamID)
	}
}

// UnregisterStream 注销一个 Stream。
//
// 从 StreamManager 中移除指定的 Stream，释放 Stream ID，
// 并调用清理回调函数。同时更新连接的 Stream 计数。
//
// 参数：
//   - streamID: 要注销的 Stream ID
//
// 返回值：
//   - targetAddr: 目标地址（用于清理亲和性映射）
//   - isEmpty: 该连接是否已无活跃 Stream
func (sm *StreamManager) UnregisterStream(streamID byte) (targetAddr string, isEmpty bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	connIDStr := formatConnID(sm.conn.ConnectionID)

	if s, exists := sm.streams[streamID]; exists {
		targetAddr = s.TargetAddr

		// 调用清理回调
		if s.Handler != nil && s.Handler.OnCleanup != nil {
			s.Handler.OnCleanup()
		}

		delete(sm.streams, streamID)

		// 清除位图标记（释放 Stream ID）
		sm.clearBit(streamID)

		sm.conn.mu.Lock()
		if sm.conn.Streams.Load() > 0 {
			sm.conn.Streams.Add(-1)
		}
		sm.conn.mu.Unlock()

		// 减少活跃 Stream 计数
		sm.conn.Traffic.DecStream()

		sm.log.Debug("连接 [%s] Stream[%02x] 已注销 -> %s (剩余: %d)",
			connIDStr, streamID, targetAddr, len(sm.streams))

		return targetAddr, len(sm.streams) == 0
	}

	return "", len(sm.streams) == 0
}

// DispatchMessage 分发消息到对应的 Stream。
//
// 根据消息中的 Stream ID 查找对应的 Stream，
// 并调用其消息处理器的 OnMessage 回调。
//
// 参数：
//   - msg: 要分发的协议消息
func (sm *StreamManager) DispatchMessage(msg *protocol.Message) {
	sm.mu.RLock()
	s, exists := sm.streams[msg.StreamID]
	sm.mu.RUnlock()

	if exists && s.Handler != nil && s.Handler.OnMessage != nil {
		s.LastActiveAt = time.Now()
		s.Handler.OnMessage(msg)
	}
}

// HandleConnectionClose 处理连接关闭事件。
//
// 当 WebSocket 连接关闭时调用，通知所有 Stream 的处理器，
// 并清空所有 Stream 和位图分配状态。
func (sm *StreamManager) HandleConnectionClose() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	connIDStr := formatConnID(sm.conn.ConnectionID)
	sm.log.Debug("连接 [%s] 关闭，清理 %d 个 stream", connIDStr, len(sm.streams))

	// 通知所有 stream 连接已关闭
	for _, s := range sm.streams {
		if s.Handler != nil && s.Handler.OnClose != nil {
			s.Handler.OnClose()
		}
	}

	// 清空所有 stream
	sm.streams = make(map[byte]*Stream)

	// 清空位图（重置所有分配状态）
	sm.allocBitmap = [4]uint64{}
	sm.nextHint = 0
}

// GetStreamCount 获取当前活跃 Stream 数量。
//
// 返回值：当前 StreamManager 管理的 Stream 数量。
func (sm *StreamManager) GetStreamCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.streams)
}

// HasTarget 检查是否正在服务指定目标地址。
//
// 遍历所有 Stream，检查是否有 Stream 的目标地址匹配。
//
// 参数：
//   - targetAddr: 要检查的目标地址
//
// 返回值：如果有 Stream 正在服务该地址返回 true，否则返回 false。
func (sm *StreamManager) HasTarget(targetAddr string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, s := range sm.streams {
		if s.TargetAddr == targetAddr {
			return true
		}
	}
	return false
}

// GetLoadFactor 获取负载因子。
//
// 计算当前 Stream 数量占最大 Stream 数量的比例。
//
// 返回值：负载因子（0.0 - 1.0），1.0 表示已满。
func (sm *StreamManager) GetLoadFactor() float64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.max <= 0 {
		return 0
	}
	if len(sm.streams) >= sm.max {
		return 1.0
	}
	return float64(len(sm.streams)) / float64(sm.max)
}

// GetTargetCount 获取服务的不同目标地址数量。
//
// 统计所有 Stream 的目标地址，去重后返回数量。
//
// 返回值：不同目标地址的数量。
func (sm *StreamManager) GetTargetCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	targetSet := make(map[string]struct{})
	for _, s := range sm.streams {
		if s.TargetAddr != "" {
			targetSet[s.TargetAddr] = struct{}{}
		}
	}
	return len(targetSet)
}

// GetStreamInfo 获取所有 Stream 的信息。
//
// 返回所有 Stream 的详细信息，用于调试和监控。
//
// 返回值：Stream 信息列表。
func (sm *StreamManager) GetStreamInfo() []StreamInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	info := make([]StreamInfo, 0, len(sm.streams))
	for _, s := range sm.streams {
		info = append(info, StreamInfo{
			ID:         s.ID,
			TargetAddr: s.TargetAddr,
			Duration:   time.Since(s.CreatedAt),
			IdleTime:   time.Since(s.LastActiveAt),
		})
	}
	return info
}

// StreamInfo 表示 Stream 的信息快照。
//
// StreamInfo 用于调试和监控，包含 Stream 的基本信息和时间统计。
type StreamInfo struct {
	ID         byte          // Stream ID
	TargetAddr string        // 目标地址
	Duration   time.Duration // 存活时间
	IdleTime   time.Duration // 空闲时间
}

// String 返回 StreamInfo 的字符串表示。
//
// 格式：[ID:目标地址:存活时间:空闲时间]
//
// 返回值：格式化的字符串。
func (si StreamInfo) String() string {
	return fmt.Sprintf("[%02x:%s:%.1fs:%.1fs]",
		si.ID, si.TargetAddr,
		si.Duration.Seconds(), si.IdleTime.Seconds())
}

// ============================================================================
// 窗口流控方法 (借鉴 yamux 设计)
// ============================================================================

// WaitForSendWindow 等待发送窗口有足够空间。
//
// 使用原子操作检查并消耗发送窗口，如果窗口不足则等待。
// 实现了类似 TCP 的流量控制机制，防止发送端过快发送数据。
//
// 参数：
//   - n: 需要的窗口大小（字节）
//
// 返回值：如果超时或 Stream 已关闭返回 error，否则返回 nil。
func (s *Stream) WaitForSendWindow(n int) error {
	if n <= 0 {
		return nil
	}

	deadline := time.Now().Add(s.windowTimeout)
	for {
		// 原子读取当前窗口
		window := atomic.LoadInt64(&s.sendWindow)
		if window >= int64(n) {
			// 窗口足够，原子减少
			if atomic.CompareAndSwapInt64(&s.sendWindow, window, window-int64(n)) {
				return nil
			}
			// CAS 失败，重试
			continue
		}

		// 窗口不足，等待通知或超时
		if time.Now().After(deadline) {
			return fmt.Errorf("send window timeout after %v", WindowTimeout)
		}

		select {
		case <-s.sendBlocked:
			// 窗口可能已恢复，重新检查
		case <-time.After(100 * time.Millisecond):
			// 定期重试
		}
	}
}

// ConsumeRecvWindow 消耗接收窗口。
//
// 接收数据时调用，原子减少接收窗口。如果窗口低于 50%，
// 自动补充窗口以保持流畅接收。
//
// 参数：
//   - n: 接收的字节数
//
// 返回值：如果窗口耗尽返回 error，否则返回 nil。
func (s *Stream) ConsumeRecvWindow(n int) error {
	if n <= 0 {
		return nil
	}

	// 原子减少接收窗口
	newWindow := atomic.AddInt64(&s.recvWindow, -int64(n))
	if newWindow < 0 {
		// 窗口耗尽，需要补充
		return fmt.Errorf("recv window exhausted")
	}

	// 如果窗口低于 50%，自动补充
	windowSize := atomic.LoadInt64(&s.windowSize)
	if newWindow < windowSize/2 {
		s.RefillRecvWindow()
	}

	return nil
}

// RefillRecvWindow 补充接收窗口。
//
// 将接收窗口重置为默认窗口大小，用于窗口耗尽后的恢复。
func (s *Stream) RefillRecvWindow() {
	windowSize := atomic.LoadInt64(&s.windowSize)
	atomic.StoreInt64(&s.recvWindow, windowSize)
}

// RefillSendWindow 补充发送窗口。
//
// 接收到对端确认时调用，增加发送窗口并通知等待的发送者。
//
// 参数：
//   - n: 补充的字节数
func (s *Stream) RefillSendWindow(n int) {
	if n <= 0 {
		return
	}

	// 原子增加发送窗口
	atomic.AddInt64(&s.sendWindow, int64(n))

	// 通知等待的发送者
	select {
	case s.sendBlocked <- struct{}{}:
	default:
	}
}

// ============================================================================
// 拥塞控制方法 (借鉴 smux 设计)
// ============================================================================

// RecordRTT 记录 RTT 样本。
//
// 将 RTT 样本添加到历史记录（环形缓冲区），并更新基线 RTT。
// 用于拥塞检测和窗口大小调整。
//
// 参数：
//   - rtt: RTT 样本值
func (s *Stream) RecordRTT(rtt time.Duration) {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()

	// 更新 RTT 历史（环形缓冲区）
	s.rttHistory[s.rttIndex] = rtt
	s.rttIndex = (s.rttIndex + 1) % len(s.rttHistory)

	// 更新基线 RTT（取最小值）
	if s.baselineRTT == 0 || rtt < s.baselineRTT {
		s.baselineRTT = rtt
	}
}

// GetAverageRTT 获取平均 RTT。
//
// 计算历史记录中所有有效 RTT 样本的平均值。
//
// 返回值：平均 RTT，如果没有样本返回 0。
func (s *Stream) GetAverageRTT() time.Duration {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()
	return s.getAverageRTTLocked()
}

// getAverageRTTLocked 获取平均 RTT（调用者必须持有 windowMu 锁）
func (s *Stream) getAverageRTTLocked() time.Duration {
	var sum time.Duration
	count := 0
	for _, rtt := range s.rttHistory {
		if rtt > 0 {
			sum += rtt
			count++
		}
	}

	if count == 0 {
		return 0
	}
	return sum / time.Duration(count)
}

// DetectCongestion 检测是否发生拥塞。
//
// 基于 RTT 和丢包率判断是否发生拥塞：
//   - RTT 超过基线 RTT 的 2 倍，或
//   - 丢包率超过 5%
//
// 返回值：如果检测到拥塞返回 true，否则返回 false。
func (s *Stream) DetectCongestion() bool {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()

	// 如果没有足够的 RTT 样本，不判断拥塞
	if s.baselineRTT == 0 {
		return false
	}

	avgRTT := s.getAverageRTTLocked()
	if avgRTT == 0 {
		return false
	}

	// 计算丢包率
	totalCount := atomic.LoadInt64(&s.successCount) + atomic.LoadInt64(&s.timeoutCount)
	if totalCount == 0 {
		return false
	}
	lossRate := float64(atomic.LoadInt64(&s.timeoutCount)) / float64(totalCount)

	// 拥塞判断条件：RTT 翻倍 或 丢包率 > 5%
	return avgRTT > s.baselineRTT*2 || lossRate > 0.05
}

// RecordSuccess 记录成功的操作。
//
// 增加成功计数，用于计算丢包率和拥塞检测。
func (s *Stream) RecordSuccess() {
	atomic.AddInt64(&s.successCount, 1)
}

// RecordTimeout 记录超时的操作。
//
// 增加超时计数，用于计算丢包率和拥塞检测。
func (s *Stream) RecordTimeout() {
	atomic.AddInt64(&s.timeoutCount, 1)
}

// AdjustWindowSize 自适应调整窗口大小。
//
// 使用 TCP 风格的 AIMD（加性增、乘性减）算法：
//   - 检测到拥塞：窗口减半（最小为 minWindowSize）
//   - 无拥塞：窗口增加 8KB（最大为 maxWindowSize）
//
// 定期调用此方法以动态调整窗口大小，适应网络状况。
func (s *Stream) AdjustWindowSize() {
	if s.DetectCongestion() {
		// 拥塞：乘性减（减半）
		currentSize := atomic.LoadInt64(&s.windowSize)
		newSize := currentSize / 2
		if newSize < s.minWindowSize {
			newSize = s.minWindowSize
		}
		atomic.StoreInt64(&s.windowSize, newSize)

		// 同时调整发送窗口（使用 CAS 循环避免覆盖已消耗的窗口）
		for {
			oldWindow := atomic.LoadInt64(&s.sendWindow)
			// 只有当前窗口大于新窗口时才需要缩小
			if oldWindow > newSize {
				if atomic.CompareAndSwapInt64(&s.sendWindow, oldWindow, newSize) {
					break
				}
				// CAS 失败，重试
			} else {
				// 当前窗口已经小于等于新窗口，无需调整
				break
			}
		}
	} else {
		// 无拥塞：加性增（每次增加 8KB）
		currentSize := atomic.LoadInt64(&s.windowSize)
		newSize := currentSize + 8*1024
		if newSize > s.maxWindowSize {
			newSize = s.maxWindowSize
		}
		atomic.StoreInt64(&s.windowSize, newSize)
	}
}

// GetLossRate 获取丢包率。
//
// 计算超时次数占总操作次数的比例。
//
// 返回值：丢包率（0.0 - 1.0），如果没有操作记录返回 0。
func (s *Stream) GetLossRate() float64 {
	totalCount := atomic.LoadInt64(&s.successCount) + atomic.LoadInt64(&s.timeoutCount)
	if totalCount == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&s.timeoutCount)) / float64(totalCount)
}

// ============================================================================
// 状态机方法 (借鉴 yamux 设计)
// ============================================================================

// TransitionState 执行状态转换。
//
// 验证状态转换的合法性，只允许以下转换：
//   - Idle → SynSent
//   - SynSent → Established 或 Closed
//   - Established → FinWait 或 Closed
//   - FinWait → Closed
//
// 参数：
//   - newState: 目标状态
//
// 返回值：如果状态转换非法返回 error，否则返回 nil。
func (s *Stream) TransitionState(newState StreamState) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// 验证状态转换合法性
	valid := false
	switch s.state {
	case StreamStateIdle:
		valid = newState == StreamStateSynSent
	case StreamStateSynSent:
		valid = newState == StreamStateEstablished || newState == StreamStateClosed
	case StreamStateEstablished:
		valid = newState == StreamStateFinWait || newState == StreamStateClosed
	case StreamStateFinWait:
		valid = newState == StreamStateClosed
	case StreamStateClosed:
		valid = false // 已关闭，不能再转换
	}

	if !valid {
		return fmt.Errorf("invalid state transition: %v -> %v", s.state, newState)
	}

	s.state = newState
	return nil
}

// GetState 获取当前状态。
//
// 返回值：Stream 的当前状态。
func (s *Stream) GetState() StreamState {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.state
}

// SetPriority 设置 Stream 优先级。
//
// 优先级范围：0（低）- 2（高），超出范围会自动调整。
//
// 参数：
//   - priority: 优先级值（0-2）
func (s *Stream) SetPriority(priority int) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if priority < 0 {
		priority = 0
	}
	if priority > 2 {
		priority = 2
	}
	s.priority = priority
}

// GetPriority 获取 Stream 优先级。
//
// 返回值：Stream 的优先级（0-2）。
func (s *Stream) GetPriority() int {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.priority
}
