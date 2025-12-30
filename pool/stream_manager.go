package pool

import (
	"fmt"
	"sync"
	"time"

	"github.com/gcm/gcm/logger"
	"github.com/gcm/gcm/protocol"
)

// Stream 表示单个流的状态
type Stream struct {
	ID           byte
	TargetAddr   string
	CreatedAt    time.Time
	Handler      *StreamHandler
	BytesSent    int64
	BytesRecv    int64
	LastActiveAt time.Time
}

// StreamManager 管理单个 WebSocket 连接上的所有 stream
// 每条 WebSocket 连接对应一个 StreamManager
type StreamManager struct {
	conn    *ConnItem           // 所属的连接
	max     int                 // 最大 stream 数量
	streams map[byte]*Stream    // Stream ID -> Stream
	mu      sync.RWMutex        // 保护 streams 映射
	log     *logger.Logger      // 日志器
}

// NewStreamManager 创建新的 StreamManager
func NewStreamManager(conn *ConnItem, maxStreams int) *StreamManager {
	return &StreamManager{
		conn:    conn,
		max:     maxStreams,
		streams: make(map[byte]*Stream),
		log:     logger.GetLogger("StreamMgr"),
	}
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

	// 尝试最多 10 次随机分配
	for i := 0; i < 10; i++ {
		streamID := protocol.GenerateStreamID()
		if _, exists := sm.streams[streamID]; !exists {
			// 预先注册占位符
			sm.streams[streamID] = &Stream{
				ID:           streamID,
				TargetAddr:   targetAddr,
				CreatedAt:    time.Now(),
				LastActiveAt: time.Now(),
			}
			sm.conn.mu.Lock()
			sm.conn.Streams++
			sm.conn.Traffic.IncStream()  // 同步增加流量计数器
			sm.conn.mu.Unlock()

			sm.log.Debug("连接 [%s] 分配 Stream[%02x] -> %s",
				connIDStr, streamID, targetAddr)
			return streamID, true
		}
	}

	// 随机分配失败，尝试线性扫描
	for streamID := byte(0); ; streamID++ {
		if _, exists := sm.streams[streamID]; !exists {
			sm.streams[streamID] = &Stream{
				ID:           streamID,
				TargetAddr:   targetAddr,
				CreatedAt:    time.Now(),
				LastActiveAt: time.Now(),
			}
			sm.conn.mu.Lock()
			sm.conn.Streams++
			sm.conn.Traffic.IncStream()  // 同步增加流量计数器
			sm.conn.mu.Unlock()

			sm.log.Debug("连接 [%s] 分配 Stream[%02x] (扫描) -> %s",
				connIDStr, streamID, targetAddr)
			return streamID, true
		}
		// 检查是否溢出
		if streamID == 255 {
			break
		}
	}

	return 0, false
}

// AllocateStream 分配一个新的 Stream ID
// 返回 Stream ID 和是否成功（超时返回 false）
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

		// 尝试最多 10 次随机分配
		for i := 0; i < 10; i++ {
			streamID := protocol.GenerateStreamID()
			if _, exists := sm.streams[streamID]; !exists {
				// 预先注册占位符
				sm.streams[streamID] = &Stream{
					ID:           streamID,
					TargetAddr:   targetAddr,
					CreatedAt:    time.Now(),
					LastActiveAt: time.Now(),
				}
				sm.conn.mu.Lock()
				sm.conn.Streams++
				sm.conn.Traffic.IncStream() // 同步增加流量计数器
				sm.conn.mu.Unlock()
				sm.mu.Unlock()

				sm.log.Debug("连接 [%s] 分配 Stream[%02x] -> %s",
					connIDStr, streamID, targetAddr)
				return streamID, true
			}
		}

		// 随机分配失败，尝试线性扫描
		for streamID := byte(0); ; streamID++ {
			if _, exists := sm.streams[streamID]; !exists {
				sm.streams[streamID] = &Stream{
					ID:           streamID,
					TargetAddr:   targetAddr,
					CreatedAt:    time.Now(),
					LastActiveAt: time.Now(),
				}
				sm.conn.mu.Lock()
				sm.conn.Streams++
				sm.conn.Traffic.IncStream() // 同步增加流量计数器
				sm.conn.mu.Unlock()
				sm.mu.Unlock()

				sm.log.Debug("连接 [%s] 分配 Stream[%02x] (扫描) -> %s",
					connIDStr, streamID, targetAddr)
				return streamID, true
			}
			// 检查是否溢出
			if streamID == 255 {
				break
			}
		}

		sm.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	return 0, false
}

// RegisterHandler 注册 stream 的消息处理器
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

// UnregisterStream 注销一个 stream
// 返回目标地址（用于清理亲和性映射）和是否该连接已无活跃 stream
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

		sm.conn.mu.Lock()
		if sm.conn.Streams > 0 {
			sm.conn.Streams--
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

// DispatchMessage 分发消息到对应的 stream
func (sm *StreamManager) DispatchMessage(msg *protocol.Message) {
	sm.mu.RLock()
	s, exists := sm.streams[msg.StreamID]
	sm.mu.RUnlock()

	if exists && s.Handler != nil && s.Handler.OnMessage != nil {
		s.LastActiveAt = time.Now()
		s.Handler.OnMessage(msg)
	}
}

// HandleConnectionClose 处理连接关闭
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
}

// GetStreamCount 获取当前活跃 stream 数量
func (sm *StreamManager) GetStreamCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.streams)
}

// HasTarget 检查是否正在服务指定目标地址
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

// GetLoadFactor 获取负载因子 (0.0 - 1.0)
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

// GetTargetCount 获取服务的不同目标地址数量
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

// GetStreamInfo 获取所有 stream 的信息（用于调试）
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

// StreamInfo stream 信息（用于调试）
type StreamInfo struct {
	ID         byte
	TargetAddr string
	Duration   time.Duration
	IdleTime   time.Duration
}

// String 返回 StreamInfo 的字符串表示
func (si StreamInfo) String() string {
	return fmt.Sprintf("[%02x:%s:%.1fs:%.1fs]",
		si.ID, si.TargetAddr,
		si.Duration.Seconds(), si.IdleTime.Seconds())
}
