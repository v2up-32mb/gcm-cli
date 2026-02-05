package pool

import (
	"fmt"
	"sync"
	"time"

	"gcm/config"
	"gcm/logger"
)

// SessionRotator 表示连接会话自动轮换器。
//
// SessionRotator 管理 WebSocket 连接的生命周期，应对 Cloudflare Worker
// 的 110 秒时长限制。当连接达到最大寿命时，进入排空状态（不再接受新请求），
// 等待现有流完成后关闭连接。采用平滑销毁策略，每次只处理一个连接。
//
// 工作流程：
//  1. 定期检查所有连接的年龄
//  2. 达到 maxLifetime 的连接进入排空状态
//  3. 排空期间等待现有流完成
//  4. 超过 drainTimeout 或流数为 0 时关闭连接
//
// 并发安全：所有公开方法都是并发安全的。
type SessionRotator struct {
	pool          *ConnectionPool
	maxLifetime   time.Duration
	drainTimeout  time.Duration
	log           *logger.Logger
	mu            sync.RWMutex
	drainingConns map[*ConnItem]time.Time // 正在排空的连接 -> 排空开始时间
	stopChan      chan struct{}
}

// NewSessionRotator 创建并初始化会话轮换器。
//
// 参数：
//   - pool: 连接池实例
//   - cfg: 配置对象
//
// 返回值：初始化完成的 SessionRotator 实例（自动启动后台轮换循环）。
func NewSessionRotator(pool *ConnectionPool, cfg *config.Config) *SessionRotator {
	sr := &SessionRotator{
		pool:          pool,
		maxLifetime:   cfg.GetMaxSessionLifetime(),
		drainTimeout:  cfg.GetSessionDrainTimeout(),
		log:           logger.GetLogger("Rotator"),
		drainingConns: make(map[*ConnItem]time.Time),
		stopChan:      make(chan struct{}),
	}

	// 启动轮换循环
	go sr.rotationLoop()

	return sr
}

// rotationLoop 定期检查并轮换老旧连接
func (sr *SessionRotator) rotationLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sr.checkAndRotateSessions()
		case <-sr.stopChan:
			return
		}
	}
}

// checkAndRotateSessions 检查并轮换需要更新的连接（平滑销毁：每次只处理一个）
func (sr *SessionRotator) checkAndRotateSessions() {
	connections := sr.pool.GetAllActiveConnections()
	now := time.Now()

	// 平滑销毁：每次只处理一个连接
	var connToClose *ConnItem

	for _, conn := range connections {
		if conn.WS == nil {
			continue
		}

		age := now.Sub(conn.CreatedAt)

		// 检查是否需要开始排空
		if age >= sr.maxLifetime {
			sr.mu.Lock()
			if _, draining := sr.drainingConns[conn]; !draining {
				sr.drainingConns[conn] = now
				connIDStr := formatConnID(conn.ConnectionID)
				sr.log.Info("连接 [%s] 达到最大寿命 %v，开始排空 (流数:%d)",
					connIDStr, age, conn.Streams.Load())
			}
			sr.mu.Unlock()
		}

		// 检查是否需要强制关闭
		sr.mu.Lock()
		drainStart, draining := sr.drainingConns[conn]
		sr.mu.Unlock()

		if draining {
			drainAge := now.Sub(drainStart)
			conn.mu.Lock()
			streams := conn.Streams.Load()
			conn.mu.Unlock()

			if drainAge > sr.drainTimeout || streams == 0 {
				connToClose = conn
				break // 每次只关闭一个
			}
		}
	}

	// 在循环外关闭连接
	if connToClose != nil {
		connIDStr := formatConnID(connToClose.ConnectionID)
		sr.log.Info("连接 [%s] 排空完成，关闭连接", connIDStr)

		connToClose.closing.Store(true)
		connToClose.WS.Close()

		sr.mu.Lock()
		delete(sr.drainingConns, connToClose)
		sr.mu.Unlock()
	}
}

// IsDraining 检查连接是否正在排空。
//
// 参数：
//   - conn: 要检查的连接
//
// 返回值：如果连接正在排空返回 true，否则返回 false。
func (sr *SessionRotator) IsDraining(conn *ConnItem) bool {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	_, draining := sr.drainingConns[conn]
	return draining
}

// ShouldUseConnection 检查连接是否应该被使用。
//
// 参数：
//   - conn: 要检查的连接
//
// 返回值：如果连接可以使用（非排空状态）返回 true，否则返回 false。
func (sr *SessionRotator) ShouldUseConnection(conn *ConnItem) bool {
	return !sr.IsDraining(conn)
}

// Stop 停止会话轮换器。
//
// 停止后台轮换循环，释放资源。
// 调用此方法后，SessionRotator 实例不应再被使用。
func (sr *SessionRotator) Stop() {
	close(sr.stopChan)
}

// GetDrainingCount 获取正在排空的连接数量。
//
// 返回值：当前正在排空的连接数量。
func (sr *SessionRotator) GetDrainingCount() int {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return len(sr.drainingConns)
}

// GetStats 获取轮换器统计信息。
//
// 返回值：包含以下字段的统计信息 map：
//   - draining_count: 正在排空的连接数量
//   - max_lifetime: 最大连接寿命
//   - drain_timeout: 排空超时时间
//   - draining_connections: 正在排空的连接详情列表
func (sr *SessionRotator) GetStats() map[string]interface{} {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	stats := map[string]interface{}{
		"draining_count": len(sr.drainingConns),
		"max_lifetime":   sr.maxLifetime.String(),
		"drain_timeout":  sr.drainTimeout.String(),
	}

	// 添加正在排空的连接详情
	drainingList := make([]string, 0, len(sr.drainingConns))
	now := time.Now()
	for conn, drainStart := range sr.drainingConns {
		connIDStr := formatConnID(conn.ConnectionID)
		drainAge := now.Sub(drainStart)
		conn.mu.Lock()
		streams := conn.Streams.Load()
		conn.mu.Unlock()
		drainingList = append(drainingList,
			fmt.Sprintf("[%s] age=%v streams=%d", connIDStr, drainAge, streams))
	}
	stats["draining_connections"] = drainingList

	return stats
}

// RemoveConnection 从排空列表中移除连接。
//
// 用于连接异常关闭时的清理，确保排空列表不会累积已关闭的连接。
//
// 参数：
//   - conn: 要移除的连接
func (sr *SessionRotator) RemoveConnection(conn *ConnItem) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	delete(sr.drainingConns, conn)
}
