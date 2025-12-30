package metrics

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gcm/gcm/config"
	"github.com/gcm/gcm/dns"
	"github.com/gcm/gcm/logger"
	"github.com/gcm/gcm/pool"
	"github.com/gcm/gcm/relay"
)

// Server Metrics 服务器
type Server struct {
	cfg          *config.Config
	log          *logger.Logger
	pool         *pool.ConnectionPool
	relayManager *relay.RelayManager
	dnsCache     *dns.DNSCache
	server       *http.Server
	requestCount int64
	mu           sync.RWMutex
}

// NewServer 创建 Metrics 服务器
func NewServer(cfg *config.Config, p *pool.ConnectionPool, rm *relay.RelayManager, dc *dns.DNSCache) *Server {
	return &Server{
		cfg:          cfg,
		log:          logger.GetLogger("Metrics"),
		pool:         p,
		relayManager: rm,
		dnsCache:     dc,
	}
}

// Start 启动 Metrics 服务器
func (s *Server) Start() error {
	if !s.cfg.EnableMetrics {
		s.log.Debug("Metrics 端点未启用")
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/monitor", s.handleMonitor)

	// 静态文件服务
	fs := http.FileServer(http.Dir("statics"))
	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.cfg.MetricsPort),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,  // 读取请求超时
		WriteTimeout: 30 * time.Second,  // 写入响应超时（metrics可能较大）
		IdleTimeout:  60 * time.Second,  // 空闲连接超时
	}

	go func() {
		s.log.Info("监听端口: http://0.0.0.0:%d/metrics", s.cfg.MetricsPort)
		s.log.Info("端点: /metrics (Prometheus), /health (健康检查), /monitor (监控面板)")
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("启动失败: %v", err)
		}
	}()

	return nil
}

// handleMetrics 处理 /metrics 请求
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requestCount++
	s.mu.Unlock()

	clientAddr := r.RemoteAddr
	s.log.Debug("Metrics 请求来自: %s", clientAddr)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	// 使用 channel 和 select 实现超时保护
	type result struct {
		metrics string
	}
	resultChan := make(chan result, 1)

	go func() {
		resultChan <- result{metrics: s.generateMetrics()}
	}()

	select {
	case res := <-resultChan:
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(res.metrics))
	case <-time.After(10 * time.Second):
		// 生成超时，返回错误信息
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("# Metrics generation timeout\n"))
		s.log.Warn("Metrics 生成超时 (>10s) 来自: %s", clientAddr)
	}
}

// handleHealth 处理 /health 请求
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	clientAddr := r.RemoteAddr
	s.log.Debug("健康检查来自: %s", clientAddr)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))
}

// handleMonitor 处理 /monitor 请求
func (s *Server) handleMonitor(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requestCount++
	s.mu.Unlock()

	clientAddr := r.RemoteAddr
	s.log.Debug("Monitor 页面请求来自: %s", clientAddr)

	// 使用 channel 和 select 实现超时保护
	type result struct {
		content []byte
		err     error
	}
	resultChan := make(chan result, 1)

	go func() {
		// 构建静态文件路径 (从当前工作目录)
		staticPath := filepath.Join("statics", "monitor.html")
		content, err := os.ReadFile(staticPath)
		resultChan <- result{content: content, err: err}
	}()

	select {
	case res := <-resultChan:
		if res.err != nil {
			s.log.Error("读取 monitor.html 失败: %v", res.err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(res.content)
	case <-time.After(5 * time.Second):
		// 读取超时
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("<html><body>Monitor page load timeout</body></html>"))
		s.log.Warn("Monitor 页面加载超时 (>5s) 来自: %s", clientAddr)
	}
}

// generateMetrics 生成 Prometheus 格式的指标
func (s *Server) generateMetrics() string {
	s.log.Debug("[METRICS] 开始生成...")
	lines := make([]string, 0)

	// 连接池指标
	s.log.Debug("[METRICS] 获取 pool stats...")
	poolStats := s.pool.GetEnhancedStats()

	lines = append(lines, "# HELP gcm_pool_idle 连接池空闲连接数")
	lines = append(lines, "# TYPE gcm_pool_idle gauge")
	lines = append(lines, fmt.Sprintf("gcm_pool_idle %d", poolStats.PoolSize))

	lines = append(lines, "# HELP gcm_pool_active 连接池活跃连接数")
	lines = append(lines, "# TYPE gcm_pool_active gauge")
	lines = append(lines, fmt.Sprintf("gcm_pool_active %d", poolStats.ActiveConnections))

	lines = append(lines, "# HELP gcm_pool_pending 连接池正在建立的连接数")
	lines = append(lines, "# TYPE gcm_pool_pending gauge")
	lines = append(lines, fmt.Sprintf("gcm_pool_pending %d", poolStats.PendingConnections))

	lines = append(lines, "# HELP gcm_pool_queued 连接池等待中的请求数")
	lines = append(lines, "# TYPE gcm_pool_queued gauge")
	lines = append(lines, fmt.Sprintf("gcm_pool_queued %d", poolStats.QueuedRequests))

	// 请求统计指标
	lines = append(lines, "# HELP gcm_requests_total 总请求数")
	lines = append(lines, "# TYPE gcm_requests_total counter")
	lines = append(lines, fmt.Sprintf("gcm_requests_total %d", poolStats.Requests))

	lines = append(lines, "# HELP gcm_requests_success_total 成功请求数")
	lines = append(lines, "# TYPE gcm_requests_success_total counter")
	lines = append(lines, fmt.Sprintf("gcm_requests_success_total %d", poolStats.Successes))

	lines = append(lines, "# HELP gcm_requests_failure_total 失败请求数")
	lines = append(lines, "# TYPE gcm_requests_failure_total counter")
	lines = append(lines, fmt.Sprintf("gcm_requests_failure_total %d", poolStats.Failures))

	lines = append(lines, "# HELP gcm_requests_timeout_total 超时请求数")
	lines = append(lines, "# TYPE gcm_requests_timeout_total counter")
	lines = append(lines, fmt.Sprintf("gcm_requests_timeout_total %d", poolStats.Timeouts))

	lines = append(lines, "# HELP gcm_requests_success_rate 请求成功率(%)")
	lines = append(lines, "# TYPE gcm_requests_success_rate gauge")
	lines = append(lines, fmt.Sprintf("gcm_requests_success_rate %.2f", poolStats.SuccessRate))

	lines = append(lines, "# HELP gcm_request_duration_min_ms 最低请求延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_request_duration_min_ms gauge")
	if poolStats.MinResponseTime >= 0 {
		lines = append(lines, fmt.Sprintf("gcm_request_duration_min_ms %.0f", poolStats.MinResponseTime))
	} else {
		lines = append(lines, "gcm_request_duration_min_ms 0")
	}

	lines = append(lines, "# HELP gcm_request_duration_max_ms 最高请求延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_request_duration_max_ms gauge")
	lines = append(lines, fmt.Sprintf("gcm_request_duration_max_ms %.0f", poolStats.MaxResponseTime))

	// Per-connection 流量指标说明
	lines = append(lines, "# HELP gcm_conn_bytes_sent 单个 WebSocket 连接的发送字节数")
	lines = append(lines, "# TYPE gcm_conn_bytes_sent gauge")
	lines = append(lines, "# HELP gcm_conn_bytes_received 单个 WebSocket 连接的接收字节数")
	lines = append(lines, "# TYPE gcm_conn_bytes_received gauge")
	lines = append(lines, "# HELP gcm_conn_stream_count 单个 WebSocket 连接的 Stream 数量")
	lines = append(lines, "# TYPE gcm_conn_stream_count gauge")
	lines = append(lines, "# HELP gcm_conn_rtt 单个 WebSocket 连接的 RTT 延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_conn_rtt gauge")

	// Per-connection 流量数据（使用优化的方法获取连接数据）
	s.log.Debug("[METRICS] 获取 active connections...")
	connData := s.pool.GetConnectionsData()
	s.log.Debug("[METRICS] 获取到 %d 个连接", len(connData))

	for _, cd := range connData {
		wsID := formatConnID(cd.ConnectionID)
		sent, recv := cd.Sent, cd.Recv
		streams := cd.StreamCount // 使用 StreamManager.GetStreamCount() 作为权威来源
		relay := cd.RelayAddr
		rtt := cd.RTT.Milliseconds()

		lines = append(lines, fmt.Sprintf(`gcm_conn_bytes_sent{ws_id="%s",relay="%s"} %d`, wsID, relay, sent))
		lines = append(lines, fmt.Sprintf(`gcm_conn_bytes_received{ws_id="%s",relay="%s"} %d`, wsID, relay, recv))
		lines = append(lines, fmt.Sprintf(`gcm_conn_stream_count{ws_id="%s",relay="%s"} %d`, wsID, relay, streams))
		lines = append(lines, fmt.Sprintf(`gcm_conn_rtt{ws_id="%s",relay="%s"} %d`, wsID, relay, rtt))
	}

	// DNS 缓存指标
	s.log.Debug("[METRICS] 获取 DNS stats...")
	dnsStats := s.dnsCache.GetStats()

	lines = append(lines, "# HELP gcm_dns_cache_size DNS缓存条目数")
	lines = append(lines, "# TYPE gcm_dns_cache_size gauge")
	lines = append(lines, fmt.Sprintf("gcm_dns_cache_size %d", dnsStats.Size))

	lines = append(lines, "# HELP gcm_dns_cache_hits_total DNS缓存命中次数")
	lines = append(lines, "# TYPE gcm_dns_cache_hits_total counter")
	lines = append(lines, fmt.Sprintf("gcm_dns_cache_hits_total %d", dnsStats.Hits))

	lines = append(lines, "# HELP gcm_dns_cache_misses_total DNS缓存未命中次数")
	lines = append(lines, "# TYPE gcm_dns_cache_misses_total counter")
	lines = append(lines, fmt.Sprintf("gcm_dns_cache_misses_total %d", dnsStats.Misses))

	lines = append(lines, "# HELP gcm_dns_cache_hit_rate DNS缓存命中率")
	lines = append(lines, "# TYPE gcm_dns_cache_hit_rate gauge")
	lines = append(lines, fmt.Sprintf("gcm_dns_cache_hit_rate %.2f", dnsStats.HitRate))

	// 全局流量统计
	lines = append(lines, "# HELP gcm_bytes_sent_total 总发送字节数")
	lines = append(lines, "# TYPE gcm_bytes_sent_total counter")
	lines = append(lines, fmt.Sprintf("gcm_bytes_sent_total %d", poolStats.BytesSent))

	lines = append(lines, "# HELP gcm_bytes_received_total 总接收字节数")
	lines = append(lines, "# TYPE gcm_bytes_received_total counter")
	lines = append(lines, fmt.Sprintf("gcm_bytes_received_total %d", poolStats.BytesReceived))

	// 连接池创建/关闭统计
	lines = append(lines, "# HELP gcm_pool_created_total 创建连接总数")
	lines = append(lines, "# TYPE gcm_pool_created_total counter")
	lines = append(lines, fmt.Sprintf("gcm_pool_created_total %d", poolStats.CreatedConnections))

	lines = append(lines, "# HELP gcm_pool_closed_total 关闭连接总数")
	lines = append(lines, "# TYPE gcm_pool_closed_total counter")
	lines = append(lines, fmt.Sprintf("gcm_pool_closed_total %d", poolStats.ClosedConnections))

	// 全局速率统计（聚合所有连接）
	var totalSendAvg, totalSendMax, totalRecvAvg, totalRecvMax float64
	connData = s.pool.GetConnectionsData()
	if len(connData) > 0 {
		// 计算所有连接的平均速率的平均值，以及最大速率的最大值
		var sumSendAvg, sumRecvAvg float64
		for _, cd := range connData {
			// 从连接的 TrafficCounter 获取速率数据
			sumSendAvg += cd.RateSnapshot.AvgSent
			sumRecvAvg += cd.RateSnapshot.AvgRecv
			if cd.RateSnapshot.MaxSent > totalSendMax {
				totalSendMax = cd.RateSnapshot.MaxSent
			}
			if cd.RateSnapshot.MaxRecv > totalRecvMax {
				totalRecvMax = cd.RateSnapshot.MaxRecv
			}
		}
		totalSendAvg = sumSendAvg / float64(len(connData))
		totalRecvAvg = sumRecvAvg / float64(len(connData))
	}

	lines = append(lines, "# HELP gcm_rate_send_avg 平均发送速率(字节/秒)")
	lines = append(lines, "# TYPE gcm_rate_send_avg gauge")
	lines = append(lines, fmt.Sprintf("gcm_rate_send_avg %.0f", totalSendAvg))

	lines = append(lines, "# HELP gcm_rate_send_max 最大发送速率(字节/秒)")
	lines = append(lines, "# TYPE gcm_rate_send_max gauge")
	lines = append(lines, fmt.Sprintf("gcm_rate_send_max %.0f", totalSendMax))

	lines = append(lines, "# HELP gcm_rate_recv_avg 平均接收速率(字节/秒)")
	lines = append(lines, "# TYPE gcm_rate_recv_avg gauge")
	lines = append(lines, fmt.Sprintf("gcm_rate_recv_avg %.0f", totalRecvAvg))

	lines = append(lines, "# HELP gcm_rate_recv_max 最大接收速率(字节/秒)")
	lines = append(lines, "# TYPE gcm_rate_recv_max gauge")
	lines = append(lines, fmt.Sprintf("gcm_rate_recv_max %.0f", totalRecvMax))

	// 中转节点指标（带超时保护）
	s.log.Debug("[METRICS] 获取 relay stats...")
	type relayResult struct {
		stats relay.RelayStats
	}
	relayChan := make(chan relayResult, 1)
	done := make(chan struct{})

	go func() {
		stats := s.relayManager.GetStats()
		select {
		case relayChan <- relayResult{stats: stats}:
		case <-done:
			// 超时，丢弃结果
		}
	}()

	var relayStats relay.RelayStats
	select {
	case res := <-relayChan:
		relayStats = res.stats
		close(done)
	case <-time.After(500 * time.Millisecond):
		s.log.Warn("[METRICS] Relay stats 获取超时 (>500ms)")
		close(done)
		// 使用空值继续
	}

	lines = append(lines, "# HELP gcm_relay_nodes_total 中转节点总数")
	lines = append(lines, "# TYPE gcm_relay_nodes_total gauge")
	lines = append(lines, fmt.Sprintf("gcm_relay_nodes_total %d", relayStats.TotalNodes))

	lines = append(lines, "# HELP gcm_relay_latency_avg 中转节点平均延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_relay_latency_avg gauge")
	lines = append(lines, fmt.Sprintf("gcm_relay_latency_avg %d", relayStats.AvgLatency.Milliseconds()))

	lines = append(lines, "# HELP gcm_relay_latency_best 中转节点最低延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_relay_latency_best gauge")
	lines = append(lines, fmt.Sprintf("gcm_relay_latency_best %d", relayStats.BestLatency.Milliseconds()))

	lines = append(lines, "# HELP gcm_relay_latency_worst 中转节点最高延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_relay_latency_worst gauge")
	lines = append(lines, fmt.Sprintf("gcm_relay_latency_worst %d", relayStats.WorstLatency.Milliseconds()))

	lines = append(lines, "# HELP gcm_relay_total_tests 总测速次数")
	lines = append(lines, "# TYPE gcm_relay_total_tests counter")
	lines = append(lines, fmt.Sprintf("gcm_relay_total_tests %d", relayStats.TotalTests))

	lines = append(lines, "# HELP gcm_relay_removed_nodes 移除节点总数")
	lines = append(lines, "# TYPE gcm_relay_removed_nodes counter")
	lines = append(lines, fmt.Sprintf("gcm_relay_removed_nodes %d", relayStats.Removed))

	// 生成输出
	s.log.Debug("[METRICS] 生成输出...")
	result := ""
	for _, line := range lines {
		result += line + "\n"
	}
	s.log.Debug("[METRICS] 生成完成，返回 %d 字节", len(result))
	return result
}

// Close 关闭服务器
func (s *Server) Close() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

// formatConnID 将连接 ID 格式化为十六进制字符串
func formatConnID(connID []byte) string {
	if len(connID) < 3 {
		return "??????"
	}
	return fmt.Sprintf("%02x%02x%02x", connID[0], connID[1], connID[2])
}
