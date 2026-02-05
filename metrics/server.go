// Package metrics 提供 Prometheus 格式的监控指标暴露功能。
//
// 主要功能:
//   - HTTP 端点暴露 Prometheus 格式指标
//   - 连接池状态监控（空闲/活跃/排队）
//   - 请求统计（成功率、延迟、超时）
//   - DNS 缓存统计（命中率、大小）
//   - 中转节点状态（延迟、负载、质量）
//   - 流量统计（发送/接收速率）
//   - 窗口流控和拥塞控制指标
//   - 健康检查端点
//   - 监控面板（Web UI）
package metrics

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gcm/config"
	"gcm/dns"
	"gcm/logger"
	"gcm/pool"
	"gcm/relay"
)

// Server 表示 Prometheus Metrics 服务器。
//
// Server 提供 HTTP 端点暴露系统运行指标，支持 Prometheus 抓取。
// 包含超时保护机制，防止指标生成阻塞。
//
// 并发安全：所有公开方法都是并发安全的。
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

// NewServer 创建并初始化 Metrics 服务器。
//
// 参数:
//   - cfg: 配置对象
//   - p: WebSocket 连接池实例
//   - rm: 中转节点管理器实例
//   - dc: DNS 缓存实例
//
// 返回值: 初始化完成的 Server 实例（需要调用 Start 方法启动监听）。
func NewServer(cfg *config.Config, p *pool.ConnectionPool, rm *relay.RelayManager, dc *dns.DNSCache) *Server {
	return &Server{
		cfg:          cfg,
		log:          logger.GetLogger("Metrics"),
		pool:         p,
		relayManager: rm,
		dnsCache:     dc,
	}
}

// Start 启动 Metrics HTTP 服务器。
//
// 在配置的端口上开始监听 HTTP 请求，提供以下端点：
//   - /metrics - Prometheus 格式指标
//   - /health - 健康检查
//   - /monitor - 监控面板（Web UI）
//   - /static/ - 静态文件服务
//
// 如果配置中禁用了 Metrics，此方法将直接返回 nil。
//
// 返回值: 监听失败时返回错误。
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
		ReadTimeout:  10 * time.Second, // 读取请求超时
		WriteTimeout: 30 * time.Second, // 写入响应超时（metrics可能较大）
		IdleTimeout:  60 * time.Second, // 空闲连接超时
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

	// 获取连接数据用于计算负载率
	s.log.Debug("[METRICS] 获取 active connections...")
	connData := s.pool.GetConnectionsData()
	s.log.Debug("[METRICS] 获取到 %d 个连接", len(connData))

	// 计算负载率（多路复用模式下）
	var loadRate float64
	if s.cfg.EnableMultiplex && len(connData) > 0 {
		totalStreams := 0
		for _, cd := range connData {
			totalStreams += cd.StreamCount
		}
		totalCapacity := len(connData) * int(s.cfg.MaxStreamsPerConnection)
		if totalCapacity > 0 {
			loadRate = float64(totalStreams) / float64(totalCapacity) * 100
		}
	} else if poolStats.ActiveConnections > 0 {
		// 非多路复用模式：活跃连接数/总连接数
		total := poolStats.PoolSize + poolStats.ActiveConnections
		if total > 0 {
			loadRate = float64(poolStats.ActiveConnections) / float64(total) * 100
		}
	}

	lines = append(lines, "# HELP gcm_pool_load_rate 连接池负载率(%)")
	lines = append(lines, "# TYPE gcm_pool_load_rate gauge")
	lines = append(lines, fmt.Sprintf("gcm_pool_load_rate %.2f", loadRate))

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

	lines = append(lines, "# HELP gcm_request_duration_min 最低请求延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_request_duration_min gauge")
	if poolStats.MinResponseTime >= 0 {
		lines = append(lines, fmt.Sprintf("gcm_request_duration_min %.0f", poolStats.MinResponseTime))
	} else {
		lines = append(lines, "gcm_request_duration_min 0")
	}

	lines = append(lines, "# HELP gcm_request_duration_max 最高请求延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_request_duration_max gauge")
	lines = append(lines, fmt.Sprintf("gcm_request_duration_max %.0f", poolStats.MaxResponseTime))

	lines = append(lines, "# HELP gcm_request_duration_avg 平均请求延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_request_duration_avg gauge")
	lines = append(lines, fmt.Sprintf("gcm_request_duration_avg %.2f", poolStats.AvgResponseTime))

	// Per-connection 流量指标说明
	lines = append(lines, "# HELP gcm_conn_bytes_sent 单个 WebSocket 连接的发送字节数")
	lines = append(lines, "# TYPE gcm_conn_bytes_sent gauge")
	lines = append(lines, "# HELP gcm_conn_bytes_received 单个 WebSocket 连接的接收字节数")
	lines = append(lines, "# TYPE gcm_conn_bytes_received gauge")
	lines = append(lines, "# HELP gcm_conn_stream_count 单个 WebSocket 连接的 Stream 数量")
	lines = append(lines, "# TYPE gcm_conn_stream_count gauge")
	lines = append(lines, "# HELP gcm_conn_rtt 单个 WebSocket 连接的 RTT 延迟(毫秒)")
	lines = append(lines, "# TYPE gcm_conn_rtt gauge")

	// Per-connection 流量数据（connData 已在前面获取）
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

	// DNS 总请求数
	totalDnsRequests := dnsStats.Hits + dnsStats.Misses
	lines = append(lines, "# HELP gcm_dns_requests_total DNS总请求数")
	lines = append(lines, "# TYPE gcm_dns_requests_total counter")
	lines = append(lines, fmt.Sprintf("gcm_dns_requests_total %d", totalDnsRequests))

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

	// 全局速率统计（复用之前获取的 connData）
	var totalSendAvg, totalSendMax, totalRecvAvg, totalRecvMax float64
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

	lines = append(lines, "# HELP gcm_relay_nodes_optimal 有效中转节点数")
	lines = append(lines, "# TYPE gcm_relay_nodes_optimal gauge")
	lines = append(lines, fmt.Sprintf("gcm_relay_nodes_optimal %d", relayStats.OptimalNodes))

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

	// 负载均衡指标 - 节点级别详细信息（带超时保护）
	s.log.Debug("[METRICS] 获取节点详细信息...")
	type detailedResult struct {
		nodes []relay.DetailedNodeInfo
	}
	detailedChan := make(chan detailedResult, 1)
	detailedDone := make(chan struct{})

	go func() {
		detailedChan <- detailedResult{nodes: s.relayManager.GetDetailedNodes()}
	}()

	detailedNodes := []relay.DetailedNodeInfo{}
	select {
	case res := <-detailedChan:
		detailedNodes = res.nodes
		close(detailedDone)
	case <-time.After(500 * time.Millisecond):
		s.log.Warn("[METRICS] 节点详细信息获取超时 (>500ms)，跳过详细指标")
		close(detailedDone)
		// 使用空切片继续
	}

	if len(detailedNodes) > 0 {
		lines = append(lines, "# HELP gcm_relay_active_connections 节点当前活跃连接数")
		lines = append(lines, "# TYPE gcm_relay_active_connections gauge")
		lines = append(lines, "# HELP gcm_relay_total_connections 节点累计创建连接数")
		lines = append(lines, "# TYPE gcm_relay_total_connections counter")
		lines = append(lines, "# HELP gcm_relay_quality_score 节点平均质量评分(0-100)")
		lines = append(lines, "# TYPE gcm_relay_quality_score gauge")
		lines = append(lines, "# HELP gcm_relay_weight 节点动态权重")
		lines = append(lines, "# TYPE gcm_relay_weight gauge")

		for _, node := range detailedNodes {
			relayLabel := fmt.Sprintf("%s:%d", node.IP, node.Port)
			lines = append(lines, fmt.Sprintf(`gcm_relay_active_connections{relay="%s"} %d`, relayLabel, node.ActiveConnections))
			lines = append(lines, fmt.Sprintf(`gcm_relay_total_connections{relay="%s"} %d`, relayLabel, node.TotalConnections))
			lines = append(lines, fmt.Sprintf(`gcm_relay_quality_score{relay="%s"} %.2f`, relayLabel, node.AvgQualityScore))
			lines = append(lines, fmt.Sprintf(`gcm_relay_weight{relay="%s"} %.2f`, relayLabel, node.Weight))
		}
	}

	// 运行时间
	lines = append(lines, "# HELP gcm_uptime_seconds 运行时间(秒)")
	lines = append(lines, "# TYPE gcm_uptime_seconds gauge")
	lines = append(lines, fmt.Sprintf("gcm_uptime_seconds %.2f", poolStats.Uptime.Seconds()))

	// 窗口流控和拥塞控制指标
	s.log.Debug("[METRICS] 获取流控统计...")
	avgWindow, minWindow, maxWindow, avgRTT, avgLossRate, streamCount := s.pool.GetFlowControlStats()

	lines = append(lines, "# HELP gcm_flow_window_avg 平均窗口大小(字节)")
	lines = append(lines, "# TYPE gcm_flow_window_avg gauge")
	lines = append(lines, fmt.Sprintf("gcm_flow_window_avg %d", avgWindow))

	lines = append(lines, "# HELP gcm_flow_window_min 最小窗口大小(字节)")
	lines = append(lines, "# TYPE gcm_flow_window_min gauge")
	lines = append(lines, fmt.Sprintf("gcm_flow_window_min %d", minWindow))

	lines = append(lines, "# HELP gcm_flow_window_max 最大窗口大小(字节)")
	lines = append(lines, "# TYPE gcm_flow_window_max gauge")
	lines = append(lines, fmt.Sprintf("gcm_flow_window_max %d", maxWindow))

	lines = append(lines, "# HELP gcm_flow_rtt_avg 平均RTT(毫秒)")
	lines = append(lines, "# TYPE gcm_flow_rtt_avg gauge")
	lines = append(lines, fmt.Sprintf("gcm_flow_rtt_avg %d", avgRTT.Milliseconds()))

	lines = append(lines, "# HELP gcm_flow_loss_rate 平均丢包率")
	lines = append(lines, "# TYPE gcm_flow_loss_rate gauge")
	lines = append(lines, fmt.Sprintf("gcm_flow_loss_rate %.4f", avgLossRate))

	lines = append(lines, "# HELP gcm_flow_stream_count 活跃Stream数量")
	lines = append(lines, "# TYPE gcm_flow_stream_count gauge")
	lines = append(lines, fmt.Sprintf("gcm_flow_stream_count %d", streamCount))

	// 生成输出
	s.log.Debug("[METRICS] 生成输出...")
	result := ""
	for _, line := range lines {
		result += line + "\n"
	}
	s.log.Debug("[METRICS] 生成完成，返回 %d 字节", len(result))
	return result
}

// Close 关闭 Metrics HTTP 服务器。
//
// 停止监听新请求，现有请求会自然完成。
//
// 返回值: 关闭失败时返回错误。
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
