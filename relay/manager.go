package relay

import (
	"math/rand/v2"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gcm/config"
	"gcm/dns"
	"gcm/logger"
)

// RelayNode 中转节点
type RelayNode struct {
	IP        string
	Port      int
	Source    string
	Latency   time.Duration
	FailCount int
	LastCheck time.Time
	Score     int // 分数 = 延迟(ms) + 失败惩罚

	// 负载均衡新增字段
	ActiveConnections int32   // 当前活跃连接数（原子操作）
	TotalConnections  int64   // 累计创建连接数（原子操作）
	AvgQualityScore   float64 // 平均连接质量评分（0-100）
	Weight            float64 // 动态权重（用于加权轮询）
}

// ParseHostPort 解析 "host:port" 或 "[ipv6]:port" 或 "host"
func ParseHostPort(input string) (host string, port int) {
	port = 443 // 默认端口

	// 查找最后一个冒号
	lastColon := strings.LastIndex(input, ":")
	closeBracket := strings.LastIndex(input, "]")

	// 如果有冒号，且冒号在方括号后面（针对 [ipv6]:port），或者是 ipv4/domain
	if lastColon > -1 && lastColon > closeBracket {
		portPart := input[lastColon+1:]
		if p, err := strconv.Atoi(portPart); err == nil {
			port = p
			host = input[:lastColon]
		} else {
			host = input
		}
	} else {
		host = input
	}

	// 去除 IPv6 包裹
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	return host, port
}

// RelayManager 中转节点管理器
type RelayManager struct {
	mu                   sync.RWMutex
	rawRelays            []string
	optimalRelays        []*RelayNode
	allNodes             []*RelayNode // 所有已配置的节点（包括高延迟的）
	isInitialized        bool
	totalTestCount       int
	totalRemovedCount    int
	lastForceRescoreTime time.Time
	cfg                  *config.Config
	log                  *logger.Logger
	dnsCache             *dns.DNSCache
	stopChan             chan struct{}
}

// NewRelayManager 创建中转节点管理器
func NewRelayManager(relayList []string, cfg *config.Config, dnsCache *dns.DNSCache) *RelayManager {
	return &RelayManager{
		rawRelays: relayList,
		cfg:       cfg,
		log:       logger.GetLogger("Relay"),
		dnsCache:  dnsCache,
		stopChan:  make(chan struct{}),
	}
}

// Init 初始化中转节点
func (rm *RelayManager) Init() error {
	startTime := time.Now()

	if len(rm.rawRelays) == 0 {
		rm.log.Warn("未配置中转节点，将使用直连模式")
		rm.isInitialized = true
		return nil
	}

	rm.log.Info("开始初始化中转节点，配置数: %d...", len(rm.rawRelays))

	// 解析所有输入：域名解析得到的全部 IP 都直接加入候选列表，
	// 不在域名分支内做测速截断，统一在最后做一次 batchTestLatency。
	candidateNodes := rm.resolveCandidates(rm.rawRelays)

	// 初始测速并初始化节点状态
	rm.log.Debug("开始批量测速 %d 个候选节点...", len(candidateNodes))
	results := rm.batchTestLatency(candidateNodes)
	rm.totalTestCount += len(results)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 保存所有已测速的节点
	rm.allNodes = make([]*RelayNode, 0, len(results))
	for _, r := range results {
		r.LastCheck = time.Now()
		r.Score = rm.calculateScore(r.Latency, r.FailCount)
		rm.allNodes = append(rm.allNodes, r)
	}

	// 仅保留低于延迟阈值的节点作为最优节点
	rm.optimalRelays = make([]*RelayNode, 0)
	for _, r := range rm.allNodes {
		if r.Latency < rm.cfg.GetRelayMaxLatency() {
			r.FailCount = 0
			rm.optimalRelays = append(rm.optimalRelays, r)
		}
	}

	filteredCount := len(rm.allNodes) - len(rm.optimalRelays)
	elapsed := time.Since(startTime)

	if len(rm.optimalRelays) > 0 {
		rm.log.Info("初始化完成: 有效节点%d个 (过滤%d个高延迟节点), 耗时%dms",
			len(rm.optimalRelays), filteredCount, elapsed.Milliseconds())
		rm.log.Info("优选节点列表 (Top %d):", min(5, len(rm.optimalRelays)))
		for i, r := range rm.optimalRelays[:min(5, len(rm.optimalRelays))] {
			rm.log.Info("  [%d] %s:%d (%dms, 分数:%d) [来自: %s]",
				i+1, r.IP, r.Port, r.Latency.Milliseconds(), r.Score, r.Source)
		}
	} else {
		rm.log.Warn("未找到可用中转节点 (测速%d个，全部超过%dms阈值)，降级为直连模式",
			len(results), rm.cfg.GetRelayMaxLatency().Milliseconds())
	}

	rm.isInitialized = true

	// 启动后台定期重评
	go rm.rescoreLoop()

	return nil
}

// resolveCandidates 从原始配置列表重新解析域名并构建候选节点列表。
// IP 类节点直接加入；域名类节点解析出全部 IP 后逐个加入（不在域名层做测速截断）。
// 多个域名的解析并发执行以减少整体解析耗时。
func (rm *RelayManager) resolveCandidates(rawRelays []string) []*RelayNode {
	if len(rawRelays) == 0 {
		return nil
	}

	// 预先将 IP 类节点直接放入，域名类收集到 domainEntries 并发解析。
	candidateNodes := make([]*RelayNode, 0, len(rawRelays))
	var domainEntries []string

	for _, raw := range rawRelays {
		host, port := ParseHostPort(raw)
		if ip := net.ParseIP(host); ip != nil {
			rm.log.Debug("直接添加 IP 节点: %s:%d", host, port)
			candidateNodes = append(candidateNodes, &RelayNode{
				IP:     host,
				Port:   port,
				Source: raw,
			})
		} else {
			domainEntries = append(domainEntries, raw)
		}
	}

	// 无域名类节点，直接返回。
	if len(domainEntries) == 0 {
		return candidateNodes
	}

	// 并发解析域名（问题 11）。
	type resolveResult struct {
		host string
		port int
		ips  []string
		err  error
	}
	resultChan := make(chan resolveResult, len(domainEntries))
	var wg sync.WaitGroup
	for _, raw := range domainEntries {
		host, port := ParseHostPort(raw)
		wg.Add(1)
		go func(h string, p int) {
			defer wg.Done()
			if rm.dnsCache == nil {
				// 无 DNS 缓存（如测试环境）：尝试系统 DNS 解析。
				ips, err := net.LookupHost(h)
				resultChan <- resolveResult{host: h, port: p, ips: ips, err: err}
				return
			}
			ips, err := rm.dnsCache.LookupIPs(h)
			resultChan <- resolveResult{host: h, port: p, ips: ips, err: err}
		}(host, port)
	}
	wg.Wait()
	close(resultChan)

	for rr := range resultChan {
		if rr.err != nil {
			rm.log.Error("解析中转域名 %s 失败: %v", rr.host, rr.err)
			continue
		}
		if len(rr.ips) == 0 {
			rm.log.Warn("域名 %s 解析结果为空", rr.host)
			continue
		}
		rm.log.Debug("域名 %s 解析到 %d 个 IP 地址", rr.host, len(rr.ips))
		for _, ip := range rr.ips {
			candidateNodes = append(candidateNodes, &RelayNode{
				IP:     ip,
				Port:   rr.port,
				Source: rr.host,
			})
		}
	}

	return candidateNodes
}

// calculateScore 计算节点分数
func (rm *RelayManager) calculateScore(latency time.Duration, failCount int) int {
	return int(latency.Milliseconds()) + (failCount * 500)
}

// rescoreLoop 定期重新评分
func (rm *RelayManager) rescoreLoop() {
	ticker := time.NewTicker(rm.cfg.GetRelayRescoreInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.doRescore("定期重评")
		case <-rm.stopChan:
			return
		}
	}
}

// doRescore 统一重评入口：从 rawRelays 重新解析域名并测速，更新 allNodes/optimalRelays。
// 定期重评和强制重评共用此函数，确保 DNS 变化被感知且被剔除节点可恢复。
// trigger 仅用于日志。
func (rm *RelayManager) doRescore(trigger string) {
	startTime := time.Now()

	// 从原始配置重新解析域名（改造 A：定期与强制重评统一）。
	candidateNodes := rm.resolveCandidates(rm.rawRelays)

	if len(candidateNodes) == 0 {
		rm.log.Debug("重评(%s): 无候选节点，跳过", trigger)
		// 仍要清空现有节点列表以反映配置变化。
		rm.mu.Lock()
		rm.allNodes = nil
		rm.optimalRelays = nil
		rm.mu.Unlock()
		return
	}

	rm.log.Info("重评(%s): 开始测速 %d 个候选节点...", trigger, len(candidateNodes))

	// 在锁外测速（candidateNodes 是新解析的临时节点指针，测速只更新其 Latency）。
	tested := rm.batchTestLatency(candidateNodes)

	rm.mu.Lock()
	rm.totalTestCount += len(tested)

	beforeCount := len(rm.optimalRelays)

	// 构建 IP+Port -> 旧节点 映射（allNodes + optimalRelays 合集去重），
	// 用于在新一轮解析结果中识别已存在的节点并复用其原指针，
	// 以保留 ActiveConnections/AvgQualityScore/TotalConnections/FailCount 等稳定元信息。
	oldByAddr := make(map[string]*RelayNode, len(rm.allNodes)+len(rm.optimalRelays))
	for _, n := range rm.allNodes {
		oldByAddr[net.JoinHostPort(n.IP, strconv.Itoa(n.Port))] = n
	}
	for _, n := range rm.optimalRelays {
		key := net.JoinHostPort(n.IP, strconv.Itoa(n.Port))
		if _, ok := oldByAddr[key]; !ok {
			oldByAddr[key] = n
		}
	}

	// 更新所有节点的状态：优先复用旧指针并原地更新 Latency/LastCheck/Score，
	// 避免进行中的连接池 UpdateNodeLoad(-1) 在重评后错加到新对象上（问题 3）。
	rm.allNodes = make([]*RelayNode, 0, len(tested))
	for _, r := range tested {
		key := net.JoinHostPort(r.IP, strconv.Itoa(r.Port))
		if old, ok := oldByAddr[key]; ok {
			// 复用旧指针，仅更新测速数据。
			old.Latency = r.Latency
			old.LastCheck = time.Now()
			old.Score = rm.calculateScore(old.Latency, old.FailCount)
			rm.allNodes = append(rm.allNodes, old)
		} else {
			// 全新节点（如 DNS 新增 IP）。
			r.LastCheck = time.Now()
			r.Score = rm.calculateScore(r.Latency, r.FailCount)
			rm.allNodes = append(rm.allNodes, r)
		}
	}

	// 重建 optimalRelays ：按延迟从 allNodes 过滤，并按延迟排序。
	rm.rebuildOptimalRelays()
	validCount := len(rm.optimalRelays)
	rm.mu.Unlock()

	elapsed := time.Since(startTime)
	diff := validCount - beforeCount
	if diff >= 0 {
		rm.log.Info("重评(%s)完成: 有效%d个 (新增%d个), 耗时%dms",
			trigger, validCount, diff, elapsed.Milliseconds())
	} else {
		rm.log.Info("重评(%s)完成: 有效%d个 (移除%d个), 耗时%dms",
			trigger, validCount, -diff, elapsed.Milliseconds())
	}
	// logTopRelays acquires a read lock. Call it only after releasing the
	// rescore write lock so periodic rescoring cannot deadlock relay selection.
	rm.logTopRelays()
}

// rebuildOptimalRelays 根据当前 allNodes 重建 optimalRelays。
// 调用者必须持有写锁。
// doRescore 已在外层完成"旧指针复用"逻辑，此处仅做延迟过滤与排序。
func (rm *RelayManager) rebuildOptimalRelays() {
	rm.optimalRelays = make([]*RelayNode, 0)
	for _, r := range rm.allNodes {
		if r.Latency < rm.cfg.GetRelayMaxLatency() {
			r.FailCount = 0
			rm.optimalRelays = append(rm.optimalRelays, r)
		}
	}
	// 按延迟排序（问题 7：统一按 Latency 排序）。
	sort.Slice(rm.optimalRelays, func(i, j int) bool {
		return rm.optimalRelays[i].Latency < rm.optimalRelays[j].Latency
	})
}

// resortByLatencyLocked 按延迟重新排序（调用者必须持有写锁）
func (rm *RelayManager) resortByLatencyLocked() {
	sort.Slice(rm.optimalRelays, func(i, j int) bool {
		return rm.optimalRelays[i].Latency < rm.optimalRelays[j].Latency
	})
}

// ReportFailure 记录失败
func (rm *RelayManager) ReportFailure(ip string, port int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	idx := -1
	for i, r := range rm.optimalRelays {
		if r.IP == ip && r.Port == port {
			idx = i
			break
		}
	}

	if idx != -1 {
		rm.optimalRelays[idx].FailCount++
		prevScore := rm.optimalRelays[idx].Score
		rm.optimalRelays[idx].Score = rm.calculateScore(
			rm.optimalRelays[idx].Latency,
			rm.optimalRelays[idx].FailCount,
		)
		rm.log.Debug("节点 %s:%d 失败报告: %d/%d, 分数: %d -> %d",
			ip, port, rm.optimalRelays[idx].FailCount, rm.cfg.RelayFailureThreshold,
			prevScore, rm.optimalRelays[idx].Score)

		if rm.optimalRelays[idx].FailCount >= rm.cfg.RelayFailureThreshold {
			rm.totalRemovedCount++
			rm.log.Warn("节点 %s:%d 连续失败 %d 次，已移除 (累计移除: %d)",
				ip, port, rm.cfg.RelayFailureThreshold, rm.totalRemovedCount)
			// 从 optimalRelays 和 allNodes 同步移除（问题 5）。
			rm.optimalRelays = append(rm.optimalRelays[:idx], rm.optimalRelays[idx+1:]...)
			rm.removeFromAllNodesLocked(ip, port)
		} else {
			rm.resortByLatencyLocked()
		}
	}
}

// removeFromAllNodesLocked 从 allNodes 中移除指定节点（调用者必须持有写锁）
func (rm *RelayManager) removeFromAllNodesLocked(ip string, port int) {
	for i, n := range rm.allNodes {
		if n.IP == ip && n.Port == port {
			rm.allNodes = append(rm.allNodes[:i], rm.allNodes[i+1:]...)
			return
		}
	}
}

// getNextRelayLocked 获取最优节点（调用者必须持有锁）
func (rm *RelayManager) getNextRelayLocked() *RelayNode {
	if !rm.isInitialized || len(rm.optimalRelays) == 0 {
		return nil
	}
	return rm.optimalRelays[0]
}

// GetNextRelay 获取最优节点（公开版本，自动加锁）
func (rm *RelayManager) GetNextRelay() *RelayNode {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.getNextRelayLocked()
}

// GetCurrentBest 获取当前最优节点
func (rm *RelayManager) GetCurrentBest() *RelayNode {
	return rm.GetNextRelay()
}

// GetBestRelayExcluding 获取最优节点（排除指定地址）
func (rm *RelayManager) GetBestRelayExcluding(excludeAddr string) *RelayNode {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if !rm.isInitialized || len(rm.optimalRelays) == 0 {
		return nil
	}

	// 遍历优选节点列表，找到第一个不是 excludeAddr 的节点
	for _, node := range rm.optimalRelays {
		addr := net.JoinHostPort(node.IP, strconv.Itoa(node.Port))
		if addr != excludeAddr {
			return node
		}
	}

	// 如果所有节点都被排除，返回 nil
	return nil
}

// testLatency 单个节点测速
func (rm *RelayManager) testLatency(node *RelayNode) *RelayNode {
	start := time.Now()
	address := net.JoinHostPort(node.IP, strconv.Itoa(node.Port))

	// 问题 8：使用配置的连接超时，与真实拨号一致；nil cfg 兜底 2 秒。
	timeout := 2 * time.Second
	if rm.cfg != nil {
		timeout = rm.cfg.GetConnectionTimeout()
	}
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		node.Latency = 9999 * time.Millisecond
		return node
	}
	defer conn.Close()

	node.Latency = time.Since(start)
	return node
}

// batchTestLatency 批量测速（问题 3：原地更新传入节点的 Latency，不创建新指针对象）。
// 每个 goroutine 操作不同的节点指针因此无竞争；返回的切片包含同一组指针。
func (rm *RelayManager) batchTestLatency(nodes []*RelayNode) []*RelayNode {
	if len(nodes) == 0 {
		return nodes
	}

	// 并发测速：原地更新节点的 Latency 字段。
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *RelayNode) {
			defer wg.Done()
			rm.testLatency(n) // 直接更新 n.Latency
		}(node)
	}
	wg.Wait()

	// 复制结果切片并按延迟排序（问题 10：sort.Slice 替代冒泡）。
	resultNodes := make([]*RelayNode, len(nodes))
	copy(resultNodes, nodes)
	sort.Slice(resultNodes, func(i, j int) bool {
		return resultNodes[i].Latency < resultNodes[j].Latency
	})

	return resultNodes
}

// ForceRescore 强制重新评分
func (rm *RelayManager) ForceRescore() bool {
	// 防抖检查
	now := time.Now()
	rm.mu.Lock()
	if !rm.lastForceRescoreTime.IsZero() && now.Sub(rm.lastForceRescoreTime) < rm.cfg.GetRelayForceRescoreCooldown() {
		rm.mu.Unlock()
		rm.log.Debug("强制重评冷却中，跳过本次重评")
		return false
	}
	rm.lastForceRescoreTime = now

	var beforeBest *RelayNode
	if rm.isInitialized && len(rm.optimalRelays) > 0 {
		beforeBest = rm.optimalRelays[0]
	}
	rm.mu.Unlock()

	if beforeBest != nil {
		rm.log.Warn("触发强制重新评分 (原最优: %s:%d %dms)...",
			beforeBest.IP, beforeBest.Port, beforeBest.Latency.Milliseconds())
	} else {
		rm.log.Warn("触发强制重新评分 (无可用节点)...")
	}

	rm.doRescore("强制重评")

	rm.mu.RLock()
	afterBest := rm.getNextRelayLocked()
	validCount := len(rm.optimalRelays)
	rm.mu.RUnlock()

	rm.log.Info("强制重评完成: 有效节点%d个", validCount)
	if beforeBest != nil && afterBest != nil {
		rm.log.Info("最优节点: %s:%d(%dms) -> %s:%d(%dms)",
			beforeBest.IP, beforeBest.Port, beforeBest.Latency.Milliseconds(),
			afterBest.IP, afterBest.Port, afterBest.Latency.Milliseconds())
	}

	return true
}

// logTopRelays 输出当前 Top 节点
func (rm *RelayManager) logTopRelays() {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if len(rm.optimalRelays) == 0 {
		rm.log.Warn("无可用中转节点")
		return
	}

	rm.log.Info("当前最优节点 (Top %d):", min(5, len(rm.optimalRelays)))
	for i, r := range rm.optimalRelays[:min(5, len(rm.optimalRelays))] {
		rm.log.Info("  [%d] %s:%d (延迟:%dms, 失败:%d, 分数:%d) [来自: %s]",
			i+1, r.IP, r.Port, r.Latency.Milliseconds(), r.FailCount, r.Score, r.Source)
	}
}

// GetStats 获取统计信息
func (rm *RelayManager) GetStats() RelayStats {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stats := RelayStats{
		TotalNodes: len(rm.allNodes),
		TotalTests: rm.totalTestCount,
		Removed:    rm.totalRemovedCount,
	}

	// 优先从最优节点获取延迟统计，如果没有则使用所有节点
	nodes := rm.optimalRelays
	if len(nodes) == 0 && len(rm.allNodes) > 0 {
		nodes = rm.allNodes
	}

	if len(nodes) > 0 {
		total := time.Duration(0)
		for _, r := range nodes {
			total += r.Latency
		}
		stats.AvgLatency = total / time.Duration(len(nodes))
		stats.BestLatency = nodes[0].Latency
		stats.WorstLatency = nodes[len(nodes)-1].Latency
	}

	return stats
}

// Close 关闭管理器
func (rm *RelayManager) Close() {
	close(rm.stopChan)
}

// UpdateNodeLoad 更新节点负载信息（原子操作）
func (rm *RelayManager) UpdateNodeLoad(ip string, port int, delta int32) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// 在 optimalRelays 中查找节点
	for _, node := range rm.optimalRelays {
		if node.IP == ip && node.Port == port {
			newLoad := atomic.AddInt32(&node.ActiveConnections, delta)
			if delta > 0 {
				atomic.AddInt64(&node.TotalConnections, 1)
			}
			rm.log.Debug("节点 %s:%d 负载更新: %d (delta=%d)", ip, port, newLoad, delta)
			return
		}
	}
	// 节点未找到时记录警告（可能已被移除）
	rm.log.Warn("节点 %s:%d 不在优选列表中，负载更新失败 (delta=%d)", ip, port, delta)
}

// UpdateNodeQuality 更新节点质量评分（使用 EMA 平滑）
func (rm *RelayManager) UpdateNodeQuality(ip string, port int, score float64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 在 optimalRelays 中查找节点
	for _, node := range rm.optimalRelays {
		if node.IP == ip && node.Port == port {
			// 使用 EMA (指数移动平均) 平滑质量评分
			// 新评分 = 0.7 * 旧评分 + 0.3 * 新评分
			if node.AvgQualityScore == 0 {
				node.AvgQualityScore = score
			} else {
				node.AvgQualityScore = 0.7*node.AvgQualityScore + 0.3*score
			}
			rm.log.Debug("节点 %s:%d 质量评分更新: %.2f", ip, port, node.AvgQualityScore)
			return
		}
	}
}

// calculateWeight 计算节点权重
// 权重 = 基础权重 × 负载因子 × 质量因子
func (rm *RelayManager) calculateWeight(node *RelayNode) float64 {
	// 基础权重 = 1000 / (延迟ms + 1)
	baseWeight := 1000.0 / (float64(node.Latency.Milliseconds()) + 1.0)

	// 负载因子 = 1.0 - (当前连接数 / 最大连接数) × 0.5
	// 假设最大连接数为配置的 MaxPoolSize
	activeConns := float64(atomic.LoadInt32(&node.ActiveConnections))
	maxConns := float64(rm.cfg.MaxPoolSize)
	if maxConns == 0 {
		maxConns = 1 // 防御性编程：避免除零
	}
	loadFactor := 1.0 - (activeConns/maxConns)*0.5
	if loadFactor < 0.1 {
		loadFactor = 0.1 // 最低保留 10% 权重
	}

	// 质量因子 = 平均质量评分 / 100
	qualityFactor := node.AvgQualityScore / 100.0
	if qualityFactor == 0 {
		qualityFactor = 1.0 // 默认满分
	}

	weight := baseWeight * loadFactor * qualityFactor
	return weight
}

// weightedNode 带权重的节点（用于加权选择，避免修改原始节点）
type weightedNode struct {
	node   *RelayNode
	weight float64
}

// selectByWeight 按权重随机选择节点（加权轮询）
// 问题 1：使用 math/rand/v2 全局并发安全 API 替代非线程安全的 *rand.Rand。
func (rm *RelayManager) selectByWeight(candidates []*RelayNode) *RelayNode {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	// 计算所有候选节点的权重（使用局部变量，不修改原始节点）
	weights := make([]weightedNode, len(candidates))
	totalWeight := 0.0
	for i, node := range candidates {
		weight := rm.calculateWeight(node)
		weights[i] = weightedNode{node: node, weight: weight}
		totalWeight += weight
	}

	if totalWeight == 0 {
		// 所有权重为0，随机选择
		return candidates[rand.IntN(len(candidates))]
	}

	// 加权随机选择
	r := rand.Float64() * totalWeight
	cumulative := 0.0
	for _, wn := range weights {
		cumulative += wn.weight
		if r <= cumulative {
			return wn.node
		}
	}

	// 兜底：返回最后一个节点
	return candidates[len(candidates)-1]
}

// GetNextRelayWithLoadBalance 负载均衡选择节点
func (rm *RelayManager) GetNextRelayWithLoadBalance() *RelayNode {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if !rm.isInitialized || len(rm.optimalRelays) == 0 {
		return nil
	}

	// 选择 Top 5 个节点作为候选池
	candidateSize := min(5, len(rm.optimalRelays))
	candidates := rm.optimalRelays[:candidateSize]

	// 使用加权轮询选择节点
	selected := rm.selectByWeight(candidates)
	if selected != nil {
		// 实时计算权重用于日志（避免读取可能过期的 Weight 字段）
		weight := rm.calculateWeight(selected)
		rm.log.Debug("负载均衡选择节点: %s:%d (权重=%.2f, 负载=%d)",
			selected.IP, selected.Port, weight, atomic.LoadInt32(&selected.ActiveConnections))
	}

	return selected
}

// RelayStats 中转节点统计
type RelayStats struct {
	TotalNodes   int
	TotalTests   int
	Removed      int
	AvgLatency   time.Duration
	BestLatency  time.Duration
	WorstLatency time.Duration
}

// min 返回最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
