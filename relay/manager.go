package relay

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
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

	// 解析所有输入
	candidateNodes := make([]*RelayNode, 0)
	for _, raw := range rm.rawRelays {
		host, port := ParseHostPort(raw)

		if ip := net.ParseIP(host); ip != nil {
			rm.log.Debug("直接添加 IP 节点: %s:%d", host, port)
			candidateNodes = append(candidateNodes, &RelayNode{
				IP:     host,
				Port:   port,
				Source: raw,
			})
		} else {
			// 域名解析
			rm.log.Debug("正在解析域名: %s ...", host)
			ips, err := dns.LookupIP(host)
			if err != nil {
				rm.log.Error("解析中转域名 %s 失败: %v", host, err)
				continue
			}

			if len(ips) > 0 {
				rm.log.Debug("域名 %s 解析到 %d 个 IP 地址", host, len(ips))
				// 测速并选择最优的
				domainCandidates := make([]*RelayNode, 0)
				for _, ip := range ips {
					domainCandidates = append(domainCandidates, &RelayNode{
						IP:     ip,
						Port:   port,
						Source: host,
					})
				}

				testedNodes := rm.batchTestLatency(domainCandidates)
				// 选择 Top 2
				bestOfDomain := min(len(testedNodes), 2)
				candidateNodes = append(candidateNodes, testedNodes[:bestOfDomain]...)
				rm.log.Debug("域名 %s 优选了 %d 个节点", host, bestOfDomain)
			} else {
				rm.log.Warn("域名 %s 解析结果为空", host)
			}
		}
	}

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
			rm.rescoreAll()
		case <-rm.stopChan:
			return
		}
	}
}

// rescoreAll 全面重新评分
func (rm *RelayManager) rescoreAll() {
	rm.mu.RLock()
	if len(rm.allNodes) == 0 {
		rm.mu.RUnlock()
		rm.log.Debug("无可用节点，跳过重新评分")
		return
	}
	relays := make([]*RelayNode, len(rm.allNodes))
	copy(relays, rm.allNodes)
	rm.mu.RUnlock()

	startTime := time.Now()
	rm.log.Info("开始全面重新评分 %d 个节点...", len(relays))

	results := rm.batchTestLatency(relays)
	rm.totalTestCount += len(results)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 更新所有节点的状态
	rm.allNodes = make([]*RelayNode, 0, len(results))
	for _, r := range results {
		r.LastCheck = time.Now()
		r.Score = rm.calculateScore(r.Latency, r.FailCount)
		rm.allNodes = append(rm.allNodes, r)
	}

	// 仅保留低于延迟阈值的节点作为最优节点
	beforeCount := len(rm.optimalRelays)
	rm.optimalRelays = make([]*RelayNode, 0)
	for _, r := range rm.allNodes {
		if r.Latency < rm.cfg.GetRelayMaxLatency() {
			r.FailCount = 0
			rm.optimalRelays = append(rm.optimalRelays, r)
		}
	}

	elapsed := time.Since(startTime)
	removed := beforeCount - len(rm.optimalRelays)
	rm.log.Info("重新评分完成: 有效%d个 (移除%d个), 耗时%dms",
		len(rm.optimalRelays), removed, elapsed.Milliseconds())
	rm.logTopRelays()
}

// resortByScore 按分数重新排序
func (rm *RelayManager) resortByScore() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 按分数排序
	for i := 0; i < len(rm.optimalRelays)-1; i++ {
		for j := i + 1; j < len(rm.optimalRelays); j++ {
			if rm.optimalRelays[i].Score > rm.optimalRelays[j].Score {
				rm.optimalRelays[i], rm.optimalRelays[j] = rm.optimalRelays[j], rm.optimalRelays[i]
			}
		}
	}
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
			// 移除节点
			rm.optimalRelays = append(rm.optimalRelays[:idx], rm.optimalRelays[idx+1:]...)
		} else {
			rm.resortByScore()
		}
	}
}

// GetNextRelay 获取最优节点
func (rm *RelayManager) GetNextRelay() *RelayNode {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if !rm.isInitialized || len(rm.optimalRelays) == 0 {
		return nil
	}
	return rm.optimalRelays[0]
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
		addr := fmt.Sprintf("%s:%d", node.IP, node.Port)
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
	address := fmt.Sprintf("%s:%d", node.IP, node.Port)

	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		node.Latency = 9999 * time.Millisecond
		return node
	}
	defer conn.Close()

	node.Latency = time.Since(start)
	return node
}

// batchTestLatency 批量测速
func (rm *RelayManager) batchTestLatency(nodes []*RelayNode) []*RelayNode {
	if len(nodes) == 0 {
		return nodes
	}

	// 并发测速
	type result struct {
		node *RelayNode
	}
	results := make(chan result, len(nodes))

	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *RelayNode) {
			defer wg.Done()
			// 复制节点避免并发问题
			nodeCopy := &RelayNode{
				IP:     n.IP,
				Port:   n.Port,
				Source: n.Source,
			}
			results <- result{node: rm.testLatency(nodeCopy)}
		}(node)
	}

	wg.Wait()
	close(results)

	// 收集结果并排序
	resultNodes := make([]*RelayNode, 0, len(nodes))
	for r := range results {
		resultNodes = append(resultNodes, r.node)
	}

	// 按延迟排序
	for i := 0; i < len(resultNodes)-1; i++ {
		for j := i + 1; j < len(resultNodes); j++ {
			if resultNodes[i].Latency > resultNodes[j].Latency {
				resultNodes[i], resultNodes[j] = resultNodes[j], resultNodes[i]
			}
		}
	}

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
	beforeBest := rm.GetNextRelay()
	rm.mu.Unlock()

	rm.log.Warn("触发强制重新评分 (原最优: %s:%d %dms)...",
		beforeBest.IP, beforeBest.Port, beforeBest.Latency.Milliseconds())

	// 重新解析原始节点列表并测速
	candidateNodes := make([]*RelayNode, 0)
	for _, raw := range rm.rawRelays {
		host, port := ParseHostPort(raw)
		if ip := net.ParseIP(host); ip != nil {
			candidateNodes = append(candidateNodes, &RelayNode{
				IP:     host,
				Port:   port,
				Source: raw,
			})
		} else {
			ips, err := dns.LookupIP(host)
			if err != nil {
				rm.log.Error("解析域名 %s 失败: %v", host, err)
				continue
			}
			if len(ips) > 0 {
				for _, ip := range ips {
					candidateNodes = append(candidateNodes, &RelayNode{
						IP:     ip,
						Port:   port,
						Source: host,
					})
				}
			}
		}
	}

	// 批量测速并更新
	results := rm.batchTestLatency(candidateNodes)
	rm.totalTestCount += len(results)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 更新所有节点
	rm.allNodes = make([]*RelayNode, 0, len(results))
	for _, r := range results {
		r.LastCheck = now
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

	rm.resortByScore()

	afterBest := rm.GetNextRelay()
	rm.log.Info("强制重评完成: 有效节点%d个", len(rm.optimalRelays))
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
