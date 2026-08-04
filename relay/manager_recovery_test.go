package relay

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"gcm/config"
)

// newTestRelayManager 构建一个最小可用的 RelayManager 用于测试。
// rawRelays 通常为 IP:Port 形式，以避免依赖 DNS。
func newTestRelayManager(t *testing.T, rawRelays []string) *RelayManager {
	t.Helper()
	return NewRelayManager(rawRelays, config.DefaultConfig(), nil)
}

// TestRescoreReleasesManagerLock 验证重评（doRescore 路径）不会因持有写锁而
// 死锁后续的 GetNextRelayWithLoadBalance 选择。
// 改造 A 后 rescoreAll 与 ForceRescore 都从 rawRelays 重解析，
// 因此通过 IP:Port 注入原始配置而非直接修改 allNodes。
func TestRescoreReleasesManagerLock(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse listener port: %v", err)
	}

	// 注入原始配置 IP:Port；改造 A 后重评从此重解析。
	rm := newTestRelayManager(t, []string{net.JoinHostPort(host, portText)})
	rm.isInitialized = true

	// 预置一个旧 allNodes 指针以验证 batchTestLatency 原地更新语义（问题 3）：
	// 重评后节点仍是原指针（IP:Port 一致），而不是全新对象。
	oldNode := &RelayNode{IP: host, Port: port, Source: "test"}
	rm.allNodes = []*RelayNode{oldNode}
	rm.optimalRelays = []*RelayNode{oldNode}

	done := make(chan struct{})
	go func() {
		rm.doRescore("test")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("doRescore deadlocked while logging relays under the manager lock")
	}

	// 问题 3：重评后该 IP:Port 节点的指针应保持原对象（batchTestLatency 原地更新）。
	rm.mu.RLock()
	var got *RelayNode
	for _, n := range rm.allNodes {
		if n.IP == host && n.Port == port {
			got = n
			break
		}
	}
	rm.mu.RUnlock()
	if got == nil {
		t.Fatal("rescore dropped the listener node")
	}
	if got != oldNode {
		t.Fatalf("rescore replaced node pointer (expected same pointer, got different); batchTestLatency must update in-place")
	}

	// 重评后选择不应阻塞——若重评仍持锁则会超时。
	selected := make(chan *RelayNode, 1)
	go func() {
		selected <- rm.GetNextRelayWithLoadBalance()
	}()
	select {
	case r := <-selected:
		if r == nil {
			t.Fatal("relay selection returned nil after successful rescore")
		}
	case <-time.After(time.Second):
		t.Fatal("relay selection remained blocked after rescore")
	}
}

// TestConcurrentGetNextRelayWithLoadBalance 验证问题 1 的修复：
// 并发调用 GetNextRelayWithLoadBalance 不应因 rand.Rand 并发不安全而 panic。
func TestConcurrentGetNextRelayWithLoadBalance(t *testing.T) {
	// 起两个 listener 作为候选节点以触发加权随机路径（selectByWeight）。
	listeners := make([]net.Listener, 2)
	relays := make([]string, 2)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen[%d]: %v", i, err)
		}
		listeners[i] = l
		relays[i] = l.Addr().String()
	}
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()

	rm := newTestRelayManager(t, relays)
	rm.isInitialized = true
	// 直接构造 optimalRelays 以触发 selectByWeight 的并发路径。
	rm.mu.Lock()
	rm.allNodes = make([]*RelayNode, 0, 2)
	rm.optimalRelays = make([]*RelayNode, 0, 2)
	for i, r := range relays {
		host, portText, _ := net.SplitHostPort(r)
		port, _ := strconv.Atoi(portText)
		node := &RelayNode{IP: host, Port: port, Source: "test", Latency: time.Duration(i+1) * time.Millisecond}
		rm.allNodes = append(rm.allNodes, node)
		rm.optimalRelays = append(rm.optimalRelays, node)
	}
	rm.mu.Unlock()

	const concurrency = 32
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_ = rm.GetNextRelayWithLoadBalance()
		}()
	}
	wg.Wait()
	// 若 rand.Rand 不安全则会 panic 或 fatal error，测试自动失败。
}

// TestReportFailureRemovesFromAllNodes 验证问题 5 的修复：
// ReportFailure 达阈值后从 optimalRelays 和 allNodes 同步移除。
func TestReportFailureRemovesFromAllNodes(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	host, portText, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(portText)

	cfg := config.DefaultConfig()
	// 阈值设为 2 以便测试快速触发。
	cfg.RelayFailureThreshold = 2
	rm := NewRelayManager(nil, cfg, nil)
	rm.isInitialized = true
	node := &RelayNode{IP: host, Port: port, Source: "test"}
	rm.mu.Lock()
	rm.allNodes = []*RelayNode{node}
	rm.optimalRelays = []*RelayNode{node}
	rm.mu.Unlock()

	// 第一次失败：仅 FailCount+1，节点仍在。
	rm.ReportFailure(host, port)
	rm.mu.RLock()
	if len(rm.optimalRelays) != 1 || len(rm.allNodes) != 1 {
		rm.mu.RUnlock()
		t.Fatalf("after first failure expected 1/1 nodes, got %d/%d", len(rm.optimalRelays), len(rm.allNodes))
	}
	rm.mu.RUnlock()

	// 第二次失败：达到阈值，应同时从 optimalRelays 和 allNodes 移除。
	rm.ReportFailure(host, port)
	rm.mu.RLock()
	if len(rm.optimalRelays) != 0 || len(rm.allNodes) != 0 {
		rm.mu.RUnlock()
		t.Fatalf("after threshold failures expected 0/0 nodes, got %d/%d", len(rm.optimalRelays), len(rm.allNodes))
	}
	rm.mu.RUnlock()
}

// TestInitAllIPsPopulateCandidates 验证改造 B 的精神：
// Init 时通过 IP 类 relay 注入的每个节点都应进入 allNodes。
// （域名全 IP 入候选的等价验证：所有解析到的 IP 平等待测，不被内部截断。）
func TestInitAllIPsPopulateCandidates(t *testing.T) {
	// 启三个 listener 模拟三个节点。
	listeners := make([]net.Listener, 3)
	ipPorts := make([]string, 3)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen[%d]: %v", i, err)
		}
		listeners[i] = l
		ipPorts[i] = l.Addr().String()
	}
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()

	cfg := config.DefaultConfig()
	// 放宽阈值以包含全部 listener（默认 500ms 在 CI 抖动下可能误杀）。
	cfg.RelayMaxLatency = config.NewYamlDuration(10 * time.Second)
	rm := NewRelayManager(ipPorts, cfg, nil)
	if err := rm.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if len(rm.allNodes) != 3 {
		t.Fatalf("expected 3 nodes in allNodes, got %d", len(rm.allNodes))
	}
	// 每个 listener 都应在 allNodes 中。
	for _, ip := range ipPorts {
		host, portText, _ := net.SplitHostPort(ip)
		port, _ := strconv.Atoi(portText)
		found := false
		for _, n := range rm.allNodes {
			if n.IP == host && n.Port == port {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected node %s in allNodes, missing", ip)
		}
	}
}
