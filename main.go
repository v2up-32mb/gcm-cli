package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	xsharedconfig "github.com/v2up-32mb/xshared/config"

	gcmlib "github.com/v2up-32mb/gcm"
	"github.com/v2up-32mb/gcm/pool"
	"github.com/v2up-32mb/gcm/relay"
	"github.com/v2up-32mb/xshared/dns"
	"github.com/v2up-32mb/xshared/ech"
	"github.com/v2up-32mb/xshared/httpproxy"
	"github.com/v2up-32mb/xshared/logger"
	"github.com/v2up-32mb/xshared/routing"
	"github.com/v2up-32mb/xshared/socks5"

	"gcm/config" // 兼容 shim：配置核心在 xshared
)

// websocketLogger 过滤 websocket 库的冗余日志
type websocketLogger struct {
	io.Writer
}

func (w *websocketLogger) Write(p []byte) (n int, err error) {
	// 过滤掉 "failed to close network connection" 这类无害警告
	if bytes.Contains(p, []byte("failed to close network connection")) {
		return len(p), nil
	}
	return w.Writer.Write(p)
}

var (
	cfg          *config.Config
	relayManager *relay.RelayManager
	dnsCache     *dns.DNSCache
	dohClient    *dns.DoHClient
	echManager   *ech.EchManager
	connPool     *pool.ConnectionPool
	socks5Server *socks5.Server
	httpServer   *httpproxy.Server
)

// b2i bool→int（bypass 规则行数统计用）
func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func main() {
	// 配置 websocket 库的日志过滤器（抑制无害的 "already closed" 警告）
	log.SetOutput(&websocketLogger{Writer: os.Stderr})

	// 加载配置
	var err error
	cfg, err = config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}

	// --help/--version 等路径不执行 Action，cfg 为 nil：帮助文本已输出，直接退出
	if cfg == nil {
		os.Exit(0)
	}

	// 初始化日志
	logger.InitGlobalLogger(cfg)
	defer logger.Close() // 确保在所有资源关闭后才关闭日志
	log := logger.GetLogger("System")

	printStartupInfo(log)

	// 初始化 DoH 客户端
	dohClient = dns.NewDoHClient(cfg)

	// 初始化 DNS 缓存
	dnsCache = dns.NewDNSCache(cfg, dohClient)
	defer dnsCache.Close()

	// 初始化中转节点管理器
	relayManager = relay.NewRelayManager(cfg.RelayIPs, cfg, dnsCache)
	if err := relayManager.Init(); err != nil {
		log.Error("初始化中转节点管理器失败: %v", err)
		os.Exit(1)
	}
	defer relayManager.Close()

	// 初始化 ECH 管理器（如果启用）
	if cfg.EnableECH {
		log.Info("正在初始化 ECH 管理器...")
		echManager = ech.NewEchManager(
			dohClient,
			cfg.ECHDomain,
			cfg.GetECHCacheTTL(),
			cfg.GetECHRefreshInterval(),
		)
		log.Debug("ECH 管理器初始化完成 (查询域名: %s)", cfg.ECHDomain)

		// M3: 在 DoH 代理启用前预取 ECH 配置，避免冷启动循环依赖
		log.Debug("预取 ECH 配置 (通过直连 DoH)...")
		if echConfig, err := dohClient.GetECHConfig(cfg.ECHDomain); err == nil {
			echManager.CacheConfig(cfg.ECHDomain, echConfig)
			log.Debug("ECH 配置预取成功")
		} else {
			log.Warn("ECH 配置预取失败: %v (将回退到标准 TLS)", err)
		}
	}

	// 初始化连接池
	log.Info("正在初始化连接池...")
	connPool = pool.NewConnectionPool(cfg, relayManager, echManager)
	defer connPool.Close()
	log.Debug("连接池初始化完成")

	// 设置 DoH 代理（如果启用）
	if cfg.EnableDoHProxy {
		log.Info("正在设置 DoH 代理模式...")
		proxyTransport := pool.NewProxyTransport(connPool)
		dohClient.EnableProxy(proxyTransport)
		log.Debug("DoH 代理模式已启用")
	}

	// 连接池预热（完全异步执行，确保不阻塞主线程）
	// warmupDone channel 用于 DNS 预热等待连接池预热完成（替代硬编码 sleep）
	warmupDone := make(chan struct{})
	log.Debug("启动预热 goroutine...")
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("连接池预热 panic: %v", r)
			}
			close(warmupDone)
		}()
		log.Debug("预热 goroutine 开始执行")
		if err := connPool.Warmup(); err != nil {
			log.Warn("连接池预热失败: %v", err)
		} else {
			log.Info("连接池预热完成")
		}
		log.Debug("预热 goroutine 退出")
	}()

	// DNS 缓存预热（异步执行，等待连接池预热完成）
	if cfg.EnableDNSWarmup {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("DNS 预热 panic: %v", r)
				}
			}()
			// 等待连接池预热完成后再预热 DNS
			log.Debug("等待连接池预热后开始 DNS 预热...")
			<-warmupDone
			log.Info("开始 DNS 缓存预热...")
			dnsCache.Warmup(cfg.DNSWarmupDomains)
		}()
	}

	// 立即继续启动 SOCKS5 服务器，不等待预热
	// 路由绕过：与 x-client gcm backend 相同的参数面（private/geoip-cn/geosite-cn/手动规则）
	var bypassMatcher *routing.Matcher
	if config.Overrides.BypassPrivate || config.Overrides.BypassGeoIPCN || config.Overrides.BypassGeoSiteCN || strings.TrimSpace(config.Overrides.BypassRules) != "" {
		geoIPPath, geoSitePath, giFound, gsFound := config.ResolveGeoPaths()
		m, err := routing.NewMatcherWithGeoFile(config.Overrides.BypassPrivate, config.Overrides.BypassGeoIPCN, config.Overrides.BypassGeoSiteCN, config.Overrides.BypassRules, geoIPPath, geoSitePath)
		if err != nil {
			log.Error("绕过规则无效: %v", err)
			os.Exit(1)
		}
		bypassMatcher = m
		log.Info("路由绕过: private=%v geoip-cn=%v geosite-cn=%v 手动规则=%d 行 | 数据文件: geoip.dat=%v geosite.dat=%v",
			config.Overrides.BypassPrivate, config.Overrides.BypassGeoIPCN, config.Overrides.BypassGeoSiteCN,
			strings.Count(config.Overrides.BypassRules, "\n")+b2i(config.Overrides.BypassRules != ""),
			giFound, gsFound)
	}

	log.Info("正在启动 SOCKS5 服务器...")
	socks5Opts := []socks5.Option{socks5.WithDNSCache(dnsCache)}
	if bypassMatcher != nil {
		socks5Opts = append(socks5Opts, socks5.WithBypassMatcher(bypassMatcher))
	}
	socks5Server = socks5.NewServer(cfg, gcmlib.NewStreamDialer(connPool), socks5Opts...)
	if err := socks5Server.Start(); err != nil {
		log.Error("启动 SOCKS5 服务器失败: %v", err)
		os.Exit(1)
	}
	defer socks5Server.Close()
	log.Debug("SOCKS5 服务器启动完成")

	// 可选 HTTP 代理监听（同一数据面与 bypass 策略）
	if listen := strings.TrimSpace(config.Overrides.HTTPListen); listen != "" {
		httpOpts := []httpproxy.Option{}
		if bypassMatcher != nil {
			httpOpts = append(httpOpts, httpproxy.WithBypassMatcher(bypassMatcher))
		}
		httpServer = httpproxy.NewServer(&xsharedconfig.Config{ListenAddress: listen}, gcmlib.NewStreamDialer(connPool), httpOpts...)
		if err := httpServer.Start(); err != nil {
			log.Error("启动 HTTP 代理服务器失败: %v", err)
			os.Exit(1)
		}
		defer httpServer.Close()
		log.Info("HTTP 代理监听: %s", listen)
	}

	// 启动 ECH 定时刷新任务（如果启用）
	if cfg.EnableECH && echManager != nil {
		log.Info("正在启动 ECH 定时刷新任务...")
		echManager.StartAutoRefresh()
		defer echManager.StopAutoRefresh()
		log.Debug("ECH 定时刷新任务已启动")
	}

	printReadyInfo(log)

	// 等待信号并优雅关闭
	waitForSignal(log)
	// 函数返回后，所有 defer 按逆序执行：
	// echManager.StopAutoRefresh → socks5Server.Close → connPool.Close
	// → relayManager.Close → dnsCache.Close → logger.Close
}

func printStartupInfo(log *logger.Logger) {
	log.Info("GCM 代理客户端 v1.0")
	log.Info("Worker: %s | 监听: %s | DoH: %v", cfg.WorkerHost, cfg.ListenAddress, cfg.EnableDoH)
	if cfg.ProxyIP != "" {
		log.Info("出口代理IP: %s", cfg.ProxyIP)
	}
	if cfg.ProxyAll {
		log.Info("强制走回退出口: 已启用（跳过直连；需 Worker 支持 ?proxy-all=）")
		if cfg.ProxyIP == "" {
			log.Warn("--proxy-all 已开但未指定 --proxy-ip，将完全依赖 Worker 侧自带的出口池" +
				"（动态节点/静态 FALLBACK_IPS）；若 Worker 未配任何出口，所有流会被直接关断")
		}
	}
	if cfg.EnableECH {
		log.Info("ECH: 已启用 (%s)", cfg.ECHDomain)
	}
	if cfg.UserID != "" {
		log.Info("用户ID: %s", cfg.UserID)
	}
	if len(cfg.RelayIPs) > 0 {
		log.Info("中转节点: %d 个", len(cfg.RelayIPs))
	}
}

func printReadyInfo(log *logger.Logger) {
	log.Info("SOCKS5 代理已就绪: socks5://%s", cfg.ListenAddress)
}

func waitForSignal(log *logger.Logger) os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Info("收到信号 %v，正在优雅关闭...", sig)
	return sig
}
