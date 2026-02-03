package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gcm/config"
	"gcm/dns"
	"gcm/ech"
	"gcm/logger"
	"gcm/metrics"
	"gcm/pool"
	"gcm/relay"
	"gcm/socks5"
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
	cfg             *config.Config
	relayManager    *relay.RelayManager
	dnsCache        *dns.DNSCache
	dohClient       *dns.DoHClient
	echManager      *ech.EchManager
	connPool        *pool.ConnectionPool
	qualityMonitor  *pool.ConnectionQualityMonitor
	socks5Server    *socks5.Server
	metricsSrv      *metrics.Server
)

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

	// 初始化日志
	logger.InitGlobalLogger(cfg)
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
	log.Debug("启动预热 goroutine...")
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("连接池预热 panic: %v", r)
			}
		}()
		log.Debug("预热 goroutine 开始执行")
		if err := connPool.Warmup(); err != nil {
			log.Warn("连接池预热失败: %v", err)
		} else {
			log.Info("连接池预热完成")
		}
		log.Debug("预热 goroutine 退出")
	}()

	// DNS 缓存预热（异步执行）
	if cfg.EnableDNSWarmup {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("DNS 预热 panic: %v", r)
				}
			}()
			// 等待连接池预热完成后再预热 DNS
			log.Debug("等待连接池预热后开始 DNS 预热...")
			time.Sleep(3 * time.Second)
			log.Info("开始 DNS 缓存预热...")
			dnsCache.Warmup(cfg.DNSWarmupDomains)
		}()
	}

	// 立即继续启动 SOCKS5 服务器，不等待预热
	log.Info("正在启动 SOCKS5 服务器...")
	socks5Server = socks5.NewServer(cfg, connPool, dnsCache)
	if err := socks5Server.Start(); err != nil {
		log.Error("启动 SOCKS5 服务器失败: %v", err)
		os.Exit(1)
	}
	defer socks5Server.Close()
	log.Debug("SOCKS5 服务器启动完成")

	// 启动 Metrics 服务器（可选）
	if cfg.EnableMetrics {
		log.Info("正在启动 Metrics 服务器...")
		metricsSrv = metrics.NewServer(cfg, connPool, relayManager, dnsCache)
		if err := metricsSrv.Start(); err != nil {
			log.Error("启动 Metrics 服务器失败: %v", err)
		} else {
			log.Debug("Metrics 服务器启动完成")
			defer metricsSrv.Close()
		}
	}

	// 启动 ECH 定时刷新任务（如果启用）
	if cfg.EnableECH && echManager != nil {
		log.Info("正在启动 ECH 定时刷新任务...")
		echManager.StartAutoRefresh()
		defer echManager.StopAutoRefresh()
		log.Debug("ECH 定时刷新任务已启动")
	}

	// 启动连接质量监控器（如果启用）
	if cfg.EnableQualityMonitor {
		log.Info("正在启动连接质量监控器...")
		qualityMonitor = pool.NewConnectionQualityMonitor(connPool, cfg, logger.GetLogger("QualityMonitor"))
		qualityMonitor.Start()
		defer qualityMonitor.Stop()
		log.Debug("连接质量监控器已启动 (检查间隔: %v)", cfg.GetQualityCheckInterval())
	}

	printReadyInfo(log)

	// 等待信号
	waitForSignal(log)
}

func printStartupInfo(log *logger.Logger) {
	log.Info("========================================")
	log.Info("  GCM 代理客户端启动中...")
	log.Info("========================================")
	log.Info("Worker: %s", cfg.WorkerHost)
	log.Info("监听地址: %s", cfg.ListenAddress)
	log.Info("DoH: %v (%s)", cfg.EnableDoH, cfg.DoHUrl)
	log.Info("ECH: %v", cfg.EnableECH)
	log.Info("连接池: Min=%d, Max=%d", cfg.MinPoolSize, cfg.MaxPoolSize)
	log.Info("DNS缓存TTL: %d秒", int(cfg.GetDNSCacheTTL().Seconds()))
	log.Info("日志级别: %s", cfg.LogLevel)
	log.Info("Metrics: %v", cfg.EnableMetrics)
	if cfg.EnableMetrics {
		log.Info("Metrics 端口: %d", cfg.MetricsPort)
	}
	log.Info("连接池预热: %v", cfg.EnablePoolWarmup)
	log.Info("断线重连: %v", cfg.EnableAutoReconnect)
	log.Info("动态池调整: %v", cfg.EnableDynamicPool)
	log.Info("多路复用: %v", cfg.EnableMultiplex)
	log.Info("质量监控: %v", cfg.EnableQualityMonitor)
	if cfg.EnableQualityMonitor {
		log.Info("质量检查间隔: %v", cfg.GetQualityCheckInterval())
	}
	log.Info("----------------------------------------")
}

func printReadyInfo(log *logger.Logger) {
	log.Info("========================================")
	log.Info("  服务已就绪，等待连接...")
	log.Info("========================================")
}

func waitForSignal(log *logger.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Info("收到信号 %v，正在优雅关闭...", sig)

	// 优雅关闭
	logger.Close()
	os.Exit(0)
}
