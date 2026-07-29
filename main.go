package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gcm/gcm/config"
	"github.com/gcm/gcm/dns"
	"github.com/gcm/gcm/logger"
	"github.com/gcm/gcm/pool"
	"github.com/gcm/gcm/relay"
	"github.com/gcm/gcm/socks5"
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
	connPool     *pool.ConnectionPool
	socks5Server *socks5.Server
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

	// 初始化连接池
	log.Info("正在初始化连接池...")
	connPool = pool.NewConnectionPool(cfg, relayManager)
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
	log.Info("正在启动 SOCKS5 服务器...")
	socks5Server = socks5.NewServer(cfg, connPool, dnsCache)
	if err := socks5Server.Start(); err != nil {
		log.Error("启动 SOCKS5 服务器失败: %v", err)
		os.Exit(1)
	}
	defer socks5Server.Close()
	log.Debug("SOCKS5 服务器启动完成")

	printReadyInfo(log)

	// 等待信号并优雅关闭
	waitForSignal(log)
	// 函数返回后，所有 defer 按逆序执行：
	// socks5Server.Close → connPool.Close
	// → relayManager.Close → dnsCache.Close → logger.Close
}

func printStartupInfo(log *logger.Logger) {
	log.Info("GCM 代理客户端 v1.0")
	log.Info("Worker: %s | 监听: %s | DoH: %v", cfg.WorkerHost, cfg.ListenAddress, cfg.EnableDoH)
	if cfg.ProxyIP != "" {
		log.Info("出口代理IP: %s", cfg.ProxyIP)
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
