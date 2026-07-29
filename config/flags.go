package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
)

// DefineFlags 定义所有命令行参数
func DefineFlags() []cli.Flag {
	return []cli.Flag{
		// ===== 基本配置 =====
		&cli.StringFlag{
			Name:     "config",
			Aliases:  []string{"c"},
			Usage:    "配置文件路径 (YAML/JSON)",
			Category: "基本",
		},
		&cli.StringFlag{
			Name:     "worker",
			Aliases:  []string{"w"},
			Usage:    "Cloudflare Worker 地址 (必须指定)",
			Category: "基本",
		},
		&cli.StringFlag{
			Name:     "listen",
			Aliases:  []string{"l"},
			Usage:    "SOCKS5 监听地址 (如 :1080 或 0.0.0.0:1080)",
			Value:    ":1080",
			Category: "基本",
		},
		&cli.StringFlag{
			Name:     "user-id",
			Aliases:  []string{"u"},
			Usage:    "用户鉴权ID",
			Category: "基本",
		},
		&cli.StringFlag{
			Name:     "proxy-ip",
			Aliases:  []string{"p"},
			Usage:    "出口端代理IP (留空时 Worker 使用自身已配置的 proxyIP)",
			Category: "基本",
		},
		&cli.StringFlag{
			Name:     "log-level",
			Usage:    "日志级别: DEBUG/INFO/WARN/ERROR",
			Value:    "INFO",
			Category: "基本",
		},

		// ===== DNS 配置 =====
		&cli.StringFlag{
			Name:     "doh",
			Aliases:  []string{"d"},
			Usage:    "DoH 服务地址",
			Value:    "https://v.recipes/dns-query",
			Category: "网络",
		},
		&cli.DurationFlag{
			Name:     "doh-timeout",
			Usage:    "DoH 查询超时时间",
			Value:    3 * time.Second,
			Category: "网络",
		},
		&cli.BoolFlag{
			Name:     "no-doh",
			Usage:    "禁用 DoH",
			Category: "网络",
		},
		&cli.BoolFlag{
			Name:     "no-dns-warmup",
			Usage:    "禁用 DNS 预热",
			Category: "高级",
		},
		&cli.BoolFlag{
			Name:     "doh-proxy",
			Usage:    "启用 DoH 通过代理访问",
			Category: "高级",
		},

		// ===== 中转节点配置 =====
		&cli.StringSliceFlag{
			Name:     "relay",
			Aliases:  []string{"r"},
			Usage:    "中转节点列表（可多次指定或逗号分隔）",
			Category: "网络",
		},

		// ===== 连接池配置 =====
		&cli.IntFlag{
			Name:     "min-pool",
			Usage:    "最小连接池大小",
			Value:    5,
			Category: "连接池",
		},
		&cli.IntFlag{
			Name:     "max-pool",
			Usage:    "最大连接池大小",
			Value:    15,
			Category: "连接池",
		},
		&cli.BoolFlag{
			Name:     "no-warmup",
			Usage:    "禁用连接池预热",
			Category: "连接池",
		},
		&cli.BoolFlag{
			Name:     "no-reconnect",
			Usage:    "禁用断线自动重连",
			Category: "连接池",
		},
		&cli.BoolFlag{
			Name:     "no-dynamic-pool",
			Usage:    "禁用动态池大小调整",
			Category: "连接池",
		},

		// ===== 日志配置 =====
		&cli.StringFlag{
			Name:     "log-file",
			Usage:    "启用日志文件输出到指定路径",
			Category: "日志",
		},

		// ===== 隧道配置 =====
		&cli.IntFlag{
			Name:     "timeout",
			Usage:    "隧道超时时间（秒）",
			Value:    60,
			Category: "网络",
		},
		&cli.BoolFlag{
			Name:     "no-mux",
			Usage:    "禁用多路复用",
			Category: "连接池",
		},

		// ===== 高级时间配置 =====
		&cli.DurationFlag{
			Name:     "connection-ttl",
			Usage:    "连接最大存活时间",
			Value:    5 * time.Minute,
			Category: "高级",
		},
		&cli.DurationFlag{
			Name:     "connection-timeout",
			Usage:    "连接超时时间",
			Value:    time.Second,
			Category: "高级",
		},
		&cli.DurationFlag{
			Name:     "heartbeat-interval",
			Usage:    "心跳间隔",
			Value:    15 * time.Second,
			Category: "高级",
		},
		&cli.DurationFlag{
			Name:     "heartbeat-timeout",
			Usage:    "心跳响应超时",
			Value:    3 * time.Second,
			Category: "高级",
		},
		&cli.DurationFlag{
			Name:     "dns-cache-ttl",
			Usage:    "DNS 缓存过期时间",
			Value:    5 * time.Minute,
			Category: "高级",
		},
		&cli.DurationFlag{
			Name:     "relay-monitor-interval",
			Usage:    "节点监控间隔",
			Value:    30 * time.Second,
			Category: "高级",
		},
		&cli.DurationFlag{
			Name:     "relay-max-latency",
			Usage:    "节点最大可接受延迟",
			Value:    500 * time.Millisecond,
			Category: "高级",
		},

		// ===== 窗口流控配置 =====
		&cli.StringFlag{
			Name:     "default-window-size",
			Usage:    "默认窗口大小 (如 1MB, 2MB)",
			Value:    "1MB",
			Category: "高级",
		},
		&cli.StringFlag{
			Name:     "min-window-size",
			Usage:    "最小窗口大小 (如 64KB)",
			Value:    "64KB",
			Category: "高级",
		},
		&cli.StringFlag{
			Name:     "max-window-size",
			Usage:    "最大窗口大小 (如 4MB)",
			Value:    "4MB",
			Category: "高级",
		},
		&cli.DurationFlag{
			Name:     "window-timeout",
			Usage:    "窗口等待超时时间",
			Value:    5 * time.Second,
			Category: "高级",
		},

		// ===== 拥塞控制配置 =====
		&cli.DurationFlag{
			Name:     "congestion-control-interval",
			Usage:    "拥塞控制检查间隔",
			Value:    time.Minute,
			Category: "高级",
		},
	}
}

// ApplyFlags 将命令行参数应用到 Config
// urfave/cli v2 使用 *cli.Context
func ApplyFlags(cfg *Config, ctx *cli.Context) error {
	// ===== 基本配置 =====
	if ctx.IsSet("worker") {
		cfg.WorkerHost = ctx.String("worker")
	}
	if ctx.IsSet("listen") {
		cfg.ListenAddress = ctx.String("listen")
	}
	if ctx.IsSet("user-id") {
		cfg.UserID = ctx.String("user-id")
	}
	if ctx.IsSet("proxy-ip") {
		cfg.ProxyIP = ctx.String("proxy-ip")
	}
	if ctx.IsSet("log-level") {
		cfg.LogLevel = ParseLogLevel(ctx.String("log-level"))
	}

	// ===== DNS 配置 =====
	if ctx.IsSet("doh") {
		cfg.DoHUrl = ctx.String("doh")
	}
	if ctx.IsSet("doh-timeout") {
		cfg.DoHTimeout = yamlDuration{ctx.Duration("doh-timeout")}
	}
	if ctx.IsSet("no-doh") && ctx.Bool("no-doh") {
		cfg.EnableDoH = false
	}
	if ctx.IsSet("no-dns-warmup") && ctx.Bool("no-dns-warmup") {
		cfg.EnableDNSWarmup = false
	}
	if ctx.IsSet("doh-proxy") && ctx.Bool("doh-proxy") {
		cfg.EnableDoHProxy = true
	}

	// ===== 中转节点配置 =====
	if ctx.IsSet("relay") {
		relayList := ctx.StringSlice("relay")
		if len(relayList) > 0 {
			cfg.RelayIPs = flattenStringSlice(relayList)
		}
	}
	if ctx.IsSet("min-pool") {
		cfg.MinPoolSize = ctx.Int("min-pool")
	}
	if ctx.IsSet("max-pool") {
		cfg.MaxPoolSize = ctx.Int("max-pool")
	}

	// ===== 连接池配置 =====
	if ctx.IsSet("no-warmup") && ctx.Bool("no-warmup") {
		cfg.EnablePoolWarmup = false
	}
	if ctx.IsSet("no-reconnect") && ctx.Bool("no-reconnect") {
		cfg.EnableAutoReconnect = false
	}
	if ctx.IsSet("no-dynamic-pool") && ctx.Bool("no-dynamic-pool") {
		cfg.EnableDynamicPool = false
	}

	// ===== 日志配置 =====
	if ctx.IsSet("log-file") {
		cfg.EnableLogFile = true
		cfg.LogFilePath = ctx.String("log-file")
	}

	// ===== 隧道配置 =====
	if ctx.IsSet("timeout") {
		cfg.TunnelTimeout = yamlDuration{time.Duration(ctx.Int("timeout")) * time.Second}
	}
	if ctx.IsSet("no-mux") && ctx.Bool("no-mux") {
		cfg.EnableMultiplex = false
	}

	// ===== 高级时间配置 =====
	if ctx.IsSet("connection-ttl") {
		cfg.ConnectionTTL = yamlDuration{ctx.Duration("connection-ttl")}
	}
	if ctx.IsSet("connection-timeout") {
		cfg.ConnectionTimeout = yamlDuration{ctx.Duration("connection-timeout")}
	}
	if ctx.IsSet("heartbeat-interval") {
		cfg.HeartbeatInterval = yamlDuration{ctx.Duration("heartbeat-interval")}
	}
	if ctx.IsSet("heartbeat-timeout") {
		cfg.HeartbeatTimeout = yamlDuration{ctx.Duration("heartbeat-timeout")}
	}
	if ctx.IsSet("dns-cache-ttl") {
		cfg.DNSCacheTTL = yamlDuration{ctx.Duration("dns-cache-ttl")}
	}
	if ctx.IsSet("relay-monitor-interval") {
		cfg.RelayMonitorInterval = yamlDuration{ctx.Duration("relay-monitor-interval")}
	}
	if ctx.IsSet("relay-max-latency") {
		cfg.RelayMaxLatency = yamlDuration{ctx.Duration("relay-max-latency")}
	}

	// ===== 窗口流控配置 =====
	if ctx.IsSet("default-window-size") {
		bytes, err := parseByteSize(ctx.String("default-window-size"))
		if err != nil {
			return fmt.Errorf("无效的默认窗口大小: %w", err)
		}
		cfg.DefaultWindowSize = yamlByteSize{bytes}
	}
	if ctx.IsSet("min-window-size") {
		bytes, err := parseByteSize(ctx.String("min-window-size"))
		if err != nil {
			return fmt.Errorf("无效的最小窗口大小: %w", err)
		}
		cfg.MinWindowSize = yamlByteSize{bytes}
	}
	if ctx.IsSet("max-window-size") {
		bytes, err := parseByteSize(ctx.String("max-window-size"))
		if err != nil {
			return fmt.Errorf("无效的最大窗口大小: %w", err)
		}
		cfg.MaxWindowSize = yamlByteSize{bytes}
	}
	if ctx.IsSet("window-timeout") {
		cfg.WindowTimeout = yamlDuration{ctx.Duration("window-timeout")}
	}

	// ===== 拥塞控制配置 =====
	if ctx.IsSet("congestion-control-interval") {
		cfg.CongestionControlInterval = yamlDuration{ctx.Duration("congestion-control-interval")}
	}

	return nil
}

// flattenStringSlice 将字符串切片中的逗号分隔元素展开
func flattenStringSlice(items []string) []string {
	var result []string
	for _, item := range items {
		// 检查是否包含逗号
		for _, part := range splitComma(item) {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				result = append(result, trimmed)
			}
		}
	}
	return result
}

// splitComma 按逗号分割字符串
func splitComma(s string) []string {
	return strings.Split(s, ",")
}
