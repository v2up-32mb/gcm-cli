package config

import (
	"strings"
	"time"

	"github.com/urfave/cli/v2"
)

// DefineFlags 定义所有命令行参数
func DefineFlags() []cli.Flag {
	return []cli.Flag{
		// ===== 配置文件 =====
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "配置文件路径 (YAML/JSON)",
		},

		// ===== 基本配置 =====
		&cli.StringFlag{
			Name:    "worker",
			Aliases: []string{"w"},
			Usage:   "Worker 地址 (必须指定)",
		},
		&cli.StringFlag{
			Name:    "listen",
			Aliases: []string{"l"},
			Usage:   "SOCKS5 监听地址 (如 :10080 或 0.0.0.0:10080)",
			Value:   ":10080",
		},
		&cli.StringFlag{
			Name:    "token",
			Aliases: []string{"t"},
			Usage:   "代理访问令牌",
		},
		&cli.StringFlag{
			Name:    "log-level",
			Usage:   "日志级别: DEBUG/INFO/WARN/ERROR",
			Value:   "INFO",
		},

		// ===== DNS 配置 =====
		&cli.StringFlag{
			Name:    "doh",
			Aliases: []string{"d"},
			Usage:   "DoH 服务地址",
			Value:   "https://v.recipes/dns-query",
		},
		&cli.BoolFlag{
			Name:  "no-doh",
			Usage: "禁用 DoH",
		},
		&cli.BoolFlag{
			Name:  "dns-warmup",
			Usage: "启用 DNS 预热",
		},
		&cli.BoolFlag{
			Name:  "doh-proxy",
			Usage: "启用 DoH 通过代理访问",
		},

		// ===== 中转节点配置 =====
		&cli.StringSliceFlag{
			Name:    "relay",
			Aliases: []string{"r"},
			Usage:   "中转节点列表（可多次指定或逗号分隔）",
		},
		&cli.IntFlag{
			Name:  "min-pool",
			Usage: "最小连接池大小",
			Value: 3,
		},
		&cli.IntFlag{
			Name:  "max-pool",
			Usage: "最大连接池大小",
			Value: 15,
		},

		// ===== Metrics 配置 =====
		&cli.BoolFlag{
			Name:  "metrics",
			Usage: "启用 Metrics 端点",
		},
		&cli.IntFlag{
			Name:  "metrics-port",
			Usage: "Metrics 端口",
			Value: 9090,
		},

		// ===== 连接池配置 =====
		&cli.BoolFlag{
			Name:  "no-warmup",
			Usage: "禁用连接池预热",
		},
		&cli.BoolFlag{
			Name:  "no-reconnect",
			Usage: "禁用断线自动重连",
		},
		&cli.BoolFlag{
			Name:  "no-dynamic-pool",
			Usage: "禁用动态池大小调整",
		},

		// ===== 日志配置 =====
		&cli.StringFlag{
			Name:  "log-file",
			Usage: "启用日志文件输出到指定路径",
		},

		// ===== 隧道配置 =====
		&cli.IntFlag{
			Name:  "timeout",
			Usage: "隧道超时时间（秒）",
			Value: 60,
		},
		&cli.BoolFlag{
			Name:  "no-mux",
			Usage: "禁用多路复用",
		},

		// ===== 高级时间配置 =====
		&cli.DurationFlag{
			Name:  "connection-ttl",
			Usage: "连接最大存活时间",
			Value: 5 * time.Minute,
		},
		&cli.DurationFlag{
			Name:  "connection-timeout",
			Usage: "连接超时时间",
			Value: time.Second,
		},
		&cli.DurationFlag{
			Name:  "heartbeat-interval",
			Usage: "心跳间隔",
			Value: 15 * time.Second,
		},
		&cli.DurationFlag{
			Name:  "heartbeat-timeout",
			Usage: "心跳响应超时",
			Value: 3 * time.Second,
		},
		&cli.DurationFlag{
			Name:  "dns-cache-ttl",
			Usage: "DNS 缓存过期时间",
			Value: 5 * time.Minute,
		},
		&cli.DurationFlag{
			Name:  "relay-monitor-interval",
			Usage: "节点监控间隔",
			Value: 30 * time.Second,
		},
		&cli.DurationFlag{
			Name:  "relay-max-latency",
			Usage: "节点最大可接受延迟",
			Value: 500 * time.Millisecond,
		},
	}
}

// ApplyFlags 将命令行参数应用到 Config
// urfave/cli v2 使用 *cli.Context
func ApplyFlags(cfg *Config, ctx *cli.Context) error {
	// 从 ctx 获取参数值
	// ===== 基本配置 =====
	if ctx.IsSet("worker") {
		cfg.WorkerHost = ctx.String("worker")
	}
	if ctx.IsSet("listen") {
		cfg.ListenAddress = ctx.String("listen")
	}
	if ctx.IsSet("token") {
		cfg.ProxyToken = ctx.String("token")
	}
	if ctx.IsSet("log-level") {
		cfg.LogLevel = ParseLogLevel(ctx.String("log-level"))
	}

	// ===== DNS 配置 =====
	if ctx.IsSet("doh") {
		cfg.DoHUrl = ctx.String("doh")
	}
	if ctx.IsSet("no-doh") && ctx.Bool("no-doh") {
		cfg.EnableDoH = false
	}
	if ctx.IsSet("dns-warmup") && ctx.Bool("dns-warmup") {
		cfg.EnableDNSWarmup = true
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

	// ===== Metrics 配置 =====
	if ctx.IsSet("metrics") && ctx.Bool("metrics") {
		cfg.EnableMetrics = true
	}
	if ctx.IsSet("metrics-port") {
		cfg.MetricsPort = ctx.Int("metrics-port")
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
