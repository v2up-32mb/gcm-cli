package config

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
)

// DefineFlags 返回所有命令行参数定义。
//
// 返回的参数按功能分类：
//   - 配置文件：指定配置文件路径
//   - 基本配置：Worker 地址、监听地址、日志级别
//   - DNS 配置：DoH 服务、缓存、预热
//   - 中转节点配置：节点列表、连接池大小
//   - Metrics 配置：监控端点开关和端口
//   - 连接池配置：预热、重连、动态调整
//   - 日志配置：日志文件输出
//   - 隧道配置：超时、多路复用
//   - 高级时间配置：连接 TTL、心跳等
//   - 窗口流控配置：接收/发送窗口大小
//   - 拥塞控制配置：动态窗口调整间隔
//   - ECH 配置：TLS Encrypted Client Hello
func DefineFlags() []cli.Flag {
	return []cli.Flag{
		// ===== 配置文件 =====
		&cli.StringFlag{
			Name:     "config",
			Aliases:  []string{"c"},
			Usage:    "配置文件路径 (YAML/JSON)",
			OnlyOnce: true,
		},

		// ===== 基本配置 =====
		&cli.StringFlag{
			Name:     "worker",
			Aliases:  []string{"w"},
			Usage:    "Worker 地址 (必须指定)",
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:     "listen",
			Aliases:  []string{"l"},
			Usage:    "SOCKS5 监听地址 (如 :10080 或 0.0.0.0:10080)",
			// 移除默认值，避免覆盖配置文件
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:     "user-id",
			Aliases:  []string{"u"},
			Usage:    "用户鉴权ID",
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:     "log-level",
			Usage:    "日志级别: DEBUG/INFO/WARN/ERROR",
			Value:    "INFO",
			OnlyOnce: true,
		},

		// ===== DNS 配置 =====
		&cli.StringFlag{
			Name:     "doh",
			Aliases:  []string{"d"},
			Usage:    "DoH 服务地址",
			Value:    "https://v.recipes/dns-query",
			OnlyOnce: true,
		},
		&cli.BoolFlag{
			Name:     "no-doh",
			Usage:    "禁用 DoH",
			OnlyOnce: true,
		},
		&cli.BoolFlag{
			Name:     "dns-warmup",
			Usage:    "启用 DNS 预热",
			OnlyOnce: true,
		},
		&cli.BoolFlag{
			Name:     "doh-proxy",
			Usage:    "启用 DoH 通过代理访问",
			OnlyOnce: true,
		},

		// ===== 中转节点配置 =====
		&cli.StringSliceFlag{
			Name:    "relay",
			Aliases: []string{"r"},
			Usage:   "中转节点列表（可多次指定或逗号分隔）",
		},
		&cli.IntFlag{
			Name:     "min-pool",
			Usage:    "最小连接池大小",
			Value:    10,
			OnlyOnce: true,
		},
		&cli.IntFlag{
			Name:     "max-pool",
			Usage:    "最大连接池大小",
			Value:    50,
			OnlyOnce: true,
		},

		// ===== Metrics 配置 =====
		&cli.BoolFlag{
			Name:     "metrics",
			Usage:    "启用 Metrics 端点",
			OnlyOnce: true,
		},
		&cli.IntFlag{
			Name:     "metrics-port",
			Usage:    "Metrics 端口",
			Value:    9090,
			OnlyOnce: true,
		},

		// ===== 连接池配置 =====
		&cli.BoolFlag{
			Name:     "no-warmup",
			Usage:    "禁用连接池预热",
			OnlyOnce: true,
		},
		&cli.BoolFlag{
			Name:     "no-reconnect",
			Usage:    "禁用断线自动重连",
			OnlyOnce: true,
		},
		&cli.BoolFlag{
			Name:     "no-dynamic-pool",
			Usage:    "禁用动态池大小调整",
			OnlyOnce: true,
		},

		// ===== 日志配置 =====
		&cli.StringFlag{
			Name:     "log-file",
			Usage:    "启用日志文件输出到指定路径",
			OnlyOnce: true,
		},

		// ===== 隧道配置 =====
		&cli.IntFlag{
			Name:     "timeout",
			Usage:    "隧道超时时间（秒）",
			Value:    60,
			OnlyOnce: true,
		},
		&cli.BoolFlag{
			Name:     "no-mux",
			Usage:    "禁用多路复用",
			OnlyOnce: true,
		},

		// ===== 高级时间配置 =====
		&cli.DurationFlag{
			Name:     "connection-ttl",
			Usage:    "连接最大存活时间",
			Value:    5 * time.Minute,
			OnlyOnce: true,
		},
		&cli.DurationFlag{
			Name:     "connection-timeout",
			Usage:    "连接超时时间",
			Value:    time.Second,
			OnlyOnce: true,
		},
		&cli.DurationFlag{
			Name:     "heartbeat-interval",
			Usage:    "心跳间隔",
			Value:    15 * time.Second,
			OnlyOnce: true,
		},
		&cli.DurationFlag{
			Name:     "heartbeat-timeout",
			Usage:    "心跳响应超时",
			Value:    3 * time.Second,
			OnlyOnce: true,
		},
		&cli.DurationFlag{
			Name:     "dns-cache-ttl",
			Usage:    "DNS 缓存过期时间",
			Value:    5 * time.Minute,
			OnlyOnce: true,
		},
		&cli.DurationFlag{
			Name:     "relay-monitor-interval",
			Usage:    "节点监控间隔",
			Value:    30 * time.Second,
			OnlyOnce: true,
		},
		&cli.DurationFlag{
			Name:     "relay-max-latency",
			Usage:    "节点最大可接受延迟",
			Value:    500 * time.Millisecond,
			OnlyOnce: true,
		},

		// ===== 窗口流控配置 =====
		&cli.StringFlag{
			Name:     "default-window-size",
			Usage:    "默认窗口大小 (如 256KB, 1MB)",
			Value:    "256KB",
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:     "min-window-size",
			Usage:    "最小窗口大小 (如 32KB)",
			Value:    "32KB",
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:     "max-window-size",
			Usage:    "最大窗口大小 (如 1MB)",
			Value:    "1MB",
			OnlyOnce: true,
		},
		&cli.DurationFlag{
			Name:     "window-timeout",
			Usage:    "窗口等待超时时间",
			Value:    5 * time.Second,
			OnlyOnce: true,
		},

		// ===== 拥塞控制配置 =====
		&cli.DurationFlag{
			Name:     "congestion-control-interval",
			Usage:    "拥塞控制检查间隔",
			Value:    time.Minute,
			OnlyOnce: true,
		},

		// ===== ECH 配置 =====
		&cli.BoolFlag{
			Name:     "enable-ech",
			Aliases:  []string{"e"},
			Usage:    "启用 TLS ECH (Encrypted Client Hello)",
                        Value:    true,
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:     "ech-domain",
			Usage:    "ECH 隐藏域名",
			Value:    "cloudflare-ech.com",
			OnlyOnce: true,
		},
	}
}

// ApplyFlags 将命令行参数应用到配置结构体。
//
// 该函数从命令行参数中读取值并更新 cfg。
// 命令行参数的优先级高于配置文件。
// urfave/cli v3 使用 context.Context 而不是 *cli.Context。
func ApplyFlags(cfg *Config, ctx context.Context, cmd *cli.Command) error {
	// 从 cmd 获取参数值
	// ===== 基本配置 =====
	if cmd.IsSet("worker") {
		cfg.WorkerHost = cmd.String("worker")
	}
	if cmd.IsSet("listen") {
		cfg.ListenAddress = cmd.String("listen")
	}
	if cmd.IsSet("user-id") {
		cfg.UserID = cmd.String("user-id")
	}
	if cmd.IsSet("log-level") {
		cfg.LogLevel = ParseLogLevel(cmd.String("log-level"))
	}

	// ===== DNS 配置 =====
	if cmd.IsSet("doh") {
		cfg.DoHUrl = cmd.String("doh")
	}
	if cmd.IsSet("no-doh") && cmd.Bool("no-doh") {
		cfg.EnableDoH = false
	}
	if cmd.IsSet("dns-warmup") && cmd.Bool("dns-warmup") {
		cfg.EnableDNSWarmup = true
	}
	if cmd.IsSet("doh-proxy") && cmd.Bool("doh-proxy") {
		cfg.EnableDoHProxy = true
	}

	// ===== 中转节点配置 =====
	if cmd.IsSet("relay") {
		relayList := cmd.StringSlice("relay")
		if len(relayList) > 0 {
			cfg.RelayIPs = flattenStringSlice(relayList)
		}
	}
	if cmd.IsSet("min-pool") {
		cfg.MinPoolSize = cmd.Int("min-pool")
	}
	if cmd.IsSet("max-pool") {
		cfg.MaxPoolSize = cmd.Int("max-pool")
	}

	// ===== Metrics 配置 =====
	if cmd.IsSet("metrics") && cmd.Bool("metrics") {
		cfg.EnableMetrics = true
	}
	if cmd.IsSet("metrics-port") {
		cfg.MetricsPort = cmd.Int("metrics-port")
	}

	// ===== 连接池配置 =====
	if cmd.IsSet("no-warmup") && cmd.Bool("no-warmup") {
		cfg.EnablePoolWarmup = false
	}
	if cmd.IsSet("no-reconnect") && cmd.Bool("no-reconnect") {
		cfg.EnableAutoReconnect = false
	}
	if cmd.IsSet("no-dynamic-pool") && cmd.Bool("no-dynamic-pool") {
		cfg.EnableDynamicPool = false
	}

	// ===== 日志配置 =====
	if cmd.IsSet("log-file") {
		cfg.EnableLogFile = true
		cfg.LogFilePath = cmd.String("log-file")
	}

	// ===== 隧道配置 =====
	if cmd.IsSet("timeout") {
		cfg.TunnelTimeout = yamlDuration{time.Duration(cmd.Int("timeout")) * time.Second}
	}
	if cmd.IsSet("no-mux") && cmd.Bool("no-mux") {
		cfg.EnableMultiplex = false
	}

	// ===== 高级时间配置 =====
	if cmd.IsSet("connection-ttl") {
		cfg.ConnectionTTL = yamlDuration{cmd.Duration("connection-ttl")}
	}
	if cmd.IsSet("connection-timeout") {
		cfg.ConnectionTimeout = yamlDuration{cmd.Duration("connection-timeout")}
	}
	if cmd.IsSet("heartbeat-interval") {
		cfg.HeartbeatInterval = yamlDuration{cmd.Duration("heartbeat-interval")}
	}
	if cmd.IsSet("heartbeat-timeout") {
		cfg.HeartbeatTimeout = yamlDuration{cmd.Duration("heartbeat-timeout")}
	}
	if cmd.IsSet("dns-cache-ttl") {
		cfg.DNSCacheTTL = yamlDuration{cmd.Duration("dns-cache-ttl")}
	}
	if cmd.IsSet("relay-monitor-interval") {
		cfg.RelayMonitorInterval = yamlDuration{cmd.Duration("relay-monitor-interval")}
	}
	if cmd.IsSet("relay-max-latency") {
		cfg.RelayMaxLatency = yamlDuration{cmd.Duration("relay-max-latency")}
	}

	// ===== 窗口流控配置 =====
	if cmd.IsSet("default-window-size") {
		bytes, err := parseByteSize(cmd.String("default-window-size"))
		if err != nil {
			return fmt.Errorf("无效的默认窗口大小: %w", err)
		}
		cfg.DefaultWindowSize = yamlByteSize{bytes}
	}
	if cmd.IsSet("min-window-size") {
		bytes, err := parseByteSize(cmd.String("min-window-size"))
		if err != nil {
			return fmt.Errorf("无效的最小窗口大小: %w", err)
		}
		cfg.MinWindowSize = yamlByteSize{bytes}
	}
	if cmd.IsSet("max-window-size") {
		bytes, err := parseByteSize(cmd.String("max-window-size"))
		if err != nil {
			return fmt.Errorf("无效的最大窗口大小: %w", err)
		}
		cfg.MaxWindowSize = yamlByteSize{bytes}
	}
	if cmd.IsSet("window-timeout") {
		cfg.WindowTimeout = yamlDuration{cmd.Duration("window-timeout")}
	}

	// ===== 拥塞控制配置 =====
	if cmd.IsSet("congestion-control-interval") {
		cfg.CongestionControlInterval = yamlDuration{cmd.Duration("congestion-control-interval")}
	}

	// ===== ECH 配置 =====
	if cmd.IsSet("enable-ech") {
		cfg.EnableECH = cmd.Bool("enable-ech")
	}

	return nil
}

// flattenStringSlice 将字符串切片中的逗号分隔元素展开。
//
// 例如：["a,b", "c"] -> ["a", "b", "c"]。
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

// splitComma 按逗号分割字符串。
func splitComma(s string) []string {
	return strings.Split(s, ",")
}
