package config

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// LogLevel 日志级别类型
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

// String 实现 Stringer 接口
func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "INFO"
	}
}

// MarshalYAML 实现 yaml.Marshaler 接口
func (l LogLevel) MarshalYAML() (interface{}, error) {
	return l.String(), nil
}

// UnmarshalYAML 实现 yaml.Unmarshaler 接口
func (l *LogLevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	*l = ParseLogLevel(s)
	return nil
}

// MarshalJSON 实现 json.Marshaler 接口
func (l LogLevel) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}

// UnmarshalJSON 实现 json.Unmarshaler 接口
func (l *LogLevel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*l = ParseLogLevel(s)
	return nil
}

// ParseLogLevel 解析日志级别字符串
func ParseLogLevel(s string) LogLevel {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN":
		return WARN
	case "ERROR":
		return ERROR
	default:
		return INFO
	}
}

// yamlDuration 是 time.Duration 的包装器，支持 YAML 中的字符串格式
type yamlDuration struct {
	time.Duration
}

// MarshalYAML 实现 yaml.Marshaler 接口
func (yd yamlDuration) MarshalYAML() (interface{}, error) {
	return yd.Duration.String(), nil
}

// UnmarshalYAML 实现 yaml.Unmarshaler 接口
func (yd *yamlDuration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var v interface{}
	if err := unmarshal(&v); err != nil {
		return err
	}

	switch value := v.(type) {
	case float64:
		// JSON 数字格式（毫秒）
		yd.Duration = time.Duration(value) * time.Millisecond
	case int:
		// JSON 整数格式（毫秒）
		yd.Duration = time.Duration(value) * time.Millisecond
	case string:
		// YAML 字符串格式（如 "5m", "1s"）
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("无法解析持续时间: %q: %w", value, err)
		}
		yd.Duration = d
	default:
		return fmt.Errorf("无效的持续时间类型: %T", v)
	}

	return nil
}

// Config 应用配置
type Config struct {
	// 基本配置
	WorkerHost    string   `yaml:"workerHost" json:"workerHost"`
	ListenAddress string   `yaml:"listenAddress" json:"listenAddress"` // 监听地址，如 ":10080" 或 "0.0.0.0:10080"
	UserID        string   `yaml:"userID,omitempty" json:"userID,omitempty"`
	LogLevel      LogLevel `yaml:"logLevel" json:"logLevel"`

	// 连接池配置
	MinPoolSize       int          `yaml:"minPoolSize" json:"minPoolSize"`
	MaxPoolSize       int          `yaml:"maxPoolSize" json:"maxPoolSize"`
	ConnectionTTL     yamlDuration `yaml:"connectionTTL" json:"connectionTTL"`
	ConnectionTimeout yamlDuration `yaml:"connectionTimeout" json:"connectionTimeout"`

	// 中转节点配置
	RelayIPs                  []string     `yaml:"relayIPs" json:"relayIPs"`
	RelayMonitorInterval      yamlDuration `yaml:"relayMonitorInterval" json:"relayMonitorInterval"`
	RelayMaxLatency           yamlDuration `yaml:"relayMaxLatency" json:"relayMaxLatency"`
	RelayFailureThreshold     int          `yaml:"relayFailureThreshold" json:"relayFailureThreshold"`
	RelayRescoreInterval      yamlDuration `yaml:"relayRescoreInterval" json:"relayRescoreInterval"`
	RelayForceRescoreCooldown yamlDuration `yaml:"relayForceRescoreCooldown" json:"relayForceRescoreCooldown"`

	// DNS 缓存配置
	EnableDoH               bool         `yaml:"enableDoH" json:"enableDoH"`
	DoHUrl                  string       `yaml:"dohUrl" json:"dohUrl"`
	DNSCacheTTL             yamlDuration `yaml:"dnsCacheTTL" json:"dnsCacheTTL"`
	DNSCacheCleanupInterval yamlDuration `yaml:"dnsCacheCleanupInterval" json:"dnsCacheCleanupInterval"`
	EnableDNSWarmup         bool         `yaml:"enableDNSWarmup" json:"enableDNSWarmup"`
	DNSWarmupDomains        []string     `yaml:"dnsWarmupDomains" json:"dnsWarmupDomains"`
	EnableDoHProxy          bool         `yaml:"enableDoHProxy" json:"enableDoHProxy"`

	// 心跳保活配置
	HeartbeatInterval yamlDuration `yaml:"heartbeatInterval" json:"heartbeatInterval"`
	HeartbeatTimeout  yamlDuration `yaml:"heartbeatTimeout" json:"heartbeatTimeout"`
	EnableTcpNoDelay  bool         `yaml:"enableTcpNoDelay" json:"enableTcpNoDelay"`

	// Metrics 配置
	EnableMetrics bool `yaml:"enableMetrics" json:"enableMetrics"`
	MetricsPort   int  `yaml:"metricsPort" json:"metricsPort"`

	// 连接池预热配置
	EnablePoolWarmup  bool         `yaml:"enablePoolWarmup" json:"enablePoolWarmup"`
	WarmupConcurrency int          `yaml:"warmupConcurrency" json:"warmupConcurrency"`
	WarmupTimeout     yamlDuration `yaml:"warmupTimeout" json:"warmupTimeout"`

	// 断线重连配置
	EnableAutoReconnect  bool         `yaml:"enableAutoReconnect" json:"enableAutoReconnect"`
	MaxReconnectAttempts int          `yaml:"maxReconnectAttempts" json:"maxReconnectAttempts"`
	ReconnectDelay       yamlDuration `yaml:"reconnectDelay" json:"reconnectDelay"`

	// 请求超时配置
	TunnelTimeout yamlDuration `yaml:"tunnelTimeout" json:"tunnelTimeout"`

	// 连接池动态调整配置
	EnableDynamicPool        bool         `yaml:"enableDynamicPool" json:"enableDynamicPool"`
	DynamicPoolInterval      yamlDuration `yaml:"dynamicPoolInterval" json:"dynamicPoolInterval"`
	DynamicPoolMinSize       int          `yaml:"dynamicPoolMinSize" json:"dynamicPoolMinSize"`
	DynamicPoolMaxSize       int          `yaml:"dynamicPoolMaxSize" json:"dynamicPoolMaxSize"`
	DynamicPoolLowThreshold  float64      `yaml:"dynamicPoolLowThreshold" json:"dynamicPoolLowThreshold"`
	DynamicPoolHighThreshold float64      `yaml:"dynamicPoolHighThreshold" json:"dynamicPoolHighThreshold"`

	// 日志文件配置
	EnableLogFile      bool   `yaml:"enableLogFile" json:"enableLogFile"`
	LogFilePath        string `yaml:"logFilePath" json:"logFilePath"`
	LogFileMaxSize     int64  `yaml:"logFileMaxSize" json:"logFileMaxSize"`
	LogFileBackupCount int    `yaml:"logFileBackupCount" json:"logFileBackupCount"`

	// 统计增强配置
	EnableStats bool `yaml:"enableStats" json:"enableStats"`

	// 多路复用配置
	EnableMultiplex         bool `yaml:"enableMultiplex" json:"enableMultiplex"`
	MaxStreamsPerConnection int  `yaml:"maxStreamsPerConnection" json:"maxStreamsPerConnection"`
}

// GetConnectionTTL 返回连接 TTL 的 time.Duration 值
func (c *Config) GetConnectionTTL() time.Duration {
	return c.ConnectionTTL.Duration
}

// GetConnectionTimeout 返回连接超时的 time.Duration 值
func (c *Config) GetConnectionTimeout() time.Duration {
	return c.ConnectionTimeout.Duration
}

// GetRelayMonitorInterval 返回节点监控间隔的 time.Duration 值
func (c *Config) GetRelayMonitorInterval() time.Duration {
	return c.RelayMonitorInterval.Duration
}

// GetRelayMaxLatency 返回节点最大延迟的 time.Duration 值
func (c *Config) GetRelayMaxLatency() time.Duration {
	return c.RelayMaxLatency.Duration
}

// GetRelayRescoreInterval 返回节点重评间隔的 time.Duration 值
func (c *Config) GetRelayRescoreInterval() time.Duration {
	return c.RelayRescoreInterval.Duration
}

// GetRelayForceRescoreCooldown 返回强制重评冷却时间的 time.Duration 值
func (c *Config) GetRelayForceRescoreCooldown() time.Duration {
	return c.RelayForceRescoreCooldown.Duration
}

// GetDNSCacheTTL 返回 DNS 缓存 TTL 的 time.Duration 值
func (c *Config) GetDNSCacheTTL() time.Duration {
	return c.DNSCacheTTL.Duration
}

// GetDNSCacheCleanupInterval 返回 DNS 缓存清理间隔的 time.Duration 值
func (c *Config) GetDNSCacheCleanupInterval() time.Duration {
	return c.DNSCacheCleanupInterval.Duration
}

// GetHeartbeatInterval 返回心跳间隔的 time.Duration 值
func (c *Config) GetHeartbeatInterval() time.Duration {
	return c.HeartbeatInterval.Duration
}

// GetHeartbeatTimeout 返回心跳超时的 time.Duration 值
func (c *Config) GetHeartbeatTimeout() time.Duration {
	return c.HeartbeatTimeout.Duration
}

// GetWarmupTimeout 返回预热超时的 time.Duration 值
func (c *Config) GetWarmupTimeout() time.Duration {
	return c.WarmupTimeout.Duration
}

// GetReconnectDelay 返回重连延迟的 time.Duration 值
func (c *Config) GetReconnectDelay() time.Duration {
	return c.ReconnectDelay.Duration
}

// GetTunnelTimeout 返回隧道超时的 time.Duration 值
func (c *Config) GetTunnelTimeout() time.Duration {
	return c.TunnelTimeout.Duration
}

// GetDynamicPoolInterval 返回动态池调整间隔的 time.Duration 值
func (c *Config) GetDynamicPoolInterval() time.Duration {
	return c.DynamicPoolInterval.Duration
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		// 基本配置
		WorkerHost:    "", // 必须通过参数或配置文件指定
		ListenAddress: ":10080",
		UserID:        "",
		LogLevel:      INFO,

		// 连接池配置
		MinPoolSize:       5,
		MaxPoolSize:       15,
		ConnectionTTL:     yamlDuration{5 * time.Minute},
		ConnectionTimeout: yamlDuration{time.Second},

		// 中转节点配置
		RelayIPs:                  []string{"36.140.124.162:10009", "v6.gh-proxy.org"},
		RelayMonitorInterval:      yamlDuration{30 * time.Second},
		RelayMaxLatency:           yamlDuration{500 * time.Millisecond},
		RelayFailureThreshold:     3,
		RelayRescoreInterval:      yamlDuration{10 * time.Minute},
		RelayForceRescoreCooldown: yamlDuration{time.Minute},

		// DNS 缓存配置
		EnableDoH:               true,
		DoHUrl:                  "https://v.recipes/dns-query",
		DNSCacheTTL:             yamlDuration{5 * time.Minute},
		DNSCacheCleanupInterval: yamlDuration{time.Minute},
		EnableDNSWarmup:         false,
		DNSWarmupDomains:        []string{},
		EnableDoHProxy:          false,

		// 心跳保活配置
		HeartbeatInterval: yamlDuration{15 * time.Second},
		HeartbeatTimeout:  yamlDuration{3 * time.Second},
		EnableTcpNoDelay:  true,

		// Metrics 配置
		EnableMetrics: false,
		MetricsPort:   9090,

		// 连接池预热配置
		EnablePoolWarmup:  true,
		WarmupConcurrency: 3,
		WarmupTimeout:     yamlDuration{30 * time.Second},

		// 断线重连配置
		EnableAutoReconnect:  true,
		MaxReconnectAttempts: 3,
		ReconnectDelay:       yamlDuration{time.Second},

		// 请求超时配置
		TunnelTimeout: yamlDuration{time.Minute},

		// 连接池动态调整配置
		EnableDynamicPool:        true,
		DynamicPoolInterval:      yamlDuration{time.Minute},
		DynamicPoolMinSize:       5,
		DynamicPoolMaxSize:       15,
		DynamicPoolLowThreshold:  0.3,
		DynamicPoolHighThreshold: 0.8,

		// 日志文件配置
		EnableLogFile:      false,
		LogFilePath:        "./gcm.log",
		LogFileMaxSize:     10 * 1024 * 1024,
		LogFileBackupCount: 3,

		// 统计增强配置
		EnableStats: true,

		// 多路复用配置
		EnableMultiplex:         true,
		MaxStreamsPerConnection: 5,
	}
}
