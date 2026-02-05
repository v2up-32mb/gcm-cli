# config 包

## 概述

config 包提供了 GCM 代理客户端的配置管理功能。

## 类型定义

### LogLevel

```go
type LogLevel int
```

日志级别类型。

#### 常量

```go
const (
    DEBUG LogLevel = 0
    INFO  LogLevel = 1
    WARN  LogLevel = 2
    ERROR LogLevel = 3
)
```

#### 方法

- `String() string` - 返回日志级别的字符串表示
- `MarshalYAML() (interface{}, error)` - YAML 序列化
- `UnmarshalYAML(unmarshal func(interface{}) error) error` - YAML 反序列化
- `MarshalJSON() ([]byte, error)` - JSON 序列化
- `UnmarshalJSON(data []byte) error` - JSON 反序列化

### Config

```go
type Config struct {
    WorkerHost    string        // Worker 地址
    ListenAddress string        // 监听地址
    LogLevel      LogLevel      // 日志级别
    MinPoolSize   int           // 最小连接池
    MaxPoolSize   int           // 最大连接池
    // ... 更多配置字段
}
```

主配置结构体。

## 函数

### DefaultConfig

```go
func DefaultConfig() *Config
```

返回默认配置实例。

---

## loader.go

### LoadConfig

```go
func LoadConfig() (*Config, error)
```

加载配置（优先级：命令行参数 > 配置文件 > 默认值）。

### mergeConfig

```go
func mergeConfig(base *Config, overrides *Config) *Config
```

合并配置，overrides 优先级更高。

---

## flags.go

### RegisterFlags

```go
func RegisterFlags(cmd *cli.Command)
```

注册命令行参数。

### ParseFlags

```go
func ParseFlags(ctx *cli.Context, cfg *Config)
```

解析命令行参数到配置结构体。
