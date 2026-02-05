# logger 包

## 概述

logger 包提供分级日志系统，支持控制台输出和文件日志（带轮转）。

---

## 类型定义

### Logger

```go
type Logger struct {
    level      config.LogLevel
    scope      string
    fileLogger *FileLogger
}
```

日志器。

### FileLogger

```go
type FileLogger struct {
    enabled      bool
    filePath     string
    maxSize      int64
    backupCount  int
    file         *os.File
    currentSize  int64
    writtenBytes int64
    rotatedCount int
}
```

文件日志写入器，支持自动轮转。

### MultiWriter

```go
type MultiWriter struct {
    writers []io.Writer
}
```

多输出写入器（将日志写入多个目标）。

---

## 构造函数

### NewLogger

```go
func NewLogger(level config.LogLevel, scope string, fileLogger *FileLogger) *Logger
```

创建新的日志器。

### NewFileLogger

```go
func NewFileLogger(cfg *config.Config) *FileLogger
```

创建文件日志写入器。

### NewMultiWriter

```go
func NewMultiWriter(writers ...io.Writer) *MultiWriter
```

创建多输出写入器。

---

## Logger 方法

### SetLevel

```go
func (l *Logger) SetLevel(level config.LogLevel)
```

设置日志级别。

### 日志输出方法

```go
func (l *Logger) Debug(format string, args ...interface{})
func (l *Logger) Info(format string, args ...interface{})
func (l *Logger) Warn(format string, args ...interface{})
func (l *Logger) Error(format string, args ...interface{})
```

输出各级别日志。格式：`[时间] [级别] [作用域] 消息`

---

## FileLogger 方法

### Write

```go
func (fl *FileLogger) Write(msg string)
```

写入日志（自动轮转）。

### GetStats

```go
func (fl *FileLogger) GetStats() map[string]interface{}
```

获取统计信息：
- `enabled` - 是否启用
- `filePath` - 文件路径
- `currentSize` - 当前文件大小
- `writtenBytes` - 总写入字节数
- `rotatedCount` - 轮转次数
- `utilization` - 文件利用率百分比

### Close

```go
func (fl *FileLogger) Close()
```

关闭文件日志。

---

## MultiWriter 方法

### Write

```go
func (mw *MultiWriter) Write(p []byte) (n int, err error)
```

实现 io.Writer 接口，写入所有输出器。

### AddWriter

```go
func (mw *MultiWriter) AddWriter(w io.Writer)
```

添加新的输出器。

---

## 全局函数

### InitGlobalLogger

```go
func InitGlobalLogger(cfg *config.Config)
```

初始化全局日志器。

### GetLogger

```go
func GetLogger(scope string) *Logger
```

获取指定作用域的日志器（单例模式，按 scope 缓存）。

### SetGlobalLevel

```go
func SetGlobalLevel(level config.LogLevel)
```

设置全局日志级别（影响所有已创建的日志器）。

### Close

```go
func Close()
```

关闭全局日志器（关闭文件日志）。
