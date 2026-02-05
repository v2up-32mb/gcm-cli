// Package logger 提供分级日志系统和文件日志输出功能。
//
// 日志级别：
//   - DEBUG: 最详细的调试信息
//   - INFO: 常规运行信息
//   - WARN: 警告信息
//   - ERROR: 错误信息
//
// 功能：
//   - 控制台输出（彩色标记）
//   - 文件日志（支持自动轮转）
//   - 按作用域分组的日志器
//   - 全局日志器管理
package logger

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"gcm/config"
)

// Logger 提供带作用域的分级日志记录功能。
type Logger struct {
	mu         sync.RWMutex
	level      config.LogLevel
	scope      string
	fileLogger *FileLogger
}

// NewLogger 创建新的日志器。
//
// 参数：
//   - level: 日志级别
//   - scope: 作用域名称（用于日志前缀）
//   - fileLogger: 文件日志写入器（可选）
func NewLogger(level config.LogLevel, scope string, fileLogger *FileLogger) *Logger {
	return &Logger{
		level:      level,
		scope:      scope,
		fileLogger: fileLogger,
	}
}

// SetLevel 设置日志级别。
func (l *Logger) SetLevel(level config.LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// log 内部日志方法
func (l *Logger) log(level config.LogLevel, format string, args ...interface{}) {
	l.mu.RLock()
	if level < l.level {
		l.mu.RUnlock()
		return
	}
	l.mu.RUnlock()

	var levelTag string
	switch level {
	case config.DEBUG:
		levelTag = "D"
	case config.INFO:
		levelTag = "I"
	case config.WARN:
		levelTag = "W"
	case config.ERROR:
		levelTag = "E"
	default:
		levelTag = "I"
	}

	timestamp := time.Now().Format("15:04:05")
	msg := fmt.Sprintf(format, args...)
	logMsg := fmt.Sprintf("[%s] [%s] [%s] %s", timestamp, levelTag, l.scope, msg)

	// 输出到控制台
	fmt.Println(logMsg)

	// 写入文件
	if l.fileLogger != nil {
		l.fileLogger.Write(logMsg)
	}
}

// Debug 输出 DEBUG 级别日志。
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(config.DEBUG, format, args...)
}

// Info 输出 INFO 级别日志。
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(config.INFO, format, args...)
}

// Warn 输出 WARN 级别日志。
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(config.WARN, format, args...)
}

// Error 输出 ERROR 级别日志。
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(config.ERROR, format, args...)
}

// FileLogger 提供文件日志写入功能，支持自动轮转。
//
// 轮转策略：当文件大小达到 maxSize 时，将当前文件重命名为 .1，
// .1 -> .2，依此类推，超过 backupCount 的备份会被删除。
type FileLogger struct {
	mu           sync.Mutex
	enabled      bool
	filePath     string  // 日志文件路径
	maxSize      int64   // 单个日志文件最大大小（字节）
	backupCount  int     // 保留的备份文件数量
	file         *os.File
	currentSize  int64   // 当前文件大小（字节）
	writtenBytes int64   // 累计写入字节数
	rotatedCount int     // 累计轮转次数
}

// NewFileLogger 创建文件日志写入器。
//
// 如果 cfg.EnableLogFile 为 false，返回禁用的 FileLogger。
func NewFileLogger(cfg *config.Config) *FileLogger {
	if !cfg.EnableLogFile {
		return &FileLogger{enabled: false}
	}

	fl := &FileLogger{
		enabled:      true,
		filePath:     cfg.LogFilePath,
		maxSize:      cfg.LogFileMaxSize,
		backupCount:  cfg.LogFileBackupCount,
		currentSize:  0,
		writtenBytes: 0,
		rotatedCount: 0,
	}

	if err := fl.init(); err != nil {
		fmt.Printf("[FileLogger] 初始化失败: %v\n", err)
		fl.enabled = false
		return fl
	}

	fmt.Printf("[FileLogger] 文件日志已启用: %s (最大%.1fMB, 保留%d个备份)\n",
		fl.filePath, float64(fl.maxSize)/1024/1024, fl.backupCount)

	return fl
}

// init 初始化文件日志
func (fl *FileLogger) init() error {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// 检查并处理日志文件轮转
	if info, err := os.Stat(fl.filePath); err == nil {
		fl.currentSize = info.Size()
		if fl.currentSize >= fl.maxSize {
			fmt.Printf("[FileLogger] 初始化时检测到日志文件已满 (%dKB)，执行轮转\n", fl.currentSize/1024)
			fl.rotate()
		}
	}

	// 打开文件（追加模式）
	file, err := os.OpenFile(fl.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	fl.file = file

	return nil
}

// rotate 执行日志轮转
func (fl *FileLogger) rotate() {
	startTime := time.Now()
	fl.rotatedCount++

	// 删除最老的备份
	oldestBackup := fmt.Sprintf("%s.%d", fl.filePath, fl.backupCount)
	if _, err := os.Stat(oldestBackup); err == nil {
		os.Remove(oldestBackup)
		fmt.Printf("[FileLogger] 已删除最老备份: %s\n", oldestBackup)
	}

	// 轮转现有备份
	rotated := 0
	for i := fl.backupCount - 1; i >= 1; i-- {
		currentBackup := fmt.Sprintf("%s.%d", fl.filePath, i)
		nextBackup := fmt.Sprintf("%s.%d", fl.filePath, i+1)
		if _, err := os.Stat(currentBackup); err == nil {
			os.Rename(currentBackup, nextBackup)
			rotated++
		}
	}

	// 将当前日志文件重命名为 .1
	if fl.file != nil {
		fl.file.Close()
	}
	if _, err := os.Stat(fl.filePath); err == nil {
		os.Rename(fl.filePath, fmt.Sprintf("%s.1", fl.filePath))
	}

	// 重新打开文件
	file, err := os.OpenFile(fl.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("[FileLogger] 轮转后重新打开文件失败: %v\n", err)
		return
	}
	fl.file = file
	fl.currentSize = 0

	elapsed := time.Since(startTime)
	fmt.Printf("[FileLogger] 日志轮转完成，耗时%dms，轮转%d个备份文件\n", elapsed.Milliseconds(), rotated)
}

// Write 写入日志（如果已启用）。
//
// 写入前会检查文件大小，超过 maxSize 时自动触发轮转。
func (fl *FileLogger) Write(msg string) {
	if !fl.enabled || fl.file == nil {
		return
	}

	fl.mu.Lock()
	defer fl.mu.Unlock()

	logLine := msg + "\n"
	lineSize := int64(len(logLine))
	fl.currentSize += lineSize
	fl.writtenBytes += lineSize

	// 检查是否需要轮转
	if fl.currentSize >= fl.maxSize {
		fl.rotate()
	}

	fl.file.WriteString(logLine)
}

// GetStats 获取统计信息。
//
// 返回包含以下字段的 map：
//   - enabled: 是否启用
//   - filePath: 日志文件路径
//   - currentSize: 当前文件大小（字节）
//   - writtenBytes: 累计写入字节数
//   - rotatedCount: 累计轮转次数
//   - utilization: 当前文件利用率（百分比）
func (fl *FileLogger) GetStats() map[string]interface{} {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	utilization := 0.0
	if fl.maxSize > 0 {
		utilization = float64(fl.currentSize) / float64(fl.maxSize) * 100
	}

	return map[string]interface{}{
		"enabled":      fl.enabled,
		"filePath":     fl.filePath,
		"currentSize":  fl.currentSize,
		"writtenBytes": fl.writtenBytes,
		"rotatedCount": fl.rotatedCount,
		"utilization":  fmt.Sprintf("%.1f%%", utilization),
	}
}

// Close 关闭文件日志并输出统计信息。
func (fl *FileLogger) Close() {
	if fl.file != nil {
		stats := fl.GetStats()
		fmt.Printf("[FileLogger] 关闭文件日志: 总写入%.1fKB，轮转%d次\n",
			float64(stats["writtenBytes"].(int64))/1024, stats["rotatedCount"].(int))
		fl.file.Close()
		fl.file = nil
	}
}

// MultiWriter 多输出写入器，将日志同时写入多个目标。
type MultiWriter struct {
	writers []io.Writer
	mu      sync.Mutex
}

// NewMultiWriter 创建多输出写入器。
func NewMultiWriter(writers ...io.Writer) *MultiWriter {
	return &MultiWriter{
		writers: writers,
	}
}

// Write 实现 io.Writer 接口
func (mw *MultiWriter) Write(p []byte) (n int, err error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	for _, w := range mw.writers {
		w.Write(p)
	}
	return len(p), nil
}

// AddWriter 添加写入器
func (mw *MultiWriter) AddWriter(w io.Writer) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.writers = append(mw.writers, w)
}

// GlobalLogger 全局日志器实例
var (
	globalLevel      config.LogLevel = config.INFO
	globalFileLogger *FileLogger
	loggerMap        = make(map[string]*Logger)
	loggerMapMu      sync.RWMutex
)

// InitGlobalLogger 初始化全局日志器。
//
// 根据 cfg 创建全局文件日志器并设置全局日志级别。
func InitGlobalLogger(cfg *config.Config) {
	globalLevel = cfg.LogLevel
	globalFileLogger = NewFileLogger(cfg)
}

// GetLogger 获取指定作用域的日志器（单例模式）。
//
// 按作用域缓存日志器实例，相同作用域返回同一实例。
func GetLogger(scope string) *Logger {
	loggerMapMu.RLock()
	if logger, ok := loggerMap[scope]; ok {
		loggerMapMu.RUnlock()
		return logger
	}
	loggerMapMu.RUnlock()

	loggerMapMu.Lock()
	defer loggerMapMu.Unlock()

	// 再次检查，防止并发创建
	if logger, ok := loggerMap[scope]; ok {
		return logger
	}

	logger := NewLogger(globalLevel, scope, globalFileLogger)
	loggerMap[scope] = logger
	return logger
}

// SetGlobalLevel 设置全局日志级别（影响所有已创建的日志器）。
func SetGlobalLevel(level config.LogLevel) {
	loggerMapMu.Lock()
	defer loggerMapMu.Unlock()

	globalLevel = level
	for _, logger := range loggerMap {
		logger.SetLevel(level)
	}
}

// Close 关闭全局日志器（关闭文件日志）。
func Close() {
	if globalFileLogger != nil {
		globalFileLogger.Close()
	}
}
