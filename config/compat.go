// 兼容 shim：配置核心已迁移至 xshared（github.com/v2up-32mb/xshared/config）。
// 本文件保留类型别名与转发，使 CLI 层（flags.go / loader.go）零改动。
package config

import (
	"fmt"
	"math"
	"strings"

	xshared "github.com/v2up-32mb/xshared/config"
)

type (
	Config   = xshared.Config
	LogLevel = xshared.LogLevel
)

const (
	DEBUG = xshared.DEBUG
	INFO  = xshared.INFO
	WARN  = xshared.WARN
	ERROR = xshared.ERROR
)

var (
	DefaultConfig = xshared.DefaultConfig
	ParseLogLevel = xshared.ParseLogLevel
	NewDuration   = xshared.NewDuration
	NewByteSize   = xshared.NewByteSize
)

// parseByteSize 解析人类可读的字节大小（"1MB" / "512KB" / "1073741824"）。
// 原 config.go 的包内助手，随核心迁移后保留在 CLI 侧。
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))

	var num float64
	var unit string
	_, err := fmt.Sscanf(s, "%f%s", &num, &unit)
	if err != nil {
		_, err2 := fmt.Sscanf(s, "%f", &num)
		if err2 != nil {
			return 0, fmt.Errorf("无效的字节大小格式: %s", s)
		}
		return int64(num), nil
	}

	var multiplier int64
	switch unit {
	case "B", "":
		multiplier = 1
	case "KB", "K":
		multiplier = 1024
	case "MB", "M":
		multiplier = 1024 * 1024
	case "GB", "G":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("未知的字节单位: %s", unit)
	}

	result := num * float64(multiplier)
	if result > float64(math.MaxInt64) {
		return 0, fmt.Errorf("字节大小超出范围: %s (最大支持 %d 字节)", s, math.MaxInt64)
	}

	return int64(result), nil
}
