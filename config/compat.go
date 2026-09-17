// 兼容 shim：配置核心已迁移至 xshared（github.com/v2up-32mb/xshared/config）。
// 本文件保留类型别名与转发，使 CLI 层（flags.go / loader.go）零改动。
package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
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

// ProxyOverrides bypass 分流与 HTTP 监听的 CLI 侧配置。
// 不进入 xshared Config（bypass 由 SOCKS5/HTTP 服务器 Option 注入；
// xshared Config 无对应字段且 Config 为类型别名不可扩展）。
// flags.go 经 urfave/cli Destination 直接绑定到此变量。
type ProxyOverrides struct {
	BypassPrivate   bool
	BypassGeoIPCN   bool
	BypassGeoSiteCN bool
	BypassRules     string
	HTTPListen      string // 可选 HTTP 代理监听地址（空 = 不启用）
	GeoIPPath       string // geoip.dat 路径（空 = 自动探测可执行文件同目录）
	GeoSitePath     string // geosite.dat 路径（空 = 自动探测可执行文件同目录）
}

// Overrides 全局覆盖项（flags 定义时经 Destination 绑定，Action 阶段即已就绪）
var Overrides ProxyOverrides

// ResolveGeoPaths 解析 geoip.dat/geosite.dat 路径：显式 flag 优先；
// 留空时探测可执行文件同目录（v2ray 生态惯例），文件不存在返回空（库内静默回退内置数据）。
// 返回 (geoipPath, geoSitePath, foundGeoip, foundGeosite)。
func ResolveGeoPaths() (string, string, bool, bool) {
	exeDir := ""
	if exe, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(exe)
	}
	join := func(flagVal, name string) (string, bool) {
		if p := strings.TrimSpace(flagVal); p != "" {
			_, err := os.Stat(p)
			return p, err == nil
		}
		if exeDir != "" {
			p := filepath.Join(exeDir, name)
			if _, err := os.Stat(p); err == nil {
				return p, true
			}
		}
		return "", false
	}
	gi, giFound := join(Overrides.GeoIPPath, "geoip.dat")
	gs, gsFound := join(Overrides.GeoSitePath, "geosite.dat")
	return gi, gs, giFound, gsFound
}

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
