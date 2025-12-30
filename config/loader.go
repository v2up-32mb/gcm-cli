package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

var globalConfig *Config

// LoadConfig 加载配置（主入口）
func LoadConfig() (*Config, error) {
	// 先检查是否是 help 参数
	for _, arg := range os.Args {
		if arg == "-h" || arg == "--help" {
			// 显示帮助并退出程序
			app := &cli.App{
				Name:  "gcm",
				Usage: "GCM - Cloudflare Worker Proxy 客户端",
				Flags: DefineFlags(),
			}
			_ = app.Run(os.Args)
			os.Exit(0)
		}
	}

	app := &cli.App{
		Name:  "gcm",
		Usage: "GCM - Cloudflare Worker Proxy 客户端",
		Flags: DefineFlags(),
		Action: func(ctx *cli.Context) error {
			// 1. 从默认配置开始
			cfg := DefaultConfig()

			// 2. 如果指定了配置文件，先加载
			if ctx.IsSet("config") {
				configPath := ctx.String("config")
				fileCfg, err := LoadFile(configPath)
				if err != nil {
					return fmt.Errorf("加载配置文件失败: %w", err)
				}
				// 使用配置文件中的值
				cfg = fileCfg
			}

			// 3. 应用命令行参数覆盖
			if err := ApplyFlags(cfg, ctx); err != nil {
				return err
			}

			// 4. 验证配置
			if cfg.WorkerHost == "" {
				return fmt.Errorf("worker 地址必须指定（通过 --worker 参数或配置文件）")
			}

			// 5. 设置全局配置
			globalConfig = cfg
			return nil
		},
	}

	// 运行 cli 命令
	if err := app.Run(os.Args); err != nil {
		return nil, err
	}

	return globalConfig, nil
}

// LoadFile 从文件加载配置（支持 YAML 和 JSON）
func LoadFile(filepath string) (*Config, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	// 根据文件扩展名选择解析器
	ext := strings.ToLower(getFileExt(filepath))
	cfg := DefaultConfig()

	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("解析 YAML 失败: %w", err)
		}
	case ".json":
		// JSON 兼容性支持
		if err := loadFromJSON(data, cfg); err != nil {
			return nil, fmt.Errorf("解析 JSON 失败: %w", err)
		}
	default:
		// 尝试自动检测：先尝试 YAML，失败则尝试 JSON
		errYAML := yaml.Unmarshal(data, cfg)
		if errYAML == nil {
			return cfg, nil
		}
		errJSON := loadFromJSON(data, cfg)
		if errJSON == nil {
			return cfg, nil
		}
		return nil, fmt.Errorf("无法解析配置文件（尝试了 YAML 和 JSON）: YAML错误=%v, JSON错误=%v", errYAML, errJSON)
	}

	return cfg, nil
}

// getFileExt 获取文件扩展名
func getFileExt(filepath string) string {
	// 手动实现，避免依赖问题
	idx := strings.LastIndex(filepath, ".")
	if idx == -1 {
		return ""
	}
	return filepath[idx:]
}

// getFileDir 获取文件目录
func getFileDir(filepath string) string {
	// 手动实现，避免依赖问题
	idx := strings.LastIndex(filepath, "/")
	if idx == -1 {
		idx = strings.LastIndex(filepath, "\\")
	}
	if idx == -1 {
		return "."
	}
	return filepath[:idx]
}

// loadFromJSON 从 JSON 数据加载配置（向后兼容）
func loadFromJSON(data []byte, cfg *Config) error {
	// 使用 encoding/json 直接解析到结构体
	// 由于保留了 json tags，可以正常工作
	return json.Unmarshal(data, cfg)
}

// LoadYAML 从 YAML 文件加载配置
func LoadYAML(filepath string) (*Config, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析 YAML 失败: %w", err)
	}

	return cfg, nil
}

// SaveYAML 将配置保存为 YAML 文件
func SaveYAML(cfg *Config, filepath string) error {
	// 确保目录存在
	dir := getFileDir(filepath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("创建目录失败: %w", err)
		}
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("序列化 YAML 失败: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}

	return nil
}

// GetDefaultConfigPath 获取默认配置文件路径
func GetDefaultConfigPath() string {
	// 检查当前目录下的 config.yaml
	if _, err := os.Stat("config.yaml"); err == nil {
		return "config.yaml"
	}
	// 检查当前目录下的 gcm.yaml
	if _, err := os.Stat("gcm.yaml"); err == nil {
		return "gcm.yaml"
	}
	// 检查当前目录下的 config.json（向后兼容）
	if _, err := os.Stat("config.json"); err == nil {
		return "config.json"
	}
	return ""
}

// FindConfigFile 在常见位置查找配置文件
func FindConfigFile() string {
	// 查找顺序：config.yaml > gcm.yaml > config.json
	paths := []string{"config.yaml", "gcm.yaml", "config.json"}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// 检查用户主目录
	homeDir, _ := os.UserHomeDir()
	if homeDir != "" {
		configPaths := []string{
			filepath.Join(homeDir, ".config", "gcm", "config.yaml"),
			filepath.Join(homeDir, ".gcm.yaml"),
			filepath.Join(homeDir, ".gcmrc"),
		}
		for _, path := range configPaths {
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
	}

	return ""
}
