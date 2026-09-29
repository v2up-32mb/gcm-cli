package config

import (
	"context"
	"testing"

	"github.com/urfave/cli/v3"
)

// runFlags 走一遍真实的 urfave/cli 参数解析 → ApplyFlags，返回落进 Config 的结果。
// 出口参数是 gcm-worker 的对外契约（?fallbackip= / ?proxy-all=），
// flag 名或映射写错都会静默退化成「仍先直连」，故在此锁住。
func runFlags(t *testing.T, args ...string) *Config {
	t.Helper()
	cfg := &Config{}
	cmd := &cli.Command{
		Name:  "gcm",
		Flags: DefineFlags(),
		Action: func(ctx context.Context, c *cli.Command) error {
			return ApplyFlags(cfg, ctx, c)
		},
	}
	if err := cmd.Run(context.Background(), append([]string{"gcm"}, args...)); err != nil {
		t.Fatalf("Run(%v) 报错: %v", args, err)
	}
	return cfg
}

func Test出口Flag映射(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantProxy string
		wantAll   bool
	}{
		{"缺省：两个都不设", []string{}, "", false},
		{"仅 proxy-ip", []string{"--proxy-ip", "1.2.3.4:8443"}, "1.2.3.4:8443", false},
		{"仅 proxy-all（出口池交由 Worker 侧提供）", []string{"--proxy-all"}, "", true},
		{
			"proxy-ip + proxy-all（典型强制走出口组合）",
			[]string{"--proxy-ip", "1.2.3.4:8443", "--proxy-all"}, "1.2.3.4:8443", true,
		},
		{"显式 --proxy-all=false 关闭", []string{"--proxy-ip", "1.2.3.4", "--proxy-all=false"}, "1.2.3.4", false},
		{"短名 -p", []string{"-p", "9.9.9.9"}, "9.9.9.9", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := runFlags(t, tc.args...)
			if cfg.ProxyIP != tc.wantProxy {
				t.Errorf("ProxyIP = %q, want %q", cfg.ProxyIP, tc.wantProxy)
			}
			if cfg.ProxyAll != tc.wantAll {
				t.Errorf("ProxyAll = %v, want %v", cfg.ProxyAll, tc.wantAll)
			}
		})
	}
}
