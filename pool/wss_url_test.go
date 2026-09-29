package pool

import (
	"testing"

	"github.com/gcm/gcm/config"
)

// buildWSSURL 拼出的 query 是 Worker 的对外契约（?fallbackip= / ?proxy-all=），
// 参数名或拼接方式变了，Worker 侧就读不到出口偏好了。
func TestBuildWSSURL出口参数(t *testing.T) {
	tests := []struct {
		name     string
		proxyIP  string
		proxyAll bool
		want     string
	}{
		{"无出口参数", "", false, "wss://worker.example/uid"},
		{"仅 proxyIP", "1.2.3.4", false, "wss://worker.example/uid?fallbackip=1.2.3.4"},
		{"仅 proxyAll（无 fallbackip：Worker 侧会零拨号直接关流）", "", true, "wss://worker.example/uid?proxy-all=true"},
		{"proxyIP + proxyAll", "1.2.3.4:8443", true, "wss://worker.example/uid?fallbackip=1.2.3.4:8443&proxy-all=true"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &ConnectionPool{cfg: &config.Config{
				WorkerHost: "worker.example",
				UserID:     "uid",
				ProxyIP:    tc.proxyIP,
				ProxyAll:   tc.proxyAll,
			}}
			if got := p.buildWSSURL(); got != tc.want {
				t.Fatalf("buildWSSURL() = %q, want %q", got, tc.want)
			}
		})
	}
}
