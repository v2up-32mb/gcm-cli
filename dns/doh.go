package dns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gcm/gcm/config"
	"github.com/gcm/gcm/logger"
)

// DoHClient DNS over HTTPS 客户端
type DoHClient struct {
	dohURL  string
	client  *http.Client
	enabled bool
	log     *logger.Logger
}

// DoHResponse DoH 响应结构
type DoHResponse struct {
	Status   int  `json:"Status"`
	TC       bool `json:"TC"`
	RD       bool `json:"RD"`
	RA       bool `json:"RA"`
	AD       bool `json:"AD"`
	CD       bool `json:"CD"`
	Question []struct {
		Name string `json:"name"`
		Type int    `json:"type"`
	} `json:"Question"`
	Answer []struct {
		Name string `json:"name"`
		Type int    `json:"type"`
		Data string `json:"data"`
	} `json:"Answer"`
}

// NewDoHClient 创建 DoH 客户端
func NewDoHClient(cfg *config.Config) *DoHClient {
	return &DoHClient{
		dohURL:  cfg.DoHUrl,
		enabled: cfg.EnableDoH,
		client: &http.Client{
			Timeout: time.Second, // 1秒超时，快速失败
		},
		log: logger.GetLogger("DoH"),
	}
}

// EnableProxy 启用代理模式
// proxyTransport 应该是 pool.ProxyTransport 实例
func (d *DoHClient) EnableProxy(proxyTransport http.RoundTripper) {
	d.client.Transport = proxyTransport
	d.log.Info("DoH 客户端已启用代理模式")
}

// Resolve 解析域名（A 记录或 AAAA 记录）
func (d *DoHClient) Resolve(domain string, queryType string) (string, error) {
	if !d.enabled {
		d.log.Debug("DoH 未启用，跳过解析: %s (%s)", domain, queryType)
		return "", fmt.Errorf("DoH 未启用")
	}

	// 重试机制：TLS 握手可能因数据交错而失败
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			d.log.Debug("DoH 重试 %d/%d: %s (%s)", attempt, maxRetries-1, domain, queryType)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}

		result, err := d.resolveAttempt(domain, queryType)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// 如果是 TLS 错误或连接错误，继续重试
		if strings.Contains(err.Error(), "TLS") || strings.Contains(err.Error(), "connection") || strings.Contains(err.Error(), "EOF") {
			continue
		}

		// 其他错误直接返回
		break
	}

	return "", lastErr
}

// resolveAttempt 单次解析尝试
func (d *DoHClient) resolveAttempt(domain string, queryType string) (string, error) {
	startTime := time.Now()

	// 构建请求 URL
	reqURL, err := url.Parse(d.dohURL)
	if err != nil {
		return "", fmt.Errorf("解析 DoH URL 失败: %w", err)
	}

	// Google DoH 特殊处理：将 /dns-query 替换为 /resolve（JSON API）
	// Google 的 /dns-query 是 RFC8484 格式，/resolve 才是 JSON 格式
	if strings.Contains(reqURL.Host, "dns.google") || strings.Contains(reqURL.Host, "google.com") {
		if reqURL.Path == "/dns-query" || reqURL.Path == "" {
			reqURL.Path = "/resolve"
			d.log.Debug("检测到 Google DoH，使用 JSON API: %s", reqURL.String())
		}
	}

	// 添加查询参数
	q := reqURL.Query()
	q.Set("name", domain)
	q.Set("type", queryType)
	reqURL.RawQuery = q.Encode()

	d.log.Debug("正在解析: %s (%s) via %s", domain, queryType, reqURL.String())

	// 创建请求
	req, err := http.NewRequest("GET", reqURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Accept", "application/dns-json")

	// 发送请求
	resp, err := d.client.Do(req)
	if err != nil {
		elapsed := time.Since(startTime)
		d.log.Debug("请求错误: %v, 耗时%dms", err, elapsed.Milliseconds())
		return "", err
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		elapsed := time.Since(startTime)
		d.log.Debug("读取响应失败: %v, 耗时%dms", err, elapsed.Milliseconds())
		return "", err
	}

	// 解析 JSON
	var dohResp DoHResponse
	if err := json.Unmarshal(body, &dohResp); err != nil {
		elapsed := time.Since(startTime)
		d.log.Debug("解析响应失败: %v, 耗时%dms", err, elapsed.Milliseconds())
		return "", err
	}

	elapsed := time.Since(startTime)

	// 查找答案
	recordType := 1 // A 记录
	if queryType == "AAAA" {
		recordType = 28 // AAAA 记录
	}

	if len(dohResp.Answer) > 0 {
		for _, ans := range dohResp.Answer {
			if ans.Type == recordType {
				d.log.Debug("解析成功: %s -> %s (%s), 耗时%dms", domain, ans.Data, queryType, elapsed.Milliseconds())
				return ans.Data, nil
			}
		}
	}

	d.log.Debug("解析无结果: %s (%s), 耗时%dms", domain, queryType, elapsed.Milliseconds())
	return "", fmt.Errorf("无解析结果")
}

// ResolveA 解析 A 记录 (IPv4)
func (d *DoHClient) ResolveA(domain string) (string, error) {
	return d.Resolve(domain, "A")
}

// ResolveAAAA 解析 AAAA 记录 (IPv6)
func (d *DoHClient) ResolveAAAA(domain string) (string, error) {
	return d.Resolve(domain, "AAAA")
}

// IsIPv6 检查是否为 IPv6 地址
func IsIPv6(ip string) bool {
	return net.ParseIP(ip).To4() == nil
}

// FormatIPv6 格式化 IPv6 地址（添加方括号）
func FormatIPv6(ip string) string {
	if IsIPv6(ip) && !bytes.HasPrefix([]byte(ip), []byte("[")) {
		return fmt.Sprintf("[%s]", ip)
	}
	return ip
}

// LookupIP 标准 DNS 查询（本地 DNS 解析）
func LookupIP(host string) ([]string, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(ips))
	for _, ip := range ips {
		// 优先返回 IPv4
		if ip4 := ip.To4(); ip4 != nil {
			result = append(result, ip4.String())
		}
	}
	// 再添加 IPv6
	for _, ip := range ips {
		if ip.To4() == nil {
			result = append(result, ip.String())
		}
	}

	return result, nil
}
