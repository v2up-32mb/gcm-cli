package dns

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gcm/config"
	"gcm/logger"
	"golang.org/x/net/dns/dnsmessage"
)

// DNS 记录类型常量
const (
	RecordTypeA     = 1
	RecordTypeAAAA  = 28
	RecordTypeHTTPS = 65
)

// HTTPSRecord HTTPS 记录结构
type HTTPSRecord struct {
	Priority int
	Target   string
	Params   map[string]string
	ECH      []byte // ECH 配置信息 (已解码)
	raw      []byte // 原始记录数据
}

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

// Resolve 解析域名（支持 A/AAAA/HTTPS 记录）
func (d *DoHClient) Resolve(domain string, queryType string) (string, error) {
	if !d.enabled {
		d.log.Debug("DoH 未启用，跳过解析: %s (%s)", domain, queryType)
		return "", fmt.Errorf("DoH 未启用")
	}

	// 重试机制：TLS 握手可能因数据交错而失败
	maxRetries := 3
	var lastErr error
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 检查 Context 是否已取消
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		if attempt > 0 {
			d.log.Debug("DoH 重试 %d/%d: %s (%s)", attempt, maxRetries-1, domain, queryType)
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt*100) * time.Millisecond):
			}
		}

		result, err := d.resolveAttempt(ctx, domain, queryType)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// 如果是 TLS 错误或连接错误，继续重试
		errStr := err.Error()
		if strings.Contains(errStr, "TLS") || strings.Contains(errStr, "connection") || strings.Contains(errStr, "EOF") {
			continue
		}

		// 其他错误直接返回
		break
	}

	return "", lastErr
}

// resolveAttempt 单次解析尝试（优先 RFC 8484，失败时回退到 JSON API）
func (d *DoHClient) resolveAttempt(ctx context.Context, domain string, queryType string) (string, error) {
	// 优先尝试 RFC 8484 (Standard DoH)
	// 这提供了最好的兼容性，支持 Aliyun, DNSPod, Cloudflare, Google 等
	res, err := d.resolveRFC8484(ctx, domain, queryType)
	if err == nil {
		return res, nil
	}

	// 如果 RFC 8484 失败，且不是上下文取消导致的，尝试 JSON API (Google/Cloudflare style)
	// 主要作为回退，或者针对仅支持 JSON 的旧服务
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	// JSON API fallback
	return d.resolveJSON(ctx, domain, queryType)
}

// resolveRFC8484 使用 RFC 8484 标准 (application/dns-message) 解析
func (d *DoHClient) resolveRFC8484(ctx context.Context, domain string, queryTypeStr string) (string, error) {
	// 转换查询类型
	var qType dnsmessage.Type
	switch queryTypeStr {
	case "A":
		qType = dnsmessage.TypeA
	case "AAAA":
		qType = dnsmessage.TypeAAAA
	case "HTTPS":
		qType = dnsmessage.Type(65)
	default:
		qType = dnsmessage.TypeA
	}

	// 构造 DNS 查询消息
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:                 0,
		Response:           false,
		OpCode:             0,
		Authoritative:      false,
		Truncated:          false,
		RecursionDesired:   true,
		RecursionAvailable: false,
		RCode:              dnsmessage.RCodeSuccess,
	})
	b.EnableCompression()

	if err := b.StartQuestions(); err != nil {
		return "", err
	}

	// 构造域名（防止非法域名导致 panic）
	name, err := dnsmessage.NewName(domain + ".")
	if err != nil {
		return "", fmt.Errorf("invalid domain name: %w", err)
	}

	if err := b.Question(dnsmessage.Question{
		Name:  name,
		Type:  qType,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		return "", err
	}
	msgBytes, err := b.Finish()
	if err != nil {
		return "", err
	}

	// 发送 POST 请求
	req, err := http.NewRequestWithContext(ctx, "POST", d.dohURL, bytes.NewReader(msgBytes))
	if err != nil {
		return "", fmt.Errorf("create request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/dns-message")
	req.Header.Set("Accept", "application/dns-message")

	startTime := time.Now()
	resp, err := d.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("DoH RFC8484 request failed: Status=%d, Body=%s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// 解析响应
	var parser dnsmessage.Parser
	if _, err := parser.Start(body); err != nil {
		return "", fmt.Errorf("parse dns response header failed: %w", err)
	}

	// Skip questions
	if err := parser.SkipAllQuestions(); err != nil {
		return "", err
	}

	// Parse answers
	for {
		h, err := parser.AnswerHeader()
		if err == dnsmessage.ErrSectionDone {
			break
		}
		if err != nil {
			return "", fmt.Errorf("parse answer failed: %w", err)
		}

		// 检查类型是否匹配
		if h.Type == qType {
			switch h.Type {
			case dnsmessage.TypeA:
				r, err := parser.AResource() // AResource() 已消费当前 Answer
				if err != nil {
					return "", err
				}
				elapsed := time.Since(startTime)
				result := net.IP(r.A[:]).String()
				d.log.Debug("解析成功 (RFC8484): %s -> %s (%s), 耗时%dms", domain, result, queryTypeStr, elapsed.Milliseconds())
				return result, nil // 直接返回，无需 SkipAnswer
			case dnsmessage.TypeAAAA:
				r, err := parser.AAAAResource() // AAAAResource() 已消费当前 Answer
				if err != nil {
					return "", err
				}
				elapsed := time.Since(startTime)
				result := net.IP(r.AAAA[:]).String()
				d.log.Debug("解析成功 (RFC8484): %s -> %s (%s), 耗时%dms", domain, result, queryTypeStr, elapsed.Milliseconds())
				return result, nil // 直接返回，无需 SkipAnswer
			case dnsmessage.Type(65): // HTTPS
				r, err := parser.UnknownResource() // UnknownResource() 已消费当前 Answer
				if err != nil {
					return "", fmt.Errorf("failed to parse HTTPS resource: %w", err)
				}
				// 将二进制数据转换为 wire format 的 hex 字符串
				hexStr := hex.EncodeToString(r.Data)
				result := fmt.Sprintf("\\# %d %s", len(r.Data), hexStr)
				elapsed := time.Since(startTime)
				d.log.Debug("解析成功 (RFC8484): %s -> %s (%s), 耗时%dms", domain, result, queryTypeStr, elapsed.Milliseconds())
				return result, nil // 直接返回，无需 SkipAnswer
			}
		}

		// 类型不匹配，跳过当前 Answer 继续查找
		if err := parser.SkipAnswer(); err != nil {
			return "", err
		}
	}

	return "", fmt.Errorf("no answer found")
}

// ResolveA 解析 A 记录 (IPv4)
func (d *DoHClient) ResolveA(domain string) (string, error) {
	return d.Resolve(domain, "A")
}

// ResolveAAAA 解析 AAAA 记录 (IPv6)
func (d *DoHClient) ResolveAAAA(domain string) (string, error) {
	return d.Resolve(domain, "AAAA")
}

// ResolveHTTPS 解析 HTTPS 记录
func (d *DoHClient) ResolveHTTPS(domain string) (*HTTPSRecord, error) {
	recordString, err := d.Resolve(domain, "HTTPS")
	if err != nil {
		return nil, fmt.Errorf("解析 HTTPS 记录失败: %w", err)
	}
	return parseHTTPSRecord(recordString)
}

// GetECHConfig 获取域名的 ECH 配置
func (d *DoHClient) GetECHConfig(domain string) ([]byte, error) {
	record, err := d.ResolveHTTPS(domain)
	if err != nil {
		return nil, fmt.Errorf("获取 HTTPS 记录失败: %w", err)
	}

	if len(record.ECH) == 0 {
		return nil, fmt.Errorf("HTTPS 记录中未找到 ECH 配置")
	}

	d.log.Info("成功获取 %s 的 ECH 配置 (长度: %d 字节)", domain, len(record.ECH))
	return record.ECH, nil
}

// resolveJSON 使用 JSON API 解析 (Google/Cloudflare style)
func (d *DoHClient) resolveJSON(ctx context.Context, domain string, queryType string) (string, error) {
	startTime := time.Now()

	// 构建请求 URL
	reqURL, err := url.Parse(d.dohURL)
	if err != nil {
		return "", fmt.Errorf("解析 DoH URL 失败: %w", err)
	}

	// Google DoH 特殊处理：将 /dns-query 替换为 /resolve（JSON API）
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

	d.log.Debug("正在解析 (JSON): %s (%s) via %s", domain, queryType, reqURL.String())

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL.String(), nil)
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

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DoH JSON 请求失败: Status=%d, Body=%s", resp.StatusCode, string(body))
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
	var recordType int
	switch queryType {
	case "A":
		recordType = RecordTypeA
	case "AAAA":
		recordType = RecordTypeAAAA
	case "HTTPS":
		recordType = RecordTypeHTTPS
	default:
		recordType = RecordTypeA
	}

	if len(dohResp.Answer) > 0 {
		for _, ans := range dohResp.Answer {
			if ans.Type == recordType {
				d.log.Debug("解析成功 (JSON): %s -> %s (%s), 耗时%dms", domain, ans.Data, queryType, elapsed.Milliseconds())
				return ans.Data, nil
			}
		}
	}

	d.log.Debug("解析无结果: %s (%s), 耗时%dms", domain, queryType, elapsed.Milliseconds())
	return "", fmt.Errorf("无解析结果")
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

// parseHTTPSRecord 解析 HTTPS 记录数据字符串
func parseHTTPSRecord(data string) (*HTTPSRecord, error) {
	// 检查是否为 wire format (RFC 3597 格式)
	if strings.HasPrefix(data, "\\#") {
		return parseHTTPSRecordWire(data)
	}

	// 文本格式解析
	parts := splitHTTPSRecord(data)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid HTTPS record data: %s", data)
	}

	priority, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid priority: %s", parts[0])
	}

	record := &HTTPSRecord{
		Priority: priority,
		Target:   parts[1],
		Params:   make(map[string]string),
		raw:      []byte(data),
	}

	// 解析参数
	for i := 2; i < len(parts); i++ {
		kv := parts[i]
		if strings.Contains(kv, "=") {
			split := strings.SplitN(kv, "=", 2)
			key := split[0]
			val := split[1]
			// 移除引号
			if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
				val = val[1 : len(val)-1]
			}
			record.Params[key] = val

			// 解析 ECH 配置
			if key == "ech" {
				if decoded, err := base64.StdEncoding.DecodeString(val); err == nil {
					record.ECH = decoded
				} else if decoded, err := base64.RawStdEncoding.DecodeString(val); err == nil {
					record.ECH = decoded
				}
			}
		} else {
			record.Params[kv] = ""
		}
	}

	return record, nil
}

// parseHTTPSRecordWire 解析 RFC 3597 格式的 hex 记录
func parseHTTPSRecordWire(data string) (*HTTPSRecord, error) {
	parts := strings.SplitN(data, " ", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid wire format")
	}

	// parts[2] is hex data, may contain spaces
	hexStr := strings.ReplaceAll(parts[2], " ", "")
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("hex decode failed: %w", err)
	}

	if len(raw) < 2 {
		return nil, fmt.Errorf("data too short")
	}

	priority := int(binary.BigEndian.Uint16(raw[0:2]))
	offset := 2

	target, n, err := parseDomainName(raw[offset:])
	if err != nil {
		return nil, fmt.Errorf("parse domain failed: %w", err)
	}
	offset += n

	params := make(map[string]string)
	var ech []byte

	for offset < len(raw) {
		if offset+4 > len(raw) {
			break
		}
		key := binary.BigEndian.Uint16(raw[offset : offset+2])
		valLen := int(binary.BigEndian.Uint16(raw[offset+2 : offset+4]))
		offset += 4

		if offset+valLen > len(raw) {
			return nil, fmt.Errorf("param value buffer overflow")
		}
		val := raw[offset : offset+valLen]
		offset += valLen

		switch key {
		case 5: // ech
			ech = make([]byte, len(val))
			copy(ech, val)
			params["ech"] = base64.StdEncoding.EncodeToString(val)
		case 1: // alpn
			params["alpn_hex"] = hex.EncodeToString(val)
		default:
			params[fmt.Sprintf("key%d", key)] = hex.EncodeToString(val)
		}
	}

	return &HTTPSRecord{
		Priority: priority,
		Target:   target,
		Params:   params,
		ECH:      ech,
	}, nil
}

// parseDomainName 解析 wire format 的域名
func parseDomainName(data []byte) (string, int, error) {
	var parts []string
	offset := 0
	for {
		if offset >= len(data) {
			return "", 0, fmt.Errorf("buffer overflow reading domain")
		}
		lenByte := int(data[offset])
		offset++
		if lenByte == 0 {
			break
		}
		if offset+lenByte > len(data) {
			return "", 0, fmt.Errorf("label overflow")
		}
		parts = append(parts, string(data[offset:offset+lenByte]))
		offset += lenByte
	}
	if len(parts) == 0 {
		return ".", offset, nil
	}
	return strings.Join(parts, "."), offset, nil
}

// splitHTTPSRecord 辅助函数：分割 HTTPS 记录字符串，考虑引号
func splitHTTPSRecord(data string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for _, r := range data {
		if r == '"' {
			inQuote = !inQuote
			current.WriteRune(r)
		} else if r == ' ' && !inQuote {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}
