package pool

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"gcm/protocol"
	"github.com/gorilla/websocket"
)

// tunnelConn 表示 WebSocket 隧道连接。
//
// tunnelConn 实现 net.Conn 接口，用于在 WebSocket Stream 上建立 TLS 握手
// 和 HTTP 通信。它将 net.Conn 的读写操作映射到 WebSocket 消息的收发，
// 支持 HTTP over WebSocket 和 HTTPS over WebSocket。
//
// 主要用途：
//   - DoH (DNS over HTTPS) 请求通过 WebSocket 隧道访问
//   - TLS 握手建立在 WebSocket Stream 之上
//   - HTTP 请求/响应通过隧道传输
//
// 并发安全：Read 和 Write 方法使用互斥锁保护。
type tunnelConn struct {
	connItem *ConnItem
	streamID byte
	target   string

	readChan  chan []byte
	closeChan chan struct{}
	mu        sync.Mutex
	closed    bool

	// 本地/远程地址
	localAddr  net.Addr
	remoteAddr net.Addr
}

// newTunnelConn 创建并初始化隧道连接。
//
// 参数：
//   - connItem: WebSocket 连接项
//   - streamID: Stream ID（0-255）
//   - target: 目标地址（格式：host:port）
//
// 返回值：初始化完成的 tunnelConn 实例。
func newTunnelConn(connItem *ConnItem, streamID byte, target string) *tunnelConn {
	return &tunnelConn{
		connItem:   connItem,
		streamID:   streamID,
		target:     target,
		readChan:   make(chan []byte, 100), // 缓冲区
		closeChan:  make(chan struct{}),
		localAddr:  &tunnelAddr{net: "tcp", addr: "127.0.0.1:0"},
		remoteAddr: &tunnelAddr{net: "tcp", addr: target},
	}
}

// tunnelAddr 表示隧道连接的网络地址。
//
// tunnelAddr 实现 net.Addr 接口，用于 tunnelConn 的 LocalAddr 和 RemoteAddr 方法。
// 提供虚拟的本地地址（127.0.0.1:0）和实际的远程目标地址。
type tunnelAddr struct {
	net, addr string
}

// Network 返回网络类型（通常为 "tcp"）。
func (a *tunnelAddr) Network() string { return a.net }

// String 返回地址字符串（格式：host:port）。
func (a *tunnelAddr) String() string { return a.addr }

// Read 实现 net.Conn.Read 接口。
//
// 从 readChan 读取数据，如果数据大于缓冲区则分批读取。
// 当连接关闭时返回 io.EOF。
//
// 参数：
//   - b: 读取缓冲区
//
// 返回值：读取的字节数和错误（如果有）。
func (c *tunnelConn) Read(b []byte) (n int, err error) {
	select {
	case data := <-c.readChan:
		if len(data) > len(b) {
			copy(b, data[:len(b)])
			// 将剩余数据放回队列
			c.mu.Lock()
			select {
			case c.readChan <- data[len(b):]:
			default:
				// 队列满，丢弃
			}
			c.mu.Unlock()
			return len(b), nil
		}
		copy(b, data)
		return len(data), nil
	case <-c.closeChan:
		return 0, io.EOF
	}
}

// Write 实现 net.Conn.Write 接口。
//
// 将数据封装为 DATA 消息并通过 WebSocket 发送。
// 如果连接已关闭则返回 io.ErrClosedPipe。
//
// 参数：
//   - b: 要写入的数据
//
// 返回值：写入的字节数和错误（如果有）。
func (c *tunnelConn) Write(b []byte) (n int, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.mu.Unlock()

	dataMsg := protocol.NewDataMessage(c.connItem.ConnectionID, c.streamID, b)
	if err := c.connItem.WriteMessage(websocket.BinaryMessage, dataMsg.Encode()); err != nil {
		return 0, fmt.Errorf("tunnelConn write error: %w", err)
	}
	return len(b), nil
}

// Close 实现 net.Conn.Close 接口。
//
// 发送 CLOSE 消息到 Worker 端，关闭 closeChan 通道。
// 多次调用 Close 是安全的（使用 closeOnce 保证只执行一次）。
//
// 返回值：错误（如果有）。
func (c *tunnelConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	closeMsg := protocol.NewCloseMessage(c.connItem.ConnectionID, c.streamID)
	if err := c.connItem.WriteMessage(websocket.BinaryMessage, closeMsg.Encode()); err != nil {
		// 记录但继续关闭（连接即将关闭，无法恢复）
	}

	close(c.closeChan)
	return nil
}

// LocalAddr 实现 net.Conn.LocalAddr 接口。
//
// 返回虚拟的本地地址（127.0.0.1:0）。
//
// 返回值：本地网络地址。
func (c *tunnelConn) LocalAddr() net.Addr {
	return c.localAddr
}

// RemoteAddr 实现 net.Conn.RemoteAddr 接口。
//
// 返回实际的远程目标地址（格式：host:port）。
//
// 返回值：远程网络地址。
func (c *tunnelConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// SetDeadline 实现 net.Conn.SetDeadline 接口。
//
// 当前实现不支持设置截止时间，总是返回 nil。
//
// 参数：
//   - t: 截止时间
//
// 返回值：错误（当前总是返回 nil）。
func (c *tunnelConn) SetDeadline(t time.Time) error {
	// 暂不支持
	return nil
}

// SetReadDeadline 实现 net.Conn.SetReadDeadline 接口。
//
// 当前实现不支持设置读取截止时间，总是返回 nil。
//
// 参数：
//   - t: 读取截止时间
//
// 返回值：错误（当前总是返回 nil）。
func (c *tunnelConn) SetReadDeadline(t time.Time) error {
	// 暂不支持
	return nil
}

// SetWriteDeadline 实现 net.Conn.SetWriteDeadline 接口。
//
// 当前实现不支持设置写入截止时间，总是返回 nil。
//
// 参数：
//   - t: 写入截止时间
//
// 返回值：错误（当前总是返回 nil）。
func (c *tunnelConn) SetWriteDeadline(t time.Time) error {
	// 暂不支持
	return nil
}

// ProxyTransport 表示 HTTP over WebSocket 代理传输层。
//
// ProxyTransport 实现 http.RoundTripper 接口，通过 WebSocket 隧道发送 HTTP/HTTPS 请求。
// 主要用于 DoH (DNS over HTTPS) 请求通过 GCM 代理访问，解决 DoH 服务器被墙的问题。
//
// 工作流程：
//  1. 从连接池获取 WebSocket 连接和 Stream ID
//  2. 发送 CONNECT 消息建立隧道
//  3. 等待 CONNECTED 响应
//  4. 创建 tunnelConn 实现 net.Conn 接口
//  5. 对于 HTTPS 请求，在 tunnelConn 上建立 TLS 握手
//  6. 发送 HTTP 请求并读取响应
//  7. 自动清理资源（通过 cleanupReadCloser 包装器）
//
// 并发安全：可以安全地在多个 goroutine 中调用 RoundTrip 方法。
type ProxyTransport struct {
	pool *ConnectionPool
}

// NewProxyTransport 创建并初始化代理传输层。
//
// 参数：
//   - pool: WebSocket 连接池实例
//
// 返回值：初始化完成的 ProxyTransport 实例。
func NewProxyTransport(pool *ConnectionPool) *ProxyTransport {
	return &ProxyTransport{
		pool: pool,
	}
}

// RoundTrip 实现 http.RoundTripper 接口。
//
// RoundTrip 通过 WebSocket 隧道发送 HTTP/HTTPS 请求并返回响应。
// 这是 ProxyTransport 的核心方法，用于 DoH 请求代理。
//
// 工作流程：
//  1. 解析目标地址（host:port）
//  2. 从连接池获取 WebSocket 连接和 Stream ID
//  3. 注册 Stream 消息处理器（处理 CONNECTED、DATA、CLOSE 消息）
//  4. 发送 CONNECT 消息到 Worker 端
//  5. 等待 CONNECTED 响应（超时 5 秒）
//  6. 创建 tunnelConn 实现 net.Conn 接口
//  7. 对于 HTTPS 请求，在 tunnelConn 上建立 TLS 握手
//  8. 发送 HTTP 请求并读取响应
//  9. 包装响应体为 cleanupReadCloser，确保资源自动释放
//
// 参数：
//   - req: HTTP 请求对象
//
// 返回值：HTTP 响应对象和错误（如果有）。
//
// 注意：
//   - 调用方必须关闭响应体（resp.Body.Close()）以释放资源
//   - 如果不关闭响应体，Stream 和连接将泄漏
//   - 对于 4xx/5xx 错误响应，资源会立即释放
func (t *ProxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	startTime := time.Now()

	// 1. 解析目标地址
	host := req.URL.Host
	port := uint16(443) // 默认 HTTPS 端口
	if req.URL.Scheme == "http" {
		port = 80
	}

	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		host = parts[0]
		if len(parts) > 1 {
			fmt.Sscanf(parts[1], "%d", &port)
		}
	}

	targetAddr := fmt.Sprintf("%s:%d", host, port)

	// 2. 从连接池获取连接和 Stream
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connItem, streamID, err := t.pool.GetConnectionWithStream(ctx, targetAddr)
	if err != nil {
		return nil, fmt.Errorf("获取连接失败: %w", err)
	}

	wsID := connItem.ConnectionID

	// 3. 先注册 handler（在发送 CONNECT 之前），避免竞态条件
	connectedChan := make(chan struct{}, 1)
	dataChan := make(chan []byte, 100) // DATA 消息缓冲
	closeChan := make(chan struct{}, 1)
	streamRegistered := true // 标记 Stream 是否需要清理

	handler := &StreamHandler{
		OnMessage: func(msg *protocol.Message) {
			if msg.StreamID != streamID {
				return
			}

			switch msg.Type {
			case protocol.MsgTypeConnected:
				select {
				case connectedChan <- struct{}{}:
				default:
				}
			case protocol.MsgTypeData:
				select {
				case dataChan <- msg.Data:
				default:
					// 缓冲区满，丢弃
				}
			case protocol.MsgTypeClose:
				select {
				case closeChan <- struct{}{}:
				default:
				}
			}
		},
		OnClose: func() {
			select {
			case closeChan <- struct{}{}:
			default:
			}
		},
	}

	t.pool.RegisterStreamHandler(connItem, streamID, handler, targetAddr)

	// 4. 发送 CONNECT 消息
	connectMsg := protocol.NewConnectMessage(wsID, streamID, host, port)
	if err := connItem.WriteMessage(websocket.BinaryMessage, connectMsg.Encode()); err != nil {
		t.pool.UnregisterStreamHandler(connItem, streamID)
		return nil, fmt.Errorf("发送 CONNECT 失败: %w", err)
	}

	// 5. 等待 CONNECTED 响应
	select {
	case <-connectedChan:
	case <-closeChan:
		t.pool.UnregisterStreamHandler(connItem, streamID)
		return nil, fmt.Errorf("连接被关闭")
	case <-time.After(5 * time.Second):
		t.pool.UnregisterStreamHandler(connItem, streamID)
		return nil, fmt.Errorf("连接超时")
	}

	// 6. 创建 tunnel（使用已注册的 dataChan 接收数据）
	tunnel := &tunnelConn{
		connItem:   connItem,
		streamID:   streamID,
		target:     targetAddr,
		readChan:   dataChan,
		closeChan:  make(chan struct{}),
		localAddr:  &tunnelAddr{net: "tcp", addr: "127.0.0.1:0"},
		remoteAddr: &tunnelAddr{net: "tcp", addr: targetAddr},
	}

	// 启动后台 goroutine 监听 closeChan 并关闭 tunnel
	go func() {
		<-closeChan
		tunnel.Close()
	}()

	// 清理函数：确保 Stream 总是被正确释放
	defer func() {
		if streamRegistered {
			t.pool.UnregisterStreamHandler(connItem, streamID)
			t.pool.ReleaseConnection(connItem)
		}
		// 关闭 tunnel（如果还没有关闭）
		tunnel.Close()
	}()

	// 7. 如果是 HTTPS，建立 TLS 连接
	var conn net.Conn = tunnel

	if req.URL.Scheme == "https" {
		tlsConfig := &tls.Config{
			ServerName: host,
			// 不验证证书（因为是用于 DoH）
			InsecureSkipVerify: true,
			// 使用较旧的 TLS 版本以提高兼容性
			MinVersion: tls.VersionTLS12,
		}

		tlsConn := tls.Client(tunnel, tlsConfig)

		// TLS 握手
		if err := tlsConn.Handshake(); err != nil {
			return nil, fmt.Errorf("TLS 握手失败: %w", err)
		}

		conn = tlsConn
	}

	// 8. 创建 HTTP 客户端连接并发送请求
	// 使用 http.NewRequestWithContext 创建新请求
	// 注意：需要复制请求体

	var bodyReader io.Reader
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("读取请求体失败: %w", err)
		}
		bodyReader = bytes.NewReader(body)
	}

	// 构建新的 HTTP 请求
	proxyReq, err := http.NewRequestWithContext(
		context.Background(),
		req.Method,
		req.URL.String(),
		bodyReader,
	)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}

	// 复制请求头
	for key, values := range req.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// 9. 发送请求并读取响应
	err = proxyReq.Write(conn)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %w", err)
	}

	// 读取响应
	resp, err := http.ReadResponse(bufio.NewReader(conn), proxyReq)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("[ProxyTransport] 请求完成: %s://%s%s -> %d，耗时: %dms\n",
		req.URL.Scheme, req.URL.Host, req.URL.RequestURI(), resp.StatusCode, elapsed.Milliseconds())

	// 如果是 4xx/5xx 错误，读取响应体内容用于调试
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("[ProxyTransport] 错误响应 (%d): %s\n", resp.StatusCode, string(body))
		// 错误响应也需要清理资源
		t.pool.UnregisterStreamHandler(connItem, streamID)
		t.pool.ReleaseConnection(connItem)
		tunnel.Close()
		return resp, nil
	}

	// 包装响应体，在关闭时自动清理资源
	// 这样确保无论调用方是否正确关闭响应体，资源都能被释放
	resp.Body = &cleanupReadCloser{
		ReadCloser: resp.Body,
		onClose: func() {
			t.pool.UnregisterStreamHandler(connItem, streamID)
			t.pool.ReleaseConnection(connItem)
			tunnel.Close()
		},
	}

	// 标记为已管理，跳过 defer 清理
	streamRegistered = false

	return resp, nil
}

// cleanupReadCloser 表示带清理回调的 ReadCloser 包装器。
//
// cleanupReadCloser 包装 io.ReadCloser，在 Close 方法被调用时执行清理回调。
// 用于确保 HTTP 响应体关闭时自动释放 Stream 和连接资源，防止资源泄漏。
//
// 工作原理：
//   - 包装 http.Response.Body
//   - 当调用方调用 resp.Body.Close() 时，自动执行清理回调
//   - 清理回调包括：UnregisterStreamHandler、ReleaseConnection、tunnel.Close()
//   - 防止重复调用（onClose 设置为 nil）
//
// 这是解决 Stream 泄漏问题的关键机制。
type cleanupReadCloser struct {
	io.ReadCloser
	onClose func()
}

func (c *cleanupReadCloser) Close() error {
	err := c.ReadCloser.Close()
	if c.onClose != nil {
		c.onClose()
		c.onClose = nil // 防止重复调用
	}
	return err
}
