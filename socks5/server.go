package socks5

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gcm/gcm/config"
	"github.com/gcm/gcm/dns"
	"github.com/gcm/gcm/logger"
	"github.com/gcm/gcm/pool"
	"github.com/gcm/gcm/protocol"
	"github.com/gorilla/websocket"
)

const (
	socks5Version = 0x05
	authNone      = 0x00
	cmdConnect    = 0x01
	atypIPv4      = 0x01
	atypDomain    = 0x03
	atypIPv6      = 0x04
)

// Server SOCKS5 服务器
type Server struct {
	cfg           *config.Config
	log           *logger.Logger
	pool          *pool.ConnectionPool
	dnsCache      *dns.DNSCache
	server        net.Listener
	activeTunnels int32
}

// NewServer 创建 SOCKS5 服务器
func NewServer(cfg *config.Config, p *pool.ConnectionPool, dc *dns.DNSCache) *Server {
	return &Server{
		cfg:      cfg,
		log:      logger.GetLogger("Socks5"),
		pool:     p,
		dnsCache: dc,
	}
}

// Start 启动 SOCKS5 服务器
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("监听失败: %w", err)
	}

	s.server = listener
	s.log.Info("监听地址: %s", s.cfg.ListenAddress)

	go s.acceptLoop()

	return nil
}

// acceptLoop 接受连接循环
func (s *Server) acceptLoop() {
	for {
		conn, err := s.server.Accept()
		if err != nil {
			s.log.Error("接受连接失败: %v", err)
			return
		}

		go s.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (s *Server) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	clientAddr := clientConn.RemoteAddr().String()
	s.log.Debug("新客户端连接: %s", clientAddr)

	// 1. 认证阶段
	if err := s.handleAuth(clientConn); err != nil {
		s.log.Debug("认证失败: %v", err)
		return
	}

	// 2. 请求阶段
	originalHost, resolvedHost, port, err := s.handleRequest(clientConn)
	if err != nil {
		s.log.Debug("请求处理失败: %v", err)
		return
	}

	// 3. 创建隧道
	s.createTunnel(clientConn, originalHost, resolvedHost, port)
}

// handleAuth 处理认证
func (s *Server) handleAuth(conn net.Conn) error {
	buf := make([]byte, 256)

	// 读取认证请求
	n, err := conn.Read(buf)
	if err != nil || n < 3 {
		return fmt.Errorf("读取认证请求失败")
	}

	if buf[0] != socks5Version {
		return fmt.Errorf("不支持的 SOCKS 版本: %d", buf[0])
	}

	// 响应：无需认证
	_, err = conn.Write([]byte{socks5Version, authNone})
	return err
}

// handleRequest 处理请求
// 返回: 原始主机名(用于日志), 解析后的主机(用于连接), 端口, 错误
func (s *Server) handleRequest(conn net.Conn) (originalHost, resolvedHost string, port uint16, err error) {
	buf := make([]byte, 256)

	n, err := conn.Read(buf)
	if err != nil || n < 4 {
		return "", "", 0, fmt.Errorf("读取请求失败")
	}

	// 检查版本和命令
	if buf[0] != socks5Version {
		return "", "", 0, fmt.Errorf("不支持的 SOCKS 版本: %d", buf[0])
	}

	if buf[1] != cmdConnect {
		// 发送不支持的命令响应
		conn.Write([]byte{socks5Version, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return "", "", 0, fmt.Errorf("不支持的命令: %d", buf[1])
	}

	// 解析目标地址
	addrType := buf[3]

	switch addrType {
	case atypIPv4:
		if n < 10 {
			return "", "", 0, fmt.Errorf("IPv4 地址长度不足")
		}
		originalHost = fmt.Sprintf("%d.%d.%d.%d", buf[4], buf[5], buf[6], buf[7])
		resolvedHost = originalHost
		port = binary.BigEndian.Uint16(buf[8:10])
		s.log.Debug("IPv4 请求: %s:%d", originalHost, port)

	case atypDomain:
		if n < 5 {
			return "", "", 0, fmt.Errorf("域名长度不足")
		}
		domainLen := int(buf[4])
		if n < 5+domainLen+2 {
			return "", "", 0, fmt.Errorf("域名数据不完整")
		}
		domain := string(buf[5 : 5+domainLen])
		port = binary.BigEndian.Uint16(buf[5+domainLen : 7+domainLen])
		originalHost = domain
		resolvedHost = domain

		s.log.Debug("域名请求: %s", domain)

		// DNS 预解析
		if s.cfg.EnableDoH {
			if ip, _, err := s.dnsCache.ResolveAny(domain); err == nil {
				s.log.Debug("DNS 解析: %s -> %s", domain, ip)
				resolvedHost = ip
			}
		}

	case atypIPv6:
		if n < 22 {
			return "", "", 0, fmt.Errorf("IPv6 地址长度不足")
		}
		ipv6Buf := buf[4:20]
		originalHost = net.IP(ipv6Buf).String()
		resolvedHost = originalHost
		port = binary.BigEndian.Uint16(buf[20:22])
		s.log.Debug("IPv6 请求: %s:%d", originalHost, port)

	default:
		conn.Write([]byte{socks5Version, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return "", "", 0, fmt.Errorf("不支持的地址类型: %d", addrType)
	}

	// IPv6 格式修正
	if ip := net.ParseIP(resolvedHost); ip != nil && ip.To4() == nil {
		resolvedHost = fmt.Sprintf("[%s]", resolvedHost)
	}

	s.log.Debug("收到代理请求 -> %s:%d (解析后: %s:%d)", originalHost, port, resolvedHost, port)

	return originalHost, resolvedHost, port, nil
}

// createTunnel 创建隧道
// originalHost: 原始主机名（用于日志显示）
// resolvedHost: 解析后的主机（用于实际连接）
func (s *Server) createTunnel(clientConn net.Conn, originalHost, resolvedHost string, port uint16) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.GetTunnelTimeout())
	defer cancel()

	// 构造目标地址字符串（用于亲和性路由，使用解析后的地址）
	targetAddr := fmt.Sprintf("%s:%d", resolvedHost, port)

	// 记录请求开始
	var requestStartTime int64
	if s.cfg.EnableStats {
		requestStartTime = s.pool.RecordRequestStart()
	}

	// 原子化地获取连接并分配流 ID
	connItem, streamID, err := s.pool.GetConnectionWithStream(ctx, targetAddr)
	if err != nil {
		s.log.Warn("获取连接+流失败: %v", err)
		if s.cfg.EnableStats {
			s.pool.RecordRequestFailure()
		}
		return
	}

	wsID := connItem.ConnectionID
	connIDStr := fmt.Sprintf("%06x", wsID[0]<<16|wsID[1]<<8|wsID[2])
	streamIDStr := protocol.StreamIDToString(streamID)

	// 日志使用原始主机名
	s.log.Info("新请求 -> %s:%d | WS[%s] Stream[%s]", originalHost, port, connIDStr, streamIDStr)

	atomic.AddInt32(&s.activeTunnels, 1)
	defer atomic.AddInt32(&s.activeTunnels, -1)

	// 发送 CONNECT 消息（使用解析后的地址）
	connectMsg := protocol.NewConnectMessage(wsID, streamID, resolvedHost, port)
	if err := connItem.WriteMessage(websocket.BinaryMessage, connectMsg.Encode()); err != nil {
		s.log.Error("发送 CONNECT 消息失败: %v", err)
		s.pool.UnregisterStreamHandler(connItem, streamID)
		s.pool.ReleaseConnection(connItem)
		return
	}

	// 设置超时定时器
	timeoutTimer := time.NewTimer(s.cfg.GetTunnelTimeout())
	defer timeoutTimer.Stop()

	// 创建完成信号通道
	done := make(chan struct{})
	// 创建关闭信号通道
	closed := make(chan struct{})

	connected := false
	var bytesSent, bytesReceived int64

	// 清理函数
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			// 主动发送 CLOSE 消息到 Worker，通知 Stream 关闭
			closeMsg := protocol.NewCloseMessage(wsID, streamID)
			if err := connItem.WriteMessage(websocket.BinaryMessage, closeMsg.Encode()); err != nil {
				s.log.Debug("发送 CLOSE 消息失败: %v", err)
			} else {
				s.log.Debug("发送 CLOSE 消息 -> Stream[%s]", streamIDStr)
			}

			timeoutTimer.Stop()
			if s.cfg.EnableStats && (bytesSent > 0 || bytesReceived > 0) {
				s.pool.RecordDataTransfer(bytesSent, bytesReceived)
			}
			clientConn.Close()
			targetAddr, _ := s.pool.UnregisterStreamHandler(connItem, streamID)
			s.pool.ReleaseConnection(connItem)
			s.log.Debug("清理完成: WS[%s] Stream[%s] -> %s", connIDStr, streamIDStr, targetAddr)

			// 通知主 goroutine 连接已关闭
			select {
			case <-closed:
				// 已经关闭
			default:
				close(closed)
			}
		})
	}

	// 注册流处理器
	handler := &pool.StreamHandler{
		OnMessage: func(msg *protocol.Message) {
			if msg.StreamID != streamID {
				return
			}

			if !connected {
				if msg.Type == protocol.MsgTypeConnected {
					connected = true
					// 增加 Stream 计数
					connItem.Traffic.IncStream()
					timeoutTimer.Stop()
					if s.cfg.EnableStats {
						s.pool.RecordRequestSuccess(requestStartTime)
					}
					// 发送 SOCKS5 连接成功响应
					if _, err := clientConn.Write([]byte{socks5Version, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
						s.log.Debug("发送 SOCKS5 响应失败: %v", err)
						cleanup()
						return
					}
					// 通知主 goroutine 连接成功
					select {
					case <-done:
						// 已经关闭
					default:
						close(done)
					}
				} else if msg.Type == protocol.MsgTypeClose {
					cleanup()
				}
			} else {
				if msg.Type == protocol.MsgTypeData {
					if len(msg.Data) > 0 {
						bytesReceived += int64(len(msg.Data))
						// 更新连接流量统计（接收）
						connItem.Traffic.AddRecv(int64(len(msg.Data)))
						if _, err := clientConn.Write(msg.Data); err != nil {
							s.log.Debug("写入客户端失败: %v", err)
							cleanup()
							return
						}
					}
				} else if msg.Type == protocol.MsgTypeClose {
					cleanup()
				}
			}
		},
		OnClose: func() {
			cleanup()
		},
		OnCleanup: func() {
			// 清理工作已在 cleanup 中处理
		},
	}

	s.pool.RegisterStreamHandler(connItem, streamID, handler, targetAddr)

	// 客户端数据转发
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := clientConn.Read(buf)
			if err != nil {
				// 连接关闭（包括 EOF）都需要清理
				cleanup()
				return
			}

			if connected {
				bytesSent += int64(n)
				// 更新连接流量统计（发送）
				connItem.Traffic.AddSent(int64(n))
				dataMsg := protocol.NewDataMessage(wsID, streamID, buf[:n])
				if err := connItem.WriteMessage(websocket.BinaryMessage, dataMsg.Encode()); err != nil {
					cleanup()
					return
				}
			}
			// 未连接时静默丢弃数据
		}
	}()

	// 等待连接成功或超时
	select {
	case <-done:
		// 连接成功，等待连接关闭
		s.log.Debug("隧道建立成功: %s:%d", originalHost, port)
		// 等待连接真正关闭
		<-closed
	case <-timeoutTimer.C:
		// 连接超时
		if !connected {
			s.log.Warn("隧道超时: %s:%d", originalHost, port)
			if s.cfg.EnableStats {
				s.pool.RecordRequestTimeout()
			}
			cleanup()
		}
	case <-ctx.Done():
		// 上下文取消
		if !connected {
			s.log.Warn("隧道建立被取消: %s:%d", originalHost, port)
			cleanup()
		}
	case <-closed:
		// 连接在建立前就关闭了
		s.log.Debug("连接在建立前关闭: %s:%d", originalHost, port)
	}
}

// Close 关闭服务器
func (s *Server) Close() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}
