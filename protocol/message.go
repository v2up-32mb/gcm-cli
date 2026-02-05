// Package protocol 定义 GCM Worker 通信协议的消息格式和编解码。
//
// 协议格式：
//   [WS_ID: 3字节][STREAM_ID: 1字节][TYPE: 1字节][DATA...]
//
// 支持的消息类型：
//   - CONNECT (0): 发起连接请求
//   - CONNECTED (1): 连接建立成功
//   - DATA (2): 数据传输
//   - CLOSE (3): 关闭连接
package protocol

import (
	"bytes"
	"fmt"
)

// 消息类型常量
const (
	// MsgTypeConnect 表示发起连接请求的消息类型。
	MsgTypeConnect = iota
	// MsgTypeConnected 表示连接建立成功的消息类型。
	MsgTypeConnected
	// MsgTypeData 表示数据传输的消息类型。
	MsgTypeData
	// MsgTypeClose 表示关闭连接的消息类型。
	MsgTypeClose
)

// HeaderSize 是协议头的大小（字节）。
// 头结构：[WS_ID:3][STREAM_ID:1][TYPE:1] = 5 字节。
const HeaderSize = 5

// Message 表示协议消息。
//
// 消息格式：[WS_ID: 3字节][STREAM_ID: 1字节][TYPE: 1字节][DATA...]
type Message struct {
	// WSID 是 WebSocket 连接的唯一标识符（3 字节）。
	WSID []byte
	// StreamID 是流的唯一标识符（1 字节）。
	StreamID byte
	// Type 是消息类型（CONNECT/CONNECTED/DATA/CLOSE）。
	Type byte
	// Data 是消息的负载数据。
	Data []byte
}

// NewMessage 创建新消息。
//
// 如果 wsID 长度不为 3，会自动调整为 3 字节。
func NewMessage(wsID []byte, streamID byte, msgType byte, data []byte) *Message {
	if len(wsID) != 3 {
		wsID = make([]byte, 3)
		copy(wsID, wsID)
	}
	return &Message{
		WSID:     wsID,
		StreamID: streamID,
		Type:     msgType,
		Data:     data,
	}
}

// Encode 将消息编码为字节序列。
//
// 返回格式：[WS_ID:3][STREAM_ID:1][TYPE:1][DATA...]
func (m *Message) Encode() []byte {
	buf := new(bytes.Buffer)
	buf.Write(m.WSID)         // 3 bytes
	buf.WriteByte(m.StreamID) // 1 byte
	buf.WriteByte(m.Type)     // 1 byte
	buf.Write(m.Data)         // data
	return buf.Bytes()
}

// Decode 从字节序列解码消息。
//
// 如果数据长度小于 HeaderSize（5 字节），返回错误。
func Decode(data []byte) (*Message, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("invalid message size: %d < %d", len(data), HeaderSize)
	}

	return &Message{
		WSID:     data[0:3],
		StreamID: data[3],
		Type:     data[4],
		Data:     data[5:],
	}, nil
}

// NewConnectMessage 创建 CONNECT 消息。
//
// 负载格式："host:port|"
func NewConnectMessage(wsID []byte, streamID byte, host string, port uint16) *Message {
	payload := fmt.Sprintf("%s:%d|", host, port)
	return NewMessage(wsID, streamID, MsgTypeConnect, []byte(payload))
}

// NewDataMessage 创建 DATA 消息。
func NewDataMessage(wsID []byte, streamID byte, data []byte) *Message {
	return NewMessage(wsID, streamID, MsgTypeData, data)
}

// NewCloseMessage 创建 CLOSE 消息。
func NewCloseMessage(wsID []byte, streamID byte) *Message {
	return NewMessage(wsID, streamID, MsgTypeClose, nil)
}

// StreamIDToString 将流 ID 转换为十六进制字符串（用于日志输出）。
//
// 例如：0 -> "00"，255 -> "ff"。
func StreamIDToString(streamID byte) string {
	return fmt.Sprintf("%02x", streamID)
}
