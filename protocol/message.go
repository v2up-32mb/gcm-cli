package protocol

import (
	"bytes"
	"fmt"
)

// 消息类型常量
const (
	MsgTypeConnect   = 0 // 发起连接请求
	MsgTypeConnected = 1 // 连接建立成功
	MsgTypeData      = 2 // 数据传输
	MsgTypeClose     = 3 // 关闭连接
)

// 协议头大小: WS_ID(3) + STREAM_ID(1) + TYPE(1) = 5 bytes
const HeaderSize = 5

// Message 表示协议消息
// 格式: [WS_ID:3字节][STREAM_ID:1字节][TYPE:1字节][DATA...]
type Message struct {
	WSID     []byte // 3 bytes
	StreamID byte   // 1 byte
	Type     byte   // 1 byte
	Data     []byte
}

// NewMessage 创建新消息
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

// Encode 将消息编码为字节
func (m *Message) Encode() []byte {
	buf := new(bytes.Buffer)
	buf.Write(m.WSID)         // 3 bytes
	buf.WriteByte(m.StreamID) // 1 byte
	buf.WriteByte(m.Type)     // 1 byte
	buf.Write(m.Data)         // data
	return buf.Bytes()
}

// Decode 从字节解码消息
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

// NewConnectMessage 创建 CONNECT 消息
func NewConnectMessage(wsID []byte, streamID byte, host string, port uint16) *Message {
	payload := fmt.Sprintf("%s:%d|", host, port)
	return NewMessage(wsID, streamID, MsgTypeConnect, []byte(payload))
}

// NewDataMessage 创建 DATA 消息
func NewDataMessage(wsID []byte, streamID byte, data []byte) *Message {
	return NewMessage(wsID, streamID, MsgTypeData, data)
}

// NewCloseMessage 创建 CLOSE 消息
func NewCloseMessage(wsID []byte, streamID byte) *Message {
	return NewMessage(wsID, streamID, MsgTypeClose, nil)
}

// StreamIDToString 将流 ID 转换为十六进制字符串
func StreamIDToString(streamID byte) string {
	return fmt.Sprintf("%02x", streamID)
}
