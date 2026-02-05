# protocol 包

## 概述

protocol 包定义 GCM Worker 通信协议的消息格式和编解码。

---

## 协议格式

```
[WS_ID: 3字节][STREAM_ID: 1字节][TYPE: 1字节][DATA...]
```

协议头大小：5 字节

---

## 常量

```go
const (
    MsgTypeConnect   = 0  // 发起连接请求
    MsgTypeConnected = 1  // 连接建立成功
    MsgTypeData      = 2  // 数据传输
    MsgTypeClose     = 3  // 关闭连接
)

const HeaderSize = 5
```

---

## 类型定义

### Message

```go
type Message struct {
    WSID     []byte // 3 bytes
    StreamID byte   // 1 byte
    Type     byte   // 1 byte
    Data     []byte
}
```

协议消息。

---

## 构造函数

### NewMessage

```go
func NewMessage(wsID []byte, streamID byte, msgType byte, data []byte) *Message
```

创建新消息。如果 wsID 长度不为 3，会自动调整。

---

## 工厂函数

### NewConnectMessage

```go
func NewConnectMessage(wsID []byte, streamID byte, host string, port uint16) *Message
```

创建 CONNECT 消息。

Data 格式：`"host:port|"`

### NewDataMessage

```go
func NewDataMessage(wsID []byte, streamID byte, data []byte) *Message
```

创建 DATA 消息。

### NewCloseMessage

```go
func NewCloseMessage(wsID []byte, streamID byte) *Message
```

创建 CLOSE 消息。

---

## 方法

### Encode

```go
func (m *Message) Encode() []byte
```

将消息编码为字节序列。

---

## 函数

### Decode

```go
func Decode(data []byte) (*Message, error)
```

从字节序列解码消息。

如果数据长度 < HeaderSize，返回错误。

### StreamIDToString

```go
func StreamIDToString(streamID byte) string
```

将 Stream ID 转换为十六进制字符串（用于日志）。

---

## 消息流程示例

```
客户端 → Worker:
[WSID][StreamID][0x00]"example.com:443|"

Worker → 客户端:
[WSID][StreamID][0x01]

数据传输（双向）:
[WSID][StreamID][0x02][二进制数据]

关闭连接:
[WSID][StreamID][0x03]
```
