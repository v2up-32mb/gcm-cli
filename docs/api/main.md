# main 包

## 概述

main 包是 GCM 代理客户端的入口点，负责初始化所有模块并启动服务。

---

## 全局变量

```go
var (
    cfg             *config.Config
    relayManager    *relay.RelayManager
    dnsCache        *dns.DNSCache
    dohClient       *dns.DoHClient
    echManager      *ech.EchManager
    connPool        *pool.ConnectionPool
    qualityMonitor  *pool.ConnectionQualityMonitor
    socks5Server    *socks5.Server
    metricsSrv      *metrics.Server
)
```

全局模块实例。

---

## 函数

### main

```go
func main()
```

程序入口点。

**初始化流程：**
1. 加载配置
2. 初始化日志系统
3. 初始化 DoH 客户端
4. 初始化 DNS 缓存
5. 初始化中转节点管理器
6. 初始化 ECH 管理器（如果启用）
7. 初始化连接池
8. 设置 DoH 代理（如果启用）
9. 启动连接池预热（异步）
10. 启动 DNS 缓存预热（异步）
11. 启动 SOCKS5 服务器
12. 启动 Metrics 服务器（如果启用）
13. 启动 ECH 定时刷新（如果启用）
14. 启动连接质量监控（如果启用）
15. 等待退出信号

### printStartupInfo

```go
func printStartupInfo(log *logger.Logger)
```

打印启动信息（配置摘要）。

### printReadyInfo

```go
func printReadyInfo(log *logger.Logger)
```

打印服务就绪信息。

### waitForSignal

```go
func waitForSignal(log *logger.Logger)
```

等待系统信号（SIGINT/SIGTERM）并优雅关闭。

---

## 自定义类型

### websocketLogger

```go
type websocketLogger struct {
    io.Writer
}
```

过滤 websocket 库的冗余日志。

#### 方法

```go
func (w *websocketLogger) Write(p []byte) (n int, err error)
```

实现 io.Writer 接口，过滤 "failed to close network connection" 等无害警告。

---

## 启动顺序

```
1. 配置加载
   ↓
2. 日志初始化
   ↓
3. DoH 客户端初始化
   ↓
4. DNS 缓存初始化
   ↓
5. 中转节点管理器初始化
   ↓
6. ECH 管理器初始化（可选）
   ↓
7. 连接池初始化
   ↓
8. DoH 代理设置（可选）
   ↓
9. 连接池预热（异步，不阻塞）
   ↓
10. DNS 缓存预热（异步，不阻塞）
   ↓
11. SOCKS5 服务器启动
   ↓
12. Metrics 服务器启动（可选）
   ↓
13. ECH 定时刷新启动（可选）
   ↓
14. 连接质量监控启动（可选）
   ↓
15. 等待退出信号
```

---

## 优雅关闭

收到 SIGINT 或 SIGTERM 信号时：
1. 关闭 DNS 缓存
2. 关闭中转节点管理器
3. 关闭连接池
4. 关闭 SOCKS5 服务器
5. 关闭 Metrics 服务器
6. 停止 ECH 定时刷新
7. 停止连接质量监控
8. 关闭日志文件
9. 退出程序
