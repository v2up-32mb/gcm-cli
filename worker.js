/**
 * Cloudflare Worker - GCM Proxy 服务端
 *
 * 对应客户端: gcm.cjs
 * 功能: 通过 WebSocket 接收 SOCKS5 代理请求，转发到目标服务器
 *
 * 协议格式 (2字节头, 仅多路复用):
 * - 客户端 -> Worker: [STREAM_ID:1][TYPE:1=0]{host:port}|
 * - 客户端 -> Worker: [STREAM_ID:1][TYPE:1=2][binary_data]
 * - 客户端 -> Worker: [STREAM_ID:1][TYPE:1=3]
 * - Worker -> 客户端: [STREAM_ID:1][TYPE:1=1]
 * - Worker -> 客户端: [STREAM_ID:1][TYPE:1=2][binary_data]
 * - Worker -> 客户端: [STREAM_ID:1][TYPE:1=3]
 *
 * 部署说明:
 * 1. 登录 Cloudflare Dashboard
 * 2. 进入 Workers & Pages
 * 3. 创建新 Worker
 * 4. 将此代码粘贴到编辑器
 * 5. 部署并记录 Worker URL
 * 6. 在 gcm/config.json 中设置 workerHost
 */

import { connect } from "cloudflare:sockets";

// ==================== 常量 ====================
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

// ==================== 默认配置 ====================
const DEFAULT_KV_CONFIG = {
  enableFallback: true,
  connectTimeout: 1000,
  enableLogging: false,
  maxStreamsPerConnection: 16,
};

const DEFAULT_FALLBACK_IPS = [
  "proxyip.us.cmliussss.net",
  "proxyip.hk.cmliussss.net",
  "proxyip.jp.cmliussss.net",
  "tw.william.us.ci",
];

// ==================== 消息类型常量 ====================
const MSG_TYPE = {
  CONNECT: 0,
  CONNECTED: 1,
  DATA: 2,
  CLOSE: 3,
};

// ==================== 伪装页面 HTML ====================
const FAKE_PAGE_HTML = `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="3;url=https://www.whitehouse.gov/">
    <title>Access Denied</title>
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #f0f0f0; }
        .box { background: white; padding: 40px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); text-align: center; max-width: 400px; width: 90%; }
        h1 { color: #d32f2f; margin: 0 0 16px; font-size: 24px; }
        p { color: #666; margin: 0; font-size: 14px; }
        .spinner { width: 20px; height: 20px; border: 2px solid #f3f3f3; border-top: 2px solid #666; border-radius: 50%; animation: spin 1s linear infinite; margin: 20px auto 0; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="box">
        <h1>You are not allowed to access this site.</h1>
        <p>System has detected unauthorized access attempt.</p>
        <p style="margin-top: 10px; font-size: 12px; color: #999;">Redirecting to security center...</p>
        <div class="spinner"></div>
    </div>
</body>
</html>`;

// ==================== 工具函数 ====================
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function log(scope, message, enableLogging = false) {
  if (enableLogging) {
    console.log(`[${scope}] ${message}`);
  }
}

function logError(scope, message) {
  console.error(`[${scope}] ${message}`);
}

// 解析地址 (Host:Port)
function parseAddress(addr) {
  // 处理 IPv6 格式 [::1]:80
  if (addr[0] === "[") {
    const end = addr.indexOf("]");
    return {
      host: addr.substring(1, end),
      port: parseInt(addr.substring(end + 2), 10),
    };
  }
  // 处理 IPv4 或 域名 host:80
  const sep = addr.lastIndexOf(":");
  return {
    host: addr.substring(0, sep),
    port: parseInt(addr.substring(sep + 1), 10),
  };
}

// 判断是否为 CF 内部连接错误
function isCFError(err) {
  const msg = err?.message?.toLowerCase() || "";
  return (
    msg.includes("proxy request") ||
    msg.includes("cannot connect") ||
    msg.includes("cloudflare")
  );
}

// 安全关闭 WebSocket
function safeCloseWebSocket(ws) {
  try {
    if (
      ws.readyState === WS_READY_STATE_OPEN ||
      ws.readyState === WS_READY_STATE_CLOSING
    ) {
      ws.close(1000, "Server closed");
    }
  } catch {}
}

// ==================== 多路复用流管理器 ====================
/**
 * 流管理器 - 管理多个多路复用流
 * 每个流对应一个到目标服务器的 TCP 连接
 */
class StreamManager {
  constructor(webSocket, config) {
    this.webSocket = webSocket;
    this.config = config;
    // streams: Map<streamId, { remoteSocket, remoteWriter, remoteReader, isClosed }>
    this.streams = new Map();
    this.streamCount = 0;
  }

  log(message) {
    log("Mux", message, this.config.enableLogging);
  }

  /**
   * 创建新流并连接目标服务器
   */
  async createStream(streamId, targetAddr) {
    // 检查流是否已存在
    if (this.streams.has(streamId)) {
      this.log(`流 ${streamId} 已存在，关闭旧流`);
      this.closeStream(streamId);
    }

    // 检查最大并发流数
    if (this.streamCount >= this.config.maxStreamsPerConnection) {
      this.sendError(streamId, "Maximum streams exceeded");
      return false;
    }

    let { host, port } = parseAddress(targetAddr);
    const attempts = this.config.enableFallback
      ? [null, ...this.config.cfFallbackIPs]
      : [null];

    for (let i = 0; i < attempts.length; i++) {
      const attemptHost = attempts[i] || host;
      const attemptDesc = attempts[i] === null ? "直连" : `fallback[${i}]`;

      try {
        this.log(`[${streamId}] 尝试${attemptDesc}: ${attemptHost}:${port}`);

        const remoteSocket = connect({
          hostname: attemptHost,
          port,
        });

        // 添加超时控制
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(
            () => reject(new Error("Connection timeout")),
            this.config.connectTimeout,
          );
        });

        // 等待连接建立或超时
        await Promise.race([remoteSocket.opened, timeoutPromise]);

        const remoteWriter = remoteSocket.writable.getWriter();
        const remoteReader = remoteSocket.readable.getReader();

        this.streams.set(streamId, {
          remoteSocket,
          remoteWriter,
          remoteReader,
          isClosed: false,
        });
        this.streamCount++;

        this.log(`[${streamId}] ${attemptDesc}成功`);
        this.sendConnected(streamId);

        // 启动数据转发
        this.pumpRemoteToWebSocket(streamId, remoteReader);

        return true;
      } catch (err) {
        this.log(`[${streamId}] ${attemptDesc}失败: ${err.message}`);

        if (!isCFError(err) || i === attempts.length - 1) {
          this.sendError(streamId, err.message);
          return false;
        }
      }
    }
    return false;
  }

  /**
   * 获取流
   */
  getStream(streamId) {
    return this.streams.get(streamId);
  }

  /**
   * 写入数据到流
   */
  async writeStream(streamId, data) {
    const stream = this.getStream(streamId);
    if (!stream || stream.isClosed) {
      return false;
    }

    try {
      if (data instanceof Uint8Array) {
        await stream.remoteWriter.write(data);
      } else {
        await stream.remoteWriter.write(encoder.encode(data));
      }
      return true;
    } catch (e) {
      this.log(`[${streamId}] 写入失败: ${e.message}`);
      this.closeStream(streamId);
      return false;
    }
  }

  /**
   * 关闭流
   */
  closeStream(streamId) {
    const stream = this.getStream(streamId);
    if (!stream || stream.isClosed) return;

    stream.isClosed = true;

    try {
      stream.remoteWriter?.releaseLock();
    } catch {}
    try {
      stream.remoteReader?.releaseLock();
    } catch {}
    try {
      stream.remoteSocket?.close();
    } catch {}

    this.streams.delete(streamId);
    this.streamCount--;

    this.log(`[${streamId}] 流已关闭，剩余流: ${this.streamCount}`);
  }

  /**
   * 发送 CONNECTED 响应
   */
  sendConnected(streamId) {
    try {
      const header = new Uint8Array([
        streamId, // Stream ID (1 byte)
        MSG_TYPE.CONNECTED, // Type (1 byte)
      ]);
      this.webSocket.send(header);
    } catch {}
  }

  /**
   * 发送错误响应
   */
  sendError(streamId, errorMsg) {
    try {
      // 错误响应需要额外信息，暂时用 CLOSE 代替
      this.sendClose(streamId);
    } catch {}
  }

  /**
   * 发送数据到客户端
   */
  sendData(streamId, data) {
    try {
      const header = new Uint8Array([
        streamId, // Stream ID (1 byte)
        MSG_TYPE.DATA, // Type (1 byte)
      ]);
      const combined = new Uint8Array(header.length + data.length);
      combined.set(header);
      combined.set(data, header.length);
      this.webSocket.send(combined);
    } catch {}
  }

  /**
   * 发送流关闭通知
   */
  sendClose(streamId) {
    try {
      const header = new Uint8Array([
        streamId, // Stream ID (1 byte)
        MSG_TYPE.CLOSE, // Type (1 byte)
      ]);
      this.webSocket.send(header);
    } catch {}
  }

  /**
   * 将远程 Socket 数据转发给 WebSocket
   */
  async pumpRemoteToWebSocket(streamId, remoteReader) {
    try {
      while (true) {
        const { done, value } = await remoteReader.read();

        if (done) break;
        if (value?.byteLength > 0) {
          this.sendData(streamId, value);
        }
      }
    } catch (e) {
      this.log(`[${streamId}] 转发异常: ${e.message}`);
    }

    // 转发结束，关闭流
    this.sendClose(streamId);
    this.closeStream(streamId);
  }

  /**
   * 关闭所有流
   */
  closeAll() {
    for (const [streamId] of this.streams) {
      this.closeStream(streamId);
    }
  }

  /**
   * 获取统计信息
   */
  getStats() {
    return {
      activeStreams: this.streamCount,
      maxStreams: this.config.maxStreamsPerConnection,
    };
  }
}

// ==================== 主入口 ====================
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // 0. 获取认证 ID (优先环境变量，否则使用默认 UUID)
      // 如果环境变量未设置，使用默认 UUID 作为路径，相当于一种弱保护或后门
      const userID = (env.USER_ID || "uuid-placeholder").toLowerCase();
      const validPath = `/${userID}`;

      // 1. 路由判断: 仅 /USER_ID 处理 WebSocket
      if (url.pathname === validPath) {
        const upgradeHeader = request.headers.get("Upgrade");
        if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
          return new Response("Expected WebSocket", { status: 426 });
        }

        // 2. 加载配置 (仅在确认 WS 连接后)
        let kvConfig = null;
        try {
          if (env.GCM_KV) {
            kvConfig = await env.GCM_KV.get("config", { type: "json" });
            if (!kvConfig) {
              ctx.waitUntil(
                env.GCM_KV.put("config", JSON.stringify(DEFAULT_KV_CONFIG)),
              );
            }
          }
        } catch (err) {
          console.error("Failed to load KV config:", err);
        }

        const baseConfig = kvConfig || DEFAULT_KV_CONFIG;

        // 3. 解析 Fallback IPs
        let fallbackIPs = DEFAULT_FALLBACK_IPS;
        const queryFallback = url.searchParams.get("fallbackip");
        if (queryFallback) {
          fallbackIPs = [queryFallback];
        } else if (env.FALLBACK_IPS) {
          fallbackIPs = env.FALLBACK_IPS.split(",")
            .map((s) => s.trim())
            .filter(Boolean);
        }

        const config = { ...baseConfig, cfFallbackIPs: fallbackIPs };

        // 4. 建立连接
        const [client, server] = Object.values(new WebSocketPair());
        server.accept();

        log("WS", "连接已建立", config.enableLogging);

        // 处理会话
        handleSession(server, config).catch(() => safeCloseWebSocket(server));

        // 返回 101 Switching Protocols
        return new Response(null, {
          status: 101,
          webSocket: client,
        });
      }

      // 其他所有路径处理 (包括原本的 /ws 如果 USER_ID 不匹配)
      return new Response(FAKE_PAGE_HTML, {
        status: 403,
        headers: { "Content-Type": "text/html;charset=UTF-8" },
      });
    } catch (err) {
      logError("Server", err.toString());
      return new Response(err.toString(), { status: 500 });
    }
  },
};

// ==================== 会话处理 ====================
async function handleSession(webSocket, config) {
  let isClosed = false;
  // 立即初始化流管理器 - 只使用新协议
  const streamManager = new StreamManager(webSocket, config);

  // 清理资源
  const cleanup = () => {
    if (isClosed) return;
    isClosed = true;
    streamManager.closeAll();
    safeCloseWebSocket(webSocket);
  };

  // 协议头长度常量
  const HEADER_LEN = 2; // [STREAM_ID:1][TYPE:1]

  // 监听客户端消息
  webSocket.addEventListener("message", async (event) => {
    if (isClosed) return;

    try {
      const data = event.data;

      // 处理二进制数据
      const uint8Array =
        data instanceof ArrayBuffer ? new Uint8Array(data) : data;

      if (uint8Array.length < HEADER_LEN) {
        logError("Mux", `消息太短: ${uint8Array.length} 字节`);
        return;
      }

      // 解析头部 (2字节精简协议: [STREAM_ID:1][TYPE:1])
      const streamId = uint8Array[0]; // 1 byte: Stream ID
      const msgType = uint8Array[1]; // 1 byte: Type

      // 处理不同类型的消息
      if (msgType === MSG_TYPE.CONNECT) {
        // CONNECT 消息: [STREAM_ID:1][TYPE:1]{host:port}|
        // 提取目标地址
        const payload = decoder.decode(uint8Array.slice(HEADER_LEN));
        const targetAddr = payload.substring(0, payload.lastIndexOf("|"));
        streamManager.log(`[${streamId.toString(16)}] 连接请求: ${targetAddr}`);
        await streamManager.createStream(streamId, targetAddr);
      } else if (msgType === MSG_TYPE.DATA) {
        // DATA 消息: [STREAM_ID:1][TYPE:1][binary_data]
        const binaryData = uint8Array.slice(HEADER_LEN);
        await streamManager.writeStream(streamId, binaryData);
      } else if (msgType === MSG_TYPE.CLOSE) {
        // CLOSE 消息: [STREAM_ID:1][TYPE:1]
        streamManager.log(`[${streamId.toString(16)}] 关闭流`);
        streamManager.closeStream(streamId);
      } else {
        logError("Mux", `未知消息类型: ${msgType}`);
      }
    } catch (err) {
      logError("Handler", err.message);
      cleanup();
    }
  });

  webSocket.addEventListener("close", () => {
    log("WS", "连接已关闭", config.enableLogging);
    cleanup();
  });

  webSocket.addEventListener("error", (err) => {
    logError("WS", err?.message || "Unknown error");
    cleanup();
  });
}