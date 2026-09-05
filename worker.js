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
 * 出口顺序: 直连原始 host > 客户端 ?fallbackip= > 动态节点 API（env.DYNAMIC_NODES_URL）> 静态 fallback（env.FALLBACK_IPS，
 *   每项支持 host 或 host:port；无硬编码默认值）
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

// ==================== 默认配置（兜底值；一切配置统一从环境变量读取） ====================
const DEFAULT_CONFIG = {
  enableFallback: true,
  connectTimeout: 1000,
  enableLogging: false,
  maxStreamsPerConnection: 16,
  enableDynamicNodes: true,
  dynamicNodesUrl: "",
  dynamicNodesTimeout: 3000, // 拉取动态列表超时 3s，失败直接降级
};

function parseEnvBool(v, fallback) {
  if (v === undefined || v === null || String(v).trim() === "") return fallback;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(s)) return true;
  if (["0", "false", "no", "off"].includes(s)) return false;
  return fallback;
}

function parseEnvInt(v, fallback, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const n = parseInt(String(v ?? "").trim(), 10);
  if (!Number.isSafeInteger(n) || n < min || n > max) return fallback;
  return n;
}

// 一切配置从环境变量组装，不依赖 KV
function buildConfigFromEnv(env) {
  return {
    enableFallback: parseEnvBool(env.ENABLE_FALLBACK, DEFAULT_CONFIG.enableFallback),
    connectTimeout: parseEnvInt(env.CONNECT_TIMEOUT, DEFAULT_CONFIG.connectTimeout, { min: 100, max: 30000 }),
    enableLogging: parseEnvBool(env.ENABLE_LOGGING, DEFAULT_CONFIG.enableLogging),
    maxStreamsPerConnection: parseEnvInt(env.MAX_STREAMS_PER_CONNECTION, DEFAULT_CONFIG.maxStreamsPerConnection, { min: 1, max: 256 }),
    enableDynamicNodes: parseEnvBool(env.ENABLE_DYNAMIC_NODES, DEFAULT_CONFIG.enableDynamicNodes),
    dynamicNodesUrl: (env.DYNAMIC_NODES_URL || "").trim(),
    dynamicNodesTimeout: parseEnvInt(env.DYNAMIC_NODES_TIMEOUT, DEFAULT_CONFIG.dynamicNodesTimeout, { min: 500, max: 30000 }),
  };
}

// 保留空数组仅为兼容历史引用，静态 fallback 只从环境变量 FALLBACK_IPS 读取
const DEFAULT_FALLBACK_IPS = [];

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

// 解析 fallback 条目，支持 `host`、`host:port`、`[ipv6]`、`[ipv6]:port`
// 不带端口时继承目标端口（保持旧行为）
function parseFallbackEntry(entry, defaultPort) {
  const s = String(entry || "").trim();
  // [ipv6] 或 [ipv6]:port
  if (s[0] === "[") {
    const end = s.indexOf("]");
    if (end === -1) return null;
    const host = s.substring(1, end);
    if (!host) return null;
    const rest = s.substring(end + 1);
    if (!rest) return { host, port: defaultPort };
    if (rest[0] !== ":") return null;
    const port = parseInt(rest.substring(1), 10);
    if (!Number.isSafeInteger(port) || port <= 0 || port > 65535) return null;
    return { host, port };
  }
  // host / host:port（IPv6 裸地址必须加方括号，否则冒号无法区分端口）
  const sep = s.lastIndexOf(":");
  if (sep === -1) {
    if (!s) return null;
    return { host: s, port: defaultPort };
  }
  const port = parseInt(s.substring(sep + 1), 10);
  if (!Number.isSafeInteger(port) || port <= 0 || port > 65535) {
    // 冒号后缀不是合法端口：视为非法条目，跳过（避免把裸 IPv6 误解析）
    return null;
  }
  const host = s.substring(0, sep);
  if (!host) return null;
  return { host, port };
}

// ==================== 动态兜底节点 ====================
// Workers isolate 生命周期短，不做 TTL 缓存：每次用到都现拉 API。
// 仅保留单飞（并发流共用一次请求）+ 失败时复用上次结果的 stale 降级。
let dynamicNodesCache = { entries: [], inflight: null };

function normalizeNodeEntry(host, port) {
  let h = String(host ?? "").trim();
  if (!h) return null;
  // API 可能返回裸 IPv6，统一加方括号，后续走 parseFallbackEntry 解析
  if (h.includes(":") && h[0] !== "[") h = `[${h}]`;
  if (port !== undefined && port !== null && String(port).trim() !== "") {
    const p = parseInt(port, 10);
    if (!Number.isSafeInteger(p) || p <= 0 || p > 65535) return null;
    return `${h}:${p}`;
  }
  return h;
}

async function fetchDynamicNodes(config) {
  // 地址统一从环境变量读取，无硬编码默认值；未配置则直接降级为空
  const url = (config.dynamicNodesUrl || "").trim();
  if (!url) return [];
  const timeoutMs = config.dynamicNodesTimeout || 3000;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const arr = Array.isArray(data) ? data : data.nodes || data.data || [];
    const out = [];
    const seen = new Set();
    for (const item of arr) {
      if (!item || typeof item !== "object") continue;
      const entry = normalizeNodeEntry(
        item.ip || item.host || item.address,
        item.port,
      );
      if (!entry) continue;
      const key = entry.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(entry);
      if (out.length >= 20) break;
    }
    return out;
  } finally {
    clearTimeout(timer);
  }
}

async function getDynamicFallbacks(config) {
  if (config.enableDynamicNodes === false) return [];
  if (!(config.dynamicNodesUrl || "").trim()) return [];
  // 无 TTL：每次现拉，仅单飞合并并发请求
  if (dynamicNodesCache.inflight) {
    try {
      return await dynamicNodesCache.inflight;
    } catch {
      return dynamicNodesCache.entries;
    }
  }
  const p = fetchDynamicNodes(config)
    .then((entries) => {
      dynamicNodesCache.entries = entries;
      dynamicNodesCache.inflight = null;
      return entries;
    })
    .catch((err) => {
      console.error(`[DynamicNodes] 拉取失败: ${err.message}`);
      dynamicNodesCache.inflight = null;
      return dynamicNodesCache.entries; // 有 stale 用 stale，全新失败则为 []
    });
  dynamicNodesCache.inflight = p;
  return p;
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

    // 预注册 stream（乐观模式）：TCP 尚未连上时缓存客户端早期数据
    // 客户端在发 CONNECT 后会乐观发送 SOCKS5 成功并开始转发数据
    // 这些数据可能在 CONNECTED 之前到达，需要缓存等 TCP 连上后 flush
    this.streams.set(streamId, {
      remoteSocket: null,
      remoteWriter: null,
      remoteReader: null,
      isClosed: false,
      tcpConnected: false,
      pendingBuffer: [],
    });
    this.streamCount++;

    let { host, port } = parseAddress(targetAddr);
    // 出口顺序：直连 > 客户端传入 fallback > 动态节点 > 静态 fallback
    // 1. 直连优先
    {
      const r = await this.tryDial(streamId, host, port, "直连");
      if (r === "connected") return true;
      if (r === "aborted") return false;
    }

    if (this.config.enableFallback) {
      // 去重集合：前面已试过的，后面不再重复试
      const seenHosts = new Set();
      const markOrSeen = (parsed) => {
        const key = `${parsed.host.toLowerCase()}:${parsed.port}`;
        if (seenHosts.has(key)) return true;
        seenHosts.add(key);
        return false;
      };

      // 2. 客户端传入的 fallback（?fallbackip=）
      const queries = this.config.cfQueryFallbackIPs || [];
      for (let i = 0; i < queries.length; i++) {
        const parsed = parseFallbackEntry(queries[i], port);
        if (!parsed) {
          this.log(`[${streamId}] 跳过非法query-fallback[${i + 1}/${queries.length}]: ${queries[i]}`);
          continue;
        }
        if (markOrSeen(parsed)) continue;
        const r = await this.tryDial(
          streamId,
          parsed.host,
          parsed.port,
          `query-fallback[${i + 1}/${queries.length}]`,
        );
        if (r === "connected") return true;
        if (r === "aborted") return false;
      }

      // 3. 动态节点
      const dynamics = await getDynamicFallbacks(this.config);
      let dynIndex = 0;
      for (const entry of dynamics) {
        const parsed = parseFallbackEntry(entry, port);
        if (!parsed) continue;
        if (markOrSeen(parsed)) continue;
        dynIndex++;
        const r = await this.tryDial(
          streamId,
          parsed.host,
          parsed.port,
          `dynamic[${dynIndex}/${dynamics.length}]`,
        );
        if (r === "connected") return true;
        if (r === "aborted") return false;
      }

      // 4. 静态 fallback（仅 env.FALLBACK_IPS）
      const statics = this.config.cfFallbackIPs || [];
      for (let i = 0; i < statics.length; i++) {
        const parsed = parseFallbackEntry(statics[i], port);
        if (!parsed) {
          this.log(`[${streamId}] 跳过非法fallback[${i + 1}/${statics.length}]: ${statics[i]}`);
          continue;
        }
        if (markOrSeen(parsed)) continue;
        const r = await this.tryDial(
          streamId,
          parsed.host,
          parsed.port,
          `fallback[${i + 1}/${statics.length}]`,
        );
        if (r === "connected") return true;
        if (r === "aborted") return false;
      }
    }

    // 所有尝试都失败，清理预注册的 stream
    this.closeStream(streamId);
    return false;
  }

  /**
   * 单次拨号并绑定到流
   * @returns {Promise<"connected" | "aborted" | "failed">} connected=成功停手，aborted=流已没（停手不再试），failed=可试下一个
   */
  async tryDial(streamId, attemptHost, attemptPort, attemptDesc) {
    try {
      this.log(`[${streamId}] 尝试${attemptDesc}: ${attemptHost}:${attemptPort}`);

      const remoteSocket = connect({
        hostname: attemptHost,
        port: attemptPort,
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

      // 更新已预注册的 stream：绑定真实的 socket 并 flush 缓存数据
      const stream = this.streams.get(streamId);
      if (!stream || stream.isClosed) {
        // 在 TCP 连接过程中流已被关闭
        try { remoteWriter.releaseLock(); } catch {}
        try { remoteSocket.close(); } catch {}
        return "aborted";
      }
      stream.remoteSocket = remoteSocket;
      stream.remoteWriter = remoteWriter;
      stream.remoteReader = remoteReader;
      stream.tcpConnected = true;

      this.log(`[${streamId}] ${attemptDesc}成功`);

      // Flush 缓存的早期数据到远程 socket
      if (stream.pendingBuffer.length > 0) {
        this.log(`[${streamId}] flush ${stream.pendingBuffer.length} 条缓存数据`);
        for (const pending of stream.pendingBuffer) {
          try {
            await remoteWriter.write(pending);
          } catch (e) {
            this.log(`[${streamId}] flush 写入失败: ${e.message}`);
            this.closeStream(streamId);
            return "aborted";
          }
        }
        stream.pendingBuffer = [];
      }

      this.sendConnected(streamId);

      // 启动数据转发
      this.pumpRemoteToWebSocket(streamId, remoteReader);

      return "connected";
    } catch (err) {
      this.log(`[${streamId}] ${attemptDesc}失败: ${err.message}`);
      return "failed";
    }
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

    // TCP 尚未连上时缓存数据，等连接成功后 flush
    if (!stream.tcpConnected) {
      const bufData = data instanceof Uint8Array ? data : encoder.encode(data);
      stream.pendingBuffer.push(bufData);
      return true;
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

    // 清理缓存
    stream.pendingBuffer = [];

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
  async fetch(request, env) {
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

        // 2. 加载配置（仅环境变量，无 KV 依赖）
        const baseConfig = buildConfigFromEnv(env);

        // 3. 解析 Fallback IPs（地址统一从环境变量读取，无硬编码）：
        // ?fallbackip=（客户端传入，支持逗号分隔与重复参数，每项支持 host 或 host:port）
        // env.FALLBACK_IPS（服务端静态，唯一来源）
        // 尝试顺序由 StreamManager.createStream() 按 直连 > 客户端 > 动态 > 静态 执行
        const splitList = (v) =>
          String(v || "")
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
        const dedupList = (arr) => {
          const out = [];
          const seen = new Set();
          for (const h of arr) {
            const key = h.toLowerCase();
            if (!seen.has(key)) {
              seen.add(key);
              out.push(h);
            }
          }
          return out;
        };
        const queryFallbackIPs = dedupList(
          url.searchParams.getAll("fallbackip").flatMap(splitList),
        );
        const envFallbackIPs = splitList(env.FALLBACK_IPS);
        // 静态 fallback 只从环境变量读取，不再合并硬编码默认值
        const staticFallbackIPs = dedupList([...envFallbackIPs]);

        const config = {
          ...baseConfig,
          cfQueryFallbackIPs: queryFallbackIPs,
          cfFallbackIPs: staticFallbackIPs,
        };

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