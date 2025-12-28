/**
 * Cloudflare Worker Proxy - 高级 Node.js 客户端
 *
 * 功能特性:
 * 1. 自动连接池管理 (心跳检测、空闲销毁 5分钟)
 * 2. 智能中转节点优选 (支持 IP/域名/端口混合格式，选 Top 2)
 * 3. 高级 DNS 策略 (支持 DoH 预解析，防止 DNS 污染)
 * 4. IPv6 自动兼容处理
 * 5. 命令行参数支持
 *
 * 依赖安装:
 *   npm install ws
 */

const WebSocket = require('ws');
const net = require('net');
const dns = require('dns').promises;
const https = require('https');
const { URL } = require('url');
const { EventEmitter } = require('events');
const fs = require('fs');
const path = require('path');

// --- 命令行参数解析 ---
function parseCommandLineArgs() {
    const args = process.argv.slice(2);
    const parsed = {
        configFile: null,
        workerHost: null,
        dohUrl: null,
        localPort: null,
        logLevel: null,
        proxyToken: null,
        minPoolSize: null,
        maxPoolSize: null,
        relayIPs: null,
        enableDoH: null,
        enableMetrics: null,
        metricsPort: null,
        enablePoolWarmup: null,
        enableAutoReconnect: null,
        enableDynamicPool: null,
        enableLogFile: null,
        logFilePath: null,
        tunnelTimeout: null,
        enableMultiplex: null,
        help: false
    };

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];
        const nextArg = args[i + 1];

        switch (arg) {
            case '--config':
            case '-c':
                parsed.configFile = nextArg;
                i++;
                break;
            case '--worker':
            case '-w':
                parsed.workerHost = nextArg;
                i++;
                break;
            case '--doh':
            case '-d':
                parsed.dohUrl = nextArg;
                i++;
                break;
            case '--port':
            case '-p':
                parsed.localPort = parseInt(nextArg, 10);
                i++;
                break;
            case '--log-level':
            case '-l':
                parsed.logLevel = nextArg.toUpperCase();
                i++;
                break;
            case '--token':
            case '-t':
                parsed.proxyToken = nextArg;
                i++;
                break;
            case '--min-pool':
                parsed.minPoolSize = parseInt(nextArg, 10);
                i++;
                break;
            case '--max-pool':
                parsed.maxPoolSize = parseInt(nextArg, 10);
                i++;
                break;
            case '--relay':
            case '-r':
                parsed.relayIPs = nextArg.split(',').map(s => s.trim());
                i++;
                break;
            case '--no-doh':
                parsed.enableDoH = false;
                break;
            case '--metrics':
                parsed.enableMetrics = true;
                break;
            case '--metrics-port':
                parsed.metricsPort = parseInt(nextArg, 10);
                i++;
                break;
            case '--no-warmup':
                parsed.enablePoolWarmup = false;
                break;
            case '--no-reconnect':
                parsed.enableAutoReconnect = false;
                break;
            case '--no-dynamic-pool':
                parsed.enableDynamicPool = false;
                break;
            case '--log-file':
                parsed.enableLogFile = true;
                parsed.logFilePath = nextArg;
                i++;
                break;
            case '--timeout':
                parsed.tunnelTimeout = parseInt(nextArg, 10) * 1000;
                i++;
                break;
            case '--no-mux':
                parsed.enableMultiplex = false;
                break;
            case '--help':
            case '-h':
                parsed.help = true;
                break;
        }
    }

    return parsed;
}

function showHelp() {
    console.log(`
GCM - Cloudflare Worker Proxy 客户端

用法:
  node gcm.cjs [选项]

选项:
  配置文件:
  -c, --config <file>          配置文件路径 (可选，不指定则使用默认配置)

  基本配置:
  -w, --worker <host>          Worker 地址 (默认: gcm.ics.de5.net)
  -p, --port <number>          SOCKS5 监听端口 (默认: 1080)
  -t, --token <token>          代理访问令牌 (可选)
  -l, --log-level <level>      日志级别: DEBUG/INFO/WARN/ERROR (默认: INFO)

  DNS配置:
  -d, --doh <url>              DoH 服务地址 (默认: https://1.1.1.1/dns-query)
      --no-doh                 禁用 DoH

  中转节点配置:
  -r, --relay <ip1,ip2,...>    中转节点列表，逗号分隔
      --min-pool <number>      最小连接池大小 (默认: 3)
      --max-pool <number>      最大连接池大小 (默认: 15)

  Metrics配置:
      --metrics                启用 Metrics 端点 (默认: 禁用)
      --metrics-port <number>  Metrics 端口 (默认: 9090)

  连接池配置:
      --no-warmup              禁用连接池预热
      --no-reconnect           禁用断线自动重连
      --no-dynamic-pool        禁用动态池大小调整

  日志配置:
      --log-file <path>        启用日志文件输出到指定路径

  隧道配置:
      --timeout <seconds>      隧道超时时间（秒）(默认: 60)
      --no-mux                 禁用多路复用（需 Worker 端支持）(默认: 启用)

  其他:
  -h, --help                   显示帮助信息

示例:
  node gcm-single.cjs                                           # 使用默认配置
  node gcm-single.cjs -w example.com -p 1088                   # 自定义 Worker 和端口
  node gcm-single.cjs -c my-config.json                         # 使用指定配置文件
  node gcm-single.cjs -r "1.1.1.1:443,2.2.2.2:443" --min-pool 20  # 自定义中转节点
  node gcm-single.cjs --no-doh --log-file ./gcm.log              # 禁用 DoH，启用日志文件
  node gcm-single.cjs -w example.com -t myToken -l DEBUG        # 完整配置示例

配置文件:
  配置文件为可选项，通过 -c/--config 参数指定。
  命令行参数优先级高于配置文件。
  不指定配置文件时，使用代码内嵌的默认配置。
`);
}

// --- 全局配置 ---
// 默认配置
const DEFAULT_CONFIG = {
    workerHost: 'gcm.ics.de5.net',
    localPort: 1080,
    proxyToken: null,
    minPoolSize: 3,
    maxPoolSize: 15,
    connectionTTL: 5 * 60 * 1000,  // 5分钟 - 减少频繁销毁和重建
    relayIPs: ["36.140.124.162:10009", "v6.gh-proxy.org"],
    enableDoH: true,
    dohUrl: 'https://1.1.1.1/dns-query',
    connectionTimeout: 1000,
    dnsCacheTTL: 5 * 60 * 1000,
    dnsCacheCleanupInterval: 60 * 1000,
    relayMonitorInterval: 30 * 1000,
    relayMaxLatency: 500,
    relayFailureThreshold: 3,
    relayRescoreInterval: 10 * 60 * 1000,      // 后台备用重评间隔 (10分钟)
    relayForceRescoreCooldown: 60000,         // 强制重评冷却时间 (1分钟，防止频繁触发)
    heartbeatInterval: 15 * 1000,
    heartbeatTimeout: 1000,
    enableTcpNoDelay: true,
    logLevel: 'INFO',  // 日志级别: DEBUG, INFO, WARN, ERROR
    // Metrics 配置
    enableMetrics: false,  // 是否启用 metrics 端点
    metricsPort: 9090,    // metrics HTTP 端口
    // 连接池预热配置
    enablePoolWarmup: true,  // 是否启用连接池预热
    warmupConcurrency: 3,   // 预热并发数（一次创建多少个连接）
    warmupTimeout: 30000,    // 预热总超时时间 (ms)
    // 断线重连配置
    enableAutoReconnect: true,  // 是否启用断线自动重连
    maxReconnectAttempts: 3,    // 最大重连尝试次数
    reconnectDelay: 1000,       // 重连延迟 (ms)
    // 请求超时配置
    tunnelTimeout: 60000,       // SOCKS5 隧道超时时间 (ms, 默认60秒)
    // 连接池动态调整配置
    enableDynamicPool: true,    // 是否启用动态池大小调整
    dynamicPoolInterval: 60000, // 动态调整检查间隔 (ms, 默认60秒)
    dynamicPoolMinSize: 2,      // 动态调整时的最小值
    dynamicPoolMaxSize: 15,    // 动态调整时的最大值
    dynamicPoolLowThreshold: 0.3,  // 低负载阈值 (活跃/空闲比 < 30%)
    dynamicPoolHighThreshold: 0.8, // 高负载阈值 (活跃/空闲比 > 80%)
    // 日志文件配置
    enableLogFile: false,      // 是否启用日志文件输出
    logFilePath: './gcm.log',  // 日志文件路径
    logFileMaxSize: 10 * 1024 * 1024,  // 日志文件最大大小 (10MB)
    logFileBackupCount: 3,     // 保留的日志备份数量
    // 统计增强配置
    enableStats: true,         // 是否启用统计增强
    // 连接池复用/多路复用配置
    enableMultiplex: true,    // 是否启用多路复用（单连接多请求，需 Worker 端支持）
    maxStreamsPerConnection: 5, // 每个连接最大并发流数
};

// 日志级别常量
const LOG_LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };

// 解析命令行参数
const cliArgs = parseCommandLineArgs();

// 显示帮助信息
if (cliArgs.help) {
    showHelp();
    process.exit(0);
}

// 临时日志函数（用于配置加载阶段）
let currentLogLevel = LOG_LEVELS.INFO;
const tempLog = (scope, msg, level = 'INFO') => {
    const msgLevel = LOG_LEVELS[level] ?? LOG_LEVELS.INFO;
    if (msgLevel >= currentLogLevel) {
        const levelTag = level === 'DEBUG' ? 'D' : level === 'INFO' ? 'I' : level === 'WARN' ? 'W' : 'E';
        console.log(`[${new Date().toLocaleTimeString()}] [${levelTag}] [${scope}] ${msg}`);
    }
};

// 加载配置文件
function loadConfig() {
    let configPath = null;
    let config = { ...DEFAULT_CONFIG };

    // 检查是否指定了配置文件
    if (cliArgs.configFile) {
        configPath = cliArgs.configFile;
        if (fs.existsSync(configPath)) {
            try {
                const fileContent = fs.readFileSync(configPath, 'utf8');
                const userConfig = JSON.parse(fileContent);
                tempLog('Config', `从 ${configPath} 加载配置`, 'INFO');
                config = { ...config, ...userConfig };
            } catch (e) {
                tempLog('Config', `读取配置文件失败: ${e.message}，使用默认配置`, 'ERROR');
            }
        } else {
            tempLog('Config', `配置文件不存在: ${configPath}，使用默认配置`, 'WARN');
        }
    }

    // 应用命令行参数（优先级高于配置文件）
    if (cliArgs.workerHost) config.workerHost = cliArgs.workerHost;
    if (cliArgs.dohUrl) config.dohUrl = cliArgs.dohUrl;
    if (cliArgs.localPort) config.localPort = cliArgs.localPort;
    if (cliArgs.logLevel) config.logLevel = cliArgs.logLevel;
    if (cliArgs.proxyToken !== null) config.proxyToken = cliArgs.proxyToken || null;
    if (cliArgs.minPoolSize !== null) config.minPoolSize = cliArgs.minPoolSize;
    if (cliArgs.maxPoolSize !== null) config.maxPoolSize = cliArgs.maxPoolSize;
    if (cliArgs.relayIPs) config.relayIPs = cliArgs.relayIPs;
    if (cliArgs.enableDoH !== null) config.enableDoH = cliArgs.enableDoH;
    if (cliArgs.enableMetrics !== null) config.enableMetrics = cliArgs.enableMetrics;
    if (cliArgs.metricsPort !== null) config.metricsPort = cliArgs.metricsPort;
    if (cliArgs.enablePoolWarmup !== null) config.enablePoolWarmup = cliArgs.enablePoolWarmup;
    if (cliArgs.enableAutoReconnect !== null) config.enableAutoReconnect = cliArgs.enableAutoReconnect;
    if (cliArgs.enableDynamicPool !== null) config.enableDynamicPool = cliArgs.enableDynamicPool;
    if (cliArgs.enableLogFile) {
        config.enableLogFile = cliArgs.enableLogFile;
        if (cliArgs.logFilePath) config.logFilePath = cliArgs.logFilePath;
    }
    if (cliArgs.tunnelTimeout !== null) config.tunnelTimeout = cliArgs.tunnelTimeout;
    if (cliArgs.enableMultiplex) config.enableMultiplex = cliArgs.enableMultiplex;

    // 记录命令行参数覆盖
    const overrides = [];
    if (cliArgs.configFile) overrides.push(`config=${cliArgs.configFile}`);
    if (cliArgs.workerHost) overrides.push(`worker=${cliArgs.workerHost}`);
    if (cliArgs.dohUrl) overrides.push(`doh=${cliArgs.dohUrl}`);
    if (cliArgs.localPort) overrides.push(`port=${cliArgs.localPort}`);
    if (cliArgs.logLevel) overrides.push(`log-level=${cliArgs.logLevel}`);
    if (cliArgs.proxyToken !== null) overrides.push(`token=***`);
    if (cliArgs.minPoolSize !== null) overrides.push(`min-pool=${cliArgs.minPoolSize}`);
    if (cliArgs.maxPoolSize !== null) overrides.push(`max-pool=${cliArgs.maxPoolSize}`);
    if (cliArgs.relayIPs) overrides.push(`relay=${cliArgs.relayIPs.join(',')}`);
    if (cliArgs.enableDoH === false) overrides.push('no-doh');
    if (cliArgs.enableMetrics === false) overrides.push('no-metrics');
    if (cliArgs.metricsPort !== null) overrides.push(`metrics-port=${cliArgs.metricsPort}`);
    if (cliArgs.enablePoolWarmup === false) overrides.push('no-warmup');
    if (cliArgs.enableAutoReconnect === false) overrides.push('no-reconnect');
    if (cliArgs.enableDynamicPool === false) overrides.push('no-dynamic-pool');
    if (cliArgs.enableLogFile) overrides.push(`log-file=${cliArgs.logFilePath}`);
    if (cliArgs.tunnelTimeout !== null) overrides.push(`timeout=${cliArgs.tunnelTimeout/1000}s`);
    if (cliArgs.enableMultiplex) overrides.push('multiplex');
    if (overrides.length > 0) {
        tempLog('Config', `命令行参数: ${overrides.join(', ')}`, 'INFO');
    }

    return config;
}

const CONFIG = loadConfig();
currentLogLevel = LOG_LEVELS[CONFIG.logLevel] ?? LOG_LEVELS.INFO;

// --- 文件日志写入器 ---
class FileLogger {
    constructor() {
        this.enabled = CONFIG.enableLogFile;
        this.filePath = CONFIG.logFilePath;
        this.maxSize = CONFIG.logFileMaxSize;
        this.backupCount = CONFIG.logFileBackupCount;
        this.writeStream = null;
        this.currentSize = 0;
        this.writtenBytes = 0;  // 总写入字节数统计
        this.rotatedCount = 0;  // 轮转次数统计

        if (this.enabled) {
            this.init();
            tempLog('FileLogger', `文件日志已启用: ${this.filePath} (最大${(this.maxSize/1024/1024).toFixed(1)}MB, 保留${this.backupCount}个备份)`, 'INFO');
        } else {
            tempLog('FileLogger', '文件日志未启用', 'DEBUG');
        }
    }

    init() {
        try {
            // 检查并处理日志文件轮转
            if (fs.existsSync(this.filePath)) {
                const stats = fs.statSync(this.filePath);
                this.currentSize = stats.size;

                if (this.currentSize >= this.maxSize) {
                    tempLog('FileLogger', `初始化时检测到日志文件已满 (${(this.currentSize/1024).toFixed(0)}KB)，执行轮转`, 'DEBUG');
                    this.rotate();
                }
            }

            // 创建写入流（追加模式）
            this.writeStream = fs.createWriteStream(this.filePath, { flags: 'a' });

            this.writeStream.on('error', (err) => {
                console.error(`文件日志写入错误: ${err.message}`);
            });

            tempLog('FileLogger', `日志文件流已创建，当前大小: ${(this.currentSize/1024).toFixed(0)}KB`, 'DEBUG');

        } catch (e) {
            tempLog('FileLogger', `初始化失败: ${e.message}`, 'ERROR');
            this.enabled = false;
        }
    }

    rotate() {
        const startTime = Date.now();
        this.rotatedCount++;

        tempLog('FileLogger', `开始日志轮转 (第${this.rotatedCount}次)...`, 'DEBUG');

        // 删除最老的备份
        const oldestBackup = `${this.filePath}.${this.backupCount}`;
        if (fs.existsSync(oldestBackup)) {
            fs.unlinkSync(oldestBackup);
            tempLog('FileLogger', `已删除最老备份: ${oldestBackup}`, 'DEBUG');
        }

        // 轮转现有备份
        let rotated = 0;
        for (let i = this.backupCount - 1; i >= 1; i--) {
            const currentBackup = `${this.filePath}.${i}`;
            const nextBackup = `${this.filePath}.${i + 1}`;
            if (fs.existsSync(currentBackup)) {
                fs.renameSync(currentBackup, nextBackup);
                rotated++;
            }
        }

        // 将当前日志文件重命名为 .1
        if (fs.existsSync(this.filePath)) {
            const stats = fs.statSync(this.filePath);
            fs.renameSync(this.filePath, `${this.filePath}.1`);
        }

        this.currentSize = 0;
        const elapsed = Date.now() - startTime;
        tempLog('FileLogger', `日志轮转完成，耗时${elapsed}ms，轮转${rotated}个备份文件`, 'DEBUG');
    }

    write(message) {
        if (!this.enabled || !this.writeStream) {
            return;
        }

        const timestamp = new Date().toISOString();
        const logLine = `[${timestamp}] ${message}\n`;
        const lineSize = Buffer.byteLength(logLine, 'utf8');

        this.currentSize += lineSize;
        this.writtenBytes += lineSize;

        // 检查是否需要轮转
        if (this.currentSize >= this.maxSize) {
            this.rotate();
            // 重新初始化写入流
            this.writeStream.end();
            this.writeStream = fs.createWriteStream(this.filePath, { flags: 'a' });
        }

        this.writeStream.write(logLine);
    }

    getStats() {
        return {
            enabled: this.enabled,
            filePath: this.filePath,
            currentSize: this.currentSize,
            writtenBytes: this.writtenBytes,
            rotatedCount: this.rotatedCount,
            utilization: ((this.currentSize / this.maxSize) * 100).toFixed(1) + '%'
        };
    }

    close() {
        if (this.writeStream) {
            const stats = this.getStats();
            tempLog('FileLogger', `关闭文件日志: 总写入${(stats.writtenBytes/1024).toFixed(1)}KB，轮转${stats.rotatedCount}次`, 'INFO');
            this.writeStream.end();
            this.writeStream = null;
        }
    }
}

// 创建文件日志实例
const fileLogger = new FileLogger();

// --- 日志工具 ---
// 获取当前日志级别数值
function getCurrentLogLevel() {
    const level = CONFIG.logLevel?.toUpperCase() || 'INFO';
    return LOG_LEVELS[level] ?? LOG_LEVELS.INFO;
}

// 日志函数
const log = (scope, msg, level = 'INFO') => {
    const currentLevel = getCurrentLogLevel();
    const msgLevel = LOG_LEVELS[level] ?? LOG_LEVELS.INFO;
    if (msgLevel >= currentLevel) {
        const levelTag = level === 'DEBUG' ? 'D' : level === 'INFO' ? 'I' : level === 'WARN' ? 'W' : 'E';
        const logMsg = `[${levelTag}] [${scope}] ${msg}`;
        console.log(`[${new Date().toLocaleTimeString()}] ${logMsg}`);

        // 写入文件
        fileLogger.write(logMsg);
    }
};

// 便捷函数
const logDebug = (scope, msg) => log(scope, msg, 'DEBUG');
const logInfo = (scope, msg) => log(scope, msg, 'INFO');
const logWarn = (scope, msg) => log(scope, msg, 'WARN');
const logError = (scope, msg) => log(scope, msg, 'ERROR');

// 向后兼容（旧代码使用 log/err）
const err = (scope, msg) => logError(scope, msg);

/**
 * DoH 客户端
 * 用于安全的 DNS 解析 (优化超时: 1秒快速失败)
 */
class DoHClient {
    static async resolve(domain, type = 'A') {
        if (!CONFIG.enableDoH) {
            logDebug('DoH', `DoH 未启用，跳过解析: ${domain} (${type})`);
            return null;
        }

        const startTime = Date.now();

        return new Promise((resolve) => {
            try {
                const dohUrl = new URL(CONFIG.dohUrl);
                dohUrl.searchParams.set('name', domain);
                dohUrl.searchParams.set('type', type);
                dohUrl.searchParams.set('ct', 'application/dns-json');

                logDebug('DoH', `正在解析: ${domain} (${type}) via ${CONFIG.dohUrl}`);

                const req = https.request(dohUrl, {
                    method: 'GET',
                    headers: { 'Accept': 'application/dns-json' },
                    timeout: 1000  // 优化: 3秒 -> 1秒，快速失败
                }, (res) => {
                    let data = '';
                    res.on('data', chunk => data += chunk);
                    res.on('end', () => {
                        const elapsed = Date.now() - startTime;
                        try {
                            const json = JSON.parse(data);
                            if (json.Answer && json.Answer.length > 0) {
                                const record = json.Answer.find(r => r.type === (type === 'A' ? 1 : 28));
                                if (record) {
                                    logDebug('DoH', `解析成功: ${domain} -> ${record.data} (${type}), 耗时${elapsed}ms`);
                                    resolve(record.data);
                                    return;
                                }
                            }
                            logDebug('DoH', `解析无结果: ${domain} (${type}), 耗时${elapsed}ms`);
                            resolve(null);
                        } catch (e) {
                            logDebug('DoH', `解析响应失败: ${e.message}, 耗时${elapsed}ms`);
                            resolve(null);
                        }
                    });
                });

                req.on('error', (e) => {
                    const elapsed = Date.now() - startTime;
                    logDebug('DoH', `请求错误: ${e.message}, 耗时${elapsed}ms`);
                    resolve(null);
                });
                req.on('timeout', () => {
                    const elapsed = Date.now() - startTime;
                    logDebug('DoH', `请求超时 (${CONFIG.dohUrl}), 耗时${elapsed}ms`);
                    req.destroy();
                    resolve(null);
                });
                req.end();
            } catch (e) {
                const elapsed = Date.now() - startTime;
                logDebug('DoH', `异常: ${e.message}, 耗时${elapsed}ms`);
                resolve(null);
            }
        });
    }
}

/**
 * DNS 缓存管理器
 * 缓存 DoH 解析结果，减少重复查询延迟
 */
class DNSCache {
    constructor() {
        this.cache = new Map(); // { 'example.com:A': { ip, expiresAt } }
        this.stats = { hits: 0, misses: 0 };
        this.lastCleanupTime = Date.now();

        // 定期清理过期缓存
        setInterval(() => this.cleanup(), CONFIG.dnsCacheCleanupInterval);
        logDebug('DNSCache', `DNS缓存已初始化 (TTL: ${CONFIG.dnsCacheTTL/1000}秒, 清理间隔: ${CONFIG.dnsCacheCleanupInterval/1000}秒)`);
    }

    /**
     * 生成缓存键
     */
    getKey(domain, type) {
        return `${domain}:${type}`;
    }

    /**
     * 获取缓存的 IP
     */
    get(domain, type = 'A') {
        const key = this.getKey(domain, type);
        const entry = this.cache.get(key);

        if (entry) {
            if (Date.now() < entry.expiresAt) {
                this.stats.hits++;
                const ttl = Math.floor((entry.expiresAt - Date.now()) / 1000);
                logDebug('DNSCache', `缓存命中: ${domain} (${type}) -> ${entry.ip} (TTL:${ttl}s)`);
                return entry.ip;
            } else {
                // 过期，删除
                logDebug('DNSCache', `缓存过期: ${domain} (${type})`);
                this.cache.delete(key);
            }
        }

        this.stats.misses++;
        return null;
    }

    /**
     * 设置缓存
     */
    set(domain, type, ip) {
        if (!ip) return;
        const key = this.getKey(domain, type);
        const expiresAt = Date.now() + CONFIG.dnsCacheTTL;
        this.cache.set(key, {
            ip,
            expiresAt
        });
        logDebug('DNSCache', `缓存添加: ${domain} (${type}) -> ${ip} (TTL:${CONFIG.dnsCacheTTL/1000}s)`);
    }

    /**
     * 清理过期缓存
     */
    cleanup() {
        const startTime = Date.now();
        const now = Date.now();
        let cleaned = 0;
        const beforeSize = this.cache.size;

        for (const [key, entry] of this.cache.entries()) {
            if (now >= entry.expiresAt) {
                this.cache.delete(key);
                cleaned++;
            }
        }

        if (cleaned > 0 || beforeSize > 0) {
            const elapsed = Date.now() - startTime;
            logDebug('DNSCache', `清理完成: 清除${cleaned}条过期缓存, 剩余${this.cache.size}条, 耗时${elapsed}ms`);
        }

        this.lastCleanupTime = now;
    }

    /**
     * 获取缓存统计
     */
    getStats() {
        const total = this.stats.hits + this.stats.misses;
        const hitRate = total > 0 ? ((this.stats.hits / total) * 100).toFixed(1) : 0;
        return {
            size: this.cache.size,
            hits: this.stats.hits,
            misses: this.stats.misses,
            hitRate: hitRate + '%'
        };
    }

    /**
     * 预热缓存（常用域名）
     */
    async warmup(domains) {
        logInfo('DNSCache', `开始预热 ${domains.length} 个域名...`);
        const startTime = Date.now();
        let successCount = 0;

        for (const domain of domains) {
            const cachedA = await this.resolveCached(domain, 'A');
            if (cachedA) successCount++;
            const cachedAAAA = await this.resolveCached(domain, 'AAAA');
            if (cachedAAAA) successCount++;
        }

        const elapsed = Date.now() - startTime;
        logInfo('DNSCache', `预热完成: 成功${successCount}条, 当前缓存${this.cache.size}条, 耗时${elapsed}ms`);
    }

    /**
     * 带缓存的解析方法
     */
    async resolveCached(domain, type = 'A') {
        // 1. 先查缓存
        const cached = this.get(domain, type);
        if (cached) {
            return cached;
        }

        // 2. 缓存未命中，执行 DoH 查询
        const ip = await DoHClient.resolve(domain, type);
        if (ip) {
            this.set(domain, type, ip);
        }
        return ip;
    }
}

// 全局 DNS 缓存实例
const dnsCache = new DNSCache();

/**
 * 中转节点管理器 (动态优选版)
 * 支持格式: IP, IP:Port, Domain, Domain:Port
 */
class RelayManager {
    constructor(relayList) {
        this.rawRelays = relayList;
        this.optimalRelays = []; // [{ ip, port, latency, failureCount, lastCheck, score }]
        this.isInitialized = false;
        this.monitorTimer = null;
        this.rescoreTimer = null;
        this.totalTestCount = 0;  // 总测速次数
        this.totalRemovedCount = 0;  // 总移除节点数
        this.lastForceRescoreTime = 0;  // 上次强制重评时间
        logDebug('Relay', `中转节点管理器已初始化，配置节点数: ${relayList.length}`);
    }

    // 解析 "host:port" 或 "[ipv6]:port" 或 "host"
    parseHostPort(input) {
        let host = input;
        let port = 443; // 默认端口

        const lastColon = input.lastIndexOf(':');
        const closeBracket = input.lastIndexOf(']');

        // 如果有冒号，且冒号在方括号后面（针对 [ipv6]:port），或者是 ipv4/domain
        if (lastColon > -1 && lastColon > closeBracket) {
            const portPart = input.substring(lastColon + 1);
            if (/^\d+$/.test(portPart)) {
                port = parseInt(portPart, 10);
                host = input.substring(0, lastColon);
            }
        }

        // 去除 IPv6 包裹
        if (host.startsWith('[') && host.endsWith(']')) {
            host = host.slice(1, -1);
        }

        return { host, port };
    }

    async init() {
        const startTime = Date.now();

        if (this.rawRelays.length === 0) {
            logWarn('Relay', '未配置中转节点，将使用直连模式');
            this.isInitialized = true;
            return;
        }

        logInfo('Relay', `开始初始化中转节点，配置数: ${this.rawRelays.length}...`);
        const candidateNodes = [];

        // 1. 解析所有输入
        for (const raw of this.rawRelays) {
            const { host, port } = this.parseHostPort(raw);

            if (net.isIP(host)) {
                logDebug('Relay', `直接添加 IP 节点: ${host}:${port}`);
                candidateNodes.push({ ip: host, port, source: raw });
            } else {
                try {
                    logDebug('Relay', `正在解析域名: ${host} ...`);
                    const addresses = await dns.lookup(host, { all: true });

                    if (addresses.length > 0) {
                        logDebug('Relay', `域名 ${host} 解析到 ${addresses.length} 个 IP 地址`);
                        const domainCandidates = addresses.map(a => ({ ip: a.address, port, source: host }));
                        const testedDomainCandidates = await this.batchTestLatency(domainCandidates);

                        const bestOfDomain = testedDomainCandidates.slice(0, 2);
                        candidateNodes.push(...bestOfDomain);
                        logDebug('Relay', `域名 ${host} 优选了 ${bestOfDomain.length} 个节点`);
                    } else {
                        logWarn('Relay', `域名 ${host} 解析结果为空`);
                    }
                } catch (e) {
                    logError('Relay', `解析中转域名 ${host} 失败: ${e.message}`);
                }
            }
        }

        // 2. 初始测速并初始化节点状态
        logDebug('Relay', `开始批量测速 ${candidateNodes.length} 个候选节点...`);
        const results = await this.batchTestLatency(candidateNodes);
        this.totalTestCount += results.length;

        this.optimalRelays = results
            .filter(r => r.latency < CONFIG.relayMaxLatency)
            .map(r => ({
                ...r,
                failureCount: 0,
                lastCheck: Date.now(),
                score: this.calculateScore(r.latency, 0)
            }));

        const filteredCount = results.length - this.optimalRelays.length;
        const elapsed = Date.now() - startTime;

        if (this.optimalRelays.length > 0) {
            logInfo('Relay', `初始化完成: 有效节点${this.optimalRelays.length}个 (过滤${filteredCount}个高延迟节点), 耗时${elapsed}ms`);
            logInfo('Relay', `优选节点列表 (Top ${Math.min(5, this.optimalRelays.length)}):`);
            this.optimalRelays.slice(0, 5).forEach((r, i) => {
                logInfo('Relay', `  [${i+1}] ${r.ip}:${r.port} (${r.latency}ms, 分数:${r.score}) [来自: ${r.source}]`);
            });
        } else {
            logWarn('Relay', `未找到可用中转节点 (测速${results.length}个，全部超过${CONFIG.relayMaxLatency}ms阈值)，降级为直连模式`);
        }

        this.isInitialized = true;

        // 3. 启动后台备用重评（不主动切换节点，仅 ConnectionPool 失败时才切换）
        this.startRescoring();
        logDebug('Relay', `后台备用重评已启动 (间隔:${CONFIG.relayRescoreInterval/1000}秒)`);
    }

    /**
     * 计算节点分数 (越低越好)
     * 分数 = 延迟 + 失败惩罚
     */
    calculateScore(latency, failureCount) {
        return latency + (failureCount * 500); // 每次失败惩罚 500ms
    }

    /**
     * 启动全面重新评分 (定期重新测速所有节点)
     * 这是后台备用机制，不主动切换当前使用的节点
     */
    startRescoring() {
        this.rescoreTimer = setInterval(async () => {
            if (this.optimalRelays.length === 0) {
                logDebug('Relay', '无可用节点，跳过重新评分');
                return;
            }

            const startTime = Date.now();
            logInfo('Relay', `开始全面重新评分 ${this.optimalRelays.length} 个节点...`);

            const results = await this.batchTestLatency(this.optimalRelays);
            this.totalTestCount += results.length;

            const beforeCount = this.optimalRelays.length;

            // 更新节点信息
            this.optimalRelays = results
                .filter(r => r.latency < CONFIG.relayMaxLatency)
                .map(r => ({
                    ...r,
                    failureCount: 0,
                    lastCheck: Date.now(),
                    score: this.calculateScore(r.latency, 0)
                }));

            const elapsed = Date.now() - startTime;
            const removed = beforeCount - this.optimalRelays.length;
            logInfo('Relay', `重新评分完成: 有效${this.optimalRelays.length}个 (移除${removed}个), 耗时${elapsed}ms`);
            this.logTopRelays();

        }, CONFIG.relayRescoreInterval);
    }

    /**
     * 按分数重新排序
     */
    resortByScore() {
        const before = this.optimalRelays.map(r => ({ ip: r.ip, port: r.port, score: r.score }));
        this.optimalRelays.sort((a, b) => a.score - b.score);

        // 检查排序是否有变化
        let changed = false;
        for (let i = 0; i < before.length; i++) {
            if (before[i].score !== this.optimalRelays[i]?.score) {
                changed = true;
                break;
            }
        }

        if (changed) {
            logDebug('Relay', '节点排序已更新');
        }
    }

    /**
     * 记录失败（由连接池调用）
     */
    reportFailure(ip, port) {
        const idx = this.optimalRelays.findIndex(r => r.ip === ip && r.port === port);
        if (idx !== -1) {
            this.optimalRelays[idx].failureCount++;
            const prevScore = this.optimalRelays[idx].score;
            this.optimalRelays[idx].score = this.calculateScore(
                this.optimalRelays[idx].latency,
                this.optimalRelays[idx].failureCount
            );
            logDebug('Relay', `节点 ${ip}:${port} 失败报告: ${this.optimalRelays[idx].failureCount}/${CONFIG.relayFailureThreshold}, 分数: ${prevScore} -> ${this.optimalRelays[idx].score}`);

            if (this.optimalRelays[idx].failureCount >= CONFIG.relayFailureThreshold) {
                this.totalRemovedCount++;
                logWarn('Relay', `节点 ${ip}:${port} 连续失败 ${CONFIG.relayFailureThreshold} 次，已移除 (累计移除: ${this.totalRemovedCount})`);
                this.optimalRelays.splice(idx, 1);
            } else {
                this.resortByScore();
            }
        }
    }

    /**
     * 获取最优节点 (最低延迟优先)
     */
    getNextRelay() {
        if (!this.isInitialized || this.optimalRelays.length === 0) return null;

        // 返回分数最低（最优）的节点，不再轮询
        return this.optimalRelays[0];
    }

    /**
     * 单个节点测速
     */
    async testLatency(node) {
        return new Promise(resolve => {
            const start = Date.now();
            const socket = new net.Socket();
            socket.setTimeout(2000);

            socket.connect(node.port, node.ip, () => {
                const duration = Date.now() - start;
                socket.destroy();
                resolve(duration);
            });

            socket.on('error', () => resolve(9999));
            socket.on('timeout', () => {
                socket.destroy();
                resolve(9999);
            });
        });
    }

    /**
     * 批量测速
     */
    async batchTestLatency(nodes) {
        if (nodes.length === 0) return [];

        const tests = nodes.map(async (node) => {
            const latency = await this.testLatency(node);
            return { ...node, latency };
        });

        const results = await Promise.all(tests);
        return results.sort((a, b) => a.latency - b.latency);
    }

    /**
     * 输出当前 Top 节点
     */
    logTopRelays() {
        if (this.optimalRelays.length === 0) {
            logWarn('Relay', '无可用中转节点');
            return;
        }

        logInfo('Relay', `当前最优节点 (Top ${Math.min(5, this.optimalRelays.length)}):`);
        this.optimalRelays.slice(0, 5).forEach((r, i) => {
            logInfo('Relay', `  [${i+1}] ${r.ip}:${r.port} (延迟:${r.latency}ms, 失败:${r.failureCount}, 分数:${r.score}) [来自: ${r.source}]`);
        });
    }

    /**
     * 强制重新评分所有中转节点（由 ConnectionPool 在连接失败时调用）
     * @returns {Promise<boolean>} 是否成功完成重评并更新了节点
     */
    async forceRescore() {
        // 防抖检查
        const now = Date.now();
        if (this.lastForceRescoreTime && now - this.lastForceRescoreTime < CONFIG.relayForceRescoreCooldown) {
            logDebug('Relay', '强制重评冷却中，跳过本次重评');
            return false;
        }

        this.lastForceRescoreTime = now;

        // 保存当前最优节点用于比较
        const beforeBest = this.optimalRelays[0];
        logWarn('Relay', `触发强制重新评分 (原最优: ${beforeBest?.ip}:${beforeBest?.port} ${beforeBest?.latency}ms)...`);

        // 重新解析原始节点列表并测速
        const candidateNodes = [];
        for (const raw of this.rawRelays) {
            const { host, port } = this.parseHostPort(raw);
            if (net.isIP(host)) {
                candidateNodes.push({ ip: host, port, source: raw });
            } else {
                try {
                    const addresses = await dns.lookup(host, { all: true });
                    if (addresses.length > 0) {
                        const domainCandidates = addresses.map(a => ({ ip: a.address, port, source: host }));
                        const testedDomainCandidates = await this.batchTestLatency(domainCandidates);
                        candidateNodes.push(...testedDomainCandidates.slice(0, 2));
                    }
                } catch (e) {
                    logError('Relay', `解析域名 ${host} 失败: ${e.message}`);
                }
            }
        }

        // 批量测速并更新
        const results = await this.batchTestLatency(candidateNodes);
        this.totalTestCount += results.length;

        this.optimalRelays = results
            .filter(r => r.latency < CONFIG.relayMaxLatency)
            .map(r => ({
                ...r,
                failureCount: 0,
                lastCheck: now,
                score: this.calculateScore(r.latency, 0)
            }));

        // 按分数排序
        this.resortByScore();

        const afterBest = this.optimalRelays[0];
        logInfo('Relay', `强制重评完成: 有效节点${this.optimalRelays.length}个`);
        if (beforeBest && afterBest) {
            logInfo('Relay', `最优节点: ${beforeBest.ip}:${beforeBest.port}(${beforeBest.latency}ms) -> ${afterBest.ip}:${afterBest.port}(${afterBest.latency}ms)`);
        }

        return true;
    }

    /**
     * 获取当前最优中转节点
     */
    getCurrentBest() {
        if (!this.isInitialized || this.optimalRelays.length === 0) return null;
        return this.optimalRelays[0];
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            totalNodes: this.optimalRelays.length,
            totalTestCount: this.totalTestCount,
            totalRemovedCount: this.totalRemovedCount,
            avgLatency: this.optimalRelays.length > 0
                ? Math.round(this.optimalRelays.reduce((sum, r) => sum + r.latency, 0) / this.optimalRelays.length)
                : 0,
            bestLatency: this.optimalRelays.length > 0 ? this.optimalRelays[0].latency : 0,
            worstLatency: this.optimalRelays.length > 0 ? this.optimalRelays[this.optimalRelays.length - 1].latency : 0
        };
    }

    /**
     * 停止监控
     */
    destroy() {
        if (this.monitorTimer) clearInterval(this.monitorTimer);
        if (this.rescoreTimer) clearInterval(this.rescoreTimer);
        logDebug('Relay', '监控定时器已停止');
    }
}

/**
 * WebSocket 连接池 (带心跳保活)
 */
class ConnectionPool extends EventEmitter {
    constructor(relayManager) {
        super();
        this.relayManager = relayManager;
        this.pool = [];
        this.pendingConnections = 0; // 正在建立中的连接数
        this.activeConnections = 0;  // 正在被 SOCKS 使用的连接数
        this.activeConnectionsMap = new Map(); // ws -> connectionItem（跟踪活跃连接的完整信息）
        this.requestQueue = [];      // 等待连接的请求队列
        this.pendingHeartbeats = new Map(); // 待响应心跳 { id: { timestamp, timer } }
        this.reconnectTimer = null;  // 重连定时器
        this.dynamicPoolTimer = null; // 动态池调整定时器

        // 多路复用流管理: ws -> Map<streamId, { onMessage, onClose, onError }>
        this.streamHandlers = new Map();

        // 动态 minPoolSize（初始值为配置值）
        this.currentMinPoolSize = CONFIG.minPoolSize;

        // 中转节点缓存
        this.currentRelay = null;        // 缓存的当前中转节点
        this.lastRelayFetchTime = 0;     // 上次获取节点的时间
        this.isRelayInitializing = false; // 节点初始化锁

        // 统计数据
        this.stats = {
            requests: 0,           // 总请求数
            successes: 0,          // 成功请求数
            failures: 0,           // 失败请求数
            timeouts: 0,           // 超时请求数
            totalResponseTime: 0,  // 总响应时间 (ms)
            minResponseTime: Infinity,  // 最小响应时间
            maxResponseTime: 0,    // 最大响应时间
            bytesReceived: 0,      // 接收字节数
            bytesSent: 0,          // 发送字节数
            startTime: Date.now(), // 启动时间
            createdConnections: 0, // 创建的连接总数
            closedConnections: 0,  // 关闭的连接总数
        };

        logDebug('Pool', `连接池已初始化 (Min:${CONFIG.minPoolSize}, Max:${CONFIG.maxPoolSize}, 多路复用:${CONFIG.enableMultiplex ? '开启' : '关闭'})`);

        // 维护循环
        setInterval(() => this.maintainPool(), 1000);
        setInterval(() => this.cullOldConnections(), 5000);
        // 状态日志
        setInterval(() => this.logStats(), 10000);
        // 心跳检测
        setInterval(() => this.sendHeartbeat(), CONFIG.heartbeatInterval);
        // 动态池大小调整
        if (CONFIG.enableDynamicPool) {
            this.dynamicPoolTimer = setInterval(() => this.adjustPoolSize(), CONFIG.dynamicPoolInterval);
        }
    }

    /**
     * 连接池预热（启动时并发创建连接）
     */
    async warmup() {
        if (!CONFIG.enablePoolWarmup || this.currentMinPoolSize <= 0) {
            return;
        }

        logInfo('Pool', `开始预热连接池，目标: ${this.currentMinPoolSize} 个连接...`);

        const startTime = Date.now();
        const targetSize = this.currentMinPoolSize;
        const concurrency = CONFIG.warmupConcurrency;
        let created = 0;
        let failed = 0;

        // 分批次创建连接
        while (created < targetSize) {
            const remaining = targetSize - created;
            const batchSize = Math.min(remaining, concurrency);

            // 并发创建一批连接
            const promises = [];
            for (let i = 0; i < batchSize; i++) {
                promises.push(this.createConnection('预热'));
            }

            // 等待一批完成
            await Promise.allSettled(promises);

            // 统计当前空闲连接数
            const currentPoolSize = this.pool.length;
            created = currentPoolSize;

            // 检查超时
            if (Date.now() - startTime > CONFIG.warmupTimeout) {
                logWarn('Pool', `预热超时，已创建 ${created}/${targetSize} 个连接`);
                break;
            }

            // 如果已经达到目标，退出
            if (created >= targetSize) {
                break;
            }

            // 短暂等待，避免过快重试
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        const elapsed = Date.now() - startTime;
        logInfo('Pool', `预热完成，创建 ${created} 个连接，耗时 ${elapsed}ms`);

        // 初始化中转节点
        await this.initializeRelay();
    }

    /**
     * 初始化当前使用的节点
     */
    async initializeRelay() {
        if (this.isRelayInitializing) return;
        this.isRelayInitializing = true;

        try {
            this.currentRelay = this.relayManager.getCurrentBest();
            this.lastRelayFetchTime = Date.now();

            if (this.currentRelay) {
                logInfo('Pool', `当前中转节点: ${this.currentRelay.ip}:${this.currentRelay.port} (${this.currentRelay.latency}ms)`);
            } else {
                logWarn('Pool', '无可用的中转节点，将使用直连模式');
            }
        } finally {
            this.isRelayInitializing = false;
        }
    }

    /**
     * 安排重连（延迟执行，避免频繁重连）
     */
    scheduleReconnect(reason) {
        // 如果已有重连任务在等待，不再重复安排
        if (this.reconnectTimer) {
            return;
        }

        this.reconnectTimer = setTimeout(async () => {
            this.reconnectTimer = null;

            // 检查当前连接数，如果已达到最小值，不需要重连
            const currentSize = this.pool.length + this.activeConnections + this.pendingConnections;
            if (currentSize >= this.currentMinPoolSize) {
                return;
            }

            // 尝试重连
            logDebug('Pool', `${reason} 触发，补充连接...`);
            await this.createConnection(reason);
        }, CONFIG.reconnectDelay);
    }

    /**
     * 发送心跳包到所有空闲连接
     */
    sendHeartbeat() {
        const now = Date.now();
        let sent = 0;
        let timeout = 0;

        for (const item of this.pool) {
            if (item.ws.readyState === WebSocket.OPEN) {
                // 检查是否有待响应的心跳
                if (this.pendingHeartbeats.has(item.connectionId)) {
                    const pending = this.pendingHeartbeats.get(item.connectionId);
                    if (now - pending.timestamp > CONFIG.heartbeatTimeout) {
                        // 心跳超时，连接假死，移除
                        logDebug('Heartbeat', `连接 [${item.connectionId}] 心跳超时，移除`);
                        try { item.ws.terminate(); } catch {}
                        this.pendingHeartbeats.delete(item.connectionId);
                        timeout++;
                    }
                } else {
                    // 发送新心跳
                    try {
                        item.ws.ping();
                        this.pendingHeartbeats.set(item.connectionId, { timestamp: now });
                        sent++;
                    } catch (e) {
                        // 发送失败，连接可能已断开
                        try { item.ws.terminate(); } catch {}
                    }
                }
            }
        }

        if (sent > 0 || timeout > 0) {
            // log('Heartbeat', `发送: ${sent}, 超时: ${timeout}, 活跃: ${this.pool.length}`);
        }
    }

    /**
     * 处理心跳响应 (pong)
     */
    handlePong(ws, id) {
        if (this.pendingHeartbeats.has(id)) {
            this.pendingHeartbeats.delete(id);
        }
    }

    logStats() {
        const total = this.pool.length + this.activeConnections + this.pendingConnections;
        if (total > 0) {
            logDebug('Stats', `连接池状态: 空闲 ${this.pool.length} | 活跃 ${this.activeConnections} | 建立中 ${this.pendingConnections} | 等待队列 ${this.requestQueue.length}`);

            // 输出所有连接的RTT延迟（DEBUG级别）- 包括空闲和活跃连接
            const allConnections = [];

            // 添加空闲连接
            for (const item of this.pool) {
                allConnections.push({ ...item, status: 'idle' });
            }

            // 添加活跃连接
            for (const [ws, item] of this.activeConnectionsMap) {
                allConnections.push({ ...item, status: 'active' });
            }

            if (allConnections.length > 0) {
                // 按 RTT 排序
                allConnections.sort((a, b) => (a.rtt || Infinity) - (b.rtt || Infinity));

                const rttList = allConnections.map(item => {
                    const connId = item.connectionId || 'N/A';
                    const rtt = item.rtt || 0;
                    const streams = item.streams || 0;
                    const status = item.status === 'idle' ? 'I' : 'A';
                    return `[${connId}:${rtt}ms:${streams}s:${status}]`;
                }).join(' ');
                logDebug('Stats', `所有连接RTT: ${rttList}`);
            }
        }

        // 每 30 秒输出一次详细统计
        if (Date.now() % 30000 < 10000) {
            const dnsStats = dnsCache.getStats();
            logInfo('Stats', `DNS缓存: ${dnsStats.size}条 | 命中率: ${dnsStats.hitRate} | 命中:${dnsStats.hits} 未命中:${dnsStats.misses}`);

            const relayStats = this.relayManager.getStats();
            if (relayStats.totalNodes > 0) {
                logInfo('Stats', `中转节点: ${relayStats.totalNodes}个 | 平均延迟:${relayStats.avgLatency}ms | 最佳:${relayStats.bestLatency}ms | 最差:${relayStats.worstLatency}ms`);
            }

            // 增强统计信息
            if (CONFIG.enableStats && this.stats.requests > 0) {
                const successRate = ((this.stats.successes / this.stats.requests) * 100).toFixed(1);
                const avgResponseTime = this.stats.successes > 0
                    ? Math.round(this.stats.totalResponseTime / this.stats.successes)
                    : 0;
                const uptime = Math.floor((Date.now() - this.stats.startTime) / 1000);
                logInfo('Stats', `请求: ${this.stats.requests}次 | 成功率: ${successRate}% | 平均响应: ${avgResponseTime}ms | 超时: ${this.stats.timeouts}次 | 上传: ${(this.stats.bytesSent/1024).toFixed(1)}KB | 下载: ${(this.stats.bytesReceived/1024).toFixed(1)}KB | 运行: ${uptime}秒`);
            }
        }
    }

    /**
     * 获取增强统计信息（用于 Metrics 端点）
     */
    getEnhancedStats() {
        const uptime = Date.now() - this.stats.startTime;
        const successRate = this.stats.requests > 0
            ? (this.stats.successes / this.stats.requests) * 100
            : 0;
        const avgResponseTime = this.stats.successes > 0
            ? this.stats.totalResponseTime / this.stats.successes
            : 0;

        return {
            ...this.stats,
            uptime,
            successRate,
            avgResponseTime,
            currentPoolSize: this.pool.length,
            activeConnections: this.activeConnections,
            pendingConnections: this.pendingConnections,
            queuedRequests: this.requestQueue.length,
        };
    }

    /**
     * 记录请求开始
     */
    recordRequestStart() {
        this.stats.requests++;
        return Date.now();
    }

    /**
     * 记录请求成功
     */
    recordRequestSuccess(startTime) {
        this.stats.successes++;
        const responseTime = Date.now() - startTime;
        this.stats.totalResponseTime += responseTime;
        if (responseTime < this.stats.minResponseTime) this.stats.minResponseTime = responseTime;
        if (responseTime > this.stats.maxResponseTime) this.stats.maxResponseTime = responseTime;
    }

    /**
     * 记录请求失败
     */
    recordRequestFailure() {
        this.stats.failures++;
    }

    /**
     * 记录请求超时
     */
    recordRequestTimeout() {
        this.stats.timeouts++;
    }

    /**
     * 记录数据传输
     */
    recordDataTransfer(bytesSent, bytesReceived) {
        this.stats.bytesSent += bytesSent;
        this.stats.bytesReceived += bytesReceived;
    }

    // 维护连接池大小
    async maintainPool() {
        // 当前总数 = 空闲 + 活跃 + 正在建立
        const currentSize = this.pool.length + this.activeConnections + this.pendingConnections;

        if (currentSize < this.currentMinPoolSize) {
            this.createConnection('维护补给');
        } else if (this.requestQueue.length > 0 && currentSize < CONFIG.maxPoolSize) {
            // 如果有积压请求且未达最大上限，激进创建
            this.createConnection('按需扩容');
        }
    }

    /**
     * 动态调整连接池大小（根据负载）
     */
    adjustPoolSize() {
        const idle = this.pool.length;
        const active = this.activeConnections;
        const total = idle + active;

        if (total === 0) {
            return; // 无连接数据，不调整
        }

        // 计算活跃连接比例
        const activeRatio = active / total;

        // 计算当前使用率（包括等待队列）
        const queued = this.requestQueue.length;
        const utilizationRatio = (active + queued) / (total + queued);

        let newSize = this.currentMinPoolSize;
        let reason = '';

        if (utilizationRatio > CONFIG.dynamicPoolHighThreshold) {
            // 高负载：增加连接池大小
            newSize = Math.min(
                Math.ceil(this.currentMinPoolSize * 1.5),
                CONFIG.dynamicPoolMaxSize
            );
            reason = `高负载 (利用率 ${(utilizationRatio * 100).toFixed(1)}%)`;
        } else if (activeRatio < CONFIG.dynamicPoolLowThreshold && this.currentMinPoolSize > CONFIG.dynamicPoolMinSize) {
            // 低负载：减少连接池大小
            newSize = Math.max(
                Math.floor(this.currentMinPoolSize * 0.7),
                CONFIG.dynamicPoolMinSize
            );
            reason = `低负载 (活跃率 ${(activeRatio * 100).toFixed(1)}%)`;
        } else {
            // 负载正常，不调整
            return;
        }

        if (newSize !== this.currentMinPoolSize) {
            logInfo('DynamicPool', `调整 minPoolSize: ${this.currentMinPoolSize} -> ${newSize} (${reason})`);
            this.currentMinPoolSize = newSize;

            // 如果是扩容，立即创建新连接
            if (newSize > total) {
                const needed = newSize - total;
                for (let i = 0; i < Math.min(needed, 5); i++) {
                    this.createConnection('动态扩容');
                }
            }
        }
    }

    cullOldConnections() {
        const now = Date.now();
        const beforeSize = this.pool.length;

        if (beforeSize === 0) return;

        // 仅清理空闲连接，但保留至少 minPoolSize 个连接
        // 先按创建时间排序（最老的在前）
        const sortedPool = [...this.pool].sort((a, b) => a.createdAt - b.createdAt);

        // 找出需要保留的最小数量（取 minPoolSize 和 currentMinPoolSize 中的较小值）
        const keepMin = Math.min(CONFIG.minPoolSize, this.currentMinPoolSize);

        // 标记需要清理的连接（超过TTL且不是前keepMin个）
        const toRemove = new Set();
        for (let i = keepMin; i < sortedPool.length; i++) {
            const item = sortedPool[i];
            if (now - item.createdAt > CONFIG.connectionTTL) {
                toRemove.add(item);
            }
        }

        // 清理标记的连接
        if (toRemove.size > 0) {
            this.pool = this.pool.filter(item => {
                if (toRemove.has(item)) {
                    try {
                        item.ws.terminate();
                        this.stats.closedConnections++;
                    } catch {}
                    return false;
                }
                return true;
            });
            logDebug('Pool', `清理过期连接: 清除${toRemove.size}个 (${beforeSize} -> ${this.pool.length})`);
        }
    }

    async createConnection(reason = '') {
        // 双重检查，防止瞬间并发超标
        const currentSize = this.pool.length + this.activeConnections + this.pendingConnections;
        if (currentSize >= CONFIG.maxPoolSize) {
            logDebug('Pool', `连接池已满 (${currentSize}/${CONFIG.maxPoolSize})，跳过创建: ${reason}`);
            return;
        }

        this.pendingConnections++;
        // 使用缓存的节点，如果不存在则获取
        if (!this.currentRelay) {
            this.currentRelay = this.relayManager.getCurrentBest();
        }
        const relay = this.currentRelay;
        this.stats.createdConnections++;

        let url;
        let options = {
            headers: {
                'Host': CONFIG.workerHost,
                'User-Agent': 'NodeClient/4.0'
            },
            servername: CONFIG.workerHost,
            rejectUnauthorized: false
        };

        if (CONFIG.proxyToken) {
            options.headers['Sec-WebSocket-Protocol'] = CONFIG.proxyToken;
        }

        if (relay) {
            // 支持 IPv6 字面量在 URL 中
            const ipStr = net.isIPv6(relay.ip) ? `[${relay.ip}]` : relay.ip;
            url = `wss://${ipStr}:${relay.port}/`;
            logDebug('Pool', `创建连接 (${reason}) -> 中转: ${relay.ip}:${relay.port}`);
        } else {
            url = `wss://${CONFIG.workerHost}/`;
            logDebug('Pool', `创建连接 (${reason}) -> 直连: ${CONFIG.workerHost}`);
        }

        try {
            const startT = Date.now();
            const ws = new WebSocket(url, options);
            // 生成6位16进制连接ID（与Socks5Server保持一致）
            const connectionId = Math.random().toString(16).substring(2, 8).padStart(6, '0');

            // 返回一个 Promise，当连接建立或失败时 resolve
            return new Promise((resolve) => {
                ws.on('open', () => {
                    const lat = Date.now() - startT;
                    this.pendingConnections--;

                    logDebug('Pool', `新连接 [${connectionId}] 已就绪 (${reason}), 握手延迟: ${lat}ms`);

                    // 如果有等待的请求，直接分配
                    if (this.requestQueue.length > 0) {
                        const req = this.requestQueue.shift();
                        this.activeConnections++;
                        // streams 将在 req.resolve 中被设置为 1
                        req.resolve({ ws, connectionId, createdAt: Date.now(), rtt: lat, streams: 0 });
                    } else {
                        this.pool.push({ ws, connectionId, createdAt: Date.now(), rtt: lat, streams: 0 });
                        this.emit('connection_available');
                    }
                    resolve(true); // 连接成功
                });

                ws.on('error', (e) => {
                    this.pendingConnections--;
                    this.stats.failures++;
                    logDebug('Pool', `连接失败 [${connectionId}] (${reason}): ${e.message}`);

                    // 触发强制重评（异步执行，不阻塞重连）
                    this.relayManager.forceRescore().then((success) => {
                        if (success && this.relayManager.getCurrentBest()) {
                            const oldRelay = this.currentRelay;
                            this.currentRelay = this.relayManager.getCurrentBest();
                            const changed = oldRelay?.ip !== this.currentRelay.ip || oldRelay?.port !== this.currentRelay.port;
                            if (changed) {
                                logInfo('Pool', `已切换中转节点: ${oldRelay?.ip}:${oldRelay?.port} -> ${this.currentRelay.ip}:${this.currentRelay.port} (${this.currentRelay.latency}ms)`);
                            }
                        }
                    }).catch(err => {
                        logError('Pool', `强制重评异常: ${err.message}`);
                    });

                    // 如果启用了断线重连，尝试补充连接
                    if (CONFIG.enableAutoReconnect) {
                        this.scheduleReconnect(reason);
                    }
                    resolve(false); // 连接失败
                });

                ws.on('close', () => {
                    // 清理心跳状态
                    this.pendingHeartbeats.delete(connectionId);
                    // 清理活跃连接映射
                    this.activeConnectionsMap.delete(ws);
                    // 仅从空闲池移除
                    const idx = this.pool.findIndex(p => p.connectionId === connectionId);
                    const wasInPool = idx !== -1;
                    if (wasInPool) {
                        this.pool.splice(idx, 1);
                        this.stats.closedConnections++;
                        // 空闲连接意外断开，触发重连
                        if (CONFIG.enableAutoReconnect) {
                            this.scheduleReconnect('断线重连');
                        }
                    }
                });

                // 心跳响应处理
                ws.on('pong', () => {
                    this.handlePong(ws, connectionId);
                });

                // TCP 优化: 设置 NODELAY（禁用 Nagle 算法）
                ws.on('socket', (socket) => {
                    if (socket && CONFIG.enableTcpNoDelay) {
                        socket.setNoDelay(true);
                    }
                });
            });

        } catch (e) {
            this.pendingConnections--;
            this.stats.failures++;
            logError('Pool', `创建连接异常 (${reason}): ${e.message}`);
            return false; // 连接失败
        }
    }

    // 获取连接 (Promise 模式，支持排队)
    // 始终使用流计数管理，但只有启用多路复用时才复用连接
    async getConnection() {
        // 1. 如果启用多路复用，首先检查活跃连接是否有可复用的
        if (CONFIG.enableMultiplex) {
            for (const [ws, item] of this.activeConnectionsMap) {
                // 检查连接是否可用且未达到最大流数
                if (ws.readyState === WebSocket.OPEN && item.streams < CONFIG.maxStreamsPerConnection) {
                    // 复用这个活跃连接
                    item.streams++;
                    // 注意：不增加 activeConnections，因为连接已经在活跃状态
                    // 不需要重新加入 activeConnectionsMap，因为已经在里面了
                    return item;
                }
            }
        }

        // 2. 从空闲池获取连接（按RTT排序选择）
        if (this.pool.length > 0) {
            // 按 RTT 排序所有空闲连接
            this.pool.sort((a, b) => (a.rtt || Infinity) - (b.rtt || Infinity));

            // 找到第一个 OPEN 状态的连接
            while (this.pool.length > 0) {
                const item = this.pool.shift();
                if (item.ws.readyState === WebSocket.OPEN) {
                    item.streams = (item.streams || 0) + 1;
                    this.activeConnections++;
                    // 记录到活跃连接映射
                    this.activeConnectionsMap.set(item.ws, item);
                    return item;
                }
                // 连接已关闭，跳过继续找下一个
            }
            // 没有找到可用连接，继续下一步
        }

        // 3. 如果没空闲，检查是否能新建
        const currentSize = this.pool.length + this.activeConnections + this.pendingConnections;
        if (currentSize < CONFIG.maxPoolSize) {
            this.createConnection('请求触发');
        }

        // 4. 加入等待队列
        return new Promise((resolve, reject) => {
            // 设置超时，防止无限等待
            const timer = setTimeout(() => {
                const idx = this.requestQueue.findIndex(r => r === reqEntry);
                if (idx !== -1) {
                    this.requestQueue.splice(idx, 1);
                    reject(new Error('Connection Pool Timeout'));
                }
            }, 10000);

            const reqEntry = {
                resolve: (conn) => {
                    clearTimeout(timer);
                    if (conn) {
                        conn.streams = (conn.streams || 0) + 1;
                        // 记录到活跃连接映射
                        this.activeConnectionsMap.set(conn.ws, conn);
                    }
                    resolve(conn);
                },
                reject
            };
            this.requestQueue.push(reqEntry);
        });
    }

    releaseConnection(ws) {
        // 检查连接是否还在活跃连接映射中
        if (!this.activeConnectionsMap.has(ws)) {
            // 已经被释放过了，直接返回
            return;
        }

        // 从活跃连接映射中获取完整连接信息
        const item = this.activeConnectionsMap.get(ws);
        if (!item) {
            // 没有找到，可能是旧代码路径，尝试兼容处理
            return;
        }

        // 减少流计数
        item.streams--;

        // 如果启用多路复用
        if (CONFIG.enableMultiplex) {
            // 多路复用模式：只在 streams = 0 时才放回池中
            if (item.streams <= 0) {
                // 没有活跃流了，从活跃连接映射移除，放回池中
                this.activeConnectionsMap.delete(ws);
                this.activeConnections--;
                item.streams = 0;
                this.pool.push(item);

                // 清理该连接上的所有流处理器
                this.streamHandlers.delete(ws);
            }
            // 如果 streams > 0，连接保持在 activeConnectionsMap 中，不放回池中
        } else {
            // 非多路复用模式：每次都放回池中
            this.activeConnectionsMap.delete(ws);
            this.activeConnections--;
            item.streams = 0;
            this.pool.push(item);
        }
    }

    /**
     * 注册流处理器（多路复用模式）
     * @param {WebSocket} ws WebSocket 连接
     * @param {string} streamId 流 ID
     * @param {Object} handlers 处理器 { onMessage, onClose, onError }
     */
    registerStreamHandlers(ws, streamId, handlers) {
        if (!this.streamHandlers.has(ws)) {
            // 首次为这个 WebSocket 注册流，创建流映射
            this.streamHandlers.set(ws, new Map());

            // 检查是否已经设置过统一的消息处理器
            if (!ws._hasMultiplexHandler) {
                ws._hasMultiplexHandler = true;

                // 为这个 WebSocket 设置统一的消息处理器（只设置一次）
                ws.on('message', (msg) => {
                    // 新协议: [WS_ID:3][STREAM_ID:1][TYPE:1][DATA...]
                    if (msg.length < 5) return;

                    const msgStreamId = msg[3].toString(16).padStart(2, '0');
                    const handlersMap = this.streamHandlers.get(ws);
                    if (handlersMap && handlersMap.has(msgStreamId)) {
                        const stream = handlersMap.get(msgStreamId);
                        if (stream.onMessage) {
                            stream.onMessage(msg);
                        }
                    }
                });

                // WebSocket 关闭时清理所有流
                ws.on('close', () => {
                    const handlersMap = this.streamHandlers.get(ws);
                    if (handlersMap) {
                        for (const [sid, stream] of handlersMap) {
                            if (stream.onClose) {
                                try { stream.onClose(); } catch {}
                            }
                        }
                    }
                    this.streamHandlers.delete(ws);
                    delete ws._hasMultiplexHandler;
                });

                // WebSocket 错误处理
                ws.on('error', () => {
                    const handlersMap = this.streamHandlers.get(ws);
                    if (handlersMap) {
                        for (const [sid, stream] of handlersMap) {
                            if (stream.onError) {
                                try { stream.onError(); } catch {}
                            }
                        }
                    }
                });
            }
        }

        const handlersMap = this.streamHandlers.get(ws);
        handlersMap.set(streamId, handlers);
    }

    /**
     * 注销流处理器（多路复用模式）
     * @param {WebSocket} ws WebSocket 连接
     * @param {string} streamId 流 ID
     */
    unregisterStreamHandlers(ws, streamId) {
        const handlersMap = this.streamHandlers.get(ws);
        if (handlersMap) {
            const stream = handlersMap.get(streamId);
            if (stream) {
                // 调用清理回调
                if (stream.onCleanup) {
                    stream.onCleanup();
                }
            }
            handlersMap.delete(streamId);
        }
    }
}

/**
 * Metrics 服务器 (Prometheus 格式)
 */
class MetricsServer {
    constructor(pool, relayManager, dnsCache) {
        this.pool = pool;
        this.relayManager = relayManager;
        this.dnsCache = dnsCache;
        this.server = null;
        this.requestCount = 0;  // 请求计数
        logDebug('Metrics', 'Metrics 服务器已创建');
    }

    start() {
        if (!CONFIG.enableMetrics) {
            logDebug('Metrics', 'Metrics 端点未启用');
            return;
        }

        const http = require('http');

        this.server = http.createServer((req, res) => {
            const clientAddr = req.socket.remoteAddress;
            this.requestCount++;

            if (req.url === '/metrics') {
                logDebug('Metrics', `Metrics 请求来自: ${clientAddr}`);
                res.setHeader('Content-Type', 'text/plain; charset=utf-8');
                res.writeHead(200);
                res.end(this.generateMetrics());
            } else if (req.url === '/health') {
                logDebug('Metrics', `健康检查来自: ${clientAddr}`);
                res.writeHead(200);
                res.end('OK\n');
            } else {
                logDebug('Metrics', `未知路径: ${req.url} 来自: ${clientAddr}`);
                res.writeHead(404);
                res.end('Not Found\n');
            }
        });

        this.server.listen(CONFIG.metricsPort, () => {
            logInfo('Metrics', `监听端口: http://0.0.0.0:${CONFIG.metricsPort}/metrics`);
            logInfo('Metrics', `端点: /metrics (Prometheus), /health (健康检查)`);
        });

        this.server.on('error', (e) => {
            logError('Metrics', `启动失败: ${e.message}`);
        });
    }

    generateMetrics() {
        const lines = [];
        const now = Date.now();

        // 连接池指标
        const poolIdle = this.pool.pool.length;
        const poolActive = this.pool.activeConnections;
        const poolPending = this.pool.pendingConnections;
        const poolQueued = this.pool.requestQueue.length;

        lines.push('# HELP gcm_pool_idle 连接池空闲连接数');
        lines.push('# TYPE gcm_pool_idle gauge');
        lines.push(`gcm_pool_idle ${poolIdle}`);

        lines.push('# HELP gcm_pool_active 连接池活跃连接数');
        lines.push('# TYPE gcm_pool_active gauge');
        lines.push(`gcm_pool_active ${poolActive}`);

        lines.push('# HELP gcm_pool_pending 连接池正在建立的连接数');
        lines.push('# TYPE gcm_pool_pending gauge');
        lines.push(`gcm_pool_pending ${poolPending}`);

        lines.push('# HELP gcm_pool_queued 连接池等待中的请求数');
        lines.push('# TYPE gcm_pool_queued gauge');
        lines.push(`gcm_pool_queued ${poolQueued}`);

        // DNS 缓存指标
        const dnsStats = this.dnsCache.getStats();
        lines.push('# HELP gcm_dns_cache_size DNS缓存条目数');
        lines.push('# TYPE gcm_dns_cache_size gauge');
        lines.push(`gcm_dns_cache_size ${dnsStats.size}`);

        lines.push('# HELP gcm_dns_cache_hits_total DNS缓存命中次数');
        lines.push('# TYPE gcm_dns_cache_hits_total counter');
        lines.push(`gcm_dns_cache_hits_total ${dnsStats.hits}`);

        lines.push('# HELP gcm_dns_cache_misses_total DNS缓存未命中次数');
        lines.push('# TYPE gcm_dns_cache_misses_total counter');
        lines.push(`gcm_dns_cache_misses_total ${dnsStats.misses}`);

        // 中转节点指标
        const relayStats = this.relayManager.getStats();
        lines.push('# HELP gcm_relay_nodes_total 中转节点总数');
        lines.push('# TYPE gcm_relay_nodes_total gauge');
        lines.push(`gcm_relay_nodes_total ${relayStats.totalNodes}`);

        lines.push('# HELP gcm_relay_latency_avg 中转节点平均延迟(毫秒)');
        lines.push('# TYPE gcm_relay_latency_avg gauge');
        lines.push(`gcm_relay_latency_avg ${relayStats.avgLatency}`);

        lines.push('# HELP gcm_relay_latency_best 中转节点最低延迟(毫秒)');
        lines.push('# TYPE gcm_relay_latency_best gauge');
        lines.push(`gcm_relay_latency_best ${relayStats.bestLatency}`);

        lines.push('# HELP gcm_relay_latency_worst 中转节点最高延迟(毫秒)');
        lines.push('# TYPE gcm_relay_latency_worst gauge');
        lines.push(`gcm_relay_latency_worst ${relayStats.worstLatency}`);

        // 请求统计指标（增强统计）
        if (CONFIG.enableStats) {
            const stats = this.pool.getEnhancedStats();

            lines.push('# HELP gcm_requests_total 总请求数');
            lines.push('# TYPE gcm_requests_total counter');
            lines.push(`gcm_requests_total ${stats.requests}`);

            lines.push('# HELP gcm_requests_successes_total 成功请求数');
            lines.push('# TYPE gcm_requests_successes_total counter');
            lines.push(`gcm_requests_successes_total ${stats.successes}`);

            lines.push('# HELP gcm_requests_failures_total 失败请求数');
            lines.push('# TYPE gcm_requests_failures_total counter');
            lines.push(`gcm_requests_failures_total ${stats.failures}`);

            lines.push('# HELP gcm_requests_timeouts_total 超时请求数');
            lines.push('# TYPE gcm_requests_timeouts_total counter');
            lines.push(`gcm_requests_timeouts_total ${stats.timeouts}`);

            lines.push('# HELP gcm_request_success_rate 请求成功率(百分比)');
            lines.push('# TYPE gcm_request_success_rate gauge');
            lines.push(`gcm_request_success_rate ${stats.successRate.toFixed(2)}`);

            lines.push('# HELP gcm_request_duration_avg 平均请求响应时间(毫秒)');
            lines.push('# TYPE gcm_request_duration_avg gauge');
            lines.push(`gcm_request_duration_avg ${stats.avgResponseTime.toFixed(2)}`);

            lines.push('# HELP gcm_request_duration_min 最小请求响应时间(毫秒)');
            lines.push('# TYPE gcm_request_duration_min gauge');
            lines.push(`gcm_request_duration_min ${stats.minResponseTime === Infinity ? 0 : stats.minResponseTime}`);

            lines.push('# HELP gcm_request_duration_max 最大请求响应时间(毫秒)');
            lines.push('# TYPE gcm_request_duration_max gauge');
            lines.push(`gcm_request_duration_max ${stats.maxResponseTime}`);

            lines.push('# HELP gcm_bytes_sent_total 总发送字节数');
            lines.push('# TYPE gcm_bytes_sent_total counter');
            lines.push(`gcm_bytes_sent_total ${stats.bytesSent}`);

            lines.push('# HELP gcm_bytes_received_total 总接收字节数');
            lines.push('# TYPE gcm_bytes_received_total counter');
            lines.push(`gcm_bytes_received_total ${stats.bytesReceived}`);

            lines.push('# HELP gcm_uptime_seconds 运行时间(秒)');
            lines.push('# TYPE gcm_uptime_seconds gauge');
            lines.push(`gcm_uptime_seconds ${(stats.uptime / 1000).toFixed(2)}`);
        }

        return lines.join('\n') + '\n';
    }

    destroy() {
        if (this.server) {
            this.server.close();
        }
    }
}

/**
 * SOCKS5 服务器
 */
class Socks5Server {
    constructor(pool) {
        this.pool = pool;
        this.server = net.createServer(this.handleConnection.bind(this));
        this.activeTunnels = 0;  // 活跃隧道数
        logDebug('Socks5', 'SOCKS5 服务器已创建');
    }

    start() {
        this.server.listen(CONFIG.localPort, () => {
            logInfo('Socks5', `监听端口: 0.0.0.0:${CONFIG.localPort}`);
        });

        this.server.on('error', (e) => logError('Socks5', `启动失败: ${e.message}`));
        logDebug('Socks5', 'SOCKS5 服务器启动中...');
    }

    handleConnection(socket) {
        const clientAddr = `${socket.remoteAddress}:${socket.remotePort}`;
        logDebug('Socks5', `新客户端连接: ${clientAddr}`);
        let state = 'AUTH';

        socket.on('data', async (chunk) => {
            try {
                if (state === 'AUTH') {
                    if (chunk[0] !== 0x05) {
                        logDebug('Socks5', `不支持的 SOCKS 版本 (${clientAddr})`);
                        return socket.end();
                    }
                    socket.write(Buffer.from([0x05, 0x00]));
                    logDebug('Socks5', `认证完成 (${clientAddr})`);
                    state = 'REQUEST';
                }
                else if (state === 'REQUEST') {
                    if (chunk[1] !== 0x01) { // Only CONNECT
                         logDebug('Socks5', `不支持的命令 (${clientAddr}): ${chunk[1]}`);
                         socket.end(Buffer.from([0x05, 0x07, 0, 1, 0,0,0,0, 0,0]));
                         return;
                    }

                    let addrType = chunk[3];
                    let targetAddr;
                    let targetPort;
                    let portOffset;
                    let originalTarget = null; // 保存原始目标地址用于日志

                    // 解析请求地址
                    if (addrType === 0x01) { // IPv4
                        targetAddr = chunk.slice(4, 8).join('.');
                        portOffset = 8;
                        originalTarget = `${targetAddr}`;
                        logDebug('Socks5', `IPv4 请求 (${clientAddr}): ${targetAddr}:${targetPort}`);
                    } else if (addrType === 0x03) { // Domain
                        const len = chunk[4];
                        const domain = chunk.slice(5, 5 + len).toString();
                        portOffset = 5 + len;
                        targetAddr = domain;
                        originalTarget = `${domain}`; // 保存原始域名
                        logDebug('Socks5', `域名请求 (${clientAddr}): ${domain}`);

                        // --- 智能 DNS 预解析 (带缓存，Fix: 解决本地 DNS 污染) ---
                        if (CONFIG.enableDoH) {
                            // 优先尝试 A 记录 (IPv4)
                            let resolvedIP = await dnsCache.resolveCached(domain, 'A');
                            if (!resolvedIP) {
                                // 尝试 AAAA (IPv6)
                                resolvedIP = await dnsCache.resolveCached(domain, 'AAAA');
                            }

                            if (resolvedIP) {
                                logDebug('Socks5', `DNS 解析: ${domain} -> ${resolvedIP}`);
                                targetAddr = resolvedIP; // 替换为干净的 IP
                            }
                        }
                    } else if (addrType === 0x04) { // IPv6
                        const buf = chunk.slice(4, 20);
                        const parts = [];
                        for (let i = 0; i < 16; i += 2) {
                            parts.push(buf.readUInt16BE(i).toString(16));
                        }
                        targetAddr = parts.join(':');
                        portOffset = 20;
                        originalTarget = targetAddr;
                        logDebug('Socks5', `IPv6 请求 (${clientAddr}): ${targetAddr}:${targetPort}`);
                    }

                    targetPort = chunk.readUInt16BE(portOffset);

                    // IPv6 格式修正: 如果是 IPv6 且没有被 [] 包裹，加上 []
                    // Cloudflare Worker 的 connect() 需要 [::1] 格式
                    if (net.isIPv6(targetAddr) && !targetAddr.startsWith('[')) {
                         targetAddr = `[${targetAddr}]`;
                    }

                    // DEBUG: 打印目标地址信息
                    logDebug('Socks5', `收到代理请求 -> ${targetAddr}:${targetPort}`);

                    this.createTunnel(socket, targetAddr, targetPort, originalTarget);
                    state = 'STREAM';
                }
            } catch (e) {
                err('SocksHandler', e.message);
                socket.destroy();
            }
        });

        socket.on('error', () => {});
    }

    /**
     * 生成流 ID（用于多路复用）
     * 使用固定 2 位 16 进制格式 (0-FF, 256个流)
     */
    generateStreamId() {
        return Math.random().toString(16).substring(2, 4).padStart(2, '0');
    }

    async createTunnel(clientSocket, host, port, originalTarget = null) {
        let timeoutTimer = null;
        let requestStartTime = null;
        let bytesSent = 0;
        let bytesReceived = 0;

        try {
            // 记录请求开始
            if (CONFIG.enableStats) {
                requestStartTime = this.pool.recordRequestStart();
            }

            const connItem = await Promise.race([
                this.pool.getConnection(),
                new Promise((_, reject) =>
                    setTimeout(() => reject(new Error('Connection pool timeout')), CONFIG.tunnelTimeout)
                )
            ]);
            const ws = connItem.ws;
            // 生成2位16进制 streamId (0-FF, 256个流)
            const streamId = this.generateStreamId();
            // 获取 connectionId (6位16进制，已在 ConnectionPool 中生成)
            const connectionId = connItem.connectionId || 'unknown';

            // INFO: 输出代理请求信息（原始目标、WS连接ID、Stream ID）
            const targetToLog = originalTarget || host;
            logInfo('Proxy', `新请求 -> ${targetToLog}:${port} | WS[${connectionId}] Stream[${streamId}]`);

            // 设置隧道超时
            timeoutTimer = setTimeout(() => {
                logWarn('Tunnel', `隧道超时 (${CONFIG.tunnelTimeout}ms): ${host}:${port}`);
                // 记录请求超时
                if (CONFIG.enableStats) {
                    this.pool.recordRequestTimeout();
                }
                cleanup();
            }, CONFIG.tunnelTimeout);

            // 新协议: [WS_ID:3字节][STREAM_ID:1字节][TYPE:1字节][DATA...]
            // TYPE: 0=CONNECT, 1=CONNECTED, 2=DATA, 3=CLOSE
            const MSG_TYPE = { CONNECT: 0, CONNECTED: 1, DATA: 2, CLOSE: 3 };

            // 将16进制字符串转为字节
            const wsIdBytes = Buffer.from(connectionId, 'hex');
            const streamIdByte = parseInt(streamId, 16);

            // 发送 CONNECT 消息
            const connectHeader = Buffer.concat([
                wsIdBytes,                                      // 3 bytes: WS ID
                Buffer.from([streamIdByte, MSG_TYPE.CONNECT])  // 2 bytes: Stream ID + Type
            ]);
            const connectPayload = Buffer.from(`${host}:${port}|`);
            const connectMsg = Buffer.concat([connectHeader, connectPayload]);
            ws.send(connectMsg);

            let connected = false;

            // 根据是否启用多路复用，使用不同的监听器管理方式
            if (CONFIG.enableMultiplex) {
                // 多路复用模式：使用流处理器注册机制
                this.pool.registerStreamHandlers(ws, streamId, {
                    onMessage: (msg) => {
                        // 新协议: [WS_ID:3][STREAM_ID:1][TYPE:1][DATA...]
                        // 消息已经在 ConnectionPool 中分发到这里
                        if (msg.length < 5) return;

                        const msgWsId = msg.toString('hex', 0, 3);
                        const msgStreamId = msg[3];
                        const msgType = msg[4];

                        // 检查是否为当前连接和流的消息
                        if (msgWsId !== connectionId || msgStreamId !== streamIdByte) {
                            return;  // 不是本流的消息，忽略
                        }

                        if (!connected) {
                            // 等待 CONNECTED 响应
                            if (msgType === MSG_TYPE.CONNECTED) {
                                connected = true;
                                if (timeoutTimer) {
                                    clearTimeout(timeoutTimer);
                                    timeoutTimer = null;
                                }
                                if (CONFIG.enableStats && requestStartTime) {
                                    this.pool.recordRequestSuccess(requestStartTime);
                                }
                                clientSocket.write(Buffer.from([0x05, 0x00, 0x00, 0x01, 0,0,0,0, 0,0]));
                            } else if (msgType === MSG_TYPE.CLOSE) {
                                cleanup();
                            }
                        } else {
                            // connected = true 后的数据转发
                            if (msgType === MSG_TYPE.DATA) {
                                if (msg.length > 5) {
                                    const data = msg.slice(5);
                                    const msgLen = data.length;
                                    if (CONFIG.enableStats) {
                                        bytesReceived += msgLen;
                                    }
                                    clientSocket.write(data);
                                }
                            } else if (msgType === MSG_TYPE.CLOSE) {
                                cleanup();
                            }
                        }
                    },
                    onClose: () => {
                        cleanup();
                    },
                    onCleanup: () => {
                        // 清理资源
                        if (timeoutTimer) {
                            clearTimeout(timeoutTimer);
                            timeoutTimer = null;
                        }
                        if (CONFIG.enableStats && (bytesSent > 0 || bytesReceived > 0)) {
                            this.pool.recordDataTransfer(bytesSent, bytesReceived);
                        }
                    }
                });
            } else {
                // 非多路复用模式：直接在 WebSocket 上添加监听器
                const onMsg = (msg) => {
                    // 新协议: [WS_ID:3][STREAM_ID:1][TYPE:1][DATA...]
                    if (msg.length < 5) return;

                    const msgWsId = msg.toString('hex', 0, 3);
                    const msgStreamId = msg[3];
                    const msgType = msg[4];

                    // 检查是否为当前连接和流的消息
                    if (msgWsId !== connectionId || msgStreamId !== streamIdByte) {
                        return;
                    }

                    if (!connected) {
                        if (msgType === MSG_TYPE.CONNECTED) {
                            connected = true;
                            if (timeoutTimer) {
                                clearTimeout(timeoutTimer);
                                timeoutTimer = null;
                            }
                            if (CONFIG.enableStats && requestStartTime) {
                                this.pool.recordRequestSuccess(requestStartTime);
                            }
                            clientSocket.write(Buffer.from([0x05, 0x00, 0x00, 0x01, 0,0,0,0, 0,0]));
                        } else if (msgType === MSG_TYPE.CLOSE) {
                            cleanup();
                        }
                    } else {
                        if (msgType === MSG_TYPE.DATA) {
                            if (msg.length > 5) {
                                const data = msg.slice(5);
                                const msgLen = data.length;
                                if (CONFIG.enableStats) {
                                    bytesReceived += msgLen;
                                }
                                clientSocket.write(data);
                            }
                        } else if (msgType === MSG_TYPE.CLOSE) {
                            cleanup();
                        }
                    }
                };

                ws.on('message', onMsg);
                ws.on('close', () => cleanup());
                ws.on('error', () => cleanup());

                // 保存引用以便 cleanup 时移除
                ws._tempOnMsg = onMsg;
            }

            let isCleaned = false;
            const cleanup = () => {
                if (isCleaned) return;
                isCleaned = true;

                // 清除超时定时器
                if (timeoutTimer) {
                    clearTimeout(timeoutTimer);
                    timeoutTimer = null;
                }

                // 记录数据传输统计
                if (CONFIG.enableStats && (bytesSent > 0 || bytesReceived > 0)) {
                    this.pool.recordDataTransfer(bytesSent, bytesReceived);
                }

                // 清理监听器
                if (CONFIG.enableMultiplex) {
                    // 多路复用模式：注销流处理器
                    this.pool.unregisterStreamHandlers(ws, streamId);
                } else {
                    // 非多路复用模式：移除 WebSocket 监听器
                    if (ws._tempOnMsg) {
                        ws.removeListener('message', ws._tempOnMsg);
                        delete ws._tempOnMsg;
                    }
                    ws.removeAllListeners('close');
                    ws.removeAllListeners('error');
                }

                clientSocket.destroy();

                // 释放连接
                if (CONFIG.enableMultiplex) {
                    // 复用模式：放回连接池
                    this.pool.releaseConnection(ws);
                } else {
                    // 非复用模式：关闭连接
                    try { ws.terminate(); } catch {}
                }
            };

            clientSocket.on('data', (data) => {
                if (connected) {
                    const dataLen = data.length;
                    if (CONFIG.enableStats) {
                        bytesSent += dataLen;
                    }
                    try {
                        // 发送 DATA 消息
                        const dataHeader = Buffer.concat([
                            wsIdBytes,                                      // 3 bytes: WS ID
                            Buffer.from([streamIdByte, MSG_TYPE.DATA])     // 2 bytes: Stream ID + Type
                        ]);
                        const dataMsg = Buffer.concat([dataHeader, data]);
                        ws.send(dataMsg);
                    } catch (e) { cleanup(); }
                }
            });

            clientSocket.on('close', () => {
                try {
                    // 发送 CLOSE 消息
                    const closeHeader = Buffer.concat([
                        wsIdBytes,                                       // 3 bytes: WS ID
                        Buffer.from([streamIdByte, MSG_TYPE.CLOSE])     // 2 bytes: Stream ID + Type
                    ]);
                    ws.send(closeHeader);
                } catch {}
                cleanup();
            });

            clientSocket.on('error', () => cleanup());

        } catch (e) {
            if (timeoutTimer) {
                clearTimeout(timeoutTimer);
            }
            // 记录请求失败
            if (CONFIG.enableStats) {
                this.pool.recordRequestFailure();
            }
            logWarn('Tunnel', `建立隧道失败: ${e.message} (${host}:${port})`);
            clientSocket.destroy();
        }
    }
}

async function main() {
    logInfo('System', '========================================');
    logInfo('System', '  GCM 代理客户端启动中...');
    logInfo('System', '========================================');
    logInfo('System', `Worker: ${CONFIG.workerHost}`);
    logInfo('System', `本地端口: ${CONFIG.localPort}`);
    logInfo('System', `DoH: ${CONFIG.enableDoH ? '开启' : '关闭'} (${CONFIG.dohUrl})`);
    logInfo('System', `连接池: Min=${CONFIG.minPoolSize}, Max=${CONFIG.maxPoolSize}`);
    logInfo('System', `DNS缓存TTL: ${CONFIG.dnsCacheTTL / 1000}秒`);
    logInfo('System', `节点监控: ${CONFIG.relayMonitorInterval / 1000}秒`);
    logInfo('System', `日志级别: ${CONFIG.logLevel}`);
    logInfo('System', `Metrics: ${CONFIG.enableMetrics ? `开启 (端口:${CONFIG.metricsPort})` : '关闭'}`);
    logInfo('System', `连接池预热: ${CONFIG.enablePoolWarmup ? '开启' : '关闭'}`);
    logInfo('System', `断线重连: ${CONFIG.enableAutoReconnect ? '开启' : '关闭'}`);
    logInfo('System', `动态池调整: ${CONFIG.enableDynamicPool ? '开启' : '关闭'}`);
    logInfo('System', '----------------------------------------');

    const relayMgr = new RelayManager(CONFIG.relayIPs);
    await relayMgr.init();

    const pool = new ConnectionPool(relayMgr);

    // 连接池预热
    await pool.warmup();

    const server = new Socks5Server(pool);

    server.start();

    // 启动 Metrics 服务器
    const metricsServer = new MetricsServer(pool, relayMgr, dnsCache);
    metricsServer.start();

    logInfo('System', '========================================');
    logInfo('System', '  服务已就绪，等待连接...');
    logInfo('System', '========================================');
}

// 进程退出处理
process.on('exit', () => {
    fileLogger.close();
});

process.on('SIGINT', () => {
    logInfo('System', '收到 SIGINT 信号，正在退出...');
    fileLogger.close();
    process.exit(0);
});

process.on('SIGTERM', () => {
    logInfo('System', '收到 SIGTERM 信号，正在退出...');
    fileLogger.close();
    process.exit(0);
});

main().catch(e => console.error(e));

