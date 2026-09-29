# CHANGELOG — gcm-cli

记录 `gcm-cli`（CLI 壳）各版本变更与升级动作。

---

## 未发版（待批准打 tag；帮助/文档文本同步 gcm-worker 语义演进）

**Docs（无功能代码变更）**

- `--proxy-ip` / `--proxy-all` 帮助文本、`config.yaml` 注释、README 参数表同步 gcm-worker
  v0.1.3 的**新语义**：`--proxy-all` 打开时连接 URL 带 `proxy-all=true`，Worker **只用 socks5
  出口**（跳过直连与 L4 回退链）——此时 `--proxy-ip`/`?fallbackip=` 必须是 socks5 服务器配置
  `[socks5h?://][user:pass@]host[:port]`，不是 L4 覆盖项。
- 启动日志：`强制走 socks5 出口: 已启用`；未配 `--proxy-ip` 时 Warn 改为提示依赖 Worker 侧
  `SOCKS5_PROXY`。
- **破坏性提示**：v1.1.0 若开了 `--proxy-all` 且 `--proxy-ip` 是 L4 覆盖项，搭配 gcm-worker
  v0.1.3+ 会失败——请改配真实 socks5 代理（不开 proxy-all 的部署不受影响）。

---

## v1.1.0 — 2026-09-29

**Build**

- 依赖升级：`github.com/v2up-32mb/gcm v0.1.0 → v0.1.2`（`?proxy-all=true` 出口开关 +
  修两处出口参数丢失；Go API 无变更，patch）、`xshared v0.1.1 → v0.1.2`（`Config.ProxyAll`）。

**Added**

- **`--proxy-all` 开关**：强制所有流量走回退出口——连接 URL 带 `proxy-all=true`，
  Worker **跳过「直连」一级**，直接从 `--proxy-ip` / Worker 出口池起步。
  也可写进 `config.yaml` 的 `proxyAll: true`。默认 `false` = 旧行为，**非破坏性**。
  - 启动时会显式打印「强制走回退出口: 已启用」；若同时**没给** `--proxy-ip`，
    会额外 Warn 提醒完全依赖 Worker 侧出口池，若 Worker 未配任何出口则**所有流会被直接关断**
    （这是「强制」的应有语义：不回落直连）。
  - 目标 Worker 须用支持 `?proxy-all=` 的 gcm-worker 版本；旧版 Worker 静默忽略该 query，
    退化为「仍先直连」。
- **新增首个单测** `config/flags_test.go`：锁住 `--proxy-ip` / `--proxy-all` 到 `Config` 的映射
  （出口参数名写错会静默退化成「仍先直连」，必须有回归网）。

**Fixed / Docs**

- 补记：`--proxy-ip` 的值现由 `gcm` 侧做 URL 转义（gcm v0.1.2）。含 `[ipv6]` 方括号、
  空格、`#`、`&` 的出口地址之前会在 query 里被截断成半个，现在能正确送达 Worker。
- `config.yaml` 示例补 `proxyAll`；README 常用参数表补 `--proxy-all`。

**Changelog 归档修正（不影响代码）**

- `v1.0.1` tag 实际已包含两笔变更，但当时仍误留在「未发版」区，本次一并归档：
  路由数据文件支持（`--geo-ip` / `--geo-site`）与 `xshared` 从 pseudo-version 收敛为
  正式 tag v0.1.1。按发版纪律不重打旧 tag，故在此说明。

**升级指引**

- **无需改动配置文件**，行为默认与 v1.0.x 完全一致（`proxyAll` 缺省 `false`）。
- 想用新功能：命令行 `--proxy-all` 或 `config.yaml` 加 `proxyAll: true`。
  注意**必须同时有出口**（`--proxy-ip`，或 Worker 侧配了动态节点 / 静态 `FALLBACK_IPS`），
  否则所有流会被 Worker 直接关断——「强制」不回落直连。
- `--proxy-ip` 的值现已做 URL 转义：含 `[ipv6]` 方括号、空格、`#`、`&` 的出口地址
  之前会被 query 截断成半个，现在能正确送达 Worker。

---

## v1.0.0 — 2026-09-25

**初始版本**

- GCM 协议命令行客户端（Cloudflare Worker 代理）。
- 协议栈依赖 `github.com/v2up-32mb/gcm v0.1.0`，共享能力依赖 `xshared`。
- 能力：全局/子命令 CLI（`urfave/cli/v3`）、配置文件（yaml）、各平台静态二进制发布。