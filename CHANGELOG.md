# CHANGELOG — gcm-cli

记录 `gcm-cli`（CLI 壳）各版本变更与升级动作。

---

## 未发版（main HEAD，待人工批准打 tag）

**Build / Docs**

- 依赖升级：`github.com/v2up-32mb/xshared` 从 pseudo-version
  （`v0.1.1-0.2026...99d0b0a4cf0d`，指向非正式 commit）收敛为**正式 tag v0.1.1**。
- 新增 `AGENTS.md`（壳约束 + 发版铁律）与 `CHANGELOG.md` 三件套。

**升级指引**：无代码改动；依赖 pin 规范后随下次发版生效。

---

## v1.0.0 — 2026-09-25

**初始版本**

- GCM 协议命令行客户端（Cloudflare Worker 代理）。
- 协议栈依赖 `github.com/v2up-32mb/gcm v0.1.0`，共享能力依赖 `xshared`。
- 能力：全局/子命令 CLI（`urfave/cli/v3`）、配置文件（yaml）、各平台静态二进制发布。