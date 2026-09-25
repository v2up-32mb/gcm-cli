# AGENTS.md — gcm-cli 协作指引

面向在该仓库工作的开发者与 AI agent。采用通用的 `AGENTS.md` 命名
（取代厂商专有命名 `CLAUDE.md`），任何 agent / 编辑器 / 工具均按此约定读取。

## 项目定位

`gcm-cli` 是 **GCM 协议的命令行客户端（Cloudflare Worker 代理）**。
协议栈在 [`gcm`](https://github.com/v2up-32mb/gcm) 核心库，本仓只保留 **CLI 壳** 与
服务端 Worker 部署指引。发布物为各平台静态二进制（linux/darwin/windows × amd64/arm64）。

## 硬性约束（不得违反）

1. **协议逻辑不在此仓**：一切 GCM 协议、连接池、流管理在 `gcm` 核心库；
   本仓只做参数解析（`urfave/cli`）、配置加载（yaml）、进程生命周期与输出。
2. **依赖正式 tag**：`gcm` 用正式 tag（`v0.1.x`）、`xshared` 用正式 tag（`v0.1.x`），
   不用 pseudo-version/replace/submodule/vendoring。
3. **壳不重复通用能力**：SOCKS5 解析/鉴权等用 `xshared/socks5`，不自行实现。
4. **发版铁律（最高优先级）**：**绝不未经人工确认就自行打 tag 并推送**。
   任何发版动作（打 tag、`push --tags`、发布 Release 二进制）必须先向用户明确汇报
   版本号与发布内容并获得批准；提交/推送日常分支不在此限。

## 发版流程

1. 代码完成：`go build ./... && go vet ./... && go test ./... -race` 全绿。
2. 各平台二进制构建验证 + `CHANGELOG.md` 记入。
3. **向用户汇报版本号与发布内容并获批准**，才打 tag 并推送（含触发 Release 构建）。

## 结构速览

| 文件/目录 | 职责 |
|---|---|
| `main.go` | CLI 入口（子命令、参数、进程生命周期） |
| `config/` | 配置加载（config.yaml） |
| `docs/` | Worker 部署指引 |
| `config.yaml` | 示例配置 |

## 测试

```bash
go build ./... && go vet ./... && go test ./... -race
```