# gcm-cli

GCM 协议的命令行客户端（Cloudflare Worker 代理）。协议栈在 [`gcm`](https://github.com/v2up-32mb/gcm) 库，
本仓只保留 CLI 壳与服务端 Worker 部署指引。

## 安装

从 [Releases](https://github.com/v2up-32mb/gcm-cli/releases) 下载对应平台二进制
（linux/darwin/windows × amd64/arm64，纯静态），加执行权限直接运行。

## 快速开始

```bash
gcm --worker gcm.example.com --user-id <USER_ID> -l :1080
curl --socks5-hostname 127.0.0.1:1080 https://www.google.com/generate_204
```

## 常用参数

```
基本
  --worker, -w        Worker 地址（必须）
  --user-id, -u       用户鉴权 ID
  --proxy-ip, -p      出口端代理 IP（留空用 Worker 自身配置）
  --listen, -l        SOCKS5 监听地址（默认 :1080）

连接池
  --min-pool/--max-pool   连接池上下限（默认 3/15）
  --no-mux                禁用多路复用
  --no-dynamic-pool       禁用动态池

网络
  --relay, -r         中转节点列表（可多次/逗号分隔，测速超阈自动直连）
  --doh, -d           DoH 地址（默认 https://v.recipes/dns-query）
  --enable-ech, -e    启用 TLS ECH（默认关闭）

路由绕过（与 x-client Android 端参数面一致）
  --bypass-private       绕过私有/局域网地址
  --bypass-geoip-cn      绕过中国大陆 IP
  --bypass-geosite-cn    绕过中国大陆域名
  --bypass-rules         自定义规则（domain:/full:/IP/CIDR）
  --http <addr>          额外启用 HTTP 代理监听（同一数据面与 bypass 策略）
  --geo-ip / --geo-site  v2ray 格式 geoip.dat/geosite.dat 路径（覆盖内置 CN 数据）

数据文件：内置 CN 规则快照已随程序打包；如需跟随最新路由数据，将 v2ray 生态的
`geoip.dat` / `geosite.dat`（推荐 [Loyalsoldier/v2ray-rules-dat](https://github.com/Loyalsoldier/v2ray-rules-dat/releases)）
放在程序同目录（或用 --geo-ip/--geo-site 指定路径），启动时自动加载并覆盖内置数据，
日志会标注是否加载成功。release 附件已预置一份构建时快照可直接下载使用。
```

完整参数见 `gcm -h`；YAML/JSON 配置文件用 `--config`（CLI 参数优先）。

## 协议与服务端

协议格式见 [`gcm`](https://github.com/v2up-32mb/gcm) 库仓 `README` 与 `protocol/`；
Worker 服务端（`worker.js` + 部署说明）已独立建仓
[`gcm-worker`](https://github.com/v2up-32mb/gcm-worker)。

## 构建

```bash
go build -o gcm .
```

## 版本与发版

- 逐版本变更与升级指引见 `CHANGELOG.md`;协作约束（含**发版铁律：不得未经人工批准自行打 tag 并推送**）见 `AGENTS.md`。
