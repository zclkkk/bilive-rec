# bilive-rec

`bilive-rec` 是一个基于 Rust 编写的 Bilibili 直播自动录制与自动上传工具。
核心设计理念是**「在现实世界故障下的足够性」**——在断网、流异常、进程崩溃等真实场景下提供事务级的状态恢复。

## Features

- **自动录制与上传**：同时监听多个直播间，自动分段录制、上传与投稿发布
- **事务级状态一致性**：采用 `redb` 嵌入式数据库，在所有高风险操作前先持久化状态，保证崩溃后可恢复
- **诚实的故障处理**：区分 Confirmed / Ambiguous / Failed 投稿状态，不在不确定时盲目重试

## Quick Start

```bash
# 编译
cargo build --release

# 准备配置
cp config.example.toml config.toml
# 编辑 config.toml，填入房间 URL 和 Cookie 路径

# 验证直播间
bilive-rec check https://live.bilibili.com/123456

# 启动守护进程
bilive-rec run
```

详细步骤参阅 [入门指南](docs/guide/getting-started.md)。

## CLI 概览

| 命令 | 说明 |
|------|------|
| `run` | 启动全自动录制上传守护进程 |
| `check <url>` | 检查直播间状态和可用流地址 |
| `record <url>` | 一次性录制（不上传） |
| `upload <files...>` | 手动上传视频文件 |
| `state inspect` | 查看内部状态 |
| `state recover` | 崩溃恢复（dry-run / --apply） |
| `state resolve-submission` | 人工裁定投稿状态 |

完整参数说明参阅 [CLI Reference](docs/reference/cli.md)。

## Documentation

- [入门指南](docs/guide/getting-started.md) — 安装、配置、首次运行
- [工作原理](docs/guide/how-it-works.md) — 状态机、录制生命周期、崩溃恢复
- [配置详解](docs/guide/configuration.md) — 各配置区块、模板系统、覆盖机制
- [CLI Reference](docs/reference/cli.md) — 命令完整用法
- [配置字段速查](docs/reference/config-fields.md) — 所有字段的类型、默认值
- [架构设计](docs/dev/architecture.md) — 模块结构、错误分类、持久化设计（面向开发者）

## License

Licensed under the [Parity Public License 7.0.0](LICENSE).
