# bilive-rec

> [!WARNING]
> **0.2.x 是一次不兼容的 Fresh Start。** 更新前请备份旧 `config.toml` 和整个
> `data/` 目录（包括状态库与录像），然后从运行目录移除这些旧数据。更新后必须基于
> 当前 `config.example.toml` 重新创建配置，并让程序生成全新的 `data/state.redb`。
> 0.2.x 不读取、迁移或恢复任何早期版本的配置与状态。

`bilive-rec` 是一个基于 Rust 编写的 Bilibili 直播自动录制与自动上传工具。
核心设计理念是**「在现实世界故障下的足够性」**——在断网、流异常、进程崩溃等真实场景下提供事务级的状态恢复。

## Features

- **自动录制与上传**：同时监听多个直播间，自动分段录制、上传与投稿发布
- **事务级状态一致性**：采用 `redb` 嵌入式数据库，在所有高风险操作前先持久化状态，保证崩溃后可恢复
- **诚实的故障处理**：上传和投稿都区分确认成功、明确失败与结果不明，不在不确定时盲目重试

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

省略配置中的 `[upload]` 即为只录制模式；存在 `[upload]` 时房间默认自动上传，
可用 `[rooms.<name>.upload] enabled = false` 关闭单个房间的上传。输出意图会在
Session 创建时持久化，后续配置变化不会追溯改变历史录像。

配置中的相对路径统一相对于 `--config` 指向的配置文件所在目录解析，而不是启动命令的当前目录。上传 Session 同时冻结凭据名称、绝对路径和 Bilibili `mid`；每次远端操作都会重新认证并核对真实账号。

详细步骤参阅 [入门指南](docs/guide/getting-started.md)。

## CLI 概览

| 命令 | 说明 |
|------|------|
| `run` | 启动全自动录制上传守护进程 |
| `check <url>` | 检查直播间状态和可用流地址 |
| `status` | 查看录制、上传、投稿状态和待处理问题 |
| `recover recording` | 裁定异常结束的录制 Session |
| `recover upload` | 人工裁定不确定的上传结果 |
| `recover submission` | 人工裁定并恢复投稿 |
| `recover segment` | 裁定 `.part` / `.flv` 本地文件冲突 |

`status` 与 `recover` 直接打开独占状态库，执行前必须先停止 `run`。

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
