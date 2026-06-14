# CLI Reference

## 命令格式

```
bilive-rec <command>
```

`--config`, `-c` 不是全局选项；它由需要配置文件的子命令各自提供。用法示例：

```bash
bilive-rec run --config prod.toml
bilive-rec state --config prod.toml inspect
bilive-rec check https://live.bilibili.com/123456 --config prod.toml
```

---

## check

检查直播间开播状态和可用流地址。

```
bilive-rec check <room_url>
```

- `room_url`：B 站直播间 URL（如 `https://live.bilibili.com/123456`）

**示例：**

```bash
bilive-rec check https://live.bilibili.com/123456
```

输出包括：开播状态、可用 CDN 列表（`cdn=...` 名称可用于配置文件的 `cdn` 字段）。

---

## record

对单个房间进行一次性录制（不支持自动上传）。按 `Ctrl-C` 安全停止。

```
bilive-rec record <room_url> [--config <path>]
```

- `room_url`：B 站直播间 URL
- `--config`, `-c`：配置文件路径

**示例：**

```bash
bilive-rec record https://live.bilibili.com/123456
```

录制文件保存到 `[record].output_dir` 目录。

---

## upload

手动上传离线视频文件到 B 站投稿系统。

```
bilive-rec upload <files...> [--title <title>] [--config <path>]
```

- `files`：一个或多个视频文件路径
- `--title`：投稿标题
- `--config`, `-c`：配置文件路径

**示例：**

```bash
bilive-rec upload data/recordings/*.flv --title "直播录像"
```

---

## run

启动全自动录制上传守护进程。持续监听所有配置的房间，开播时自动录制并在分段完成后上传投稿。

```
bilive-rec run [--config <path>]
```

- `--config`, `-c`：配置文件路径，默认 `config.toml`

**示例：**

```bash
bilive-rec run
bilive-rec run --config /path/to/config.toml
```

按 `Ctrl-C` 优雅退出（等待当前操作完成），再按一次强制退出。

---

## state

查看和管理持久化状态。所有 `state` 子命令需要配置文件。

```
bilive-rec state [--config <path>] <action>
```

### state inspect

打印当前系统的内部状态摘要，包括所有 LiveSession、Segment、Submission 的状态。

```bash
bilive-rec state inspect
```

### state recover

在崩溃后生成恢复计划。默认为 dry-run 模式，仅打印计划不执行。

```bash
bilive-rec state recover
bilive-rec state recover --apply
```

选项：

- `--apply`：实际执行恢复操作（不加则只打印计划）
- `--reset-room <room_id>`：将指定房间状态从 Failed 重置为 Idle
- `--retry-upload <session_id>`：为指定会话重新上传缺失 UploadedPart 的 Finalized 分段

### state resolve-submission

手动裁定处于 Pending 或 Ambiguous 状态的投稿记录。在 B 站网页端确认稿件实际状态后使用。

```bash
bilive-rec state resolve-submission <session_id> --as submitted --bvid BV1xx411x7xx
bilive-rec state resolve-submission <session_id> --as failed
```

参数：

- `session_id`：目标会话的 UUID
- `--as submitted | failed`：确认的最终状态
- `--aid <id>`：B 站稿件 ID（`--as submitted` 时与 `--bvid` 二选一）
- `--bvid <bvid>`：B 站视频 BVID（`--as submitted` 时与 `--aid` 二选一）

此命令拒绝覆盖已处于 Submitted 或 Failed 状态的记录。
