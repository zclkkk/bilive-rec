# CLI Reference

## 命令格式

```text
bilive-rec [--config <path>] <command>
```

`--config` / `-c` 是全局选项，可以写在子命令前后。需要持久化状态的命令默认读取 `./config.toml`；`check` 在没有配置文件时使用录制默认值。

```bash
bilive-rec -c prod.toml run
bilive-rec status -c prod.toml
bilive-rec check https://live.bilibili.com/123456
```

公开命令只有 `run`、`check`、`status` 和 `recover`。项目不提供脱离持久化生命周期的一次性录制或任意文件上传命令。

## run

持续监听配置中的所有房间，开播时自动录制。每个 Session 创建时会冻结输出意图：

- 没有全局 `[upload]`：房间只录制并永久保留本地文件。
- 存在全局 `[upload]`：房间默认上传并投稿。
- `[rooms.<name>.upload].enabled = false`：该房间只录制。

```bash
bilive-rec run
bilive-rec -c /path/to/config.toml run
```

第一次 `Ctrl-C` 请求优雅退出：已经开始的单个上传、投稿或文件操作会完成并持久化结果，但不会再开始队列中的下一项；第二次会强制退出，可能留下需要人工裁定的远端操作。

`run` 会先打开并恢复已有状态，再准备当前房间。当前房间的临时网络故障不会阻断历史上传、投稿或清理。允许暂时不配置房间，但此时必须已经存在可执行的持久化工作；无状态库、无房间、无工作的空运行会被拒绝，且不会创建空数据库。

## check

只读检查直播间状态、可用 FLV 流和健康 CDN，不创建 Session。

```bash
bilive-rec check https://live.bilibili.com/123456
```

## status

显示持久化状态摘要、房间状态以及需要人工处理的问题。异常项会给出准确的 `recover` 命令。

```bash
bilive-rec status
bilive-rec status --verbose
```

`--verbose` 额外显示 Session 的冻结计划与录制裁决、Segment close reason、实际文件
矩阵、上传/投稿 attempt 和人工 resolution 历史，适合支持诊断。

`status` 直接打开 redb 独占状态库；它是离线检查命令，执行前必须先停止 `run`。

## recover

`recover` 只处理必须由操作者确认的异常结果。能够由本地事实唯一确定的崩溃恢复由 `run` 启动时自动执行。
执行任何 `recover` 命令前应先停止正在运行的 `bilive-rec run`，避免人工裁决与仍在进行的远端操作竞争。

### recover recording

致命录制故障会把 Session 标记为 `RecoveryRequired` 并冻结房间所有权。保留可用分段并结束 Session：

```bash
bilive-rec recover recording <session_id> --finalize
```

如果 Session 含有失败的 `.part` 文件，默认拒绝 finalize；确认保留但不上传这些文件时使用：

```bash
bilive-rec recover recording <session_id> --finalize --exclude-failed \
  --note "checked partial files"
```

放弃该 Session 后续上传与投稿：

```bash
bilive-rec recover recording <session_id> --abandon
```

三种裁决都会与房间释放在同一事务中持久化。

### recover upload

远端上传已经开始，但本地没有得到可持久化的确定结果时使用。

确认 Bilibili 没有创建远端文件：

```bash
bilive-rec recover upload <session_id> <segment_index> --not-uploaded
```

这是一项事实裁决，不是“强制重试”开关：活动或 Completed Session 会回到可调度状态，
已经 Abandoned 的 Session 会变成 `Cancelled`，不会重新上传。

如果从可靠日志中取得了准确的远端文件名：

```bash
bilive-rec recover upload <session_id> <segment_index> \
  --uploaded "<bili_filename>" \
  --part-title "Part 1"
```

该操作会把远端 proof 原子内嵌到 `UploadState::Uploaded`。不要猜测 `bili_filename`。

两种操作都可使用 `--note <text>` 保存人工核查说明。

### recover submission

确认 Bilibili 没有创建稿件：

```bash
bilive-rec recover submission <session_id> --not-submitted
```

这会追加一条 resolution 并进入 `RetryAuthorized`；下一次 attempt 不会覆盖既有历史。

确认稿件实际已经创建：

```bash
bilive-rec recover submission <session_id> --submitted --bvid BV1xx411x7xx
bilive-rec recover submission <session_id> --submitted --aid 123456
```

`--submitted` 至少需要 `--aid` 或 `--bvid` 之一。

两种操作都支持 `--note <text>`。

### recover segment

本地提交 intent 与文件矩阵冲突时需要人工选择；通常表现为 `.part` 与 `.flv` 同时存在。
检查实际存在的文件后选择一个事实：

```bash
bilive-rec recover segment <session_id> <segment_index> --keep-part
bilive-rec recover segment <session_id> <segment_index> --keep-final
bilive-rec recover segment <session_id> <segment_index> --exclude
```

`--keep-part` 要求 part 存在，删除冲突的 final，并由提交协议重新完成 rename；
`--keep-final` 删除仍存在的 part 并确认 final；`--exclude` 不猜测内容，保留文件但排除后续
上传。三者互斥并支持 `--note`。
