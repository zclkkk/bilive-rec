# 工作原理

## 一条自动生命周期

`bilive-rec run` 先解析全部房间的 canonical ID 并整体查重，随后恢复本地文件 intent、
对账持久化房间 owner，最后才启动房间监督器和上传 worker。任何配置别名冲突都发生在
创建 Session 之前，不会留下“某个并发任务先赢了”的半启动状态。

房间开播时创建 `LiveSession`，同时冻结：

- 完整录制计划：Cookie、输出目录、分段时长/大小、最小文件、清晰度和 CDN；
- 输出计划：LocalOnly，或 Bilibili 上传 target、投稿字段与提交后删除策略。

因此配置更新只影响未来 Session。中断后继续历史 Session 时使用历史计划，不用当前配置
猜测原本的路径或凭据。

## Session 与房间

Session lifecycle：

| 状态 | 含义 |
|---|---|
| `Open` | 仍可录制并上传已经完成的 Segment |
| `RecoveryRequired` | 本地录制事实需要操作者裁决，新远端动作被冻结 |
| `Closed(Completed)` | 有可用录像，可完成上传和投稿 |
| `Closed(NoUsableRecording)` | 没有可投稿的录像；不会伪造一个失败 Submission |
| `Closed(Abandoned)` | 操作者终止后续上传与投稿 |

房间只保存 `Ready / Owned(session) / Blocked(session)`。Session 与房间所有权总在同一 redb
事务中改变，不存在“Session 已结束但房间仍被旧 owner 占用”的正常路径。

`close_session` 会在同一事务内读取全部 Segment。未完成的文件 intent 或 Failed Segment
会得到 `RecoveryRequired + Blocked`；全部被过滤或零 Segment 会得到
`NoUsableRecording + Ready`；只有存在可用录像才是 `Completed + Ready`。

## Segment 与文件提交

Segment 有两个正交事实：

- Artifact：本地文件状态；
- Upload：远端文件状态。

Artifact state 自己携带 close reason 或 failure reason：

```text
Writing
  ├─> Finalizing -> Ready
  ├─> Discarding -> Filtered
  └─> Failed

Ready -> Deleting -> Deleted
```

part 文件用 `create_new` 创建。保留录像时先 flush/sync 文件，再持久化 `Finalizing`，
然后执行不会覆盖既有 final 的原子 rename、sync 父目录，最后写 `Ready`。过滤与删除同样
先写 `Discarding`/`Deleting`，再执行 remove、sync 父目录，最后写终态。进程可以在任一点
中断，下一次 `run` 都会用同一份文件矩阵重放，而不是从日志猜测。

如果本地 intent 与文件矩阵冲突（通常是 `.part` 与 `.flv` 同时存在），程序不会擅自覆盖
或删除。检查内容后使用：

```bash
bilive-rec recover segment <session_id> <index> --keep-part
bilive-rec recover segment <session_id> <index> --keep-final
bilive-rec recover segment <session_id> <index> --exclude
```

## 上传

上传 proof（B 站文件名与分 P 标题）直接内嵌在 `UploadState::Uploaded`，不会再与另一张
UploadedPart 表分叉。每次 attempt 和每次人工 resolution 都 append 保存。

远端结果分为四类：

| 结果 | 行为 |
|---|---|
| `Confirmed` | 原子写入 Uploaded proof |
| `RetryableKnownFailure` | 确定尚未越过接受边界，持久化指数退避时间 |
| `BlockedKnownFailure` | Cookie、文件、配置或明确拒绝；停止自动重试 |
| `Ambiguous` | B 站可能已接受；必须人工核实 |

target 级故障还会打开持久化 circuit breaker，避免同一份坏 Cookie 在所有待上传 Segment
上重复失败。退避从 30 秒指数增长，最大 30 分钟。

Ambiguous/Blocked 需要确认真实结果：

```bash
bilive-rec recover upload <session_id> <index> --not-uploaded
bilive-rec recover upload <session_id> <index> --uploaded <bili_filename>
```

`--not-uploaded` 的含义是“确认不存在”，不是无条件重试：Abandoned Session 会进入
Cancelled，不会复活上传。

## 投稿与本地清理

只有 `Closed(Completed)` 且所有可用 Segment 都具有 Uploaded proof 时才会创建投稿
attempt。没有 Submission row 表示尚未跨越投稿风险边界。

明确远端拒绝进入 `Blocked`，安全的连接前失败进入有 `retry_at` 的 `RetryScheduled`，请求
可能已被接受时进入 `Ambiguous`。人工裁决使用：

```bash
bilive-rec recover submission <session_id> --not-submitted
bilive-rec recover submission <session_id> --submitted --bvid <BV...>
```

`--not-submitted` 追加 resolution 并进入 `RetryAuthorized`；下一次 attempt 不覆盖过去的
attempt 或 resolution。

开启 `delete_after_submit` 时，只有已确认 Submitted、Ready 且 Uploaded 的文件才进入
`Deleting -> Deleted` 协议。先删除文件、后补状态的崩溃窗口不存在。

## Abandon 的真实语义

Abandon 关闭 Session 并释放房间，但不抹除远端事实：

- Uploaded 保持 Uploaded；
- Ambiguous 保持 Ambiguous；
- Attempting 先归一为 Ambiguous；
- Pending 与明确未产生远端对象的 Blocked 才变成 Cancelled；
- 不会投稿。

## 状态检查

`status`/`recover` 使用 `open_existing` 打开状态库。配置中的 `data.dir` 错误或数据库缺失会
直接报错，绝不在错误位置创建空库。

```bash
bilive-rec status --verbose
```

verbose 输出包含冻结计划、Session 录制事件、Artifact close/failure reason、part/final
文件矩阵、上传与投稿 attempt/resolution、target gate，以及每个异常的下一条命令。
执行离线状态命令前先停止 `run`，避免与正在进行的远端操作竞争。
