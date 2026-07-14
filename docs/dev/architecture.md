# Architecture

> 面向贡献者和 AI agent。设计约束以仓库根目录 `AGENTS.md` 为准。

## 边界与所有权

```text
src/
├── main.rs                    CLI 分发、bootstrap、任务协调与停机
├── config/                    严格 TOML 边界与 resolved config
├── bilibili/                  房间、直播流、WBI 与 B 站响应边界
├── pipeline/
│   ├── bootstrap.rs           全量 canonicalize、去重、owner 对账
│   ├── state_machine.rs       仅进程内的瞬时 RoomState
│   └── supervisor.rs          单房间网络与录制控制流
├── recorder/
│   ├── artifact_commit.rs     rename/remove/delete 的唯一提交与恢复协议
│   └── ...                    FLV 解析、规范化、分段
├── state/
│   ├── model.rs               持久化领域聚合
│   ├── store.rs               redb codec 与受限事务原语
│   ├── transitions.rs         所有领域状态变化的唯一入口
│   ├── inspection.rs          一致的只读诊断投影
│   └── recovery.rs            需要操作者确认的事实裁决
└── uploader/
    ├── types.rs               远端 boundary outcome
    ├── biliup_adapter.rs      biliup 协议适配和错误分类
    └── worker.rs              仅从 durable state 推导工作
```

Supervisor 不直接写数据库，Recorder 不直接改 Segment，Worker 不重建或覆盖历史行。
所有写操作进入 `state::transitions`，在一个 redb write transaction 中重新读取相关聚合并
校验 expected state。raw `StoreTxn` 不对 state 模块外公开。

单房间 I/O 故障或 `RecoveryRequired` 只结束该房间 task；其他房间继续运行。数据库、事务、
状态不变量破坏和 upload worker 失败才触发全局停机。

## Fresh Start schema

状态库固定为 `data.dir/state.redb`，`format_id = "bilive-rec-state"`，schema 2。
0.2.x 是 Fresh Start：没有迁移器、字段 fallback、serde alias 或旧状态测试。

| 表 | Key | Value |
|---|---|---|
| `meta` | string | format identity 与 schema version |
| `sessions` | UUID | `LiveSession`，包含冻结的录制与输出计划 |
| `segments` | `{uuid}:{index:010}` | `Segment`，包含 artifact、上传 proof 和完整历史 |
| `submissions` | UUID | `Submission` 尝试、结果与人工裁决历史 |
| `room_states` | canonical room ID | `RoomState` |
| `upload_target_states` | serialized target | 跨重启 target backoff / blocked gate；非 Ready gate 携带具体 remote attempt owner |

`run` 对已有状态使用 `StateStore::open_existing`，仅在存在当前房间的首次启动中创建新库；`status` 与 `recover` 只能使用 `StateStore::open_existing`。给错 `data.dir` 时必须报错，不能生成一个看似正常的空库。

## Session 聚合

每个 Session 冻结完整的 `RecordingPlan` 和 `OutputPlan`。恢复旧 Session 时使用其历史
credential、output directory、分段阈值、清晰度、CDN、上传 principal/target 和投稿内容；上传 principal 包含配置时读取的非零 `expected_mid`。每次上传或投稿都重新登录并用 Bilibili 认证响应核对真实 mid；当前配置只
影响未来 Session。

```text
SessionLifecycle
├── Open
├── RecoveryRequired { reason, detected_at }
└── Closed
    ├── Completed
    ├── NoUsableRecording
    └── Abandoned
```

房间 durable lifecycle 只有 `Ready / Owned(session) / Blocked(session)`。`Blocked` 必须指向
一个真实 Session，不存在没有 owner 的泛化 Failed 状态。

`close_session` 是唯一关门入口：

- `Writing / Finalizing / Discarding / Deleting` 表示仍有未对账的本地 intent，Session 进入
  `RecoveryRequired`；
- 任一 `Failed` artifact 令 Session 进入 `RecoveryRequired`；
- 没有可用 Segment（包括全部被过滤）得到 `NoUsableRecording`，不会创建失败投稿；
- 有可用 Segment 才得到 `Completed`；
- 所有判断、Session 更新和房间 release/block 在同一事务内完成。

## Artifact 文件事务

Artifact state 自己携带 close reason 或 failure reason，不再依赖可产生无效组合的顶层
`Segment.error` / `close_reason`。

```text
Writing
  ├─> Finalizing(close_reason) -> Ready(close_reason)
  ├─> Discarding(close_reason) -> Filtered(close_reason)
  └─> Failed(reason)

Ready -> Deleting -> Deleted
```

保留文件：

```text
create_new(part) -> rewrite metadata -> flush -> file sync_all
-> persist Finalizing -> atomic no-replace rename -> parent directory sync_all -> persist Ready
```

过滤文件：

```text
flush -> file sync_all
-> persist Discarding -> remove -> parent directory sync_all -> persist Filtered
```

投稿后清理：

```text
persist Deleting -> remove -> parent directory sync_all -> persist Deleted
```

Recorder、Worker 和启动恢复共用 `artifact_commit`，不重复实现文件矩阵。part 使用
`create_new`，final commit 使用操作系统的 no-replace rename，因此既不复用旧 part，也不
覆盖已有 final。part 与 final 同时存在，或 discard intent 意外遇到 final 时，没有唯一
安全答案，必须通过
`recover segment --keep-part | --keep-final | --exclude` 记录操作者决定。

## 上传与投稿边界

远端调用只有四类结果：

- `Confirmed`：有 durable proof；
- `RetryableKnownFailure`：确定未越过接受边界，可以按持久化 `retry_at` 自动重试；
- `BlockedKnownFailure`：明确失败但需要修正文件、配置、Cookie 或远端拒绝原因，不轮询重试；
- `Ambiguous`：B 站可能已接受，禁止自动重试。

上传 proof 内嵌在 `UploadState::Uploaded`，不再存在第二张 `uploaded_parts` 真相表。Segment
分别保存 append-only attempt history 和 operator resolution history。投稿同理；不存在
Submission row 表示尚未跨越风险边界，而不是一种伪 Pending 状态。

安全的瞬时失败使用指数退避。target scope 的失败同时更新带 attempt owner 的持久化 target gate，防止一份坏 Cookie 或上传线路在所有 Segment 上放大。普通 item 结果不写共享 gate；只有 owner 对应的显式恢复可以清除 Blocked gate。Abandon 也不能用“放弃 Session”冒充“target 已修复”。

Worker gating：

- `Open` 与 `Closed(Completed)` 可继续上传；
- 只有 `Closed(Completed)` 可投稿；
- `RecoveryRequired` 冻结新远端动作；
- `Abandoned / NoUsableRecording` 禁止新上传和投稿；
- abandon 保留 Uploaded 和 Ambiguous，Attempting 先转为 Ambiguous，确定未产生远端对象的
  Pending/Blocked 才转为 Cancelled。

## Bootstrap 与恢复

`run` 按 durable truth 与当前房间两个阶段启动：

1. resolve 并严格校验全部配置；
2. 若状态库存在，先从一个一致快照审计完整 Session/room ownership 图；硬冲突在任何写入前拒绝启动，可唯一认领的不一致被持久化为 `RecoveryRequired + Blocked`；
3. 重放 artifact 与中断的远端 attempt，并从持久化 Session 派生上传、投稿和清理工作；
4. upload worker 可在当前房间 registry 尚未完成时处理 Closed Session，Open Session 等待 registry 对账完成；
5. 按 room name 排序并解析当前房间 canonical ID；临时网络错误按 backoff 重试，不阻断 durable work；
6. 建立唯一 `canonical ID -> room config` registry，重复即整体失败；配置删除的 Open Session 进入恢复状态；
7. 最后启动可运行房间的 supervisors。

人工恢复命令只记录经操作者验证的事实：

```text
recover recording <sid> --finalize [--exclude-failed] | --abandon
recover upload <sid> <index> --not-uploaded | --uploaded <filename>
recover submission <sid> --not-submitted | --submitted (--aid ... | --bvid ...)
recover segment <sid> <index> --keep-part | --keep-final | --exclude
```

每次裁决都 append 到 durable history；后续 attempt 不得覆盖它。

## 完成标准

修改状态或 I/O 代码时至少证明：风险动作前有 durable intent；崩溃后 state 与文件矩阵能
解释发生了什么；未知远端结果不会降级为自动重试；所有异常都能由 `status --verbose`
给出准确的下一步。happy path 通过不代表修改完成。
