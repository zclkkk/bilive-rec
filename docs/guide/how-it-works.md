# How It Works

bilive-rec 由两个独立循环组成：按房间运行的录制监督器，以及从 `redb`
推导任务的上传 worker。录制、上传、投稿都持久化到本地状态库；崩溃后从
状态和文件本身恢复，而不是从内存事件恢复。

---

## 房间生命周期

每个房间由 `RoomState` 状态机控制：

```
Idle -> Resolving -> Recording -> WaitingReconnect -> ReResolving
  ^        |             |                  |               |
  |        v             v                  v               v
  +----- Offline       Failed             Idle          Recording
```

| 状态 | 含义 |
|------|------|
| `Idle` | 空闲，等待下次轮询 |
| `Resolving` | 正在查询房间信息和流地址 |
| `Recording` | 正在录制直播流 |
| `WaitingReconnect` | 断流后等待重连 |
| `ReResolving` | 重连时重新查询流地址 |
| `Offline` | 主播未开播 |
| `Failed` | 需要人工干预 |

房间状态只描述录制生命周期，不描述上传和投稿。上传或投稿很慢时，房间监督器仍会继续监听下一场直播。

---

## 录制生命周期

### 1. 流捕获

录制从 HTTP 响应流开始。`FlvRecorder` 有两个阶段：

- **WaitSync**：等待 FLV 元数据、AVC/AAC 序列头和第一个关键帧全部到齐。在此之前的所有帧被丢弃。
- **Recording**：开始写入分段文件。

### 2. 分段策略

录制过程中按以下条件自动分段：

- **大小阈值**：分段文件达到 `segment_size` 时轮转
- **时间阈值**：分段录制时长达到 `segment_time` 时轮转
- **序列头变更**：CDN 切换导致序列头变化时轮转

每个分段先以 `.part` 扩展名写入，结束后重命名为 `.flv`。

### 3. DB 先于文件

新分段开始前，先在 `redb` 中写入 `Segment` 记录（状态为 `Recording`）。如果在文件创建前崩溃，recovery 会看到一个没有对应文件的 Recording 分段并标记为 Failed。孤立的 `.part` 文件不会被当作事实来源。

### 4. 断流处理

直播流中断后，进入 `WaitingReconnect`：

- 记录断流时间，开始计算宽限期（`offline_grace_s`）
- 宽限期内复播：继续录制到同一个 `LiveSession`
- 宽限期超时：将 `LiveSession` 标记为 `Finalized`，释放房间回到监听状态

---

## 上传与投稿

上传和投稿由独立 worker 负责。worker 不接收“任务事件”作为事实来源，而是扫描 `redb`：

- `Finalized` 分段且没有 `UploadedPart`：可以上传
- `Finalized` session 且所有分段已满足上传条件：可以投稿
- `Submitted` session 且配置允许清理：可以删除本地录制文件

### 上传

worker 上传所有可证明安全的 `Finalized` 分段：

- 每个分段上传前标记为 `Uploading`
- 上传成功后，`UploadedPart` 和 `SegmentStatus::Uploaded` 在同一个事务中落盘
- 上传失败但状态可恢复时，分段回到 `Finalized`，等待下次 worker 扫描
- `Uploading` 表示远端结果未知，不会自动重试

### 投稿

session 创建时会持久化 `SubmissionPlan`，冻结本次投稿的标题、简介、分区、tag、投稿身份和提交接口。用户之后修改配置，不会悄悄改变历史 session 的投稿事实。

session 被录制监督器标记为 `Finalized` 后，worker 才允许投稿：

1. 检查是否已有投稿记录
2. 校验所有分段都已满足上传条件
3. 收集所有 `UploadedPart`，按序号排序
4. **先持久化 `Pending` 投稿记录**，再调用 B 站投稿 API
5. 根据返回结果记录最终状态

| 结果 | 投稿状态 | 含义 |
|------|----------|------|
| 返回 aid/bvid | `Submitted` | 确认成功 |
| HTTP 200 + code=0 但无标识符 | `Ambiguous` | 可能成功，需人工确认 |
| 请求失败 | `Failed` | 明确失败 |

`delete_after_submit` 仅在 `Submitted` 状态下触发。`Pending`、`Ambiguous`、`Failed` 不会删除文件。

---

## 崩溃恢复

崩溃重启后，`RoomSupervisor` 从 `redb` 读取持久化的 `RoomState`，恢复仍处于录制生命周期内的房间。上传 worker 重新扫描 `Segment`、`UploadedPart`、`SubmissionPlan` 和 `Submission`，继续处理未完成的上传/投稿。

### state inspect

```bash
bilive-rec state inspect
```

输出示例：

```
Summary:
  sessions: 3
  segments: 15
  uploaded_parts: 12
  submissions: 2
  submission_plans: 3

Room states:
  room 123456: Recording  session=550e8400-...  last_error=[2024-05-15T20:30:00Z] network error: connection refused
  room 789012: Idle
```

`last_error` 字段记录最近一次暂时性录制错误。成功状态转换会清除它。

### state recover

```bash
bilive-rec state recover          # dry-run，只打印计划
bilive-rec state recover --apply  # 实际执行
```

recovery 会检测以下异常并生成恢复计划：

| 异常 | 恢复动作 |
|------|----------|
| Session 卡在 Recording（无活跃房间状态） | 标记为 Failed |
| Segment 卡在 Recording（无活跃房间状态） | 标记为 Failed |
| Finalized Segment 没有 UploadedPart | 正常 worker backlog；显式 `--retry-upload` 时才计划重传 |
| Room 卡在 Failed | `--reset-room` 重置为 Idle |
| 投稿 Pending/Ambiguous | 不自动处理，需 `resolve-submission` |

recovery 是幂等的：多次运行不会产生副作用。

### state resolve-submission

```bash
bilive-rec state resolve-submission <session_id> --as submitted --bvid BV1xx411x7xx
bilive-rec state resolve-submission <session_id> --as failed
```

---

## 错误分类

系统将错误分为三类，决定是否重试：

| 分类 | 错误类型 | 行为 |
|------|----------|------|
| 可重试 | 网络错误、B 站 API 错误、流协议错误、重复媒体数据 | 持久化错误信息，进入 WaitingReconnect 重试 |
| 致命 | IO 错误、配置错误、数据库错误、状态逻辑错误 | 终止当前房间，进入 Failed |
| 特殊 | 优雅关闭（Ctrl-C） | 保持当前状态不变，等待重启恢复 |

错误信息通过 `last_error` 持久化，可通过 `state inspect` 查看，无需翻阅日志。
