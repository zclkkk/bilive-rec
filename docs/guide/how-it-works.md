# How It Works

bilive-rec 的核心是一个**按房间独立运行的状态机**。每个房间由一个独立的异步任务驱动，状态持久化到 `redb` 嵌入式数据库，保证崩溃后可恢复。

---

## 状态机生命周期

每个房间的生命周期由 `PipelineState` 状态机控制：

```
Idle → Resolving → Recording → Uploading → Submitting → Submitted → Idle
                  ↘ WaitingReconnect → ReResolving ↗
                                       ↘ Failed
         ↘ Offline
```

| 状态 | 含义 |
|------|------|
| `Idle` | 空闲，等待下次轮询 |
| `Resolving` | 正在查询房间信息和流地址 |
| `Recording` | 正在录制直播流 |
| `WaitingReconnect` | 断流后等待重连（带退避） |
| `ReResolving` | 重连时重新查询流地址 |
| `Uploading` | 录制结束，正在上传分段 |
| `Submitting` | 所有分段已上传，正在投稿 |
| `Submitted` | 投稿完成 |
| `Offline` | 主播未开播 |
| `Failed` | 需要人工干预 |

状态转换是严格校验的——非法跳转会被拒绝。一个房间的崩溃不会影响其他房间。

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

新分段开始前，先在 `redb` 中写入 `Segment` 记录（状态为 `Recording`）。如果在文件创建前崩溃，recovery 会看到一个没有对应文件的 Recording 分段并标记为 Failed。如果在文件创建后但 DB 写入前崩溃，recovery 会忽略孤立的 `.part` 文件。

### 4. 断流处理

直播流中断后，进入 `WaitingReconnect` 状态：

- 记录断流时间，开始计算宽限期（`offline_grace_s`）
- 宽限期内复播：继续录制到同一个 LiveSession
- 宽限期超时：结束当前 LiveSession，进入 Uploading

---

## 上传与投稿

### 上传

进入 `Uploading` 后，依次上传所有 `Finalized` 状态的分段：

- 每个分段上传前标记为 `Uploading`，上传成功后标记为 `Uploaded`
- 上传结果持久化为 `UploadedPart`（包含 B 站返回的文件名）
- 如果上传失败但状态写入成功，分段保持 `Finalized`，下次重试

### 投稿

所有分段上传完成后进入 `Submitting`：

1. 检查是否已有投稿记录（幂等处理）
2. 将 Session 标记为 `Finalized`
3. 收集所有 `UploadedPart`，按序号排序
4. 渲染标题/简介模板
5. **先持久化 `Pending` 投稿记录**，再调用 B 站投稿 API
6. 根据返回结果记录最终状态：

| 结果 | 投稿状态 | 含义 |
|------|----------|------|
| 返回 aid/bvid | `Submitted` | 确认成功 |
| HTTP 200 + code=0 但无标识符 | `Ambiguous` | 可能成功，需人工确认 |
| 请求失败 | `Failed` | 明确失败 |

`delete_after_submit` 仅在 `Submitted` 状态下触发。`Pending`、`Ambiguous`、`Failed` 不会删除文件。

---

## 崩溃恢复

崩溃重启后，`RoomSupervisor` 从 `redb` 读取持久化的 `PipelineState`，恢复到中断前的状态。

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

Pipeline states:
  room 123456: Recording  session=550e8400-...  last_error=[2024-05-15T20:30:00Z] network error: connection refused
  room 789012: Idle

Sessions:
  id: 550e8400-...
    room_key: 123456
    title: 某主播的直播间
    started_at: 2024-05-15T20:00:00+08:00
    status: Recording
```

**`last_error` 字段**：当录制过程中出现暂时性错误（网络断开、流异常等）时，错误信息和时间戳会持久化到 `RoomPipelineState`。成功的状态转换会清除该字段。如果 pipeline 停在 `WaitingReconnect` 或 `Recording` 且有 `last_error`，说明最近一次重连尝试失败了。

### state recover

```bash
bilive-rec state recover          # dry-run，只打印计划
bilive-rec state recover --apply  # 实际执行
```

recovery 会检测以下异常并生成恢复计划：

| 异常 | 恢复动作 |
|------|----------|
| Session 卡在 Recording（无活跃 pipeline） | 标记为 Failed |
| Segment 卡在 Recording（无活跃 pipeline） | 标记为 Failed |
| Finalized Segment 没有 UploadedPart | 提示手动上传或 `--retry-upload` |
| Pipeline 卡在 Failed | `--reset-room` 重置为 Idle |
| 投稿 Pending/Ambiguous | 不自动处理，需 `resolve-submission` |

recovery 是幂等的——多次运行不会产生副作用。

### state resolve-submission

```bash
# 在 B 站确认稿件存在后
bilive-rec state resolve-submission <session_id> --as submitted --bvid BV1xx411x7xx

# 在 B 站确认稿件不存在后
bilive-rec state resolve-submission <session_id> --as failed
```

---

## 错误分类

系统将错误分为三类，决定是否重试：

| 分类 | 错误类型 | 行为 |
|------|----------|------|
| 可重试 | 网络错误、B 站 API 错误、流协议错误、重复媒体数据 | 持久化错误信息，进入 WaitingReconnect 重试 |
| 致命 | IO 错误、配置错误、数据库错误、状态逻辑错误 | 终止当前 pipeline，进入 Failed |
| 特殊 | 优雅关闭（Ctrl-C） | 保持当前状态不变，等待重启恢复 |

错误信息通过 `last_error` 持久化，可通过 `state inspect` 查看，无需翻阅日志。
