# Architecture

> 面向贡献者和 AI agent 的内部架构文档。用户文档参阅 [How It Works](../guide/how-it-works.md)。

---

## 模块结构

```
src/
├── main.rs              # 入口，CLI 分发，优雅退出
├── cli.rs               # Clap CLI 定义
├── credential.rs        # CredentialIdentity（凭据标识）
├── error.rs             # AppError 枚举，AppResult 类型别名
├── submission_template.rs # 标题/简介模板渲染
├── config/              # 配置解析与校验
│   ├── raw.rs           # TOML 反序列化结构体（deny_unknown_fields）
│   ├── resolved.rs      # 运行时解析后的配置类型
│   ├── defaults.rs      # 默认值
│   └── validation.rs    # 时长/大小解析
├── bilibili/            # B 站 API 客户端
│   ├── client.rs        # HTTP 客户端，WBI key 缓存
│   ├── room.rs          # 房间 ID 解析，房间信息获取
│   ├── stream.rs        # PlayInfo 获取，流候选排序，CDN 健康检查
│   ├── cdn.rs           # CDN 健康探测
│   ├── wbi.rs           # WBI 签名
│   └── types.rs         # API 响应类型
├── pipeline/            # 房间状态机与录制协调器
│   ├── state_machine.rs # RoomState 枚举，can_transition_to()
│   ├── session.rs       # RoomStateMachine（状态机包装）
│   └── supervisor.rs    # RoomSupervisor（房间生命周期管理）
├── recorder/            # FLV 录制引擎
│   ├── mod.rs           # FlvRecorder，record_flv() 核心循环
│   ├── flv.rs           # FLV 标签/头部解析与写入
│   ├── flv_pipeline.rs  # FlvNormalizer，MediaGroupBuffer
│   ├── flv_metadata.rs  # FLV 元数据重写
│   └── segment.rs       # 分段策略，路径辅助，事件类型
├── state/               # 持久化状态中心
│   ├── model.rs         # 数据模型：LiveSession，Segment，Submission 等
│   ├── store.rs         # StateStore（redb 封装）
│   └── recovery.rs      # 崩溃恢复：异常检测 → 计划生成 → 执行
└── uploader/            # 上传与投稿
    ├── types.rs         # Uploader trait，UploadRequest，SubmissionRequest
    ├── biliup_adapter.rs # BiliupUploader（biliup crate 适配）
    ├── validation.rs    # 上传对账，分段校验
    └── worker.rs        # UploadWorker（从 redb 推导上传/投稿任务）
```

---

## 错误分类体系

`AppError` 枚举（`error.rs`）是全项目统一的错误类型。错误在边界处按语义分类，由上下文决定是否重试。

### 错误变体

| 变体 | 来源 | 语义 |
|------|------|------|
| `Io { path, source }` | 文件系统 | IO 错误，携带路径上下文 |
| `Config(String)` | 配置解析 | 配置错误 |
| `Database/Table/Transaction/Storage/Commit` | redb | 数据库各层错误 |
| `State(String)` | 内部逻辑 | 状态不变量违反 |
| `Network(reqwest::Error)` | HTTP 层 | 网络连接错误（send 失败） |
| `Bilibili(String)` | B 站 API | API 响应异常（code≠0、json 解析失败等） |
| `StreamProtocol(String)` | FLV 流 | 流协议格式错误（FLV 解析、AVC 格式等） |
| `StreamRepeatedData(String)` | FLV 流 | 重复媒体数据，指示需要重连 |
| `GracefulShutdown` | 信号 | 优雅关闭 |

### 上下文局部分类

错误的重试/致命分类不是全局的，而是由调用上下文决定。`recording_retry_reason()` 函数（`supervisor.rs`）为录制上下文定义分类：

- **可重试**：`Network`、`Bilibili`、`StreamProtocol`、`StreamRepeatedData` → 持久化错误信息，进入 `WaitingReconnect`
- **致命**：`Io`、`Config`、`Database`、`Table`、`Transaction`、`Storage`、`Commit`、`State`、`GracefulShutdown` → 终止当前房间

其他上下文（上传、投稿）有自己的错误处理逻辑，不复用此函数。

---

## 持久化设计

### redb 表结构

| 表 | Key | Value | 用途 |
|----|-----|-------|------|
| `meta` | `&str` | `&[u8]` | schema_version |
| `sessions` | UUID 字符串 | JSON `LiveSession` | 直播会话 |
| `segments` | `{uuid}:{index:010}` | JSON `Segment` | 录制分段 |
| `uploaded_parts` | `{uuid}:{index:010}` | JSON `UploadedPart` | 已上传分段 |
| `submissions` | UUID 字符串 | JSON `Submission` | 投稿记录 |
| `submission_plans` | UUID 字符串 | JSON `SubmissionPlan` | session 创建时冻结的投稿计划 |
| `room_states` | `u64` (room_id) | JSON `PersistedRoomState` | 房间录制状态 |

### 事务性写入

`StateStore::write()` 在单个 redb 事务中执行闭包内的所有写入。原子写入的关键场景：

- `create_recording_session()`：`LiveSession`、`SubmissionPlan` 和 `RoomState::Recording` 同时写入
- `finalize_session_and_release_room()`：`LiveSession::Finalized` 和 `RoomState::Idle` 同时写入
- 分段 finalize：rename `.part` → `.flv` 后立即更新 DB 状态；rename 失败时回滚

### PersistedRoomState

```rust
struct PersistedRoomState {
    state: RoomState,
    active_session_id: Option<Uuid>,
    last_error: Option<String>,        // 最近一次暂时性错误
    last_error_at: Option<Timestamp>,  // 错误发生时间
}
```

`last_error` 在暂时性错误时持久化（如网络断开、流异常），在成功状态转换时清除。`state inspect` 命令会显示该字段。

### SubmissionPlan

`SubmissionPlan` 在 session 创建时写入，用来冻结本次投稿的标题、简介、分区、tag、投稿身份、提交接口和清理策略。上传 worker 后续只读取这个计划，不从当前配置重新推导历史 session 的投稿事实。

---

## FLV 录制引擎

### FlvRecorder

两阶段运行：

1. **WaitSync**：缓存 FLV 标签直到元数据、AVC 序列头、AAC 序列头、第一个关键帧全部到齐
2. **Recording**：写入分段文件

关键设计：
- `push_chunk()` 使用 `read_pos` 前缀推进而非逐标签 drain，避免 O(n²)
- `open_new_segment()` 先写 DB 再创建文件（persist truth before risk）
- `finalize_current_segment()` rename 失败时回滚并持久化 Failed 状态

### FlvNormalizer

- 缓存序列头，检测序列头变更（触发分段轮转）
- 跨 CDN 切换的时间戳规范化（WaitSync 重基）
- AVC filler NALU 清理
- 重复媒体组检测（指纹哈希，阈值触发重连）

### MediaGroupBuffer

媒体标签按关键帧边界分组。每组计算指纹哈希，与前一组比较。连续重复组超过阈值（`DUPLICATE_RECONNECT_THRESHOLD`）时返回 `Duplicate`，触发 `StreamRepeatedData` 错误和重连。

---

## Uploader 设计

上传和投稿不属于房间状态机。`RoomSupervisor` 只负责录制并把 session 标记为 `Finalized`；`UploadWorker` 扫描 redb，从 `Segment`、`UploadedPart`、`SubmissionPlan` 和 `Submission` 推导下一步动作。

### Uploader trait

```rust
trait Uploader {
    async fn check_login(&self) -> AppResult<()>;
    async fn upload_segment(&self, req: UploadRequest) -> AppResult<UploadedPart>;
    async fn submit(&self, req: SubmissionRequest) -> AppResult<SubmissionOutcome>;
}
```

### SubmissionOutcome

```rust
enum SubmissionOutcome {
    Confirmed { aid: Option<u64>, bvid: Option<String> },
    Ambiguous { reason: String },
}
```

`Ambiguous` 处理 B 站 API 返回 HTTP 200 + code=0 但不返回 aid/bvid 的情况。不自动重试，交由人工确认。

### 上传对账

`reconcile_session_uploads()` 比较 Segment 状态和 UploadedPart 记录，生成：
- `needs_upload`：Finalized 但无 UploadedPart 的分段
- `blocked`：Recording/Uploading/Failed 等不能自动处理的分段
- `ready`：所有分段已满足上传条件

---

## Recovery 设计

### 三阶段架构

1. **`detect_anomalies(store)`**：只读扫描，返回 `Vec<Anomaly>`
2. **`plan_recovery(store, flags)`**：基于异常和用户标志生成 `RecoveryPlan`
3. **`apply_recovery(store, plan, uploader)`**：幂等执行恢复计划

`RecoveryContext` 结构体共享数据加载和 lookup 构建，避免 `detect_anomalies` 和 `plan_recovery` 之间的代码重复。

### 幂等性

每个恢复动作在执行前重新验证前提条件。如果状态已变化（如 Segment 已从 Recording 变为 Failed），动作被跳过而非报错。

### 投稿边界

`plan_recovery` 拒绝对已有 Submission 记录的 Session 执行上传恢复——必须先通过 `state resolve-submission` 确认投稿状态。
