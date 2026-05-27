# Architecture

> **Be harsh where reality enters; be lean where the model owns the truth.**

bilive-rec 的架构设计高度重视「在现实世界故障下的足够性」。在外部输入（网络、文件系统、API）进入系统的边界处保持严苛；在内部领域模型拥有真理的地方保持精简。

---

## 核心设计原则

- **Persist Truth Before Risk**：在执行任何有风险或不可逆操作之前，先将状态持久化到 `redb` 数据库。恢复应从持久化状态推导，而非依赖日志、时间假设或乐观的控制流。
- **Failure Must Be Boring**：断网、流过期、进程被杀、上传失败、投稿状态不明都是正常的。所有失败必须产生可供检查的状态和明确的下一步操作。
- **Ownership As Design**：利用 Rust 的所有权模型让职责清晰。修改只发生在其所有权归属的地方。
- **Harsh At Boundaries, Lean Inside**：外部输入不可信。数据跨越边界进入内部模型后，不应再散布冗余的防御代码。

---

## 模块结构

```
src/
├── main.rs              # 入口，CLI 分发，优雅退出
├── cli.rs               # Clap CLI 定义
├── config/              # 配置解析与校验
├── bilibili/            # B 站 API 客户端
├── pipeline/            # 状态机与房间协调器
├── recorder/            # FLV 录制引擎
├── state/               # 持久化状态中心
└── uploader/            # 上传与投稿
```

### pipeline/ — 状态机与调度

每个房间由独立的 `RoomSupervisor` 异步任务驱动，通过 `PipelineState` 状态机控制流程：

```
Idle → Resolving → Recording → Uploading → Submitting → Submitted → Idle
                  ↘ WaitingReconnect → ReResolving ↗
                                       ↘ Failed (需手动干预)
         ↘ Offline (主播下线)
```

状态转换通过穷举的 `can_transition_to()` 校验，非法跳转会被拒绝。一个房间的崩溃不会传染给其他房间。

### state/ — 持久化中心

采用 `redb` 嵌入式键值数据库，包含 6 张表：meta、sessions、segments、uploaded_parts、submissions、pipeline_states。

核心实体：
- `LiveSession`：一次直播的生命周期
- `Segment`：录制分段，含文件路径和状态（Recording → Finalized → Uploading → Uploaded → Cleaned）
- `UploadedPart`：已上传到 B 站的分段信息
- `Submission`：投稿状态（Pending → Submitted / Ambiguous / Failed）

关键设计：`put_session_and_pipeline_state()` 在单个 redb 事务中原子写入会话和流水线状态。

### recorder/ — 流捕获与 FLV 处理

- **FlvRecorder**：处理 HTTP 响应流，解析 FLV 标签。两阶段运行：WaitSync（等待元数据 + 序列头 + 关键帧）→ Recording
- **FlvNormalizer**：缓存序列头、检测序列头变更、跨 CDN 切换的时间戳规范化
- **MediaGroupBuffer**：媒体标签批处理、指纹去重、检测重复媒体组（指示需要重连）
- **分段生命周期**：DB 先于文件持久化，`.part` → `.flv` 在分段结束时重命名

### bilibili/ — 外部 API

所有 B 站交互限制在此模块。区分 `client`（API 请求，15s 超时）和 `stream_client`（长连接，无请求超时）。包含 WBI 签名、房间 ID 解析、流候选排序和 CDN 健康检查。

### uploader/ — 上传与投稿

- `Uploader` trait 定义三个操作：`check_login`、`upload_segment`、`submit`
- `upload_and_persist_segment`：上传生命周期跟踪，失败时回滚状态
- `SubmissionOutcome` 三态结果：Confirmed（返回 aid/bvid）、Ambiguous（接口成功但无标识符）、Err（已知失败）

---

## 典型工作流

### 全自动录制上传 (`run`)

1. 读取配置，为每个房间创建 `RoomSupervisor`
2. 轮询房间状态，开播时创建 `LiveSession`，进入 Recording
3. 录制过程中按大小/时间分段，每段先持久化到 DB 再写文件
4. 断流后进入 `WaitingReconnect`，宽限期内复播则继续录制
5. 超时后进入 Uploading，依次上传所有 Finalized 分段
6. 所有分段上传完成后 Submitting，调用 B 站接口投稿
7. 记录 Submission 结果，回到 Idle 等待下次开播

### 故障恢复

崩溃重启后，`RoomSupervisor` 从 redb 读取 `PipelineState`，发现中断的 Session：
- 被中断的 Recording 状态：残余 `.part` 文件通过 `state recover` 转为 Finalized 或 Failed
- 中断的上传：`state recover --retry-upload` 重新上传缺失的分段
- 不确定的投稿：`state resolve-submission` 人工裁定

---

## 健壮性设计

- **原子写**：新段开始录制前，先在 redb 中落盘 Recording 记录，避免「文件已写但数据库无记录」的幽灵文件
- **上传失败分类**：区分 `FatalState`、`Ambiguous`、`Reconcileable`，不在不确定外部状态时鲁莽重试
- **Ambiguous 状态**：投稿接口返回 HTTP 200 + code=0 但无 aid/bvid 时，持久化为 Ambiguous 交由人工核查
