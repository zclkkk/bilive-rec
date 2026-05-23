# bilive-rec 架构与设计文档

`bilive-rec` 是一个用于录制 Bilibili 直播并自动上传的 Rust 应用程序。本项目的设计高度重视“在现实世界故障下的足够性 (Adequacy under real-world failure)”。

核心指导语：
> **Be harsh where reality enters; be lean where the model owns the truth.**
> 在现实（如网络输入、文件系统、外部 API）进入系统的边界处保持严苛；在内部领域模型拥有真理的地方保持精简。

---

## 1. 核心设计原则

- **Persist Truth Before Risk (在风险前持久化真相)**:
  状态不是私有的实现细节，而是用户在程序被中断后理解发生了什么的方式。在执行任何有风险或不可逆的操作（例如新建文件、发起网络上传请求）之前，系统会先将状态持久化到内置的 `redb` 数据库中。
- **Failure Must Be Boring (故障必须是枯燥的)**:
  断网、直播流过期、进程被杀、上传失败或提交状态不明等都是正常的。所有的失败都必须产生可供检查的状态（Inspectable State）以及明确的下一步操作。系统不会为了强行推进流程而隐藏失败或删除文件。
- **Ownership As Design (所有权即设计)**:
  利用 Rust 的所有权模型让职责清晰。修改只发生在其所有权归属的地方。

---

## 2. 核心模块与目录结构

整个项目被划分为以下几个主要模块：

### `src/main.rs` & `src/cli.rs`
负责提供应用的命令行接口（CLI）。系统去除了虚假的“尚未实现”存根，对外暴露最真实的命令。
支持：
- `run`: 核心命令，启动持久运行的 Supervisor 循环。
- `record` & `check` & `upload`: 单次任务命令（One-shot execution），为临时和手动操作设计。
- `state`: 检查和恢复系统状态（Inspect, Recover, ResolveSubmission）。

### `src/pipeline/` (状态机与流水线调度)
定义并驱动每个直播间的录制流水线 (`RoomSupervisor`)，是全自动录制上传的核心协调者。
- **PipelineState**: 严格定义的有限状态机。状态转换被原子的 `transition` 保护，拒绝非法的状态跃迁。
  `Idle -> Resolving -> Recording -> Uploading -> Submitting -> Submitted`。同时包括诸如 `WaitingReconnect`、`Offline` 和 `Failed` 等异常状态。
- **RoomSupervisor**: 为每个房间建立独立的异步任务隔离运行（在 `main.rs` 的 run 循环中调度）。一个房间的崩溃或异常不会传染给其他房间。

### `src/state/` (持久化状态中心)
整个系统的真理之源（Source of Truth），采用嵌入式键值数据库 `redb`。
核心实体：
- `LiveSession`: 一次直播的生命周期记录。
- `Segment`: 录制产生的视频分段，包含其文件路径和状态 (`Recording`, `Finalized`, `Uploading`, `Failed` 等)。
- `UploadedPart`: 已经成功上传到 Bilibili 的分段信息。
- `Submission`: 最终发布的视频稿件状态（`Pending`, `Submitted`, `Ambiguous`, `Failed`）。

### `src/recorder/` (流捕获与 FLV 处理)
负责抓取网络流、解析 FLV 容器并进行无损分段。
- **FLV 处理 (`flv.rs`)**: 在边界处严格校验 FLV 头部、Tag 大小和音视频 Sequence Header，缓存 Sequence Header 以供分段时重新注入，防止分段后视频损坏。
- **分段策略 (`segment.rs`)**: 基于设定的时间、大小限制以及底层 AVC 关键帧进行轮转（Rotation）。若分段过小则执行过滤（Filtered）。

### `src/uploader/` (上传与发布)
基于 `biliup` 适配 Bilibili 的上传机制。
- **状态调和 (Reconciliation)**: 上传前验证所有的本地 Segment。严格区分并记录远程上传失败（`Remote`）、更新状态失败（`StateBeforeRemote` / `StateAfterRemote`）等错误情况。
- **提交歧义处理 (Ambiguous State)**: 当稿件发布接口 (submit) 返回 HTTP 200 且 code=0，却未返回 `aid/bvid` 或发生超时等未知错误时，不再盲目重试或判定为失败，而是持久化为 `Ambiguous`，交由操作员通过 `state resolve-submission` 手动裁定。

### `src/bilibili/` (外部 API 与协议解析)
所有对 Bilibili API 的交互均限制在此模块。包含 `Room`, `Stream`, `Wbi`, `Client` 的严格数据反序列化与鉴权签名逻辑。区分了 `client`（通用请求）和 `stream_client`（持续长连接），以防全局超时限制中断正常下载。

---

## 3. 典型工作流解析

### 3.1 全自动录制与上传循环 (`run` 命令)
1. **启动与检查**：读取 `config.toml`，验证配置。启动多线程，为每个配置的房间分配一个 `RoomSupervisor`。在进入循环前执行一次登录状态检查（`uploader.check_login()`）。
2. **状态机流转**：
   - `Resolving`：调用 Bilibili API 获取房间信息。若开播则开启一个 `LiveSession`，原子的进入 `Recording`。
   - `Recording`：请求最高画质的流地址。启动 `FlvRecorder` 开启网络拉流（支持 30 秒静默超时退出）。录制期间，根据关键帧和文件大小/时间配置切分 `Segment`。当录制结束（断流、关闭），状态流转为 `WaitingReconnect`。
   - `WaitingReconnect`：进入离线缓冲期。如果在 `offline_grace_s` 内复播，将流转为 `ReResolving` 继续录制在同一个 Session 下；若超时未复播，流转为 `Uploading`。
   - `Uploading`：筛选该 Session 的所有 `Finalized` 的分段，依次调用 Bilibili 接口上传。每个分段上传成功后持久化一条 `UploadedPart`。
   - `Submitting`：组合所有的 `UploadedPart`，附加房间标题、配置标签等信息，调用 Web/App 接口发布视频。持久化 `Submission` 记录发布结果。

### 3.2 故障恢复机制
在发生进程崩溃或意外关闭后，重启 `bilive-rec run`。
1. `RoomSupervisor` 会读取 `redb` 中的 `RoomPipelineState`。
2. 发现被中断的 Session 状态。如果中断时状态是 `Recording`，它不会盲目覆盖，而是允许通过 `state recover` 进行修复，或者系统自动进入重试逻辑，确保所有的残余 `*.part` 文件都能被转化为正确的状态 (`Failed` 或 `Finalized`)。
3. `state recover` 提供了事务性、无损的数据库状态修复计划，允许将崩溃的流转换为可上传状态。

## 4. 健壮性设计的体现 (Boring Failures)

最近的核心架构升级进一步深化了这些理念：
- **移除虚假表象**：移除了不诚实的、伪造的 CLI stub 命令。CLI 所呈现的就是实际能够工作的。
- **安全的原子写**：任何新段开始录制前，先在红黑树 `redb` 中落盘 `Recording` 记录。这避免了因“已经写了硬盘文件但没进库”导致的幽灵文件，使恢复变得直截了当。
- **上传失败分类**：详细区分了恢复期的失败：`FatalState`、`Ambiguous`、`Reconcileable`，阻止了系统在不确定外部世界真实状态的情况下鲁莽地进行重试发布，保障了极高的自动化确定性。
