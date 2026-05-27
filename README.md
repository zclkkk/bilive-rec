# bilive-rec

`bilive-rec` 是一个基于 Rust 编写的 Bilibili 直播自动录制与自动上传工具。
本项目的核心设计理念是**“在现实世界故障下的足够性 (Adequacy under real-world failure)”**——不仅关注正常流程，更在断网、流异常过期、进程崩溃等真实场景下提供可靠的、事务级的状态恢复。

## 功能特性 (Features)

- **稳定可靠的自动录制与上传**: 支持同时监听多个直播间，自动分段录制、无缝衔接上传与投稿发布。
- **事务级的状态一致性**: 内部采用嵌入式键值数据库 `redb`，在所有高风险操作前（如写盘、调用外部网络 API）先持久化真理状态，保证“幽灵文件”永不产生。
- **无状态损坏的优雅恢复**: 面临不可抗力的进程宕机重启后，提供细粒度的 `state recover` 进行离线修复。
- **诚实的外部边界处理**: 对外部环境持极度严苛的态度，例如通过 `Ambiguous` 状态标记服务器未返回 `bvid/aid` 时的存疑状态，交由操作员进行清晰地人工核查。

## 快速开始 (Getting Started)

### 1. 准备配置

复制默认的配置文件示例进行修改：
```bash
cp config.example.toml config.toml
```

`config.toml` 包含以下主要配置区块及字段：

| 模块 / 字段 | 说明 | 缺省值 | 示例值 |
| --- | --- | --- | --- |
| **`[data]`** | **数据存储配置 (可选)** | | |
| `dir` | 内部持久化状态数据库 (`state.redb`) 的存放路径。 | `./data` | |
| **`[credentials.<name>]`** | **账号凭据配置（按名字复用）** | | |
| `cookie_file` | B站登录 Cookie JSON 文件路径。录制和上传通过 credential 名字引用它。 | **(必填)** | `./data/cookies.json` |
| **`[record]`** | **录制参数配置 (可选)** | | |
| `credential` | 默认录制账号名（用于获取最高画质）。不配置则匿名拉流。 | `无 (可选)` | `main` |
| `output_dir` | 录制分段 `.flv` 视频的存放目录。 | `./data/recordings` | |
| `segment_time` | 切片时间阈值（格式 `HH:MM:SS`），到达此长度将切割新分段。 | `无 (可选)` | `01:00:00` |
| `segment_size` | 切片大小阈值，到达此大小将切割新分段。 | `无 (可选)` | `2GiB` |
| `min_segment_size` | 最小切片大小。小于此大小的分段会被过滤（防止碎片）。 | `20MiB` | |
| `qn` | 视频画质档位（10000 对应原画/蓝光）。 | `10000` | |
| `cdn` | CDN 偏好列表。填写 `bilive-rec check` 输出中的 `cdn=...` 名字，例如 `cn-gotcha04`；它只影响候选排序，不是硬性白名单，不可用或健康检查失败的候选会被跳过。 | `[]` | `["cn-gotcha04", "cn-gotcha01"]` |
| `delete_after_submit` | B站返回 `aid/bvid` 后删除本地 `.flv` 文件，并把分段状态记为 `Cleaned`。默认关闭；`Pending` / `Ambiguous` / `Failed` 投稿不会触发删除。注意：这里的“成功”只代表提交接口确认创建稿件，不代表审核通过；如果后续审核失败，本地源文件可能已经无法找回。 | `false` | `true` |
| **`[upload]`** | **上传执行配置 (`run` / `upload` / 上传恢复必需；`check` / `record` 不需要)** | | |
| `credential` | 默认上传账号名。`upload` 命令必填；`run` 可由每个房间的 `[rooms.<name>.upload]` 覆盖。 | `无 (按命令校验)` | `main` |
| `line` | B站上传线路选择（可选 `auto` 或 `bda2`）。 | `auto` | |
| `threads` | 单个文件内部上传并发数。 | `3` | |
| `submit_api` | 发布使用的 API 接口（可选 `app` 或 `web`）。 | `app` | |
| **`[submit]`** | **投稿元数据默认值 (可选)** | | |
| `title` | 投稿标题模板。支持 `{title}`/`{room_title}`, `{name}`/`{room_name}`, `{room_id}`, `{url}` 以及按本机时区格式化的录制开始时间 `{started_at:%Y-%m-%d %H:%M:%S}`。 | `无 (使用直播标题)` | `{title}` |
| `description` | 投稿简介模板，支持同一组占位符。 | `无 (空简介)` | `{name} 直播录像...` |
| `category_id` | 投稿分区 ID（如 171 代表电子竞技）。 | `171` | |
| `copyright` | 投稿版权类型：`original` 或 `reprint`。`reprint` 要求 `source` 非空。 | `reprint` | |
| `source` | 转载来源。`copyright = "original"` 时内部会清空此字段。 | `直播录像` | |
| `tags` | 投稿附带的标签数组。 | `[]` | `["直播录像"]` |
| `private` | 是否投稿为仅自己可见。 | `false` | |
| `dynamic` | 投稿同步动态文案。 | `""` | |
| `forbid_reprint` | 是否声明未经允许禁止转载。 | `false` | |
| `charging_panel` | 是否开启充电面板。 | `false` | |
| `close_reply` / `close_danmu` / `featured_reply` | 评论、弹幕和精选评论控制。 | `false` | |
| **`[pipeline]`** | **流水线调度配置 (可选)** | | |
| `poll_interval_s` | 闲置或失败状态下的房间探测轮询间隔（秒）。 | `60` | |
| `offline_grace_s` | 断流掉线宽限期（秒）。超时则打包上传。 | `60` | |
| `backoff_s` | 网络重连退避的起始间隔（秒）。 | `15` | |
| `max_backoff_s` | 网络重连退避的最大间隔（秒）。 | `300` | |
| **`[rooms.<name>]`** | **命名直播间配置（必填，支持配置多个房间）** | | |
| `url` | **[必填]** B站直播间完整的 URL。 | **(必填)** | `https://live.bilibili.com/123` |
| **`[rooms.<name>.record]`** | **当前房间录制覆盖（可选）** | | |
| `credential` / `qn` / `cdn` / `delete_after_submit` | 覆盖当前房间的拉流账号、画质、CDN 偏好和投稿确认后清理策略；缺省字段继承 `[record]`。`cdn` 同样使用 `check` 输出里的 CDN 名字。 | `继承 [record]` | |
| **`[rooms.<name>.upload]`** | **当前房间上传覆盖（可选）** | | |
| `credential` | 覆盖当前房间的上传账号；缺省继承 `[upload].credential`。 | `继承 [upload]` | |
| **`[rooms.<name>.submit]`** | **当前房间投稿覆盖（可选）** | | |
| `title` / `description` / `category_id` / `copyright` / `source` / `tags` / `private` / `dynamic` / `forbid_reprint` / `charging_panel` / `close_reply` / `close_danmu` / `featured_reply` | 覆盖当前房间的投稿元数据；缺省字段继承 `[submit]`。 | `继承 [submit]` | |

### 2. 准备鉴权文件

你需要获取你的 Bilibili 网页版登录 Cookie 以供程序进行原画质直播流拉取和视频上传发布。
将获取的 Cookie 保存至 `data/cookies.json`，并在 `[credentials.main]` 中引用。推荐使用专门的小号进行 Cookie 的提取。

### 3. 开始运行

**自动化监听、录制与上传工作流：**
```bash
bilive-rec run
```
这条指令会启动所有配置在 `config.toml` 中房间的守护进程，并且持续监听开播状态。一旦开播即自动录制并在分段完毕后静默完成上传与投稿。

## CLI 工具箱 (CLI Commands)

除了守护进程，`bilive-rec` 还提供了一系列单次任务的实用工具：

*   **`bilive-rec check <room_url>`**:
    快速检查某个直播间的开播状态并列出可用的直播流节点。
*   **`bilive-rec record <room_url>`**:
    临时对单个房间启动一次录制（不支持自动上传），停止可使用 `Ctrl-C` 安全退出。
*   **`bilive-rec upload <file1> <file2> ...`**:
    手动上传离线视频文件到 Bilibili 投稿系统。
*   **`bilive-rec state inspect`**:
    打印当前系统的内部红黑树存储状态、排查异常及历史流传记录。
*   **`bilive-rec state recover --apply`**:
    如果在崩溃后留下无法自动流转的中间状态分段视频，此命令能够生成并安全应用恢复计划。
*   **`bilive-rec state resolve-submission <session_id> --as [submitted|failed]`**:
    人工裁定处于存疑 (`Ambiguous`) 状态的发布记录。

## 文档指引 (Documentation)

针对开发者和对项目设计哲学感兴趣的用户，请参阅：
*   **[架构与设计文档 (Architecture Document)](docs/ARCHITECTURE.md)**: 详细描述了状态机流水线的设计、模块划分、“Boring Failures” 处理哲学以及故障恢复保证。

---

> *“Be harsh where reality enters; be lean where the model owns the truth.”*
