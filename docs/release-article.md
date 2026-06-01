# bilive-rec：一个边界明确的 B 站直播录制投稿工具

## 为什么写它

我想要的工具其实很窄：

录制 B 站直播，录完自动上传到 B 站投稿。

不需要多平台，不需要 Web UI，不需要插件系统，也不需要把一件很具体的事包装成一个通用自动化平台。但这不等于随便写个脚本把 `ffmpeg` 和上传 API 粘起来。直播录制面对的是很不稳定的现实世界：

- 直播流会断
- CDN 会切
- 时间戳会跳
- 进程会崩
- 上传可能只成功一半
- 投稿接口可能返回了成功，却没有给出 aid/bvid

这些事情发生以后，工具不能只留下几个文件和一堆日志，让人反过来猜“刚才到底发生了什么”。

`bilive-rec` 的目标就是：在“只录 B 站并投稿 B 站”这个范围里，把状态、失败和恢复做得足够诚实。

## 和现有工具的关系

### 录播姬

录播姬（BililiveRecorder）是非常成熟的纯录制工具。它的 FLV 处理管线很值得尊重：filler data 清理、重复数据处理、时间戳修复、序列头变化处理，这些都是长期踩坑后沉淀出来的工程经验。

`bilive-rec` 在 FLV 处理上参考了录播姬的问题划分，但没有照搬它的结构。我们用 Rust 从零实现了自己需要的边界处理：

- 等待 metadata、AVC sequence header、AAC sequence header 和关键帧齐备后才开始写文件
- 清理 H.264 filler NALU
- 检测并丢弃重复媒体组
- 对明显的时间戳跳变做规范化
- 缓存并重注入必要的音视频头，让分段独立可播放

这不是说我们完整复刻了录播姬，也不是说我们在录制层面全面超过它。录播姬是一款成熟录制器；`bilive-rec` 的不同点在于，它把录制、上传、投稿和恢复放进同一套持久化状态模型里。

### biliup

`biliup` 是一个覆盖面很大的项目：多平台录制、多平台上传、配置项丰富，生态也成熟。如果你需要管理很多平台，它仍然是更合适的选择。

但我的需求刚好相反。我只需要 B 站直播录制和 B 站投稿。对这个窄场景来说，通用框架会带来一些额外负担：

- 多平台抽象让 B 站流的特殊处理不容易自然落在核心模型里
- Python、Rust、Web UI、服务端等多层组合让部署和理解成本变高
- 上传和投稿状态如果不能和录制状态进入同一个 durable model，恢复时仍然需要额外判断

所以 `bilive-rec` 的选择是：录制链路自己写，上传链路直接 vendor `biliup` 的 Rust crate。B 站投稿协议维护成本很高，`biliup` 在这块已经做得足够好，没有必要重复造轮子。

## 项目哲学

`bilive-rec` 的核心哲学可以概括成一句话：

> Be harsh where reality enters; be lean where the model owns the truth.

现实进入系统的地方必须严厉：用户配置、Cookie 文件、B 站接口响应、直播流数据、磁盘文件、上传结果，都不能默认可信。

一旦数据通过边界校验，进入我们自己的模型，内部就应该尽量精简。不要在系统内部到处补防御性判断；如果内部到处都要怀疑，说明边界或模型设计错了。

这也是为什么项目很强调状态持久化。状态不是实现细节，而是用户在故障之后理解事实的入口。

在执行会改变本地或远端事实的高风险动作之前，先持久化能解释这个动作的状态。这样进程中断以后，恢复逻辑可以从数据库和磁盘事实出发，而不是从日志、时间顺序或运气出发。

## 它具体做什么

### 只支持 B 站 FLV AVC 直播录制

这是一个有意为之的边界。

`bilive-rec` 目前只请求和录制 B 站 FLV AVC 流，不支持 HLS，也不支持 HEVC。这样做的结果是：

- 录制链路可以保持非常明确
- 写出的文件就是 FLV，不做转码
- 录制侧的边界处理都围绕 AVC FLV 这个事实展开
- 不会在配置里暴露一个“看起来能选，但实际录不了”的协议选项

代价也很明确：如果某些直播间 HEVC 流体积明显更小，`bilive-rec` 不会为了省空间去选择 HEVC。这是项目当前的边界，不是疏漏。

### 尽量减少不必要分段

直播断流和 CDN 切换很容易造成大量小分段。`bilive-rec` 不承诺“一场直播必然只有一个文件”，但会尽力避免因为边界噪声产生不必要的分段。

目前录制侧会做这些处理：

- **WaitSync**：没有拿到完整 FLV header、metadata、AVC/AAC sequence header 和关键帧前，不开始写正式分段
- **H.264 filler 清理**：删除 NALU type 12 filler data，减少无意义填充
- **重复媒体组去重**：CDN 重连后如果收到重复媒体数据，直接丢弃
- **时间戳规范化**：遇到明显跳变时，把输出时间线 rebase 到连续区间
- **序列头变化处理**：检测 AVC/AAC sequence header 变化，只在确实需要时触发分段
- **最小分段过滤**：结束时删除低于阈值的碎片分段
- **分段原因持久化**：每个分段为什么关闭，会写入状态库，而不是只打一行日志

这些处理的目标不是把复杂性藏起来，而是让最后留下来的文件更接近“真实的一场直播”，同时让不可避免的分段有原因可查。

### 自动录制、上传、投稿

`bilive-rec run` 会按配置监听多个直播间：

1. 主播开播后开始录制
2. 分段 finalized 后进入上传流程
3. 上传成功的 part 会立即写入 redb
4. 投稿开始前写入 `Pending` submission
5. 投稿返回 aid/bvid 后标记为 `Submitted`
6. 明确失败标记为 `Failed`
7. 结果不确定标记为 `Ambiguous`

这里最重要的是第 3 和第 4 步：上传和投稿之间存在天然断点。视频文件可能已经传到 B 站，但投稿还没完成；投稿请求可能已经被 B 站接受，但本地还没收到完整响应。`bilive-rec` 不会在这些地方假装自己知道结果。

### 诚实处理投稿不确定性

投稿状态目前分为：

- `Pending`：投稿动作已经开始，但结果还不能确定
- `Submitted`：B 站返回了 aid 或 bvid，稿件已经创建
- `Ambiguous`：远端可能已经接受，但本地无法确认最终稿件标识
- `Failed`：明确失败

`Ambiguous` 不会被自动重试。因为盲目重试可能造成重复投稿。

确认 B 站后台实际情况后，可以用：

```bash
bilive-rec state resolve-submission <session-id> --as submitted --bvid <BV...>
bilive-rec state resolve-submission <session-id> --as failed
```

解析为 `submitted` 时必须提供 `--aid` 或 `--bvid`。这个命令只处理 `Pending` 或 `Ambiguous`，不会覆盖已经确定的 `Submitted` 或 `Failed`。

### 可审计恢复

`bilive-rec` 使用 redb 作为本地嵌入式状态库，记录 session、segment、uploaded part、submission plan、submission 和房间状态。

崩溃后可以先看状态：

```bash
bilive-rec state inspect
```

再生成恢复计划：

```bash
bilive-rec state recover
```

默认是 dry-run。确认计划后，再显式执行：

```bash
bilive-rec state recover --apply
```

如果涉及重新上传缺失 part，也需要明确指定 session：

```bash
bilive-rec state recover --retry-upload <session-id> --apply
```

恢复逻辑的原则是保守的：能从本地事实安全推出的动作才自动执行；远端结果不确定时，宁可停下来让人确认，也不替用户猜。

### Named credentials

录制和上传可以使用不同账号。

配置里先声明凭据：

```toml
[credentials.main]
cookie_file = "./data/cookies.json"

[credentials.captain]
cookie_file = "./data/captain_cookies.json"
```

然后在全局或房间级配置中引用：

```toml
[record]
credential = "main"

[upload]
credential = "main"

[rooms.example.record]
credential = "captain"
```

这样可以清楚表达“哪个房间用哪个身份拉流，哪个身份投稿”。session 里也会持久化当时使用的 credential identity，避免恢复时因为用户改了配置而悄悄换账号。

### 投稿元数据和房间级覆盖

投稿配置支持常见字段：标题、简介、分区、版权、来源、标签、动态、私密投稿、禁止转载、充电面板、关闭评论/弹幕等。

全局默认值放在 `[submit]`，房间可以用 `[rooms.<name>.submit]` 覆盖。录制和上传配置也同样支持房间级覆盖。

例如：

```toml
[submit]
title = "{title} {started_at:%Y-%m-%d}"
tags = ["直播录像"]
private = false

[rooms.example.submit]
tags = ["直播录像", "example"]
private = true
```

模板里的 `started_at` 是本项目开始录制的时间，不是 B 站直播间的开播时间。这个边界是故意保留的：录制工具能可靠知道自己的录制开始时间，但不应该把平台侧开播时间塞进模板系统里制造不必要复杂度。

### 可选清理本地文件

可以配置 `delete_after_submit = true`，在 B 站返回 aid/bvid、submission 被标记为 `Submitted` 后删除本地录制文件。

这个选项默认关闭。

原因很简单：B 站返回 aid/bvid 只表示稿件已创建，不代表一定审核通过。如果之后审核失败，而本地文件已经删除，你就无法从本地恢复这次投稿。只有在你接受这个风险时，才应该打开自动清理。

## 快速开始

```bash
# 编译
cargo build --release

# 准备配置
cp config.example.toml config.toml
# 编辑 config.toml，填入房间 URL 和 Cookie 路径

# 验证直播间
target/release/bilive-rec check https://live.bilibili.com/123456

# 启动守护进程
target/release/bilive-rec run
```

也可以开发期直接运行：

```bash
cargo run -- check https://live.bilibili.com/123456
cargo run -- run
```

常用命令：

| 命令 | 用途 |
|------|------|
| `check <url>` | 检查直播间状态、候选流、CDN 名称和最终选择 |
| `record <url>` | 一次性录制，不上传 |
| `upload <files...>` | 手动上传并投稿 |
| `run` | 启动多房间自动录制上传流程 |
| `state inspect` | 查看持久化状态 |
| `state recover` | 生成或执行恢复计划 |
| `state resolve-submission` | 人工裁定不确定投稿 |

## 适合谁，不适合谁

适合：

- 只关心 B 站直播录制和 B 站投稿
- 希望进程崩溃后有明确状态可查
- 希望自动录制上传链路尽量少猜、少隐式行为
- 接受用配置文件管理房间、账号和投稿元数据

不适合：

- 需要斗鱼、虎牙、YouTube、Twitch 等多平台
- 需要 Web UI
- 需要 HLS、HEVC 或转码
- 希望工具自动处理所有远端不确定结果

`bilive-rec` 不是“大而全”的项目。它更像一个小型、强边界、状态可恢复的录制投稿系统。

## 许可证考虑

`bilive-rec` 使用 The Parity Public License 7.0.0，不是 MIT、Apache 这类宽松许可证。

这个选择和项目气质有关。B 站直播录制、投稿协议、流处理边界这些知识，很多来自公开项目和真实使用者长期踩坑后的积累。我希望这个工具可以被自由使用、学习和分享；但如果有人基于它继续开发、长期运行一套服务，或者把它接进自己的系统里形成新的软件，也应该按许可证要求把相应源码贡献出来。

简单说：

- 自己使用、学习、分享源码：可以
- 修改、分发，或基于它做长期使用的派生项目：请认真阅读仓库里的 `LICENSE`，按 Parity 的 share-alike 要求公开相应源码
- 短期内部原型：许可证里有 prototype 例外，但有时间和使用范围限制
- 重新分发时：保留许可证文本、贡献者信息和源码地址

这不是法律建议；具体权利和义务以仓库里的 `LICENSE` 为准。选择这个许可证不是为了拦住正常使用，而是希望围绕这个问题域产生的改进继续留在公共视野里。

## 致谢

感谢 `biliup` 和录播姬。

`biliup` 的 B 站上传实现是 `bilive-rec` 投稿能力的基础。项目目前直接 vendor 了它的 Rust crate，复用其登录、上传线路、分片上传和投稿协议实现。

录播姬在 FLV 处理领域积累了非常多真实经验。`bilive-rec` 的录制侧虽然是 Rust 从零实现，但很多问题意识都来自对录播姬处理管线的学习：哪些数据该等待，哪些重复该丢弃，哪些异常不能假装不存在。

项目地址：

https://github.com/zclkkk/bilive-rec
