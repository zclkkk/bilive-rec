# Configuration

## 配置文件结构

配置文件为 TOML 格式，支持 **全局 → 房间** 分层继承。房间级配置中的字段会覆盖全局默认值，未指定的字段自动继承。

```
config.toml
├── [data]                  # 数据存储路径
├── [credentials.<name>]    # 账号凭据（可定义多个，按名字引用）
├── [record]                # 录制参数（全局默认）
├── [upload]                # 可选；存在时默认自动上传
├── [submit]                # 投稿元数据（全局默认）
├── [pipeline]              # 房间轮询与重连参数
└── [rooms.<name>]          # 当前要监听的房间（正常运行至少一个）
    ├── [rooms.<name>.record]   # 覆盖该房间的录制参数
    ├── [rooms.<name>.upload]   # 覆盖该房间的上传参数
    └── [rooms.<name>.submit]   # 覆盖该房间的投稿参数
```

所有配置区块均使用 `#[serde(deny_unknown_fields)]`，拼写错误的字段名会在启动时报错。

## 各区块说明

### [data]

数据存储路径。

```toml
[data]
dir = "./data"
```

- `dir`：内部状态数据库 (`state.redb`) 的存放目录。默认 `./data`。

所有相对路径（`data.dir`、`record.output_dir`、`credentials.*.cookie_file`）都相对于命令行传入的配置文件 locator 所在目录，而不是进程当前工作目录。若该 locator 是符号链接，则以符号链接所在目录为基准，不以链接目标目录为基准。程序生成并持久化绝对 locator，但保留路径中的 `..`，不会为尚不存在的输出目录执行 `canonicalize`。

可以在仅处理历史持久化工作时暂时移除全部 `[rooms.*]`；若既没有当前房间，也没有可执行的历史工作，`run` 会明确拒绝启动。

---

### [credentials]

定义账号凭据，按名字在其他配置中引用。可定义多个。

```toml
[credentials.main]
cookie_file = "./data/cookies.json"

[credentials.captain]
cookie_file = "./data/captain_cookies.json"
```

- `cookie_file`：B 站登录 Cookie 文件路径。用于录制时支持 biliup JSON 或原始
  `name=value; ...`；用于上传时必须是可读写的 biliup `LoginInfo` JSON，因为 biliup
  可能原地刷新 token。配置解析会在启动远端任务前检查格式与写权限。

---

### [record]

录制参数。可在房间级 `[rooms.<name>.record]` 中覆盖。

```toml
[record]
credential = "main"          # 拉流账号（用于获取最高画质），不填则匿名
output_dir = "./data/recordings"  # 录制文件存放目录
segment_time = "01:00:00"    # 分段时长阈值（HH:MM:SS 格式）
segment_size = "2GiB"        # 分段大小阈值（支持 B/KiB/MiB/GiB 后缀）
min_segment_size = "20MiB"   # 最小分段大小，小于此值的分段会被过滤
qn = 10000                   # 画质档位（10000 = 原画/蓝光）
cdn = []                     # CDN 偏好列表（使用 `check` 输出中的名字）
```

`segment_time` 和 `segment_size` 是轮转阈值，配置后必须大于 0。
`min_segment_size = "0"` 合法，表示不按大小过滤分段。大小单位只接受
`B`、`KiB`、`MiB`、`GiB`；不带单位时按字节解析。

---

### [upload]

上传参数。整个区块可省略；省略时所有房间都以 `LocalOnly` 模式录制。
存在该区块时，房间默认自动上传，可以通过房间级 `enabled = false` 关闭。

```toml
[upload]
credential = "main"   # 上传账号
line = "auto"         # 上传线路：auto 或 bda2
threads = 3           # 单文件上传并发数
submit_api = "app"    # 投稿 API：app、web 或 bcut_android
delete_after_submit = false
```

`delete_after_submit` 仅在 B 站返回 `aid/bvid` 后触发。`Pending`、
`Ambiguous`、`RetryScheduled`、`Blocked` 状态不会删除文件。接口确认创建稿件不代表审核通过。

---

### [submit]

投稿元数据默认值。可在房间级 `[rooms.<name>.submit]` 中覆盖。

```toml
[submit]
title = "{title} {started_at:%Y-%m-%d}"
description = "{name} 直播录像\n录制开始：{started_at:%Y-%m-%d %H:%M:%S}\n原直播间：{url}"
category_id = 171
copyright = "reprint"        # original 或 reprint
source = "{url}"              # reprint 来源；缺省为当前房间 URL
tags = ["直播录像"]
private = false
dynamic = ""
forbid_reprint = false
charging_panel = false
close_reply = false
close_danmu = false
featured_reply = false
```

`source` 是 B 站转载来源字段。`copyright = "reprint"` 且未配置时，默认使用
`{url}`，即当前房间 URL。

---

### [pipeline]

房间轮询与断流重连参数。

```toml
[pipeline]
poll_interval_s = 60    # 闲置/失败状态下的轮询间隔（秒）
offline_grace_s = 60    # 断流宽限期（秒），超时则结束当前 session
backoff_s = 15          # 重连退避起始间隔（秒）
max_backoff_s = 300     # 重连退避最大间隔（秒）
```

---

### [rooms.<name>]

房间配置。每个房间需要一个唯一名称和必填的 `url` 字段。

```toml
[rooms.my_room]
url = "https://live.bilibili.com/123456"
```

房间级覆盖（均为可选，缺省继承全局配置）：

```toml
[rooms.my_room.record]
credential = "captain"
qn = 4000
cdn = ["cn-gotcha04"]

[rooms.my_room.upload]
credential = "alt_uploader"
delete_after_submit = true

[rooms.my_room.submit]
category_id = 65
tags = ["直播录像", "my_room"]
private = true
```

---

## 模板系统

`title` 和 `description` 支持以下占位符：

| 占位符 | 说明 |
|--------|------|
| `{title}` 或 `{room_title}` | 直播间标题 |
| `{name}` 或 `{room_name}` | 房间配置名称 |
| `{room_id}` | 房间 ID |
| `{url}` | 房间 URL |
| `{started_at:FORMAT}` | 录制开始时间（本机时区） |

`started_at` 必须带格式，使用 [Jiff strtime 格式](https://docs.rs/jiff/latest/jiff/fmt/strtime/index.html)：

```
{started_at:%Y-%m-%d}           → 2024-05-15
{started_at:%Y-%m-%d %H:%M:%S} → 2024-05-15 20:30:00
{started_at:%s}                 → 1715784600 (Unix timestamp)
```

未使用 `started_at:` 前缀（如 `{started_at}`）会在配置加载时报错。未知占位符同样会被拒绝。

---

## 常见配置模式

### 全部房间只录制

省略整个 `[upload]` 区块即可。Session 会把 `LocalOnly` 作为持久化输出意图，
以后增加上传配置也不会追溯上传这些历史录像。

```toml
[rooms.archive]
url = "https://live.bilibili.com/111"
```

### 部分房间只录制

```toml
[upload]
credential = "uploader"

[rooms.published]
url = "https://live.bilibili.com/111"

[rooms.archive]
url = "https://live.bilibili.com/222"

[rooms.archive.upload]
enabled = false
```

### 多账号分工

录制用一个账号（获取高画质），上传用另一个账号：

```toml
[credentials.recorder]
cookie_file = "./data/recorder_cookies.json"

[credentials.uploader]
cookie_file = "./data/uploader_cookies.json"

[record]
credential = "recorder"

[upload]
credential = "uploader"
```

### 不同房间不同画质

```toml
[rooms.high_quality]
url = "https://live.bilibili.com/111"

[rooms.high_quality.record]
qn = 10000

[rooms.low_quality]
url = "https://live.bilibili.com/222"

[rooms.low_quality.record]
qn = 400
```

### 投稿后自动清理

```toml
[upload]
delete_after_submit = true
```

### 断流后快速上传

减小宽限期，让断流后的分段尽快上传：

```toml
[pipeline]
offline_grace_s = 30
```
