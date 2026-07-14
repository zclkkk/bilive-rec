# Configuration Fields Reference

完整的配置字段速查表。详细的使用说明和示例请参阅 [配置详解](../guide/configuration.md)。

所有相对文件路径均以 `--config` 指向的配置文件 locator 所在目录为基准；配置 locator 是符号链接时，以链接所在目录为准。运行时只持久化不依赖 CWD 的绝对 locator。

## [data]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `dir` | path | `./data` | 否 | 状态数据库存放目录 |

## [credentials.\<name\>]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `cookie_file` | path | — | **是** | Cookie 文件；上传使用时必须为可读写的 biliup LoginInfo JSON |

## [record]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `credential` | string | 无 | 否 | 拉流账号名（不填则匿名拉流） |
| `output_dir` | path | `./data/recordings` | 否 | 录制文件存放目录 |
| `segment_time` | string | 无 | 否 | 分段时长阈值，格式 `HH:MM:SS`，配置后必须大于 0 |
| `segment_size` | string | 无 | 否 | 分段大小阈值，支持 `B`/`KiB`/`MiB`/`GiB` 后缀，配置后必须大于 0 |
| `min_segment_size` | string | `20MiB` | 否 | 最小分段大小；可设为 `0` 表示不过滤；配置 `segment_size` 时不得大于该轮转上限 |
| `qn` | u32 | `10000` | 否 | 画质档位（10000 = 原画） |
| `cdn` | string[] | `[]` | 否 | CDN 偏好列表 |

## [upload]

可选。省略整个区块时，所有房间只录制；存在时，房间默认自动上传投稿。

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `credential` | string | 无 | 上传房间必填 | 上传账号名 |
| `line` | string | `auto` | 否 | 上传线路：`auto` 或 `bda2` |
| `threads` | usize | `3` | 否 | 单文件上传并发数 |
| `submit_api` | string | `app` | 否 | 投稿 API：`app`、`web` 或 `bcut_android` |
| `delete_after_submit` | bool | `false` | 否 | 投稿确认后删除本地文件 |

## [submit]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `title` | string | 无 | 否 | 投稿标题模板 |
| `description` | string | 无 | 否 | 投稿简介模板 |
| `category_id` | u16 | `171` | 否 | 投稿分区 ID |
| `copyright` | string | `reprint` | 否 | 版权类型：`original` 或 `reprint` |
| `source` | string | `{url}` | 否 | 转载来源，支持房间模板 |
| `tags` | string[] | `[]` | 否 | 投稿标签 |
| `private` | bool | `false` | 否 | 仅自己可见 |
| `dynamic` | string | `""` | 否 | 同步动态文案 |
| `forbid_reprint` | bool | `false` | 否 | 禁止转载 |
| `charging_panel` | bool | `false` | 否 | 开启充电面板 |
| `close_reply` | bool | `false` | 否 | 关闭评论 |
| `close_danmu` | bool | `false` | 否 | 关闭弹幕 |
| `featured_reply` | bool | `false` | 否 | 精选评论 |

## [pipeline]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `poll_interval_s` | u64 | `60` | 否 | 闲置/失败状态轮询间隔（秒） |
| `offline_grace_s` | u64 | `60` | 否 | 断流宽限期（秒） |
| `backoff_s` | u64 | `15` | 否 | 重连退避起始间隔（秒） |
| `max_backoff_s` | u64 | `300` | 否 | 重连退避最大间隔（秒） |

## [rooms.\<name\>]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `url` | string | — | **是** | B 站直播间 URL |

### [rooms.\<name\>.record]

所有字段均为可选，缺省继承 `[record]`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `credential` | string | 拉流账号名 |
| `qn` | u32 | 画质档位 |
| `cdn` | string[] | CDN 偏好列表 |

### [rooms.\<name\>.upload]

| 字段 | 类型 | 说明 |
|------|------|------|
| `enabled` | bool | 是否自动上传；存在全局 `[upload]` 时默认 `true`，否则默认 `false` |
| `credential` | string | 上传账号名（缺省继承 `[upload].credential`） |
| `delete_after_submit` | bool | 投稿确认后删除本地文件（缺省继承 `[upload]`） |

### [rooms.\<name\>.submit]

所有字段均为可选，缺省继承 `[submit]`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `title` | string | 投稿标题模板 |
| `description` | string | 投稿简介模板 |
| `category_id` | u16 | 投稿分区 ID |
| `copyright` | string | 版权类型 |
| `source` | string | 转载来源；支持房间模板 |
| `tags` | string[] | 投稿标签 |
| `private` | bool | 仅自己可见 |
| `dynamic` | string | 同步动态文案 |
| `forbid_reprint` | bool | 禁止转载 |
| `charging_panel` | bool | 开启充电面板 |
| `close_reply` | bool | 关闭评论 |
| `close_danmu` | bool | 关闭弹幕 |
| `featured_reply` | bool | 精选评论 |
