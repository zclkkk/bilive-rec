# Configuration Fields Reference

完整的配置字段速查表。详细的使用说明和示例请参阅 [配置详解](../guide/configuration.md)。

## [data]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `dir` | path | `./data` | 否 | 状态数据库存放目录 |

## [credentials.\<name\>]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `cookie_file` | path | — | **是** | B 站登录 Cookie 文件路径 |

## [record]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `credential` | string | 无 | 否 | 拉流账号名（不填则匿名拉流） |
| `output_dir` | path | `./data/recordings` | 否 | 录制文件存放目录 |
| `segment_time` | string | 无 | 否 | 分段时长阈值，格式 `HH:MM:SS` |
| `segment_size` | string | 无 | 否 | 分段大小阈值，支持 `KiB`/`MiB`/`GiB` 后缀 |
| `min_segment_size` | string | `20MiB` | 否 | 最小分段大小 |
| `qn` | u32 | `10000` | 否 | 画质档位（10000 = 原画） |
| `cdn` | string[] | `[]` | 否 | CDN 偏好列表 |
| `delete_after_submit` | bool | `false` | 否 | 投稿确认后删除本地文件 |

## [upload]

`run` 和 `upload` 命令必须配置此区块。

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `credential` | string | 无 | 视命令 | 上传账号名 |
| `line` | string | `auto` | 否 | 上传线路：`auto` 或 `bda2` |
| `threads` | usize | `3` | 否 | 单文件上传并发数 |
| `submit_api` | string | `app` | 否 | 投稿 API：`app`、`web` 或 `bcut_android` |

## [submit]

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `title` | string | 无 | 否 | 投稿标题模板 |
| `description` | string | 无 | 否 | 投稿简介模板 |
| `category_id` | u16 | `171` | 否 | 投稿分区 ID |
| `copyright` | string | `reprint` | 否 | 版权类型：`original` 或 `reprint` |
| `source` | string | run: `{url}`；upload: 无 | 否 | 转载来源。run 模式支持房间模板；手动 upload 的 `reprint` 投稿必须显式填写普通字符串 |
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
| `delete_after_submit` | bool | 投稿确认后删除本地文件 |

### [rooms.\<name\>.upload]

| 字段 | 类型 | 说明 |
|------|------|------|
| `credential` | string | 上传账号名（缺省继承 `[upload].credential`） |

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
