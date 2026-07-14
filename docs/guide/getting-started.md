# Getting Started

## Installation

从源码编译：

```bash
git clone https://github.com/zclkkk/bilive-rec.git
cd bilive-rec
cargo build --release
```

编译产物位于 `target/release/bilive-rec`。

## 准备配置

复制示例配置文件：

```bash
cp config.example.toml config.toml
```

只录制的最小配置只需要一个房间：

```toml
[rooms.example]
url = "https://live.bilibili.com/123456"
```

需要自动上传时增加上传账号和 `[upload]`：

```toml
[credentials.main]
cookie_file = "./data/cookies.json"

[upload]
credential = "main"

[rooms.example]
url = "https://live.bilibili.com/123456"
```

其余配置项均有默认值，详见 [配置详解](configuration.md)。没有 `[upload]`
时，`run` 仍使用完整的持久化录制和崩溃恢复流程，但不会上传或投稿。

## 准备 Cookie

上传需要 Bilibili 登录 Cookie。匿名可访问的直播流可以不带 Cookie 录制；
需要登录画质时可单独配置录制账号。

1. 在浏览器中登录 [bilibili.com](https://www.bilibili.com)
2. 导出 Cookie 为 JSON 格式，保存到 `data/cookies.json`
3. 在配置文件的 `[credentials.<name>]` 中引用该文件路径

推荐使用专门的小号进行录制和上传。

## 首次运行

验证直播间是否可用：

```bash
bilive-rec check https://live.bilibili.com/123456
```

该命令会输出房间的开播状态和可用流地址列表。

启动全自动录制守护进程：

```bash
bilive-rec run
```

程序会持续监听所有配置的房间，开播时自动录制，分段完成后自动上传和投稿。按 `Ctrl-C` 优雅退出（再按一次强制退出）。
