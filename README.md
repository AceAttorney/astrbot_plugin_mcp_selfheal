# astrbot_plugin_mcp_selfheal

通用 MCP 会话自愈插件。解决「MCP 服务端重启后，AstrBot 侧永久失联、必须重启进程才能恢复」这一类问题。

## 它解决什么问题

典型故障链（与 MCP 服务端是什么、用什么语言写完全无关）：

1. MCP 服务端自身重启（更新、崩溃、容器重建），内存中的会话表丢失；
2. AstrBot 仍持旧 `Mcp-Session-Id` 发请求，服务端返回 HTTP 404；
3. `mcp` Python SDK 把 404 翻译为 `McpError(32600, "Session terminated")`；
4. AstrBot 内核 `mcp_client.py` 的 `call_tool_with_reconnect` 重试条件只认 `anyio.ClosedResourceError`，32600 不在其中 → 不重连、不重试；
5. 失效 session 被一直沿用，此后每次调用都失败，直到重启 AstrBot 进程。

关键点：**这是一次性永久失效，不是网络抖动**，重试无意义，必须重建会话。

## 三层防御

| 层 | 作用 | 说明 |
|---|---|---|
| L1 判定 | `session_death_reason(exc)` 识别会话死亡 | 强信号优先（`McpError` 32600、HTTP 404/410、`anyio` 的 `ClosedResourceError`/`BrokenResourceError`/`EndOfStream`、`httpx` 的 `RemoteProtocolError`/`ConnectError`/`ReadError`/`WriteError`、AstrBot 的 `ValueError: session is not available`），弱特征串兜底；先递归拆 `ExceptionGroup` |
| L2 自愈 | 命中即调 AstrBot 自带 `_reconnect()`（复用其 `_reconnect_lock`）后原样重试 | 最多 2 轮，防重连风暴；`*args, **kwargs` 全量透传以抗升级签名变更 |
| L3 探活 | 后台定时 `send_ping()` 提前发现并重建失效会话 | 把「用户调用时才炸」变「后台悄悄修好」，顺带保活抑制空闲 TTL；不支持 ping 的服务端自动跳过，只保留 L2 |

### 明确不重试的情况

超时类异常（`ReadTimeout`、`asyncio.TimeoutError`、`TimeoutError`）**绝不重试**：工具可能已经在服务端执行完成，重试会产生副作用。而 32600 / 404 发生在 HTTP 请求阶段，服务端根本没执行，重试安全。

## 安装

把本目录放到 AstrBot 的 `data/plugins/` 下，重启容器或在 WebUI 插件页重载即可。无第三方依赖，不需要 `requirements.txt`。

要求 AstrBot `>=4.26,<5`。

## 配置项

可在 WebUI 插件配置页调整（优先级：**WebUI 配置 > 环境变量 > 默认值**）。

| 键 | 环境变量 | 默认 | 说明 |
|---|---|---|---|
| `enabled` | `MCP_SELFHEAL_ENABLED` | `true` | 总开关 |
| `heal_attempts` | `MCP_SELFHEAL_ATTEMPTS` | `2` | 命中后最多重连重试轮数 |
| `backoff` | — | `0.6` | 每轮之间的退避秒数 |
| `ping_enabled` | `MCP_SELFHEAL_PING` | `true` | 是否启用后台探活 |
| `ping_interval` | — | `180` | 探活间隔秒数（下限 30） |
| `ping_timeout` | — | `15` | 单次探活超时秒数 |
| `ping_start_delay` | — | `30` | 启动后延迟多久开始探活 |

## 指令

`mcp_selfheal`（别名 `mcp自愈`、`mcp_heal`）

输出内容：补丁安装状态、被探活的客户端数、自愈成功/失败次数、探活重建次数、触发原因分布、当前生效参数。

统计持久化在 `data/plugin_data/astrbot_plugin_mcp_selfheal/stats.json`（原子写 + 启动时合并）。

## 日志

每次自愈都会打印判定原因，便于回溯是哪类异常触发：

```
[astrbot_plugin_mcp_selfheal] 会话失效（McpError 32600: Session terminated），第 1/2 轮重建后重试
```

## 已知限制与风险

- 本插件通过**运行时 monkeypatch** 包装 `MCPClient` 的 `_reconnect` / `session` / `_server_name`。AstrBot 目前没有暴露「MCP 会话失效自动重建」的公开钩子，因此只能在客户端层包一层。不属于文档覆盖范围，但不修改任何磁盘文件，升级后自动重打。
- 若 AstrBot 大版本重命名了这些内部方法/属性，补丁会安装失败并记录日志，插件本身不会拖垮 AstrBot，只是退化为无自愈状态。
- **根治建议**：给 AstrBot 提 issue/PR，让核心 `mcp_client.py` 的重试条件也认 32600。本插件是可回滚的过渡方案。

## 卸载 / 回滚

```bash
rm -rf /AstrBot/data/plugins/astrbot_plugin_mcp_selfheal
docker restart astrbot
```

补丁只存在于运行时内存，磁盘上除本目录外零改动。
