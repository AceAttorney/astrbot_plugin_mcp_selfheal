"""通用 MCP 会话自愈插件（mcp_selfheal）。

## 要解决的是一类问题，不是某个服务

任何 MCP 服务端（streamable_http / SSE / stdio）只要它自己重启过，内存里的
会话表就没了。客户端还在用旧的 Mcp-Session-Id 发请求：

    旧 session id --POST--> 服务端不认识 --> 404
                          --mcp SDK 翻译--> McpError(32600, "Session terminated")

而 AstrBot 的 MCPClient.call_tool_with_reconnect 用 tenacity 重试时只认
anyio.ClosedResourceError，32600 不在其中 —— 于是不重连、不重试，坏掉的
session id 被一直沿用，此后每一次调用都以同样错误失败，直到进程重启。

这与 MCP 是哪个服务、用什么语言写的，完全无关，所以补丁打在 AstrBot 客户端层。

## 三层防御（全部服务无关）

L1 判定  session_death_reason()：只看异常的语义，不看具体服务。覆盖
        McpError 32600 / anyio 资源类异常 / HTTP 404·410 / httpx 协议类异常 /
        AstrBot 自己的 "session is not available"。
L2 自愈  call_tool_with_reconnect 命中判定 -> _reconnect() -> 原样重试。
        复用 AstrBot 自带的 _reconnect_lock，不另造并发控制。
L3 探活  后台定时 send_ping()，提前发现失效会话并修好，把"用户调用时才炸"
        变成"后台悄悄修好"；顺带保活，抑制空闲 TTL。

## 关于本插件的实现位置（README 级说明）

本插件是**运行时补丁**，不是 AstrBot 官方 API 能覆盖的能力：官方未暴露
"会话失效后自动重建"的钩子，故只能包装 MCPClient 的实例方法。因此它依赖
MCPClient 的 _reconnect / session / _server_name 三个私有属性；已用
*args/**kwargs 全量透传 + getattr 取值降低耦合，AstrBot 大版本若重命名这些
私有成员，本插件会记录"自愈失败"并原样抛错，不会拖垮 AstrBot（见 terminate
与 _install_patches 的错误处理）。
"""

from __future__ import annotations

import asyncio
import json
import os
import weakref
from contextlib import suppress
from pathlib import Path
from typing import Any

try:  # Python 3.11+ 才有内建异常组
    from builtins import BaseExceptionGroup as _ExceptionGroup
except ImportError:  # pragma: no cover - 3.10 及更早没有异常组
    _ExceptionGroup = None

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools

PLUGIN_NAME = "astrbot_plugin_mcp_selfheal"

# ---- 可调参数 -------------------------------------------------------------
# 取值优先级：WebUI 插件配置（_conf_schema.json）> 环境变量 > 此处默认值。
# 环境变量只在配置项缺失时生效，方便不改配置就临时实验。
DEFAULT_PARAMS: dict[str, Any] = {
    "enabled": True,
    "heal_attempts": 2,
    "backoff": 0.6,
    "ping_enabled": True,
    "ping_interval": 180.0,
    "ping_timeout": 15.0,
    "ping_start_delay": 30.0,
}

_ENV_NAMES = {
    "enabled": "MCP_SELFHEAL_ENABLED",
    "heal_attempts": "MCP_SELFHEAL_ATTEMPTS",
    "backoff": "MCP_SELFHEAL_BACKOFF",
    "ping_enabled": "MCP_SELFHEAL_PING",
    "ping_interval": "MCP_SELFHEAL_PING_INTERVAL",
    "ping_timeout": "MCP_SELFHEAL_PING_TIMEOUT",
    "ping_start_delay": "MCP_SELFHEAL_PING_START",
}

# 弱特征串：仅在没有任何强信号时才用于兜底判定
_SESSION_HINTS = (
    "session terminated",
    "session not found",
    "session expired",
    "session does not exist",
    "no valid session",
    "invalid session",
    "missing session",
    "mcp-session-id",
    "streamable http session",
)

_RESOURCE_ERRORS = {
    "ClosedResourceError",
    "BrokenResourceError",
    "EndOfStream",
    "BrokenPipeError",
    "ConnectionResetError",
    "ConnectionAbortedError",
    "SessionTerminated",
}

# 当前活跃的插件实例。WebUI 重载插件会新建实例，补丁是类级的、不会重装，
# 所以补丁必须每次从活跃实例取统计与参数，否则会一直写到被卸载的旧实例上。
_ACTIVE: MCPNetworkSelfHeal | None = None


def _coerce(key: str, raw: Any) -> Any:
    """把配置值宽松地转成目标类型，转换失败保留默认值。"""
    default = DEFAULT_PARAMS[key]
    if isinstance(default, bool):
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    if isinstance(default, float):
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default
    if isinstance(default, int):
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            return default
    return raw


def load_params(config: Any) -> dict[str, Any]:
    """合并配置来源，并对越界值做收敛。"""
    cfg = config if isinstance(config, dict) else {}
    params = dict(DEFAULT_PARAMS)
    for key in DEFAULT_PARAMS:
        if key in cfg and cfg[key] is not None:
            params[key] = _coerce(key, cfg[key])
            continue
        env = os.environ.get(_ENV_NAMES.get(key, ""))
        if env not in (None, ""):
            params[key] = _coerce(key, env)

    params["heal_attempts"] = max(0, int(params["heal_attempts"]))
    params["backoff"] = max(0.0, float(params["backoff"]))
    # 探活间隔太短会制造无意义的重连风暴，给个下限
    params["ping_interval"] = max(30.0, float(params["ping_interval"]))
    params["ping_timeout"] = max(1.0, float(params["ping_timeout"]))
    params["ping_start_delay"] = max(0.0, float(params["ping_start_delay"]))
    return params


def _current(default: MCPNetworkSelfHeal) -> MCPNetworkSelfHeal:
    """取活跃实例；被卸载后回退到补丁安装时的实例。"""
    return _ACTIVE if _ACTIVE is not None else default


def _iter_exceptions(exc: BaseException):
    """拆开 ExceptionGroup，anyio 的 task group 常把它包一层。"""
    if _ExceptionGroup is not None and isinstance(exc, _ExceptionGroup):
        for sub in exc.exceptions:
            yield from _iter_exceptions(sub)
    else:
        yield exc


def session_death_reason(exc: BaseException) -> str | None:
    """判断异常是否表示『MCP 会话已失效』。返回原因字符串，否则 None。

    强信号优先，弱特征串兜底。注意刻意**不含** ReadTimeout / TimeoutError：
    超时可能意味着工具已经执行过，重试会产生副作用（发消息、写文件、扣费），
    不能当作会话失效；而 32600/404 发生在 HTTP 请求阶段，服务端并未执行，
    重试是安全的。
    """
    for e in _iter_exceptions(exc):
        name = type(e).__name__

        # 1) anyio / 底层流资源类
        if name in _RESOURCE_ERRORS:
            return f"resource:{name}"

        # 2) McpError(32600 "Session terminated") —— 本次故障链的直接出口
        err = getattr(e, "error", None)
        code = getattr(err, "code", None)
        if code is None:
            code = getattr(e, "code", None)
        if code == 32600:
            msg = getattr(err, "message", "") or str(e)
            return f"mcp:32600({msg})"

        # 3) HTTP 404 / 410：服务端明确说不认识这个端点/会话
        resp = getattr(e, "response", None)
        status = getattr(resp, "status_code", None)
        if status in (404, 410):
            return f"http:{status}"

        # 4) 传输层协议异常
        if name in ("RemoteProtocolError", "ConnectError", "ReadError", "WriteError"):
            return f"transport:{name}"

        # 5) AstrBot 自己的空会话守卫
        text = str(e).lower()
        if isinstance(e, ValueError) and "session is not available" in text:
            return "astrbot:session-unavailable"

        # 6) 弱特征兜底
        for hint in _SESSION_HINTS:
            if hint in text:
                return f"hint:{hint}"

    return None


class MCPNetworkSelfHeal(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config
        self.params = load_params(config)
        self._stop = asyncio.Event()
        self._worker: asyncio.Task | None = None
        self._live: weakref.WeakSet = weakref.WeakSet()
        self.stats: dict[str, Any] = {
            "healed_calls": 0,
            "failed_heals": 0,
            "ping_reconnects": 0,
            "reasons": {},
        }
        self._patched = False
        self._stats_file = self._resolve_stats_file()

    # ------------------------------------------------------------------ 生命周期
    async def initialize(self) -> None:
        """插件被激活时调用（由 star_manager 在实例化后 await）。"""
        global _ACTIVE
        _ACTIVE = self
        self._load_stats()

        if not self.params["enabled"]:
            logger.info(f"[{PLUGIN_NAME}] 配置中已禁用，跳过安装补丁")
            return

        try:
            self._install_patches()
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] 安装补丁失败（不影响 AstrBot 运行）: {e}")
            return

        self._stop.clear()
        if self.params["ping_enabled"]:
            self._worker = asyncio.create_task(
                self._watchdog(), name="mcp-selfheal-watchdog"
            )
            logger.info(
                f"[{PLUGIN_NAME}] 已启用：调用级自愈 + 后台探活"
                f"（间隔 {self.params['ping_interval']:g}s）"
            )
        else:
            logger.info(f"[{PLUGIN_NAME}] 已启用：仅调用级自愈（探活在配置中关闭）")

    async def terminate(self) -> None:
        """插件被停用/重载时调用。"""
        global _ACTIVE
        self._stop.set()
        if self._worker and not self._worker.done():
            self._worker.cancel()
            with suppress(BaseException):
                await self._worker
        self._worker = None
        self._save_stats()
        if _ACTIVE is self:
            _ACTIVE = None
        logger.info(f"[{PLUGIN_NAME}] 已停止后台探活（类级补丁保留至进程结束）")

    # -------------------------------------------------------------- L1+L2 补丁
    def _install_patches(self) -> None:
        from astrbot.core.agent.mcp_client import MCPClient

        if getattr(MCPClient, "_mcp_selfheal_installed", False):
            self._patched = True
            logger.debug(f"[{PLUGIN_NAME}] 补丁已存在，跳过安装")
            return

        orig_call = MCPClient.call_tool_with_reconnect
        orig_connect = MCPClient.connect_to_server
        fallback = self

        async def connect_to_server(client_self, *args, **kwargs):
            # 全量透传：AstrBot 升级若增删参数，本补丁不会因签名不匹配而崩
            result = await orig_connect(client_self, *args, **kwargs)
            with suppress(Exception):
                _current(fallback)._live.add(client_self)
            return result

        async def call_tool_with_reconnect(client_self, *args, **kwargs):
            pl = _current(fallback)
            p = getattr(pl, "params", None) or DEFAULT_PARAMS
            tool_name = args[0] if args else kwargs.get("tool_name", "<unknown>")
            try:
                return await orig_call(client_self, *args, **kwargs)
            except BaseException as exc:
                reason = session_death_reason(exc)
                if reason is None:
                    raise
                server = getattr(client_self, "_server_name", None) or "?"
                pl._count_reason(reason)
                logger.warning(
                    f"[{PLUGIN_NAME}] {server} 工具 {tool_name} 命中会话失效"
                    f"（{reason}），启动自愈"
                )
                if p["heal_attempts"] <= 0:
                    logger.warning(f"[{PLUGIN_NAME}] 自愈轮数为 0，直接抛出原错")
                    raise

                last: BaseException = exc
                for attempt in range(1, p["heal_attempts"] + 1):
                    try:
                        await client_self._reconnect()
                    except BaseException as re_exc:
                        logger.error(
                            f"[{PLUGIN_NAME}] {server} 第 {attempt} 次重连失败: {re_exc}"
                        )
                        last = re_exc
                        await asyncio.sleep(p["backoff"] * attempt)
                        continue

                    if getattr(client_self, "session", None) is None:
                        logger.warning(
                            f"[{PLUGIN_NAME}] {server} 重连后仍无会话，重试下一轮"
                        )
                        await asyncio.sleep(p["backoff"] * attempt)
                        continue

                    try:
                        out = await orig_call(client_self, *args, **kwargs)
                    except BaseException as exc2:
                        reason2 = session_death_reason(exc2)
                        if reason2 is None:
                            raise
                        last = exc2
                        pl._count_reason(reason2)
                        logger.warning(
                            f"[{PLUGIN_NAME}] {server} 重连后仍失败"
                            f"（{reason2}），第 {attempt} 轮结束"
                        )
                        await asyncio.sleep(p["backoff"] * attempt)
                        continue

                    pl.stats["healed_calls"] += 1
                    logger.info(
                        f"[{PLUGIN_NAME}] {server} 自愈成功，工具 {tool_name} 已恢复"
                    )
                    return out

                pl.stats["failed_heals"] += 1
                logger.error(
                    f"[{PLUGIN_NAME}] {server} 自愈失败，放弃；"
                    f"原始错误将抛给上层（工具 {tool_name}）"
                )
                raise last

        MCPClient.call_tool_with_reconnect = call_tool_with_reconnect
        MCPClient.connect_to_server = connect_to_server
        MCPClient._mcp_selfheal_installed = True
        self._patched = True

    def _count_reason(self, reason: str) -> None:
        key = reason.split("(")[0]
        reasons = self.stats["reasons"]
        reasons[key] = reasons.get(key, 0) + 1

    # ---------------------------------------------------------------- L3 探活
    async def _watchdog(self) -> None:
        try:
            await asyncio.sleep(self.params["ping_start_delay"])
            while not self._stop.is_set():
                try:
                    await self._ping_round()
                    self._save_stats()
                except asyncio.CancelledError:
                    raise
                except BaseException as e:
                    # 单轮探活出任何意外都不能让看门狗整体死掉
                    logger.error(f"[{PLUGIN_NAME}] 探活轮次异常（已忽略）: {e}")
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.params["ping_interval"]
                    )
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            logger.error(f"[{PLUGIN_NAME}] 探活线程退出: {e}")

    async def _ping_round(self) -> None:
        p = self.params
        for client in list(self._live):
            if self._stop.is_set():
                return
            if not getattr(client, "active", True):
                continue
            server = getattr(client, "_server_name", None) or "?"
            session = getattr(client, "session", None)
            if session is None:
                # 客户端还没有会话（尚未连接或已断开），顺手补一个
                with suppress(Exception):
                    await client._reconnect()
                continue
            if getattr(client, "_selfheal_ping_off", False):
                continue
            try:
                await asyncio.wait_for(session.send_ping(), timeout=p["ping_timeout"])
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                reason = session_death_reason(exc)
                if reason is None:
                    # 例如服务端不支持 ping：只对这一个客户端永久关掉探活，
                    # 调用级自愈照常生效，避免每轮刷无意义的重连。
                    client._selfheal_ping_off = True
                    logger.info(
                        f"[{PLUGIN_NAME}] {server} 不支持探活 ping"
                        f"（{type(exc).__name__}），仅保留调用级自愈"
                    )
                    continue
                logger.warning(
                    f"[{PLUGIN_NAME}] 探活发现 {server} 会话失效（{reason}），主动重建"
                )
                try:
                    await client._reconnect()
                    self.stats["ping_reconnects"] += 1
                    logger.info(f"[{PLUGIN_NAME}] {server} 探活重建成功")
                except BaseException as re_exc:
                    logger.error(f"[{PLUGIN_NAME}] {server} 探活重建失败: {re_exc}")

    # ------------------------------------------------------------ 统计持久化
    @staticmethod
    def _resolve_stats_file() -> Path:
        """统计放 data 目录（插件目录会在更新/重装时被覆盖）。"""
        try:
            base = Path(StarTools.get_data_dir(PLUGIN_NAME))
        except Exception:
            base = Path("data") / "plugin_data" / PLUGIN_NAME
        return base / "stats.json"

    def _load_stats(self) -> None:
        try:
            if not self._stats_file.exists():
                return
            data = json.loads(self._stats_file.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return
            for key, cur in self.stats.items():
                val = data.get(key)
                if isinstance(cur, dict) and isinstance(val, dict):
                    self.stats[key] = {str(k): int(v) for k, v in val.items()}
                elif isinstance(cur, int) and isinstance(val, int):
                    self.stats[key] = val
            logger.info(
                f"[{PLUGIN_NAME}] 已载入历史统计：自愈 {self.stats['healed_calls']} /"
                f" 失败 {self.stats['failed_heals']} /"
                f" 探活重建 {self.stats['ping_reconnects']}"
            )
        except Exception as e:
            logger.warning(f"[{PLUGIN_NAME}] 读取历史统计失败（忽略）: {e}")

    def _save_stats(self) -> None:
        try:
            self._stats_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._stats_file.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(self.stats, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            tmp.replace(self._stats_file)
        except Exception as e:
            logger.debug(f"[{PLUGIN_NAME}] 写入统计失败（忽略）: {e}")

    # ------------------------------------------------------------------ 观测
    @filter.command("mcp_selfheal", alias={"mcp自愈", "mcp_heal"})
    async def cmd_status(self, event: AstrMessageEvent):
        """查看 MCP 会话自愈的实时统计。"""
        s = self.stats
        reason_txt = "、".join(f"{k}×{v}" for k, v in s["reasons"].items()) or "无"
        p = self.params
        lines = [
            "MCP 会话自愈状态",
            f"补丁已装: {'是' if self._patched else '否'}",
            f"探活客户端: {len(self._live)}",
            f"调用级自愈成功: {s['healed_calls']}",
            f"自愈失败: {s['failed_heals']}",
            f"探活重建: {s['ping_reconnects']}",
            f"触发原因: {reason_txt}",
            f"参数: 轮数={p['heal_attempts']} 间隔={p['ping_interval']:g}s"
            f" 探活={'开' if p['ping_enabled'] else '关'}",
        ]
        yield event.plain_result("\n".join(lines))
