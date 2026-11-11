# Changelog

## 1.1.0

### 新增
- `README.md`、`CHANGELOG.md`，补齐 WebUI 插件页展示所需的文档资源。
- `_conf_schema.json`，7 项配置可在 WebUI 直接调整；优先级为 WebUI 配置 > 环境变量 > 默认值。
- 模块级 `_ACTIVE` 引用当前活跃实例：WebUI 重载插件后，统计与补丁自动归新实例，不再写入已卸载的旧实例。

### 修复
- `ExceptionGroup` 拆分错误：内建名 `BaseExceptionGroup` 不能用 `globals().get()` 取（内建名不在模块 globals 里，永远得到 `None`），改为 `from builtins import` 显式导入，修复包在异常组内的 `McpError 32600` 漏判。
- 后台探活单轮异常会终止整个看门狗线程：改为单轮失败仅记录日志、继续下一轮。
- 删除残留的无用变量 `self._dirty`。

### 规范
- `astrbot_version` 由 `>=4.26` 收紧为 `>=4.26,<5`（实测 4.29.4 在范围内）。
- `metadata.yaml` 补充 `short_desc`，移除空的 `repo` 字段。
- 通过 `ruff check` / `ruff format --check`。

## 1.0.0

### 新增
- 三层防御：L1 会话死亡判定（`session_death_reason`）、L2 自动重建并重试、L3 后台定时探活。
- 判定层支持 `McpError` 32600、HTTP 404/410、`anyio` 资源类异常、`httpx` 传输类异常、`ValueError: session is not available`，并递归拆分 `ExceptionGroup`。
- 超时类异常明确不重试，避免对已执行工具产生副作用。
- `mcp_selfheal` 指令（别名 `mcp自愈`、`mcp_heal`）输出运行状态与统计。
- 统计持久化至 `data/plugin_data/astrbot_plugin_mcp_selfheal/stats.json`（原子写 + 启动合并）。
