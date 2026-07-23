# QMT 只读接入准备手册

本手册只准备只读核验。当前版本不能向券商提交或撤销委托。

## 前提

1. 准备独立的 64 位 Windows 主机，不在当前 macOS 开发机安装 XtQuant。
2. 安装券商提供的 MiniQMT，人工登录正确账户，并保持客户端运行。
3. 向券商确认 XtQuant 程序化交易权限。`userdata_mini` 中缺少
   `up_queue_xtquant` 时，按权限未开通处理并联系券商。
4. 使用项目锁定的 Python 3.11 环境。XtQuant 版本以券商实际提供且验证通过的制品为准，
   记录安装包版本与 SHA-256；不要把二进制包提交到仓库。
5. Windows 节点只允许回环或受控内网管理，不公开 Web 控制台和数据库端口。

## 本地秘密配置

在 Windows 项目根目录的未跟踪 `.env` 中增加：

```dotenv
AQ_QMT_USERDATA_PATH=C:\path\to\userdata_mini
AQ_QMT_ACCOUNT_ID=实际资金账号
AQ_QMT_SESSION_ID=一个与同机其他策略不同的正整数
```

同时保留：

```dotenv
AQ_LIVE_TRADING_ENABLED=false
```

确保 `.env` 仅当前运行账户可读。账号、路径和会话号不会出现在 `qmt-check` 输出中。

## 运行预检

先确保 PostgreSQL 可访问，且 `AQ_PAPER_ACCOUNT_ID` 对应的持久化停机开关已初始化并
处于激活状态，然后运行：

```powershell
uv run autoquant db-check
uv run autoquant qmt-check
```

`qmt-check` 输出十项 `pass`/`blocked`，但 `live_trading_ready` 永远为 `false`，也不会
连接 MiniQMT。当前 CLI 尚未连接跨进程会话注册表，因此 `session_id_unique` 会诚实地
保持 `blocked`；只有后续受管网关向预检提供活动会话集合时，整体就绪状态才可能通过。

## 失败处理

- `windows_runtime`：命令不在 Windows 节点运行。
- `python_64_bit`：Python 不是 64 位。
- `userdata_path`：路径不是绝对路径、目录不存在或末级目录不是 `userdata_mini`。
- `account_id` / `session_id`：秘密配置缺失；不要把值贴到工单或聊天。
- `session_id_unique`：会话号已被另一适配器占用，分配新值。
- `xtquant_module`：当前 Python 环境无法发现券商提供的模块。
- `order_permission`：缺少 `up_queue_xtquant`，联系券商确认权限。
- `kill_switch`：停机开关未激活或数据库不可验证；先修复持久化控制面。

## 仍然禁止的操作

- 不修改 `AQ_LIVE_TRADING_ENABLED`；当前配置模型会拒绝 `true`。
- 不直接调用 `order_stock`、`order_stock_async` 或撤单函数做“连通性测试”。
- 不把查询的 `None` 当作空持仓、空委托或空成交。
- 不在 XtQuant 回调线程内执行同步查询。
- 不用本阶段的适配器或预检结果宣称可盈利或可上实盘。

下一阶段由代码增加真实只读连接、账户订阅、回调/查询汇合和断线恢复演练，完成前无需
向系统提供任何交易授权。
