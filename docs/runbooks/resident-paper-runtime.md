# 驻留模拟盘运行手册

本手册只启动 QMT 实时行情和 PostgreSQL 模拟券商。当前版本不导入 XtTrader，也没有任何
真实委托或撤单路径。

## 安全边界

- 只能在 `AQ_ENVIRONMENT=paper` 下装配；
- `AQ_LIVE_TRADING_ENABLED` 必须保持 `false`；
- 冷启动时持久化停机开关必须为 active，进程不会自动复位；
- 必须存在一条仍有效的 OOS 策略批准记录；
- 内部订单、模拟券商、scheduler 证据链和当前交易日日历必须完整重放；
- 行情连接前必须取得以上全部证据，失败时不会订阅 QMT；
- 行情断开、回调异常、队列溢出、租约丢失、证据写入失败都会重新激活停机开关；
- 单进程租约不允许同一模拟账户被两个 scheduler 同时驱动。

## Windows 节点配置

先完成 [QMT 只读接入准备](qmt-read-only-preparation.md)，并使用券商提供的固定 XtQuant
制品。驻留模拟盘另需配置：

```dotenv
AQ_ENVIRONMENT=paper
AQ_LIVE_TRADING_ENABLED=false
AQ_PAPER_ACCOUNT_ID=paper-main
AQ_PAPER_STRATEGY_ID=validated-sma-paper
AQ_PAPER_INITIAL_CASH=1000000

AQ_PAPER_SCHEDULER_HOLDER_ID=paper-node-01
AQ_PAPER_SCHEDULER_LEASE_TOKEN=至少32字符的独立随机秘密
AQ_PAPER_POLL_INTERVAL_SECONDS=1
AQ_PAPER_SCHEDULER_LEASE_TTL_SECONDS=30
AQ_PAPER_SCHEDULER_RENEWAL_SECONDS=10
```

`holder_id` 标识固定服务节点；`lease_token` 只保存在该节点受限的 `.env` 中，不能复制到
日志、工单或代码仓库。轮换 token 前必须先停止旧进程，并等待旧租约明确释放或过期。

## 冷启动检查

先刷新目标交易日的精确日历。若当日开市，还要刷新策略标的的 session reference。不得
用“工作日”猜测交易日，也不得把尚未发布的 `stk_limit` 当作完整数据。

```powershell
uv run autoquant db-check
uv run autoquant operations-readiness-report --campaign-hash <可选的数据活动哈希>
uv run autoquant qmt-check
uv run autoquant paper-runtime-check
```

统一报告的字段、退出码和归档要求见
[运维就绪证据报告](operations-readiness-report.md)。报告返回 2 时不得继续启动。

`qmt-check` 还必须显示 `trusted_clock=pass`。它以 PostgreSQL 时钟约束 Windows 主机时间；
时钟未知、最坏偏差超过 2 秒或测量往返超过 2 秒时，不得打开 XtTrader。

`paper-runtime-check` 不导入 XtQuant、不连接 MiniQMT、不复位停机开关。成功状态
`ready_for_quote_connection` 仅表示数据库证据允许打开行情连接，不表示允许模拟下单，更
不表示允许实盘。

如果返回 `paper runtime readiness check failed`，依次检查：

1. 是否已有当前策略的 active 批准记录；
2. 停机开关是否仍为 active；
3. 当前上海日期的 `trade_cal` 是否已刷新并有 PostgreSQL 来源证据；
4. PostgreSQL 订单与模拟券商的数量、未结订单是否收敛；
5. scheduler 事件链是否可完整重放。

## 启动和停止

全部冷启动检查通过后，在授权 Windows 节点运行：

```powershell
uv run autoquant run-paper
```

启动顺序固定为：

1. 重放冷启动证据；
2. 调用 `get_full_tick` 建立完整交易池基线；
3. 调用 `subscribe_whole_quote`；
4. 获取账户级 scheduler 租约；
5. 在停机开关 active 状态下驻留并记录 locked/idle/盘前周期。

当前进程不会自动解锁，所以首次启动不会生成订单。只有在驻留进程已经持有有效 scheduler
租约、当日 session 已初始化且处于连续交易时段时，才可在另一个受控终端执行：

```powershell
uv run autoquant unlock-paper `
  --actor operator `
  --confirm-paper-unlock
```

该命令只调用只读 `get_full_tick`，不会另建交易连接。它把完整实时行情、当前 active
策略批准、当日 session-risk 状态、内部订单与模拟券商即时对账、scheduler
holder/token/generation 组合成不可变证据，再由 PostgreSQL 在一次事务内重新验证全部
状态并复位模拟盘停机开关。证据最多允许 3 秒；租约释放/过期、策略撤销、session
状态变化、行情关闭或对账变化都会拒绝复位。

`paper_unlocked` 仅允许 PostgreSQL 模拟券商接受经过风控的订单；真实 QMT 提交和撤单
仍然代码级硬锁。每次进程重启都必须从 active 停机开关重新开始，不能复用旧证据。
如果上次进程被强制终止并遗留 inactive 状态，下一次冷启动检查会先重新激活停机开关，
然后拒绝本次启动；修复退出原因后再次启动并重新收集解锁证据。

正常维护优先使用控制台中断进程。中断会停止调度、反订阅行情、尝试释放租约并保持停机
开关 active。若进程崩溃，租约到期后才允许新实例接管；不得通过修改数据库绕过租约。

## 运行健康与静默停摆

交易控制台不会把“历史事件能够重放”当作“驻留进程当前健康”。`GET /api/v1/trading`
中的 execution 状态同时检查：

- PostgreSQL 中仍有效且策略身份一致的 scheduler 租约；
- 最新 scheduler 事件的数据库写入时间，而不是运行主机自报时间；
- 最新事件的策略身份、阶段、状态和错误码；
- 最新写入距数据库当前时间不超过 `max(lease_ttl, 3 × poll_interval)`。

`scheduler_runtime_status` 可能为 `healthy`、`starting`、`stopped`、`stale`、
`failed` 或 `identity_mismatch`。除 `healthy` 外都会保留
`scheduler_runtime_liveness` 门槛；事件哈希链完整但租约失效或写入陈旧时，也不能作为
持续模拟盘运行证据。

独立监控节点可定时执行：

```bash
uv run autoquant paper-watchdog-check
```

该命令只读 PostgreSQL，不连接 XtData、MiniQMT 或任何订单接口。仅 `healthy` 返回退出码
0，其余状态返回退出码 2，并输出稳定的 JSON，可直接交给 cron、systemd、Prometheus
exporter 或告警脚本处理。监控进程必须与 `run-paper` 分离，避免驻留进程自身卡死时同时
失去检测能力。

需要自动保护时，外部监控应调用：

```bash
uv run autoquant paper-watchdog-enforce
```

该命令在运行健康时不改变控制状态；发现 `stopped`、`stale`、`failed` 或
`identity_mismatch` 时，只允许将模拟盘停机开关激活，并把确定性的 incident hash
写入控制审计链。它没有复位停机开关、连接券商或提交订单的能力。故障状态即使成功
fail closed 仍返回退出码 2，以保证告警不会因保护动作成功而被吞掉。

## Windows 计划任务安装

不要把 Token、数据库 DSN、QMT 账号或租约 Token 放进任务参数。两个安装器只保存
`uv.exe`、项目目录和固定子命令；运行时仍从项目根目录的 `.env` 读取配置。
安装前必须在 Windows 项目目录运行一次 `uv sync --frozen`。安装器要求已提交的
`uv.lock` 和 Windows `.venv\Scripts\python.exe` 同时存在；计划任务使用
`--frozen --no-sync`，运行期间不会下载依赖或改写虚拟环境。

先在授权 Windows 节点使用 `-WhatIf` 检查驻留任务：

```powershell
.\scripts\windows\install-paper-runtime-task.ps1 `
  -ProjectPath (Resolve-Path .) `
  -UvPath "$env:USERPROFILE\.local\bin\uv.exe" `
  -WhatIf
```

确认路径后去掉 `-WhatIf`。任务默认在当前用户登录两分钟后启动，使用交互式低权限
Token，失败后每分钟重试，但安装过程不会立即启动进程。已存在同名任务时必须显式传
`-Replace`，防止静默覆盖人工配置。该任务只运行 `run-paper`，仍从 active 停机开关
开始且不会自动解锁。

watchdog 应优先安装在另一台能够访问 PostgreSQL 的主机：

```powershell
.\scripts\windows\install-paper-watchdog-task.ps1 `
  -ProjectPath (Resolve-Path .) `
  -UvPath "$env:USERPROFILE\.local\bin\uv.exe" `
  -IntervalMinutes 1 `
  -WhatIf
```

去掉 `-WhatIf` 后每分钟执行一次 `paper-watchdog-enforce`。若必须在无人登录的独立
Windows 监控节点运行，可使用 `-RunAsSystem`，但必须先确认 SYSTEM 对项目目录和
`.env` 只有所需的读取权限，并能访问 PostgreSQL；不得通过放宽 `.env` 到所有用户可读
来解决权限问题。同一台 QMT 主机上的 watchdog 只能作为第二层保护，不能替代真正独立
的监控节点。

## 行情恢复

XtData 回调线程只复制原始标量到有界队列。任何异常都会使整条行情失效，不能继续使用
最后价格：

1. 停止 scheduler；
2. 激活停机开关；
3. 反订阅旧 subscription；
4. 重新读取精确日历；
5. 重新调用 `get_full_tick` 建立新基线；
6. 再订阅全推行情并进行只读观察。

在 Windows 节点完成断网、MiniQMT 重启、跨午休、跨交易日和进程崩溃演练之前，不得把该
进程列为生产就绪。
