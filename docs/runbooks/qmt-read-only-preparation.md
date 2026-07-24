# QMT 只读接入准备手册

本手册只准备只读核验。当前版本不能向券商提交或撤销委托。

代码同时提供 `QmtCanaryOrderCandidate` 和 `QmtOrderCorrelationBook`，用于提前固化未来金丝雀
网关的输入边界。候选单逐笔绑定已接受的 live 风控决策、晋级报告、合规批准、QMT 只读
验收、最新对账、Windows 节点和 QMT session；最长有效 30 秒且只覆盖一笔订单。异步请求
号、券商订单号和 `client_order_id` 必须一一对应，未知或冲突映射按券商状态未知处理。
这些对象只生成可哈希证据，`broker_mutation_allowed` 固定为 `false`，调用执行检查仍会
抛出发布锁异常；它们不包含也不调用任何 XtQuant 报单/撤单函数。
schema v37 将候选、异步请求号预留和券商订单号绑定拆成三张不可变追加表，并将关联范围
绑定到 holder、QMT session 和数据库租约 generation。Windows 进程重启时必须从当前
generation 的账本恢复关联簿后再解释委托回报；持久化事实缺失、哈希不符或身份重复时
不得把订单猜测为新单，也不得继续接受新的候选。新的租约 generation 可以安全重用
XtQuant 重新计数的请求序号，但不能跨 generation 解释回报。
候选外键必须指向同一 holder 的真实 `acquire` 事件；预留事务还会锁定并检查当前租约
未释放、未过期且 generation 未变化。仅知道 session id 或历史 generation 不能写入预留。

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
AQ_QMT_HOLDER_ID=windows-qmt-readonly-01
AQ_QMT_LEASE_TOKEN=至少32字符的独立随机秘密
AQ_QMT_LEASE_TTL_SECONDS=30
```

同时保留：

```dotenv
AQ_LIVE_TRADING_ENABLED=false
```

确保 `.env` 仅当前运行账户可读。账号、路径和会话号不会出现在 `qmt-check` 输出中。
租约令牌也不得写入日志、命令行或版本库；数据库仅保存其 SHA-256。

## 运行预检

先确保 PostgreSQL 可访问，且 `AQ_PAPER_ACCOUNT_ID` 对应的持久化停机开关已初始化并
处于激活状态，然后运行：

```powershell
uv run autoquant db-check
uv run autoquant qmt-check
```

`qmt-check` 输出十项 `pass`/`blocked`，但 `live_trading_ready` 永远为 `false`，也不会
连接 MiniQMT。`session_id_unique` 来自 schema v12 的跨进程活动租约查询；数据库不可用、
未迁移或相同会话号已有有效租约时保持 `blocked`。实际网关连接前仍必须原子获取租约，
预检本身不占用会话号。

## 生成只读验收证据

完成预检并确认 MiniQMT 已人工登录后，在 Windows 节点运行：

```powershell
uv run autoquant qmt-readonly-accept `
  --actor operator `
  --confirm-read-only
```

该命令会原子取得 schema v12 的 QMT 会话租约，加载券商 XtQuant 包，计算包目录制品清单
SHA-256，连接并订阅配置账户，确认账户状态为正常，然后在回调游标不变化的窗口内依次读取
资产、持仓、当日委托和当日成交；整组查询最多允许 5 秒。四项数据会经过账户一致性、
资产平衡、委托/成交收敛校验。
通过后仅把 schema v17 的脱敏验收证据写入 PostgreSQL；真实资金账号、余额、持仓明细、
路径和租约原文都不会写入证据表或命令输出。

无论成功失败，命令都会停止 XtTrader 并释放会话租约。租约释放失败或任一查询事实不明确时
命令失败，持久化停机开关保持或恢复为激活。该命令不包含任何下单、撤单或资金划拨调用。

验收后可在受认证的 `/trading` 页面或 `GET /api/v1/qmt` 查看最近证据时间、脱敏记录数量、
当前控制台主机的逐项 `pass`/`blocked` 和剩余演练门禁。证据超过 24 小时会显示为过期；
页面不提供验收、解锁、下单或撤单操作。

## 故障恢复演练

schema v18 使用 30 分钟挑战窗口，分别记录断网和 MiniQMT 重启恢复。每次演练先运行上面的
`qmt-readonly-accept` 取得新鲜基线，然后开始挑战：

```powershell
uv run autoquant qmt-drill-start `
  --kind disconnect_recovery `
  --actor operator `
  --confirm-controlled-drill
```

保存输出中的 `drill_id`，不要保存 `.env`。随后启动 `run-paper`，按演练类型执行受控断网
或人工重启 MiniQMT，并确认驻留进程失败关闭。恢复网络/MiniQMT 后，再次运行
`qmt-readonly-accept` 取得新证据，最后在挑战过期前执行：

```powershell
uv run autoquant qmt-drill-complete `
  --drill-id <drill_id> `
  --actor operator `
  --confirm-intervention-complete
```

完成命令不会只相信人工确认。PostgreSQL 必须同时看到：

1. 开始时绑定的新鲜 QMT 只读基线；
2. 开始之后由运行时写入的 `dependency_unavailable` 或 `recovery_failed` 停机事件；
3. 停机事件之后生成、且不同于基线的 QMT 只读验收；
4. 完成时停机开关仍为 active。

缺少任一事实、挑战超时或重复并发挑战都会失败关闭。完成
`disconnect_recovery` 后还必须用同样流程单独完成
`miniqmt_restart_recovery`；一个事件不能同时满足两类演练。当前流程证明“故障被停机控制
捕获，且环境随后恢复到只读可验收状态”，不证明自动重连，也不授权真实交易。

## 失败处理

- `windows_runtime`：命令不在 Windows 节点运行。
- `python_64_bit`：Python 不是 64 位。
- `userdata_path`：路径不是绝对路径、目录不存在或末级目录不是 `userdata_mini`。
- `account_id` / `session_id`：秘密配置缺失；不要把值贴到工单或聊天。
- `session_id_unique`：会话号已被另一适配器占用，分配新值。
- `xtquant_module`：当前 Python 环境无法发现券商提供的模块。
- `order_permission`：缺少 `up_queue_xtquant`，联系券商确认权限。
- `kill_switch`：停机开关未激活或数据库不可验证；先修复持久化控制面。
- 查询期间出现回调：停止其他客户端操作后重试，不能拼接两次查询结果。
- 查询返回 `None`：官方接口无法区分失败与空集合；按未知状态处理，不得手工改成空列表
  绕过。

## 仍然禁止的操作

- 不修改 `AQ_LIVE_TRADING_ENABLED`；当前配置模型会拒绝 `true`。
- 不直接调用 `order_stock`、`order_stock_async` 或撤单函数做“连通性测试”。
- 不把查询的 `None` 当作空持仓、空委托或空成交。
- 不在 XtQuant 回调线程内执行同步查询。
- 不用本阶段的适配器或预检结果宣称可盈利或可上实盘。

## 行情接入边界

代码已提供不导入 XtQuant 的 `QmtWholeQuoteBridge`。Windows 装配层必须：

1. 只对配置交易池调用 `get_full_tick`，将完整结果作为初始/重连基线；
2. 通过 `subscribe_whole_quote` 接收 `{stock_code: tick}` 回调；
3. 在回调时附带由 point-in-time 日历和交易时钟判定的阶段，再交给有界队列；
4. 由单一消费线程 drain，不能在 XtData 回调线程内运行策略或同步查询；
5. 断线、队列溢出或任一异常后停止调度，重新连接并取得完整新基线，不能靠增量自愈。

XtData 的回调结构、tick 字段、证券状态和毫秒时间戳以
[迅投官方 XtData 文档](https://dict.thinktrader.net/nativeApi/xtdata.html) 为准。

## 交易侧只读基线

Windows 装配层建立连接并成功订阅账户后，按以下顺序构建只读基线：

1. 记录 `QmtCallbackBuffer.cursor`；
2. 依次查询 `query_stock_asset`、`query_stock_positions`、`query_stock_orders` 和
   `query_stock_trades`；
3. 再次记录 cursor；前后不同则丢弃全部查询结果并重试，不能拼接新旧快照；
4. 只复制官方字段到普通标量字典；股票 `order_type` 通过当前 XtQuant 包的
   `xtconstant.STOCK_BUY` / `STOCK_SELL` 映射为 `buy` / `sell`；
5. 使用配置中的真实账号验证每条返回记录，但把内部 `AQ_PAPER_ACCOUNT_ID` 作为 logical
   account 交给持久化对账；
6. 四项任一返回 `None`、状态未知、资产不平、委托/成交不收敛，都激活停机开关；
7. 基线后持续 drain 回调。只有账户状态 `0` 心跳不触发重查，其余回调先撤销快照信任，
   再完整执行上述四项查询。

官方文档明确说明资产查询 `None` 是失败；委托、成交和持仓的 `None` 可能同时代表失败或
空集合。因此 AutoQuant 对四类 `None` 均不作“空账户”推断。

schema v14 同时要求常驻模拟盘 scheduler 在启动前获取账户级进程租约并持续续租。租约
丢失会激活停机开关；不能以重启进程绕过。下一阶段仍需在 Windows 增加真实只读连接、
账户订阅、回调/查询汇合和断线恢复演练，完成前无需向系统提供任何交易授权。
