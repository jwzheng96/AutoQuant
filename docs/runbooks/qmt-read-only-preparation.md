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
schema v38 在候选行上增加强制预提交阶段。未来网关必须先持久化完整候选、`stage_hash`
和由候选哈希派生的 24 字符 ASCII `order_remark`，成功后才可把该 remark 传给
`order_stock_async`；取得正整数 `seq` 后才能追加请求号预留。请求号预留不再隐式创建
候选。当前租约 owner 可以跨历史 generation 读取“已预提交但没有请求号”的候选清单，
但不能自动重试：它代表进程可能恰好在券商调用后、请求号落库前崩溃，必须保持停机并用
完整券商委托查询和 remark 做后续恢复。
schema v39 增加只读 remark 恢复账本。由于官方查询接口只返回“当日所有委托”，且没有
承诺 `order_id` 跨交易日唯一，候选、当前 lease 的取得时间、数据库观察时间和完整 QMT
基线必须属于同一上海日期；进程跨日必须释放并重新取得 lease generation。恢复时只有
同日基线中恰好一笔委托同时匹配 remark、证券、方向、数量和价格才可追加恢复绑定，且
基线落库延迟最多 5 秒。没有匹配不能推断“未下单”，重复 remark 或字段冲突按券商状态
未知处理。恢复账本仍固定 `broker_mutation_allowed=false`，不会重试、撤单或修改券商。
schema v40 增加未来 Windows 交易协调器使用的不可变 QMT 回调收件箱。XtQuant 回调线程仍
只把普通标量复制到有界内存队列，单一消费者必须先按严格字段白名单验证并在 5 秒内持久化，
然后才能解释该回调。真实资金账号只在内存中与配置值比较，入库使用 logical account；
自由文本 `status_msg` 和真实账号都不会写入回调证据。事件绑定当前 holder、session、
lease generation 和上海日期，并按本地回调序号形成哈希链。相同序号、相同内容的重试是
幂等的；缺号、同序号内容冲突、跨 scope 回放、租约失效或数据库不可用都必须停止消费并
按券商状态未知处置，不能猜测订单结果。收件箱没有券商调用能力，数据库也约束
`broker_mutation_allowed=false`。
异步委托回报只有在完整收件箱哈希链证明该事件已经持久化后，才可写入订单关联账本。
logical account、XtQuant `seq`、券商 `order_id` 和预提交的 24 字符 `order_remark` 必须
同时指向同一候选；进程崩溃后可幂等重放同一事件，但任一身份不符都保持券商状态未知。
候选外键必须指向同一 holder 的真实 `acquire` 事件；预留事务还会锁定并检查当前租约
未释放、未过期且 generation 未变化。候选预留、异步券商订单号绑定和重启恢复都必须
提交当前租约 bearer token；数据库只比较其 SHA-256，并以数据库时钟在持有 lease 行锁
期间重新验证 holder、generation 和有效期。仅知道 session id、holder 或历史 generation
不能写入或恢复映射，旧进程在租约释放、超时或被接管后也不能处理迟到回报。

迅投原生交易文档说明，异步委托先返回请求序号 `seq`，之后
`on_order_stock_async_response` 才提供 `order_id`；`order_remark` 会进入委托、成交和
错误回报，长度最多 24 个英文字符。AutoQuant 的预提交顺序和恢复标签严格按这一边界
设计，参见 [XtQuant XtTrader 官方文档](https://dict.thinktrader.net/nativeApi/xttrader.html)。

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

实现会在取得会话租约之前完成 XtQuant 包清单哈希，因为该步骤不连接 MiniQMT，不能无意义
消耗租约窗口。取得租约后，独立守护任务在连接、最多三次一致性查询和证据落库期间按 TTL
的三分之一续租；证据写入前再次验证 holder、token、session 和 generation。续租、验证或
最终释放任一失败都会使命令失败并保持停机，不能把旧 generation 的结果当成有效新验收。
如果进程在券商同步查询期间收到取消，主协程会延迟传播取消，直到工作线程退出并执行
XtTrader `unsubscribe`/`stop`，之后才释放数据库租约。查询线程长期不返回时应终止整个
Windows 进程并等待租约过期，不能另起同 session id 的验收进程。

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
- 不在 schema v40 回调事实持久化成功前更新内部订单状态；持久化失败必须停机和全量重查。
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
   `xtconstant.STOCK_BUY` / `STOCK_SELL` 映射为 `buy` / `sell`；委托和成交中的
   `order_remark` 必须逐笔复制、验证 24 字节上限并纳入基线哈希；
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
