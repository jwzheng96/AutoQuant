# QMT 隔离网关边界设计

- 日期：2026-07-23
- 状态：契约、预检、硬锁、查询/回调只读汇合核心已实现；未连接 MiniQMT，未读取真实
  账户，未开放委托
- 目标：先证明 Windows QMT 节点可观测、可恢复、可对账，再讨论任何资金实盘权限

## 1. 信任边界

QMT 只能运行在单独管理的 Windows 节点。研究服务不直接加载券商组件，QMT 回调线程
不查询账户、不写交易状态，也不调用策略。回调只复制为不可变信封并进入单消费者队列；
持久化协调器负责排序、幂等、状态迁移和对账。

当前 `LockedQmtGateway` 的提交和撤单方法无条件抛出 `LiveTradingLockedError`。
环境变量、MiniQMT 登录、权限文件或预检通过都不能解除此代码级硬锁。

## 2. 配置和预检

只在 Windows 网关节点的未跟踪 `.env` 中配置：

- `AQ_QMT_USERDATA_PATH`：MiniQMT 的绝对 `userdata_mini` 路径；
- `AQ_QMT_ACCOUNT_ID`：实际资金账号，按秘密处理，不进入日志和预检输出；
- `AQ_QMT_SESSION_ID`：正整数，并且必须与同机其他 Python 策略不同。

`autoquant qmt-check` 只读取配置和持久化停机开关。它不导入 XtQuant、不启动或连接
MiniQMT，也不查询账户。逐项检查 Windows、64 位 Python、目录、账号、会话号、模块、
`up_queue_xtquant` 权限标志和停机开关。schema v12 的跨进程会话租约表提供活动会话
集合；数据库不可用或 schema 未迁移时，会话号唯一性保持“未验证”而不是猜测通过。
开发机缺少其他条件时返回 `blocked` 是预期行为。

会话租约使用 PostgreSQL 事务级 advisory lock 串行化同一个 `session_id`。原始租约
令牌只存在于持有进程，数据库仅存 SHA-256；租约最长五分钟，续租必须同时匹配持有者、
令牌和未过期状态。获取与释放事件形成不可修改的哈希链。未来 Windows 适配器必须在
连接 MiniQMT 前原子获取租约，并在续租失败时立即断开并触发停机开关。

## 3. 数据契约

- 内部标的 `600000.XSHG` / `000001.XSHE` 显式映射为 QMT 的 `600000.SH` /
  `000001.SZ`；北交所和未知后缀在建立完整规则前拒绝，不猜测交易所。
- QMT 委托状态先验证原始状态码、委托量和累计成交量的一致性；矛盾或未知事实映射为
  `unknown`，触发后续停机处置，不能映射为“未成交”。
- 委托和成交查询的 `None` 不能与空列表区分，因此按券商状态未知处理；只有明确的空
  集合才代表没有记录。
- 成交量始终使用累计值，持久化序号必须由单消费者协调器分配，不能依赖回调到达顺序。

`QmtReadOnlyBaseline` 进一步约束四类官方查询：

1. `XtAsset`、`XtPosition`、`XtOrder`、`XtTrade` 必须使用同一个查询完成时间；
2. 查询前后 callback cursor 必须相同，查询期间出现回调就丢弃结果并重查；
3. 资产 `cash + frozen_cash + position market_value` 必须与 `total_asset` 对平，逐持仓市值
   合计必须与资产总市值一致；
4. 委托订单号、映射后的 client order ID 和成交编号必须唯一；
5. 每笔委托累计成交量必须与当日成交逐笔合计一致，成交方向、标的和均价也必须收敛；
6. 未知状态、孤立成交、重复事实、跨账号记录、未来/不一致观察时间都按 broker state
   unknown 处理；
7. QMT 真实资金账号只用于内存中的响应校验，生成的 `ExecutionAccountSnapshot` 使用受控
   logical account alias，持久化对账快照不写入真实账号。

`QmtReadOnlyRecoveryState` 只允许账号状态 `0`（正常）的连续心跳保留基线。断线、非正常
账号状态、回调序号缺口，以及任何委托、成交或错误回调都会清除可信快照并要求全量重查。
回调队列有固定容量，溢出后整个进程实例必须重连，不能继续 drain 后假装恢复。

Windows shim 只负责从 XtQuant 对象复制官方字段，并用该节点实际 `xtconstant` 将股票
买卖映射成 `buy` / `sell`；核心模块不硬编码券商包中的委托类型数值。

## 4. 晋级条件

下一阶段只能增加只读连接和双源对账，仍不得实现下单。至少需要：

1. Windows 节点上的固定 XtQuant 版本和制品哈希；
2. MiniQMT 登录与账户订阅的真实只读证据；
3. 在真实 Windows 节点验证资产、持仓、当日委托、当日成交字段和 logical account 映射；
4. 在真实 Windows 节点完成 `None`、断线、查询中回调、进程重启和 MiniQMT 重启演练；
5. QMT 与内部账本差异触发持久化停机开关；
6. 连续模拟盘证据、策略样本外门槛和合规审批。

任何单项通过都不是盈利证据，也不授权资金实盘。

## 5. 官方依据

- [XtQuant 快速开始](https://dict.thinktrader.net/nativeApi/start_now.html)
- [XtQuant 交易接口](https://dict.thinktrader.net/nativeApi/xttrader.html)
- [XtQuant 行情接口](https://dict.thinktrader.net/nativeApi/xtdata.html)
- [XtQuant 常见问题](https://dict.thinktrader.net/nativeApi/question_function.html)
