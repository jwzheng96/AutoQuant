# QMT 隔离网关边界设计

- 日期：2026-07-23
- 状态：契约、预检和硬锁已实现；未连接 MiniQMT，未读取真实账户，未开放委托
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
`up_queue_xtquant` 权限标志和停机开关。跨进程会话注册表尚未接入时，会话号唯一性保持
“未验证”而不是猜测通过。开发机缺少这些条件时返回 `blocked` 是预期行为。

## 3. 数据契约

- 内部标的 `600000.XSHG` / `000001.XSHE` 显式映射为 QMT 的 `600000.SH` /
  `000001.SZ`；北交所和未知后缀在建立完整规则前拒绝，不猜测交易所。
- QMT 委托状态先验证原始状态码、委托量和累计成交量的一致性；矛盾或未知事实映射为
  `unknown`，触发后续停机处置，不能映射为“未成交”。
- 委托和成交查询的 `None` 不能与空列表区分，因此按券商状态未知处理；只有明确的空
  集合才代表没有记录。
- 成交量始终使用累计值，持久化序号必须由单消费者协调器分配，不能依赖回调到达顺序。

## 4. 晋级条件

下一阶段只能增加只读连接和双源对账，仍不得实现下单。至少需要：

1. Windows 节点上的固定 XtQuant 版本和制品哈希；
2. MiniQMT 登录与账户订阅的真实只读证据；
3. 资产、持仓、当日委托、当日成交的查询与回调汇合；
4. `None`、断线、回调乱序、重复回调、进程重启和 MiniQMT 重启演练；
5. QMT 与内部账本差异触发持久化停机开关；
6. 连续模拟盘证据、策略样本外门槛和合规审批。

任何单项通过都不是盈利证据，也不授权资金实盘。

## 5. 官方依据

- [XtQuant 快速开始](https://dict.thinktrader.net/nativeApi/start_now.html)
- [XtQuant 交易接口](https://dict.thinktrader.net/nativeApi/xttrader.html)
- [XtQuant 行情接口](https://dict.thinktrader.net/nativeApi/xtdata.html)
- [XtQuant 常见问题](https://dict.thinktrader.net/nativeApi/question_function.html)
