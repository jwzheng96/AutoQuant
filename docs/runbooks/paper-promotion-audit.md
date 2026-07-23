# 模拟盘晋级审计手册

本手册用于检查模拟盘证据是否接近“小资金金丝雀实盘”的候选标准。当前版本不具备真实
下单或撤单能力；审计通过也不会自动解锁实盘。

## 固定安全边界

- 仅允许 `AQ_ENVIRONMENT=paper`；
- `AQ_LIVE_TRADING_ENABLED` 必须为 `false`；
- 审计只读 PostgreSQL，不复位停机开关、不连接 QMT、不写审批；
- 所有事实在同一个 `REPEATABLE READ READ ONLY` 事务快照中读取；
- 事实、政策和报告分别生成 SHA-256，便于复查同一时点的结论；
- 命令和 Web API 不返回券商账号、QMT 路径、租约原文、余额或持仓明细；
- 当前报告的 `live_trading_ready` 永远为 `false`。

## 执行

在可信运行主机加载未跟踪的 `.env` 后运行：

```bash
uv run autoquant promotion-check
```

未达标时命令返回退出码 2 和 `status=blocked`，这是预期的失败关闭行为。不要通过脚本把
退出码改成成功，也不要直接修改数据库证据。控制台的受认证 `/trading` 页面和
`GET /api/v1/promotion` 展示同一审计结果。

输出中的三个哈希分别代表：

- `policy_hash`：当前阈值和政策版本；
- `fact_hash`：单个数据库快照中的规范化事实；
- `report_hash`：政策、事实、逐项判断和评估时点组成的最终报告。

保存审计记录时应保存完整 JSON 和三个哈希，但不得附加 `.env`、数据库 DSN 或 QMT
配置。

## 当前门禁

| 门禁 | 当前要求 |
| --- | --- |
| `live_release_lock` | 真实交易发布锁保持锁定 |
| `kill_switch_active` | 持久化紧急停机处于 active |
| `strategy_approved` | 配置策略存在有效的模拟盘批准记录 |
| `qmt_acceptance_fresh` | 脱敏 QMT 只读验收证据不超过 24 小时，且不能来自未来 |
| `paper_session_count` | 最近 180 日证据中至少 60 个收盘会话 |
| `scheduler_coverage` | 最近 60 个会话每天至少 216 个连续竞价健康分钟 |
| `scheduler_failure_free` | 上述窗口没有 scheduler 失败或错误码 |
| `reconciliation_coverage` | 每个会话 14:55 后有通过的对账，窗口内没有失败报告 |
| `filled_order_count` | 证据窗口内至少 30 个最终成交模拟订单 |
| `unknown_order_free` | 没有 `unknown` 模拟订单 |
| `paper_total_return` | 逐日收益复合后大于 0 |
| `paper_max_drawdown` | 逐日复合净值最大回撤不超过 10% |
| `profitable_session_rate` | 盈利会话比例至少 50% |
| `kill_switch_drills` | 至少 3 个不同日期的 `reason=drill` 激活记录 |
| `windows_recovery_drills` | 断线和 MiniQMT 重启恢复证据已持久化 |
| `compliance_approval` | 存在明确、可撤销、可审计的合规批准工件 |

“收盘会话”要求 session-risk 的最后观测时间不早于上海时间 14:55。健康分钟只统计
`morning_continuous` 或 `afternoon_continuous` 阶段、具有行情证据、无错误且状态为
`no_intents` 或 `completed` 的不同分钟。周末或休市日不会因为日期连续而自动计数。

这些阈值是首版运营政策，不是盈利承诺或统计显著性证明。每次调整阈值都会改变
`policy_hash`，不得为了让现有样本过关而临时降低门槛。

## 阻断处理

按以下顺序处理，避免用后续证据掩盖基础故障：

1. 先修复停机开关、策略批准和数据库证据链；
2. 在 Windows 节点完成新鲜 QMT 只读验收；
3. 连续运行驻留模拟盘，积累完整会话、调度、对账和成交证据；
4. 对 `unknown` 订单或任一对账失败做根因分析，不能删记录；
5. 完成至少三次停机演练；
6. 实现并执行 Windows 断线、MiniQMT 重启恢复演练工件；
7. 最后接入独立合规批准和撤销流程。

当前 `windows_recovery_drills` 与 `compliance_approval` 会显示 `not_persisted`，因为项目尚未
实现这两类不可变工件。它们是明确的后续开发项，不允许用人工口头确认、截图或数据库
补值绕过。

## 晋级原则

即使未来所有证据门禁通过，也只能进入单独设计和审批的金丝雀阶段：独立账户或子账户、
极小资金上限、单标的/白名单、每次启动短期人工授权租约、盘中自动停机和盘后人工复核。
在该执行链、授权模型和券商侧 Windows 演练全部实现并复核前，保持真实交易代码级硬锁。
