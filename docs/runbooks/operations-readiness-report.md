# 运维就绪证据报告

本报告把 QMT 主机预检、驻留模拟盘冷启动证据、独立看门狗、模拟盘晋级审计和可选的
研究数据补偿计划合并成一份带哈希的 JSON 快照。它不导入 XtQuant、不连接 MiniQMT、
不请求 Tushare，也不会写数据库、启动采集、复位停机开关或触发任何券商操作。

## 执行

在目标 Windows/MiniQMT 主机的项目目录中加载受限环境变量，然后运行：

```powershell
$env:AQ_ENVIRONMENT = "paper"
uv run autoquant operations-readiness-report `
  --campaign-hash <research-data-campaign-hash>
```

没有正在处理的数据补偿计划时可以省略 `--campaign-hash`。命令只在所有已纳入检查的门槛
都通过时返回 0；`status=blocked` 或任一依赖不可用时返回 2。退出码 2 是安全阻断结果，
不能用 shell 选项忽略后继续启动服务。

同一份只读快照也可在经过 HTTP Basic 认证的操作台“交易中心”生成，或通过 API 获取：

```text
GET /api/v1/operations/readiness
GET /api/v1/operations/readiness?campaign_hash=<64位小写SHA-256>
```

这是只读 GET 接口，没有补偿授权、采集或交易操作。操作台服务必须使用
`AQ_ENVIRONMENT=paper` 启动；环境不符时接口返回依赖不可用并保持锁定。

## 输出判读

- `report_hash` 绑定本次所有 `sections` 和报告版本，可随验收记录归档；
- `blockers` 是稳定、可机读的待办代码，先处理 `*.unavailable`，再处理具体证据门槛；
- `qmt.*` 只能在实际 Windows/MiniQMT 主机消除；
- `paper_runtime.*` 表示行情连接前的数据库恢复证据不完整；
- `paper_watchdog.*` 表示常驻 scheduler 未运行、租约无效或心跳过期；
- `promotion.*` 是模拟盘样本、成交、对账、恢复演练或合规证据不足；
- `research_data.retry_authorization_required` 只表示存在一份等待人工确认的精确补偿计划，
  报告不会自动授权或执行它。

每份报告都必须同时包含：

```json
{
  "broker_mutation_allowed": false,
  "collection_started": false,
  "live_trading_locked": true,
  "storage_mutation_allowed": false,
  "vendor_request_started": false
}
```

若缺少任一字段、字段值不同、JSON 解析失败或报告哈希无法与原始归档对应，按未知状态
处理并停止后续操作。不得把 `status=ready` 解释为实盘许可；它最多表示可以进入受控的
模拟盘启动/验收步骤，真实交易仍由代码级发布锁禁止。

## Windows 交接顺序

1. 先运行本报告，保存原始 JSON、提交 SHA 和目标主机时间；
2. 按 `qmt.*` 阻断项完成路径、账号、64 位 Python、会话号和 PostgreSQL 时钟配置；
3. 运行 `qmt-readonly-accept --confirm-read-only`，全程不得给交易权限；
4. 再次运行本报告，确认新的 `report_hash` 和 QMT 只读证据；
5. 按驻留模拟盘手册启动 scheduler，并由独立看门狗持续检查；
6. 只有持续模拟盘证据、对账和恢复演练达标后，才处理晋级与独立合规流程。

相关细节见 [QMT 只读接入准备](qmt-read-only-preparation.md)、
[驻留模拟盘运行手册](resident-paper-runtime.md) 和
[模拟盘晋级审计](paper-promotion-audit.md)。
