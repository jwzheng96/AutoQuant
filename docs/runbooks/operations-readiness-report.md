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

## 导出和离线验真

不要从终端复制截断的 JSON。使用原子导出命令生成仅当前用户可读的完整工件：

```powershell
uv run autoquant operations-readiness-export `
  --campaign-hash <可选的数据活动哈希> `
  --output .\evidence\readiness-20260803.json
```

输出目录必须事先存在，目标文件默认不允许覆盖；确需替换同一路径时显式增加
`--replace`。导出器会先严格复验顶层字段、安全边界、时区时间戳和 `report_hash`，再在
同目录写临时文件、刷新到磁盘并原子替换。即使报告为 `blocked`，工件也会成功归档，随后
命令返回 2，防止脚本把“已有报告”误判成“已经就绪”。

接收方无需数据库或 MiniQMT 即可离线验证：

```powershell
uv run autoquant operations-readiness-verify `
  --input .\evidence\readiness-20260803.json
```

验证会拒绝符号链接、非 UTF-8/超大/多余字段、被篡改的分区、失效安全字段和错误哈希。
有效但仍阻断的工件同样返回 2；只有有效且没有阻断项的工件返回 0。

上述 SHA-256 只能验证内容一致性，不能单独证明来源。首次部署时在受控 Windows 节点生成
Ed25519 密钥对；私钥必须放在项目目录之外，且命令不会覆盖已有密钥：

```powershell
uv run autoquant operations-readiness-keygen `
  --private-key "$env:USERPROFILE\.autoquant\keys\readiness.private.pem" `
  --public-key "$env:USERPROFILE\.autoquant\keys\readiness.public.pem" `
  --confirm-new-key
```

把命令输出的 `key_id` 和公钥通过独立可信渠道交给复核人并固定下来。私钥不得提交、复制
到数据库或放入共享目录；Windows 上还必须用 NTFS ACL 限制为运行账户只读。轮换时生成
新文件名和新 `key_id`，不能覆盖旧信任锚。

导出后生成脱离式签名，并用预先固定的公钥验证来源、完整字节和 24 小时新鲜度：

```powershell
uv run autoquant operations-readiness-sign `
  --input .\evidence\readiness-20260803.json `
  --private-key "$env:USERPROFILE\.autoquant\keys\readiness.private.pem" `
  --signature-output .\evidence\readiness-20260803.sig.json

uv run autoquant operations-readiness-verify-signed `
  --input .\evidence\readiness-20260803.json `
  --signature .\evidence\readiness-20260803.sig.json `
  --public-key <复核方固定的公钥路径> `
  --max-age-hours 24
```

签名绑定原始 JSON 的精确字节、内部 `report_hash`、签名时间和公钥指纹。换行变化、重算
内部哈希、替换公钥、错钥、未来时间或过期工件都会失败关闭。公钥必须预先固定；若把工件
和攻击者自带的公钥一起接受，签名不提供来源保证。

Windows QMT 节点应使用冻结环境包装器，避免命令执行时下载或更新依赖：

```powershell
.\scripts\windows\export-readiness-evidence.ps1 `
  -ProjectPath (Resolve-Path .) `
  -UvPath "$env:USERPROFILE\.local\bin\uv.exe" `
  -OutputPath (Join-Path (Resolve-Path .\evidence) 'readiness-20260803.json') `
  -SignatureOutputPath (Join-Path (Resolve-Path .\evidence) 'readiness-20260803.sig.json') `
  -SigningPrivateKeyPath "$env:USERPROFILE\.autoquant\keys\readiness.private.pem" `
  -SigningPublicKeyPath <复核方固定的公钥路径> `
  -CampaignHash <可选的数据活动哈希> `
  -WhatIf
```

先检查 `-WhatIf` 输出，再去掉该参数。包装器要求 `.env` 明确包含
`AQ_ENVIRONMENT=paper`、锁文件和 Windows 虚拟环境均存在，并只调用
冻结的导出、Ed25519 签名和签名验真命令。三步状态和安全字段必须一致，包装器才输出最终
摘要。它不运行 QMT 验收、补偿授权或任何券商接口。

仓库的 `Windows PowerShell safety` 工作流会在无秘密的 `windows-latest` runner 上解析
全部 PowerShell 文件，并用不可执行的占位 `uv.exe`、密钥和虚拟环境运行包装器
`-WhatIf`。测试必须证明没有创建报告/签名，也没有执行占位程序。该 CI 只验证脚本语法和
无副作用预演；它不能替代真实 Windows/MiniQMT 主机上的配置、签名和只读验收。

## Windows 交接顺序

1. 先导出并离线验证本报告，保存完整 JSON、提交 SHA 和目标主机时间；
2. 按 `qmt.*` 阻断项完成路径、账号、64 位 Python、会话号和 PostgreSQL 时钟配置；
3. 运行 `qmt-readonly-accept --confirm-read-only`，全程不得给交易权限；
4. 再次运行本报告，确认新的 `report_hash` 和 QMT 只读证据；
5. 按驻留模拟盘手册启动 scheduler，并由独立看门狗持续检查；
6. 只有持续模拟盘证据、对账和恢复演练达标后，才处理晋级与独立合规流程。

相关细节见 [QMT 只读接入准备](qmt-read-only-preparation.md)、
[驻留模拟盘运行手册](resident-paper-runtime.md) 和
[模拟盘晋级审计](paper-promotion-audit.md)。
