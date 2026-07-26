"use strict";

const page = document.body.dataset.page;
const csrf = document.querySelector('meta[name="autoquant-csrf"]').content;
const toast = document.getElementById("toast");

function showToast(message) {
  toast.textContent = message;
  toast.classList.add("visible");
  window.setTimeout(() => toast.classList.remove("visible"), 3200);
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, { cache: "no-store", ...options });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.detail || payload.error || `HTTP ${response.status}`);
  return payload;
}

function setText(id, value) {
  const element = document.getElementById(id);
  if (element) element.textContent = value ?? "—";
}

function statusPill(element, value) {
  element.textContent = value;
  const style = value === "ok" || value === "completed" || value === "pass"
    ? "ok"
    : value === "degraded" || value === "running" || value === "queued"
      ? "warning"
      : value === "研究模式"
        ? "neutral"
        : "failed";
  element.className = `pill ${style}`;
}

function dateDefaults(startId, endId) {
  const end = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - 30);
  setTextValue(startId, localDateValue(start));
  setTextValue(endId, localDateValue(end));
}

function localDateValue(value) {
  const local = new Date(value.getTime() - value.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 10);
}

function setTextValue(id, value) {
  const element = document.getElementById(id);
  if (element && !element.value) element.value = value;
}

async function loadOverview() {
  try {
    const data = await requestJson("/api/v1/overview");
    statusPill(document.getElementById("global-status"), data.status);
    setText("postgres-status", data.postgres);
    setText("clickhouse-status", data.clickhouse);
    setText("daily-rows", data.coverage?.daily_rows?.toLocaleString() ?? "0");
    setText("factor-rows", data.coverage?.factor_rows?.toLocaleString() ?? "0");
    setText("daily-range", data.coverage?.first_session ? `${data.coverage.first_session} — ${data.coverage.last_session}` : "暂无数据");
    setText("quality-reports", data.control?.quality_reports?.toLocaleString() ?? "0");
    setText("manifest-count", data.control?.manifests?.toLocaleString() ?? "0");
    setText("checkpoint-count", data.control?.checkpoints?.toLocaleString() ?? "0");
    setText("audit-count", data.control?.audit_events?.toLocaleString() ?? "0");
  } catch (error) {
    statusPill(document.getElementById("global-status"), "unavailable");
    showToast(`状态读取失败：${error.message}`);
  }
}

function renderChart(items) {
  const host = document.getElementById("price-chart");
  host.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "chart-empty";
    empty.textContent = "所选区间没有可见日线数据";
    host.append(empty);
    return;
  }
  const values = items.map(item => Number(item.close));
  const low = Math.min(...values);
  const high = Math.max(...values);
  const range = high - low || 1;
  const points = values.map((value, index) => {
    const x = items.length === 1 ? 50 : (index / (items.length - 1)) * 100;
    const y = 92 - ((value - low) / range) * 78;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(" ");
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 100 100");
  svg.setAttribute("preserveAspectRatio", "none");
  const defs = document.createElementNS(ns, "defs");
  const gradient = document.createElementNS(ns, "linearGradient");
  gradient.id = "priceGradient";
  gradient.setAttribute("x1", "0"); gradient.setAttribute("y1", "0"); gradient.setAttribute("x2", "0"); gradient.setAttribute("y2", "1");
  [["0%", ".25"], ["100%", "0"]].forEach(([offset, opacity]) => {
    const stop = document.createElementNS(ns, "stop"); stop.setAttribute("offset", offset); stop.setAttribute("stop-color", "#51e6a8"); stop.setAttribute("stop-opacity", opacity); gradient.append(stop);
  });
  defs.append(gradient); svg.append(defs);
  const area = document.createElementNS(ns, "polygon");
  area.setAttribute("points", `0,100 ${points} 100,100`); area.setAttribute("class", "area"); svg.append(area);
  const line = document.createElementNS(ns, "polyline"); line.setAttribute("points", points); line.setAttribute("class", "line"); svg.append(line);
  host.append(svg);
}

function renderEquityChart(items) {
  const host = document.getElementById("equity-chart");
  host.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "chart-empty";
    empty.textContent = "运行完成后显示日度权益";
    host.append(empty);
    return;
  }
  const values = items.map(item => Number(item.equity));
  const low = Math.min(...values);
  const high = Math.max(...values);
  const range = high - low || 1;
  const points = values.map((value, index) => {
    const x = items.length === 1 ? 50 : (index / (items.length - 1)) * 100;
    const y = 92 - ((value - low) / range) * 78;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(" ");
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 100 100");
  svg.setAttribute("preserveAspectRatio", "none");
  const line = document.createElementNS(ns, "polyline");
  line.setAttribute("points", points);
  line.setAttribute("class", "line");
  svg.append(line);
  host.append(svg);
}

function renderBars(items) {
  const table = document.getElementById("bars-table");
  table.replaceChildren();
  items.slice(-60).reverse().forEach(item => {
    const row = document.createElement("tr");
    [item.session_date, item.open, item.high, item.low, item.close, Number(item.volume).toLocaleString()].forEach(value => {
      const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
    });
    table.append(row);
  });
  setText("bars-count", `${items.length} 条`);
  renderChart(items);
}

async function queryBars(event) {
  event.preventDefault();
  const instrument = document.getElementById("bars-instrument").value.trim().toUpperCase();
  const start = document.getElementById("bars-start").value;
  const end = document.getElementById("bars-end").value;
  const params = new URLSearchParams({ instrument, start, end, as_of: new Date().toISOString() });
  try {
    const data = await requestJson(`/api/v1/bars?${params}`);
    setText("chart-title", instrument);
    renderBars(data.items);
  } catch (error) { showToast(`查询失败：${error.message}`); }
}

function jobResult(job) {
  if (job.result) return `${job.result.persisted_bars ?? 0} 日线 / ${job.result.persisted_factors ?? 0} 因子`;
  return job.error_code || "—";
}

async function loadJobs() {
  try {
    const data = await requestJson("/api/v1/jobs?limit=50");
    const table = document.getElementById("jobs-table");
    table.replaceChildren();
    data.items.forEach(job => {
      const row = document.createElement("tr");
      const values = [new Date(job.created_at).toLocaleString(), job.request.instruments.join(", "), `${job.request.start} — ${job.request.end}`];
      values.forEach(value => { const cell = document.createElement("td"); cell.textContent = value; row.append(cell); });
      const stateCell = document.createElement("td"); const badge = document.createElement("span"); statusPill(badge, job.state); stateCell.append(badge); row.append(stateCell);
      const resultCell = document.createElement("td"); resultCell.textContent = jobResult(job); row.append(resultCell);
      table.append(row);
    });
  } catch (error) { showToast(`任务读取失败：${error.message}`); }
}

async function createJob(event) {
  event.preventDefault();
  const instruments = document.getElementById("job-instruments").value.split(",").map(value => value.trim().toUpperCase()).filter(Boolean);
  const payload = { instruments, start: document.getElementById("job-start").value, end: document.getElementById("job-end").value, idempotency_key: `web-${crypto.randomUUID()}` };
  try {
    await requestJson("/api/v1/jobs/daily-ingestion", { method: "POST", headers: { "Content-Type": "application/json", "X-AutoQuant-CSRF": csrf }, body: JSON.stringify(payload) });
    showToast("采集任务已创建并写入审计链");
    await loadJobs();
  } catch (error) { showToast(`任务创建失败：${error.message}`); }
}

async function loadTrading() {
  try {
    const data = await requestJson("/api/v1/trading");
    setText("trading-reason", data.reason);
    setText("risk-engine-state", data.risk?.status === "locked" ? "盘前硬限制已就绪，实盘锁定" : "风控状态不可用");
    setText("risk-decision-count", data.risk?.decision_count ?? 0);
    setText("risk-remaining-gates", data.risk?.remaining_gates?.join(", ") ?? "—");
    setText("execution-store-state", data.execution?.recovery_verified ? "持久化与重放校验已就绪，网关未接入" : "执行恢复状态不可用");
    setText("execution-order-count", data.execution?.order_count ?? 0);
    setText("execution-event-count", data.execution?.event_count ?? 0);
    setText("execution-remaining-gates", data.execution?.remaining_gates?.join(", ") ?? "—");
    setText("kill-switch-state", data.execution?.kill_switch_active ? "ACTIVE" : "RESET");
    setText("kill-switch-reason", data.execution?.kill_switch_reason ?? "—");
    setText("simulated-broker-state", data.execution?.simulated_broker_recovery_verified ? "持久化模拟券商事实链已验证" : "模拟券商不可用");
    setText("scheduler-evidence-state", data.execution?.scheduler_recovery_verified ? "调度周期哈希链已验证" : "调度证据不可用");
    setText("scheduler-cycle-count", data.execution?.scheduler_cycle_count ?? 0);
    setText("paper-strategy-state", data.strategy?.active ? "样本外证据已批准（仅模拟盘）" : "未批准");
    setText("paper-strategy-version", data.strategy?.strategy_version ?? data.strategy?.strategy_id ?? "—");
    setText(
      "paper-strategy-evidence",
      data.strategy?.active
        ? data.strategy.deployment_kind === "portfolio"
          ? `${data.strategy.instruments.join(", ")} · ${data.strategy.components.length} 个独立样本外组件 · 总配置 ${(Number(data.strategy.total_allocation) * 100).toFixed(2)}% · 组合 OOS ${(Number(data.strategy.portfolio_oos.compounded_return) * 100).toFixed(2)}% · 回撤上界 ${(Number(data.strategy.portfolio_oos.maximum_drawdown) * 100).toFixed(2)}%`
          : `${data.strategy.instrument} · SMA ${data.strategy.fast_sessions}/${data.strategy.slow_sessions} · ${(Number(data.strategy.allocation) * 100).toFixed(2)}%`
        : "当前没有可进入调度器的策略工件",
    );
    setText("paper-strategy-gates", data.strategy?.remaining_gates?.join(", ") ?? "—");
    setText(
      "qmt-acceptance-state",
      data.qmt?.evidence_fresh
        ? "已记录新鲜脱敏证据（仅只读）"
        : data.qmt?.status === "stale"
          ? "证据已过期"
          : "尚未记录",
    );
    setText(
      "qmt-acceptance-time",
      data.qmt?.latest_observed_at
        ? new Date(data.qmt.latest_observed_at).toLocaleString()
        : "—",
    );
    setText("qmt-position-count", data.qmt?.position_count ?? "—");
    setText("qmt-order-count", data.qmt?.order_count ?? "—");
    setText("qmt-trade-count", data.qmt?.trade_count ?? "—");
    const blockedQmtChecks = Object.entries(data.qmt?.checks ?? {})
      .filter(([, state]) => state !== "pass")
      .map(([name]) => name);
    setText(
      "qmt-host-checks",
      data.qmt?.current_host_read_only_ready
        ? "只读预检通过"
        : blockedQmtChecks.join(", ") || "不可用",
    );
    setText("qmt-remaining-gates", data.qmt?.remaining_gates?.join(", ") ?? "—");
    const qmtOperations = data.qmt_operations ?? {};
    const qmtStateLabels = {
      reconciled: "完整性与全量对账均通过",
      pending: "等待新鲜全量对账",
      unknown: "券商状态未知，必须人工处置",
      idle: "尚无 QMT 回调会话",
      unavailable: "运维证据不可用",
    };
    setText(
      "qmt-ledger-state",
      qmtOperations.integrity_verified
        ? qmtStateLabels[qmtOperations.status] ?? qmtOperations.status
        : "完整性复验失败",
    );
    setText(
      "qmt-lease-scope",
      qmtOperations.gateway_holder_id
        ? `${qmtOperations.gateway_holder_id} / session ${qmtOperations.qmt_session_id} / generation ${qmtOperations.qmt_lease_generation} · ${qmtOperations.lease_active ? "ACTIVE" : "EXPIRED"}`
        : "—",
    );
    setText("qmt-callback-cursor", qmtOperations.callback_cursor ?? 0);
    setText(
      "qmt-processing-hash",
      qmtOperations.processing_hash
        ? `${qmtOperations.processing_hash.slice(0, 16)}… (${qmtOperations.processing_event_count ?? 0} events)`
        : "—",
    );
    setText(
      "qmt-broker-state",
      qmtOperations.broker_state_known
        ? "KNOWN"
        : `UNKNOWN${qmtOperations.fatal_reason ? ` · ${qmtOperations.fatal_reason}` : ""}`,
    );
    setText(
      "qmt-reconciliation-state",
      qmtOperations.reconciliation_state
        ? `${qmtOperations.reconciliation_state.toUpperCase()} · ${qmtOperations.reconciliation_current ? "CURRENT" : "STALE"}`
        : "尚无报告",
    );
    setText(
      "qmt-reconciliation-hash",
      qmtOperations.reconciliation_report_hash
        ? `${qmtOperations.reconciliation_report_hash.slice(0, 16)}…`
        : "—",
    );
    setText(
      "qmt-reconciliation-issues",
      qmtOperations.reconciliation_issues?.join(", ") || "—",
    );
    const qmtOrdersTable = document.getElementById("qmt-orders-table");
    qmtOrdersTable.replaceChildren();
    (qmtOperations.orders ?? []).forEach(order => {
      const row = document.createElement("tr");
      [
        order.client_order_id,
        order.instrument,
        order.side,
        `${order.quantity} @ ${order.limit_price}`,
        order.order_state,
        `${order.trade_volume} / ${order.reported_traded_volume ?? "—"}`,
        order.convergence,
        new Date(order.updated_at).toLocaleString(),
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      qmtOrdersTable.append(row);
    });
    const qmtTradesTable = document.getElementById("qmt-trades-table");
    qmtTradesTable.replaceChildren();
    (qmtOperations.trades ?? []).forEach(trade => {
      const row = document.createElement("tr");
      [
        trade.trade_id,
        trade.client_order_id,
        trade.instrument,
        trade.side,
        trade.volume,
        trade.price,
        trade.amount,
        new Date(trade.observed_at).toLocaleString(),
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      qmtTradesTable.append(row);
    });
    setText(
      "promotion-state",
      data.promotion?.evidence_gates_passed
        ? "证据门禁通过，真实交易仍锁定"
        : "未达到晋级标准，真实交易锁定",
    );
    setText(
      "promotion-report-hash",
      data.promotion?.report_hash
        ? `${data.promotion.report_hash.slice(0, 16)}…`
        : "尚无可审计报告",
    );
    setText(
      "promotion-blockers",
      data.promotion?.blockers?.join(", ") || "—",
    );
    const promotionTable = document.getElementById("promotion-gates-table");
    promotionTable.replaceChildren();
    Object.entries(data.promotion?.gates ?? {})
      .sort(([left], [right]) => left.localeCompare(right))
      .forEach(([name, gate]) => {
        const row = document.createElement("tr");
        [name, gate.actual, gate.required].forEach(value => {
          const cell = document.createElement("td");
          cell.textContent = value;
          row.append(cell);
        });
        const stateCell = document.createElement("td");
        const badge = document.createElement("span");
        statusPill(badge, gate.status);
        stateCell.append(badge);
        row.append(stateCell);
        promotionTable.append(row);
      });
  }
  catch (error) { showToast(`能力读取失败：${error.message}`); }
}

async function activateKillSwitch() {
  try {
    await requestJson("/api/v1/execution/kill-switch/activate", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-AutoQuant-CSRF": csrf },
      body: JSON.stringify({ command_id: `web-kill-switch-${crypto.randomUUID()}`, reason: "manual" }),
    });
    showToast("紧急停机已激活并写入不可变审计链");
    await loadTrading();
  } catch (error) { showToast(`紧急停机失败：${error.message}`); }
}

const researchManifests = new Map();

async function loadResearchUniverses() {
  try {
    const data = await requestJson("/api/v1/research/universes?limit=50");
    const table = document.getElementById("universes-table");
    table.replaceChildren();
    data.items.forEach(snapshot => {
      const row = document.createElement("tr");
      [
        snapshot.reference_date,
        snapshot.index_code,
        snapshot.index_constituent_date,
        snapshot.liquidity_date,
        snapshot.member_count,
        snapshot.snapshot_hash.slice(0, 16),
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      table.append(row);
    });
  } catch (error) {
    showToast(`股票池读取失败：${error.message}`);
  }
}

async function loadResearchManifests() {
  const select = document.getElementById("backtest-manifest");
  const validationSelect = document.getElementById("validation-manifest");
  const portfolioSelect = document.getElementById("portfolio-validation-manifest");
  try {
    const data = await requestJson("/api/v1/research/manifests?limit=100");
    select.replaceChildren();
    validationSelect.replaceChildren();
    portfolioSelect.replaceChildren();
    researchManifests.clear();
    data.items.forEach(item => {
      researchManifests.set(item.manifest_hash, item);
      const option = document.createElement("option");
      option.value = item.manifest_hash;
      option.textContent = `${item.instruments.join(", ")} · ${item.start_time.slice(0, 10)} — ${item.end_time.slice(0, 10)} · ${item.manifest_hash.slice(0, 10)}`;
      select.append(option);
      validationSelect.append(option.cloneNode(true));
      if (item.instruments.length >= 3) portfolioSelect.append(option.cloneNode(true));
    });
    if (!data.items.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "暂无生产完整的数据清单";
      select.append(option);
      validationSelect.append(option.cloneNode(true));
    }
    if (!portfolioSelect.options.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "暂无至少包含 3 个标的的数据清单";
      portfolioSelect.append(option);
    }
    syncManifestInstrument();
    syncValidationManifestInstrument();
  } catch (error) {
    select.replaceChildren();
    validationSelect.replaceChildren();
    portfolioSelect.replaceChildren();
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "数据清单读取失败";
    select.append(option);
    validationSelect.append(option.cloneNode(true));
    portfolioSelect.append(option.cloneNode(true));
    showToast(`数据清单读取失败：${error.message}`);
  }
}

function syncManifestInstrument() {
  const selected = researchManifests.get(document.getElementById("backtest-manifest").value);
  if (selected?.instruments?.length) document.getElementById("backtest-instrument").value = selected.instruments[0];
}

function syncValidationManifestInstrument() {
  const selected = researchManifests.get(document.getElementById("validation-manifest").value);
  if (selected?.instruments?.length) document.getElementById("validation-instrument").value = selected.instruments[0];
}

function formatPercent(value) {
  if (value === null || value === undefined) return "—";
  return `${(Number(value) * 100).toFixed(2)}%`;
}

async function loadBacktestDetail(runId) {
  try {
    const detail = await requestJson(`/api/v1/backtests/${runId}`);
    const host = document.getElementById("backtest-detail");
    host.hidden = false;
    const metrics = detail.run.metrics;
    setText("bt-ending-equity", metrics ? Number(metrics.ending_equity).toLocaleString(undefined, { maximumFractionDigits: 2 }) : "—");
    setText("bt-total-return", formatPercent(metrics?.total_return));
    setText("bt-max-drawdown", formatPercent(metrics?.max_drawdown));
    setText("bt-total-fees", metrics ? Number(metrics.total_fees).toLocaleString(undefined, { maximumFractionDigits: 2 }) : "—");
    setText("equity-title", `${detail.run.request.instrument} · ${detail.run.state}`);
    setText("snapshot-count", `${detail.snapshots.length} 日`);
    renderEquityChart(detail.snapshots);
    const table = document.getElementById("executions-table");
    table.replaceChildren();
    detail.executions.forEach(execution => {
      const row = document.createElement("tr");
      const fee = Number(execution.commission) + Number(execution.stamp_duty) + Number(execution.transfer_fee);
      [execution.session_date, execution.side, execution.state, execution.requested_quantity.toLocaleString(), execution.fill_price ?? "—", fee.toFixed(2), execution.rejection_code ?? "—"].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      table.append(row);
    });
  } catch (error) { showToast(`回测明细读取失败：${error.message}`); }
}

async function loadBacktests() {
  try {
    const data = await requestJson("/api/v1/backtests?limit=50");
    const table = document.getElementById("backtests-table");
    table.replaceChildren();
    data.items.forEach(run => {
      const row = document.createElement("tr");
      row.className = "selectable-row";
      row.tabIndex = 0;
      const manifest = researchManifests.get(run.request.manifest_hash);
      const interval = manifest ? `${manifest.start_time.slice(0, 10)} — ${manifest.end_time.slice(0, 10)}` : run.request.manifest_hash.slice(0, 10);
      [new Date(run.created_at).toLocaleString(), run.request.instrument, interval].forEach(value => {
        const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
      });
      const stateCell = document.createElement("td");
      const badge = document.createElement("span"); statusPill(badge, run.state); stateCell.append(badge); row.append(stateCell);
      [formatPercent(run.metrics?.total_return), formatPercent(run.metrics?.max_drawdown)].forEach(value => {
        const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
      });
      const open = () => loadBacktestDetail(run.run_id);
      row.addEventListener("click", open);
      row.addEventListener("keydown", event => { if (event.key === "Enter" || event.key === " ") open(); });
      table.append(row);
    });
    if (data.items.length) await loadBacktestDetail(data.items[0].run_id);
  } catch (error) { showToast(`回测运行读取失败：${error.message}`); }
}

async function createBacktest(event) {
  event.preventDefault();
  const payload = {
    manifest_hash: document.getElementById("backtest-manifest").value,
    instrument: document.getElementById("backtest-instrument").value.trim().toUpperCase(),
    initial_cash: document.getElementById("backtest-cash").value,
    allocation: document.getElementById("backtest-allocation").value,
    slippage_bps: document.getElementById("backtest-slippage").value,
    liquidate_at_end: document.getElementById("backtest-liquidate").checked,
    idempotency_key: `web-backtest-${crypto.randomUUID()}`,
  };
  try {
    const run = await requestJson("/api/v1/backtests", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-AutoQuant-CSRF": csrf },
      body: JSON.stringify(payload),
    });
    showToast("回测已进入持久化队列");
    await loadBacktests();
    window.setTimeout(loadBacktests, 1200);
    await loadBacktestDetail(run.run_id);
  } catch (error) { showToast(`回测创建失败：${error.message}`); }
}

async function loadValidationDetail(experimentId) {
  try {
    const detail = await requestJson(`/api/v1/validations/${experimentId}`);
    const host = document.getElementById("validation-detail");
    host.hidden = false;
    const summary = detail.experiment.summary;
    setText("val-oos-return", formatPercent(summary?.compounded_oos_return));
    setText("val-benchmark-return", formatPercent(summary?.benchmark_compounded_oos_return));
    setText("val-excess-return", formatPercent(summary?.excess_oos_return));
    setText("val-evidence-status", summary?.evidence_status ?? "—");
    setText("val-gate-failures", summary?.gate_failures?.length ? summary.gate_failures.join(", ") : "预筛门槛通过，仍非实盘放行");
    setText("val-worst-drawdown", formatPercent(summary?.worst_oos_drawdown));
    setText("val-profitable-rate", formatPercent(summary?.profitable_fold_rate));
    setText("val-optimism", formatPercent(summary?.selection_optimism));
    setText("val-oos-sessions", summary?.oos_sessions ?? "—");
    const table = document.getElementById("validation-folds-table");
    table.replaceChildren();
    detail.folds.forEach(fold => {
      const row = document.createElement("tr");
      [fold.sequence, `${fold.train_start} — ${fold.train_end}`, `${fold.test_start} — ${fold.test_end}`, `${fold.selected.fast_sessions}/${fold.selected.slow_sessions}`, formatPercent(fold.training.total_return), formatPercent(fold.test.total_return), formatPercent(fold.benchmark?.total_return), formatPercent(fold.test.max_drawdown)].forEach(value => {
        const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
      });
      table.append(row);
    });
  } catch (error) { showToast(`验证明细读取失败：${error.message}`); }
}

async function loadValidations() {
  try {
    const data = await requestJson("/api/v1/validations?limit=50");
    const table = document.getElementById("validations-table");
    table.replaceChildren();
    data.items.forEach(experiment => {
      const row = document.createElement("tr");
      row.className = "selectable-row";
      row.tabIndex = 0;
      [new Date(experiment.created_at).toLocaleString(), experiment.request.instrument, `${experiment.request.train_sessions}/${experiment.request.test_sessions}`].forEach(value => {
        const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
      });
      const stateCell = document.createElement("td");
      const badge = document.createElement("span"); statusPill(badge, experiment.state); stateCell.append(badge); row.append(stateCell);
      [formatPercent(experiment.summary?.compounded_oos_return), formatPercent(experiment.summary?.profitable_fold_rate)].forEach(value => {
        const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
      });
      const open = () => loadValidationDetail(experiment.experiment_id);
      row.addEventListener("click", open);
      row.addEventListener("keydown", event => { if (event.key === "Enter" || event.key === " ") open(); });
      table.append(row);
    });
    if (data.items.length) await loadValidationDetail(data.items[0].experiment_id);
  } catch (error) { showToast(`验证实验读取失败：${error.message}`); }
}

async function loadValidationCampaigns() {
  try {
    const data = await requestJson("/api/v1/validation-campaigns?limit=50");
    const table = document.getElementById("validation-campaigns-table");
    table.replaceChildren();
    data.items.forEach(campaign => {
      const row = document.createElement("tr");
      const completed = campaign.components.filter(component => component.state === "completed").length;
      const gateFailures = campaign.components.reduce(
        (count, component) => count + (component.gate_failures?.length ?? 0),
        0,
      );
      [
        new Date(campaign.created_at).toLocaleString(),
        campaign.campaign_key,
        campaign.instruments.length,
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      const stateCell = document.createElement("td");
      const badge = document.createElement("span");
      statusPill(badge, campaign.status);
      stateCell.append(badge);
      row.append(stateCell);
      [`${completed}/${campaign.components.length}`, gateFailures].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      table.append(row);
    });
  } catch (error) { showToast(`验证活动读取失败：${error.message}`); }
}

async function loadFundamentalValidationDetail(resultHash) {
  try {
    const detail = await requestJson(`/api/v1/fundamental-validations/${resultHash}`);
    const summary = detail.summary;
    document.getElementById("fundamental-validation-detail").hidden = false;
    setText("fundamental-oos-return", formatPercent(summary.compounded_oos_return));
    setText(
      "fundamental-benchmark-return",
      formatPercent(summary.benchmark_compounded_oos_return),
    );
    setText("fundamental-excess-return", formatPercent(summary.excess_oos_return));
    setText("fundamental-evidence-status", summary.evidence_status);
    setText(
      "fundamental-gate-failures",
      summary.gate_failures.length
        ? summary.gate_failures.join(", ")
        : "研究门槛通过仍不等于模拟盘或实盘批准",
    );
    setText("fundamental-worst-drawdown", formatPercent(summary.worst_oos_drawdown));
    setText("fundamental-profitable-rate", formatPercent(summary.profitable_fold_rate));
    setText("fundamental-train-test-gap", formatPercent(summary.train_test_gap));
    setText("fundamental-oos-sessions", summary.oos_sessions);
    setText(
      "fundamental-strategy-open",
      summary.strategy_unresolved_position_count,
    );
    setText(
      "fundamental-benchmark-open",
      summary.benchmark_unresolved_position_count,
    );
    setText("fundamental-rejections", summary.rejected_order_count);
    setText(
      "fundamental-integrity",
      detail.integrity_verified ? "哈希复验通过" : "未验证",
    );
    const table = document.getElementById("fundamental-validation-folds-table");
    table.replaceChildren();
    detail.folds.forEach(fold => {
      const row = document.createElement("tr");
      [
        fold.sequence,
        `${fold.train_start} — ${fold.train_end}`,
        `${fold.test_start} — ${fold.test_end}`,
        formatPercent(fold.training.total_return),
        formatPercent(fold.test.total_return),
        formatPercent(fold.benchmark.total_return),
        formatPercent(fold.test.max_drawdown),
        `${fold.training.unresolved_position_count + fold.test.unresolved_position_count}/${fold.benchmark.unresolved_position_count}`,
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      table.append(row);
    });
  } catch (error) {
    showToast(`基本面验证明细读取失败：${error.message}`);
  }
}

async function loadFundamentalValidations() {
  try {
    const data = await requestJson("/api/v1/fundamental-validations?limit=20");
    const table = document.getElementById("fundamental-validations-table");
    table.replaceChildren();
    data.items.forEach(summary => {
      const row = document.createElement("tr");
      row.className = "selectable-row";
      row.tabIndex = 0;
      [
        new Date(summary.completed_at).toLocaleString(),
        summary.strategy_id,
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      const stateCell = document.createElement("td");
      const badge = document.createElement("span");
      statusPill(badge, summary.evidence_status);
      stateCell.append(badge);
      row.append(stateCell);
      [
        formatPercent(summary.compounded_oos_return),
        formatPercent(summary.benchmark_compounded_oos_return),
        formatPercent(summary.excess_oos_return),
        summary.gate_failures.length ? summary.gate_failures.join(", ") : "—",
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      const open = () => loadFundamentalValidationDetail(summary.result_hash);
      row.addEventListener("click", open);
      row.addEventListener("keydown", event => {
        if (event.key === "Enter" || event.key === " ") open();
      });
      table.append(row);
    });
    if (data.items.length) {
      await loadFundamentalValidationDetail(data.items[0].result_hash);
    }
  } catch (error) {
    showToast(`基本面验证证据读取失败：${error.message}`);
  }
}

async function loadLowVolatilityValidationDetail(resultHash) {
  try {
    const detail = await requestJson(
      `/api/v1/low-volatility-validations/${resultHash}`,
    );
    const summary = detail.summary;
    document.getElementById("low-volatility-validation-detail").hidden = false;
    setText("low-volatility-oos-return", formatPercent(summary.compounded_oos_return));
    setText(
      "low-volatility-benchmark-return",
      formatPercent(summary.benchmark_compounded_oos_return),
    );
    setText("low-volatility-excess-return", formatPercent(summary.excess_oos_return));
    setText("low-volatility-evidence-status", summary.evidence_status);
    setText(
      "low-volatility-gate-failures",
      summary.gate_failures.length
        ? summary.gate_failures.join(", ")
        : "研究门槛通过仍不等于模拟盘或实盘批准",
    );
    setText(
      "low-volatility-worst-drawdown",
      formatPercent(summary.worst_oos_drawdown),
    );
    setText(
      "low-volatility-profitable-rate",
      formatPercent(summary.profitable_fold_rate),
    );
    setText("low-volatility-train-test-gap", formatPercent(summary.train_test_gap));
    setText("low-volatility-oos-sessions", summary.oos_sessions);
    setText(
      "low-volatility-strategy-diagnostics",
      `${summary.strategy_rejected_order_count}/${summary.strategy_unresolved_position_count}`,
    );
    setText(
      "low-volatility-benchmark-diagnostics",
      `${summary.benchmark_rejected_order_count}/${summary.benchmark_unresolved_position_count}`,
    );
    setText(
      "low-volatility-integrity",
      detail.integrity_verified ? "哈希复验通过" : "未验证",
    );
    const table = document.getElementById(
      "low-volatility-validation-folds-table",
    );
    table.replaceChildren();
    detail.folds.forEach(fold => {
      const row = document.createElement("tr");
      [
        fold.sequence,
        `${fold.train_start} — ${fold.train_end}`,
        `${fold.test_start} — ${fold.test_end}`,
        formatPercent(fold.training.total_return),
        formatPercent(fold.test.total_return),
        formatPercent(fold.benchmark.total_return),
        formatPercent(fold.test.max_drawdown),
        `${fold.training.unresolved_position_count + fold.test.unresolved_position_count}/${fold.benchmark.unresolved_position_count}`,
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      table.append(row);
    });
  } catch (error) {
    showToast(`低波动验证明细读取失败：${error.message}`);
  }
}

async function loadLowVolatilityValidations() {
  try {
    const data = await requestJson(
      "/api/v1/low-volatility-validations?limit=20",
    );
    const table = document.getElementById(
      "low-volatility-validations-table",
    );
    table.replaceChildren();
    data.items.forEach(summary => {
      const row = document.createElement("tr");
      row.className = "selectable-row";
      row.tabIndex = 0;
      [
        new Date(summary.completed_at).toLocaleString(),
        summary.strategy_id,
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      const stateCell = document.createElement("td");
      const badge = document.createElement("span");
      statusPill(badge, summary.evidence_status);
      stateCell.append(badge);
      row.append(stateCell);
      [
        formatPercent(summary.compounded_oos_return),
        formatPercent(summary.benchmark_compounded_oos_return),
        formatPercent(summary.excess_oos_return),
        summary.gate_failures.length ? summary.gate_failures.join(", ") : "—",
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      const open = () => loadLowVolatilityValidationDetail(
        summary.result_hash,
      );
      row.addEventListener("click", open);
      row.addEventListener("keydown", event => {
        if (event.key === "Enter" || event.key === " ") open();
      });
      table.append(row);
    });
    if (data.items.length) {
      await loadLowVolatilityValidationDetail(
        data.items[0].result_hash,
      );
    }
  } catch (error) {
    showToast(`低波动验证证据读取失败：${error.message}`);
  }
}

async function loadLowVolatilityForwardProgress() {
  try {
    const progress = await requestJson(
      "/api/v1/low-volatility-forward-progress",
    );
    const labels = {
      backfill_required: "存在待补交易日",
      calendar_conflict: "日历修订冲突",
      collecting_forward_sessions: "积累前瞻交易日",
      session_gate_complete_awaiting_evaluation: "交易日数量达标，等待评估",
      forward_evaluation_passed_awaiting_paper_approval:
        "前瞻评估通过，等待模拟盘显式批准",
      forward_evaluation_rejected: "前瞻评估未通过",
    };
    setText(
      "low-volatility-forward-status",
      labels[progress.status] ?? progress.status,
    );
    setText(
      "low-volatility-forward-completed",
      `${progress.completed_required_sessions}/${progress.minimum_forward_sessions}`,
    );
    setText(
      "low-volatility-forward-remaining",
      progress.remaining_required_sessions,
    );
    setText(
      "low-volatility-forward-observed",
      progress.observed_open_sessions,
    );
    setText(
      "low-volatility-forward-cutoff",
      `安全截止日 ${progress.safe_cutoff_date}`,
    );
    setText(
      "low-volatility-forward-missing",
      progress.calendar_conflict_dates.length
        ? `冲突 ${progress.calendar_conflict_dates.join(", ")}`
        : progress.missing_session_dates.length
          ? progress.missing_session_dates.join(", ")
          : "0",
    );
    setText(
      "low-volatility-forward-paper",
      progress.paper_trading_eligible
        ? `${progress.minimum_paper_sessions} 日（候选，仍未部署）`
        : `${progress.minimum_paper_sessions} 日（未解锁）`,
    );
    const compatibilityLabels = {
      not_configured: "服务未配置",
      not_preregistered: "尚未预注册",
      awaiting_terminal_evaluation: "等待 126 日终验",
      compatible: "兼容（仍未授权）",
      incompatible: "不兼容",
    };
    setText(
      "low-volatility-forward-compatibility",
      compatibilityLabels[progress.compatibility_status]
        ?? progress.compatibility_status,
    );
    setText(
      "low-volatility-forward-compatibility-detail",
      progress.compatibility_gate_failures.length
        ? `失败门禁：${progress.compatibility_gate_failures.join(", ")}`
        : progress.compatibility_spec_hash
          ? `规格 ${progress.compatibility_spec_hash.slice(0, 12)}…`
          : "没有兼容性规格",
    );
    const contractLabels = {
      not_configured: "契约服务未配置",
      not_frozen: "v50 契约未冻结",
      frozen_without_activation_authority: "v50 已冻结（不授权）",
    };
    setText(
      "low-volatility-forward-deployment",
      `${contractLabels[progress.deployment_contract_status]
        ?? progress.deployment_contract_status} / ${
        progress.ready_for_runtime ? "可启动" : "阻断"
      }`,
    );
    setText(
      "low-volatility-forward-blockers",
      progress.deployment_blockers.join(", "),
    );
    setText(
      "low-volatility-forward-signal",
      progress.daily_signal_hash
        ? `v51 ${progress.daily_signal_hash.slice(0, 12)}…（不授权）`
        : progress.decision_signal_status === "not_available"
          ? "尚无 v51 决策信号"
          : "v51 信号服务未配置",
    );
    setText(
      "low-volatility-forward-signal-detail",
      progress.candidate_approval_hash
        ? `候选 ${progress.candidate_approval_hash.slice(0, 12)}…`
        : "尚无已批准候选",
    );
    const table = document.getElementById(
      "low-volatility-forward-sessions-table",
    );
    table.replaceChildren();
    progress.sessions.forEach(session => {
      const row = document.createElement("tr");
      [
        session.session_date,
        session.snapshot_reference_date,
        session.instrument_count,
        new Date(session.completed_at).toLocaleString(),
        `${session.binding_hash.slice(0, 12)}…`,
        `${session.dataset_manifest_hash.slice(0, 12)}…`,
      ].forEach((value, index) => {
        const cell = document.createElement("td");
        cell.textContent = value;
        if (index === 4) cell.title = session.binding_hash;
        if (index === 5) {
          cell.title = session.dataset_manifest_hash;
        }
        row.append(cell);
      });
      table.append(row);
    });
  } catch (error) {
    showToast(`前瞻交易日进度读取失败：${error.message}`);
  }
}

async function loadPortfolioValidationDetail(experimentId) {
  try {
    const detail = await requestJson(`/api/v1/portfolio-validations/${experimentId}`);
    const host = document.getElementById("portfolio-validation-detail");
    host.hidden = false;
    const summary = detail.experiment.summary;
    setText("portfolio-oos-return", formatPercent(summary?.compounded_oos_return));
    setText("portfolio-benchmark-return", formatPercent(summary?.benchmark_compounded_oos_return));
    setText("portfolio-excess-return", formatPercent(summary?.excess_oos_return));
    setText("portfolio-evidence-status", summary?.evidence_status ?? "—");
    setText(
      "portfolio-gate-failures",
      summary?.gate_failures?.length
        ? summary.gate_failures.join(", ")
        : "门槛通过仍只代表研究候选，实盘保持锁定",
    );
    setText("portfolio-worst-drawdown", formatPercent(summary?.worst_oos_drawdown));
    setText("portfolio-profitable-rate", formatPercent(summary?.profitable_fold_rate));
    setText("portfolio-optimism", formatPercent(summary?.selection_optimism));
    setText("portfolio-rejections", summary?.rejected_order_count ?? "—");
    const diagnostics = detail.diagnostics;
    setText(
      "portfolio-positive-excess-rate",
      formatPercent(diagnostics?.positive_excess_fold_rate),
    );
    setText(
      "portfolio-mean-positive-excess",
      formatPercent(diagnostics?.mean_positive_fold_excess),
    );
    setText(
      "portfolio-mean-nonpositive-excess",
      formatPercent(diagnostics?.mean_nonpositive_fold_excess),
    );
    setText(
      "portfolio-first-half-excess",
      formatPercent(diagnostics?.first_half_excess_return),
    );
    setText(
      "portfolio-second-half-excess",
      formatPercent(diagnostics?.second_half_excess_return),
    );
    setText(
      "portfolio-selection-share",
      formatPercent(diagnostics?.maximum_selection_share),
    );
    setText(
      "portfolio-diagnostic-codes",
      diagnostics?.diagnostic_codes?.length
        ? diagnostics.diagnostic_codes.join(", ")
        : "未发现预定义诊断标签；仍禁止使用 OOS 调参",
    );
    const table = document.getElementById("portfolio-validation-folds-table");
    table.replaceChildren();
    detail.folds.forEach(fold => {
      const row = document.createElement("tr");
      [
        fold.sequence,
        `${fold.train_start} — ${fold.train_end}`,
        `${fold.test_start} — ${fold.test_end}`,
        `${fold.selected.lookback_sessions}/${fold.selected.rebalance_sessions}/${fold.selected.selection_count}`,
        formatPercent(fold.training.total_return),
        formatPercent(fold.test.total_return),
        formatPercent(fold.benchmark.total_return),
        formatPercent(fold.test.max_drawdown),
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      table.append(row);
    });
  } catch (error) {
    showToast(`组合验证明细读取失败：${error.message}`);
  }
}

async function loadPortfolioValidations() {
  try {
    const data = await requestJson("/api/v1/portfolio-validations?limit=50");
    const table = document.getElementById("portfolio-validations-table");
    table.replaceChildren();
    data.items.forEach(experiment => {
      const row = document.createElement("tr");
      row.className = "selectable-row";
      row.tabIndex = 0;
      const manifest = researchManifests.get(experiment.request.manifest_hash);
      [
        new Date(experiment.created_at).toLocaleString(),
        manifest?.instruments?.length ?? "—",
        `${experiment.request.train_sessions}/${experiment.request.test_sessions}`,
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      const stateCell = document.createElement("td");
      const badge = document.createElement("span");
      statusPill(badge, experiment.state);
      stateCell.append(badge);
      row.append(stateCell);
      [
        formatPercent(experiment.summary?.compounded_oos_return),
        formatPercent(experiment.summary?.excess_oos_return),
        experiment.summary?.evidence_status ?? experiment.error_code ?? "—",
      ].forEach(value => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      const open = () => loadPortfolioValidationDetail(experiment.experiment_id);
      row.addEventListener("click", open);
      row.addEventListener("keydown", event => {
        if (event.key === "Enter" || event.key === " ") open();
      });
      table.append(row);
    });
    if (data.items.length) {
      await loadPortfolioValidationDetail(data.items[0].experiment_id);
    }
  } catch (error) {
    showToast(`组合验证实验读取失败：${error.message}`);
  }
}

function parseMomentumCandidates(value) {
  return value.split(",").map(candidate => {
    const [lookback, rebalance, selection, extra] = candidate.trim().split("/");
    if (!lookback || !rebalance || !selection || extra) {
      throw new Error("动量候选格式应为 20/5/3,60/10/3");
    }
    return {
      lookback_sessions: Number(lookback),
      rebalance_sessions: Number(rebalance),
      selection_count: Number(selection),
    };
  });
}

async function createPortfolioValidation(event) {
  event.preventDefault();
  try {
    const payload = {
      manifest_hash: document.getElementById("portfolio-validation-manifest").value,
      initial_cash: document.getElementById("portfolio-validation-cash").value,
      gross_allocation: document.getElementById("portfolio-validation-gross").value,
      maximum_order_notional: document.getElementById("portfolio-validation-order-cap").value,
      slippage_bps: document.getElementById("portfolio-validation-slippage").value,
      train_sessions: Number(document.getElementById("portfolio-validation-train").value),
      test_sessions: Number(document.getElementById("portfolio-validation-test").value),
      embargo_sessions: Number(document.getElementById("portfolio-validation-embargo").value),
      candidates: parseMomentumCandidates(
        document.getElementById("portfolio-validation-candidates").value,
      ),
      idempotency_key: `web-portfolio-validation-${crypto.randomUUID()}`,
    };
    const experiment = await requestJson("/api/v1/portfolio-validations", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-AutoQuant-CSRF": csrf },
      body: JSON.stringify(payload),
    });
    showToast("组合级样本外验证已进入持久化队列");
    await loadPortfolioValidations();
    window.setTimeout(loadPortfolioValidations, 1800);
    await loadPortfolioValidationDetail(experiment.experiment_id);
  } catch (error) {
    showToast(`组合验证创建失败：${error.message}`);
  }
}

function parseCandidates(value) {
  return value.split(",").map(pair => {
    const [fast, slow, extra] = pair.trim().split("/");
    if (!fast || !slow || extra) throw new Error("候选参数格式应为 5/20,10/30");
    return { fast_sessions: Number(fast), slow_sessions: Number(slow) };
  });
}

async function createValidation(event) {
  event.preventDefault();
  try {
    const payload = {
      manifest_hash: document.getElementById("validation-manifest").value,
      instrument: document.getElementById("validation-instrument").value.trim().toUpperCase(),
      initial_cash: document.getElementById("backtest-cash").value,
      allocation: document.getElementById("backtest-allocation").value,
      slippage_bps: document.getElementById("backtest-slippage").value,
      train_sessions: Number(document.getElementById("validation-train").value),
      test_sessions: Number(document.getElementById("validation-test").value),
      embargo_sessions: Number(document.getElementById("validation-embargo").value),
      candidates: parseCandidates(document.getElementById("validation-candidates").value),
      idempotency_key: `web-validation-${crypto.randomUUID()}`,
    };
    const experiment = await requestJson("/api/v1/validations", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-AutoQuant-CSRF": csrf },
      body: JSON.stringify(payload),
    });
    showToast("样本外验证已进入持久化队列");
    await loadValidations();
    window.setTimeout(loadValidations, 1800);
    await loadValidationDetail(experiment.experiment_id);
  } catch (error) { showToast(`验证创建失败：${error.message}`); }
}

if (page === "/") {
  document.getElementById("page-title").textContent = "系统总览";
  document.getElementById("refresh-overview").addEventListener("click", loadOverview);
  loadOverview();
} else if (page === "/data") {
  statusPill(document.getElementById("global-status"), "研究模式");
  document.getElementById("page-title").textContent = "行情数据";
  dateDefaults("bars-start", "bars-end");
  document.getElementById("bars-form").addEventListener("submit", queryBars);
  renderChart([]);
} else if (page === "/ingestion") {
  statusPill(document.getElementById("global-status"), "研究模式");
  document.getElementById("page-title").textContent = "采集任务";
  dateDefaults("job-start", "job-end");
  document.getElementById("ingestion-form").addEventListener("submit", createJob);
  document.getElementById("refresh-jobs").addEventListener("click", loadJobs);
  loadJobs();
} else if (page === "/research") {
  statusPill(document.getElementById("global-status"), "研究模式");
  document.getElementById("page-title").textContent = "回测研究";
  document.getElementById("backtest-form").addEventListener("submit", createBacktest);
  document.getElementById("backtest-manifest").addEventListener("change", syncManifestInstrument);
  document.getElementById("validation-manifest").addEventListener("change", syncValidationManifestInstrument);
  document.getElementById("refresh-backtests").addEventListener("click", loadBacktests);
  document.getElementById("refresh-universes").addEventListener(
    "click",
    loadResearchUniverses,
  );
  document.getElementById("validation-form").addEventListener("submit", createValidation);
  document.getElementById("portfolio-validation-form").addEventListener(
    "submit",
    createPortfolioValidation,
  );
  document.getElementById("refresh-portfolio-validations").addEventListener(
    "click",
    loadPortfolioValidations,
  );
  document.getElementById("refresh-fundamental-validations").addEventListener(
    "click",
    loadFundamentalValidations,
  );
  document.getElementById(
    "refresh-low-volatility-validations",
  ).addEventListener("click", loadLowVolatilityValidations);
  document.getElementById(
    "refresh-low-volatility-forward",
  ).addEventListener("click", loadLowVolatilityForwardProgress);
  document.getElementById("refresh-validations").addEventListener(
    "click",
    () => Promise.all([loadValidations(), loadValidationCampaigns()]),
  );
  renderEquityChart([]);
  loadResearchManifests().then(() => Promise.all([
    loadBacktests(),
    loadValidations(),
    loadValidationCampaigns(),
    loadPortfolioValidations(),
    loadFundamentalValidations(),
    loadLowVolatilityValidations(),
    loadLowVolatilityForwardProgress(),
    loadResearchUniverses(),
  ]));
} else {
  statusPill(document.getElementById("global-status"), "研究模式");
  document.getElementById("page-title").textContent = "交易中心";
  document.getElementById("activate-kill-switch").addEventListener("click", activateKillSwitch);
  loadTrading();
}
