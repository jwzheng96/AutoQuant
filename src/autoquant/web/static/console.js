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
  const style = value === "ok" || value === "completed"
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

async function loadResearchManifests() {
  const select = document.getElementById("backtest-manifest");
  const validationSelect = document.getElementById("validation-manifest");
  try {
    const data = await requestJson("/api/v1/research/manifests?limit=100");
    select.replaceChildren();
    validationSelect.replaceChildren();
    researchManifests.clear();
    data.items.forEach(item => {
      researchManifests.set(item.manifest_hash, item);
      const option = document.createElement("option");
      option.value = item.manifest_hash;
      option.textContent = `${item.instruments.join(", ")} · ${item.start_time.slice(0, 10)} — ${item.end_time.slice(0, 10)} · ${item.manifest_hash.slice(0, 10)}`;
      select.append(option);
      validationSelect.append(option.cloneNode(true));
    });
    if (!data.items.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "暂无生产完整的数据清单";
      select.append(option);
      validationSelect.append(option.cloneNode(true));
    }
    syncManifestInstrument();
    syncValidationManifestInstrument();
  } catch (error) {
    select.replaceChildren();
    validationSelect.replaceChildren();
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "数据清单读取失败";
    select.append(option);
    validationSelect.append(option.cloneNode(true));
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
  document.getElementById("validation-form").addEventListener("submit", createValidation);
  document.getElementById("refresh-validations").addEventListener("click", loadValidations);
  renderEquityChart([]);
  loadResearchManifests().then(() => Promise.all([loadBacktests(), loadValidations()]));
} else {
  statusPill(document.getElementById("global-status"), "研究模式");
  document.getElementById("page-title").textContent = "交易中心";
  document.getElementById("activate-kill-switch").addEventListener("click", activateKillSwitch);
  loadTrading();
}
