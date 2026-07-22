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
  try { const data = await requestJson("/api/v1/trading"); setText("trading-reason", data.reason); }
  catch (error) { showToast(`能力读取失败：${error.message}`); }
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
} else {
  statusPill(document.getElementById("global-status"), "研究模式");
  document.getElementById("page-title").textContent = "交易中心";
  loadTrading();
}
