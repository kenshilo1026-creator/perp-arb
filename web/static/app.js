const $ = (id) => document.getElementById(id);
let state = { kind: "perp_perp", days: 7 };
let chart = null;

async function jget(url) { return (await fetch(url)).json(); }

async function init() {
  const [{ venues }, { symbols }] = await Promise.all([jget("/api/venues"), jget("/api/symbols")]);
  for (const v of venues) {
    $("short-venue").add(new Option(v, v));
    $("long-venue").add(new Option(v, v));
  }
  for (const s of symbols) {
    const opt = document.createElement("option");
    opt.value = s;
    $("symbol-list").appendChild(opt);
  }

  document.querySelectorAll("#kind-tabs button").forEach((b) =>
    (b.onclick = () => { setActive("#kind-tabs", b); state.kind = b.dataset.kind; onKindChange(); }));
  document.querySelectorAll("#days button").forEach((b) =>
    (b.onclick = () => { setActive("#days", b); state.days = +b.dataset.days; loadApy(); }));
  $("symbol").onchange = loadDefaultsThenApy;
  $("short-venue").onchange = loadApy;
  $("long-venue").onchange = loadApy;
  $("btn-preview").onclick = doPreview;
  $("btn-submit").onclick = doExecute;
  onKindChange();
}

function setActive(sel, btn) {
  document.querySelectorAll(sel + " button").forEach((b) => b.classList.remove("active"));
  btn.classList.add("active");
}

function onKindChange() {
  const isSpot = state.kind === "spot_perp";
  $("long-venue-wrap").style.display = isSpot ? "none" : "";
  $("spot-venue-note").style.display = isSpot ? "" : "none";
  if ($("symbol").value) loadDefaultsThenApy();
}

async function loadDefaultsThenApy() {
  const sym = $("symbol").value.trim().toUpperCase();
  if (!sym) return;
  const d = await jget(`/api/defaults?symbol=${sym}&kind=${state.kind}`);
  if (!d.error) {
    if (state.kind === "spot_perp") { $("short-venue").value = d.perp_venue; }
    else { $("short-venue").value = d.short_venue; $("long-venue").value = d.long_venue; }
  }
  loadApy();
}

async function loadApy() {
  const sym = $("symbol").value.trim().toUpperCase();
  if (!sym) return;
  const p = new URLSearchParams({ symbol: sym, kind: state.kind, days: state.days,
    short_venue: $("short-venue").value, long_venue: $("long-venue").value });
  const r = await jget("/api/apy?" + p.toString());
  const s = r.stats;
  $("apy-stats").textContent = s
    ? `年化 APY: ${(s.annualized_avg * 100).toFixed(2)}%   avg_hourly: ${(s.avg_hourly * 100).toFixed(4)}%   ` +
      `pos: ${(s.positive_ratio * 100).toFixed(0)}%   score: ${s.score.toFixed(2)}   signal: ${s.signal}`
    : "資料不足";
  drawChart(r.series || []);
}

function drawChart(series) {
  const xs = series.map((s) => s.ts_ms / 1000);
  const ys = series.map((s) => (s.spread_hourly ?? s.hourly) * 100);
  const opts = { width: $("apy-chart").clientWidth || 800, height: 220,
    series: [{}, { label: "spread %/h", stroke: "#2563eb", width: 1.5 }],
    scales: { x: { time: true } } };
  if (chart) chart.destroy();
  chart = new uPlot(opts, [xs, ys], $("apy-chart"));
}

function bodyFromForm(mode) {
  return { kind: state.kind, mode, exec_mode: $("exec-mode").value,
    symbol: $("symbol").value.trim().toUpperCase(), short_venue: $("short-venue").value,
    long_venue: state.kind === "spot_perp" ? "mexc" : $("long-venue").value,
    leverage: +$("leverage").value, total_size: $("total-size").value, clip_size: $("clip-size").value,
    interval_ms: +$("interval-ms").value, debounce_ms: +$("debounce-ms").value };
}

async function doPreview() {
  const body = bodyFromForm($("mode").value);
  const r = await fetch("/api/preview", { method: "POST",
    headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const data = await r.json();
  $("preview-box").textContent = JSON.stringify(data, null, 2);
  $("btn-submit").disabled = !r.ok || body.exec_mode === "taker";
  if (body.exec_mode === "taker") $("preview-box").textContent += "\n\n純吃單尚未實作，無法送出。";
}

function doExecute() {
  const body = bodyFromForm($("mode").value);
  const ws = new WebSocket(`ws://${location.host}/ws/execute`);
  $("progress-log").textContent = "";
  ws.onopen = () => ws.send(JSON.stringify({ live: true, params: body }));
  ws.onmessage = (e) => {
    const ev = JSON.parse(e.data);
    $("progress-log").textContent += formatEvent(ev) + "\n";
    if (ev.type === "result" || ev.type === "error") ws.close();
  };
  ws.onerror = () => { $("progress-log").textContent += "WebSocket 連線錯誤\n"; };
}

function formatEvent(ev) {
  switch (ev.type) {
    case "close_pair": return `平倉配對: 做空 ${ev.short_venue}(${ev.short_qty}) / 做多 ${ev.long_venue}(${ev.long_qty})`;
    case "batch_start": return `batch ${ev.index}/${ev.total}  clip=${ev.clip_size}`;
    case "debounce_wait": return ev.waited_ms > 0 ? `  防抖等待 ${Math.round(ev.waited_ms)}ms` : "";
    case "batch_filled": return `  成交 限價方 ${ev.maker_price}  市價方 ${ev.taker_price}  qty ${ev.qty}`;
    case "batch_failed": return `  ✗ batch ${ev.index} 失敗: ${ev.error}`;
    case "done": return `完成: ${ev.batches} 批, 共 ${ev.executed_qty}`;
    case "result": return `結果: ${JSON.stringify(ev)}`;
    case "error": return `錯誤: ${ev.error}`;
    default: return JSON.stringify(ev);
  }
}

init();
