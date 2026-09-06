/* Swarm dashboard v2 — vanilla JS + SSE. Lazy file lists, live speed, ETA. */

const $ = (id) => document.getElementById(id);

const state = {
  lastBytes: null, lastTs: null, speedEMA: 0,
  spark: [], jobFilter: {}, evFilter: "all",
  renderToken: 0,
};

function fmtBytes(n) {
  if (n == null) return "—";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0, v = n;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return v.toFixed(i === 0 ? 0 : 1) + " " + u[i];
}
const fmtSpeed = (kbps) => kbps >= 1024 ? (kbps / 1024).toFixed(1) + " MB/s" : (kbps || 0).toFixed(0) + " KB/s";

function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function fmtETA(seconds) {
  if (!isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 90) return Math.round(seconds) + "s";
  if (seconds < 5400) return Math.round(seconds / 60) + " min";
  return (seconds / 3600).toFixed(1) + " h";
}

async function api(path, opts) {
  const r = await fetch(path, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || `HTTP ${r.status}`);
  return body;
}

function toast(msg, kind = "info") {
  const t = document.createElement("div");
  t.className = `toast ${kind}`;
  t.textContent = msg;
  $("toasts").appendChild(t);
  setTimeout(() => t.classList.add("show"), 10);
  setTimeout(() => { t.classList.remove("show"); setTimeout(() => t.remove(), 400); }, 4000);
}

// ── global speed meter ─────────────────────────────────────────────────
function updateSpeedMeter(summaryBytes) {
  const now = performance.now();
  if (state.lastBytes != null) {
    const dt = (now - state.lastTs) / 1000;
    if (dt > 0.3) {
      const bps = Math.max(0, (summaryBytes - state.lastBytes) / dt); // bytes/s
      const kbps = bps / 1024;
      state.speedEMA = state.speedEMA ? state.speedEMA * 0.6 + kbps * 0.4 : kbps;
      state.lastBytes = summaryBytes; state.lastTs = now;
    }
  } else {
    state.lastBytes = summaryBytes; state.lastTs = now;
  }
  const v = state.speedEMA;
  $("speed-val").textContent = fmtSpeed(v);
  state.spark.push(v);
  if (state.spark.length > 44) state.spark.shift();
  drawSpark();
}

function drawSpark() {
  const cv = $("sparkline"), ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, cv.width, cv.height);
  const data = state.spark, w = cv.width, h = cv.height;
  if (data.length < 2) return;
  const max = Math.max(...data, 64);
  ctx.beginPath();
  data.forEach((v, i) => {
    const x = (i / (data.length - 1)) * (w - 2) + 1;
    const y = h - 2 - (v / max) * (h - 6);
    i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  });
  ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue("--accent-bright");
  ctx.lineWidth = 1.5; ctx.stroke();
  ctx.lineTo(w - 1, h); ctx.lineTo(1, h); ctx.closePath();
  ctx.fillStyle = "rgba(90,157,224,.12)"; ctx.fill();
}

// ── jobs (summary endpoint — never renders 5k file rows) ───────────────
async function loadJobs(force = false) {
  try {
    const { jobs } = await api("/api/jobs/summary");
    const totalBytes = jobs.reduce((a, j) => a + (j.bytes_done || 0), 0);
    updateSpeedMeter(totalBytes);

    const el = $("jobs");
    el.innerHTML = jobs.map(j => {
      const pct = j.bytes_total ? (j.bytes_done / j.bytes_total) * 100 : 0;
      const remain = state.speedEMA > 8 ? ((j.bytes_total - j.bytes_done) / 1024) / state.speedEMA : NaN;
      const runHours = remain > 3600 ? " · ETA " + fmtETA(remain) : (isFinite(remain) ? " · ETA " + fmtETA(remain) : "");
      const active = (j.active || []).map(f => {
        const p = f.size ? Math.min(100, (f.bytes_done / f.size) * 100) : 0;
        return `<div class="file-row">
          <span class="file-name" title="${f.relpath || f.name}">${f.name}</span>
          <div class="bar"><div class="bar-fill" style="width:${p}%"></div></div>
          <span class="pct">${p.toFixed(0)}%</span>
          <span class="badge downloading">${f.status}</span>
        </div>`;
      }).join("");
      const fil = state.jobFilter[j.id] || "all";
      const filesOpen = state.filesOpen && state.filesOpen[j.id];
      return `<div class="job">
        <div class="job-head">
          <span class="job-link" title="${j.link}">${j.link}</span>
          <span class="job-actions">
            <span class="badge ${j.status}">${j.status}</span>
            ${j.status === "running" ? `<button class="ghost" onclick="pauseJob(${j.id})" title="pause">⏸</button>` : ""}
            ${["queued", "paused", "failed"].includes(j.status) ? `<button class="ghost" onclick="resumeJob(${j.id})" title="resume">▶</button>` : ""}
            ${j.status !== "done" && j.status !== "cancelled" ? `<button class="ghost" onclick="cancelJob(${j.id})" title="cancel">✕</button>` : ""}
          </span>
        </div>
        <div class="job-meta">
          <span><b class="green">${j.files_done}</b> done</span>
          <span><b class="blue">${j.files_downloading}</b> active</span>
          <span><b class="orange">${j.files_failed}</b> failed</span>
          <span>${fmtBytes(j.bytes_done)} / ${fmtBytes(j.bytes_total)}</span>
          <span class="job-eta">${j.status === "running" ? runHours : ""}</span>
        </div>
        <div class="bar big"><div class="bar-fill ${j.status === "done" ? "done" : ""}" style="width:${pct}%"></div></div>
        ${active}
        <div class="files-toggle" data-job="${j.id}">
          <button class="ghost" onclick="toggleFiles(${j.id})">${filesOpen ? "▾ hide files" : "▸ files"}</button>
          ${filesOpen ? `
            <span class="mini-filter ${fil !== "all" ? "on" : ""}">
              <button class="ghost kbtn ${fil === "all" ? "active" : ""}" onclick="setJobFilter(${j.id},'all')">all</button>
              <button class="ghost kbtn ${fil === "downloading" ? "active" : ""}" onclick="setJobFilter(${j.id},'downloading')">active</button>
              <button class="ghost kbtn ${fil === "failed" ? "active" : ""}" onclick="setJobFilter(${j.id},'failed')">failed</button>
              <button class="ghost kbtn ${fil === "done" ? "active" : ""}" onclick="setJobFilter(${j.id},'done')">done</button>
              <input id="fileq-${j.id}" placeholder="search…" value="${(state.fileQuery || {})[j.id] || ""}"
                     oninput="setFileQuery(${j.id}, this.value)">
            </span>` : ""}
        </div>
        <div id="files-${j.id}"></div>
      </div>`;
    }).join("");

    // hydrate open file lists (paginated, filtered) — only for expanded jobs
    const token = ++state.renderToken;
    for (const j of jobs) {
      if (state.filesOpen && state.filesOpen[j.id]) {
        loadFileRows(j.id, token);   // async, not awaited
      }
    }
  } catch (e) { console.error(e); }
}

async function loadFileRows(jobId, token) {
  try {
    const fil = state.jobFilter[jobId] || "all";
    const q = ((state.fileQuery || {})[jobId] || "").trim();
    let url = `/api/jobs/${jobId}/files?limit=100&offset=0&sort=bytes_done&dir=desc`;
    if (fil !== "all") url += `&status=${fil}`;
    if (q) url += `&q=${encodeURIComponent(q)}`;
    const data = await api(url);
    if (token !== state.renderToken) return;   // a newer render took over
    const host = $(`files-${jobId}`);
    if (!host) return;
    const rows = (data.files || []).map(f => {
      const p = f.size ? Math.min(100, (f.bytes_done / f.size) * 100) : 0;
      return `<tr>
        <td class="fname" title="${f.relpath || f.name}">${f.name}</td>
        <td><div class="bar"><div class="bar-fill ${f.status === "done" ? "done" : ""}" style="width:${p}%"></div></div></td>
        <td class="pct">${p.toFixed(0)}%</td>
        <td>${fmtBytes(f.size)}</td>
        <td><span class="chip ${f.status}">${f.status}</span></td>
      </tr>`;
    }).join("");
    host.innerHTML = `<table class="files-table"><tr><th>File</th><th style="width:26%">Progress</th><th style="width:6%"></th><th style="width:10%">Size</th><th style="width:10%">State</th></tr>${rows}</table>
      ${data.total > 100 ? `<div class="msg">… and ${data.total - 100} more — use the filters</div>` : ""}`;
  } catch (e) { console.error(e); }
}

function toggleFiles(jobId) {
  state.filesOpen = state.filesOpen || {};
  state.filesOpen[jobId] = !state.filesOpen[jobId];
  loadJobs(true);
}
function setJobFilter(jobId, fil) {
  state.jobFilter[jobId] = fil;
  loadJobs(true);
}
function setFileQuery(jobId, q) {
  state.fileQuery = state.fileQuery || {};
  state.fileQuery[jobId] = q;
  clearTimeout(state._qTimer);
  state._qTimer = setTimeout(() => loadFileRows(jobId, state.renderToken), 250);
}

function pauseJob(id)  { api(`/api/jobs/${id}/pause`,  { method: "POST" }).then(() => { toast("Job paused"); loadJobs(true); }); }
function resumeJob(id) { api(`/api/jobs/${id}/resume`, { method: "POST" }).then(() => { toast("Job resumed"); loadJobs(true); }); }
async function cancelJob(id) {
  await api(`/api/jobs/${id}`, { method: "DELETE" });
  toast("Job cancelled", "warn");
  loadJobs(true);
}

$("job-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const msg = $("job-msg");
  msg.textContent = "Inspecting link…"; msg.className = "msg";
  try {
    const body = { link: $("link").value };
    if ($("dest").value) body.dest = $("dest").value;
    const r = await api("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    msg.textContent = ""; toast(`Job #${r.job_id} created`);
    $("link").value = ""; $("dest").value = "";
    loadJobs(true);
  } catch (e) { msg.textContent = e.message; msg.className = "msg error"; }
});

// ── proxies ────────────────────────────────────────────────────────────
let proxySort = { key: "score", dir: -1 };

async function loadProxies() {
  try {
    const { proxies, stats } = await api("/api/proxies?limit=200");
    const modeBadge = stats.mode === "nord"
      ? `<span style="color:var(--accent-bright)">● nord</span>` +
        (stats.nord_leases_cap ? ` <span style="color:var(--muted-fg)">(cap ${stats.nord_leases_cap})</span>` : "")
      : `<span style="color:var(--muted-fg)">● public</span>`;
    $("proxy-summary").innerHTML =
      `${modeBadge} · ` +
      `<span class="green">● ${stats.ready}</span> ready · ` +
      `<span style="color:var(--accent-bright)">● ${stats.leased}</span> busy · ` +
      `<span style="color:var(--orange)">● ${stats.cooldown}</span> cd · ` +
      `<span class="red">● ${stats.dead}</span> dead`;

    $("proxy-stats").innerHTML = ["ready", "leased", "cooldown", "dead"].map(k =>
      `<div class="stat-chip ${k}"><div class="num">${stats[k] ?? 0}</div><div class="lbl">${k}</div></div>`
    ).join("");

    const isNordUrl = u => u.includes("nordvpn.com:") || u.includes("nordhold.net:");
    const key = proxySort.key, dir = proxySort.dir;
    const rows = proxies
      .filter(p => ["ready", "leased", "cooldown"].includes(p.state))
      .filter(p => stats.mode === "nord" ? isNordUrl(p.url) : !isNordUrl(p.url))
      .sort((a, b) => {
        const av = key === "url" ? a.url : (a[key] ?? -1);
        const bv = key === "url" ? b.url : (b[key] ?? -1);
        return (av > bv ? 1 : av < bv ? -1 : 0) * dir;
      })
      .slice(0, 30)
      .map(p => `<tr>
        <td class="purl" title="${p.url}">${p.url.replace(/^\w+:\/\//, "").replace(/:89$/, "")}</td>
        <td><span class="chip ${p.state}">${p.state}</span></td>
        <td>${p.score != null ? p.score.toFixed(0) : "—"}</td>
        <td>${p.throughput_kbps != null ? fmtSpeed(p.throughput_kbps) : "—"}</td>
        <td>${p.latency_ms != null ? p.latency_ms.toFixed(0) + " ms" : "—"}</td>
        <td>${p.quota_count || 0}</td>
        <td>${p.country || "?"}</td>
      </tr>`).join("");
    const th = (label, k) =>
      `<th class="sortable" onclick="setProxySort('${k}')">${label}${proxySort.key === k ? (proxySort.dir < 0 ? " ▾" : " ▴") : ""}</th>`;
    $("proxy-table").innerHTML = rows
      ? `<table><tr>${th("Proxy", "url")}${th("State", "state")}${th("Score", "score")}${th("Speed", "throughput_kbps")}${th("Latency", "latency_ms")}${th("Quotas", "quota_count")}${th("CC", "country")}</tr>${rows}</table>`
      : "<div class='msg'>No proxies yet. Import a list or wait for the refresh loop.</div>";
  } catch (e) { console.error(e); }
}
function setProxySort(k) {
  proxySort = proxySort.key === k ? { key: k, dir: -proxySort.dir } : { key: k, dir: -1 };
  loadProxies();
}

function importList() { $("import-dialog").showModal(); }

async function doImport() {
  const text = $("import-text").value;
  try {
    const r = await api("/api/proxies/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
    $("import-dialog").close();
    $("import-text").value = "";
    toast(`Imported ${r.imported} proxies — benching`, "ok");
    loadProxies();
  } catch (e) { toast(e.message, "err"); }
}

async function benchNow() {
  await api("/api/proxies/bench", { method: "POST" });
  toast("Bench pass triggered", "ok");
  setTimeout(loadProxies, 3000);
}

// ── events (filtered client-side) ──────────────────────────────────────
let eventsCache = [];
const KIND_GROUP = {
  file: ["file"], job: ["job"], proxy: ["proxy", "proxy_dead"],
  throttle: ["throttle"], error: ["error", "quota"],
};
function renderEvents() {
  const f = state.evFilter;
  const list = f === "all" ? eventsCache
    : eventsCache.filter(e => (KIND_GROUP[f] || [f]).includes(e.kind));
  $("events").innerHTML = list.map(e =>
    `<div class="event-row">
      <span class="event-ts">${fmtTime(e.ts)}</span>
      <span class="event-kind ${e.kind}">${e.kind}</span>
      <span>${e.message}</span>
    </div>`).join("");
}
async function loadEvents() {
  try {
    const { events } = await api("/api/events?limit=80");
    eventsCache = events.slice(0, 80);
    renderEvents();
  } catch (e) { console.error(e); }
}
$("kind-filter").addEventListener("click", (e) => {
  const b = e.target.closest(".kbtn");
  if (!b) return;
  state.evFilter = b.dataset.k;
  document.querySelectorAll("#kind-filter .kbtn").forEach(x => x.classList.toggle("active", x === b));
  renderEvents();
});

// ── SSE live updates ───────────────────────────────────────────────────
let es;
function connectSSE() {
  if (es) es.close();
  es = new EventSource("/api/stream");
  es.addEventListener("state", () => {
    loadJobs();          // 1 Hz — summary endpoint, cheap
  });
  es.onerror = () => { es.close(); setTimeout(connectSSE, 5000); };
}

// initial + slower backstops
loadJobs(true); loadProxies(); loadEvents(); connectSSE();
setInterval(loadProxies, 5000);
setInterval(loadEvents, 10000);
