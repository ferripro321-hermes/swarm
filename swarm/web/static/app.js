/* Swarm dashboard logic — vanilla JS + SSE */

const $ = (id) => document.getElementById(id);

function fmtBytes(n) {
  if (n == null) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0, v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return v.toFixed(i === 0 ? 0 : 1) + " " + units[i];
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

async function api(path, opts) {
  const r = await fetch(path, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || `HTTP ${r.status}`);
  return body;
}

// ── jobs ───────────────────────────────────────────────────────────────
async function loadJobs() {
  try {
    const { jobs } = await api("/api/jobs");
    const el = $("jobs");
    if (!jobs.length) { el.innerHTML = "<div class='msg'>No jobs yet — paste a MEGA link above.</div>"; return; }
    el.innerHTML = jobs.map(j => {
      const files = (j.files || []).map(f => {
        const pct = f.size ? Math.min(100, (f.bytes_done / f.size) * 100) : 0;
        return `<div class="file-row">
          <span class="file-name" title="${f.relpath || f.name}">${f.name}</span>
          <div class="bar"><div class="bar-fill ${f.status === "done" ? "done" : ""}" style="width:${pct}%"></div></div>
          <span class="pct">${pct.toFixed(0)}%</span>
          <span class="badge ${f.status}">${f.status}</span>
        </div>`;
      }).join("");
      return `<div class="job">
        <div class="job-head">
          <span class="job-link">${j.link}</span>
          <span>
            <span class="badge ${j.status}">${j.status}</span>
            ${j.status !== "done" ? `<button class="ghost" onclick="cancelJob(${j.id})">✕</button>` : ""}
          </span>
        </div>
        ${files}
      </div>`;
    }).join("");
  } catch (e) { console.error(e); }
}

async function cancelJob(id) {
  await api(`/api/jobs/${id}`, { method: "DELETE" });
  loadJobs();
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
    msg.textContent = `Job #${r.job_id} started`;
    $("link").value = ""; $("dest").value = "";
    loadJobs();
  } catch (e) {
    msg.textContent = e.message; msg.className = "msg error";
  }
});

// ── proxies ────────────────────────────────────────────────────────────
async function loadProxies() {
  try {
    const { proxies, stats } = await api("/api/proxies?limit=200");
    const modeBadge = stats.mode === "nord"
      ? `<span style="color:var(--accent-bright)">● nord</span>` +
        (stats.nord_leases_cap ? ` <span style="color:var(--muted-fg)">(${stats.nord_leases_cap} max)</span>` : "")
      : `<span style="color:var(--muted-fg)">● public</span>`;
    $("proxy-summary").innerHTML =
      `${modeBadge} · ` +
      `<span style="color:var(--green)">● ${stats.ready}</span> ready · ` +
      `<span style="color:var(--accent-bright)">● ${stats.leased}</span> busy · ` +
      `<span style="color:var(--orange)">● ${stats.cooldown}</span> cooldown · ` +
      `<span style="color:var(--red)">● ${stats.dead}</span> dead`;

    $("proxy-stats").innerHTML = ["ready", "leased", "cooldown", "dead"].map(k =>
      `<div class="stat-chip ${k}"><div class="num">${stats[k] ?? 0}</div><div class="lbl">${k}</div></div>`
    ).join("");

    const isNordUrl = u => u.includes("nordvpn.com:") || u.includes("nordhold.net:");
    const rows = proxies
      .filter(p => ["ready", "leased", "cooldown"].includes(p.state))
      .filter(p => stats.mode === "nord" ? isNordUrl(p.url) : !isNordUrl(p.url))
      .sort((a, b) => (b.score ?? -1) - (a.score ?? -1))
      .slice(0, 25)
      .map(p => `<tr>
        <td>${p.url.replace(/^\w+:\/\//, "")}</td>
        <td><span class="chip ${p.state}">${p.state}</span></td>
        <td>${p.score != null ? p.score.toFixed(0) : "—"}</td>
        <td>${p.throughput_kbps != null ? p.throughput_kbps.toFixed(0) + " KB/s" : "—"}</td>
        <td>${p.latency_ms != null ? p.latency_ms.toFixed(0) + " ms" : "—"}</td>
        <td>${p.quota_count || 0}</td>
      </tr>`).join("");
    $("proxy-table").innerHTML = rows
      ? `<table><tr><th>Proxy</th><th>State</th><th>Score</th><th>Speed</th><th>Latency</th><th>Quotas</th></tr>${rows}</table>`
      : "<div class='msg'>No proxies yet. Import a list or wait for the refresh loop.</div>";
  } catch (e) { console.error(e); }
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
    alert(`Imported ${r.imported} proxies — benching now`);
    loadProxies();
  } catch (e) { alert(e.message); }
}

async function benchNow() {
  await api("/api/proxies/bench", { method: "POST" });
  setTimeout(loadProxies, 3000);
}

// ── events ─────────────────────────────────────────────────────────────
function renderEvents(events) {
  $("events").innerHTML = events.map(e =>
    `<div class="event-row">
      <span class="event-ts">${fmtTime(e.ts)}</span>
      <span class="event-kind ${e.kind}">${e.kind}</span>
      <span>${e.message}</span>
    </div>`).join("");
}

async function loadEvents() {
  try {
    const { events } = await api("/api/events?limit=30");
    renderEvents(events.slice(0, 30));
  } catch (e) { console.error(e); }
}

// ── SSE live updates ───────────────────────────────────────────────────
let es;
function connectSSE() {
  if (es) es.close();
  es = new EventSource("/api/stream");
  es.addEventListener("state", (ev) => {
    // lightweight 1Hz refresh of everything (small data)
    loadJobs(); loadProxies();
  });
  es.onerror = () => { es.close(); setTimeout(connectSSE, 5000); };
}

// initial + timers
loadJobs(); loadProxies(); loadEvents(); connectSSE();
setInterval(loadProxies, 5000);
setInterval(loadEvents, 10000);
