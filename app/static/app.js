async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

const pager = {
  page: 1,
  hasNext: false,
  hasPrev: false,
};
const PAGE_SIZE = 80;

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function renderCoreMetrics(items) {
  const root = document.getElementById("coreMetrics");
  root.innerHTML = "";
  for (const item of items) {
    const div = document.createElement("div");
    div.className = "metric";
    div.innerHTML = `
      <div class="label">${item.friendly_name || item.entity_id}</div>
      <div class="number">${item.state ?? "-"} ${item.unit || ""}</div>
      <div class="muted">${item.entity_id}</div>
    `;
    root.appendChild(div);
  }
}

function renderEntities(items) {
  const body = document.getElementById("entitiesBody");
  body.innerHTML = "";
  for (const item of items) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${item.entity_id}</td>
      <td>${item.state ?? "-"}</td>
      <td>${item.unit || ""}</td>
      <td>${item.friendly_name || ""}</td>
    `;
    body.appendChild(tr);
  }
}

function buildEntityUrl() {
  const params = new URLSearchParams();
  const domain = document.getElementById("domainInput").value.trim();
  const brand = document.getElementById("brandInput").value.trim();
  const q = document.getElementById("qInput").value.trim();

  if (domain) params.set("domain", domain);
  if (brand) params.set("brand", brand);
  if (q) params.set("q", q);
  params.set("page", String(pager.page));
  params.set("page_size", String(PAGE_SIZE));

  return `/api/entities?${params.toString()}`;
}

async function loadSummary() {
  try {
    const [health, ha, core] = await Promise.all([
      fetchJson("/api/health"),
      fetchJson("/api/ha/ping"),
      fetchJson("/api/entities/core"),
    ]);
    setText("healthValue", health.status || "ok");
    setText("haValue", ha.ok ? "connected" : "error");
    setText("coreCount", String(core.count));
    setText("coreUpdatedAt", `Updated: ${new Date().toLocaleString()}`);
    renderCoreMetrics(core.items || []);
  } catch (err) {
    setText("healthValue", "error");
    setText("haValue", "error");
    setText("coreCount", "-");
    setText("coreUpdatedAt", String(err));
  }
}

async function loadEntities() {
  const url = buildEntityUrl();
  try {
    const payload = await fetchJson(url);
    pager.hasNext = Boolean(payload.has_next);
    pager.hasPrev = Boolean(payload.has_prev);
    const totalPages = Math.max(1, Math.ceil((payload.total || 0) / (payload.page_size || PAGE_SIZE)));
    setText("entityCount", `Total ${payload.total} entities`);
    setText("pageInfo", `Page ${payload.page}/${totalPages} (showing ${payload.count})`);
    document.getElementById("prevPageBtn").disabled = !pager.hasPrev;
    document.getElementById("nextPageBtn").disabled = !pager.hasNext;
    renderEntities(payload.items || []);
  } catch (err) {
    setText("entityCount", `Load failed: ${err}`);
    setText("pageInfo", "Page -");
    renderEntities([]);
  }
}

async function reloadAll() {
  await loadSummary();
  await loadEntities();
}

document.getElementById("refreshBtn").addEventListener("click", reloadAll);
document.getElementById("filterForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  pager.page = 1;
  await loadEntities();
});
document.getElementById("prevPageBtn").addEventListener("click", async () => {
  if (!pager.hasPrev || pager.page <= 1) return;
  pager.page -= 1;
  await loadEntities();
});
document.getElementById("nextPageBtn").addEventListener("click", async () => {
  if (!pager.hasNext) return;
  pager.page += 1;
  await loadEntities();
});

reloadAll();
