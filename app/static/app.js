async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

const I18N = {
  en: {
    subtitle: "SAJ monitoring panel",
    languageLabel: "Language",
    refreshBtn: "Refresh",
    dashboardTab: "Dashboard",
    entitiesTab: "Entities",
    systemTitle: "System",
    haTitle: "Home Assistant",
    coreTitle: "Core Entities",
    flowTitle: "Real-time Energy Flow",
    solarTitle: "Solar",
    gridTitle: "Grid",
    inverterTitle: "Inverter",
    loadTitle: "Home Load",
    batteryTitle: "Battery",
    socLabel: "SOC",
    entityExplorerTitle: "Entity Explorer",
    domainLabel: "Domain",
    brandLabel: "Brand",
    queryLabel: "Query",
    domainPlaceholder: "sensor",
    brandPlaceholder: "saj",
    queryPlaceholder: "battery",
    searchBtn: "Search",
    prevBtn: "Prev",
    nextBtn: "Next",
    tableEntity: "Entity",
    tableState: "State",
    tableUnit: "Unit",
    tableName: "Name",
    stateIdle: "Idle",
    stateProducing: "Producing",
    stateImporting: "Importing",
    stateExporting: "Exporting",
    stateConsuming: "Consuming",
    stateCharging: "Charging",
    stateDischarging: "Discharging",
    stateBatteryIdle: "Idle",
    balanceLabel: "Balance",
    updatedAt: "Updated",
    connected: "Connected",
    error: "Error",
    totalEntities: "Total {total} entities",
    pageInfo: "Page {page}/{totalPages} (showing {count})",
    pageDash: "Page -",
    loadFailed: "Load failed: {error}",
    inverterRunning: "Running",
    inverterOffline: "Offline",
    inverterStandby: "Standby",
    inverterUnknown: "Unknown",
  },
  zh: {
    subtitle: "SAJ 监控面板",
    languageLabel: "语言",
    refreshBtn: "刷新",
    dashboardTab: "总览",
    entitiesTab: "实体",
    systemTitle: "系统状态",
    haTitle: "Home Assistant",
    coreTitle: "核心实体",
    flowTitle: "实时能量流向",
    solarTitle: "太阳能",
    gridTitle: "电网",
    inverterTitle: "逆变器",
    loadTitle: "家庭负载",
    batteryTitle: "电池",
    socLabel: "电池电量",
    entityExplorerTitle: "实体浏览",
    domainLabel: "域",
    brandLabel: "品牌",
    queryLabel: "查询",
    domainPlaceholder: "sensor",
    brandPlaceholder: "saj",
    queryPlaceholder: "battery",
    searchBtn: "搜索",
    prevBtn: "上一页",
    nextBtn: "下一页",
    tableEntity: "实体",
    tableState: "状态",
    tableUnit: "单位",
    tableName: "名称",
    stateIdle: "空闲",
    stateProducing: "发电中",
    stateImporting: "购电中",
    stateExporting: "回馈电网",
    stateConsuming: "用电中",
    stateCharging: "正在充电",
    stateDischarging: "正在放电",
    stateBatteryIdle: "空闲",
    balanceLabel: "功率平衡",
    updatedAt: "更新时间",
    connected: "已连接",
    error: "错误",
    totalEntities: "共 {total} 个实体",
    pageInfo: "第 {page}/{totalPages} 页（当前 {count} 条）",
    pageDash: "第 - 页",
    loadFailed: "加载失败：{error}",
    inverterRunning: "运行中",
    inverterOffline: "离线",
    inverterStandby: "待机",
    inverterUnknown: "未知",
  },
};

const pager = {
  page: 1,
  hasNext: false,
  hasPrev: false,
};
const PAGE_SIZE = 80;

const stateCache = {
  lastSummary: null,
  lastEntities: null,
};

function getLang() {
  const saved = localStorage.getItem("lang");
  if (saved === "en" || saved === "zh") return saved;
  const browserLang = (navigator.language || "").toLowerCase();
  return browserLang.startsWith("zh") ? "zh" : "en";
}

let currentLang = getLang();
let currentTab = localStorage.getItem("activeTab") === "entities" ? "entities" : "dashboard";

function t(key, params = {}) {
  const table = I18N[currentLang] || I18N.en;
  let text = table[key] || I18N.en[key] || key;
  for (const [name, value] of Object.entries(params)) {
    text = text.replaceAll(`{${name}}`, String(value));
  }
  return text;
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function applyTranslations() {
  document.documentElement.lang = currentLang === "zh" ? "zh-CN" : "en";
  const langSelect = document.getElementById("langSelect");
  if (langSelect) langSelect.value = currentLang;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.getAttribute("data-i18n");
    if (!key) return;
    const text = t(key);
    if (el.tagName === "LABEL") {
      const input = el.querySelector("input");
      el.childNodes.forEach((node) => {
        if (node.nodeType === Node.TEXT_NODE && node.textContent.trim()) {
          node.textContent = `${text}\n            `;
        }
      });
      if (!input && el.textContent) el.textContent = text;
      return;
    }
    el.textContent = text;
  });

  document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
    const key = el.getAttribute("data-i18n-placeholder");
    if (!key) return;
    el.setAttribute("placeholder", t(key));
  });

  if (stateCache.lastSummary) renderSummary(stateCache.lastSummary);
  if (stateCache.lastEntities) renderEntitiesPage(stateCache.lastEntities);
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function formatPower(value, unit = "W") {
  if (value === null) return "-";
  const abs = Math.abs(value);
  if (abs >= 1000) return `${(value / 1000).toFixed(2)} k${unit}`;
  if (abs >= 100) return `${value.toFixed(0)} ${unit}`;
  return `${value.toFixed(1)} ${unit}`;
}

function setFlowLine(id, active, reverse = false) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle("active", Boolean(active));
  el.classList.toggle("reverse", Boolean(reverse));
}

function setModeClass(id, mode) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove("mode-positive", "mode-negative");
  if (mode === "positive") el.classList.add("mode-positive");
  if (mode === "negative") el.classList.add("mode-negative");
}

function findCoreItem(items, entityId) {
  return items.find((item) => item.entity_id === entityId) || null;
}

function findCoreByKeywords(items, keywords) {
  for (const item of items) {
    const haystack = `${item.entity_id || ""} ${item.friendly_name || ""}`.toLowerCase();
    if (keywords.every((kw) => haystack.includes(kw))) return item;
  }
  return null;
}

function renderEnergyFlow(items) {
  const pv =
    findCoreItem(items, "sensor.saj_pv_power") ||
    findCoreByKeywords(items, ["pv", "power"]) ||
    findCoreByKeywords(items, ["solar", "power"]);
  const grid =
    findCoreItem(items, "sensor.saj_total_grid_power") ||
    findCoreByKeywords(items, ["grid", "power"]);
  const battery =
    findCoreItem(items, "sensor.saj_battery_power") ||
    findCoreByKeywords(items, ["battery", "power"]);
  const load =
    findCoreItem(items, "sensor.saj_total_load_power") ||
    findCoreByKeywords(items, ["load", "power"]);
  const soc =
    findCoreItem(items, "sensor.saj_battery_energy_percent") ||
    findCoreByKeywords(items, ["battery", "percent"]) ||
    findCoreByKeywords(items, ["battery", "soc"]);
  const inverter =
    findCoreItem(items, "sensor.saj_inverter_status") ||
    findCoreByKeywords(items, ["inverter", "status"]);

  const pvPower = toNumber(pv?.state);
  const gridPower = toNumber(grid?.state);
  const batteryPower = toNumber(battery?.state);
  const loadPower = toNumber(load?.state);
  const batterySoc = toNumber(soc?.state);
  const inverterStateRaw = String(inverter?.state || "").toLowerCase();
  let inverterStateText = String(inverter?.state || "-");
  if (inverterStateRaw === "running") inverterStateText = t("inverterRunning");
  else if (inverterStateRaw === "offline") inverterStateText = t("inverterOffline");
  else if (inverterStateRaw === "standby") inverterStateText = t("inverterStandby");
  else if (!inverterStateRaw) inverterStateText = t("inverterUnknown");

  setText("solarPowerValue", formatPower(pvPower, pv?.unit || "W"));
  setText("gridPowerValue", formatPower(gridPower, grid?.unit || "W"));
  const batteryPowerAbs = batteryPower === null ? null : Math.abs(batteryPower);
  setText("batteryPowerValue", formatPower(batteryPowerAbs, battery?.unit || "W"));
  setText("loadPowerValue", formatPower(loadPower, load?.unit || "W"));
  setText("inverterStatusValue", inverterStateText);

  const solarActive = pvPower !== null && pvPower > 5;
  setText("solarState", solarActive ? t("stateProducing") : t("stateIdle"));
  setFlowLine("lineSolar", solarActive, false);

  const gridActive = gridPower !== null && Math.abs(gridPower) > 5;
  const gridImport = gridPower !== null && gridPower > 0;
  setText("gridState", gridActive ? (gridImport ? t("stateImporting") : t("stateExporting")) : t("stateIdle"));
  setFlowLine("lineGrid", gridActive, !gridImport);

  const loadActive = loadPower !== null && loadPower > 5;
  setText("loadState", loadActive ? t("stateConsuming") : t("stateIdle"));
  setFlowLine("lineLoad", loadActive, false);

  // SAJ sign convention assumption: positive battery power means discharging.
  const batteryActive = batteryPower !== null && Math.abs(batteryPower) > 5;
  const batteryDischarging = batteryPower !== null && batteryPower > 0;
  const batteryModeText = batteryActive
    ? batteryDischarging
      ? t("stateDischarging")
      : t("stateCharging")
    : t("stateBatteryIdle");
  setText("batteryState", batteryModeText);

  if (batteryPower !== null && batteryPower > 0) {
    setModeClass("batteryPowerValue", "positive");
    setModeClass("batteryState", "positive");
  } else if (batteryPower !== null && batteryPower < 0) {
    setModeClass("batteryPowerValue", "negative");
    setModeClass("batteryState", "negative");
  } else {
    setModeClass("batteryPowerValue", "");
    setModeClass("batteryState", "");
  }
  setFlowLine("lineBattery", batteryActive, batteryDischarging);

  if (batterySoc === null) {
    setText("batterySocValue", "-");
    const socFill = document.getElementById("batterySocFill");
    if (socFill) socFill.style.width = "0%";
  } else {
    const clampedSoc = Math.max(0, Math.min(100, batterySoc));
    setText("batterySocValue", `${clampedSoc.toFixed(0)}%`);
    const socFill = document.getElementById("batterySocFill");
    if (socFill) socFill.style.width = `${clampedSoc}%`;
  }

  if (pvPower === null || gridPower === null || batteryPower === null || loadPower === null) {
    setText("systemBalance", `${t("balanceLabel")} -`);
  } else {
    const balance = pvPower + gridPower + batteryPower - loadPower;
    setText("systemBalance", `${t("balanceLabel")} ${formatPower(balance, "W")}`);
  }
}

function renderSummary(payload) {
  const { health, ha, core } = payload;
  setText("healthValue", health.status || "ok");
  setText("haValue", ha.ok ? t("connected") : t("error"));
  setText("coreCount", String(core.count));
  setText("coreUpdatedAt", `${t("updatedAt")}: ${new Date().toLocaleString()}`);
  renderEnergyFlow(core.items || []);
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

function renderEntitiesPage(payload) {
  const totalPages = Math.max(1, Math.ceil((payload.total || 0) / (payload.page_size || PAGE_SIZE)));
  setText("entityCount", t("totalEntities", { total: payload.total }));
  setText(
    "pageInfo",
    t("pageInfo", {
      page: payload.page,
      totalPages,
      count: payload.count,
    }),
  );
  document.getElementById("prevPageBtn").disabled = !Boolean(payload.has_prev);
  document.getElementById("nextPageBtn").disabled = !Boolean(payload.has_next);
  renderEntities(payload.items || []);
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
    stateCache.lastSummary = { health, ha, core };
    renderSummary(stateCache.lastSummary);
  } catch (err) {
    setText("healthValue", t("error"));
    setText("haValue", t("error"));
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
    stateCache.lastEntities = payload;
    renderEntitiesPage(payload);
  } catch (err) {
    setText("entityCount", t("loadFailed", { error: String(err) }));
    setText("pageInfo", t("pageDash"));
    renderEntities([]);
  }
}

async function loadCurrentTab() {
  if (currentTab === "entities") {
    await loadEntities();
    return;
  }
  await loadSummary();
}

function setActiveTab(tab, load = true) {
  currentTab = tab === "entities" ? "entities" : "dashboard";
  localStorage.setItem("activeTab", currentTab);

  const dashboardView = document.getElementById("dashboardView");
  const entitiesView = document.getElementById("entitiesView");
  const tabDashboard = document.getElementById("tabDashboard");
  const tabEntities = document.getElementById("tabEntities");

  const dashboardActive = currentTab === "dashboard";
  dashboardView.classList.toggle("hidden", !dashboardActive);
  entitiesView.classList.toggle("hidden", dashboardActive);
  tabDashboard.classList.toggle("active", dashboardActive);
  tabEntities.classList.toggle("active", !dashboardActive);

  if (load) {
    void loadCurrentTab();
  }
}

document.getElementById("langSelect").addEventListener("change", (event) => {
  const nextLang = event.target.value === "zh" ? "zh" : "en";
  currentLang = nextLang;
  localStorage.setItem("lang", nextLang);
  applyTranslations();
});

document.getElementById("refreshBtn").addEventListener("click", () => {
  void loadCurrentTab();
});
document.getElementById("tabDashboard").addEventListener("click", () => {
  setActiveTab("dashboard");
});
document.getElementById("tabEntities").addEventListener("click", () => {
  setActiveTab("entities");
});
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

applyTranslations();
setActiveTab(currentTab, false);
void loadCurrentTab();
