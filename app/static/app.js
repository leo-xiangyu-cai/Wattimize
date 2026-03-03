async function fetchJson(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 8000);
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  const response = await fetch(url, { signal: controller.signal });
  window.clearTimeout(timeoutId);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

const I18N = {
  en: {
    subtitle: "SAJ + Solplanet monitoring panel",
    languageLabel: "Language",
    autoRefreshLabel: "Auto Refresh",
    autoRefreshOff: "Off",
    autoRefresh5s: "Every 5s",
    autoRefresh10s: "Every 10s",
    refreshBtn: "Refresh",
    dashboardTab: "Dashboard",
    solplanetRawTab: "Solplanet Raw",
    entitiesTab: "Entities",
    systemTitle: "System",
    haTitle: "Home Assistant",
    coreTitle: "Core Entities",
    flowTitle: "Real-time Energy Flow Comparison",
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
    balanceStatusTitle: "Dynamic Balance Status",
    balanceStatusBalanced: "Cleared",
    balanceStatusUnbalanced: "Not Cleared",
    balanceStatusNoData: "Not enough data",
    balanceResidualLabel: "Net {value}",
    balanceFormulaLabel: "Balance Formula",
    updatedAt: "Updated",
    connected: "Connected",
    error: "Error",
    coreDualLabel: "SAJ {saj} / Solplanet {solplanet}",
    totalEntities: "Total {total} entities",
    pageInfo: "Page {page}/{totalPages} (showing {count})",
    pageDash: "Page -",
    loadFailed: "Load failed: {error}",
    inverterRunning: "Running",
    inverterOffline: "Offline",
    inverterStandby: "Standby",
    inverterUnknown: "Unknown",
    sourceStatusOk: "Data OK ({count}/6)",
    sourceStatusPartial: "Partial data ({count}/6)",
    sourceStatusStale: "Using cached data ({count}/6)",
    sourceStatusFailed: "Fetch failed",
    flowMetaIdle: "Idle · Updated: -",
    flowMetaLoading: "Loading · Updated: -",
    flowMetaDoneOk: "Updated · {updated}",
    flowMetaDonePartial: "Partial ({count}/6) · {updated}",
    flowMetaDoneStale: "Cached ({count}/6) · {updated}",
    flowMetaFailed: "Load failed · {updated}",
    solplanetRawTitle: "Solplanet Raw API Dump",
    solplanetRawMeta: "Fetch {ms} ms · inverter {inverter} · battery {battery}",
    solplanetRawMetaDash: "Fetch - ms · inverter - · battery -",
    endpointOk: "OK",
    endpointError: "Error",
    endpointPath: "Path",
    rawLoadFailed: "Raw load failed: {error}",
    rawLoading: "Loading",
    rawDone: "Loaded",
    rawSummary: "Updated {updated} · OK {ok}/{total} · Failed {failed}",
    rawSummaryDash: "Updated - · OK -/- · Failed -",
    rawApiGetdev2: "Device 2 Info",
    rawApiGetdevdata2: "Device 2 Data",
    rawApiGetdevdata3: "Device 3 Data",
    rawApiGetdevdata4: "Device 4 Data",
    rawApiGetdefine: "Schedule",
  },
  zh: {
    subtitle: "SAJ + Solplanet 监控面板",
    languageLabel: "语言",
    autoRefreshLabel: "自动刷新",
    autoRefreshOff: "关闭",
    autoRefresh5s: "每 5 秒",
    autoRefresh10s: "每 10 秒",
    refreshBtn: "刷新",
    dashboardTab: "总览",
    solplanetRawTab: "Solplanet 原始",
    entitiesTab: "实体",
    systemTitle: "系统状态",
    haTitle: "Home Assistant",
    coreTitle: "核心实体",
    flowTitle: "实时能量流向对比",
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
    balanceStatusTitle: "动态平衡状态",
    balanceStatusBalanced: "已清零",
    balanceStatusUnbalanced: "未清零",
    balanceStatusNoData: "数据不足",
    balanceResidualLabel: "净值 {value}",
    balanceFormulaLabel: "平衡公式",
    updatedAt: "更新时间",
    connected: "已连接",
    error: "错误",
    coreDualLabel: "SAJ {saj} / Solplanet {solplanet}",
    totalEntities: "共 {total} 个实体",
    pageInfo: "第 {page}/{totalPages} 页（当前 {count} 条）",
    pageDash: "第 - 页",
    loadFailed: "加载失败：{error}",
    inverterRunning: "运行中",
    inverterOffline: "离线",
    inverterStandby: "待机",
    inverterUnknown: "未知",
    sourceStatusOk: "数据正常（{count}/6）",
    sourceStatusPartial: "数据不完整（{count}/6）",
    sourceStatusStale: "使用缓存数据（{count}/6）",
    sourceStatusFailed: "请求失败",
    flowMetaIdle: "空闲 · 更新时间: -",
    flowMetaLoading: "加载中 · 更新时间: -",
    flowMetaDoneOk: "已更新 · {updated}",
    flowMetaDonePartial: "部分数据（{count}/6）· {updated}",
    flowMetaDoneStale: "缓存数据（{count}/6）· {updated}",
    flowMetaFailed: "加载失败 · {updated}",
    solplanetRawTitle: "Solplanet 原始接口数据",
    solplanetRawMeta: "耗时 {ms} ms · 逆变器 {inverter} · 电池 {battery}",
    solplanetRawMetaDash: "耗时 - ms · 逆变器 - · 电池 -",
    endpointOk: "成功",
    endpointError: "错误",
    endpointPath: "路径",
    rawLoadFailed: "原始数据加载失败：{error}",
    rawLoading: "加载中",
    rawDone: "已加载",
    rawSummary: "更新时间 {updated} · 成功 {ok}/{total} · 失败 {failed}",
    rawSummaryDash: "更新时间 - · 成功 -/- · 失败 -",
    rawApiGetdev2: "设备2信息",
    rawApiGetdevdata2: "设备2实时",
    rawApiGetdevdata3: "设备3实时",
    rawApiGetdevdata4: "设备4实时",
    rawApiGetdefine: "调度配置",
  },
};

const pager = {
  page: 1,
  hasNext: false,
  hasPrev: false,
};
const PAGE_SIZE = 80;
const AUTO_REFRESH_KEY = "autoRefreshSeconds";
const AUTO_REFRESH_OPTIONS = [0, 5, 10];
const SOLPLANET_RAW_APIS = [
  { key: "getdev_device_2", titleKey: "rawApiGetdev2", url: "/api/solplanet/cgi/getdev-device-2" },
  { key: "getdevdata_device_2", titleKey: "rawApiGetdevdata2", url: "/api/solplanet/cgi/getdevdata-device-2" },
  { key: "getdevdata_device_3", titleKey: "rawApiGetdevdata3", url: "/api/solplanet/cgi/getdevdata-device-3" },
  { key: "getdevdata_device_4", titleKey: "rawApiGetdevdata4", url: "/api/solplanet/cgi/getdevdata-device-4" },
  { key: "getdefine", titleKey: "rawApiGetdefine", url: "/api/solplanet/cgi/getdefine" },
];
const stateCache = {
  lastSummary: null,
  lastEntities: null,
  lastSolplanetRaw: {},
  systemLoadMeta: {
    saj: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
    solplanet: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
  },
};

function getLang() {
  const saved = localStorage.getItem("lang");
  if (saved === "en" || saved === "zh") return saved;
  const browserLang = (navigator.language || "").toLowerCase();
  return browserLang.startsWith("zh") ? "zh" : "en";
}

let currentLang = getLang();
let currentTab = ["dashboard", "entities", "solplanetRaw"].includes(localStorage.getItem("activeTab"))
  ? localStorage.getItem("activeTab")
  : "dashboard";
let autoRefreshTimerId = null;
let isLoadingCurrentTab = false;
let autoRefreshSeconds = getAutoRefreshSeconds();
let summaryRequestId = 0;

function getAutoRefreshSeconds() {
  const saved = Number(localStorage.getItem(AUTO_REFRESH_KEY));
  return AUTO_REFRESH_OPTIONS.includes(saved) ? saved : 5;
}

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

function flowId(system, key) {
  return `${system}-${key}`;
}

function formatUpdatedAt(isoText) {
  if (!isoText) return `${t("updatedAt")}: -`;
  const dt = new Date(isoText);
  if (Number.isNaN(dt.getTime())) return `${t("updatedAt")}: -`;
  return `${t("updatedAt")}: ${dt.toLocaleString()}`;
}

function setSystemLoadMeta(system, patch = {}) {
  const prev = stateCache.systemLoadMeta[system] || { phase: "idle", updatedAt: null, quality: "ok", count: 0 };
  stateCache.systemLoadMeta[system] = {
    phase: patch.phase || prev.phase,
    updatedAt: Object.prototype.hasOwnProperty.call(patch, "updatedAt") ? patch.updatedAt : prev.updatedAt,
    quality: patch.quality || prev.quality,
    count: Number.isFinite(Number(patch.count)) ? Number(patch.count) : prev.count,
  };
  renderSystemLoadMeta(system);
}

function renderSystemLoadMeta(system) {
  const meta = stateCache.systemLoadMeta[system] || { phase: "idle", updatedAt: null, quality: "ok", count: 0 };
  const updatedId = flowId(system, "updatedAt");
  const spinnerId = flowId(system, "loadingSpinner");
  const updatedText = formatUpdatedAt(meta.updatedAt);
  let lineText = t("flowMetaIdle");
  if (meta.phase === "loading") {
    lineText = t("flowMetaLoading");
  } else if (meta.phase === "failed") {
    lineText = t("flowMetaFailed", { updated: updatedText });
  } else if (meta.phase === "done") {
    if (meta.quality === "stale") lineText = t("flowMetaDoneStale", { count: meta.count, updated: updatedText });
    else if (meta.quality === "partial") lineText = t("flowMetaDonePartial", { count: meta.count, updated: updatedText });
    else lineText = t("flowMetaDoneOk", { updated: updatedText });
  }
  setText(updatedId, lineText);
  const spinner = document.getElementById(spinnerId);
  if (spinner) {
    spinner.classList.toggle("is-hidden", meta.phase !== "loading");
  }
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

  const autoRefreshSelect = document.getElementById("autoRefreshSelect");
  if (autoRefreshSelect) autoRefreshSelect.value = String(autoRefreshSeconds);

  if (stateCache.lastSummary) renderSummary(stateCache.lastSummary);
  renderSystemLoadMeta("saj");
  renderSystemLoadMeta("solplanet");
  renderSolplanetRawFromCache();
  if (stateCache.lastEntities) renderEntitiesPage(stateCache.lastEntities);
}

function formatPowerKwFromWatts(watts) {
  if (watts === null || watts === undefined) return "-";
  return `${(Number(watts) / 1000).toFixed(3)} kW`;
}

function formatSignedKwFromWatts(watts) {
  if (watts === null || watts === undefined) return "-";
  const value = Number(watts);
  const sign = value >= 0 ? "+" : "-";
  return `${sign}${Math.abs(value / 1000).toFixed(3)} kW`;
}

function setFlowLine(id, active, reverse = false) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle("active", Boolean(active));
  el.classList.toggle("reverse", Boolean(reverse));
}

function getFlowQuality(flowPayload, matchedEntities) {
  if (flowPayload && flowPayload.__load_error) return "failed";
  if (flowPayload && flowPayload.stale) return "stale";
  if (matchedEntities >= 6) return "ok";
  return "partial";
}

function setModeClass(id, mode) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove("mode-positive", "mode-negative");
  if (mode === "positive") el.classList.add("mode-positive");
  if (mode === "negative") el.classList.add("mode-negative");
}

function inverterStateText(raw) {
  const inverterStateRaw = String(raw || "").toLowerCase();
  if (inverterStateRaw === "running") return t("inverterRunning");
  if (inverterStateRaw === "offline") return t("inverterOffline");
  if (inverterStateRaw === "standby") return t("inverterStandby");
  if (!inverterStateRaw) return t("inverterUnknown");
  return String(raw || "-");
}

function setBalanceStatus(system, balanceW) {
  const statusId = flowId(system, "balanceStatusText");
  const residualId = flowId(system, "balanceResidualValue");
  const statusEl = document.getElementById(statusId);
  if (!statusEl) return;

  statusEl.classList.remove("status-balanced", "status-unbalanced");
  if (balanceW === null || balanceW === undefined) {
    setText(statusId, t("balanceStatusNoData"));
    setText(residualId, t("balanceResidualLabel", { value: "-" }));
    return;
  }

  const balanced = Math.round(balanceW) === 0;
  setText(statusId, balanced ? t("balanceStatusBalanced") : t("balanceStatusUnbalanced"));
  statusEl.classList.add(balanced ? "status-balanced" : "status-unbalanced");
  setText(residualId, t("balanceResidualLabel", { value: formatPowerKwFromWatts(balanceW) }));
}

function setBalanceFormula(system, pvW, gridW, batteryW, loadW, netW) {
  const formulaId = flowId(system, "balanceFormulaText");
  if (
    pvW === null ||
    pvW === undefined ||
    gridW === null ||
    gridW === undefined ||
    batteryW === null ||
    batteryW === undefined ||
    loadW === null ||
    loadW === undefined ||
    netW === null ||
    netW === undefined
  ) {
    setText(formulaId, `${t("balanceFormulaLabel")}: -`);
    return;
  }

  const batteryDischargeW = Math.max(batteryW, 0);
  const batteryChargeW = Math.max(-batteryW, 0);
  const gridImportW = Math.max(gridW, 0);
  const gridExportW = Math.max(-gridW, 0);
  const formula =
    `${t("balanceFormulaLabel")}: ` +
    `(${formatPowerKwFromWatts(pvW)}) + ` +
    `(${formatPowerKwFromWatts(batteryDischargeW)}) + ` +
    `(${formatPowerKwFromWatts(gridImportW)}) - ` +
    `(${formatPowerKwFromWatts(loadW)}) - ` +
    `(${formatPowerKwFromWatts(batteryChargeW)}) - ` +
    `(${formatPowerKwFromWatts(gridExportW)}) = ` +
    `${formatSignedKwFromWatts(netW)}`;
  setText(formulaId, formula);
}

function renderEnergyFlow(system, flowPayload) {
  const metrics = (flowPayload && flowPayload.metrics) || {};
  const matchedEntitiesRaw = metrics.matched_entities;
  const matchedEntities =
    matchedEntitiesRaw === null || matchedEntitiesRaw === undefined ? 0 : Number(matchedEntitiesRaw);

  const pvW = metrics.pv_w ?? null;
  const gridW = metrics.grid_w ?? null;
  const batteryW = metrics.battery_w ?? null;
  const loadW = metrics.load_w ?? null;
  const batterySoc = metrics.battery_soc_percent ?? null;
  const inverterStatus = metrics.inverter_status ?? null;
  const balanceW = metrics.balance_w ?? null;

  setText(flowId(system, "solarPowerValue"), formatPowerKwFromWatts(pvW));
  setText(flowId(system, "gridPowerValue"), formatPowerKwFromWatts(gridW === null ? null : Math.abs(gridW)));
  setText(
    flowId(system, "batteryPowerValue"),
    formatPowerKwFromWatts(batteryW === null ? null : Math.abs(batteryW)),
  );
  setText(flowId(system, "loadPowerValue"), formatPowerKwFromWatts(loadW));
  setText(flowId(system, "inverterStatusValue"), inverterStateText(inverterStatus));

  const solarActive = Boolean(metrics.solar_active);
  setText(flowId(system, "solarState"), solarActive ? t("stateProducing") : t("stateIdle"));
  setFlowLine(flowId(system, "lineSolar"), solarActive, false);

  const gridActive = Boolean(metrics.grid_active);
  const gridImport = Boolean(metrics.grid_import);
  setText(flowId(system, "gridState"), gridActive ? (gridImport ? t("stateImporting") : t("stateExporting")) : t("stateIdle"));
  setFlowLine(flowId(system, "lineGrid"), gridActive, !gridImport);
  if (gridActive) {
    setModeClass(flowId(system, "gridPowerValue"), gridImport ? "positive" : "negative");
    setModeClass(flowId(system, "gridState"), gridImport ? "positive" : "negative");
  } else {
    setModeClass(flowId(system, "gridPowerValue"), "");
    setModeClass(flowId(system, "gridState"), "");
  }

  const loadActive = Boolean(metrics.load_active);
  setText(flowId(system, "loadState"), loadActive ? t("stateConsuming") : t("stateIdle"));
  setFlowLine(flowId(system, "lineLoad"), loadActive, false);

  const batteryActive = Boolean(metrics.battery_active);
  const batteryDischarging = Boolean(metrics.battery_discharging);
  const batteryModeText = batteryActive
    ? batteryDischarging
      ? t("stateDischarging")
      : t("stateCharging")
    : t("stateBatteryIdle");
  setText(flowId(system, "batteryState"), batteryModeText);

  if (batteryW !== null && batteryW > 0) {
    setModeClass(flowId(system, "batteryPowerValue"), "positive");
    setModeClass(flowId(system, "batteryState"), "positive");
  } else if (batteryW !== null && batteryW < 0) {
    setModeClass(flowId(system, "batteryPowerValue"), "negative");
    setModeClass(flowId(system, "batteryState"), "negative");
  } else {
    setModeClass(flowId(system, "batteryPowerValue"), "");
    setModeClass(flowId(system, "batteryState"), "");
  }
  setFlowLine(flowId(system, "lineBattery"), batteryActive, batteryDischarging);

  if (batterySoc === null || batterySoc === undefined) {
    setText(flowId(system, "batterySocValue"), "-");
    const socFill = document.getElementById(flowId(system, "batterySocFill"));
    if (socFill) socFill.style.width = "0%";
  } else {
    const clampedSoc = Math.max(0, Math.min(100, Number(batterySoc)));
    setText(flowId(system, "batterySocValue"), `${clampedSoc.toFixed(0)}%`);
    const socFill = document.getElementById(flowId(system, "batterySocFill"));
    if (socFill) socFill.style.width = `${clampedSoc}%`;
  }

  if (balanceW === null || balanceW === undefined) {
    setText(flowId(system, "systemBalance"), `${t("balanceLabel")} -`);
    setBalanceStatus(system, null);
    setBalanceFormula(system, null, null, null, null, null);
  } else {
    setText(flowId(system, "systemBalance"), `${t("balanceLabel")} ${formatPowerKwFromWatts(balanceW)}`);
    setBalanceStatus(system, balanceW);
    setBalanceFormula(system, pvW, gridW, batteryW, loadW, balanceW);
  }
}

function renderSummary(payload) {
  const { health, ha, sajFlow, solplanetFlow } = payload;
  setText("healthValue", health.status || "ok");
  setText("haValue", ha.ok ? t("connected") : t("error"));
  const sajCount = sajFlow?.metrics?.matched_entities ?? 0;
  const solplanetCount = solplanetFlow?.metrics?.matched_entities ?? 0;
  setSystemLoadMeta("saj", { quality: getFlowQuality(sajFlow, Number(sajCount) || 0), count: Number(sajCount) || 0 });
  setSystemLoadMeta("solplanet", {
    quality: getFlowQuality(solplanetFlow, Number(solplanetCount) || 0),
    count: Number(solplanetCount) || 0,
  });
  setText("coreCount", t("coreDualLabel", { saj: sajCount, solplanet: solplanetCount }));
  renderEnergyFlow("saj", sajFlow);
  renderEnergyFlow("solplanet", solplanetFlow);
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

function ensureRawCard(key, titleKey) {
  const body = document.getElementById("solplanetRawBody");
  const cardId = `raw-card-${key}`;
  let card = document.getElementById(cardId);
  if (!card) {
    card = document.createElement("article");
    card.className = "raw-card";
    card.id = cardId;
    card.innerHTML = `
      <h3 id="raw-title-${key}"></h3>
      <div id="raw-progress-${key}" class="raw-progress"><div class="raw-progress-fill"></div></div>
      <p id="raw-meta-${key}" class="raw-meta">-</p>
      <pre id="raw-pre-${key}" class="raw-pre">-</pre>
    `;
    body.appendChild(card);
  }
  setText(`raw-title-${key}`, t(titleKey));
}

function renderSolplanetRawCard(api, state) {
  ensureRawCard(api.key, api.titleKey);
  const progress = document.getElementById(`raw-progress-${api.key}`);
  const meta = document.getElementById(`raw-meta-${api.key}`);
  const pre = document.getElementById(`raw-pre-${api.key}`);
  if (progress) {
    progress.classList.remove("loading", "done", "failed");
    if (state.phase === "loading") progress.classList.add("loading");
    else if (state.phase === "failed") progress.classList.add("failed");
    else if (state.phase === "done") progress.classList.add("done");
  }
  if (meta) {
    meta.className = `raw-meta${state.phase === "failed" ? " error" : ""}`;
    if (state.phase === "loading") {
      meta.textContent = `${t("rawLoading")} · ${t("endpointPath")}: ${state.path || "-"}`;
    } else if (state.phase === "failed") {
      meta.textContent = `${t("endpointError")} · ${state.error || "-"}`;
    } else if (state.phase === "done") {
      meta.textContent = `${t("endpointPath")}: ${state.path || "-"} · ${t("endpointOk")} · ${state.fetch_ms ?? "-"} ms`;
    } else {
      meta.textContent = "-";
    }
  }
  if (pre) pre.textContent = JSON.stringify(state.payload ?? null, null, 2);
}

function renderSolplanetRawSummary() {
  const states = Object.values(stateCache.lastSolplanetRaw || {});
  if (!states.length) {
    setText("solplanetRawMeta", t("rawSummaryDash"));
    setText("solplanetRawUpdatedAt", `${t("updatedAt")}: -`);
    return;
  }
  const okCount = states.filter((item) => item.phase === "done").length;
  const failedCount = states.filter((item) => item.phase === "failed").length;
  const latest = states
    .map((item) => item.updated_at)
    .filter(Boolean)
    .sort()
    .slice(-1)[0];
  const updatedText = latest ? new Date(latest).toLocaleString() : "-";
  setText(
    "solplanetRawMeta",
    t("rawSummary", { updated: updatedText, ok: okCount, total: states.length, failed: failedCount }),
  );
  setText("solplanetRawUpdatedAt", `${t("updatedAt")}: ${updatedText}`);
}

function renderSolplanetRawFromCache() {
  for (const api of SOLPLANET_RAW_APIS) {
    const state = stateCache.lastSolplanetRaw[api.key] || {
      phase: "idle",
      path: api.url,
      payload: null,
      error: null,
      fetch_ms: null,
      updated_at: null,
    };
    renderSolplanetRawCard(api, state);
  }
  renderSolplanetRawSummary();
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
  const requestId = ++summaryRequestId;
  const summary = stateCache.lastSummary || {
    health: { status: "ok" },
    ha: { ok: false },
    sajFlow: { metrics: {} },
    solplanetFlow: { metrics: {} },
  };
  stateCache.lastSummary = summary;
  setSystemLoadMeta("saj", { phase: "loading", updatedAt: null });
  setSystemLoadMeta("solplanet", { phase: "loading", updatedAt: null });
  renderSummary(summary);

  const baseResults = await Promise.allSettled([
    fetchJson("/api/health", { timeoutMs: 4000 }),
    fetchJson("/api/ha/ping", { timeoutMs: 5000 }),
    fetchJson("/api/energy-flow/saj", { timeoutMs: 5000 }),
  ]);
  if (requestId !== summaryRequestId) return;

  if (baseResults[0].status === "fulfilled") summary.health = baseResults[0].value;
  if (baseResults[1].status === "fulfilled") summary.ha = baseResults[1].value;
  if (baseResults[2].status === "fulfilled") {
    summary.sajFlow = { ...baseResults[2].value, __load_error: false };
    setSystemLoadMeta("saj", {
      phase: "done",
      updatedAt: baseResults[2].value?.updated_at || new Date().toISOString(),
    });
  } else {
    summary.sajFlow = { metrics: {}, __load_error: true };
    setSystemLoadMeta("saj", { phase: "failed", updatedAt: null, quality: "failed", count: 0 });
  }
  renderSummary(summary);

  // Solplanet loads independently so it cannot block SAJ rendering.
  void fetchJson("/api/energy-flow/solplanet", { timeoutMs: 7000 })
    .then((solplanetFlow) => {
      if (requestId !== summaryRequestId) return;
      summary.solplanetFlow = { ...solplanetFlow, __load_error: false };
      setSystemLoadMeta("solplanet", {
        phase: "done",
        updatedAt: solplanetFlow?.updated_at || new Date().toISOString(),
      });
      renderSummary(summary);
    })
    .catch(() => {
      if (requestId !== summaryRequestId) return;
      summary.solplanetFlow = { metrics: {}, __load_error: true };
      setSystemLoadMeta("solplanet", { phase: "failed", updatedAt: null, quality: "failed", count: 0 });
      renderSummary(summary);
    });
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

async function loadSolplanetRaw() {
  setText("solplanetRawMeta", t("rawSummaryDash"));
  setText("solplanetRawUpdatedAt", `${t("updatedAt")}: -`);
  for (const api of SOLPLANET_RAW_APIS) {
    stateCache.lastSolplanetRaw[api.key] = {
      phase: "loading",
      path: api.url,
      payload: null,
      error: null,
      fetch_ms: null,
      updated_at: null,
    };
    renderSolplanetRawCard(api, stateCache.lastSolplanetRaw[api.key]);
  }
  renderSolplanetRawSummary();

  const tasks = SOLPLANET_RAW_APIS.map(async (api) => {
    try {
      const response = await fetchJson(api.url, { timeoutMs: 20000 });
      stateCache.lastSolplanetRaw[api.key] = {
        phase: response?.ok ? "done" : "failed",
        path: response?.path || api.url,
        payload: response?.payload ?? null,
        error: response?.error || null,
        fetch_ms: response?.fetch_ms ?? null,
        updated_at: response?.updated_at || new Date().toISOString(),
      };
    } catch (err) {
      stateCache.lastSolplanetRaw[api.key] = {
        phase: "failed",
        path: api.url,
        payload: null,
        error: String(err),
        fetch_ms: null,
        updated_at: new Date().toISOString(),
      };
    }
    renderSolplanetRawCard(api, stateCache.lastSolplanetRaw[api.key]);
    renderSolplanetRawSummary();
  });

  await Promise.allSettled(tasks);
}

async function loadCurrentTab() {
  if (isLoadingCurrentTab) return;
  isLoadingCurrentTab = true;
  try {
    if (currentTab === "entities") {
      await loadEntities();
      return;
    }
    if (currentTab === "solplanetRaw") {
      await loadSolplanetRaw();
      return;
    }
    await loadSummary();
  } finally {
    isLoadingCurrentTab = false;
  }
}

function setAutoRefresh(seconds) {
  const safeSeconds = AUTO_REFRESH_OPTIONS.includes(seconds) ? seconds : 5;
  autoRefreshSeconds = safeSeconds;
  localStorage.setItem(AUTO_REFRESH_KEY, String(safeSeconds));

  if (autoRefreshTimerId) clearInterval(autoRefreshTimerId);
  autoRefreshTimerId = null;

  if (safeSeconds > 0) {
    autoRefreshTimerId = window.setInterval(() => {
      if (currentTab !== "dashboard") return;
      void loadCurrentTab();
    }, safeSeconds * 1000);
  }

  const autoRefreshSelect = document.getElementById("autoRefreshSelect");
  if (autoRefreshSelect) autoRefreshSelect.value = String(safeSeconds);
}

function setActiveTab(tab, load = true) {
  currentTab = tab === "entities" || tab === "solplanetRaw" ? tab : "dashboard";
  localStorage.setItem("activeTab", currentTab);

  const dashboardView = document.getElementById("dashboardView");
  const solplanetRawView = document.getElementById("solplanetRawView");
  const entitiesView = document.getElementById("entitiesView");
  const tabDashboard = document.getElementById("tabDashboard");
  const tabSolplanetRaw = document.getElementById("tabSolplanetRaw");
  const tabEntities = document.getElementById("tabEntities");

  const dashboardActive = currentTab === "dashboard";
  const rawActive = currentTab === "solplanetRaw";
  dashboardView.classList.toggle("hidden", !dashboardActive);
  solplanetRawView.classList.toggle("hidden", !rawActive);
  entitiesView.classList.toggle("hidden", dashboardActive || rawActive);
  tabDashboard.classList.toggle("active", dashboardActive);
  tabSolplanetRaw.classList.toggle("active", rawActive);
  tabEntities.classList.toggle("active", currentTab === "entities");

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

document.getElementById("autoRefreshSelect").addEventListener("change", (event) => {
  setAutoRefresh(Number(event.target.value));
});

document.getElementById("refreshBtn").addEventListener("click", () => {
  void loadCurrentTab();
});

document.getElementById("tabDashboard").addEventListener("click", () => {
  setActiveTab("dashboard");
});
document.getElementById("tabSolplanetRaw").addEventListener("click", () => {
  setActiveTab("solplanetRaw");
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
setAutoRefresh(autoRefreshSeconds);
void loadCurrentTab();
