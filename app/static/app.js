async function fetchJson(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 8000);
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  const response = await fetch(url, {
    method: options.method || "GET",
    headers: options.headers || {},
    body: options.body,
    signal: controller.signal,
  });
  window.clearTimeout(timeoutId);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

async function readErrorMessage(response) {
  const text = await response.text();
  if (!text) return `HTTP ${response.status}`;
  try {
    const parsed = JSON.parse(text);
    if (typeof parsed?.detail === "string") return parsed.detail;
  } catch (_err) {
    // Ignore parse failures and fallback to raw text.
  }
  return text;
}

const I18N = {
  en: {
    subtitle: "SAJ + Solplanet monitoring panel",
    languageLabel: "Language",
    autoRefreshLabel: "Auto Refresh",
    autoRefreshOff: "Off",
    autoRefresh5s: "Every 5s",
    autoRefresh10s: "Every 10s",
    configBtn: "Config",
    configCloseBtn: "Close",
    configSaveBtn: "Save",
    configModalTitle: "Initial Setup",
    configModalDesc: "Fill Home Assistant connection info first. It takes effect immediately after saving.",
    configHaUrlLabel: "HA URL",
    configHaTokenLabel: "HA Token",
    configDongleHostLabel: "Solplanet Dongle Host",
    configSajSampleIntervalLabel: "SAJ Sampling Interval",
    configSolplanetSampleIntervalLabel: "Solplanet Sampling Interval",
    configHaUrlPlaceholder: "http://<home-assistant-host>:8123",
    configHaTokenPlaceholder: "Long-lived access token",
    configDongleHostPlaceholder: "<solplanet-dongle-host>",
    interval5s: "5 seconds",
    interval10s: "10 seconds",
    interval30s: "30 seconds",
    interval1m: "1 minute",
    interval5m: "5 minutes",
    configNeedSave: "Please fill and save configuration first",
    configStatusCheckFailed: "Config status check failed: {error}",
    configLoadFailed: "Failed to load config: {error}",
    configSaving: "Saving...",
    configSaved: "Saved",
    configSaveFailed: "Save failed: {error}",
    configMustSaveFirst: "Configuration is not complete yet. Please save first.",
    refreshBtn: "Refresh",
    dashboardTab: "Dashboard",
    solplanetRawTab: "Solplanet Raw",
    sajRawTab: "SAJ Raw",
    sajControlTab: "SAJ Control",
    entitiesTab: "Entities",
    samplingTab: "Sampling",
    sajControlTitle: "SAJ Control",
    sajControlModeTitle: "Working Mode",
    sajControlModeExplain: "Mode code mapping may differ by firmware. Test 0~2 against app labels.",
    sajControlModeCodeLabel: "Mode Code",
    sajModeOption0: "0 - Self-Consumption Mode",
    sajModeOption1: "1 - Time of Use Mode",
    sajModeOption2: "2 - Backup Mode",
    sajControlEnableTitle: "Enable & Switches",
    sajControlChargeEnableMaskLabel: "Charge Enable Mask",
    sajControlDischargeEnableMaskLabel: "Discharge Enable Mask",
    sajControlChargeSwitchLabel: "Charge Switch",
    sajControlDischargeSwitchLabel: "Discharge Switch",
    sajControlChargeSlotsEnableLabel: "Charge Slots Enable",
    sajControlDischargeSlotsEnableLabel: "Discharge Slots Enable",
    sajControlLimitsTitle: "Power Limits",
    sajControlBatteryChargeLimitLabel: "Battery Charge Limit",
    sajControlBatteryDischargeLimitLabel: "Battery Discharge Limit",
    sajControlGridChargeLimitLabel: "Grid Max Charge",
    sajControlGridDischargeLimitLabel: "Grid Max Discharge",
    sajControlChargeTitle: "Charge Slot",
    sajControlDischargeTitle: "Discharge Slot",
    sajControlSlotLabel: "Slot",
    sajControlStartLabel: "Start",
    sajControlEndLabel: "End",
    sajControlPowerLabel: "Power (%)",
    sajControlDayMaskLabel: "Day Mask",
    sajControlApplyBtn: "Apply",
    sajControlLoadFailed: "Control state load failed: {error}",
    sajControlApplyFailed: "Control apply failed: {error}",
    sajControlApplyDone: "Applied",
    sajControlCurrentStateTitle: "Current Slot State",
    sajControlEnableCol: "Enable",
    sajControlTypeCol: "Type",
    sajControlSlotCol: "Slot",
    sajControlInputStartCol: "Input Start",
    sajControlInputEndCol: "Input End",
    sajControlInputPowerCol: "Input Power(%)",
    sajControlInputMaskCol: "Input Mask",
    sajControlActualStartCol: "Actual Start",
    sajControlActualEndCol: "Actual End",
    sajControlActualPowerCol: "Actual Power(%/W)",
    sajControlActualMaskCol: "Actual Mask",
    sajDayMaskPopupTitle: "Select Weekdays",
    sajDayMaskAllDays: "All",
    sajDayMaskCancelBtn: "Cancel",
    sajDayMaskConfirmBtn: "Confirm",
    sajControlTypeCharge: "charge",
    sajControlTypeDischarge: "discharge",
    weekdayMon: "Mon",
    weekdayTue: "Tue",
    weekdayWed: "Wed",
    weekdayThu: "Thu",
    weekdayFri: "Fri",
    weekdaySat: "Sat",
    weekdaySun: "Sun",
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
    samplingTitle: "Sampling Records",
    samplingSystemLabel: "System",
    samplingRangeModeLabel: "Range",
    samplingRangeDay: "Day",
    samplingRangeWeek: "Week",
    samplingRangeMonth: "Month",
    samplingRangeCustomDate: "Custom Date",
    samplingRangeCustomDateTime: "Custom Date + Time",
    samplingWeekDisplay: "Week {week} ({start} ~ {end})",
    samplingMonthYear: "{year} UTC",
    samplingDayLabel: "Day (UTC)",
    samplingWeekLabel: "Week Anchor (UTC)",
    samplingMonthLabel: "Month (UTC)",
    samplingStartLabel: "Start (UTC)",
    samplingEndLabel: "End (UTC)",
    samplingStorageMeta: "DB {sizeMb} MB · Rows {rows} · Interval {interval}s · Estimated/day {estMb} MB",
    samplingUsageMeta: "Range usage ({system}, {range}): Load {load} kWh · PV {pv} kWh · Grid import {gridImport} kWh · Grid export {gridExport} kWh",
    samplingUsageMetaNoData: "Range usage ({system}, {range}): not enough samples yet",
    samplingChartTitle: "Power Trend",
    samplingChartMeta: "{system} · {range} · {count} points",
    samplingChartNoData: "No chart data yet for this range",
    samplingSmoothLabel: "Smoothing",
    samplingSmoothModeDetail: "Detail",
    samplingSmoothModeSmooth: "Smooth",
    samplingSeriesPv: "PV",
    samplingSeriesGrid: "Grid",
    samplingSeriesBattery: "Battery",
    samplingSeriesLoad: "Load",
    samplingTableTime: "Sampled At (UTC)",
    samplingTableSystem: "System",
    samplingTablePv: "PV(W)",
    samplingTableGrid: "Grid(W)",
    samplingTableBattery: "Battery(W)",
    samplingTableLoad: "Load(W)",
    samplingTableSoc: "SOC(%)",
    samplingTableBalance: "Balance(W)",
    samplingTableInverter: "Inverter",
    samplingExportBtn: "Export CSV",
    samplingImportBtn: "Import CSV",
    samplingExporting: "Exporting...",
    samplingImporting: "Importing...",
    samplingImportConfirmReplace: "Import will replace all existing sampling records. Continue?",
    samplingImportDone: "Import completed: {count} rows",
    totalSamples: "Total {total} samples",
    samplePageInfo: "Page {page}/{totalPages} (showing {count})",
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
    samplingExportFailed: "Export failed: {error}",
    samplingImportFailed: "Import failed: {error}",
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
    sajRawTitle: "SAJ Raw (Dynamic + Static)",
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
    rawApiGetdev3: "Device 3 Info",
    rawApiGetdevdata2: "Device 2 Data",
    rawApiGetdevdata3: "Device 3 Data",
    rawApiGetdevdata4: "Device 4 Data",
    rawApiGetdefine: "Schedule",
    rawApiSajDashboardSources: "Live Dashboard Sources (Dynamic)",
    rawApiSajCoreEntities: "Configured Entity List (Static)",
    rawViewExplain: "Notes",
    rawViewJson: "JSON",
    rawExplainTitle: "Field Notes",
    rawExplainField: "Field",
    rawExplainValue: "Current",
    rawExplainMeaning: "Meaning",
    rawExplainPrimary: "Primary source",
    rawExplainBackup: "Backup source",
    rawExplainRule: "Backup is used only when primary data is missing.",
    rawExplainUsedBy: "Used by Dashboard",
    rawExplainUnknown: "No preset note yet",
  },
  zh: {
    subtitle: "SAJ + Solplanet 监控面板",
    languageLabel: "语言",
    autoRefreshLabel: "自动刷新",
    autoRefreshOff: "关闭",
    autoRefresh5s: "每 5 秒",
    autoRefresh10s: "每 10 秒",
    configBtn: "配置",
    configCloseBtn: "关闭",
    configSaveBtn: "保存配置",
    configModalTitle: "首次配置",
    configModalDesc: "请先填写 Home Assistant 连接信息，保存后立即生效。",
    configHaUrlLabel: "HA URL",
    configHaTokenLabel: "HA Token",
    configDongleHostLabel: "Solplanet Dongle Host",
    configSajSampleIntervalLabel: "SAJ 采样频率",
    configSolplanetSampleIntervalLabel: "Solplanet 采样频率",
    configHaUrlPlaceholder: "http://<home-assistant-host>:8123",
    configHaTokenPlaceholder: "Long-lived access token",
    configDongleHostPlaceholder: "<solplanet-dongle-host>",
    interval5s: "5 秒",
    interval10s: "10 秒",
    interval30s: "30 秒",
    interval1m: "1 分钟",
    interval5m: "5 分钟",
    configNeedSave: "请先填写配置并保存",
    configStatusCheckFailed: "配置状态检查失败: {error}",
    configLoadFailed: "读取配置失败: {error}",
    configSaving: "保存中...",
    configSaved: "保存成功",
    configSaveFailed: "保存失败: {error}",
    configMustSaveFirst: "当前还未完成配置，请先保存",
    refreshBtn: "刷新",
    dashboardTab: "总览",
    solplanetRawTab: "Solplanet 原始",
    sajRawTab: "SAJ 原始",
    sajControlTab: "SAJ 管理",
    entitiesTab: "实体",
    samplingTab: "采样",
    sajControlTitle: "SAJ 管理",
    sajControlModeTitle: "工作模式",
    sajControlModeExplain: "mode code 与名称可能因固件不同，请按 APP 对照测试 0~2。",
    sajControlModeCodeLabel: "模式编码",
    sajModeOption0: "0 - 自发自用模式",
    sajModeOption1: "1 - 分时电价模式",
    sajModeOption2: "2 - 备电模式",
    sajControlEnableTitle: "启用与开关",
    sajControlChargeEnableMaskLabel: "充电启用掩码",
    sajControlDischargeEnableMaskLabel: "放电启用掩码",
    sajControlChargeSwitchLabel: "充电开关",
    sajControlDischargeSwitchLabel: "放电开关",
    sajControlChargeSlotsEnableLabel: "充电时段启用",
    sajControlDischargeSlotsEnableLabel: "放电时段启用",
    sajControlLimitsTitle: "功率上限",
    sajControlBatteryChargeLimitLabel: "电池充电上限",
    sajControlBatteryDischargeLimitLabel: "电池放电上限",
    sajControlGridChargeLimitLabel: "电网最大充电",
    sajControlGridDischargeLimitLabel: "电网最大放电",
    sajControlChargeTitle: "充电时段",
    sajControlDischargeTitle: "放电时段",
    sajControlSlotLabel: "时段",
    sajControlStartLabel: "开始",
    sajControlEndLabel: "结束",
    sajControlPowerLabel: "功率 (%)",
    sajControlDayMaskLabel: "星期掩码",
    sajControlApplyBtn: "应用",
    sajControlLoadFailed: "管理状态加载失败：{error}",
    sajControlApplyFailed: "应用失败：{error}",
    sajControlApplyDone: "已应用",
    sajControlCurrentStateTitle: "当前时段状态",
    sajControlEnableCol: "启用",
    sajControlTypeCol: "类型",
    sajControlSlotCol: "时段",
    sajControlInputStartCol: "输入开始",
    sajControlInputEndCol: "输入结束",
    sajControlInputPowerCol: "输入功率(%)",
    sajControlInputMaskCol: "输入掩码",
    sajControlActualStartCol: "生效开始",
    sajControlActualEndCol: "生效结束",
    sajControlActualPowerCol: "生效功率(%/W)",
    sajControlActualMaskCol: "生效掩码",
    sajDayMaskPopupTitle: "选择生效星期",
    sajDayMaskAllDays: "全选",
    sajDayMaskCancelBtn: "取消",
    sajDayMaskConfirmBtn: "确认",
    sajControlTypeCharge: "充电",
    sajControlTypeDischarge: "放电",
    weekdayMon: "周一",
    weekdayTue: "周二",
    weekdayWed: "周三",
    weekdayThu: "周四",
    weekdayFri: "周五",
    weekdaySat: "周六",
    weekdaySun: "周日",
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
    samplingTitle: "采样记录",
    samplingSystemLabel: "系统",
    samplingRangeModeLabel: "范围",
    samplingRangeDay: "天",
    samplingRangeWeek: "周",
    samplingRangeMonth: "月",
    samplingRangeCustomDate: "自定义日期",
    samplingRangeCustomDateTime: "自定义日期+时间",
    samplingWeekDisplay: "第{week}周（{start} ~ {end}）",
    samplingMonthYear: "{year} UTC",
    samplingDayLabel: "日期 (UTC)",
    samplingWeekLabel: "周锚点 (UTC)",
    samplingMonthLabel: "月份 (UTC)",
    samplingStartLabel: "开始时间 (UTC)",
    samplingEndLabel: "结束时间 (UTC)",
    samplingStorageMeta: "数据库 {sizeMb} MB · 记录 {rows} 条 · 采样间隔 {interval}s · 预计每天 {estMb} MB",
    samplingUsageMeta: "区间统计 ({system}, {range}): 负载 {load} kWh · 光伏 {pv} kWh · 电网购电 {gridImport} kWh · 电网上网 {gridExport} kWh",
    samplingUsageMetaNoData: "区间统计 ({system}, {range}): 当前样本不足",
    samplingChartTitle: "功率趋势图",
    samplingChartMeta: "{system} · {range} · {count} 个点",
    samplingChartNoData: "当前时间范围暂无图表数据",
    samplingSmoothLabel: "平滑度",
    samplingSmoothModeDetail: "细节优先",
    samplingSmoothModeSmooth: "平滑",
    samplingSeriesPv: "光伏",
    samplingSeriesGrid: "电网",
    samplingSeriesBattery: "电池",
    samplingSeriesLoad: "负载",
    samplingTableTime: "采样时间 (UTC)",
    samplingTableSystem: "系统",
    samplingTablePv: "光伏(W)",
    samplingTableGrid: "电网(W)",
    samplingTableBattery: "电池(W)",
    samplingTableLoad: "负载(W)",
    samplingTableSoc: "SOC(%)",
    samplingTableBalance: "平衡(W)",
    samplingTableInverter: "逆变器",
    samplingExportBtn: "导出 CSV",
    samplingImportBtn: "导入 CSV",
    samplingExporting: "导出中...",
    samplingImporting: "导入中...",
    samplingImportConfirmReplace: "导入会覆盖现有采样数据，是否继续？",
    samplingImportDone: "导入完成：{count} 条",
    totalSamples: "共 {total} 条采样",
    samplePageInfo: "第 {page}/{totalPages} 页（当前 {count} 条）",
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
    samplingExportFailed: "导出失败：{error}",
    samplingImportFailed: "导入失败：{error}",
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
    sajRawTitle: "SAJ 原始数据（动态 + 静态）",
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
    rawApiGetdev3: "设备3信息",
    rawApiGetdevdata2: "设备2实时",
    rawApiGetdevdata3: "设备3实时",
    rawApiGetdevdata4: "设备4实时",
    rawApiGetdefine: "调度配置",
    rawApiSajDashboardSources: "实时展示来源（动态）",
    rawApiSajCoreEntities: "配置实体清单（静态）",
    rawViewExplain: "说明",
    rawViewJson: "JSON",
    rawExplainTitle: "字段说明",
    rawExplainField: "字段",
    rawExplainValue: "当前值",
    rawExplainMeaning: "含义",
    rawExplainPrimary: "主来源",
    rawExplainBackup: "备用来源",
    rawExplainRule: "仅当主来源缺失时，才会使用备用来源。",
    rawExplainUsedBy: "Dashboard 使用",
    rawExplainUnknown: "暂未预置说明",
  },
};

const pager = {
  page: 1,
  hasNext: false,
  hasPrev: false,
};
const samplingPager = {
  page: 1,
  hasNext: false,
  hasPrev: false,
};
const PAGE_SIZE = 80;
const SAMPLING_PAGE_SIZE = 100;
const AUTO_REFRESH_KEY = "autoRefreshSeconds";
const AUTO_REFRESH_OPTIONS = [0, 5, 10];
const CONFIG_SAMPLE_INTERVAL_OPTIONS = [5, 10, 30, 60, 300];
const SOLPLANET_RAW_APIS = [
  { key: "getdev_device_2", titleKey: "rawApiGetdev2", url: "/api/solplanet/cgi/getdev-device-2" },
  { key: "getdev_device_3", titleKey: "rawApiGetdev3", url: "/api/solplanet/cgi/getdev-device-3" },
  { key: "getdevdata_device_2", titleKey: "rawApiGetdevdata2", url: "/api/solplanet/cgi/getdevdata-device-2" },
  { key: "getdevdata_device_3", titleKey: "rawApiGetdevdata3", url: "/api/solplanet/cgi/getdevdata-device-3" },
  { key: "getdevdata_device_4", titleKey: "rawApiGetdevdata4", url: "/api/solplanet/cgi/getdevdata-device-4" },
  { key: "getdefine", titleKey: "rawApiGetdefine", url: "/api/solplanet/cgi/getdefine" },
];
const SAJ_RAW_APIS = [
  { key: "saj_dashboard_sources", titleKey: "rawApiSajDashboardSources", url: "/api/saj/raw/dashboard-sources" },
  { key: "saj_core_entities", titleKey: "rawApiSajCoreEntities", url: "/api/saj/raw/core-entities" },
];
const SAMPLING_SERIES = [
  { key: "pv_w", labelKey: "samplingSeriesPv", color: "#f59e0b" },
  { key: "grid_w", labelKey: "samplingSeriesGrid", color: "#2563eb" },
  { key: "battery_w", labelKey: "samplingSeriesBattery", color: "#10b981" },
  { key: "load_w", labelKey: "samplingSeriesLoad", color: "#ef4444" },
];
const SOLPLANET_RAW_FIELD_HELP = {
  getdev_device_2: {
    isn: { zh: "逆变器序列号", en: "Inverter serial number" },
    add: { zh: "设备地址/编号", en: "Device address/index" },
    safety: { zh: "安规/并网规则代码", en: "Grid/safety profile code" },
    rate: { zh: "逆变器额定功率（W）", en: "Inverter rated power (W)" },
    msw: { zh: "主控固件版本", en: "Main firmware version" },
    ssw: { zh: "通信/从控固件版本", en: "Subsystem firmware version" },
    tsw: { zh: "触摸屏/终端固件版本", en: "Terminal firmware version" },
    pac: { zh: "当前有功功率（W）", en: "Current active power (W)" },
    etd: { zh: "当日发电量（通常 kWh）", en: "Today yield (typically kWh)" },
    eto: { zh: "累计发电量（通常 kWh）", en: "Lifetime yield (typically kWh)" },
    err: { zh: "故障码（0 通常表示无故障）", en: "Error code (0 usually means no fault)" },
    cmv: { zh: "通信模块版本", en: "Communication module version" },
    mty: { zh: "机型类型编码", en: "Model type code" },
    typ: { zh: "设备类别编码", en: "Device category code" },
    model: { zh: "设备型号", en: "Model name" },
    Isw: { zh: "逆变器软件版本", en: "Inverter software version" },
    afci: { zh: "AFCI 相关状态/版本（空为未上报）", en: "AFCI status/version (empty when not reported)" },
    battery_topo: { zh: "电池拓扑列表（包含电池 SN/ID）", en: "Battery topology list (battery SN/ID)" },
    bat_sn: { zh: "主电池序列号", en: "Primary battery serial number" },
    bat_id: { zh: "主电池编号", en: "Primary battery id" },
    host: { zh: "主从拓扑角色编码", en: "Topology host role code" },
    "battery_topo.0.bat_sn": { zh: "电池序列号", en: "Battery serial number" },
    "battery_topo.0.bat_id": { zh: "电池编号", en: "Battery id" },
  },
  getdev_device_3: {
    mod: { zh: "电表模式编码", en: "Meter mode code" },
    enb: { zh: "电表使能状态", en: "Meter enable state" },
    exp_m: { zh: "并网/导出模式编码", en: "Export mode code" },
    regulate: { zh: "调节模式编码", en: "Regulation mode code" },
    enb_PF: { zh: "功率因数控制使能", en: "Power factor control enabled" },
    target_PF: { zh: "目标功率因数", en: "Target power factor" },
    abs: { zh: "绝对控制参数", en: "Absolute control parameter" },
    abs_offset: { zh: "绝对控制偏移量", en: "Absolute control offset" },
    total_pac: { zh: "总有功功率（W）", en: "Total active power (W)" },
    total_fac: { zh: "总频率（常见 0.01Hz 缩放）", en: "Total frequency (often 0.01Hz scaled)" },
    meter_pac: { zh: "电表有功功率（W）", en: "Meter active power (W)" },
  },
  getdevdata_device_2: {
    flg: { zh: "数据有效标记（1=有效）", en: "Data valid flag (1=valid)" },
    tim: { zh: "设备时间（YYYYMMDDhhmmss）", en: "Device timestamp (YYYYMMDDhhmmss)" },
    tmp: { zh: "逆变器温度（常见 0.1°C 缩放）", en: "Inverter temperature (often 0.1C scaled)" },
    fac: { zh: "电网频率（常见 0.01Hz 缩放）", en: "Grid frequency (often 0.01Hz scaled)" },
    pac: { zh: "逆变器有功功率（W）", en: "Inverter active power (W)" },
    sac: { zh: "视在功率（VA）", en: "Apparent power (VA)" },
    qac: { zh: "无功功率（var）", en: "Reactive power (var)" },
    eto: { zh: "累计发电量（通常 kWh）", en: "Lifetime yield (typically kWh)" },
    etd: { zh: "当日发电量（通常 kWh）", en: "Today yield (typically kWh)" },
    hto: { zh: "累计运行时长（通常小时）", en: "Total runtime (typically hours)" },
    pf: { zh: "功率因数（可能有缩放）", en: "Power factor (may be scaled)" },
    err: { zh: "故障码", en: "Error code" },
    vac: { zh: "交流相电压数组（常见 0.1V 缩放）", en: "AC phase voltage array (often 0.1V scaled)" },
    iac: { zh: "交流相电流数组（常见 0.1A 缩放）", en: "AC phase current array (often 0.1A scaled)" },
    vpv: { zh: "PV 路电压数组", en: "PV string voltage array" },
    ipv: { zh: "PV 路电流数组", en: "PV string current array" },
    str: { zh: "组串状态/字符串信息", en: "String status/info array" },
    stu: { zh: "逆变器状态码（0待机/1运行/2故障/4检查）", en: "Inverter status code (0 standby/1 running/2 fault/4 checking)" },
    pac1: { zh: "L1 有功功率（-1 常表示无数据）", en: "L1 active power (-1 often means N/A)" },
    qac1: { zh: "L1 无功功率（-1 常表示无数据）", en: "L1 reactive power (-1 often means N/A)" },
    pac2: { zh: "L2 有功功率（-1 常表示无数据）", en: "L2 active power (-1 often means N/A)" },
    qac2: { zh: "L2 无功功率（-1 常表示无数据）", en: "L2 reactive power (-1 often means N/A)" },
    pac3: { zh: "L3 有功功率（-1 常表示无数据）", en: "L3 active power (-1 often means N/A)" },
    qac3: { zh: "L3 无功功率（-1 常表示无数据）", en: "L3 reactive power (-1 often means N/A)" },
    grid_sts: { zh: "电网状态码", en: "Grid status code" },
  },
  getdevdata_device_3: {
    flg: { zh: "电表数据有效标记（0 常为无有效数据）", en: "Meter data valid flag (0 often means invalid/no data)" },
    tim: { zh: "电表时间（YYYYMMDDhhmmss）", en: "Meter timestamp (YYYYMMDDhhmmss)" },
    pac: { zh: "电表有功功率（W），用于电网功率判断", en: "Meter active power (W), used as grid power" },
    itd: { zh: "当日购电量（常见含义）", en: "Daily imported energy (common meaning)" },
    otd: { zh: "当日上网电量（常见含义）", en: "Daily exported energy (common meaning)" },
    iet: { zh: "累计购电量（常见含义）", en: "Total imported energy (common meaning)" },
    oet: { zh: "累计上网电量（常见含义）", en: "Total exported energy (common meaning)" },
    mod: { zh: "电表模式编码", en: "Meter mode code" },
    enb: { zh: "使能状态编码", en: "Enable state code" },
    meter_general: { zh: "电表总览指标", en: "Meter aggregate metrics" },
    "meter_general.prc": { zh: "电价/费率相关值（待厂家定义）", en: "Price/tariff related value (vendor-defined)" },
    "meter_general.sac": { zh: "总视在功率（VA）", en: "Total apparent power (VA)" },
    "meter_general.iac": { zh: "总电流", en: "Total current" },
    "meter_general.avg_v": { zh: "平均电压", en: "Average voltage" },
    "meter_general.avg_i": { zh: "平均电流", en: "Average current" },
    "meter_general.fac": { zh: "电网频率", en: "Grid frequency" },
    "meter_general.pf": { zh: "功率因数", en: "Power factor" },
    vac_phs: { zh: "各相相电压数组", en: "Phase voltage array" },
    iac_phs: { zh: "各相相电流数组", en: "Phase current array" },
    vac_line: { zh: "各相线电压数组", en: "Line voltage array" },
    pac_phs: { zh: "各相有功功率数组", en: "Phase active power array" },
    pf_phs: { zh: "各相功率因数数组", en: "Phase power factor array" },
  },
  getdevdata_device_4: {
    flg: { zh: "电池数据有效标记（1=有效）", en: "Battery data valid flag (1=valid)" },
    tim: { zh: "电池时间（YYYYMMDDhhmmss）", en: "Battery timestamp (YYYYMMDDhhmmss)" },
    ppv: { zh: "PV 侧功率（W）", en: "PV-side power (W)" },
    etdpv: { zh: "当日 PV 能量", en: "Daily PV energy" },
    etopv: { zh: "累计 PV 能量", en: "Total PV energy" },
    cst: { zh: "控制状态码", en: "Control state code" },
    bst: { zh: "电池状态码", en: "Battery status code" },
    eb1: { zh: "电池参数计数值 1（厂家私有）", en: "Battery counter 1 (vendor-specific)" },
    wb1: { zh: "电池参数计数值 2（厂家私有）", en: "Battery counter 2 (vendor-specific)" },
    vb: { zh: "电池电压（常见 0.01V 缩放）", en: "Battery voltage (often 0.01V scaled)" },
    cb: { zh: "电池电流（常见有缩放）", en: "Battery current (often scaled)" },
    pb: { zh: "电池功率（W，正值常表示放电）", en: "Battery power (W, positive often means discharge)" },
    tb: { zh: "电池温度（常见 0.1C 缩放）", en: "Battery temperature (often 0.1C scaled)" },
    soc: { zh: "SOC 电量百分比（%）", en: "State of charge (%)" },
    soh: { zh: "SOH 健康度（%）", en: "State of health (%)" },
    cli: { zh: "充电电流限制", en: "Charge current limit" },
    clo: { zh: "放电电流限制", en: "Discharge current limit" },
    ebi: { zh: "累计充电能量计数", en: "Total charged energy counter" },
    ebo: { zh: "累计放电能量计数", en: "Total discharged energy counter" },
    eaci: { zh: "交流侧累计输入能量", en: "AC-side cumulative import energy" },
    eaco: { zh: "交流侧累计输出能量", en: "AC-side cumulative export energy" },
    vesp: { zh: "电压设定值", en: "Voltage setpoint" },
    cesp: { zh: "电流设定值", en: "Current setpoint" },
    fesp: { zh: "频率设定值", en: "Frequency setpoint" },
    pesp: { zh: "功率设定值", en: "Power setpoint" },
    rpesp: { zh: "无功设定值", en: "Reactive power setpoint" },
    etdesp: { zh: "当日 ESP 能量", en: "Daily ESP energy" },
    etoesp: { zh: "累计 ESP 能量", en: "Total ESP energy" },
    iibs: { zh: "电池输入电流限制", en: "Battery input current limit" },
    iobs: { zh: "电池输出电流限制", en: "Battery output current limit" },
    vl1esp: { zh: "L1 电压设定", en: "L1 voltage setpoint" },
    il1esp: { zh: "L1 电流设定", en: "L1 current setpoint" },
    pac1esp: { zh: "L1 有功设定", en: "L1 active power setpoint" },
    qac1esp: { zh: "L1 无功设定", en: "L1 reactive power setpoint" },
    vl2esp: { zh: "L2 电压设定", en: "L2 voltage setpoint" },
    il2esp: { zh: "L2 电流设定", en: "L2 current setpoint" },
    pac2esp: { zh: "L2 有功设定", en: "L2 active power setpoint" },
    qac2esp: { zh: "L2 无功设定", en: "L2 reactive power setpoint" },
    vl3esp: { zh: "L3 电压设定", en: "L3 voltage setpoint" },
    il3esp: { zh: "L3 电流设定", en: "L3 current setpoint" },
    pac3esp: { zh: "L3 有功设定", en: "L3 active power setpoint" },
    qac3esp: { zh: "L3 无功设定", en: "L3 reactive power setpoint" },
    vbinv: { zh: "逆变器侧电池电压", en: "Inverter-side battery voltage" },
    cbinv: { zh: "逆变器侧电池电流", en: "Inverter-side battery current" },
  },
  getdefine: {
    Pin: { zh: "充电功率上限（W）", en: "Charge power limit (W)" },
    Pout: { zh: "放电功率上限（W）", en: "Discharge power limit (W)" },
    Sun: { zh: "周日时段配置（6 段编码）", en: "Sunday schedule config (6 encoded slots)" },
    Mon: { zh: "周一时段配置（6 段编码）", en: "Monday schedule config (6 encoded slots)" },
    Tus: { zh: "周二时段配置（6 段编码）", en: "Tuesday schedule config (6 encoded slots)" },
    Wen: { zh: "周三时段配置（6 段编码）", en: "Wednesday schedule config (6 encoded slots)" },
    Thu: { zh: "周四时段配置（6 段编码）", en: "Thursday schedule config (6 encoded slots)" },
    Fri: { zh: "周五时段配置（6 段编码）", en: "Friday schedule config (6 encoded slots)" },
    Sat: { zh: "周六时段配置（6 段编码）", en: "Saturday schedule config (6 encoded slots)" },
  },
  saj_dashboard_sources: {
    dashboard_values: { zh: "Dashboard 实际显示值（后端计算后）", en: "Final values shown on Dashboard" },
    "dashboard_values.solar_power_w": { zh: "太阳能节点功率（W）", en: "Solar node power (W)" },
    "dashboard_values.grid_power_w": { zh: "电网节点功率（W）", en: "Grid node power (W)" },
    "dashboard_values.battery_power_w": { zh: "电池节点功率（W）", en: "Battery node power (W)" },
    "dashboard_values.home_load_power_w": { zh: "家庭负载节点功率（W）", en: "Home load node power (W)" },
    "dashboard_values.battery_soc_percent": { zh: "电池 SOC 百分比", en: "Battery SOC percent" },
    "dashboard_values.inverter_status": { zh: "逆变器状态", en: "Inverter status" },
    "dashboard_values.balance_w": { zh: "功率平衡净值（W）", en: "Power balance net (W)" },
    source_entities: { zh: "各 Dashboard 值对应的实体原始状态", en: "Raw HA entity states used by Dashboard values" },
    "source_entities.pv": { zh: "太阳能功率来源实体", en: "Source entity for solar value" },
    "source_entities.grid": { zh: "电网功率来源实体", en: "Source entity for grid value" },
    "source_entities.battery": { zh: "电池功率来源实体", en: "Source entity for battery value" },
    "source_entities.load": { zh: "家庭负载来源实体", en: "Source entity for home load value" },
    "source_entities.soc": { zh: "电池 SOC 来源实体", en: "Source entity for battery SOC value" },
    "source_entities.inverter": { zh: "逆变器状态来源实体", en: "Source entity for inverter status" },
    matched_entities: { zh: "匹配到的 Dashboard 实体数量", en: "Matched dashboard entity count" },
  },
  saj_core_entities: {
    configured_entity_ids: { zh: "配置的 SAJ 核心实体 ID 列表", en: "Configured SAJ core entity ids" },
    items: { zh: "核心实体原始状态列表", en: "Raw state items for core entities" },
    "items.0.configured_entity_id": { zh: "配置项里的实体 ID", en: "Configured entity id" },
    "items.0.state": { zh: "对应实体的原始状态对象", en: "Raw state object for this entity" },
  },
};
const SOLPLANET_DASHBOARD_FIELD_MAP = {
  getdev_device_3: {
    meter_pac: [{ metric: "grid_w", kind: "backup", noteZh: "当 device=3 实时包无效时可作为电网功率参考", noteEn: "Fallback grid reference when device-3 realtime payload is invalid" }],
    total_pac: [{ metric: "load_w", kind: "backup", noteZh: "可作为家庭负载功率的参考值", noteEn: "Reference value for home load power" }],
  },
  getdevdata_device_2: {
    ppv: [{ metric: "pv_w", kind: "primary", noteZh: "对应 Dashboard 的太阳能卡片数值", noteEn: "Feeds the Solar value shown on Dashboard" }],
    vpv: [{ metric: "pv_w", kind: "backup", noteZh: "与 ipv 组合后作为太阳能卡片备用来源", noteEn: "With ipv, acts as backup source for Solar value" }],
    ipv: [{ metric: "pv_w", kind: "backup", noteZh: "与 vpv 组合后作为太阳能卡片备用来源", noteEn: "With vpv, acts as backup source for Solar value" }],
    pac: [{ metric: "load_w", kind: "primary", noteZh: "对应 Dashboard 的家庭负载卡片数值", noteEn: "Feeds the Home Load value shown on Dashboard" }],
    stu: [{ metric: "inverter_status", kind: "primary", noteZh: "对应 Dashboard 的逆变器状态", noteEn: "Feeds the Inverter status shown on Dashboard" }],
  },
  getdevdata_device_3: {
    pac: [{ metric: "grid_w", kind: "primary", noteZh: "对应 Dashboard 的电网卡片数值", noteEn: "Feeds the Grid value shown on Dashboard" }],
  },
  getdevdata_device_4: {
    pb: [{ metric: "battery_w", kind: "primary", noteZh: "对应 Dashboard 的电池卡片功率", noteEn: "Feeds the Battery power shown on Dashboard" }],
    soc: [{ metric: "battery_soc_percent", kind: "primary", noteZh: "对应 Dashboard 的电池 SOC 百分比", noteEn: "Feeds the Battery SOC shown on Dashboard" }],
    ppv: [{ metric: "pv_w", kind: "backup", noteZh: "太阳能卡片数值备用来源", noteEn: "Backup source for Solar value on Dashboard" }],
  },
  saj_dashboard_sources: {
    "dashboard_values.solar_power_w": [{ metric: "solar_power_w", kind: "primary", noteZh: "Dashboard 太阳能卡片显示值", noteEn: "Dashboard Solar card value" }],
    "dashboard_values.grid_power_w": [{ metric: "grid_power_w", kind: "primary", noteZh: "Dashboard 电网卡片显示值", noteEn: "Dashboard Grid card value" }],
    "dashboard_values.battery_power_w": [{ metric: "battery_power_w", kind: "primary", noteZh: "Dashboard 电池卡片显示值", noteEn: "Dashboard Battery card value" }],
    "dashboard_values.home_load_power_w": [{ metric: "home_load_power_w", kind: "primary", noteZh: "Dashboard 家庭负载卡片显示值", noteEn: "Dashboard Home Load card value" }],
    "dashboard_values.battery_soc_percent": [{ metric: "battery_soc_percent", kind: "primary", noteZh: "Dashboard 电池 SOC 显示值", noteEn: "Dashboard Battery SOC value" }],
    "dashboard_values.inverter_status": [{ metric: "inverter_status", kind: "primary", noteZh: "Dashboard 逆变器状态显示值", noteEn: "Dashboard Inverter status value" }],
  },
};
const stateCache = {
  lastSummary: null,
  lastEntities: null,
  lastSolplanetRaw: {},
  lastSajRaw: {},
  lastSajControl: null,
  lastSamplingStatus: null,
  lastSamplingDaily: null,
  lastSamplingPage: null,
  lastSamplingSeries: null,
  rawCardMode: {},
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
let currentTab = ["dashboard", "entities", "solplanetRaw", "sajRaw", "sajControl", "sampling"].includes(localStorage.getItem("activeTab"))
  ? localStorage.getItem("activeTab")
  : "dashboard";
let autoRefreshTimerId = null;
let isLoadingCurrentTab = false;
let autoRefreshSeconds = getAutoRefreshSeconds();
let summaryRequestId = 0;
let configReady = false;
let samplingChart = null;
let samplingChartFocusSeries = null;
let samplingChartLastPayload = null;
let samplingChartHandlersBound = false;
let samplingRangeApplyingFromBrush = false;
let samplingLegendSyncing = false;
const samplingRangeState = {
  day: "",
  week: "",
  month: "",
  monthYear: 0,
  startDate: "",
  endDate: "",
  startDateTime: "",
  endDateTime: "",
};

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

function monthLabel(month) {
  const m = Number(month);
  if (currentLang === "zh") return `${m}月`;
  const enMonths = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ];
  return enMonths[m - 1] || `Month ${m}`;
}

function wattsToKwText(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${(n / 1000).toFixed(digits)} kW`;
}

function downsampleByBucket(points, targetCount) {
  if (!Array.isArray(points) || points.length <= targetCount) return points;
  const size = points.length;
  const bucketSize = Math.max(1, Math.floor(size / targetCount));
  const out = [];
  for (let i = 0; i < size; i += bucketSize) {
    const slice = points.slice(i, Math.min(size, i + bucketSize));
    if (!slice.length) continue;
    const mid = slice[Math.floor(slice.length / 2)];
    out.push(mid);
  }
  if (out[out.length - 1]?.[0] !== points[points.length - 1]?.[0]) out.push(points[points.length - 1]);
  return out;
}

function movingAverage(points, windowSize) {
  if (!Array.isArray(points) || points.length < 3) return points;
  const half = Math.max(1, Math.floor(windowSize / 2));
  const out = [];
  for (let i = 0; i < points.length; i += 1) {
    let sum = 0;
    let count = 0;
    for (let j = i - half; j <= i + half; j += 1) {
      if (j < 0 || j >= points.length) continue;
      sum += Number(points[j][1]);
      count += 1;
    }
    out.push([points[i][0], count > 0 ? sum / count : points[i][1]]);
  }
  return out;
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
      const control = el.querySelector("input, select, textarea");
      el.childNodes.forEach((node) => {
        if (node.nodeType === Node.TEXT_NODE && node.textContent.trim()) {
          node.textContent = `${text}\n            `;
        }
      });
      if (!control && el.textContent) el.textContent = text;
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
  renderSajRawFromCache();
  renderSajControlFromCache();
  if (stateCache.lastEntities) renderEntitiesPage(stateCache.lastEntities);
}

function setConfigModalVisible(visible) {
  const modal = document.getElementById("configModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function fillConfigForm(payload = {}) {
  document.getElementById("cfgHaUrl").value = payload.ha_url || "";
  document.getElementById("cfgHaToken").value = payload.ha_token || "";
  document.getElementById("cfgDongleHost").value = payload.solplanet_dongle_host || "";
  const sajInterval = Number(payload.saj_sample_interval_seconds);
  const solplanetInterval = Number(payload.solplanet_sample_interval_seconds);
  document.getElementById("cfgSajSampleIntervalSeconds").value = String(
    CONFIG_SAMPLE_INTERVAL_OPTIONS.includes(sajInterval) ? sajInterval : 5
  );
  document.getElementById("cfgSolplanetSampleIntervalSeconds").value = String(
    CONFIG_SAMPLE_INTERVAL_OPTIONS.includes(solplanetInterval) ? solplanetInterval : 60
  );
}

function buildConfigPayloadFromForm() {
  const sajInterval = Number(document.getElementById("cfgSajSampleIntervalSeconds").value);
  const solplanetInterval = Number(document.getElementById("cfgSolplanetSampleIntervalSeconds").value);
  return {
    ha_url: document.getElementById("cfgHaUrl").value.trim(),
    ha_token: document.getElementById("cfgHaToken").value.trim(),
    solplanet_dongle_host: document.getElementById("cfgDongleHost").value.trim(),
    saj_sample_interval_seconds: CONFIG_SAMPLE_INTERVAL_OPTIONS.includes(sajInterval) ? sajInterval : 5,
    solplanet_sample_interval_seconds: CONFIG_SAMPLE_INTERVAL_OPTIONS.includes(solplanetInterval)
      ? solplanetInterval
      : 60,
  };
}

async function ensureConfigReady() {
  const saveMsg = document.getElementById("configSaveMsg");
  try {
    const status = await fetchJson("/api/config/status", { timeoutMs: 5000 });
    if (status.configured) {
      configReady = true;
      setConfigModalVisible(false);
      return true;
    }
    configReady = false;
    setConfigModalVisible(true);
    const current = await fetchJson("/api/config", { timeoutMs: 5000 });
    fillConfigForm(current);
    if (saveMsg) saveMsg.textContent = t("configNeedSave");
    return false;
  } catch (err) {
    configReady = false;
    setConfigModalVisible(true);
    if (saveMsg) saveMsg.textContent = t("configStatusCheckFailed", { error: String(err) });
    return false;
  }
}

async function openConfigModal() {
  const saveMsg = document.getElementById("configSaveMsg");
  try {
    const current = await fetchJson("/api/config", { timeoutMs: 5000 });
    fillConfigForm(current);
    setConfigModalVisible(true);
    if (saveMsg) saveMsg.textContent = "-";
  } catch (err) {
    setConfigModalVisible(true);
    if (saveMsg) saveMsg.textContent = t("configLoadFailed", { error: String(err) });
  }
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

function formatMaybeNumber(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toFixed(digits);
}

function toUtcIsoFromDateOnly(dateText) {
  const d = new Date(`${dateText}T00:00:00Z`);
  return d.toISOString();
}

function toUtcIsoFromDateEndExclusive(dateText) {
  const d = new Date(`${dateText}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString();
}

function toUtcIsoFromDateTimeLocal(dateTimeText) {
  const d = new Date(dateTimeText);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toLocalDateTimeInputValueFromMs(ms) {
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return "";
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  return `${y}-${m}-${day}T${hh}:${mm}`;
}

function toEpochMs(value) {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return null;
    // ECharts time axis usually uses ms; keep seconds fallback just in case.
    return value > 1e11 ? value : value * 1000;
  }
  if (typeof value === "string") {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return null;
    return d.getTime();
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.getTime();
  }
  return null;
}

function extractBrushTimeRange(areas) {
  const list = Array.isArray(areas) ? areas : [];
  if (!list.length) return null;
  const first = list[0];
  const coordRange = first?.coordRange;
  if (!Array.isArray(coordRange) || coordRange.length < 2) return null;
  const xRange = Array.isArray(coordRange[0]) ? coordRange[0] : coordRange;
  if (!Array.isArray(xRange) || xRange.length < 2) return null;
  const rawStart = xRange[0];
  const rawEnd = xRange[1];
  const startMs = toEpochMs(rawStart);
  const endMs = toEpochMs(rawEnd);
  if (startMs === null || endMs === null) return null;
  const from = Math.min(startMs, endMs);
  const to = Math.max(startMs, endMs);
  if (!Number.isFinite(from) || !Number.isFinite(to) || from >= to) return null;
  return { startMs: from, endMs: to };
}

async function applyChartSelectionRange(startMs, endMs) {
  if (samplingRangeApplyingFromBrush) return;
  const start = Number(startMs);
  const end = Number(endMs);
  if (!Number.isFinite(start) || !Number.isFinite(end) || start >= end) return;
  samplingRangeApplyingFromBrush = true;
  samplingRangeState.startDateTime = toLocalDateTimeInputValueFromMs(start);
  samplingRangeState.endDateTime = toLocalDateTimeInputValueFromMs(end);
  const modeSelect = document.getElementById("samplingRangeModeSelect");
  if (modeSelect) modeSelect.value = "custom_datetime";
  renderSamplingRangeInputContainer();
  samplingPager.page = 1;
  try {
    await loadSampling();
  } finally {
    samplingRangeApplyingFromBrush = false;
  }
}

function getUtcDateText(offsetDays = 0) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + offsetDays);
  return d.toISOString().slice(0, 10);
}

function getWeekInfo(anchorDateText) {
  const base = new Date(`${anchorDateText}T00:00:00Z`);
  const anchor = Number.isNaN(base.getTime()) ? new Date(`${getUtcDateText()}T00:00:00Z`) : base;
  const day = anchor.getUTCDay();
  const diffToMonday = day === 0 ? -6 : 1 - day;
  const monday = new Date(anchor);
  monday.setUTCDate(anchor.getUTCDate() + diffToMonday);
  const endExclusive = new Date(monday);
  endExclusive.setUTCDate(monday.getUTCDate() + 7);
  const sunday = new Date(monday);
  sunday.setUTCDate(monday.getUTCDate() + 6);

  const thursday = new Date(monday);
  thursday.setUTCDate(monday.getUTCDate() + 3);
  const year = thursday.getUTCFullYear();
  const jan4 = new Date(Date.UTC(year, 0, 4));
  const jan4Day = jan4.getUTCDay() || 7;
  const firstThursday = new Date(Date.UTC(year, 0, 4 + (4 - jan4Day)));
  const week = 1 + Math.floor((thursday.getTime() - firstThursday.getTime()) / (7 * 86400000));

  return {
    anchor: anchor.toISOString().slice(0, 10),
    week,
    monday: monday.toISOString().slice(0, 10),
    sunday: sunday.toISOString().slice(0, 10),
    startUtc: monday.toISOString(),
    endUtc: endExclusive.toISOString(),
  };
}

function getSamplingRange() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const dayText = samplingRangeState.day || getUtcDateText();
  const weekText = samplingRangeState.week || dayText;
  const monthNumber = Number(samplingRangeState.month || `${new Date().getUTCMonth() + 1}`);
  const monthYear = Number(samplingRangeState.monthYear || new Date().getUTCFullYear());
  const startDate = samplingRangeState.startDate || "";
  const endDate = samplingRangeState.endDate || "";
  const startDateTime = samplingRangeState.startDateTime || "";
  const endDateTime = samplingRangeState.endDateTime || "";

  if (mode === "week") {
    const info = getWeekInfo(weekText);
    return {
      mode,
      startUtc: info.startUtc,
      endUtc: info.endUtc,
      label: t("samplingWeekDisplay", { week: info.week, start: info.monday, end: info.sunday }),
    };
  }
  if (mode === "month") {
    const safeMonth = Math.max(1, Math.min(12, monthNumber || 1));
    const start = new Date(Date.UTC(monthYear, safeMonth - 1, 1, 0, 0, 0));
    const end = new Date(Date.UTC(monthYear, safeMonth, 1, 0, 0, 0));
    return {
      mode,
      startUtc: start.toISOString(),
      endUtc: end.toISOString(),
      label: `${monthYear}-${String(safeMonth).padStart(2, "0")} UTC`,
    };
  }
  if (mode === "custom_date") {
    const startUtc = startDate ? toUtcIsoFromDateOnly(startDate) : null;
    const endUtc = endDate ? toUtcIsoFromDateEndExclusive(endDate) : null;
    const startLabel = startDate || "-";
    const endLabel = endDate || "-";
    return {
      mode,
      startUtc,
      endUtc,
      label: `${startLabel} ~ ${endLabel} UTC`,
      invalid: !startUtc || !endUtc || startUtc >= endUtc,
    };
  }
  if (mode === "custom_datetime") {
    const startUtc = toUtcIsoFromDateTimeLocal(startDateTime);
    const endUtc = toUtcIsoFromDateTimeLocal(endDateTime);
    const startLabel = startUtc ? startUtc.replace("T", " ").slice(0, 16) : "-";
    const endLabel = endUtc ? endUtc.replace("T", " ").slice(0, 16) : "-";
    return {
      mode,
      startUtc,
      endUtc,
      label: `${startLabel} ~ ${endLabel} UTC`,
      invalid: !startUtc || !endUtc || startUtc >= endUtc,
    };
  }
  const startUtc = toUtcIsoFromDateOnly(dayText);
  const dayEndDate = new Date(startUtc);
  dayEndDate.setUTCDate(dayEndDate.getUTCDate() + 1);
  return {
    mode: "day",
    startUtc,
    endUtc: dayEndDate.toISOString(),
    label: `${dayText} UTC`,
  };
}

function renderSamplingRangeInputContainer() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const container = document.getElementById("samplingRangeInputContainer");
  if (!container) return;

  if (mode === "week") {
    const info = getWeekInfo(samplingRangeState.week || getUtcDateText());
    samplingRangeState.week = info.anchor;
    container.innerHTML = `
      <label>
        ${t("samplingWeekLabel")}
        <div class="sampling-week-nav">
          <button id="samplingWeekPrevBtn" type="button" class="btn secondary">${t("prevBtn")}</button>
          <div id="samplingWeekDisplayText" class="sampling-week-display">${t("samplingWeekDisplay", { week: info.week, start: info.monday, end: info.sunday })}</div>
          <button id="samplingWeekNextBtn" type="button" class="btn secondary">${t("nextBtn")}</button>
        </div>
      </label>
    `;
  } else if (mode === "month") {
    const year = Number(samplingRangeState.monthYear || new Date().getUTCFullYear());
    const selectedMonth = Number(samplingRangeState.month || `${new Date().getUTCMonth() + 1}`);
    const monthOptions = Array.from({ length: 12 }, (_, i) => i + 1)
      .map((m) => `<option value="${m}"${m === selectedMonth ? " selected" : ""}>${monthLabel(m)}</option>`)
      .join("");
    container.innerHTML = `
      <label>
        ${t("samplingMonthLabel")}
        <div class="sampling-month-row">
          <span class="muted">${t("samplingMonthYear", { year })}</span>
          <div id="samplingMonthField" class="sampling-field">
            <select id="samplingMonthInput">${monthOptions}</select>
          </div>
        </div>
      </label>
    `;
  } else if (mode === "custom_date" || mode === "custom_datetime") {
    const isDateTime = mode === "custom_datetime";
    container.innerHTML = `
      <div class="sampling-custom-grid">
        <label>
          ${t("samplingStartLabel")}
          <div id="samplingStartField" class="sampling-field">
            <input id="samplingStartInput" type="${isDateTime ? "datetime-local" : "date"}" ${isDateTime ? 'step="60"' : ""} value="${isDateTime ? (samplingRangeState.startDateTime || "") : (samplingRangeState.startDate || "")}" />
          </div>
        </label>
        <label>
          ${t("samplingEndLabel")}
          <div id="samplingEndField" class="sampling-field">
            <input id="samplingEndInput" type="${isDateTime ? "datetime-local" : "date"}" ${isDateTime ? 'step="60"' : ""} value="${isDateTime ? (samplingRangeState.endDateTime || "") : (samplingRangeState.endDate || "")}" />
          </div>
        </label>
      </div>
    `;
  } else {
    container.innerHTML = `
      <label data-i18n="samplingDayLabel">
        ${t("samplingDayLabel")}
        <div id="samplingDayField" class="sampling-field">
          <input id="samplingDayInput" type="date" value="${samplingRangeState.day || ""}" />
        </div>
      </label>
    `;
  }

  bindSamplingRangeInputEvents();
}

function bindSamplingRangeInputEvents() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const bindPicker = (fieldId, inputId, onChange) => {
    const field = document.getElementById(fieldId);
    const input = document.getElementById(inputId);
    if (!input) return;
    input.addEventListener("change", async () => {
      onChange(input.value || "");
      samplingPager.page = 1;
      await loadSampling();
    });
    if (field) {
      field.addEventListener("click", () => {
        if (typeof input.showPicker === "function") input.showPicker();
        else input.focus();
      });
    }
  };

  if (mode === "week") {
    const prevBtn = document.getElementById("samplingWeekPrevBtn");
    const nextBtn = document.getElementById("samplingWeekNextBtn");
    const moveWeek = async (deltaDays) => {
      const current = new Date(`${samplingRangeState.week || getUtcDateText()}T00:00:00Z`);
      current.setUTCDate(current.getUTCDate() + deltaDays);
      samplingRangeState.week = current.toISOString().slice(0, 10);
      renderSamplingRangeInputContainer();
      samplingPager.page = 1;
      await loadSampling();
    };
    if (prevBtn) prevBtn.addEventListener("click", async () => moveWeek(-7));
    if (nextBtn) nextBtn.addEventListener("click", async () => moveWeek(7));
    return;
  }
  if (mode === "month") {
    bindPicker("samplingMonthField", "samplingMonthInput", (v) => {
      samplingRangeState.month = v;
    });
    return;
  }
  if (mode === "custom_date" || mode === "custom_datetime") {
    const isDateTime = mode === "custom_datetime";
    bindPicker("samplingStartField", "samplingStartInput", (v) => {
      if (isDateTime) samplingRangeState.startDateTime = v;
      else samplingRangeState.startDate = v;
    });
    bindPicker("samplingEndField", "samplingEndInput", (v) => {
      if (isDateTime) samplingRangeState.endDateTime = v;
      else samplingRangeState.endDate = v;
    });
    return;
  }
  bindPicker("samplingDayField", "samplingDayInput", (v) => {
    samplingRangeState.day = v;
  });
}

function buildSamplingUrl() {
  const params = new URLSearchParams();
  const system = document.getElementById("samplingSystemSelect")?.value || "";
  const range = getSamplingRange();
  if (system) params.set("system", system);
  if (range.startUtc) params.set("start_utc", range.startUtc);
  if (range.endUtc) params.set("end_utc", range.endUtc);
  params.set("page", String(samplingPager.page));
  params.set("page_size", String(SAMPLING_PAGE_SIZE));
  return `/api/storage/samples?${params.toString()}`;
}

function renderSamplingRows(items) {
  const body = document.getElementById("samplingBody");
  body.innerHTML = "";
  for (const item of items) {
    const tr = document.createElement("tr");
    const sampledAt = item.sampled_at_utc ? new Date(item.sampled_at_utc).toLocaleString() : "-";
    tr.innerHTML = `
      <td>${sampledAt}</td>
      <td>${item.system || "-"}</td>
      <td>${formatMaybeNumber(item.pv_w, 1)}</td>
      <td>${formatMaybeNumber(item.grid_w, 1)}</td>
      <td>${formatMaybeNumber(item.battery_w, 1)}</td>
      <td>${formatMaybeNumber(item.load_w, 1)}</td>
      <td>${formatMaybeNumber(item.battery_soc_percent, 1)}</td>
      <td>${formatMaybeNumber(item.balance_w, 1)}</td>
      <td>${item.inverter_status || "-"}</td>
    `;
    body.appendChild(tr);
  }
}

function renderSamplingPage(payload) {
  const totalPages = Math.max(1, Math.ceil((payload.total || 0) / (payload.page_size || SAMPLING_PAGE_SIZE)));
  setText("samplingCount", t("totalSamples", { total: payload.total || 0 }));
  setText(
    "samplingPageInfo",
    t("samplePageInfo", {
      page: payload.page || 1,
      totalPages,
      count: payload.count || 0,
    }),
  );
  document.getElementById("samplingPrevPageBtn").disabled = !Boolean(payload.has_prev);
  document.getElementById("samplingNextPageBtn").disabled = !Boolean(payload.has_next);
  renderSamplingRows(payload.items || []);
}

function renderSamplingStatus(status) {
  const sizeMb = status?.db_size_bytes ? (Number(status.db_size_bytes) / (1024 * 1024)).toFixed(2) : "0.00";
  const estMb = status?.estimated_mb_per_day_total ?? 0;
  const selectedSystem = document.getElementById("samplingSystemSelect")?.value || "saj";
  let interval = status?.sample_interval_seconds;
  if (selectedSystem === "saj" && status?.saj_sample_interval_seconds !== undefined) {
    interval = status.saj_sample_interval_seconds;
  }
  if (selectedSystem === "solplanet" && status?.solplanet_sample_interval_seconds !== undefined) {
    interval = status.solplanet_sample_interval_seconds;
  }
  setText(
    "samplingStorageMeta",
    t("samplingStorageMeta", {
      sizeMb,
      rows: status?.rows ?? 0,
      interval: interval ?? "-",
      estMb: Number(estMb).toFixed(2),
    }),
  );
  const updatedAt = status?.last_sample_utc ? new Date(status.last_sample_utc).toLocaleString() : "-";
  setText("samplingUpdatedAt", `${t("updatedAt")}: ${updatedAt}`);
}

function renderSamplingUsage(usage, rangeLabel) {
  const system = usage?.system || "-";
  const energy = usage?.energy_kwh || {};
  const hasData = Number(usage?.samples || 0) >= 2;
  if (!hasData) {
    setText("samplingDailyMeta", t("samplingUsageMetaNoData", { system, range: rangeLabel }));
    return;
  }
  setText(
    "samplingDailyMeta",
    t("samplingUsageMeta", {
      system,
      range: rangeLabel,
      load: formatMaybeNumber(energy.home_load, 3),
      pv: formatMaybeNumber(energy.solar_generation, 3),
      gridImport: formatMaybeNumber(energy.grid_import, 3),
      gridExport: formatMaybeNumber(energy.grid_export, 3),
    }),
  );
}

function ensureSamplingChart() {
  if (samplingChart) return samplingChart;
  const canvas = document.getElementById("samplingChartCanvas");
  if (!canvas) return null;
  if (typeof window.echarts === "undefined") return null;
  samplingChart = window.echarts.init(canvas, null, { renderer: "canvas" });
  if (!samplingChartHandlersBound) {
    samplingChart.on("click", (params) => {
      if (params?.componentType !== "series") return;
      const picked = params.seriesName || params.name;
      if (!picked) return;
      samplingChartFocusSeries = samplingChartFocusSeries === picked ? null : picked;
      if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
    });
    samplingChart.on("legendselectchanged", (params) => {
      if (samplingLegendSyncing) return;
      const picked = params?.name;
      if (!picked) return;
      samplingChartFocusSeries = samplingChartFocusSeries === picked ? null : picked;
      if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
      // Keep all legend items selected; focus is controlled by line styles, not hide/show.
      if (samplingChart) {
        samplingLegendSyncing = true;
        samplingChart.dispatchAction({ type: "legendAllSelect" });
        samplingLegendSyncing = false;
      }
    });
    samplingChart.on("restore", () => {
      samplingChartFocusSeries = null;
      if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
    });
    const applyFromAreas = (areas) => {
      const range = extractBrushTimeRange(areas);
      if (!range) return;
      if (samplingChart) samplingChart.dispatchAction({ type: "brush", areas: [] });
      void applyChartSelectionRange(range.startMs, range.endMs);
    };
    samplingChart.on("brushEnd", (event) => {
      applyFromAreas(event?.areas);
    });
    samplingChart.on("brushSelected", (event) => {
      const batch = Array.isArray(event?.batch) ? event.batch : [];
      applyFromAreas(batch[0]?.areas);
    });
    samplingChartHandlersBound = true;
  }
  return samplingChart;
}

function getSamplingSmoothConfig() {
  const mode = document.getElementById("samplingSmoothModeSelect")?.value || "smooth";
  if (mode === "detail") {
    return { targetCount: 420, window1: 5, window2: 1, smooth: 0.35 };
  }
  return { targetCount: 180, window1: 15, window2: 11, smooth: 0.85 };
}

function renderSamplingChart(seriesPayload) {
  samplingChartLastPayload = seriesPayload;
  const chart = ensureSamplingChart();
  if (!chart) return;
  const items = Array.isArray(seriesPayload?.items) ? seriesPayload.items : [];
  const startMs = new Date(seriesPayload?.start_at_utc || "").getTime();
  const endMs = new Date(seriesPayload?.end_at_utc || "").getTime();
  const xMin = Number.isFinite(startMs) ? startMs : null;
  const xMax = Number.isFinite(endMs) ? endMs : null;

  const smoothCfg = getSamplingSmoothConfig();
  const preparedSeries = SAMPLING_SERIES.map((meta) => {
    const focused = !samplingChartFocusSeries || samplingChartFocusSeries === meta.key;
    const color = focused ? meta.color : "#b8c2bc";
    const lineOpacity = focused ? 0.95 : 0.35;
    const lineWidth = focused ? 2.8 : 1.4;
    const rawData = items
      .map((item) => [new Date(item.sampled_at_utc || "").getTime(), Number(item[meta.key])])
      .filter((pair) => Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
    // Visual-only heavy smoothing for dashboard readability.
    const sampledData = downsampleByBucket(rawData, smoothCfg.targetCount);
    const pass1 = movingAverage(sampledData, smoothCfg.window1);
    const data = movingAverage(pass1, smoothCfg.window2);

    return {
      meta,
      focused,
      color,
      lineOpacity,
      lineWidth,
      data,
    };
  });

  let maxPoint = null;
  const maxCandidates = preparedSeries.filter((item) => item.focused && Array.isArray(item.data));
  for (const item of maxCandidates) {
    for (const pair of item.data) {
      const y = Number(pair[1]);
      if (!Number.isFinite(y)) continue;
      if (!maxPoint || y > maxPoint.y) {
        maxPoint = { x: Number(pair[0]), y, key: item.meta.key };
      }
    }
  }

  const series = preparedSeries.map((item) => {
    const withMax = Boolean(maxPoint && maxPoint.key === item.meta.key);
    return {
      name: item.meta.key,
      type: "line",
      smooth: smoothCfg.smooth,
      showSymbol: false,
      data: item.data,
      lineStyle: { width: Math.max(2.2, item.lineWidth), opacity: item.lineOpacity, color: item.color, cap: "round", join: "round" },
      itemStyle: { color: item.color },
      emphasis: { focus: "series" },
      blur: { lineStyle: { opacity: 0.2 } },
      markPoint: withMax
        ? {
            symbol: "circle",
            symbolSize: 10,
            data: [{ coord: [maxPoint.x, maxPoint.y], value: maxPoint.y }],
            itemStyle: { color: item.color, borderColor: "#fff", borderWidth: 2 },
            label: {
              show: true,
              position: "top",
              distance: 7,
              offset: [0, -2],
              color: "#fff",
              backgroundColor: item.color,
              borderRadius: 4,
              padding: [3, 6],
              formatter: ({ value }) => wattsToKwText(value, 2),
            },
          }
        : undefined,
      animation: false,
    };
  });

  chart.setOption(
    {
      animation: false,
      grid: { left: 58, right: 18, top: 58, bottom: 62 },
      legend: {
        top: 10,
        left: 12,
        selectedMode: true,
        selected: Object.fromEntries(SAMPLING_SERIES.map((item) => [item.key, true])),
        itemWidth: 12,
        itemHeight: 8,
        formatter: (name) => {
          const hit = SAMPLING_SERIES.find((item) => item.key === name);
          const label = hit ? t(hit.labelKey) : String(name);
          if (!samplingChartFocusSeries) return label;
          return samplingChartFocusSeries === name ? `● ${label}` : `○ ${label}`;
        },
        textStyle: { color: "#56675d", fontSize: 12 },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "cross",
          lineStyle: { type: "dashed", color: "#7d9588" },
          crossStyle: { color: "#7d9588" },
        },
        formatter: (params) => {
          if (!Array.isArray(params) || !params.length) return "";
          const ts = params[0].axisValue;
          const d = new Date(ts);
          const hh = String(d.getUTCHours()).padStart(2, "0");
          const mm = String(d.getUTCMinutes()).padStart(2, "0");
          const lines = [`${hh}:${mm} UTC`];
          for (const p of params) {
            const hit = SAMPLING_SERIES.find((item) => item.key === p.seriesName);
            const label = hit ? t(hit.labelKey) : p.seriesName;
            lines.push(`${p.marker}${label}: ${wattsToKwText(Number(p.value?.[1] ?? p.value), 2)}`);
          }
          return lines.join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        min: xMin,
        max: xMax,
        axisLine: { lineStyle: { color: "#adc2b5" } },
        splitLine: { lineStyle: { color: "#eef4ee" } },
        axisLabel: {
          color: "#6b7f72",
          formatter: (value) => {
            const d = new Date(value);
            const hh = String(d.getUTCHours()).padStart(2, "0");
            const mm = String(d.getUTCMinutes()).padStart(2, "0");
            return `${hh}:${mm}`;
          },
        },
      },
      yAxis: {
        type: "value",
        scale: true,
        name: "kW",
        nameTextStyle: { color: "#6b7f72", fontSize: 12, padding: [0, 0, 0, 8] },
        axisLine: { lineStyle: { color: "#adc2b5" } },
        splitLine: { lineStyle: { color: "#dbe7df", type: "dashed" } },
        axisLabel: { color: "#6b7f72", formatter: (v) => Number(v / 1000).toFixed(2) },
      },
      brush: {
        toolbox: false,
        xAxisIndex: 0,
        brushMode: "single",
        throttleType: "debounce",
        throttleDelay: 200,
      },
      series,
      graphic: items.length
        ? []
        : [
            {
              type: "text",
              left: "center",
              top: "middle",
              style: { text: t("samplingChartNoData"), fill: "#6b7f72", fontSize: 14 },
            },
          ],
    },
    true,
  );
  chart.dispatchAction({
    type: "takeGlobalCursor",
    key: "brush",
    brushOption: {
      brushType: "rect",
      brushMode: "single",
    },
  });
}

function getRawCardMode(key) {
  const mode = stateCache.rawCardMode[key];
  return mode === "json" ? "json" : "explain";
}

function setRawCardMode(key, mode) {
  stateCache.rawCardMode[key] = mode === "json" ? "json" : "explain";
  const explainBtn = document.getElementById(`raw-tab-explain-${key}`);
  const jsonBtn = document.getElementById(`raw-tab-json-${key}`);
  const explain = document.getElementById(`raw-explain-${key}`);
  const pre = document.getElementById(`raw-pre-${key}`);
  const isJson = getRawCardMode(key) === "json";
  if (explainBtn) explainBtn.classList.toggle("active", !isJson);
  if (jsonBtn) jsonBtn.classList.toggle("active", isJson);
  if (explain) explain.classList.toggle("is-hidden", isJson);
  if (pre) pre.classList.toggle("is-hidden", !isJson);
}

function formatRawFieldValue(value) {
  if (value === null || value === undefined) return "-";
  if (Array.isArray(value)) {
    try {
      return JSON.stringify(value);
    } catch {
      return `[${value.length} items]`;
    }
  }
  if (typeof value === "object") {
    try {
      const text = JSON.stringify(value);
      return text.length > 200 ? `${text.slice(0, 197)}...` : text;
    } catch {
      return "{...}";
    }
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function flattenRawPayload(payload, prefix = "", depth = 0, maxDepth = 6) {
  const rows = [];
  if (depth > maxDepth || payload === null || payload === undefined) return rows;

  if (Array.isArray(payload)) {
    const size = payload.length;
    rows.push({ field: prefix || "(root)", value: payload });
    const limit = Math.min(size, 20);
    for (let i = 0; i < limit; i += 1) {
      const nextPrefix = prefix ? `${prefix}.${i}` : String(i);
      rows.push(...flattenRawPayload(payload[i], nextPrefix, depth + 1, maxDepth));
    }
    return rows;
  }

  if (typeof payload !== "object") {
    rows.push({ field: prefix || "(root)", value: payload });
    return rows;
  }

  if (prefix) rows.push({ field: prefix, value: payload });
  for (const [key, value] of Object.entries(payload)) {
    const nextPrefix = prefix ? `${prefix}.${key}` : key;
    rows.push(...flattenRawPayload(value, nextPrefix, depth + 1, maxDepth));
  }
  return rows;
}

function getRawFieldMeaning(apiKey, field) {
  const table = SOLPLANET_RAW_FIELD_HELP[apiKey] || {};
  const note = table[field];
  if (!note) return t("rawExplainUnknown");
  if (typeof note === "string") return note;
  if (currentLang === "zh") return note.zh || note.en || t("rawExplainUnknown");
  return note.en || note.zh || t("rawExplainUnknown");
}

function getRawFieldDashboardUsage(apiKey, field) {
  const table = SOLPLANET_DASHBOARD_FIELD_MAP[apiKey] || {};
  return Array.isArray(table[field]) ? table[field] : [];
}

function getDashboardMetricDisplayName(metric) {
  const zhMap = {
    pv_w: "太阳能节点功率",
    grid_w: "电网节点功率",
    battery_w: "电池节点功率",
    load_w: "家庭负载节点功率",
    battery_soc_percent: "电池 SOC 百分比",
    inverter_status: "逆变器状态",
    solar_power_w: "太阳能节点功率",
    grid_power_w: "电网节点功率",
    battery_power_w: "电池节点功率",
    home_load_power_w: "家庭负载节点功率",
  };
  const enMap = {
    pv_w: "Solar node power",
    grid_w: "Grid node power",
    battery_w: "Battery node power",
    load_w: "Home load node power",
    battery_soc_percent: "Battery SOC",
    inverter_status: "Inverter status",
    solar_power_w: "Solar node power",
    grid_power_w: "Grid node power",
    battery_power_w: "Battery node power",
    home_load_power_w: "Home load node power",
  };
  if (currentLang === "zh") return zhMap[metric] || metric;
  return enMap[metric] || metric;
}

function formatDashboardUsageList(usages) {
  if (!usages.length) return "-";
  return usages
    .map((usage) => {
      const kindText = usage.kind === "backup" ? t("rawExplainBackup") : t("rawExplainPrimary");
      const note = currentLang === "zh" ? usage.noteZh || "" : usage.noteEn || "";
      const wrappedNote = note ? (currentLang === "zh" ? `（${note}）` : ` (${note})`) : "";
      const metricLabel = getDashboardMetricDisplayName(usage.metric);
      return `${kindText}: ${metricLabel}${wrappedNote}`;
    })
    .join(currentLang === "zh" ? "；" : "; ");
}

function formatDashboardUsageInline(usages) {
  if (!usages.length) return "";
  return `<div class="raw-dashboard-note">${t("rawExplainUsedBy")}: ${formatDashboardUsageList(usages)}</div>`;
}

function renderRawExplainTable(api, state) {
  const container = document.getElementById(`raw-explain-${api.key}`);
  if (!container) return;
  const payload = state && state.payload && typeof state.payload === "object" ? state.payload : {};
  const seen = new Set();
  const rows = flattenRawPayload(payload).filter((row) => {
    if (seen.has(row.field)) return false;
    seen.add(row.field);
    return true;
  });
  if (!rows.length) {
    container.innerHTML = `<p class="raw-help-empty">${t("rawExplainUnknown")}</p>`;
    return;
  }
  const body = rows
    .map((row) => {
      const meaning = getRawFieldMeaning(api.key, row.field);
      const usages = getRawFieldDashboardUsage(api.key, row.field);
      const hasPrimary = usages.some((item) => item.kind === "primary");
      const hasBackup = usages.some((item) => item.kind === "backup");
      const trClass = hasPrimary
        ? ' class="raw-row-dashboard-primary"'
        : hasBackup
          ? ' class="raw-row-dashboard-backup"'
          : "";
      return `
        <tr${trClass}>
          <td>${row.field}</td>
          <td>${formatRawFieldValue(row.value)}</td>
          <td>${meaning}${formatDashboardUsageInline(usages)}</td>
        </tr>
      `;
    })
    .join("");
  container.innerHTML = `
    <div class="raw-help-title">${t("rawExplainTitle")}</div>
    <p class="raw-help-legend">${t("rawExplainRule")}</p>
    <div class="raw-help-wrap">
      <table class="raw-help-table">
        <thead>
          <tr>
            <th>${t("rawExplainField")}</th>
            <th>${t("rawExplainValue")}</th>
            <th>${t("rawExplainMeaning")}</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function ensureRawCard(key, titleKey, bodyId) {
  const body = document.getElementById(bodyId);
  if (!body) return;
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
      <div class="raw-switch">
        <button id="raw-tab-explain-${key}" type="button" class="raw-tab-btn active"></button>
        <button id="raw-tab-json-${key}" type="button" class="raw-tab-btn"></button>
      </div>
      <div id="raw-explain-${key}" class="raw-explain"></div>
      <pre id="raw-pre-${key}" class="raw-pre">-</pre>
    `;
    body.appendChild(card);

    const explainBtn = document.getElementById(`raw-tab-explain-${key}`);
    const jsonBtn = document.getElementById(`raw-tab-json-${key}`);
    if (explainBtn) explainBtn.addEventListener("click", () => setRawCardMode(key, "explain"));
    if (jsonBtn) jsonBtn.addEventListener("click", () => setRawCardMode(key, "json"));
  }
  setText(`raw-title-${key}`, t(titleKey));
  setText(`raw-tab-explain-${key}`, t("rawViewExplain"));
  setText(`raw-tab-json-${key}`, t("rawViewJson"));
  setRawCardMode(key, getRawCardMode(key));
}

function renderRawCard(api, state, bodyId) {
  ensureRawCard(api.key, api.titleKey, bodyId);
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
  renderRawExplainTable(api, state);
}

function renderRawSummary(rawStateMap, metaId, updatedId) {
  const states = Object.values(rawStateMap || {});
  if (!states.length) {
    setText(metaId, t("rawSummaryDash"));
    setText(updatedId, `${t("updatedAt")}: -`);
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
  setText(metaId, t("rawSummary", { updated: updatedText, ok: okCount, total: states.length, failed: failedCount }));
  setText(updatedId, `${t("updatedAt")}: ${updatedText}`);
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
    renderRawCard(api, state, "solplanetRawBody");
  }
  renderRawSummary(stateCache.lastSolplanetRaw, "solplanetRawMeta", "solplanetRawUpdatedAt");
}

function renderSajRawFromCache() {
  for (const api of SAJ_RAW_APIS) {
    const state = stateCache.lastSajRaw[api.key] || {
      phase: "idle",
      path: api.url,
      payload: null,
      error: null,
      fetch_ms: null,
      updated_at: null,
    };
    renderRawCard(api, state, "sajRawBody");
  }
  renderRawSummary(stateCache.lastSajRaw, "sajRawMeta", "sajRawUpdatedAt");
}

const WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
let sajDayMaskEditingTargetId = null;

function clampMask7(raw) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(127, Math.trunc(value)));
}

function _enableMaskInputId(kind, quick = false) {
  if (quick) return kind === "charge" ? "sajQuickChargeEnableMaskInput" : "sajQuickDischargeEnableMaskInput";
  return kind === "charge" ? "sajChargeEnableMaskInput" : "sajDischargeEnableMaskInput";
}

function _enableSlotCheckboxId(kind, slot, quick = false) {
  if (quick) return kind === "charge" ? `sajTableChargeEnableSlot${slot}` : `sajTableDischargeEnableSlot${slot}`;
  return kind === "charge" ? `sajChargeEnableSlot${slot}` : `sajDischargeEnableSlot${slot}`;
}

function syncEnableCheckboxesFromInput(kind, quick = false) {
  const inputId = _enableMaskInputId(kind, quick);
  const mask = clampMask7(document.getElementById(inputId)?.value || "0");
  for (let i = 1; i <= 7; i += 1) {
    const id = _enableSlotCheckboxId(kind, i, quick);
    const chk = document.getElementById(id);
    if (chk) chk.checked = (mask & (1 << (i - 1))) !== 0;
  }
}

function syncEnableInputFromCheckboxes(kind, quick = false) {
  let mask = 0;
  for (let i = 1; i <= 7; i += 1) {
    const id = _enableSlotCheckboxId(kind, i, quick);
    const chk = document.getElementById(id);
    if (chk?.checked) mask |= 1 << (i - 1);
  }
  const inputId = _enableMaskInputId(kind, quick);
  const input = document.getElementById(inputId);
  if (input) input.value = String(mask);
}

function mirrorQuickEnableMaskInputsToMain() {
  const quickCharge = document.getElementById("sajQuickChargeEnableMaskInput");
  const quickDischarge = document.getElementById("sajQuickDischargeEnableMaskInput");
  const mainCharge = document.getElementById("sajChargeEnableMaskInput");
  const mainDischarge = document.getElementById("sajDischargeEnableMaskInput");
  if (quickCharge && mainCharge) mainCharge.value = quickCharge.value;
  if (quickDischarge && mainDischarge) mainDischarge.value = quickDischarge.value;
}

function _normalizeSajTimeForInput(value) {
  if (typeof value !== "string") return "";
  const normalized = value.trim();
  return /^(0\d|1\d|2[0-3]):[0-5]\d$/.test(normalized) ? normalized : "";
}

function setSajDayMaskModalVisible(visible) {
  const modal = document.getElementById("sajDayMaskModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function _getSajDayMaskPopupMask() {
  let mask = 0;
  WEEKDAY_ORDER.forEach((key, idx) => {
    const chk = document.getElementById(`sajDayMask${key}`);
    if (chk?.checked) mask |= 1 << idx;
  });
  return mask;
}

function _syncSajDayMaskAllDaysCheckbox() {
  const allDays = document.getElementById("sajDayMaskAllDays");
  if (!allDays) return;
  allDays.checked = _getSajDayMaskPopupMask() === 127;
}

function _setSajDayMaskPopupFromMask(rawMask) {
  const mask = clampMask7(rawMask);
  WEEKDAY_ORDER.forEach((key, idx) => {
    const chk = document.getElementById(`sajDayMask${key}`);
    if (chk) chk.checked = (mask & (1 << idx)) !== 0;
  });
  _syncSajDayMaskAllDaysCheckbox();
}

function openSajDayMaskModalForInput(inputId) {
  const input = document.getElementById(inputId);
  if (!input) return;
  sajDayMaskEditingTargetId = inputId;
  _setSajDayMaskPopupFromMask(input.value || "0");
  setSajDayMaskModalVisible(true);
}

function confirmSajDayMaskModal() {
  if (!sajDayMaskEditingTargetId) {
    setSajDayMaskModalVisible(false);
    return;
  }
  const input = document.getElementById(sajDayMaskEditingTargetId);
  if (input) input.value = String(_getSajDayMaskPopupMask());
  sajDayMaskEditingTargetId = null;
  setSajDayMaskModalVisible(false);
}

function cancelSajDayMaskModal() {
  sajDayMaskEditingTargetId = null;
  setSajDayMaskModalVisible(false);
}

function syncDayCheckboxesFromInput(kind) {
  const inputId = kind === "charge" ? "sajChargeDayMaskInput" : "sajDischargeDayMaskInput";
  const mask = clampMask7(document.getElementById(inputId)?.value || "0");
  WEEKDAY_ORDER.forEach((key, idx) => {
    const id = kind === "charge" ? `sajChargeDay${key}` : `sajDischargeDay${key}`;
    const chk = document.getElementById(id);
    if (chk) chk.checked = (mask & (1 << idx)) !== 0;
  });
}

function syncDayInputFromCheckboxes(kind) {
  let mask = 0;
  WEEKDAY_ORDER.forEach((key, idx) => {
    const id = kind === "charge" ? `sajChargeDay${key}` : `sajDischargeDay${key}`;
    const chk = document.getElementById(id);
    if (chk?.checked) mask |= 1 << idx;
  });
  const inputId = kind === "charge" ? "sajChargeDayMaskInput" : "sajDischargeDayMaskInput";
  const input = document.getElementById(inputId);
  if (input) input.value = String(mask);
}

function setSajControlInputsFromState(controlState) {
  if (!controlState || typeof controlState !== "object") return;
  const modeInputValue = controlState?.working_mode?.mode_input;
  if (modeInputValue !== null && modeInputValue !== undefined && Number(modeInputValue) >= 0 && Number(modeInputValue) <= 2) {
    const el = document.getElementById("sajModeCodeInput");
    if (el) el.value = String(modeInputValue);
  }

  const chargeSlot = Number(document.getElementById("sajChargeSlotInput")?.value || "1");
  const dischargeSlot = Number(document.getElementById("sajDischargeSlotInput")?.value || "1");
  const chargeItems = Array.isArray(controlState?.charge?.slots) ? controlState.charge.slots : [];
  const dischargeItems = Array.isArray(controlState?.discharge?.slots) ? controlState.discharge.slots : [];
  const charge = chargeItems.find((item) => Number(item?.slot) === chargeSlot);
  const discharge = dischargeItems.find((item) => Number(item?.slot) === dischargeSlot);

  if (charge) {
    const startEl = document.getElementById("sajChargeStartInput");
    const endEl = document.getElementById("sajChargeEndInput");
    const powerEl = document.getElementById("sajChargePowerInput");
    const dayMaskEl = document.getElementById("sajChargeDayMaskInput");
    if (startEl && charge.start_time) startEl.value = String(charge.start_time);
    if (endEl && charge.end_time) endEl.value = String(charge.end_time);
    if (powerEl && charge.power_percent !== null && charge.power_percent !== undefined) {
      powerEl.value = String(charge.power_percent);
    }
    if (dayMaskEl && charge.day_mask !== null && charge.day_mask !== undefined) {
      dayMaskEl.value = String(charge.day_mask);
    }
  }
  if (discharge) {
    const startEl = document.getElementById("sajDischargeStartInput");
    const endEl = document.getElementById("sajDischargeEndInput");
    const powerEl = document.getElementById("sajDischargePowerInput");
    const dayMaskEl = document.getElementById("sajDischargeDayMaskInput");
    if (startEl && discharge.start_time) startEl.value = String(discharge.start_time);
    if (endEl && discharge.end_time) endEl.value = String(discharge.end_time);
    if (powerEl && discharge.power_percent !== null && discharge.power_percent !== undefined) {
      powerEl.value = String(discharge.power_percent);
    }
    if (dayMaskEl && discharge.day_mask !== null && discharge.day_mask !== undefined) {
      dayMaskEl.value = String(discharge.day_mask);
    }
  }

  const chargeEnableMask = controlState?.charge?.time_enable_mask;
  const dischargeEnableMask = controlState?.discharge?.time_enable_mask;
  const chargeSwitch = controlState?.charge?.control_switch;
  const dischargeSwitch = controlState?.discharge?.control_switch;
  const batteryChargeLimit = controlState?.limits?.battery_charge_power_limit;
  const batteryDischargeLimit = controlState?.limits?.battery_discharge_power_limit;
  const gridChargeLimit = controlState?.limits?.grid_max_charge_power;
  const gridDischargeLimit = controlState?.limits?.grid_max_discharge_power;

  const chargeEnableEl = document.getElementById("sajChargeEnableMaskInput");
  const dischargeEnableEl = document.getElementById("sajDischargeEnableMaskInput");
  const chargeSwitchEl = document.getElementById("sajChargeSwitchInput");
  const dischargeSwitchEl = document.getElementById("sajDischargeSwitchInput");
  const batteryChargeLimitEl = document.getElementById("sajBatteryChargeLimitInput");
  const batteryDischargeLimitEl = document.getElementById("sajBatteryDischargeLimitInput");
  const gridChargeLimitEl = document.getElementById("sajGridChargeLimitInput");
  const gridDischargeLimitEl = document.getElementById("sajGridDischargeLimitInput");

  if (chargeEnableEl && chargeEnableMask !== null && chargeEnableMask !== undefined) {
    chargeEnableEl.value = String(chargeEnableMask);
  }
  if (dischargeEnableEl && dischargeEnableMask !== null && dischargeEnableMask !== undefined) {
    dischargeEnableEl.value = String(dischargeEnableMask);
  }
  const quickChargeEnableEl = document.getElementById("sajQuickChargeEnableMaskInput");
  const quickDischargeEnableEl = document.getElementById("sajQuickDischargeEnableMaskInput");
  if (quickChargeEnableEl && chargeEnableMask !== null && chargeEnableMask !== undefined) {
    quickChargeEnableEl.value = String(chargeEnableMask);
  }
  if (quickDischargeEnableEl && dischargeEnableMask !== null && dischargeEnableMask !== undefined) {
    quickDischargeEnableEl.value = String(dischargeEnableMask);
  }
  if (chargeSwitchEl && typeof chargeSwitch === "boolean") {
    chargeSwitchEl.value = chargeSwitch ? "on" : "off";
  }
  if (dischargeSwitchEl && typeof dischargeSwitch === "boolean") {
    dischargeSwitchEl.value = dischargeSwitch ? "on" : "off";
  }
  if (batteryChargeLimitEl && batteryChargeLimit !== null && batteryChargeLimit !== undefined) {
    batteryChargeLimitEl.value = String(batteryChargeLimit);
  }
  if (batteryDischargeLimitEl && batteryDischargeLimit !== null && batteryDischargeLimit !== undefined) {
    batteryDischargeLimitEl.value = String(batteryDischargeLimit);
  }
  if (gridChargeLimitEl && gridChargeLimit !== null && gridChargeLimit !== undefined) {
    gridChargeLimitEl.value = String(gridChargeLimit);
  }
  if (gridDischargeLimitEl && gridDischargeLimit !== null && gridDischargeLimit !== undefined) {
    gridDischargeLimitEl.value = String(gridDischargeLimit);
  }

  syncEnableCheckboxesFromInput("charge");
  syncEnableCheckboxesFromInput("discharge");
  syncEnableCheckboxesFromInput("charge", true);
  syncEnableCheckboxesFromInput("discharge", true);
  syncDayCheckboxesFromInput("charge");
  syncDayCheckboxesFromInput("discharge");
}

function renderSajControlSlotsTable(controlState) {
  const body = document.getElementById("sajControlSlotsBody");
  if (!body) return;
  body.innerHTML = "";

  const chargeInput = Array.isArray(controlState?.charge?.slots) ? controlState.charge.slots : [];
  const dischargeInput = Array.isArray(controlState?.discharge?.slots) ? controlState.discharge.slots : [];
  const chargeActual = Array.isArray(controlState?.charge?.effective_slots) ? controlState.charge.effective_slots : [];
  const dischargeActual = Array.isArray(controlState?.discharge?.effective_slots) ? controlState.discharge.effective_slots : [];
  const chargeEnableMask = clampMask7(controlState?.charge?.time_enable_mask ?? 0);
  const dischargeEnableMask = clampMask7(controlState?.discharge?.time_enable_mask ?? 0);

  const renderActualPower = (actual) => {
    const percent = actual?.power_percent;
    const watts = actual?.power_w_estimate;
    if (percent === null || percent === undefined) return "-";
    if (watts === null || watts === undefined) return `${percent}%`;
    return `${percent}% (${watts}W)`;
  };

  const renderRows = (kind, typeLabel, inputRows, actualRows, enableMask) => {
    for (let slot = 1; slot <= 7; slot += 1) {
      const input = inputRows.find((item) => Number(item?.slot) === slot) || {};
      const actual = actualRows.find((item) => Number(item?.slot) === slot) || {};
      const checked = (enableMask & (1 << (slot - 1))) !== 0 ? "checked" : "";
      const checkboxId = kind === "charge" ? `sajTableChargeEnableSlot${slot}` : `sajTableDischargeEnableSlot${slot}`;
      const startValue = _normalizeSajTimeForInput(input.start_time);
      const endValue = _normalizeSajTimeForInput(input.end_time);
      const powerValue = input.power_percent === null || input.power_percent === undefined ? "" : String(input.power_percent);
      const dayMaskValue = input.day_mask === null || input.day_mask === undefined ? "0" : String(clampMask7(input.day_mask));
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><input id="${checkboxId}" type="checkbox" ${checked} /></td>
        <td>${typeLabel}</td>
        <td>${slot}</td>
        <td><input id="sajTable${kind}Slot${slot}StartInput" type="time" value="${startValue}" /></td>
        <td><input id="sajTable${kind}Slot${slot}EndInput" type="time" value="${endValue}" /></td>
        <td><input id="sajTable${kind}Slot${slot}PowerInput" type="number" min="0" max="100" step="1" value="${powerValue}" /></td>
        <td><input id="sajTable${kind}Slot${slot}DayMaskInput" class="saj-mask-input-trigger" type="text" value="${dayMaskValue}" readonly /></td>
        <td>${actual.start_time ?? "-"}</td>
        <td>${actual.end_time ?? "-"}</td>
        <td>${renderActualPower(actual)}</td>
        <td>${actual.day_mask ?? "-"}</td>
      `;
      body.appendChild(tr);
    }
  };

  renderRows("charge", t("sajControlTypeCharge"), chargeInput, chargeActual, chargeEnableMask);
  renderRows("discharge", t("sajControlTypeDischarge"), dischargeInput, dischargeActual, dischargeEnableMask);
}

function renderSajControlFromCache() {
  const payload = stateCache.lastSajControl;
  if (!payload) {
    setText("sajControlMeta", "-");
    setText("sajControlUpdatedAt", `${t("updatedAt")}: -`);
    setText("sajControlStateJson", "-");
    const body = document.getElementById("sajControlSlotsBody");
    if (body) body.innerHTML = "";
    return;
  }
  const state = payload?.control_state || payload?.state || null;
  const updatedAt = state?.updated_at ? new Date(state.updated_at).toLocaleString() : "-";
  setText("sajControlUpdatedAt", `${t("updatedAt")}: ${updatedAt}`);
  const chargeEnableMask = state?.charge?.time_enable_mask ?? "-";
  const dischargeEnableMask = state?.discharge?.time_enable_mask ?? "-";
  const chargeSwitch = state?.charge?.control_switch;
  const dischargeSwitch = state?.discharge?.control_switch;
  const batterySoc = state?.battery?.soc_percent;
  const batteryPowerW = state?.battery?.power_w;
  const ratedPowerW = state?.inverter?.rated_power_w;
  setText(
    "sajControlMeta",
    `charge_enable=${chargeEnableMask}, discharge_enable=${dischargeEnableMask}, ` +
      `charge_switch=${chargeSwitch}, discharge_switch=${dischargeSwitch}, ` +
      `battery_soc=${batterySoc ?? "-"}%, battery_power=${batteryPowerW ?? "-"}W, ` +
      `rated_power=${ratedPowerW ?? "-"}W`,
  );
  const pre = document.getElementById("sajControlStateJson");
  if (pre) pre.textContent = JSON.stringify(state || payload, null, 2);
  renderSajControlSlotsTable(state);
  setSajControlInputsFromState(state);
}

async function loadSajControl() {
  try {
    const payload = await fetchJson("/api/saj/control/state", { timeoutMs: 8000 });
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
  } catch (err) {
    setText("sajControlMeta", t("sajControlLoadFailed", { error: String(err) }));
  }
}

async function applySajWorkingMode() {
  const modeCode = Number(document.getElementById("sajModeCodeInput")?.value || "0");
  try {
    const payload = await fetchJson("/api/saj/control/working-mode", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode_code: modeCode }),
      timeoutMs: 10000,
    });
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
  } catch (err) {
    setText("sajControlMeta", t("sajControlApplyFailed", { error: String(err) }));
  }
}

async function applySajSlot(kind) {
  const slotInputId = kind === "charge" ? "sajChargeSlotInput" : "sajDischargeSlotInput";
  const startInputId = kind === "charge" ? "sajChargeStartInput" : "sajDischargeStartInput";
  const endInputId = kind === "charge" ? "sajChargeEndInput" : "sajDischargeEndInput";
  const powerInputId = kind === "charge" ? "sajChargePowerInput" : "sajDischargePowerInput";
  const dayMaskInputId = kind === "charge" ? "sajChargeDayMaskInput" : "sajDischargeDayMaskInput";
  const slot = Number(document.getElementById(slotInputId)?.value || "1");
  const start = document.getElementById(startInputId)?.value || "";
  const end = document.getElementById(endInputId)?.value || "";
  const power = Number(document.getElementById(powerInputId)?.value || "0");
  const dayMask = Number(document.getElementById(dayMaskInputId)?.value || "127");
  try {
    const payload = await fetchJson(`/api/saj/control/${kind}-slots/${slot}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        start_time: start,
        end_time: end,
        power_percent: power,
        day_mask: dayMask,
      }),
      timeoutMs: 12000,
    });
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
  } catch (err) {
    setText("sajControlMeta", t("sajControlApplyFailed", { error: String(err) }));
  }
}

async function applySajToggles() {
  const chargeEnableMask = Number(document.getElementById("sajChargeEnableMaskInput")?.value || "0");
  const dischargeEnableMask = Number(document.getElementById("sajDischargeEnableMaskInput")?.value || "0");
  const chargeSwitch = (document.getElementById("sajChargeSwitchInput")?.value || "off") === "on";
  const dischargeSwitch = (document.getElementById("sajDischargeSwitchInput")?.value || "off") === "on";
  try {
    const payload = await fetchJson("/api/saj/control/toggles", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        charging_control: chargeSwitch,
        discharging_control: dischargeSwitch,
        charge_time_enable_mask: chargeEnableMask,
        discharge_time_enable_mask: dischargeEnableMask,
      }),
      timeoutMs: 12000,
    });
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
  } catch (err) {
    setText("sajControlMeta", t("sajControlApplyFailed", { error: String(err) }));
  }
}

async function applySajEnableMasksOnly() {
  const state = stateCache.lastSajControl?.control_state || stateCache.lastSajControl?.state;
  const chargeEnableMask = clampMask7(document.getElementById("sajQuickChargeEnableMaskInput")?.value || "0");
  const dischargeEnableMask = clampMask7(document.getElementById("sajQuickDischargeEnableMaskInput")?.value || "0");
  mirrorQuickEnableMaskInputsToMain();

  const edits = [];
  for (const kind of ["charge", "discharge"]) {
    const source = Array.isArray(state?.[kind]?.slots) ? state[kind].slots : [];
    for (let slot = 1; slot <= 7; slot += 1) {
      const original = source.find((item) => Number(item?.slot) === slot) || {};
      const startId = `sajTable${kind}Slot${slot}StartInput`;
      const endId = `sajTable${kind}Slot${slot}EndInput`;
      const powerId = `sajTable${kind}Slot${slot}PowerInput`;
      const dayMaskId = `sajTable${kind}Slot${slot}DayMaskInput`;
      const startInput = document.getElementById(startId)?.value || "";
      const endInput = document.getElementById(endId)?.value || "";
      const powerRaw = document.getElementById(powerId)?.value || "";
      const dayMaskRaw = document.getElementById(dayMaskId)?.value || "";

      const payload = {};
      if (startInput && startInput !== String(original.start_time || "")) payload.start_time = startInput;
      if (endInput && endInput !== String(original.end_time || "")) payload.end_time = endInput;
      if (powerRaw !== "") {
        const powerValue = Math.max(0, Math.min(100, Math.trunc(Number(powerRaw))));
        if (Number.isFinite(powerValue) && powerValue !== Number(original.power_percent)) {
          payload.power_percent = powerValue;
        }
      }
      if (dayMaskRaw !== "") {
        const dayMaskValue = clampMask7(dayMaskRaw);
        if (dayMaskValue !== clampMask7(original.day_mask)) payload.day_mask = dayMaskValue;
      }
      if (Object.keys(payload).length) edits.push({ kind, slot, payload });
    }
  }

  try {
    let payload = await fetchJson("/api/saj/control/toggles", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        charge_time_enable_mask: chargeEnableMask,
        discharge_time_enable_mask: dischargeEnableMask,
      }),
      timeoutMs: 12000,
    });
    for (const edit of edits) {
      payload = await fetchJson(`/api/saj/control/${edit.kind}-slots/${edit.slot}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(edit.payload),
        timeoutMs: 12000,
      });
    }
    payload = await fetchJson("/api/saj/control/refresh-touch", {
      method: "POST",
      timeoutMs: 12000,
    });
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
  } catch (err) {
    setText("sajControlMeta", t("sajControlApplyFailed", { error: String(err) }));
  }
}

async function applySajLimits() {
  const batteryCharge = Number(document.getElementById("sajBatteryChargeLimitInput")?.value || "0");
  const batteryDischarge = Number(document.getElementById("sajBatteryDischargeLimitInput")?.value || "0");
  const gridCharge = Number(document.getElementById("sajGridChargeLimitInput")?.value || "0");
  const gridDischarge = Number(document.getElementById("sajGridDischargeLimitInput")?.value || "0");
  try {
    const payload = await fetchJson("/api/saj/control/limits", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        battery_charge_power_limit: batteryCharge,
        battery_discharge_power_limit: batteryDischarge,
        grid_max_charge_power: gridCharge,
        grid_max_discharge_power: gridDischarge,
      }),
      timeoutMs: 12000,
    });
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
  } catch (err) {
    setText("sajControlMeta", t("sajControlApplyFailed", { error: String(err) }));
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
  void fetchJson("/api/energy-flow/solplanet", { timeoutMs: 30000 })
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

async function loadRawPanel(apis, stateMap, bodyId, metaId, updatedId) {
  setText(metaId, t("rawSummaryDash"));
  setText(updatedId, `${t("updatedAt")}: -`);
  for (const api of apis) {
    stateMap[api.key] = {
      phase: "loading",
      path: api.url,
      payload: null,
      error: null,
      fetch_ms: null,
      updated_at: null,
    };
    renderRawCard(api, stateMap[api.key], bodyId);
  }
  renderRawSummary(stateMap, metaId, updatedId);

  const tasks = apis.map(async (api) => {
    try {
      const response = await fetchJson(api.url, { timeoutMs: 30000 });
      stateMap[api.key] = {
        phase: response?.ok ? "done" : "failed",
        path: response?.path || api.url,
        payload: response?.payload ?? null,
        error: response?.error || null,
        fetch_ms: response?.fetch_ms ?? null,
        updated_at: response?.updated_at || new Date().toISOString(),
      };
    } catch (err) {
      stateMap[api.key] = {
        phase: "failed",
        path: api.url,
        payload: null,
        error: String(err),
        fetch_ms: null,
        updated_at: new Date().toISOString(),
      };
    }
    renderRawCard(api, stateMap[api.key], bodyId);
    renderRawSummary(stateMap, metaId, updatedId);
  });

  await Promise.allSettled(tasks);
}

async function loadSolplanetRaw() {
  await loadRawPanel(SOLPLANET_RAW_APIS, stateCache.lastSolplanetRaw, "solplanetRawBody", "solplanetRawMeta", "solplanetRawUpdatedAt");
}

async function loadSajRaw() {
  await loadRawPanel(SAJ_RAW_APIS, stateCache.lastSajRaw, "sajRawBody", "sajRawMeta", "sajRawUpdatedAt");
}

function setSamplingActionBusy(busy, exporting = false, importing = false) {
  const exportBtn = document.getElementById("samplingExportBtn");
  const importBtn = document.getElementById("samplingImportBtn");
  if (exportBtn) {
    exportBtn.disabled = busy;
    exportBtn.textContent = exporting ? t("samplingExporting") : t("samplingExportBtn");
  }
  if (importBtn) {
    importBtn.disabled = busy;
    importBtn.textContent = importing ? t("samplingImporting") : t("samplingImportBtn");
  }
}

async function exportSamplingCsv() {
  setSamplingActionBusy(true, true, false);
  try {
    const response = await fetch("/api/storage/export.csv", { method: "GET" });
    if (!response.ok) {
      const message = await readErrorMessage(response);
      throw new Error(message);
    }
    const blob = await response.blob();
    const filenameMatch = (response.headers.get("Content-Disposition") || "").match(/filename=\"([^\"]+)\"/);
    const filename = filenameMatch?.[1] || "energy_samples.csv";
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  } catch (err) {
    window.alert(t("samplingExportFailed", { error: String(err) }));
  } finally {
    setSamplingActionBusy(false, false, false);
  }
}

async function importSamplingCsv(file) {
  if (!file) return;
  if (!window.confirm(t("samplingImportConfirmReplace"))) return;
  setSamplingActionBusy(true, false, true);
  try {
    const formData = new FormData();
    formData.append("file", file);
    const response = await fetch("/api/storage/import.csv?replace_existing=true", {
      method: "POST",
      body: formData,
    });
    if (!response.ok) {
      const message = await readErrorMessage(response);
      throw new Error(message);
    }
    const payload = await response.json();
    window.alert(t("samplingImportDone", { count: payload.imported_rows || 0 }));
    samplingPager.page = 1;
    await loadSampling();
  } catch (err) {
    window.alert(t("samplingImportFailed", { error: String(err) }));
  } finally {
    setSamplingActionBusy(false, false, false);
    const input = document.getElementById("samplingImportFileInput");
    if (input) input.value = "";
  }
}

async function loadSampling() {
  const system = document.getElementById("samplingSystemSelect")?.value || "saj";
  const range = getSamplingRange();
  if (!range.startUtc || !range.endUtc || range.invalid) {
    setText("samplingDailyMeta", t("loadFailed", { error: "Invalid time range" }));
    renderSamplingRows([]);
    renderSamplingChart({ items: [] });
    return;
  }
  const usageUrl =
    `/api/storage/usage-range?system=${encodeURIComponent(system)}&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}`;
  const seriesUrl =
    `/api/storage/series?system=${encodeURIComponent(system)}&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}&max_points=500`;

  const [statusResult, usageResult, samplesResult, seriesResult] = await Promise.allSettled([
    fetchJson("/api/storage/status", { timeoutMs: 6000 }),
    fetchJson(usageUrl, { timeoutMs: 6000 }),
    fetchJson(buildSamplingUrl(), { timeoutMs: 6000 }),
    fetchJson(seriesUrl, { timeoutMs: 6000 }),
  ]);

  if (statusResult.status === "fulfilled") {
    stateCache.lastSamplingStatus = statusResult.value;
    renderSamplingStatus(statusResult.value);
  } else {
    setText("samplingStorageMeta", t("loadFailed", { error: String(statusResult.reason) }));
  }

  if (usageResult.status === "fulfilled") {
    stateCache.lastSamplingDaily = usageResult.value;
    renderSamplingUsage(usageResult.value, range.label);
  } else {
    setText("samplingDailyMeta", t("loadFailed", { error: String(usageResult.reason) }));
  }

  if (samplesResult.status === "fulfilled") {
    const payload = samplesResult.value;
    samplingPager.hasNext = Boolean(payload.has_next);
    samplingPager.hasPrev = Boolean(payload.has_prev);
    stateCache.lastSamplingPage = payload;
    renderSamplingPage(payload);
  } else {
    setText("samplingCount", t("loadFailed", { error: String(samplesResult.reason) }));
    setText("samplingPageInfo", t("pageDash"));
    renderSamplingRows([]);
  }

  if (seriesResult.status === "fulfilled") {
    const payload = seriesResult.value;
    stateCache.lastSamplingSeries = payload;
    setText(
      "samplingChartMeta",
      t("samplingChartMeta", {
        system: payload.system || system,
        range: range.label,
        count: payload.count || 0,
      }),
    );
    renderSamplingChart(payload);
  } else {
    setText("samplingChartMeta", t("loadFailed", { error: String(seriesResult.reason) }));
    renderSamplingChart({ items: [] });
  }
}

async function loadCurrentTab() {
  if (!configReady) return;
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
    if (currentTab === "sajRaw") {
      await loadSajRaw();
      return;
    }
    if (currentTab === "sajControl") {
      await loadSajControl();
      return;
    }
    if (currentTab === "sampling") {
      await loadSampling();
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
      if (!configReady) return;
      if (currentTab !== "dashboard") return;
      void loadCurrentTab();
    }, safeSeconds * 1000);
  }

  const autoRefreshSelect = document.getElementById("autoRefreshSelect");
  if (autoRefreshSelect) autoRefreshSelect.value = String(safeSeconds);
}

function setActiveTab(tab, load = true) {
  currentTab =
    tab === "entities" || tab === "solplanetRaw" || tab === "sajRaw" || tab === "sajControl" || tab === "sampling"
      ? tab
      : "dashboard";
  localStorage.setItem("activeTab", currentTab);

  const dashboardView = document.getElementById("dashboardView");
  const solplanetRawView = document.getElementById("solplanetRawView");
  const sajRawView = document.getElementById("sajRawView");
  const sajControlView = document.getElementById("sajControlView");
  const entitiesView = document.getElementById("entitiesView");
  const samplingView = document.getElementById("samplingView");
  const tabDashboard = document.getElementById("tabDashboard");
  const tabSolplanetRaw = document.getElementById("tabSolplanetRaw");
  const tabSajRaw = document.getElementById("tabSajRaw");
  const tabSajControl = document.getElementById("tabSajControl");
  const tabEntities = document.getElementById("tabEntities");
  const tabSampling = document.getElementById("tabSampling");

  const dashboardActive = currentTab === "dashboard";
  const solplanetRawActive = currentTab === "solplanetRaw";
  const sajRawActive = currentTab === "sajRaw";
  const sajControlActive = currentTab === "sajControl";
  const samplingActive = currentTab === "sampling";
  const anyRawActive = solplanetRawActive || sajRawActive;
  dashboardView.classList.toggle("hidden", !dashboardActive);
  solplanetRawView.classList.toggle("hidden", !solplanetRawActive);
  sajRawView.classList.toggle("hidden", !sajRawActive);
  sajControlView.classList.toggle("hidden", !sajControlActive);
  entitiesView.classList.toggle("hidden", dashboardActive || anyRawActive || samplingActive || sajControlActive);
  samplingView.classList.toggle("hidden", !samplingActive);
  tabDashboard.classList.toggle("active", dashboardActive);
  tabSolplanetRaw.classList.toggle("active", solplanetRawActive);
  tabSajRaw.classList.toggle("active", sajRawActive);
  tabSajControl.classList.toggle("active", sajControlActive);
  tabEntities.classList.toggle("active", currentTab === "entities");
  tabSampling.classList.toggle("active", samplingActive);

  if (load) {
    void loadCurrentTab();
  }
}

document.getElementById("langSelect").addEventListener("change", (event) => {
  const nextLang = event.target.value === "zh" ? "zh" : "en";
  currentLang = nextLang;
  localStorage.setItem("lang", nextLang);
  applyTranslations();
  renderSamplingRangeInputContainer();
});

document.getElementById("autoRefreshSelect").addEventListener("change", (event) => {
  setAutoRefresh(Number(event.target.value));
});

document.getElementById("refreshBtn").addEventListener("click", () => {
  void loadCurrentTab();
});
document.getElementById("configBtn").addEventListener("click", () => {
  void openConfigModal();
});

document.getElementById("tabDashboard").addEventListener("click", () => {
  setActiveTab("dashboard");
});
document.getElementById("tabSolplanetRaw").addEventListener("click", () => {
  setActiveTab("solplanetRaw");
});
document.getElementById("tabSajRaw").addEventListener("click", () => {
  setActiveTab("sajRaw");
});
document.getElementById("tabSajControl").addEventListener("click", () => {
  setActiveTab("sajControl");
});

document.getElementById("tabEntities").addEventListener("click", () => {
  setActiveTab("entities");
});
document.getElementById("tabSampling").addEventListener("click", () => {
  setActiveTab("sampling");
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

document.getElementById("samplingPrevPageBtn").addEventListener("click", async () => {
  if (!samplingPager.hasPrev || samplingPager.page <= 1) return;
  samplingPager.page -= 1;
  await loadSampling();
});

document.getElementById("samplingNextPageBtn").addEventListener("click", async () => {
  if (!samplingPager.hasNext) return;
  samplingPager.page += 1;
  await loadSampling();
});

document.getElementById("samplingSystemSelect").addEventListener("change", async () => {
  samplingPager.page = 1;
  await loadSampling();
});

document.getElementById("samplingRangeModeSelect").addEventListener("change", async () => {
  renderSamplingRangeInputContainer();
  samplingPager.page = 1;
  await loadSampling();
});

document.getElementById("samplingSmoothModeSelect").addEventListener("change", () => {
  if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
});
document.getElementById("samplingExportBtn").addEventListener("click", () => {
  void exportSamplingCsv();
});
document.getElementById("samplingImportBtn").addEventListener("click", () => {
  const fileInput = document.getElementById("samplingImportFileInput");
  if (!fileInput) return;
  if (typeof fileInput.showPicker === "function") {
    fileInput.showPicker();
    return;
  }
  fileInput.click();
});
document.getElementById("samplingImportFileInput").addEventListener("change", (event) => {
  const input = event.target;
  const file = input?.files?.[0];
  if (!file) return;
  void importSamplingCsv(file);
});

function bindClickIfPresent(id, handler) {
  const el = document.getElementById(id);
  if (el) el.addEventListener("click", handler);
}

function bindInputIfPresent(id, handler) {
  const el = document.getElementById(id);
  if (el) el.addEventListener("input", handler);
}

function bindChangeIfPresent(id, handler) {
  const el = document.getElementById(id);
  if (el) el.addEventListener("change", handler);
}

bindClickIfPresent("sajModeApplyBtn", () => {
  void applySajWorkingMode();
});
const sajMaskSaveBtn = document.getElementById("sajMaskSaveBtn");
if (sajMaskSaveBtn) {
  sajMaskSaveBtn.addEventListener("click", () => {
    void applySajEnableMasksOnly();
  });
}
bindClickIfPresent("sajToggleApplyBtn", () => {
  void applySajToggles();
});
bindClickIfPresent("sajLimitsApplyBtn", () => {
  void applySajLimits();
});
bindClickIfPresent("sajChargeApplyBtn", () => {
  void applySajSlot("charge");
});
bindClickIfPresent("sajDischargeApplyBtn", () => {
  void applySajSlot("discharge");
});
bindChangeIfPresent("sajChargeSlotInput", () => {
  const state = stateCache.lastSajControl?.control_state || stateCache.lastSajControl?.state;
  setSajControlInputsFromState(state);
});
bindChangeIfPresent("sajDischargeSlotInput", () => {
  const state = stateCache.lastSajControl?.control_state || stateCache.lastSajControl?.state;
  setSajControlInputsFromState(state);
});
bindInputIfPresent("sajChargeEnableMaskInput", () => {
  syncEnableCheckboxesFromInput("charge");
});
bindInputIfPresent("sajDischargeEnableMaskInput", () => {
  syncEnableCheckboxesFromInput("discharge");
});
const sajQuickChargeEnableMaskInput = document.getElementById("sajQuickChargeEnableMaskInput");
if (sajQuickChargeEnableMaskInput) {
  sajQuickChargeEnableMaskInput.addEventListener("input", () => {
    syncEnableCheckboxesFromInput("charge", true);
    mirrorQuickEnableMaskInputsToMain();
  });
}
const sajQuickDischargeEnableMaskInput = document.getElementById("sajQuickDischargeEnableMaskInput");
if (sajQuickDischargeEnableMaskInput) {
  sajQuickDischargeEnableMaskInput.addEventListener("input", () => {
    syncEnableCheckboxesFromInput("discharge", true);
    mirrorQuickEnableMaskInputsToMain();
  });
}
const sajControlSlotsBody = document.getElementById("sajControlSlotsBody");
if (sajControlSlotsBody) {
  sajControlSlotsBody.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.id.includes("DayMaskInput")) openSajDayMaskModalForInput(target.id);
  });
  sajControlSlotsBody.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.id.startsWith("sajTableChargeEnableSlot")) {
      syncEnableInputFromCheckboxes("charge", true);
      mirrorQuickEnableMaskInputsToMain();
      return;
    }
    if (target.id.startsWith("sajTableDischargeEnableSlot")) {
      syncEnableInputFromCheckboxes("discharge", true);
      mirrorQuickEnableMaskInputsToMain();
    }
  });
}

bindClickIfPresent("sajDayMaskCancelBtn", () => {
  cancelSajDayMaskModal();
});
bindClickIfPresent("sajDayMaskConfirmBtn", () => {
  confirmSajDayMaskModal();
});
bindChangeIfPresent("sajDayMaskAllDays", () => {
  const allDays = document.getElementById("sajDayMaskAllDays");
  _setSajDayMaskPopupFromMask(allDays?.checked ? 127 : 0);
});
for (const key of WEEKDAY_ORDER) {
  bindChangeIfPresent(`sajDayMask${key}`, () => {
    _syncSajDayMaskAllDaysCheckbox();
  });
}
for (let i = 1; i <= 7; i += 1) {
  const chargeEnableId = `sajChargeEnableSlot${i}`;
  const dischargeEnableId = `sajDischargeEnableSlot${i}`;
  bindChangeIfPresent(chargeEnableId, () => {
    syncEnableInputFromCheckboxes("charge");
  });
  bindChangeIfPresent(dischargeEnableId, () => {
    syncEnableInputFromCheckboxes("discharge");
  });
}
bindInputIfPresent("sajChargeDayMaskInput", () => {
  syncDayCheckboxesFromInput("charge");
});
bindInputIfPresent("sajDischargeDayMaskInput", () => {
  syncDayCheckboxesFromInput("discharge");
});
for (const key of WEEKDAY_ORDER) {
  const chargeDayId = `sajChargeDay${key}`;
  const dischargeDayId = `sajDischargeDay${key}`;
  bindChangeIfPresent(chargeDayId, () => {
    syncDayInputFromCheckboxes("charge");
  });
  bindChangeIfPresent(dischargeDayId, () => {
    syncDayInputFromCheckboxes("discharge");
  });
}

document.getElementById("configForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const saveMsg = document.getElementById("configSaveMsg");
  if (saveMsg) saveMsg.textContent = t("configSaving");
  try {
    const payload = buildConfigPayloadFromForm();
    const result = await fetchJson("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      timeoutMs: 10000,
    });
    fillConfigForm(result);
    configReady = true;
    setConfigModalVisible(false);
    if (saveMsg) saveMsg.textContent = t("configSaved");
    void loadCurrentTab();
  } catch (err) {
    configReady = false;
    if (saveMsg) saveMsg.textContent = t("configSaveFailed", { error: String(err) });
  }
});
document.getElementById("configCloseBtn").addEventListener("click", () => {
  const saveMsg = document.getElementById("configSaveMsg");
  if (!configReady) {
    if (saveMsg) saveMsg.textContent = t("configMustSaveFirst");
    return;
  }
  setConfigModalVisible(false);
  if (saveMsg) saveMsg.textContent = "-";
});

window.addEventListener("resize", () => {
  if (samplingChart) samplingChart.resize();
});

applyTranslations();
samplingRangeState.day = new Date().toISOString().slice(0, 10);
samplingRangeState.week = samplingRangeState.day;
samplingRangeState.monthYear = new Date().getUTCFullYear();
samplingRangeState.month = String(new Date().getUTCMonth() + 1);
samplingRangeState.endDate = samplingRangeState.day;
samplingRangeState.startDate = getUtcDateText(-1);
{
  const now = new Date();
  now.setUTCMinutes(0, 0, 0);
  samplingRangeState.endDateTime = now.toISOString().slice(0, 16);
  samplingRangeState.startDateTime = new Date(now.getTime() - 6 * 3600 * 1000).toISOString().slice(0, 16);
}
renderSamplingRangeInputContainer();
setActiveTab(currentTab, false);
setAutoRefresh(autoRefreshSeconds);
void ensureConfigReady().then((ready) => {
  if (ready) void loadCurrentTab();
});
