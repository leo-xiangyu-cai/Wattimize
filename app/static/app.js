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
    configHaUrlPlaceholder: "http://<home-assistant-host>:8123",
    configHaTokenPlaceholder: "Long-lived access token",
    configDongleHostPlaceholder: "<solplanet-dongle-host>",
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
    configHaUrlPlaceholder: "http://<home-assistant-host>:8123",
    configHaTokenPlaceholder: "Long-lived access token",
    configDongleHostPlaceholder: "<solplanet-dongle-host>",
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
const SAJ_RAW_APIS = [
  { key: "saj_dashboard_sources", titleKey: "rawApiSajDashboardSources", url: "/api/saj/raw/dashboard-sources" },
  { key: "saj_core_entities", titleKey: "rawApiSajCoreEntities", url: "/api/saj/raw/core-entities" },
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
let currentTab = ["dashboard", "entities", "solplanetRaw", "sajRaw"].includes(localStorage.getItem("activeTab"))
  ? localStorage.getItem("activeTab")
  : "dashboard";
let autoRefreshTimerId = null;
let isLoadingCurrentTab = false;
let autoRefreshSeconds = getAutoRefreshSeconds();
let summaryRequestId = 0;
let configReady = false;

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
  renderSajRawFromCache();
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
}

function buildConfigPayloadFromForm() {
  return {
    ha_url: document.getElementById("cfgHaUrl").value.trim(),
    ha_token: document.getElementById("cfgHaToken").value.trim(),
    solplanet_dongle_host: document.getElementById("cfgDongleHost").value.trim(),
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
  if (Array.isArray(value)) return `[${value.length} items]`;
  if (typeof value === "object") return "{...}";
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
      const response = await fetchJson(api.url, { timeoutMs: 20000 });
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
  currentTab = tab === "entities" || tab === "solplanetRaw" || tab === "sajRaw" ? tab : "dashboard";
  localStorage.setItem("activeTab", currentTab);

  const dashboardView = document.getElementById("dashboardView");
  const solplanetRawView = document.getElementById("solplanetRawView");
  const sajRawView = document.getElementById("sajRawView");
  const entitiesView = document.getElementById("entitiesView");
  const tabDashboard = document.getElementById("tabDashboard");
  const tabSolplanetRaw = document.getElementById("tabSolplanetRaw");
  const tabSajRaw = document.getElementById("tabSajRaw");
  const tabEntities = document.getElementById("tabEntities");

  const dashboardActive = currentTab === "dashboard";
  const solplanetRawActive = currentTab === "solplanetRaw";
  const sajRawActive = currentTab === "sajRaw";
  const anyRawActive = solplanetRawActive || sajRawActive;
  dashboardView.classList.toggle("hidden", !dashboardActive);
  solplanetRawView.classList.toggle("hidden", !solplanetRawActive);
  sajRawView.classList.toggle("hidden", !sajRawActive);
  entitiesView.classList.toggle("hidden", dashboardActive || anyRawActive);
  tabDashboard.classList.toggle("active", dashboardActive);
  tabSolplanetRaw.classList.toggle("active", solplanetRawActive);
  tabSajRaw.classList.toggle("active", sajRawActive);
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

applyTranslations();
setActiveTab(currentTab, false);
setAutoRefresh(autoRefreshSeconds);
void ensureConfigReady().then((ready) => {
  if (ready) void loadCurrentTab();
});
