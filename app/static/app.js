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
    configInverterSnLabel: "Solplanet Inverter SN",
    configBatterySnLabel: "Solplanet Battery SN",
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
    combinedDebugTitle: "Combined Debug",
    combinedDebugMeta: "Source {source} · storage_backed {storageBacked} · stale {stale} · sample age {sampleAge}s · kv items {kvCount}",
    combinedCollectorMeta: "Collector: SAJ {saj} · Solplanet {solplanet} · Combined {combined}",
    solplanetRawTab: "Solplanet Raw",
    sajRawTab: "SAJ Raw",
    sajControlTab: "SAJ Control",
    solplanetControlTab: "Solplanet Control",
    entitiesTab: "Entities",
    samplingTab: "Sampling",
    workerLogsTab: "Worker Logs",
    failureLogTab: "Failure Logs",
    sajControlTitle: "SAJ Control",
    solplanetControlTitle: "Solplanet Control",
    solplanetControlLimitsTitle: "Power Limits",
    solplanetControlScheduleTitle: "Day Schedule (6 slots)",
    solplanetControlDayLabel: "Day",
    solplanetControlPinLabel: "Charge Limit Pin (W)",
    solplanetControlPoutLabel: "Discharge Limit Pout (W)",
    solplanetControlHourLabel: "Hour",
    solplanetControlMinuteLabel: "Minute",
    solplanetControlPowerByteLabel: "Power Byte",
    solplanetControlModeByteLabel: "Mode Byte",
    solplanetControlEncodedLabel: "Encoded",
    solplanetControlLoadFailed: "Control state load failed: {error}",
    solplanetControlApplyFailed: "Control apply failed: {error}",
    solplanetControlSaveDone: "Schedule saved.",
    solplanetControlSignalsTitle: "Mode/State Signals (Live)",
    solplanetControlSignalSourceCol: "Source",
    solplanetControlSignalKeyCol: "Key",
    solplanetControlSignalValueCol: "Value",
    solplanetControlModeMeta: "stu={stu}, meter.mod={meterMod}, meter.enb={meterEnb}, battery.cst={batteryCst}, battery.bst={batteryBst}",
    meterOfflineBannerText: "Smart meter offline (flg=0) — grid & load values are estimates only",
    solplanetControlRawSettingTitle: "Raw setting.cgi payload",
    solplanetControlRawSettingExplain: "Use this for advanced keys not yet exposed in the UI.",
    solplanetControlRawSettingApplyBtn: "Apply Raw Payload",
    solplanetControlRestartApiBtn: "Force Restart Backend API",
    solplanetControlRawSettingInvalidJson: "Invalid JSON payload: {error}",
    solplanetControlPopupSuccessTitle: "Success",
    solplanetControlPopupLimitsSummary: "Power limits have been applied.",
    solplanetControlPopupScheduleSummary: "Day schedule has been saved for {day}.",
    solplanetControlPopupRawSummary: "Raw payload has been sent.",
    solplanetControlPopupRestartSummary: "Backend API loop has been force restarted.",
    solplanetControlPopupResultTitle: "Result JSON",
    solplanetControlFetchMeta: "Last fetch: {time} · state API {stateMs} ms · live API {liveMs} ms · total {totalMs} ms",
    sajControlModeTitle: "Working Mode",
    sajControlModeExplain: "Mode code mapping may differ by firmware. Test 0~8 against app labels.",
    sajControlModeReadback: "mode_input={modeInput}, mode_sensor={modeSensor}, inverter_mode={inverterMode}",
    sajControlModeSignals: "app_mode(input/actual): {modeInput} / {modeSensor}, inverter_working_mode(actual): {inverterMode}",
    sajControlModeInputHint: "(input={modeInput})",
    sajControlModeCodeLabel: "Mode Code",
    sajControlInverterModeCodeLabel: "Inverter Working Mode (Target Code)",
    sajControlInverterModeHint: "Inverter working mode is readback-only in HA entities. This control writes app_mode_input and then verifies inverter_working_mode sensor.",
    sajModeOption0: "0 - Self-Consumption Mode",
    sajModeOption1: "1 - Time of Use Mode",
    sajModeOption2: "2 - Backup Mode",
    sajModeOption3: "3 - Passive Mode",
    sajModeOption4: "4 - Reserved/Unknown",
    sajModeOption5: "5 - Reserved/Unknown",
    sajModeOption6: "6 - Reserved/Unknown",
    sajModeOption7: "7 - Reserved/Unknown",
    sajModeOption8: "8 - Micro Grid Mode",
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
    sajControlInputLabel: "Input",
    sajControlActualLabel: "Actual",
    sajControlEditStartPrompt: "Edit start time (HH:MM)",
    sajControlEditEndPrompt: "Edit end time (HH:MM)",
    sajControlEditPowerPrompt: "Edit power percent (0-100)",
    sajControlEditMaskPrompt: "Edit day mask (0-127)",
    sajControlEditInvalidTime: "Invalid time, expected HH:MM.",
    sajControlEditInvalidPower: "Invalid power, expected number 0-100.",
    sajControlEditInvalidMask: "Invalid mask, expected integer 0-127.",
    sajControlApplyBtn: "Apply",
    sajControlLoadFailed: "Control state load failed: {error}",
    sajControlApplyFailed: "Control apply failed: {error}",
    sajControlApplyDone: "Applied",
    sajControlPassiveAlert: "Warning: SAJ is in Passive Mode (sensor app_mode={modeSensor}). Slot schedules may be ignored and battery may charge/discharge unexpectedly.",
    sajControlDebugModeLabel: "Debug Mode",
    sajControlPopupSuccessTitle: "Success",
    sajControlPopupCloseBtn: "Close",
    sajControlPopupWorkingModeSummary: "Working mode has been applied (mode_code={modeCode}).",
    sajControlPopupInverterModeSummary: "Inverter target mode apply sent (mode_code={modeCode}, via app_mode_input).",
    sajControlPopupSaveSummary: "Save completed for enable masks and slot edits.",
    sajControlDebugApiTitle: "API Calls",
    sajControlDebugNoApi: "No API calls recorded.",
    sajDebugPurposeSetMode: "Update SAJ app mode input with selected mode code.",
    sajDebugPurposeSetInverterModeTarget: "Apply target inverter mode by writing SAJ app mode input.",
    sajDebugPurposeSetTogglesMask: "Update charge/discharge enable masks.",
    sajDebugPurposeSetSlot: "Update {kind} slot {slot} fields that changed.",
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
    integratedFlowTitle: "Dashboard",
    integratedFlowSubtitle: "SAJ solar/grid + SAJ/Solplanet batteries, dual inverters in parallel",
    solarTitle: "Solar",
    gridTitle: "Grid",
    inverterTitle: "Inverter",
    switchboardTitle: "Switchboard",
    inverter1Title: "Inverter 1",
    inverter2Title: "Inverter 2",
    inverterConversionUnavailable: "-",
    inverterConversionMixed: "Mixed",
    dataKindReal: "Real data from the system",
    dataKindEstimate: "Estimated data",
    dataKindCalculated: "Calculated from other readings",
    loadTitle: "Home Load",
    teslaChargingLabel: "Tesla",
    teslaChargingIncludedHint: "Total Load = Home Load + Tesla",
    teslaChargingPowerLabel: "Power",
    teslaChargingCurrentLabel: "Current",
    teslaChargingVoltageLabel: "Voltage",
    teslaCableStatusConnected: "Charger plugged",
    teslaCableStatusDisconnected: "Charger unplugged",
    teslaConnectionStateUnplugged: "Charger unplugged",
    teslaConnectionStatePluggedNotCharging: "Charger plugged",
    teslaConnectionStateCharging: "Charging",
    teslaConnectionStateUnknown: "Connection unknown",
    teslaControlStateSwitch: "Control: switch",
    teslaControlStateButtons: "Control: buttons",
    teslaControlStateUnavailable: "Control: unavailable",
    teslaControlStart: "Start Charging",
    teslaControlStop: "Stop Charging",
    teslaControlBusy: "Updating...",
    teslaControlUnavailable: "Control Unavailable",
    teslaControlApplyFailed: "Tesla charging control failed: {error}",
    batteryTitle: "Battery",
    battery1Title: "Battery 1",
    battery2Title: "Battery 2",
    batteryUsableRemaining: "Usable {value} kWh",
    batteryRuntimeEstimateCharging: "Full {hours}h · {time}",
    batteryRuntimeEstimateDischarging: "Empty {hours}h · {time}",
    switchboardStateActive: "Bus Active",
    switchboardStateIdle: "Bus Idle",
    socLabel: "SOC",
    entityExplorerTitle: "Entity Explorer",
    samplingTitle: "Sampling Records",
    samplingSystemLabel: "System",
    samplingSystemOverall: "Overall",
    samplingRangeModeLabel: "Range",
    samplingRangeDay: "Day",
    samplingRangeWeek: "Week",
    samplingRangeMonth: "Month",
    samplingRangeRelative: "Relative",
    samplingRangeCustomDate: "Custom Date",
    samplingRangeCustomDateTime: "Custom Date + Time",
    samplingWeekDisplay: "Week {week} ({start} ~ {end})",
    samplingMonthYear: "{year}",
    samplingDayLabel: "Day",
    samplingWeekLabel: "Week",
    samplingMonthLabel: "Month",
    samplingRelativeLabel: "Relative Range",
    samplingRelativePlaceholder: "-3h",
    samplingRelativeHelp: "Use values like -15m, -3h, -2d, -1mo. Range is from now backwards.",
    samplingRelativePresetMinute: "-1m",
    samplingRelativePresetHour: "-1h",
    samplingRelativePresetDay: "-1d",
    samplingRelativePresetMonth: "-1mo",
    samplingRelativeInvalid: "Invalid relative range. Use values like -3h, -15m, -2d, or -1mo.",
    samplingStartLabel: "Start",
    samplingEndLabel: "End",
    samplingStorageMeta: "DB {sizeMb} MB · Rows {rows} · Interval {interval}s · Estimated/day {estMb} MB",
    samplingUsageMeta: "Range usage ({system}, {range}): Load {load} kWh · PV {pv} kWh · Grid import {gridImport} kWh · Grid export {gridExport} kWh",
    samplingUsageMetaNoData: "Range usage ({system}, {range}): not enough samples yet",
    samplingChartTitle: "Power Trend",
    samplingChartMeta: "{system} · {range} · {count} points",
    samplingChartNoData: "No chart data yet for this range",
    samplingTotalsTitle: "Trend Totals",
    samplingTotalsMeta: "{system} · {range} · directional energy totals",
    samplingTotalsMetaNoData: "{system} · {range} · not enough samples for totals yet",
    samplingTotalsScope: "Scope",
    samplingTotalsMetric: "Metric",
    samplingTotalsValue: "Value (kWh)",
    samplingTotalsScopeOverall: "Overall",
    samplingTotalsScopeSystem: "{system}",
    samplingTotalsScopeSajBattery: "SAJ Battery",
    samplingTotalsScopeSolplanetBattery: "Solplanet Battery",
    samplingTotalsGridTitle: "Grid",
    samplingTotalsBatteryTitle: "Battery",
    samplingTotalsNoData: "No totals available for this range",
    samplingOverallMetricPv: "PV Generation",
    samplingOverallMetricGridImport: "Grid Import",
    samplingOverallMetricGridExport: "Grid Export",
    samplingOverallMetricBatteryCharge: "Battery Charge",
    samplingOverallMetricBatteryDischarge: "Battery Discharge",
    samplingOverallSeriesSajBatteryCharge: "SAJ Battery Charge",
    samplingOverallSeriesSajBatteryDischarge: "SAJ Battery Discharge",
    samplingOverallSeriesSolplanetBatteryCharge: "Solplanet Battery Charge",
    samplingOverallSeriesSolplanetBatteryDischarge: "Solplanet Battery Discharge",
    samplingSmoothLabel: "Smoothing",
    samplingSmoothModeDetail: "Detail",
    samplingSmoothModeSmooth: "Smooth",
    samplingSeriesPv: "PV",
    samplingSeriesGrid: "Grid",
    samplingSeriesBattery: "Battery",
    samplingSeriesLoad: "Load",
    samplingTableTime: "Sampled At",
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
    workerLogsTitle: "Worker API Logs",
    workerLogsCategoryLabel: "Category",
    workerLogsCategoryAll: "All",
    workerLogsCategorySaj: "SAJ",
    workerLogsCategorySolplanet: "SoulPlanet",
    workerLogsCategoryCombined: "Combined",
    workerLogsCategoryTesla: "Tesla",
    workerLogsSystemLabel: "System",
    workerLogsSystemAll: "All",
    workerLogsServiceLabel: "Service",
    workerLogsServiceAll: "All",
    workerLogsConfigMeta: "Solplanet config: host {host}",
    workerLogsTableTime: "Time (UTC)",
    workerLogsTableRound: "Run ID",
    workerLogsTableSystem: "System",
    workerLogsTableService: "Service",
    workerLogsTableMethod: "Method",
    workerLogsTableLink: "API Link",
    workerLogsTableStatus: "Status",
    workerLogsTableDuration: "Duration(ms)",
    workerLogsTableResult: "Result",
    workerLogsTotal: "Total {total} logs",
    workerLogsPageInfo: "Page {page}/{totalPages} (showing {count})",
    workerLogsStatusOk: "OK",
    workerLogsStatusFailed: "Failed",
    workerLogsStatusPending: "Pending",
    workerLogsStatusSkipped: "Skipped",
    workerLogsStatusApplied: "Applied",
    workerLogsStatusNoop: "Noop",
    workerLogsStatusTimeout: "Timeout",
    workerLogDetailTitle: "Worker Log Detail",
    workerLogDetailMeta: "{service} · {status} · {time}",
    failureLogTitle: "Worker Failure Logs",
    failureLogMeta: "File {path} · lines {fromLine}-{toLine} / {total}",
    failureLogLoadMore: "Load More",
    failureLogShowing: "Showing {count}/{total} lines",
    failureLogEmpty: "No failure logs yet.",
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
    batteryRuntimeDischarging: "Est. empty {time}",
    batteryRuntimeCharging: "Est. full {time}",
    batteryRuntimeIdle: "No discharge at the moment",
    batteryRuntimeNoData: "Runtime unavailable",
    balanceLabel: "Balance",
    balanceStatusTitle: "Dynamic Balance Status",
    balanceStatusBalanced: "Cleared",
    balanceStatusUnbalanced: "Not Cleared",
    balanceStatusNoData: "Not enough data",
    balanceResidualLabel: "Net {value}",
    teslaChargingFormulaNote: "Total load {total} = Home load {home} + Tesla charging {tesla}",
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
    solplanetRawModeCards: "API Cards",
    solplanetRawModeTable: "DB Table",
    sajRawTitle: "SAJ Raw (Dynamic + Static)",
    solplanetRawMeta: "Fetch {ms} ms · inverter {inverter} · battery {battery}",
    solplanetRawMetaDash: "Fetch - ms · inverter - · battery -",
    endpointOk: "OK",
    endpointError: "Error",
    endpointPath: "Path",
    endpointUrl: "URL",
    rawStatusLabel: "Status",
    rawStatusSuccess: "Success",
    rawStatusFailed: "Failed",
    rawStatusStale: "Stale",
    rawLastRequest: "Last request",
    rawLastSuccess: "Last success",
    rawLatency: "Latency",
    rawAgoJustNow: "just now",
    rawAgoSeconds: "{n}s ago",
    rawAgoMinutes: "{n}m ago",
    rawAgoHours: "{n}h ago",
    rawAgoDays: "{n}d ago",
    rawLoadFailed: "Raw load failed: {error}",
    rawLoading: "Loading",
    rawDone: "Loaded",
    rawSummary: "Updated {updated} · OK {ok}/{total} · Failed {failed}",
    rawSummaryDash: "Updated - · OK -/- · Failed -",
    rawApiGetdev0: "Dongle Info",
    rawApiGetdev2: "Device 2 Info",
    rawApiGetdev3: "Device 3 Info",
    rawApiGetdev4: "Device 4 Info",
    rawApiGetdevdata2: "Device 2 Data",
    rawApiGetdevdata3: "Device 3 Data",
    rawApiGetdevdata4: "Device 4 Data",
    rawApiGetdevdata5: "Device 5 Data",
    rawApiGetdefine: "Schedule",
    rawApiSajDashboardSources: "Live Dashboard Sources (Dynamic)",
    rawApiSajCoreEntities: "Configured Entity List (Static)",
    rawViewExplain: "Notes",
    rawViewJson: "JSON",
    rawKvAttr: "Attribute",
    rawKvValue: "Value",
    rawKvSource: "Source",
    rawKvMeta: "Rows {count}",
    rawKvMetaDash: "Rows -",
    rawKvEmpty: "No data yet",
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
    configInverterSnLabel: "Solplanet Inverter SN",
    configBatterySnLabel: "Solplanet Battery SN",
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
    combinedDebugTitle: "整合数据调试",
    combinedDebugMeta: "来源 {source} · storage_backed {storageBacked} · stale {stale} · 样本年龄 {sampleAge}s · KV 条数 {kvCount}",
    combinedCollectorMeta: "采集器: SAJ {saj} · Solplanet {solplanet} · Combined {combined}",
    solplanetRawTab: "Solplanet 原始",
    sajRawTab: "SAJ 原始",
    sajControlTab: "SAJ 管理",
    solplanetControlTab: "Solplanet 管理",
    entitiesTab: "实体",
    samplingTab: "采样",
    workerLogsTab: "Worker日志",
    failureLogTab: "失败日志",
    sajControlTitle: "SAJ 管理",
    solplanetControlTitle: "Solplanet 管理",
    solplanetControlLimitsTitle: "功率限制",
    solplanetControlScheduleTitle: "日程配置（6 槽位）",
    solplanetControlDayLabel: "星期",
    solplanetControlPinLabel: "充电上限 Pin (W)",
    solplanetControlPoutLabel: "放电上限 Pout (W)",
    solplanetControlHourLabel: "小时",
    solplanetControlMinuteLabel: "分钟",
    solplanetControlPowerByteLabel: "功率字节",
    solplanetControlModeByteLabel: "模式字节",
    solplanetControlEncodedLabel: "编码值",
    solplanetControlLoadFailed: "控制状态加载失败: {error}",
    solplanetControlApplyFailed: "控制下发失败: {error}",
    solplanetControlSaveDone: "日程保存成功。",
    solplanetControlSignalsTitle: "Mode/状态信号（实时）",
    solplanetControlSignalSourceCol: "来源",
    solplanetControlSignalKeyCol: "字段",
    solplanetControlSignalValueCol: "值",
    solplanetControlModeMeta: "stu={stu}, meter.mod={meterMod}, meter.enb={meterEnb}, battery.cst={batteryCst}, battery.bst={batteryBst}",
    meterOfflineBannerText: "智能电表离线（flg=0）— 电网与负载数值为估算值",
    solplanetControlRawSettingTitle: "Raw setting.cgi 负载",
    solplanetControlRawSettingExplain: "用于发送当前界面未覆盖的高级字段。",
    solplanetControlRawSettingApplyBtn: "应用 Raw 负载",
    solplanetControlRestartApiBtn: "强制重启后台 API",
    solplanetControlRawSettingInvalidJson: "JSON 格式错误: {error}",
    solplanetControlPopupSuccessTitle: "操作成功",
    solplanetControlPopupLimitsSummary: "功率限制已应用。",
    solplanetControlPopupScheduleSummary: "已保存 {day} 的日程配置。",
    solplanetControlPopupRawSummary: "Raw 负载已发送。",
    solplanetControlPopupRestartSummary: "后台 API 循环已强制重启。",
    solplanetControlPopupResultTitle: "返回结果 JSON",
    solplanetControlFetchMeta: "最近拉取: {time} · 状态接口 {stateMs} ms · 实时接口 {liveMs} ms · 总耗时 {totalMs} ms",
    sajControlModeTitle: "工作模式",
    sajControlModeExplain: "mode code 与名称可能因固件不同，请按 APP 对照测试 0~8。",
    sajControlModeReadback: "mode_input={modeInput}, mode_sensor={modeSensor}, inverter_mode={inverterMode}",
    sajControlModeSignals: "app_mode(输入/实际): {modeInput} / {modeSensor}, inverter_working_mode(实际): {inverterMode}",
    sajControlModeInputHint: "（input={modeInput}）",
    sajControlModeCodeLabel: "模式编码",
    sajControlInverterModeCodeLabel: "逆变器工作模式（目标编码）",
    sajControlInverterModeHint: "HA 实体里 inverter working mode 是只读回传。这里会写入 app_mode_input，再观察 inverter_working_mode 传感器。",
    sajModeOption0: "0 - 自发自用模式",
    sajModeOption1: "1 - 分时电价模式",
    sajModeOption2: "2 - 备电模式",
    sajModeOption3: "3 - 被动模式",
    sajModeOption4: "4 - 预留/未知",
    sajModeOption5: "5 - 预留/未知",
    sajModeOption6: "6 - 预留/未知",
    sajModeOption7: "7 - 预留/未知",
    sajModeOption8: "8 - 微电网模式",
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
    sajControlInputLabel: "Input",
    sajControlActualLabel: "Actual",
    sajControlEditStartPrompt: "编辑开始时间（HH:MM）",
    sajControlEditEndPrompt: "编辑结束时间（HH:MM）",
    sajControlEditPowerPrompt: "编辑功率百分比（0-100）",
    sajControlEditMaskPrompt: "编辑星期掩码（0-127）",
    sajControlEditInvalidTime: "时间格式错误，应为 HH:MM。",
    sajControlEditInvalidPower: "功率输入错误，应为 0-100。",
    sajControlEditInvalidMask: "掩码输入错误，应为 0-127 的整数。",
    sajControlApplyBtn: "应用",
    sajControlLoadFailed: "管理状态加载失败：{error}",
    sajControlApplyFailed: "应用失败：{error}",
    sajControlApplyDone: "已应用",
    sajControlPassiveAlert: "警告：当前 SAJ 处于 Passive Mode（sensor app_mode={modeSensor}）。时段配置可能被忽略，电池可能出现非预期充/放电。",
    sajControlDebugModeLabel: "Debug 模式",
    sajControlPopupSuccessTitle: "操作成功",
    sajControlPopupCloseBtn: "关闭",
    sajControlPopupWorkingModeSummary: "工作模式已应用（mode_code={modeCode}）。",
    sajControlPopupInverterModeSummary: "已发送逆变器目标模式下发（mode_code={modeCode}，通过 app_mode_input）。",
    sajControlPopupSaveSummary: "已保存启用掩码和时段修改。",
    sajControlDebugApiTitle: "API 调用明细",
    sajControlDebugNoApi: "本次没有记录到 API 调用。",
    sajDebugPurposeSetMode: "将所选 mode code 写入 SAJ App 模式输入值。",
    sajDebugPurposeSetInverterModeTarget: "通过写入 SAJ App 模式输入值来触发逆变器目标模式。",
    sajDebugPurposeSetTogglesMask: "更新充/放电启用掩码。",
    sajDebugPurposeSetSlot: "更新 {kind} 第 {slot} 段发生变化的字段。",
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
    integratedFlowTitle: "Dashboard",
    integratedFlowSubtitle: "SAJ 的 solar/grid + SAJ/Solplanet 电池，双逆变器并联",
    solarTitle: "太阳能",
    gridTitle: "电网",
    inverterTitle: "逆变器",
    switchboardTitle: "母线配电盘",
    inverter1Title: "逆变器 1",
    inverter2Title: "逆变器 2",
    inverterConversionUnavailable: "-",
    inverterConversionMixed: "混合",
    dataKindReal: "系统实时读取的真实数据",
    dataKindEstimate: "估算数据",
    dataKindCalculated: "基于其他读数计算得到的数据",
    loadTitle: "家庭负载",
    teslaChargingLabel: "特斯拉",
    teslaChargingIncludedHint: "总负载 = 家庭负载 + 特斯拉",
    teslaChargingPowerLabel: "功率",
    teslaChargingCurrentLabel: "电流",
    teslaChargingVoltageLabel: "电压",
    teslaCableStatusConnected: "已连接",
    teslaCableStatusDisconnected: "未连接",
    teslaConnectionStateUnplugged: "未连接",
    teslaConnectionStatePluggedNotCharging: "已连未充",
    teslaConnectionStateCharging: "已连充电",
    teslaConnectionStateUnknown: "连接状态未知",
    teslaControlStateSwitch: "控制方式：开关",
    teslaControlStateButtons: "控制方式：按钮",
    teslaControlStateUnavailable: "控制不可用",
    teslaControlStart: "开始充电",
    teslaControlStop: "停止充电",
    teslaControlBusy: "正在更新...",
    teslaControlUnavailable: "控制不可用",
    teslaControlApplyFailed: "特斯拉充电控制失败：{error}",
    batteryTitle: "电池",
    battery1Title: "电池 1",
    battery2Title: "电池 2",
    batteryUsableRemaining: "可用余量 {value} kWh",
    batteryRuntimeEstimateCharging: "按当前功率约 {hours} 小时充满 · 到 {time}",
    batteryRuntimeEstimateDischarging: "按当前功率约 {hours} 小时耗空 · 到 {time}",
    switchboardStateActive: "母线工作中",
    switchboardStateIdle: "母线空闲",
    socLabel: "电池电量",
    entityExplorerTitle: "实体浏览",
    samplingTitle: "采样记录",
    samplingSystemLabel: "系统",
    samplingSystemOverall: "综合",
    samplingRangeModeLabel: "范围",
    samplingRangeDay: "天",
    samplingRangeWeek: "周",
    samplingRangeMonth: "月",
    samplingRangeRelative: "相对时间",
    samplingRangeCustomDate: "自定义日期",
    samplingRangeCustomDateTime: "自定义日期+时间",
    samplingWeekDisplay: "第{week}周（{start} ~ {end}）",
    samplingMonthYear: "{year}",
    samplingDayLabel: "日期",
    samplingWeekLabel: "周",
    samplingMonthLabel: "月份",
    samplingRelativeLabel: "相对范围",
    samplingRelativePlaceholder: "-3h",
    samplingRelativeHelp: "支持 -15m、-3h、-2d、-1mo 这种写法，表示从当前时间往前回溯。",
    samplingRelativePresetMinute: "-1m",
    samplingRelativePresetHour: "-1h",
    samplingRelativePresetDay: "-1d",
    samplingRelativePresetMonth: "-1mo",
    samplingRelativeInvalid: "相对时间格式无效，请使用 -3h、-15m、-2d 或 -1mo。",
    samplingStartLabel: "开始时间",
    samplingEndLabel: "结束时间",
    samplingStorageMeta: "数据库 {sizeMb} MB · 记录 {rows} 条 · 采样间隔 {interval}s · 预计每天 {estMb} MB",
    samplingUsageMeta: "区间统计 ({system}, {range}): 负载 {load} kWh · 光伏 {pv} kWh · 电网购电 {gridImport} kWh · 电网上网 {gridExport} kWh",
    samplingUsageMetaNoData: "区间统计 ({system}, {range}): 当前样本不足",
    samplingChartTitle: "功率趋势图",
    samplingChartMeta: "{system} · {range} · {count} 个点",
    samplingChartNoData: "当前时间范围暂无图表数据",
    samplingTotalsTitle: "趋势累计值",
    samplingTotalsMeta: "{system} · {range} · 分方向累计电量",
    samplingTotalsMetaNoData: "{system} · {range} · 当前样本不足，无法统计累计值",
    samplingTotalsScope: "范围",
    samplingTotalsMetric: "指标",
    samplingTotalsValue: "数值 (kWh)",
    samplingTotalsScopeOverall: "综合",
    samplingTotalsScopeSystem: "{system}",
    samplingTotalsScopeSajBattery: "SAJ 电池",
    samplingTotalsScopeSolplanetBattery: "Solplanet 电池",
    samplingTotalsGridTitle: "电网",
    samplingTotalsBatteryTitle: "电池",
    samplingTotalsNoData: "当前区间暂无可显示的累计值",
    samplingOverallMetricPv: "光伏发电",
    samplingOverallMetricGridImport: "电网购电",
    samplingOverallMetricGridExport: "电网上网",
    samplingOverallMetricBatteryCharge: "电池充电",
    samplingOverallMetricBatteryDischarge: "电池放电",
    samplingOverallSeriesSajBatteryCharge: "SAJ 电池充电",
    samplingOverallSeriesSajBatteryDischarge: "SAJ 电池放电",
    samplingOverallSeriesSolplanetBatteryCharge: "Solplanet 电池充电",
    samplingOverallSeriesSolplanetBatteryDischarge: "Solplanet 电池放电",
    samplingSmoothLabel: "平滑度",
    samplingSmoothModeDetail: "细节优先",
    samplingSmoothModeSmooth: "平滑",
    samplingSeriesPv: "光伏",
    samplingSeriesGrid: "电网",
    samplingSeriesBattery: "电池",
    samplingSeriesLoad: "负载",
    samplingTableTime: "采样时间",
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
    workerLogsTitle: "Worker API 日志",
    workerLogsCategoryLabel: "分类",
    workerLogsCategoryAll: "全部",
    workerLogsCategorySaj: "SAJ",
    workerLogsCategorySolplanet: "SoulPlanet",
    workerLogsCategoryCombined: "整合",
    workerLogsCategoryTesla: "特斯拉",
    workerLogsSystemLabel: "系统",
    workerLogsSystemAll: "全部",
    workerLogsServiceLabel: "服务",
    workerLogsServiceAll: "全部",
    workerLogsConfigMeta: "Solplanet 配置：host {host}",
    workerLogsTableTime: "时间 (UTC)",
    workerLogsTableRound: "运行ID",
    workerLogsTableSystem: "系统",
    workerLogsTableService: "服务",
    workerLogsTableMethod: "方法",
    workerLogsTableLink: "API 链接",
    workerLogsTableStatus: "状态",
    workerLogsTableDuration: "耗时(ms)",
    workerLogsTableResult: "结果",
    workerLogsTotal: "共 {total} 条日志",
    workerLogsPageInfo: "第 {page}/{totalPages} 页（当前 {count} 条）",
    workerLogsStatusOk: "成功",
    workerLogsStatusFailed: "失败",
    workerLogsStatusPending: "等待中",
    workerLogsStatusSkipped: "跳过",
    workerLogsStatusApplied: "已执行",
    workerLogsStatusNoop: "无操作",
    workerLogsStatusTimeout: "超时",
    workerLogDetailTitle: "Worker 日志详情",
    workerLogDetailMeta: "{service} · {status} · {time}",
    failureLogTitle: "Worker 失败日志",
    failureLogMeta: "文件 {path} · 第 {fromLine}-{toLine} 行 / 共 {total} 行",
    failureLogLoadMore: "加载更多",
    failureLogShowing: "已显示 {count}/{total} 行",
    failureLogEmpty: "暂时没有失败日志。",
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
    batteryRuntimeDischarging: "预计 {time} 耗空",
    batteryRuntimeCharging: "预计 {time} 充满",
    batteryRuntimeIdle: "当前未在放电",
    batteryRuntimeNoData: "暂时无法估算",
    balanceLabel: "功率平衡",
    balanceStatusTitle: "动态平衡状态",
    balanceStatusBalanced: "已清零",
    balanceStatusUnbalanced: "未清零",
    balanceStatusNoData: "数据不足",
    balanceResidualLabel: "净值 {value}",
    teslaChargingFormulaNote: "总负载 {total} = 家庭负载 {home} + 特斯拉充电 {tesla}",
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
    solplanetRawModeCards: "接口卡片",
    solplanetRawModeTable: "数据库表",
    sajRawTitle: "SAJ 原始数据（动态 + 静态）",
    solplanetRawMeta: "耗时 {ms} ms · 逆变器 {inverter} · 电池 {battery}",
    solplanetRawMetaDash: "耗时 - ms · 逆变器 - · 电池 -",
    endpointOk: "成功",
    endpointError: "错误",
    endpointPath: "路径",
    endpointUrl: "完整地址",
    rawStatusLabel: "状态",
    rawStatusSuccess: "成功",
    rawStatusFailed: "失败",
    rawStatusStale: "已过期",
    rawLastRequest: "上次请求",
    rawLastSuccess: "最后成功",
    rawLatency: "耗时",
    rawAgoJustNow: "刚刚",
    rawAgoSeconds: "{n}秒前",
    rawAgoMinutes: "{n}分钟前",
    rawAgoHours: "{n}小时前",
    rawAgoDays: "{n}天前",
    rawLoadFailed: "原始数据加载失败：{error}",
    rawLoading: "加载中",
    rawDone: "已加载",
    rawSummary: "更新时间 {updated} · 成功 {ok}/{total} · 失败 {failed}",
    rawSummaryDash: "更新时间 - · 成功 -/- · 失败 -",
    rawApiGetdev0: "Dongle 信息",
    rawApiGetdev2: "设备2信息",
    rawApiGetdev3: "设备3信息",
    rawApiGetdev4: "设备4信息",
    rawApiGetdevdata2: "设备2实时",
    rawApiGetdevdata3: "设备3实时",
    rawApiGetdevdata4: "设备4实时",
    rawApiGetdevdata5: "设备5实时",
    rawApiGetdefine: "调度配置",
    rawApiSajDashboardSources: "实时展示来源（动态）",
    rawApiSajCoreEntities: "配置实体清单（静态）",
    rawViewExplain: "说明",
    rawViewJson: "JSON",
    rawKvAttr: "属性",
    rawKvValue: "值",
    rawKvSource: "来源",
    rawKvMeta: "共 {count} 行",
    rawKvMetaDash: "共 - 行",
    rawKvEmpty: "暂无数据",
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
const workerLogsPager = {
  page: 1,
  hasNext: false,
  hasPrev: false,
};
const workerFailureLogState = {
  before: 0,
  hasMore: false,
};
let workerLogsDefaultsApplied = false;
const PAGE_SIZE = 80;
const SAMPLING_PAGE_SIZE = 100;
const WORKER_FAILURE_LOG_PAGE_SIZE = 100;
const AUTO_REFRESH_KEY = "autoRefreshSeconds";
const SOLPLANET_RAW_MODE_KEY = "solplanetRawMode";
const SAJ_ACTION_DEBUG_MODE_KEY = "sajActionDebugMode";
const AUTO_REFRESH_OPTIONS = [0, 5, 10];
const SAJ_CONTROL_EDIT_GRACE_MS = 15000;
const CONFIG_SAMPLE_INTERVAL_OPTIONS = [5, 10, 30, 60, 300];
const BALANCE_TOLERANCE_W = 120;
const SOLPLANET_REALTIME_KV_URL = "/api/solplanet/realtime-kv";
const SOLPLANET_RAW_APIS = [
  { key: "getdevdata_device_2", titleKey: "rawApiGetdevdata2", url: "/api/solplanet/cgi/getdevdata-device-2" },
  { key: "getdevdata_device_3", titleKey: "rawApiGetdevdata3", url: "/api/solplanet/cgi/getdevdata-device-3" },
  { key: "getdevdata_device_4", titleKey: "rawApiGetdevdata4", url: "/api/solplanet/cgi/getdevdata-device-4" },
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
const SAMPLING_OVERALL_SERIES = [
  { key: "pv_total_w", labelKey: "samplingOverallMetricPv", color: "#f59e0b" },
  { key: "grid_import_w", labelKey: "samplingOverallMetricGridImport", color: "#2563eb" },
  { key: "grid_export_w", labelKey: "samplingOverallMetricGridExport", color: "#7c3aed" },
  { key: "saj_battery_charge_w", labelKey: "samplingOverallSeriesSajBatteryCharge", color: "#0f766e" },
  { key: "saj_battery_discharge_w", labelKey: "samplingOverallSeriesSajBatteryDischarge", color: "#10b981" },
  { key: "solplanet_battery_charge_w", labelKey: "samplingOverallSeriesSolplanetBatteryCharge", color: "#9a3412" },
  { key: "solplanet_battery_discharge_w", labelKey: "samplingOverallSeriesSolplanetBatteryDischarge", color: "#ef4444" },
];
const SOLPLANET_RAW_FIELD_HELP = {
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
    pac: [{ metric: "load_w", kind: "backup", noteZh: "仅在无法推导家庭负载时作为兜底估算（取绝对值/扣除电池充电）", noteEn: "Fallback estimate only when derived home-load is unavailable (abs / minus battery charge)" }],
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
  solplanet_metrics: {
    "notes.solplanet_load_w_source:inverter_pac_plus_grid_pac": [{ metric: "load_w", kind: "primary", noteZh: "家庭负载主来源：逆变器功率 + 电网功率", noteEn: "Primary home-load source: inverter power + grid power" }],
    "notes.solplanet_load_w_source:inverter_pac_minus_battery_charge_abs": [{ metric: "load_w", kind: "fallback", noteZh: "兜底：|inverter.pac| - |battery.pb(充电)|", noteEn: "Fallback: |inverter.pac| - |battery.pb(charging)|" }],
    "notes.solplanet_load_w_source:inverter_pac_minus_battery_discharge_abs": [{ metric: "load_w", kind: "fallback", noteZh: "兜底：|inverter.pac| - |battery.pb(放电)|", noteEn: "Fallback: |inverter.pac| - |battery.pb(discharging)|" }],
    "notes.solplanet_load_w_source:abs_inverter_pac": [{ metric: "load_w", kind: "fallback", noteZh: "兜底：|inverter.pac|", noteEn: "Fallback: |inverter.pac|" }],
    "notes.solplanet_grid_w_source:inferred_from_balance_assume_no_export": [{ metric: "grid_w", kind: "fallback", noteZh: "电网功率由功率平衡反推（假设无并网回馈）", noteEn: "Grid inferred from power balance (assumes no export)" }],
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
  lastCollectorStatus: null,
  lastEntities: null,
  lastSolplanetRaw: {},
  lastSolplanetKv: { phase: "idle", items: [], updated_at: null, error: null },
  lastSajRaw: {},
  lastSajControl: null,
  lastSolplanetControl: null,
  lastSolplanetControlLive: null,
  lastSolplanetControlFetch: null,
  lastSamplingStatus: null,
  lastSamplingDaily: null,
  lastSamplingUsageBySystem: null,
  lastSamplingPage: null,
  lastSamplingSeries: null,
  lastWorkerLogsPage: null,
  lastWorkerFailureLog: null,
  rawCardMode: {},
  systemLoadMeta: {
    saj: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
    solplanet: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
    combined: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
  },
};
let teslaControlBusy = false;

function getLang() {
  const saved = localStorage.getItem("lang");
  if (saved === "en" || saved === "zh") return saved;
  const browserLang = (navigator.language || "").toLowerCase();
  return browserLang.startsWith("zh") ? "zh" : "en";
}

let currentLang = getLang();
let currentTab = ["dashboard", "entities", "solplanetRaw", "sajRaw", "sajControl", "solplanetControl", "sampling", "workerLogs", "workerFailureLog"].includes(localStorage.getItem("activeTab"))
  ? localStorage.getItem("activeTab")
  : "dashboard";
const ALL_TABS = ["dashboard", "entities", "solplanetRaw", "sajRaw", "sajControl", "solplanetControl", "sampling", "workerLogs", "workerFailureLog"];
let solplanetRawMode = localStorage.getItem(SOLPLANET_RAW_MODE_KEY) === "table" ? "table" : "cards";
let sajActionDebugMode = localStorage.getItem(SAJ_ACTION_DEBUG_MODE_KEY) === "1";
let autoRefreshTimerId = null;
let autoRefreshSeconds = getAutoRefreshSeconds();
let sajControlLastEditAt = 0;
let solplanetControlBusy = false;
let summaryRequestId = 0;
let configReady = false;
let samplingChart = null;
let samplingChartFocusSeries = null;
let samplingChartLastPayload = null;
let samplingChartHandlersBound = false;
let samplingRangeApplyingFromBrush = false;
let samplingLegendSyncing = false;
const tabLoadState = {
  dashboard: { inFlight: false },
  entities: { inFlight: false },
  solplanetRaw: { inFlight: false },
  sajRaw: { inFlight: false },
  sajControl: { inFlight: false },
  solplanetControl: { inFlight: false },
  sampling: { inFlight: false },
  workerLogs: { inFlight: false },
  workerFailureLog: { inFlight: false },
};
const samplingRangeState = {
  day: "",
  week: "",
  month: "",
  monthYear: 0,
  relative: "",
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
  if (Math.abs(n) < 1000) return `${Math.round(n)} W`;
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

function setHtml(id, html) {
  const el = document.getElementById(id);
  if (el) {
    el.innerHTML = html;
    syncDataKindBadgeTooltip(el);
  }
}

function setNodeSourceTip(id, tipText) {
  const el = document.getElementById(id);
  if (!el) return;
  if (tipText === null || tipText === undefined || tipText === "") {
    el.classList.remove("has-source-tip");
    el.removeAttribute("data-source-tip");
    syncDataKindBadgeTooltip(el);
    return;
  }
  const normalized = String(tipText || "-");
  el.classList.add("has-source-tip");
  el.setAttribute("data-source-tip", normalized);
  syncDataKindBadgeTooltip(el);
}

function formatMetricSourceText(system, metricKey, sourceValue) {
  const sourceText = typeof sourceValue === "string" && sourceValue.trim() ? sourceValue.trim() : "unavailable";
  const sourceLabel = currentLang === "zh" ? "来源" : "Source";
  const calcLabel = currentLang === "zh" ? "计算" : "Calculated";
  const fieldLabel = currentLang === "zh" ? "字段" : "Field";
  const batteryNote = currentLang === "zh"
    ? "说明: battery 功率正=放电, 负=充电"
    : "Note: battery power positive=discharge, negative=charge";
  const kind = sourceText.startsWith("calc:") ? calcLabel : fieldLabel;
  const detail = sourceText.startsWith("calc:") ? sourceText.slice(5) : sourceText;
  if (metricKey === "battery") {
    return `${sourceLabel}: ${kind} ${detail}\n${batteryNote}`;
  }
  return `${sourceLabel}: ${kind} ${detail}`;
}

function dataKindFromSource(sourceValue, fallbackKind = null) {
  const sourceText = typeof sourceValue === "string" ? sourceValue.trim() : "";
  if (!sourceText || sourceText === "unavailable") return fallbackKind;
  if (/\bestimate\b/i.test(sourceText)) return "estimate";
  if (sourceText.startsWith("calc:")) return "calculated";
  return "real";
}

function dataKindLabel(kind) {
  if (kind === "real") return t("dataKindReal");
  if (kind === "estimate") return t("dataKindEstimate");
  if (kind === "calculated") return t("dataKindCalculated");
  return "";
}

function dataKindBadgeLetter(kind) {
  if (kind === "real") return "R";
  if (kind === "estimate") return "E";
  if (kind === "calculated") return "C";
  return "";
}

function dataKindLegendText() {
  return [
    `R = ${dataKindLabel("real")}`,
    `E = ${dataKindLabel("estimate")}`,
    `C = ${dataKindLabel("calculated")}`,
  ].join("\n");
}

function dataKindTooltipText(kind, sourceTip = "") {
  const sections = [dataKindLegendText()];
  const normalizedSourceTip = String(sourceTip || "").trim();
  if (normalizedSourceTip) sections.push(normalizedSourceTip);
  return sections.join("\n\n");
}

function syncDataKindBadgeTooltip(el) {
  if (!el) return;
  const badge = el.querySelector(".data-kind-badge");
  if (!badge) return;
  const kind = badge.getAttribute("data-kind") || "";
  const sourceTip = el.getAttribute("data-source-tip") || "";
  const tooltip = dataKindTooltipText(kind, sourceTip);
  badge.setAttribute("data-tooltip", tooltip);
  badge.setAttribute("title", tooltip);
  badge.setAttribute("aria-label", tooltip);
}

function formatValueWithDataKindHtml(text, kind) {
  const normalized = String(text ?? "-");
  if (!kind || normalized.trim() === "-") return escapeHtml(normalized);
  const letter = dataKindBadgeLetter(kind);
  const tooltip = dataKindTooltipText(kind);
  return (
    `<span class="data-kind-value">` +
    `<span class="data-kind-main">${escapeHtml(normalized)}</span>` +
    `<span class="data-kind-badge data-kind-${escapeHtml(kind)}" data-kind="${escapeHtml(kind)}" data-tooltip="${escapeHtml(tooltip)}" title="${escapeHtml(tooltip)}" aria-label="${escapeHtml(tooltip)}">${escapeHtml(letter)}</span>` +
    `</span>`
  );
}

function formatValueWithDataKind(text, kind) {
  const normalized = String(text ?? "-");
  if (!kind || normalized.trim() === "-") return normalized;
  return `${normalized} ${dataKindBadgeLetter(kind)}`;
}

function flowId(system, key) {
  return `${system}-${key}`;
}

function formatUpdatedAt(isoText) {
  return `${t("updatedAt")}: ${formatDateTimeWithAgo(isoText)}`;
}

function formatNoWrapText(text) {
  return String(text || "").replaceAll(" ", "\u00A0");
}

function formatDateTimeWithAgo(isoText, options = {}) {
  if (!isoText) return "-";
  const dt = new Date(isoText);
  if (Number.isNaN(dt.getTime())) return "-";
  const absoluteText = dt.toLocaleString();
  const agoText = formatRelativeAgo(isoText);
  const combined = agoText && agoText !== "-" ? `${absoluteText} (${agoText})` : absoluteText;
  return formatNoWrapText(combined);
}

function formatSamplingDateTime(isoText) {
  return formatDateTimeWithAgo(isoText);
}

function formatSamplingClock(value, withZone = false) {
  const dt = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(dt.getTime())) return "-";
  const text = dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
  if (!withZone) return text;
  return text;
}

function formatLocalDateTime(isoText) {
  return formatDateTimeWithAgo(isoText);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatRelativeAgo(isoText) {
  if (!isoText) return "-";
  const dt = new Date(isoText);
  if (Number.isNaN(dt.getTime())) return "-";
  const seconds = Math.max(0, Math.floor((Date.now() - dt.getTime()) / 1000));
  if (seconds < 3) return t("rawAgoJustNow");
  if (seconds < 60) return t("rawAgoSeconds", { n: seconds });
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return t("rawAgoMinutes", { n: minutes });
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return t("rawAgoHours", { n: hours });
  const days = Math.floor(hours / 24);
  return t("rawAgoDays", { n: days });
}

function markSajControlLocalEdit() {
  sajControlLastEditAt = Date.now();
}

function isSajControlLocalEditing() {
  const body = document.getElementById("sajControlSlotsBody");
  const active = document.activeElement;
  if (body && active instanceof HTMLElement && body.contains(active)) return true;
  if (sajControlLastEditAt <= 0) return false;
  return Date.now() - sajControlLastEditAt < SAJ_CONTROL_EDIT_GRACE_MS;
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
  const sajDebugInput = document.getElementById("sajActionDebugModeInput");
  if (sajDebugInput) sajDebugInput.checked = sajActionDebugMode;
  updateSamplingTimeHeader();

  if (stateCache.lastSummary) renderSummary(stateCache.lastSummary);
  renderSystemLoadMeta("saj");
  renderSystemLoadMeta("solplanet");
  renderSystemLoadMeta("combined");
  setSolplanetRawMode(solplanetRawMode, false);
  renderSolplanetRawFromCache();
  renderSolplanetKvFromCache();
  renderSajRawFromCache();
  renderSajControlFromCache();
  renderSolplanetControlFromCache();
  rerenderSamplingViewFromCache();
  if (stateCache.lastEntities) renderEntitiesPage(stateCache.lastEntities);
}

function updateSamplingTimeHeader() {
  const tableHeader = document.querySelector("#samplingView th[data-i18n='samplingTableTime']");
  if (!tableHeader) return;
  tableHeader.textContent = t("samplingTableTime");
}

function setConfigModalVisible(visible) {
  const modal = document.getElementById("configModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function setWorkerLogDetailModalVisible(visible) {
  const modal = document.getElementById("workerLogDetailModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function fillConfigForm(payload = {}) {
  document.getElementById("cfgHaUrl").value = payload.ha_url || "";
  document.getElementById("cfgHaToken").value = payload.ha_token || "";
  document.getElementById("cfgDongleHost").value = payload.solplanet_dongle_host || "";
  document.getElementById("cfgSolplanetInverterSn").value = payload.solplanet_inverter_sn || "";
  document.getElementById("cfgSolplanetBatterySn").value = payload.solplanet_battery_sn || "";
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
  const value = Number(watts);
  if (!Number.isFinite(value)) return "-";
  if (Math.abs(value) < 1000) return `${Math.round(value)} W`;
  return `${(value / 1000).toFixed(3)} kW`;
}

function formatSignedKwFromWatts(watts) {
  if (watts === null || watts === undefined) return "-";
  const value = Number(watts);
  if (!Number.isFinite(value)) return "-";
  const sign = value >= 0 ? "+" : "-";
  if (Math.abs(value) < 1000) return `${sign}${Math.round(Math.abs(value))} W`;
  return `${sign}${Math.abs(value / 1000).toFixed(3)} kW`;
}

function formatPowerKwFromWattsWithDataKind(watts, kind) {
  return formatValueWithDataKind(formatPowerKwFromWatts(watts), kind);
}

function formatSignedKwFromWattsWithDataKind(watts, kind) {
  return formatValueWithDataKind(formatSignedKwFromWatts(watts), kind);
}

const BATTERY_CAPACITY_KWH = {
  saj: 15,
  solplanet: 40,
};

const BATTERY_MIN_DISCHARGE_SOC = {
  saj: 20,
  solplanet: 10,
};

const POWER_FLOW_ACTIVE_THRESHOLD_W = 30;
const BATTERY_SOC_COLOR_STOPS = [
  { pct: 0, rgb: [180, 88, 88] },
  { pct: 25, rgb: [205, 146, 78] },
  { pct: 50, rgb: [203, 167, 84] },
  { pct: 75, rgb: [163, 177, 98] },
  { pct: 100, rgb: [96, 163, 116] },
];

function formatTrimmedDecimal(value, digits = 1) {
  const fixed = Number(value).toFixed(digits);
  return fixed.replace(/\.0$/, "");
}

function formatBatteryEnergyKwh(system, batterySoc) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  if (!Number.isFinite(capacityKwh)) return "-";
  if (batterySoc === null || batterySoc === undefined) return `- / ${formatTrimmedDecimal(capacityKwh, 1)} kWh`;
  const clampedSoc = Math.max(0, Math.min(100, Number(batterySoc)));
  const currentKwh = (capacityKwh * clampedSoc) / 100;
  return `${formatTrimmedDecimal(currentKwh, 1)} / ${formatTrimmedDecimal(capacityKwh, 1)} kWh`;
}

function formatBatteryUsableKwh(system, batterySoc) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  const minDischargeSoc = BATTERY_MIN_DISCHARGE_SOC[system] ?? 0;
  if (!Number.isFinite(capacityKwh) || batterySoc === null || batterySoc === undefined) {
    return t("batteryUsableRemaining", { value: "-" });
  }
  const clampedSoc = Math.max(0, Math.min(100, Number(batterySoc)));
  const usableSoc = Math.max(0, clampedSoc - minDischargeSoc);
  const usableKwh = (capacityKwh * usableSoc) / 100;
  return t("batteryUsableRemaining", { value: formatTrimmedDecimal(usableKwh, 1) });
}

function formatBatteryRuntimeTargetTime(targetAt) {
  if (!(targetAt instanceof Date) || Number.isNaN(targetAt.getTime())) return null;
  const now = new Date();
  const timeText = targetAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const sameDay =
    targetAt.getFullYear() === now.getFullYear() &&
    targetAt.getMonth() === now.getMonth() &&
    targetAt.getDate() === now.getDate();
  if (sameDay) return timeText;
  return currentLang === "zh" ? `明天 ${timeText}` : `tomorrow ${timeText}`;
}

function getBatteryRuntimeProjection(system, batterySoc, batteryW) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  const minDischargeSoc = BATTERY_MIN_DISCHARGE_SOC[system] ?? 0;
  if (!Number.isFinite(capacityKwh) || batterySoc === null || batterySoc === undefined || batteryW === null || batteryW === undefined) {
    return null;
  }

  const powerW = Number(batteryW);
  if (!Number.isFinite(powerW)) return null;
  if (Math.abs(powerW) < POWER_FLOW_ACTIVE_THRESHOLD_W) return { mode: "idle" };

  const clampedSoc = Math.max(0, Math.min(100, Number(batterySoc)));
  if (powerW <= -POWER_FLOW_ACTIVE_THRESHOLD_W) {
    const remainingKwh = (capacityKwh * Math.max(0, 100 - clampedSoc)) / 100;
    if (remainingKwh <= 0) return null;
    const runtimeHours = remainingKwh / (Math.abs(powerW) / 1000);
    if (!Number.isFinite(runtimeHours) || runtimeHours < 0) return null;
    const targetAt = new Date(Date.now() + runtimeHours * 3600 * 1000);
    const timeText = formatBatteryRuntimeTargetTime(targetAt);
    if (!timeText) return null;
    return { mode: "charging", runtimeHours, timeText };
  }

  const usableSoc = Math.max(0, clampedSoc - minDischargeSoc);
  if (usableSoc <= 0) return null;
  const usableKwh = (capacityKwh * usableSoc) / 100;
  const runtimeHours = usableKwh / (powerW / 1000);
  if (!Number.isFinite(runtimeHours) || runtimeHours < 0) return null;
  const targetAt = new Date(Date.now() + runtimeHours * 3600 * 1000);
  const timeText = formatBatteryRuntimeTargetTime(targetAt);
  if (!timeText) return null;
  return { mode: "discharging", runtimeHours, timeText };
}

function formatBatteryRuntimeEstimate(system, batterySoc, batteryW) {
  const projection = getBatteryRuntimeProjection(system, batterySoc, batteryW);
  if (!projection) return t("batteryRuntimeNoData");
  if (projection.mode === "idle") return t("batteryRuntimeIdle");
  return t(
    projection.mode === "charging" ? "batteryRuntimeEstimateCharging" : "batteryRuntimeEstimateDischarging",
    {
      hours: formatTrimmedDecimal(projection.runtimeHours, 1),
      time: projection.timeText,
    },
  );
}

function formatInverterConversion(inverterW, batteryW, solarInputW = null) {
  const inverterSigned = toFiniteNumber(inverterW);
  const batterySigned = toFiniteNumber(batteryW);
  const solarInputSigned = toFiniteNumber(solarInputW);
  if (inverterSigned === null || batterySigned === null) return t("inverterConversionUnavailable");

  const battery = Math.abs(batterySigned);
  const solarInput = Math.max(solarInputSigned || 0, 0);

  if (battery < POWER_FLOW_ACTIVE_THRESHOLD_W) {
    const inverterOutput = Math.max(inverterSigned, 0);
    if (solarInput < POWER_FLOW_ACTIVE_THRESHOLD_W || inverterOutput < POWER_FLOW_ACTIVE_THRESHOLD_W) {
      return t("inverterConversionUnavailable");
    }
    return `${Math.round((Math.min(inverterOutput, solarInput) / Math.max(inverterOutput, solarInput)) * 100)}%`;
  }

  if (batterySigned < -POWER_FLOW_ACTIVE_THRESHOLD_W) {
    const solarSurplus = Math.max(solarInput - Math.max(inverterSigned, 0), 0);
    const acInput = Math.max(-inverterSigned, 0);
    const totalInput = solarSurplus + acInput;
    if (totalInput < POWER_FLOW_ACTIVE_THRESHOLD_W) return t("inverterConversionUnavailable");
    return `${Math.round((Math.min(battery, totalInput) / Math.max(battery, totalInput)) * 100)}%`;
  }

  if (batterySigned > POWER_FLOW_ACTIVE_THRESHOLD_W && inverterSigned > POWER_FLOW_ACTIVE_THRESHOLD_W) {
    if (solarInput >= POWER_FLOW_ACTIVE_THRESHOLD_W) return t("inverterConversionUnavailable");
    const inverter = Math.abs(inverterSigned);
    if (inverter < POWER_FLOW_ACTIVE_THRESHOLD_W) return t("inverterConversionUnavailable");
    return `${Math.round((Math.min(inverter, battery) / Math.max(inverter, battery)) * 100)}%`;
  }

  return t("inverterConversionUnavailable");
}

function formatBatteryRuntimeText(system, batterySoc, batteryW) {
  const projection = getBatteryRuntimeProjection(system, batterySoc, batteryW);
  if (!projection) return t("batteryRuntimeNoData");
  if (projection.mode === "idle") return t("batteryRuntimeIdle");
  return t(
    projection.mode === "charging" ? "batteryRuntimeCharging" : "batteryRuntimeDischarging",
    { time: projection.timeText },
  );
}

const flowDiagrams = {
  byBoard: new Map(),
  byEdgeId: new Map(),
  byLabelId: new Map(),
};

function buildSystemDiagramSpec(system) {
  const prefix = String(system);
  return {
    layout: "hub",
    viewport: { width: 760, height: 520 },
    nodes: [
      {
        id: `${prefix}-solarNode`,
        kind: "solar",
        icon: "solar",
        title: "Solar",
        titleKey: "solarTitle",
        width: 176,
        height: 102,
        lines: [
          { id: `${prefix}-solarPowerValue`, className: "node-value", text: "-" },
          { id: `${prefix}-solarState`, className: "node-state muted", text: "idle" },
        ],
      },
      {
        id: `${prefix}-gridNode`,
        kind: "grid",
        icon: "grid",
        title: "Grid",
        titleKey: "gridTitle",
        width: 176,
        height: 102,
        lines: [
          { id: `${prefix}-gridPowerValue`, className: "node-value", text: "-" },
          { id: `${prefix}-gridState`, className: "node-state muted", text: "idle" },
        ],
      },
      {
        id: `${prefix}-inverterNode`,
        kind: "inverter",
        icon: "inverter",
        title: "Inverter",
        titleKey: "inverterTitle",
        width: 192,
        height: 112,
        lines: [
          { id: `${prefix}-inverterStatusValue`, className: "node-value", text: "-" },
          { id: `${prefix}-systemBalance`, className: "node-state muted", text: "balance -" },
        ],
      },
      {
        id: `${prefix}-loadNode`,
        kind: "load",
        icon: "load",
        title: "Home Load",
        titleKey: "loadTitle",
        width: 176,
        height: 102,
        lines: [
          { id: `${prefix}-loadPowerValue`, className: "node-value", text: "-" },
          { id: `${prefix}-loadState`, className: "node-state muted", text: "idle" },
        ],
      },
      {
        id: `${prefix}-batteryNode`,
        kind: "battery",
        icon: "battery",
        title: "battery",
        width: 196,
        height: 154,
        lines: [
          {
            type: "soc",
            fillId: `${prefix}-batterySocFill`,
            valueId: `${prefix}-batterySocValue`,
            energyId: `${prefix}-batteryEnergyValue`,
            usableId: `${prefix}-batteryUsableValue`,
            runtimeId: `${prefix}-batteryRuntimeValue`,
          },
        ],
      },
    ],
    edges: [
      { id: `${prefix}-lineSolar`, source: `${prefix}-solarNode`, target: `${prefix}-inverterNode` },
      { id: `${prefix}-lineGrid`, source: `${prefix}-gridNode`, target: `${prefix}-inverterNode` },
      { id: `${prefix}-lineLoad`, source: `${prefix}-inverterNode`, target: `${prefix}-loadNode` },
      { id: `${prefix}-lineBattery`, source: `${prefix}-batteryNode`, target: `${prefix}-inverterNode` },
    ],
  };
}

function buildCombinedDiagramSpec() {
  return {
    layout: "power",
    viewport: { width: 1100, height: 720 },
    nodes: [
      {
        id: "combined-gridNode",
        kind: "grid",
        icon: "grid",
        title: "Grid",
        titleKey: "gridTitle",
        width: 176,
        height: 102,
        lines: [
          { id: "combined-gridPowerValue", className: "node-value", text: "-" },
          { id: "combined-gridState", className: "node-state muted", text: "idle" },
        ],
      },
      {
        id: "combined-solarNode",
        kind: "solar",
        icon: "solar",
        title: "Solar",
        titleKey: "solarTitle",
        width: 176,
        height: 102,
        lines: [
          { id: "combined-solarPowerValue", className: "node-value", text: "-" },
          { id: "combined-solarState", className: "node-state muted", text: "idle" },
        ],
      },
      {
        id: "combined-switchboardNode",
        kind: "switchboard",
        icon: "switchboard",
        title: "Switchboard",
        titleKey: "switchboardTitle",
        width: 224,
        height: 152,
        lines: [],
      },
      {
        id: "combined-loadNode",
        kind: "load",
        icon: "load",
        title: "Home Load",
        titleKey: "loadTitle",
        width: 184,
        height: 110,
        lines: [
          { id: "combined-loadPowerValue", className: "node-value", text: "-" },
          { id: "combined-loadState", className: "node-state muted", text: "idle" },
        ],
      },
      {
        id: "combined-teslaNode",
        kind: "tesla",
        icon: "tesla",
        title: "Tesla",
        titleKey: "teslaChargingLabel",
        width: 176,
        height: 314,
        lines: [
          {
            type: "soc",
            fillId: "combined-teslaSocFill",
            valueId: "combined-teslaSocValue",
          },
          { id: "combined-teslaChargingPowerValue", className: "node-sub-value", text: "-" },
          { id: "combined-teslaChargingCurrentValue", className: "node-mini-value muted", text: "-" },
          { id: "combined-teslaConnectionValue", className: "node-mini-value muted", text: "-" },
          { type: "button", id: "combined-teslaChargingToggleBtn", className: "tesla-control-btn btn secondary", text: "-" },
        ],
      },
      {
        id: "combined-battery1Node",
        kind: "battery",
        side: "left",
        icon: "battery",
        title: "SAJ Battery",
        width: 198,
        height: 160,
        lines: [
          {
            type: "soc",
            fillId: "combined-battery1SocFill",
            valueId: "combined-battery1SocValue",
            energyId: "combined-battery1EnergyValue",
            usableId: "combined-battery1UsableValue",
            runtimeId: "combined-battery1RuntimeValue",
          },
        ],
      },
      {
        id: "combined-inverter1Node",
        kind: "inverter",
        side: "left",
        icon: "inverter",
        title: "SAJ Inverter",
        width: 176,
        height: 120,
        lines: [
          { id: "combined-inverter1RatioValue", className: "node-value", text: "-" },
          { id: "combined-inverter1State", className: "node-state muted", text: "-" },
        ],
      },
      {
        id: "combined-inverter2Node",
        kind: "inverter",
        side: "right",
        icon: "inverter",
        title: "Solplanet Inverter",
        width: 176,
        height: 120,
        lines: [
          { id: "combined-inverter2RatioValue", className: "node-value", text: "-" },
          { id: "combined-inverter2State", className: "node-state muted", text: "-" },
        ],
      },
      {
        id: "combined-battery2Node",
        kind: "battery",
        side: "right",
        icon: "battery",
        title: "Solplanet Battery",
        width: 198,
        height: 160,
        lines: [
          {
            type: "soc",
            fillId: "combined-battery2SocFill",
            valueId: "combined-battery2SocValue",
            energyId: "combined-battery2EnergyValue",
            usableId: "combined-battery2UsableValue",
            runtimeId: "combined-battery2RuntimeValue",
          },
        ],
      },
    ],
    edges: [
      { id: "combined-lineGridToSwitchboard", source: "combined-gridNode", target: "combined-switchboardNode", labelId: "combined-flowLabelGridToSwitchboard" },
      { id: "combined-lineSolarToInverter1B", source: "combined-solarNode", target: "combined-inverter1Node", labelId: "combined-flowLabelSolarToInverter1" },
      {
        id: "combined-lineSwitchboardToTotalLoad",
        source: "combined-switchboardNode",
        target: "combined-teslaNode",
        labelId: "combined-flowLabelSwitchboardToTotalLoad",
        glowWidth: 28,
        lineWidth: 18,
        coreWidth: 6,
      },
      { id: "combined-lineSwitchboardToHomeLoad", source: "combined-switchboardNode", target: "combined-loadNode", labelId: "combined-flowLabelSwitchboardToHomeLoad" },
      { id: "combined-lineSwitchboardToTeslaB", source: "combined-switchboardNode", target: "combined-teslaNode", labelId: "combined-flowLabelSwitchboardToTesla" },
      { id: "combined-lineBattery1ToInverter1", source: "combined-battery1Node", target: "combined-inverter1Node", labelId: "combined-flowLabelBattery1ToInverter1" },
      { id: "combined-lineBattery2ToInverter2", source: "combined-battery2Node", target: "combined-inverter2Node", labelId: "combined-flowLabelBattery2ToInverter2" },
      { id: "combined-lineInverter1ToSwitchboardB", source: "combined-inverter1Node", target: "combined-switchboardNode", labelId: "combined-flowLabelInverter1ToSwitchboard" },
      { id: "combined-lineInverter2ToSwitchboardB", source: "combined-inverter2Node", target: "combined-switchboardNode", labelId: "combined-flowLabelInverter2ToSwitchboard" },
    ],
  };
}

function registerFlowDiagram(boardId, spec) {
  const container = document.getElementById(boardId);
  if (!container || typeof EnergyFlowDiagram !== "function") return;
  const diagram = new EnergyFlowDiagram({ container, spec });
  flowDiagrams.byBoard.set(boardId, diagram);
  spec.edges.forEach((edge) => {
    flowDiagrams.byEdgeId.set(edge.id, diagram);
    if (edge.labelId) flowDiagrams.byLabelId.set(edge.labelId, diagram);
  });
}

function initFlowDiagrams() {
  registerFlowDiagram("energyFlowCombined", buildCombinedDiagramSpec());
  registerFlowDiagram("energyFlowSaj", buildSystemDiagramSpec("saj"));
  registerFlowDiagram("energyFlowSolplanet", buildSystemDiagramSpec("solplanet"));
}

function refreshFlowDiagrams() {
  flowDiagrams.byBoard.forEach((diagram) => {
    diagram.fit();
  });
}

function setFlowLine(id, active, reverse = false, theme = "default") {
  const diagram = flowDiagrams.byEdgeId.get(id);
  if (diagram && diagram.setEdgeState(id, active, reverse, theme)) return;
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

function setSocTextContrastBySocValueId(socValueId, socPercent) {
  const textLayer = document.getElementById(socValueId)?.parentElement;
  if (!textLayer) return;
  textLayer.style.setProperty("--soc-text-color", "#352c25");
  textLayer.style.setProperty("--soc-text-shadow", "rgba(255, 255, 255, 0.5)");
}

function getBatterySocColor(socPercent) {
  const pct = Math.max(0, Math.min(100, Number(socPercent) || 0));
  for (let i = 1; i < BATTERY_SOC_COLOR_STOPS.length; i += 1) {
    const prev = BATTERY_SOC_COLOR_STOPS[i - 1];
    const next = BATTERY_SOC_COLOR_STOPS[i];
    if (pct <= next.pct) {
      const span = next.pct - prev.pct || 1;
      const ratio = (pct - prev.pct) / span;
      const rgb = prev.rgb.map((value, channel) => {
        const nextValue = next.rgb[channel];
        return Math.round(value + (nextValue - value) * ratio);
      });
      return `rgb(${rgb[0]}, ${rgb[1]}, ${rgb[2]})`;
    }
  }
  const last = BATTERY_SOC_COLOR_STOPS[BATTERY_SOC_COLOR_STOPS.length - 1].rgb;
  return `rgb(${last[0]}, ${last[1]}, ${last[2]})`;
}

function setSocFillLevel(socFillId, socPercent) {
  const socFill = document.getElementById(socFillId);
  if (!socFill) return;

  const clampedSoc = Math.max(0, Math.min(100, Number(socPercent) || 0));
  socFill.style.height = `${clampedSoc}%`;
  socFill.style.background = getBatterySocColor(clampedSoc);

  if (clampedSoc <= 0) {
    return;
  }
}

function renderBatterySocDisplay({
  system,
  soc,
  batteryW,
  socValueId,
  socFillId,
  energyValueId,
  usableValueId,
  runtimeValueId,
  socDataKind = "real",
  energyDataKind = "estimate",
  usableDataKind = "estimate",
  runtimeDataKind = "estimate",
}) {
  if (soc === null || soc === undefined) {
    setText(socValueId, "-");
    if (energyValueId) setHtml(energyValueId, formatValueWithDataKindHtml(formatBatteryEnergyKwh(system, null), energyDataKind));
    if (usableValueId) setHtml(usableValueId, formatValueWithDataKindHtml(formatBatteryUsableKwh(system, null), usableDataKind));
    if (runtimeValueId) setHtml(runtimeValueId, formatValueWithDataKindHtml(formatBatteryRuntimeEstimate(system, null, batteryW), runtimeDataKind));
    setSocFillLevel(socFillId, 0);
    setSocTextContrastBySocValueId(socValueId, 0);
    return;
  }

  const clampedSoc = Math.max(0, Math.min(100, Number(soc)));
  setHtml(socValueId, formatValueWithDataKindHtml(`${clampedSoc.toFixed(0)}%`, socDataKind));
  if (energyValueId) setHtml(energyValueId, formatValueWithDataKindHtml(formatBatteryEnergyKwh(system, clampedSoc), energyDataKind));
  if (usableValueId) setHtml(usableValueId, formatValueWithDataKindHtml(formatBatteryUsableKwh(system, clampedSoc), usableDataKind));
  if (runtimeValueId) setHtml(runtimeValueId, formatValueWithDataKindHtml(formatBatteryRuntimeEstimate(system, clampedSoc, batteryW), runtimeDataKind));
  setSocFillLevel(socFillId, clampedSoc);
  setSocTextContrastBySocValueId(socValueId, clampedSoc);
}

function inverterStateText(raw) {
  const inverterStateRaw = String(raw || "").toLowerCase();
  if (inverterStateRaw === "running") return t("inverterRunning");
  if (inverterStateRaw === "offline") return t("inverterOffline");
  if (inverterStateRaw === "standby") return t("inverterStandby");
  if (!inverterStateRaw) return t("inverterUnknown");
  return String(raw || "-");
}

function _stripModeCodePrefix(label) {
  return String(label || "").replace(/^\s*\d+\s*-\s*/, "").trim();
}

function _sajModeTextByCode(modeCode) {
  const n = Number(modeCode);
  if (!Number.isFinite(n)) return "";
  const code = Math.trunc(n);
  if (code < 0 || code > 8) return "";
  return _stripModeCodePrefix(t(`sajModeOption${code}`));
}

function getSajDashboardModeText() {
  const state = stateCache.lastSajControl?.control_state || stateCache.lastSajControl?.state || null;
  if (!state) return "";
  const working = state?.working_mode || {};
  const inverterModeRaw = working?.inverter_working_mode_sensor;
  if (typeof inverterModeRaw === "string" && inverterModeRaw.trim()) {
    const raw = inverterModeRaw.trim();
    if (!Number.isFinite(Number(raw))) return raw;
  }

  const modeSensor = Number(working?.mode_sensor);
  if (Number.isFinite(modeSensor)) {
    const mapped = _sajModeTextByCode(modeSensor);
    if (mapped) return mapped;
  }
  const modeInput = Number(working?.mode_input);
  if (Number.isFinite(modeInput)) {
    const mapped = _sajModeTextByCode(modeInput);
    if (mapped) return mapped;
  }
  const inverterModeNum = Number(inverterModeRaw);
  if (Number.isFinite(inverterModeNum)) {
    const mapped = _sajModeTextByCode(inverterModeNum);
    if (mapped) return mapped;
  }
  return "";
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

  const balanced = Math.abs(Number(balanceW) || 0) <= BALANCE_TOLERANCE_W;
  setText(statusId, balanced ? t("balanceStatusBalanced") : t("balanceStatusUnbalanced"));
  statusEl.classList.add(balanced ? "status-balanced" : "status-unbalanced");
  setHtml(residualId, escapeHtml(t("balanceResidualLabel", { value: "__VALUE__" })).replace("__VALUE__", formatValueWithDataKindHtml(formatPowerKwFromWatts(balanceW), "calculated")));
}

function setBalanceFormula(system, pvW, gridW, batteryW, loadW, netW, kinds = {}) {
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
  const pvKind = kinds.pv || "real";
  const batteryKind = kinds.battery || "real";
  const gridKind = kinds.grid || "real";
  const loadKind = kinds.load || "real";
  const formula =
    `${escapeHtml(t("balanceFormulaLabel"))}: ` +
    `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(pvW), pvKind)}) + ` +
    `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(batteryDischargeW), batteryKind)}) + ` +
    `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(gridImportW), gridKind)}) - ` +
    `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(loadW), loadKind)}) - ` +
    `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(batteryChargeW), batteryKind)}) - ` +
    `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(gridExportW), gridKind)}) = ` +
    `${formatValueWithDataKindHtml(formatSignedKwFromWatts(netW), "calculated")}`;
  setHtml(formulaId, formula);
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
  const solarKind = dataKindFromSource(metrics.pv_source, "real");
  const gridKind = dataKindFromSource(metrics.grid_source, "real");
  const batteryKind = dataKindFromSource(metrics.battery_source, "real");
  const loadKind = dataKindFromSource(metrics.load_source, "real");

  setHtml(flowId(system, "solarPowerValue"), formatValueWithDataKindHtml(formatPowerKwFromWatts(pvW), solarKind));
  setHtml(flowId(system, "gridPowerValue"), formatValueWithDataKindHtml(formatPowerKwFromWatts(gridW === null ? null : Math.abs(gridW)), gridKind));
  setHtml(flowId(system, "loadPowerValue"), formatValueWithDataKindHtml(formatPowerKwFromWatts(loadW), loadKind));
  setNodeSourceTip(flowId(system, "solarPowerValue"), formatMetricSourceText(system, "solar", metrics.pv_source));
  setNodeSourceTip(flowId(system, "gridPowerValue"), formatMetricSourceText(system, "grid", metrics.grid_source));
  setNodeSourceTip(flowId(system, "loadPowerValue"), formatMetricSourceText(system, "load", metrics.load_source));
  let inverterDisplayText = inverterStateText(inverterStatus);
  if (system === "saj") {
    const modeText = getSajDashboardModeText();
    if (modeText) inverterDisplayText = modeText;
  }
  setText(flowId(system, "inverterStatusValue"), inverterDisplayText);

  const solarActive = Boolean(metrics.solar_active);
  setText(flowId(system, "solarState"), solarActive ? t("stateProducing") : t("stateIdle"));
  setFlowLine(flowId(system, "lineSolar"), solarActive, false);

  const gridActive = gridW !== null && Math.abs(gridW) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const gridImport = gridW !== null && gridW > 0;
  setText(flowId(system, "gridState"), gridActive ? (gridImport ? t("stateImporting") : t("stateExporting")) : t("stateIdle"));
  setFlowLine(
    flowId(system, "lineGrid"),
    gridActive,
    !gridImport,
    gridImport ? "gridImport" : "gridExport",
  );
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
  setFlowLine(
    flowId(system, "lineBattery"),
    batteryActive,
    batteryDischarging,
    batteryDischarging ? "batteryDischarge" : "batteryCharge",
  );

  renderBatterySocDisplay({
    system,
    soc: batterySoc,
    batteryW,
    socValueId: flowId(system, "batterySocValue"),
    socFillId: flowId(system, "batterySocFill"),
    energyValueId: flowId(system, "batteryEnergyValue"),
    usableValueId: flowId(system, "batteryUsableValue"),
    runtimeValueId: flowId(system, "batteryRuntimeValue"),
    socDataKind: "real",
    energyDataKind: "estimate",
    usableDataKind: "estimate",
    runtimeDataKind: "estimate",
  });
  if (balanceW === null || balanceW === undefined) {
    setText(flowId(system, "systemBalance"), `${t("balanceLabel")} -`);
    setBalanceStatus(system, null);
    setBalanceFormula(system, null, null, null, null, null);
  } else {
    setHtml(
      flowId(system, "systemBalance"),
      `${escapeHtml(t("balanceLabel"))} ${formatValueWithDataKindHtml(formatPowerKwFromWatts(balanceW), "calculated")}`,
    );
    setBalanceStatus(system, balanceW);
    setBalanceFormula(system, pvW, gridW, batteryW, loadW, balanceW, {
      pv: solarKind,
      grid: gridKind,
      battery: batteryKind,
      load: loadKind,
    });
  }

  if (system === "solplanet") {
    const notes = metrics.notes || [];
    const meterOffline = notes.some((n) => n === "solplanet_meter_data_valid:false");
    const banner = document.getElementById("solplanet-meterOfflineBanner");
    if (banner) {
      banner.classList.toggle("is-hidden", !meterOffline);
    }
  }
}

function setFlowValueLabel(id, wattsValue, active, dataKind = null) {
  const hasValue = wattsValue !== null && wattsValue !== undefined && !Number.isNaN(Number(wattsValue));
  const diagram = flowDiagrams.byLabelId.get(id);
  if (diagram) {
    const text = hasValue ? formatValueWithDataKindHtml(formatPowerKwFromWatts(Math.abs(Number(wattsValue))), dataKind) : "-";
    if (diagram.setEdgeLabel(id, text, active)) return;
  }
  const el = document.getElementById(id);
  if (!el) return;
  if (!hasValue) {
    el.textContent = "-";
    el.classList.remove("active");
    return;
  }
  el.innerHTML = formatValueWithDataKindHtml(formatPowerKwFromWatts(Math.abs(Number(wattsValue))), dataKind);
  el.classList.toggle("active", Boolean(active));
}

function setFlowTextLabel(id, text, active = true) {
  const diagram = flowDiagrams.byLabelId.get(id);
  if (diagram) {
    if (diagram.setEdgeLabel(id, text, active)) return;
  }
  const el = document.getElementById(id);
  if (!el) return;
  if (!active || text === null || text === undefined || text === "-") {
    el.textContent = "-";
    el.classList.remove("active");
    return;
  }
  el.textContent = String(text);
  el.classList.add("active");
}

function toFiniteNumber(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function latestIsoTime(...isoTexts) {
  let latestMs = null;
  let latestIso = null;
  for (const iso of isoTexts) {
    if (!iso) continue;
    const ms = new Date(iso).getTime();
    if (!Number.isFinite(ms)) continue;
    if (latestMs === null || ms > latestMs) {
      latestMs = ms;
      latestIso = iso;
    }
  }
  return latestIso;
}

function wattsFromStateUnit(stateValue, unitValue) {
  const numeric = toFiniteNumber(stateValue);
  if (numeric === null) return null;
  const unit = String(unitValue || "").trim().toLowerCase();
  if (!unit || unit === "w" || unit === "watt" || unit === "watts") return numeric;
  if (unit === "kw") return numeric * 1000;
  if (unit === "mw") return numeric * 1000000;
  return numeric;
}

function pickTeslaChargingEntity(items) {
  if (!Array.isArray(items) || !items.length) return null;
  const scored = items
    .map((item) => {
      const entityId = String(item?.entity_id || "").toLowerCase();
      const friendly = String(item?.friendly_name || "").toLowerCase();
      const numericW = wattsFromStateUnit(item?.state, item?.unit);
      let score = 0;
      if (entityId.includes("charger_power")) score += 120;
      if (entityId.includes("charge_power")) score += 100;
      if (friendly.includes("charger power")) score += 90;
      if (friendly.includes("charging power")) score += 80;
      if (entityId.includes("tesla")) score += 20;
      if (numericW !== null) score += 15;
      return { item, score };
    })
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score);
  return scored[0]?.item || null;
}

function pickTeslaMetricEntity(items, metricKind) {
  if (!Array.isArray(items) || !items.length) return null;
  const target = String(metricKind || "").toLowerCase();
  const scored = items
    .map((item) => {
      const entityId = String(item?.entity_id || "").toLowerCase();
      const friendly = String(item?.friendly_name || "").toLowerCase();
      const numeric = toFiniteNumber(item?.state);
      const unit = String(item?.unit || "").toLowerCase();
      let score = 0;
      if (target === "current") {
        if (entityId.includes("charger_current")) score += 120;
        if (entityId.includes("charge_current")) score += 100;
        if (friendly.includes("charger current")) score += 90;
        if (friendly.includes("charging current")) score += 80;
        if (unit === "a") score += 30;
      } else if (target === "voltage") {
        if (entityId.includes("charger_voltage")) score += 120;
        if (entityId.includes("charge_voltage")) score += 100;
        if (friendly.includes("charger voltage")) score += 90;
        if (friendly.includes("charging voltage")) score += 80;
        if (unit === "v") score += 30;
      } else if (target === "soc") {
        if (entityId.includes("battery_level")) score += 140;
        if (entityId.includes("usable_battery_level")) score += 130;
        if (entityId.includes("charge_level")) score += 120;
        if (entityId.includes("state_of_charge")) score += 120;
        if (friendly.includes("battery level")) score += 110;
        if (friendly.includes("charge level")) score += 100;
        if (friendly.includes("state of charge")) score += 100;
        if (friendly.includes("battery")) score += 20;
        if (unit === "%") score += 35;
        if (numeric !== null && numeric >= 0 && numeric <= 100) score += 20;
        if (entityId.includes("charger_") || entityId.includes("charge_current") || entityId.includes("charge_voltage") || entityId.includes("charge_power")) {
          score -= 80;
        }
      }
      if (entityId.includes("tesla")) score += 20;
      if (numeric !== null) score += 10;
      return { item, score };
    })
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score);
  return scored[0]?.item || null;
}

function formatTeslaCurrentValue(value, unit) {
  const numeric = toFiniteNumber(value);
  const unitText = String(unit || "").trim();
  if (numeric === null) return `${t("teslaChargingCurrentLabel")} -`;
  const normalizedUnit = currentLang === "zh" && unitText.toLowerCase() === "a"
    ? "安"
    : (unitText || (currentLang === "zh" ? "安" : "A"));
  return `${t("teslaChargingCurrentLabel")} ${numeric.toFixed(1)}${normalizedUnit}`;
}

function formatTeslaPowerValue(watts) {
  return `${t("teslaChargingPowerLabel")} ${formatPowerKwFromWatts(watts)}`;
}

function formatTeslaConnectionSummary(teslaInfo = null) {
  const cableConnected = teslaInfo?.teslaCableConnected;
  const connectionState = String(teslaInfo?.teslaConnectionState || "").trim().toLowerCase();
  if (connectionState === "charging") return t("teslaConnectionStateCharging");
  if (connectionState === "plugged_not_charging") return t("teslaConnectionStatePluggedNotCharging");
  if (connectionState === "unplugged") {
    if (cableConnected === true) return t("teslaCableStatusConnected");
    return t("teslaConnectionStateUnplugged");
  }
  if (cableConnected === true) return t("teslaCableStatusConnected");
  if (cableConnected === false) return t("teslaCableStatusDisconnected");
  return t("teslaConnectionStateUnknown");
}

function formatTeslaControlSummary(teslaInfo = null) {
  const controlMode = String(teslaInfo?.controlMode || "unavailable").trim().toLowerCase();
  if (controlMode === "switch") return t("teslaControlStateSwitch");
  if (controlMode === "buttons") return t("teslaControlStateButtons");
  return t("teslaControlStateUnavailable");
}

function mergeTeslaControlInfo(baseInfo, controlPayload) {
  const controlState = controlPayload?.control_state || controlPayload || {};
  const observation = controlPayload?.observation || {};
  const charging = observation?.charging || {};
  const battery = observation?.battery || {};
  const fallbackControlMode = observation?.control_mode || "unavailable";
  const resolvedControlMode = controlState?.control_mode || fallbackControlMode || "unavailable";
  const resolvedControlAvailable = typeof controlState?.available === "boolean"
    ? controlState.available
    : (resolvedControlMode !== "unavailable");
  const resolvedChargingEnabled = typeof controlState?.charging_enabled === "boolean"
    ? controlState.charging_enabled
    : (typeof charging?.enabled === "boolean" ? charging.enabled : null);
  const resolvedChargeRequestedEnabled = typeof controlState?.charge_requested_enabled === "boolean"
    ? controlState.charge_requested_enabled
    : (typeof charging?.requested_enabled === "boolean" ? charging.requested_enabled : null);
  return {
    ...baseInfo,
    chargingW: toFiniteNumber(charging?.power_w),
    updatedAt: latestIsoTime(baseInfo?.updatedAt, controlPayload?.updated_at, charging?.entity?.last_updated, battery?.entity?.last_updated),
    currentA: toFiniteNumber(charging?.current_amps),
    currentUnit: "A",
    socPercent: toFiniteNumber(battery?.level_percent),
    teslaConnectionState: observation?.charging?.connection_state || null,
    teslaCableConnected: typeof charging?.cable_connected === "boolean" ? charging.cable_connected : null,
    controlAvailable: resolvedControlAvailable,
    controlMode: resolvedControlMode,
    chargingEnabled: resolvedChargingEnabled,
    chargeRequestedEnabled: resolvedChargeRequestedEnabled,
    canStart: Boolean(controlState?.can_start),
    canStop: Boolean(controlState?.can_stop),
    controlSwitchEntityId: controlState?.switch_entity?.entity_id || null,
    controlStartButtonEntityId: controlState?.start_button_entity?.entity_id || null,
    controlStopButtonEntityId: controlState?.stop_button_entity?.entity_id || null,
  };
}

function teslaInfoFromCombinedFlow(combinedFlow) {
  const rawTesla = combinedFlow?.raw?.tesla || {};
  const observation = rawTesla?.observation || {};
  const charging = observation?.charging || {};
  const battery = observation?.battery || {};
  const controlState = rawTesla?.control_state || {};
  return mergeTeslaControlInfo({
    chargingW: toFiniteNumber(combinedFlow?.metrics?.tesla_charge_power_w ?? charging?.power_w),
    entityId: null,
    friendlyName: null,
    updatedAt: latestIsoTime(combinedFlow?.updated_at, rawTesla?.executed_at_utc, battery?.entity?.last_updated),
    currentA: toFiniteNumber(combinedFlow?.metrics?.tesla_charge_current_amps ?? charging?.current_amps),
    currentEntityId: null,
    currentUnit: "A",
    socPercent: toFiniteNumber(combinedFlow?.metrics?.tesla_battery_soc_percent ?? battery?.level_percent),
    socEntityId: null,
    teslaConnectionState: combinedFlow?.metrics?.tesla_connection_state || charging?.connection_state || null,
    teslaCableConnected: typeof combinedFlow?.metrics?.tesla_cable_connected === "boolean"
      ? combinedFlow.metrics.tesla_cable_connected
      : (typeof charging?.cable_connected === "boolean" ? charging.cable_connected : null),
    controlAvailable: false,
    controlMode: "unavailable",
    chargingEnabled: null,
    chargeRequestedEnabled: null,
    canStart: false,
    canStop: false,
    controlSwitchEntityId: null,
    controlStartButtonEntityId: null,
    controlStopButtonEntityId: null,
  }, { control_state: controlState, observation });
}

function renderTeslaControlButton(teslaInfo = null) {
  const button = document.getElementById("combined-teslaChargingToggleBtn");
  if (!button) return;
  const controlAvailable = Boolean(teslaInfo?.controlAvailable);
  const chargingEnabled = teslaInfo?.chargingEnabled === true;
  let label = t("teslaControlUnavailable");
  if (teslaControlBusy) {
    label = t("teslaControlBusy");
  } else if (controlAvailable) {
    label = chargingEnabled ? t("teslaControlStop") : t("teslaControlStart");
  }
  button.textContent = label;
  button.disabled = teslaControlBusy || !controlAvailable;
  button.classList.toggle("active", controlAvailable && chargingEnabled);
}

function ensureTeslaCardUnifiedContent() {
  const socValue = document.getElementById("combined-teslaSocValue");
  if (!socValue) return;
  const stack = socValue.parentElement;
  if (!stack || !stack.classList.contains("soc-value-inside")) return;
  [
    "combined-teslaChargingPowerValue",
    "combined-teslaChargingCurrentValue",
    "combined-teslaConnectionValue",
    "combined-teslaChargingToggleBtn",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (el && el.parentElement !== stack) stack.appendChild(el);
  });
}

function buildCombinedFlowMetrics(combinedFlow) {
  const metrics = combinedFlow?.metrics || {};
  const solarW = toFiniteNumber(metrics.solar_primary_w);
  const solar2W = toFiniteNumber(metrics.solar_secondary_w);
  const gridW = toFiniteNumber(metrics.grid_w);
  const battery1W = toFiniteNumber(metrics.battery1_w);
  const battery2W = toFiniteNumber(metrics.battery2_w);
  const inverter1W = toFiniteNumber(metrics.inverter1_w);
  const inverter2W = toFiniteNumber(metrics.inverter2_w);
  const inverter1Status = metrics.inverter1_status ?? null;
  const inverter2Status = metrics.inverter2_status ?? null;
  const solarKind = dataKindFromSource(metrics.pv_source, "real");
  const gridKind = dataKindFromSource(metrics.grid_source, "real");
  const battery1Kind = dataKindFromSource(metrics.battery1_source, "real");
  const battery2Kind = dataKindFromSource(metrics.battery2_source, "real");
  const inverter1Kind = dataKindFromSource(metrics.inverter1_power_source, "real");
  const inverter2Kind = dataKindFromSource(metrics.inverter2_power_source, "real");
  const totalLoadW = toFiniteNumber(metrics.load_w);
  const solarToInverter1W = solarW !== null ? Math.max(solarW, 0) : 0;
  const availableCount = [solarW, gridW, inverter1W, inverter2W, totalLoadW].filter((v) => v !== null).length;
  return {
    solarW,
    solar2W,
    gridW,
    battery1W,
    battery2W,
    inverter1W,
    inverter2W,
    inverter1Status,
    inverter2Status,
    totalLoadW,
    homeLoadW: totalLoadW,
    battery1Soc: toFiniteNumber(metrics.battery1_soc_percent),
    battery2Soc: toFiniteNumber(metrics.battery2_soc_percent),
    solarToInverter1W,
    availableCount,
    dataKinds: {
      solar: solarKind,
      grid: gridKind,
      battery1: battery1Kind,
      battery2: battery2Kind,
      inverter1: inverter1Kind,
      inverter2: inverter2Kind,
      totalLoad: "calculated",
      homeLoad: "calculated",
      teslaCurrent: "real",
      teslaSoc: "real",
      inverterRatio: "calculated",
      solarToInverter1: "estimate",
    },
    sources: {
      solar: metrics.pv_source || "unavailable",
      grid: metrics.grid_source || "unavailable",
      battery1: metrics.battery1_source || "unavailable",
      battery2: metrics.battery2_source || "unavailable",
      load: metrics.load_source || "unavailable",
    },
  };
}

function renderCombinedEnergyFlow(combinedFlow, teslaInfo = null) {
  const combined = buildCombinedFlowMetrics(combinedFlow);
  const {
    solarW,
    solar2W,
    gridW,
    battery1W,
    battery2W,
    inverter1W,
    inverter2W,
    inverter1Status,
    inverter2Status,
    totalLoadW,
    battery1Soc,
    battery2Soc,
    solarToInverter1W,
    sources,
    dataKinds,
  } = combined;
  const teslaChargingW = toFiniteNumber(teslaInfo?.chargingW);
  const teslaCurrentA = toFiniteNumber(teslaInfo?.currentA);
  const teslaSoc = toFiniteNumber(teslaInfo?.socPercent);
  let homeLoadW = totalLoadW;
  if (homeLoadW !== null) {
    const teslaW = teslaChargingW === null ? 0 : teslaChargingW;
    homeLoadW = Math.max(0, homeLoadW - teslaW);
    if (Math.abs(homeLoadW) <= BALANCE_TOLERANCE_W) homeLoadW = 0;
  }

  const combinedLoadError = Boolean(combinedFlow?.__load_error);
  const combinedPending = !combinedLoadError && solarW === null && gridW === null;
  const hasCombinedBase = solarW !== null && gridW !== null;
  setSystemLoadMeta("combined", {
    phase: combinedPending ? "loading" : (hasCombinedBase ? "done" : "failed"),
    updatedAt: latestIsoTime(combinedFlow?.updated_at, teslaInfo?.updatedAt),
    quality: hasCombinedBase ? getFlowQuality(combinedFlow, Number(combinedFlow?.metrics?.matched_entities) || 0) : "failed",
    count: combined.availableCount,
  });

  setHtml("combined-solarPowerValue", formatValueWithDataKindHtml(formatPowerKwFromWatts(solarW), dataKinds.solar));
  setHtml("combined-gridPowerValue", formatValueWithDataKindHtml(formatPowerKwFromWatts(gridW === null ? null : Math.abs(gridW)), dataKinds.grid));
  setHtml(
    "combined-inverter1RatioValue",
    formatValueWithDataKindHtml(formatInverterConversion(inverter1W, battery1W, solarToInverter1W), dataKinds.inverterRatio),
  );
  setHtml(
    "combined-inverter2RatioValue",
    formatValueWithDataKindHtml(formatInverterConversion(inverter2W, battery2W, solar2W), dataKinds.inverterRatio),
  );
  setHtml("combined-loadPowerValue", formatValueWithDataKindHtml(formatPowerKwFromWatts(homeLoadW), dataKinds.homeLoad));
  setHtml(
    "combined-teslaChargingPowerValue",
    formatValueWithDataKindHtml(formatTeslaPowerValue(teslaChargingW), "real"),
  );
  setHtml(
    "combined-teslaChargingCurrentValue",
    formatValueWithDataKindHtml(formatTeslaCurrentValue(teslaCurrentA, teslaInfo?.currentUnit || "A"), dataKinds.teslaCurrent),
  );
  setText("combined-teslaConnectionValue", formatTeslaConnectionSummary(teslaInfo));
  renderBatterySocDisplay({
    system: null,
    soc: teslaSoc,
    batteryW: null,
    socValueId: "combined-teslaSocValue",
    socFillId: "combined-teslaSocFill",
    socDataKind: dataKinds.teslaSoc,
  });
  ensureTeslaCardUnifiedContent();
  renderTeslaControlButton(teslaInfo);
  setText("combined-switchboardValue", "-");
  setText("combined-inverter1State", getSajDashboardModeText() || inverterStateText(inverter1Status));
  setText("combined-inverter2State", inverterStateText(inverter2Status));

  setNodeSourceTip("combined-solarPowerValue", formatMetricSourceText("saj", "solar", sources.solar));
  setNodeSourceTip("combined-gridPowerValue", formatMetricSourceText("saj", "grid", sources.grid));
  setNodeSourceTip(
    "combined-loadPowerValue",
    currentLang === "zh"
      ? `来源: 计算 ${sources.load}\n说明: 家庭负载(不含 Tesla) = 总负载 - 特斯拉充电`
      : `Source: Calculated ${sources.load}\nNote: Home load (excluding Tesla) = total load - Tesla charging`,
  );
  setNodeSourceTip("combined-teslaChargingPowerValue", null);
  setNodeSourceTip("combined-teslaChargingCurrentValue", null);
  setNodeSourceTip(
    "combined-teslaConnectionValue",
    currentLang === "zh"
      ? "显示特斯拉是否插了充电枪，以及当前连接/充电状态。"
      : "Shows whether the Tesla is plugged in and its current connection/charging state.",
  );
  setNodeSourceTip("combined-teslaSocValue", null);

  const solarActive = solarW !== null && solarW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  setText("combined-solarState", solarActive ? t("stateProducing") : t("stateIdle"));
  const gridActive = gridW !== null && Math.abs(gridW) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const gridImport = gridW !== null && gridW > 0;
  setText("combined-gridState", gridActive ? (gridImport ? t("stateImporting") : t("stateExporting")) : t("stateIdle"));
  const switchboardActive = solarActive || gridActive || (totalLoadW !== null && totalLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W);
  setText("combined-switchboardState", switchboardActive ? t("switchboardStateActive") : t("switchboardStateIdle"));
  const loadActive = homeLoadW !== null && homeLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const teslaChargingActive = teslaChargingW !== null && teslaChargingW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const totalLoadActive = totalLoadW !== null && totalLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  setText("combined-loadState", loadActive ? t("stateConsuming") : t("stateIdle"));
  setFlowLine("combined-lineSwitchboardToTotalLoad", totalLoadActive, false);
  setFlowLine("combined-lineSwitchboardToHomeLoad", loadActive, false);
  setFlowLine("combined-lineSwitchboardToTeslaB", teslaChargingActive, false);

  const battery1Active = battery1W !== null && Math.abs(battery1W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const battery1Discharging = battery1W !== null && battery1W > 0;
  setFlowLine(
    "combined-lineBattery1ToInverter1",
    battery1Active,
    !battery1Discharging,
    battery1Discharging ? "batteryDischarge" : "batteryCharge",
  );

  const battery2Active = battery2W !== null && Math.abs(battery2W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const battery2Discharging = battery2W !== null && battery2W > 0;
  setFlowLine(
    "combined-lineBattery2ToInverter2",
    battery2Active,
    !battery2Discharging,
    battery2Discharging ? "batteryDischarge" : "batteryCharge",
  );

  const inverter1Active = inverter1W !== null && Math.abs(inverter1W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const inverter1Exporting = inverter1W !== null && inverter1W > 0;
  const inverter2Active = inverter2W !== null && Math.abs(inverter2W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const inverter2Exporting = inverter2W !== null && inverter2W > 0;
  setFlowLine("combined-lineInverter1ToSwitchboardB", inverter1Active, !inverter1Exporting);
  setFlowLine("combined-lineInverter2ToSwitchboardB", inverter2Active, !inverter2Exporting);

  const solarToInverter1Active = solarToInverter1W >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  setFlowLine("combined-lineSolarToInverter1B", solarToInverter1Active, false);
  setFlowLine(
    "combined-lineGridToSwitchboard",
    gridActive,
    !gridImport,
    gridImport ? "gridImport" : "gridExport",
  );

  renderBatterySocDisplay({
    system: "saj",
    soc: battery1Soc,
    batteryW: battery1W,
    socValueId: "combined-battery1SocValue",
    socFillId: "combined-battery1SocFill",
    energyValueId: "combined-battery1EnergyValue",
    usableValueId: "combined-battery1UsableValue",
    runtimeValueId: "combined-battery1RuntimeValue",
    socDataKind: "real",
    energyDataKind: "estimate",
    usableDataKind: "estimate",
    runtimeDataKind: "estimate",
  });
  renderBatterySocDisplay({
    system: "solplanet",
    soc: battery2Soc,
    batteryW: battery2W,
    socValueId: "combined-battery2SocValue",
    socFillId: "combined-battery2SocFill",
    energyValueId: "combined-battery2EnergyValue",
    usableValueId: "combined-battery2UsableValue",
    runtimeValueId: "combined-battery2RuntimeValue",
    socDataKind: "real",
    energyDataKind: "estimate",
    usableDataKind: "estimate",
    runtimeDataKind: "estimate",
  });

  const gridHigh = gridW !== null && Math.abs(gridW) > 15000;
  const gridPowerEl = document.getElementById("combined-gridPowerValue");
  const gridStateEl = document.getElementById("combined-gridState");
  if (gridPowerEl) gridPowerEl.classList.toggle("alert-high", gridHigh);
  if (gridStateEl) gridStateEl.classList.toggle("alert-high", gridHigh);

  setFlowValueLabel("combined-flowLabelGridToSwitchboard", gridW, gridActive, dataKinds.grid);
  setFlowValueLabel("combined-flowLabelSolarToInverter1", solarToInverter1W, solarToInverter1Active, dataKinds.solarToInverter1);
  setFlowValueLabel("combined-flowLabelSwitchboardToTotalLoad", totalLoadW, totalLoadActive, dataKinds.totalLoad);
  setFlowValueLabel("combined-flowLabelSwitchboardToHomeLoad", homeLoadW, loadActive, dataKinds.homeLoad);
  setFlowValueLabel("combined-flowLabelSwitchboardToTesla", teslaChargingW, teslaChargingActive, dataKinds.teslaCurrent);
  setFlowValueLabel("combined-flowLabelBattery1ToInverter1", battery1W, battery1Active, dataKinds.battery1);
  setFlowValueLabel("combined-flowLabelBattery2ToInverter2", battery2W, battery2Active, dataKinds.battery2);
  setFlowValueLabel("combined-flowLabelInverter1ToSwitchboard", inverter1W, inverter1Active, dataKinds.inverter1);
  setFlowValueLabel("combined-flowLabelInverter2ToSwitchboard", inverter2W, inverter2Active, dataKinds.inverter2);
  const combinedDiagram = flowDiagrams.byBoard.get("energyFlowCombined");
  const switchboardMetrics = combinedDiagram?.getNodeMetrics?.("combined-switchboardNode");
  if (switchboardMetrics) {
    const guideInset = 12;
    const leftDistancePx = Math.round(switchboardMetrics.centerX - guideInset);
    const rightDistancePx = Math.round((switchboardMetrics.viewportWidth - guideInset) - switchboardMetrics.centerX);
  }

  const battery1SocText = battery1Soc === null ? "-" : formatValueWithDataKindHtml(`${Math.max(0, Math.min(100, battery1Soc)).toFixed(0)}%`, "real");
  const battery2SocText = battery2Soc === null ? "-" : formatValueWithDataKindHtml(`${Math.max(0, Math.min(100, battery2Soc)).toFixed(0)}%`, "real");
  const teslaSuffix = t("teslaChargingFormulaNote", {
    total: formatPowerKwFromWatts(totalLoadW),
    home: formatPowerKwFromWatts(homeLoadW),
    tesla: formatPowerKwFromWatts(teslaChargingW),
  });
  const formula =
    `${escapeHtml(t("balanceFormulaLabel"))}: ` +
    `${formatValueWithDataKindHtml(formatSignedKwFromWatts(inverter1W), dataKinds.inverter1)} + ${formatValueWithDataKindHtml(formatSignedKwFromWatts(inverter2W), dataKinds.inverter2)} + ${formatValueWithDataKindHtml(formatSignedKwFromWatts(gridW), dataKinds.grid)} = ${formatValueWithDataKindHtml(formatPowerKwFromWatts(totalLoadW), dataKinds.totalLoad)} ` +
    `(SOC1 ${battery1SocText}, SOC2 ${battery2SocText})` +
    ` · Solar→Inverter1 ${formatValueWithDataKindHtml(formatPowerKwFromWatts(solarToInverter1W), dataKinds.solarToInverter1)}` +
    ` · ${formatValueWithDataKindHtml(teslaSuffix, dataKinds.homeLoad)}`;
  setHtml("combined-loadFormulaText", formula);
}

function renderSummary(payload) {
  const { combinedFlow, collectorStatus } = payload;
  const tesla = teslaInfoFromCombinedFlow(combinedFlow);
  const combinedCount = combinedFlow?.metrics?.matched_entities ?? 0;
  setSystemLoadMeta("combined", {
    quality: getFlowQuality(combinedFlow, Number(combinedCount) || 0),
    count: Number(combinedCount) || 0,
  });
  renderCombinedEnergyFlow(combinedFlow, tesla);
  renderCombinedDebug(combinedFlow, collectorStatus);
}

function formatCollectorSystemStatus(label, systemState) {
  const state = systemState || {};
  const review = state.last_round_review || {};
  const result = state.last_round_result || {};
  const successAt = formatDateTimeWithAgo(state.last_success_at || null);
  const stored = result.stored_sample === true ? "stored" : (result.stored_sample === false ? "not-stored" : "-");
  const reason = result.reason || result.error || "-";
  const reviewSummary = `${Number(review.success_count || 0)}/${Number(review.attempted_count || 0)}`;
  return `${label}: ${successAt}, review ${reviewSummary}, ${stored}, ${reason}`;
}

function renderCombinedDebug(combinedFlow, collectorStatus) {
  const sourceType = combinedFlow?.source?.type || "-";
  const storageBacked = combinedFlow?.storage_backed ? "true" : "false";
  const stale = combinedFlow?.stale ? `true (${combinedFlow?.stale_reason || "-"})` : "false";
  const sampleAge = combinedFlow?.sample_age_seconds === null || combinedFlow?.sample_age_seconds === undefined
    ? "-"
    : formatMaybeNumber(combinedFlow.sample_age_seconds, 1);
  const kvCount = combinedFlow?.kv_item_count === null || combinedFlow?.kv_item_count === undefined
    ? "-"
    : String(combinedFlow.kv_item_count);
  setText(
    "combinedDebugMeta",
    t("combinedDebugMeta", {
      source: sourceType,
      storageBacked,
      stale,
      sampleAge,
      kvCount,
    }),
  );

  const systems = collectorStatus?.systems || {};
  setText(
    "combinedCollectorMeta",
    t("combinedCollectorMeta", {
      saj: formatCollectorSystemStatus("saj", systems.saj),
      solplanet: formatCollectorSystemStatus("solplanet", systems.solplanet),
      combined: formatCollectorSystemStatus("combined", systems.combined),
    }),
  );
  setText("combinedDebugUpdatedAt", formatUpdatedAt(combinedFlow?.updated_at || collectorStatus?.updated_at || null));

  const debugPre = document.getElementById("combinedDebugPre");
  if (debugPre) {
    debugPre.textContent = JSON.stringify(
      {
        collector_status: collectorStatus || null,
        combined_flow: combinedFlow || null,
      },
      null,
      2,
    );
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

function localDateParts(dateText) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(dateText || ""));
  if (!match) return null;
  const year = Number(match[1]);
  const monthIndex = Number(match[2]) - 1;
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(monthIndex) || !Number.isInteger(day)) return null;
  return { year, monthIndex, day };
}

function toUtcIsoFromDateOnly(dateText) {
  const parts = localDateParts(dateText);
  if (!parts) return null;
  const d = new Date(parts.year, parts.monthIndex, parts.day, 0, 0, 0, 0);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toUtcIsoFromDateEndExclusive(dateText) {
  const parts = localDateParts(dateText);
  if (!parts) return null;
  const d = new Date(parts.year, parts.monthIndex, parts.day + 1, 0, 0, 0, 0);
  if (Number.isNaN(d.getTime())) return null;
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

function getLocalDateText(offsetDays = 0) {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function getWeekInfo(anchorDateText) {
  const anchorParts = localDateParts(anchorDateText);
  const now = new Date();
  const anchor = anchorParts
    ? new Date(anchorParts.year, anchorParts.monthIndex, anchorParts.day, 0, 0, 0, 0)
    : new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  const day = anchor.getDay();
  const diffToMonday = day === 0 ? -6 : 1 - day;
  const monday = new Date(anchor);
  monday.setDate(anchor.getDate() + diffToMonday);
  const endExclusive = new Date(monday);
  endExclusive.setDate(monday.getDate() + 7);
  const sunday = new Date(monday);
  sunday.setDate(monday.getDate() + 6);

  const thursday = new Date(monday);
  thursday.setDate(monday.getDate() + 3);
  const year = thursday.getFullYear();
  const jan4 = new Date(year, 0, 4, 0, 0, 0, 0);
  const jan4Day = jan4.getDay() || 7;
  const firstThursday = new Date(year, 0, 4 + (4 - jan4Day), 0, 0, 0, 0);
  const week = 1 + Math.floor((thursday.getTime() - firstThursday.getTime()) / (7 * 86400000));

  const localDateText = (date) => {
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const dayText = String(date.getDate()).padStart(2, "0");
    return `${y}-${m}-${dayText}`;
  };

  return {
    anchor: localDateText(anchor),
    week,
    monday: localDateText(monday),
    sunday: localDateText(sunday),
    startUtc: monday.toISOString(),
    endUtc: endExclusive.toISOString(),
  };
}

function parseSamplingRelativeInput(rawValue) {
  const text = String(rawValue || "").trim().toLowerCase();
  const match = /^(-)?(\d+)\s*(mo|m|h|d)$/.exec(text);
  if (!match) return null;
  const amount = Number(match[2]);
  const unit = match[3];
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return { amount, unit, normalized: `-${amount}${unit}` };
}

function applyRelativeOffset(date, relative) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime()) || !relative) return null;
  const next = new Date(date.getTime());
  if (relative.unit === "m") {
    next.setMinutes(next.getMinutes() - relative.amount);
    return next;
  }
  if (relative.unit === "h") {
    next.setHours(next.getHours() - relative.amount);
    return next;
  }
  if (relative.unit === "d") {
    next.setDate(next.getDate() - relative.amount);
    return next;
  }
  if (relative.unit === "mo") {
    next.setMonth(next.getMonth() - relative.amount);
    return next;
  }
  return null;
}

function getSamplingRange() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const dayText = samplingRangeState.day || getLocalDateText();
  const weekText = samplingRangeState.week || dayText;
  const monthNumber = Number(samplingRangeState.month || `${new Date().getMonth() + 1}`);
  const monthYear = Number(samplingRangeState.monthYear || new Date().getFullYear());
  const relativeText = samplingRangeState.relative || "";
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
    const start = new Date(monthYear, safeMonth - 1, 1, 0, 0, 0, 0);
    const end = new Date(monthYear, safeMonth, 1, 0, 0, 0, 0);
    return {
      mode,
      startUtc: start.toISOString(),
      endUtc: end.toISOString(),
      label: `${monthYear}-${String(safeMonth).padStart(2, "0")}`,
    };
  }
  if (mode === "relative") {
    const relative = parseSamplingRelativeInput(relativeText);
    const end = new Date();
    const start = relative ? applyRelativeOffset(end, relative) : null;
    return {
      mode,
      startUtc: start ? start.toISOString() : null,
      endUtc: end.toISOString(),
      label: relative?.normalized || relativeText || "-",
      invalid: !relative || !start || start >= end,
      invalidReason: !relative ? t("samplingRelativeInvalid") : null,
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
      label: `${startLabel} ~ ${endLabel}`,
      invalid: !startUtc || !endUtc || startUtc >= endUtc,
    };
  }
  if (mode === "custom_datetime") {
    const startUtc = toUtcIsoFromDateTimeLocal(startDateTime);
    const endUtc = toUtcIsoFromDateTimeLocal(endDateTime);
    const startLabel = startDateTime ? startDateTime.replace("T", " ") : "-";
    const endLabel = endDateTime ? endDateTime.replace("T", " ") : "-";
    return {
      mode,
      startUtc,
      endUtc,
      label: `${startLabel} ~ ${endLabel}`,
      invalid: !startUtc || !endUtc || startUtc >= endUtc,
    };
  }
  const startUtc = toUtcIsoFromDateOnly(dayText);
  const endUtc = toUtcIsoFromDateEndExclusive(dayText);
  return {
    mode: "day",
    startUtc,
    endUtc,
    label: dayText,
  };
}

function renderSamplingRangeInputContainer() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const container = document.getElementById("samplingRangeInputContainer");
  if (!container) return;

  if (mode === "week") {
    const info = getWeekInfo(samplingRangeState.week || getLocalDateText());
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
    const year = Number(samplingRangeState.monthYear || new Date().getFullYear());
    const selectedMonth = Number(samplingRangeState.month || `${new Date().getMonth() + 1}`);
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
  } else if (mode === "relative") {
    container.innerHTML = `
      <div class="sampling-relative-grid">
        <label>
          ${t("samplingRelativeLabel")}
          <div id="samplingRelativeField" class="sampling-field">
            <input
              id="samplingRelativeInput"
              type="text"
              inputmode="text"
              spellcheck="false"
              placeholder="${t("samplingRelativePlaceholder")}"
              value="${escapeHtml(samplingRangeState.relative || "")}"
            />
          </div>
        </label>
        <div class="sampling-relative-presets">
          <button id="samplingRelativePresetMinuteBtn" type="button" class="btn secondary">${t("samplingRelativePresetMinute")}</button>
          <button id="samplingRelativePresetHourBtn" type="button" class="btn secondary">${t("samplingRelativePresetHour")}</button>
          <button id="samplingRelativePresetDayBtn" type="button" class="btn secondary">${t("samplingRelativePresetDay")}</button>
          <button id="samplingRelativePresetMonthBtn" type="button" class="btn secondary">${t("samplingRelativePresetMonth")}</button>
        </div>
        <p class="muted sampling-relative-help">${t("samplingRelativeHelp")}</p>
      </div>
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
      const parts = localDateParts(samplingRangeState.week || getLocalDateText());
      const current = parts
        ? new Date(parts.year, parts.monthIndex, parts.day, 0, 0, 0, 0)
        : new Date();
      current.setDate(current.getDate() + deltaDays);
      samplingRangeState.week = `${current.getFullYear()}-${String(current.getMonth() + 1).padStart(2, "0")}-${String(current.getDate()).padStart(2, "0")}`;
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
  if (mode === "relative") {
    const input = document.getElementById("samplingRelativeInput");
    const applyRelative = async (value) => {
      samplingRangeState.relative = String(value || "").trim();
      samplingPager.page = 1;
      await loadSampling();
    };
    if (input) {
      input.addEventListener("change", async () => {
        await applyRelative(input.value);
      });
      input.addEventListener("keydown", async (event) => {
        if (event.key !== "Enter") return;
        event.preventDefault();
        await applyRelative(input.value);
      });
    }
    const bindPreset = (id, value) => {
      const button = document.getElementById(id);
      if (!button) return;
      button.addEventListener("click", async () => {
        if (input) input.value = value;
        await applyRelative(value);
      });
    };
    bindPreset("samplingRelativePresetMinuteBtn", "-1m");
    bindPreset("samplingRelativePresetHourBtn", "-1h");
    bindPreset("samplingRelativePresetDayBtn", "-1d");
    bindPreset("samplingRelativePresetMonthBtn", "-1mo");
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
  if (system && system !== "overall") params.set("system", system);
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
    const sampledAt = formatSamplingDateTime(item.sampled_at_utc);
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

function buildWorkerLogsUrl() {
  const params = new URLSearchParams();
  const category = document.getElementById("workerLogsCategorySelect")?.value || "all";
  if (category && category !== "all") params.set("category", category);
  params.set("page", String(workerLogsPager.page));
  params.set("page_size", "100");
  return `/api/worker/logs?${params.toString()}`;
}

function compactWorkerLogResultText(value, head = 36, tail = 24) {
  const text = String(value || "").replaceAll(/\s+/g, " ").trim();
  if (!text) return "-";
  if (text.length <= head + tail + 3) return text;
  return `${text.slice(0, head)}...${text.slice(-tail)}`;
}

function workerSourcePatternClass(item) {
  const system = String(item?.system || "").trim().toLowerCase();
  const service = String(item?.service || "").trim().toLowerCase();
  if (service === "tesla_home_assistant_collection") return "worker-service-tesla";
  if (system === "combined" || service === "combined_assembly") return "worker-service-combined";
  if (system === "saj") return "worker-service-saj";
  if (system === "solplanet") return "worker-service-solplanet";
  return "";
}

function workerLogStatusPresentation(item) {
  const rawStatus = String(item?.status || "").trim().toLowerCase();
  if (rawStatus === "pending") return { text: t("workerLogsStatusPending"), className: "worker-status-pending" };
  if (rawStatus === "skipped") return { text: t("workerLogsStatusSkipped"), className: "worker-status-skipped" };
  if (rawStatus === "applied") return { text: t("workerLogsStatusApplied"), className: "worker-status-ok" };
  if (rawStatus === "noop") return { text: t("workerLogsStatusNoop"), className: "worker-status-skipped" };
  if (rawStatus === "ok") return { text: t("workerLogsStatusOk"), className: "worker-status-ok" };
  if (rawStatus === "timeout") return { text: t("workerLogsStatusTimeout"), className: "worker-status-failed" };
  if (rawStatus === "failed") return { text: t("workerLogsStatusFailed"), className: "worker-status-failed" };
  return item?.ok
    ? { text: t("workerLogsStatusOk"), className: "worker-status-ok" }
    : { text: t("workerLogsStatusFailed"), className: "worker-status-failed" };
}

function openWorkerLogDetailModal(item) {
  const statusPresentation = workerLogStatusPresentation(item || {});
  setText(
    "workerLogDetailMeta",
    t("workerLogDetailMeta", {
      service: String(item?.service || "-"),
      status: statusPresentation.text,
      time: formatDateTimeWithAgo(item?.requested_at_utc || null),
    }),
  );
  const detailRows = [
    ["Time (UTC)", String(item?.requested_at_utc || "-")],
    ["Run ID", String(item?.round_id || "-")],
    ["System", String(item?.system || "-")],
    ["Service", String(item?.service || "-")],
    ["Method", String(item?.method || "-")],
    ["API Link", String(item?.api_link || "-")],
    ["Status", statusPresentation.text],
    ["OK", item?.ok == null ? "-" : String(Boolean(item.ok))],
    ["Status Code", item?.status_code == null ? "-" : String(item.status_code)],
    ["Duration (ms)", item?.duration_ms == null ? "-" : String(item.duration_ms)],
    ["Error", String(item?.error_text || "-")],
    ["Result", String(item?.result_text || "-")],
  ];
  const body = document.getElementById("workerLogDetailBody");
  if (body) {
    body.innerHTML = detailRows
      .map(
        ([label, value]) => `
          <tr>
            <th>${escapeHtml(label)}</th>
            <td><div class="worker-log-detail-value">${escapeHtml(value)}</div></td>
          </tr>
        `,
      )
      .join("");
  }
  setWorkerLogDetailModalVisible(true);
}

function buildWorkerFailureLogUrl(before = 0) {
  const params = new URLSearchParams();
  params.set("limit", String(WORKER_FAILURE_LOG_PAGE_SIZE));
  params.set("before", String(Math.max(0, Number(before) || 0)));
  return `/api/worker/failure-log?${params.toString()}`;
}

function renderWorkerLogsRows(items) {
  const body = document.getElementById("workerLogsBody");
  if (!body) return;
  body.innerHTML = "";
  const roundColorMap = new Map();
  let nextRoundColor = 1;
  for (const item of items || []) {
    const tr = document.createElement("tr");
    tr.classList.add("worker-log-row");
    const roundId = String(item.round_id || "").trim();
    let roundColorClass = "";
    if (roundId) {
      if (!roundColorMap.has(roundId)) {
        roundColorMap.set(roundId, `worker-round-color-${nextRoundColor}`);
        nextRoundColor = nextRoundColor >= 7 ? 1 : nextRoundColor + 1;
      }
      roundColorClass = roundColorMap.get(roundId) || "";
    }
    const serviceClass = workerSourcePatternClass(item);
    if (roundColorClass) tr.classList.add(roundColorClass);
    const sampledAt = formatDateTimeWithAgo(item.requested_at_utc);
    const statusPresentation = workerLogStatusPresentation(item);
    const resultText = item.error_text || item.result_text || "";
    const resultPreview = compactWorkerLogResultText(resultText);
    tr.innerHTML = `
      <td>${escapeHtml(sampledAt)}</td>
      <td class="worker-link" title="${escapeHtml(item.round_id || "-")}">${escapeHtml(item.round_id || "-")}</td>
      <td>${escapeHtml(item.system || "-")}</td>
      <td class="${escapeHtml(serviceClass)}">${escapeHtml(item.service || "-")}</td>
      <td>${escapeHtml(item.method || "-")}</td>
      <td class="worker-link" title="${escapeHtml(item.api_link || "-")}">${escapeHtml(item.api_link || "-")}</td>
      <td class="${escapeHtml(statusPresentation.className)}">${escapeHtml(statusPresentation.text)}</td>
      <td>${escapeHtml(formatMaybeNumber(item.duration_ms, 1))}</td>
      <td><pre class="worker-result-pre" title="${escapeHtml(resultText || "-")}">${escapeHtml(resultPreview)}</pre></td>
    `;
    tr.addEventListener("click", () => {
      openWorkerLogDetailModal(item);
    });
    body.appendChild(tr);
  }
}

function renderWorkerLogsPage(payload) {
  const totalPages = Math.max(1, Math.ceil((payload.total || 0) / (payload.page_size || 100)));
  setText("workerLogsCount", t("workerLogsTotal", { total: payload.total || 0 }));
  setText(
    "workerLogsPageInfo",
    t("workerLogsPageInfo", {
      page: payload.page || 1,
      totalPages,
      count: payload.count || 0,
    }),
  );
  setText("workerLogsUpdatedAt", formatUpdatedAt(payload.updated_at || null));
  document.getElementById("workerLogsPrevPageBtn").disabled = !Boolean(payload.has_prev);
  document.getElementById("workerLogsNextPageBtn").disabled = !Boolean(payload.has_next);
  renderWorkerLogsRows(payload.items || []);
}

function renderWorkerLogsConfigMeta(configPayload) {
  const host = String(configPayload?.solplanet_dongle_host || "").trim() || "-";
  setText("workerLogsConfigMeta", t("workerLogsConfigMeta", { host }));
}

function renderWorkerFailureLogPage(payload, { appendOlder = false } = {}) {
  const currentLines = appendOlder ? stateCache.lastWorkerFailureLog?.lines || [] : [];
  const mergedLines = appendOlder ? [...(payload.lines || []), ...currentLines] : payload.lines || [];
  const mergedPayload = {
    ...payload,
    lines: mergedLines,
    from_line: mergedLines[0]?.number ?? null,
    to_line: mergedLines[mergedLines.length - 1]?.number ?? null,
  };
  stateCache.lastWorkerFailureLog = mergedPayload;
  workerFailureLogState.before = Number(payload.next_before || mergedLines.length || 0);
  workerFailureLogState.hasMore = Boolean(payload.has_more);
  setText(
    "workerFailureLogMeta",
    t("failureLogMeta", {
      path: payload.path || "-",
      fromLine: mergedPayload.from_line ?? "-",
      toLine: mergedPayload.to_line ?? "-",
      total: payload.total_lines || 0,
    }),
  );
  setText("workerFailureLogCount", t("failureLogShowing", { count: mergedLines.length, total: payload.total_lines || 0 }));
  setText("workerFailureLogUpdatedAt", formatUpdatedAt(payload.updated_at || null));
  const pre = document.getElementById("workerFailureLogPre");
  if (pre) {
    pre.textContent = mergedLines.length
      ? mergedLines.map((item) => `${String(item.number).padStart(6, " ")} | ${item.text}`).join("\n")
      : t("failureLogEmpty");
  }
  const loadMoreBtn = document.getElementById("workerFailureLogLoadMoreBtn");
  if (loadMoreBtn) loadMoreBtn.disabled = !Boolean(payload.has_more);
}

function renderSamplingStatus(status) {
  const sizeMb = status?.db_size_bytes ? (Number(status.db_size_bytes) / (1024 * 1024)).toFixed(2) : "0.00";
  const estMb = status?.estimated_mb_per_day_total ?? 0;
  const selectedSystem = document.getElementById("samplingSystemSelect")?.value || "overall";
  let interval = status?.sample_interval_seconds;
  if (selectedSystem === "saj" && status?.saj_sample_interval_seconds !== undefined) {
    interval = status.saj_sample_interval_seconds;
  }
  if (selectedSystem === "solplanet" && status?.solplanet_sample_interval_seconds !== undefined) {
    interval = status.solplanet_sample_interval_seconds;
  }
  if (selectedSystem === "overall") {
    const sajInterval = status?.saj_sample_interval_seconds;
    const solplanetInterval = status?.solplanet_sample_interval_seconds;
    interval =
      sajInterval !== undefined && solplanetInterval !== undefined ? `${sajInterval}/${solplanetInterval}` : interval ?? "-";
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
  const updatedAt = formatSamplingDateTime(status?.last_sample_utc);
  setText("samplingUpdatedAt", `${t("updatedAt")}: ${updatedAt}`);
}

function renderSamplingUsage(usage, rangeLabel) {
  const system = formatSamplingSystemLabel(usage?.system || "-");
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

function formatEnergyKwhText(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${formatTrimmedDecimal(n, 3)} kWh`;
}

function formatSamplingSystemLabel(system) {
  if (system === "overall") return t("samplingSystemOverall");
  if (system === "solplanet") return "Solplanet";
  if (system === "saj") return "SAJ";
  return String(system || "-");
}

function getSamplingChartSeriesMeta(system) {
  return system === "overall" ? SAMPLING_OVERALL_SERIES : SAMPLING_SERIES;
}

function combineSamplingUsageForOverall(usageBySystem) {
  const sajUsage = usageBySystem?.saj || null;
  const solplanetUsage = usageBySystem?.solplanet || null;
  const sajEnergy = sajUsage?.energy_kwh || {};
  const solplanetEnergy = solplanetUsage?.energy_kwh || {};
  const sajHasData = Number(sajUsage?.samples || 0) >= 2;
  const solplanetHasData = Number(solplanetUsage?.samples || 0) >= 2;
  return {
    system: "overall",
    samples: sajHasData || solplanetHasData ? 2 : 0,
    energy_kwh: {
      home_load: (sajHasData ? Number(sajEnergy.home_load || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.home_load || 0) : 0),
      solar_generation:
        (sajHasData ? Number(sajEnergy.solar_generation || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.solar_generation || 0) : 0),
      grid_import: (sajHasData ? Number(sajEnergy.grid_import || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.grid_import || 0) : 0),
      grid_export: (sajHasData ? Number(sajEnergy.grid_export || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.grid_export || 0) : 0),
      battery_charge:
        (sajHasData ? Number(sajEnergy.battery_charge || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.battery_charge || 0) : 0),
      battery_discharge:
        (sajHasData ? Number(sajEnergy.battery_discharge || 0) : 0) +
        (solplanetHasData ? Number(solplanetEnergy.battery_discharge || 0) : 0),
    },
  };
}

function buildSamplingTotalsRows(usageBySystem, selectedSystem) {
  const sajUsage = usageBySystem?.saj || null;
  const solplanetUsage = usageBySystem?.solplanet || null;
  const sajEnergy = sajUsage?.energy_kwh || {};
  const solplanetEnergy = solplanetUsage?.energy_kwh || {};
  const sajHasData = Number(sajUsage?.samples || 0) >= 2;
  const solplanetHasData = Number(solplanetUsage?.samples || 0) >= 2;

  if (selectedSystem === "overall") {
    return [
      {
        scope: t("samplingTotalsScopeOverall"),
        title: t("samplingOverallMetricPv"),
        leftLabel: "",
        leftValue: 0,
        leftKind: "pv",
        rightLabel: t("samplingOverallMetricPv"),
        rightValue: sajHasData || solplanetHasData ? Number(sajEnergy.solar_generation || 0) + Number(solplanetEnergy.solar_generation || 0) : 0,
        rightKind: "pv",
      },
      {
        scope: t("samplingTotalsScopeOverall"),
        title: t("samplingTotalsGridTitle"),
        leftLabel: t("samplingOverallMetricGridImport"),
        leftValue: sajHasData || solplanetHasData ? Number(sajEnergy.grid_import || 0) + Number(solplanetEnergy.grid_import || 0) : 0,
        leftKind: "import",
        rightLabel: t("samplingOverallMetricGridExport"),
        rightValue: sajHasData || solplanetHasData ? Number(sajEnergy.grid_export || 0) + Number(solplanetEnergy.grid_export || 0) : 0,
        rightKind: "export",
      },
      {
        scope: t("samplingTotalsScopeSajBattery"),
        title: t("samplingTotalsBatteryTitle"),
        leftLabel: t("samplingOverallMetricBatteryCharge"),
        leftValue: sajHasData ? Number(sajEnergy.battery_charge || 0) : 0,
        leftKind: "charge",
        rightLabel: t("samplingOverallMetricBatteryDischarge"),
        rightValue: sajHasData ? Number(sajEnergy.battery_discharge || 0) : 0,
        rightKind: "discharge",
      },
      {
        scope: t("samplingTotalsScopeSolplanetBattery"),
        title: t("samplingTotalsBatteryTitle"),
        leftLabel: t("samplingOverallMetricBatteryCharge"),
        leftValue: solplanetHasData ? Number(solplanetEnergy.battery_charge || 0) : 0,
        leftKind: "charge",
        rightLabel: t("samplingOverallMetricBatteryDischarge"),
        rightValue: solplanetHasData ? Number(solplanetEnergy.battery_discharge || 0) : 0,
        rightKind: "discharge",
      },
    ];
  }

  const selectedUsage = selectedSystem === "solplanet" ? solplanetUsage : sajUsage;
  const selectedEnergy = selectedUsage?.energy_kwh || {};
  const selectedHasData = Number(selectedUsage?.samples || 0) >= 2;
  const selectedLabel = formatSamplingSystemLabel(selectedSystem);
  return [
    {
      scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
      title: t("samplingOverallMetricPv"),
      leftLabel: "",
      leftValue: 0,
      leftKind: "pv",
      rightLabel: t("samplingOverallMetricPv"),
      rightValue: selectedHasData ? Number(selectedEnergy.solar_generation || 0) : 0,
      rightKind: "pv",
    },
    {
      scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
      title: t("samplingTotalsGridTitle"),
      leftLabel: t("samplingOverallMetricGridImport"),
      leftValue: selectedHasData ? Number(selectedEnergy.grid_import || 0) : 0,
      leftKind: "import",
      rightLabel: t("samplingOverallMetricGridExport"),
      rightValue: selectedHasData ? Number(selectedEnergy.grid_export || 0) : 0,
      rightKind: "export",
    },
    {
      scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
      title: t("samplingTotalsBatteryTitle"),
      leftLabel: t("samplingOverallMetricBatteryCharge"),
      leftValue: selectedHasData ? Number(selectedEnergy.battery_charge || 0) : 0,
      leftKind: "charge",
      rightLabel: t("samplingOverallMetricBatteryDischarge"),
      rightValue: selectedHasData ? Number(selectedEnergy.battery_discharge || 0) : 0,
      rightKind: "discharge",
    },
  ];
}

function pickActiveOverallValue(latest, ts, maxGapMs, key) {
  if (!latest) return 0;
  if (!Number.isFinite(ts - latest.ts) || ts - latest.ts > maxGapMs) return 0;
  return Number(latest[key] || 0);
}

function normalizeOverallSeriesItems(items, system) {
  return (Array.isArray(items) ? items : [])
    .map((item) => {
      const ts = new Date(item.sampled_at_utc || "").getTime();
      if (!Number.isFinite(ts)) return null;
      const pv = Math.max(Number(item.pv_w || 0), 0);
      const grid = Number(item.grid_w || 0);
      const battery = Number(item.battery_w || 0);
      return {
        ts,
        sampled_at_utc: item.sampled_at_utc,
        pv_total_w: pv,
        grid_import_w: Math.max(grid, 0),
        grid_export_w: Math.max(-grid, 0),
        [`${system}_battery_charge_w`]: Math.max(-battery, 0),
        [`${system}_battery_discharge_w`]: Math.max(battery, 0),
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.ts - b.ts);
}

function buildOverallSeriesPayload(seriesBySystem, status, range) {
  const sajItems = normalizeOverallSeriesItems(seriesBySystem?.saj?.items, "saj");
  const solplanetItems = normalizeOverallSeriesItems(seriesBySystem?.solplanet?.items, "solplanet");
  const tsSet = new Set([...sajItems.map((item) => item.ts), ...solplanetItems.map((item) => item.ts)]);
  const timestamps = Array.from(tsSet).sort((a, b) => a - b);
  const sajGapMs = Number(status?.saj_sample_interval_seconds || 5) * 2500;
  const solplanetGapMs = Number(status?.solplanet_sample_interval_seconds || 60) * 2500;
  let sajIndex = 0;
  let solplanetIndex = 0;
  let latestSaj = null;
  let latestSolplanet = null;

  const items = timestamps.map((ts) => {
    while (sajIndex < sajItems.length && sajItems[sajIndex].ts <= ts) {
      latestSaj = sajItems[sajIndex];
      sajIndex += 1;
    }
    while (solplanetIndex < solplanetItems.length && solplanetItems[solplanetIndex].ts <= ts) {
      latestSolplanet = solplanetItems[solplanetIndex];
      solplanetIndex += 1;
    }
    const pvTotal =
      pickActiveOverallValue(latestSaj, ts, sajGapMs, "pv_total_w") + pickActiveOverallValue(latestSolplanet, ts, solplanetGapMs, "pv_total_w");
    const gridImport =
      pickActiveOverallValue(latestSaj, ts, sajGapMs, "grid_import_w") +
      pickActiveOverallValue(latestSolplanet, ts, solplanetGapMs, "grid_import_w");
    const gridExport =
      pickActiveOverallValue(latestSaj, ts, sajGapMs, "grid_export_w") +
      pickActiveOverallValue(latestSolplanet, ts, solplanetGapMs, "grid_export_w");
    return {
      sampled_at_utc: new Date(ts).toISOString(),
      pv_total_w: pvTotal,
      grid_import_w: gridImport,
      grid_export_w: gridExport,
      saj_battery_charge_w: pickActiveOverallValue(latestSaj, ts, sajGapMs, "saj_battery_charge_w"),
      saj_battery_discharge_w: pickActiveOverallValue(latestSaj, ts, sajGapMs, "saj_battery_discharge_w"),
      solplanet_battery_charge_w: pickActiveOverallValue(latestSolplanet, ts, solplanetGapMs, "solplanet_battery_charge_w"),
      solplanet_battery_discharge_w: pickActiveOverallValue(
        latestSolplanet,
        ts,
        solplanetGapMs,
        "solplanet_battery_discharge_w",
      ),
    };
  });

  return {
    system: "overall",
    count: items.length,
    items,
    start_at_utc: range.startUtc,
    end_at_utc: range.endUtc,
  };
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
  const chartSeriesMeta = getSamplingChartSeriesMeta(seriesPayload?.system || (document.getElementById("samplingSystemSelect")?.value || "overall"));
  if (samplingChartFocusSeries && !chartSeriesMeta.some((meta) => meta.key === samplingChartFocusSeries)) {
    samplingChartFocusSeries = null;
  }
  const startMs = new Date(seriesPayload?.start_at_utc || "").getTime();
  const endMs = new Date(seriesPayload?.end_at_utc || "").getTime();
  const xMin = Number.isFinite(startMs) ? startMs : null;
  const xMax = Number.isFinite(endMs) ? endMs : null;

  const smoothCfg = getSamplingSmoothConfig();
  const preparedSeries = chartSeriesMeta.map((meta) => {
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
        selected: Object.fromEntries(chartSeriesMeta.map((item) => [item.key, true])),
        itemWidth: 12,
        itemHeight: 8,
        formatter: (name) => {
          const hit = chartSeriesMeta.find((item) => item.key === name);
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
          const lines = [formatSamplingClock(ts, true)];
          for (const p of params) {
            const hit = chartSeriesMeta.find((item) => item.key === p.seriesName);
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
          formatter: (value) => formatSamplingClock(value, false),
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

function renderSamplingTotals(usageBySystem, selectedSystem, rangeLabel, options = {}) {
  const body = document.getElementById("samplingTotalsBody");
  if (!body) return;
  const systemText = formatSamplingSystemLabel(selectedSystem);
  const rows = buildSamplingTotalsRows(usageBySystem, selectedSystem);
  const hasData = rows.some((row) => Number(row.leftValue || 0) > 0 || Number(row.rightValue || 0) > 0);
  const maxValue = Math.max(0, ...rows.map((row) => Number(row.leftValue || 0)), ...rows.map((row) => Number(row.rightValue || 0)));

  if (options.metaText !== undefined) {
    setText("samplingTotalsMeta", options.metaText);
  } else {
    setText(
      "samplingTotalsMeta",
      hasData
        ? t("samplingTotalsMeta", { system: systemText, range: rangeLabel })
        : t("samplingTotalsMetaNoData", { system: systemText, range: rangeLabel }),
    );
  }

  if (!rows.length) {
    body.innerHTML = `<div class="sampling-total-empty">${escapeHtml(t("samplingTotalsNoData"))}</div>`;
    return;
  }
  if (!hasData) {
    body.innerHTML = `<div class="sampling-total-empty">${escapeHtml(t("samplingTotalsNoData"))}</div>`;
    return;
  }

  const widthPct = (value) => {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0 || maxValue <= 0) return 0;
    return Math.max(0, Math.min(100, (n / maxValue) * 100));
  };

  body.innerHTML = rows
    .map((row) => {
      const leftWidth = widthPct(row.leftValue);
      const rightWidth = widthPct(row.rightValue);
      const leftValueText = Number(row.leftValue || 0) > 0 ? formatEnergyKwhText(row.leftValue) : "-";
      const rightValueText = Number(row.rightValue || 0) > 0 ? formatEnergyKwhText(row.rightValue) : "-";
      return `
        <div class="sampling-total-row">
          <div class="sampling-total-head">
            <span class="sampling-total-scope">${escapeHtml(row.scope)}</span>
            <span class="sampling-total-title">${escapeHtml(row.title)}</span>
          </div>
          <div class="sampling-total-bar">
            <div class="sampling-total-side is-left">
              <div class="sampling-total-fill is-${escapeHtml(row.leftKind)}" style="width:${leftWidth}%;"></div>
            </div>
            <div class="sampling-total-axis"></div>
            <div class="sampling-total-side is-right">
              <div class="sampling-total-fill is-${escapeHtml(row.rightKind)}" style="width:${rightWidth}%;"></div>
            </div>
          </div>
          <div class="sampling-total-labels">
            <div class="sampling-total-label">
              <span class="sampling-total-label-name">${escapeHtml(row.leftLabel || "")}</span>
              <span class="sampling-total-label-value">${escapeHtml(leftValueText)}</span>
            </div>
            <div class="sampling-total-label is-right">
              <span class="sampling-total-label-name">${escapeHtml(row.rightLabel || "")}</span>
              <span class="sampling-total-label-value">${escapeHtml(rightValueText)}</span>
            </div>
          </div>
        </div>
      `;
    })
    .join("");
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

function buildEndpointFullUrl(state) {
  const path = typeof state?.path === "string" ? state.path.trim() : "";
  if (!path) return "";
  if (/^https?:\/\//i.test(path)) return path;
  const source = state?.source && typeof state.source === "object" ? state.source : null;
  const scheme = typeof source?.scheme === "string" && source.scheme ? source.scheme : "";
  const host = typeof source?.host === "string" && source.host ? source.host : "";
  const port = Number(source?.port);
  if (!scheme || !host || !Number.isFinite(port) || port <= 0) return "";
  const cleanPath = path.replace(/^\/+/, "");
  return `${scheme}://${host}:${port}/${cleanPath}`;
}

function formatEndpointLinkHtml(url) {
  const safeUrl = escapeHtml(url || "-");
  if (!url) return safeUrl;
  return `<a class="raw-meta-link" href="${safeUrl}" target="_blank" rel="noreferrer">${safeUrl}</a>`;
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
    meta.className = `raw-meta${state.phase === "failed" && state.status !== "stale" ? " error" : ""}`;
    const endpointUrl = buildEndpointFullUrl(state);
    const statusText = state.status || (state.phase === "done" ? "success" : state.phase === "failed" ? "failed" : "-");
    const requestText = formatLocalDateTime(state.last_requested_at || state.updated_at);
    const successText = formatLocalDateTime(state.last_success_at);
    const requestAgo = formatRelativeAgo(state.last_requested_at || state.updated_at);
    const successAgo = formatRelativeAgo(state.last_success_at);
    const normalizedStatus = String(statusText).toLowerCase();
    const statusBadgeText = normalizedStatus === "stale"
      ? t("rawStatusStale")
      : (normalizedStatus === "success" || state.phase === "done" ? t("rawStatusSuccess") : t("rawStatusFailed"));
    const statusBadgeClass = normalizedStatus === "stale"
      ? "raw-status-badge stale"
      : (normalizedStatus === "success" || state.phase === "done" ? "raw-status-badge success" : "raw-status-badge failed");
    if (state.phase === "loading") {
      meta.innerHTML =
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawLoading"))}</span><span>${escapeHtml(state.path || "-")}</span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("endpointUrl"))}</span><span>${formatEndpointLinkHtml(endpointUrl)}</span></div>`;
    } else if (state.phase === "failed") {
      meta.innerHTML =
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("endpointPath"))}</span><span>${escapeHtml(state.path || "-")}</span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("endpointUrl"))}</span><span>${formatEndpointLinkHtml(endpointUrl)}</span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawStatusLabel"))}</span><span class="${statusBadgeClass}">${escapeHtml(statusBadgeText)}</span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawLastRequest"))}</span><span>${escapeHtml(requestText)} <span class="raw-time-ago">(${escapeHtml(requestAgo)})</span></span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawLastSuccess"))}</span><span>${escapeHtml(successText)} <span class="raw-time-ago">(${escapeHtml(successAgo)})</span></span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("endpointError"))}</span><span>${escapeHtml(state.error || "-")}</span></div>`;
    } else if (state.phase === "done") {
      meta.innerHTML =
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("endpointPath"))}</span><span>${escapeHtml(state.path || "-")}</span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("endpointUrl"))}</span><span>${formatEndpointLinkHtml(endpointUrl)}</span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawStatusLabel"))}</span><span class="${statusBadgeClass}">${escapeHtml(statusBadgeText)}</span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawLastRequest"))}</span><span>${escapeHtml(requestText)} <span class="raw-time-ago">(${escapeHtml(requestAgo)})</span></span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawLastSuccess"))}</span><span>${escapeHtml(successText)} <span class="raw-time-ago">(${escapeHtml(successAgo)})</span></span></div>` +
        `<div class="raw-meta-line"><span class="raw-meta-label">${escapeHtml(t("rawLatency"))}</span><span>${escapeHtml(String(state.fetch_ms ?? "-"))} ms</span></div>`;
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
  const updatedText = formatDateTimeWithAgo(latest);
  setText(metaId, t("rawSummary", { updated: updatedText, ok: okCount, total: states.length, failed: failedCount }));
  setText(updatedId, `${t("updatedAt")}: ${updatedText}`);
}

function setSolplanetRawMode(mode, load = true) {
  solplanetRawMode = mode === "table" ? "table" : "cards";
  localStorage.setItem(SOLPLANET_RAW_MODE_KEY, solplanetRawMode);
  const cardsBtn = document.getElementById("solplanetRawModeCardsBtn");
  const tableBtn = document.getElementById("solplanetRawModeTableBtn");
  const cardBody = document.getElementById("solplanetRawBody");
  const tableWrap = document.getElementById("solplanetRawTableWrap");
  if (cardsBtn) cardsBtn.classList.toggle("active", solplanetRawMode === "cards");
  if (tableBtn) tableBtn.classList.toggle("active", solplanetRawMode === "table");
  if (cardBody) cardBody.classList.toggle("hidden", solplanetRawMode !== "cards");
  if (tableWrap) tableWrap.classList.toggle("hidden", solplanetRawMode !== "table");
  if (solplanetRawMode === "cards") renderSolplanetRawFromCache();
  else renderSolplanetKvFromCache();
  if (load && currentTab === "solplanetRaw") void loadCurrentTab();
}

function renderSolplanetRawFromCache() {
  if (solplanetRawMode !== "cards") return;
  for (const api of SOLPLANET_RAW_APIS) {
    const state = stateCache.lastSolplanetRaw[api.key] || {
      phase: "idle",
      path: api.url,
      payload: null,
      error: null,
      fetch_ms: null,
      updated_at: null,
      status: null,
      last_requested_at: null,
      last_success_at: null,
    };
    renderRawCard(api, state, "solplanetRawBody");
  }
  renderRawSummary(stateCache.lastSolplanetRaw, "solplanetRawMeta", "solplanetRawUpdatedAt");
}

function renderSolplanetKvFromCache() {
  if (solplanetRawMode !== "table") return;
  const state = stateCache.lastSolplanetKv || { phase: "idle", items: [], updated_at: null, error: null };
  const tbody = document.getElementById("solplanetRawTableBody");
  if (!tbody) return;
  tbody.innerHTML = "";
  const items = Array.isArray(state.items) ? state.items : [];
  if (state.phase === "loading" && !items.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 3;
    td.textContent = t("rawLoading");
    tr.appendChild(td);
    tbody.appendChild(tr);
    setText("solplanetRawMeta", t("rawKvMetaDash"));
    setText("solplanetRawUpdatedAt", `${t("updatedAt")}: -`);
    return;
  }
  if (state.phase === "failed" && !items.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 3;
    td.textContent = t("rawLoadFailed", { error: state.error || "-" });
    tr.appendChild(td);
    tbody.appendChild(tr);
    setText("solplanetRawMeta", t("rawKvMetaDash"));
    setText("solplanetRawUpdatedAt", `${t("updatedAt")}: -`);
    return;
  }

  if (!items.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 3;
    td.textContent = t("rawKvEmpty");
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const item of items) {
      const tr = document.createElement("tr");
      const attr = document.createElement("td");
      const value = document.createElement("td");
      const source = document.createElement("td");
      attr.textContent = String(item?.attribute || "-");
      value.textContent = formatRawFieldValue(item?.value);
      source.textContent = String(item?.source || "-");
      tr.appendChild(attr);
      tr.appendChild(value);
      tr.appendChild(source);
      tbody.appendChild(tr);
    }
  }
  const updatedText = formatDateTimeWithAgo(state.updated_at);
  const meta = t("rawKvMeta", { count: items.length });
  if (state.phase === "loading") setText("solplanetRawMeta", `${meta} · ${t("rawLoading")}`);
  else if (state.phase === "failed") setText("solplanetRawMeta", `${meta} · ${t("rawStatusFailed")}`);
  else setText("solplanetRawMeta", meta);
  setText("solplanetRawUpdatedAt", `${t("updatedAt")}: ${updatedText}`);
}

async function loadSolplanetKvTable() {
  const prev = stateCache.lastSolplanetKv || { items: [], updated_at: null };
  stateCache.lastSolplanetKv = {
    phase: "loading",
    items: Array.isArray(prev.items) ? prev.items : [],
    updated_at: prev.updated_at || null,
    error: null,
  };
  renderSolplanetKvFromCache();
  try {
    const payload = await fetchJson(SOLPLANET_REALTIME_KV_URL, { timeoutMs: 12000 });
    stateCache.lastSolplanetKv = {
      phase: "done",
      items: Array.isArray(payload?.items) ? payload.items : [],
      updated_at: payload?.updated_at || new Date().toISOString(),
      error: null,
    };
  } catch (err) {
    const last = stateCache.lastSolplanetKv || { items: [], updated_at: null };
    stateCache.lastSolplanetKv = {
      phase: "failed",
      items: Array.isArray(last.items) ? last.items : [],
      updated_at: last.updated_at || null,
      error: String(err),
    };
  }
  renderSolplanetKvFromCache();
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
const SAJ_TIME_RE = /^(0\d|1\d|2[0-3]):[0-5]\d$/;
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

function setSajActionModalVisible(visible) {
  const modal = document.getElementById("sajActionModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function setSajActionDebugMode(enabled) {
  sajActionDebugMode = Boolean(enabled);
  localStorage.setItem(SAJ_ACTION_DEBUG_MODE_KEY, sajActionDebugMode ? "1" : "0");
  const input = document.getElementById("sajActionDebugModeInput");
  if (input) input.checked = sajActionDebugMode;
}

function _apiCallDebugLine(call) {
  const method = String(call?.method || "GET").toUpperCase();
  const path = String(call?.path || "-");
  const purposeKey = String(call?.purposeKey || "");
  const params = typeof call?.purposeParams === "object" && call.purposeParams ? call.purposeParams : {};
  const purpose = purposeKey ? t(purposeKey, params) : String(call?.purpose || "-");
  return `${method} ${path}: ${purpose}`;
}

function showSajActionSuccess(summaryText, apiCalls = []) {
  setText("sajActionModalSummary", summaryText || t("sajControlApplyDone"));
  const debugBlock = document.getElementById("sajActionDebugBlock");
  if (debugBlock) debugBlock.classList.toggle("hidden", !sajActionDebugMode);
  const list = document.getElementById("sajActionModalApiList");
  if (list) {
    list.innerHTML = "";
    const lines = Array.isArray(apiCalls) && apiCalls.length ? apiCalls.map(_apiCallDebugLine) : [t("sajControlDebugNoApi")];
    for (const line of lines) {
      const li = document.createElement("li");
      li.textContent = line;
      list.appendChild(li);
    }
  }
  setSajActionModalVisible(true);
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
  markSajControlLocalEdit();
  sajDayMaskEditingTargetId = null;
  setSajDayMaskModalVisible(false);
}

function cancelSajDayMaskModal() {
  sajDayMaskEditingTargetId = null;
  setSajDayMaskModalVisible(false);
}

function _sajRatedPowerForUi() {
  const rated = Number(
    stateCache.lastSajControl?.control_state?.inverter?.rated_power_w
      ?? stateCache.lastSajControl?.state?.inverter?.rated_power_w
      ?? 5000,
  );
  return Number.isFinite(rated) && rated > 0 ? rated : 5000;
}

function _formatSajPowerDisplay(rawPercent) {
  const percent = Number(rawPercent);
  if (!Number.isFinite(percent)) return "-";
  const watts = Math.round((_sajRatedPowerForUi() * percent) / 100);
  return `${Math.trunc(percent)}% (${watts}W)`;
}

function _normalizeSajTableEditValue(field, rawValue) {
  const trimmed = String(rawValue ?? "").trim();
  if (field === "start_time" || field === "end_time") {
    if (!SAJ_TIME_RE.test(trimmed)) return null;
    return trimmed;
  }
  if (field === "power_percent") {
    const n = Number(trimmed);
    if (!Number.isFinite(n)) return null;
    return String(Math.max(0, Math.min(100, Math.trunc(n))));
  }
  if (field === "day_mask") {
    const n = Number(trimmed);
    if (!Number.isFinite(n)) return null;
    return String(clampMask7(Math.trunc(n)));
  }
  return null;
}

function _refreshSajTableInputDisplay(inputId, field) {
  const inputEl = document.getElementById(inputId);
  const displayEl = document.getElementById(`${inputId}Display`);
  if (!(inputEl instanceof HTMLInputElement) || !displayEl) return;
  if (field === "power_percent") {
    displayEl.textContent = _formatSajPowerDisplay(inputEl.value);
    return;
  }
  displayEl.textContent = inputEl.value || "-";
}

function _openSajTableCellEditor(buttonEl) {
  const field = String(buttonEl.dataset.field || "");
  const inputId = String(buttonEl.dataset.inputId || "");
  const kind = String(buttonEl.dataset.kind || "");
  const slot = Number(buttonEl.dataset.slot || "0");
  const inputEl = document.getElementById(inputId);
  if (!(inputEl instanceof HTMLInputElement) || !field) return;
  const actualRaw = String(buttonEl.dataset.actualValue || "").trim();
  const defaultValue = actualRaw || String(inputEl.value || "").trim();
  const promptKey = field === "start_time"
    ? "sajControlEditStartPrompt"
    : field === "end_time"
      ? "sajControlEditEndPrompt"
      : field === "power_percent"
        ? "sajControlEditPowerPrompt"
        : "sajControlEditMaskPrompt";
  const invalidKey = field === "start_time" || field === "end_time"
    ? "sajControlEditInvalidTime"
    : field === "power_percent"
      ? "sajControlEditInvalidPower"
      : "sajControlEditInvalidMask";

  const nextRaw = window.prompt(t(promptKey), defaultValue);
  if (nextRaw === null) return;
  const normalized = _normalizeSajTableEditValue(field, nextRaw);
  if (normalized === null) {
    window.alert(t(invalidKey));
    return;
  }
  inputEl.value = normalized;
  _refreshSajTableInputDisplay(inputId, field);
  _applySajLocalEditToCache(kind, slot, field, normalized);
}

function _applySajLocalEditToCache(kind, slot, field, normalizedValue) {
  if ((kind !== "charge" && kind !== "discharge") || !Number.isInteger(slot) || slot < 1 || slot > 7) return;
  const payload = stateCache.lastSajControl;
  if (!payload || typeof payload !== "object") return;
  const state = payload.control_state || payload.state;
  if (!state || typeof state !== "object") return;
  const slots = Array.isArray(state?.[kind]?.slots) ? state[kind].slots : null;
  if (!slots) return;
  const target = slots.find((item) => Number(item?.slot) === slot);
  if (!target || typeof target !== "object") return;

  if (field === "start_time" || field === "end_time") {
    target[field] = normalizedValue;
    return;
  }
  if (field === "power_percent" || field === "day_mask") {
    const n = Number(normalizedValue);
    if (!Number.isFinite(n)) return;
    target[field] = Math.trunc(n);
  }
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
  const modeSensorValue = controlState?.working_mode?.mode_sensor;
  // Mode dropdown should reflect actual/readback mode first.
  if (modeSensorValue !== null && modeSensorValue !== undefined && Number(modeSensorValue) >= 0 && Number(modeSensorValue) <= 8) {
    const el = document.getElementById("sajModeCodeInput");
    if (el) el.value = String(modeSensorValue);
  } else if (modeInputValue !== null && modeInputValue !== undefined && Number(modeInputValue) >= 0 && Number(modeInputValue) <= 8) {
    const el = document.getElementById("sajModeCodeInput");
    if (el) el.value = String(modeInputValue);
  }
  setText("sajModeInputHint", t("sajControlModeInputHint", { modeInput: modeInputValue ?? "-" }));
  const inverterModeValue = controlState?.working_mode?.inverter_working_mode_sensor;
  if (inverterModeValue !== null && inverterModeValue !== undefined && Number(inverterModeValue) >= 0 && Number(inverterModeValue) <= 8) {
    const el = document.getElementById("sajInverterModeCodeInput");
    if (el) el.value = String(inverterModeValue);
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
  const ratedPowerW = Number(controlState?.inverter?.rated_power_w);
  const safeRatedPowerW = Number.isFinite(ratedPowerW) && ratedPowerW > 0 ? ratedPowerW : 5000;

  const renderPowerFromPercent = (rawPercent) => {
    if (rawPercent === null || rawPercent === undefined || rawPercent === "") return "-";
    const percent = Number(rawPercent);
    if (!Number.isFinite(percent)) return "-";
    const watts = Math.round((safeRatedPowerW * percent) / 100);
    return `${Math.trunc(percent)}% (${watts}W)`;
  };

  const renderActualPower = (actual) => {
    const percent = actual?.power_percent;
    const watts = actual?.power_w_estimate;
    if (percent === null || percent === undefined) return "-";
    if (watts === null || watts === undefined) return `${percent}%`;
    return `${percent}% (${watts}W)`;
  };

  const renderDualValueCell = ({
    kind,
    slot,
    field,
    inputId,
    inputValue,
    inputText,
    actualValue,
    actualText,
  }) => `
    <td>
      <button
        type="button"
        class="saj-cell-editor"
        data-kind="${kind}"
        data-slot="${slot}"
        data-field="${field}"
        data-input-id="${inputId}"
        data-actual-value="${escapeHtml(String(actualValue ?? ""))}"
      >
        <span class="saj-cell-line is-input"><span class="saj-cell-tag">${t("sajControlInputLabel")}</span><span id="${inputId}Display" class="saj-cell-value">${escapeHtml(String(inputText ?? "-"))}</span></span>
        <span class="saj-cell-line is-actual"><span class="saj-cell-tag">${t("sajControlActualLabel")}</span><span class="saj-cell-value">${escapeHtml(String(actualText ?? "-"))}</span></span>
      </button>
      <input id="${inputId}" type="hidden" value="${escapeHtml(String(inputValue ?? ""))}" />
    </td>
  `;

  const renderRows = (kind, typeLabel, inputRows, actualRows, enableMask) => {
    for (let slot = 1; slot <= 7; slot += 1) {
      const input = inputRows.find((item) => Number(item?.slot) === slot) || {};
      const actual = actualRows.find((item) => Number(item?.slot) === slot) || {};
      const checked = (enableMask & (1 << (slot - 1))) !== 0 ? "checked" : "";
      const checkboxId = kind === "charge" ? `sajTableChargeEnableSlot${slot}` : `sajTableDischargeEnableSlot${slot}`;
      const startId = `sajTable${kind}Slot${slot}StartInput`;
      const endId = `sajTable${kind}Slot${slot}EndInput`;
      const powerId = `sajTable${kind}Slot${slot}PowerInput`;
      const dayMaskId = `sajTable${kind}Slot${slot}DayMaskInput`;
      const startValue = _normalizeSajTimeForInput(input.start_time) || "";
      const endValue = _normalizeSajTimeForInput(input.end_time) || "";
      const powerValue = input.power_percent === null || input.power_percent === undefined ? "" : String(Math.trunc(Number(input.power_percent)));
      const dayMaskValue = input.day_mask === null || input.day_mask === undefined ? "0" : String(clampMask7(input.day_mask));
      const actualStart = _normalizeSajTimeForInput(actual.start_time) || "";
      const actualEnd = _normalizeSajTimeForInput(actual.end_time) || "";
      const actualPowerPercent = actual.power_percent === null || actual.power_percent === undefined ? "" : String(Math.trunc(Number(actual.power_percent)));
      const actualDayMask = actual.day_mask === null || actual.day_mask === undefined ? "" : String(clampMask7(actual.day_mask));
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><input id="${checkboxId}" type="checkbox" ${checked} /></td>
        <td>${typeLabel}</td>
        <td>${slot}</td>
        ${renderDualValueCell({
    kind,
    slot,
    field: "start_time",
    inputId: startId,
    inputValue: startValue,
    inputText: startValue || "-",
    actualValue: actualStart,
    actualText: actualStart || "-",
  })}
        ${renderDualValueCell({
    kind,
    slot,
    field: "end_time",
    inputId: endId,
    inputValue: endValue,
    inputText: endValue || "-",
    actualValue: actualEnd,
    actualText: actualEnd || "-",
  })}
        ${renderDualValueCell({
    kind,
    slot,
    field: "power_percent",
    inputId: powerId,
    inputValue: powerValue,
    inputText: renderPowerFromPercent(powerValue),
    actualValue: actualPowerPercent,
    actualText: renderActualPower(actual),
  })}
        ${renderDualValueCell({
    kind,
    slot,
    field: "day_mask",
    inputId: dayMaskId,
    inputValue: dayMaskValue,
    inputText: dayMaskValue,
    actualValue: actualDayMask,
    actualText: actualDayMask || "-",
  })}
      `;
      body.appendChild(tr);
    }
  };

  renderRows("charge", t("sajControlTypeCharge"), chargeInput, chargeActual, chargeEnableMask);
  renderRows("discharge", t("sajControlTypeDischarge"), dischargeInput, dischargeActual, dischargeEnableMask);
}

function renderSajControlFromCache() {
  const payload = stateCache.lastSajControl;
  const passiveAlertEl = document.getElementById("sajPassiveModeAlert");
  if (!payload) {
    setText("sajControlMeta", "-");
    setText("sajControlUpdatedAt", `${t("updatedAt")}: -`);
    setText("sajControlStateJson", "-");
    setText("sajModeReadbackText", t("sajControlModeReadback", { modeInput: "-", modeSensor: "-", inverterMode: "-" }));
    setText("sajModeSignalText", t("sajControlModeSignals", { modeInput: "-", modeSensor: "-", inverterMode: "-" }));
    if (passiveAlertEl) {
      passiveAlertEl.classList.add("hidden");
      passiveAlertEl.textContent = "-";
    }
    const body = document.getElementById("sajControlSlotsBody");
    if (body) body.innerHTML = "";
    return;
  }
  const state = payload?.control_state || payload?.state || null;
  const updatedAt = formatDateTimeWithAgo(state?.updated_at);
  setText("sajControlUpdatedAt", `${t("updatedAt")}: ${updatedAt}`);
  const chargeEnableMask = state?.charge?.time_enable_mask ?? "-";
  const dischargeEnableMask = state?.discharge?.time_enable_mask ?? "-";
  const chargeSwitch = state?.charge?.control_switch;
  const dischargeSwitch = state?.discharge?.control_switch;
  const batterySoc = state?.battery?.soc_percent;
  const batteryPowerW = state?.battery?.power_w;
  const ratedPowerW = state?.inverter?.rated_power_w;
  const modeInput = state?.working_mode?.mode_input ?? "-";
  const modeSensorValue = state?.working_mode?.mode_sensor ?? "-";
  const inverterMode = state?.working_mode?.inverter_working_mode_sensor ?? "-";
  setText(
    "sajControlMeta",
    `charge_enable=${chargeEnableMask}, discharge_enable=${dischargeEnableMask}, ` +
      `charge_switch=${chargeSwitch}, discharge_switch=${dischargeSwitch}, ` +
      `battery_soc=${batterySoc ?? "-"}%, battery_power=${batteryPowerW ?? "-"}W, ` +
      `rated_power=${ratedPowerW ?? "-"}W`,
  );
  setText("sajModeReadbackText", t("sajControlModeReadback", { modeInput, modeSensor: modeSensorValue, inverterMode }));
  setText("sajModeSignalText", t("sajControlModeSignals", { modeInput, modeSensor: modeSensorValue, inverterMode }));
  const modeSensor = Number(modeSensorValue);
  const isPassiveMode = Number.isFinite(modeSensor) && modeSensor === 3;
  if (passiveAlertEl) {
    passiveAlertEl.classList.toggle("hidden", !isPassiveMode);
    passiveAlertEl.textContent = isPassiveMode ? t("sajControlPassiveAlert", { modeSensor }) : "-";
  }
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

async function applySajWorkingMode(modeCodeArg = null, summaryKey = "sajControlPopupWorkingModeSummary", purposeKey = "sajDebugPurposeSetMode") {
  const fallbackModeCode = Number(document.getElementById("sajModeCodeInput")?.value || "0");
  const modeCode = Number.isFinite(Number(modeCodeArg)) ? Number(modeCodeArg) : fallbackModeCode;
  try {
    const payload = await fetchJson("/api/saj/control/working-mode", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode_code: modeCode }),
      timeoutMs: 10000,
    });
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
    showSajActionSuccess(t(summaryKey, { modeCode }), [
      {
        method: "PUT",
        path: "/api/saj/control/working-mode",
        purposeKey,
      },
    ]);
  } catch (err) {
    setText("sajControlMeta", t("sajControlApplyFailed", { error: String(err) }));
  }
}

async function applySajInverterModeTarget() {
  const modeCode = Number(document.getElementById("sajInverterModeCodeInput")?.value || "0");
  await applySajWorkingMode(modeCode, "sajControlPopupInverterModeSummary", "sajDebugPurposeSetInverterModeTarget");
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
  const debugCalls = [
    {
      method: "PUT",
      path: "/api/saj/control/toggles",
      purposeKey: "sajDebugPurposeSetTogglesMask",
    },
  ];
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
      if (Object.keys(payload).length) {
        edits.push({ kind, slot, payload });
        debugCalls.push({
          method: "PUT",
          path: `/api/saj/control/${kind}-slots/${slot}`,
          purposeKey: "sajDebugPurposeSetSlot",
          purposeParams: { kind, slot },
        });
      }
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
    stateCache.lastSajControl = payload;
    renderSajControlFromCache();
    showSajActionSuccess(t("sajControlPopupSaveSummary"), debugCalls);
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

const SOLPLANET_SCHEDULE_DAYS = ["Mon", "Tus", "Wen", "Thu", "Fri", "Sat", "Sun"];

function _solplanetControlStateFromCache() {
  const payload = stateCache.lastSolplanetControl;
  return payload?.control_state || payload?.state || null;
}

function setSolplanetControlLoading(loading) {
  const spinner = document.getElementById("solplanetControlLoadingSpinner");
  if (spinner) spinner.classList.toggle("is-hidden", !loading);
}

function setSolplanetControlActionBusy(busy) {
  solplanetControlBusy = Boolean(busy);
  const ids = ["solplanetLimitsApplyBtn", "solplanetScheduleSaveBtn", "solplanetRawSettingApplyBtn"];
  for (const id of ids) {
    const btn = document.getElementById(id);
    if (btn) btn.disabled = solplanetControlBusy;
  }
}

function setSolplanetActionModalVisible(visible) {
  const modal = document.getElementById("solplanetActionModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function showSolplanetActionSuccess(summaryText, apiCalls = [], resultPayload = null) {
  setText("solplanetActionModalSummary", summaryText || t("sajControlApplyDone"));
  const list = document.getElementById("solplanetActionModalApiList");
  if (list) {
    list.innerHTML = "";
    const lines = Array.isArray(apiCalls) && apiCalls.length ? apiCalls.map(_apiCallDebugLine) : [t("sajControlDebugNoApi")];
    for (const line of lines) {
      const li = document.createElement("li");
      li.textContent = line;
      list.appendChild(li);
    }
  }
  const pre = document.getElementById("solplanetActionModalResultJson");
  if (pre) {
    pre.textContent = resultPayload ? JSON.stringify(resultPayload, null, 2) : "-";
  }
  setSolplanetActionModalVisible(true);
}

function _solplanetSelectedDay() {
  const day = String(document.getElementById("solplanetScheduleDayInput")?.value || "Mon");
  return SOLPLANET_SCHEDULE_DAYS.includes(day) ? day : "Mon";
}

function _solplanetSetRawPayloadTemplateFromState(state) {
  const input = document.getElementById("solplanetRawSettingInput");
  if (!(input instanceof HTMLTextAreaElement)) return;
  if (input.value.trim()) return;
  const day = _solplanetSelectedDay();
  const slots = Array.isArray(state?.days?.[day]?.encoded_slots) ? state.days[day].encoded_slots : [0, 0, 0, 0, 0, 0];
  input.value = JSON.stringify(
    {
      Pin: Number(state?.limits?.pin || 0),
      Pout: Number(state?.limits?.pout || 0),
      [day]: slots,
    },
    null,
    2,
  );
}

function renderSolplanetControlSlotsTable() {
  const body = document.getElementById("solplanetControlSlotsBody");
  if (!body) return;
  body.innerHTML = "";
  const state = _solplanetControlStateFromCache();
  const selectedDay = _solplanetSelectedDay();
  const dayState = state?.days?.[selectedDay] || {};
  const decodedSlots = Array.isArray(dayState?.decoded_slots) ? dayState.decoded_slots : [];

  for (let slot = 1; slot <= 6; slot += 1) {
    const item = decodedSlots.find((entry) => Number(entry?.slot) === slot) || {};
    const enabled = Boolean(item?.enabled);
    const hour = Number.isFinite(Number(item?.hour)) ? Math.max(0, Math.min(23, Math.trunc(Number(item.hour)))) : 0;
    const minute = Number.isFinite(Number(item?.minute)) ? Math.max(0, Math.min(59, Math.trunc(Number(item.minute)))) : 0;
    const power = Number.isFinite(Number(item?.power)) ? Math.max(0, Math.min(255, Math.trunc(Number(item.power)))) : 0;
    const mode = Number.isFinite(Number(item?.mode)) ? Math.max(0, Math.min(255, Math.trunc(Number(item.mode)))) : 0;
    const encoded = Number.isFinite(Number(item?.encoded)) ? Math.trunc(Number(item.encoded)) : 0;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input id="solplanetSlotEnabled${slot}" type="checkbox" ${enabled ? "checked" : ""} /></td>
      <td>${slot}</td>
      <td><input id="solplanetSlotHour${slot}" type="number" min="0" max="23" step="1" value="${hour}" /></td>
      <td><input id="solplanetSlotMinute${slot}" type="number" min="0" max="59" step="1" value="${minute}" /></td>
      <td><input id="solplanetSlotPower${slot}" type="number" min="0" max="255" step="1" value="${power}" /></td>
      <td><input id="solplanetSlotMode${slot}" type="number" min="0" max="255" step="1" value="${mode}" /></td>
      <td><code id="solplanetSlotEncoded${slot}">${encoded}</code></td>
    `;
    body.appendChild(tr);
  }
}

function renderSolplanetModeSignals() {
  const body = document.getElementById("solplanetControlSignalsBody");
  if (!body) return;
  body.innerHTML = "";
  const live = stateCache.lastSolplanetControlLive;
  const endpoints = live?.endpoints || {};
  const rows = [];

  const add = (source, key, value) => {
    rows.push({ source, key, value: value === undefined || value === null ? "-" : String(value) });
  };

  const inverterData = endpoints?.getdevdata_device_2?.payload || {};
  const meterData = endpoints?.getdevdata_device_3?.payload || {};
  const meterInfo = endpoints?.getdev_device_3?.payload || {};
  const batteryData = endpoints?.getdevdata_device_4?.payload || {};

  add("getdevdata_device_2", "stu", inverterData?.stu);
  add("getdevdata_device_3", "mod", meterData?.mod);
  add("getdevdata_device_3", "enb", meterData?.enb);
  add("getdev_device_3", "mod", meterInfo?.mod);
  add("getdev_device_3", "enb", meterInfo?.enb);
  add("getdevdata_device_4", "cst", batteryData?.cst);
  add("getdevdata_device_4", "bst", batteryData?.bst);

  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${escapeHtml(row.source)}</td><td>${escapeHtml(row.key)}</td><td>${escapeHtml(row.value)}</td>`;
    body.appendChild(tr);
  }

  setText(
    "solplanetControlModeMeta",
    t("solplanetControlModeMeta", {
      stu: inverterData?.stu ?? "-",
      meterMod: meterData?.mod ?? meterInfo?.mod ?? "-",
      meterEnb: meterData?.enb ?? meterInfo?.enb ?? "-",
      batteryCst: batteryData?.cst ?? "-",
      batteryBst: batteryData?.bst ?? "-",
    }),
  );
}

function renderSolplanetControlFromCache() {
  const payload = stateCache.lastSolplanetControl;
  if (!payload) {
    setText("solplanetControlMeta", "-");
    setText("solplanetControlFetchMeta", "-");
    setText("solplanetControlUpdatedAt", `${t("updatedAt")}: -`);
    setText("solplanetControlStateJson", "-");
    setText("solplanetControlModeMeta", "-");
    const body = document.getElementById("solplanetControlSlotsBody");
    if (body) body.innerHTML = "";
    const modeBody = document.getElementById("solplanetControlSignalsBody");
    if (modeBody) modeBody.innerHTML = "";
    return;
  }
  const state = _solplanetControlStateFromCache();
  const updatedAt = formatDateTimeWithAgo(state?.updated_at);
  setText("solplanetControlUpdatedAt", `${t("updatedAt")}: ${updatedAt}`);
  const pin = state?.limits?.pin ?? "-";
  const pout = state?.limits?.pout ?? "-";
  setText("solplanetControlMeta", `Pin=${pin}W, Pout=${pout}W`);
  const fetchMeta = stateCache.lastSolplanetControlFetch || null;
  if (fetchMeta) {
    const timeText = fetchMeta.requested_at ? formatLocalDateTime(fetchMeta.requested_at) : "-";
    setText(
      "solplanetControlFetchMeta",
      t("solplanetControlFetchMeta", {
        time: timeText,
        stateMs: fetchMeta.state_ms ?? "-",
        liveMs: fetchMeta.live_ms ?? "-",
        totalMs: fetchMeta.total_ms ?? "-",
      }),
    );
  } else {
    setText("solplanetControlFetchMeta", "-");
  }
  const pinInput = document.getElementById("solplanetPinInput");
  const poutInput = document.getElementById("solplanetPoutInput");
  if (pinInput && pin !== "-") pinInput.value = String(pin);
  if (poutInput && pout !== "-") poutInput.value = String(pout);
  const pre = document.getElementById("solplanetControlStateJson");
  if (pre) pre.textContent = JSON.stringify(state || payload, null, 2);
  _solplanetSetRawPayloadTemplateFromState(state);
  renderSolplanetControlSlotsTable();
  renderSolplanetModeSignals();
}

async function loadSolplanetControl() {
  setSolplanetControlLoading(true);
  const requestStartedAt = Date.now();
  try {
    const stateStarted = performance.now();
    const statePromise = fetchJson("/api/solplanet/control/state", { timeoutMs: 15000 });
    const liveStarted = performance.now();
    const livePromise = fetchJson("/api/solplanet/cgi-dump", { timeoutMs: 20000 });

    const [stateResult, liveResult] = await Promise.allSettled([statePromise, livePromise]);
    const stateMs = Math.max(0, Math.round(performance.now() - stateStarted));
    const liveMs = Math.max(0, Math.round(performance.now() - liveStarted));
    const totalMs = Math.max(0, Date.now() - requestStartedAt);

    if (stateResult.status === "fulfilled") {
      stateCache.lastSolplanetControl = stateResult.value;
    } else {
      throw stateResult.reason;
    }
    stateCache.lastSolplanetControlLive = liveResult.status === "fulfilled" ? liveResult.value : null;
    stateCache.lastSolplanetControlFetch = {
      requested_at: new Date().toISOString(),
      state_ms: stateMs,
      live_ms: liveMs,
      total_ms: totalMs,
    };
    renderSolplanetControlFromCache();
  } catch (err) {
    setText("solplanetControlMeta", t("solplanetControlLoadFailed", { error: String(err) }));
  } finally {
    setSolplanetControlLoading(false);
  }
}

async function applySolplanetLimits() {
  if (solplanetControlBusy) return;
  const pin = Number(document.getElementById("solplanetPinInput")?.value || "0");
  const pout = Number(document.getElementById("solplanetPoutInput")?.value || "0");
  setSolplanetControlActionBusy(true);
  setSolplanetControlLoading(true);
  try {
    const payload = await fetchJson("/api/solplanet/control/limits", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pin, pout }),
      timeoutMs: 12000,
    });
    stateCache.lastSolplanetControl = payload;
    renderSolplanetControlFromCache();
    showSolplanetActionSuccess(t("solplanetControlPopupLimitsSummary"), [
      { method: "PUT", path: "/api/solplanet/control/limits", purpose: "Update Pin/Pout limits." },
    ], payload);
  } catch (err) {
    const errorText = String(err);
    setText("solplanetControlMeta", t("solplanetControlApplyFailed", { error: errorText }));
    showSolplanetActionSuccess(t("solplanetControlApplyFailed", { error: errorText }), [
      { method: "PUT", path: "/api/solplanet/control/limits", purpose: "Update Pin/Pout limits." },
    ], { ok: false, error: errorText });
  } finally {
    setSolplanetControlLoading(false);
    setSolplanetControlActionBusy(false);
  }
}

async function applySolplanetDaySchedule() {
  if (solplanetControlBusy) return;
  const day = _solplanetSelectedDay();
  const slots = [];
  for (let slot = 1; slot <= 6; slot += 1) {
    const enabled = Boolean(document.getElementById(`solplanetSlotEnabled${slot}`)?.checked);
    const hour = Math.max(0, Math.min(23, Math.trunc(Number(document.getElementById(`solplanetSlotHour${slot}`)?.value || "0"))));
    const minute = Math.max(0, Math.min(59, Math.trunc(Number(document.getElementById(`solplanetSlotMinute${slot}`)?.value || "0"))));
    const power = Math.max(0, Math.min(255, Math.trunc(Number(document.getElementById(`solplanetSlotPower${slot}`)?.value || "0"))));
    const mode = Math.max(0, Math.min(255, Math.trunc(Number(document.getElementById(`solplanetSlotMode${slot}`)?.value || "0"))));
    const encoded = enabled ? ((((hour & 0xff) << 24) | ((minute & 0xff) << 16) | ((power & 0xff) << 8) | (mode & 0xff)) >>> 0) : 0;
    slots.push(encoded);
  }
  setSolplanetControlActionBusy(true);
  setSolplanetControlLoading(true);
  try {
    const payload = await fetchJson(`/api/solplanet/control/day-schedule/${day}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ slots }),
      timeoutMs: 15000,
    });
    stateCache.lastSolplanetControl = payload;
    renderSolplanetControlFromCache();
    showSolplanetActionSuccess(t("solplanetControlPopupScheduleSummary", { day }), [
      { method: "PUT", path: `/api/solplanet/control/day-schedule/${day}`, purpose: "Save selected day schedule." },
    ], payload);
  } catch (err) {
    const errorText = String(err);
    setText("solplanetControlMeta", t("solplanetControlApplyFailed", { error: errorText }));
    showSolplanetActionSuccess(t("solplanetControlApplyFailed", { error: errorText }), [
      { method: "PUT", path: `/api/solplanet/control/day-schedule/${day}`, purpose: "Save selected day schedule." },
    ], { ok: false, error: errorText });
  } finally {
    setSolplanetControlLoading(false);
    setSolplanetControlActionBusy(false);
  }
}

async function applySolplanetRawSetting() {
  if (solplanetControlBusy) return;
  const input = document.getElementById("solplanetRawSettingInput");
  const rawText = String(input?.value || "").trim();
  if (!rawText) return;
  let payloadObj;
  try {
    payloadObj = JSON.parse(rawText);
  } catch (err) {
    window.alert(t("solplanetControlRawSettingInvalidJson", { error: String(err) }));
    return;
  }
  setSolplanetControlActionBusy(true);
  setSolplanetControlLoading(true);
  try {
    const payload = await fetchJson("/api/solplanet/control/raw-setting", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ payload: payloadObj }),
      timeoutMs: 20000,
    });
    stateCache.lastSolplanetControl = payload;
    renderSolplanetControlFromCache();
    showSolplanetActionSuccess(t("solplanetControlPopupRawSummary"), [
      { method: "PUT", path: "/api/solplanet/control/raw-setting", purpose: "Send raw setting payload to setting.cgi." },
    ], payload);
  } catch (err) {
    const errorText = String(err);
    setText("solplanetControlMeta", t("solplanetControlApplyFailed", { error: errorText }));
    showSolplanetActionSuccess(t("solplanetControlApplyFailed", { error: errorText }), [
      { method: "PUT", path: "/api/solplanet/control/raw-setting", purpose: "Send raw setting payload to setting.cgi." },
    ], { ok: false, error: errorText });
  } finally {
    setSolplanetControlLoading(false);
    setSolplanetControlActionBusy(false);
  }
}

async function restartSolplanetBackendApi() {
  if (solplanetControlBusy) return;
  setSolplanetControlLoading(true);
  const btn = document.getElementById("solplanetRestartApiBtn");
  if (btn) btn.disabled = true;
  try {
    const payload = await fetchJson("/api/solplanet/control/restart-api", {
      method: "POST",
      timeoutMs: 20000,
    });
    setText("solplanetRawMeta", t("solplanetControlPopupRestartSummary"));
    showSolplanetActionSuccess(t("solplanetControlPopupRestartSummary"), [
      { method: "POST", path: "/api/solplanet/control/restart-api", purpose: "Force stop collector loop and restart it." },
    ], payload);
    await loadSolplanetRaw();
  } catch (err) {
    const errorText = String(err);
    setText("solplanetRawMeta", t("solplanetControlApplyFailed", { error: errorText }));
    showSolplanetActionSuccess(t("solplanetControlApplyFailed", { error: errorText }), [
      { method: "POST", path: "/api/solplanet/control/restart-api", purpose: "Force stop collector loop and restart it." },
    ], { ok: false, error: errorText });
  } finally {
    setSolplanetControlLoading(false);
    if (btn) btn.disabled = false;
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

async function toggleTeslaCharging() {
  if (teslaControlBusy) return;
  const summary = stateCache.lastSummary;
  const teslaInfo = teslaInfoFromCombinedFlow(summary?.combinedFlow || {});
  if (!teslaInfo?.controlAvailable) return;
  const targetEnabled = teslaInfo?.chargingEnabled === true ? false : true;
  teslaControlBusy = true;
  renderTeslaControlButton(teslaInfo);
  try {
    await fetchJson("/api/tesla/control/charging", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: targetEnabled }),
      timeoutMs: 10000,
    });
    await loadSummary();
  } catch (err) {
    window.alert(t("teslaControlApplyFailed", { error: String(err) }));
  } finally {
    teslaControlBusy = false;
    renderTeslaControlButton(teslaInfoFromCombinedFlow(stateCache.lastSummary?.combinedFlow || {}));
  }
}

async function loadSummary() {
  const requestId = ++summaryRequestId;
  const summary = stateCache.lastSummary || {
    combinedFlow: { metrics: {} },
    collectorStatus: null,
    tesla: {
      chargingW: null, entityId: null, friendlyName: null, updatedAt: null,
      currentA: null, currentEntityId: null, currentUnit: "A",
      socPercent: null, socEntityId: null,
      controlAvailable: false, controlMode: "unavailable", chargingEnabled: null,
      canStart: false, canStop: false,
      controlSwitchEntityId: null, controlStartButtonEntityId: null, controlStopButtonEntityId: null,
    },
  };
  stateCache.lastSummary = summary;
  setSystemLoadMeta("combined", { phase: "loading", updatedAt: null });
  renderSummary(summary);

  const baseResults = await Promise.allSettled([
    fetchJson("/api/collector/status", { timeoutMs: 6000 }),
  ]);
  if (requestId !== summaryRequestId) return;

  if (baseResults[0].status === "fulfilled") {
    summary.collectorStatus = baseResults[0].value;
    stateCache.lastCollectorStatus = baseResults[0].value;
  }
  renderSummary(summary);

  void fetchJson("/api/energy-flow/combined", { timeoutMs: 30000 })
    .then((combinedFlow) => {
      if (requestId !== summaryRequestId) return;
      summary.combinedFlow = { ...combinedFlow, __load_error: false };
      summary.tesla = teslaInfoFromCombinedFlow(summary.combinedFlow);
      setSystemLoadMeta("combined", {
        phase: "done",
        updatedAt: combinedFlow?.updated_at || new Date().toISOString(),
      });
      renderSummary(summary);
    })
    .catch(() => {
      if (requestId !== summaryRequestId) return;
      summary.combinedFlow = { metrics: {}, __load_error: true };
      setSystemLoadMeta("combined", { phase: "failed", updatedAt: null, quality: "failed", count: 0 });
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
  for (const api of apis) {
    const prev = stateMap[api.key] || {};
    stateMap[api.key] = {
      phase: "loading",
      path: prev.path || api.url,
      payload: prev.payload ?? null,
      error: null,
      fetch_ms: prev.fetch_ms ?? null,
      updated_at: prev.updated_at || null,
      status: prev.status || null,
      last_requested_at: new Date().toISOString(),
      last_success_at: prev.last_success_at || null,
    };
    renderRawCard(api, stateMap[api.key], bodyId);
  }
  renderRawSummary(stateMap, metaId, updatedId);

  const tasks = apis.map(async (api) => {
    try {
      const response = await fetchJson(api.url, { timeoutMs: 30000 });
      stateMap[api.key] = {
        phase: response?.status === "stale" ? "failed" : (response?.ok ? "done" : "failed"),
        path: response?.path || api.url,
        payload: response?.payload ?? null,
        error: response?.error || null,
        fetch_ms: response?.fetch_ms ?? null,
        updated_at: response?.updated_at || new Date().toISOString(),
        status: response?.status || (response?.ok ? "success" : "failed"),
        last_requested_at: response?.last_requested_at || response?.updated_at || null,
        last_success_at: response?.last_success_at || null,
        source: response?.source || null,
      };
    } catch (err) {
      const prev = stateMap[api.key] || {};
      stateMap[api.key] = {
        phase: "failed",
        path: api.url,
        payload: prev.payload ?? null,
        error: String(err),
        fetch_ms: prev.fetch_ms ?? null,
        updated_at: new Date().toISOString(),
        status: "failed",
        last_requested_at: prev.last_requested_at || null,
        last_success_at: prev.last_success_at || null,
        source: prev.source || null,
      };
    }
    renderRawCard(api, stateMap[api.key], bodyId);
    renderRawSummary(stateMap, metaId, updatedId);
  });

  await Promise.allSettled(tasks);
}

async function loadSolplanetRaw() {
  if (solplanetRawMode === "table") {
    await loadSolplanetKvTable();
    return;
  }
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
  const system = document.getElementById("samplingSystemSelect")?.value || "overall";
  const overallMode = system === "overall";
  const range = getSamplingRange();
  if (!range.startUtc || !range.endUtc || range.invalid) {
    const invalidMessage = range.invalidReason || "Invalid time range";
    stateCache.lastSamplingDaily = null;
    stateCache.lastSamplingUsageBySystem = null;
    stateCache.lastSamplingPage = null;
    stateCache.lastSamplingSeries = null;
    setText("samplingDailyMeta", t("loadFailed", { error: invalidMessage }));
    setText("samplingChartMeta", t("loadFailed", { error: invalidMessage }));
    renderSamplingRows([]);
    renderSamplingChart({ items: [] });
    renderSamplingTotals(null, system, range.label, { metaText: t("loadFailed", { error: invalidMessage }) });
    return;
  }
  const sajUsageUrl =
    `/api/storage/usage-range?system=saj&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}`;
  const solplanetUsageUrl =
    `/api/storage/usage-range?system=solplanet&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}`;
  const sajSeriesUrl =
    `/api/storage/series?system=saj&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}&max_points=500`;
  const solplanetSeriesUrl =
    `/api/storage/series?system=solplanet&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}&max_points=500`;
  const selectedSeriesUrl = overallMode ? sajSeriesUrl : system === "solplanet" ? solplanetSeriesUrl : sajSeriesUrl;

  const [statusResult, sajUsageResult, solplanetUsageResult, samplesResult, seriesResult, overallPeerSeriesResult] = await Promise.allSettled([
    fetchJson("/api/storage/status", { timeoutMs: 6000 }),
    fetchJson(sajUsageUrl, { timeoutMs: 6000 }),
    fetchJson(solplanetUsageUrl, { timeoutMs: 6000 }),
    fetchJson(buildSamplingUrl(), { timeoutMs: 6000 }),
    fetchJson(selectedSeriesUrl, { timeoutMs: 6000 }),
    overallMode ? fetchJson(solplanetSeriesUrl, { timeoutMs: 6000 }) : Promise.resolve(null),
  ]);

  if (statusResult.status === "fulfilled") {
    stateCache.lastSamplingStatus = statusResult.value;
    renderSamplingStatus(statusResult.value);
  } else {
    setText("samplingStorageMeta", t("loadFailed", { error: String(statusResult.reason) }));
  }

  const usageBySystem = {
    saj: sajUsageResult.status === "fulfilled" ? sajUsageResult.value : null,
    solplanet: solplanetUsageResult.status === "fulfilled" ? solplanetUsageResult.value : null,
  };
  stateCache.lastSamplingUsageBySystem = usageBySystem;
  const selectedUsage = overallMode
    ? combineSamplingUsageForOverall(usageBySystem)
    : system === "solplanet"
      ? usageBySystem.solplanet
      : usageBySystem.saj;
  stateCache.lastSamplingDaily = selectedUsage || null;

  if (selectedUsage) {
    renderSamplingUsage(selectedUsage, range.label);
  } else {
    const selectedUsageError = system === "solplanet" ? solplanetUsageResult.reason : sajUsageResult.reason;
    setText("samplingDailyMeta", t("loadFailed", { error: String(selectedUsageError) }));
  }
  renderSamplingTotals(usageBySystem, system, range.label);

  if (samplesResult.status === "fulfilled") {
    const payload = samplesResult.value;
    samplingPager.hasNext = Boolean(payload.has_next);
    samplingPager.hasPrev = Boolean(payload.has_prev);
    stateCache.lastSamplingPage = payload;
    renderSamplingPage(payload);
  } else {
    stateCache.lastSamplingPage = null;
    setText("samplingCount", t("loadFailed", { error: String(samplesResult.reason) }));
    setText("samplingPageInfo", t("pageDash"));
    renderSamplingRows([]);
  }

  const overallHasAnySeries =
    overallMode && (seriesResult.status === "fulfilled" || overallPeerSeriesResult.status === "fulfilled");
  if (seriesResult.status === "fulfilled" || overallHasAnySeries) {
    let payload = seriesResult.status === "fulfilled" ? seriesResult.value : { items: [], count: 0 };
    if (overallMode) {
      const sajSeries = seriesResult.status === "fulfilled" ? seriesResult.value : null;
      const solplanetSeries = overallPeerSeriesResult.status === "fulfilled" ? overallPeerSeriesResult.value : null;
      payload = buildOverallSeriesPayload({ saj: sajSeries, solplanet: solplanetSeries }, stateCache.lastSamplingStatus, range);
    }
    stateCache.lastSamplingSeries = payload;
    setText(
      "samplingChartMeta",
      t("samplingChartMeta", {
        system: formatSamplingSystemLabel(payload.system || system),
        range: range.label,
        count: payload.count || 0,
      }),
    );
    renderSamplingChart(payload);
  } else {
    stateCache.lastSamplingSeries = null;
    setText("samplingChartMeta", t("loadFailed", { error: String(seriesResult.reason) }));
    renderSamplingChart({ items: [] });
  }
}

function rerenderSamplingViewFromCache() {
  if (stateCache.lastSamplingStatus) renderSamplingStatus(stateCache.lastSamplingStatus);
  if (stateCache.lastSamplingDaily) {
    const range = getSamplingRange();
    renderSamplingUsage(stateCache.lastSamplingDaily, range.label);
  }
  if (stateCache.lastSamplingUsageBySystem) {
    const range = getSamplingRange();
    const system = document.getElementById("samplingSystemSelect")?.value || "overall";
    renderSamplingTotals(stateCache.lastSamplingUsageBySystem, system, range.label);
  }
  if (stateCache.lastSamplingPage) renderSamplingPage(stateCache.lastSamplingPage);
  if (stateCache.lastSamplingSeries) {
    const range = getSamplingRange();
    const payload = stateCache.lastSamplingSeries;
    setText(
      "samplingChartMeta",
      t("samplingChartMeta", {
        system: formatSamplingSystemLabel(payload.system || (document.getElementById("samplingSystemSelect")?.value || "overall")),
        range: range.label,
        count: payload.count || 0,
      }),
    );
    renderSamplingChart(payload);
  }
}

async function loadWorkerLogs() {
  if (!workerLogsDefaultsApplied) {
    const categorySelect = document.getElementById("workerLogsCategorySelect");
    if (categorySelect && !categorySelect.value) categorySelect.value = "all";
    workerLogsDefaultsApplied = true;
  }
  try {
    const [payload, configPayload] = await Promise.all([
      fetchJson(buildWorkerLogsUrl(), { timeoutMs: 10000 }),
      fetchJson("/api/config", { timeoutMs: 5000 }),
    ]);
    workerLogsPager.hasNext = Boolean(payload.has_next);
    workerLogsPager.hasPrev = Boolean(payload.has_prev);
    stateCache.lastWorkerLogsPage = payload;
    renderWorkerLogsPage(payload);
    renderWorkerLogsConfigMeta(configPayload);
  } catch (err) {
    setText("workerLogsCount", t("loadFailed", { error: String(err) }));
    setText("workerLogsPageInfo", t("pageDash"));
    setText("workerLogsUpdatedAt", formatUpdatedAt(null));
    setText("workerLogsConfigMeta", t("workerLogsConfigMeta", { host: "-" }));
    renderWorkerLogsRows([]);
  }
}

async function loadWorkerFailureLog({ appendOlder = false } = {}) {
  try {
    const before = appendOlder ? workerFailureLogState.before : 0;
    const payload = await fetchJson(buildWorkerFailureLogUrl(before), { timeoutMs: 10000 });
    renderWorkerFailureLogPage(payload, { appendOlder });
  } catch (err) {
    if (appendOlder) return;
    stateCache.lastWorkerFailureLog = null;
    workerFailureLogState.before = 0;
    workerFailureLogState.hasMore = false;
    setText("workerFailureLogMeta", t("loadFailed", { error: String(err) }));
    setText("workerFailureLogCount", t("failureLogShowing", { count: 0, total: 0 }));
    setText("workerFailureLogUpdatedAt", formatUpdatedAt(null));
    const pre = document.getElementById("workerFailureLogPre");
    if (pre) pre.textContent = t("failureLogEmpty");
    const loadMoreBtn = document.getElementById("workerFailureLogLoadMoreBtn");
    if (loadMoreBtn) loadMoreBtn.disabled = true;
  }
}

async function loadCurrentTab(fromAutoRefresh = false) {
  return loadTabWithGuard(currentTab, fromAutoRefresh);
}

function tabHasCachedData(tab) {
  if (tab === "dashboard") return Boolean(stateCache.lastSummary);
  if (tab === "entities") return Boolean(stateCache.lastEntities);
  if (tab === "solplanetRaw") {
    if (solplanetRawMode === "table") return stateCache.lastSolplanetKv?.phase && stateCache.lastSolplanetKv.phase !== "idle";
    return SOLPLANET_RAW_APIS.some((api) => stateCache.lastSolplanetRaw?.[api.key]?.payload !== undefined);
  }
  if (tab === "sajRaw") return SAJ_RAW_APIS.some((api) => stateCache.lastSajRaw?.[api.key]?.payload !== undefined);
  if (tab === "sajControl") return Boolean(stateCache.lastSajControl);
  if (tab === "solplanetControl") return Boolean(stateCache.lastSolplanetControl);
  if (tab === "sampling") return Boolean(stateCache.lastSamplingPage || stateCache.lastSamplingStatus || stateCache.lastSamplingSeries);
  if (tab === "workerLogs") return Boolean(stateCache.lastWorkerLogsPage);
  if (tab === "workerFailureLog") return Boolean(stateCache.lastWorkerFailureLog);
  return false;
}

async function loadTabWithGuard(tab, fromAutoRefresh = false) {
  if (!configReady) return false;
  const tabKey = ALL_TABS.includes(tab) ? tab : "dashboard";
  const slot = tabLoadState[tabKey];
  if (slot?.inFlight) return false;
  if (slot) slot.inFlight = true;
  try {
    if (tabKey === "entities") {
      await loadEntities();
      return true;
    }
    if (tabKey === "solplanetRaw") {
      await loadSolplanetRaw();
      return true;
    }
    if (tabKey === "sajRaw") {
      await loadSajRaw();
      return true;
    }
    if (tabKey === "sajControl") {
      if (fromAutoRefresh && stateCache.lastSajControl && isSajControlLocalEditing()) {
        return false;
      }
      await loadSajControl();
      return true;
    }
    if (tabKey === "solplanetControl") {
      if (fromAutoRefresh && solplanetControlBusy) {
        return false;
      }
      await loadSolplanetControl();
      return true;
    }
    if (tabKey === "sampling") {
      await loadSampling();
      return true;
    }
    if (tabKey === "workerLogs") {
      await loadWorkerLogs();
      return true;
    }
    if (tabKey === "workerFailureLog") {
      await loadWorkerFailureLog();
      return true;
    }
    await loadSummary();
    return true;
  } finally {
    if (slot) slot.inFlight = false;
  }
}

function runAutoRefreshRound() {
  if (!configReady) return;
  void loadTabWithGuard(currentTab, true);
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
      runAutoRefreshRound();
    }, safeSeconds * 1000);
  }

  const autoRefreshSelect = document.getElementById("autoRefreshSelect");
  if (autoRefreshSelect) autoRefreshSelect.value = String(safeSeconds);
}

function setActiveTab(tab, load = true) {
  currentTab =
    tab === "entities" ||
    tab === "solplanetRaw" ||
    tab === "sajRaw" ||
    tab === "sajControl" ||
    tab === "solplanetControl" ||
    tab === "sampling" ||
    tab === "workerLogs" ||
    tab === "workerFailureLog"
      ? tab
      : "dashboard";
  localStorage.setItem("activeTab", currentTab);

  const dashboardView = document.getElementById("dashboardView");
  const solplanetRawView = document.getElementById("solplanetRawView");
  const sajRawView = document.getElementById("sajRawView");
  const sajControlView = document.getElementById("sajControlView");
  const solplanetControlView = document.getElementById("solplanetControlView");
  const entitiesView = document.getElementById("entitiesView");
  const samplingView = document.getElementById("samplingView");
  const workerLogsView = document.getElementById("workerLogsView");
  const workerFailureLogView = document.getElementById("workerFailureLogView");
  const tabDashboard = document.getElementById("tabDashboard");
  const tabSolplanetRaw = document.getElementById("tabSolplanetRaw");
  const tabSajRaw = document.getElementById("tabSajRaw");
  const tabSajControl = document.getElementById("tabSajControl");
  const tabSolplanetControl = document.getElementById("tabSolplanetControl");
  const tabEntities = document.getElementById("tabEntities");
  const tabSampling = document.getElementById("tabSampling");
  const tabWorkerLogs = document.getElementById("tabWorkerLogs");
  const tabWorkerFailureLog = document.getElementById("tabWorkerFailureLog");

  const dashboardActive = currentTab === "dashboard";
  const solplanetRawActive = currentTab === "solplanetRaw";
  const sajRawActive = currentTab === "sajRaw";
  const sajControlActive = currentTab === "sajControl";
  const solplanetControlActive = currentTab === "solplanetControl";
  const samplingActive = currentTab === "sampling";
  const workerLogsActive = currentTab === "workerLogs";
  const workerFailureLogActive = currentTab === "workerFailureLog";
  const anyRawActive = solplanetRawActive || sajRawActive;
  if (dashboardView) dashboardView.classList.toggle("hidden", !dashboardActive);
  if (solplanetRawView) solplanetRawView.classList.toggle("hidden", !solplanetRawActive);
  if (sajRawView) sajRawView.classList.toggle("hidden", !sajRawActive);
  if (sajControlView) sajControlView.classList.toggle("hidden", !sajControlActive);
  if (solplanetControlView) solplanetControlView.classList.toggle("hidden", !solplanetControlActive);
  if (entitiesView) {
    entitiesView.classList.toggle(
      "hidden",
      dashboardActive ||
        anyRawActive ||
        samplingActive ||
        sajControlActive ||
        solplanetControlActive ||
        workerLogsActive ||
        workerFailureLogActive
    );
  }
  if (samplingView) samplingView.classList.toggle("hidden", !samplingActive);
  if (workerLogsView) workerLogsView.classList.toggle("hidden", !workerLogsActive);
  if (workerFailureLogView) workerFailureLogView.classList.toggle("hidden", !workerFailureLogActive);
  if (tabDashboard) tabDashboard.classList.toggle("active", dashboardActive);
  if (tabSolplanetRaw) tabSolplanetRaw.classList.toggle("active", solplanetRawActive);
  if (tabSajRaw) tabSajRaw.classList.toggle("active", sajRawActive);
  if (tabSajControl) tabSajControl.classList.toggle("active", sajControlActive);
  if (tabSolplanetControl) tabSolplanetControl.classList.toggle("active", solplanetControlActive);
  if (tabEntities) tabEntities.classList.toggle("active", currentTab === "entities");
  if (tabSampling) tabSampling.classList.toggle("active", samplingActive);
  if (tabWorkerLogs) tabWorkerLogs.classList.toggle("active", workerLogsActive);
  if (tabWorkerFailureLog) tabWorkerFailureLog.classList.toggle("active", workerFailureLogActive);
  if (dashboardActive) {
    window.requestAnimationFrame(() => {
      refreshFlowDiagrams();
    });
  }

  if (load && !tabHasCachedData(currentTab)) {
    void loadCurrentTab();
  }
}

bindChangeIfPresent("langSelect", (event) => {
  const nextLang = event.target.value === "zh" ? "zh" : "en";
  currentLang = nextLang;
  localStorage.setItem("lang", nextLang);
  applyTranslations();
  refreshFlowDiagrams();
  renderSamplingRangeInputContainer();
});

bindChangeIfPresent("autoRefreshSelect", (event) => {
  setAutoRefresh(Number(event.target.value));
});

bindClickIfPresent("refreshBtn", () => {
  void loadCurrentTab();
});
bindClickIfPresent("combined-teslaChargingToggleBtn", () => {
  void toggleTeslaCharging();
});
bindClickIfPresent("configBtn", () => {
  void openConfigModal();
});

bindClickIfPresent("tabDashboard", () => {
  setActiveTab("dashboard");
});
bindClickIfPresent("tabSolplanetRaw", () => {
  setActiveTab("solplanetRaw");
});
bindClickIfPresent("solplanetRawModeCardsBtn", () => {
  setSolplanetRawMode("cards");
});
bindClickIfPresent("solplanetRawModeTableBtn", () => {
  setSolplanetRawMode("table");
});
bindClickIfPresent("tabSajRaw", () => {
  setActiveTab("sajRaw");
});
bindClickIfPresent("tabSajControl", () => {
  setActiveTab("sajControl");
});
bindClickIfPresent("tabSolplanetControl", () => {
  setActiveTab("solplanetControl");
});

bindClickIfPresent("tabEntities", () => {
  setActiveTab("entities");
});
bindClickIfPresent("tabSampling", () => {
  setActiveTab("sampling");
});
bindClickIfPresent("tabWorkerLogs", () => {
  setActiveTab("workerLogs");
});
bindClickIfPresent("tabWorkerFailureLog", () => {
  setActiveTab("workerFailureLog");
});

{
  const filterForm = document.getElementById("filterForm");
  if (filterForm) {
    filterForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      pager.page = 1;
      await loadEntities();
    });
  }
}

bindClickIfPresent("prevPageBtn", async () => {
  if (!pager.hasPrev || pager.page <= 1) return;
  pager.page -= 1;
  await loadEntities();
});

bindClickIfPresent("nextPageBtn", async () => {
  if (!pager.hasNext) return;
  pager.page += 1;
  await loadEntities();
});

bindClickIfPresent("samplingPrevPageBtn", async () => {
  if (!samplingPager.hasPrev || samplingPager.page <= 1) return;
  samplingPager.page -= 1;
  await loadSampling();
});

bindClickIfPresent("samplingNextPageBtn", async () => {
  if (!samplingPager.hasNext) return;
  samplingPager.page += 1;
  await loadSampling();
});

bindChangeIfPresent("samplingSystemSelect", async () => {
  samplingPager.page = 1;
  await loadSampling();
});

bindChangeIfPresent("samplingRangeModeSelect", async () => {
  renderSamplingRangeInputContainer();
  samplingPager.page = 1;
  await loadSampling();
});

bindChangeIfPresent("samplingSmoothModeSelect", () => {
  if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
});
bindClickIfPresent("samplingExportBtn", () => {
  void exportSamplingCsv();
});
bindClickIfPresent("samplingImportBtn", () => {
  const fileInput = document.getElementById("samplingImportFileInput");
  if (!fileInput) return;
  if (typeof fileInput.showPicker === "function") {
    fileInput.showPicker();
    return;
  }
  fileInput.click();
});

{
  const input = document.getElementById("samplingImportFileInput");
  if (input) {
    input.addEventListener("change", (event) => {
      const target = event.target;
      const file = target?.files?.[0];
      if (!file) return;
      void importSamplingCsv(file);
    });
  }
}

bindClickIfPresent("workerLogsPrevPageBtn", async () => {
  if (!workerLogsPager.hasPrev || workerLogsPager.page <= 1) return;
  workerLogsPager.page -= 1;
  await loadWorkerLogs();
});

bindClickIfPresent("workerLogsNextPageBtn", async () => {
  if (!workerLogsPager.hasNext) return;
  workerLogsPager.page += 1;
  await loadWorkerLogs();
});

bindClickIfPresent("workerFailureLogLoadMoreBtn", async () => {
  if (!workerFailureLogState.hasMore) return;
  await loadWorkerFailureLog({ appendOlder: true });
});

bindClickIfPresent("workerLogDetailCloseBtn", () => {
  setWorkerLogDetailModalVisible(false);
});
{
  const modal = document.getElementById("workerLogDetailModal");
  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target === modal) setWorkerLogDetailModalVisible(false);
    });
  }
}
window.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  const modal = document.getElementById("workerLogDetailModal");
  if (modal && !modal.classList.contains("hidden")) {
    setWorkerLogDetailModalVisible(false);
  }
});

bindChangeIfPresent("workerLogsCategorySelect", async () => {
  workerLogsPager.page = 1;
  await loadWorkerLogs();
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
bindClickIfPresent("sajInverterModeApplyBtn", () => {
  void applySajInverterModeTarget();
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
bindClickIfPresent("solplanetLimitsApplyBtn", () => {
  void applySolplanetLimits();
});
bindClickIfPresent("solplanetScheduleSaveBtn", () => {
  void applySolplanetDaySchedule();
});
bindClickIfPresent("solplanetRawSettingApplyBtn", () => {
  void applySolplanetRawSetting();
});
bindClickIfPresent("solplanetRestartApiBtn", () => {
  void restartSolplanetBackendApi();
});
bindChangeIfPresent("solplanetScheduleDayInput", () => {
  renderSolplanetControlSlotsTable();
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
    if (!(target instanceof Element)) return;
    const editor = target.closest(".saj-cell-editor");
    if (!(editor instanceof HTMLButtonElement)) return;
    markSajControlLocalEdit();
    _openSajTableCellEditor(editor);
  });
  sajControlSlotsBody.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    markSajControlLocalEdit();
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
bindClickIfPresent("sajActionModalCloseBtn", () => {
  setSajActionModalVisible(false);
});
bindClickIfPresent("solplanetActionModalCloseBtn", () => {
  setSolplanetActionModalVisible(false);
});
bindChangeIfPresent("sajActionDebugModeInput", (event) => {
  const next = event?.target?.checked;
  setSajActionDebugMode(next);
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

{
  const configForm = document.getElementById("configForm");
  if (configForm) {
    configForm.addEventListener("submit", async (event) => {
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
  }
}

bindClickIfPresent("configCloseBtn", () => {
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
  refreshFlowDiagrams();
});

initFlowDiagrams();
bindClickIfPresent("combined-teslaChargingToggleBtn", () => {
  void toggleTeslaCharging();
});
applyTranslations();
samplingRangeState.day = new Date().toISOString().slice(0, 10);
samplingRangeState.week = samplingRangeState.day;
samplingRangeState.day = getLocalDateText();
samplingRangeState.week = samplingRangeState.day;
samplingRangeState.monthYear = new Date().getFullYear();
samplingRangeState.month = String(new Date().getMonth() + 1);
samplingRangeState.relative = "-6h";
samplingRangeState.endDate = samplingRangeState.day;
samplingRangeState.startDate = getLocalDateText(-1);
{
  const now = new Date();
  now.setMinutes(0, 0, 0);
  samplingRangeState.endDateTime = toLocalDateTimeInputValueFromMs(now.getTime());
  samplingRangeState.startDateTime = toLocalDateTimeInputValueFromMs(now.getTime() - 6 * 3600 * 1000);
}
renderSamplingRangeInputContainer();
setActiveTab(currentTab, false);
setAutoRefresh(autoRefreshSeconds);
void ensureConfigReady().then((ready) => {
  if (ready) {
    void loadCurrentTab();
  }
});
