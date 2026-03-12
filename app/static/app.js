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
    notificationMatrixTab: "Notification",
    notificationMatrixTitle: "Notification",
    notificationMatrixIntro: "Each panel shows the worker notifications configured for that time window. Checkboxes are currently fixed on because the system behavior is already enabled.",
    notificationMatrixNoNotifications: "No dedicated notification is configured for this window at the moment.",
    notificationMatrixAlwaysNote: "This watch is always active and is shown separately from the 24-hour time-window schedule.",
    notificationMatrixCurrentWindowBadge: "Current Window",
    notificationMatrixExpandWindow: "Expand window",
    notificationMatrixCollapseWindow: "Collapse window",
    notificationMatrixProgressWatching: "Watching",
    notificationMatrixProgressConditionMet: "Condition met now",
    notificationMatrixProgressNotified: "Already notified",
    notificationMatrixProgressNoData: "Waiting for live data",
    notificationMatrixProgressWindowTracking: "Only tracked during this window",
    notificationMatrixProgressCurrent: "Current {value}",
    notificationMatrixProgressTriggerAtMost: "Trigger <= {value}",
    notificationMatrixProgressTriggerAtLeast: "Trigger >= {value}",
    notificationMatrixProgressTriggerAbove: "Trigger > {value}",
    notificationMatrixProgressRemaining: "{value} remaining",
    notificationMatrixProgressAboveThreshold: "{value} above threshold",
    notificationMatrixProgressBelowThreshold: "{value} below threshold",
    notificationMatrixWindowFreeEnergyTitle: "Free Energy Window",
    notificationMatrixWindowFreeEnergySummary: "11:00-14:00. Worker mainly controls Tesla charging current here and does not emit a dedicated notification.",
    notificationMatrixWindowAfterFreeShoulderTitle: "After-Free Shoulder",
    notificationMatrixWindowAfterFreeShoulderSummary: "14:00-16:00. Worker watches Solplanet battery headroom before the export period.",
    notificationMatrixWindowAfterFreePeakTitle: "After-Free Peak",
    notificationMatrixWindowAfterFreePeakSummary: "16:00-18:00. Worker keeps watching Solplanet battery headroom before export pricing starts.",
    notificationMatrixWindowExportTitle: "Export Window",
    notificationMatrixWindowExportSummary: "18:00-20:00. Worker tracks export behavior and low-battery/import alarms while Tesla charging follows surplus logic.",
    notificationMatrixWindowPostExportPeakTitle: "Post-Export Peak",
    notificationMatrixWindowPostExportPeakSummary: "20:00-23:00. Worker tries to avoid expensive import and raises early warnings when that risk appears.",
    notificationMatrixWindowOvernightTitle: "Overnight Shoulder",
    notificationMatrixWindowOvernightSummary: "23:00-11:00 next day. Worker observes SAJ battery SOC and sends threshold reminders once per overnight window.",
    notificationMatrixWindowAlwaysTitle: "Always-On Battery Watch",
    notificationMatrixWindowAlwaysSummary: "Not part of the 24-hour window split. Worker sends a one-time notification when either battery newly reaches 100% SOC.",
    notificationRuleSolplanetLowAvailableCapacityTitle: "Solplanet available capacity is low",
    notificationRuleSolplanetLowAvailableCapacityTrigger: "Triggers when Solplanet available capacity drops below 25 kWh during this window.",
    notificationRuleSolplanetLowBatteryTitle: "Solplanet battery is low during export",
    notificationRuleSolplanetLowBatteryTrigger: "Triggers when Solplanet SOC drops below 20% during the export window.",
    notificationRuleGridImportStartedTitle: "Grid import started during export",
    notificationRuleGridImportStartedTrigger: "Triggers once when grid import changes from inactive to active during the export window.",
    notificationRuleSolarSurplusExportEnergyReached5000Title: "Export energy reached 5 kWh",
    notificationRuleSolarSurplusExportEnergyReached5000Trigger: "Triggers once when exported energy in the export window reaches 5 kWh.",
    notificationRuleSolarSurplusExportEnergyReached9000Title: "Export energy reached 9 kWh",
    notificationRuleSolarSurplusExportEnergyReached9000Trigger: "Triggers once when exported energy in the export window reaches 9 kWh.",
    notificationRuleSolplanetLowBatteryPostExportPeakTitle: "Solplanet battery is low in the expensive period",
    notificationRuleSolplanetLowBatteryPostExportPeakTrigger: "Triggers when Solplanet SOC drops below 20% during the post-export peak window.",
    notificationRuleGridImportStartedPostExportPeakTitle: "Grid import started in the expensive period",
    notificationRuleGridImportStartedPostExportPeakTrigger: "Triggers once when grid import changes from inactive to active during the post-export peak window.",
    notificationRuleSajBatteryWatch50Title: "SAJ battery reached 50%",
    notificationRuleSajBatteryWatch50Trigger: "Triggers once per overnight window when SAJ SOC falls to 50% or below.",
    notificationRuleSajBatteryWatch20Title: "SAJ battery reached 20%",
    notificationRuleSajBatteryWatch20Trigger: "Triggers once per overnight window when SAJ SOC falls to 20% or below.",
    notificationRuleSajBatteryFullTitle: "SAJ battery reached 100%",
    notificationRuleSajBatteryFullTrigger: "Triggers once when SAJ battery newly reaches 100% SOC.",
    notificationRuleSolplanetBatteryFullTitle: "Solplanet battery reached 100%",
    notificationRuleSolplanetBatteryFullTrigger: "Triggers once when Solplanet battery newly reaches 100% SOC.",
    combinedDebugTitle: "Combined Debug",
    combinedDebugMeta: "Source {source} · storage_backed {storageBacked} · stale {stale} · sample age {sampleAge}s · kv items {kvCount}",
    combinedCollectorMeta: "Collector: SAJ {saj} · Solplanet {solplanet} · Combined {combined}",
    rawDataTab: "Raw Data",
    rawDataTitle: "Raw Data",
    rawDataSystemLabel: "System",
    rawDataSystemSolplanet: "Solplanet",
    rawDataSystemSaj: "SAJ",
    sajControlTab: "SAJ Control",
    solplanetControlTab: "Solplanet Control",
    entitiesTab: "Entities",
    samplingTab: "Sampling",
    databaseTab: "Database",
    workerLogsTab: "Worker Logs",
    failureLogTab: "Failure Logs",
    databaseTitle: "Database Browser",
    databaseTableLabel: "Table",
    databaseTablePlaceholder: "Select a table",
    databaseMeta: "Table {table} · {columns} columns",
    databaseTotal: "Total {total} rows",
    databasePageInfo: "Page {page}/{totalPages} (showing {count})",
    databaseEmpty: "Select a table to view data.",
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
    sajControlPopupErrorTitle: "Error",
    sajControlPopupCloseBtn: "Close",
    sajControlPopupWorkingModeSummary: "Working mode has been applied (mode_code={modeCode}).",
    sajControlPopupInverterModeSummary: "Inverter target mode apply sent (mode_code={modeCode}, via app_mode_input).",
    sajControlPopupSaveSummary: "Save completed for enable masks and slot edits.",
    sajControlDebugApiTitle: "API Calls",
    sajControlDebugNoApi: "No API calls recorded.",
    sajDebugPurposeSetMode: "Update SAJ app mode input with selected mode code.",
    sajDebugPurposeSetProfile: "Apply a simplified SAJ profile made of one or more low-level settings.",
    sajDebugPurposeSetInverterModeTarget: "Apply target inverter mode by writing SAJ app mode input.",
    sajDebugPurposeSetTogglesMask: "Update charge/discharge enable masks.",
    sajDebugPurposeSetSlot: "Update {kind} slot {slot} fields that changed.",
    sajProfilePanelKicker: "SAJ Profile",
    sajProfileSelectedLabel: "Selected",
    sajProfileOptionSelfConsumption: "Self Consumption",
    sajProfileOptionTimeOfUse: "Time of Use",
    sajProfileOptionMicrogrid: "Microgrid",
    sajProfileDesiredText: "Selected profile: {profile}",
    sajProfileActualText: "Remote profile: {profile}",
    sajProfileStatusPending: "Status: waiting for HA readback to match the selected profile",
    sajProfileStatusSynced: "Status: selected profile matches remote state",
    sajProfileStatusCustom: "Status: remote SAJ state is custom or unsupported by the simple profile set",
    sajProfileStatusUnknown: "Status: remote SAJ state unavailable",
    sajProfileApplyBtn: "Apply",
    sajProfileApplyFailed: "SAJ profile apply failed: {error}",
    sajProfileLoadFailed: "SAJ profile load failed: {error}",
    sajProfileApplySummary: "SAJ profile applied: {profile}.",
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
    dashboardNotificationsTitle: "Notifications",
    dashboardNotificationStateNotified: "Notified",
    dashboardNotificationStateDismissed: "Dismissed",
    dashboardNotificationDismiss: "Dismiss",
    dashboardNotificationLevelAlarm: "Alarm",
    dashboardNotificationLevelWarning: "Warning",
    dashboardNotificationLevelInfo: "Info",
    dashboardNotificationMeta: "Window {window} · Trigger {trigger} · {time}",
    integratedFlowSubtitle: "SAJ solar/grid + SAJ/Solplanet batteries, dual inverters in parallel",
    mobileFlowTitle: "Phone Flow",
    mobileFlowHint: "Compact icon flow with linked devices for phone screens.",
    mobileFlowGridStep: "Grid -> Switchboard",
    mobileFlowSajStep: "SAJ path -> Switchboard",
    mobileFlowSolplanetStep: "Solplanet path -> Switchboard",
    mobileFlowLoadStep: "Switchboard -> Load",
    mobileFlowAdvancedTitle: "Advanced formula",
    mobileFlowSolarToInverter: "Solar -> Inverter 1",
    mobileFlowBatterySoc: "Battery SOC",
    mobileFlowTeslaSoc: "Tesla SOC",
    mobileFlowControl: "Charging Control",
    mobileFlowHomeTitle: "Home",
    mobileFlowSajInverterShort: "SAJ Inv",
    mobileFlowSolplanetInverterShort: "SP Inv",
    mobileFlowSajBatteryShort: "SAJ Batt",
    mobileFlowSolplanetBatteryShort: "SP Batt",
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
    teslaChargingCurrentActualLabel: "Actual",
    teslaChargingCurrentConfiguredLabel: "Set",
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
    batteryRuntimeEstimateCharging: "{hours}h left",
    batteryRuntimeEstimateDischarging: "{hours}h left",
    batteryRuntimeTargetCharging: "{time} Full",
    batteryRuntimeTargetDischarging: "{time} Empty",
    batteryRateCharging: "Charge rate {value}%/h",
    batteryRateDischarging: "Discharge rate {value}%/h",
    batteryRateIdle: "No active charge/discharge",
    batteryRateNoData: "Rate unavailable",
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
    workerLogsCategoryNotification: "Notification",
    workerLogsCategoryOperation: "Operation",
    workerLogsCategoryAlert: "Alert",
    workerLogsSystemLabel: "System",
    workerLogsSystemAll: "All",
    workerLogsServiceLabel: "Service",
    workerLogsServiceAll: "All",
    workerLogsServiceGridSupport: "Grid Support",
    workerLogsServiceSolarSurplus: "Solar Surplus",
    workerLogsServicePeakPriceGuard: "Peak Price Guard",
    workerLogsServiceTeslaObserve: "Tesla Observe",
    workerLogsServiceCombinedAssembly: "Combined Assembly",
    workerLogsTypeFiltersTitle: "Visible Types",
    workerLogsTypeFiltersHint: "Uncheck to hide a type on this page",
    workerLogsTypeFiltersAll: "All",
    workerLogsConfigMeta: "Solplanet config: host {host}",
    workerLogsTableTime: "Time",
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
    workerLogsStatusSend: "Send",
    workerLogsStatusTimeout: "Timeout",
    workerLogsStatusOutsideWindow: "Outside Window",
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
    rawExpandRequest: "Expand request",
    rawCollapseRequest: "Collapse request",
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
    notificationMatrixTab: "通知",
    notificationMatrixTitle: "Notification",
    notificationMatrixIntro: "每个面板展示该时间窗口下 worker 已配置的通知。当前复选框固定为开启，因为系统行为已经默认启用。",
    notificationMatrixNoNotifications: "这个时间窗口当前没有单独配置专门通知。",
    notificationMatrixAlwaysNote: "这个监控始终开启，单独展示，不属于 24 小时时间窗口切分的一部分。",
    notificationMatrixCurrentWindowBadge: "当前窗口",
    notificationMatrixExpandWindow: "展开窗口",
    notificationMatrixCollapseWindow: "折叠窗口",
    notificationMatrixProgressWatching: "监控中",
    notificationMatrixProgressConditionMet: "当前已满足条件",
    notificationMatrixProgressNotified: "已通知",
    notificationMatrixProgressNoData: "等待实时数据",
    notificationMatrixProgressWindowTracking: "只在这个窗口内跟踪",
    notificationMatrixProgressCurrent: "当前 {value}",
    notificationMatrixProgressTriggerAtMost: "触发条件 <= {value}",
    notificationMatrixProgressTriggerAtLeast: "触发条件 >= {value}",
    notificationMatrixProgressTriggerAbove: "触发条件 > {value}",
    notificationMatrixProgressRemaining: "还差 {value}",
    notificationMatrixProgressAboveThreshold: "高于阈值 {value}",
    notificationMatrixProgressBelowThreshold: "低于阈值 {value}",
    notificationMatrixWindowFreeEnergyTitle: "免费电时段",
    notificationMatrixWindowFreeEnergySummary: "11:00-14:00。这个窗口里 worker 主要调整 Tesla 充电行为，不单独发通知。",
    notificationMatrixWindowAfterFreeShoulderTitle: "免费电后肩时段",
    notificationMatrixWindowAfterFreeShoulderSummary: "14:00-16:00。这个窗口里 worker 主要观察 Solplanet 电池余量，为后续 export 时段做准备。",
    notificationMatrixWindowAfterFreePeakTitle: "免费电后峰前时段",
    notificationMatrixWindowAfterFreePeakSummary: "16:00-18:00。这个窗口继续观察 Solplanet 电池余量，确保 export 时段前有足够储能。",
    notificationMatrixWindowExportTitle: "Export 时段",
    notificationMatrixWindowExportSummary: "18:00-20:00。这个窗口会跟踪外送行为，同时监控低电量和电网反向转正向导入告警。",
    notificationMatrixWindowPostExportPeakTitle: "Export 后高价时段",
    notificationMatrixWindowPostExportPeakSummary: "20:00-23:00。这个窗口会尽量避免高价购电，并在风险出现时尽早提醒。",
    notificationMatrixWindowOvernightTitle: "夜间肩时段",
    notificationMatrixWindowOvernightSummary: "23:00-次日11:00。这个窗口只观察 SAJ 电池 SOC，并在每个夜间窗口内按阈值提醒一次。",
    notificationMatrixWindowAlwaysTitle: "始终开启的满电监控",
    notificationMatrixWindowAlwaysSummary: "不属于 24 小时时段切分。任一电池新达到 100% SOC 时发送一次通知。",
    notificationRuleSolplanetLowAvailableCapacityTitle: "Solplanet 可用电量过低",
    notificationRuleSolplanetLowAvailableCapacityTrigger: "当这个窗口内 Solplanet 可用容量低于 25 kWh 时触发。",
    notificationRuleSolplanetLowBatteryTitle: "Export 时段 Solplanet 电量过低",
    notificationRuleSolplanetLowBatteryTrigger: "当 export 时段内 Solplanet SOC 低于 20% 时触发。",
    notificationRuleGridImportStartedTitle: "Export 时段开始从电网取电",
    notificationRuleGridImportStartedTrigger: "当 export 时段内电网状态从未取电切换到开始取电时触发一次。",
    notificationRuleSolarSurplusExportEnergyReached5000Title: "Export 能量达到 5 kWh",
    notificationRuleSolarSurplusExportEnergyReached5000Trigger: "当 export 时段累计外送电量达到 5 kWh 时触发一次。",
    notificationRuleSolarSurplusExportEnergyReached9000Title: "Export 能量达到 9 kWh",
    notificationRuleSolarSurplusExportEnergyReached9000Trigger: "当 export 时段累计外送电量达到 9 kWh 时触发一次。",
    notificationRuleSolplanetLowBatteryPostExportPeakTitle: "高价时段 Solplanet 电量过低",
    notificationRuleSolplanetLowBatteryPostExportPeakTrigger: "当 export 后高价时段内 Solplanet SOC 低于 20% 时触发。",
    notificationRuleGridImportStartedPostExportPeakTitle: "高价时段开始从电网取电",
    notificationRuleGridImportStartedPostExportPeakTrigger: "当 export 后高价时段内电网状态从未取电切换到开始取电时触发一次。",
    notificationRuleSajBatteryWatch50Title: "SAJ 电池降到 50%",
    notificationRuleSajBatteryWatch50Trigger: "每个夜间窗口内，当 SAJ SOC 降到 50% 或更低时触发一次。",
    notificationRuleSajBatteryWatch20Title: "SAJ 电池降到 20%",
    notificationRuleSajBatteryWatch20Trigger: "每个夜间窗口内，当 SAJ SOC 降到 20% 或更低时触发一次。",
    notificationRuleSajBatteryFullTitle: "SAJ 电池达到 100%",
    notificationRuleSajBatteryFullTrigger: "当 SAJ 电池新达到 100% SOC 时触发一次。",
    notificationRuleSolplanetBatteryFullTitle: "Solplanet 电池达到 100%",
    notificationRuleSolplanetBatteryFullTrigger: "当 Solplanet 电池新达到 100% SOC 时触发一次。",
    combinedDebugTitle: "整合数据调试",
    combinedDebugMeta: "来源 {source} · storage_backed {storageBacked} · stale {stale} · 样本年龄 {sampleAge}s · KV 条数 {kvCount}",
    combinedCollectorMeta: "采集器: SAJ {saj} · Solplanet {solplanet} · Combined {combined}",
    rawDataTab: "Raw Data",
    rawDataTitle: "Raw Data",
    rawDataSystemLabel: "系统",
    rawDataSystemSolplanet: "Solplanet",
    rawDataSystemSaj: "SAJ",
    sajControlTab: "SAJ 管理",
    solplanetControlTab: "Solplanet 管理",
    entitiesTab: "实体",
    samplingTab: "采样",
    databaseTab: "Database",
    workerLogsTab: "Worker日志",
    failureLogTab: "失败日志",
    databaseTitle: "数据库浏览",
    databaseTableLabel: "数据表",
    databaseTablePlaceholder: "选择一个数据表",
    databaseMeta: "表 {table} · {columns} 列",
    databaseTotal: "共 {total} 行",
    databasePageInfo: "第 {page}/{totalPages} 页（当前 {count} 行）",
    databaseEmpty: "请选择一个数据表查看数据。",
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
    sajControlPopupErrorTitle: "操作失败",
    sajControlPopupCloseBtn: "关闭",
    sajControlPopupWorkingModeSummary: "工作模式已应用（mode_code={modeCode}）。",
    sajControlPopupInverterModeSummary: "已发送逆变器目标模式下发（mode_code={modeCode}，通过 app_mode_input）。",
    sajControlPopupSaveSummary: "已保存启用掩码和时段修改。",
    sajControlDebugApiTitle: "API 调用明细",
    sajControlDebugNoApi: "本次没有记录到 API 调用。",
    sajDebugPurposeSetMode: "将所选 mode code 写入 SAJ App 模式输入值。",
    sajDebugPurposeSetProfile: "应用一个由多个底层配置组成的简化 SAJ 档位。",
    sajDebugPurposeSetInverterModeTarget: "通过写入 SAJ App 模式输入值来触发逆变器目标模式。",
    sajDebugPurposeSetTogglesMask: "更新充/放电启用掩码。",
    sajDebugPurposeSetSlot: "更新 {kind} 第 {slot} 段发生变化的字段。",
    sajProfilePanelKicker: "SAJ 档位",
    sajProfileSelectedLabel: "已选择",
    sajProfileOptionSelfConsumption: "自发自用",
    sajProfileOptionTimeOfUse: "分时电价",
    sajProfileOptionMicrogrid: "微电网",
    sajProfileDesiredText: "已选择档位：{profile}",
    sajProfileActualText: "远端实际档位：{profile}",
    sajProfileStatusPending: "状态：等待 Home Assistant 读回与已选档位一致",
    sajProfileStatusSynced: "状态：已选档位与远端状态一致",
    sajProfileStatusCustom: "状态：远端 SAJ 处于自定义或当前简化档位未覆盖的状态",
    sajProfileStatusUnknown: "状态：暂时无法判断远端 SAJ 状态",
    sajProfileApplyBtn: "应用",
    sajProfileApplyFailed: "SAJ 档位应用失败：{error}",
    sajProfileLoadFailed: "SAJ 档位加载失败：{error}",
    sajProfileApplySummary: "已应用 SAJ 档位：{profile}。",
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
    dashboardNotificationsTitle: "通知",
    dashboardNotificationStateNotified: "已通知",
    dashboardNotificationStateDismissed: "已忽略",
    dashboardNotificationDismiss: "忽略",
    dashboardNotificationLevelAlarm: "告警",
    dashboardNotificationLevelWarning: "提醒",
    dashboardNotificationLevelInfo: "信息",
    dashboardNotificationMeta: "窗口 {window} · 条件 {trigger} · {time}",
    integratedFlowSubtitle: "SAJ 的 solar/grid + SAJ/Solplanet 电池，双逆变器并联",
    mobileFlowTitle: "手机流程视图",
    mobileFlowHint: "手机端的紧凑图标连线视图。",
    mobileFlowGridStep: "电网 -> 母线配电盘",
    mobileFlowSajStep: "SAJ 路径 -> 母线配电盘",
    mobileFlowSolplanetStep: "Solplanet 路径 -> 母线配电盘",
    mobileFlowLoadStep: "母线配电盘 -> 负载",
    mobileFlowAdvancedTitle: "高级公式",
    mobileFlowSolarToInverter: "太阳能 -> 逆变器 1",
    mobileFlowBatterySoc: "电池 SOC",
    mobileFlowTeslaSoc: "Tesla SOC",
    mobileFlowControl: "充电控制",
    mobileFlowHomeTitle: "家庭",
    mobileFlowSajInverterShort: "SAJ 逆变器",
    mobileFlowSolplanetInverterShort: "SP 逆变器",
    mobileFlowSajBatteryShort: "SAJ 电池",
    mobileFlowSolplanetBatteryShort: "SP 电池",
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
    teslaChargingCurrentActualLabel: "实际",
    teslaChargingCurrentConfiguredLabel: "设置",
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
    batteryRuntimeEstimateCharging: "还有 {hours} 小时",
    batteryRuntimeEstimateDischarging: "还有 {hours} 小时",
    batteryRuntimeTargetCharging: "{time} 充满",
    batteryRuntimeTargetDischarging: "{time} 用完",
    batteryRateCharging: "充电速率 {value}%/小时",
    batteryRateDischarging: "放电速率 {value}%/小时",
    batteryRateIdle: "当前未在充放电",
    batteryRateNoData: "暂时无法估算速率",
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
    workerLogsCategoryNotification: "通知",
    workerLogsCategoryOperation: "操作",
    workerLogsCategoryAlert: "提醒",
    workerLogsSystemLabel: "系统",
    workerLogsSystemAll: "全部",
    workerLogsServiceLabel: "服务",
    workerLogsServiceAll: "全部",
    workerLogsServiceGridSupport: "电网支撑窗口",
    workerLogsServiceSolarSurplus: "太阳能富余窗口",
    workerLogsServicePeakPriceGuard: "高价保护窗口",
    workerLogsServiceTeslaObserve: "特斯拉观测",
    workerLogsServiceCombinedAssembly: "整合组装",
    workerLogsTypeFiltersTitle: "类型显示",
    workerLogsTypeFiltersHint: "取消勾选即可隐藏该类型",
    workerLogsTypeFiltersAll: "全部",
    workerLogsConfigMeta: "Solplanet 配置：host {host}",
    workerLogsTableTime: "时间",
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
    workerLogsStatusSend: "发送通知",
    workerLogsStatusTimeout: "超时",
    workerLogsStatusOutsideWindow: "窗口外",
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
    rawExpandRequest: "展开请求",
    rawCollapseRequest: "折叠请求",
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

const samplingPager = {
  page: 1,
  hasNext: false,
  hasPrev: false,
};
const databasePager = {
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
const workerLogsFilterState = {
  hiddenStatuses: new Set(),
};
let workerLogsDefaultsApplied = false;
const WORKER_LOGS_TABLE_MODE_KEY = "workerLogsTableMode";
const DEFAULT_WORKER_LOG_SCHEMA = {
  categories: {
    all: { value: "all", system: null, services: ["tesla", "notification", "operation", "combined_assembly"], statuses: ["pending", "ok", "failed", "timeout", "skipped", "send", "applied", "noop", "outside_window"] },
    saj: { value: "saj", system: "saj", services: [], statuses: ["pending", "ok", "failed", "timeout", "skipped"] },
    solplanet: { value: "solplanet", system: "solplanet", services: [], statuses: ["pending", "ok", "failed", "timeout", "skipped"] },
    combined: { value: "combined", system: "combined", services: ["combined_assembly"], statuses: ["pending", "ok", "failed", "timeout"] },
    tesla: { value: "tesla", system: "tesla", services: ["tesla"], statuses: ["pending", "ok", "failed", "timeout", "skipped"] },
    notification: { value: "notification", system: "notification", services: ["notification"], statuses: ["pending", "send", "noop", "failed", "timeout"] },
    operation: { value: "operation", system: "operation", services: ["operation"], statuses: ["pending", "applied", "noop", "outside_window", "skipped", "timeout", "failed"] },
  },
  services: {
    tesla: { value: "tesla", category: "tesla", system: "tesla", statuses: ["pending", "ok", "failed", "timeout", "skipped"] },
    notification: { value: "notification", category: "notification", system: "notification", statuses: ["pending", "send", "noop", "failed", "timeout"] },
    operation: { value: "operation", category: "operation", system: "operation", statuses: ["pending", "applied", "noop", "outside_window", "skipped", "timeout", "failed"] },
    combined_assembly: { value: "combined_assembly", category: "combined", system: "combined", statuses: ["pending", "ok", "failed", "timeout"] },
  },
  statuses: {
    pending: { value: "pending" },
    ok: { value: "ok" },
    failed: { value: "failed" },
    timeout: { value: "timeout" },
    skipped: { value: "skipped" },
    outside_window: { value: "outside_window" },
    noop: { value: "noop" },
    applied: { value: "applied" },
    send: { value: "send" },
    dismissed: { value: "dismissed" },
  },
};
let workerLogSchema = DEFAULT_WORKER_LOG_SCHEMA;
let workerLogSchemaPromise = null;
const WORKER_LOG_HUMAN_COLUMNS = [
  "id",
  "time",
  "system",
  "service",
  "status",
  "duration",
  "error_text",
  "result_text",
];
const WORKER_LOG_RAW_COLUMNS = [
  "id",
  "request_token",
  "requested_at_utc",
  "requested_at_epoch",
  "round_id",
  "worker",
  "system",
  "service",
  "method",
  "api_link",
  "ok",
  "status",
  "status_code",
  "duration_ms",
  "error_text",
  "result_text",
  "payload_json",
];
const PAGE_SIZE = 80;
const SAMPLING_PAGE_SIZE = 100;
const DATABASE_PAGE_SIZE = 50;
const WORKER_FAILURE_LOG_PAGE_SIZE = 100;
const AUTO_REFRESH_KEY = "autoRefreshSeconds";
const SOLPLANET_RAW_MODE_KEY = "solplanetRawMode";
const RAW_CARD_COLLAPSE_KEY = "rawCardCollapseState";
const RAW_DATA_SYSTEM_KEY = "rawDataSystem";
const SAJ_ACTION_DEBUG_MODE_KEY = "sajActionDebugMode";
const AUTO_REFRESH_OPTIONS = [0, 5, 10];
const SAJ_CONTROL_EDIT_GRACE_MS = 15000;
const CONFIG_SAMPLE_INTERVAL_OPTIONS = [5, 10, 30, 60, 300];
const BALANCE_TOLERANCE_W = 100;
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
  lastDashboardNotifications: null,
  lastCollectorStatus: null,
  lastSolplanetRaw: {},
  lastSolplanetKv: { phase: "idle", items: [], updated_at: null, error: null },
  lastSajRaw: {},
  lastSajControl: null,
  lastSajProfile: null,
  lastSolplanetControl: null,
  lastSolplanetControlLive: null,
  lastSolplanetControlFetch: null,
  lastSamplingStatus: null,
  lastSamplingDaily: null,
  lastSamplingUsageBySystem: null,
  lastSamplingPage: null,
  lastSamplingSeries: null,
  lastDatabaseTables: null,
  lastDatabasePage: null,
  lastWorkerLogsPage: null,
  lastWorkerFailureLog: null,
  rawCardMode: {},
  rawCardCollapse: (() => {
    try {
      const raw = localStorage.getItem(RAW_CARD_COLLAPSE_KEY);
      const parsed = raw ? JSON.parse(raw) : {};
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  })(),
  systemLoadMeta: {
    saj: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
    solplanet: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
    combined: { phase: "idle", updatedAt: null, quality: "ok", count: 0 },
  },
};
let teslaControlBusy = false;

const NOTIFICATION_MATRIX_WINDOWS = [
  {
    id: "free_energy",
    schedule: "11:00-14:00",
    titleKey: "notificationMatrixWindowFreeEnergyTitle",
    summaryKey: "notificationMatrixWindowFreeEnergySummary",
    notifications: [],
  },
  {
    id: "after_free_shoulder",
    schedule: "14:00-16:00",
    titleKey: "notificationMatrixWindowAfterFreeShoulderTitle",
    summaryKey: "notificationMatrixWindowAfterFreeShoulderSummary",
    notifications: [
      {
        level: "alarm",
        code: "solplanet_low_available_capacity",
        titleKey: "notificationRuleSolplanetLowAvailableCapacityTitle",
        triggerKey: "notificationRuleSolplanetLowAvailableCapacityTrigger",
      },
    ],
  },
  {
    id: "after_free_peak",
    schedule: "16:00-18:00",
    titleKey: "notificationMatrixWindowAfterFreePeakTitle",
    summaryKey: "notificationMatrixWindowAfterFreePeakSummary",
    notifications: [
      {
        level: "alarm",
        code: "solplanet_low_available_capacity",
        titleKey: "notificationRuleSolplanetLowAvailableCapacityTitle",
        triggerKey: "notificationRuleSolplanetLowAvailableCapacityTrigger",
      },
    ],
  },
  {
    id: "export_window",
    schedule: "18:00-20:00",
    titleKey: "notificationMatrixWindowExportTitle",
    summaryKey: "notificationMatrixWindowExportSummary",
    notifications: [
      {
        level: "warning",
        code: "solplanet_low_battery",
        titleKey: "notificationRuleSolplanetLowBatteryTitle",
        triggerKey: "notificationRuleSolplanetLowBatteryTrigger",
      },
      {
        level: "alarm",
        code: "grid_import_started",
        titleKey: "notificationRuleGridImportStartedTitle",
        triggerKey: "notificationRuleGridImportStartedTrigger",
      },
      {
        level: "alarm",
        code: "solar_surplus_export_energy_reached_5000",
        titleKey: "notificationRuleSolarSurplusExportEnergyReached5000Title",
        triggerKey: "notificationRuleSolarSurplusExportEnergyReached5000Trigger",
      },
      {
        level: "alarm",
        code: "solar_surplus_export_energy_reached_9000",
        titleKey: "notificationRuleSolarSurplusExportEnergyReached9000Title",
        triggerKey: "notificationRuleSolarSurplusExportEnergyReached9000Trigger",
      },
    ],
  },
  {
    id: "post_export_peak",
    schedule: "20:00-23:00",
    titleKey: "notificationMatrixWindowPostExportPeakTitle",
    summaryKey: "notificationMatrixWindowPostExportPeakSummary",
    notifications: [
      {
        level: "warning",
        code: "solplanet_low_battery_post_export_peak",
        titleKey: "notificationRuleSolplanetLowBatteryPostExportPeakTitle",
        triggerKey: "notificationRuleSolplanetLowBatteryPostExportPeakTrigger",
      },
      {
        level: "alarm",
        code: "grid_import_started_post_export_peak",
        titleKey: "notificationRuleGridImportStartedPostExportPeakTitle",
        triggerKey: "notificationRuleGridImportStartedPostExportPeakTrigger",
      },
    ],
  },
  {
    id: "overnight_shoulder",
    schedule: "23:00-11:00(+1d)",
    titleKey: "notificationMatrixWindowOvernightTitle",
    summaryKey: "notificationMatrixWindowOvernightSummary",
    notifications: [
      {
        level: "warning",
        code: "saj_battery_watch_50_percent",
        titleKey: "notificationRuleSajBatteryWatch50Title",
        triggerKey: "notificationRuleSajBatteryWatch50Trigger",
      },
      {
        level: "alarm",
        code: "saj_battery_watch_20_percent",
        titleKey: "notificationRuleSajBatteryWatch20Title",
        triggerKey: "notificationRuleSajBatteryWatch20Trigger",
      },
    ],
  },
  {
    id: "always",
    schedule: "always",
    titleKey: "notificationMatrixWindowAlwaysTitle",
    summaryKey: "notificationMatrixWindowAlwaysSummary",
    noteKey: "notificationMatrixAlwaysNote",
    notifications: [
      {
        level: "info",
        code: "saj_battery_full",
        titleKey: "notificationRuleSajBatteryFullTitle",
        triggerKey: "notificationRuleSajBatteryFullTrigger",
      },
      {
        level: "info",
        code: "solplanet_battery_full",
        titleKey: "notificationRuleSolplanetBatteryFullTitle",
        triggerKey: "notificationRuleSolplanetBatteryFullTrigger",
      },
    ],
  },
];

function getLang() {
  const saved = localStorage.getItem("lang");
  if (saved === "en" || saved === "zh") return saved;
  const browserLang = (navigator.language || "").toLowerCase();
  return browserLang.startsWith("zh") ? "zh" : "en";
}

let currentLang = getLang();
let currentTab = ["dashboard", "notificationMatrix", "rawData", "solplanetControl", "sampling", "database", "workerLogs", "workerFailureLog"].includes(localStorage.getItem("activeTab"))
  ? localStorage.getItem("activeTab")
  : "dashboard";
const ALL_TABS = ["dashboard", "notificationMatrix", "rawData", "solplanetControl", "sampling", "database", "workerLogs", "workerFailureLog"];
let solplanetRawMode = localStorage.getItem(SOLPLANET_RAW_MODE_KEY) === "table" ? "table" : "cards";
let rawDataSystem = localStorage.getItem(RAW_DATA_SYSTEM_KEY) === "saj" ? "saj" : "solplanet";
let sajActionDebugMode = localStorage.getItem(SAJ_ACTION_DEBUG_MODE_KEY) === "1";
let autoRefreshTimerId = null;
let autoRefreshSeconds = getAutoRefreshSeconds();
let sajControlLastEditAt = 0;
let solplanetControlBusy = false;
const dashboardNotificationDismissBusy = new Set();
let summaryRequestId = 0;
let configReady = false;
let samplingChart = null;
let samplingChartFocusSeries = null;
let samplingChartLastPayload = null;
let samplingChartHandlersBound = false;
let samplingTotalsChart = null;
let samplingRangeApplyingFromBrush = false;
let samplingLegendSyncing = false;
let notificationMatrixCollapseState = {};
let notificationMatrixCollapseInitialized = false;
const tabLoadState = {
  dashboard: { inFlight: false },
  notificationMatrix: { inFlight: false },
  entities: { inFlight: false },
  rawData: { inFlight: false },
  solplanetControl: { inFlight: false },
  sampling: { inFlight: false },
  database: { inFlight: false },
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
  if (!kind || normalized.trim() === "-" || normalized.trim() === "") return escapeHtml(normalized);
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

function formatWorkerLogLocalTimeFromEpoch(epochSeconds) {
  const raw = Number(epochSeconds);
  if (!Number.isFinite(raw)) return "-";
  const dt = new Date(raw * 1000);
  if (Number.isNaN(dt.getTime())) return "-";
  return formatDateTimeWithAgo(dt.toISOString());
}

function getWorkerLogsTableMode() {
  const raw = document.getElementById("workerLogsTableModeSelect")?.value || "human_readable_table";
  return raw === "raw_table" ? "raw_table" : "human_readable_table";
}

function workerLogColumnsForMode(mode) {
  return mode === "raw_table" ? WORKER_LOG_RAW_COLUMNS : WORKER_LOG_HUMAN_COLUMNS;
}

function renderWorkerLogsHead() {
  const head = document.getElementById("workerLogsHead");
  if (!head) return;
  const columns = workerLogColumnsForMode(getWorkerLogsTableMode());
  head.innerHTML = `<tr>${columns.map((name) => `<th>${escapeHtml(name)}</th>`).join("")}</tr>`;
}

function workerLogCellValue(item, column, statusPresentation) {
  if (column === "time") {
    const formattedUtc = formatDateTimeWithAgo(item?.requested_at_utc || null);
    return formattedUtc !== "-" ? formattedUtc : formatWorkerLogLocalTimeFromEpoch(item?.requested_at_epoch);
  }
  if (column === "duration") {
    const raw = Number(item?.duration_ms);
    if (!Number.isFinite(raw)) return "-";
    if (raw < 1000) return `${raw.toFixed(raw < 100 ? 1 : 0)} ms`;
    return `${(raw / 1000).toFixed(raw < 10_000 ? 2 : 1)} s`;
  }
  if (column === "status") {
    return item.status || statusPresentation.text || "-";
  }
  if (column === "payload_json") {
    return item?.payload_json == null ? "-" : JSON.stringify(item.payload_json);
  }
  if (column === "ok") {
    return item.ok == null ? "-" : String(Boolean(item.ok));
  }
  const value = item?.[column];
  if (value == null || value === "") return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
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
  renderSajProfilePanel();
  renderSolplanetControlFromCache();
  renderNotificationMatrix();
  rerenderSamplingViewFromCache();
  rerenderDatabaseViewFromCache();
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

function setDatabaseRowDetailModalVisible(visible) {
  const modal = document.getElementById("databaseRowDetailModal");
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
  return currentLang === "zh" ? `明天 ${timeText}` : `tmr ${timeText}`;
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
  if (!projection) return "";
  if (projection.mode === "idle") return "";
  return t(
    projection.mode === "charging" ? "batteryRuntimeEstimateCharging" : "batteryRuntimeEstimateDischarging",
    {
      hours: formatTrimmedDecimal(projection.runtimeHours, 1),
    },
  );
}

function formatBatteryRuntimeTarget(system, batterySoc, batteryW) {
  const projection = getBatteryRuntimeProjection(system, batterySoc, batteryW);
  if (!projection) return "";
  if (projection.mode === "idle") return "";
  return t(
    projection.mode === "charging" ? "batteryRuntimeTargetCharging" : "batteryRuntimeTargetDischarging",
    {
      time: projection.timeText,
    },
  );
}

function formatBatteryRuntimeEstimateHtml(system, batterySoc, batteryW) {
  const estimateText = formatBatteryRuntimeEstimate(system, batterySoc, batteryW);
  const targetText = formatBatteryRuntimeTarget(system, batterySoc, batteryW);
  if (!estimateText && !targetText) return "";
  return [
    estimateText ? `<span class="battery-runtime-line">${escapeHtml(estimateText)}</span>` : "",
    targetText ? `<span class="battery-runtime-line">${escapeHtml(targetText)}</span>` : "",
  ].join("");
}

function getBatterySocRateProjection(system, batteryW) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  if (!Number.isFinite(capacityKwh) || batteryW === null || batteryW === undefined) {
    return null;
  }

  const powerW = Number(batteryW);
  if (!Number.isFinite(powerW)) return null;
  if (Math.abs(powerW) < POWER_FLOW_ACTIVE_THRESHOLD_W) return { mode: "idle" };

  const percentPerHour = (Math.abs(powerW) * 100) / (capacityKwh * 1000);
  if (!Number.isFinite(percentPerHour) || percentPerHour < 0) return null;
  return {
    mode: powerW < 0 ? "charging" : "discharging",
    percentPerHour,
  };
}

function formatBatterySocRateEstimate(system, batteryW) {
  const projection = getBatterySocRateProjection(system, batteryW);
  if (!projection) return "";
  if (projection.mode === "idle") return "";
  return t(
    projection.mode === "charging" ? "batteryRateCharging" : "batteryRateDischarging",
    { value: formatTrimmedDecimal(projection.percentPerHour, 1) },
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
  byMarkerId: new Map(),
};

const SAJ_BATTERY_CARD_SCALE = 1.5;
const SAJ_SYSTEM_BATTERY_NODE_SIZE = {
  width: Math.round(196 * SAJ_BATTERY_CARD_SCALE),
  height: Math.round(168 * SAJ_BATTERY_CARD_SCALE),
};
const SAJ_COMBINED_BATTERY_NODE_SIZE = {
  width: 230,
  height: Math.round(176 * SAJ_BATTERY_CARD_SCALE),
};
const SOLPLANET_COMBINED_BATTERY_NODE_SIZE = {
  width: 230,
  height: Math.round(176 * SAJ_BATTERY_CARD_SCALE),
};
const COMBINED_DIAGRAM_VIEWPORT = { width: 1340, height: 720 };
const COMBINED_LAYOUT = {
  nodes: {
    grid: { x: 582, y: 594 },
    solar: { x: 8, y: 594 },
    switchboard: { x: 558, y: 318 },
    load: { x: 312, y: 136 },
    tesla: { x: 830, y: 0 },
    battery1: { x: 24, y: 32 },
    inverter1: { x: 24, y: 360 },
    inverter2: { x: 1113, y: 354 },
    battery2: { x: 1086, y: 32 },
  },
  edges: {
    gridToSwitchboard: {
      points: [{ x: 670, y: 594 }, { x: 670, y: 470 }],
      labelPosition: { x: 620, y: 532 },
    },
    solarToInverter1: {
      points: [{ x: 96, y: 594 }, { x: 96, y: 468 }],
      labelPosition: { x: 130, y: 531 },
    },
    switchboardToTotalLoad: {
      points: [{ x: 670, y: 318 }, { x: 670, y: 205 }],
      labelPosition: { x: 670, y: 300 },
      lineCap: "butt",
      lineJoin: "miter",
    },
    switchboardToTesla: {
      points: [{ x: 670, y: 198 }, { x: 830, y: 198 }],
      labelPosition: { x: 750, y: 230 },
      lineCap: "butt",
      lineJoin: "miter",
      tailUpperGapLength: 9,
      tailRoundedCorner: true,
    },
    switchboardToHomeLoad: {
      points: [{ x: 670, y: 198 }, { x: 496, y: 198 }],
      labelPosition: { x: 583, y: 230 },
      lineCap: "butt",
      lineJoin: "miter",
      tailUpperGapLength: 9,
      tailRoundedCorner: true,
    },
    battery1ToInverter1: {
      points: [{ x: 114, y: 296 }, { x: 114, y: 360 }],
      labelPosition: { x: 180, y: 328 },
    },
    battery2ToInverter2: {
      points: [{ x: 1201, y: 296 }, { x: 1201, y: 354 }],
      labelPosition: { x: 1135, y: 325 },
    },
    inverter1ToSwitchboard: {
      points: [{ x: 204, y: 414 }, { x: 558, y: 414 }],
      labelPosition: { x: 381, y: 446 },
    },
    inverter2ToSwitchboard: {
      points: [{ x: 1113, y: 414 }, { x: 782, y: 414 }],
      labelPosition: { x: 948, y: 446 },
    },
  },
};

function buildSystemDiagramSpec(system) {
  const prefix = String(system);
  const batteryNodeSize = system === "saj"
    ? SAJ_SYSTEM_BATTERY_NODE_SIZE
    : { width: 196, height: 168 };
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
        width: batteryNodeSize.width,
        height: batteryNodeSize.height,
        lines: [
          {
            type: "soc",
            fillId: `${prefix}-batterySocFill`,
            valueId: `${prefix}-batterySocValue`,
            energyId: `${prefix}-batteryEnergyValue`,
            usableId: `${prefix}-batteryUsableValue`,
            runtimeId: `${prefix}-batteryRuntimeValue`,
            rateId: `${prefix}-batteryRateValue`,
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
  const layout = COMBINED_LAYOUT;
  return {
    layout: "explicit",
    coordinateSystem: "bottom-left",
    fixedViewport: true,
    showViewportFrame: true,
    viewport: COMBINED_DIAGRAM_VIEWPORT,
    nodes: [
      {
        id: "combined-gridNode",
        kind: "grid",
        icon: "grid",
        title: "Grid",
        titleKey: "gridTitle",
        width: 176,
        height: 102,
        position: layout.nodes.grid,
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
        position: layout.nodes.solar,
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
        position: layout.nodes.switchboard,
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
        position: layout.nodes.load,
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
        position: layout.nodes.tesla,
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
        width: SAJ_COMBINED_BATTERY_NODE_SIZE.width,
        height: SAJ_COMBINED_BATTERY_NODE_SIZE.height,
        position: layout.nodes.battery1,
        lines: [
          {
            type: "soc",
            fillId: "combined-battery1SocFill",
            valueId: "combined-battery1SocValue",
            energyId: "combined-battery1EnergyValue",
            usableId: "combined-battery1UsableValue",
            runtimeId: "combined-battery1RuntimeValue",
            rateId: "combined-battery1RateValue",
          },
        ],
      },
      {
        id: "combined-inverter1Node",
        kind: "inverter",
        side: "left",
        icon: "inverter",
        title: "SAJ Inverter",
        width: 180,
        height: 108,
        position: layout.nodes.inverter1,
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
        position: layout.nodes.inverter2,
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
        width: SOLPLANET_COMBINED_BATTERY_NODE_SIZE.width,
        height: SOLPLANET_COMBINED_BATTERY_NODE_SIZE.height,
        position: layout.nodes.battery2,
        lines: [
          {
            type: "soc",
            fillId: "combined-battery2SocFill",
            valueId: "combined-battery2SocValue",
            energyId: "combined-battery2EnergyValue",
            usableId: "combined-battery2UsableValue",
            runtimeId: "combined-battery2RuntimeValue",
            rateId: "combined-battery2RateValue",
          },
        ],
      },
    ],
    edges: [
      {
        id: "combined-lineGridToSwitchboard",
        source: "combined-gridNode",
        target: "combined-switchboardNode",
        labelId: "combined-flowLabelGridToSwitchboard",
        points: layout.edges.gridToSwitchboard.points,
        labelPosition: layout.edges.gridToSwitchboard.labelPosition,
      },
      {
        id: "combined-lineSolarToInverter1B",
        source: "combined-solarNode",
        target: "combined-inverter1Node",
        labelId: "combined-flowLabelSolarToInverter1",
        points: layout.edges.solarToInverter1.points,
        labelPosition: layout.edges.solarToInverter1.labelPosition,
      },
      {
        id: "combined-lineSwitchboardToTeslaB",
        source: "combined-switchboardNode",
        target: "combined-teslaNode",
        labelId: "combined-flowLabelSwitchboardToTesla",
        markerId: "combined-flowMarkerSwitchboardToTesla",
        markerClassName: "flow-edge-marker-disconnected",
        points: layout.edges.switchboardToTesla.points,
        labelPosition: layout.edges.switchboardToTesla.labelPosition,
        lineCap: layout.edges.switchboardToTesla.lineCap,
        lineJoin: layout.edges.switchboardToTesla.lineJoin,
        tailUpperGapLength: layout.edges.switchboardToTesla.tailUpperGapLength,
        tailRoundedCorner: layout.edges.switchboardToTesla.tailRoundedCorner,
      },
      {
        id: "combined-lineSwitchboardToHomeLoad",
        source: "combined-switchboardNode",
        target: "combined-loadNode",
        labelId: "combined-flowLabelSwitchboardToHomeLoad",
        points: layout.edges.switchboardToHomeLoad.points,
        labelPosition: layout.edges.switchboardToHomeLoad.labelPosition,
        lineCap: layout.edges.switchboardToHomeLoad.lineCap,
        lineJoin: layout.edges.switchboardToHomeLoad.lineJoin,
        tailUpperGapLength: layout.edges.switchboardToHomeLoad.tailUpperGapLength,
        tailRoundedCorner: layout.edges.switchboardToHomeLoad.tailRoundedCorner,
      },
      {
        id: "combined-lineSwitchboardToTotalLoad",
        source: "combined-switchboardNode",
        target: "combined-teslaNode",
        labelId: "combined-flowLabelSwitchboardToTotalLoad",
        glowWidth: 28,
        lineWidth: 18,
        coreWidth: 6,
        points: layout.edges.switchboardToTotalLoad.points,
        labelPosition: layout.edges.switchboardToTotalLoad.labelPosition,
        lineCap: layout.edges.switchboardToTotalLoad.lineCap,
        lineJoin: layout.edges.switchboardToTotalLoad.lineJoin,
      },
      {
        id: "combined-lineBattery1ToInverter1",
        source: "combined-battery1Node",
        target: "combined-inverter1Node",
        labelId: "combined-flowLabelBattery1ToInverter1",
        points: layout.edges.battery1ToInverter1.points,
        labelPosition: layout.edges.battery1ToInverter1.labelPosition,
      },
      {
        id: "combined-lineBattery2ToInverter2",
        source: "combined-battery2Node",
        target: "combined-inverter2Node",
        labelId: "combined-flowLabelBattery2ToInverter2",
        points: layout.edges.battery2ToInverter2.points,
        labelPosition: layout.edges.battery2ToInverter2.labelPosition,
      },
      {
        id: "combined-lineInverter1ToSwitchboardB",
        source: "combined-inverter1Node",
        target: "combined-switchboardNode",
        labelId: "combined-flowLabelInverter1ToSwitchboard",
        points: layout.edges.inverter1ToSwitchboard.points,
        labelPosition: layout.edges.inverter1ToSwitchboard.labelPosition,
      },
      {
        id: "combined-lineInverter2ToSwitchboardB",
        source: "combined-inverter2Node",
        target: "combined-switchboardNode",
        labelId: "combined-flowLabelInverter2ToSwitchboard",
        points: layout.edges.inverter2ToSwitchboard.points,
        labelPosition: layout.edges.inverter2ToSwitchboard.labelPosition,
      },
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
    if (edge.markerId) flowDiagrams.byMarkerId.set(edge.markerId, diagram);
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

function setFlowEdgeMarker(id, visible) {
  const diagram = flowDiagrams.byMarkerId.get(id);
  if (diagram && typeof diagram.setEdgeMarker === "function" && diagram.setEdgeMarker(id, visible)) return;
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle("visible", Boolean(visible));
}

function getFlowQuality(flowPayload, matchedEntities) {
  if (flowPayload && flowPayload.__load_error) return "failed";
  if (flowPayload && (flowPayload.stale || flowPayload?.meta?.stale)) return "stale";
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
  rateValueId,
  socDataKind = "real",
  energyDataKind = "estimate",
  usableDataKind = "estimate",
  runtimeDataKind = "estimate",
  rateDataKind = "estimate",
}) {
  if (soc === null || soc === undefined) {
    setText(socValueId, "-");
    if (energyValueId) setHtml(energyValueId, formatValueWithDataKindHtml(formatBatteryEnergyKwh(system, null), energyDataKind));
    if (usableValueId) setHtml(usableValueId, formatValueWithDataKindHtml(formatBatteryUsableKwh(system, null), usableDataKind));
    if (runtimeValueId) setHtml(runtimeValueId, formatBatteryRuntimeEstimateHtml(system, null, batteryW));
    if (rateValueId) setText(rateValueId, formatBatterySocRateEstimate(system, batteryW));
    setSocFillLevel(socFillId, 0);
    setSocTextContrastBySocValueId(socValueId, 0);
    return;
  }

  const clampedSoc = Math.max(0, Math.min(100, Number(soc)));
  setHtml(socValueId, formatValueWithDataKindHtml(`${clampedSoc.toFixed(0)}%`, socDataKind));
  if (energyValueId) setHtml(energyValueId, formatValueWithDataKindHtml(formatBatteryEnergyKwh(system, clampedSoc), energyDataKind));
  if (usableValueId) setHtml(usableValueId, formatValueWithDataKindHtml(formatBatteryUsableKwh(system, clampedSoc), usableDataKind));
  if (runtimeValueId) setHtml(runtimeValueId, formatBatteryRuntimeEstimateHtml(system, clampedSoc, batteryW));
  if (rateValueId) setText(rateValueId, formatBatterySocRateEstimate(system, batteryW));
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

function sajProfileLabel(profileId) {
  if (profileId === "self_consumption") return t("sajProfileOptionSelfConsumption");
  if (profileId === "time_of_use") return t("sajProfileOptionTimeOfUse");
  if (profileId === "microgrid") return t("sajProfileOptionMicrogrid");
  return "-";
}

function setSajDashboardProfilePopoverVisible(visible) {
  const popover = document.getElementById("sajDashboardProfilePopover");
  if (!popover) return;
  if (!visible) {
    const active = document.activeElement;
    if (active instanceof HTMLElement && popover.contains(active)) active.blur();
  }
  popover.classList.toggle("hidden", !visible);
  popover.setAttribute("aria-hidden", visible ? "false" : "true");
}

function toggleSajDashboardProfilePopover() {
  const popover = document.getElementById("sajDashboardProfilePopover");
  if (!popover) return;
  setSajDashboardProfilePopoverVisible(popover.classList.contains("hidden"));
}

function renderSajProfileFields(payload, ids) {
  const select = document.getElementById(ids.selectId);
  if (!payload) {
    if (select) select.value = "self_consumption";
    setText(ids.desiredId, "-");
    setText(ids.actualId, "-");
    setText(ids.statusId, "-");
    return;
  }

  const selectedProfile = payload.selected_profile || payload.effective_profile || "self_consumption";
  const actualProfile = payload.actual_profile || null;
  if (select) select.value = selectedProfile;
  setText(ids.desiredId, t("sajProfileDesiredText", { profile: sajProfileLabel(selectedProfile) }));
  setText(ids.actualId, t("sajProfileActualText", { profile: sajProfileLabel(actualProfile) }));

  let statusKey = "sajProfileStatusUnknown";
  if (payload.pending_remote_sync) statusKey = "sajProfileStatusPending";
  else if (actualProfile && selectedProfile && actualProfile === selectedProfile) statusKey = "sajProfileStatusSynced";
  else if (payload.is_custom_remote_state) statusKey = "sajProfileStatusCustom";
  setText(ids.statusId, t(statusKey));
}

function getSajDashboardModeText() {
  const profileState = stateCache.lastSajProfile?.profile_state || stateCache.lastSajProfile || null;
  const actualProfile = profileState?.actual_profile;
  const selectedProfile = profileState?.selected_profile || profileState?.effective_profile || null;
  const actualLabel = actualProfile ? sajProfileLabel(actualProfile) : "";
  const selectedLabel = selectedProfile ? sajProfileLabel(selectedProfile) : "";
  if (actualLabel && selectedLabel) {
    return actualProfile === selectedProfile ? actualLabel : `${actualLabel} -> ${selectedLabel}`;
  }
  if (actualLabel) return actualLabel;
  if (selectedLabel) return selectedLabel;

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

function renderSajProfilePanel() {
  const payload = stateCache.lastSajProfile?.profile_state || stateCache.lastSajProfile || null;
  renderSajProfileFields(payload, {
    selectId: "sajProfileSelect",
    desiredId: "sajProfileDesiredText",
    actualId: "sajProfileActualText",
    statusId: "sajProfileStatusText",
  });
  renderSajProfileFields(payload, {
    selectId: "sajDashboardProfileSelect",
    desiredId: "sajDashboardProfileDesiredText",
    actualId: "sajDashboardProfileActualText",
    statusId: "sajDashboardProfileStatusText",
  });
}

async function loadSajProfile() {
  try {
    const payload = await fetchJson("/api/saj/control/profile", { timeoutMs: 8000 });
    stateCache.lastSajProfile = payload;
    renderSajProfilePanel();
    renderSummary(stateCache.lastSummary || { combinedFlow: { metrics: {} } });
  } catch (err) {
    setText("sajProfileStatusText", t("sajProfileLoadFailed", { error: String(err) }));
  }
}

async function applySajProfile(selectId = "sajProfileSelect") {
  const primarySelect = document.getElementById(selectId);
  const fallbackSelect = document.getElementById(selectId === "sajProfileSelect" ? "sajDashboardProfileSelect" : "sajProfileSelect");
  const profileId = primarySelect?.value || fallbackSelect?.value || "self_consumption";
  const fromDashboardPopover = selectId === "sajDashboardProfileSelect";
  try {
    if (fromDashboardPopover) setSajDashboardProfilePopoverVisible(false);
    const payload = await fetchJson("/api/saj/control/profile", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ profile_id: profileId }),
      timeoutMs: 12000,
    });
    showSajActionSuccess(t("sajProfileApplySummary", { profile: sajProfileLabel(profileId) }), [
      {
        method: "PUT",
        path: "/api/saj/control/profile",
        purposeKey: "sajDebugPurposeSetProfile",
      },
    ]);
    stateCache.lastSajProfile = payload.profile_state || payload;
    stateCache.lastSajControl = payload;
    try {
      renderSajProfilePanel();
      renderSajControlFromCache();
      renderSummary(stateCache.lastSummary || { combinedFlow: { metrics: {} } });
    } catch (renderErr) {
      console.error("SAJ profile apply render failed", renderErr);
    }
  } catch (err) {
    const errorText = t("sajProfileApplyFailed", { error: String(err) });
    setText("sajProfileStatusText", errorText);
    setText("sajDashboardProfileStatusText", errorText);
    showSajActionError(errorText, [
      {
        method: "PUT",
        path: "/api/saj/control/profile",
        purposeKey: "sajDebugPurposeSetProfile",
      },
    ]);
  }
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
    rateValueId: flowId(system, "batteryRateValue"),
    socDataKind: "real",
    energyDataKind: "estimate",
    usableDataKind: "estimate",
    runtimeDataKind: "estimate",
    rateDataKind: "estimate",
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

function formatTeslaCurrentValue(actualValue, configuredValue, unit) {
  const actualNumeric = toFiniteNumber(actualValue);
  const configuredNumeric = toFiniteNumber(configuredValue);
  const unitText = String(unit || "").trim();
  const normalizedUnit = currentLang === "zh" && unitText.toLowerCase() === "a"
    ? "安"
    : (unitText || (currentLang === "zh" ? "安" : "A"));
  if (actualNumeric === null && configuredNumeric === null) return `${t("teslaChargingCurrentLabel")} -`;
  const actualText = actualNumeric === null ? "-" : `${actualNumeric.toFixed(1)}${normalizedUnit}`;
  const configuredText = configuredNumeric === null ? "-" : `${configuredNumeric.toFixed(1)}${normalizedUnit}`;
  return `${t("teslaChargingCurrentLabel")} ${t("teslaChargingCurrentActualLabel")} ${actualText} · ${t("teslaChargingCurrentConfiguredLabel")} ${configuredText}`;
}

function formatTeslaCurrentValueHtml(actualValue, configuredValue, unit, kind) {
  const actualNumeric = toFiniteNumber(actualValue);
  const configuredNumeric = toFiniteNumber(configuredValue);
  const unitText = String(unit || "").trim();
  const normalizedUnit = currentLang === "zh" && unitText.toLowerCase() === "a"
    ? "安"
    : (unitText || (currentLang === "zh" ? "安" : "A"));
  const actualText = actualNumeric === null ? "-" : `${actualNumeric.toFixed(1)}${normalizedUnit}`;
  const configuredText = configuredNumeric === null ? "-" : `${configuredNumeric.toFixed(1)}${normalizedUnit}`;
  const letter = dataKindBadgeLetter(kind);
  const tooltip = dataKindTooltipText(kind);
  return (
    `<span class="data-kind-value tesla-current-stack">` +
    `<span class="data-kind-main tesla-current-line">${escapeHtml(t("teslaChargingCurrentActualLabel"))} ${escapeHtml(actualText)}</span>` +
    `<span class="data-kind-main tesla-current-line">${escapeHtml(t("teslaChargingCurrentConfiguredLabel"))} ${escapeHtml(configuredText)}</span>` +
    `<span class="data-kind-badge data-kind-${escapeHtml(kind)}" data-kind="${escapeHtml(kind)}" data-tooltip="${escapeHtml(tooltip)}" title="${escapeHtml(tooltip)}" aria-label="${escapeHtml(tooltip)}">${escapeHtml(letter)}</span>` +
    `</span>`
  );
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

function isTeslaConnected(teslaInfo = null) {
  const cableConnected = teslaInfo?.teslaCableConnected;
  if (typeof cableConnected === "boolean") return cableConnected;
  const connectionState = String(teslaInfo?.teslaConnectionState || "").trim().toLowerCase();
  if (connectionState === "charging" || connectionState === "plugged_not_charging") return true;
  if (connectionState === "unplugged") return false;
  return null;
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
    configuredCurrentA: toFiniteNumber(charging?.configured_current_amps),
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
  const displayItems = combinedFlow?.display?.items || {};
  const tesla = combinedFlow?.tesla || {};
  const charging = tesla?.charging || {};
  const battery = tesla?.battery || {};
  const control = tesla?.control || {};
  return {
    chargingW: toFiniteNumber(displayItems.tesla_charge_power?.value ?? charging?.power_w),
    entityId: null,
    friendlyName: null,
    updatedAt: combinedFlow?.updated_at || null,
    currentA: toFiniteNumber(displayItems.tesla_charge_current?.value ?? charging?.current_amps),
    configuredCurrentA: toFiniteNumber(displayItems.tesla_configured_current?.value ?? charging?.configured_current_amps),
    currentEntityId: null,
    currentUnit: "A",
    socPercent: toFiniteNumber(displayItems.tesla_soc?.value ?? battery?.level_percent),
    socEntityId: null,
    teslaConnectionState: displayItems.tesla_connection_state?.value || charging?.connection_state || null,
    teslaCableConnected: typeof charging?.cable_connected === "boolean" ? charging.cable_connected : null,
    controlAvailable: Boolean(control?.available),
    controlMode: control?.control_mode || "unavailable",
    chargingEnabled: typeof control?.charging_enabled === "boolean" ? control.charging_enabled : null,
    chargeRequestedEnabled: typeof control?.charge_requested_enabled === "boolean" ? control.charge_requested_enabled : null,
    canStart: Boolean(control?.can_start),
    canStop: Boolean(control?.can_stop),
    controlSwitchEntityId: null,
    controlStartButtonEntityId: null,
    controlStopButtonEntityId: null,
  };
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

function combinedDisplayItem(combinedFlow, key) {
  const items = combinedFlow?.display?.items || {};
  const item = items[key];
  return item && typeof item === "object" ? item : null;
}

function buildCombinedFlowMetrics(combinedFlow) {
  const displayItems = combinedFlow?.display?.items || {};
  const pvW = toFiniteNumber(displayItems.solar?.value);
  const solarW = toFiniteNumber(displayItems.solar_primary?.value);
  const solar2W = toFiniteNumber(displayItems.solar_secondary?.value);
  const gridW = toFiniteNumber(displayItems.grid?.value);
  const batteryW = toFiniteNumber(displayItems.battery_total?.value);
  const battery1W = toFiniteNumber(displayItems.battery1_power?.value);
  const battery2W = toFiniteNumber(displayItems.battery2_power?.value);
  const inverter1W = toFiniteNumber(displayItems.inverter1_power?.value);
  const inverter2W = toFiniteNumber(displayItems.inverter2_power?.value);
  const inverter1Status = displayItems.inverter1_status?.value ?? null;
  const inverter2Status = displayItems.inverter2_status?.value ?? null;
  const solarKind = displayItems.solar?.kind || "real";
  const gridKind = displayItems.grid?.kind || "real";
  const battery1Kind = displayItems.battery1_power?.kind || "real";
  const battery2Kind = displayItems.battery2_power?.kind || "real";
  const inverter1Kind = displayItems.inverter1_power?.kind || "real";
  const inverter2Kind = displayItems.inverter2_power?.kind || "real";
  const totalLoadW = toFiniteNumber(displayItems.total_load?.value);
  const homeLoadW = toFiniteNumber(displayItems.home_load?.value ?? totalLoadW);
  const balanceW = toFiniteNumber(displayItems.balance?.value);
  const balanceStatus = displayItems.balance_status?.value ?? null;
  const balanceKind = displayItems.balance?.kind || "calculated";
  const solarToInverter1W = solarW !== null ? Math.max(solarW, 0) : 0;
  const availableCount = Array.isArray(combinedFlow?.display?.order)
    ? combinedFlow.display.order.length
    : [solarW, gridW, inverter1W, inverter2W, totalLoadW].filter((v) => v !== null).length;
  return {
    pvW,
    solarW,
    solar2W,
    gridW,
    batteryW,
    battery1W,
    battery2W,
    inverter1W,
    inverter2W,
    inverter1Status,
    inverter2Status,
    totalLoadW,
    homeLoadW,
    balanceW,
    balanceStatus,
    battery1Soc: toFiniteNumber(displayItems.battery1_soc?.value),
    battery2Soc: toFiniteNumber(displayItems.battery2_soc?.value),
    solarToInverter1W,
    availableCount,
    dataKinds: {
      solar: solarKind,
      grid: gridKind,
      battery1: battery1Kind,
      battery2: battery2Kind,
      inverter1: inverter1Kind,
      inverter2: inverter2Kind,
      totalLoad: displayItems.total_load?.kind || "calculated",
      homeLoad: displayItems.home_load?.kind || "calculated",
      teslaCurrent: displayItems.tesla_charge_current?.kind || "real",
      teslaSoc: displayItems.tesla_soc?.kind || "real",
      balance: balanceKind,
      inverterRatio: "calculated",
      solarToInverter1: "estimate",
    },
    sources: {
      solar: displayItems.solar?.source || "unavailable",
      grid: displayItems.grid?.source || "unavailable",
      battery1: displayItems.battery1_power?.source || "unavailable",
      battery2: displayItems.battery2_power?.source || "unavailable",
      load: displayItems.total_load?.source || "unavailable",
      balance: displayItems.balance?.source || "unavailable",
    },
  };
}

function mobileFlowIconMarkup(kind) {
  if (kind === "solar") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<circle cx='12' cy='12' r='4'></circle>" +
      "<path d='M12 2v3'></path>" +
      "<path d='M12 19v3'></path>" +
      "<path d='M2 12h3'></path>" +
      "<path d='M19 12h3'></path>" +
      "</svg>"
    );
  }
  if (kind === "grid") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<path d='M12 3l7 4v10l-7 4-7-4V7z'></path>" +
      "<path d='M7 9h10'></path>" +
      "<path d='M7 13h10'></path>" +
      "</svg>"
    );
  }
  if (kind === "battery") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<rect x='6' y='6' width='10' height='12' rx='2' ry='2'></rect>" +
      "<rect x='16' y='10' width='2' height='4'></rect>" +
      "</svg>"
    );
  }
  if (kind === "inverter") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<rect x='5' y='6' width='14' height='12' rx='2' ry='2'></rect>" +
      "<circle cx='9' cy='12' r='1.5'></circle>" +
      "<path d='M13 10h4'></path>" +
      "<path d='M13 14h4'></path>" +
      "</svg>"
    );
  }
  if (kind === "tesla" || kind === "ev") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<path d='M7 4h10l2 4v8l-2 4H7l-2-4V8z'></path>" +
      "<path d='M10 9l4-2-2 5h3l-5 5 2-5H9z'></path>" +
      "</svg>"
    );
  }
  if (kind === "load") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<rect x='4' y='7' width='16' height='10' rx='2' ry='2'></rect>" +
      "<path d='M7 20v-3'></path>" +
      "<path d='M17 20v-3'></path>" +
      "<path d='M7 11h10'></path>" +
      "</svg>"
    );
  }
  return (
    "<svg viewBox='0 0 24 24' focusable='false'>" +
    "<path d='M3.5 11L12 3.75L20.5 11'></path>" +
    "<path d='M5.75 10.25V19C5.75 19.83 6.42 20.5 7.25 20.5H16.75C17.58 20.5 18.25 19.83 18.25 19V10.25'></path>" +
    "<path d='M8 20.5V15.5C8 14.67 8.67 14 9.5 14H14.5C15.33 14 16 14.67 16 15.5V20.5'></path>" +
    "</svg>"
  );
}

function renderMobileEnergyNode({ cls, kind, title, valueHtml, subValueHtml, active = false, dimmed = false }) {
  const stateClass = `${active ? " is-active" : ""}${dimmed ? " is-dimmed" : ""}`;
  return (
    `<article class="mobile-energy-node ${escapeHtml(cls)}${stateClass}">` +
    `<div class="mobile-energy-icon mobile-energy-icon-${escapeHtml(kind)}">${mobileFlowIconMarkup(kind)}</div>` +
    `<p class="mobile-energy-title">${escapeHtml(title)}</p>` +
    `<p class="mobile-energy-value">${valueHtml || "-"}</p>` +
    `<p class="mobile-energy-subvalue">${subValueHtml || "&nbsp;"}</p>` +
    "</article>"
  );
}

function renderMobileEnergyLine(cls, active = false, reverse = false) {
  const stateClass = `${active ? " is-active" : ""}${reverse ? " is-reverse" : ""}`;
  return `<div class="mobile-energy-line ${escapeHtml(cls)}${stateClass}" aria-hidden="true"></div>`;
}

function renderMobileCombinedFlow({
  combined,
  teslaInfo,
  homeLoadW,
  totalLoadW,
  teslaChargingW,
  teslaCurrentA,
  teslaSoc,
  battery1Soc,
  battery2Soc,
  formulaHtml,
}) {
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
    solarToInverter1W,
    dataKinds,
  } = combined;

  const gridActive = gridW !== null && Math.abs(gridW) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const gridImport = gridW !== null && gridW > 0;
  const totalLoadActive = totalLoadW !== null && totalLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const teslaChargingActive = teslaChargingW !== null && teslaChargingW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const battery1Active = battery1W !== null && Math.abs(battery1W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const battery2Active = battery2W !== null && Math.abs(battery2W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const inverter1Active = inverter1W !== null && Math.abs(inverter1W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const inverter2Active = inverter2W !== null && Math.abs(inverter2W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const solar1Active = solarToInverter1W !== null && solarToInverter1W >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const solar2Active = solar2W !== null && solar2W >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const homeLoadActive = homeLoadW !== null && homeLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const sajModeText = getSajDashboardModeText() || inverterStateText(inverter1Status);
  const solplanetModeText = inverterStateText(inverter2Status);
  const teslaConnection = formatTeslaConnectionSummary(teslaInfo);
  const teslaControl = formatTeslaControlSummary(teslaInfo);

  const html =
    `<div class="mobile-energy-card">` +
    `<div class="mobile-energy-card-head">` +
    `<p class="mobile-energy-card-title">${escapeHtml(t("mobileFlowTitle"))}</p>` +
    `<p class="mobile-energy-card-text">${escapeHtml(t("mobileFlowHint"))}</p>` +
    `</div>` +
    `<div class="mobile-energy-scene">` +
    renderMobileEnergyLine("line-solar-inverter1", solar1Active, false) +
    renderMobileEnergyLine("line-grid-home", gridActive, !gridImport) +
    renderMobileEnergyLine("line-battery1-inverter1", battery1Active, !battery1W || battery1W < 0) +
    renderMobileEnergyLine("line-inverter1-home", inverter1Active, !inverter1W || inverter1W < 0) +
    renderMobileEnergyLine("line-inverter2-home", inverter2Active, !inverter2W || inverter2W < 0) +
    renderMobileEnergyLine("line-battery2-inverter2", battery2Active, Boolean(battery2W && battery2W > 0)) +
    renderMobileEnergyLine("line-home-load", homeLoadActive, false) +
    renderMobileEnergyLine("line-home-tesla", teslaChargingActive, false) +
    renderMobileEnergyNode({
      cls: "node-solar",
      kind: "solar",
      title: "PV",
      valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(solarW), dataKinds.solar),
      subValueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(solarToInverter1W), dataKinds.solarToInverter1),
      active: solar1Active,
    }) +
    renderMobileEnergyNode({
      cls: "node-grid",
      kind: "grid",
      title: t("gridTitle"),
      valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(gridW === null ? null : Math.abs(gridW)), dataKinds.grid),
      subValueHtml: escapeHtml(gridActive ? (gridImport ? t("stateImporting") : t("stateExporting")) : t("stateIdle")),
      active: gridActive,
    }) +
    renderMobileEnergyNode({
      cls: "node-home",
      kind: "home",
      title: t("mobileFlowHomeTitle"),
      valueHtml: escapeHtml(totalLoadActive ? t("switchboardStateActive") : t("switchboardStateIdle")),
      subValueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(totalLoadW), dataKinds.totalLoad),
      active: totalLoadActive || gridActive || inverter1Active || inverter2Active,
    }) +
    renderMobileEnergyNode({
      cls: "node-inverter1",
      kind: "inverter",
      title: t("mobileFlowSajInverterShort"),
      valueHtml: formatValueWithDataKindHtml(formatSignedKwFromWatts(inverter1W), dataKinds.inverter1),
      subValueHtml: escapeHtml(sajModeText),
      active: inverter1Active,
    }) +
    renderMobileEnergyNode({
      cls: "node-battery1",
      kind: "battery",
      title: t("mobileFlowSajBatteryShort"),
      valueHtml: formatValueWithDataKindHtml(formatSignedKwFromWatts(battery1W), dataKinds.battery1),
      subValueHtml: formatValueWithDataKindHtml(battery1Soc === null ? "-" : `${Math.max(0, Math.min(100, battery1Soc)).toFixed(0)}%`, "real"),
      active: battery1Active,
    }) +
    renderMobileEnergyNode({
      cls: "node-inverter2",
      kind: "inverter",
      title: t("mobileFlowSolplanetInverterShort"),
      valueHtml: formatValueWithDataKindHtml(formatSignedKwFromWatts(inverter2W), dataKinds.inverter2),
      subValueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(solar2W), dataKinds.solar),
      active: inverter2Active || solar2Active,
    }) +
    renderMobileEnergyNode({
      cls: "node-battery2",
      kind: "battery",
      title: t("mobileFlowSolplanetBatteryShort"),
      valueHtml: formatValueWithDataKindHtml(formatSignedKwFromWatts(battery2W), dataKinds.battery2),
      subValueHtml: formatValueWithDataKindHtml(battery2Soc === null ? "-" : `${Math.max(0, Math.min(100, battery2Soc)).toFixed(0)}%`, "real"),
      active: battery2Active,
    }) +
    renderMobileEnergyNode({
      cls: "node-load",
      kind: "load",
      title: t("loadTitle"),
      valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(homeLoadW), dataKinds.homeLoad),
      subValueHtml: escapeHtml(homeLoadActive ? t("stateConsuming") : t("stateIdle")),
      active: homeLoadActive,
    }) +
    renderMobileEnergyNode({
      cls: "node-tesla",
      kind: "tesla",
      title: t("teslaChargingLabel"),
      valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(teslaChargingW), "real"),
      subValueHtml: formatValueWithDataKindHtml(teslaSoc === null ? teslaConnection : `${Math.max(0, Math.min(100, teslaSoc)).toFixed(0)}%`, teslaSoc === null ? null : dataKinds.teslaSoc),
      active: teslaChargingActive,
      dimmed: !teslaChargingActive && teslaChargingW === null,
    }) +
    `</div>` +
    `<details class="mobile-energy-detail">` +
    `<summary>${escapeHtml(t("mobileFlowAdvancedTitle"))}</summary>` +
    `<div class="mobile-energy-detail-body">` +
    `<p>${escapeHtml(t("mobileFlowSolarToInverter"))}: ${formatValueWithDataKindHtml(formatPowerKwFromWatts(solarToInverter1W), dataKinds.solarToInverter1)}</p>` +
    `<p>${escapeHtml(t("mobileFlowTeslaSoc"))}: ${formatValueWithDataKindHtml(teslaSoc === null ? "-" : `${Math.max(0, Math.min(100, teslaSoc)).toFixed(0)}%`, dataKinds.teslaSoc)}</p>` +
    `<p>${escapeHtml(t("mobileFlowControl"))}: ${escapeHtml(teslaControl)}</p>` +
    `<div class="mobile-energy-formula">${formulaHtml || "-"}</div>` +
    `</div>` +
    `</details>` +
    `</div>`;

  setHtml("energyFlowCombinedMobile", html);
}

function renderCombinedEnergyFlow(combinedFlow, teslaInfo = null) {
  const combined = buildCombinedFlowMetrics(combinedFlow);
  const {
    pvW,
    solarW,
    solar2W,
    gridW,
    batteryW,
    battery1W,
    battery2W,
    inverter1W,
    inverter2W,
    inverter1Status,
    inverter2Status,
    totalLoadW,
    homeLoadW: displayHomeLoadW,
    balanceW: displayBalanceW,
    balanceStatus: displayBalanceStatus,
    battery1Soc,
    battery2Soc,
    solarToInverter1W,
    sources,
    dataKinds,
  } = combined;
  const teslaChargingW = toFiniteNumber(teslaInfo?.chargingW);
  const teslaCurrentA = toFiniteNumber(teslaInfo?.currentA);
  const teslaConfiguredCurrentA = toFiniteNumber(teslaInfo?.configuredCurrentA);
  const teslaSoc = toFiniteNumber(teslaInfo?.socPercent);
  let homeLoadW = displayHomeLoadW;
  if (homeLoadW === null && totalLoadW !== null) {
    const teslaW = teslaChargingW === null ? 0 : teslaChargingW;
    homeLoadW = Math.max(0, totalLoadW - teslaW);
    if (Math.abs(homeLoadW) <= BALANCE_TOLERANCE_W) homeLoadW = 0;
  }

  const combinedLoadError = Boolean(combinedFlow?.__load_error);
  const combinedPending = !combinedLoadError && solarW === null && gridW === null;
  const hasCombinedBase = solarW !== null && gridW !== null;
  setSystemLoadMeta("combined", {
    phase: combinedPending ? "loading" : (hasCombinedBase ? "done" : "failed"),
    updatedAt: latestIsoTime(combinedFlow?.updated_at, teslaInfo?.updatedAt),
    quality: hasCombinedBase ? getFlowQuality(combinedFlow, combined.availableCount) : "failed",
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
    formatTeslaCurrentValueHtml(teslaCurrentA, teslaConfiguredCurrentA, teslaInfo?.currentUnit || "A", dataKinds.teslaCurrent),
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
  const sajModeTrigger = document.getElementById("combined-inverter1State");
  if (sajModeTrigger) {
    sajModeTrigger.classList.add("is-clickable");
    sajModeTrigger.setAttribute("title", t("sajProfilePanelKicker"));
  }

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
  const teslaConnected = isTeslaConnected(teslaInfo);
  const totalLoadActive = totalLoadW !== null && totalLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  setText("combined-loadState", loadActive ? t("stateConsuming") : t("stateIdle"));
  setFlowLine("combined-lineSwitchboardToTotalLoad", totalLoadActive, false);
  setFlowLine("combined-lineSwitchboardToHomeLoad", loadActive, false);
  setFlowLine("combined-lineSwitchboardToTeslaB", teslaChargingActive, false);
  setFlowEdgeMarker("combined-flowMarkerSwitchboardToTesla", teslaConnected === false);

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
    rateValueId: "combined-battery1RateValue",
    socDataKind: "real",
    energyDataKind: "estimate",
    usableDataKind: "estimate",
    runtimeDataKind: "estimate",
    rateDataKind: "estimate",
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
    rateValueId: "combined-battery2RateValue",
    socDataKind: "real",
    energyDataKind: "estimate",
    usableDataKind: "estimate",
    runtimeDataKind: "estimate",
    rateDataKind: "estimate",
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

  let formula = `${t("balanceFormulaLabel")}: -`;
  if (
    pvW !== null &&
    gridW !== null &&
    batteryW !== null &&
    totalLoadW !== null
  ) {
    const batteryDischargeW = Math.max(batteryW, 0);
    const batteryChargeW = Math.max(-batteryW, 0);
    const gridImportW = Math.max(gridW, 0);
    const gridExportW = Math.max(-gridW, 0);
    const computedBalanceW = pvW + batteryDischargeW + gridImportW - totalLoadW - batteryChargeW - gridExportW;
    const balanceW = displayBalanceW ?? computedBalanceW;
    const balanceState = displayBalanceStatus === "cleared"
      ? t("balanceStatusBalanced")
      : displayBalanceStatus === "not_cleared"
        ? t("balanceStatusUnbalanced")
        : (Math.abs(Number(balanceW) || 0) <= BALANCE_TOLERANCE_W ? t("balanceStatusBalanced") : t("balanceStatusUnbalanced"));
    formula =
      `${escapeHtml(t("balanceFormulaLabel"))}: ` +
      `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(pvW), dataKinds.solar)}) + ` +
      `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(batteryDischargeW), dataKinds.balance)}) + ` +
      `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(gridImportW), dataKinds.grid)}) - ` +
      `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(totalLoadW), dataKinds.totalLoad)}) - ` +
      `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(batteryChargeW), dataKinds.balance)}) - ` +
      `(${formatValueWithDataKindHtml(formatPowerKwFromWatts(gridExportW), dataKinds.grid)}) = ` +
      `${formatValueWithDataKindHtml(formatSignedKwFromWatts(balanceW), dataKinds.balance)} ` +
      `(${escapeHtml(balanceState)})`;
  }
  setHtml("combined-loadFormulaText", formula);
  renderMobileCombinedFlow({
    combined,
    teslaInfo,
    homeLoadW,
    totalLoadW,
    teslaChargingW,
    teslaCurrentA,
    teslaSoc,
    battery1Soc,
    battery2Soc,
    formulaHtml: formula,
  });
}

function renderSummary(payload) {
  const { combinedFlow, collectorStatus } = payload;
  const tesla = teslaInfoFromCombinedFlow(combinedFlow);
  const combinedCount = Array.isArray(combinedFlow?.display?.order)
    ? combinedFlow.display.order.length
    : 0;
  setSystemLoadMeta("combined", {
    quality: getFlowQuality(combinedFlow, Number(combinedCount) || 0),
    count: Number(combinedCount) || 0,
  });
  renderDashboardNotifications(stateCache.lastDashboardNotifications);
  renderCombinedEnergyFlow(combinedFlow, tesla);
  renderCombinedDebug(combinedFlow, collectorStatus);
  renderNotificationMatrix();
}

function dashboardNotificationLevelText(level) {
  if (level === "alarm") return t("dashboardNotificationLevelAlarm");
  if (level === "warning") return t("dashboardNotificationLevelWarning");
  return t("dashboardNotificationLevelInfo");
}

function dashboardNotificationStateText(state) {
  if (state === "dismissed") return t("dashboardNotificationStateDismissed");
  return t("dashboardNotificationStateNotified");
}

function renderDashboardNotifications(payload) {
  const section = document.getElementById("dashboardNotificationsSection");
  const list = document.getElementById("dashboardNotificationsList");
  if (!section || !list) return;
  const items = Array.isArray(payload?.items) ? payload.items : [];
  section.classList.toggle("hidden", items.length === 0);
  setText("dashboardNotificationsUpdatedAt", formatUpdatedAt(payload?.updated_at || null));
  list.innerHTML = "";
  for (const item of items) {
    const article = document.createElement("article");
    const level = String(item?.level || "info").trim().toLowerCase();
    const notificationKey = String(item?.notification_key || "");
    article.className = `dashboard-notification-item level-${escapeHtml(level)}`;
    const dismissBusy = dashboardNotificationDismissBusy.has(notificationKey);
    article.innerHTML = `
      <div class="dashboard-notification-main">
        <div class="dashboard-notification-topline">
          <span class="dashboard-notification-state">${escapeHtml(dashboardNotificationStateText(String(item?.state || "active").trim().toLowerCase()))}</span>
          <span class="dashboard-notification-level">${escapeHtml(dashboardNotificationLevelText(level))}</span>
        </div>
        <p class="dashboard-notification-message">${escapeHtml(String(item?.message || "-"))}</p>
        <div class="dashboard-notification-meta">
          ${escapeHtml(
            t("dashboardNotificationMeta", {
              window: String(item?.window || "-"),
              trigger: String(item?.trigger_text || "-"),
              time: formatDateTimeWithAgo(item?.requested_at_utc || null),
            }),
          )}
        </div>
      </div>
      <button
        class="dashboard-notification-dismiss"
        type="button"
        aria-label="${escapeHtml(t("dashboardNotificationDismiss"))}"
        title="${escapeHtml(t("dashboardNotificationDismiss"))}"
        ${dismissBusy ? "disabled" : ""}
      >&times;</button>
    `;
    const dismissButton = article.querySelector(".dashboard-notification-dismiss");
    if (dismissButton) {
      dismissButton.addEventListener("click", (event) => {
        event.stopPropagation();
        void dismissDashboardNotification(notificationKey);
      });
    }
    list.appendChild(article);
  }
}

function renderNotificationMatrix() {
  const root = document.getElementById("notificationMatrixPanels");
  if (!root) return;
  root.innerHTML = "";
  const currentWindowId = getCurrentNotificationMatrixWindowId(stateCache.lastSummary);
  if (!notificationMatrixCollapseInitialized) {
    notificationMatrixCollapseState = {};
    for (const windowItem of NOTIFICATION_MATRIX_WINDOWS) {
      notificationMatrixCollapseState[windowItem.id] = windowItem.id !== currentWindowId;
    }
    notificationMatrixCollapseInitialized = true;
  }
  const toggleNotificationMatrixWindow = (windowId) => {
    const normalizedWindowId = String(windowId || "").trim();
    if (!normalizedWindowId) return;
    notificationMatrixCollapseState[normalizedWindowId] = !Boolean(notificationMatrixCollapseState[normalizedWindowId]);
    renderNotificationMatrix();
  };
  for (const windowItem of NOTIFICATION_MATRIX_WINDOWS) {
    const panel = document.createElement("article");
    const isCurrentWindow = windowItem.id === currentWindowId;
    const isCollapsed = Boolean(notificationMatrixCollapseState[windowItem.id]);
    panel.className = `notification-window-panel${isCurrentWindow ? " is-current-window" : ""}${isCollapsed ? " is-collapsed" : ""}`;
    const notifications = Array.isArray(windowItem.notifications) ? windowItem.notifications : [];
    const listHtml = notifications.length
      ? notifications.map((item) => {
        const level = String(item.level || "info").trim().toLowerCase();
        const progress = getNotificationMatrixProgress(item, windowItem, stateCache.lastSummary);
        return `
          <article class="notification-rule-item level-${escapeHtml(level)}">
            <label class="notification-rule-check" aria-label="${escapeHtml(t(item.titleKey))}">
              <input type="checkbox" checked disabled />
            </label>
            <div class="notification-rule-main">
              <div class="notification-rule-topline">
                <span class="notification-rule-level">${escapeHtml(dashboardNotificationLevelText(level))}</span>
                <span class="notification-rule-code">${escapeHtml(String(item.code || "-"))}</span>
              </div>
              <p class="notification-rule-title">${escapeHtml(t(item.titleKey))}</p>
              <p class="notification-rule-trigger">${escapeHtml(t(item.triggerKey))}</p>
              ${renderNotificationMatrixProgress(progress)}
            </div>
          </article>
        `;
      }).join("")
      : `<div class="notification-window-empty">${escapeHtml(t("notificationMatrixNoNotifications"))}</div>`;

    panel.innerHTML = `
      <header
        class="notification-window-panel-header"
        data-window-id="${escapeHtml(windowItem.id)}"
        ${isCollapsed ? "" : `role="button" tabindex="0" aria-expanded="true" aria-label="${escapeHtml(t("notificationMatrixCollapseWindow"))}"`}
      >
        <div class="notification-window-title-row">
          <div class="notification-window-title-block">
            <h3 class="notification-window-title">${escapeHtml(t(windowItem.titleKey))}</h3>
            <span class="notification-window-title-time">${escapeHtml(String(windowItem.schedule || "-"))}</span>
          </div>
          <div class="notification-window-header-actions">
            ${isCurrentWindow ? `<span class="notification-window-current-badge">${escapeHtml(t("notificationMatrixCurrentWindowBadge"))}</span>` : ""}
            <span class="notification-window-toggle${isCollapsed ? " is-collapsed" : ""}" aria-hidden="true">
              <span class="notification-window-toggle-chevron" aria-hidden="true"></span>
            </span>
          </div>
        </div>
      </header>
      <div class="notification-window-body${isCollapsed ? " hidden" : ""}">
        <p class="notification-window-summary">${escapeHtml(t(windowItem.summaryKey))}</p>
        ${windowItem.noteKey ? `<p class="notification-window-note">${escapeHtml(t(windowItem.noteKey))}</p>` : ""}
        <div class="notification-window-list">${listHtml}</div>
      </div>
    `;
    if (isCollapsed) {
      panel.setAttribute("role", "button");
      panel.setAttribute("tabindex", "0");
      panel.setAttribute("aria-expanded", "false");
      panel.setAttribute("aria-label", t("notificationMatrixExpandWindow"));
    }
    const toggleFromTarget = (target) => {
      if (!target) return;
      toggleNotificationMatrixWindow(target.getAttribute("data-window-id"));
    };
    if (isCollapsed) {
      panel.setAttribute("data-window-id", windowItem.id);
      panel.addEventListener("click", () => {
        toggleFromTarget(panel);
      });
      panel.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        toggleFromTarget(panel);
      });
    }
    const header = panel.querySelector(".notification-window-panel-header");
    if (header && !isCollapsed) {
      header.addEventListener("click", () => {
        toggleNotificationMatrixWindow(header.getAttribute("data-window-id"));
      });
      header.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        toggleNotificationMatrixWindow(header.getAttribute("data-window-id"));
      });
    }
    root.appendChild(panel);
  }
}

function getCurrentNotificationMatrixWindowId(summary = null, now = new Date()) {
  const combined = summary?.collectorStatus?.systems?.combined || {};
  const middayWindow = String(combined?.last_midday_window_check?.window_mode || "").trim();
  if (middayWindow && middayWindow !== "off") return middayWindow;
  const sajWatchWindow = String(combined?.last_saj_battery_watch_check?.window_mode || "").trim();
  if (sajWatchWindow && sajWatchWindow !== "off") return sajWatchWindow;
  const hour = now.getHours();
  if (hour >= 11 && hour < 14) return "free_energy";
  if (hour >= 14 && hour < 16) return "after_free_shoulder";
  if (hour >= 16 && hour < 18) return "after_free_peak";
  if (hour >= 18 && hour < 20) return "export_window";
  if (hour >= 20 && hour < 23) return "post_export_peak";
  return "overnight_shoulder";
}

function resetNotificationMatrixCollapseState() {
  notificationMatrixCollapseState = {};
  notificationMatrixCollapseInitialized = false;
}

function renderNotificationMatrixProgress(progress) {
  if (!progress) return "";
  const width = Math.max(0, Math.min(100, Number(progress.ratio ?? 0) * 100));
  const markerRatio = Number(progress.markerRatio);
  const markerHtml = Number.isFinite(markerRatio) && markerRatio > 0 && markerRatio < 0.995
    ? `<span class="notification-rule-progress-marker${progress.markerLevel ? ` level-${escapeHtml(progress.markerLevel)}` : ""}" style="left: ${(Math.max(0, Math.min(1, markerRatio)) * 100).toFixed(1)}%"></span>`
    : "";
  return `
    <div class="notification-rule-progress">
      <div class="notification-rule-progress-topline">
        <span class="notification-rule-progress-state">${escapeHtml(progress.statusText || t("notificationMatrixProgressWatching"))}</span>
        <span class="notification-rule-progress-gap">${escapeHtml(progress.deltaText || "")}</span>
      </div>
      <div class="notification-rule-progress-metrics">
        <span>${escapeHtml(progress.currentText || "-")}</span>
        <span>${escapeHtml(progress.triggerText || "-")}</span>
      </div>
      <div class="notification-rule-progress-bar" aria-hidden="true">
        <span class="notification-rule-progress-fill" style="width: ${width.toFixed(1)}%"></span>
        ${markerHtml}
      </div>
    </div>
  `;
}

function getNotificationMatrixProgress(item, windowItem, summary) {
  const code = String(item?.code || "").trim();
  const combinedSystems = summary?.collectorStatus?.systems?.combined || {};
  const live = summary?.combinedFlow?.notification_metrics || {};
  const midday = combinedSystems.last_midday_window_check || {};
  const sajWatch = combinedSystems.last_saj_battery_watch_check || {};
  const batteryFull = combinedSystems.last_battery_full_notification_check || {};
  if (code === "solplanet_low_available_capacity") {
    const current = Number(live?.solplanet_battery_energy_kwh);
    return buildThresholdProgress({
      current,
      threshold: 25,
      kind: "kwh",
      direction: "down",
      currentText: formatNotificationCurrent("kwh", current),
      triggerText: t("notificationMatrixProgressTriggerAtMost", { value: formatNotificationValue("kwh", 25) }),
    });
  }
  if (code === "solplanet_low_battery" || code === "solplanet_low_battery_post_export_peak") {
    const current = Number(live?.solplanet_battery_soc_percent);
    return buildThresholdProgress({
      current,
      threshold: 20,
      kind: "percent",
      direction: "down",
      barMax: 100,
      markerRatio: 0.2,
      markerLevel: "alarm",
      currentText: formatNotificationCurrent("percent", current),
      triggerText: t("notificationMatrixProgressTriggerAtMost", { value: formatNotificationValue("percent", 20) }),
    });
  }
  if (code === "grid_import_started" || code === "grid_import_started_post_export_peak") {
    const current = Math.max(Number(live?.grid_w) || 0, 0);
    const met = current > 0;
    return {
      ratio: met ? 1 : 0,
      statusText: t(met ? "notificationMatrixProgressConditionMet" : "notificationMatrixProgressWatching"),
      currentText: formatNotificationCurrent("watts", current),
      triggerText: t("notificationMatrixProgressTriggerAbove", { value: formatNotificationValue("watts", 0) }),
      deltaText: "",
    };
  }
  if (code === "solar_surplus_export_energy_reached_5000" || code === "solar_surplus_export_energy_reached_9000") {
    const threshold = code.endsWith("_5000") ? 5 : 9;
    const tracking = midday?.solar_surplus_export_tracking || null;
    const notifiedMap = tracking?.threshold_notified_map || {};
    const notified = Boolean(notifiedMap?.[String(threshold * 1000)]);
    const current = Number(tracking?.total_export_kwh);
    if (!Number.isFinite(current)) {
      return {
        ratio: 0,
        statusText: t("notificationMatrixProgressNoData"),
        currentText: t("notificationMatrixProgressCurrent", { value: "-" }),
        triggerText: t("notificationMatrixProgressTriggerAtLeast", { value: formatNotificationValue("kwh", threshold) }),
        deltaText: t("notificationMatrixProgressWindowTracking"),
      };
    }
    return buildThresholdProgress({
      current,
      threshold,
      kind: "kwh",
      direction: "up",
      barMax: threshold,
      notified,
      currentText: formatNotificationCurrent("kwh", current),
      triggerText: t("notificationMatrixProgressTriggerAtLeast", { value: formatNotificationValue("kwh", threshold) }),
    });
  }
  if (code === "saj_battery_watch_50_percent" || code === "saj_battery_watch_20_percent") {
    const threshold = code.includes("_50_") ? 50 : 20;
    const current = Number(live?.saj_battery_soc_percent);
    const reminderState = sajWatch?.notification_state || {};
    const notified = threshold === 50
      ? Boolean(reminderState?.reminder_50_sent)
      : Boolean(reminderState?.reminder_20_sent);
    return buildThresholdProgress({
      current,
      threshold,
      kind: "percent",
      direction: "down",
      barMax: 100,
      markerRatio: threshold / 100,
      markerLevel: threshold <= 20 ? "alarm" : "warning",
      notified,
      currentText: formatNotificationCurrent("percent", current),
      triggerText: t("notificationMatrixProgressTriggerAtMost", { value: formatNotificationValue("percent", threshold) }),
    });
  }
  if (code === "saj_battery_full" || code === "solplanet_battery_full") {
    const current = code === "saj_battery_full"
      ? Number(live?.saj_battery_soc_percent)
      : Number(live?.solplanet_battery_soc_percent);
    const state = batteryFull?.notification_state || {};
    const notified = code === "saj_battery_full"
      ? Boolean(state?.saj_full)
      : Boolean(state?.solplanet_full);
    return buildThresholdProgress({
      current,
      threshold: 100,
      kind: "percent",
      direction: "up",
      barMax: 100,
      notified,
      currentText: formatNotificationCurrent("percent", current),
      triggerText: t("notificationMatrixProgressTriggerAtLeast", { value: formatNotificationValue("percent", 100) }),
    });
  }
  return {
    ratio: 0,
    statusText: t("notificationMatrixProgressNoData"),
    currentText: t("notificationMatrixProgressCurrent", { value: "-" }),
    triggerText: "-",
    deltaText: "",
  };
}

function buildThresholdProgress({
  current,
  threshold,
  kind,
  direction,
  barMax = null,
  markerRatio = null,
  markerLevel = "",
  notified = false,
  currentText,
  triggerText,
}) {
  if (!Number.isFinite(current)) {
    return {
      ratio: 0,
      statusText: t("notificationMatrixProgressNoData"),
      currentText: t("notificationMatrixProgressCurrent", { value: "-" }),
      triggerText,
      deltaText: "",
      markerRatio,
      markerLevel,
    };
  }
  const normalizedBarMax = Number.isFinite(Number(barMax)) ? Math.max(Number(barMax), 1) : Math.max(Number(threshold), 1);
  if (direction === "up") {
    const remaining = Math.max(threshold - current, 0);
    const met = current >= threshold;
    return {
      ratio: Math.max(0, Math.min(1, current / normalizedBarMax)),
      statusText: t(notified ? "notificationMatrixProgressNotified" : met ? "notificationMatrixProgressConditionMet" : "notificationMatrixProgressWatching"),
      currentText,
      triggerText,
      deltaText: met
        ? t("notificationMatrixProgressConditionMet")
        : t("notificationMatrixProgressRemaining", { value: formatNotificationDelta(current, threshold, kind) || formatNotificationValue(kind, remaining) }),
      markerRatio,
      markerLevel,
    };
  }
  const met = current <= threshold;
  const gap = Math.abs(current - threshold);
  return {
    ratio: Math.max(0, Math.min(1, current / normalizedBarMax)),
    statusText: t(notified ? "notificationMatrixProgressNotified" : met ? "notificationMatrixProgressConditionMet" : "notificationMatrixProgressWatching"),
    currentText,
    triggerText,
    deltaText: met
      ? t("notificationMatrixProgressBelowThreshold", { value: formatNotificationDelta(current, threshold, kind) })
      : t("notificationMatrixProgressAboveThreshold", { value: formatNotificationDelta(current, threshold, kind) || formatNotificationValue(kind, gap) }),
    markerRatio,
    markerLevel,
  };
}

function formatNotificationCurrent(kind, value) {
  return t("notificationMatrixProgressCurrent", { value: formatNotificationValue(kind, value) });
}

function formatNotificationValue(kind, value) {
  if (!Number.isFinite(Number(value))) return "-";
  if (kind === "percent") return `${formatTrimmedDecimal(Number(value), 1)}%`;
  if (kind === "kwh") return formatEnergyKwhText(Number(value));
  if (kind === "watts") return wattsToKwText(Number(value), 1);
  return String(value);
}

function formatNotificationDelta(current, threshold, kind) {
  if (!Number.isFinite(Number(current)) || !Number.isFinite(Number(threshold))) return "";
  const diff = Math.abs(Number(threshold) - Number(current));
  return formatNotificationValue(kind, diff);
}

async function dismissDashboardNotification(notificationKey) {
  const normalizedKey = String(notificationKey || "").trim();
  if (!normalizedKey || dashboardNotificationDismissBusy.has(normalizedKey)) return;
  dashboardNotificationDismissBusy.add(normalizedKey);
  renderDashboardNotifications(stateCache.lastDashboardNotifications);
  try {
    await fetchJson("/api/dashboard/notifications/dismiss", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notification_key: normalizedKey }),
      timeoutMs: 8000,
    });
    const current = stateCache.lastDashboardNotifications || { items: [], updated_at: null };
    stateCache.lastDashboardNotifications = {
      ...current,
      items: (Array.isArray(current.items) ? current.items : []).filter((item) => item?.notification_key !== normalizedKey),
      updated_at: new Date().toISOString(),
    };
  } catch (err) {
    window.alert(String(err));
  } finally {
    dashboardNotificationDismissBusy.delete(normalizedKey);
    renderDashboardNotifications(stateCache.lastDashboardNotifications);
  }
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
  const meta = combinedFlow?.meta || {};
  const sourceType = meta?.source_type || "-";
  const storageBacked = meta?.storage_backed ? "true" : "false";
  const stale = meta?.stale ? `true (${meta?.stale_reason || "-"})` : "false";
  const sampleAge = meta?.sample_age_seconds === null || meta?.sample_age_seconds === undefined
    ? "-"
    : formatMaybeNumber(meta.sample_age_seconds, 1);
  const kvCount = meta?.kv_item_count === null || meta?.kv_item_count === undefined
    ? "-"
    : String(meta.kv_item_count);
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
  const endUtc = dayText === getLocalDateText() ? new Date().toISOString() : toUtcIsoFromDateEndExclusive(dayText);
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

function buildDatabaseUrl() {
  const params = new URLSearchParams();
  const table = document.getElementById("databaseTableSelect")?.value || "";
  if (table) params.set("table", table);
  params.set("page", String(databasePager.page));
  params.set("page_size", String(DATABASE_PAGE_SIZE));
  return `/api/database/table?${params.toString()}`;
}

function renderDatabaseTableOptions(payload) {
  const select = document.getElementById("databaseTableSelect");
  if (!select) return;
  const currentValue = select.value;
  const tables = Array.isArray(payload?.tables) ? payload.tables : [];
  select.innerHTML =
    `<option value="">${escapeHtml(t("databaseTablePlaceholder"))}</option>` +
    tables.map((item) => {
      const tableName = String(item?.name || "").trim();
      return `<option value="${escapeHtml(tableName)}">${escapeHtml(tableName)}</option>`;
    }).join("");
  const preferredValue = currentValue || stateCache.lastDatabasePage?.table || String(tables[0]?.name || "");
  if (preferredValue && tables.some((item) => String(item?.name || "") === preferredValue)) {
    select.value = preferredValue;
  }
}

function renderDatabaseRows(payload) {
  const head = document.getElementById("databaseHead");
  const body = document.getElementById("databaseBody");
  if (!head || !body) return;
  const columns = Array.isArray(payload?.columns) ? payload.columns : [];
  head.innerHTML = `<tr>${columns.map((column) => `<th>${escapeHtml(column?.name || "-")}</th>`).join("")}</tr>`;
  body.innerHTML = "";
  const items = Array.isArray(payload?.items) ? payload.items : [];
  for (const item of items) {
    const tr = document.createElement("tr");
    tr.classList.add("database-row");
    tr.innerHTML = columns.map((column) => {
      const key = String(column?.name || "");
      const rawValue = item?.[key];
      const text =
        rawValue === null || rawValue === undefined || rawValue === ""
          ? "-"
          : typeof rawValue === "object"
            ? JSON.stringify(rawValue)
            : String(rawValue);
      return `<td class="database-cell" title="${escapeHtml(text)}"><span class="database-cell-text">${escapeHtml(text)}</span></td>`;
    }).join("");
    tr.addEventListener("click", () => {
      openDatabaseRowDetailModal(payload, item);
    });
    body.appendChild(tr);
  }
}

function renderDatabasePage(payload) {
  const totalPages = Math.max(1, Math.ceil((payload.total || 0) / (payload.page_size || DATABASE_PAGE_SIZE)));
  setText("databaseMeta", t("databaseMeta", { table: payload.table || "-", columns: payload.columns?.length || 0 }));
  setText("databaseCount", t("databaseTotal", { total: payload.total || 0 }));
  setText(
    "databasePageInfo",
    t("databasePageInfo", {
      page: payload.page || 1,
      totalPages,
      count: payload.count || 0,
    }),
  );
  setText("databaseUpdatedAt", formatUpdatedAt(payload.updated_at || null));
  document.getElementById("databasePrevPageBtn").disabled = !Boolean(payload.has_prev);
  document.getElementById("databaseNextPageBtn").disabled = !Boolean(payload.has_next);
  renderDatabaseRows(payload);
}

function renderDatabaseEmptyState() {
  const head = document.getElementById("databaseHead");
  const body = document.getElementById("databaseBody");
  if (head) head.innerHTML = "";
  if (body) body.innerHTML = "";
  setText("databaseMeta", t("databaseEmpty"));
  setText("databaseCount", t("databaseTotal", { total: 0 }));
  setText("databasePageInfo", t("pageDash"));
  setText("databaseUpdatedAt", formatUpdatedAt(null));
  const prevBtn = document.getElementById("databasePrevPageBtn");
  const nextBtn = document.getElementById("databaseNextPageBtn");
  if (prevBtn) prevBtn.disabled = true;
  if (nextBtn) nextBtn.disabled = true;
}

function openDatabaseRowDetailModal(payload, item) {
  const tableName = String(payload?.table || "-");
  const columns = Array.isArray(payload?.columns) ? payload.columns : [];
  setText("databaseRowDetailMeta", `Table ${tableName}`);
  const body = document.getElementById("databaseRowDetailBody");
  if (body) {
    body.innerHTML = columns.map((column) => {
      const key = String(column?.name || "");
      const rawValue = item?.[key];
      const text =
        rawValue === null || rawValue === undefined || rawValue === ""
          ? "-"
          : typeof rawValue === "object"
            ? JSON.stringify(rawValue, null, 2)
            : String(rawValue);
      return `
        <tr>
          <th>${escapeHtml(key || "-")}</th>
          <td><div class="worker-log-detail-value">${escapeHtml(text)}</div></td>
        </tr>
      `;
    }).join("");
  }
  setDatabaseRowDetailModalVisible(true);
}

function rerenderDatabaseViewFromCache() {
  if (stateCache.lastDatabaseTables) renderDatabaseTableOptions(stateCache.lastDatabaseTables);
  if (stateCache.lastDatabasePage) {
    renderDatabasePage(stateCache.lastDatabasePage);
    return;
  }
  renderDatabaseEmptyState();
}

function buildWorkerLogsUrl() {
  const params = new URLSearchParams();
  const category = document.getElementById("workerLogsCategorySelect")?.value || "all";
  const service = document.getElementById("workerLogsServiceSelect")?.value || "all";
  if (service && service !== "all") params.set("service", service);
  else if (category && category !== "all") params.set("category", category);
  const excludedStatuses = [...workerLogsFilterState.hiddenStatuses].filter(Boolean);
  if (excludedStatuses.length) params.set("exclude_status", excludedStatuses.join(","));
  params.set("page", String(workerLogsPager.page));
  params.set("page_size", "100");
  return `/api/worker/logs?${params.toString()}`;
}

async function ensureWorkerLogSchemaLoaded() {
  if (workerLogSchemaPromise) return workerLogSchemaPromise;
  workerLogSchemaPromise = fetchJson("/static/worker-log-schema.json", { timeoutMs: 4000 })
    .then((payload) => {
      if (payload && payload.categories && payload.services && payload.statuses) {
        workerLogSchema = payload;
      }
      return workerLogSchema;
    })
    .catch(() => workerLogSchema)
    .finally(() => {
      workerLogSchemaPromise = null;
    });
  return workerLogSchemaPromise;
}

function workerLogCategoryConfig(category) {
  const normalized = String(category || "").trim().toLowerCase();
  return Object.values(workerLogSchema?.categories || {}).find((entry) => String(entry?.value || "").trim().toLowerCase() === normalized) || null;
}

function workerLogServiceConfig(service) {
  const normalized = String(service || "").trim().toLowerCase();
  return Object.values(workerLogSchema?.services || {}).find((entry) => String(entry?.value || "").trim().toLowerCase() === normalized) || null;
}

function workerLogCategoryStatuses(category) {
  return Array.isArray(workerLogCategoryConfig(category)?.statuses) ? workerLogCategoryConfig(category).statuses : [];
}

function workerLogServiceStatuses(service) {
  return Array.isArray(workerLogServiceConfig(service)?.statuses) ? workerLogServiceConfig(service).statuses : [];
}

function workerLogCategoryServices(category) {
  return Array.isArray(workerLogCategoryConfig(category)?.services) ? workerLogCategoryConfig(category).services : [];
}

function workerLogServiceLabel(service) {
  const normalized = String(service || "").trim().toLowerCase();
  if (normalized === "notification") return t("workerLogsCategoryNotification");
  if (normalized === "operation") return t("workerLogsCategoryOperation");
  if (normalized === "tesla") return t("workerLogsServiceTeslaObserve");
  if (normalized === "combined_assembly") return t("workerLogsServiceCombinedAssembly");
  return normalized || "-";
}

function normalizeWorkerLogStatus(status) {
  return String(status || "").trim().toLowerCase();
}

function workerLogStatusText(status) {
  const normalized = normalizeWorkerLogStatus(status);
  return normalized || "-";
}

function defaultWorkerLogStatusesForFilters(category, service) {
  const selectedService = String(service || "").trim().toLowerCase();
  const selectedCategory = String(category || "all").trim().toLowerCase();
  return selectedService ? workerLogServiceStatuses(selectedService) : workerLogCategoryStatuses(selectedCategory || "all");
}

function workerLogFilterStatuses(payloadOrItems) {
  const payload = Array.isArray(payloadOrItems) ? null : payloadOrItems;
  const items = Array.isArray(payloadOrItems) ? payloadOrItems : payload?.items;
  const category = document.getElementById("workerLogsCategorySelect")?.value || "all";
  const service = document.getElementById("workerLogsServiceSelect")?.value || "all";
  const schemaStatuses = defaultWorkerLogStatusesForFilters(category, service);
  const countedStatuses = Array.isArray(payload?.status_counts)
    ? [...new Set(payload.status_counts.map((item) => normalizeWorkerLogStatus(item?.status)).filter(Boolean))]
    : [];
  const seenStatuses = countedStatuses.length
    ? countedStatuses
    : [...new Set((items || []).map((item) => normalizeWorkerLogStatus(item?.status)).filter(Boolean))];
  if (!schemaStatuses.length) return seenStatuses;
  return [...schemaStatuses, ...seenStatuses.filter((status) => !schemaStatuses.includes(status))];
}

function renderWorkerLogsTypeFilters(payloadOrItems, { forceHide = false } = {}) {
  const container = document.getElementById("workerLogsTypeFilters");
  if (!container) return;
  if (forceHide) {
    workerLogsFilterState.hiddenStatuses.clear();
    container.classList.add("hidden");
    container.innerHTML = "";
    return;
  }
  const payload = Array.isArray(payloadOrItems) ? null : payloadOrItems;
  const items = Array.isArray(payloadOrItems) ? payloadOrItems : payload?.items;
  const statuses = workerLogFilterStatuses(payloadOrItems);
  if (!statuses.length) {
    workerLogsFilterState.hiddenStatuses.clear();
    container.classList.add("hidden");
    container.innerHTML = "";
    return;
  }
  const activeStatusSet = new Set(statuses);
  for (const status of [...workerLogsFilterState.hiddenStatuses]) {
    if (!activeStatusSet.has(status)) workerLogsFilterState.hiddenStatuses.delete(status);
  }
  const counts = new Map();
  if (Array.isArray(payload?.status_counts) && payload.status_counts.length) {
    for (const item of payload.status_counts) {
      const status = normalizeWorkerLogStatus(item?.status);
      if (!status) continue;
      counts.set(status, Number(item?.count) || 0);
    }
  } else {
    for (const item of items || []) {
      const status = normalizeWorkerLogStatus(item?.status);
      if (!status) continue;
      counts.set(status, (counts.get(status) || 0) + 1);
    }
  }
  const allChecked = statuses.every((status) => !workerLogsFilterState.hiddenStatuses.has(status));
  const allCount = [...counts.values()].reduce((sum, count) => sum + count, 0);
  container.classList.remove("hidden");
  container.innerHTML = `
    <div class="worker-logs-type-filters-head">
      <span class="worker-logs-type-filters-title">${escapeHtml(t("workerLogsTypeFiltersTitle"))}</span>
      <span class="worker-logs-type-filters-hint">${escapeHtml(t("workerLogsTypeFiltersHint"))}</span>
    </div>
    <div class="worker-logs-type-filters-list">
      <label class="worker-logs-type-chip worker-logs-type-chip-all${allChecked ? "" : " is-hidden"}">
        <input type="checkbox" data-worker-log-status-all="true" ${allChecked ? "checked" : ""} />
        <span class="worker-logs-type-chip-label">${escapeHtml(t("workerLogsTypeFiltersAll"))}</span>
        <span class="worker-logs-type-chip-count">${allCount}</span>
      </label>
      ${statuses
        .map((status) => {
          const checked = !workerLogsFilterState.hiddenStatuses.has(status);
          const count = counts.get(status) || 0;
          return `
            <label class="worker-logs-type-chip${checked ? "" : " is-hidden"}">
              <input type="checkbox" data-worker-log-status="${escapeHtml(status)}" ${checked ? "checked" : ""} />
              <span class="worker-logs-type-chip-label">${escapeHtml(workerLogStatusText(status))}</span>
              <span class="worker-logs-type-chip-count">${count}</span>
            </label>
          `;
        })
        .join("")}
    </div>
  `;
  const allInput = container.querySelector("input[data-worker-log-status-all]");
  if (allInput) {
    allInput.addEventListener("change", async (event) => {
      const target = event.target;
      if (target.checked) workerLogsFilterState.hiddenStatuses.clear();
      else {
        workerLogsFilterState.hiddenStatuses = new Set(statuses);
      }
      workerLogsPager.page = 1;
      await loadWorkerLogs();
    });
  }
  for (const input of container.querySelectorAll("input[data-worker-log-status]")) {
    input.addEventListener("change", async (event) => {
      const target = event.target;
      const status = normalizeWorkerLogStatus(target?.dataset?.workerLogStatus);
      if (!status) return;
      if (target.checked) workerLogsFilterState.hiddenStatuses.delete(status);
      else workerLogsFilterState.hiddenStatuses.add(status);
      workerLogsPager.page = 1;
      await loadWorkerLogs();
    });
  }
}

function syncWorkerLogsFilters() {
  const categorySelect = document.getElementById("workerLogsCategorySelect");
  const serviceSelect = document.getElementById("workerLogsServiceSelect");
  const category = categorySelect?.value || "all";
  if (serviceSelect) {
    const selectedService = serviceSelect.value || "all";
    const allowedServices = workerLogCategoryServices(category);
    serviceSelect.innerHTML = [
      `<option value="all">${escapeHtml(t("workerLogsServiceAll"))}</option>`,
      ...allowedServices.map((serviceValue) => `<option value="${escapeHtml(serviceValue)}">${escapeHtml(workerLogServiceLabel(serviceValue))}</option>`),
    ].join("");
    serviceSelect.value = allowedServices.includes(selectedService) ? selectedService : "all";
    serviceSelect.disabled = allowedServices.length === 0;
  }
}

function workerLogMatchesAlertFilter(item, filterValue) {
  if (!item || filterValue !== "warning") return true;
  const resultText = String(item.error_text || item.result_text || "").toLowerCase();
  return (
    resultText.includes("notification ") ||
    resultText.includes("warning") ||
    resultText.includes("notification") ||
    resultText.includes("alarm") ||
    resultText.includes("solplanet_low_battery") ||
    resultText.includes("grid_import_started") ||
    resultText.includes("solar_surplus_export_energy_reached")
  );
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
  if (service === "tesla" || system === "tesla") return "worker-service-tesla";
  if (system === "notification" || service === "notification") return "worker-service-tesla";
  if (system === "operation" || service === "operation") return "worker-service-combined";
  if (system === "combined" || service === "combined_assembly") return "worker-service-combined";
  if (system === "saj") return "worker-service-saj";
  if (system === "solplanet") return "worker-service-solplanet";
  return "";
}

function workerLogStatusPresentation(item) {
  const rawStatus = String(item?.status || "").trim().toLowerCase();
  if (rawStatus === "pending") return { text: rawStatus, className: "worker-status-pending" };
  if (rawStatus === "skipped" || rawStatus === "outside_window" || rawStatus === "noop" || rawStatus === "no_notification" || rawStatus === "nop") {
    return { text: rawStatus, className: "worker-status-skipped" };
  }
  if (rawStatus === "applied" || rawStatus === "send" || rawStatus === "ok" || rawStatus === "notified" || rawStatus === "operation") {
    return { text: rawStatus, className: "worker-status-ok" };
  }
  if (rawStatus === "timeout" || rawStatus === "failed" || rawStatus === "alarmed" || rawStatus === "notification") {
    return { text: rawStatus, className: "worker-status-failed" };
  }
  return item?.ok
    ? { text: rawStatus || "ok", className: "worker-status-ok" }
    : { text: rawStatus || "failed", className: "worker-status-failed" };
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
  const payloadPre = document.getElementById("workerLogDetailPayload");
  if (payloadPre) {
    const payload = item?.payload_json ?? null;
    payloadPre.textContent = payload == null ? "-" : JSON.stringify(payload, null, 2);
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
  const columns = workerLogColumnsForMode(getWorkerLogsTableMode());
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
    const statusPresentation = workerLogStatusPresentation(item);
    tr.innerHTML = columns.map((column) => {
      const value = workerLogCellValue(item, column, statusPresentation);
      const className =
        column === "service" ? serviceClass :
        column === "status" ? statusPresentation.className :
        (column === "round_id" || column === "request_token" || column === "api_link") ? "worker-link" : "";
      const isLong = column === "error_text" || column === "result_text" || column === "payload_json";
      if (isLong) {
        return `<td class="${escapeHtml(className)}"><pre class="worker-result-pre" title="${escapeHtml(value)}">${escapeHtml(value)}</pre></td>`;
      }
      const title = (column === "round_id" || column === "request_token" || column === "api_link") ? ` title="${escapeHtml(value)}"` : "";
      return `<td class="${escapeHtml(className)}"${title}>${escapeHtml(value)}</td>`;
    }).join("");
    tr.addEventListener("click", () => {
      openWorkerLogDetailModal(item);
    });
    body.appendChild(tr);
  }
}

function renderWorkerLogsPage(payload) {
  const sourceItems = Array.isArray(payload.items) ? payload.items : [];
  renderWorkerLogsTypeFilters(payload);
  const filteredItems = sourceItems.filter((item) => !workerLogsFilterState.hiddenStatuses.has(normalizeWorkerLogStatus(item?.status)));
  const totalPages = Math.max(1, Math.ceil((payload.total || 0) / (payload.page_size || 100)));
  setText("workerLogsCount", t("workerLogsTotal", { total: payload.total || 0 }));
  setText(
    "workerLogsPageInfo",
    t("workerLogsPageInfo", {
      page: payload.page || 1,
      totalPages,
      count: filteredItems.length,
    }),
  );
  setText("workerLogsUpdatedAt", formatUpdatedAt(payload.updated_at || null));
  document.getElementById("workerLogsPrevPageBtn").disabled = !Boolean(payload.has_prev);
  document.getElementById("workerLogsNextPageBtn").disabled = !Boolean(payload.has_next);
  renderWorkerLogsRows(filteredItems);
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

function buildSamplingTotalsSummary(usageBySystem, selectedSystem) {
  const sajUsage = usageBySystem?.saj || null;
  const solplanetUsage = usageBySystem?.solplanet || null;
  const sajEnergy = sajUsage?.energy_kwh || {};
  const solplanetEnergy = solplanetUsage?.energy_kwh || {};
  const sajHasData = Number(sajUsage?.samples || 0) >= 2;
  const solplanetHasData = Number(solplanetUsage?.samples || 0) >= 2;

  if (selectedSystem === "overall") {
    const cards = [
      {
        title: t("samplingOverallMetricPv"),
        value: (sajHasData ? Number(sajEnergy.solar_generation || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.solar_generation || 0) : 0),
        kind: "pv",
        scope: t("samplingTotalsScopeOverall"),
        layout: "single",
      },
      {
        title: t("samplingTotalsGridTitle"),
        leftLabel: t("samplingOverallMetricGridImport"),
        leftValue: (sajHasData ? Number(sajEnergy.grid_import || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.grid_import || 0) : 0),
        leftKind: "import",
        rightLabel: t("samplingOverallMetricGridExport"),
        rightValue: (sajHasData ? Number(sajEnergy.grid_export || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.grid_export || 0) : 0),
        rightKind: "export",
        scope: t("samplingTotalsScopeOverall"),
        layout: "dual",
      },
      {
        title: t("samplingTotalsScopeSajBattery"),
        leftLabel: t("samplingOverallMetricBatteryCharge"),
        leftValue: sajHasData ? Number(sajEnergy.battery_charge || 0) : 0,
        leftKind: "charge",
        rightLabel: t("samplingOverallMetricBatteryDischarge"),
        rightValue: sajHasData ? Number(sajEnergy.battery_discharge || 0) : 0,
        rightKind: "discharge",
        scope: t("samplingTotalsScopeOverall"),
        layout: "dual",
      },
      {
        title: t("samplingTotalsScopeSolplanetBattery"),
        leftLabel: t("samplingOverallMetricBatteryCharge"),
        leftValue: solplanetHasData ? Number(solplanetEnergy.battery_charge || 0) : 0,
        leftKind: "charge",
        rightLabel: t("samplingOverallMetricBatteryDischarge"),
        rightValue: solplanetHasData ? Number(solplanetEnergy.battery_discharge || 0) : 0,
        rightKind: "discharge",
        scope: t("samplingTotalsScopeOverall"),
        layout: "dual",
      },
    ];
    return { cards, split: [] };
  }

  const selectedUsage = selectedSystem === "solplanet" ? solplanetUsage : sajUsage;
  const selectedEnergy = selectedUsage?.energy_kwh || {};
  const selectedHasData = Number(selectedUsage?.samples || 0) >= 2;
  const selectedLabel = formatSamplingSystemLabel(selectedSystem);
  return {
    cards: [
      {
        title: t("samplingOverallMetricPv"),
        value: selectedHasData ? Number(selectedEnergy.solar_generation || 0) : 0,
        kind: "pv",
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "single",
      },
      {
        title: t("samplingTotalsGridTitle"),
        leftLabel: t("samplingOverallMetricGridImport"),
        leftValue: selectedHasData ? Number(selectedEnergy.grid_import || 0) : 0,
        leftKind: "import",
        rightLabel: t("samplingOverallMetricGridExport"),
        rightValue: selectedHasData ? Number(selectedEnergy.grid_export || 0) : 0,
        rightKind: "export",
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "dual",
      },
      {
        title: t("samplingOverallMetricBatteryCharge"),
        value: selectedHasData ? Number(selectedEnergy.battery_charge || 0) : 0,
        kind: "charge",
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "single",
      },
      {
        title: t("samplingOverallMetricBatteryDischarge"),
        value: selectedHasData ? Number(selectedEnergy.battery_discharge || 0) : 0,
        kind: "discharge",
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "single",
      },
    ],
    split: [],
  };
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

function ensureSamplingTotalsChart() {
  if (samplingTotalsChart) return samplingTotalsChart;
  const canvas = document.getElementById("samplingTotalsChartCanvas");
  if (!canvas) return null;
  if (typeof window.echarts === "undefined") return null;
  samplingTotalsChart = window.echarts.init(canvas, null, { renderer: "canvas" });
  return samplingTotalsChart;
}

function buildSamplingTotalsChartRows(cards) {
  const rows = [];
  for (const card of Array.isArray(cards) ? cards : []) {
    if (card.layout === "dual") {
      const suffix =
        card.title === t("samplingTotalsGridTitle")
          ? ""
          : card.title === t("samplingTotalsScopeSajBattery")
            ? " (SAJ)"
            : card.title === t("samplingTotalsScopeSolplanetBattery")
              ? " (Solplanet)"
              : "";
      rows.push({
        label: `${card.leftLabel || ""}${suffix}`.trim(),
        value: Math.max(Number(card.leftValue || 0), 0),
        kind: card.leftKind,
      });
      rows.push({
        label: `${card.rightLabel || ""}${suffix}`.trim(),
        value: Math.max(Number(card.rightValue || 0), 0),
        kind: card.rightKind,
      });
    } else {
      rows.push({
        label: card.title,
        value: Math.max(Number(card.value || 0), 0),
        kind: card.kind,
      });
    }
  }
  return rows;
}

function renderSamplingTotalsChart(cards) {
  const chart = ensureSamplingTotalsChart();
  if (!chart) return;
  const rows = buildSamplingTotalsChartRows(cards);
  const hasData = rows.some((row) => Number(row.value || 0) !== 0);
  const labels = rows.map((row) => row.label);
  const values = rows.map((row) => row.value);
  const maxAbs = Math.max(0, ...rows.map((row) => Math.abs(Number(row.value || 0))));
  chart.setOption(
    {
      animation: false,
      grid: { left: 220, right: 72, top: 6, bottom: 6, containLabel: false },
      xAxis: {
        type: "value",
        min: 0,
        max: maxAbs > 0 ? maxAbs : 1,
        splitNumber: 2,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
      },
      yAxis: {
        type: "category",
        inverse: true,
        data: labels,
        axisTick: { show: false },
        axisLine: { show: false },
        axisLabel: { color: "#395346", fontSize: 12, fontWeight: 700 },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const list = Array.isArray(params) ? params : [];
          if (!list.length) return "";
          const idx = Number(list[0]?.dataIndex || 0);
          const row = rows[idx];
          if (!row) return "";
          return `${escapeHtml(row.label)}<br/>${formatEnergyKwhText(row.value)}`;
        },
      },
      visualMap: [],
      graphic: hasData
        ? []
        : [
            {
              type: "text",
              left: "center",
              top: "middle",
              style: { text: t("samplingTotalsNoData"), fill: "#6b7f72", fontSize: 13 },
            },
          ],
      series: [
        {
          type: "bar",
          barWidth: 12,
          data: values,
          itemStyle: {
            borderRadius: [999, 999, 999, 999],
            color: (params) => {
              const row = rows[params.dataIndex];
              if (row?.kind === "import") return "#4f8df7";
              if (row?.kind === "export") return "#8b5cf6";
              if (row?.kind === "pv") return "#f59e0b";
              if (row?.kind === "charge") return "#d97706";
              return "#22c55e";
            },
          },
          label: {
            show: true,
            position: "right",
            distance: 8,
            color: "#5b6d63",
            fontSize: 11,
            formatter: ({ value }) => (Number(value) ? `${formatTrimmedDecimal(Math.abs(Number(value)), 2)} kWh` : ""),
          },
          markLine: {
            silent: true,
            symbol: "none",
            label: { show: false },
            lineStyle: { color: "rgba(82, 104, 92, 0.18)", width: 2 },
            data: [{ xAxis: 0 }],
          },
          emphasis: { disabled: true },
        },
      ],
    },
    true,
  );
}

function renderSamplingTotals(usageBySystem, selectedSystem, rangeLabel, options = {}) {
  const body = document.getElementById("samplingTotalsBody");
  if (!body) return;
  const systemText = formatSamplingSystemLabel(selectedSystem);
  const summary = buildSamplingTotalsSummary(usageBySystem, selectedSystem);
  const cards = Array.isArray(summary?.cards) ? summary.cards : [];
  const split = Array.isArray(summary?.split) ? summary.split : [];
  const hasData = cards.some((card) =>
    card.layout === "dual" ? Number(card.leftValue || 0) > 0 || Number(card.rightValue || 0) > 0 : Number(card.value || 0) > 0,
  );
  const maxValue = Math.max(
    0,
    ...cards.map((card) =>
      card.layout === "dual" ? Math.max(Number(card.leftValue || 0), Number(card.rightValue || 0)) : Number(card.value || 0),
    ),
  );

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

  if (!cards.length) {
    renderSamplingTotalsChart([]);
    body.innerHTML = `<div class="sampling-total-empty">${escapeHtml(t("samplingTotalsNoData"))}</div>`;
    return;
  }
  if (!hasData) {
    renderSamplingTotalsChart([]);
    body.innerHTML = `<div class="sampling-total-empty">${escapeHtml(t("samplingTotalsNoData"))}</div>`;
    return;
  }
  renderSamplingTotalsChart(cards);

  const widthPct = (value) => {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0 || maxValue <= 0) return 0;
    return Math.max(0, Math.min(100, (n / maxValue) * 100));
  };

  body.innerHTML = `
    <div class="sampling-total-grid">
      ${cards
        .map((card) => {
          if (card.layout === "dual") {
            const leftValueText = card.leftValue > 0 ? formatEnergyKwhText(card.leftValue) : "-";
            const rightValueText = card.rightValue > 0 ? formatEnergyKwhText(card.rightValue) : "-";
            return `
              <article class="sampling-total-card sampling-total-card-dual">
                <div class="sampling-total-card-head">
                  <span class="sampling-total-card-title">${escapeHtml(card.title)}</span>
                  <span class="sampling-total-card-scope">${escapeHtml(card.scope)}</span>
                </div>
                <div class="sampling-total-dual-values">
                  <div class="sampling-total-dual-value is-left">
                    <span class="sampling-total-dual-label">${escapeHtml(card.leftLabel || "")}</span>
                    <span class="sampling-total-dual-number">${escapeHtml(leftValueText)}</span>
                  </div>
                  <div class="sampling-total-dual-divider"></div>
                  <div class="sampling-total-dual-value is-right">
                    <span class="sampling-total-dual-label">${escapeHtml(card.rightLabel || "")}</span>
                    <span class="sampling-total-dual-number">${escapeHtml(rightValueText)}</span>
                  </div>
                </div>
                <div class="sampling-total-balance-track">
                  <div class="sampling-total-balance-center"></div>
                  <div class="sampling-total-balance-fill is-left is-${escapeHtml(card.leftKind)}" style="width:${widthPct(card.leftValue)}%;"></div>
                  <div class="sampling-total-balance-fill is-right is-${escapeHtml(card.rightKind)}" style="width:${widthPct(card.rightValue)}%;"></div>
                </div>
              </article>
            `;
          }
          const valueText = card.value > 0 ? formatEnergyKwhText(card.value) : "-";
          return `
            <article class="sampling-total-card is-${escapeHtml(card.kind)}">
              <div class="sampling-total-card-head">
                <span class="sampling-total-card-title">${escapeHtml(card.title)}</span>
                <span class="sampling-total-card-scope">${escapeHtml(card.scope)}</span>
              </div>
              <div class="sampling-total-card-value">${escapeHtml(valueText)}</div>
              <div class="sampling-total-card-track">
                <div class="sampling-total-fill is-${escapeHtml(card.kind)}" style="width:${widthPct(card.value)}%;"></div>
              </div>
            </article>
          `;
        })
        .join("")}
    </div>
    ${
      split.length
        ? `
          <div class="sampling-total-split">
            ${split
              .map((item) => `
                <div class="sampling-total-split-row">
                  <span class="sampling-total-split-scope">${escapeHtml(item.scope)}</span>
                  <div class="sampling-total-split-values">
                    <span class="sampling-total-split-pill is-charge">${escapeHtml(t("samplingOverallMetricBatteryCharge"))}: ${escapeHtml(
                      item.charge > 0 ? formatEnergyKwhText(item.charge) : "-",
                    )}</span>
                    <span class="sampling-total-split-pill is-discharge">${escapeHtml(t("samplingOverallMetricBatteryDischarge"))}: ${escapeHtml(
                      item.discharge > 0 ? formatEnergyKwhText(item.discharge) : "-",
                    )}</span>
                  </div>
                </div>
              `)
              .join("")}
          </div>
        `
        : ""
    }
  `;
}

function getRawCardMode(key) {
  const mode = stateCache.rawCardMode[key];
  return mode === "json" ? "json" : "explain";
}

function getRawCardCollapsed(key) {
  return Boolean(stateCache.rawCardCollapse?.[key]);
}

function setRawCardCollapsed(key, collapsed) {
  if (!stateCache.rawCardCollapse || typeof stateCache.rawCardCollapse !== "object") {
    stateCache.rawCardCollapse = {};
  }
  stateCache.rawCardCollapse[key] = Boolean(collapsed);
  try {
    localStorage.setItem(RAW_CARD_COLLAPSE_KEY, JSON.stringify(stateCache.rawCardCollapse));
  } catch {
    // Ignore storage failures so the UI still works.
  }
  syncRawCardCollapsedUi(key);
}

function toggleRawCardCollapsed(key) {
  setRawCardCollapsed(key, !getRawCardCollapsed(key));
}

function syncRawCardCollapsedUi(key) {
  const header = document.getElementById(`raw-header-${key}`);
  const body = document.getElementById(`raw-body-${key}`);
  const toggle = document.getElementById(`raw-toggle-${key}`);
  const collapsed = getRawCardCollapsed(key);
  if (header) {
    header.setAttribute("aria-expanded", collapsed ? "false" : "true");
    header.setAttribute("aria-label", t(collapsed ? "rawExpandRequest" : "rawCollapseRequest"));
  }
  if (body) body.classList.toggle("hidden", collapsed);
  if (toggle) toggle.classList.toggle("is-collapsed", collapsed);
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
      <header
        id="raw-header-${key}"
        class="raw-card-header"
        role="button"
        tabindex="0"
      >
        <div class="raw-card-title-row">
          <h3 id="raw-title-${key}"></h3>
          <span id="raw-toggle-${key}" class="raw-card-toggle" aria-hidden="true">
            <span class="raw-card-toggle-chevron" aria-hidden="true"></span>
          </span>
        </div>
        <p id="raw-summary-${key}" class="raw-card-summary">-</p>
      </header>
      <div id="raw-body-${key}" class="raw-card-body">
        <div id="raw-progress-${key}" class="raw-progress"><div class="raw-progress-fill"></div></div>
        <p id="raw-meta-${key}" class="raw-meta">-</p>
        <div class="raw-switch">
          <button id="raw-tab-explain-${key}" type="button" class="raw-tab-btn active"></button>
          <button id="raw-tab-json-${key}" type="button" class="raw-tab-btn"></button>
        </div>
        <div id="raw-explain-${key}" class="raw-explain"></div>
        <pre id="raw-pre-${key}" class="raw-pre">-</pre>
      </div>
    `;
    body.appendChild(card);

    const header = document.getElementById(`raw-header-${key}`);
    const explainBtn = document.getElementById(`raw-tab-explain-${key}`);
    const jsonBtn = document.getElementById(`raw-tab-json-${key}`);
    if (header) {
      header.addEventListener("click", () => toggleRawCardCollapsed(key));
      header.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        toggleRawCardCollapsed(key);
      });
    }
    if (explainBtn) explainBtn.addEventListener("click", () => setRawCardMode(key, "explain"));
    if (jsonBtn) jsonBtn.addEventListener("click", () => setRawCardMode(key, "json"));
  }
  setText(`raw-title-${key}`, t(titleKey));
  setText(`raw-tab-explain-${key}`, t("rawViewExplain"));
  setText(`raw-tab-json-${key}`, t("rawViewJson"));
  setRawCardMode(key, getRawCardMode(key));
  syncRawCardCollapsedUi(key);
}

function renderRawCard(api, state, bodyId) {
  ensureRawCard(api.key, api.titleKey, bodyId);
  const progress = document.getElementById(`raw-progress-${api.key}`);
  const meta = document.getElementById(`raw-meta-${api.key}`);
  const pre = document.getElementById(`raw-pre-${api.key}`);
  const summary = document.getElementById(`raw-summary-${api.key}`);
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
    if (summary) {
      summary.innerHTML =
        `<span class="raw-card-summary-path">${escapeHtml(state.path || api.url || "-")}</span>` +
        `<span class="${statusBadgeClass}">${escapeHtml(statusBadgeText)}</span>`;
    }
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
  syncRawCardCollapsedUi(api.key);
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

function syncRawDataSystemUi() {
  const select = document.getElementById("rawDataSystemSelect");
  const solplanetPanel = document.getElementById("solplanetRawPanel");
  const sajPanel = document.getElementById("sajRawPanel");
  if (select) select.value = rawDataSystem;
  if (solplanetPanel) solplanetPanel.classList.toggle("hidden", rawDataSystem !== "solplanet");
  if (sajPanel) sajPanel.classList.toggle("hidden", rawDataSystem !== "saj");
}

function setRawDataSystem(system, { load = true } = {}) {
  rawDataSystem = system === "saj" ? "saj" : "solplanet";
  localStorage.setItem(RAW_DATA_SYSTEM_KEY, rawDataSystem);
  syncRawDataSystemUi();
  if (currentTab !== "rawData") return;
  if (rawDataSystem === "solplanet") renderSolplanetRawFromCache();
  else renderSajRawFromCache();
  if (load) void loadCurrentTab();
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
  if (load && currentTab === "rawData" && rawDataSystem === "solplanet") void loadCurrentTab();
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

function renderSajRawFromCache() {
  for (const api of SAJ_RAW_APIS) {
    const state = stateCache.lastSajRaw[api.key] || {
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
    renderRawCard(api, state, "sajRawBody");
  }
  renderRawSummary(stateCache.lastSajRaw, "sajRawMeta", "sajRawUpdatedAt");
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

function showSajActionMessage(titleText, summaryText, apiCalls = []) {
  setText("sajActionModalTitle", titleText || t("sajControlPopupSuccessTitle"));
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

function showSajActionSuccess(summaryText, apiCalls = []) {
  showSajActionMessage(t("sajControlPopupSuccessTitle"), summaryText, apiCalls);
}

function showSajActionError(summaryText, apiCalls = []) {
  showSajActionMessage(t("sajControlPopupErrorTitle"), summaryText, apiCalls);
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
  renderSajProfilePanel();

  const baseResults = await Promise.allSettled([
    fetchJson("/api/collector/status", { timeoutMs: 6000 }),
    fetchJson("/api/saj/control/profile", { timeoutMs: 8000 }),
    fetchJson("/api/dashboard/notifications", { timeoutMs: 8000 }),
  ]);
  if (requestId !== summaryRequestId) return;

  if (baseResults[0].status === "fulfilled") {
    summary.collectorStatus = baseResults[0].value;
    stateCache.lastCollectorStatus = baseResults[0].value;
  }
  if (baseResults[1].status === "fulfilled") {
    stateCache.lastSajProfile = baseResults[1].value;
    renderSajProfilePanel();
  } else {
    setText("sajProfileStatusText", t("sajProfileLoadFailed", { error: String(baseResults[1].reason) }));
  }
  if (baseResults[2].status === "fulfilled") {
    stateCache.lastDashboardNotifications = baseResults[2].value;
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

async function loadDatabase() {
  try {
    const tablesPayload = await fetchJson("/api/database/tables", { timeoutMs: 6000 });
    stateCache.lastDatabaseTables = tablesPayload;
    renderDatabaseTableOptions(tablesPayload);

    const selectedTable = document.getElementById("databaseTableSelect")?.value || "";
    if (!selectedTable) {
      stateCache.lastDatabasePage = null;
      renderDatabaseEmptyState();
      return;
    }

    const payload = await fetchJson(buildDatabaseUrl(), { timeoutMs: 10000 });
    databasePager.hasNext = Boolean(payload.has_next);
    databasePager.hasPrev = Boolean(payload.has_prev);
    stateCache.lastDatabasePage = payload;
    renderDatabasePage(payload);
  } catch (err) {
    stateCache.lastDatabasePage = null;
    databasePager.hasNext = false;
    databasePager.hasPrev = false;
    setText("databaseMeta", t("loadFailed", { error: String(err) }));
    setText("databaseCount", t("databaseTotal", { total: 0 }));
    setText("databasePageInfo", t("pageDash"));
    setText("databaseUpdatedAt", formatUpdatedAt(null));
    const head = document.getElementById("databaseHead");
    const body = document.getElementById("databaseBody");
    const prevBtn = document.getElementById("databasePrevPageBtn");
    const nextBtn = document.getElementById("databaseNextPageBtn");
    if (head) head.innerHTML = "";
    if (body) body.innerHTML = "";
    if (prevBtn) prevBtn.disabled = true;
    if (nextBtn) nextBtn.disabled = true;
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
  await ensureWorkerLogSchemaLoaded();
  if (!workerLogsDefaultsApplied) {
    const categorySelect = document.getElementById("workerLogsCategorySelect");
    const serviceSelect = document.getElementById("workerLogsServiceSelect");
    const tableModeSelect = document.getElementById("workerLogsTableModeSelect");
    if (categorySelect && !categorySelect.value) categorySelect.value = "all";
    if (serviceSelect && !serviceSelect.value) serviceSelect.value = "all";
    if (tableModeSelect) {
      const savedMode = localStorage.getItem(WORKER_LOGS_TABLE_MODE_KEY);
      tableModeSelect.value = savedMode === "raw_table" ? "raw_table" : "human_readable_table";
    }
    syncWorkerLogsFilters();
    renderWorkerLogsHead();
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
    stateCache.lastWorkerLogsPage = null;
    setText("workerLogsCount", t("loadFailed", { error: String(err) }));
    setText("workerLogsPageInfo", t("pageDash"));
    setText("workerLogsUpdatedAt", formatUpdatedAt(null));
    setText("workerLogsConfigMeta", t("workerLogsConfigMeta", { host: "-" }));
    renderWorkerLogsTypeFilters([], { forceHide: true });
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
  if (tab === "notificationMatrix") return Boolean(stateCache.lastSummary);
  if (tab === "rawData") {
    if (rawDataSystem === "saj") return SAJ_RAW_APIS.some((api) => stateCache.lastSajRaw?.[api.key]?.payload !== undefined);
    if (solplanetRawMode === "table") return stateCache.lastSolplanetKv?.phase && stateCache.lastSolplanetKv.phase !== "idle";
    return SOLPLANET_RAW_APIS.some((api) => stateCache.lastSolplanetRaw?.[api.key]?.payload !== undefined);
  }
  if (tab === "solplanetControl") return Boolean(stateCache.lastSolplanetControl);
  if (tab === "sampling") return Boolean(stateCache.lastSamplingPage || stateCache.lastSamplingStatus || stateCache.lastSamplingSeries);
  if (tab === "database") return Boolean(stateCache.lastDatabaseTables || stateCache.lastDatabasePage);
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
    if (tabKey === "notificationMatrix") {
      if (!stateCache.lastSummary || fromAutoRefresh) {
        await loadSummary();
      } else {
        renderNotificationMatrix();
      }
      return true;
    }
    if (tabKey === "rawData") {
      syncRawDataSystemUi();
      if (rawDataSystem === "saj") await loadSajRaw();
      else await loadSolplanetRaw();
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
    if (tabKey === "database") {
      await loadDatabase();
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
  const previousTab = currentTab;
  currentTab =
    tab === "notificationMatrix" ||
    tab === "rawData" ||
    tab === "solplanetControl" ||
    tab === "sampling" ||
    tab === "database" ||
    tab === "workerLogs" ||
    tab === "workerFailureLog"
      ? tab
      : "dashboard";
  if (currentTab === "notificationMatrix" && previousTab !== "notificationMatrix") {
    resetNotificationMatrixCollapseState();
  }
  localStorage.setItem("activeTab", currentTab);

  const dashboardView = document.getElementById("dashboardView");
  const notificationMatrixView = document.getElementById("notificationMatrixView");
  const rawDataView = document.getElementById("rawDataView");
  const solplanetControlView = document.getElementById("solplanetControlView");
  const samplingView = document.getElementById("samplingView");
  const databaseView = document.getElementById("databaseView");
  const workerLogsView = document.getElementById("workerLogsView");
  const workerFailureLogView = document.getElementById("workerFailureLogView");
  const tabDashboard = document.getElementById("tabDashboard");
  const tabNotificationMatrix = document.getElementById("tabNotificationMatrix");
  const tabRawData = document.getElementById("tabRawData");
  const tabSolplanetControl = document.getElementById("tabSolplanetControl");
  const tabSampling = document.getElementById("tabSampling");
  const tabDatabase = document.getElementById("tabDatabase");
  const tabWorkerLogs = document.getElementById("tabWorkerLogs");
  const tabWorkerFailureLog = document.getElementById("tabWorkerFailureLog");

  const dashboardActive = currentTab === "dashboard";
  const notificationMatrixActive = currentTab === "notificationMatrix";
  const rawDataActive = currentTab === "rawData";
  const solplanetControlActive = currentTab === "solplanetControl";
  const samplingActive = currentTab === "sampling";
  const databaseActive = currentTab === "database";
  const workerLogsActive = currentTab === "workerLogs";
  const workerFailureLogActive = currentTab === "workerFailureLog";
  if (dashboardView) dashboardView.classList.toggle("hidden", !dashboardActive);
  if (notificationMatrixView) notificationMatrixView.classList.toggle("hidden", !notificationMatrixActive);
  if (rawDataView) rawDataView.classList.toggle("hidden", !rawDataActive);
  if (solplanetControlView) solplanetControlView.classList.toggle("hidden", !solplanetControlActive);
  if (samplingView) samplingView.classList.toggle("hidden", !samplingActive);
  if (databaseView) databaseView.classList.toggle("hidden", !databaseActive);
  if (workerLogsView) workerLogsView.classList.toggle("hidden", !workerLogsActive);
  if (workerFailureLogView) workerFailureLogView.classList.toggle("hidden", !workerFailureLogActive);
  if (tabDashboard) tabDashboard.classList.toggle("active", dashboardActive);
  if (tabNotificationMatrix) tabNotificationMatrix.classList.toggle("active", notificationMatrixActive);
  if (tabRawData) tabRawData.classList.toggle("active", rawDataActive);
  if (tabSolplanetControl) tabSolplanetControl.classList.toggle("active", solplanetControlActive);
  if (tabSampling) tabSampling.classList.toggle("active", samplingActive);
  if (tabDatabase) tabDatabase.classList.toggle("active", databaseActive);
  if (tabWorkerLogs) tabWorkerLogs.classList.toggle("active", workerLogsActive);
  if (tabWorkerFailureLog) tabWorkerFailureLog.classList.toggle("active", workerFailureLogActive);
  [
    [tabDashboard, dashboardActive],
    [tabNotificationMatrix, notificationMatrixActive],
    [tabRawData, rawDataActive],
    [tabSolplanetControl, solplanetControlActive],
    [tabSampling, samplingActive],
    [tabDatabase, databaseActive],
    [tabWorkerLogs, workerLogsActive],
    [tabWorkerFailureLog, workerFailureLogActive],
  ].forEach(([tab, active]) => {
    if (!tab) return;
    tab.setAttribute("aria-selected", active ? "true" : "false");
    tab.tabIndex = active ? 0 : -1;
  });
  syncRawDataSystemUi();
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
  syncWorkerLogsFilters();
  if (stateCache.lastWorkerLogsPage) renderWorkerLogsPage(stateCache.lastWorkerLogsPage);
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
bindClickIfPresent("tabNotificationMatrix", () => {
  setActiveTab("notificationMatrix");
});
bindClickIfPresent("tabRawData", () => {
  setActiveTab("rawData");
});
bindChangeIfPresent("rawDataSystemSelect", (event) => {
  setRawDataSystem(event.target.value, { load: true });
});
bindClickIfPresent("solplanetRawModeCardsBtn", () => {
  setSolplanetRawMode("cards");
});
bindClickIfPresent("solplanetRawModeTableBtn", () => {
  setSolplanetRawMode("table");
});
bindClickIfPresent("tabSolplanetControl", () => {
  setActiveTab("solplanetControl");
});

bindClickIfPresent("tabSampling", () => {
  setActiveTab("sampling");
});
bindClickIfPresent("tabDatabase", () => {
  setActiveTab("database");
});
bindClickIfPresent("tabWorkerLogs", () => {
  setActiveTab("workerLogs");
});
bindClickIfPresent("tabWorkerFailureLog", () => {
  setActiveTab("workerFailureLog");
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

bindClickIfPresent("databasePrevPageBtn", async () => {
  if (!databasePager.hasPrev || databasePager.page <= 1) return;
  databasePager.page -= 1;
  await loadDatabase();
});

bindClickIfPresent("databaseNextPageBtn", async () => {
  if (!databasePager.hasNext) return;
  databasePager.page += 1;
  await loadDatabase();
});

bindChangeIfPresent("databaseTableSelect", async () => {
  databasePager.page = 1;
  await loadDatabase();
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
bindClickIfPresent("databaseRowDetailCloseBtn", () => {
  setDatabaseRowDetailModalVisible(false);
});
{
  const modal = document.getElementById("databaseRowDetailModal");
  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target === modal) setDatabaseRowDetailModalVisible(false);
    });
  }
}
window.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  const modal = document.getElementById("workerLogDetailModal");
  if (modal && !modal.classList.contains("hidden")) {
    setWorkerLogDetailModalVisible(false);
  }
  const databaseModal = document.getElementById("databaseRowDetailModal");
  if (databaseModal && !databaseModal.classList.contains("hidden")) {
    setDatabaseRowDetailModalVisible(false);
  }
});

bindChangeIfPresent("workerLogsCategorySelect", async () => {
  syncWorkerLogsFilters();
  workerLogsFilterState.hiddenStatuses.clear();
  workerLogsPager.page = 1;
  await loadWorkerLogs();
});

bindChangeIfPresent("workerLogsServiceSelect", async () => {
  syncWorkerLogsFilters();
  workerLogsFilterState.hiddenStatuses.clear();
  workerLogsPager.page = 1;
  await loadWorkerLogs();
});

bindChangeIfPresent("workerLogsTableModeSelect", () => {
  const mode = getWorkerLogsTableMode();
  localStorage.setItem(WORKER_LOGS_TABLE_MODE_KEY, mode);
  renderWorkerLogsHead();
  if (stateCache.lastWorkerLogsPage) renderWorkerLogsPage(stateCache.lastWorkerLogsPage);
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
bindClickIfPresent("sajProfileApplyBtn", (event) => {
  event?.stopPropagation?.();
  void applySajProfile("sajProfileSelect");
});
bindClickIfPresent("sajDashboardProfileApplyBtn", (event) => {
  event?.stopPropagation?.();
  void applySajProfile("sajDashboardProfileSelect");
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
  if (samplingTotalsChart) samplingTotalsChart.resize();
  refreshFlowDiagrams();
});

document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  if (target.closest("#combined-inverter1State")) {
    toggleSajDashboardProfilePopover();
    return;
  }
  const insidePopover = target.closest("#sajDashboardProfilePopover");
  if (!insidePopover) setSajDashboardProfilePopoverVisible(false);
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
syncRawDataSystemUi();
setActiveTab(currentTab, false);
setAutoRefresh(autoRefreshSeconds);
void ensureConfigReady().then((ready) => {
  if (ready) {
    void loadCurrentTab();
  }
});
