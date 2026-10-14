async function fetchJson(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 8000);
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
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
  } catch (err) {
    window.clearTimeout(timeoutId);
    if (err?.name === "AbortError") {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw err;
  }
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

function formatBinarySize(bytes) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value < 0) return "-";
  if (value < 1024) return `${Math.round(value)} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function xhrRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open(options.method || "GET", url, true);
    xhr.responseType = options.responseType || "";
    const headers = options.headers || {};
    for (const [name, value] of Object.entries(headers)) {
      xhr.setRequestHeader(name, value);
    }
    if (typeof options.onDownloadProgress === "function") {
      xhr.addEventListener("progress", (event) => options.onDownloadProgress(event, xhr));
    }
    if (xhr.upload && typeof options.onUploadProgress === "function") {
      xhr.upload.addEventListener("progress", (event) => options.onUploadProgress(event, xhr));
    }
    if (xhr.upload && typeof options.onUploadComplete === "function") {
      xhr.upload.addEventListener("load", () => options.onUploadComplete(xhr));
    }
    xhr.addEventListener("load", () => resolve(xhr));
    xhr.addEventListener("error", () => reject(new Error("Network request failed")));
    xhr.addEventListener("abort", () => reject(new Error("Request aborted")));
    xhr.send(options.body);
  });
}

const I18N = {
  en: {
    subtitle: "SAJ + Solplanet monitoring panel",
    siteTimeLabel: "Site time",
    languageLabel: "Language",
    autoRefreshLabel: "Auto Refresh",
    autoRefreshOff: "Off",
    autoRefresh30s: "Every 30s",
    autoRefresh60s: "Every 60s",
    configBtn: "Config",
    configCloseBtn: "Close",
    configSaveBtn: "Save",
    configAutoDiscoverBtn: "Auto Fetch Inverter / Battery SN",
    configModalTitle: "Initial Setup",
    configModalDesc: "Fill Home Assistant connection info first. It takes effect immediately after saving.",
    configHaUrlLabel: "HA URL",
    configHaTokenLabel: "HA Token",
    configDongleHostLabel: "Solplanet Dongle Host",
    configInverterSnLabel: "Solplanet Inverter SN",
    configBatterySnLabel: "Solplanet Battery SN",
    configSajSampleIntervalLabel: "SAJ Sampling Interval",
    configSolplanetSampleIntervalLabel: "Solplanet Sampling Interval",
    configDetailRetentionLabel: "Detailed History Retention",
    retention1Month: "1 month",
    retention2Months: "2 months",
    retention3Months: "3 months",
    retention6Months: "6 months",
    retention12Months: "12 months",
    configPruneRetentionBtn: "Clean Expired Detail",
    configPruneRetentionConfirm: "Delete detailed samples older than the selected retention period and compact the database?",
    configPruningRetention: "Cleaning detailed history...",
    configPruneRetentionDone: "Detailed history cleaned. Reclaimed {size}.",
    configPruneRetentionFailed: "Detailed history cleanup failed: {error}",
    configWeatherLatLabel: "Weather Latitude",
    configWeatherLonLabel: "Weather Longitude",
    configExportReserveFloorLabel: "Solplanet export reserve",
    configWeatherLatPlaceholder: "e.g. -33.8688 (Sydney)",
    configWeatherLonPlaceholder: "e.g. 151.2093 (Sydney)",
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
    configAutoDiscovering: "Discovering...",
    configAutoDiscovered: "Inverter and battery SN loaded",
    configAutoDiscoverFailed: "Auto fetch failed: {error}",
    configSaved: "Saved",
    configSaveFailed: "Save failed: {error}",
    configMustSaveFirst: "Configuration is not complete yet. Complete setup first.",
    dashboardReserveFloorLabel: "Export reserve",
    dashboardReserveFloorInputLabel: "Keep",
    dashboardReserveFloorSaveBtn: "Save",
    dashboardReserveFloorDisplay: "Reserve {value} before late selling",
    dashboardReserveFloorSaving: "Saving...",
    dashboardReserveFloorSaved: "Saved",
    dashboardReserveFloorSaveFailed: "Save failed: {error}",
    weatherTitle: "Solar Weather Forecast",
    weatherSolarLabel: "Solar",
    weatherYesterday: "Yesterday",
    weatherToday: "Today",
    weatherTomorrow: "Tomorrow",
    weatherSunHours: "☀ {h}h sunshine",
    weatherTempRange: "{min}°C – {max}°C",
    weatherSolarPct: "{pct}%",
    weatherLoadError: "Weather unavailable: {error}",
    refreshBtn: "Refresh",
    dashboardTab: "Dashboard",
    billsTab: "Bills",
    planTab: "Plan",
    planTitle: "Electricity Plan",
    planMatrixTitle: "Plan Timetable",
    planMatrixHint: "Cell tint follows buy price: green is lower, orange is higher.",
    planMatrixType: "Time",
    planChangeTitle: "Rate Changes",
    planChangeHint: "Bill-derived change history for the current account.",
    planBeforeDiscount: "Before",
    planAfterDiscount: "After",
    planRecordedAt: "Recorded {date} · {distributor} · {postcode}",
    planUpdatedAt: "Updated {date}",
    planAddressNeedsCheck: "Address house number still needs confirmation.",
    planAddressConfirmed: "Confirmed by bills",
    planNoData: "No plan data available.",
    planLoadFailed: "Plan data unavailable: {error}",
    planNotListed: "Not listed",
    planCapKwhPerDay: "{value} kWh/day cap",
    planRateDaily: "Daily charge",
    planRatePeak: "Peak usage",
    planRateOffPeakFirst: "Free/off-peak window",
    planRateOffPeakBalance: "Off-peak balance",
    planRateShoulder: "Shoulder usage",
    planRateControlledLoad: "Controlled load",
    planRateMetering: "Metering charge",
    planRateFitEvening: "Solar feed-in 4pm-11pm",
    planRateFitOther: "Solar feed-in 11pm-4pm",
    planRateSuperExportTopUp: "Super Export top-up",
    planRateZeroHeroCredit: "ZEROHERO credit",
    planNoRateChanges: "No rate changes recorded.",
    planChangeSource: "Source: {source}",
    planChangeFromTo: "{from} -> {to}",
    planSummaryRetailer: "Retailer",
    planSummaryDistributor: "Distributor",
    planSummaryPostcode: "Postcode",
    planSummaryStatus: "Status",
    planTimetableFixed: "Fixed / non-time",
    planTimetableBuy: "Buy",
    planTimetableSell: "Sell",
    planTimetableSpecial: "Special",
    planTimetableNone: "None",
    planTimetableFreeUsage: "Free usage",
    planTimetableDailySupply: "Daily supply",
    planTimetableMetering: "Metering",
    planTimetableControlledLoad: "Controlled load",
    planTimetableSuperExport: "Super Export",
    planTimetableTotalExport: "total {value}",
    planTimetableZeroHero: "ZEROHERO credit",
    planTimetableAfterCap: "after cap {value}",
    reportDailyCostOverCap: "over cap",
    notificationMatrixTab: "Time Window",
    notificationMatrixTitle: "Time Window",
    notificationMatrixIntro: "Each panel groups rules by Operation and Notification so you can quickly distinguish automated actions from alerts. Tesla and pool pump auto control are managed only from Priority Scores, not from each time-window rule.",
    notificationMatrixPriorityTitle: "Priority Scores",
    notificationMatrixPrioritySummary: "Tesla uses a non-linear SOC score: below 80% it rises faster and below 50% it becomes high quickly. Pool pump priority is based on day/night runtime hours times weight.",
    notificationMatrixPriorityWinner: "Current winner: {winner}",
    notificationMatrixPriorityWinnerNone: "No auto control",
    notificationMatrixPriorityBusinessDay: "Business day: {value}",
    notificationMatrixPriorityFormula: "Formula",
    notificationMatrixPriorityTeslaLabel: "Tesla",
    notificationMatrixPriorityPumpLabel: "Pool Pump",
    notificationMatrixPrioritySoc: "SOC",
    notificationMatrixPriorityRollingRuntime: "24h runtime",
    notificationMatrixPriorityModel: "Priority model",
    notificationMatrixPriorityWeightedRuntime: "Priority hours",
    notificationMatrixPriorityRawRuntime: "Raw runtime",
    notificationMatrixPriorityDayRuntime: "Day runtime",
    notificationMatrixPriorityNightRuntime: "Night runtime",
    notificationMatrixPriorityWeightDay: "Day",
    notificationMatrixPriorityWeightNight: "Night",
    notificationMatrixPriorityDeficit: "Deficit",
    notificationMatrixPriorityCurrentWindow: "Today window",
    notificationMatrixPriorityPreviousWindow: "Yesterday window",
    notificationMatrixPriorityWeights: "Day/Night weights",
    notificationMatrixPriorityOverrideLabel: "Highest priority override",
    notificationMatrixPriorityOverrideAuto: "Auto",
    notificationMatrixPriorityOverrideTesla: "Tesla",
    notificationMatrixPriorityOverridePump: "Pool Pump",
    notificationMatrixPriorityOverriddenScore: "{score} ({original})",
    notificationMatrixPriorityTeslaAutoControlLabel: "Tesla auto control",
    notificationMatrixPriorityTeslaAutoControlHint: "Checked: worker may start or stop Tesla charging automatically. Unchecked: Tesla stays manual only.",
    notificationMatrixPriorityTeslaAutoControlEnabled: "Auto enabled",
    notificationMatrixPriorityTeslaAutoControlDisabled: "Manual only",
    notificationMatrixPriorityTeslaForceThresholdLabel: "Forced charge threshold",
    notificationMatrixPriorityTeslaForceThresholdHint: "When Tesla SOC is below this percentage, overnight control skips the shared-power calculation and charges at max current.",
    notificationMatrixPriorityTeslaForceThresholdInvalid: "Enter a whole-number threshold between 0 and 100.",
    notificationMatrixPriorityPumpAutoControlLabel: "Pool pump auto control",
    notificationMatrixPriorityPumpAutoControlHint: "Checked: worker may start or stop the pump automatically. Unchecked: the pump stays manual only.",
    notificationMatrixPriorityPumpAutoControlEnabled: "Auto enabled",
    notificationMatrixPriorityPumpAutoControlDisabled: "Manual only",
    notificationMatrixPriorityPumpForceRuntimeLabel: "Forced runtime threshold",
    notificationMatrixPriorityPumpForceRuntimeHint: "When the pool pump has run less than this many hours in the last 24 hours, overnight control force-runs it before shared-power allocation.",
    notificationMatrixPriorityPumpForceRuntimeInvalid: "Enter a valid threshold between 0 and {maxHours} hours.",
    notificationMatrixLoadingTitle: "Loading Time Window",
    notificationMatrixLoadingBody: "Refreshing rules, priority scores, and live window state...",
    notificationMatrixPriorityExpand: "Expand priority",
    notificationMatrixPriorityCollapse: "Collapse priority",
    notificationMatrixNoNotifications: "No rule is configured for this window at the moment.",
    notificationMatrixAlwaysNote: "This watch is always active and is shown separately from the 24-hour time-window schedule.",
    notificationMatrixCurrentWindowBadge: "Current Window",
    notificationMatrixExpandWindow: "Expand window",
    notificationMatrixCollapseWindow: "Collapse window",
    notificationMatrixOperationConditionsTitle: "Conditions",
    notificationMatrixOperationConditionsMatched: "{matched}/{total} matched",
    notificationMatrixConditionMatched: "Matched",
    notificationMatrixConditionNotMatched: "Not matched",
    notificationMatrixConditionCurrentWindow: "Window active",
    notificationMatrixConditionTeslaConnected: "Tesla connected",
    notificationMatrixConditionTeslaReady: "Tesla ready",
    notificationMatrixConditionTeslaGridCap: "Predicted grid within 15.0kW cap",
    notificationMatrixConditionTeslaSocStart: "Solplanet SOC >= 95% start threshold",
    notificationMatrixConditionTeslaSocKeep: "Solplanet SOC >= 90% keep threshold",
    notificationMatrixConditionTeslaOvernightReserve: "Solplanet SOC > 10% overnight reserve",
    notificationMatrixConditionTeslaOvernightBudget: "Available for Tesla",
    notificationMatrixConditionTeslaOvernightBatteryTotal: "Usable battery (both)",
    notificationMatrixConditionTeslaOvernightHomeLoad: "Home load to 11am",
    notificationMatrixConditionTeslaOvernightPumpLoad: "Pool pump to 5am",
    notificationMatrixConditionTeslaOvernightChargeState: "Charging state",
    notificationMatrixConditionTeslaOvernightLastAction: "Last adjustment",
    notificationMatrixConditionPoolPumpGridCap: "Pool pump keeps predicted grid within 15.0kW cap",
    notificationMatrixConditionPoolPumpTeslaPriority: "Tesla priority already satisfied",
    notificationMatrixConditionPoolPumpSchedule: "Pump schedule",
    notificationMatrixConditionPoolPumpRuntimeBudget: "Runtime budget",
    notificationMatrixConditionValueCurrentNeed: "{current} (need {need})",
    notificationMatrixConditionSajProfileApplied: "Target SAJ profile applied",
    notificationMatrixConditionSajSyncCleared: "Remote sync cleared",
    notificationMatrixProgressWatching: "Watching",
    notificationMatrixProgressConditionMet: "Condition met now",
    notificationMatrixProgressNotified: "Already notified",
    notificationMatrixProgressNoData: "Waiting for live data",
    notificationMatrixProgressScheduled: "Scheduled for this window",
    notificationMatrixProgressApplied: "Applied in this round",
    notificationMatrixProgressHolding: "Holding target state",
    notificationMatrixProgressPendingSync: "Waiting for device readback",
    notificationMatrixProgressDisabled: "Disabled",
    notificationMatrixProgressSaving: "Saving",
    notificationMatrixProgressWindowTracking: "Only tracked during this window",
    notificationMatrixProgressCurrent: "Current {value}",
    notificationMatrixProgressTarget: "Target {value}",
    notificationMatrixProgressTriggerAtMost: "Trigger <= {value}",
    notificationMatrixProgressTriggerAtLeast: "Trigger >= {value}",
    notificationMatrixProgressTriggerAbove: "Trigger > {value}",
    notificationMatrixProgressRemaining: "{value} remaining",
    notificationMatrixProgressAboveThreshold: "{value} above threshold",
    notificationMatrixProgressBelowThreshold: "{value} below threshold",
    notificationMatrixRateFree: "Buy $0/kWh · Sell 0c/kWh",
    notificationMatrixRateShoulder: "Buy 37.5c/kWh · Sell 0c/kWh",
    notificationMatrixRateExport: "Buy 57.8c/kWh · Sell 8c/kWh",
    notificationMatrixWindowFreeEnergyTitle: "Free Window",
    notificationMatrixWindowFreeEnergySummary: "11:00-15:00. FOUR4FREE buy rate is $0/kWh and feed-in is 0c/kWh; worker prioritizes Tesla first, then allows the pool pump within the 15.0kW grid cap.",
    notificationMatrixWindowAfterFreeShoulderTitle: "Solar Prep Window",
    notificationMatrixWindowAfterFreeShoulderSummary: "15:00-16:00. Buy rate is 37.5c/kWh and feed-in is 0c/kWh; worker watches Solplanet battery headroom before the export window.",
    notificationMatrixWindowExportTitle: "Export Window",
    notificationMatrixWindowExportSummary: "16:00-23:00. Buy rate is 57.8c/kWh and feed-in is 8c/kWh; when Tesla is below the forced threshold, worker charges it while usable battery stays above the configured export reserve.",
    notificationMatrixWindowOvernightTitle: "Overnight Budget Window",
    notificationMatrixWindowOvernightSummary: "23:00-11:00 next day. Buy rate is 37.5c/kWh and feed-in is 0c/kWh; worker runs Tesla charging and pool pump within the overnight battery budget.",
    notificationMatrixRuleTypeNotification: "Notification",
    notificationMatrixRuleTypeOperation: "Operation",
    notificationRuleSajProfileFreeEnergyTitle: "SAJ inverter uses Time of Use in Free Window",
    notificationRuleSajProfileFreeEnergyTrigger: "During 11:00-14:50 target SAJ profile is Time of Use, then it switches back to Self Consumption for the last 10 minutes.",
    notificationRuleSajProfileSelfConsumptionTitle: "SAJ inverter stays in Self Consumption",
    notificationRuleSajProfileSelfConsumptionTrigger: "Outside the main Free Window period, target SAJ profile stays on Self Consumption.",
    notificationRuleTeslaFreeEnergyTitle: "Tesla charging current follows Free Window rule",
    notificationRuleTeslaFreeEnergyTrigger: "During 11:00-15:00 the worker adjusts Tesla charging current under the configured grid cap.",
    notificationRulePoolPumpFreeEnergyTitle: "Pool pump follows Tesla priority in Free Window",
    notificationRulePoolPumpFreeEnergyTrigger: "During 11:00-15:00 start the pool pump only when Tesla does not need to start first, or Tesla is already charging, and predicted grid stays within 15.0kW.",
    notificationRuleTeslaAfterFreeShoulderTitle: "Tesla charging follows Solplanet SOC during Solar Prep Window",
    notificationRuleTeslaAfterFreeShoulderTrigger: "Start when Solplanet SOC reaches 95%, stop when it falls below 90%, otherwise hold the current state.",
    notificationRuleTeslaOvernightShoulderTitle: "Tesla charging follows Overnight Budget Window",
    notificationRuleTeslaOvernightShoulderTrigger: "During 23:00-11:00, start Tesla charging when projected battery minus home load to 11am minus pool pump to 5am is above 1.0 kWh; stop when that budget is exhausted.",
    notificationRulePoolPumpOvernightTitle: "Pool pump uses Overnight Budget Window battery budget",
    notificationRulePoolPumpOvernightTrigger: "During 23:00-05:00, estimate how long the pump can run from the overnight battery forecast, then use only 80% of that available runtime.",
    notificationRuleTeslaExportWindowTitle: "Tesla charging follows forced threshold during Export Window",
    notificationRuleTeslaExportWindowTrigger: "When Tesla SOC is below the forced threshold, charge while usable battery remains above the configured export reserve; stop at or below that reserve.",
    notificationMatrixTeslaTriggerGridCap: "Target <= 15.0kW grid",
    notificationMatrixTeslaTriggerSolplanetSoc: "Solplanet {soc} (start >= 95%, keep >= 90%)",
    notificationMatrixTeslaTriggerSolplanetReserve: "Solplanet {soc} (keep > 10%)",
    notificationMatrixTeslaTriggerOvernightBudget: "{battery} − {home} (home {homeKw}×{homeH}) − {pump} (pump {pumpKw}×{pumpH}) = {available}",
    notificationMatrixTeslaTriggerExport: "Export {value} / surplus solar",
    notificationMatrixTeslaTriggerStop: "Target charging off",
    notificationMatrixTeslaCurrent: "Tesla {actual} / set {configured}",
    notificationRuleGridImportStartedTitle: "Grid import started during Export Window",
    notificationRuleGridImportStartedTrigger: "Triggers once when grid import changes from inactive to active during Export Window.",
    notificationRuleGridImportStartedPostExportPeakTitle: "Grid import started during Peak Guard Window",
    notificationRuleGridImportStartedPostExportPeakTrigger: "Triggers once when grid import changes from inactive to active during the Peak Guard Window.",
    rawDataTab: "Raw Data",
    rawDataTitle: "Raw Data",
    rawDataSystemLabel: "System",
    rawDataSystemSolplanet: "Solplanet",
    rawDataSystemSaj: "SAJ",
    sajControlTab: "SAJ Control",
    solplanetControlTab: "Solplanet Control",
    entitiesTab: "Entities",
    samplingTab: "History",
    reportTab: "Report",
    databaseTab: "Database",
    workerLogsTab: "Worker Logs",
    operationTimelineTab: "Operations",
    failureLogTab: "Failure Logs",
    databaseTitle: "Database Browser",
    databaseTableLabel: "Table",
    databaseTablePlaceholder: "Select a table",
    databaseMeta: "Table {table} · {columns} columns",
    databaseTotal: "Total {total} rows",
    databasePageInfo: "Page {page}/{totalPages} (showing {count})",
    databaseEmpty: "Select a table to view data.",
    databaseTransferExportTitle: "Exporting database",
    databaseTransferImportTitle: "Importing database",
    databaseTransferPreparing: "Preparing...",
    databaseTransferProcessing: "Processing...",
    databaseTransferPercent: "{value}%",
    databaseTransferDownloaded: "Downloaded {loaded} / {total}",
    databaseTransferUploaded: "Uploaded {loaded} / {total}",
    databaseTransferExportStarting: "Preparing export file...",
    databaseTransferExportStreaming: "Downloading database file...",
    databaseTransferExportFinishing: "Finalizing file...",
    databaseTransferExportDone: "Export complete. Download should start shortly.",
    databaseTransferImportUploading: "Uploading database file...",
    databaseTransferImportApplying: "Replacing local database. Please wait...",
    databaseTransferImportRefreshing: "Refreshing page data...",
    databaseTransferImportDone: "Import complete.",
    databaseTransferBackupTitle: "Creating database backup",
    databaseTransferBackupRunning: "Copying a consistent SQLite snapshot...",
    databaseTransferBackupDone: "Backup complete.",
    databaseTransferFailed: "Failed",
    databaseBackupMeta: "Backups {count}/{retention} · Latest {latest} ({size}) · Total {totalSize} · {directory}",
    databaseBackupMetaEmpty: "Backups none yet · keeping latest {retention} · {directory}",
    databaseBackupMetaFailed: "Backup status failed: {error}",
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
    solplanetDashboardPinModalTitle: "Solplanet Buy/Sell Plan",
    solplanetDashboardPinModalIntro: "Adjust Solplanet buy/sell power and schedule.",
    solplanetDashboardPinCurrentLabel: "Current Pin (W)",
    solplanetDashboardPinTargetLabel: "Target Pin (W)",
    solplanetDashboardBuyTitle: "Buy / charge",
    solplanetDashboardBuyPowerLabel: "Buy power Pin (W)",
    solplanetDashboardBuyStartLabel: "Buy start",
    solplanetDashboardBuyEndLabel: "Buy end",
    solplanetDashboardSellTitle: "Sell / discharge",
    solplanetDashboardSellPowerLabel: "Sell power Pout (W)",
    solplanetDashboardSellStartLabel: "Sell start",
    solplanetDashboardSellEndLabel: "Sell end",
    solplanetDashboardPinApplyBtn: "Apply plan",
    solplanetDashboardPinLoading: "Loading current Solplanet control state...",
    solplanetDashboardPinReady: "Current Solplanet plan loaded.",
    solplanetDashboardPinApplying: "Applying Solplanet plan...",
    solplanetDashboardPinApplied: "Solplanet plan updated: Pin {pin}W, Pout {pout}W.",
    solplanetDashboardPinOpenHint: "Click the Solplanet inverter node to adjust buy/sell power and schedule.",
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
    sajTouDirectionSell: "Sell",
    sajTouDirectionBuy: "Buy",
    sajTouPowerLabel: "Power (W)",
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
    dashboardUsableBatteryTitle: "Usable Battery Total",
    dashboardNotificationsTitle: "Notifications",
    dashboardNotificationStateNotified: "Notified",
    dashboardNotificationStateDismissed: "Dismissed",
    dashboardNotificationDismiss: "Dismiss",
    dashboardNotificationLevelAlarm: "Alarm",
    dashboardNotificationLevelWarning: "Warning",
    dashboardNotificationLevelInfo: "Info",
    dashboardNotificationMeta: "Window {window} · Trigger {trigger} · {time}",
    integratedFlowSubtitle: "SAJ solar/grid + SAJ/Solplanet batteries, dual inverters in parallel",
    poolPumpKicker: "Home Load Device",
    poolPumpTitle: "Pool Pump",
    poolPumpMeta: "Mounted under Home Load. Does not change the combined load split.",
    poolPumpPowerLabel: "Current Power",
    poolPumpStateLabel: "State",
    poolPumpStateOn: "Running",
    poolPumpStateOff: "Stopped",
    poolPumpStateUnknown: "Unknown",
    poolPumpStateUnavailable: "Unavailable",
    poolPumpActionTurnOn: "Turn On Pump",
    poolPumpActionTurnOff: "Turn Off Pump",
    poolPumpActionBusy: "Updating...",
    poolPumpControlFailed: "Pool pump control failed: {error}",
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
    mobileFlowHomeTitle: "Home",
    mobileFlowSajInverterShort: "SAJ Inverter",
    mobileFlowSolplanetInverterShort: "Solplanet Inverter",
    mobileFlowSajBatteryShort: "SAJ Battery",
    mobileFlowSolplanetBatteryShort: "Solplanet Battery",
    mobileFlowInvertersCombinedShort: "Inverters",
    mobileFlowBatteriesCombinedShort: "Battery Bank",
    mobileDashboardFocusTitle: "Dashboard Focus",
    mobileDashboardFocusHint: "Keep the phone view centered on load power, usable battery, and live flow.",
    mobileDashboardLoadPower: "Working power",
    mobileDashboardUsableBattery: "Battery remaining",
    mobileDashboardNext8: "By 08:00",
    mobileDashboardNext11: "By 11:00",
    mobileDashboardDetailsTitle: "More details",
    mobileDashboardFlowSummary: "Flow summary",
    mobileDashboardFlowDirectionIdle: "Idle",
    mobileDashboardFlowDirectionImport: "Importing",
    mobileDashboardFlowDirectionExport: "Exporting",
    mobileDashboardFlowDirectionCharge: "Charging",
    mobileDashboardFlowDirectionDischarge: "Discharging",
    mobileDashboardFlowDirectionActive: "Active",
    mobileDashboardFlowDirectionOffline: "Offline",
    mobileDashboardFlowGrid: "Grid",
    mobileDashboardFlowSaj: "SAJ path",
    mobileDashboardFlowSolplanet: "Solplanet path",
    mobileDashboardFlowLoad: "Home load",
    mobileDashboardFlowTesla: "Tesla",
    mobileDashboardFlowPoolPump: "Pool pump",
    mobileDashboardFlowBatterySaj: "SAJ battery",
    mobileDashboardFlowBatterySolplanet: "Solplanet battery",
    mobileDashboardFlowExtra: "Extra details",
    mobileDashboardHubTitle: "Hub",
    mobileDashboardFreshnessLoading: "Loading latest data",
    mobileDashboardFreshnessRefreshing: "Refreshing",
    mobileDashboardFreshnessFresh: "Data updated",
    mobileDashboardFreshnessStale: "Data stale",
    mobileDashboardFreshnessFailed: "Refresh failed",
    mobileDashboardFreshnessReasonPrefix: "Cause: {reason}",
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
    teslaCableStatusConnected: "Connected",
    teslaCableStatusDisconnected: "Disconnected",
    teslaConnectionStateUnplugged: "Disconnected",
    teslaConnectionStatePluggedNotCharging: "Connected",
    teslaConnectionStateCharging: "Charging",
    teslaConnectionStateUnknown: "Connection unknown",
    teslaLocationHome: "Location: home",
    teslaLocationNonHome: "Location: non-home",
    teslaLocationUnknown: "Location unknown",
    teslaControlStart: "Start Charging",
    teslaControlStop: "Stop Charging",
    teslaControlStarting: "Starting...",
    teslaControlStopping: "Stopping...",
    teslaControlBusy: "Updating...",
    teslaControlUnavailable: "Control Unavailable",
    teslaControlApplyFailed: "Tesla charging control failed: {error}",
    teslaCurrentApplyFailed: "Tesla current update failed: {error}",
    teslaCurrentLabel: "Charge current",
    teslaCurrentApply: "Apply",
    teslaCurrentApplying: "Applying...",
    teslaCurrentModalIntro: "Update Tesla charging current.",
    teslaCurrentModalStatusReady: "Current setpoint {value}",
    teslaControlStatusRequestStart: "Sending start request...",
    teslaControlStatusRequestStop: "Sending stop request...",
    teslaControlStatusAwaitingStart: "Start requested. Waiting for Tesla to respond.",
    teslaControlStatusAwaitingStop: "Stop requested. Waiting for Tesla to respond.",
    teslaControlStatusDelayedStart: "Start request was sent, but charging has not started yet.",
    teslaControlStatusDelayedStop: "Stop request was sent, but charging has not stopped yet.",
    teslaControlStatusFailedStart: "Start request did not go through.",
    teslaControlStatusFailedStop: "Stop request did not go through.",
    teslaControlStatusStarted: "Charging started.",
    teslaControlStatusStopped: "Charging stopped.",
    teslaBatteryEnergyUnavailable: "- / - kWh",
    teslaChargeEtaUnavailable: "Est. full -",
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
    batteryWindowForecastFreeLabel: "Free",
    batteryWindowForecastDischargeLabel: "Discharge",
    batteryWindowForecastCompact: "{window} end: {soc}",
    batteryWindowForecastCompactNoData: "{window} end: -",
    switchboardStateActive: "Bus Active",
    switchboardStateIdle: "Bus Idle",
    socLabel: "SOC",
    entityExplorerTitle: "Entity Explorer",
    samplingTitle: "Sampling Records",
    reportTitle: "Daily Report",
    reportGeneratedMeta: "Saved daily report for {day} · generated {time}",
    reportNoDataMeta: "Daily report for {day} has no data · generated {time}",
    reportGeneratingMeta: "Generating daily report for {day}...",
    reportLoadingMeta: "Loading daily report for {day}...",
    reportLoadingModalTitle: "Loading Daily Report",
    reportLoadingModalBody: "Preparing saved report data...",
    reportUnavailable: "No saved daily report is available for {day}",
    reportNotReady: "Future dates are not ready yet.",
    reportGenerateFailed: "Daily report generation failed for {day}: {error}",
    reportLatestBadge: "Latest",
    reportDailyCostTitle: "Daily Cost",
    reportDailyCostMeta: "{plan} · {basis} · {samples} samples",
    reportDailyCostNoData: "Cost estimate unavailable for this report.",
    reportDailyCostEstimated: "Estimated",
    reportDailyCostBillActual: "Usage actual",
    reportDailyCostSystemNoDailyCap: "System no daily cap",
    reportDailyCostSystemDailyCap: "System daily cap",
    reportDailyCostUsageNoDailyCap: "Usage no daily cap",
    reportDailyCostUsageDailyCap: "Usage daily cap",
    reportDailyCostBillAllocated: "calculated",
    reportDailyCostNoBill: "No usage uploaded for this day",
    reportDailyCostDelta: "Delta",
    reportDailyCostFixed: "Fixed",
    reportDailyCostImport: "Import",
    reportDailyCostCredits: "Credits",
    reportDailyCostZeroHero: "ZEROHERO",
    reportDailyCostBreakdown: "Breakdown",
    reportDailyCostComponent: "Component",
    reportDailyCostFormula: "Formula",
    reportDailyCostBill: "Actual",
    reportDailyCostEstimateMinusBill: "Estimate - actual",
    reportDailyCostHigher: "estimate higher",
    reportDailyCostLower: "estimate lower",
    reportDailyCostEven: "aligned",
    reportDailyCostFixedDetail: "daily supply + metering",
    reportDailyCostImportDetail: "peak {peak} · shoulder {shoulder} · free {free}",
    reportDailyCostCreditDetail: "feed-in {fit} · super export {superExport}",
    reportDailyCostZeroHeroDetail: "{status} · red {redWindows} · yellow {yellowWindows} · coverage {coverage}",
    reportDailyCostQualified: "qualified",
    reportDailyCostNotQualified: "not qualified",
    reportDailyCostInsufficient: "insufficient data",
    reportDailyCostNotAvailable: "not available",
    reportUsageChooseBtn: "Upload usage",
    reportUsageUploading: "Uploading usage...",
    reportUsageUploaded: "Usage {name} uploaded · updated {days} reports",
    reportUsageUploadFailed: "Usage upload failed: {error}",
    reportUsageNoFile: "Choose a CSV usage file first.",
    reportBillListBtn: "Bills",
    reportBillListLoading: "Loading bills...",
    reportBillListEmpty: "No uploaded bills",
    reportBillListFailed: "Bills unavailable: {error}",
    billsTitle: "Bills",
    billsUploadBtn: "Upload bill",
    billsMeta: "{count} uploaded bills · sorted by billing period",
    billsUpdated: "Updated {date}",
    billsLoading: "Loading bills...",
    billsEmpty: "No uploaded bills yet.",
    billsLoadFailed: "Bills unavailable: {error}",
    billsUploading: "Uploading bill...",
    billsUploaded: "Bill {invoice} uploaded",
    billsUploadFailed: "Bill upload failed: {error}",
    billsNoFile: "Choose a PDF bill first.",
    billsDropTitle: "Drop bill file to upload",
    billsDropHint: "You will be asked to confirm before upload.",
    billsDropConfirmUpload: "Upload bill file {name}?",
    billsPeriodSelectLabel: "Bill period",
    billsTotalSpend: "Total billed",
    billsLatestPeriod: "Latest period",
    billsPlanCount: "Plans seen",
    billsPeriod: "Period",
    billsBillingDays: "Billing days",
    billsDaysValue: "{days} days",
    billsInvoice: "Invoice",
    billsIssued: "Issued",
    billsDue: "Due",
    billsAmountDue: "Amount due",
    billsBillTotal: "Bill total",
    billsPay: "Pay {amount}",
    billsCreditEarned: "Credit earned {amount}",
    billsSettled: "$0.00",
    billsPlanRates: "Plan and rates",
    billsParsedLines: "Parsed line items",
    billsDocuments: "Source documents",
    billsOpenPdf: "Open PDF",
    billsOpenText: "Open extracted text",
    billsRateDaily: "Daily",
    billsRatePeak: "Peak",
    billsRateFree: "Free",
    billsRateShoulder: "Shoulder",
    billsRateFitEvening: "FIT evening",
    billsRateFitOther: "FIT other",
    billsRateSuperExport: "Super Export",
    billsRateZeroHero: "ZEROHERO",
    billsLineDescription: "Description",
    billsLinePeriod: "Period",
    billsLineQuantity: "Quantity",
    billsLineRate: "Rate",
    billsLineTotal: "Total",
    billsAnalysisType: "Analysis",
    billsComparisonTitle: "Cost comparison",
    billsComparisonBill: "Bill",
    billsComparisonUsageDailyCap: "Usage daily cap",
    billsComparisonSystemDailyCap: "System daily cap",
    billsComparisonUsageNoDailyCap: "Usage no daily cap",
    billsComparisonSystemNoDailyCap: "System no daily cap",
    billsComparisonDifference: "Difference",
    billsComparisonFixed: "Fixed",
    billsComparisonImport: "Import",
    billsComparisonSolar: "Solar feed-in",
    billsComparisonSuper: "Super Export",
    billsComparisonZeroHero: "ZEROHERO",
    billsComparisonOther: "Other",
    billsComparisonTotal: "Total",
    billsComparisonDataDays: "{usageDays}/{days} usage days · {systemDays}/{days} system days",
    billsComparisonKwhTitle: "Usage kWh",
    reportCalendarTitle: "Report Calendar",
    reportCalendarMeta: "{month} · generated reports {count}/{days}",
    reportCalendarLoading: "Loading report index...",
    reportCalendarSaved: "Saved",
    reportCalendarNoData: "No data",
    reportCalendarLive: "Live",
    reportCalendarMissing: "Generate",
    reportCalendarPending: "Not ready",
    reportCalendarChecking: "Checking",
    reportCalendarGenerating: "Generating",
    reportCalendarBillActual: "Bill actual",
    reportCalendarBillActualWithInvoice: "Bill actual ({invoice})",
    reportCalendarPlanFour4Free: "FOUR4FREE",
    reportCalendarPlanZeroHeroLegacy: "Old ZEROHERO",
    reportTodayLiveMeta: "Live report for {day} · raw data",
    reportCalendarPrevMonth: "Previous month",
    reportCalendarNextMonth: "Next month",
    reportWeekdayMon: "Mon",
    reportWeekdayTue: "Tue",
    reportWeekdayWed: "Wed",
    reportWeekdayThu: "Thu",
    reportWeekdayFri: "Fri",
    reportWeekdaySat: "Sat",
    reportWeekdaySun: "Sun",
    monthlyReportTab: "Monthly",
    monthlyReportTitle: "Monthly Report",
    monthlyReportSubtitle: "{month} · {days}/{total} days observed · {mode}",
    monthlyReportModeProjection: "month-to-date with projection",
    monthlyReportModeComplete: "complete month",
    monthlyReportModeSaved: "saved month",
    monthlyReportModeFuture: "future month",
    monthlyReportPrev: "Previous Year",
    monthlyReportNext: "Next Year",
    monthlyReportMonthLabel: "Month",
    monthlyReportPickerTitle: "{year} monthly reports",
    monthlyReportPickerLoading: "Checking reports...",
    monthlyReportPickerSaved: "Saved",
    monthlyReportPickerNoData: "No data",
    monthlyReportPickerGenerate: "Generate",
    monthlyReportPickerLive: "Live",
    monthlyReportPickerPending: "Not ready",
    monthlyReportStatusSaved: "Saved report · loaded from persistent snapshot",
    monthlyReportStatusNoData: "Saved report has no data",
    monthlyReportLoading: "Loading monthly analysis...",
    monthlyReportUnavailable: "No monthly data available for this month yet.",
    monthlyReportGenerated: "Generated {time}",
    monthlyReportRecommendationsTitle: "Recommendations",
    monthlyReportRecommendationsCount: "{count} items",
    monthlyReportProjectionTitle: "Month-End Estimate",
    monthlyReportComparisonTitle: "Comparison",
    monthlyReportHeroTitle: "Export Window",
    monthlyReportDailyTrendTitle: "Daily Trend",
    monthlyReportDailyTrendYAxis: "Energy (kWh)",
    monthlyReportAnomaliesTitle: "Anomalies",
    monthlyReportDailyTableTitle: "Daily Detail",
    monthlyReportTableDay: "Day",
    monthlyReportTableLoad: "Load",
    monthlyReportTableSolar: "Solar",
    monthlyReportTableImport: "Import",
    monthlyReportTableExport: "Export",
    monthlyReportTableHero: "Hero",
    monthlyReportTableAction: "Action",
    monthlyReportOpenDaily: "Open Daily",
    monthlyReportKpiLoad: "Home load",
    monthlyReportKpiSolar: "Solar generation",
    monthlyReportKpiImport: "Grid import",
    monthlyReportKpiExport: "Grid export",
    monthlyReportKpiNetGrid: "Net grid",
    monthlyReportKpiSelfUse: "Solar self-use",
    monthlyReportKpiGridDependence: "Grid dependence",
    monthlyReportKpiCoverage: "Data coverage",
    monthlyReportMetricProjectedLoad: "Projected load",
    monthlyReportMetricProjectedImport: "Projected import",
    monthlyReportMetricProjectedExport: "Projected export",
    monthlyReportMetricScale: "Projection scale",
    monthlyReportMetricPrevLoadDelta: "Load vs previous period",
    monthlyReportMetricPrevImportDelta: "Import vs previous period",
    monthlyReportMetricPrevSolarDelta: "Solar vs previous period",
    monthlyReportMetricAvgImportDelta: "Import vs 4-week norm",
    monthlyReportMetricHeroImportWindows: "Import-observed windows",
    monthlyReportMetricHeroNetImportWindows: "Net-import windows",
    monthlyReportMetricHeroImportEnergy: "Export-window import energy",
    monthlyReportMetricHeroNoData: "No-data windows",
    monthlyReportAnomalyTopImport: "Highest import days",
    monthlyReportAnomalyTopBaseLoad: "Highest base-load days",
    monthlyReportAnomalyWeakSolar: "Weak solar days",
    monthlyReportAnomalyHeroIssue: "Export Window issue days",
    monthlyReportAnomalyLegendImport: "High import",
    monthlyReportAnomalyLegendBaseLoad: "High base load",
    monthlyReportAnomalyLegendWeakSolar: "Weak solar",
    monthlyReportAnomalyLegendHero: "Export-window import",
    monthlyReportAnomalyHintImport: "One of the five highest grid-import days this month.",
    monthlyReportAnomalyHintBaseLoad: "One of the five highest other-home-load days, excluding Tesla and pool pump.",
    monthlyReportAnomalyHintWeakSolar: "Solar generation was below 65% of the monthly median, with at least 50% data coverage.",
    monthlyReportAnomalyHintHero: "Grid import was observed during the 16:00-23:00 Export Window.",
    monthlyReportAnomalyEventCount: "{count} signals",
    monthlyReportAnomalyNoData: "No data",
    monthlyReportAnomalyDetailTitle: "Daily detail",
    monthlyReportAnomalyDetailNoEvents: "No anomaly signals for this day.",
    monthlyReportAnomalyDetailCoverage: "Data coverage",
    monthlyReportAnomalyDetailSamples: "Samples",
    monthlyReportAnomalyDetailHeroWindows: "Export-window import windows",
    monthlyReportAnomalyDetailHeroEnergy: "Export-window import energy",
    monthlyReportNoItems: "No items for this section.",
    monthlyReportConfidence: "Confidence {value}",
    monthlyReportSeverityHigh: "High",
    monthlyReportSeverityMedium: "Medium",
    monthlyReportSeverityLow: "Low",
    monthlyReportSeverityInfo: "Info",
    monthlyReportLegend: "Load / Solar / Import / Export",
    samplingSystemLabel: "System",
    samplingSystemOverall: "Overall",
    samplingRangeModeLabel: "Range",
    samplingRangeDay: "Day",
    samplingRangeWeek: "Week",
    samplingRangeMonth: "Month",
    samplingRangeTodayBadge: "Today",
    samplingRangeThisWeekBadge: "This week",
    samplingRangeThisMonthBadge: "This month",
    samplingRangeStartMini: "Start",
    samplingRangeEndMini: "End",
    samplingRangeDaysMini: "Days",
    samplingRangeRelative: "Relative",
    samplingRangeCustomDate: "Custom Date",
    samplingRangeCustomDateTime: "Custom Date + Time",
    samplingWeekDisplay: "Week {week} ({start} ~ {end})",
    samplingMonthYear: "{year}",
    samplingMonthDisplay: "{month} {year}",
    samplingDayLabel: "Day",
    samplingWeekLabel: "Week",
    samplingMonthLabel: "Month",
    samplingArrowPrev: "Previous",
    samplingArrowNext: "Next",
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
    samplingZeroHeroTitle: "Current 5-Minute Export Window",
    samplingZeroHeroMetaActive: "Live export-window observation from current state",
    samplingZeroHeroMetaInactive: "Appears here during the 16:00-23:00 export window",
    samplingZeroHeroStateEligible: "Zero Import",
    samplingZeroHeroStateImportObserved: "Import Seen",
    samplingZeroHeroStateLiveImport: "Importing",
    samplingZeroHeroStateMonitoring: "Monitoring",
    samplingZeroHeroStateInactive: "Idle",
    samplingZeroHeroFieldWindow: "Window",
    samplingZeroHeroFieldRemaining: "Remaining",
    samplingZeroHeroFieldObservedImport: "Observed Import",
    samplingZeroHeroFieldObservedExport: "Observed Export",
    samplingZeroHeroFieldLiveImport: "Live Import",
    samplingZeroHeroFieldLiveExport: "Live Export",
    samplingZeroHeroFieldCoverage: "Coverage",
    samplingZeroHeroFieldSolplanetRecommendation: "Solplanet Plan",
    samplingZeroHeroFieldSolplanetPhase: "Sell Phase",
    samplingZeroHeroFieldDailyExport: "Daily Export",
    samplingZeroHeroFieldUsableBattery: "Usable Battery",
    samplingZeroHeroFieldActiveReserve: "Active Reserve",
    samplingZeroHeroFieldReserve: "Reserve Margin",
    solplanetExportMonitorPhasePrePeak: "16:00-18:00, hold for home load",
    solplanetExportMonitorPhasePeakHold: "18:00-21:00, sell above SolPlanet reserve",
    solplanetExportMonitorPhaseLateTarget: "21:00-23:00, sell above SolPlanet reserve",
    solplanetExportMonitorPhaseOutside: "Outside export window",
    solplanetExportMonitorRecommendationSell: "Sell at {power}",
    solplanetExportMonitorRecommendationSellSurplus: "Sell surplus at {power}",
    solplanetExportMonitorRecommendationStop: "Stop selling",
    solplanetExportMonitorRecommendationHold: "Hold",
    solplanetExportMonitorRecommendationHoldUntilLate: "Hold",
    solplanetExportMonitorRecommendationObserve: "Observe only",
    samplingZeroHeroAwaiting: "No live export-window observation yet.",
    samplingZeroHeroCoverageText: "{samples}/{expected} samples · {coverage}%",
    samplingZeroHeroHistoryTitle: "16:00-23:00 Export Window Review",
    samplingZeroHeroHistoryBadgeImport: "{count} import windows · {netCount} net import",
    samplingZeroHeroHistoryBadgeMixed: "{count} import windows · net export",
    samplingZeroHeroHistoryBadgeClean: "Export only",
    samplingZeroHeroHistoryBadgeNoData: "{count} no data",
    samplingZeroHeroHistoryMetaText: "{day} · {start}-{end} · {count} windows",
    samplingZeroHeroHistoryAwaiting: "No 16:00-23:00 window history for the selected day yet.",
    samplingZeroHeroHistoryStateImport: "Net Import",
    samplingZeroHeroHistoryStateMixedExport: "Import Seen / Net Export",
    samplingZeroHeroHistoryStateExport: "Pure Export",
    samplingZeroHeroHistoryStateIdle: "Idle",
    samplingZeroHeroHistoryStateNoData: "No data",
    samplingZeroHeroHistoryStatePending: "Pending",
    samplingZeroHeroHistoryToday: "Today",
    samplingZeroHeroHistoryDaySummary:
      "Grid import {importCount} · Import with net export {mixedCount} · Export only {exportCount} · Idle {idleCount} · No data {noDataCount} · Pending {pendingCount} · Partial {partialCount}",
    samplingZeroHeroHistoryDayEnergy: "Buy {import} · Sell {export}",
    samplingZeroHeroHistoryEnergyText: "Buy {import} · Sell {export}",
    samplingZeroHeroHistoryCoverageText: "{samples}/{expected} samples",
    samplingZeroHeroHistoryCoveragePartial: "Partial coverage",
    samplingZeroHeroHistoryInProgress: "In progress",
    samplingChartTitle: "Power Timeline",
    samplingChartMeta: "{system} · {range} · {count} points",
    samplingChartNoData: "No chart data yet for this range",
    samplingLegendGroupSolar: "Solar",
    samplingLegendGroupGrid: "Grid",
    samplingLegendGroupLoad: "Home Load",
    samplingLegendGroupBattery: "Battery",
    samplingLegendGroupSajBattery: "SAJ Battery",
    samplingLegendGroupSolplanetBattery: "Solplanet Battery",
    samplingLegendSeriesGeneration: "Generation",
    samplingLegendSeriesImport: "Import",
    samplingLegendSeriesExport: "Export",
    samplingLegendSeriesOther: "Other",
    samplingLegendSeriesCharge: "Charge",
    samplingLegendSeriesDischarge: "Discharge",
    samplingBatteryTimelineTitle: "Battery timeline",
    samplingBatteryTimelineMeta: "{range} · SAJ + Solplanet SOC and usable battery remaining · {count} points",
    samplingBatteryTimelineNoData: "No battery timeline data yet for this range",
    samplingPumpStatusTitle: "Pool Pump Status",
    samplingPumpStatusMeta: "{range} · Pool pump on/off · {count} points",
    samplingPumpStatusCurrentRange: "Current range",
    samplingPumpStatusRecent48h: "Last 48h",
    samplingPumpStatusStarts: "Starts",
    samplingPumpStatusLongestRun: "Longest run",
    samplingPumpStatusDayNightDetail: "Day {dayHours}h · Night {nightHours}h",
    samplingPumpStatusDutyDetail: "{duty}% on time",
    samplingPumpStatusStartsDetail: "{range} · observed transitions",
    samplingPumpStatusLongestDetail: "{range}",
    samplingPumpStatusNoData: "No pool pump status data yet for this range",
    samplingPumpStatusOff: "Off",
    samplingPumpStatusOn: "On",
    samplingBatteryTimelineSeriesSaj: "SAJ Battery",
    samplingBatteryTimelineSeriesSolplanet: "Solplanet Battery",
    samplingBatteryTimelineSeriesCombined: "SAJ + Solplanet Batteries",
    samplingBatteryTimelineSeriesTotalRemaining: "Total Battery Remaining",
    samplingBatteryTimelineSeriesTesla: "Tesla Battery",
    samplingTeslaFreeChargeTitle: "Tesla Free Charging",
    samplingTeslaFreeChargeMeta: "{day} · 11:00-15:00 gross SOC charged",
    samplingTeslaFreeChargeNoData: "No Tesla SOC samples for the free window",
    samplingTeslaFreeChargeDetail: "{kwh} · used {used} · net {net} · {startTime}-{endTime}",
    samplingFreeEnergyUseTitle: "Free Grid Electricity",
    samplingFreeEnergyUseMeta: "{day} · 11:00-15:00 estimated split",
    samplingFreeEnergyUseTesla: "Tesla",
    samplingFreeEnergyUseHomeLoad: "Home Load",
    samplingFreeEnergyUseBatteryCharge: "Battery Charging",
    samplingFreeEnergyUseNoData: "No free-window grid import samples",
    samplingTotalsTitle: "Energy Breakdown",
    samplingTotalsMeta: "{system} · {range} · directional energy totals",
    samplingTotalsMetaNoData: "{system} · {range} · not enough samples for totals yet",
    samplingTotalsScopeOverall: "Overall",
    samplingTotalsScopeSystem: "{system}",
    samplingTotalsScopeHomeLoad: "Home Load",
    samplingTotalsScopeSajBattery: "SAJ Battery",
    samplingTotalsScopeSolplanetBattery: "Solplanet Battery",
    samplingTotalsGridTitle: "Grid",
    samplingTotalsNoData: "No totals available for this range",
    samplingTotalsWindowFree: "Free",
    samplingTotalsWindowPaid: "Paid",
    samplingTotalsWindowOther: "Other",
    samplingTotalsWindowTotal: "Total",
    samplingTotalsPointsCompact: "{samples} pts",
    samplingPhaseBatteryTitle: "Stage-End Battery Remaining",
    samplingPhaseBatteryMeta: "{day} · SAJ + Solplanet usable battery at each stage end",
    samplingPhaseBatteryNoData: "No stage-end battery data for this day",
    samplingPhaseBatteryUnavailable: "No data",
    samplingPhaseBatteryEndAt: "End {time}",
    samplingPhaseBatteryDetail: "SAJ {saj} · Solplanet {solplanet}",
    samplingOverallMetricPv: "PV Generation",
    samplingOverallMetricGridImport: "Grid Import",
    samplingOverallMetricGridExport: "Grid Export",
    samplingOverallMetricHomeLoadTesla: "Tesla",
    samplingOverallMetricHomeLoadPoolPump: "Pool Pump",
    samplingOverallMetricHomeLoadOther: "Other Home Load",
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
    databaseExportBtn: "Export Database",
    databaseImportBtn: "Import Database",
    databaseBackupNowBtn: "Back Up Now",
    databaseExporting: "Exporting...",
    databaseImporting: "Importing...",
    databaseBackupRunning: "Backing up...",
    databaseImportConfirmReplace: "Import will replace the current database file. Continue?",
    databaseImportDone: "Database import completed",
    databaseBackupDone: "Database backup completed",
    databaseBackupSkipped: "Backup already exists for today",
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
    operationTimelineTitle: "Operation Timeline",
    operationTimelineTableTime: "Time",
    operationTimelineTableAction: "Operation",
    operationTimelineTableReason: "Reason",
    operationTimelineCount: "Total {total} operations",
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
    databaseExportFailed: "Database export failed: {error}",
    databaseImportFailed: "Database import failed: {error}",
    databaseBackupFailed: "Database backup failed: {error}",
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
    flowMetaRefreshing: "Refreshing in background · {updated}",
    flowMetaDoneOk: "{updated}",
    flowMetaDonePartial: "Partial ({count}/6) · {updated}",
    flowMetaDoneStale: "Cached ({count}/6) · {updated}",
    flowMetaDoneStaleCombined: "Stale snapshot, collector may be stuck ({count}/6) · {updated}",
    flowMetaIssueReason: "Reason: {reason}",
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
    siteTimeLabel: "电站时间",
    languageLabel: "语言",
    autoRefreshLabel: "自动刷新",
    autoRefreshOff: "关闭",
    autoRefresh30s: "每 30 秒",
    autoRefresh60s: "每 60 秒",
    configBtn: "配置",
    configCloseBtn: "关闭",
    configSaveBtn: "保存配置",
    configAutoDiscoverBtn: "自动获取 Inverter / Battery SN",
    configModalTitle: "首次配置",
    configModalDesc: "请先填写 Home Assistant 连接信息，保存后立即生效。",
    configHaUrlLabel: "HA URL",
    configHaTokenLabel: "HA Token",
    configDongleHostLabel: "Solplanet Dongle Host",
    configInverterSnLabel: "Solplanet Inverter SN",
    configBatterySnLabel: "Solplanet Battery SN",
    configSajSampleIntervalLabel: "SAJ 采样频率",
    configSolplanetSampleIntervalLabel: "Solplanet 采样频率",
    configDetailRetentionLabel: "详细数据保留期",
    retention1Month: "1 个月",
    retention2Months: "2 个月",
    retention3Months: "3 个月",
    retention6Months: "6 个月",
    retention12Months: "12 个月",
    configPruneRetentionBtn: "立即清理过期详细数据",
    configPruneRetentionConfirm: "将删除超过当前保留期的详细采样并压缩数据库，是否继续？",
    configPruningRetention: "正在清理详细数据...",
    configPruneRetentionDone: "详细数据已清理，释放 {size}。",
    configPruneRetentionFailed: "详细数据清理失败: {error}",
    configWeatherLatLabel: "天气纬度",
    configWeatherLonLabel: "天气经度",
    configExportReserveFloorLabel: "Solplanet 卖电保留线",
    configWeatherLatPlaceholder: "例如 -33.8688（悉尼）",
    configWeatherLonPlaceholder: "例如 151.2093（悉尼）",
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
    configAutoDiscovering: "自动获取中...",
    configAutoDiscovered: "逆变器和电池 SN 已写入",
    configAutoDiscoverFailed: "自动获取失败: {error}",
    configSaved: "保存成功",
    configSaveFailed: "保存失败: {error}",
    configMustSaveFirst: "当前配置还不完整，请先完成配置",
    dashboardReserveFloorLabel: "卖电保留线",
    dashboardReserveFloorInputLabel: "保留",
    dashboardReserveFloorSaveBtn: "保存",
    dashboardReserveFloorDisplay: "后期卖电前保留 {value}",
    dashboardReserveFloorSaving: "保存中...",
    dashboardReserveFloorSaved: "已保存",
    dashboardReserveFloorSaveFailed: "保存失败: {error}",
    weatherTitle: "太阳能天气预报",
    weatherSolarLabel: "日照",
    weatherYesterday: "昨天",
    weatherToday: "今天",
    weatherTomorrow: "明天",
    weatherSunHours: "☀ 日照 {h}h",
    weatherTempRange: "{min}°C – {max}°C",
    weatherSolarPct: "{pct}%",
    weatherLoadError: "天气获取失败: {error}",
    refreshBtn: "刷新",
    dashboardTab: "总览",
    billsTab: "账单",
    planTab: "电价",
    planTitle: "电价计划",
    planMatrixTitle: "电价时间表",
    planMatrixHint: "底色按买入价变化：绿色更低，橙色更高。",
    planMatrixType: "时间段",
    planChangeTitle: "费率变化",
    planChangeHint: "按账单整理的当前账户调价记录。",
    planBeforeDiscount: "折前",
    planAfterDiscount: "折后",
    planRecordedAt: "记录 {date} · {distributor} · {postcode}",
    planUpdatedAt: "更新 {date}",
    planAddressNeedsCheck: "门牌号仍需确认。",
    planAddressConfirmed: "账单已确认",
    planNoData: "没有计划数据。",
    planLoadFailed: "计划数据不可用：{error}",
    planNotListed: "未列出",
    planCapKwhPerDay: "{value} kWh/天 cap",
    planRateDaily: "每日供电费",
    planRatePeak: "Peak 用电",
    planRateOffPeakFirst: "免费/Off-peak 时段",
    planRateOffPeakBalance: "Off-peak 超出部分",
    planRateShoulder: "Shoulder 用电",
    planRateControlledLoad: "Controlled load",
    planRateMetering: "Metering charge",
    planRateFitEvening: "Solar feed-in 4pm-11pm",
    planRateFitOther: "Solar feed-in 11pm-4pm",
    planRateSuperExportTopUp: "Super Export top-up",
    planRateZeroHeroCredit: "ZEROHERO credit",
    planNoRateChanges: "暂无费率变化记录。",
    planChangeSource: "来源：{source}",
    planChangeFromTo: "{from} -> {to}",
    planSummaryRetailer: "零售商",
    planSummaryDistributor: "供电区",
    planSummaryPostcode: "邮编",
    planSummaryStatus: "状态",
    planTimetableFixed: "固定 / 非时段",
    planTimetableBuy: "买入",
    planTimetableSell: "卖出",
    planTimetableSpecial: "特殊",
    planTimetableNone: "无",
    planTimetableFreeUsage: "免费用电",
    planTimetableDailySupply: "每日供电费",
    planTimetableMetering: "Metering",
    planTimetableControlledLoad: "Controlled load",
    planTimetableSuperExport: "Super Export",
    planTimetableTotalExport: "合计 {value}",
    planTimetableZeroHero: "ZEROHERO 返现",
    planTimetableAfterCap: "超出 cap 后 {value}",
    reportDailyCostOverCap: "超出 cap",
    notificationMatrixTab: "Time Window",
    notificationMatrixTitle: "Time Window",
    notificationMatrixIntro: "每个面板会按 Operation 和 Notification 分组展示规则，这样你可以更快区分自动执行的动作和提醒通知。Tesla 和水泵的自动控制都只在 Priority Scores 里统一管理，不在各个 time window 规则里单独修改。",
    notificationMatrixPriorityTitle: "优先级分数",
    notificationMatrixPrioritySummary: "Tesla 改成非线性 SOC 分数：低于 80% 后上涨会越来越快，低于 50% 后会很快变高。水泵只看白天/晚上运行小时数乘权重。",
    notificationMatrixPriorityWinner: "当前优先者：{winner}",
    notificationMatrixPriorityWinnerNone: "无自动控制",
    notificationMatrixPriorityBusinessDay: "业务日：{value}",
    notificationMatrixPriorityFormula: "公式",
    notificationMatrixPriorityTeslaLabel: "Tesla",
    notificationMatrixPriorityPumpLabel: "水泵",
    notificationMatrixPrioritySoc: "SOC",
    notificationMatrixPriorityRollingRuntime: "24小时运行时长",
    notificationMatrixPriorityModel: "优先级模型",
    notificationMatrixPriorityWeightedRuntime: "优先级小时数",
    notificationMatrixPriorityRawRuntime: "原始运行时长",
    notificationMatrixPriorityDayRuntime: "白天运行时长",
    notificationMatrixPriorityNightRuntime: "晚上运行时长",
    notificationMatrixPriorityWeightDay: "白天",
    notificationMatrixPriorityWeightNight: "晚上",
    notificationMatrixPriorityDeficit: "缺口",
    notificationMatrixPriorityCurrentWindow: "今天窗口",
    notificationMatrixPriorityPreviousWindow: "昨天窗口",
    notificationMatrixPriorityWeights: "昼夜权重",
    notificationMatrixPriorityOverrideLabel: "最高优先级覆盖",
    notificationMatrixPriorityOverrideAuto: "自动",
    notificationMatrixPriorityOverrideTesla: "Tesla",
    notificationMatrixPriorityOverridePump: "水泵",
    notificationMatrixPriorityOverriddenScore: "{score}（原始 {original}）",
    notificationMatrixPriorityTeslaAutoControlLabel: "Tesla 自动控制",
    notificationMatrixPriorityTeslaAutoControlHint: "勾选后 worker 可以自动开始或停止 Tesla 充电；取消勾选后 Tesla 完全改为手动控制。",
    notificationMatrixPriorityTeslaAutoControlEnabled: "允许自动控制",
    notificationMatrixPriorityTeslaAutoControlDisabled: "仅手动",
    notificationMatrixPriorityTeslaForceThresholdLabel: "强制充电阈值",
    notificationMatrixPriorityTeslaForceThresholdHint: "当 Tesla SOC 低于这个百分比时，夜间控制会跳过共享余量计算，直接按最大电流充电。",
    notificationMatrixPriorityTeslaForceThresholdInvalid: "请输入 0 到 100 之间的整数阈值。",
    notificationMatrixPriorityPumpAutoControlLabel: "水泵自动控制",
    notificationMatrixPriorityPumpAutoControlHint: "勾选后 worker 可以自动开关水泵；取消勾选后水泵完全改为手动控制。",
    notificationMatrixPriorityPumpAutoControlEnabled: "允许自动控制",
    notificationMatrixPriorityPumpAutoControlDisabled: "仅手动",
    notificationMatrixPriorityPumpForceRuntimeLabel: "强制运行阈值",
    notificationMatrixPriorityPumpForceRuntimeHint: "当水泵过去24小时运行低于这个小时数时，夜间控制会先强制运行，再进入共享余量分配。",
    notificationMatrixPriorityPumpForceRuntimeInvalid: "请输入 0 到 {maxHours} 小时之间的有效阈值。",
    notificationMatrixLoadingTitle: "正在加载 Time Window",
    notificationMatrixLoadingBody: "正在刷新规则、优先级分数和当前窗口状态...",
    notificationMatrixPriorityExpand: "展开优先级",
    notificationMatrixPriorityCollapse: "收起优先级",
    notificationMatrixNoNotifications: "这个时间窗口当前没有配置规则。",
    notificationMatrixAlwaysNote: "这个监控始终开启，单独展示，不属于 24 小时时间窗口切分的一部分。",
    notificationMatrixCurrentWindowBadge: "当前窗口",
    notificationMatrixExpandWindow: "展开窗口",
    notificationMatrixCollapseWindow: "折叠窗口",
    notificationMatrixOperationConditionsTitle: "条件",
    notificationMatrixOperationConditionsMatched: "已匹配 {matched}/{total}",
    notificationMatrixConditionMatched: "已匹配",
    notificationMatrixConditionNotMatched: "未匹配",
    notificationMatrixConditionCurrentWindow: "当前在这个时间窗口内",
    notificationMatrixConditionTeslaConnected: "Tesla 已连接",
    notificationMatrixConditionTeslaReady: "Tesla 状态可控制",
    notificationMatrixConditionTeslaGridCap: "预测电网功率不超过 15.0kW 上限",
    notificationMatrixConditionTeslaSocStart: "Solplanet SOC 达到 95% 启动阈值",
    notificationMatrixConditionTeslaSocKeep: "Solplanet SOC 达到 90% 维持阈值",
    notificationMatrixConditionTeslaOvernightReserve: "Solplanet SOC 高于 10% 夜间保底",
    notificationMatrixConditionTeslaOvernightBudget: "特斯拉可用电量",
    notificationMatrixConditionTeslaOvernightBatteryTotal: "两块电池总可用",
    notificationMatrixConditionTeslaOvernightHomeLoad: "家庭负载到早上11点",
    notificationMatrixConditionTeslaOvernightPumpLoad: "水泵到早上5点",
    notificationMatrixConditionTeslaOvernightChargeState: "充电状态",
    notificationMatrixConditionTeslaOvernightLastAction: "上次调节",
    notificationMatrixConditionPoolPumpGridCap: "开启水泵后预测电网功率仍不超过 15.0kW",
    notificationMatrixConditionPoolPumpTeslaPriority: "Tesla 优先级条件已满足",
    notificationMatrixConditionPoolPumpSchedule: "水泵运行时段",
    notificationMatrixConditionPoolPumpRuntimeBudget: "可用运行时长",
    notificationMatrixConditionValueCurrentNeed: "{current}（需 {need}）",
    notificationMatrixConditionSajProfileApplied: "SAJ 目标模式已生效",
    notificationMatrixConditionSajSyncCleared: "远程同步已完成",
    notificationMatrixProgressWatching: "监控中",
    notificationMatrixProgressConditionMet: "当前已满足条件",
    notificationMatrixProgressNotified: "已通知",
    notificationMatrixProgressNoData: "等待实时数据",
    notificationMatrixProgressScheduled: "该窗口规则已排队",
    notificationMatrixProgressApplied: "本轮已执行",
    notificationMatrixProgressHolding: "维持目标状态中",
    notificationMatrixProgressPendingSync: "等待设备回读",
    notificationMatrixProgressDisabled: "已禁用",
    notificationMatrixProgressSaving: "保存中",
    notificationMatrixProgressWindowTracking: "只在这个窗口内跟踪",
    notificationMatrixProgressCurrent: "当前 {value}",
    notificationMatrixProgressTarget: "目标 {value}",
    notificationMatrixProgressTriggerAtMost: "触发条件 <= {value}",
    notificationMatrixProgressTriggerAtLeast: "触发条件 >= {value}",
    notificationMatrixProgressTriggerAbove: "触发条件 > {value}",
    notificationMatrixProgressRemaining: "还差 {value}",
    notificationMatrixProgressAboveThreshold: "高于阈值 {value}",
    notificationMatrixProgressBelowThreshold: "低于阈值 {value}",
    notificationMatrixRateFree: "买电 $0/kWh · 卖电 0c/kWh",
    notificationMatrixRateShoulder: "买电 37.5c/kWh · 卖电 0c/kWh",
    notificationMatrixRateExport: "买电 57.8c/kWh · 卖电 8c/kWh",
    notificationMatrixWindowFreeEnergyTitle: "免费时段",
    notificationMatrixWindowFreeEnergySummary: "11:00-15:00。FOUR4FREE 买电 $0/kWh，卖电 0c/kWh；worker 会先处理 Tesla 充电，再在预测电网功率不超过 15.0kW 时决定是否打开水泵。",
    notificationMatrixWindowAfterFreeShoulderTitle: "备电时段",
    notificationMatrixWindowAfterFreeShoulderSummary: "15:00-16:00。买电 37.5c/kWh，卖电 0c/kWh；这是免费时段和 8c/kWh 外送时段之间的特殊 1 小时。",
    notificationMatrixWindowExportTitle: "外送时段",
    notificationMatrixWindowExportSummary: "16:00-23:00。买电 57.8c/kWh，卖电 8c/kWh；当 Tesla 低于强制阈值时，只要可用电量仍高于配置的外送保留线，worker 就会让它充电。",
    notificationMatrixWindowOvernightTitle: "夜间预算时段",
    notificationMatrixWindowOvernightSummary: "23:00-次日11:00。买电 37.5c/kWh，卖电 0c/kWh；worker 会按夜间电池预算运行 Tesla 充电和水泵。",
    notificationMatrixRuleTypeNotification: "通知",
    notificationMatrixRuleTypeOperation: "操作",
    notificationRuleSajProfileFreeEnergyTitle: "免费时段 SAJ inverter 切到 Time of Use",
    notificationRuleSajProfileFreeEnergyTrigger: "11:00-14:50 目标 profile 为 Time of Use，最后 10 分钟切回 Self Consumption。",
    notificationRuleSajProfileSelfConsumptionTitle: "SAJ inverter 保持 Self Consumption",
    notificationRuleSajProfileSelfConsumptionTrigger: "免费时段以外，SAJ 目标 profile 保持在 Self Consumption。",
    notificationRuleTeslaFreeEnergyTitle: "Tesla 充电电流跟随免费时段规则",
    notificationRuleTeslaFreeEnergyTrigger: "11:00-15:00 期间，worker 会在配置的电网功率上限内调节 Tesla 充电电流。",
    notificationRulePoolPumpFreeEnergyTitle: "免费时段水泵跟随 Tesla 优先级规则",
    notificationRulePoolPumpFreeEnergyTrigger: "11:00-15:00 期间，只有在 Tesla 不需要先启动，或 Tesla 已经在充电时，且预测电网功率仍不超过 15.0kW，才会打开水泵。",
    notificationRuleTeslaAfterFreeShoulderTitle: "备电时段 Tesla 跟随 Solplanet SOC 规则",
    notificationRuleTeslaAfterFreeShoulderTrigger: "Solplanet SOC 到 95% 开始充，低于 90% 停止，区间内维持当前状态。",
    notificationRuleTeslaOvernightShoulderTitle: "夜间预算时段 Tesla 跟随电池预算规则",
    notificationRuleTeslaOvernightShoulderTrigger: "23:00-次日11:00 期间，当两块电池总可用电量扣除到早上11点的家庭负载和到早上5点的水泵用电后仍超过 1.0 kWh 时允许 Tesla 充电；预算耗尽时停止。",
    notificationRulePoolPumpOvernightTitle: "夜间预算时段按电池预算运行水泵",
    notificationRulePoolPumpOvernightTrigger: "23:00-05:00 期间，根据夜间电池预测算出水泵可运行时长，再只运行其中的 80%。",
    notificationRuleTeslaExportWindowTitle: "外送时段 Tesla 按强制阈值充电",
    notificationRuleTeslaExportWindowTrigger: "Tesla SOC 低于强制阈值时，只要可用电量高于配置的外送保留线就充电；到达或低于保留线则停止。",
    notificationMatrixTeslaTriggerGridCap: "目标保持电网 <= 15.0kW",
    notificationMatrixTeslaTriggerSolplanetSoc: "Solplanet {soc}（达到 95% 开始，>= 90% 维持）",
    notificationMatrixTeslaTriggerSolplanetReserve: "Solplanet {soc}（保持 > 10%）",
    notificationMatrixTeslaTriggerOvernightBudget: "{battery} − {home}（家 {homeKw}×{homeH}）− {pump}（泵 {pumpKw}×{pumpH}）= {available}",
    notificationMatrixTeslaTriggerExport: "当前外送 {value} / 太阳能富余",
    notificationMatrixTeslaTriggerStop: "目标为停止充电",
    notificationMatrixTeslaCurrent: "Tesla 实际 {actual} / 设定 {configured}",
    notificationRuleGridImportStartedTitle: "外送时段开始从电网取电",
    notificationRuleGridImportStartedTrigger: "当外送时段内电网状态从未取电切换到开始取电时触发一次。",
    notificationRuleGridImportStartedPostExportPeakTitle: "高价守护时段开始从电网取电",
    notificationRuleGridImportStartedPostExportPeakTrigger: "当高价守护时段内电网状态从未取电切换到开始取电时触发一次。",
    rawDataTab: "Raw Data",
    rawDataTitle: "Raw Data",
    rawDataSystemLabel: "系统",
    rawDataSystemSolplanet: "Solplanet",
    rawDataSystemSaj: "SAJ",
    sajControlTab: "SAJ 管理",
    solplanetControlTab: "Solplanet 管理",
    entitiesTab: "实体",
    samplingTab: "历史",
    reportTab: "日报",
    databaseTab: "Database",
    workerLogsTab: "Worker日志",
    operationTimelineTab: "操作记录",
    failureLogTab: "失败日志",
    databaseTitle: "数据库浏览",
    databaseTableLabel: "数据表",
    databaseTablePlaceholder: "选择一个数据表",
    databaseMeta: "表 {table} · {columns} 列",
    databaseTotal: "共 {total} 行",
    databasePageInfo: "第 {page}/{totalPages} 页（当前 {count} 行）",
    databaseEmpty: "请选择一个数据表查看数据。",
    databaseTransferExportTitle: "正在导出数据库",
    databaseTransferImportTitle: "正在导入数据库",
    databaseTransferPreparing: "准备中...",
    databaseTransferProcessing: "处理中...",
    databaseTransferPercent: "{value}%",
    databaseTransferDownloaded: "已下载 {loaded} / {total}",
    databaseTransferUploaded: "已上传 {loaded} / {total}",
    databaseTransferExportStarting: "正在准备导出文件...",
    databaseTransferExportStreaming: "正在下载数据库文件...",
    databaseTransferExportFinishing: "正在完成文件保存...",
    databaseTransferExportDone: "导出完成，浏览器应很快开始下载。",
    databaseTransferImportUploading: "正在上传数据库文件...",
    databaseTransferImportApplying: "正在替换本地数据库，请稍候...",
    databaseTransferImportRefreshing: "正在刷新页面数据...",
    databaseTransferImportDone: "导入完成。",
    databaseTransferBackupTitle: "正在备份数据库",
    databaseTransferBackupRunning: "正在复制一致性的 SQLite 快照...",
    databaseTransferBackupDone: "备份完成。",
    databaseTransferFailed: "失败",
    databaseBackupMeta: "备份 {count}/{retention} · 最近 {latest}（{size}）· 总计 {totalSize} · {directory}",
    databaseBackupMetaEmpty: "还没有备份 · 只保留最近 {retention} 个 · {directory}",
    databaseBackupMetaFailed: "备份状态加载失败：{error}",
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
    solplanetDashboardPinModalTitle: "Solplanet 买卖电计划",
    solplanetDashboardPinModalIntro: "调整 Solplanet 买电/卖电功率和时段。",
    solplanetDashboardPinCurrentLabel: "当前 Pin (W)",
    solplanetDashboardPinTargetLabel: "目标 Pin (W)",
    solplanetDashboardBuyTitle: "买电 / 充电",
    solplanetDashboardBuyPowerLabel: "买电功率 Pin (W)",
    solplanetDashboardBuyStartLabel: "买电开始",
    solplanetDashboardBuyEndLabel: "买电结束",
    solplanetDashboardSellTitle: "卖电 / 放电",
    solplanetDashboardSellPowerLabel: "卖电功率 Pout (W)",
    solplanetDashboardSellStartLabel: "卖电开始",
    solplanetDashboardSellEndLabel: "卖电结束",
    solplanetDashboardPinApplyBtn: "应用计划",
    solplanetDashboardPinLoading: "正在读取 Solplanet 当前控制状态...",
    solplanetDashboardPinReady: "已加载当前 Solplanet 计划。",
    solplanetDashboardPinApplying: "正在应用 Solplanet 计划...",
    solplanetDashboardPinApplied: "Solplanet 计划已更新: Pin {pin}W, Pout {pout}W。",
    solplanetDashboardPinOpenHint: "点击 Solplanet inverter 节点可调整买卖电功率和时段。",
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
    sajTouDirectionSell: "卖电",
    sajTouDirectionBuy: "买电",
    sajTouPowerLabel: "功率 (W)",
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
    dashboardUsableBatteryTitle: "两电池可用总电量",
    dashboardNotificationsTitle: "通知",
    dashboardNotificationStateNotified: "已通知",
    dashboardNotificationStateDismissed: "已忽略",
    dashboardNotificationDismiss: "忽略",
    dashboardNotificationLevelAlarm: "告警",
    dashboardNotificationLevelWarning: "提醒",
    dashboardNotificationLevelInfo: "信息",
    dashboardNotificationMeta: "窗口 {window} · 条件 {trigger} · {time}",
    integratedFlowSubtitle: "SAJ 的 solar/grid + SAJ/Solplanet 电池，双逆变器并联",
    poolPumpKicker: "家庭负载设备",
    poolPumpTitle: "泳池水泵",
    poolPumpMeta: "挂在 Home Load 下方显示，不改变综合负载拆分。",
    poolPumpPowerLabel: "当前功率",
    poolPumpStateLabel: "状态",
    poolPumpStateOn: "运行中",
    poolPumpStateOff: "已停止",
    poolPumpStateUnknown: "状态未知",
    poolPumpStateUnavailable: "不可用",
    poolPumpActionTurnOn: "打开水泵",
    poolPumpActionTurnOff: "关闭水泵",
    poolPumpActionBusy: "正在更新...",
    poolPumpControlFailed: "泳池水泵控制失败：{error}",
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
    mobileFlowHomeTitle: "家庭",
    mobileFlowSajInverterShort: "SAJ 逆变器",
    mobileFlowSolplanetInverterShort: "Solplanet 逆变器",
    mobileFlowSajBatteryShort: "SAJ 电池",
    mobileFlowSolplanetBatteryShort: "Solplanet 电池",
    mobileFlowInvertersCombinedShort: "双逆变器",
    mobileFlowBatteriesCombinedShort: "电池组",
    mobileDashboardFocusTitle: "手机重点面板",
    mobileDashboardFocusHint: "手机端先聚焦工作功率、电量余量和实时流向。",
    mobileDashboardLoadPower: "工作功率",
    mobileDashboardUsableBattery: "电量余量",
    mobileDashboardNext8: "到 08:00",
    mobileDashboardNext11: "到 11:00",
    mobileDashboardDetailsTitle: "更多详情",
    mobileDashboardFlowSummary: "流向摘要",
    mobileDashboardFlowDirectionIdle: "空闲",
    mobileDashboardFlowDirectionImport: "购电中",
    mobileDashboardFlowDirectionExport: "回馈中",
    mobileDashboardFlowDirectionCharge: "充电中",
    mobileDashboardFlowDirectionDischarge: "放电中",
    mobileDashboardFlowDirectionActive: "运行中",
    mobileDashboardFlowDirectionOffline: "离线",
    mobileDashboardFlowGrid: "电网",
    mobileDashboardFlowSaj: "SAJ 路径",
    mobileDashboardFlowSolplanet: "Solplanet 路径",
    mobileDashboardFlowLoad: "家庭负载",
    mobileDashboardFlowTesla: "Tesla",
    mobileDashboardFlowPoolPump: "泳池水泵",
    mobileDashboardFlowBatterySaj: "SAJ 电池",
    mobileDashboardFlowBatterySolplanet: "Solplanet 电池",
    mobileDashboardFlowExtra: "补充信息",
    mobileDashboardHubTitle: "Hub",
    mobileDashboardFreshnessLoading: "正在读取最新数据",
    mobileDashboardFreshnessRefreshing: "正在刷新",
    mobileDashboardFreshnessFresh: "数据已更新",
    mobileDashboardFreshnessStale: "数据已超时",
    mobileDashboardFreshnessFailed: "刷新失败",
    mobileDashboardFreshnessReasonPrefix: "原因：{reason}",
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
    teslaLocationHome: "位置：home",
    teslaLocationNonHome: "位置：非 home",
    teslaLocationUnknown: "位置未知",
    teslaControlStart: "开始充电",
    teslaControlStop: "停止充电",
    teslaControlStarting: "正在开始...",
    teslaControlStopping: "正在停止...",
    teslaControlBusy: "正在更新...",
    teslaControlUnavailable: "控制不可用",
    teslaControlApplyFailed: "特斯拉充电控制失败：{error}",
    teslaCurrentApplyFailed: "特斯拉电流设置失败：{error}",
    teslaCurrentLabel: "充电电流",
    teslaCurrentApply: "应用",
    teslaCurrentApplying: "正在应用...",
    teslaCurrentModalIntro: "修改 Tesla 充电电流。",
    teslaCurrentModalStatusReady: "当前设定值 {value}",
    teslaControlStatusRequestStart: "正在发送开始充电请求...",
    teslaControlStatusRequestStop: "正在发送停止充电请求...",
    teslaControlStatusAwaitingStart: "已发出开始请求，等待特斯拉响应。",
    teslaControlStatusAwaitingStop: "已发出停止请求，等待特斯拉响应。",
    teslaControlStatusDelayedStart: "开始请求已发出，但车辆还没有开始充电。",
    teslaControlStatusDelayedStop: "停止请求已发出，但车辆还没有停止充电。",
    teslaControlStatusFailedStart: "开始充电请求没有成功发出。",
    teslaControlStatusFailedStop: "停止充电请求没有成功发出。",
    teslaControlStatusStarted: "已开始充电。",
    teslaControlStatusStopped: "已停止充电。",
    teslaBatteryEnergyUnavailable: "- / - kWh",
    teslaChargeEtaUnavailable: "预计充满 -",
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
    batteryWindowForecastFreeLabel: "免费充电",
    batteryWindowForecastDischargeLabel: "放电",
    batteryWindowForecastCompact: "{window}结束：{soc}",
    batteryWindowForecastCompactNoData: "{window}结束：-",
    switchboardStateActive: "母线工作中",
    switchboardStateIdle: "母线空闲",
    socLabel: "电池电量",
    entityExplorerTitle: "实体浏览",
    samplingTitle: "采样记录",
    reportTitle: "每日日报",
    reportGeneratedMeta: "{day} 的已保存日报 · 生成于 {time}",
    reportNoDataMeta: "{day} 的日报无数据 · 生成于 {time}",
    reportGeneratingMeta: "正在生成 {day} 的每日日报...",
    reportLoadingMeta: "正在加载 {day} 的每日日报...",
    reportLoadingModalTitle: "正在加载每日日报",
    reportLoadingModalBody: "正在准备已保存的日报数据...",
    reportUnavailable: "{day} 暂无已保存日报",
    reportNotReady: "未来日期暂时不能生成日报。",
    reportGenerateFailed: "{day} 的每日日报生成失败：{error}",
    reportLatestBadge: "最新",
    reportDailyCostTitle: "当日电费",
    reportDailyCostMeta: "{plan} · {basis} · {samples} 个样本",
    reportDailyCostNoData: "此日报暂无法估算电费。",
    reportDailyCostEstimated: "系统估算",
    reportDailyCostBillActual: "用量实际",
    reportDailyCostSystemNoDailyCap: "系统不按日 cap",
    reportDailyCostSystemDailyCap: "系统按日 cap",
    reportDailyCostUsageNoDailyCap: "用量不按日 cap",
    reportDailyCostUsageDailyCap: "用量按日 cap",
    reportDailyCostBillAllocated: "已计算",
    reportDailyCostNoBill: "这一天还没有上传用量",
    reportDailyCostDelta: "差额",
    reportDailyCostFixed: "固定费",
    reportDailyCostImport: "买电",
    reportDailyCostCredits: "抵扣",
    reportDailyCostZeroHero: "ZEROHERO",
    reportDailyCostBreakdown: "明细",
    reportDailyCostComponent: "项目",
    reportDailyCostFormula: "公式",
    reportDailyCostBill: "实际",
    reportDailyCostEstimateMinusBill: "估算 - 实际",
    reportDailyCostHigher: "估算偏高",
    reportDailyCostLower: "估算偏低",
    reportDailyCostEven: "基本一致",
    reportDailyCostFixedDetail: "日供电费 + 计量费",
    reportDailyCostImportDetail: "peak {peak} · shoulder {shoulder} · free {free}",
    reportDailyCostCreditDetail: "feed-in {fit} · super export {superExport}",
    reportDailyCostZeroHeroDetail: "{status} · 红色 {redWindows} · 黄色 {yellowWindows} · 覆盖 {coverage}",
    reportDailyCostQualified: "已达标",
    reportDailyCostNotQualified: "未达标",
    reportDailyCostInsufficient: "数据不足",
    reportDailyCostNotAvailable: "不可用",
    reportUsageChooseBtn: "上传用量",
    reportUsageUploading: "正在上传用量...",
    reportUsageUploaded: "用量 {name} 已上传 · 更新 {days} 条日报",
    reportUsageUploadFailed: "用量上传失败：{error}",
    reportUsageNoFile: "请先选择 CSV 用量文件。",
    reportBillListBtn: "账单",
    reportBillListLoading: "正在加载账单...",
    reportBillListEmpty: "暂无已上传账单",
    reportBillListFailed: "账单列表不可用：{error}",
    billsTitle: "账单",
    billsUploadBtn: "上传账单",
    billsMeta: "已上传 {count} 期账单 · 按账单周期排序",
    billsUpdated: "更新于 {date}",
    billsLoading: "正在加载账单...",
    billsEmpty: "还没有上传账单。",
    billsLoadFailed: "账单不可用：{error}",
    billsUploading: "正在上传账单...",
    billsUploaded: "账单 {invoice} 已上传",
    billsUploadFailed: "账单上传失败：{error}",
    billsNoFile: "请先选择 PDF 账单。",
    billsDropTitle: "松开上传账单文件",
    billsDropHint: "上传前会先让你确认。",
    billsDropConfirmUpload: "要上传账单文件 {name} 吗？",
    billsPeriodSelectLabel: "账单周期",
    billsTotalSpend: "账单合计",
    billsLatestPeriod: "最新周期",
    billsPlanCount: "涉及计划",
    billsPeriod: "周期",
    billsBillingDays: "账单天数",
    billsDaysValue: "{days} 天",
    billsInvoice: "发票",
    billsIssued: "出账日",
    billsDue: "到期日",
    billsAmountDue: "应付金额",
    billsBillTotal: "账单总额",
    billsPay: "需要支付 {amount}",
    billsCreditEarned: "获得抵扣 {amount}",
    billsSettled: "$0.00",
    billsPlanRates: "Plan 和费率",
    billsParsedLines: "解析明细",
    billsDocuments: "原始文档",
    billsOpenPdf: "打开 PDF",
    billsOpenText: "打开提取文本",
    billsRateDaily: "日费",
    billsRatePeak: "Peak",
    billsRateFree: "免费/Offpeak",
    billsRateShoulder: "Shoulder",
    billsRateFitEvening: "傍晚上网",
    billsRateFitOther: "其他上网",
    billsRateSuperExport: "Super Export",
    billsRateZeroHero: "ZEROHERO",
    billsLineDescription: "项目",
    billsLinePeriod: "周期",
    billsLineQuantity: "数量",
    billsLineRate: "单价",
    billsLineTotal: "金额",
    billsAnalysisType: "解析类型",
    billsComparisonTitle: "费用对比",
    billsComparisonBill: "账单",
    billsComparisonUsageDailyCap: "用量按日 cap",
    billsComparisonSystemDailyCap: "系统按日 cap",
    billsComparisonUsageNoDailyCap: "用量不按日 cap",
    billsComparisonSystemNoDailyCap: "系统不按日 cap",
    billsComparisonDifference: "差额",
    billsComparisonFixed: "固定费",
    billsComparisonImport: "买电",
    billsComparisonSolar: "太阳能抵扣",
    billsComparisonSuper: "Super Export",
    billsComparisonZeroHero: "ZEROHERO",
    billsComparisonOther: "其他",
    billsComparisonTotal: "合计",
    billsComparisonDataDays: "用量 {usageDays}/{days} 天 · 系统 {systemDays}/{days} 天",
    billsComparisonKwhTitle: "用量 kWh",
    reportCalendarTitle: "日报日历",
    reportCalendarMeta: "{month} · 已生成日报 {count}/{days}",
    reportCalendarLoading: "正在加载日报索引...",
    reportCalendarSaved: "已保存",
    reportCalendarNoData: "无数据",
    reportCalendarLive: "实时",
    reportCalendarMissing: "生成",
    reportCalendarPending: "未就绪",
    reportCalendarChecking: "检查中",
    reportCalendarGenerating: "生成中",
    reportCalendarBillActual: "账单实付",
    reportCalendarBillActualWithInvoice: "账单实付（{invoice}）",
    reportCalendarPlanFour4Free: "FOUR4FREE",
    reportCalendarPlanZeroHeroLegacy: "旧 ZEROHERO",
    reportTodayLiveMeta: "{day} 的实时日报 · 使用原始数据",
    reportCalendarPrevMonth: "上个月",
    reportCalendarNextMonth: "下个月",
    reportWeekdayMon: "一",
    reportWeekdayTue: "二",
    reportWeekdayWed: "三",
    reportWeekdayThu: "四",
    reportWeekdayFri: "五",
    reportWeekdaySat: "六",
    reportWeekdaySun: "日",
    monthlyReportTab: "月报",
    monthlyReportTitle: "月度报告",
    monthlyReportSubtitle: "{month} · 已统计 {days}/{total} 天 · {mode}",
    monthlyReportModeProjection: "本月至今并预测月底",
    monthlyReportModeComplete: "完整月份",
    monthlyReportModeSaved: "已保存月报",
    monthlyReportModeFuture: "未来月份",
    monthlyReportPrev: "上一年",
    monthlyReportNext: "下一年",
    monthlyReportMonthLabel: "月份",
    monthlyReportPickerTitle: "{year} 年月报",
    monthlyReportPickerLoading: "正在检查月报...",
    monthlyReportPickerSaved: "已保存",
    monthlyReportPickerNoData: "无数据",
    monthlyReportPickerGenerate: "生成",
    monthlyReportPickerLive: "实时",
    monthlyReportPickerPending: "未就绪",
    monthlyReportStatusSaved: "已保存月报 · 从持久化快照快速加载",
    monthlyReportStatusNoData: "已保存月报无数据",
    monthlyReportLoading: "正在加载月度分析...",
    monthlyReportUnavailable: "这个月份暂时没有可分析的数据。",
    monthlyReportGenerated: "生成于 {time}",
    monthlyReportRecommendationsTitle: "优化建议",
    monthlyReportRecommendationsCount: "{count} 条",
    monthlyReportProjectionTitle: "月底估算",
    monthlyReportComparisonTitle: "对比",
    monthlyReportHeroTitle: "外送窗口",
    monthlyReportDailyTrendTitle: "每日趋势",
    monthlyReportDailyTrendYAxis: "电量 (kWh)",
    monthlyReportAnomaliesTitle: "异常项",
    monthlyReportDailyTableTitle: "每日明细",
    monthlyReportTableDay: "日期",
    monthlyReportTableLoad: "负载",
    monthlyReportTableSolar: "光伏",
    monthlyReportTableImport: "购电",
    monthlyReportTableExport: "上网",
    monthlyReportTableHero: "Hero",
    monthlyReportTableAction: "操作",
    monthlyReportOpenDaily: "打开日报",
    monthlyReportKpiLoad: "家庭用电",
    monthlyReportKpiSolar: "光伏发电",
    monthlyReportKpiImport: "电网购电",
    monthlyReportKpiExport: "电网上网",
    monthlyReportKpiNetGrid: "电网净值",
    monthlyReportKpiSelfUse: "光伏自用率",
    monthlyReportKpiGridDependence: "电网依赖度",
    monthlyReportKpiCoverage: "数据覆盖率",
    monthlyReportMetricProjectedLoad: "预计家庭用电",
    monthlyReportMetricProjectedImport: "预计电网购电",
    monthlyReportMetricProjectedExport: "预计电网上网",
    monthlyReportMetricScale: "预测倍率",
    monthlyReportMetricPrevLoadDelta: "用电 vs 上期",
    monthlyReportMetricPrevImportDelta: "购电 vs 上期",
    monthlyReportMetricPrevSolarDelta: "光伏 vs 上期",
    monthlyReportMetricAvgImportDelta: "购电 vs 近 4 周常态",
    monthlyReportMetricHeroImportWindows: "出现购电窗口",
    monthlyReportMetricHeroNetImportWindows: "净购电窗口",
    monthlyReportMetricHeroImportEnergy: "外送窗口购电量",
    monthlyReportMetricHeroNoData: "无数据窗口",
    monthlyReportAnomalyTopImport: "购电最高日期",
    monthlyReportAnomalyTopBaseLoad: "基础负载最高日期",
    monthlyReportAnomalyWeakSolar: "光伏偏弱日期",
    monthlyReportAnomalyHeroIssue: "外送窗口异常日期",
    monthlyReportAnomalyLegendImport: "购电偏高",
    monthlyReportAnomalyLegendBaseLoad: "基础负载偏高",
    monthlyReportAnomalyLegendWeakSolar: "光伏偏弱",
    monthlyReportAnomalyLegendHero: "外送窗口购电",
    monthlyReportAnomalyHintImport: "本月电网购电量最高的前五天之一。",
    monthlyReportAnomalyHintBaseLoad: "排除 Tesla 和泳池泵后，其他家庭负载最高的前五天之一。",
    monthlyReportAnomalyHintWeakSolar: "在数据覆盖率至少 50% 的前提下，光伏发电低于本月中位数的 65%。",
    monthlyReportAnomalyHintHero: "在 16:00-23:00 的外送窗口期间观察到电网购电。",
    monthlyReportAnomalyEventCount: "{count} 项异常",
    monthlyReportAnomalyNoData: "无数据",
    monthlyReportAnomalyDetailTitle: "每日详情",
    monthlyReportAnomalyDetailNoEvents: "这一天没有异常标记。",
    monthlyReportAnomalyDetailCoverage: "数据覆盖率",
    monthlyReportAnomalyDetailSamples: "采样数",
    monthlyReportAnomalyDetailHeroWindows: "外送窗口购电窗口",
    monthlyReportAnomalyDetailHeroEnergy: "外送窗口购电量",
    monthlyReportNoItems: "这个区块暂无条目。",
    monthlyReportConfidence: "可信度 {value}",
    monthlyReportSeverityHigh: "高",
    monthlyReportSeverityMedium: "中",
    monthlyReportSeverityLow: "低",
    monthlyReportSeverityInfo: "信息",
    monthlyReportLegend: "负载 / 光伏 / 购电 / 上网",
    samplingSystemLabel: "系统",
    samplingSystemOverall: "综合",
    samplingRangeModeLabel: "范围",
    samplingRangeDay: "日",
    samplingRangeWeek: "周",
    samplingRangeMonth: "月",
    samplingRangeTodayBadge: "今日",
    samplingRangeThisWeekBadge: "本周",
    samplingRangeThisMonthBadge: "本月",
    samplingRangeStartMini: "开始",
    samplingRangeEndMini: "结束",
    samplingRangeDaysMini: "天数",
    samplingRangeRelative: "相对时间",
    samplingRangeCustomDate: "自定义日期",
    samplingRangeCustomDateTime: "自定义日期+时间",
    samplingWeekDisplay: "第{week}周（{start} ~ {end}）",
    samplingMonthYear: "{year}",
    samplingMonthDisplay: "{year}年{month}",
    samplingDayLabel: "日期",
    samplingWeekLabel: "周",
    samplingMonthLabel: "月份",
    samplingArrowPrev: "上一项",
    samplingArrowNext: "下一项",
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
    samplingZeroHeroTitle: "当前 5 分钟外送窗口",
    samplingZeroHeroMetaActive: "基于当前状态的 export window 实时观测",
    samplingZeroHeroMetaInactive: "会在 16:00-23:00 的外送窗口期间显示",
    samplingZeroHeroStateEligible: "零购电",
    samplingZeroHeroStateImportObserved: "已观测购电",
    samplingZeroHeroStateLiveImport: "正在购电",
    samplingZeroHeroStateMonitoring: "监控中",
    samplingZeroHeroStateInactive: "空闲",
    samplingZeroHeroFieldWindow: "窗口",
    samplingZeroHeroFieldRemaining: "剩余",
    samplingZeroHeroFieldObservedImport: "已观测购电",
    samplingZeroHeroFieldObservedExport: "已观测卖电",
    samplingZeroHeroFieldLiveImport: "实时购电",
    samplingZeroHeroFieldLiveExport: "实时卖电",
    samplingZeroHeroFieldCoverage: "覆盖率",
    samplingZeroHeroFieldSolplanetRecommendation: "Solplanet 策略",
    samplingZeroHeroFieldSolplanetPhase: "卖电阶段",
    samplingZeroHeroFieldDailyExport: "今日卖电",
    samplingZeroHeroFieldUsableBattery: "可用电量",
    samplingZeroHeroFieldActiveReserve: "当前保底",
    samplingZeroHeroFieldReserve: "保底余量",
    solplanetExportMonitorPhasePrePeak: "16:00-18:00，保留给家庭用电",
    solplanetExportMonitorPhasePeakHold: "18:00-21:00，卖 SolPlanet 保底以上",
    solplanetExportMonitorPhaseLateTarget: "21:00-23:00，卖 SolPlanet 保底以上",
    solplanetExportMonitorPhaseOutside: "不在外送窗口",
    solplanetExportMonitorRecommendationSell: "按 {power} 卖电",
    solplanetExportMonitorRecommendationSellSurplus: "按 {power} 卖余电",
    solplanetExportMonitorRecommendationStop: "停止卖电",
    solplanetExportMonitorRecommendationHold: "等待",
    solplanetExportMonitorRecommendationHoldUntilLate: "等待",
    solplanetExportMonitorRecommendationObserve: "仅观察",
    samplingZeroHeroAwaiting: "当前还没有 export window 的实时观测。",
    samplingZeroHeroCoverageText: "{samples}/{expected} 个样本 · {coverage}%",
    samplingZeroHeroHistoryTitle: "16:00-23:00 外送窗口回顾",
    samplingZeroHeroHistoryBadgeImport: "购电 {count} 窗 · 净购电 {netCount} 窗",
    samplingZeroHeroHistoryBadgeMixed: "购电 {count} 窗 · 净卖电",
    samplingZeroHeroHistoryBadgeClean: "仅外送",
    samplingZeroHeroHistoryBadgeNoData: "无样本 {count}",
    samplingZeroHeroHistoryMetaText: "{day} · {start}-{end} · {count} 个窗口",
    samplingZeroHeroHistoryAwaiting: "当前所选日期还没有 16:00-23:00 的窗口历史。",
    samplingZeroHeroHistoryStateImport: "净购电",
    samplingZeroHeroHistoryStateMixedExport: "有购电，净卖电",
    samplingZeroHeroHistoryStateExport: "纯卖电",
    samplingZeroHeroHistoryStateIdle: "平稳",
    samplingZeroHeroHistoryStateNoData: "无样本",
    samplingZeroHeroHistoryStatePending: "未到",
    samplingZeroHeroHistoryToday: "今天",
    samplingZeroHeroHistoryDaySummary:
      "购电窗口 {importCount} · 有购电但净卖电 {mixedCount} · 仅外送 {exportCount} · 平稳 {idleCount} · 无样本 {noDataCount} · 未到 {pendingCount} · 采样不足 {partialCount}",
    samplingZeroHeroHistoryDayEnergy: "购 {import} · 卖 {export}",
    samplingZeroHeroHistoryEnergyText: "购 {import} · 卖 {export}",
    samplingZeroHeroHistoryCoverageText: "{samples}/{expected} 个样本",
    samplingZeroHeroHistoryCoveragePartial: "采样不足",
    samplingZeroHeroHistoryInProgress: "进行中",
    samplingChartTitle: "功率时间线",
    samplingChartMeta: "{system} · {range} · {count} 个点",
    samplingChartNoData: "当前时间范围暂无图表数据",
    samplingLegendGroupSolar: "光伏",
    samplingLegendGroupGrid: "电网",
    samplingLegendGroupLoad: "家庭负载",
    samplingLegendGroupBattery: "电池",
    samplingLegendGroupSajBattery: "SAJ 电池",
    samplingLegendGroupSolplanetBattery: "Solplanet 电池",
    samplingLegendSeriesGeneration: "发电",
    samplingLegendSeriesImport: "购电",
    samplingLegendSeriesExport: "上网",
    samplingLegendSeriesOther: "其他",
    samplingLegendSeriesCharge: "充电",
    samplingLegendSeriesDischarge: "放电",
    samplingBatteryTimelineTitle: "电池时间线",
    samplingBatteryTimelineMeta: "{range} · SAJ + Solplanet SOC 与可用剩余电量 · {count} 个点",
    samplingBatteryTimelineNoData: "当前时间范围暂无电池时间线数据",
    samplingPumpStatusTitle: "水泵状态",
    samplingPumpStatusMeta: "{range} · 水泵开关状态 · {count} 个点",
    samplingPumpStatusCurrentRange: "当前范围",
    samplingPumpStatusRecent48h: "最近 48 小时",
    samplingPumpStatusStarts: "启动次数",
    samplingPumpStatusLongestRun: "最长连续运行",
    samplingPumpStatusDayNightDetail: "白天 {dayHours}h · 晚上 {nightHours}h",
    samplingPumpStatusDutyDetail: "开启占比 {duty}%",
    samplingPumpStatusStartsDetail: "{range} · 观测到的状态切换",
    samplingPumpStatusLongestDetail: "{range}",
    samplingPumpStatusNoData: "当前时间范围暂无水泵状态数据",
    samplingPumpStatusOff: "关",
    samplingPumpStatusOn: "开",
    samplingBatteryTimelineSeriesSaj: "SAJ 电池",
    samplingBatteryTimelineSeriesSolplanet: "Solplanet 电池",
    samplingBatteryTimelineSeriesCombined: "SAJ + Solplanet 电池",
    samplingBatteryTimelineSeriesTotalRemaining: "总剩余可用电量",
    samplingBatteryTimelineSeriesTesla: "Tesla 电池",
    samplingTeslaFreeChargeTitle: "Tesla 免费充电",
    samplingTeslaFreeChargeMeta: "{day} · 11:00-15:00 累计充入 SOC",
    samplingTeslaFreeChargeNoData: "免费时段暂无 Tesla SOC 样本",
    samplingTeslaFreeChargeDetail: "{kwh} · 期间消耗 {used} · 净变化 {net} · {startTime}-{endTime}",
    samplingFreeEnergyUseTitle: "免费电分摊",
    samplingFreeEnergyUseMeta: "{day} · 11:00-15:00 按功率比例估算",
    samplingFreeEnergyUseTesla: "Tesla",
    samplingFreeEnergyUseHomeLoad: "家庭用电",
    samplingFreeEnergyUseBatteryCharge: "电池充电",
    samplingFreeEnergyUseNoData: "免费时段暂无电网购电样本",
    samplingTotalsTitle: "电量分布",
    samplingTotalsMeta: "{system} · {range} · 分方向累计电量",
    samplingTotalsMetaNoData: "{system} · {range} · 当前样本不足，无法统计累计值",
    samplingTotalsScopeOverall: "综合",
    samplingTotalsScopeSystem: "{system}",
    samplingTotalsScopeHomeLoad: "家庭用电",
    samplingTotalsScopeSajBattery: "SAJ 电池",
    samplingTotalsScopeSolplanetBattery: "Solplanet 电池",
    samplingTotalsGridTitle: "电网",
    samplingTotalsNoData: "当前区间暂无可显示的累计值",
    samplingTotalsWindowFree: "免费",
    samplingTotalsWindowPaid: "收费",
    samplingTotalsWindowOther: "其他",
    samplingTotalsWindowTotal: "总量",
    samplingTotalsPointsCompact: "{samples} 点",
    samplingPhaseBatteryTitle: "阶段结束剩余电量",
    samplingPhaseBatteryMeta: "{day} · 每个阶段结束时 SAJ + Solplanet 可用电量",
    samplingPhaseBatteryNoData: "当天暂无阶段结束电量数据",
    samplingPhaseBatteryUnavailable: "无数据",
    samplingPhaseBatteryEndAt: "{time} 结束",
    samplingPhaseBatteryDetail: "SAJ {saj} · Solplanet {solplanet}",
    samplingOverallMetricPv: "光伏发电",
    samplingOverallMetricGridImport: "电网购电",
    samplingOverallMetricGridExport: "电网上网",
    samplingOverallMetricHomeLoadTesla: "特斯拉",
    samplingOverallMetricHomeLoadPoolPump: "水泵",
    samplingOverallMetricHomeLoadOther: "其他家庭用电",
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
    databaseExportBtn: "导出数据库",
    databaseImportBtn: "导入数据库",
    databaseBackupNowBtn: "立即备份",
    databaseExporting: "导出中...",
    databaseImporting: "导入中...",
    databaseBackupRunning: "备份中...",
    databaseImportConfirmReplace: "导入会覆盖当前数据库文件，是否继续？",
    databaseImportDone: "数据库导入完成",
    databaseBackupDone: "数据库备份完成",
    databaseBackupSkipped: "今天已经有数据库备份",
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
    operationTimelineTitle: "操作记录",
    operationTimelineTableTime: "时间",
    operationTimelineTableAction: "操作",
    operationTimelineTableReason: "原因",
    operationTimelineCount: "共 {total} 条操作",
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
    databaseExportFailed: "数据库导出失败：{error}",
    databaseImportFailed: "数据库导入失败：{error}",
    databaseBackupFailed: "数据库备份失败：{error}",
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
    flowMetaRefreshing: "后台刷新中 · {updated}",
    flowMetaDoneOk: "{updated}",
    flowMetaDonePartial: "部分数据（{count}/6）· {updated}",
    flowMetaDoneStale: "缓存数据（{count}/6）· {updated}",
    flowMetaDoneStaleCombined: "快照未更新，collector 可能卡住（{count}/6）· {updated}",
    flowMetaIssueReason: "原因：{reason}",
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
  "key_metrics",
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
const DATABASE_PAGE_SIZE = 100;
const WORKER_FAILURE_LOG_PAGE_SIZE = 100;
const AUTO_REFRESH_KEY = "autoRefreshSeconds";
const SOLPLANET_RAW_MODE_KEY = "solplanetRawMode";
const RAW_CARD_COLLAPSE_KEY = "rawCardCollapseState";
const RAW_DATA_SYSTEM_KEY = "rawDataSystem";
const SAJ_ACTION_DEBUG_MODE_KEY = "sajActionDebugMode";
const DASHBOARD_SUMMARY_CACHE_KEY = "dashboardSummaryCache";
const AUTO_REFRESH_OPTIONS = [0, 30, 60];
const SAJ_CONTROL_EDIT_GRACE_MS = 15000;
const CONFIG_SAMPLE_INTERVAL_OPTIONS = [5, 10, 30, 60, 300];
const CONFIG_DETAIL_RETENTION_OPTIONS = [1, 2, 3, 6, 12];
const BALANCE_TOLERANCE_W = 100;
const SAMPLING_GRID_IMPORT_WINDOW = { startHour: 11, endHour: 15, label: "11:00-15:00" };
const SAMPLING_GRID_EXPORT_WINDOW = { startHour: 16, endHour: 23, label: "16:00-23:00" };
const SAMPLING_PHASE_BATTERY_ENDS = [
  { id: "overnight_shoulder", titleKey: "notificationMatrixWindowOvernightTitle", endHour: 11 },
  { id: "free_energy", titleKey: "notificationMatrixWindowFreeEnergyTitle", endHour: 15 },
  { id: "after_free_shoulder", titleKey: "notificationMatrixWindowAfterFreeShoulderTitle", endHour: 16 },
  { id: "export_window", titleKey: "notificationMatrixWindowExportTitle", endHour: 23 },
];
const SAMPLING_PHASE_BATTERY_SAMPLE_TOLERANCE_MS = 15 * 60 * 1000;
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
  { key: "pv_w", labelKey: "samplingSeriesPv", color: "#d97706" },
  { key: "grid_w", labelKey: "samplingSeriesGrid", color: "#2563eb" },
  { key: "battery_w", labelKey: "samplingSeriesBattery", color: "#16a34a" },
  { key: "load_w", labelKey: "samplingSeriesLoad", color: "#64748b" },
];
const SAMPLING_OVERALL_SERIES = [
  { key: "pv_total_w", labelKey: "samplingOverallMetricPv", color: "#d97706" },
  { key: "grid_import_w", labelKey: "samplingOverallMetricGridImport", color: "#2563eb" },
  { key: "grid_export_w", labelKey: "samplingOverallMetricGridExport", color: "#0891b2" },
  { key: "tesla_charge_power_w", labelKey: "samplingOverallMetricHomeLoadTesla", color: "#dc2626" },
  { key: "load_w", labelKey: "samplingOverallMetricHomeLoadOther", color: "#64748b" },
  { key: "saj_battery_charge_w", labelKey: "samplingOverallSeriesSajBatteryCharge", color: "#15803d" },
  { key: "saj_battery_discharge_w", labelKey: "samplingOverallSeriesSajBatteryDischarge", color: "#22c55e" },
  { key: "solplanet_battery_charge_w", labelKey: "samplingOverallSeriesSolplanetBatteryCharge", color: "#7c3aed" },
  { key: "solplanet_battery_discharge_w", labelKey: "samplingOverallSeriesSolplanetBatteryDischarge", color: "#c026d3" },
];
const SAMPLING_BATTERY_TIMELINE_SERIES = [
  { key: "saj_battery_soc_percent", labelKey: "samplingBatteryTimelineSeriesSaj", color: "#22c55e" },
  { key: "solplanet_battery_soc_percent", labelKey: "samplingBatteryTimelineSeriesSolplanet", color: "#7c3aed" },
  { key: "total_battery_usable_kwh", labelKey: "samplingBatteryTimelineSeriesTotalRemaining", color: "#0f766e" },
  { key: "tesla_battery_soc_percent", labelKey: "samplingBatteryTimelineSeriesTesla", color: "#dc2626" },
];

const SAMPLING_POWER_LEGEND_LABEL_KEYS = {
  pv_total_w: "samplingLegendSeriesGeneration",
  grid_import_w: "samplingLegendSeriesImport",
  grid_export_w: "samplingLegendSeriesExport",
  load_w: "samplingLegendSeriesOther",
  saj_battery_charge_w: "samplingLegendSeriesCharge",
  saj_battery_discharge_w: "samplingLegendSeriesDischarge",
  solplanet_battery_charge_w: "samplingLegendSeriesCharge",
  solplanet_battery_discharge_w: "samplingLegendSeriesDischarge",
};

const SAMPLING_SERIES_META_BY_KEY = Object.fromEntries(
  [...SAMPLING_SERIES, ...SAMPLING_OVERALL_SERIES, ...SAMPLING_BATTERY_TIMELINE_SERIES].map((item) => [item.key, item]),
);
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
  lastSummary: readCachedDashboardSummary(),
  lastConfig: null,
  lastTimeWindowRules: null,
  lastCollectorStatus: null,
  lastNotificationSummary: null,
  lastSolplanetRaw: {},
  lastSolplanetKv: { phase: "idle", items: [], updated_at: null, error: null },
  lastSajRaw: {},
  lastSajControl: null,
  lastSajProfile: null,
  lastSolplanetControl: null,
  lastSolplanetControlLive: null,
  lastSolplanetControlFetch: null,
  lastHomePlan: null,
  lastBills: null,
  lastSamplingStatus: null,
  lastSamplingDaily: null,
  lastSamplingUsageBySystem: null,
  lastSamplingPage: null,
  lastSamplingSeries: null,
  lastSamplingBatteryTimeline: null,
  lastSamplingPhaseBattery: null,
  lastSamplingTeslaFreeCharge: null,
  lastSamplingFreeEnergyUse: null,
  lastSamplingPumpStatus: null,
  lastSamplingRecentPumpStatus: null,
  lastSamplingZeroHeroHistory: null,
  lastDatabaseTables: null,
  lastDatabasePage: null,
  lastDatabaseBackups: null,
  lastWorkerLogsPage: null,
  lastOperationTimeline: null,
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
    saj: { phase: "idle", updatedAt: null, quality: "ok", count: 0, refreshing: false, issueText: "" },
    solplanet: { phase: "idle", updatedAt: null, quality: "ok", count: 0, refreshing: false, issueText: "" },
    combined: { phase: "idle", updatedAt: null, quality: "ok", count: 0, refreshing: false, issueText: "" },
  },
};
let teslaControlBusy = false;
let teslaCurrentControlBusy = false;
let solplanetPinModalBusy = false;
let poolPumpControlBusy = false;
let teslaAutoControlBusy = false;
let teslaForceMaxChargeThresholdBusy = false;
let poolPumpAutoControlBusy = false;
let poolPumpForceMinRuntimeBusy = false;
let timeWindowPriorityOverrideBusy = false;
const TESLA_CONTROL_CONFIRM_POLL_MS = 1500;
const TESLA_CONTROL_CONFIRM_TIMEOUT_MS = 15000;
const TESLA_CONTROL_FEEDBACK_CLEAR_MS = 4000;
let teslaControlFeedback = null;
let teslaControlFeedbackTimerId = null;
let teslaCurrentDraftA = null;
let teslaPendingCurrentSourceA = null;
let teslaPendingCurrentTargetA = null;
const timeWindowRuleBusy = new Set();

const NOTIFICATION_MATRIX_WINDOWS = [
  {
    id: "overnight_shoulder",
    schedule: "23:00-11:00",
    rateKey: "notificationMatrixRateShoulder",
    titleKey: "notificationMatrixWindowOvernightTitle",
    summaryKey: "notificationMatrixWindowOvernightSummary",
    rules: [
      {
        kind: "operation",
        level: "info",
        code: "saj_profile_self_consumption",
        titleKey: "notificationRuleSajProfileSelfConsumptionTitle",
        triggerKey: "notificationRuleSajProfileSelfConsumptionTrigger",
      },
      {
        kind: "operation",
        level: "info",
        code: "tesla_overnight_shoulder_control",
        titleKey: "notificationRuleTeslaOvernightShoulderTitle",
        triggerKey: "notificationRuleTeslaOvernightShoulderTrigger",
      },
      {
        kind: "operation",
        level: "info",
        code: "pool_pump_overnight_control",
        titleKey: "notificationRulePoolPumpOvernightTitle",
        triggerKey: "notificationRulePoolPumpOvernightTrigger",
      },
    ],
  },
  {
    id: "free_energy",
    schedule: "11:00-15:00",
    rateKey: "notificationMatrixRateFree",
    titleKey: "notificationMatrixWindowFreeEnergyTitle",
    summaryKey: "notificationMatrixWindowFreeEnergySummary",
    rules: [
      {
        kind: "operation",
        level: "info",
        code: "saj_profile_free_energy",
        titleKey: "notificationRuleSajProfileFreeEnergyTitle",
        triggerKey: "notificationRuleSajProfileFreeEnergyTrigger",
      },
      {
        kind: "operation",
        level: "info",
        code: "tesla_free_energy_control",
        titleKey: "notificationRuleTeslaFreeEnergyTitle",
        triggerKey: "notificationRuleTeslaFreeEnergyTrigger",
      },
      {
        kind: "operation",
        level: "info",
        code: "pool_pump_free_energy_control",
        titleKey: "notificationRulePoolPumpFreeEnergyTitle",
        triggerKey: "notificationRulePoolPumpFreeEnergyTrigger",
      },
    ],
  },
  {
    id: "after_free_shoulder",
    schedule: "15:00-16:00",
    rateKey: "notificationMatrixRateShoulder",
    titleKey: "notificationMatrixWindowAfterFreeShoulderTitle",
    summaryKey: "notificationMatrixWindowAfterFreeShoulderSummary",
    rules: [
      {
        kind: "operation",
        level: "info",
        code: "saj_profile_self_consumption",
        titleKey: "notificationRuleSajProfileSelfConsumptionTitle",
        triggerKey: "notificationRuleSajProfileSelfConsumptionTrigger",
      },
      {
        kind: "operation",
        level: "info",
        code: "tesla_after_free_shoulder_control",
        titleKey: "notificationRuleTeslaAfterFreeShoulderTitle",
        triggerKey: "notificationRuleTeslaAfterFreeShoulderTrigger",
      },
    ],
  },
  {
    id: "export_window",
    schedule: "16:00-23:00",
    rateKey: "notificationMatrixRateExport",
    titleKey: "notificationMatrixWindowExportTitle",
    summaryKey: "notificationMatrixWindowExportSummary",
    rules: [
      {
        kind: "operation",
        level: "info",
        code: "saj_profile_self_consumption",
        titleKey: "notificationRuleSajProfileSelfConsumptionTitle",
        triggerKey: "notificationRuleSajProfileSelfConsumptionTrigger",
      },
      {
        kind: "notification",
        level: "alarm",
        code: "grid_import_started",
        titleKey: "notificationRuleGridImportStartedTitle",
        triggerKey: "notificationRuleGridImportStartedTrigger",
      },
    ],
  },
];

function isMobileViewport() {
  return Boolean(window.matchMedia?.("(max-width: 700px)")?.matches);
}

function getLang() {
  if (isMobileViewport()) return "en";
  const saved = localStorage.getItem("lang");
  if (saved === "en" || saved === "zh") return saved;
  const browserLang = (navigator.language || "").toLowerCase();
  return browserLang.startsWith("zh") ? "zh" : "en";
}

let currentLang = getLang();
let billsDropDepth = 0;
const _hashTab = location.hash.slice(1);
const ALL_TABS = ["dashboard", "plan", "notificationMatrix", "rawData", "solplanetControl", "report", "monthlyReport", "bills", "database", "workerLogs", "operationTimeline", "workerFailureLog"];
const SAJ_PROFILE_IDS = ["self_consumption", "time_of_use", "microgrid"];
const PLAN_RATE_KEYS = {
  daily: "daily_charge_aud_per_day",
  peak: "peak_usage_aud_per_kwh",
  free: "off_peak_first_50_kwh_per_day_aud_per_kwh",
  offPeakBalance: "off_peak_balance_aud_per_kwh",
  shoulder: "shoulder_usage_aud_per_kwh",
  controlledLoad: "controlled_load_aud_per_kwh",
  metering: "metering_charge_aud_per_day",
  fitEvening: "solar_feed_in_4pm_11pm_aud_per_kwh",
  fitOther: "solar_feed_in_11pm_4pm_aud_per_kwh",
  superExport: "super_export_top_up_aud_per_kwh",
  zeroHero: "zerohero_credit_aud_per_qualified_day",
};
const PLAN_TIMETABLE_ROWS = [
  { id: "fixed", labelKey: "planTimetableFixed", fixed: true },
  { id: "2300-1100", window: "23:00-11:00", buy: PLAN_RATE_KEYS.shoulder, sell: PLAN_RATE_KEYS.fitOther },
  { id: "1100-1500", window: "11:00-15:00", buy: PLAN_RATE_KEYS.free, sell: PLAN_RATE_KEYS.fitOther },
  { id: "1500-1600", window: "15:00-16:00", buy: PLAN_RATE_KEYS.shoulder, sell: PLAN_RATE_KEYS.fitOther },
  { id: "1600-2300", window: "16:00-23:00", buy: PLAN_RATE_KEYS.peak, sell: PLAN_RATE_KEYS.fitEvening },
];
function normalizeAppTab(tab) {
  const normalized = String(tab || "").trim();
  if (normalized === "sampling") return "report";
  return ALL_TABS.includes(normalized) ? normalized : "";
}
let currentTab = normalizeAppTab(_hashTab) || normalizeAppTab(localStorage.getItem("activeTab")) || "dashboard";
let solplanetRawMode = localStorage.getItem(SOLPLANET_RAW_MODE_KEY) === "table" ? "table" : "cards";
let rawDataSystem = localStorage.getItem(RAW_DATA_SYSTEM_KEY) === "saj" ? "saj" : "solplanet";
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
let samplingBatteryTimelineCharts = {
  combined: null,
  remaining: null,
  tesla: null,
};
let samplingBatteryTimelineLastPayload = null;
let samplingPumpStatusChart = null;
let samplingPumpStatusLastPayload = null;
let samplingTotalsChart = null;
let monthlyReportDailyTrendChart = null;
let samplingLoadInFlight = false;
let samplingRangeApplyingFromBrush = false;
let samplingLegendSyncing = false;
let samplingBrushGlobalHandlersBound = false;
let samplingHoverRepairRafId = 0;
let samplingHoverRepairNeedsResize = false;
const SAMPLING_CHART_CLICK_INTERACTION_ENABLED = false;
const SAMPLING_CHART_LEGEND_SELECT_ENABLED = true;
const samplingBrushState = {
  active: false,
  pendingRange: null,
};
let notificationMatrixCollapseState = {};
let notificationMatrixCollapseInitialized = false;
let notificationMatrixPriorityCollapsed = true;
const tabLoadState = {
  dashboard: { inFlight: false },
  plan: { inFlight: false },
  notificationMatrix: { inFlight: false },
  entities: { inFlight: false },
  rawData: { inFlight: false },
  solplanetControl: { inFlight: false },
  sampling: { inFlight: false },
  report: { inFlight: false },
  monthlyReport: { inFlight: false },
  bills: { inFlight: false },
  database: { inFlight: false },
  workerLogs: { inFlight: false },
  operationTimeline: { inFlight: false },
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
const initialReportDay = getLocalDateText();
const initialReportDayParts = localDateParts(initialReportDay);
const REPORT_CALENDAR_FOUR4FREE_START_DAY = "2026-07-28";
const reportCalendarState = {
  selectedDay: initialReportDay,
  month: initialReportDayParts ? initialReportDayParts.monthIndex + 1 : new SiteDate().getMonth() + 1,
  monthYear: initialReportDayParts ? initialReportDayParts.year : new SiteDate().getFullYear(),
  availableDays: new Map(),
  loadedMonths: new Set(),
  loadingIndex: false,
  loadingDay: "",
  statusText: "",
};
const reportBillsState = {
  items: [],
  loaded: false,
  loading: false,
  error: "",
};
const monthlyReportState = {
  month: initialReportDayParts ? initialReportDayParts.monthIndex + 1 : new SiteDate().getMonth() + 1,
  monthYear: initialReportDayParts ? initialReportDayParts.year : new SiteDate().getFullYear(),
  pickerYear: initialReportDayParts ? initialReportDayParts.year : new SiteDate().getFullYear(),
  availableMonths: new Map(),
  loadedYears: new Set(),
  loadingIndex: false,
  loading: false,
  payload: null,
  statusText: "",
  selectedAnomalyDay: "",
};
const billsState = {
  selectedInvoice: "",
};

function getSelectedSamplingSystem() {
  return "overall";
}
const databaseTransferState = {
  visible: false,
  title: "",
  message: "",
  percentText: "",
  ratio: 0,
  status: "idle",
};
let databaseTransferTimerId = null;

function getAutoRefreshSeconds() {
  if (isMobileViewport()) return 0;
  const saved = Number(localStorage.getItem(AUTO_REFRESH_KEY));
  return AUTO_REFRESH_OPTIONS.includes(saved) ? saved : 30;
}

function t(key, params = {}) {
  const table = I18N[currentLang] || I18N.en;
  let text = table[key] || I18N.en[key] || key;
  for (const [name, value] of Object.entries(params)) {
    text = text.replaceAll(`{${name}}`, String(value));
  }
  return text;
}

function hasTranslation(key) {
  if (!key) return false;
  const table = I18N[currentLang] || I18N.en;
  return Object.prototype.hasOwnProperty.call(table, key) || Object.prototype.hasOwnProperty.call(I18N.en, key);
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

function shortMonthLabel(month) {
  const m = Number(month);
  if (currentLang === "zh") return `${m}`;
  const enMonthsShort = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return enMonthsShort[m - 1] || `M${m}`;
}

function formatSamplingWeekCompactDisplay(info) {
  const start = localDateParts(info?.monday);
  const end = localDateParts(info?.sunday);
  if (!start || !end) return t("samplingWeekDisplay", { week: info?.week || "-", start: info?.monday || "-", end: info?.sunday || "-" });
  if (currentLang === "zh") {
    const startText = `${start.monthIndex + 1}/${start.day}`;
    const endText = `${end.monthIndex + 1}/${end.day}`;
    return `第${info.week}周 · ${startText}-${endText}`;
  }
  const startMonth = shortMonthLabel(start.monthIndex + 1);
  const endMonth = shortMonthLabel(end.monthIndex + 1);
  const rangeText = start.monthIndex === end.monthIndex ? `${startMonth} ${start.day}-${end.day}` : `${startMonth} ${start.day}-${endMonth} ${end.day}`;
  return `W${info.week} · ${rangeText}`;
}

function formatSamplingMiniDate(dateText) {
  const parts = localDateParts(dateText);
  if (!parts) return String(dateText || "-");
  if (currentLang === "zh") return `${parts.monthIndex + 1}/${parts.day}`;
  return `${shortMonthLabel(parts.monthIndex + 1)} ${parts.day}`;
}

function buildSamplingRangeInfoGrid(items) {
  return `
    <div class="sampling-range-info-grid">
      ${items
        .map(
          (item) => `
            <div class="sampling-range-info-card">
              <span class="sampling-range-info-label">${escapeHtml(item.label)}</span>
              <span class="sampling-range-info-value">${escapeHtml(item.value)}</span>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
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
  }
}

function setReportLoadingModal(visible, message = "") {
  const modal = document.getElementById("reportLoadingModal");
  if (!modal) return;
  const shouldShow = Boolean(visible) && currentTab === "report";
  modal.classList.toggle("hidden", !shouldShow);
  modal.setAttribute("aria-hidden", shouldShow ? "false" : "true");
  setText("reportLoadingTitle", t("reportLoadingModalTitle"));
  setText("reportLoadingBody", message || t("reportLoadingModalBody"));
}

function setSamplingZeroHeroCurrentVisible(visible) {
  const card = document.getElementById("samplingZeroHeroCurrentCard");
  if (!card) return;
  card.hidden = !visible;
  card.classList.toggle("hidden", !visible);
}

function setSamplingZeroHeroHistoryVisible(visible) {
  const card = document.getElementById("samplingZeroHeroHistoryCard");
  if (!card) return;
  card.hidden = !visible;
  card.classList.toggle("hidden", !visible);
}

function setSamplingSamplesSectionVisible(visible) {
  for (const id of ["samplingCount", "samplingPager", "samplingTableWrap"]) {
    const el = document.getElementById(id);
    if (!el) continue;
    el.hidden = !visible;
    el.classList.toggle("hidden", !visible);
  }
}

function renderDatabaseTransferState() {
  const container = document.getElementById("databaseTransferStatus");
  const title = document.getElementById("databaseTransferTitle");
  const percent = document.getElementById("databaseTransferPercent");
  const message = document.getElementById("databaseTransferMessage");
  const bar = document.getElementById("databaseTransferBar");
  const fill = document.getElementById("databaseTransferFill");
  if (!container || !title || !percent || !message || !bar || !fill) return;
  container.classList.toggle("hidden", !databaseTransferState.visible);
  title.textContent = databaseTransferState.title || "-";
  percent.textContent = databaseTransferState.percentText || t("databaseTransferPreparing");
  message.textContent = databaseTransferState.message || "-";
  bar.classList.remove("indeterminate", "done", "failed");
  if (databaseTransferState.status === "done") bar.classList.add("done");
  if (databaseTransferState.status === "failed") bar.classList.add("failed");
  const width = Math.max(0, Math.min(100, Number(databaseTransferState.ratio || 0) * 100));
  fill.style.width = `${width.toFixed(1)}%`;
}

function setDatabaseTransferState(patch) {
  Object.assign(databaseTransferState, patch, { visible: true });
  if (!patch.percentText) {
    databaseTransferState.percentText = t("databaseTransferPercent", {
      value: Math.round(Math.max(0, Math.min(100, Number(databaseTransferState.ratio || 0) * 100))),
    });
  }
  renderDatabaseTransferState();
}

function stopDatabaseTransferAutoProgress() {
  if (databaseTransferTimerId) {
    window.clearInterval(databaseTransferTimerId);
    databaseTransferTimerId = null;
  }
}

function startDatabaseTransferAutoProgress(targetRatio, step = 0.01, intervalMs = 180) {
  stopDatabaseTransferAutoProgress();
  databaseTransferTimerId = window.setInterval(() => {
    const nextRatio = Math.min(targetRatio, Number(databaseTransferState.ratio || 0) + step);
    setDatabaseTransferState({ ratio: nextRatio });
    if (nextRatio >= targetRatio) {
      stopDatabaseTransferAutoProgress();
    }
  }, intervalMs);
}

function clearDatabaseTransferState() {
  stopDatabaseTransferAutoProgress();
  databaseTransferState.visible = false;
  databaseTransferState.title = "";
  databaseTransferState.message = "";
  databaseTransferState.percentText = "";
  databaseTransferState.ratio = 0;
  databaseTransferState.status = "idle";
  renderDatabaseTransferState();
}

function setNodeSourceTip(id, tipText) {
  const el = document.getElementById(id);
  if (!el) return;
  if (tipText === null || tipText === undefined || tipText === "") {
    el.classList.remove("has-source-tip");
    el.removeAttribute("data-source-tip");
    return;
  }
  const normalized = String(tipText || "-");
  el.classList.add("has-source-tip");
  el.setAttribute("data-source-tip", normalized);
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

function formatValueWithDataKindHtml(text, kind) {
  const normalized = String(text ?? "-");
  return escapeHtml(normalized);
}

function formatValueWithDataKind(text, kind) {
  const normalized = String(text ?? "-");
  return normalized;
}

function flowId(system, key) {
  return `${system}-${key}`;
}

function formatUpdatedAt(isoText) {
  return `${t("updatedAt")}: ${formatDateTimeWithAgo(isoText)}`;
}

function isIsoOlderThanSeconds(isoText, seconds) {
  if (!isoText) return false;
  const dt = new SiteDate(isoText);
  if (Number.isNaN(dt.getTime())) return false;
  return (Date.now() - dt.getTime()) / 1000 > seconds;
}

function formatNoWrapText(text) {
  return String(text || "").replaceAll(" ", "\u00A0");
}

function formatDateTimeWithAgo(isoText, options = {}) {
  if (!isoText) return "-";
  const dt = new SiteDate(isoText);
  if (Number.isNaN(dt.getTime())) return "-";
  const absoluteText = dt.toLocaleString();
  const agoText = formatRelativeAgo(isoText);
  const combined = agoText && agoText !== "-" ? `${absoluteText} (${agoText})` : absoluteText;
  return formatNoWrapText(combined);
}

function formatWorkerLogLocalTimeFromEpoch(epochSeconds) {
  const raw = Number(epochSeconds);
  if (!Number.isFinite(raw)) return "-";
  const dt = new SiteDate(raw * 1000);
  if (Number.isNaN(dt.getTime())) return "-";
  return formatDateTimeWithAgo(dt.toISOString());
}

function looksLikeIsoDateTime(value) {
  if (typeof value !== "string") return false;
  const text = value.trim();
  if (!text) return false;
  if (!/^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}/.test(text)) return false;
  const dt = new SiteDate(text);
  return !Number.isNaN(dt.getTime());
}

function unwrapDatabaseStringValue(value) {
  if (typeof value !== "string") return value;
  const text = value.trim();
  if (text.length < 2) return value;
  if ((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("'") && text.endsWith("'"))) {
    try {
      return JSON.parse(text);
    } catch {
      return text.slice(1, -1);
    }
  }
  return value;
}

function looksLikeEpochTime(key, value) {
  const keyText = String(key || "").toLowerCase();
  if (!keyText.includes("epoch") && !keyText.includes("timestamp")) return false;
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0;
}

function databaseTimeToggleInfo(key, rawValue) {
  const normalizedValue = unwrapDatabaseStringValue(rawValue);
  if (looksLikeIsoDateTime(normalizedValue)) {
    const rawText = String(rawValue);
    return { rawText, localText: new SiteDate(String(normalizedValue)).toLocaleString() };
  }
  if (looksLikeEpochTime(key, normalizedValue)) {
    const numeric = Number(normalizedValue);
    const dt = new SiteDate(numeric * 1000);
    if (!Number.isNaN(dt.getTime())) {
      return { rawText: String(rawValue), localText: dt.toLocaleString() };
    }
  }
  return null;
}

function renderDatabaseValueHtml(key, rawValue, options = {}) {
  const multilineObjects = Boolean(options.multilineObjects);
  const timeInfo = databaseTimeToggleInfo(key, rawValue);
  if (timeInfo) {
    return `
      <button
        type="button"
        class="database-time-toggle"
        data-raw="${escapeHtml(timeInfo.rawText)}"
        data-local="${escapeHtml(timeInfo.localText)}"
        data-mode="raw"
        title="Click to toggle local time"
      >${escapeHtml(timeInfo.rawText)}</button>
    `;
  }
  const text =
    rawValue === null || rawValue === undefined || rawValue === ""
      ? "-"
      : typeof rawValue === "object"
        ? JSON.stringify(rawValue, null, multilineObjects ? 2 : 0)
        : String(rawValue);
  return `<span class="database-cell-text" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
}

function toggleDatabaseTimeButton(button) {
  const mode = button.dataset.mode === "local" ? "local" : "raw";
  const nextMode = mode === "raw" ? "local" : "raw";
  button.dataset.mode = nextMode;
  button.textContent = nextMode === "local"
    ? (button.dataset.local || "-")
    : (button.dataset.raw || "-");
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
  if (column === "key_metrics") {
    const payload = item?.payload_json;
    if (!payload) return "-";
    if (item.service === "combined_assembly") {
      const parts = {};
      for (const sys of ["saj", "solplanet", "combined"]) {
        const km = payload[sys]?.key_metrics;
        if (km && typeof km === "object" && Object.keys(km).length > 0) parts[sys] = km;
      }
      return Object.keys(parts).length ? JSON.stringify(parts) : "-";
    }
    const metrics = payload?.flow?.metrics;
    if (!metrics) return "-";
    const KEY_FIELDS = [
      "pv_w", "grid_w", "battery_w", "inverter_power_w",
      "battery_soc_percent", "battery_energy_kwh", "inverter_status",
    ];
    const result = {};
    for (const f of KEY_FIELDS) {
      if (metrics[f] != null) result[f] = metrics[f];
    }
    return Object.keys(result).length ? JSON.stringify(result) : "-";
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
  const dt = new SiteDate(value);
  if (Number.isNaN(dt.getTime())) return "-";
  const text = dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
  if (!withZone) return text;
  return text;
}

function buildSamplingTimeAxisConfig(xMin, xMax) {
  const oneHourMs = 60 * 60 * 1000;
  const durationMs = Number.isFinite(xMin) && Number.isFinite(xMax) ? Math.max(0, xMax - xMin) : 0;
  const is24HourView = durationMs >= 20 * oneHourMs && durationMs <= 28 * oneHourMs;
  if (!is24HourView) {
    return {
      splitNumber: 6,
      axisTick: { show: false },
      axisLabel: {
        color: "#6b7f72",
        formatter: (value) => formatSamplingClock(value, false),
      },
    };
  }
  return {
    splitNumber: 24,
    minInterval: oneHourMs,
    maxInterval: oneHourMs,
    axisTick: {
      show: true,
      alignWithLabel: true,
      length: 6,
      lineStyle: { color: "#adc2b5" },
    },
    axisLabel: {
      color: "#6b7f72",
      hideOverlap: false,
      formatter: (value) => {
        const dt = new SiteDate(value);
        if (Number.isNaN(dt.getTime())) return "";
        return `${String(dt.getHours()).padStart(2, "0")}:00`;
      },
    },
  };
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

function parsePlanNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function formatPlanRateValue(value, unit) {
  const n = parsePlanNumber(value);
  if (n === null) return "-";
  const suffix = unit === "$/day" ? "/day" : "/kWh";
  return `$${n.toFixed(5)}${suffix}`;
}

function formatPlanTimetableRateValue(value, unit) {
  const n = parsePlanNumber(value);
  if (n === null) return "-";
  if (unit === "$/day") return `$${n.toFixed(3)}/day`;
  const cents = n * 100;
  const digits = Math.abs(cents) >= 10 ? 1 : 2;
  return `${cents.toFixed(digits)}c/kWh`;
}

function getPlanRate(plan, key, column = "after_discount") {
  return parsePlanNumber(plan?.rates?.[key]?.[column]);
}

function getPlanDisplayName(plan) {
  return String(plan?.display_name || plan?.name || plan?.id || "-");
}

function getPlanTimetableGroup(plan) {
  const text = `${plan?.id || ""} ${plan?.name || ""} ${plan?.display_name || ""}`.toLowerCase();
  if (text.includes("four4free")) return "four4free";
  if (text.includes("zerohero") && text.includes("new")) return "zerohero_new";
  if (text.includes("zerohero") && text.includes("old")) return "zerohero_old";
  return "default";
}

function getPlanTimetableRateKey(rule, group) {
  if (!rule) return null;
  if (typeof rule === "string") return rule;
  return rule[group] || rule.default || null;
}

function getPlanTimetableSpecialCodes(row, group) {
  const special = row?.special;
  if (!special) return [];
  if (Array.isArray(special)) return special;
  return special[group] || special.default || [];
}

function clampPlanTone(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(1, n));
}

function getPlanTimetableToneValue(plan, row, group) {
  if (row.fixed) {
    const daily = getPlanRate(plan, PLAN_RATE_KEYS.daily);
    return daily === null ? null : clampPlanTone((daily - 1.3) / 0.8);
  }
  const buyKey = getPlanTimetableRateKey(row.buy, group);
  const buy = getPlanRate(plan, buyKey);
  return buy === null ? null : clampPlanTone(buy / 0.6);
}

function getPlanTimetableCellAttributes(plan, row) {
  const group = getPlanTimetableGroup(plan);
  const tone = getPlanTimetableToneValue(plan, row, group);
  if (tone === null) return `class="plan-timetable-td plan-timetable-td-neutral"`;
  const hue = Math.round(142 - tone * 118);
  const saturation = Math.round(42 + tone * 24);
  const lightness = Math.round(96 - tone * 5);
  const borderLightness = Math.round(76 - tone * 10);
  return `class="plan-timetable-td" style="--plan-cell-bg: hsl(${hue} ${saturation}% ${lightness}%); --plan-cell-border: hsl(${hue} ${saturation}% ${borderLightness}%);"`;
}

function renderPlanTimetableLine(labelKey, valueText, className = "") {
  return `
    <div class="plan-timetable-line ${className}">
      <span>${escapeHtml(t(labelKey))}</span>
      <strong>${escapeHtml(valueText || "-")}</strong>
    </div>
  `;
}

function getPlanTimetableFixedLines(plan) {
  return `
    <div class="plan-timetable-cell">
      ${renderPlanTimetableLine("planTimetableDailySupply", formatPlanTimetableRateValue(getPlanRate(plan, PLAN_RATE_KEYS.daily), "$/day"))}
      ${renderPlanTimetableLine("planTimetableMetering", formatPlanTimetableRateValue(getPlanRate(plan, PLAN_RATE_KEYS.metering), "$/day"))}
      ${renderPlanTimetableLine("planTimetableControlledLoad", formatPlanTimetableRateValue(getPlanRate(plan, PLAN_RATE_KEYS.controlledLoad), "$/kWh"))}
    </div>
  `;
}

function getPlanTimetableSpecials(plan, row, buyKey, sellKey, group) {
  const specials = [];
  const buyRate = plan?.rates?.[buyKey];
  const buyAfter = getPlanRate(plan, buyKey);
  if (buyKey === PLAN_RATE_KEYS.free && buyAfter === 0) {
    const parts = [t("planTimetableFreeUsage")];
    if (buyRate?.daily_cap_kwh) parts.push(t("planCapKwhPerDay", { value: buyRate.daily_cap_kwh }));
    const afterCapRate = getPlanRate(plan, PLAN_RATE_KEYS.offPeakBalance);
    if (afterCapRate !== null) {
      parts.push(t("planTimetableAfterCap", { value: formatPlanTimetableRateValue(afterCapRate, "$/kWh") }));
    }
    specials.push(parts.join(" · "));
  }

  for (const code of getPlanTimetableSpecialCodes(row, group)) {
    if (code === "superExport") {
      const topUp = getPlanRate(plan, PLAN_RATE_KEYS.superExport);
      if (topUp === null) continue;
      const sell = getPlanRate(plan, sellKey);
      const topUpText = formatPlanTimetableRateValue(topUp, "$/kWh");
      const totalText = sell !== null ? t("planTimetableTotalExport", { value: formatPlanTimetableRateValue(sell + topUp, "$/kWh") }) : "";
      const cap = plan?.rates?.[PLAN_RATE_KEYS.superExport]?.daily_cap_kwh
        ? t("planCapKwhPerDay", { value: plan.rates[PLAN_RATE_KEYS.superExport].daily_cap_kwh })
        : "";
      specials.push([`${t("planTimetableSuperExport")} +${topUpText}`, totalText, cap].filter(Boolean).join(" · "));
    } else if (code === "zeroHero") {
      const credit = getPlanRate(plan, PLAN_RATE_KEYS.zeroHero);
      if (credit !== null) {
        specials.push(`${t("planTimetableZeroHero")} ${formatPlanTimetableRateValue(credit, "$/day")}`);
      }
    }
  }
  return specials;
}

function renderPlanTimetableCell(plan, row) {
  const group = getPlanTimetableGroup(plan);
  if (row.fixed) return getPlanTimetableFixedLines(plan);

  const buyKey = getPlanTimetableRateKey(row.buy, group);
  const sellKey = getPlanTimetableRateKey(row.sell, group);
  const buyText = formatPlanTimetableRateValue(getPlanRate(plan, buyKey), "$/kWh");
  const sellText = formatPlanTimetableRateValue(getPlanRate(plan, sellKey), "$/kWh");
  const specials = getPlanTimetableSpecials(plan, row, buyKey, sellKey, group);
  return `
    <div class="plan-timetable-cell">
      ${renderPlanTimetableLine("planTimetableBuy", buyText, "plan-timetable-buy")}
      ${renderPlanTimetableLine("planTimetableSell", sellText, "plan-timetable-sell")}
      ${renderPlanTimetableLine("planTimetableSpecial", specials.length ? specials.join("; ") : t("planTimetableNone"), "plan-timetable-special")}
    </div>
  `;
}

function renderPlanMatrix(plans) {
  const matrixHead = document.getElementById("planMatrixHead");
  const matrixBody = document.getElementById("planMatrixBody");
  if (!matrixHead || !matrixBody) return;
  matrixHead.innerHTML = `
    <th>${escapeHtml(t("planMatrixType"))}</th>
    ${plans.map((plan) => `<th>${escapeHtml(getPlanDisplayName(plan))}</th>`).join("")}
  `;
  if (!plans.length) {
    matrixBody.innerHTML = `
      <tr>
        <td colspan="1"><span class="plan-empty">${escapeHtml(t("planNoData"))}</span></td>
      </tr>
    `;
    return;
  }
  matrixBody.innerHTML = PLAN_TIMETABLE_ROWS.map((row) => `
    <tr>
      <th scope="row">
        <strong>${escapeHtml(row.labelKey ? t(row.labelKey) : row.window)}</strong>
        ${row.labelKey && row.window ? `<span>${escapeHtml(row.window)}</span>` : ""}
      </th>
      ${plans.map((plan) => `<td ${getPlanTimetableCellAttributes(plan, row)}>${renderPlanTimetableCell(plan, row)}</td>`).join("")}
    </tr>
  `).join("");
}

function renderPlanChangeHistory(changes) {
  if (!Array.isArray(changes) || !changes.length) {
    return `<div class="plan-empty">${escapeHtml(t("planNoRateChanges"))}</div>`;
  }
  return changes.map((change) => {
    const metrics = Array.isArray(change.changes) && change.changes.length ? change.changes : Array.isArray(change.rates) ? change.rates : [];
    const unchanged = Array.isArray(change.unchanged) ? change.unchanged : [];
    return `
      <article class="plan-change-item">
        <div class="plan-change-head">
          <div>
            <h3>${escapeHtml(change.title || "-")}</h3>
            <span>${escapeHtml(change.period || "-")}</span>
          </div>
        </div>
        ${change.summary ? `<p class="plan-change-summary">${escapeHtml(change.summary)}</p>` : ""}
        ${metrics.length ? `
          <div class="plan-change-metrics">
            ${metrics.map((item) => {
              const label = item.label_key ? t(item.label_key) : String(item.label || "-");
              const valueText = item.from !== undefined || item.to !== undefined
                ? t("planChangeFromTo", {
                    from: formatPlanRateValue(item.from, item.unit),
                    to: formatPlanRateValue(item.to, item.unit),
                  })
                : formatPlanRateValue(item.value, item.unit);
              return `
                <div class="plan-change-metric">
                  <span>${escapeHtml(label)}</span>
                  <strong>${escapeHtml(valueText)}</strong>
                </div>
              `;
            }).join("")}
          </div>
        ` : ""}
        ${unchanged.length ? `
          <ul class="plan-change-unchanged">
            ${unchanged.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
          </ul>
        ` : ""}
        ${change.source ? `<p class="plan-change-source">${escapeHtml(t("planChangeSource", { source: change.source }))}</p>` : ""}
      </article>
    `;
  }).join("");
}

function renderHomePlan(payload = stateCache.lastHomePlan) {
  const planMeta = document.getElementById("planMeta");
  const planUpdatedAt = document.getElementById("planUpdatedAt");
  const summaryGrid = document.getElementById("planSummaryGrid");
  const matrixBody = document.getElementById("planMatrixBody");
  const changeList = document.getElementById("planChangeList");
  const notes = document.getElementById("planNotes");
  if (!summaryGrid || !matrixBody || !changeList) return;

  const plans = Array.isArray(payload?.plans) ? payload.plans : [];
  const home = payload?.home || {};
  if (planMeta) {
    planMeta.textContent = payload
      ? t("planRecordedAt", {
          date: payload.recorded_at_local || "-",
          distributor: home.distributor || "-",
          postcode: home.postcode || "-",
        })
      : t("planNoData");
  }
  if (planUpdatedAt) planUpdatedAt.textContent = payload?.recorded_at_local ? t("planUpdatedAt", { date: payload.recorded_at_local }) : "-";

  if (!plans.length) {
    summaryGrid.innerHTML = `<div class="plan-empty">${escapeHtml(t("planNoData"))}</div>`;
    renderPlanMatrix([]);
    changeList.innerHTML = renderPlanChangeHistory([]);
    if (notes) notes.innerHTML = "";
    return;
  }

  const retailers = [...new Set(plans.map((plan) => plan.retailer).filter(Boolean))].join(", ") || "-";
  const statusText = home.address_status === "needs_house_number_confirmation"
    ? t("planAddressNeedsCheck")
    : home.address_status === "confirmed_by_bills"
      ? t("planAddressConfirmed")
      : "-";
  summaryGrid.innerHTML = [
    { label: t("planSummaryRetailer"), value: retailers },
    { label: t("planSummaryDistributor"), value: home.distributor || "-" },
    { label: t("planSummaryPostcode"), value: home.postcode || "-" },
    { label: t("planSummaryStatus"), value: statusText },
  ].map((item) => `
    <div class="plan-summary-item">
      <span>${escapeHtml(item.label)}</span>
      <strong>${escapeHtml(item.value)}</strong>
    </div>
  `).join("");

  renderPlanMatrix(plans);

  changeList.innerHTML = renderPlanChangeHistory(payload?.rate_change_history);

  const planNotes = Array.isArray(payload?.notes) ? payload.notes : [];
  if (notes) notes.innerHTML = planNotes.map((note) => `<p>${escapeHtml(note)}</p>`).join("");
}

function formatRelativeAgo(isoText) {
  if (!isoText) return "-";
  const dt = new SiteDate(isoText);
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

function isMobileDashboardLayout() {
  return typeof window !== "undefined"
    && typeof window.matchMedia === "function"
    && window.matchMedia("(max-width: 700px)").matches;
}

function readCachedDashboardSummary() {
  try {
    const raw = localStorage.getItem(DASHBOARD_SUMMARY_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function writeCachedDashboardSummary(summary) {
  try {
    if (!summary || typeof summary !== "object") {
      localStorage.removeItem(DASHBOARD_SUMMARY_CACHE_KEY);
      return;
    }
    localStorage.setItem(DASHBOARD_SUMMARY_CACHE_KEY, JSON.stringify(summary));
  } catch {
    // Keep runtime cache even if storage is unavailable.
  }
}

function hasCombinedSnapshot(combinedFlow) {
  if (!combinedFlow || typeof combinedFlow !== "object") return false;
  if (Array.isArray(combinedFlow?.display?.order) && combinedFlow.display.order.length > 0) return true;
  if (combinedFlow?.updated_at) return true;
  const metrics = combinedFlow?.metrics;
  return Boolean(metrics && typeof metrics === "object" && Object.keys(metrics).length);
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
  const prev = stateCache.systemLoadMeta[system] || { phase: "idle", updatedAt: null, quality: "ok", count: 0, refreshing: false, issueText: "" };
  stateCache.systemLoadMeta[system] = {
    phase: patch.phase || prev.phase,
    updatedAt: Object.prototype.hasOwnProperty.call(patch, "updatedAt") ? patch.updatedAt : prev.updatedAt,
    quality: patch.quality || prev.quality,
    count: Number.isFinite(Number(patch.count)) ? Number(patch.count) : prev.count,
    refreshing: Object.prototype.hasOwnProperty.call(patch, "refreshing") ? Boolean(patch.refreshing) : prev.refreshing,
    issueText: Object.prototype.hasOwnProperty.call(patch, "issueText") ? String(patch.issueText || "") : String(prev.issueText || ""),
  };
  renderSystemLoadMeta(system);
}

function renderSystemLoadMeta(system) {
  const meta = stateCache.systemLoadMeta[system] || { phase: "idle", updatedAt: null, quality: "ok", count: 0, refreshing: false, issueText: "" };
  const updatedId = flowId(system, "updatedAt");
  const spinnerId = flowId(system, "loadingSpinner");
  const updatedText = formatUpdatedAt(meta.updatedAt);
  const combinedLate = system === "combined" && meta.phase === "done" && isIsoOlderThanSeconds(meta.updatedAt, 300);
  const issueText = String(meta.issueText || "").trim();
  let lineText = t("flowMetaIdle");
  if (meta.refreshing) {
    lineText = t("flowMetaRefreshing", { updated: updatedText });
  } else if (meta.phase === "loading") {
    lineText = t("flowMetaLoading");
  } else if (meta.phase === "failed") {
    lineText = t("flowMetaFailed", { updated: updatedText });
  } else if (meta.phase === "done") {
    if (combinedLate) lineText = t("flowMetaDoneStaleCombined", { count: meta.count, updated: updatedText });
    else if (meta.quality === "partial") lineText = t("flowMetaDonePartial", { count: meta.count, updated: updatedText });
    else lineText = t("flowMetaDoneOk", { updated: updatedText });
  }
  if (issueText && (combinedLate || meta.phase === "failed" || meta.quality === "failed" || system === "combined")) {
    lineText = `${lineText} · ${t("flowMetaIssueReason", { reason: issueText })}`;
  }
  setText(updatedId, lineText);
  const updatedEl = document.getElementById(updatedId);
  if (updatedEl) {
    updatedEl.classList.toggle("flow-meta-stale", combinedLate || Boolean(issueText));
    if (issueText) updatedEl.setAttribute("title", issueText);
    else updatedEl.removeAttribute("title");
  }
  const spinner = document.getElementById(spinnerId);
  if (spinner) {
    spinner.classList.toggle("is-hidden", meta.phase !== "loading" && !meta.refreshing);
  }
  if (system === "combined") renderMobileDashboardFreshness(meta, combinedLate);
}

function renderMobileDashboardFreshness(meta, isStale = false) {
  const shell = document.getElementById("combinedMobileFreshness");
  const stateEl = document.getElementById("combinedMobileFreshnessState");
  const timeEl = document.getElementById("combinedMobileFreshnessTime");
  const reasonEl = document.getElementById("combinedMobileFreshnessReason");
  if (!shell || !stateEl || !timeEl) return;
  let stateText = t("mobileDashboardFreshnessFresh");
  let tone = "fresh";
  if (meta?.refreshing) {
    stateText = t("mobileDashboardFreshnessRefreshing");
    tone = isStale ? "stale" : "refreshing";
  } else if (meta?.phase === "loading" || meta?.phase === "idle") {
    stateText = t("mobileDashboardFreshnessLoading");
    tone = "loading";
  } else if (meta?.phase === "failed") {
    stateText = t("mobileDashboardFreshnessFailed");
    tone = "failed";
  } else if (isStale) {
    stateText = t("mobileDashboardFreshnessStale");
    tone = "stale";
  }
  shell.className = `mobile-dashboard-freshness is-${tone}`;
  stateEl.textContent = stateText;
  timeEl.textContent = formatDateTimeWithAgo(meta?.updatedAt || null);
  const issueText = String(meta?.issueText || "").trim();
  if (reasonEl) {
    const showReason = Boolean(issueText);
    reasonEl.textContent = showReason ? t("mobileDashboardFreshnessReasonPrefix", { reason: issueText }) : "";
    reasonEl.classList.toggle("hidden", !showReason);
  }
}

function applyTranslations() {
  if (isMobileViewport()) currentLang = "en";
  document.documentElement.lang = currentLang === "zh" ? "zh-CN" : "en";
  const langSelect = document.getElementById("langSelect");
  if (langSelect) langSelect.value = currentLang;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.getAttribute("data-i18n");
    if (!key) return;
    if (!hasTranslation(key)) return;
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
    if (!hasTranslation(key)) return;
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
  renderHomePlan();
  rerenderSamplingViewFromCache();
  rerenderDatabaseViewFromCache();
  if (stateCache.lastOperationTimeline) renderOperationTimelinePage(stateCache.lastOperationTimeline);
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
  stateCache.lastConfig = payload && typeof payload === "object" ? payload : {};
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
  const detailRetentionMonths = Number(payload.detail_retention_months);
  document.getElementById("cfgDetailRetentionMonths").value = String(
    CONFIG_DETAIL_RETENTION_OPTIONS.includes(detailRetentionMonths) ? detailRetentionMonths : 2
  );
  if (payload.weather_lat != null) document.getElementById("cfgWeatherLat").value = payload.weather_lat;
  if (payload.weather_lon != null) document.getElementById("cfgWeatherLon").value = payload.weather_lon;
  renderDashboardReserveFloorControl(null, payload);
  cacheSajProfileFromConfig(payload);
}

function normalizeSajProfileId(profileId) {
  const normalized = String(profileId || "").trim();
  return SAJ_PROFILE_IDS.includes(normalized) ? normalized : "self_consumption";
}

function cacheSajProfileFromConfig(payload = {}) {
  const selectedProfile = normalizeSajProfileId(payload.saj_target_profile);
  const previous = stateCache.lastSajProfile?.profile_state || stateCache.lastSajProfile || {};
  const previousMap = previous && typeof previous === "object" ? previous : {};
  stateCache.lastSajProfile = {
    ...previousMap,
    selected_profile: selectedProfile,
    input_profile: previousMap.input_profile || selectedProfile,
    effective_profile: previousMap.actual_profile || selectedProfile,
    source: previousMap.source || "config_cache",
  };
}

function hasConfigValue(value) {
  return typeof value === "string" ? value.trim().length > 0 : Boolean(value);
}

function isConfigComplete(payload = {}) {
  return hasConfigValue(payload.ha_url)
    && hasConfigValue(payload.ha_token)
    && hasConfigValue(payload.solplanet_inverter_sn)
    && hasConfigValue(payload.solplanet_battery_sn);
}

function buildConfigPayloadFromForm() {
  const sajInterval = Number(document.getElementById("cfgSajSampleIntervalSeconds").value);
  const solplanetInterval = Number(document.getElementById("cfgSolplanetSampleIntervalSeconds").value);
  const detailRetentionMonths = Number(document.getElementById("cfgDetailRetentionMonths").value);
  const reserveFloorKwh = toFiniteNumber(document.getElementById("combinedReserveFloorInput")?.value)
    ?? toFiniteNumber(stateCache.lastConfig?.solplanet_export_reserve_floor_kwh)
    ?? 18;
  return {
    ha_url: document.getElementById("cfgHaUrl").value.trim(),
    ha_token: document.getElementById("cfgHaToken").value.trim(),
    solplanet_dongle_host: document.getElementById("cfgDongleHost").value.trim(),
    saj_sample_interval_seconds: CONFIG_SAMPLE_INTERVAL_OPTIONS.includes(sajInterval) ? sajInterval : 5,
    solplanet_sample_interval_seconds: CONFIG_SAMPLE_INTERVAL_OPTIONS.includes(solplanetInterval)
      ? solplanetInterval
      : 60,
    detail_retention_months: CONFIG_DETAIL_RETENTION_OPTIONS.includes(detailRetentionMonths) ? detailRetentionMonths : 2,
    weather_lat: parseFloat(document.getElementById("cfgWeatherLat").value) || null,
    weather_lon: parseFloat(document.getElementById("cfgWeatherLon").value) || null,
    solplanet_export_reserve_floor_kwh: reserveFloorKwh,
  };
}

async function ensureConfigReady() {
  const saveMsg = document.getElementById("configSaveMsg");
  try {
    const current = await fetchJson("/api/config", { timeoutMs: 5000 });
    fillConfigForm(current);
    if (isConfigComplete(current)) {
      configReady = true;
      setConfigModalVisible(false);
      return true;
    }
    configReady = false;
    setConfigModalVisible(true);
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
  solplanet: 50,
};

const BATTERY_MIN_DISCHARGE_SOC = {
  saj: 20,
  solplanet: 10,
};

const SOLPLANET_WINDOW_FORECASTS = [
  { labelKey: "batteryWindowForecastFreeLabel", startHour: 11, endHour: 15 },
  { labelKey: "batteryWindowForecastDischargeLabel", startHour: 16, endHour: 23 },
];

const POWER_FLOW_ACTIVE_THRESHOLD_W = 30;
const POOL_PUMP_POWER_STALE_SECONDS = 30 * 60;
const PUMP_STATUS_OFF = 0;
const PUMP_STATUS_ON = 1;
const PUMP_STATUS_UNAVAILABLE = -1;
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

function formatRuntimeBudgetFormula(runtimeBudget) {
  const remainingBaseKwh = Number(runtimeBudget?.remaining_base_kwh);
  const currentHomeLoadW = Number(runtimeBudget?.current_home_load_w);
  const pumpPowerW = Number(runtimeBudget?.pump_power_w);
  const teslaRuntimePowerW = Number(runtimeBudget?.tesla_runtime_power_w);
  const runtimeLoadW = Number(runtimeBudget?.runtime_load_w);
  const availableHours = Number(runtimeBudget?.available_runtime_hours);
  const targetHours = Number(runtimeBudget?.target_runtime_hours);
  const elapsedHours = Number(runtimeBudget?.elapsed_runtime_hours);
  const remainingHours = Number(runtimeBudget?.remaining_runtime_hours);
  const runtimeFraction = Number(runtimeBudget?.runtime_fraction);
  const pumpPowerKw = Number.isFinite(pumpPowerW) ? pumpPowerW / 1000 : NaN;
  const currentHomeLoadKw = Number.isFinite(currentHomeLoadW) ? currentHomeLoadW / 1000 : NaN;
  const teslaRuntimePowerKw = Number.isFinite(teslaRuntimePowerW) ? teslaRuntimePowerW / 1000 : NaN;
  const runtimeLoadKw = Number.isFinite(runtimeLoadW) ? runtimeLoadW / 1000 : NaN;
  const hasTeslaRuntimeLoad = Number.isFinite(teslaRuntimePowerKw) && teslaRuntimePowerKw > 0;
  const lines = [];

  if (
    Number.isFinite(availableHours)
    && Number.isFinite(remainingBaseKwh)
    && Number.isFinite(runtimeLoadKw)
    && runtimeLoadKw > 0
    && Number.isFinite(pumpPowerKw)
    && Number.isFinite(currentHomeLoadKw)
  ) {
    const denominatorText = hasTeslaRuntimeLoad
      ? `(${formatTrimmedDecimal(pumpPowerKw, 2)}kW + ${formatTrimmedDecimal(currentHomeLoadKw, 2)}kW + ${formatTrimmedDecimal(teslaRuntimePowerKw, 2)}kW)`
      : `(${formatTrimmedDecimal(pumpPowerKw, 2)}kW + ${formatTrimmedDecimal(currentHomeLoadKw, 2)}kW)`;
    const formulaText = hasTeslaRuntimeLoad
      ? "pump_power + home_load + tesla_runtime_power"
      : "pump_power + home_load";
    lines.push(
      `available = usable_remaining_battery / (${formulaText}) = ${formatTrimmedDecimal(remainingBaseKwh, 2)}kWh / ${denominatorText} = ${formatTrimmedDecimal(remainingBaseKwh, 2)}kWh / ${formatTrimmedDecimal(runtimeLoadKw, 2)}kW = ${formatTrimmedDecimal(availableHours, 2)}h`
    );
  }
  if (Number.isFinite(targetHours) && Number.isFinite(availableHours) && Number.isFinite(runtimeFraction)) {
    lines.push(
      `target = available x ${formatTrimmedDecimal(runtimeFraction * 100, 0)}% = ${formatTrimmedDecimal(availableHours, 2)}h x ${formatTrimmedDecimal(runtimeFraction, 2)} = ${formatTrimmedDecimal(targetHours, 2)}h`
    );
  }
  if (Number.isFinite(remainingHours) && Number.isFinite(targetHours) && Number.isFinite(elapsedHours)) {
    lines.push(
      `remaining = target - elapsed = ${formatTrimmedDecimal(targetHours, 2)}h - ${formatTrimmedDecimal(elapsedHours, 2)}h = ${formatTrimmedDecimal(remainingHours, 2)}h`
    );
  }

  return lines.join("\n");
}

function formatBatteryEnergyKwh(system, batterySoc) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  if (!Number.isFinite(capacityKwh)) return "-";
  if (batterySoc === null || batterySoc === undefined) return `- / ${formatTrimmedDecimal(capacityKwh, 1)} kWh`;
  const clampedSoc = Math.max(0, Math.min(100, Number(batterySoc)));
  const currentKwh = (capacityKwh * clampedSoc) / 100;
  return `${formatTrimmedDecimal(currentKwh, 1)} / ${formatTrimmedDecimal(capacityKwh, 1)} kWh`;
}

function formatBatteryCurrentEnergyKwh(system, batterySoc) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  if (!Number.isFinite(capacityKwh) || batterySoc === null || batterySoc === undefined) return "- kWh";
  const clampedSoc = Math.max(0, Math.min(100, Number(batterySoc)));
  const currentKwh = (capacityKwh * clampedSoc) / 100;
  return `${formatTrimmedDecimal(currentKwh, 1)} kWh`;
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

function batteryUsableKwh(system, batterySoc) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  const minDischargeSoc = BATTERY_MIN_DISCHARGE_SOC[system] ?? 0;
  const numericSoc = toFiniteNumber(batterySoc);
  if (!Number.isFinite(capacityKwh) || numericSoc === null) return null;
  const clampedSoc = Math.max(0, Math.min(100, numericSoc));
  const usableSoc = Math.max(0, clampedSoc - minDischargeSoc);
  return (capacityKwh * usableSoc) / 100;
}

function formatBatteryRuntimeTargetTime(targetAt) {
  if (!(targetAt instanceof Date) || Number.isNaN(targetAt.getTime())) return null;
  const now = new SiteDate();
  const timeText = targetAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const sameDay =
    targetAt.getFullYear() === now.getFullYear() &&
    targetAt.getMonth() === now.getMonth() &&
    targetAt.getDate() === now.getDate();
  if (sameDay) return timeText;
  return currentLang === "zh" ? `明天 ${timeText}` : `tmr ${timeText}`;
}

function getNextWindowEndLocalTime(endHour, now = new SiteDate()) {
  const candidate = new SiteDate(now);
  candidate.setHours(endHour, 0, 0, 0);
  if (candidate.getTime() <= now.getTime()) {
    candidate.setDate(candidate.getDate() + 1);
  }
  return candidate;
}

function isLocalTimeInHourWindow(now, startHour, endHour) {
  const hour = now.getHours();
  if (!Number.isFinite(startHour) || !Number.isFinite(endHour)) return false;
  if (startHour === endHour) return true;
  if (startHour < endHour) {
    return hour >= startHour && hour < endHour;
  }
  return hour >= startHour || hour < endHour;
}

function projectBatterySocAtTarget(system, batterySoc, batteryW, targetAt) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  if (!Number.isFinite(capacityKwh)) return null;
  const socValue = Number(batterySoc);
  const powerW = Number(batteryW);
  if (!Number.isFinite(socValue) || !Number.isFinite(powerW)) return null;
  if (!(targetAt instanceof Date) || Number.isNaN(targetAt.getTime())) return null;
  const now = Date.now();
  const targetMs = targetAt.getTime();
  if (targetMs <= now) return null;
  const hoursToTarget = (targetMs - now) / 3600000;
  const currentSoc = Math.max(0, Math.min(100, socValue));
  const deltaSoc = ((-powerW / 1000) * hoursToTarget * 100) / capacityKwh;
  const projectedSoc = Math.max(0, Math.min(100, currentSoc + deltaSoc));
  return {
    socPercent: projectedSoc,
    energyKwh: (capacityKwh * projectedSoc) / 100,
  };
}

function formatSolplanetWindowForecastHtml(batterySoc, batteryW) {
  const now = new SiteDate();
  const activeWindows = SOLPLANET_WINDOW_FORECASTS.filter((item) => (
    isLocalTimeInHourWindow(now, Number(item.startHour), Number(item.endHour))
  ));
  if (!activeWindows.length) return "";

  const lines = activeWindows.map((item) => {
    const targetAt = getNextWindowEndLocalTime(item.endHour);
    const projection = projectBatterySocAtTarget("solplanet", batterySoc, batteryW, targetAt);
    const windowLabel = t(item.labelKey);
    if (!projection) {
      return (
        `<div class="battery-window-forecast-item">` +
        `<span class="battery-window-forecast-compact">${escapeHtml(t("batteryWindowForecastCompactNoData", { window: windowLabel }))}</span>` +
        `</div>`
      );
    }
    const socText = `${formatTrimmedDecimal(projection.socPercent, 0)}%`;
    return (
      `<div class="battery-window-forecast-item">` +
      `<span class="battery-window-forecast-compact">${escapeHtml(t("batteryWindowForecastCompact", { window: windowLabel, soc: socText }))}</span>` +
      `</div>`
    );
  });
  return lines.join("");
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
    const targetAt = new SiteDate(Date.now() + runtimeHours * 3600 * 1000);
    const timeText = formatBatteryRuntimeTargetTime(targetAt);
    if (!timeText) return null;
    return { mode: "charging", runtimeHours, timeText };
  }

  const usableSoc = Math.max(0, clampedSoc - minDischargeSoc);
  if (usableSoc <= 0) return null;
  const usableKwh = (capacityKwh * usableSoc) / 100;
  const runtimeHours = usableKwh / (powerW / 1000);
  if (!Number.isFinite(runtimeHours) || runtimeHours < 0) return null;
  const targetAt = new SiteDate(Date.now() + runtimeHours * 3600 * 1000);
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

function formatTeslaSocRateEstimate(totalCapacityKwh, chargingW) {
  const capacityKwh = toFiniteNumber(totalCapacityKwh);
  const powerW = toFiniteNumber(chargingW);
  if (capacityKwh === null || capacityKwh <= 0 || powerW === null) return "";
  if (Math.abs(powerW) < POWER_FLOW_ACTIVE_THRESHOLD_W) return "";
  const percentPerHour = (Math.abs(powerW) * 100) / (capacityKwh * 1000);
  if (!Number.isFinite(percentPerHour) || percentPerHour < 0) return "";
  return t("batteryRateCharging", { value: formatTrimmedDecimal(percentPerHour, 1) });
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
const COMBINED_COMPACT_LOAD_NODE_SIZE = {
  width: 184,
  height: 150,
};
const COMBINED_TESLA_NODE_SIZE = {
  width: 184,
  height: 210,
};
const COMBINED_DIAGRAM_VIEWPORT = { width: 1340, height: 720 };
const COMBINED_LAYOUT = {
  nodes: {
    grid: { x: 582, y: 594 },
    solar: { x: 8, y: 594 },
    usableBattery: { x: 1086, y: 594 },
    switchboard: { x: 558, y: 318 },
    load: { x: 354, y: 48 },
    poolPump: { x: 578, y: 48 },
    tesla: { x: 802, y: 18 },
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
      points: [{ x: 670, y: 318 }, { x: 670, y: 252 }],
      labelPosition: { x: 718, y: 288 },
      lineCap: "butt",
      lineJoin: "miter",
    },
    switchboardToTesla: {
      points: [{ x: 670, y: 252 }, { x: 894, y: 252 }, { x: 894, y: 192 }],
      labelPosition: { x: 820, y: 268 },
      lineCap: "butt",
      lineJoin: "miter",
      tailUpperGapLength: 9,
      tailRoundedCorner: true,
    },
    switchboardToHomeLoad: {
      points: [{ x: 670, y: 252 }, { x: 446, y: 252 }, { x: 446, y: 192 }],
      labelPosition: { x: 520, y: 268 },
      lineCap: "butt",
      lineJoin: "miter",
      tailUpperGapLength: 9,
      tailRoundedCorner: true,
    },
    switchboardToPoolPump: {
      points: [{ x: 670, y: 252 }, { x: 670, y: 192 }],
      labelPosition: { x: 704, y: 238 },
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
  const compact = typeof window !== "undefined" && window.matchMedia?.("(max-width: 700px)")?.matches;
  const rotatedViewport = {
    width: COMBINED_DIAGRAM_VIEWPORT.height,
    height: 1100,
  };
  const viewport = compact ? rotatedViewport : COMBINED_DIAGRAM_VIEWPORT;
  const coordinateSystem = compact ? "top-left" : "bottom-left";
  const desktopNodeSizes = {
    source: { width: 176, height: 102 },
    usableBattery: { width: 230, height: 102 },
    switchboard: { width: 224, height: 152 },
    compactLoad: COMBINED_COMPACT_LOAD_NODE_SIZE,
    poolPump: COMBINED_COMPACT_LOAD_NODE_SIZE,
    tesla: COMBINED_TESLA_NODE_SIZE,
    battery: SAJ_COMBINED_BATTERY_NODE_SIZE,
    inverter1: { width: 180, height: 108 },
    inverter2: { width: 176, height: 120 },
  };
  const compactNodeSizes = {
    ...desktopNodeSizes,
    switchboard: { ...desktopNodeSizes.switchboard, width: 136, height: 124 },
    compactLoad: { ...COMBINED_COMPACT_LOAD_NODE_SIZE, height: 116 },
    poolPump: { ...COMBINED_COMPACT_LOAD_NODE_SIZE, height: 116 },
    tesla: { width: 212, height: 198 },
    battery: { ...SAJ_COMBINED_BATTERY_NODE_SIZE, height: 208 },
    inverter1: { ...desktopNodeSizes.inverter1, width: 152 },
    inverter2: { ...desktopNodeSizes.inverter2, width: 152 },
  };
  const rotateDesktopPoint = (point) => ({
    x: point.y,
    y: point.x,
  });
  const spreadRotatedPoint = (point) => {
    if (!compact) return point;
    const center = {
      x: rotatedViewport.width / 2,
      y: rotatedViewport.height / 2,
    };
    const xSpread = 0.94;
    const ySpread = 0.74;
    const xOffset = rotatedViewport.width * -0.049;
    const yOffset = rotatedViewport.height * -0.082;
    return {
      x: center.x + ((point.x - center.x) * xSpread) + xOffset,
      y: center.y + ((point.y - center.y) * ySpread) + yOffset,
    };
  };
  const rotateDesktopNode = (point, desktopSize, renderSize = desktopSize) => {
    const center = {
      x: point.x + (desktopSize.width / 2),
      y: point.y + (desktopSize.height / 2),
    };
    const rotatedCenter = spreadRotatedPoint(rotateDesktopPoint(center));
    return {
      x: Math.round(rotatedCenter.x - (renderSize.width / 2)),
      y: Math.round(rotatedCenter.y - (renderSize.height / 2)),
    };
  };
  const compactLabelNudges = {
    gridToSwitchboard: { x: -64, y: -108 },
    solarToInverter1: { x: -52, y: -102 },
    switchboardToTotalLoad: { x: 72, y: 74 },
    battery1ToInverter1: { x: -38, y: 150 },
    battery2ToInverter2: { x: -40, y: -118 },
  };
  const rotateDesktopEdge = (edge, key = "") => {
    const labelPosition = spreadRotatedPoint(rotateDesktopPoint(edge.labelPosition));
    const nudge = compactLabelNudges[key] || { x: 0, y: 0 };
    return {
      ...edge,
      points: edge.points.map((point) => spreadRotatedPoint(rotateDesktopPoint(point))),
      labelPosition: {
        x: labelPosition.x + nudge.x,
        y: labelPosition.y + nudge.y,
      },
    };
  };
  const compactBatteryBridgeEdge = (batteryKey, inverterKey, side, labelSide = null) => {
    const battery = nodePositions[batteryKey];
    const inverter = nodePositions[inverterKey];
    const batterySize = nodeSizes.battery;
    const inverterSize = nodeSizes[inverterKey];
    const horizontalY = side === "top"
      ? Math.max(4, Math.min(battery.y, inverter.y) - 56)
      : Math.min(viewport.height - 18, Math.max(battery.y + batterySize.height, inverter.y + inverterSize.height) + 34);
    const resolvedLabelSide = labelSide || (side === "top" ? "above" : "below");
    const labelY = resolvedLabelSide === "above" ? horizontalY - 3 : horizontalY + 12;
    const source = {
      x: battery.x + (batterySize.width / 2),
      y: side === "top" ? battery.y : battery.y + batterySize.height,
    };
    const target = {
      x: inverter.x + (inverterSize.width / 2),
      y: side === "top" ? inverter.y : inverter.y + inverterSize.height,
    };
    return {
      points: [
        source,
        { x: source.x, y: horizontalY },
        { x: target.x, y: horizontalY },
        target,
      ],
      labelPosition: {
        x: (source.x + target.x) / 2,
        y: labelY,
      },
    };
  };
  const compactSwitchboardLoadEdge = (loadKey, switchboardSide) => {
    const switchboard = nodePositions.switchboard;
    const load = nodePositions[loadKey];
    const switchboardSize = nodeSizes.switchboard;
    const loadSize = nodeSizes[loadKey === "tesla" ? "tesla" : loadKey === "poolPump" ? "poolPump" : "compactLoad"];
    const loadAnchor = {
      x: load.x + loadSize.width,
      y: load.y + (loadSize.height / 2),
    };

    if (switchboardSide === "top") {
      const switchAnchor = {
        x: switchboard.x + (switchboardSize.width * 0.28),
        y: switchboard.y,
      };
      return {
        points: [
          switchAnchor,
          { x: switchAnchor.x, y: loadAnchor.y },
          loadAnchor,
        ],
        labelPosition: {
          x: (switchAnchor.x + loadAnchor.x) / 2,
          y: loadAnchor.y - 26,
        },
      };
    }

    if (switchboardSide === "bottom") {
      const switchAnchor = {
        x: switchboard.x + (switchboardSize.width * 0.28),
        y: switchboard.y + switchboardSize.height,
      };
      return {
        points: [
          switchAnchor,
          { x: switchAnchor.x, y: loadAnchor.y },
          loadAnchor,
        ],
        labelPosition: {
          x: (switchAnchor.x + loadAnchor.x) / 2,
          y: loadAnchor.y - 24,
        },
      };
    }

    return {
      points: [
        { x: switchboard.x, y: loadAnchor.y },
        loadAnchor,
      ],
      labelPosition: {
        x: (switchboard.x + loadAnchor.x) / 2,
        y: loadAnchor.y - 24,
      },
    };
  };
  const compactGridToSwitchboardEdge = () => {
    const grid = nodePositions.grid;
    const switchboard = nodePositions.switchboard;
    const gridSize = nodeSizes.source;
    const switchboardSize = nodeSizes.switchboard;
    const source = {
      x: grid.x + (gridSize.width / 2),
      y: grid.y + gridSize.height,
    };
    const target = {
      x: switchboard.x + switchboardSize.width,
      y: switchboard.y + (switchboardSize.height / 2),
    };
    return {
      points: [
        source,
        { x: source.x, y: target.y },
        target,
      ],
      labelPosition: {
        x: (source.x + target.x) / 2,
        y: target.y + 28,
      },
    };
  };
  const compactInverterToSwitchboardEdge = (inverterKey, switchboardSide) => {
    const inverter = nodePositions[inverterKey];
    const switchboard = nodePositions.switchboard;
    const inverterSize = nodeSizes[inverterKey];
    const switchboardSize = nodeSizes.switchboard;
    const laneX = switchboard.x + (switchboardSize.width * 0.72);
    const source = {
      x: laneX,
      y: switchboardSide === "top" ? inverter.y + inverterSize.height : inverter.y,
    };
    const target = {
      x: laneX,
      y: switchboardSide === "top" ? switchboard.y : switchboard.y + switchboardSize.height,
    };
    return {
      points: [source, target],
      labelPosition: {
        x: source.x + (inverterKey === "inverter1" ? 0 : -34),
        y: (source.y + target.y) / 2,
      },
    };
  };
  const nodePositions = compact
    ? {
        solar: rotateDesktopNode(layout.nodes.solar, desktopNodeSizes.source),
        grid: rotateDesktopNode(layout.nodes.grid, desktopNodeSizes.source),
        usableBattery: rotateDesktopNode(layout.nodes.usableBattery, desktopNodeSizes.usableBattery),
        switchboard: rotateDesktopNode(layout.nodes.switchboard, desktopNodeSizes.switchboard, compactNodeSizes.switchboard),
        load: rotateDesktopNode(layout.nodes.load, desktopNodeSizes.compactLoad),
        poolPump: rotateDesktopNode(layout.nodes.poolPump, desktopNodeSizes.poolPump),
        tesla: rotateDesktopNode(layout.nodes.tesla, desktopNodeSizes.tesla, compactNodeSizes.tesla),
        battery1: rotateDesktopNode(layout.nodes.battery1, desktopNodeSizes.battery),
        inverter1: rotateDesktopNode(layout.nodes.inverter1, desktopNodeSizes.inverter1),
        inverter2: rotateDesktopNode(layout.nodes.inverter2, desktopNodeSizes.inverter2),
        battery2: rotateDesktopNode(layout.nodes.battery2, desktopNodeSizes.battery),
      }
    : layout.nodes;
  const nodeSizes = compact
    ? compactNodeSizes
    : desktopNodeSizes;
  if (compact) {
    nodePositions.grid.y -= 156;
    nodePositions.switchboard.x += 24;
    nodePositions.battery2.y += 22;
  }
  const edgeLayout = compact
    ? {
        gridToSwitchboard: rotateDesktopEdge(layout.edges.gridToSwitchboard, "gridToSwitchboard"),
        solarToInverter1: rotateDesktopEdge(layout.edges.solarToInverter1, "solarToInverter1"),
        switchboardToTotalLoad: rotateDesktopEdge(layout.edges.switchboardToTotalLoad, "switchboardToTotalLoad"),
        switchboardToTesla: rotateDesktopEdge(layout.edges.switchboardToTesla, "switchboardToTesla"),
        switchboardToHomeLoad: rotateDesktopEdge(layout.edges.switchboardToHomeLoad, "switchboardToHomeLoad"),
        switchboardToPoolPump: rotateDesktopEdge(layout.edges.switchboardToPoolPump, "switchboardToPoolPump"),
        battery1ToInverter1: rotateDesktopEdge(layout.edges.battery1ToInverter1, "battery1ToInverter1"),
        battery2ToInverter2: rotateDesktopEdge(layout.edges.battery2ToInverter2, "battery2ToInverter2"),
        inverter1ToSwitchboard: rotateDesktopEdge(layout.edges.inverter1ToSwitchboard, "inverter1ToSwitchboard"),
        inverter2ToSwitchboard: rotateDesktopEdge(layout.edges.inverter2ToSwitchboard, "inverter2ToSwitchboard"),
      }
    : layout.edges;
  if (compact) {
    edgeLayout.gridToSwitchboard = compactGridToSwitchboardEdge();
    edgeLayout.battery1ToInverter1 = compactBatteryBridgeEdge("battery1", "inverter1", "top");
    edgeLayout.battery2ToInverter2 = compactBatteryBridgeEdge("battery2", "inverter2", "bottom");
    edgeLayout.battery1ToInverter1.labelPosition.x = edgeLayout.battery2ToInverter2.labelPosition.x;
    edgeLayout.switchboardToHomeLoad = compactSwitchboardLoadEdge("load", "top");
    edgeLayout.switchboardToPoolPump = compactSwitchboardLoadEdge("poolPump", "left");
    edgeLayout.switchboardToTesla = compactSwitchboardLoadEdge("tesla", "bottom");
    edgeLayout.inverter1ToSwitchboard = compactInverterToSwitchboardEdge("inverter1", "top");
    edgeLayout.inverter2ToSwitchboard = compactInverterToSwitchboardEdge("inverter2", "bottom");
  }
  return {
    layout: "explicit",
    coordinateSystem,
    fixedViewport: true,
    showViewportFrame: false,
    viewport,
    nodes: [
      {
        id: "combined-gridNode",
        kind: "grid",
        icon: "grid",
        title: "Grid",
        titleKey: "gridTitle",
        width: nodeSizes.source.width,
        height: nodeSizes.source.height,
        position: nodePositions.grid,
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
        width: nodeSizes.source.width,
        height: nodeSizes.source.height,
        position: nodePositions.solar,
        lines: [
          { id: "combined-solarPowerValue", className: "node-value", text: "-" },
          { id: "combined-solarState", className: "node-state muted", text: "idle" },
        ],
      },
      {
        id: "combined-usableBatteryNode",
        kind: "load",
        icon: "battery",
        title: "Usable Battery",
        titleKey: "dashboardUsableBatteryTitle",
        width: nodeSizes.usableBattery.width,
        height: nodeSizes.usableBattery.height,
        position: nodePositions.usableBattery,
        lines: [
          { id: "combined-usableBatteryValue", className: "node-value", text: "-" },
          { id: "combined-usableBatteryReserve", className: "node-state muted", text: "-" },
        ],
      },
      {
        id: "combined-switchboardNode",
        kind: "switchboard",
        icon: "switchboard",
        title: "Switchboard",
        titleKey: "switchboardTitle",
        width: nodeSizes.switchboard.width,
        height: nodeSizes.switchboard.height,
        position: nodePositions.switchboard,
        lines: [],
      },
      {
        id: "combined-loadNode",
        kind: "load",
        icon: "load",
        title: "Home Load",
        titleKey: "loadTitle",
        width: nodeSizes.compactLoad.width,
        height: nodeSizes.compactLoad.height,
        position: nodePositions.load,
        lines: [
          { id: "combined-loadPowerValue", className: "node-value", text: "-" },
        ],
      },
      {
        id: "combined-poolPumpNode",
        kind: "load",
        icon: "pool-pump",
        title: "Pool Pump",
        titleKey: "poolPumpTitle",
        width: nodeSizes.poolPump.width,
        height: nodeSizes.poolPump.height,
        position: nodePositions.poolPump,
        lines: [
          { id: "combined-poolPumpPowerValue", className: "node-value", text: "-" },
          { id: "combined-poolPumpState", className: "node-state muted", text: "-" },
          { type: "button", id: "combined-poolPumpToggleBtn", className: "pool-pump-control-btn btn secondary", text: "-" },
        ],
      },
      {
        id: "combined-teslaNode",
        kind: "tesla",
        icon: "tesla",
        title: "Tesla",
        titleKey: "teslaChargingLabel",
        width: nodeSizes.tesla.width,
        height: nodeSizes.tesla.height,
        position: nodePositions.tesla,
        lines: [
          {
            type: "soc",
            fillId: "combined-teslaSocFill",
            valueId: "combined-teslaSocValue",
            rateId: "combined-teslaRateValue",
          },
          { id: "combined-teslaChargingPowerValue", className: "node-sub-value", text: "-" },
          { id: "combined-teslaChargingCurrentValue", className: "node-mini-value muted", text: "-" },
          { id: "combined-teslaCurrentControlValue", className: "node-mini-value muted", text: "-" },
          { id: "combined-teslaLocationValue", className: "node-mini-value tesla-location-value muted", text: "-" },
          { type: "button", id: "combined-teslaChargingToggleBtn", className: "tesla-control-btn btn secondary", text: "-" },
        ],
      },
      {
        id: "combined-battery1Node",
        kind: "battery",
        side: "left",
        icon: "battery",
        title: "SAJ Battery",
        width: nodeSizes.battery.width,
        height: nodeSizes.battery.height,
        position: nodePositions.battery1,
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
        width: nodeSizes.inverter1.width,
        height: nodeSizes.inverter1.height,
        position: nodePositions.inverter1,
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
        width: nodeSizes.inverter2.width,
        height: nodeSizes.inverter2.height,
        position: nodePositions.inverter2,
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
        width: nodeSizes.battery.width,
        height: nodeSizes.battery.height,
        position: nodePositions.battery2,
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
        points: edgeLayout.gridToSwitchboard.points,
        labelPosition: edgeLayout.gridToSwitchboard.labelPosition,
      },
      {
        id: "combined-lineSolarToInverter1B",
        source: "combined-solarNode",
        target: "combined-inverter1Node",
        labelId: "combined-flowLabelSolarToInverter1",
        points: edgeLayout.solarToInverter1.points,
        labelPosition: edgeLayout.solarToInverter1.labelPosition,
      },
      {
        id: "combined-lineSwitchboardToTeslaB",
        source: "combined-switchboardNode",
        target: "combined-teslaNode",
        labelId: "combined-flowLabelSwitchboardToTesla",
        markerId: "combined-flowMarkerSwitchboardToTesla",
        markerClassName: "flow-edge-marker-disconnected",
        points: edgeLayout.switchboardToTesla.points,
        labelPosition: edgeLayout.switchboardToTesla.labelPosition,
        lineCap: edgeLayout.switchboardToTesla.lineCap,
        lineJoin: edgeLayout.switchboardToTesla.lineJoin,
        tailUpperGapLength: edgeLayout.switchboardToTesla.tailUpperGapLength,
        tailRoundedCorner: edgeLayout.switchboardToTesla.tailRoundedCorner,
      },
      {
        id: "combined-lineSwitchboardToHomeLoad",
        source: "combined-switchboardNode",
        target: "combined-loadNode",
        labelId: "combined-flowLabelSwitchboardToHomeLoad",
        points: edgeLayout.switchboardToHomeLoad.points,
        labelPosition: edgeLayout.switchboardToHomeLoad.labelPosition,
        lineCap: edgeLayout.switchboardToHomeLoad.lineCap,
        lineJoin: edgeLayout.switchboardToHomeLoad.lineJoin,
        tailUpperGapLength: edgeLayout.switchboardToHomeLoad.tailUpperGapLength,
        tailRoundedCorner: edgeLayout.switchboardToHomeLoad.tailRoundedCorner,
      },
      {
        id: "combined-lineSwitchboardToPoolPump",
        source: "combined-switchboardNode",
        target: "combined-poolPumpNode",
        labelId: "combined-flowLabelSwitchboardToPoolPump",
        points: edgeLayout.switchboardToPoolPump.points,
        labelPosition: edgeLayout.switchboardToPoolPump.labelPosition,
        lineCap: edgeLayout.switchboardToPoolPump.lineCap,
        lineJoin: edgeLayout.switchboardToPoolPump.lineJoin,
        tailUpperGapLength: edgeLayout.switchboardToPoolPump.tailUpperGapLength,
        tailRoundedCorner: edgeLayout.switchboardToPoolPump.tailRoundedCorner,
      },
      ...(compact
        ? []
        : [{
            id: "combined-lineSwitchboardToTotalLoad",
            source: "combined-switchboardNode",
            target: "combined-poolPumpNode",
            labelId: "combined-flowLabelSwitchboardToTotalLoad",
            glowWidth: 28,
            lineWidth: 18,
            coreWidth: 6,
            points: edgeLayout.switchboardToTotalLoad.points,
            labelPosition: edgeLayout.switchboardToTotalLoad.labelPosition,
            lineCap: edgeLayout.switchboardToTotalLoad.lineCap,
            lineJoin: edgeLayout.switchboardToTotalLoad.lineJoin,
          }]),
      {
        id: "combined-lineBattery1ToInverter1",
        source: "combined-battery1Node",
        target: "combined-inverter1Node",
        labelId: "combined-flowLabelBattery1ToInverter1",
        points: edgeLayout.battery1ToInverter1.points,
        labelPosition: edgeLayout.battery1ToInverter1.labelPosition,
      },
      {
        id: "combined-lineBattery2ToInverter2",
        source: "combined-battery2Node",
        target: "combined-inverter2Node",
        labelId: "combined-flowLabelBattery2ToInverter2",
        points: edgeLayout.battery2ToInverter2.points,
        labelPosition: edgeLayout.battery2ToInverter2.labelPosition,
      },
      {
        id: "combined-lineInverter1ToSwitchboardB",
        source: "combined-inverter1Node",
        target: "combined-switchboardNode",
        labelId: "combined-flowLabelInverter1ToSwitchboard",
        points: edgeLayout.inverter1ToSwitchboard.points,
        labelPosition: edgeLayout.inverter1ToSwitchboard.labelPosition,
      },
      {
        id: "combined-lineInverter2ToSwitchboardB",
        source: "combined-inverter2Node",
        target: "combined-switchboardNode",
        labelId: "combined-flowLabelInverter2ToSwitchboard",
        points: edgeLayout.inverter2ToSwitchboard.points,
        labelPosition: edgeLayout.inverter2ToSwitchboard.labelPosition,
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

const MOBILE_COMBINED_DIAGRAM = {
  shapeName: "mobile-energy-html",
  sceneHeight: 800,
  mobileBreakpoint: 768,
  state: {
    host: null,
    scene: null,
    surface: null,
    graph: null,
    nodes: new Map(),
    edges: new Map(),
    anchors: new Map(),
    shapeRegistered: false,
    lastModel: null,
  },
};

function shouldUseMobileCombinedX6() {
  const ua = String(window.navigator?.userAgent || "");
  const isIOS = /iPhone|iPad|iPod/i.test(ua)
    || (/Macintosh/i.test(ua) && Number(window.navigator?.maxTouchPoints || 0) > 1);
  if (isIOS) return false;
  return Boolean(window.X6?.Graph && window.X6?.Shape?.HTML);
}

function registerMobileCombinedDiagramShape() {
  const state = MOBILE_COMBINED_DIAGRAM.state;
  const X6 = window.X6;
  if (state.shapeRegistered || !X6?.Shape?.HTML) return Boolean(state.shapeRegistered);
  X6.Shape.HTML.register({
    shape: MOBILE_COMBINED_DIAGRAM.shapeName,
    inherit: "html",
    effect: ["data"],
    html(cell) {
      return renderMobileCombinedNodeMarkup(cell.getData() || {});
    },
  });
  state.shapeRegistered = true;
  return true;
}

function renderMobileCombinedNodeMarkup(data) {
  const cls = escapeHtml(String(data.cls || ""));
  const kind = String(data.kind || "hub");
  const title = escapeHtml(String(data.title || ""));
  const accent = escapeHtml(String(data.accent || "cyan"));
  const stateClass = `${data.active ? " is-active" : ""}${data.dimmed ? " is-dimmed" : ""}${data.action ? " is-clickable" : ""}`;
  const attrs = [
    "data-mobile-node='true'",
    data.action ? `data-mobile-action="${escapeHtml(data.action)}"` : "",
    data.hint ? `title="${escapeHtml(data.hint)}"` : "",
  ].filter(Boolean).join(" ");
  const valueHtml = data.valueHtml || "-";
  const subValueHtml = data.subValueHtml || "&nbsp;";
  const metaHtml = data.metaHtml || "&nbsp;";
  if (kind === "hub") {
    return (
      `<article class="mobile-energy-node mobile-energy-x6-node ${cls} accent-${accent}${stateClass}" ${attrs}>` +
      `<div class="mobile-energy-icon mobile-energy-icon-hub">${mobileFlowIconMarkup("hub")}</div>` +
      `<div class="mobile-energy-copy">` +
      `<p class="mobile-energy-title">${title}</p>` +
      `<p class="mobile-energy-value">${valueHtml}</p>` +
      `<p class="mobile-energy-subvalue">${subValueHtml}</p>` +
      "</div>" +
      "</article>"
    );
  }
  return (
    `<article class="mobile-energy-node mobile-energy-x6-node ${cls} accent-${accent}${stateClass}" ${attrs}>` +
    `<div class="mobile-energy-icon mobile-energy-icon-${escapeHtml(kind)}">${mobileFlowIconMarkup(kind)}</div>` +
    `<div class="mobile-energy-copy">` +
    `<p class="mobile-energy-title">${title}</p>` +
    `<p class="mobile-energy-value">${valueHtml}</p>` +
    `<p class="mobile-energy-subvalue">${subValueHtml}</p>` +
    `<p class="mobile-energy-meta">${metaHtml}</p>` +
    "</div>" +
    "</article>"
  );
}

function getMobileCombinedSceneWidth() {
  const state = MOBILE_COMBINED_DIAGRAM.state;
  const width = Math.round(state.scene?.clientWidth || state.surface?.clientWidth || state.host?.clientWidth || 0);
  return Math.max(320, width);
}

function computeMobileCombinedDiagramLayout(sceneWidth) {
  const pad = sceneWidth <= 380 ? 8 : 10;
  const gap = sceneWidth <= 380 ? 10 : 12;
  const inset = 6;
  const half = (sceneWidth - (pad * 2) - gap) / 2;
  const topWidth = Math.round(half - 12);
  const midWidth = Math.max(108, Math.round(topWidth - 26));
  const third = (sceneWidth - (pad * 2) - (gap * 2)) / 3;
  const bottomWidth = Math.round(third);
  const topHeight = 120;
  const midHeight = 128;
  const bottomHeight = 112;
  const hubSize = 128;
  const leftX = Math.round(pad + inset);
  const rightX = Math.round(sceneWidth - pad - inset - topWidth);
  const loadX = Math.round(pad);
  const poolX = Math.round(pad + third + gap);
  const teslaX = Math.round(pad + (third * 2) + (gap * 2));
  const solarTop = 12;
  const inverterTop = 214;
  const hubTop = 430;
  const bottomTop = 668;
  const hubX = Math.round((sceneWidth / 2) - (hubSize / 2));
  const sajX = Math.round(hubX - 22 - midWidth);
  const solplanetX = Math.round(hubX + hubSize + 22);
  const solarCenterX = Math.round(leftX + (topWidth / 2));
  const gridCenterX = Math.round(rightX + (topWidth / 2));
  const sajCenterX = Math.round(sajX + (midWidth / 2));
  const solplanetCenterX = Math.round(solplanetX + (midWidth / 2));
  const hubCenterX = Math.round(hubX + (hubSize / 2));
  const hubCenterY = Math.round(hubTop + (hubSize / 2));
  const loadCenterX = Math.round(loadX + (bottomWidth / 2));
  const poolCenterX = Math.round(poolX + (bottomWidth / 2));
  const teslaCenterX = Math.round(teslaX + (bottomWidth / 2));
  const busY = 640;
  return {
    width: sceneWidth,
    height: MOBILE_COMBINED_DIAGRAM.sceneHeight,
    nodes: {
      solar: { x: leftX, y: solarTop, width: topWidth, height: topHeight },
      grid: { x: rightX, y: solarTop, width: topWidth, height: topHeight },
      saj: { x: sajX, y: inverterTop, width: midWidth, height: midHeight },
      solplanet: { x: solplanetX, y: inverterTop, width: midWidth, height: midHeight },
      hub: { x: hubX, y: hubTop, width: hubSize, height: hubSize },
      load: { x: loadX, y: bottomTop, width: bottomWidth, height: bottomHeight },
      pool: { x: poolX, y: bottomTop, width: bottomWidth, height: bottomHeight },
      tesla: { x: teslaX, y: bottomTop, width: bottomWidth, height: bottomHeight },
    },
    anchors: {
      sajJoint: { x: sajCenterX, y: hubTop },
      solplanetJoint: { x: solplanetCenterX, y: hubTop },
      hubLeft: { x: hubX, y: hubCenterY },
      hubRight: { x: hubX + hubSize, y: hubCenterY },
      hubBottom: { x: hubCenterX, y: hubTop + hubSize },
      busCenter: { x: hubCenterX, y: busY },
      busLeft: { x: loadCenterX, y: busY },
      busRight: { x: teslaCenterX, y: busY },
    },
    edgeVertices: {
      solarDown: [],
      gridDown: [],
      sajDown: [],
      solplanetDown: [],
      sajLinkH: [],
      solplanetLinkH: [],
      hubDown: [],
      busLeft: [],
      busRight: [],
      loadDown: [],
      poolDown: [],
      teslaDown: [],
    },
  };
}

function ensureMobileCombinedFlowDiagram() {
  const state = MOBILE_COMBINED_DIAGRAM.state;
  state.host = document.getElementById("energyFlowCombinedMobile");
  if (!state.host || !shouldUseMobileCombinedX6() || !registerMobileCombinedDiagramShape() || !window.X6?.Graph) {
    if (state.host) state.host.classList.remove("is-x6-ready");
    return null;
  }
  if (!state.graph) {
    state.host.innerHTML =
      `<div class="mobile-energy-card mobile-energy-card-x6">` +
      `<div class="mobile-energy-scene mobile-energy-scene-x6">` +
      `<div class="mobile-energy-graph"></div>` +
      "</div>" +
      "</div>";
    state.scene = state.host.querySelector(".mobile-energy-scene-x6");
    state.surface = state.host.querySelector(".mobile-energy-graph");
    if (!state.surface) return null;
    state.graph = new window.X6.Graph({
      container: state.surface,
      width: getMobileCombinedSceneWidth(),
      height: MOBILE_COMBINED_DIAGRAM.sceneHeight,
      interacting: false,
      panning: false,
      mousewheel: false,
      moving: false,
      selecting: false,
      connecting: { snap: false },
      background: { color: "transparent" },
    });
    state.host.classList.add("is-x6-ready");
    buildMobileCombinedFlowGraph(state.graph);
  }
  return state.graph;
}

function buildMobileCombinedFlowGraph(graph) {
  const state = MOBILE_COMBINED_DIAGRAM.state;
  const layout = computeMobileCombinedDiagramLayout(getMobileCombinedSceneWidth());
  const hiddenNodeAttrs = {
    body: {
      fill: "transparent",
      stroke: "none",
      pointerEvents: "none",
    },
    label: {
      display: "none",
    },
  };
  const anchorEntries = Object.entries(layout.anchors);
  anchorEntries.forEach(([key, point]) => {
    const node = graph.addNode({
      id: `mobile-anchor-${key}`,
      shape: "rect",
      x: point.x,
      y: point.y,
      width: 1,
      height: 1,
      attrs: hiddenNodeAttrs,
      zIndex: 0,
    });
    state.anchors.set(key, node);
  });
  const visibleNodes = [
    ["solar", "node-solar", "solar"],
    ["grid", "node-grid", "grid"],
    ["saj", "node-saj", "chip"],
    ["solplanet", "node-solplanet", "chip"],
    ["hub", "node-hub", "hub"],
    ["load", "node-load", "home"],
    ["pool", "node-pool", "pool"],
    ["tesla", "node-tesla", "ev"],
  ];
  visibleNodes.forEach(([key, cls, kind]) => {
    const box = layout.nodes[key];
    const node = graph.addNode({
      id: `mobile-node-${key}`,
      shape: MOBILE_COMBINED_DIAGRAM.shapeName,
      x: box.x,
      y: box.y,
      width: box.width,
      height: box.height,
      data: {
        cls,
        kind,
        title: "-",
        valueHtml: "-",
        subValueHtml: "&nbsp;",
        metaHtml: "&nbsp;",
        accent: key === "solar" ? "solar" : key === "grid" ? "grid" : key === "hub" ? "cyan" : key === "load" ? "load" : key === "tesla" ? "tesla" : (key === "solplanet" ? "battery" : "cyan"),
        active: false,
        dimmed: false,
      },
      zIndex: 2,
    });
    state.nodes.set(key, node);
  });
  const edgeDefs = [
    ["solarDown", "mobile-node-solar", "mobile-node-saj"],
    ["gridDown", "mobile-node-grid", "mobile-node-solplanet"],
    ["sajDown", "mobile-node-saj", "mobile-anchor-sajJoint"],
    ["solplanetDown", "mobile-node-solplanet", "mobile-anchor-solplanetJoint"],
    ["sajLinkH", "mobile-anchor-sajJoint", "mobile-anchor-hubLeft"],
    ["solplanetLinkH", "mobile-anchor-solplanetJoint", "mobile-anchor-hubRight"],
    ["hubDown", "mobile-anchor-hubBottom", "mobile-anchor-busCenter"],
    ["busLeft", "mobile-anchor-busCenter", "mobile-anchor-busLeft"],
    ["busRight", "mobile-anchor-busCenter", "mobile-anchor-busRight"],
    ["loadDown", "mobile-anchor-busLeft", "mobile-node-load"],
    ["poolDown", "mobile-anchor-busCenter", "mobile-node-pool"],
    ["teslaDown", "mobile-anchor-busRight", "mobile-node-tesla"],
  ];
  edgeDefs.forEach(([key, source, target]) => {
    const edge = graph.addEdge({
      id: `mobile-edge-${key}`,
      source: { cell: source },
      target: { cell: target },
      vertices: layout.edgeVertices[key] || [],
      zIndex: 1,
      attrs: {
        line: {
          class: "mobile-energy-edge",
          stroke: "rgba(36, 122, 190, 0.32)",
          strokeWidth: 2,
          strokeLinecap: "round",
          strokeLinejoin: "round",
          targetMarker: null,
          sourceMarker: null,
        },
      },
    });
    state.edges.set(key, edge);
  });
}

function updateMobileCombinedFlowGraphLayout() {
  const state = MOBILE_COMBINED_DIAGRAM.state;
  if (!state.graph) return;
  const layout = computeMobileCombinedDiagramLayout(getMobileCombinedSceneWidth());
  state.graph.resize(layout.width, layout.height);
  Object.entries(layout.nodes).forEach(([key, box]) => {
    const node = state.nodes.get(key);
    if (!node) return;
    node.position(box.x, box.y);
    node.resize(box.width, box.height);
  });
  Object.entries(layout.anchors).forEach(([key, point]) => {
    const node = state.anchors.get(key);
    if (!node) return;
    node.position(point.x, point.y);
  });
  Object.entries(layout.edgeVertices).forEach(([key, vertices]) => {
    const edge = state.edges.get(key);
    if (!edge) return;
    edge.setVertices(vertices);
  });
}

function setMobileCombinedNodeState(key, nextData) {
  const node = MOBILE_COMBINED_DIAGRAM.state.nodes.get(key);
  if (!node) return;
  node.setData({ ...(node.getData() || {}), ...nextData });
}

function setMobileCombinedEdgeState(key, active = false, reverse = false) {
  const edge = MOBILE_COMBINED_DIAGRAM.state.edges.get(key);
  if (!edge) return;
  const className = `mobile-energy-edge${active ? " is-active" : ""}${reverse ? " is-reverse" : ""}`;
  edge.attr("line/class", className);
}

function refreshMobileCombinedFlowDiagram() {
  const state = MOBILE_COMBINED_DIAGRAM.state;
  if (!state.graph) return;
  updateMobileCombinedFlowGraphLayout();
  if (state.lastModel) {
    Object.entries(state.lastModel.nodes || {}).forEach(([key, value]) => {
      setMobileCombinedNodeState(key, value);
    });
    Object.entries(state.lastModel.edges || {}).forEach(([key, value]) => {
      setMobileCombinedEdgeState(key, value.active, value.reverse);
    });
  }
}

function applyMobileCombinedFlowDiagram(model) {
  const graph = ensureMobileCombinedFlowDiagram();
  if (!graph) return false;
  MOBILE_COMBINED_DIAGRAM.state.lastModel = model;
  updateMobileCombinedFlowGraphLayout();
  Object.entries(model.nodes || {}).forEach(([key, value]) => {
    setMobileCombinedNodeState(key, value);
  });
  Object.entries(model.edges || {}).forEach(([key, value]) => {
    setMobileCombinedEdgeState(key, value.active, value.reverse);
  });
  return true;
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
  if (matchedEntities >= 6) return "ok";
  return "partial";
}

function localizedCollectorMessage(message) {
  if (!message || typeof message !== "object") return "";
  const preferred = currentLang === "zh" ? message.zh : message.en;
  const fallback = currentLang === "zh" ? message.en : message.zh;
  return String(preferred || fallback || "").trim();
}

function collectorHealthIssueText(collectorStatus) {
  const health = collectorStatus?.health_summary;
  if (health?.ok === true || health?.status === "ok") return "";
  const systems = collectorStatus?.systems || {};
  const affected = new Set();
  const labels = {
    saj: "SAJ",
    solplanet: "Solplanet",
    tesla: currentLang === "zh" ? "特斯拉" : "Tesla",
    pool_pump: currentLang === "zh" ? "水泵" : "Pool pump",
  };
  const addSystem = (system) => {
    if (labels[system]) affected.add(system);
  };
  const sourceDetails = systems.combined?.last_round_result?.source_details || {};
  Object.entries(sourceDetails).forEach(([system, state]) => {
    if (state?.stored_sample === false) addSystem(system);
  });
  const sourceFailed = Object.values(sourceDetails).some((state) => state?.stored_sample === false);
  const issues = Array.isArray(health?.issues) ? health.issues : [];
  issues.forEach((issue) => {
    // Missing combined SOC is a consequence of a failed source, not a SAJ outage.
    if (sourceFailed && issue.code === "combined_saj_soc_unavailable") return;
    addSystem(issue.system);
    const sources = issue.details?.last_round_result?.source_details || {};
    Object.entries(sources).forEach(([system, state]) => {
      if (state?.stored_sample === false) addSystem(system);
    });
  });
  Object.keys(labels).forEach((system) => {
    const state = systems[system] || {};
    if (state.last_round_result?.stored_sample === false || state.last_error || state.last_persistence_error) {
      addSystem(system);
    }
  });
  if (affected.size) {
    const names = Object.keys(labels).filter((system) => affected.has(system)).map((system) => labels[system]);
    return currentLang === "zh" ? `${names.join("、")} 异常` : `${names.join(", ")} unavailable`;
  }
  if (health?.ok === false || health?.status === "degraded" || systems.combined?.last_error) {
    return currentLang === "zh" ? "数据服务异常" : "Data service unavailable";
  }
  return "";
}

function combinedDashboardIssueText(collectorStatus, combinedFlow, { isStale = false, failed = false } = {}) {
  const healthText = collectorHealthIssueText(collectorStatus);
  if (healthText) return healthText;
  if (failed || isStale || combinedFlow?.meta?.stale_reason || combinedFlow?.stale_reason) {
    return currentLang === "zh" ? "数据服务异常" : "Data service unavailable";
  }
  return "";
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

function setTeslaSocTone(socPercent) {
  const teslaNode = document.querySelector(".efd-node.efd-node-id-combined-teslaNode");
  const teslaFill = document.getElementById("combined-teslaSocFill");
  const teslaTextLayer = document.getElementById("combined-teslaSocValue")?.parentElement;
  if (!teslaNode || !teslaFill || !teslaTextLayer) return;

  if (socPercent === null || socPercent === undefined || Number.isNaN(Number(socPercent))) {
    delete teslaNode.dataset.socTone;
    teslaFill.style.background = "";
    teslaTextLayer.style.setProperty("--soc-text-color", "#352c25");
    teslaTextLayer.style.setProperty("--soc-text-shadow", "rgba(255, 255, 255, 0.5)");
    return;
  }

  const clampedSoc = Math.max(0, Math.min(100, Number(socPercent)));
  const tone = clampedSoc > 80 ? "high" : "healthy";
  teslaNode.dataset.socTone = tone;

  if (tone === "high") {
    teslaFill.style.background = "linear-gradient(180deg, rgba(248, 194, 85, 0.96) 0%, rgba(232, 140, 59, 0.98) 58%, rgba(208, 82, 50, 0.98) 100%)";
    teslaTextLayer.style.setProperty("--soc-text-color", "#4d2416");
    teslaTextLayer.style.setProperty("--soc-text-shadow", "rgba(255, 245, 238, 0.66)");
    return;
  }

  teslaFill.style.background = "linear-gradient(180deg, rgba(157, 204, 111, 0.95) 0%, rgba(104, 170, 97, 0.97) 62%, rgba(67, 126, 81, 0.98) 100%)";
  teslaTextLayer.style.setProperty("--soc-text-color", "#243126");
  teslaTextLayer.style.setProperty("--soc-text-shadow", "rgba(244, 255, 246, 0.62)");
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
  const renderSocValueHtml = (socText, inlineEnergyText = null) => {
    const mainHtml = formatValueWithDataKindHtml(socText, socDataKind);
    if (!inlineEnergyText) return mainHtml;
    return `${mainHtml} <span class="soc-inline-energy">(${escapeHtml(inlineEnergyText)})</span>`;
  };

  if (soc === null || soc === undefined) {
    const energyText = energyValueId ? formatBatteryEnergyKwh(system, null) : null;
    const inlineEnergyText = energyValueId ? formatBatteryCurrentEnergyKwh(system, null) : null;
    setHtml(socValueId, renderSocValueHtml("-", inlineEnergyText));
    if (energyValueId) setHtml(energyValueId, formatValueWithDataKindHtml(energyText, energyDataKind));
    if (usableValueId) setHtml(usableValueId, formatValueWithDataKindHtml(formatBatteryUsableKwh(system, null), usableDataKind));
    if (runtimeValueId) setText(runtimeValueId, "");
    if (rateValueId) setText(rateValueId, formatBatterySocRateEstimate(system, batteryW));
    setSocFillLevel(socFillId, 0);
    setSocTextContrastBySocValueId(socValueId, 0);
    return;
  }

  const clampedSoc = Math.max(0, Math.min(100, Number(soc)));
  const energyText = energyValueId ? formatBatteryEnergyKwh(system, clampedSoc) : null;
  const inlineEnergyText = energyValueId ? formatBatteryCurrentEnergyKwh(system, clampedSoc) : null;
  setHtml(socValueId, renderSocValueHtml(`${clampedSoc.toFixed(0)}%`, inlineEnergyText));
  if (energyValueId) setHtml(energyValueId, formatValueWithDataKindHtml(energyText, energyDataKind));
  if (usableValueId) setHtml(usableValueId, formatValueWithDataKindHtml(formatBatteryUsableKwh(system, clampedSoc), usableDataKind));
  if (runtimeValueId) setText(runtimeValueId, "");
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
    renderSajTouControls();
    setText(ids.desiredId, "-");
    setText(ids.actualId, "-");
    setText(ids.statusId, "-");
    return;
  }

  const selectedProfile = payload.selected_profile || payload.effective_profile || "self_consumption";
  const actualProfile = payload.actual_profile || null;
  const visibleProfile = actualProfile || selectedProfile;
  if (select) select.value = visibleProfile;
  renderSajTouControls();
  setText(ids.desiredId, t("sajProfileDesiredText", { profile: sajProfileLabel(visibleProfile) }));
  setText(ids.actualId, t("sajProfileActualText", { profile: sajProfileLabel(actualProfile) }));

  let statusKey = "sajProfileStatusUnknown";
  if (payload.pending_remote_sync) statusKey = "sajProfileStatusPending";
  else if (actualProfile && visibleProfile && actualProfile === visibleProfile) statusKey = "sajProfileStatusSynced";
  else if (payload.is_custom_remote_state) statusKey = "sajProfileStatusCustom";
  setText(ids.statusId, t(statusKey));
}

function sajTouPowerFromControlState() {
  const profileState = stateCache.lastSajProfile?.profile_state || stateCache.lastSajProfile || null;
  const state = profileState?.control_state || stateCache.lastSajControl?.control_state || stateCache.lastSajControl?.state || null;
  const slot = Array.isArray(state?.discharge?.slots) ? state.discharge.slots[0] : null;
  const estimate = Number(slot?.power_w_estimate);
  if (Number.isFinite(estimate) && estimate > 0) return Math.round(estimate);
  const percent = Number(slot?.power_percent);
  if (Number.isFinite(percent)) return Math.round((_sajRatedPowerForUi() * percent) / 100);
  return 5000;
}

function renderSajTouControls() {
  const select = document.getElementById("sajDashboardProfileSelect");
  const controls = document.getElementById("sajDashboardTouControls");
  if (!controls) return;
  const isTimeOfUse = select?.value === "time_of_use";
  controls.classList.toggle("hidden", !isTimeOfUse);
  const powerInput = document.getElementById("sajDashboardTouPowerInput");
  if (powerInput && isTimeOfUse && !powerInput.matches(":focus")) {
    powerInput.value = String(sajTouPowerFromControlState());
  }
}

function getSajDashboardModeText() {
  const profileState = stateCache.lastSajProfile?.profile_state || stateCache.lastSajProfile || null;
  const actualProfile = profileState?.actual_profile;
  const selectedProfile = profileState?.selected_profile || profileState?.effective_profile || null;
  const actualLabel = actualProfile ? sajProfileLabel(actualProfile) : "";
  const selectedLabel = selectedProfile ? sajProfileLabel(selectedProfile) : "";
  if (actualLabel && selectedLabel) {
    return actualLabel;
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

function sajControlApiRemovedMessage() {
  return "Old SAJ control APIs have been removed. Use PUT /api/saj/manual-discharge-power or PUT /api/saj/self-consumption.";
}

async function loadSajProfile() {
  const previous = stateCache.lastSajProfile;
  renderSajProfilePanel();
  try {
    const payload = await fetchJson("/api/saj/control/state", { timeoutMs: 12000 });
    stateCache.lastSajControl = payload;
    stateCache.lastSajProfile = payload?.profile_state || payload || null;
  } catch (err) {
    stateCache.lastSajProfile = previous || null;
    setText("sajProfileStatusText", t("sajProfileLoadFailed", { error: String(err) }));
    setText("sajDashboardProfileStatusText", t("sajProfileLoadFailed", { error: String(err) }));
  }
  renderSajProfilePanel();
  renderSummary(stateCache.lastSummary || null);
}

async function applySajProfile(selectId = "sajProfileSelect") {
  const fromDashboardPopover = selectId === "sajDashboardProfileSelect";
  if (fromDashboardPopover) setSajDashboardProfilePopoverVisible(false);
  const profileId = normalizeSajProfileId(document.getElementById(selectId)?.value || "");
  try {
    if (profileId === "self_consumption") {
      await applySajSelfConsumption();
    } else if (profileId === "time_of_use") {
      const direction = document.querySelector("input[name='sajDashboardTouDirection']:checked")?.value || "discharge";
      const rawPower = Number(document.getElementById("sajDashboardTouPowerInput")?.value || "0");
      const powerW = Math.round(Math.max(0, Math.min(_sajRatedPowerForUi(), Number.isFinite(rawPower) ? rawPower : 0)));
      if (direction === "charge") await applySajManualChargePower(powerW);
      else await applySajManualDischargePower(powerW);
    } else {
      throw new Error(sajControlApiRemovedMessage());
    }
    await loadSajProfile();
    setText("sajProfileStatusText", t("sajProfileApplySummary", { profile: sajProfileLabel(profileId) }));
    setText("sajDashboardProfileStatusText", t("sajProfileApplySummary", { profile: sajProfileLabel(profileId) }));
  } catch (err) {
    const errorText = t("sajProfileApplyFailed", { error: String(err) });
    setText("sajProfileStatusText", errorText);
    setText("sajDashboardProfileStatusText", errorText);
    showSajActionError(errorText, []);
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
    const ms = new SiteDate(iso).getTime();
    if (!Number.isFinite(ms)) continue;
    if (latestMs === null || ms > latestMs) {
      latestMs = ms;
      latestIso = iso;
    }
  }
  return latestIso;
}

function pickLatestPoolPumpDevice(previousDevice, nextDevice) {
  if (!previousDevice || typeof previousDevice !== "object") return nextDevice || null;
  if (!nextDevice || typeof nextDevice !== "object") return previousDevice;
  const previousMs = new SiteDate(previousDevice.updated_at || 0).getTime();
  const nextMs = new SiteDate(nextDevice.updated_at || 0).getTime();
  if (Number.isFinite(previousMs) && Number.isFinite(nextMs) && previousMs > nextMs) {
    return previousDevice;
  }
  return nextDevice;
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

function isTeslaActivelyCharging({ actualValue = null, chargingPowerValue = null, connectionState = null } = {}) {
  const normalizedConnectionState = String(connectionState || "").trim().toLowerCase();
  if (normalizedConnectionState === "charging") return true;
  if (normalizedConnectionState === "plugged_not_charging" || normalizedConnectionState === "unplugged") return false;
  const actualNumeric = toFiniteNumber(actualValue);
  if (actualNumeric !== null) return actualNumeric > 0;
  const chargingPowerNumeric = toFiniteNumber(chargingPowerValue);
  if (chargingPowerNumeric !== null) return chargingPowerNumeric > 0;
  return null;
}

function buildTeslaCurrentDisplay(actualValue, configuredValue, unit, { connectionState = null, chargingPowerValue = null, showConfigured = true } = {}) {
  const actualNumeric = toFiniteNumber(actualValue);
  const configuredNumeric = toFiniteNumber(configuredValue);
  const unitText = String(unit || "").trim();
  const normalizedUnit = currentLang === "zh" && unitText.toLowerCase() === "a"
    ? "安"
    : (unitText || (currentLang === "zh" ? "安" : "A"));
  if (actualNumeric === null && configuredNumeric === null) {
    return {
      actualText: "-",
      secondaryText: null,
      showConfigured: false,
      connectionText: null,
    };
  }
  const actualText = actualNumeric === null ? "-" : `${actualNumeric.toFixed(1)}${normalizedUnit}`;
  const chargingActive = isTeslaActivelyCharging({ actualValue: actualNumeric, chargingPowerValue, connectionState });
  const effectiveConfigured = chargingActive === false ? 0 : configuredNumeric;
  const configuredText = effectiveConfigured === null ? "-" : `${effectiveConfigured.toFixed(1)}${normalizedUnit}`;
  const shouldShowConfigured = showConfigured && effectiveConfigured !== null;
  return {
    actualText,
    secondaryText: shouldShowConfigured ? configuredText : null,
    showConfigured: shouldShowConfigured,
    connectionText: null,
  };
}

function formatTeslaCurrentValue(actualValue, configuredValue, unit, options = {}) {
  const display = buildTeslaCurrentDisplay(actualValue, configuredValue, unit, options);
  if (display.actualText === "-" && !display.secondaryText) return `${t("teslaChargingCurrentLabel")} -`;
  if (display.showConfigured) {
    return `${t("teslaChargingCurrentLabel")} ${t("teslaChargingCurrentActualLabel")} ${display.actualText} · ${t("teslaChargingCurrentConfiguredLabel")} ${display.secondaryText}`;
  }
  if (display.secondaryText) {
    return `${t("teslaChargingCurrentLabel")} ${t("teslaChargingCurrentActualLabel")} ${display.actualText} · ${display.secondaryText}`;
  }
  return `${t("teslaChargingCurrentLabel")} ${t("teslaChargingCurrentActualLabel")} ${display.actualText}`;
}

function formatTeslaCurrentValueHtml(actualValue, configuredValue, unit, kind, options = {}) {
  const display = buildTeslaCurrentDisplay(actualValue, configuredValue, unit, { ...options, showConfigured: false });
  const voltageValue = toFiniteNumber(options?.voltageV);
  const actualLabel = t("teslaChargingCurrentActualLabel");
  let singleLineText = `${actualLabel} ${display.actualText}`;
  if (voltageValue !== null) {
    singleLineText += ` / ${formatTrimmedDecimal(voltageValue, 0)} V`;
  }
  return (
    `<span class="data-kind-value tesla-current-stack">` +
    `<span class="data-kind-main tesla-current-line">${escapeHtml(singleLineText)}</span>` +
    `</span>`
  );
}

function formatTeslaBatteryEnergyValue(currentKwh, totalKwh) {
  const current = toFiniteNumber(currentKwh);
  const total = toFiniteNumber(totalKwh);
  if (current === null || total === null) return t("teslaBatteryEnergyUnavailable");
  return `${formatTrimmedDecimal(Math.max(current, 0), 1)} / ${formatTrimmedDecimal(Math.max(total, 0), 1)} kWh`;
}

function formatTeslaChargeEtaSummary(teslaInfo = null) {
  const completionAtText = String(teslaInfo?.chargeCompletionAt || "").trim();
  if (completionAtText) {
    const dt = new SiteDate(completionAtText);
    if (!Number.isNaN(dt.getTime())) {
      const timeText = formatBatteryRuntimeTargetTime(dt);
      if (timeText) return t("batteryRuntimeCharging", { time: timeText });
    }
  }

  let minutesToFull = toFiniteNumber(teslaInfo?.minutesToFull);
  if (minutesToFull === null) {
    const remainingKwh = toFiniteNumber(teslaInfo?.remainingToFullKwh);
    const chargingW = toFiniteNumber(teslaInfo?.chargingW);
    if (
      remainingKwh !== null &&
      chargingW !== null &&
      chargingW >= POWER_FLOW_ACTIVE_THRESHOLD_W
    ) {
      minutesToFull = (remainingKwh / (chargingW / 1000)) * 60;
    }
  }

  if (minutesToFull === null || !Number.isFinite(minutesToFull) || minutesToFull < 0) {
    return t("teslaChargeEtaUnavailable");
  }
  const targetAt = new SiteDate(Date.now() + minutesToFull * 60 * 1000);
  const timeText = formatBatteryRuntimeTargetTime(targetAt);
  if (!timeText) return t("teslaChargeEtaUnavailable");
  return t("batteryRuntimeCharging", { time: timeText });
}

function formatTeslaCurrentControlHtml(teslaInfo = null) {
  const minA = toFiniteNumber(teslaInfo?.minCurrentA);
  const maxA = toFiniteNumber(teslaInfo?.maxCurrentA);
  if (minA === null || maxA === null || maxA < minA) return "";
  const fallbackValue = toFiniteNumber(teslaInfo?.configuredCurrentA) ?? toFiniteNumber(teslaInfo?.currentA) ?? minA;
  const draftValue = teslaCurrentDraftA === null ? fallbackValue : teslaCurrentDraftA;
  const clampedValue = Math.max(minA, Math.min(maxA, draftValue));
  const clickable = !teslaControlBusy && !teslaCurrentControlBusy && Boolean(teslaInfo?.controlAvailable);
  const className = clickable ? "tesla-current-trigger is-clickable" : "tesla-current-trigger";
  let labelText = `${t("teslaChargingCurrentConfiguredLabel")} ${clampedValue.toFixed(1)}A`;
  const configuredCurrentA = toFiniteNumber(teslaInfo?.configuredCurrentA);
  const localPendingTargetA = toFiniteNumber(teslaPendingCurrentTargetA);
  const localPendingSourceA = toFiniteNumber(teslaPendingCurrentSourceA);
  const serverPendingTargetA = toFiniteNumber(teslaInfo?.pendingConfiguredTargetA);
  const serverPendingSourceA = toFiniteNumber(teslaInfo?.pendingConfiguredSourceA);
  const pendingTargetA = localPendingTargetA ?? serverPendingTargetA;
  const pendingSourceA = localPendingSourceA ?? serverPendingSourceA;
  if (
    pendingTargetA !== null &&
    configuredCurrentA !== null &&
    Math.abs(configuredCurrentA - pendingTargetA) > 0.05
  ) {
    const sourceA = pendingSourceA === null ? configuredCurrentA : pendingSourceA;
    labelText = `${t("teslaChargingCurrentConfiguredLabel")} ${sourceA.toFixed(1)}A -> ${pendingTargetA.toFixed(1)}A`;
  }
  return (
    `<button id="combined-teslaCurrentTriggerBtn" type="button" class="${escapeHtml(className)}"${clickable ? "" : " disabled"}>` +
    `${escapeHtml(labelText)}` +
    `</button>`
  );
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

function formatTeslaLocationSummary(teslaInfo = null) {
  const status = String(teslaInfo?.locationStatus || "").trim().toLowerCase();
  const state = String(teslaInfo?.locationState || "").trim().toLowerCase();
  if (teslaInfo?.atHome === true || status === "home" || state === "home") {
    return t("teslaLocationHome");
  }
  if (teslaInfo?.atHome === false || status === "non_home" || (state && state !== "unknown" && state !== "unavailable")) {
    return t("teslaLocationNonHome");
  }
  return t("teslaLocationUnknown");
}

function isTeslaConnected(teslaInfo = null) {
  const cableConnected = teslaInfo?.teslaCableConnected;
  if (typeof cableConnected === "boolean") return cableConnected;
  const connectionState = String(teslaInfo?.teslaConnectionState || "").trim().toLowerCase();
  if (connectionState === "charging" || connectionState === "plugged_not_charging") return true;
  if (connectionState === "unplugged") return false;
  return null;
}

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function clearTeslaControlFeedback() {
  if (teslaControlFeedbackTimerId) {
    window.clearTimeout(teslaControlFeedbackTimerId);
    teslaControlFeedbackTimerId = null;
  }
  teslaControlFeedback = null;
}

function scheduleTeslaControlFeedbackClear(delayMs = TESLA_CONTROL_FEEDBACK_CLEAR_MS) {
  if (teslaControlFeedbackTimerId) window.clearTimeout(teslaControlFeedbackTimerId);
  teslaControlFeedbackTimerId = window.setTimeout(() => {
    teslaControlFeedbackTimerId = null;
    teslaControlFeedback = null;
  }, delayMs);
}

function setTeslaControlFeedback(phase, targetEnabled) {
  teslaControlFeedback = {
    phase,
    targetEnabled: Boolean(targetEnabled),
    updatedAt: Date.now(),
  };
}

function effectiveTeslaControlFeedback(teslaInfo = null) {
  const persisted = teslaInfo?.controlFeedback;
  if (teslaControlBusy) return teslaControlFeedback || persisted || null;
  return persisted || teslaControlFeedback || null;
}

function teslaControlMatchesTarget(teslaInfo = null, targetEnabled = false) {
  return {
    requested: typeof teslaInfo?.chargeRequestedEnabled === "boolean"
      ? teslaInfo.chargeRequestedEnabled === targetEnabled
      : false,
    active: typeof teslaInfo?.chargingEnabled === "boolean"
      ? teslaInfo.chargingEnabled === targetEnabled
      : false,
  };
}

async function waitForTeslaControlConfirmation(targetEnabled) {
  const deadlineAt = Date.now() + TESLA_CONTROL_CONFIRM_TIMEOUT_MS;
  while (Date.now() < deadlineAt) {
    await loadSummary();
    const teslaInfo = teslaInfoFromCombinedFlow(stateCache.lastSummary?.combinedFlow || {});
    const matches = teslaControlMatchesTarget(teslaInfo, targetEnabled);
    if (matches.active) {
      setTeslaControlFeedback("success", targetEnabled);
      scheduleTeslaControlFeedbackClear();
      return true;
    }
    setTeslaControlFeedback(matches.requested ? "awaiting_vehicle" : "requesting", targetEnabled);
    await sleep(TESLA_CONTROL_CONFIRM_POLL_MS);
  }
  await loadSummary();
  const teslaInfo = teslaInfoFromCombinedFlow(stateCache.lastSummary?.combinedFlow || {});
  const matches = teslaControlMatchesTarget(teslaInfo, targetEnabled);
  setTeslaControlFeedback(matches.requested ? "delayed" : "failed", targetEnabled);
  return false;
}

function mergeTeslaControlInfo(baseInfo, controlPayload) {
  const controlState = controlPayload?.control_state || controlPayload || {};
  const observation = controlPayload?.observation || {};
  const charging = observation?.charging || {};
  const battery = observation?.battery || {};
  const location = observation?.location || {};
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
    minCurrentA: toFiniteNumber(charging?.min_current_amps),
    maxCurrentA: toFiniteNumber(charging?.max_current_amps),
    currentStepA: toFiniteNumber(charging?.current_step_amps),
    currentUnit: "A",
    voltageV: toFiniteNumber(charging?.voltage_v),
    socPercent: toFiniteNumber(battery?.level_percent),
    locationState: location?.state || baseInfo?.locationState || null,
    locationStatus: location?.status || baseInfo?.locationStatus || null,
    atHome: typeof location?.at_home === "boolean" ? location.at_home : (typeof baseInfo?.atHome === "boolean" ? baseInfo.atHome : null),
    autoControlAllowedByLocation: typeof location?.auto_control_allowed === "boolean"
      ? location.auto_control_allowed
      : (typeof baseInfo?.autoControlAllowedByLocation === "boolean" ? baseInfo.autoControlAllowedByLocation : null),
    autoControlLocationBlockReason: location?.auto_control_block_reason || baseInfo?.autoControlLocationBlockReason || null,
    teslaConnectionState: observation?.charging?.connection_state || null,
    teslaCableConnected: typeof charging?.cable_connected === "boolean" ? charging.cable_connected : null,
    controlAvailable: resolvedControlAvailable,
    controlMode: resolvedControlMode,
    chargingEnabled: resolvedChargingEnabled,
    chargeRequestedEnabled: resolvedChargeRequestedEnabled,
    canStart: Boolean(controlState?.can_start),
    canStop: Boolean(controlState?.can_stop),
    controlFeedback: controlState?.feedback || null,
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
  const location = tesla?.location || {};
  const control = tesla?.control || {};
  const operations = tesla?.operations || {};
  const currentOperation = operations?.set_charge_current || null;
  const result = {
    chargingW: toFiniteNumber(displayItems.tesla_charge_power?.value ?? charging?.power_w),
    entityId: null,
    friendlyName: null,
    updatedAt: combinedFlow?.updated_at || null,
    currentA: toFiniteNumber(displayItems.tesla_charge_current?.value ?? charging?.current_amps),
    configuredCurrentA: toFiniteNumber(displayItems.tesla_configured_current?.value ?? charging?.configured_current_amps),
    minCurrentA: toFiniteNumber(charging?.min_current_amps),
    maxCurrentA: toFiniteNumber(charging?.max_current_amps),
    currentStepA: toFiniteNumber(charging?.current_step_amps),
    currentEntityId: null,
    currentUnit: "A",
    voltageV: toFiniteNumber(charging?.voltage_v),
    minutesToFull: toFiniteNumber(charging?.minutes_to_full),
    chargeCompletionAt: charging?.completion_at || null,
    socPercent: toFiniteNumber(displayItems.tesla_soc?.value ?? battery?.level_percent),
    totalCapacityKwh: toFiniteNumber(battery?.total_capacity_kwh),
    currentEnergyKwh: toFiniteNumber(battery?.current_energy_kwh),
    remainingToFullKwh: toFiniteNumber(battery?.remaining_to_full_kwh),
    socEntityId: null,
    locationState: location?.state || null,
    locationStatus: location?.status || null,
    atHome: typeof location?.at_home === "boolean" ? location.at_home : null,
    autoControlAllowedByLocation: typeof location?.auto_control_allowed === "boolean" ? location.auto_control_allowed : null,
    autoControlLocationBlockReason: location?.auto_control_block_reason || null,
    teslaConnectionState: displayItems.tesla_connection_state?.value || charging?.connection_state || null,
    teslaCableConnected: typeof charging?.cable_connected === "boolean" ? charging.cable_connected : null,
    controlAvailable: Boolean(control?.available),
    controlMode: control?.control_mode || "unavailable",
    chargingEnabled: typeof control?.charging_enabled === "boolean" ? control.charging_enabled : null,
    chargeRequestedEnabled: typeof control?.charge_requested_enabled === "boolean" ? control.charge_requested_enabled : null,
    canStart: Boolean(control?.can_start),
    canStop: Boolean(control?.can_stop),
    controlFeedback: control?.feedback || null,
    controlSwitchEntityId: null,
    controlStartButtonEntityId: null,
    controlStopButtonEntityId: null,
    pendingConfiguredSourceA: toFiniteNumber(currentOperation?.source_amps),
    pendingConfiguredTargetA: toFiniteNumber(currentOperation?.target_amps),
    pendingConfiguredStartedAt: currentOperation?.started_at || null,
    pendingConfiguredStatus: currentOperation?.status || null,
  };
  const configuredCurrentA = toFiniteNumber(result.configuredCurrentA);
  const pendingTargetA = toFiniteNumber(teslaPendingCurrentTargetA) ?? toFiniteNumber(result.pendingConfiguredTargetA);
  if (
    pendingTargetA !== null &&
    configuredCurrentA !== null &&
    Math.abs(configuredCurrentA - pendingTargetA) <= 0.05
  ) {
    teslaPendingCurrentSourceA = null;
    teslaPendingCurrentTargetA = null;
  }
  return result;
}

function formatEnergyReferenceValue(value) {
  const numeric = toFiniteNumber(value);
  if (numeric === null) return "-";
  return `${formatTrimmedDecimal(numeric, 1)} kWh`;
}

function getMobileFlowDirectionState(kind, value) {
  const numeric = toFiniteNumber(value);
  if (numeric === null) return { text: t("mobileDashboardFlowDirectionOffline"), tone: "" };
  if (Math.abs(numeric) < POWER_FLOW_ACTIVE_THRESHOLD_W) return { text: t("mobileDashboardFlowDirectionIdle"), tone: "" };
  if (kind === "grid") {
    return numeric > 0
      ? { text: t("mobileDashboardFlowDirectionImport"), tone: "is-negative" }
      : { text: t("mobileDashboardFlowDirectionExport"), tone: "is-positive" };
  }
  if (kind === "battery") {
    return numeric > 0
      ? { text: t("mobileDashboardFlowDirectionDischarge"), tone: "is-positive" }
      : { text: t("mobileDashboardFlowDirectionCharge"), tone: "is-negative" };
  }
  return { text: t("mobileDashboardFlowDirectionActive"), tone: "is-positive" };
}

function renderMobileFlowMetricRow(label, valueHtml, tone = "") {
  const toneClass = tone ? ` ${tone}` : "";
  return (
    `<div class="mobile-flow-metric-row">` +
    `<span class="mobile-flow-metric-label">${escapeHtml(label)}</span>` +
    `<span class="mobile-flow-metric-value${toneClass}">${valueHtml || "-"}</span>` +
    `</div>`
  );
}

function renderMobileFlowStep({ title, valueHtml, stateText, stateTone = "", rows = [], open = false }) {
  const stateClass = stateTone ? ` ${stateTone}` : "";
  return (
    `<details class="mobile-flow-step"${open ? " open" : ""}>` +
    `<summary>` +
    `<div class="mobile-flow-step-main">` +
    `<span class="mobile-flow-step-title">${escapeHtml(title)}</span>` +
    `<span class="mobile-flow-step-value">${valueHtml || "-"}</span>` +
    `</div>` +
    `<span class="mobile-flow-step-state${stateClass}">${escapeHtml(stateText || "")}</span>` +
    `<span class="mobile-flow-step-chevron" aria-hidden="true"></span>` +
    `</summary>` +
    `<div class="mobile-flow-step-body">${rows.join("")}</div>` +
    `</details>`
  );
}

function renderCombinedUsableBatteryNode(combinedFlow) {
  const metrics = combinedFlow?.metrics || {};
  const displayItem = combinedDisplayItem(combinedFlow, "usable_battery_total");
  const energyReference = combinedFlow?.energy_reference || {};
  let usableBatteryKwh = toFiniteNumber(displayItem?.value ?? energyReference.usable_battery_total_kwh);
  if (usableBatteryKwh === null) {
    const battery1Soc = toFiniteNumber(
      combinedDisplayItem(combinedFlow, "battery1_soc")?.value ?? metrics.battery1_soc_percent,
    );
    const battery2Soc = toFiniteNumber(
      combinedDisplayItem(combinedFlow, "battery2_soc")?.value ?? metrics.battery2_soc_percent,
    );
    const usableValues = [
      batteryUsableKwh("saj", battery1Soc),
      batteryUsableKwh("solplanet", battery2Soc),
    ].filter((value) => value !== null);
    usableBatteryKwh = usableValues.length
      ? usableValues.reduce((total, value) => total + value, 0)
      : null;
  }
  setText("combined-usableBatteryValue", formatEnergyReferenceValue(usableBatteryKwh));
  renderDashboardReserveFloorControl(combinedFlow);
}

function dashboardReserveFloorFromSource(combinedFlow = null, configPayload = null) {
  return toFiniteNumber(configPayload?.solplanet_export_reserve_floor_kwh)
    ?? toFiniteNumber(combinedFlow?.energy_reference?.solplanet_export_reserve_floor_kwh)
    ?? toFiniteNumber(stateCache.lastConfig?.solplanet_export_reserve_floor_kwh)
    ?? 18;
}

function renderDashboardReserveFloorControl(combinedFlow = null, configPayload = null) {
  const reserveFloorKwh = dashboardReserveFloorFromSource(combinedFlow, configPayload);
  const formatted = formatSamplingKwh(reserveFloorKwh, 1);
  setText("combined-usableBatteryReserve", t("dashboardReserveFloorDisplay", { value: formatted }));
  setText("combinedReserveFloorDisplay", t("dashboardReserveFloorDisplay", { value: formatted }));
  const input = document.getElementById("combinedReserveFloorInput");
  if (input && document.activeElement !== input) {
    input.value = String(Number.isInteger(reserveFloorKwh) ? reserveFloorKwh : reserveFloorKwh.toFixed(1));
  }
}

async function saveDashboardReserveFloor() {
  const input = document.getElementById("combinedReserveFloorInput");
  const statusEl = document.getElementById("combinedReserveFloorStatus");
  const button = document.getElementById("combinedReserveFloorSaveBtn");
  const reserveFloorKwh = toFiniteNumber(input?.value);
  if (
    reserveFloorKwh === null
    || reserveFloorKwh < 0
    || reserveFloorKwh > 100
  ) {
    if (statusEl) statusEl.textContent = t("dashboardReserveFloorSaveFailed", { error: "0-100 kWh" });
    return;
  }
  if (statusEl) statusEl.textContent = t("dashboardReserveFloorSaving");
  if (button) button.disabled = true;
  try {
    const current = await fetchJson("/api/config", { timeoutMs: 5000 });
    const payload = {
      ...current,
      solplanet_export_reserve_floor_kwh: reserveFloorKwh,
    };
    const result = await fetchJson("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      timeoutMs: 10000,
    });
    stateCache.lastConfig = result;
    fillConfigForm(result);
    renderDashboardReserveFloorControl(stateCache.lastSummary?.combinedFlow || null, result);
    if (statusEl) statusEl.textContent = t("dashboardReserveFloorSaved");
    void loadCurrentTab();
  } catch (err) {
    if (statusEl) statusEl.textContent = t("dashboardReserveFloorSaveFailed", { error: String(err) });
  } finally {
    if (button) button.disabled = false;
  }
}

function renderTeslaControlButton(teslaInfo = null) {
  const button = document.getElementById("combined-teslaChargingToggleBtn");
  if (!button) return;
  const controlAvailable = Boolean(teslaInfo?.controlAvailable);
  const chargingEnabled = teslaInfo?.chargingEnabled === true;
  let label = t("teslaControlUnavailable");
  if (teslaControlBusy) {
    const targetEnabled = effectiveTeslaControlFeedback(teslaInfo)?.targetEnabled === true;
    label = targetEnabled ? t("teslaControlStarting") : t("teslaControlStopping");
  } else if (controlAvailable) {
    label = chargingEnabled ? t("teslaControlStop") : t("teslaControlStart");
  }
  button.textContent = label;
  button.disabled = teslaControlBusy || teslaCurrentControlBusy || !controlAvailable;
  button.classList.toggle("active", controlAvailable && chargingEnabled);
}

function renderTeslaCurrentControl(teslaInfo = null) {
  const el = document.getElementById("combined-teslaCurrentControlValue");
  const rateEl = document.getElementById("combined-teslaRateValue");
  const rateText = String(rateEl?.dataset?.teslaRateText || "").trim();
  const controlHtml = formatTeslaCurrentControlHtml(teslaInfo);
  if (rateEl && rateText) {
    setHtml(
      "combined-teslaRateValue",
      `<span class="tesla-rate-current-row"><span>${escapeHtml(rateText)}</span>${controlHtml}</span>`,
    );
  }
  if (el) {
    el.hidden = Boolean(rateEl && rateText);
    setHtml("combined-teslaCurrentControlValue", rateEl && rateText ? "" : controlHtml);
  }
  const triggerBtn = document.getElementById("combined-teslaCurrentTriggerBtn");
  if (triggerBtn) {
    triggerBtn.addEventListener("click", () => {
      openTeslaCurrentModal();
    });
  }
}

function ensureTeslaCardUnifiedContent() {
  const socValue = document.getElementById("combined-teslaSocValue");
  if (!socValue) return;
  const stack = socValue.parentElement;
  if (!stack || !stack.classList.contains("soc-value-inside")) return;
  [
    "combined-teslaChargingPowerValue",
    "combined-teslaChargingCurrentValue",
    "combined-teslaCurrentControlValue",
    "combined-teslaConnectionValue",
    "combined-teslaLocationValue",
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
  const gridDiagnostics = combinedFlow?.meta?.grid_diagnostics || null;
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
    gridDiagnostics,
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

function formatCombinedGridSystemName(system) {
  if (system === "saj") return "SAJ";
  if (system === "solplanet") return "SoulPlanet";
  return system ? String(system) : "-";
}

function formatCombinedGridDiagnosticsTip(diagnostics) {
  const candidates = Array.isArray(diagnostics?.candidates) ? diagnostics.candidates : [];
  if (!candidates.length) return "";
  return candidates
    .map((candidate) => {
      const system = typeof candidate?.system === "string" ? candidate.system : "";
      const gridW = toFiniteNumber(candidate?.grid_w);
      const gridText = gridW === null
        ? (currentLang === "zh" ? "不可用" : "unavailable")
        : formatSignedKwFromWatts(gridW);
      return `${formatCombinedGridSystemName(system)}: ${gridText}`;
    })
    .join("\n");
}

function formatCombinedGridValueHtml(gridW, dataKind, diagnostics) {
  const valueHtml = formatValueWithDataKindHtml(formatPowerKwFromWatts(gridW === null ? null : Math.abs(gridW)), dataKind);
  const tipText = formatCombinedGridDiagnosticsTip(diagnostics);
  if (!tipText) return valueHtml;
  const label = currentLang === "zh"
    ? "查看 SAJ 和 Solplanet 电网来源值"
    : "View SAJ and Solplanet grid source values";
  return (
    `<span class="combined-grid-value has-source-tip" tabindex="0" aria-label="${escapeHtml(label)}" data-source-tip="${escapeHtml(tipText)}">` +
    `${valueHtml}` +
    `<span class="grid-source-tip" role="img" aria-hidden="true">!</span>` +
    `</span>`
  );
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
      "<path d='M13 2L6 13h5l-1 9 8-11h-5l0-9z'></path>" +
      "</svg>"
    );
  }
  if (kind === "hub") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<path d='M12 14v5'></path>" +
      "<path d='M7 19h10'></path>" +
      "<circle cx='12' cy='10' r='2'></circle>" +
      "<path d='M8 12a4 4 0 0 1 8 0'></path>" +
      "<path d='M5 11a7 7 0 0 1 14 0'></path>" +
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
  if (kind === "inverter" || kind === "chip") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<rect x='7' y='7' width='10' height='10' rx='2' ry='2'></rect>" +
      "<path d='M10 10h4v4h-4z'></path>" +
      "<path d='M9 3v3'></path>" +
      "<path d='M15 3v3'></path>" +
      "<path d='M9 18v3'></path>" +
      "<path d='M15 18v3'></path>" +
      "<path d='M3 9h3'></path>" +
      "<path d='M3 15h3'></path>" +
      "<path d='M18 9h3'></path>" +
      "<path d='M18 15h3'></path>" +
      "</svg>"
    );
  }
  if (kind === "pool") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<path d='M4 9c1.5 2 3.5 2 5 0c1.5-2 3.5-2 5 0c1.5 2 3.5 2 5 0'></path>" +
      "<path d='M4 14c1.5 2 3.5 2 5 0c1.5-2 3.5-2 5 0c1.5 2 3.5 2 5 0'></path>" +
      "</svg>"
    );
  }
  if (kind === "home") {
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<path d='M4 11.5L12 5l8 6.5'></path>" +
      "<path d='M6.5 10.5V19h11v-8.5'></path>" +
      "<path d='M10 19v-4h4v4'></path>" +
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

function renderMobileEnergyNode({
  cls,
  kind,
  title,
  valueHtml,
  subValueHtml,
  metaHtml = "",
  accent = "cyan",
  active = false,
  dimmed = false,
  action = "",
  hint = "",
}) {
  const stateClass = `${active ? " is-active" : ""}${dimmed ? " is-dimmed" : ""}${action ? " is-clickable" : ""}`;
  const attrs = [
    action ? `data-mobile-action="${escapeHtml(action)}"` : "",
    hint ? `title="${escapeHtml(hint)}"` : "",
  ].filter(Boolean).join(" ");
  return (
    `<article class="mobile-energy-node ${escapeHtml(cls)} accent-${escapeHtml(accent)}${stateClass}" ${attrs}>` +
    `<div class="mobile-energy-icon mobile-energy-icon-${escapeHtml(kind)}">${mobileFlowIconMarkup(kind)}</div>` +
    `<div class="mobile-energy-copy">` +
    `<p class="mobile-energy-title">${escapeHtml(title)}</p>` +
    `<p class="mobile-energy-value">${valueHtml || "-"}</p>` +
    `<p class="mobile-energy-subvalue">${subValueHtml || "&nbsp;"}</p>` +
    `<p class="mobile-energy-meta">${metaHtml || "&nbsp;"}</p>` +
    `</div>` +
    "</article>"
  );
}

function renderMobileEnergyHub({ valueHtml, subValueHtml, active = false }) {
  return (
    `<article class="mobile-energy-node node-hub accent-cyan${active ? " is-active" : ""}">` +
    `<div class="mobile-energy-icon mobile-energy-icon-hub">${mobileFlowIconMarkup("hub")}</div>` +
    `<div class="mobile-energy-copy">` +
    `<p class="mobile-energy-title">${escapeHtml(t("mobileDashboardHubTitle"))}</p>` +
    `<p class="mobile-energy-value">${valueHtml || "-"}</p>` +
    `<p class="mobile-energy-subvalue">${subValueHtml || "&nbsp;"}</p>` +
    `</div>` +
    `</article>`
  );
}

function renderMobileEnergyLine(cls, active = false, reverse = false) {
  const stateClass = `${active ? " is-active" : ""}${reverse ? " is-reverse" : ""}`;
  return `<div class="mobile-energy-line ${escapeHtml(cls)}${stateClass}" aria-hidden="true"></div>`;
}

function renderMobileCombinedFlow({
  combined,
  teslaInfo,
  poolPumpDevice,
  homeLoadW,
  totalLoadW,
  teslaChargingW,
  teslaSoc,
  teslaCurrentEnergyKwh,
  teslaTotalCapacityKwh,
  battery1Soc,
  battery2Soc,
}) {
  const {
    solarW,
    solar2W,
    gridW,
    battery1W,
    battery2W,
    inverter1W,
    inverter2W,
    solarToInverter1W,
    dataKinds,
    gridDiagnostics,
  } = combined;
  const poolPumpW = toFiniteNumber(poolPumpDevice?.metrics?.power_w);
  const gridActive = gridW !== null && Math.abs(gridW) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const gridImport = gridW !== null && gridW > 0;
  const totalLoadActive = totalLoadW !== null && totalLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const teslaChargingActive = teslaChargingW !== null && teslaChargingW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const poolPumpEnabled = poolPumpDevice?.control?.enabled === true;
  const poolPumpAvailable = poolPumpDevice?.control?.available !== false && poolPumpDevice?.control?.switch_entity;
  const poolPumpActive = (poolPumpW !== null && poolPumpW >= POWER_FLOW_ACTIVE_THRESHOLD_W) || poolPumpEnabled;
  const battery1Active = battery1W !== null && Math.abs(battery1W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const battery2Active = battery2W !== null && Math.abs(battery2W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const inverter1Active = inverter1W !== null && Math.abs(inverter1W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const inverter2Active = inverter2W !== null && Math.abs(inverter2W) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const solar1Active = solarToInverter1W !== null && solarToInverter1W >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const solar2Active = solar2W !== null && solar2W >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const homeLoadActive = homeLoadW !== null && homeLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const teslaConnection = formatTeslaConnectionSummary(teslaInfo);
  const teslaLocationText = formatTeslaLocationSummary(teslaInfo);
  const poolPumpStateText = !poolPumpAvailable
    ? t("poolPumpStateUnavailable")
    : poolPumpEnabled
      ? t("poolPumpStateOn")
      : (poolPumpDevice?.control?.enabled === false ? t("poolPumpStateOff") : t("poolPumpStateUnknown"));
  const sajActive = inverter1Active || battery1Active || solar1Active;
  const solplanetActive = inverter2Active || battery2Active || solar2Active;
  const anyHubActive = totalLoadActive || gridActive || sajActive || solplanetActive;
  const teslaEnergyText = teslaCurrentEnergyKwh !== null && teslaTotalCapacityKwh !== null
    ? `${formatTrimmedDecimal(teslaCurrentEnergyKwh, 1)} / ${formatTrimmedDecimal(teslaTotalCapacityKwh, 1)} kWh`
    : teslaConnection;
  const sajSocText = battery1Soc === null ? "-" : `${Math.max(0, Math.min(100, battery1Soc)).toFixed(0)}%`;
  const solplanetSocText = battery2Soc === null ? "-" : `${Math.max(0, Math.min(100, battery2Soc)).toFixed(0)}%`;
  const sajInverterRatioHtml = formatValueWithDataKindHtml(
    formatInverterConversion(inverter1W, battery1W, solarToInverter1W),
    dataKinds.inverterRatio,
  );
  const solplanetInverterRatioHtml = formatValueWithDataKindHtml(
    formatInverterConversion(inverter2W, battery2W, solar2W),
    dataKinds.inverterRatio,
  );
  const teslaValueHtml = formatValueWithDataKindHtml(
    teslaSoc === null ? formatPowerKwFromWatts(teslaChargingW) : `${Math.max(0, Math.min(100, teslaSoc)).toFixed(0)}%`,
    teslaSoc === null ? "real" : dataKinds.teslaSoc,
  );
  const model = {
    nodes: {
      solar: {
        cls: "node-solar",
        kind: "solar",
        title: t("solarTitle"),
        valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(solarW), dataKinds.solar),
        subValueHtml: escapeHtml(solar1Active ? t("stateProducing") : t("stateIdle")),
        metaHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(solarToInverter1W), dataKinds.solarToInverter1),
        accent: "solar",
        active: solar1Active,
        dimmed: false,
      },
      grid: {
        cls: "node-grid",
        kind: "grid",
        title: t("gridTitle"),
        valueHtml: formatCombinedGridValueHtml(gridW, dataKinds.grid, gridDiagnostics),
        subValueHtml: escapeHtml(gridActive ? (gridImport ? t("stateImporting") : t("stateExporting")) : t("stateIdle")),
        metaHtml: escapeHtml(gridImport ? "AC IN" : "AC OUT"),
        accent: "grid",
        active: gridActive,
        dimmed: false,
      },
      saj: {
        cls: "node-saj",
        kind: "chip",
        title: "SAJ",
        valueHtml: escapeHtml(sajSocText),
        subValueHtml: sajInverterRatioHtml,
        metaHtml: escapeHtml(`${formatBatteryEnergyKwh("saj", battery1Soc)} · ${formatBatteryUsableKwh("saj", battery1Soc)}`),
        accent: "cyan",
        active: sajActive,
        dimmed: false,
        action: "saj-profile",
        hint: t("sajProfilePanelKicker"),
      },
      solplanet: {
        cls: "node-solplanet",
        kind: "chip",
        title: "SOLPLANET",
        valueHtml: escapeHtml(solplanetSocText),
        subValueHtml: solplanetInverterRatioHtml,
        metaHtml: escapeHtml(`${formatBatteryEnergyKwh("solplanet", battery2Soc)} · ${formatBatteryUsableKwh("solplanet", battery2Soc)}`),
        accent: "battery",
        active: solplanetActive,
        dimmed: false,
        action: "solplanet-pin",
        hint: t("solplanetDashboardPinOpenHint"),
      },
      hub: {
        cls: "node-hub",
        kind: "hub",
        title: t("mobileDashboardHubTitle"),
        valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(totalLoadW), dataKinds.totalLoad),
        subValueHtml: escapeHtml(totalLoadActive ? t("switchboardStateActive") : t("switchboardStateIdle")),
        accent: "cyan",
        active: anyHubActive,
        dimmed: false,
      },
      load: {
        cls: "node-load",
        kind: "home",
        title: t("loadTitle"),
        valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(homeLoadW), dataKinds.homeLoad),
        subValueHtml: escapeHtml(homeLoadActive ? t("stateConsuming") : t("stateIdle")),
        metaHtml: escapeHtml(homeLoadActive ? "HOME" : "-"),
        accent: "load",
        active: homeLoadActive,
        dimmed: false,
      },
      pool: {
        cls: "node-pool",
        kind: "pool",
        title: t("poolPumpTitle"),
        valueHtml: formatValueWithDataKindHtml(formatPowerKwFromWatts(poolPumpW), "real"),
        subValueHtml: escapeHtml(poolPumpStateText),
        metaHtml: escapeHtml(poolPumpAvailable ? "POOL" : t("poolPumpStateUnavailable")),
        accent: "cyan",
        active: poolPumpActive,
        dimmed: !poolPumpActive && poolPumpW === null,
      },
      tesla: {
        cls: "node-tesla",
        kind: "ev",
        title: t("teslaChargingLabel"),
        valueHtml: teslaValueHtml,
        subValueHtml: escapeHtml(teslaEnergyText),
        metaHtml: escapeHtml(teslaLocationText),
        accent: "tesla",
        active: teslaChargingActive,
        dimmed: !teslaChargingActive && teslaChargingW === null,
      },
    },
    edges: {
      solarDown: { active: solar1Active, reverse: false },
      gridDown: { active: gridActive, reverse: !gridImport },
      sajDown: { active: sajActive, reverse: false },
      solplanetDown: { active: solplanetActive, reverse: false },
      sajLinkH: { active: sajActive, reverse: false },
      solplanetLinkH: { active: solplanetActive, reverse: false },
      hubDown: { active: homeLoadActive || poolPumpActive || teslaChargingActive, reverse: false },
      busLeft: { active: homeLoadActive || poolPumpActive || teslaChargingActive, reverse: false },
      busRight: { active: homeLoadActive || poolPumpActive || teslaChargingActive, reverse: false },
      loadDown: { active: homeLoadActive, reverse: false },
      poolDown: { active: poolPumpActive, reverse: false },
      teslaDown: { active: teslaChargingActive, reverse: false },
    },
  };
  if (applyMobileCombinedFlowDiagram(model)) return;

  const html =
    `<div class="mobile-energy-card">` +
    `<div class="mobile-energy-scene">` +
    renderMobileEnergyLine("line-solar-down", solar1Active, false) +
    renderMobileEnergyLine("line-grid-down", gridActive, !gridImport) +
    renderMobileEnergyLine("line-saj-down", sajActive, false) +
    renderMobileEnergyLine("line-solplanet-down", solplanetActive, false) +
    renderMobileEnergyLine("line-saj-link-v", sajActive, false) +
    renderMobileEnergyLine("line-saj-link-h", sajActive, false) +
    renderMobileEnergyLine("line-solplanet-link-v", solplanetActive, false) +
    renderMobileEnergyLine("line-solplanet-link-h", solplanetActive, false) +
    renderMobileEnergyLine("line-hub-down", homeLoadActive || poolPumpActive || teslaChargingActive, false) +
    renderMobileEnergyLine("line-bottom-bus", homeLoadActive || poolPumpActive || teslaChargingActive, false) +
    renderMobileEnergyLine("line-load-down", homeLoadActive, false) +
    renderMobileEnergyLine("line-pool-down", poolPumpActive, false) +
    renderMobileEnergyLine("line-tesla-down", teslaChargingActive, false) +
    renderMobileEnergyNode(model.nodes.solar) +
    renderMobileEnergyNode(model.nodes.grid) +
    renderMobileEnergyNode(model.nodes.saj) +
    renderMobileEnergyNode(model.nodes.solplanet) +
    renderMobileEnergyHub(model.nodes.hub) +
    renderMobileEnergyNode(model.nodes.load) +
    renderMobileEnergyNode(model.nodes.pool) +
    renderMobileEnergyNode(model.nodes.tesla) +
    `</div>`;

  setHtml("energyFlowCombinedMobile", html);
}

function renderCombinedEnergyFlow(combinedFlow, teslaInfo = null, poolPumpDevice = null, collectorStatus = null) {
  renderCombinedUsableBatteryNode(combinedFlow);
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
    gridDiagnostics,
    sources,
    dataKinds,
  } = combined;
  const teslaChargingW = toFiniteNumber(teslaInfo?.chargingW);
  const teslaCurrentA = toFiniteNumber(teslaInfo?.currentA);
  const teslaConfiguredCurrentA = toFiniteNumber(teslaInfo?.configuredCurrentA);
  const teslaSoc = toFiniteNumber(teslaInfo?.socPercent);
  const teslaCurrentEnergyKwh = toFiniteNumber(teslaInfo?.currentEnergyKwh);
  const teslaTotalCapacityKwh = toFiniteNumber(teslaInfo?.totalCapacityKwh);
  const teslaLocationText = formatTeslaLocationSummary(teslaInfo);
  let homeLoadW = displayHomeLoadW;
  if (homeLoadW === null && totalLoadW !== null) {
    const teslaW = teslaChargingW === null ? 0 : teslaChargingW;
    homeLoadW = Math.max(0, totalLoadW - teslaW);
    if (Math.abs(homeLoadW) <= BALANCE_TOLERANCE_W) homeLoadW = 0;
  }
  const poolPumpW = toFiniteNumber(poolPumpDevice?.metrics?.power_w);
  let residualHomeLoadW = homeLoadW;
  if (residualHomeLoadW !== null && poolPumpW !== null) {
    residualHomeLoadW = Math.max(0, residualHomeLoadW - poolPumpW);
    if (Math.abs(residualHomeLoadW) <= BALANCE_TOLERANCE_W) residualHomeLoadW = 0;
  }
  const poolPumpEnabled = poolPumpDevice?.control?.enabled === true;
  const poolPumpAvailable = poolPumpDevice?.control?.available !== false && poolPumpDevice?.control?.switch_entity;

  const combinedLoadError = Boolean(combinedFlow?.__load_error);
  const combinedPending = !combinedLoadError && solarW === null && gridW === null;
  const hasCombinedBase = solarW !== null && gridW !== null;
  const preserveRefreshing = Boolean(stateCache.systemLoadMeta.combined?.refreshing) && hasCombinedBase;
  const combinedUpdatedAt = latestIsoTime(combinedFlow?.updated_at, teslaInfo?.updatedAt);
  const combinedLate = hasCombinedBase && isIsoOlderThanSeconds(combinedUpdatedAt, 300);
  const issueText = combinedDashboardIssueText(collectorStatus, combinedFlow, {
    isStale: combinedLate,
    failed: combinedLoadError || (!combinedPending && !hasCombinedBase),
  });
  setSystemLoadMeta("combined", {
    phase: combinedPending ? "loading" : (hasCombinedBase ? "done" : "failed"),
    updatedAt: combinedUpdatedAt,
    quality: hasCombinedBase ? getFlowQuality(combinedFlow, combined.availableCount) : "failed",
    count: combined.availableCount,
    refreshing: preserveRefreshing,
    issueText,
  });

  setHtml("combined-solarPowerValue", formatValueWithDataKindHtml(formatPowerKwFromWatts(solarW), dataKinds.solar));
  setHtml("combined-gridPowerValue", formatCombinedGridValueHtml(gridW, dataKinds.grid, gridDiagnostics));
  setHtml(
    "combined-inverter1RatioValue",
    formatValueWithDataKindHtml(formatInverterConversion(inverter1W, battery1W, solarToInverter1W), dataKinds.inverterRatio),
  );
  setHtml(
    "combined-inverter2RatioValue",
    formatValueWithDataKindHtml(formatInverterConversion(inverter2W, battery2W, solar2W), dataKinds.inverterRatio),
  );
  setHtml("combined-loadPowerValue", formatValueWithDataKindHtml(formatPowerKwFromWatts(residualHomeLoadW), dataKinds.homeLoad));
  setHtml("combined-poolPumpPowerValue", formatValueWithDataKindHtml(formatPowerKwFromWatts(poolPumpW), "real"));
  setText(
    "combined-poolPumpState",
    !poolPumpAvailable
      ? t("poolPumpStateUnavailable")
      : poolPumpEnabled
        ? t("poolPumpStateOn")
        : (poolPumpDevice?.control?.enabled === false ? t("poolPumpStateOff") : t("poolPumpStateUnknown")),
  );
  setHtml(
    "combined-teslaChargingPowerValue",
    formatValueWithDataKindHtml(formatTeslaBatteryEnergyValue(teslaCurrentEnergyKwh, teslaTotalCapacityKwh), "real"),
  );
  setHtml(
    "combined-teslaChargingCurrentValue",
    formatTeslaCurrentValueHtml(teslaCurrentA, teslaConfiguredCurrentA, teslaInfo?.currentUnit || "A", dataKinds.teslaCurrent, {
      connectionState: teslaInfo?.teslaConnectionState,
      chargingPowerValue: teslaInfo?.chargingW,
      voltageV: teslaInfo?.voltageV,
    }),
  );
  renderBatterySocDisplay({
    system: null,
    soc: teslaSoc,
    batteryW: null,
    socValueId: "combined-teslaSocValue",
    socFillId: "combined-teslaSocFill",
    socDataKind: dataKinds.teslaSoc,
  });
  setTeslaSocTone(teslaSoc);
  const teslaRateText = formatTeslaSocRateEstimate(teslaTotalCapacityKwh, teslaChargingW);
  setText("combined-teslaRateValue", teslaRateText);
  const teslaRateEl = document.getElementById("combined-teslaRateValue");
  if (teslaRateEl) teslaRateEl.dataset.teslaRateText = teslaRateText;
  setText("combined-teslaLocationValue", teslaLocationText);
  ensureTeslaCardUnifiedContent();
  if (teslaCurrentDraftA === null) {
    teslaCurrentDraftA = teslaConfiguredCurrentA ?? teslaCurrentA ?? teslaInfo?.minCurrentA ?? null;
  }
  renderTeslaCurrentControl(teslaInfo);
  renderTeslaControlButton(teslaInfo);
  setText("combined-switchboardValue", "-");
  setText("combined-inverter1State", getSajDashboardModeText() || inverterStateText(inverter1Status));
  setText("combined-inverter2State", inverterStateText(inverter2Status));
  const sajModeTrigger = document.getElementById("combined-inverter1State");
  if (sajModeTrigger) {
    sajModeTrigger.classList.add("is-clickable");
    sajModeTrigger.setAttribute("title", t("sajProfilePanelKicker"));
  }
  const solplanetTrigger = document.getElementById("combined-inverter2State");
  if (solplanetTrigger) {
    solplanetTrigger.classList.add("is-clickable");
    solplanetTrigger.setAttribute("title", t("solplanetDashboardPinOpenHint"));
  }

  setNodeSourceTip("combined-solarPowerValue", formatMetricSourceText("saj", "solar", sources.solar));
  setNodeSourceTip("combined-gridPowerValue", null);
  setNodeSourceTip(
    "combined-loadPowerValue",
    currentLang === "zh"
      ? `来源: 计算 ${sources.load}\n说明: 其他家庭负载 = 总负载 - 特斯拉充电 - 泳池水泵`
      : `Source: Calculated ${sources.load}\nNote: Remaining home load = total load - Tesla charging - Pool Pump`,
  );
  setNodeSourceTip("combined-poolPumpPowerValue", null);
  setNodeSourceTip("combined-teslaChargingPowerValue", null);
  setNodeSourceTip("combined-teslaChargingCurrentValue", null);
  setNodeSourceTip("combined-teslaSocValue", null);
  setNodeSourceTip("combined-teslaLocationValue", null);

  const solarActive = solarW !== null && solarW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  setText("combined-solarState", solarActive ? t("stateProducing") : t("stateIdle"));
  const gridActive = gridW !== null && Math.abs(gridW) >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const gridImport = gridW !== null && gridW > 0;
  setText("combined-gridState", gridActive ? (gridImport ? t("stateImporting") : t("stateExporting")) : t("stateIdle"));
  const switchboardActive = solarActive || gridActive || (totalLoadW !== null && totalLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W);
  setText("combined-switchboardState", switchboardActive ? t("switchboardStateActive") : t("switchboardStateIdle"));
  const loadActive = residualHomeLoadW !== null && residualHomeLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const poolPumpActive = (poolPumpW !== null && poolPumpW >= POWER_FLOW_ACTIVE_THRESHOLD_W) || poolPumpEnabled;
  const teslaChargingActive = teslaChargingW !== null && teslaChargingW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const teslaConnected = isTeslaConnected(teslaInfo);
  const totalLoadActive = totalLoadW !== null && totalLoadW >= POWER_FLOW_ACTIVE_THRESHOLD_W;
  const downstreamLoadActive = totalLoadActive || loadActive || poolPumpActive || teslaChargingActive;
  setText("combined-loadState", loadActive ? t("stateConsuming") : t("stateIdle"));
  setFlowLine("combined-lineSwitchboardToTotalLoad", downstreamLoadActive, false);
  setFlowLine("combined-lineSwitchboardToHomeLoad", loadActive, false);
  setFlowLine("combined-lineSwitchboardToPoolPump", poolPumpActive, false);
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
  const battery2WindowForecastHtml = formatSolplanetWindowForecastHtml(battery2Soc, battery2W);
  setHtml("combined-battery2RuntimeValue", battery2WindowForecastHtml);
  const battery2WindowForecastEl = document.getElementById("combined-battery2RuntimeValue");
  if (battery2WindowForecastEl) {
    battery2WindowForecastEl.classList.toggle("hidden", !battery2WindowForecastHtml);
  }

  const gridHigh = gridW !== null && Math.abs(gridW) > 15000;
  const gridPowerEl = document.getElementById("combined-gridPowerValue");
  const gridStateEl = document.getElementById("combined-gridState");
  if (gridPowerEl) gridPowerEl.classList.toggle("alert-high", gridHigh);
  if (gridStateEl) gridStateEl.classList.toggle("alert-high", gridHigh);

  setFlowValueLabel("combined-flowLabelGridToSwitchboard", gridW, gridActive, dataKinds.grid);
  setFlowValueLabel("combined-flowLabelSolarToInverter1", solarToInverter1W, solarToInverter1Active, dataKinds.solarToInverter1);
  setFlowValueLabel("combined-flowLabelSwitchboardToTotalLoad", totalLoadW, totalLoadActive, dataKinds.totalLoad);
  setFlowValueLabel("combined-flowLabelSwitchboardToHomeLoad", residualHomeLoadW, loadActive, dataKinds.homeLoad);
  setFlowValueLabel("combined-flowLabelSwitchboardToPoolPump", poolPumpW, poolPumpActive, "real");
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
    poolPumpDevice,
    homeLoadW: residualHomeLoadW,
    totalLoadW,
    teslaChargingW,
    teslaSoc,
    teslaCurrentEnergyKwh,
    teslaTotalCapacityKwh,
    battery1Soc,
    battery2Soc,
  });
  const poolPumpBtn = document.getElementById("combined-poolPumpToggleBtn");
  if (poolPumpBtn) {
    poolPumpBtn.disabled = poolPumpControlBusy || !poolPumpAvailable;
    poolPumpBtn.classList.toggle("active", poolPumpEnabled);
    if (poolPumpControlBusy) poolPumpBtn.textContent = t("poolPumpActionBusy");
    else if (!poolPumpAvailable) poolPumpBtn.textContent = t("poolPumpStateUnavailable");
    else poolPumpBtn.textContent = poolPumpEnabled ? t("poolPumpActionTurnOff") : t("poolPumpActionTurnOn");
  }
}

async function togglePoolPump() {
  const summary = stateCache.lastSummary || { combinedFlow: { metrics: {} }, collectorStatus: null };
  const device = summary.poolPump || null;
  const enabled = device?.control?.enabled;
  if (typeof enabled !== "boolean" || poolPumpControlBusy) return;
  poolPumpControlBusy = true;
  renderSummary(summary);
  try {
    const result = await fetchJson("/api/devices/pool-pump/toggle", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: !enabled }),
      timeoutMs: 12000,
    });
    summary.poolPump = result?.device || null;
    stateCache.lastSummary = summary;
    writeCachedDashboardSummary(summary);
    renderSummary(summary);
  } catch (err) {
    console.error("Pool pump toggle failed", err);
    const poolPumpStateEl = document.getElementById("combined-poolPumpState");
    if (poolPumpStateEl) poolPumpStateEl.textContent = t("poolPumpControlFailed", { error: String(err) });
  } finally {
    poolPumpControlBusy = false;
    renderSummary(stateCache.lastSummary || summary);
  }
}

function renderTeslaPlugReminder(payload) {
  const root = document.getElementById("teslaPlugReminder");
  if (!root) return;
  const item = payload?.dashboardNotifications?.items?.find((entry) => entry.code === "tesla_free_energy_plug_reminder");
  const eligible = payload?.collectorStatus?.systems?.combined?.last_tesla_plug_reminder?.eligible;
  root.classList.toggle("hidden", !item || eligible === false);
  if (!item || eligible === false) return;
  const messages = String(item.message || "").split(" / ");
  setText("teslaPlugReminderMessage", currentLang === "zh" ? messages[0] : (messages[1] || messages[0]));
  const button = document.getElementById("teslaPlugReminderDismiss");
  button.textContent = currentLang === "zh" ? "知道了" : "Dismiss";
  button.onclick = async () => {
    button.disabled = true;
    try {
      await fetchJson("/api/dashboard/notifications/dismiss", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ notification_key: item.notification_key }),
      });
      payload.dashboardNotifications.items = payload.dashboardNotifications.items.filter((entry) => entry.notification_key !== item.notification_key);
      renderTeslaPlugReminder(payload);
    } catch (error) {
      window.alert(String(error));
    } finally {
      button.disabled = false;
    }
  };
}

function renderSummary(payload) {
  renderTeslaPlugReminder(payload);
  const { combinedFlow, collectorStatus } = payload;
  const tesla = teslaInfoFromCombinedFlow(combinedFlow);
  const poolPump = payload.poolPump || combinedFlow?.pool_pump || null;
  const combinedCount = Array.isArray(combinedFlow?.display?.order)
    ? combinedFlow.display.order.length
    : 0;
  setSystemLoadMeta("combined", {
    quality: getFlowQuality(combinedFlow, Number(combinedCount) || 0),
    count: Number(combinedCount) || 0,
    issueText: combinedDashboardIssueText(collectorStatus, combinedFlow, {
      isStale: isIsoOlderThanSeconds(combinedFlow?.updated_at, 300),
      failed: Boolean(combinedFlow?.__load_error),
    }),
  });
  renderCombinedEnergyFlow(combinedFlow, tesla, poolPump, collectorStatus);
}

function dashboardNotificationLevelText(level) {
  if (level === "alarm") return t("dashboardNotificationLevelAlarm");
  if (level === "warning") return t("dashboardNotificationLevelWarning");
  return t("dashboardNotificationLevelInfo");
}

function notificationMatrixRuleKindText(kind) {
  return kind === "operation"
    ? t("notificationMatrixRuleTypeOperation")
    : t("notificationMatrixRuleTypeNotification");
}

function renderNotificationMatrixRuleItem(item, windowItem) {
  const level = String(item.level || "info").trim().toLowerCase();
  const kind = String(item.kind || "notification").trim().toLowerCase() === "operation" ? "operation" : "notification";
  const ruleCode = String(item.code || "").trim();
  const editable = isTimeWindowRuleEditable(ruleCode);
  const enabled = isTimeWindowRuleEnabled(ruleCode);
  const busy = editable ? timeWindowRuleBusy.has(ruleCode) : teslaAutoControlBusy;
  const progress = getNotificationMatrixProgress(item, windowItem, stateCache.lastNotificationSummary, { enabled, busy });
  return `
    <article class="notification-rule-item kind-${escapeHtml(kind)} level-${escapeHtml(level)}${enabled ? "" : " is-disabled"}">
      <label class="notification-rule-check" aria-label="${escapeHtml(t(item.titleKey))}">
        <input type="checkbox" ${enabled ? "checked" : ""} ${(busy || !editable) ? "disabled" : ""} data-rule-code="${escapeHtml(ruleCode)}" />
      </label>
      <div class="notification-rule-main">
        <div class="notification-rule-topline">
          <span class="notification-rule-kind kind-${escapeHtml(kind)}">${escapeHtml(notificationMatrixRuleKindText(kind))}</span>
          <span class="notification-rule-level">${escapeHtml(dashboardNotificationLevelText(level))}</span>
          <span class="notification-rule-code">${escapeHtml(String(item.code || "-"))}</span>
        </div>
        <p class="notification-rule-title">${escapeHtml(t(item.titleKey))}</p>
        <p class="notification-rule-trigger">${escapeHtml(t(item.triggerKey))}</p>
        ${kind === "operation" ? renderNotificationMatrixOperationConditions(progress) : ""}
        ${renderNotificationMatrixProgress(progress)}
      </div>
    </article>
  `;
}

function renderNotificationMatrixOperationConditions(progress) {
  const conditions = Array.isArray(progress?.conditions) ? progress.conditions : [];
  if (conditions.length === 0) return "";
  const matchedCount = conditions.filter((item) => item?.matched).length;
  return `
    <section class="notification-rule-conditions">
      <div class="notification-rule-conditions-header">
        <span class="notification-rule-conditions-title">${escapeHtml(t("notificationMatrixOperationConditionsTitle"))}</span>
        <span class="notification-rule-conditions-summary">${escapeHtml(t("notificationMatrixOperationConditionsMatched", { matched: matchedCount, total: conditions.length }))}</span>
      </div>
      <div class="notification-rule-conditions-list">
        ${conditions.map((item) => `
          <div class="notification-rule-condition${item?.matched ? " is-matched" : " is-not-matched"}">
            <span class="notification-rule-condition-state">${escapeHtml(t(item?.matched ? "notificationMatrixConditionMatched" : "notificationMatrixConditionNotMatched"))}</span>
            <span class="notification-rule-condition-label">${escapeHtml(String(item?.label || "-"))}</span>
            <span class="notification-rule-condition-value">${escapeHtml(String(item?.value || "-"))}</span>
          </div>
        `).join("")}
      </div>
    </section>
  `;
}

function renderNotificationMatrixRuleGroup(kind, items, windowItem) {
  if (!Array.isArray(items) || items.length === 0) return "";
  const label = notificationMatrixRuleKindText(kind);
  return `
    <section class="notification-rule-group kind-${escapeHtml(kind)}">
      <div class="notification-rule-group-header">
        <div class="notification-rule-group-heading">
          <span class="notification-rule-group-pill kind-${escapeHtml(kind)}">${escapeHtml(label)}</span>
          <span class="notification-rule-group-count">${escapeHtml(String(items.length))}</span>
        </div>
      </div>
      <div class="notification-rule-group-list">
        ${items.map((item) => renderNotificationMatrixRuleItem(item, windowItem)).join("")}
      </div>
    </section>
  `;
}

function getTimeWindowRuleEnabledMap() {
  const items = Array.isArray(stateCache.lastTimeWindowRules?.items) ? stateCache.lastTimeWindowRules.items : [];
  const map = {};
  for (const item of items) {
    const code = String(item?.rule_code || "").trim();
    if (!code) continue;
    map[code] = Boolean(item?.enabled);
  }
  return map;
}

function getTimeWindowRuleStateItem(ruleCode) {
  const normalized = String(ruleCode || "").trim();
  if (!normalized) return null;
  const items = Array.isArray(stateCache.lastTimeWindowRules?.items) ? stateCache.lastTimeWindowRules.items : [];
  return items.find((item) => String(item?.rule_code || "").trim() === normalized) || null;
}

function isTimeWindowRuleEnabled(ruleCode) {
  const normalized = String(ruleCode || "").trim();
  if (!normalized) return true;
  return getTimeWindowRuleEnabledMap()[normalized] !== false;
}

function isTimeWindowRuleEditable(ruleCode) {
  const item = getTimeWindowRuleStateItem(ruleCode);
  return item?.editable !== false;
}

function renderNotificationMatrixPriorityCard() {
  const priority = stateCache.lastTimeWindowRules?.priority || null;
  if (!priority) return "";
  const tesla = priority?.tesla || {};
  const pump = priority?.pool_pump || {};
  const override = priority?.override || {};
  const teslaAutoControlEnabled = tesla?.auto_control_enabled !== false;
  const pumpAutoControlEnabled = pump?.auto_control_enabled !== false;
  const windows = Array.isArray(pump?.windows) ? pump.windows : [];
  const currentWindow = windows.find((item) => item?.id === "current") || {};
  const formatHours = (value) => (Number.isFinite(Number(value)) ? `${formatTrimmedDecimal(Number(value), 2)}h` : "-");
  const formatScore = (value) => (Number.isFinite(Number(value)) ? formatTrimmedDecimal(Number(value), 1) : "-");
  const formatPercent = (value, digits = 1) => (Number.isFinite(Number(value)) ? `${formatTrimmedDecimal(Number(value), digits)}%` : "-");
  const formatScoreWithOverride = (effectiveValue, originalValue, isOverridden) => {
    if (!isOverridden) return formatScore(effectiveValue);
    return t("notificationMatrixPriorityOverriddenScore", {
      score: formatScore(effectiveValue),
      original: formatScore(originalValue),
    });
  };
  const weightText = (Array.isArray(pump?.weights) ? pump.weights : [])
    .map((item) => {
      const rawLabel = String(item?.label || "").trim().toLowerCase();
      const label = rawLabel === "night"
        ? t("notificationMatrixPriorityWeightNight")
        : rawLabel === "day"
          ? t("notificationMatrixPriorityWeightDay")
          : (item?.label || item?.range || "-");
      return `${label}${item?.range ? ` (${item.range})` : ""} × ${formatTrimmedDecimal(Number(item?.weight), 1)}`;
    })
    .join(" · ");
  const teslaPriorityText = teslaAutoControlEnabled
    ? formatScoreWithOverride(tesla?.score, tesla?.original_score, String(override?.target || "") === "tesla")
    : t("notificationMatrixPriorityTeslaAutoControlDisabled");
  const pumpPriorityText = pumpAutoControlEnabled
    ? formatScoreWithOverride(pump?.score, pump?.original_score, String(override?.target || "") === "pool_pump")
    : t("notificationMatrixPriorityPumpAutoControlDisabled");
  const teslaAutoControlStateText = teslaAutoControlBusy
    ? t("notificationMatrixProgressSaving")
    : t(teslaAutoControlEnabled
      ? "notificationMatrixPriorityTeslaAutoControlEnabled"
      : "notificationMatrixPriorityTeslaAutoControlDisabled");
  const teslaForceChargeThresholdSocPercent = Number(tesla?.force_max_charge_threshold_soc_percent);
  const teslaForceChargeThresholdInputValue = Number.isFinite(teslaForceChargeThresholdSocPercent)
    ? String(Math.round(teslaForceChargeThresholdSocPercent))
    : "45";
  const poolPumpForceMinRuntimeHours = Number(pump?.force_min_runtime_hours);
  const poolPumpForceMinRuntimeWindowHours = Number(pump?.rolling_runtime_window_hours);
  const poolPumpForceMinRuntimeLimit = Number.isFinite(poolPumpForceMinRuntimeWindowHours)
    ? poolPumpForceMinRuntimeWindowHours
    : 24;
  const poolPumpForceMinRuntimeInputValue = Number.isFinite(poolPumpForceMinRuntimeHours)
    ? formatTrimmedDecimal(poolPumpForceMinRuntimeHours, 1)
    : "1";
  const teslaSocText = formatPercent(tesla?.soc_percent, 1);
  const teslaModelText = [
    formatPercent(tesla?.target_soc_percent, 0),
    formatPercent(tesla?.high_urgency_soc_percent, 0),
    formatPercent(tesla?.critical_soc_percent, 0),
  ].join(" · ");
  const pumpRollingRuntimeText = formatHours(pump?.rolling_runtime_hours_24h);
  const pumpAutoControlStateText = poolPumpAutoControlBusy
    ? t("notificationMatrixProgressSaving")
    : t(pumpAutoControlEnabled
      ? "notificationMatrixPriorityPumpAutoControlEnabled"
      : "notificationMatrixPriorityPumpAutoControlDisabled");
  const winner = String(priority?.winner || "tie").trim();
  const winnerLabel = winner === "pool_pump"
    ? t("notificationMatrixPriorityPumpLabel")
    : winner === "none"
      ? t("notificationMatrixPriorityWinnerNone")
      : t("notificationMatrixPriorityTeslaLabel");
  const isCollapsed = notificationMatrixPriorityCollapsed;
  return `
    <article class="notification-priority-card${isCollapsed ? " is-collapsed" : ""}">
      <div class="notification-priority-header">
        <div>
          <h3 class="notification-priority-title">${escapeHtml(t("notificationMatrixPriorityTitle"))}</h3>
          ${isCollapsed ? "" : `<p class="notification-priority-summary">${escapeHtml(t("notificationMatrixPrioritySummary"))}</p>`}
          ${isCollapsed ? "" : `<p class="notification-priority-meta">${escapeHtml(t("notificationMatrixPriorityBusinessDay", { value: String(priority?.business_day?.definition || "-") }))}</p>`}
        </div>
        <div class="notification-priority-header-actions">
          <div class="notification-priority-winner">${escapeHtml(t("notificationMatrixPriorityWinner", { winner: winnerLabel }))}</div>
          <button
            type="button"
            class="notification-priority-toggle"
            aria-expanded="${isCollapsed ? "false" : "true"}"
            aria-label="${escapeHtml(t(isCollapsed ? "notificationMatrixPriorityExpand" : "notificationMatrixPriorityCollapse"))}"
          >
            <span class="notification-priority-toggle-chevron${isCollapsed ? " is-collapsed" : ""}" aria-hidden="true"></span>
          </button>
        </div>
      </div>
      <div class="notification-priority-score-strip">
        <div class="notification-priority-score-pill">
          <span>${escapeHtml(t("notificationMatrixPriorityTeslaLabel"))}</span>
          <strong>${escapeHtml(teslaPriorityText)}</strong>
        </div>
        <div class="notification-priority-score-pill">
          <span>${escapeHtml(t("notificationMatrixPriorityPumpLabel"))}</span>
          <strong>${escapeHtml(pumpPriorityText)}</strong>
        </div>
      </div>
      ${isCollapsed ? "" : `
      <div class="notification-priority-override">
        <span class="notification-priority-override-label">${escapeHtml(t("notificationMatrixPriorityOverrideLabel"))}</span>
        <label class="notification-priority-radio">
          <input type="radio" name="time-window-priority-override" value="auto" ${String(override?.target || "auto") === "auto" ? "checked" : ""} ${timeWindowPriorityOverrideBusy ? "disabled" : ""} />
          <span>${escapeHtml(t("notificationMatrixPriorityOverrideAuto"))}</span>
        </label>
        <label class="notification-priority-radio">
          <input type="radio" name="time-window-priority-override" value="tesla" ${String(override?.target || "") === "tesla" ? "checked" : ""} ${(timeWindowPriorityOverrideBusy || !teslaAutoControlEnabled || teslaAutoControlBusy) ? "disabled" : ""} />
          <span>${escapeHtml(t("notificationMatrixPriorityOverrideTesla"))}</span>
        </label>
        <label class="notification-priority-radio">
          <input type="radio" name="time-window-priority-override" value="pool_pump" ${String(override?.target || "") === "pool_pump" ? "checked" : ""} ${(timeWindowPriorityOverrideBusy || !pumpAutoControlEnabled || poolPumpAutoControlBusy) ? "disabled" : ""} />
          <span>${escapeHtml(t("notificationMatrixPriorityOverridePump"))}</span>
        </label>
      </div>
      <div class="notification-priority-grid">
        <section class="notification-priority-device">
          <div class="notification-priority-device-topline">
            <span class="notification-priority-device-name">${escapeHtml(t("notificationMatrixPriorityTeslaLabel"))}</span>
            <strong class="notification-priority-score">${escapeHtml(teslaPriorityText)}</strong>
          </div>
          <label class="notification-priority-checkbox">
            <input type="checkbox" name="tesla-auto-control" ${teslaAutoControlEnabled ? "checked" : ""} ${teslaAutoControlBusy ? "disabled" : ""} />
            <span class="notification-priority-checkbox-copy">
              <strong>${escapeHtml(t("notificationMatrixPriorityTeslaAutoControlLabel"))}</strong>
              <span>${escapeHtml(teslaAutoControlStateText)}</span>
            </span>
          </label>
          <p class="notification-priority-checkbox-hint">${escapeHtml(t("notificationMatrixPriorityTeslaAutoControlHint"))}</p>
          <label class="notification-priority-setting${!teslaAutoControlEnabled ? " is-disabled" : ""}">
            <span class="notification-priority-setting-copy">
              <strong>${escapeHtml(t("notificationMatrixPriorityTeslaForceThresholdLabel"))}</strong>
              <span>${escapeHtml(t("notificationMatrixPriorityTeslaForceThresholdHint"))}</span>
            </span>
            <span class="notification-priority-number-field">
              <input
                type="number"
                name="tesla-force-threshold"
                min="0"
                max="100"
                step="1"
                inputmode="numeric"
                value="${escapeHtml(teslaForceChargeThresholdInputValue)}"
                ${(!teslaAutoControlEnabled || teslaForceMaxChargeThresholdBusy || teslaAutoControlBusy) ? "disabled" : ""}
              />
              <span>%</span>
            </span>
          </label>
          <div class="notification-priority-stats">
            <div class="notification-priority-stat">
              <span>${escapeHtml(t("notificationMatrixPrioritySoc"))}</span>
              <strong>${escapeHtml(teslaSocText)}</strong>
            </div>
            <div class="notification-priority-stat">
              <span>${escapeHtml(t("notificationMatrixPriorityModel"))}</span>
              <strong>${escapeHtml(teslaModelText)}</strong>
            </div>
          </div>
          <div class="notification-priority-formula-label">${escapeHtml(t("notificationMatrixPriorityFormula"))}</div>
          <div class="notification-priority-formula-stack">
            <code class="notification-priority-formula">${escapeHtml(String(tesla?.formula || "-"))}</code>
          </div>
        </section>
        <section class="notification-priority-device">
          <div class="notification-priority-device-topline">
            <span class="notification-priority-device-name">${escapeHtml(t("notificationMatrixPriorityPumpLabel"))}</span>
            <strong class="notification-priority-score">${escapeHtml(pumpPriorityText)}</strong>
          </div>
          <label class="notification-priority-checkbox">
            <input type="checkbox" name="pool-pump-auto-control" ${pumpAutoControlEnabled ? "checked" : ""} ${poolPumpAutoControlBusy ? "disabled" : ""} />
            <span class="notification-priority-checkbox-copy">
              <strong>${escapeHtml(t("notificationMatrixPriorityPumpAutoControlLabel"))}</strong>
              <span>${escapeHtml(pumpAutoControlStateText)}</span>
            </span>
          </label>
          <p class="notification-priority-checkbox-hint">${escapeHtml(t("notificationMatrixPriorityPumpAutoControlHint"))}</p>
          <label class="notification-priority-setting${!pumpAutoControlEnabled ? " is-disabled" : ""}">
            <span class="notification-priority-setting-copy">
              <strong>${escapeHtml(t("notificationMatrixPriorityPumpForceRuntimeLabel"))}</strong>
              <span>${escapeHtml(t("notificationMatrixPriorityPumpForceRuntimeHint"))}</span>
            </span>
            <span class="notification-priority-number-field">
              <input
                type="number"
                name="pool-pump-force-min-runtime-hours"
                min="0"
                max="${escapeHtml(String(poolPumpForceMinRuntimeLimit))}"
                step="0.1"
                inputmode="decimal"
                value="${escapeHtml(poolPumpForceMinRuntimeInputValue)}"
                ${(!pumpAutoControlEnabled || poolPumpForceMinRuntimeBusy || poolPumpAutoControlBusy) ? "disabled" : ""}
              />
              <span>h</span>
            </span>
          </label>
          <div class="notification-priority-stats">
            <div class="notification-priority-stat">
              <span>${escapeHtml(t("notificationMatrixPriorityRollingRuntime"))}</span>
              <strong>${escapeHtml(pumpRollingRuntimeText)}</strong>
            </div>
            <div class="notification-priority-stat">
              <span>${escapeHtml(t("notificationMatrixPriorityModel"))}</span>
              <strong>${escapeHtml(weightText || "-")}</strong>
            </div>
          </div>
          <div class="notification-priority-formula-label">${escapeHtml(t("notificationMatrixPriorityFormula"))}</div>
          <div class="notification-priority-formula-stack">
            <code class="notification-priority-formula">${escapeHtml(String(pump?.formula || "-"))}</code>
            <code class="notification-priority-formula">${escapeHtml(String(pump?.runtime_formula || "-"))}</code>
          </div>
        </section>
      </div>
      <section class="notification-priority-window-panel">
        <div class="notification-priority-window-header">
          <strong>${escapeHtml(`${t("notificationMatrixPriorityPumpLabel")} · ${t("notificationMatrixPriorityCurrentWindow")}`)}</strong>
        </div>
        <div class="notification-priority-window-stats">
          <div class="notification-priority-window-stat">
            <span>${escapeHtml(t("notificationMatrixPriorityRawRuntime"))}</span>
            <strong>${escapeHtml(formatHours(currentWindow?.raw_runtime_hours))}</strong>
          </div>
          <div class="notification-priority-window-stat">
            <span>${escapeHtml(t("notificationMatrixPriorityDayRuntime"))}</span>
            <strong>${escapeHtml(formatHours(currentWindow?.day_runtime_hours))}</strong>
          </div>
          <div class="notification-priority-window-stat">
            <span>${escapeHtml(t("notificationMatrixPriorityNightRuntime"))}</span>
            <strong>${escapeHtml(formatHours(currentWindow?.night_runtime_hours))}</strong>
          </div>
          <div class="notification-priority-window-stat">
            <span>${escapeHtml(t("notificationMatrixPriorityWeightedRuntime"))}</span>
            <strong>${escapeHtml(formatHours(currentWindow?.weighted_runtime_hours))}</strong>
          </div>
          <div class="notification-priority-window-stat">
            <span>${escapeHtml(t("notificationMatrixPriorityDeficit"))}</span>
            <strong>${escapeHtml(formatHours(currentWindow?.deficit_weighted_hours))}</strong>
          </div>
        </div>
      </section>
      `}
    </article>
  `;
}

function renderNotificationMatrix() {
  const root = document.getElementById("notificationMatrixPanels");
  const overlay = document.getElementById("notificationMatrixLoadingOverlay");
  if (!root) return;
  const isLoading = Boolean(tabLoadState.notificationMatrix?.inFlight);
  if (overlay) {
    overlay.classList.add("hidden");
    overlay.setAttribute("aria-hidden", "true");
  }
  if (isLoading) {
    root.innerHTML = `
      <article class="notification-matrix-loading-shell is-standalone" aria-live="polite" aria-busy="true">
        <div class="notification-matrix-loading-visual" aria-hidden="true">
          <div class="notification-matrix-loading-glow"></div>
          <div class="notification-matrix-loading-orbit orbit-a">
            <span class="notification-matrix-loading-dot"></span>
          </div>
          <div class="notification-matrix-loading-orbit orbit-b">
            <span class="notification-matrix-loading-dot"></span>
          </div>
          <div class="notification-matrix-loading-core">
            <span class="notification-matrix-loading-core-ring ring-a"></span>
            <span class="notification-matrix-loading-core-ring ring-b"></span>
            <span class="notification-matrix-loading-core-disc"></span>
          </div>
          <span class="notification-matrix-loading-chip chip-tesla">Tesla</span>
          <span class="notification-matrix-loading-chip chip-pump">Pump</span>
          <span class="notification-matrix-loading-chip chip-rules">Rules</span>
        </div>
        <strong>${escapeHtml(t("notificationMatrixLoadingTitle"))}</strong>
        <p>${escapeHtml(t("notificationMatrixLoadingBody"))}</p>
      </article>
    `;
    return;
  }
  root.innerHTML = renderNotificationMatrixPriorityCard();
  const priorityToggle = root.querySelector(".notification-priority-toggle");
  if (priorityToggle) {
    priorityToggle.addEventListener("click", () => {
      notificationMatrixPriorityCollapsed = !notificationMatrixPriorityCollapsed;
      renderNotificationMatrix();
    });
  }
  for (const radio of root.querySelectorAll('input[name="time-window-priority-override"]')) {
    radio.addEventListener("change", (event) => {
      const target = event.currentTarget;
      void updateTimeWindowPriorityOverride(target?.value || "auto");
    });
  }
  const teslaAutoControlCheckbox = root.querySelector('input[name="tesla-auto-control"]');
  if (teslaAutoControlCheckbox) {
    teslaAutoControlCheckbox.addEventListener("change", (event) => {
      const target = event.currentTarget;
      void updateTeslaAutoControl(Boolean(target?.checked));
    });
  }
  const teslaForceThresholdInput = root.querySelector('input[name="tesla-force-threshold"]');
  if (teslaForceThresholdInput) {
    teslaForceThresholdInput.addEventListener("change", (event) => {
      const target = event.currentTarget;
      const nextValue = Number(target?.value);
      if (!Number.isFinite(nextValue) || !Number.isInteger(nextValue) || nextValue < 0 || nextValue > 100) {
        window.alert(t("notificationMatrixPriorityTeslaForceThresholdInvalid"));
        renderNotificationMatrix();
        return;
      }
      void updateTeslaForceMaxChargeThreshold(nextValue);
    });
  }
  const pumpAutoControlCheckbox = root.querySelector('input[name="pool-pump-auto-control"]');
  if (pumpAutoControlCheckbox) {
    pumpAutoControlCheckbox.addEventListener("change", (event) => {
      const target = event.currentTarget;
      void updatePoolPumpAutoControl(Boolean(target?.checked));
    });
  }
  const poolPumpForceMinRuntimeInput = root.querySelector('input[name="pool-pump-force-min-runtime-hours"]');
  if (poolPumpForceMinRuntimeInput) {
    poolPumpForceMinRuntimeInput.addEventListener("change", (event) => {
      const target = event.currentTarget;
      const nextValue = Number(target?.value);
      const maxHours = Number(target?.max || 24);
      if (!Number.isFinite(nextValue) || nextValue < 0 || nextValue > maxHours) {
        window.alert(t("notificationMatrixPriorityPumpForceRuntimeInvalid", {
          maxHours: formatTrimmedDecimal(maxHours, 1),
        }));
        renderNotificationMatrix();
        return;
      }
      void updatePoolPumpForceMinRuntimeHours(nextValue);
    });
  }
  const currentWindowId = getCurrentNotificationMatrixWindowId(stateCache.lastNotificationSummary);
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
    const rules = Array.isArray(windowItem.rules) ? windowItem.rules : [];
    const operationRules = rules.filter((item) => String(item?.kind || "").trim().toLowerCase() === "operation");
    const notificationRules = rules.filter((item) => String(item?.kind || "notification").trim().toLowerCase() !== "operation");
    const windowRateText = windowItem.rateKey ? t(windowItem.rateKey) : "";
    const windowScheduleText = [String(windowItem.schedule || "-"), windowRateText].filter(Boolean).join(" · ");
    const listHtml = rules.length
      ? [
        renderNotificationMatrixRuleGroup("operation", operationRules, windowItem),
        renderNotificationMatrixRuleGroup("notification", notificationRules, windowItem),
      ].filter(Boolean).join("")
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
            <span class="notification-window-title-time">${escapeHtml(windowScheduleText)}</span>
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
    for (const checkbox of panel.querySelectorAll(".notification-rule-check input[data-rule-code]:not([disabled])")) {
      checkbox.addEventListener("click", (event) => {
        event.stopPropagation();
      });
      checkbox.addEventListener("change", (event) => {
        const target = event.currentTarget;
        const ruleCode = target?.getAttribute("data-rule-code");
        void updateTimeWindowRuleState(ruleCode, Boolean(target?.checked));
      });
    }
    root.appendChild(panel);
  }
}

function getCurrentNotificationMatrixWindowId(summary = null, now = new SiteDate()) {
  const visibleWindowIds = new Set(NOTIFICATION_MATRIX_WINDOWS.map((item) => item.id));
  const combined = summary?.collectorStatus?.systems?.combined || {};
  const middayWindow = String(combined?.last_midday_window_check?.window_mode || "").trim();
  if (middayWindow && middayWindow !== "off" && visibleWindowIds.has(middayWindow)) return middayWindow;
  const sajWatchWindow = String(combined?.last_saj_battery_watch_check?.window_mode || "").trim();
  if (sajWatchWindow && sajWatchWindow !== "off" && visibleWindowIds.has(sajWatchWindow)) return sajWatchWindow;
  const hour = now.getHours();
  if (hour >= 11 && hour < 15) return "free_energy";
  if (hour >= 15 && hour < 16) return "after_free_shoulder";
  if (hour >= 16 && hour < 23) return "export_window";
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
  const barHtml = progress.hideBar
    ? ""
    : `
      <div class="notification-rule-progress-bar" aria-hidden="true">
        <span class="notification-rule-progress-fill" style="width: ${width.toFixed(1)}%"></span>
        ${markerHtml}
      </div>
    `;
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
      ${barHtml}
    </div>
  `;
}

function getNotificationMatrixProgress(item, windowItem, summary, options = {}) {
  const code = String(item?.code || "").trim();
  const enabled = options?.enabled !== false;
  const busy = Boolean(options?.busy);
  if (!enabled || busy) {
    return {
      ratio: 0,
      statusText: t(busy ? "notificationMatrixProgressSaving" : "notificationMatrixProgressDisabled"),
      currentText: t("notificationMatrixProgressCurrent", { value: "-" }),
      triggerText: "-",
      deltaText: "",
      hideBar: true,
    };
  }
  const currentWindowId = getCurrentNotificationMatrixWindowId(summary);
  const combinedSystems = summary?.collectorStatus?.systems?.combined || {};
  const live = summary?.combinedFlow?.notification_metrics || {};
  const metrics = summary?.combinedFlow?.metrics || {};
  const midday = combinedSystems.last_midday_window_check || {};
  const sajWatch = combinedSystems.last_saj_battery_watch_check || {};
  if (code === "saj_profile_free_energy" || code === "saj_profile_self_consumption") {
    return getNotificationMatrixSajProfileProgress(code, windowItem, currentWindowId, midday);
  }
  if (
    code === "tesla_free_energy_control"
    || code === "tesla_after_free_shoulder_control"
    || code === "tesla_after_free_peak_control"
    || code === "tesla_overnight_shoulder_control"
  ) {
    const source = code === "tesla_overnight_shoulder_control" ? sajWatch : midday;
    return getNotificationMatrixTeslaOperationProgress(code, windowItem, currentWindowId, source, metrics, live);
  }
  if (code === "pool_pump_overnight_control") {
    const source = combinedSystems.last_pool_pump_overnight_check || {};
    return getNotificationMatrixPoolPumpOperationProgress(code, windowItem, currentWindowId, source);
  }
  if (code === "pool_pump_free_energy_control") {
    const source = combinedSystems.last_pool_pump_free_energy_check || {};
    return getNotificationMatrixPoolPumpOperationProgress(code, windowItem, currentWindowId, source);
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
  return {
    ratio: 0,
    statusText: t("notificationMatrixProgressNoData"),
    currentText: t("notificationMatrixProgressCurrent", { value: "-" }),
    triggerText: "-",
    deltaText: "",
  };
}

function getNotificationMatrixSolplanetSoc(metrics, live, source = null) {
  const candidates = [
    metrics?.battery2_soc_percent,
    live?.solplanet_battery_soc_percent,
    source?.battery2_soc_percent,
    source?.overnight_shoulder_tesla?.solplanet_soc_percent,
    source?.after_free_shoulder?.solplanet_soc_percent,
    source?.after_free_peak?.solplanet_soc_percent,
    source?.export_window?.solplanet_soc_percent,
  ];
  for (const value of candidates) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return Number.NaN;
}

function getNotificationMatrixSajProfileProgress(code, windowItem, currentWindowId, midday) {
  const guard = midday?.saj_profile_guard || {};
  const expectedProfile = "self_consumption";
  const actualProfile = String(guard?.actual_profile || guard?.input_profile || guard?.selected_profile || "").trim();
  const selectedProfile = String(guard?.selected_profile || "").trim();
  const isCurrentWindow = currentWindowId === windowItem?.id;
  const profileMatched = actualProfile === expectedProfile || selectedProfile === expectedProfile;
  const syncCleared = !Boolean(guard?.pending_remote_sync);
  let statusText = t("notificationMatrixProgressScheduled");
  if (isCurrentWindow) {
    if (Boolean(guard?.pending_remote_sync)) {
      statusText = t("notificationMatrixProgressPendingSync");
    } else if (String(guard?.action || "").trim() === "apply_profile") {
      statusText = t("notificationMatrixProgressApplied");
    } else if (profileMatched) {
      statusText = t("notificationMatrixProgressHolding");
    }
  }
  return {
    ratio: profileMatched ? 1 : 0,
    statusText,
    currentText: t("notificationMatrixProgressCurrent", { value: formatSajProfileLabel(actualProfile || selectedProfile || "-") }),
    triggerText: t("notificationMatrixProgressTarget", { value: formatSajProfileLabel(expectedProfile) }),
    deltaText: String(guard?.desired_reason || "").trim(),
    conditions: [
      {
        label: t("notificationMatrixConditionCurrentWindow"),
        value: isCurrentWindow ? t("notificationMatrixCurrentWindowBadge") : windowItem?.schedule || "-",
        matched: isCurrentWindow,
      },
      {
        label: t("notificationMatrixConditionSajProfileApplied"),
        value: `${formatSajProfileLabel(actualProfile || selectedProfile || "-")} -> ${formatSajProfileLabel(expectedProfile)}`,
        matched: profileMatched,
      },
      {
        label: t("notificationMatrixConditionSajSyncCleared"),
        value: Boolean(guard?.pending_remote_sync) ? t("notificationMatrixProgressPendingSync") : t("notificationMatrixProgressHolding"),
        matched: syncCleared,
      },
    ],
    hideBar: true,
  };
}

function getNotificationMatrixTeslaOperationProgress(code, windowItem, currentWindowId, source, metrics, live) {
  const isCurrentWindow = currentWindowId === windowItem?.id;
  const action = source?.action || {};
  const decision = source?.decision || {};
  const before = source?.tesla_state_before || {};
  const actualCurrent = Number(metrics?.tesla_charge_current_amps);
  const configuredCurrent = Number(metrics?.tesla_configured_current_amps);
  const solplanetSoc = getNotificationMatrixSolplanetSoc(metrics, live, source);
  const rawConnectionState = String(metrics?.tesla_connection_state || before?.connection_state || "").trim().toLowerCase();
  const connectionState = formatTeslaConnectionText(rawConnectionState);
  const rawStatusText = String(before?.status_text || "").trim().toLowerCase();
  const connected = Boolean(rawConnectionState) && rawConnectionState !== "unplugged";
  const ready = rawStatusText ? !["disconnected", "unknown", "unavailable"].includes(rawStatusText) : connected;
  let statusText = t("notificationMatrixProgressScheduled");
  if (isCurrentWindow) {
    const actionType = String(action?.type || "").trim().toLowerCase();
    if (actionType && actionType !== "noop") {
      statusText = t("notificationMatrixProgressApplied");
    } else if (String(decision?.mode || "").trim() === "hold_current_state") {
      statusText = t("notificationMatrixProgressHolding");
    } else {
      statusText = t("notificationMatrixProgressWatching");
    }
  }
  let triggerText = "-";
  let deltaText = connectionState;
  let conditions = [
    {
      label: t("notificationMatrixConditionCurrentWindow"),
      value: isCurrentWindow ? t("notificationMatrixCurrentWindowBadge") : windowItem?.schedule || "-",
      matched: isCurrentWindow,
    },
    {
      label: t("notificationMatrixConditionTeslaConnected"),
      value: connectionState,
      matched: connected,
    },
    {
      label: t("notificationMatrixConditionTeslaReady"),
      value: rawStatusText || connectionState,
      matched: ready,
    },
  ];
  if (code === "tesla_free_energy_control") {
    triggerText = t("notificationMatrixTeslaTriggerGridCap");
    const predictedGrid = Number(decision?.predicted_grid_w);
    conditions.push({
      label: t("notificationMatrixConditionTeslaGridCap"),
      value: Number.isFinite(predictedGrid) ? `${formatTrimmedDecimal(predictedGrid, 0)}W` : "-",
      matched: Number.isFinite(predictedGrid) && predictedGrid <= 15000,
    });
  } else if (code === "tesla_overnight_shoulder_control") {
    const budget = source?.overnight_shoulder_tesla?.energy_budget;
    const availableKwh = Number(budget?.available_for_tesla_kwh);
    const batteryKwh = Number(budget?.usable_battery_total_kwh);
    const homeLoadKwh = Number(
      budget?.projected_home_load_to_target_kwh ?? budget?.projected_home_load_to_8am_kwh
    );
    const homeHours = Number(
      budget?.home_load_hours_to_target ?? budget?.home_load_hours_to_8am
    );
    const pumpKwh = Number(budget?.pool_pump_remaining_kwh);
    const pumpHours = Number(budget?.pool_pump_remaining_hours);
    const pumpPowerW = Number(budget?.pool_pump_power_w);
    const minKwh = Number(budget?.min_available_kwh ?? 1.0);
    const fmtKwh = (v) => Number.isFinite(v) ? `${formatTrimmedDecimal(v, 1)}kWh` : "-";
    const fmtH = (v) => Number.isFinite(v) ? `${formatTrimmedDecimal(v, 1)}h` : "-";
    const fmtW = (v) => Number.isFinite(v) ? `${formatTrimmedDecimal(v / 1000, 1)}kW` : "-";
    const chargingEnabled = source?.tesla_state_before?.enabled === true;
    const chargeStateText = chargingEnabled ? t("notificationMatrixConditionMatched") : t("notificationMatrixConditionNotMatched");
    conditions.push({
      label: t("notificationMatrixConditionTeslaOvernightChargeState"),
      value: chargeStateText,
      matched: chargingEnabled,
    });
    const lastActionAt = source?.tesla_overnight_last_action_at_utc;
    const lastActionType = String(source?.tesla_overnight_last_action_type || "").trim();
    conditions.push({
      label: t("notificationMatrixConditionTeslaOvernightLastAction"),
      value: lastActionAt ? `${lastActionType} · ${formatRelativeAgo(lastActionAt)}` : "-",
      matched: !!lastActionAt,
    });
    if (budget != null) {
      const homeAvgKw = (Number.isFinite(homeLoadKwh) && Number.isFinite(homeHours) && homeHours > 0)
        ? homeLoadKwh / homeHours
        : NaN;
      triggerText = t("notificationMatrixTeslaTriggerOvernightBudget", {
        battery: fmtKwh(batteryKwh),
        home: fmtKwh(homeLoadKwh),
        homeKw: fmtW(homeAvgKw * 1000),
        homeH: fmtH(homeHours),
        pump: fmtKwh(pumpKwh),
        pumpKw: fmtW(pumpPowerW),
        pumpH: fmtH(pumpHours),
        available: fmtKwh(availableKwh),
      });
      conditions.push({
        label: t("notificationMatrixConditionTeslaOvernightBudget"),
        value: t("notificationMatrixConditionValueCurrentNeed", {
          current: fmtKwh(availableKwh),
          need: `> ${fmtKwh(minKwh)}`,
        }),
        matched: Number.isFinite(availableKwh) && availableKwh > minKwh,
      });
    } else {
      const socText = Number.isFinite(solplanetSoc) ? `${formatTrimmedDecimal(solplanetSoc, 1)}%` : "-";
      triggerText = t("notificationMatrixTeslaTriggerSolplanetReserve", { soc: socText });
      conditions.push({
        label: t("notificationMatrixConditionTeslaOvernightReserve"),
        value: t("notificationMatrixConditionValueCurrentNeed", { current: socText, need: "> 10%" }),
        matched: Number.isFinite(solplanetSoc) && solplanetSoc > 10,
      });
    }
  } else if (code === "tesla_after_free_shoulder_control" || code === "tesla_after_free_peak_control") {
    const socText = Number.isFinite(solplanetSoc) ? `${formatTrimmedDecimal(solplanetSoc, 1)}%` : "-";
    triggerText = t("notificationMatrixTeslaTriggerSolplanetSoc", { soc: socText });
    conditions.push({
      label: t("notificationMatrixConditionTeslaSocStart"),
      value: t("notificationMatrixConditionValueCurrentNeed", { current: socText, need: ">= 95%" }),
      matched: Number.isFinite(solplanetSoc) && solplanetSoc >= 95,
    });
    conditions.push({
      label: t("notificationMatrixConditionTeslaSocKeep"),
      value: t("notificationMatrixConditionValueCurrentNeed", { current: socText, need: ">= 90%" }),
      matched: Number.isFinite(solplanetSoc) && solplanetSoc >= 90,
    });
  }
  if (String(source?.decision_reason || "").trim() && isCurrentWindow) {
    deltaText = String(source.decision_reason).trim();
  }
  return {
    ratio: Number.isFinite(actualCurrent) ? Math.max(0, Math.min(1, actualCurrent / 32)) : 0,
    statusText,
    currentText: formatTeslaCurrentValue(actualCurrent, configuredCurrent, "A", {
      connectionState: metrics?.tesla_connection_state || before?.connection_state || null,
      chargingPowerValue: metrics?.tesla_charge_power_w,
    }),
    triggerText,
    deltaText,
    conditions,
    hideBar: true,
  };
}

function getNotificationMatrixPoolPumpOperationProgress(code, windowItem, currentWindowId, source) {
  const isCurrentWindow = currentWindowId === windowItem?.id;
  const action = source?.action || {};
  const poolPumpState = source?.pool_pump_state_before || {};
  const pumpEnabled = poolPumpState?.control?.enabled === true;
  let statusText = t("notificationMatrixProgressScheduled");
  if (isCurrentWindow) {
    const actionType = String(action?.type || "").trim().toLowerCase();
    if (actionType && actionType !== "noop") statusText = t("notificationMatrixProgressApplied");
    else if (pumpEnabled) statusText = t("notificationMatrixProgressHolding");
    else statusText = t("notificationMatrixProgressWatching");
  }
  if (code === "pool_pump_free_energy_control") {
    const predictedGrid = Number(source?.predicted_grid_with_pump_w);
    const pumpPower = Number(source?.pool_pump_power_w);
    const teslaPriorityReady = source?.tesla_priority_ready_for_pool_pump === true;
    const gridCapMatched = Number.isFinite(predictedGrid) && predictedGrid <= 15000;
    return {
      ratio: Number.isFinite(predictedGrid) ? Math.max(0, Math.min(1, predictedGrid / 15000)) : 0,
      statusText,
      currentText: t("notificationMatrixProgressCurrent", {
        value: Number.isFinite(pumpPower) ? `${formatTrimmedDecimal(pumpPower, 0)}W` : "-",
      }),
      triggerText: t("notificationMatrixProgressTarget", { value: "<= 15.0kW" }),
      deltaText: String(source?.decision_reason || "").trim() || (pumpEnabled ? "running" : "idle"),
      conditions: [
        {
          label: t("notificationMatrixConditionCurrentWindow"),
          value: isCurrentWindow ? t("notificationMatrixCurrentWindowBadge") : windowItem?.schedule || "-",
          matched: isCurrentWindow,
        },
        {
          label: t("notificationMatrixConditionPoolPumpTeslaPriority"),
          value: String(source?.tesla_priority_reason || "-"),
          matched: teslaPriorityReady,
        },
        {
          label: t("notificationMatrixConditionPoolPumpGridCap"),
          value: Number.isFinite(predictedGrid) ? `${formatTrimmedDecimal(predictedGrid, 0)}W` : "-",
          matched: gridCapMatched,
        },
      ],
      hideBar: true,
    };
  }
  const runtimeBudget = source?.runtime_budget || {};
  const availableHours = Number(runtimeBudget?.available_runtime_hours);
  const targetHours = Number(runtimeBudget?.target_runtime_hours);
  const remainingHours = Number(runtimeBudget?.remaining_runtime_hours);
  const runtimeBudgetFormula = formatRuntimeBudgetFormula(runtimeBudget);
  const ratio = Number.isFinite(targetHours) && targetHours > 0 && Number.isFinite(remainingHours)
    ? Math.max(0, Math.min(1, 1 - (remainingHours / targetHours)))
    : 0;
  return {
    ratio,
    statusText,
    currentText: t("notificationMatrixProgressCurrent", {
      value: Number.isFinite(remainingHours) ? `${formatTrimmedDecimal(remainingHours, 2)}h` : "-",
    }),
    triggerText: t("notificationMatrixProgressTarget", {
      value: Number.isFinite(targetHours) ? `${formatTrimmedDecimal(targetHours, 2)}h` : "-",
    }),
    deltaText: String(source?.decision_reason || "").trim() || (pumpEnabled ? "running" : "idle"),
    conditions: [
      {
        label: t("notificationMatrixConditionCurrentWindow"),
        value: isCurrentWindow ? t("notificationMatrixCurrentWindowBadge") : windowItem?.schedule || "-",
        matched: isCurrentWindow,
      },
      {
        label: t("notificationMatrixConditionPoolPumpSchedule"),
        value: String(source?.pump_schedule || "23:00-05:00"),
        matched: Boolean(source?.pump_schedule_active),
      },
      {
        label: t("notificationMatrixConditionPoolPumpRuntimeBudget"),
        value: runtimeBudgetFormula || (Number.isFinite(availableHours) ? `${formatTrimmedDecimal(availableHours, 2)}h` : "-"),
        matched: Number.isFinite(availableHours) && availableHours > 0,
      },
    ],
  };
}

function formatTeslaAmpText(value) {
  if (!Number.isFinite(Number(value))) return "-";
  return `${formatTrimmedDecimal(Number(value), 0)}A`;
}

function formatTeslaConnectionText(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "charging") return t("teslaConnectionStateCharging");
  if (normalized === "plugged_not_charging") return t("teslaConnectionStatePluggedNotCharging");
  if (normalized === "unplugged") return t("teslaConnectionStateUnplugged");
  return t("teslaConnectionStateUnknown");
}

function formatSajProfileLabel(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "time_of_use") return t("sajProfileOptionTimeOfUse");
  if (normalized === "self_consumption") return t("sajProfileOptionSelfConsumption");
  if (normalized === "microgrid") return t("sajProfileOptionMicrogrid");
  return normalized || "-";
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

async function updateTimeWindowRuleState(ruleCode, enabled) {
  const normalized = String(ruleCode || "").trim();
  if (!normalized || !isTimeWindowRuleEditable(normalized) || timeWindowRuleBusy.has(normalized)) return;
  timeWindowRuleBusy.add(normalized);
  renderNotificationMatrix();
  try {
    const payload = await fetchJson(`/api/time-window-rules/${encodeURIComponent(normalized)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: Boolean(enabled) }),
      timeoutMs: 10000,
    });
    const current = stateCache.lastTimeWindowRules || { items: [], updated_at: null };
    const items = Array.isArray(current.items) ? [...current.items] : [];
    const index = items.findIndex((item) => String(item?.rule_code || "").trim() === normalized);
    const nextItem = {
      ...(index >= 0 ? items[index] : { rule_code: normalized }),
      ...payload,
      enabled: Boolean(payload?.enabled),
    };
    if (index >= 0) items[index] = nextItem;
    else items.push(nextItem);
    stateCache.lastTimeWindowRules = {
      ...current,
      updated_at: payload?.updated_at_utc || new SiteDate().toISOString(),
      items,
    };
  } catch (err) {
    window.alert(String(err));
  } finally {
    timeWindowRuleBusy.delete(normalized);
    renderNotificationMatrix();
  }
}

async function updateTimeWindowPriorityOverride(target) {
  const normalized = String(target || "auto").trim().toLowerCase() || "auto";
  if (timeWindowPriorityOverrideBusy) return;
  timeWindowPriorityOverrideBusy = true;
  renderNotificationMatrix();
  try {
    await fetchJson("/api/time-window-priority-override", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target: normalized }),
      timeoutMs: 10000,
    });
    const refreshed = await fetchJson("/api/time-window-rules", { timeoutMs: 10000 });
    stateCache.lastTimeWindowRules = refreshed;
    renderNotificationMatrix();
  } catch (err) {
    window.alert(String(err));
  } finally {
    timeWindowPriorityOverrideBusy = false;
    renderNotificationMatrix();
  }
}

async function updateTeslaAutoControl(enabled) {
  if (teslaAutoControlBusy) return;
  teslaAutoControlBusy = true;
  renderNotificationMatrix();
  try {
    await fetchJson("/api/tesla/auto-control", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: Boolean(enabled) }),
      timeoutMs: 10000,
    });
    const refreshed = await fetchJson("/api/time-window-rules", { timeoutMs: 10000 });
    stateCache.lastTimeWindowRules = refreshed;
    renderNotificationMatrix();
  } catch (err) {
    window.alert(String(err));
  } finally {
    teslaAutoControlBusy = false;
    renderNotificationMatrix();
  }
}

async function updateTeslaForceMaxChargeThreshold(thresholdPercent) {
  const normalized = Number(thresholdPercent);
  if (teslaForceMaxChargeThresholdBusy || !Number.isFinite(normalized) || !Number.isInteger(normalized)) return;
  teslaForceMaxChargeThresholdBusy = true;
  renderNotificationMatrix();
  try {
    await fetchJson("/api/tesla/force-max-charge-threshold", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ threshold_percent: normalized }),
      timeoutMs: 10000,
    });
    const refreshed = await fetchJson("/api/time-window-rules", { timeoutMs: 10000 });
    stateCache.lastTimeWindowRules = refreshed;
    renderNotificationMatrix();
  } catch (err) {
    window.alert(String(err));
  } finally {
    teslaForceMaxChargeThresholdBusy = false;
    renderNotificationMatrix();
  }
}

async function updatePoolPumpAutoControl(enabled) {
  if (poolPumpAutoControlBusy) return;
  poolPumpAutoControlBusy = true;
  renderNotificationMatrix();
  try {
    await fetchJson("/api/devices/pool-pump/auto-control", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: Boolean(enabled) }),
      timeoutMs: 10000,
    });
    const refreshed = await fetchJson("/api/time-window-rules", { timeoutMs: 10000 });
    stateCache.lastTimeWindowRules = refreshed;
    renderNotificationMatrix();
  } catch (err) {
    window.alert(String(err));
  } finally {
    poolPumpAutoControlBusy = false;
    renderNotificationMatrix();
  }
}

async function updatePoolPumpForceMinRuntimeHours(thresholdHours) {
  const normalized = Number(thresholdHours);
  if (poolPumpForceMinRuntimeBusy || !Number.isFinite(normalized)) return;
  poolPumpForceMinRuntimeBusy = true;
  renderNotificationMatrix();
  try {
    await fetchJson("/api/devices/pool-pump/force-min-runtime-hours", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ threshold_hours: normalized }),
      timeoutMs: 10000,
    });
    const refreshed = await fetchJson("/api/time-window-rules", { timeoutMs: 10000 });
    stateCache.lastTimeWindowRules = refreshed;
    renderNotificationMatrix();
  } catch (err) {
    window.alert(String(err));
  } finally {
    poolPumpForceMinRuntimeBusy = false;
    renderNotificationMatrix();
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
  const d = new SiteDate(parts.year, parts.monthIndex, parts.day, 0, 0, 0, 0);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toUtcIsoFromDateEndExclusive(dateText) {
  const parts = localDateParts(dateText);
  if (!parts) return null;
  const d = new SiteDate(parts.year, parts.monthIndex, parts.day + 1, 0, 0, 0, 0);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toUtcIsoFromDateTimeLocal(dateTimeText) {
  const d = new SiteDate(dateTimeText);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toLocalDateTimeInputValueFromMs(ms) {
  const d = new SiteDate(ms);
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
    const d = new SiteDate(value);
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
  if (currentTab !== "sampling") return;
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

function stageSamplingBrushRange(areas) {
  if (!samplingBrushState.active) return;
  samplingBrushState.pendingRange = extractBrushTimeRange(areas);
}

function clearSamplingBrushState({ clearChart = false } = {}) {
  samplingBrushState.active = false;
  samplingBrushState.pendingRange = null;
  if (samplingChart) {
    samplingChart.dispatchAction({
      type: "takeGlobalCursor",
      key: "brush",
      brushOption: {
        brushType: false,
      },
    });
  }
  if (clearChart && samplingChart) {
    samplingChart.dispatchAction({ type: "brush", areas: [] });
  }
}

function bindSamplingBrushGlobalHandlers() {
  if (samplingBrushGlobalHandlersBound) return;
  const finishBrushSelection = () => {
    if (!samplingBrushState.active) return;
    const range = samplingBrushState.pendingRange;
    clearSamplingBrushState({ clearChart: true });
    if (!range) return;
    void applyChartSelectionRange(range.startMs, range.endMs);
  };
  window.addEventListener("pointerup", finishBrushSelection);
  window.addEventListener("mouseup", finishBrushSelection);
  window.addEventListener("pointercancel", () => clearSamplingBrushState({ clearChart: true }));
  window.addEventListener("blur", () => clearSamplingBrushState({ clearChart: true }));
  samplingBrushGlobalHandlersBound = true;
}

function getLocalDateText(offsetDays = 0) {
  const d = new SiteDate();
  d.setDate(d.getDate() + offsetDays);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function getWeekInfo(anchorDateText) {
  const anchorParts = localDateParts(anchorDateText);
  const now = new SiteDate();
  const anchor = anchorParts
    ? new SiteDate(anchorParts.year, anchorParts.monthIndex, anchorParts.day, 0, 0, 0, 0)
    : new SiteDate(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  const day = anchor.getDay();
  const diffToMonday = day === 0 ? -6 : 1 - day;
  const monday = new SiteDate(anchor);
  monday.setDate(anchor.getDate() + diffToMonday);
  const endExclusive = new SiteDate(monday);
  endExclusive.setDate(monday.getDate() + 7);
  const sunday = new SiteDate(monday);
  sunday.setDate(monday.getDate() + 6);

  const thursday = new SiteDate(monday);
  thursday.setDate(monday.getDate() + 3);
  const year = thursday.getFullYear();
  const jan4 = new SiteDate(year, 0, 4, 0, 0, 0, 0);
  const jan4Day = jan4.getDay() || 7;
  const firstThursday = new SiteDate(year, 0, 4 + (4 - jan4Day), 0, 0, 0, 0);
  const week = 1 + Math.round((thursday.getTime() - firstThursday.getTime()) / (7 * 86400000));

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

function getDayInfo(dayText) {
  const fallback = getLocalDateText();
  const parts = localDateParts(dayText || fallback);
  const target = parts
    ? new SiteDate(parts.year, parts.monthIndex, parts.day, 0, 0, 0, 0)
    : new SiteDate();
  const yyyy = target.getFullYear();
  const mm = String(target.getMonth() + 1).padStart(2, "0");
  const dd = String(target.getDate()).padStart(2, "0");
  const normalized = `${yyyy}-${mm}-${dd}`;
  const endUtc = normalized === getLocalDateText() ? new SiteDate().toISOString() : toUtcIsoFromDateEndExclusive(normalized);
  return {
    day: normalized,
    startUtc: toUtcIsoFromDateOnly(normalized),
    endUtc,
    label: normalized,
  };
}

function getRecentPumpStatusRange(anchorRange = null) {
  const anchorEndMs = new SiteDate(anchorRange?.endUtc || "").getTime();
  const end = Number.isFinite(anchorEndMs) ? new SiteDate(anchorEndMs) : new SiteDate();
  const start = new SiteDate(end.getTime() - 48 * 3600000);
  return {
    startUtc: start.toISOString(),
    endUtc: end.toISOString(),
    label: t("samplingPumpStatusRecent48h"),
  };
}

function getMonthInfo(monthText, yearText) {
  const now = new SiteDate();
  const year = Number(yearText || now.getFullYear());
  const month = Math.max(1, Math.min(12, Number(monthText || `${now.getMonth() + 1}`) || 1));
  const start = new SiteDate(year, month - 1, 1, 0, 0, 0, 0);
  const end = new SiteDate(year, month, 1, 0, 0, 0, 0);
  return {
    year,
    month,
    startUtc: start.toISOString(),
    endUtc: end.toISOString(),
    label: `${year}-${String(month).padStart(2, "0")}`,
    display: t("samplingMonthDisplay", { year, month: monthLabel(month) }),
  };
}

function localDateTextFromDate(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function dateFromLocalDateText(dayText) {
  const parts = localDateParts(dayText);
  if (!parts) return null;
  return new SiteDate(parts.year, parts.monthIndex, parts.day, 0, 0, 0, 0);
}

function getReportMonthInfo() {
  return getMonthInfo(reportCalendarState.month, reportCalendarState.monthYear);
}

function getReportMonthKey() {
  return getReportMonthInfo().label;
}

function setReportCalendarMonthFromDay(dayText) {
  const parts = localDateParts(dayText);
  if (!parts) return;
  reportCalendarState.month = parts.monthIndex + 1;
  reportCalendarState.monthYear = parts.year;
}

function isReportGeneratableDay(dayText) {
  return Boolean(localDateParts(dayText)) && String(dayText) < getLocalDateText();
}

function getReportMonthCompletedDayCount(info = getReportMonthInfo()) {
  const today = getLocalDateText();
  const daysInMonth = new SiteDate(info.year, info.month, 0).getDate();
  let count = 0;
  for (let day = 1; day <= daysInMonth; day += 1) {
    const dayText = `${info.year}-${String(info.month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    if (dayText < today) count += 1;
  }
  return count;
}

function getReportBillActualMeta(report) {
  const billActualCost =
    report?.bill_actual_cost && typeof report.bill_actual_cost === "object" ? report.bill_actual_cost : null;
  const hasBillActual = Boolean(
    report?.has_bill_actual_cost ||
      billActualCost?.status === "bill_allocated" ||
      Number.isFinite(Number(billActualCost?.actual_daily_cost_aud)),
  );
  return {
    hasBillActual,
    invoice: String(report?.bill_actual_invoice_number || billActualCost?.invoice_number || ""),
  };
}

function getReportCalendarPlanLabel(dayText) {
  if (!localDateParts(dayText)) return "";
  return dayText >= REPORT_CALENDAR_FOUR4FREE_START_DAY
    ? t("reportCalendarPlanFour4Free")
    : t("reportCalendarPlanZeroHeroLegacy");
}

function isReportCalendarLegacyZeroHeroDay(dayText) {
  return localDateParts(dayText) && dayText < REPORT_CALENDAR_FOUR4FREE_START_DAY;
}

function invalidateReportCalendarMonthsForBill(bill) {
  const period = bill?.billing_period && typeof bill.billing_period === "object" ? bill.billing_period : {};
  const startDate = dateFromLocalDateText(period.start);
  const endDate = dateFromLocalDateText(period.end);
  if (!startDate || !endDate || startDate > endDate) return;

  const invoice = String(bill?.invoice_number || "");
  const cursor = new SiteDate(startDate.getTime());
  let guard = 0;
  while (cursor <= endDate && guard < 370) {
    const dayText = localDateTextFromDate(cursor);
    const monthKey = dayText.slice(0, 7);
    reportCalendarState.loadedMonths.delete(monthKey);
    const existing = reportCalendarState.availableDays.get(dayText);
    if (existing) {
      reportCalendarState.availableDays.set(dayText, {
        ...existing,
        has_bill_actual_cost: true,
        bill_actual_invoice_number: invoice,
      });
    }
    cursor.setDate(cursor.getDate() + 1);
    guard += 1;
  }
}

function getMonthlyReportMonthInfo() {
  return getMonthInfo(monthlyReportState.month, monthlyReportState.monthYear);
}

function setMonthlyReportMonthFromKey(monthKey) {
  const match = /^(\d{4})-(\d{2})$/.exec(String(monthKey || ""));
  if (!match) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isInteger(year) || month < 1 || month > 12) return false;
  monthlyReportState.monthYear = year;
  monthlyReportState.month = month;
  monthlyReportState.pickerYear = year;
  return true;
}

function shiftMonthlyReportPickerYear(offset) {
  monthlyReportState.pickerYear += offset;
}

function localizedReportText(value) {
  if (value && typeof value === "object") return String(value[currentLang] || value.en || value.zh || "");
  return String(value ?? "");
}

function monthlyUsageEnergy(payload, key) {
  return Number(payload?.summary?.usage?.energy_kwh?.[key] || 0);
}

function monthlyUsageBreakdown(payload, group, key) {
  return Number(payload?.summary?.usage?.breakdown_kwh?.[group]?.[key] || 0);
}

function monthlyProjectedEnergy(payload, key) {
  return Number(payload?.projections?.month_end_estimate?.energy_kwh?.[key] || 0);
}

function monthlyComparisonDelta(payload, comparisonKey, energyKey) {
  return payload?.comparisons?.[comparisonKey]?.delta?.energy_kwh?.[energyKey] || {};
}

function formatMonthlyPercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${formatTrimmedDecimal(n * 100, 1)}%`;
}

function formatMonthlySignedKwh(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  const sign = n > 0 ? "+" : "";
  return `${sign}${formatTrimmedDecimal(n, 2)} kWh`;
}

function formatMonthlySignedPercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  const sign = n > 0 ? "+" : "";
  return `${sign}${formatTrimmedDecimal(n, 1)}%`;
}

function formatMonthlyDelta(delta) {
  const amount = formatMonthlySignedKwh(delta?.delta);
  const percent = delta?.percent_delta === null || delta?.percent_delta === undefined ? "" : ` (${formatMonthlySignedPercent(delta.percent_delta)})`;
  return `${amount}${percent}`;
}

function renderMonthlyMetricList(id, items) {
  const html = (items || [])
    .map(
      (item) => `
        <div class="monthly-report-metric">
          <span class="monthly-report-metric-label">${escapeHtml(item.label)}</span>
          <span class="monthly-report-metric-value">${escapeHtml(item.value)}</span>
        </div>
      `,
    )
    .join("");
  setHtml(id, html || `<div class="monthly-report-empty">${escapeHtml(t("monthlyReportNoItems"))}</div>`);
}

function renderMonthlyReportLoading() {
  setMonthlyReportStatus(monthlyReportState.statusText || t("monthlyReportLoading"));
  setText("monthlyReportUpdatedAt", "-");
  setText("monthlyReportSubtitle", "-");
  setHtml("monthlyReportKpiGrid", `<div class="monthly-report-empty">${escapeHtml(t("monthlyReportLoading"))}</div>`);
  renderMonthlyMetricList("monthlyReportProjectionBody", []);
  renderMonthlyMetricList("monthlyReportComparisonBody", []);
  renderMonthlyMetricList("monthlyReportHeroBody", []);
  setText("monthlyReportRecommendationCount", "-");
  setHtml("monthlyReportRecommendations", `<div class="monthly-report-empty">${escapeHtml(t("monthlyReportLoading"))}</div>`);
  renderMonthlyDailyTrend(null);
  setHtml("monthlyReportAnomalies", "");
}

function renderMonthlyReportEmpty(message = t("monthlyReportUnavailable")) {
  setMonthlyReportStatus(message);
  setText("monthlyReportUpdatedAt", "-");
  setText("monthlyReportSubtitle", "-");
  setHtml("monthlyReportKpiGrid", `<div class="monthly-report-empty">${escapeHtml(message)}</div>`);
  renderMonthlyMetricList("monthlyReportProjectionBody", []);
  renderMonthlyMetricList("monthlyReportComparisonBody", []);
  renderMonthlyMetricList("monthlyReportHeroBody", []);
  setText("monthlyReportRecommendationCount", t("monthlyReportRecommendationsCount", { count: 0 }));
  setHtml("monthlyReportRecommendations", `<div class="monthly-report-empty">${escapeHtml(message)}</div>`);
  renderMonthlyDailyTrend(null);
  setHtml("monthlyReportAnomalies", `<div class="monthly-report-empty">${escapeHtml(message)}</div>`);
}

function renderMonthlyKpis(payload) {
  const netGrid = Number(payload?.summary?.net_grid_kwh || 0);
  const items = [
    { label: t("monthlyReportKpiLoad"), value: formatEnergyKwhText(monthlyUsageEnergy(payload, "home_load")), sub: t("monthlyReportLegend") },
    { label: t("monthlyReportKpiSolar"), value: formatEnergyKwhText(monthlyUsageEnergy(payload, "solar_generation")), sub: `${t("monthlyReportKpiSelfUse")}: ${formatMonthlyPercent(payload?.summary?.self_consumption_ratio)}` },
    { label: t("monthlyReportKpiImport"), value: formatEnergyKwhText(monthlyUsageEnergy(payload, "grid_import")), sub: `${t("samplingTotalsWindowFree")} ${formatEnergyKwhText(monthlyUsageBreakdown(payload, "grid_import", "window"))}` },
    { label: t("monthlyReportKpiExport"), value: formatEnergyKwhText(monthlyUsageEnergy(payload, "grid_export")), sub: `${t("samplingTotalsWindowOther")} ${formatEnergyKwhText(monthlyUsageBreakdown(payload, "grid_export", "other"))}` },
    { label: t("monthlyReportKpiNetGrid"), value: formatMonthlySignedKwh(netGrid), sub: netGrid > 0 ? t("monthlyReportKpiImport") : t("monthlyReportKpiExport") },
    { label: t("monthlyReportKpiGridDependence"), value: formatMonthlyPercent(payload?.summary?.grid_dependence_ratio), sub: `${t("monthlyReportKpiCoverage")}: ${formatMonthlyPercent(payload?.summary?.usage?.coverage_ratio)}` },
    { label: "Tesla", value: formatEnergyKwhText(monthlyUsageBreakdown(payload, "home_load", "tesla")), sub: `${t("samplingTotalsScopeHomeLoad")}` },
    { label: t("samplingPumpStatusTitle"), value: formatEnergyKwhText(monthlyUsageBreakdown(payload, "home_load", "pool_pump")), sub: `${t("samplingTotalsScopeHomeLoad")}` },
  ];
  setHtml(
    "monthlyReportKpiGrid",
    items
      .map(
        (item) => `
          <article class="monthly-report-kpi">
            <p class="monthly-report-kpi-label">${escapeHtml(item.label)}</p>
            <p class="monthly-report-kpi-value">${escapeHtml(item.value)}</p>
            <p class="monthly-report-kpi-sub">${escapeHtml(item.sub || "")}</p>
          </article>
        `,
      )
      .join(""),
  );
}

function renderMonthlyReportSections(payload) {
  renderMonthlyMetricList("monthlyReportProjectionBody", [
    { label: t("monthlyReportMetricProjectedLoad"), value: formatEnergyKwhText(monthlyProjectedEnergy(payload, "home_load")) },
    { label: t("monthlyReportMetricProjectedImport"), value: formatEnergyKwhText(monthlyProjectedEnergy(payload, "grid_import")) },
    { label: t("monthlyReportMetricProjectedExport"), value: formatEnergyKwhText(monthlyProjectedEnergy(payload, "grid_export")) },
    { label: t("monthlyReportMetricScale"), value: `${formatTrimmedDecimal(Number(payload?.projections?.scale_factor || 0), 2)}x` },
  ]);
  renderMonthlyMetricList("monthlyReportComparisonBody", [
    { label: t("monthlyReportMetricPrevLoadDelta"), value: formatMonthlyDelta(monthlyComparisonDelta(payload, "previous_period", "home_load")) },
    { label: t("monthlyReportMetricPrevImportDelta"), value: formatMonthlyDelta(monthlyComparisonDelta(payload, "previous_period", "grid_import")) },
    { label: t("monthlyReportMetricPrevSolarDelta"), value: formatMonthlyDelta(monthlyComparisonDelta(payload, "previous_period", "solar_generation")) },
    { label: t("monthlyReportMetricAvgImportDelta"), value: formatMonthlyDelta(monthlyComparisonDelta(payload, "trailing_4_week_average", "grid_import")) },
  ]);
  const hero = payload?.zero_hero?.totals || {};
  renderMonthlyMetricList("monthlyReportHeroBody", [
    { label: t("monthlyReportMetricHeroImportWindows"), value: String(Number(hero.import_observed_windows || 0)) },
    { label: t("monthlyReportMetricHeroNetImportWindows"), value: String(Number(hero.import_windows || 0)) },
    { label: t("monthlyReportMetricHeroImportEnergy"), value: formatEnergyKwhText(Number(hero.import_wh || 0) / 1000) },
    { label: t("monthlyReportMetricHeroNoData"), value: String(Number(hero.no_data_windows || 0)) },
  ]);
}

function renderMonthlyRecommendations(payload) {
  const recommendations = Array.isArray(payload?.recommendations) ? payload.recommendations : [];
  setText("monthlyReportRecommendationCount", t("monthlyReportRecommendationsCount", { count: recommendations.length }));
  if (!recommendations.length) {
    setHtml("monthlyReportRecommendations", `<div class="monthly-report-empty">${escapeHtml(t("monthlyReportNoItems"))}</div>`);
    return;
  }
  setHtml(
    "monthlyReportRecommendations",
    recommendations
      .map((rec) => {
        const severity = String(rec?.severity || "info");
        const severityKey = `monthlyReportSeverity${severity.charAt(0).toUpperCase()}${severity.slice(1)}`;
        const evidence = Array.isArray(rec?.evidence) ? rec.evidence : [];
        return `
          <article class="monthly-report-rec severity-${escapeHtml(severity)}">
            <div class="monthly-report-rec-head">
              <h4 class="monthly-report-rec-title">${escapeHtml(localizedReportText(rec?.title))}</h4>
              <span class="monthly-report-rec-badge">${escapeHtml(t(severityKey))}</span>
            </div>
            <p class="monthly-report-rec-text">${escapeHtml(localizedReportText(rec?.finding))}</p>
            <p class="monthly-report-rec-text">${escapeHtml(localizedReportText(rec?.action))}</p>
            <p class="monthly-report-rec-impact">${escapeHtml(localizedReportText(rec?.impact))}</p>
            <p class="monthly-report-rec-text">${escapeHtml(t("monthlyReportConfidence", { value: rec?.confidence || "-" }))}</p>
            <div class="monthly-report-rec-evidence">
              ${evidence
                .map(
                  (item) => `
                    <div class="monthly-report-rec-evidence-item">
                      <span class="monthly-report-rec-evidence-label">${escapeHtml(localizedReportText(item?.label))}</span>
                      <span class="monthly-report-rec-evidence-value">${escapeHtml(item?.value ?? "-")}</span>
                    </div>
                  `,
                )
                .join("")}
            </div>
          </article>
        `;
      })
      .join(""),
  );
}

function ensureMonthlyReportDailyTrendChart() {
  if (monthlyReportDailyTrendChart) return monthlyReportDailyTrendChart;
  const canvas = document.getElementById("monthlyReportDailyTrendChart");
  if (!canvas || typeof window.echarts === "undefined") return null;
  monthlyReportDailyTrendChart = window.echarts.init(canvas, null, { renderer: "canvas" });
  return monthlyReportDailyTrendChart;
}

function renderMonthlyDailyTrend(payload) {
  const chart = ensureMonthlyReportDailyTrendChart();
  if (!chart) return;
  const items = Array.isArray(payload?.daily?.items) ? payload.daily.items : [];
  const seriesMeta = [
    { key: "home_load", label: t("monthlyReportTableLoad"), color: "#5b8def" },
    { key: "solar_generation", label: t("monthlyReportTableSolar"), color: "#d9a441" },
    { key: "grid_import", label: t("monthlyReportTableImport"), color: "#d84f63" },
    { key: "grid_export", label: t("monthlyReportTableExport"), color: "#24a6a6" },
  ];
  const categories = items.map((item) => String(item?.date_local || ""));
  const series = seriesMeta.map((meta) => ({
    name: meta.label,
    type: "line",
    smooth: 0.2,
    showSymbol: true,
    symbol: "circle",
    symbolSize: 5,
    connectNulls: false,
    data: items.map((item) => {
      if (item?.has_data === false) return null;
      const value = Number(item?.energy_kwh?.[meta.key]);
      return Number.isFinite(value) ? value : null;
    }),
    lineStyle: { width: 2.4, color: meta.color, cap: "round", join: "round" },
    itemStyle: { color: meta.color, borderColor: "#fff", borderWidth: 1.5 },
    emphasis: { focus: "series" },
    animation: false,
  }));
  const hasData = series.some((entry) => entry.data.some((value) => Number.isFinite(value)));
  chart.setOption(
    {
      animation: false,
      color: seriesMeta.map((meta) => meta.color),
      grid: { left: 58, right: 20, top: 44, bottom: 52 },
      legend: {
        top: 4,
        itemWidth: 12,
        itemHeight: 8,
        textStyle: { color: "#536a5e", fontSize: 12, fontWeight: 700 },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "line", lineStyle: { type: "dashed", color: "#7d9588" } },
        formatter: (params) => {
          if (!Array.isArray(params) || !params.length) return "";
          const lines = [String(params[0].axisValueLabel || params[0].axisValue || "")];
          for (const point of params) {
            const value = Number(point?.value);
            if (!Number.isFinite(value)) continue;
            lines.push(`${point.marker}${point.seriesName}: ${formatEnergyKwhText(value)}`);
          }
          return lines.join("<br/>");
        },
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: categories,
        axisLine: { lineStyle: { color: "#adc2b5" } },
        axisTick: { show: false },
        axisLabel: {
          color: "#6b7f72",
          fontSize: 11,
          hideOverlap: true,
          formatter: (value) => formatSamplingMiniDate(value),
        },
      },
      yAxis: {
        type: "value",
        min: 0,
        name: t("monthlyReportDailyTrendYAxis"),
        nameTextStyle: { color: "#6b7f72", fontSize: 11, padding: [0, 0, 0, 4] },
        axisLine: { show: false },
        axisLabel: { color: "#6b7f72", formatter: (value) => formatTrimmedDecimal(Number(value), 1) },
        splitLine: { lineStyle: { color: "#dbe7df", type: "dashed" } },
      },
      graphic: hasData
        ? []
        : [{ type: "text", left: "center", top: "middle", style: { text: t("monthlyReportNoItems"), fill: "#6b7f72", fontSize: 14 } }],
      series,
    },
    true,
  );
}

function renderMonthlyAnomalies(payload) {
  const anomalies = payload?.anomalies || {};
  const groups = [
    {
      key: "top_import_days",
      title: t("monthlyReportAnomalyLegendImport"),
      description: t("monthlyReportAnomalyHintImport"),
      colorClass: "import",
      format: (item) => formatEnergyKwhText(item?.value),
    },
    {
      key: "top_other_load_days",
      title: t("monthlyReportAnomalyLegendBaseLoad"),
      description: t("monthlyReportAnomalyHintBaseLoad"),
      colorClass: "base-load",
      format: (item) => formatEnergyKwhText(item?.value),
    },
    {
      key: "weak_solar_days",
      title: t("monthlyReportAnomalyLegendWeakSolar"),
      description: t("monthlyReportAnomalyHintWeakSolar"),
      colorClass: "weak-solar",
      format: (item) => `${formatEnergyKwhText(item?.value)} / ${formatEnergyKwhText(item?.baseline)}`,
    },
    {
      key: "hero_issue_days",
      title: t("monthlyReportAnomalyLegendHero"),
      description: t("monthlyReportAnomalyHintHero"),
      colorClass: "hero",
      format: (item) => `${Number(item?.import_observed_windows || 0)} · ${formatEnergyKwhText(item?.import_wh)}`,
    },
  ];
  const monthMatch = /^(\d{4})-(\d{2})$/.exec(String(payload?.report_month_local || ""));
  if (!monthMatch) {
    setHtml("monthlyReportAnomalies", `<div class="monthly-report-empty">${escapeHtml(t("monthlyReportNoItems"))}</div>`);
    return;
  }

  const year = Number(monthMatch[1]);
  const month = Number(monthMatch[2]);
  const daysInMonth = Number(payload?.days_in_month) || new SiteDate(year, month, 0).getDate();
  const dailyItems = Array.isArray(payload?.daily?.items) ? payload.daily.items : [];
  const dailyByDate = new Map(dailyItems.filter((item) => item?.date_local).map((item) => [String(item.date_local), item]));
  const eventsByDate = new Map();
  for (const group of groups) {
    const items = Array.isArray(anomalies[group.key]) ? anomalies[group.key] : [];
    for (const item of items) {
      const day = String(item?.date_local || "");
      if (!localDateParts(day)) continue;
      const events = eventsByDate.get(day) || [];
      events.push({ colorClass: group.colorClass, title: group.title, value: group.format(item) });
      eventsByDate.set(day, events);
    }
  }

  const monthPrefix = `${year}-${String(month).padStart(2, "0")}-`;
  const eventDays = Array.from(eventsByDate.keys()).filter((day) => day.startsWith(monthPrefix)).sort();
  const observedDays = Array.from(dailyByDate.keys()).filter((day) => day.startsWith(monthPrefix)).sort();
  const fallbackDay = eventDays.at(-1) || observedDays.at(-1) || `${monthPrefix}01`;
  if (!monthlyReportState.selectedAnomalyDay.startsWith(monthPrefix)) {
    monthlyReportState.selectedAnomalyDay = fallbackDay;
  }
  const selectedDay = monthlyReportState.selectedAnomalyDay;
  const selectedDaily = dailyByDate.get(selectedDay);
  const selectedEvents = eventsByDate.get(selectedDay) || [];
  const selectedHasData = selectedDaily?.has_data !== false && Boolean(selectedDaily);
  const selectedEnergy = selectedDaily?.energy_kwh || {};
  const selectedHero = selectedDaily?.zero_hero_summary || {};
  const coverage = Number(selectedDaily?.coverage_ratio);
  const samples = Number(selectedDaily?.samples);
  const expectedSamples = Number(selectedDaily?.expected_samples);
  const detailMetric = (label, value) => `
    <div class="monthly-report-anomaly-detail-metric">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
  const detailEvents = selectedEvents.length
    ? selectedEvents
        .map(
          (event) => `
            <div class="monthly-report-anomaly-detail-event ${escapeHtml(event.colorClass)}">
              <span>${escapeHtml(event.title)}</span>
              <strong>${escapeHtml(event.value)}</strong>
            </div>
          `,
        )
        .join("")
    : `<p class="monthly-report-anomaly-detail-empty">${escapeHtml(t("monthlyReportAnomalyDetailNoEvents"))}</p>`;

  const weekdayKeys = [
    "reportWeekdayMon",
    "reportWeekdayTue",
    "reportWeekdayWed",
    "reportWeekdayThu",
    "reportWeekdayFri",
    "reportWeekdaySat",
    "reportWeekdaySun",
  ];
  const weekdayHtml = weekdayKeys.map((key) => `<div class="monthly-report-anomaly-weekday">${escapeHtml(t(key))}</div>`).join("");
  const leadingEmptyCells = (new SiteDate(year, month - 1, 1, 12, 0, 0, 0).getDay() + 6) % 7;
  const cells = Array.from({ length: leadingEmptyCells }, () => '<div class="monthly-report-anomaly-empty" aria-hidden="true"></div>');
  for (let day = 1; day <= daysInMonth; day += 1) {
    const dateLocal = `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    const daily = dailyByDate.get(dateLocal);
    const events = eventsByDate.get(dateLocal) || [];
    const hasData = daily?.has_data !== false && Boolean(daily);
    const details = events.map((event) => `${event.title}: ${event.value}`);
    if (!hasData && !events.length) details.push(t("monthlyReportAnomalyNoData"));
    const label = [dateLocal, ...details].join(" · ");
    const markers = events
      .map((event) => `<span class="monthly-report-anomaly-marker ${escapeHtml(event.colorClass)}" title="${escapeHtml(`${event.title}: ${event.value}`)}"></span>`)
      .join("");
    cells.push(`
      <button type="button" class="monthly-report-anomaly-day${events.length ? " has-events" : ""}${hasData ? "" : " is-no-data"}${dateLocal === selectedDay ? " is-selected" : ""}" data-monthly-anomaly-day="${dateLocal}" role="gridcell" aria-label="${escapeHtml(label)}" title="${escapeHtml(label)}">
        <span class="monthly-report-anomaly-date">${day}</span>
        ${events.length ? `<span class="monthly-report-anomaly-count">${escapeHtml(t("monthlyReportAnomalyEventCount", { count: events.length }))}</span>` : ""}
        <span class="monthly-report-anomaly-markers">${markers}</span>
      </button>
    `);
  }
  const legendHtml = groups
    .map(
      (group) => `
        <span class="monthly-report-anomaly-legend-item" tabindex="0" title="${escapeHtml(group.description)}" aria-label="${escapeHtml(`${group.title}: ${group.description}`)}">
          <i class="monthly-report-anomaly-legend-dot ${escapeHtml(group.colorClass)}"></i>
          <span>${escapeHtml(group.title)}</span>
        </span>
      `,
    )
    .join("");
  setHtml(
    "monthlyReportAnomalies",
    `
      <div class="monthly-report-anomaly-layout">
        <div class="monthly-report-anomaly-calendar-main">
          <div class="monthly-report-anomaly-legend">${legendHtml}</div>
          <div class="monthly-report-anomaly-weekdays">${weekdayHtml}</div>
          <div class="monthly-report-anomaly-days" role="grid">${cells.join("")}</div>
        </div>
        <aside class="monthly-report-anomaly-detail-card" aria-live="polite">
          <div class="monthly-report-anomaly-detail-head">
            <p>${escapeHtml(t("monthlyReportAnomalyDetailTitle"))}</p>
            <strong>${escapeHtml(selectedDay)}</strong>
          </div>
          <div class="monthly-report-anomaly-detail-metrics">
            ${detailMetric(t("monthlyReportTableLoad"), selectedHasData ? formatEnergyKwhText(selectedEnergy.home_load) : "-")}
            ${detailMetric(t("monthlyReportTableSolar"), selectedHasData ? formatEnergyKwhText(selectedEnergy.solar_generation) : "-")}
            ${detailMetric(t("monthlyReportTableImport"), selectedHasData ? formatEnergyKwhText(selectedEnergy.grid_import) : "-")}
            ${detailMetric(t("monthlyReportTableExport"), selectedHasData ? formatEnergyKwhText(selectedEnergy.grid_export) : "-")}
            ${detailMetric(t("monthlyReportAnomalyDetailCoverage"), Number.isFinite(coverage) ? formatMonthlyPercent(coverage) : "-")}
            ${detailMetric(t("monthlyReportAnomalyDetailSamples"), Number.isFinite(samples) ? `${samples}${Number.isFinite(expectedSamples) ? ` / ${expectedSamples}` : ""}` : "-")}
            ${detailMetric(t("monthlyReportAnomalyDetailHeroWindows"), String(Number(selectedHero.import_observed_windows || 0)))}
            ${detailMetric(t("monthlyReportAnomalyDetailHeroEnergy"), formatEnergyKwhText(Number(selectedHero.import_wh || 0) / 1000))}
          </div>
          <div class="monthly-report-anomaly-detail-events">${detailEvents}</div>
          <button type="button" class="btn secondary monthly-report-day-btn" data-monthly-report-day="${escapeHtml(selectedDay)}">${escapeHtml(t("monthlyReportOpenDaily"))}</button>
        </aside>
      </div>
    `,
  );
}

function getMonthlyReportStatus(report) {
  return report?.report_status === "no_data" ? "no_data" : "saved";
}

function formatMonthlyPickerMonth(year, month) {
  return new Intl.DateTimeFormat(currentLang === "zh" ? "zh-CN" : "en-AU", { month: "short" }).format(
    new SiteDate(year, month - 1, 1, 12, 0, 0, 0),
  );
}

function setMonthlyReportStatus(message, { saved = false, noData = false } = {}) {
  const element = document.getElementById("monthlyReportStatus");
  if (element) {
    element.classList.toggle("is-saved", saved && !noData);
    element.classList.toggle("is-no-data", noData);
  }
  setText("monthlyReportStatus", message);
}

function renderMonthlyReportMonthPicker() {
  const grid = document.getElementById("monthlyReportMonthGrid");
  if (!grid) return;
  const year = monthlyReportState.pickerYear;
  const currentMonthKey = getLocalDateText().slice(0, 7);
  const indexLoaded = monthlyReportState.loadedYears.has(year);
  setText("monthlyReportPickerYear", t("monthlyReportPickerTitle", { year }));
  const buttons = [];
  for (let month = 1; month <= 12; month += 1) {
    const monthKey = `${year}-${String(month).padStart(2, "0")}`;
    const entry = monthlyReportState.availableMonths.get(monthKey);
    const isCurrent = monthKey === currentMonthKey;
    const isFuture = monthKey > currentMonthKey;
    const isSelected = monthKey === getMonthlyReportMonthInfo().label;
    const status = monthlyReportState.loading && isSelected
      ? t("monthlyReportLoading")
      : isCurrent
        ? t("monthlyReportPickerLive")
        : isFuture
          ? t("monthlyReportPickerPending")
          : entry
            ? getMonthlyReportStatus(entry) === "no_data"
              ? t("monthlyReportPickerNoData")
              : t("monthlyReportPickerSaved")
            : indexLoaded
              ? t("monthlyReportPickerGenerate")
              : t("monthlyReportPickerLoading");
    const stateClass = isCurrent
      ? "is-live"
      : isFuture
        ? "is-pending"
        : entry
          ? getMonthlyReportStatus(entry) === "no_data"
            ? "is-no-data"
            : "is-saved"
          : "is-missing";
    const disabled = isFuture || monthlyReportState.loading || monthlyReportState.loadingIndex;
    const label = `${monthKey} ${status}`;
    buttons.push(`
      <button type="button" class="monthly-report-month-btn ${stateClass}${isSelected ? " is-selected" : ""}" data-monthly-report-month="${monthKey}" role="listitem" aria-label="${escapeHtml(label)}" title="${escapeHtml(label)}" ${disabled ? "disabled" : ""}>
        <span class="monthly-report-month-label">${escapeHtml(formatMonthlyPickerMonth(year, month))}</span>
        <span class="monthly-report-month-status">${escapeHtml(status)}</span>
      </button>
    `);
  }
  grid.innerHTML = buttons.join("");
}

function syncMonthlyReportControls() {
  const nowYear = Number(getLocalDateText().slice(0, 4));
  const nextBtn = document.getElementById("monthlyReportNextBtn");
  if (nextBtn) nextBtn.disabled = monthlyReportState.loading || monthlyReportState.loadingIndex || monthlyReportState.pickerYear >= nowYear;
  const prevBtn = document.getElementById("monthlyReportPrevBtn");
  if (prevBtn) prevBtn.disabled = monthlyReportState.loading || monthlyReportState.loadingIndex;
  renderMonthlyReportMonthPicker();
}

function renderMonthlyReport(payload = monthlyReportState.payload) {
  syncMonthlyReportControls();
  if (monthlyReportState.loading) {
    renderMonthlyReportLoading();
    return;
  }
  if (!payload || payload?.is_future_month) {
    renderMonthlyReportEmpty(payload?.is_future_month ? t("monthlyReportUnavailable") : monthlyReportState.statusText || t("monthlyReportUnavailable"));
    return;
  }
  const mode = payload.saved_report
    ? t("monthlyReportModeSaved")
    : payload.is_current_month && payload?.projections?.is_projection
    ? t("monthlyReportModeProjection")
    : payload.is_complete_month
      ? t("monthlyReportModeComplete")
      : t("monthlyReportModeProjection");
  setText(
    "monthlyReportSubtitle",
    t("monthlyReportSubtitle", {
      month: payload.report_month_local || getMonthlyReportMonthInfo().label,
      days: formatTrimmedDecimal(Number(payload.observed_days || 0), 1),
      total: payload.days_in_month || "-",
      mode,
    }),
  );
  setText("monthlyReportUpdatedAt", t("monthlyReportGenerated", { time: formatSamplingDateTime(payload.generated_at_utc) }));
  const reportStatus = getMonthlyReportStatus(payload);
  setMonthlyReportStatus(
    monthlyReportState.statusText || (payload.saved_report ? t(reportStatus === "no_data" ? "monthlyReportStatusNoData" : "monthlyReportStatusSaved") : ""),
    { saved: Boolean(payload.saved_report), noData: payload.saved_report && reportStatus === "no_data" },
  );
  renderMonthlyKpis(payload);
  renderMonthlyReportSections(payload);
  renderMonthlyRecommendations(payload);
  renderMonthlyDailyTrend(payload);
  renderMonthlyAnomalies(payload);
}

async function loadMonthlyReportIndexForVisibleYear({ force = false } = {}) {
  const year = monthlyReportState.pickerYear;
  if (!force && monthlyReportState.loadedYears.has(year)) {
    renderMonthlyReport();
    return;
  }
  if (monthlyReportState.loadingIndex) return;
  monthlyReportState.loadingIndex = true;
  renderMonthlyReport();
  try {
    const payload = await fetchJson(`/api/reports/monthly/index?year=${encodeURIComponent(year)}&limit=24`, { timeoutMs: 10000 });
    for (const item of payload.items || []) {
      if (item?.report_month_local) monthlyReportState.availableMonths.set(String(item.report_month_local), item);
    }
    monthlyReportState.loadedYears.add(year);
  } catch (err) {
    monthlyReportState.statusText = t("loadFailed", { error: String(err) });
  } finally {
    monthlyReportState.loadingIndex = false;
    renderMonthlyReport();
  }
}

async function loadMonthlyReport() {
  if (monthlyReportState.loading) return;
  if (!monthlyReportState.loadedYears.has(monthlyReportState.pickerYear)) {
    await loadMonthlyReportIndexForVisibleYear();
  }
  monthlyReportState.loading = true;
  monthlyReportState.statusText = t("monthlyReportLoading");
  renderMonthlyReport();
  const info = getMonthlyReportMonthInfo();
  try {
    const payload = await fetchJson(`/api/reports/monthly?month=${encodeURIComponent(info.label)}`, { timeoutMs: 60000 });
    monthlyReportState.payload = payload;
    monthlyReportState.statusText = "";
    if (payload?.report_month_local) {
      setMonthlyReportMonthFromKey(payload.report_month_local);
      if (payload.saved_report) {
        monthlyReportState.availableMonths.set(String(payload.report_month_local), {
          report_month_local: String(payload.report_month_local),
          timezone_name: payload.timezone_name || "",
          generated_at_utc: payload.generated_at_utc || "",
          observed_day_count: Number(payload.observed_day_count || 0),
          data_day_count: Number(payload.data_day_count || 0),
          report_status: payload.report_status || "",
        });
        monthlyReportState.loadedYears.add(monthlyReportState.monthYear);
      }
    }
  } catch (err) {
    monthlyReportState.payload = null;
    monthlyReportState.statusText = t("loadFailed", { error: String(err) });
  } finally {
    monthlyReportState.loading = false;
    renderMonthlyReport();
  }
}

function setReportCalendarVisible(visible) {
  const calendarColumn = document.getElementById("reportCalendarColumn");
  const calendarPanel = document.getElementById("reportCalendarPanel");
  const filterBar = document.getElementById("samplingFilterBar");
  if (calendarColumn) {
    calendarColumn.hidden = !visible;
    calendarColumn.classList.toggle("hidden", !visible);
    calendarColumn.setAttribute("aria-hidden", visible ? "false" : "true");
  }
  if (calendarPanel) {
    calendarPanel.hidden = !visible;
    calendarPanel.classList.toggle("hidden", !visible);
  }
  if (filterBar) {
    filterBar.hidden = visible;
    filterBar.classList.toggle("hidden", visible);
  }
}

function setReportUsageUploadVisible(visible) {
  const toolbar = document.getElementById("reportUsageUploadToolbar");
  if (!toolbar) return;
  toolbar.hidden = !visible;
  toolbar.classList.toggle("hidden", !visible);
  toolbar.setAttribute("aria-hidden", visible ? "false" : "true");
}

function renderReportCalendar() {
  const panel = document.getElementById("reportCalendarPanel");
  if (!panel) return;
  const info = getReportMonthInfo();
  const monthKey = info.label;
  const today = getLocalDateText();
  const daysInMonth = new SiteDate(info.year, info.month, 0).getDate();
  const completedDays = getReportMonthCompletedDayCount(info);
  const savedDays = Array.from(reportCalendarState.availableDays.keys()).filter((day) => day.startsWith(monthKey)).length;
  setText("reportCalendarTitle", info.display);
  setText("reportCalendarMeta", t("reportCalendarMeta", { month: monthKey, count: savedDays, days: completedDays }));
  const statusMessage = reportCalendarState.statusText || (reportCalendarState.loadingIndex ? t("reportCalendarLoading") : "");
  const statusEl = document.getElementById("reportCalendarStatus");
  if (statusEl) {
    statusEl.hidden = !statusMessage;
    statusEl.classList.toggle("hidden", !statusMessage);
    statusEl.textContent = statusMessage || "";
  }

  const prevBtn = document.getElementById("reportCalendarPrevBtn");
  const nextBtn = document.getElementById("reportCalendarNextBtn");
  if (prevBtn) {
    prevBtn.setAttribute("aria-label", t("reportCalendarPrevMonth"));
    prevBtn.disabled = reportCalendarState.loadingIndex || Boolean(reportCalendarState.loadingDay);
    prevBtn.onclick = async () => {
      const current = new SiteDate(info.year, info.month - 1, 1, 0, 0, 0, 0);
      current.setMonth(current.getMonth() - 1);
      reportCalendarState.monthYear = current.getFullYear();
      reportCalendarState.month = current.getMonth() + 1;
      reportCalendarState.statusText = "";
      renderReportCalendar();
      await loadReportIndexForVisibleMonth();
    };
  }
  if (nextBtn) {
    nextBtn.setAttribute("aria-label", t("reportCalendarNextMonth"));
    nextBtn.disabled = reportCalendarState.loadingIndex || Boolean(reportCalendarState.loadingDay);
    nextBtn.onclick = async () => {
      const current = new SiteDate(info.year, info.month - 1, 1, 0, 0, 0, 0);
      current.setMonth(current.getMonth() + 1);
      reportCalendarState.monthYear = current.getFullYear();
      reportCalendarState.month = current.getMonth() + 1;
      reportCalendarState.statusText = "";
      renderReportCalendar();
      await loadReportIndexForVisibleMonth();
    };
  }

  const weekdays = document.getElementById("reportCalendarWeekdays");
  if (weekdays) {
    weekdays.innerHTML = [
      "reportWeekdayMon",
      "reportWeekdayTue",
      "reportWeekdayWed",
      "reportWeekdayThu",
      "reportWeekdayFri",
      "reportWeekdaySat",
      "reportWeekdaySun",
    ]
      .map((key) => `<div class="report-calendar-weekday">${escapeHtml(t(key))}</div>`)
      .join("");
  }

  const grid = document.getElementById("reportCalendarGrid");
  if (!grid) return;
  const firstDay = new SiteDate(info.year, info.month - 1, 1, 0, 0, 0, 0);
  const leadingEmptyCells = (firstDay.getDay() + 6) % 7;
  const cells = [];
  for (let i = 0; i < leadingEmptyCells; i += 1) {
    cells.push('<div class="report-calendar-empty" aria-hidden="true"></div>');
  }
  const monthLoaded = reportCalendarState.loadedMonths.has(monthKey);
  for (let day = 1; day <= daysInMonth; day += 1) {
    const dayText = `${info.year}-${String(info.month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    const report = reportCalendarState.availableDays.get(dayText);
    const hasReport = Boolean(report);
    const billActualMeta = getReportBillActualMeta(report);
    const noDataReport = hasReport && getDailyReportStatus(report) === "no_data" && !billActualMeta.hasBillActual;
    const planLabel = hasReport && !noDataReport ? getReportCalendarPlanLabel(dayText) : "";
    const isLegacyZeroHero = hasReport && !noDataReport && isReportCalendarLegacyZeroHeroDay(dayText);
    const billActualText = billActualMeta.invoice
      ? t("reportCalendarBillActualWithInvoice", { invoice: billActualMeta.invoice })
      : t("reportCalendarBillActual");
    const live = dayText === today;
    const unavailable = dayText > today;
    const loading = reportCalendarState.loadingDay === dayText;
    const classes = [
      "report-calendar-day",
      hasReport ? (noDataReport ? "no-data-report" : "has-report") : "missing-report",
      isLegacyZeroHero ? "is-zerohero-legacy" : "",
      billActualMeta.hasBillActual ? "has-bill-actual" : "",
      live ? "is-live" : "",
      unavailable ? "is-unavailable" : "",
      dayText === reportCalendarState.selectedDay ? "is-selected" : "",
      loading ? "is-loading" : "",
    ]
      .filter(Boolean)
      .join(" ");
    const statusText = loading
      ? t("reportCalendarGenerating")
      : live
        ? t("reportCalendarLive")
      : unavailable
        ? t("reportCalendarPending")
        : hasReport
          ? (noDataReport ? t("reportCalendarNoData") : t("reportCalendarSaved"))
          : monthLoaded
            ? t("reportCalendarMissing")
            : t("reportCalendarChecking");
    const statusKind = loading
      ? "loading"
      : live
        ? "live"
      : unavailable
        ? "pending"
        : hasReport
          ? (noDataReport ? "no-data" : "saved")
          : monthLoaded
            ? "missing"
            : "checking";
    const disabled = unavailable || loading || reportCalendarState.loadingIndex || Boolean(reportCalendarState.loadingDay);
    const dayLabel = `${dayText} ${statusText}${planLabel ? ` - ${planLabel}` : ""}${billActualMeta.hasBillActual ? ` - ${billActualText}` : ""}`;
    cells.push(`
      <button type="button" class="${classes}" data-report-day="${escapeHtml(dayText)}" aria-label="${escapeHtml(dayLabel)}" title="${escapeHtml(dayLabel)}" ${disabled ? "disabled" : ""}>
        <span class="report-calendar-day-number">${day}</span>
        <span class="report-calendar-day-status-row">
          <span class="report-calendar-day-status-icon is-${escapeHtml(statusKind)}" aria-hidden="true" title="${escapeHtml(statusText)}"></span>
          ${
            billActualMeta.hasBillActual
              ? `<span class="report-calendar-bill-marker" aria-hidden="true" title="${escapeHtml(billActualText)}">$</span>`
              : ""
          }
        </span>
      </button>
    `);
  }
  grid.innerHTML = cells.join("");
  grid.querySelectorAll("[data-report-day]").forEach((button) => {
    button.addEventListener("click", async () => {
      const day = button.getAttribute("data-report-day") || "";
      await selectReportDay(day, { generateIfMissing: true });
    });
  });
}

async function loadReportIndexForVisibleMonth({ force = false } = {}) {
  const monthKey = getReportMonthKey();
  if (!force && reportCalendarState.loadedMonths.has(monthKey)) {
    renderReportCalendar();
    return;
  }
  reportCalendarState.loadingIndex = true;
  reportCalendarState.statusText = t("reportCalendarLoading");
  setReportLoadingModal(true, reportCalendarState.statusText);
  renderReportCalendar();
  try {
    const payload = await fetchJson(`/api/reports/daily/index?month=${encodeURIComponent(monthKey)}&limit=62`, {
      timeoutMs: 10000,
    });
    for (const item of payload.items || []) {
      if (item?.report_day_local) reportCalendarState.availableDays.set(String(item.report_day_local), item);
    }
    reportCalendarState.loadedMonths.add(monthKey);
    reportCalendarState.statusText = "";
  } catch (err) {
    reportCalendarState.statusText = t("loadFailed", { error: String(err) });
  } finally {
    reportCalendarState.loadingIndex = false;
    renderReportCalendar();
    if (!reportCalendarState.loadingDay) setReportLoadingModal(false);
  }
}

async function fetchDailyReportPayload(day, { timeoutMs = 10000 } = {}) {
  const params = new URLSearchParams({
    day_local: day,
    page: String(samplingPager.page),
    page_size: String(SAMPLING_PAGE_SIZE),
  });
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`/api/reports/daily?${params.toString()}`, { signal: controller.signal });
    window.clearTimeout(timeoutId);
    if (response.status === 404) return null;
    if (!response.ok) {
      throw new Error(await readErrorMessage(response));
    }
    return response.json();
  } catch (err) {
    window.clearTimeout(timeoutId);
    if (err?.name === "AbortError") {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw err;
  }
}

async function fetchDailyCostPayload(day, { startUtc = "", endUtc = "", timeoutMs = 12000 } = {}) {
  const params = new URLSearchParams({
    system: "combined",
    report_day_local: day,
  });
  if (startUtc) params.set("start_utc", startUtc);
  if (endUtc) params.set("end_utc", endUtc);
  return fetchJson(`/api/storage/daily-cost?${params.toString()}`, { timeoutMs });
}

function emptySamplingPagePayload() {
  return {
    count: 0,
    total: 0,
    page: 1,
    page_size: SAMPLING_PAGE_SIZE,
    has_next: false,
    has_prev: false,
    items: [],
  };
}

function getDailyReportStatus(report) {
  if (report?.report_status === "no_data" || Number(report?.sample_count) === 0) return "no_data";
  return "saved";
}

function setSamplingUpdatedAt(text, { noDataReport = false } = {}) {
  const element = document.getElementById("samplingUpdatedAt");
  if (element) element.classList.toggle("report-no-data-status", noDataReport);
  setText("samplingUpdatedAt", text);
}

function renderDailyReportEmpty(message, rangeLabel) {
  stateCache.lastSamplingDaily = null;
  stateCache.lastSamplingUsageBySystem = null;
  stateCache.lastSamplingPage = null;
  stateCache.lastSamplingSeries = null;
  stateCache.lastSamplingBatteryTimeline = null;
  stateCache.lastSamplingPhaseBattery = null;
  stateCache.lastSamplingTeslaFreeCharge = null;
  stateCache.lastSamplingFreeEnergyUse = null;
  stateCache.lastSamplingPumpStatus = null;
  stateCache.lastSamplingRecentPumpStatus = null;
  stateCache.lastSamplingZeroHeroHistory = null;
  samplingPager.hasNext = false;
  samplingPager.hasPrev = false;
  setSamplingUpdatedAt(message);
  setText("samplingDailyMeta", message);
  setText("samplingChartMeta", message);
  setText("samplingBatteryTimelineMeta", message);
  setText("samplingPumpStatusMeta", message);
  renderSamplingPage(emptySamplingPagePayload());
  renderSamplingChart({ items: [] });
  renderSamplingBatteryTimeline({ series: [] });
  renderSamplingPhaseBatteryRemaining(null, rangeLabel);
  renderTeslaFreeCharge(null);
  renderFreeEnergyUse(null);
  renderSamplingPumpStatusSummary(null, null);
  renderSamplingPumpStatusTimeline({ data: [] });
  renderSamplingTotals(null, "overall", rangeLabel, { metaText: message });
  renderSamplingDailyCost(null);
  renderSamplingZeroHeroHistory({ items: [] });
}

function reportBillArtifactUrl(path) {
  return `/api/bills/artifact?path=${encodeURIComponent(String(path || ""))}`;
}

function reportBillFileName(path) {
  const text = String(path || "");
  return text.split("/").filter(Boolean).pop() || text || "-";
}

function formatReportBillName(bill) {
  const period = bill?.billing_period && typeof bill.billing_period === "object" ? bill.billing_period : {};
  const start = String(period.start || "");
  const end = String(period.end || "");
  const invoice = String(bill?.invoice_number || "");
  const dateText = start && end ? (start === end ? start : `${start} to ${end}`) : "";
  if (dateText && invoice) return `${dateText} · ${invoice}`;
  if (dateText) return dateText;
  if (invoice) return invoice;
  return reportBillFileName(bill?.source_pdf || bill?.extracted_text || bill?.source_filename);
}

function setReportBillMenuVisible(visible) {
  const menu = document.getElementById("reportBillMenu");
  const button = document.getElementById("reportBillListBtn");
  if (menu) {
    menu.hidden = !visible;
    menu.classList.toggle("hidden", !visible);
    menu.setAttribute("aria-hidden", visible ? "false" : "true");
  }
  if (button) button.setAttribute("aria-expanded", visible ? "true" : "false");
}

function renderReportBillMenu() {
  const menu = document.getElementById("reportBillMenu");
  if (!menu) return;
  if (reportBillsState.loading) {
    menu.innerHTML = `<p class="report-bill-menu-message">${escapeHtml(t("reportBillListLoading"))}</p>`;
    return;
  }
  if (reportBillsState.error) {
    menu.innerHTML = `<p class="report-bill-menu-message is-error">${escapeHtml(
      t("reportBillListFailed", { error: reportBillsState.error }),
    )}</p>`;
    return;
  }
  const bills = [...(reportBillsState.items || [])].reverse();
  if (!bills.length) {
    menu.innerHTML = `<p class="report-bill-menu-message">${escapeHtml(t("reportBillListEmpty"))}</p>`;
    return;
  }
  menu.innerHTML = bills
    .map((bill, index) => {
      const sourcePath = String(bill?.source_pdf || bill?.extracted_text || "");
      const textPath = String(bill?.extracted_text || "");
      const name = formatReportBillName(bill);
      const fileName = reportBillFileName(sourcePath || textPath);
      return `
        <button type="button" class="report-bill-menu-item" role="menuitem" data-bill-index="${index}" ${sourcePath || textPath ? "" : "disabled"}>
          <span class="report-bill-menu-item-name">${escapeHtml(name)}</span>
          <span class="report-bill-menu-item-file">${escapeHtml(fileName)}</span>
        </button>
      `;
    })
    .join("");
  menu.querySelectorAll("[data-bill-index]").forEach((button) => {
    button.addEventListener("click", () => {
      const index = Number(button.getAttribute("data-bill-index"));
      const bill = bills[index];
      const sourcePath = String(bill?.source_pdf || bill?.extracted_text || "");
      if (!sourcePath) return;
      window.open(reportBillArtifactUrl(sourcePath), "_blank", "noopener");
      setReportBillMenuVisible(false);
    });
  });
}

async function loadReportBillsMenu({ force = false } = {}) {
  if (reportBillsState.loading) return;
  if (!force && reportBillsState.loaded) {
    renderReportBillMenu();
    return;
  }
  reportBillsState.loading = true;
  reportBillsState.error = "";
  renderReportBillMenu();
  try {
    const payload = await fetchJson("/api/bills", { timeoutMs: 10000 });
    reportBillsState.items = Array.isArray(payload?.bills) ? payload.bills : [];
    reportBillsState.loaded = true;
  } catch (err) {
    reportBillsState.error = formatApiErrorMessage(err);
  } finally {
    reportBillsState.loading = false;
    renderReportBillMenu();
  }
}

function billDateRangeText(period) {
  const start = String(period?.start || "");
  const end = String(period?.end || "");
  if (!start && !end) return "-";
  if (start === end) return start;
  return `${start || "-"} to ${end || "-"}`;
}

function formatBillRate(value, suffix) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${formatDailyCostAud(n)}${suffix}`;
}

function formatBillMoneyOutcome(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  if (Math.abs(n) < 0.005) return t("billsSettled");
  const amount = formatDailyCostAud(Math.abs(n));
  return n < 0 ? t("billsCreditEarned", { amount }) : t("billsPay", { amount });
}

function formatBillQuantity(value, unit) {
  const n = Number(value);
  const unitText = String(unit || "").trim();
  if (!Number.isFinite(n)) return unitText || "-";
  return `${formatTrimmedDecimal(n, 3)}${unitText ? ` ${unitText}` : ""}`;
}

function billRateRows(plan) {
  const rates = plan?.rates && typeof plan.rates === "object" ? plan.rates : {};
  return [
    ["billsRateDaily", "daily_charge_aud_per_day", "/day"],
    ["billsRatePeak", "peak_usage_aud_per_kwh", "/kWh"],
    ["billsRateFree", "off_peak_first_50_kwh_per_day_aud_per_kwh", "/kWh"],
    ["billsRateShoulder", "shoulder_usage_aud_per_kwh", "/kWh"],
    ["billsRateFitEvening", "solar_feed_in_4pm_11pm_aud_per_kwh", "/kWh"],
    ["billsRateFitOther", "solar_feed_in_11pm_4pm_aud_per_kwh", "/kWh"],
    ["billsRateSuperExport", "super_export_top_up_aud_per_kwh", "/kWh"],
    ["billsRateZeroHero", "zerohero_credit_aud_per_qualified_day", "/day"],
  ].map(([labelKey, key, suffix]) => {
    const item = rates[key] && typeof rates[key] === "object" ? rates[key] : {};
    const windowText = item.time_window ? ` · ${item.time_window}` : "";
    const capText = item.daily_cap_kwh ? ` · ${formatEnergyKwhText(item.daily_cap_kwh)}/day cap` : "";
    return `<span><strong>${escapeHtml(t(labelKey))}</strong> ${escapeHtml(formatBillRate(item.value, suffix))}${escapeHtml(windowText + capText)}</span>`;
  });
}

function billComparisonValue(column, key) {
  const n = Number(column?.[key]);
  return Number.isFinite(n) ? n : null;
}

function billComparisonTableHtml(comparison) {
  const columns = comparison?.columns && typeof comparison.columns === "object" ? comparison.columns : {};
  if (!columns.bill) return "";
  const columnDefs = [
    ["bill", "billsComparisonBill"],
    ["uploaded_usage_daily_cap", "billsComparisonUsageDailyCap"],
    ["system_daily_cap", "billsComparisonSystemDailyCap"],
    ["uploaded_usage_no_daily_cap", "billsComparisonUsageNoDailyCap"],
    ["system_no_daily_cap", "billsComparisonSystemNoDailyCap"],
  ];
  const rowDefs = [
    ["fixed_aud", "billsComparisonFixed"],
    ["import_aud", "billsComparisonImport"],
    ["solar_feed_in_aud", "billsComparisonSolar"],
    ["super_export_aud", "billsComparisonSuper"],
    ["zerohero_aud", "billsComparisonZeroHero"],
    ["other_aud", "billsComparisonOther"],
    ["total_aud", "billsComparisonTotal"],
  ];
  const billTotal = billComparisonValue(columns.bill, "total_aud");
  const body = rowDefs.map(([key, labelKey]) => `
    <tr class="${key === "total_aud" ? "is-total" : ""}">
      <th scope="row">${escapeHtml(t(labelKey))}</th>
      ${columnDefs.map(([columnKey]) => `<td>${escapeHtml(formatBillMoneyOutcome(billComparisonValue(columns[columnKey], key)))}</td>`).join("")}
    </tr>
  `).join("");
  const difference = `
    <tr class="is-difference">
      <th scope="row">${escapeHtml(t("billsComparisonDifference"))}</th>
      ${columnDefs.map(([columnKey]) => {
        const value = billComparisonValue(columns[columnKey], "total_aud");
        const delta = value === null || billTotal === null ? null : value - billTotal;
        return `<td>${escapeHtml(delta === null ? "-" : formatDailyCostAud(delta, { signed: true }))}</td>`;
      }).join("")}
    </tr>
  `;
  return `
    <section class="bills-detail-block">
      <div class="bills-detail-heading-row">
        <h4>${escapeHtml(t("billsComparisonTitle"))}</h4>
        <span class="muted">${escapeHtml(t("billsComparisonDataDays", {
          usageDays: comparison.uploaded_usage_days || 0,
          systemDays: comparison.system_days || 0,
          days: comparison.day_count || 0,
        }))}</span>
      </div>
      <div class="table-wrap bills-comparison-table-wrap">
        <table class="bills-lines-table bills-comparison-table">
          <thead>
            <tr>
              <th>${escapeHtml(t("reportDailyCostComponent"))}</th>
              ${columnDefs.map(([, labelKey]) => `<th>${escapeHtml(t(labelKey))}</th>`).join("")}
            </tr>
          </thead>
          <tbody>${body}${difference}</tbody>
        </table>
      </div>
    </section>
  `;
}

function billComparisonKwhTableHtml(comparison) {
  const usage = comparison?.usage_kwh && typeof comparison.usage_kwh === "object" ? comparison.usage_kwh : {};
  if (!usage.bill) return "";
  const columnDefs = [
    ["bill", "billsComparisonBill"],
    ["uploaded_usage", "reportDailyCostBillActual"],
    ["system", "reportDailyCostEstimated"],
  ];
  const rowDefs = [
    ["peak_import", "billsRatePeak"],
    ["offpeak_import", "billsRateFree"],
    ["offpeak_balance", "reportDailyCostOverCap"],
    ["shoulder_import", "billsRateShoulder"],
    ["solar_feed_in", "billsComparisonSolar"],
    ["super_export", "billsRateSuperExport"],
  ];
  const rows = rowDefs.map(([key, labelKey]) => `
    <tr>
      <th scope="row">${escapeHtml(t(labelKey))}</th>
      ${columnDefs.map(([columnKey]) => `<td>${escapeHtml(formatDailyCostKwh(usage[columnKey]?.[key]))}</td>`).join("")}
    </tr>
  `).join("");
  return `
    <section class="bills-detail-block">
      <h4>${escapeHtml(t("billsComparisonKwhTitle"))}</h4>
      <div class="table-wrap bills-comparison-table-wrap">
        <table class="bills-lines-table bills-comparison-table">
          <thead>
            <tr>
              <th>${escapeHtml(t("reportDailyCostComponent"))}</th>
              ${columnDefs.map(([, labelKey]) => `<th>${escapeHtml(t(labelKey))}</th>`).join("")}
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </section>
  `;
}

function renderBills(payload = stateCache.lastBills) {
  const list = document.getElementById("billsList");
  const grid = document.getElementById("billsSummaryGrid");
  const picker = document.getElementById("billsPeriodSelect");
  if (!list || !grid) return;
  const bills = Array.isArray(payload?.bills) ? payload.bills : [];
  setText("billsUpdatedAt", payload?.updated_at ? t("billsUpdated", { date: formatDateTimeWithAgo(payload.updated_at) }) : formatUpdatedAt(null));
  setText("billsMeta", t("billsMeta", { count: bills.length }));
  if (!payload) {
    grid.innerHTML = "";
    list.innerHTML = `<p class="bills-empty muted">${escapeHtml(t("billsLoading"))}</p>`;
    if (picker) picker.innerHTML = "";
    return;
  }
  if (payload.error) {
    grid.innerHTML = "";
    list.innerHTML = `<p class="bills-empty is-error">${escapeHtml(t("billsLoadFailed", { error: payload.error }))}</p>`;
    if (picker) picker.innerHTML = "";
    return;
  }
  if (!bills.length) {
    grid.innerHTML = "";
    list.innerHTML = `<p class="bills-empty muted">${escapeHtml(t("billsEmpty"))}</p>`;
    if (picker) picker.innerHTML = "";
    return;
  }
  if (!bills.some((bill) => String(bill.invoice_number || "") === billsState.selectedInvoice)) {
    billsState.selectedInvoice = String(bills[bills.length - 1]?.invoice_number || "");
  }
  const selectedBill = bills.find((bill) => String(bill.invoice_number || "") === billsState.selectedInvoice) || bills[bills.length - 1];
  const selectedPeriod = selectedBill?.billing_period && typeof selectedBill.billing_period === "object" ? selectedBill.billing_period : {};
  const selectedDays = Number(selectedPeriod.days);
  if (picker) {
    picker.innerHTML = bills.map((bill) => {
      const invoice = String(bill.invoice_number || "");
      const label = `#${bill.sequence_number || "-"} · ${billDateRangeText(bill.billing_period || {})}${invoice ? ` · ${invoice}` : ""}`;
      return `<option value="${escapeHtml(invoice)}">${escapeHtml(label)}</option>`;
    }).join("");
    picker.value = String(selectedBill?.invoice_number || "");
  }
  const planIds = new Set((selectedBill.rate_periods || []).map((period) => period?.plan_id).filter(Boolean));
  grid.innerHTML = [
    { label: t("billsBillTotal"), value: formatBillMoneyOutcome(selectedBill.bill_total_including_gst_aud) },
    { label: t("billsPeriod"), value: billDateRangeText(selectedBill?.billing_period || {}) },
    { label: t("billsBillingDays"), value: Number.isFinite(selectedDays) ? t("billsDaysValue", { days: formatTrimmedDecimal(selectedDays, 0) }) : "-" },
    { label: t("billsPlanCount"), value: String(planIds.size || "-") },
  ].map((item) => `
    <div class="bills-summary-card">
      <span>${escapeHtml(item.label)}</span>
      <strong>${escapeHtml(item.value)}</strong>
    </div>
  `).join("");

  const bill = selectedBill;
  const period = bill.billing_period || {};
  const sourcePdf = String(bill.source_pdf || bill.analysis?.source_documents?.pdf || "");
  const sourceText = String(bill.extracted_text || bill.analysis?.source_documents?.text || "");
  const ratePeriods = Array.isArray(bill.rate_periods) ? bill.rate_periods : [];
  const lineItems = Array.isArray(bill.line_items) ? bill.line_items : [];
  const comparisonHtml = billComparisonTableHtml(bill.cost_comparison || {});
  const comparisonKwhHtml = billComparisonKwhTableHtml(bill.cost_comparison || {});
  const rateHtml = ratePeriods.map((periodItem) => {
    const plan = periodItem.plan || {};
    const planName = plan.display_name || plan.id || "-";
    return `
      <section class="bills-rate-period">
        <div>
          <strong>${escapeHtml(planName)}</strong>
          <span class="muted">${escapeHtml(billDateRangeText(periodItem))} · ${escapeHtml(plan.price_basis || "-")}</span>
        </div>
        <div class="bills-rate-grid">${billRateRows(plan).join("")}</div>
      </section>
    `;
  }).join("");
  const linesHtml = lineItems.map((line) => `
    <tr>
      <td>${escapeHtml(line.description || "-")}</td>
      <td>${escapeHtml(billDateRangeText({ start: line.period_start, end: line.period_end }))}</td>
      <td>${escapeHtml(formatBillQuantity(line.quantity, line.unit))}</td>
      <td>${escapeHtml(formatDailyCostAud(line.rate_aud))}</td>
      <td>${escapeHtml(formatBillMoneyOutcome(line.total_aud))}</td>
    </tr>
  `).join("");
  list.innerHTML = `
      <article class="bills-item">
        <div class="bills-item-head">
          <div>
            <p class="bills-item-kicker">${escapeHtml(`#${bill.sequence_number || "-"} · ${t("billsPeriod")}`)}</p>
            <h3>${escapeHtml(billDateRangeText(period))}</h3>
          </div>
          <strong class="bills-item-total">${escapeHtml(formatBillMoneyOutcome(bill.bill_total_including_gst_aud))}</strong>
        </div>
        <div class="bills-facts">
          <span><strong>${escapeHtml(t("billsInvoice"))}</strong> ${escapeHtml(bill.invoice_number || "-")}</span>
          <span><strong>${escapeHtml(t("billsBillingDays"))}</strong> ${escapeHtml(Number.isFinite(Number(period.days)) ? t("billsDaysValue", { days: formatTrimmedDecimal(Number(period.days), 0) }) : "-")}</span>
          <span><strong>${escapeHtml(t("billsIssued"))}</strong> ${escapeHtml(bill.issue_date || "-")}</span>
          <span><strong>${escapeHtml(t("billsDue"))}</strong> ${escapeHtml(bill.due_date || "-")}</span>
          <span><strong>${escapeHtml(t("billsAmountDue"))}</strong> ${escapeHtml(formatBillMoneyOutcome(bill.amount_due_aud))}</span>
          <span><strong>${escapeHtml(t("billsAnalysisType"))}</strong> ${escapeHtml(bill.analysis?.type || "-")}</span>
        </div>
        <section class="bills-detail-block">
          <h4>${escapeHtml(t("billsPlanRates"))}</h4>
          ${rateHtml || `<p class="muted">-</p>`}
        </section>
        ${comparisonHtml}
        ${comparisonKwhHtml}
        <section class="bills-detail-block">
          <h4>${escapeHtml(t("billsParsedLines"))}</h4>
          <div class="table-wrap bills-lines-table-wrap">
            <table class="bills-lines-table">
              <thead>
                <tr>
                  <th>${escapeHtml(t("billsLineDescription"))}</th>
                  <th>${escapeHtml(t("billsLinePeriod"))}</th>
                  <th>${escapeHtml(t("billsLineQuantity"))}</th>
                  <th>${escapeHtml(t("billsLineRate"))}</th>
                  <th>${escapeHtml(t("billsLineTotal"))}</th>
                </tr>
              </thead>
              <tbody>${linesHtml}</tbody>
            </table>
          </div>
        </section>
        <section class="bills-detail-block bills-documents">
          <h4>${escapeHtml(t("billsDocuments"))}</h4>
          ${sourcePdf ? `<a class="btn secondary" href="${escapeHtml(reportBillArtifactUrl(sourcePdf))}" target="_blank" rel="noopener">${escapeHtml(t("billsOpenPdf"))}</a>` : ""}
          ${sourceText ? `<a class="btn secondary" href="${escapeHtml(reportBillArtifactUrl(sourceText))}" target="_blank" rel="noopener">${escapeHtml(t("billsOpenText"))}</a>` : ""}
        </section>
      </article>
    `;
}

async function loadBills({ force = false } = {}) {
  if (!force && stateCache.lastBills) {
    renderBills(stateCache.lastBills);
    return;
  }
  stateCache.lastBills = null;
  renderBills(null);
  try {
    const payload = await fetchJson("/api/bills", { timeoutMs: 10000 });
    stateCache.lastBills = payload;
  } catch (err) {
    stateCache.lastBills = { bills: [], error: formatApiErrorMessage(err), updated_at: null };
  }
  renderBills(stateCache.lastBills);
}

async function toggleReportBillMenu() {
  const menu = document.getElementById("reportBillMenu");
  const nextVisible = !menu || menu.classList.contains("hidden");
  setReportBillMenuVisible(nextVisible);
  if (nextVisible) await loadReportBillsMenu();
}

async function selectReportDay(day, { generateIfMissing = false } = {}) {
  if (!localDateParts(day)) return;
  reportCalendarState.selectedDay = day;
  setReportCalendarMonthFromDay(day);
  samplingPager.page = 1;
  reportCalendarState.statusText = "";
  renderReportCalendar();
  await loadReportIndexForVisibleMonth();
  await loadDailyReport({ generateIfMissing });
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
  const next = new SiteDate(date.getTime());
  if (relative.unit === "m") {
    next.setTime(next.getTime() - relative.amount * 60000);
    return next;
  }
  if (relative.unit === "h") {
    next.setTime(next.getTime() - relative.amount * 3600000);
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
  const monthNumber = Number(samplingRangeState.month || `${new SiteDate().getMonth() + 1}`);
  const monthYear = Number(samplingRangeState.monthYear || new SiteDate().getFullYear());
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
    const info = getMonthInfo(monthNumber, monthYear);
    return {
      mode,
      startUtc: info.startUtc,
      endUtc: info.endUtc,
      label: info.label,
    };
  }
  if (mode === "relative") {
    const relative = parseSamplingRelativeInput(relativeText);
    const end = new SiteDate();
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
  const info = getDayInfo(dayText);
  return {
    mode: "day",
    startUtc: info.startUtc,
    endUtc: info.endUtc,
    label: info.label,
  };
}

function getSamplingFocusLocalDay() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const today = getLocalDateText();
  if (mode === "week") {
    return getWeekInfo(samplingRangeState.week || today).anchor;
  }
  if (mode === "month") {
    const info = getMonthInfo(samplingRangeState.month, samplingRangeState.monthYear);
    const todayParts = localDateParts(today);
    if (todayParts && todayParts.year === info.year && todayParts.monthIndex + 1 === info.month) {
      return today;
    }
    return `${info.year}-${String(info.month).padStart(2, "0")}-01`;
  }
  if (mode === "relative") {
    return today;
  }
  if (mode === "custom_date") {
    const startDate = samplingRangeState.startDate || "";
    const endDate = samplingRangeState.endDate || "";
    const candidate = endDate || startDate || today;
    return localDateParts(candidate) ? candidate : today;
  }
  if (mode === "custom_datetime") {
    const candidate = String(samplingRangeState.endDateTime || samplingRangeState.startDateTime || "").split("T")[0];
    return localDateParts(candidate) ? candidate : today;
  }
  return getDayInfo(samplingRangeState.day || today).day;
}

function buildSamplingRangeCurrentBadge(mode) {
  if (mode === "day") {
    const selected = samplingRangeState.day || getLocalDateText();
    const currentDay = getLocalDateText();
    return selected === currentDay ? t("samplingRangeTodayBadge") : "";
  }
  if (mode === "week") {
    const selected = getWeekInfo(samplingRangeState.week || getLocalDateText());
    const current = getWeekInfo(getLocalDateText());
    return selected.anchor === current.anchor ? t("samplingRangeThisWeekBadge") : "";
  }
  if (mode === "month") {
    const selected = getMonthInfo(samplingRangeState.month, samplingRangeState.monthYear);
    const now = new SiteDate();
    return selected.year === now.getFullYear() && selected.month === now.getMonth() + 1 ? t("samplingRangeThisMonthBadge") : "";
  }
  return "";
}

function updateSamplingRangeJumpButton() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const button = document.getElementById("samplingRangeJumpBtn");
  if (!button) return;
  let label = "";
  if (mode === "day") label = t("samplingRangeTodayBadge");
  else if (mode === "week") label = t("samplingRangeThisWeekBadge");
  else if (mode === "month") label = t("samplingRangeThisMonthBadge");
  button.textContent = label;
  button.hidden = !label;
  button.classList.toggle("hidden", !label);
}

function setSamplingRangeControlsDisabled(disabled) {
  const filterBar = document.getElementById("samplingFilterBar");
  if (filterBar) {
    filterBar.querySelectorAll("button, select, input").forEach((node) => {
      if (node instanceof HTMLButtonElement || node instanceof HTMLSelectElement || node instanceof HTMLInputElement) {
        node.disabled = disabled;
      }
    });
  }
}

function renderSamplingRangeInputContainer() {
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  const container = document.getElementById("samplingRangeInputContainer");
  if (!container) return;
  updateSamplingRangeJumpButton();

  if (mode === "day") {
    const info = getDayInfo(samplingRangeState.day || getLocalDateText());
    const currentBadge = buildSamplingRangeCurrentBadge("day");
    samplingRangeState.day = info.day;
    container.innerHTML = `
      <label>
        ${t("samplingDayLabel")}
        <div class="sampling-range-stack">
          <div class="sampling-week-nav">
            <button id="samplingDayPrevBtn" type="button" class="btn secondary" aria-label="${escapeHtml(t("samplingArrowPrev"))}">&#x2039;</button>
            <div id="samplingDayDisplayText" class="sampling-week-display">${escapeHtml(info.label)}${currentBadge ? ` <span class="sampling-range-badge">${escapeHtml(currentBadge)}</span>` : ""}</div>
            <button id="samplingDayNextBtn" type="button" class="btn secondary" aria-label="${escapeHtml(t("samplingArrowNext"))}">&#x203A;</button>
          </div>
        </div>
        <div id="samplingDayField" class="sampling-field">
          <input id="samplingDayInput" type="date" value="${samplingRangeState.day || ""}" />
        </div>
      </label>
    `;
  } else if (mode === "week") {
    const info = getWeekInfo(samplingRangeState.week || getLocalDateText());
    const currentBadge = buildSamplingRangeCurrentBadge("week");
    samplingRangeState.week = info.anchor;
    container.innerHTML = `
      <label>
        ${t("samplingWeekLabel")}
        <div class="sampling-range-stack">
          <div class="sampling-week-nav sampling-week-nav-tall">
            <button id="samplingWeekPrevBtn" type="button" class="btn secondary sampling-week-arrow-btn" aria-label="${escapeHtml(t("samplingArrowPrev"))}">&#x2039;</button>
            <div id="samplingWeekDisplayText" class="sampling-week-display sampling-week-display-tall">
              <span class="sampling-week-display-top">W${escapeHtml(String(info.week || "-"))}${currentBadge ? ` <span class="sampling-range-badge">${escapeHtml(currentBadge)}</span>` : ""}</span>
              <span class="sampling-week-display-bottom">${escapeHtml(formatSamplingMiniDate(info.monday))} <span class="sampling-week-display-sep">|</span> ${escapeHtml(formatSamplingMiniDate(info.sunday))}</span>
            </div>
            <button id="samplingWeekNextBtn" type="button" class="btn secondary sampling-week-arrow-btn" aria-label="${escapeHtml(t("samplingArrowNext"))}">&#x203A;</button>
          </div>
        </div>
      </label>
    `;
  } else if (mode === "month") {
    const info = getMonthInfo(samplingRangeState.month, samplingRangeState.monthYear);
    const currentBadge = buildSamplingRangeCurrentBadge("month");
    samplingRangeState.month = String(info.month);
    samplingRangeState.monthYear = info.year;
    container.innerHTML = `
      <label>
        ${t("samplingMonthLabel")}
        <div class="sampling-range-stack">
          <div class="sampling-week-nav sampling-month-nav sampling-week-nav-tall">
            <button id="samplingMonthPrevBtn" type="button" class="btn secondary sampling-week-arrow-btn" aria-label="${escapeHtml(t("samplingArrowPrev"))}">&#x2039;</button>
            <div id="samplingMonthDisplayText" class="sampling-week-display sampling-week-display-tall">
              <span class="sampling-week-display-top">${escapeHtml(info.display)}</span>
              <span class="sampling-week-display-bottom">${currentBadge ? `<span class="sampling-range-badge">${escapeHtml(currentBadge)}</span>` : '<span class="sampling-range-badge sampling-range-badge-placeholder" aria-hidden="true">&nbsp;</span>'}</span>
            </div>
            <button id="samplingMonthNextBtn" type="button" class="btn secondary sampling-week-arrow-btn" aria-label="${escapeHtml(t("samplingArrowNext"))}">&#x203A;</button>
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

  if (mode === "day") {
    const prevBtn = document.getElementById("samplingDayPrevBtn");
    const nextBtn = document.getElementById("samplingDayNextBtn");
    const moveDay = async (deltaDays) => {
      const parts = localDateParts(samplingRangeState.day || getLocalDateText());
      const current = parts
        ? new SiteDate(parts.year, parts.monthIndex, parts.day, 0, 0, 0, 0)
        : new SiteDate();
      current.setDate(current.getDate() + deltaDays);
      samplingRangeState.day = `${current.getFullYear()}-${String(current.getMonth() + 1).padStart(2, "0")}-${String(current.getDate()).padStart(2, "0")}`;
      renderSamplingRangeInputContainer();
      samplingPager.page = 1;
      await loadSampling();
    };
    if (prevBtn) prevBtn.addEventListener("click", async () => moveDay(-1));
    if (nextBtn) nextBtn.addEventListener("click", async () => moveDay(1));
    bindPicker("samplingDayField", "samplingDayInput", (v) => {
      samplingRangeState.day = v;
    });
    return;
  }

  if (mode === "week") {
    const prevBtn = document.getElementById("samplingWeekPrevBtn");
    const nextBtn = document.getElementById("samplingWeekNextBtn");
    const moveWeek = async (deltaDays) => {
      const parts = localDateParts(samplingRangeState.week || getLocalDateText());
      const current = parts
        ? new SiteDate(parts.year, parts.monthIndex, parts.day, 0, 0, 0, 0)
        : new SiteDate();
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
    const prevBtn = document.getElementById("samplingMonthPrevBtn");
    const nextBtn = document.getElementById("samplingMonthNextBtn");
    const moveMonth = async (deltaMonths) => {
      const info = getMonthInfo(samplingRangeState.month, samplingRangeState.monthYear);
      const current = new SiteDate(info.year, info.month - 1, 1, 0, 0, 0, 0);
      current.setMonth(current.getMonth() + deltaMonths);
      samplingRangeState.monthYear = current.getFullYear();
      samplingRangeState.month = String(current.getMonth() + 1);
      renderSamplingRangeInputContainer();
      samplingPager.page = 1;
      await loadSampling();
    };
    if (prevBtn) prevBtn.addEventListener("click", async () => moveMonth(-1));
    if (nextBtn) nextBtn.addEventListener("click", async () => moveMonth(1));
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
}

function buildSamplingUrl(range = getSamplingRange()) {
  const params = new URLSearchParams();
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
  head.innerHTML = `<tr>${columns.map((column) => `<th>${escapeHtml(column?.name || "-")}</th>`).join("")}<th class="database-actions-head"></th></tr>`;
  body.innerHTML = "";
  const items = Array.isArray(payload?.items) ? payload.items : [];
  for (const item of items) {
    const tr = document.createElement("tr");
    tr.classList.add("database-row");
    tr.innerHTML = columns.map((column) => {
      const key = String(column?.name || "");
      const rawValue = item?.[key];
      return `<td class="database-cell">${renderDatabaseValueHtml(key, rawValue)}</td>`;
    }).join("") + `
      <td class="database-actions-cell">
        <button
          type="button"
          class="database-detail-button"
          data-role="open-database-detail"
          aria-label="Open row details"
          title="Open row details"
        >🔍</button>
      </td>
    `;
    tr.querySelectorAll(".database-time-toggle").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        toggleDatabaseTimeButton(button);
      });
    });
    tr.querySelector('[data-role="open-database-detail"]')?.addEventListener("click", (event) => {
      event.stopPropagation();
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

function renderDatabaseBackupStatus(payload) {
  const status = payload?.backup_status || payload || {};
  const backups = Array.isArray(status.backups) ? status.backups : [];
  const latest = status.latest_backup || backups[0] || null;
  const retention = Number(status.retention_count || 3);
  const directory = String(status.backup_dir || "-");
  if (!latest) {
    setText("databaseBackupMeta", t("databaseBackupMetaEmpty", { retention, directory }));
    return;
  }
  const latestText = latest.backup_date || formatDateTimeWithAgo(latest.modified_at_utc);
  setText(
    "databaseBackupMeta",
    t("databaseBackupMeta", {
      count: Number(status.count || backups.length || 0),
      retention,
      latest: latestText || "-",
      size: formatBinarySize(latest.size_bytes),
      totalSize: formatBinarySize(status.total_size_bytes),
      directory,
    }),
  );
}

async function loadDatabaseBackupStatus() {
  try {
    const payload = await fetchJson("/api/database/backups", { timeoutMs: 6000 });
    stateCache.lastDatabaseBackups = payload;
    renderDatabaseBackupStatus(payload);
    return payload;
  } catch (err) {
    stateCache.lastDatabaseBackups = null;
    setText("databaseBackupMeta", t("databaseBackupMetaFailed", { error: String(err) }));
    return null;
  }
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
      return `
        <tr>
          <th>${escapeHtml(key || "-")}</th>
          <td><div class="worker-log-detail-value">${renderDatabaseValueHtml(key, rawValue, { multilineObjects: true })}</div></td>
        </tr>
      `;
    }).join("");
    body.querySelectorAll(".database-time-toggle").forEach((button) => {
      button.addEventListener("click", () => {
        toggleDatabaseTimeButton(button);
      });
    });
  }
  setDatabaseRowDetailModalVisible(true);
}

function rerenderDatabaseViewFromCache() {
  if (stateCache.lastDatabaseBackups) renderDatabaseBackupStatus(stateCache.lastDatabaseBackups);
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
    resultText.includes("grid_import_started")
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
      const isLong = column === "error_text" || column === "result_text" || column === "payload_json" || column === "key_metrics";
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

function renderOperationTimelineRows(items) {
  const body = document.getElementById("operationTimelineBody");
  if (!body) return;
  body.innerHTML = "";
  for (const item of items || []) {
    const tr = document.createElement("tr");
    const operationText = String(item?.operation || "").trim() || "-";
    const reasonText = String(item?.reason || "").trim() || "-";
    const timeText = formatDateTimeWithAgo(item?.time_utc || null);
    tr.innerHTML = `
      <td>${escapeHtml(timeText)}</td>
      <td><pre class="worker-result-pre operation-timeline-pre" title="${escapeHtml(operationText)}">${escapeHtml(operationText)}</pre></td>
      <td><pre class="worker-result-pre operation-timeline-pre" title="${escapeHtml(reasonText)}">${escapeHtml(reasonText)}</pre></td>
    `;
    body.appendChild(tr);
  }
}

function renderOperationTimelinePage(payload) {
  const items = Array.isArray(payload?.items) ? payload.items : [];
  stateCache.lastOperationTimeline = payload || null;
  setText("operationTimelineCount", t("operationTimelineCount", { total: payload?.count || items.length || 0 }));
  setText("operationTimelineUpdatedAt", formatUpdatedAt(payload?.updated_at || null));
  renderOperationTimelineRows(items);
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
  const selectedSystem = getSelectedSamplingSystem();
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
  setSamplingUpdatedAt(`${t("updatedAt")}: ${updatedAt}`);
}

function getSamplingZeroHeroObservationSource() {
  if (currentTab === "report") return { collectorStatus: null, midday: {}, observation: null };
  const summaryCollectorStatus = stateCache.lastSummary?.collectorStatus || null;
  const collectorStatus = stateCache.lastCollectorStatus || summaryCollectorStatus || null;
  const combined = collectorStatus?.systems?.combined || summaryCollectorStatus?.systems?.combined || {};
  const midday = combined?.last_midday_window_check || {};
  const exportWindow = midday?.export_window && typeof midday.export_window === "object" ? midday.export_window : {};
  const observation =
    exportWindow?.zero_hero_window && typeof exportWindow.zero_hero_window === "object"
      ? exportWindow.zero_hero_window
      : null;
  return { collectorStatus, midday, observation };
}

function formatSamplingZeroHeroWindowRange(observation) {
  const start = observation?.window_start_local || observation?.window_start_utc;
  const end = observation?.window_end_local || observation?.window_end_utc;
  if (!start || !end) return "-";
  return `${formatSamplingClock(start)} - ${formatSamplingClock(end)}`;
}

function formatSamplingRemainingSeconds(seconds) {
  const total = Math.max(0, Math.round(Number(seconds) || 0));
  const minutes = Math.floor(total / 60);
  const remainder = total % 60;
  return `${minutes}m ${String(remainder).padStart(2, "0")}s`;
}

function formatSamplingZeroHeroEnergyWh(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  if (Math.abs(numeric) >= 1000) return `${formatTrimmedDecimal(numeric / 1000, 3)} kWh`;
  return `${formatTrimmedDecimal(numeric, 0)} Wh`;
}

function formatSamplingZeroHeroCoverage(observation) {
  const samples = Math.max(0, Math.round(Number(observation?.samples || 0)));
  const expected = Math.max(0, Math.round(Number(observation?.expected_samples || 0)));
  const coverageRatio = Number(observation?.coverage_ratio);
  if (Number.isFinite(coverageRatio) && expected > 0) {
    return t("samplingZeroHeroCoverageText", {
      samples,
      expected,
      coverage: formatTrimmedDecimal(coverageRatio * 100, 0),
    });
  }
  if (expected > 0) return `${samples}/${expected}`;
  if (samples > 0) return String(samples);
  return "-";
}

function formatSamplingKwh(value, digits = 1) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return `${formatTrimmedDecimal(numeric, digits)} kWh`;
}

function formatSolplanetExportMonitorRecommendation(monitor) {
  const recommendation = String(monitor?.recommendation || "").trim();
  const powerText = formatPowerKwFromWatts(monitor?.recommended_pout_w);
  if (recommendation === "sell") {
    return t("solplanetExportMonitorRecommendationSell", { power: powerText });
  }
  if (recommendation === "sell_surplus") {
    return t("solplanetExportMonitorRecommendationSellSurplus", { power: powerText });
  }
  if (recommendation === "stop") return t("solplanetExportMonitorRecommendationStop");
  if (recommendation === "hold") return t("solplanetExportMonitorRecommendationHold");
  if (recommendation === "hold_until_late_phase") return t("solplanetExportMonitorRecommendationHoldUntilLate");
  return t("solplanetExportMonitorRecommendationObserve");
}

function formatSolplanetExportMonitorPhase(monitor) {
  const phase = String(monitor?.phase || "").trim();
  if (phase === "pre_peak_hold" || phase === "pre_peak_surplus") return t("solplanetExportMonitorPhasePrePeak");
  if (phase === "peak_hold") return t("solplanetExportMonitorPhasePeakHold");
  if (phase === "late_target") return t("solplanetExportMonitorPhaseLateTarget");
  return t("solplanetExportMonitorPhaseOutside");
}

function solplanetExportMonitorActiveReserveKwh(monitor) {
  const hardReserveKwh = Number(monitor?.reserve_floor_kwh);
  if (Number.isFinite(hardReserveKwh)) return hardReserveKwh;
  return null;
}

function renderSamplingZeroHeroWindow() {
  const badgeEl = document.getElementById("samplingZeroHeroBadge");
  const bodyEl = document.getElementById("samplingZeroHeroBody");
  if (!badgeEl || !bodyEl) return;

  const { collectorStatus, midday, observation } = getSamplingZeroHeroObservationSource();
  const exportWindow = midday?.export_window && typeof midday.export_window === "object" ? midday.export_window : {};
  const solplanetExportMonitor =
    exportWindow?.solplanet_export_monitor && typeof exportWindow.solplanet_export_monitor === "object"
      ? exportWindow.solplanet_export_monitor
      : null;
  const windowMode = String(midday?.window_mode || "").trim();
  const observationActive = windowMode === "export_window" && observation;
  const updatedAt = midday?.executed_at_utc || collectorStatus?.updated_at || null;

  let badgeTone = "";
  let badgeText = t("samplingZeroHeroStateInactive");
  if (observationActive) {
    if (observation.live_import_active) {
      badgeTone = "danger";
      badgeText = t("samplingZeroHeroStateLiveImport");
    } else if (observation.has_observed_grid_import) {
      badgeTone = "warn";
      badgeText = t("samplingZeroHeroStateImportObserved");
    } else if (observation.estimated_zero_import_ok) {
      badgeTone = "good";
      badgeText = t("samplingZeroHeroStateEligible");
    } else {
      badgeText = t("samplingZeroHeroStateMonitoring");
    }
  }
  badgeEl.className = `sampling-zero-hero-badge${badgeTone ? ` ${badgeTone}` : ""}`;
  badgeEl.textContent = badgeText;

  setText(
    "samplingZeroHeroMeta",
    `${t(observationActive ? "samplingZeroHeroMetaActive" : "samplingZeroHeroMetaInactive")} · ${formatUpdatedAt(updatedAt)}`,
  );

  if (!observationActive) {
    bodyEl.innerHTML = `<div class="sampling-zero-hero-empty">${escapeHtml(t("samplingZeroHeroAwaiting"))}</div>`;
    return;
  }

  const metrics = [
    { label: t("samplingZeroHeroFieldWindow"), value: formatSamplingZeroHeroWindowRange(observation) },
    { label: t("samplingZeroHeroFieldRemaining"), value: formatSamplingRemainingSeconds(observation?.remaining_seconds) },
    { label: t("samplingZeroHeroFieldObservedImport"), value: formatSamplingZeroHeroEnergyWh(observation?.observed_grid_import_wh) },
    { label: t("samplingZeroHeroFieldObservedExport"), value: formatSamplingZeroHeroEnergyWh(observation?.observed_grid_export_wh) },
    { label: t("samplingZeroHeroFieldLiveImport"), value: formatPowerKwFromWatts(observation?.live_grid_import_w) },
    { label: t("samplingZeroHeroFieldLiveExport"), value: formatPowerKwFromWatts(observation?.live_grid_export_w) },
    { label: t("samplingZeroHeroFieldCoverage"), value: formatSamplingZeroHeroCoverage(observation) },
  ];
  if (solplanetExportMonitor) {
    const exportedKwh = Number(solplanetExportMonitor.exported_kwh);
    const targetKwh = Number(solplanetExportMonitor.daily_export_target_kwh);
    const usableKwh = Number(
      solplanetExportMonitor.control_usable_battery_kwh ?? solplanetExportMonitor.usable_battery_total_kwh,
    );
    const activeReserveKwh = solplanetExportMonitorActiveReserveKwh(solplanetExportMonitor);
    metrics.push(
      {
        label: t("samplingZeroHeroFieldSolplanetRecommendation"),
        value: formatSolplanetExportMonitorRecommendation(solplanetExportMonitor),
      },
      {
        label: t("samplingZeroHeroFieldSolplanetPhase"),
        value: formatSolplanetExportMonitorPhase(solplanetExportMonitor),
      },
      {
        label: t("samplingZeroHeroFieldDailyExport"),
        value: `${formatSamplingKwh(exportedKwh, 2)} / ${formatSamplingKwh(targetKwh, 1)}`,
      },
      {
        label: t("samplingZeroHeroFieldUsableBattery"),
        value: formatSamplingKwh(usableKwh, 1),
      },
      {
        label: t("samplingZeroHeroFieldActiveReserve"),
        value: activeReserveKwh === null ? "-" : formatSamplingKwh(activeReserveKwh, 1),
      },
      {
        label: t("samplingZeroHeroFieldReserve"),
        value:
          Number.isFinite(usableKwh) && activeReserveKwh !== null
            ? formatSamplingKwh(usableKwh - activeReserveKwh, 1)
            : "-",
      },
    );
  }

  bodyEl.innerHTML = metrics
    .map(
      (item) => `
        <article class="sampling-zero-hero-metric">
          <p class="sampling-zero-hero-metric-label">${escapeHtml(item.label)}</p>
          <p class="sampling-zero-hero-metric-value">${escapeHtml(item.value)}</p>
        </article>
      `,
    )
    .join("");
}

function formatSamplingZeroHeroHistoryDayLabel(dayItem) {
  const parts = localDateParts(dayItem?.date_local);
  if (!parts) return String(dayItem?.date_local || "-");
  const dt = new SiteDate(parts.year, parts.monthIndex, parts.day);
  const locale = currentLang === "zh" ? "zh-CN" : "en-AU";
  const weekday = dt.toLocaleDateString(locale, { weekday: "short" });
  if (currentLang === "zh") {
    return `${parts.monthIndex + 1}/${parts.day} ${weekday}${dayItem?.is_today ? ` · ${t("samplingZeroHeroHistoryToday")}` : ""}`;
  }
  return `${weekday}, ${shortMonthLabel(parts.monthIndex + 1)} ${parts.day}${dayItem?.is_today ? ` · ${t("samplingZeroHeroHistoryToday")}` : ""}`;
}

function formatSamplingZeroHeroHistoryWindowRange(windowItem) {
  const start = windowItem?.window_start_local || windowItem?.window_start_utc;
  const end = windowItem?.window_end_local || windowItem?.window_end_utc;
  if (!start || !end) return "-";
  return `${formatSamplingClock(start)} - ${formatSamplingClock(end)}`;
}

function formatSamplingZeroHeroHistoryState(windowItem) {
  const state = String(windowItem?.state || "").trim();
  if (state === "import") return t("samplingZeroHeroHistoryStateImport");
  if (state === "mixed_export") return t("samplingZeroHeroHistoryStateMixedExport");
  if (state === "export") return t("samplingZeroHeroHistoryStateExport");
  if (state === "no_data") return t("samplingZeroHeroHistoryStateNoData");
  if (state === "pending") return t("samplingZeroHeroHistoryStatePending");
  return t("samplingZeroHeroHistoryStateIdle");
}

function formatSamplingZeroHeroHistoryCoverage(windowItem) {
  if (windowItem?.is_future) return "-";
  const samples = Math.max(0, Math.round(Number(windowItem?.samples || 0)));
  const expected = Math.max(0, Math.round(Number(windowItem?.expected_samples || 0)));
  const parts = [];
  if (expected > 0) {
    parts.push(t("samplingZeroHeroHistoryCoverageText", { samples, expected }));
  } else if (samples > 0) {
    parts.push(String(samples));
  }
  if (windowItem?.is_in_progress) {
    parts.push(t("samplingZeroHeroHistoryInProgress"));
  } else if (windowItem?.has_partial_coverage) {
    parts.push(t("samplingZeroHeroHistoryCoveragePartial"));
  }
  return parts.join(" · ") || "-";
}

function renderSamplingZeroHeroHistory(payload = stateCache.lastSamplingZeroHeroHistory) {
  const badgeEl = document.getElementById("samplingZeroHeroHistoryBadge");
  const metaEl = document.getElementById("samplingZeroHeroHistoryMeta");
  const bodyEl = document.getElementById("samplingZeroHeroHistoryBody");
  if (!badgeEl || !metaEl || !bodyEl) return;

  const items = Array.isArray(payload?.items) ? payload.items : [];
  const totalNetImportWindows = items.reduce(
    (sum, item) => sum + Number(item?.summary?.import_windows || 0),
    0,
  );
  const totalMixedExportWindows = items.reduce(
    (sum, item) => sum + Number(item?.summary?.mixed_export_windows || 0),
    0,
  );
  const totalImportObservedWindows = items.reduce(
    (sum, item) => sum + Number(item?.summary?.import_observed_windows || 0),
    0,
  );
  const totalNoDataWindows = items.reduce(
    (sum, item) => sum + Number(item?.summary?.no_data_windows || 0),
    0,
  );

  let badgeTone = "good";
  let badgeText = t("samplingZeroHeroHistoryBadgeClean");
  if (totalNetImportWindows > 0) {
    badgeTone = "danger";
    badgeText = t("samplingZeroHeroHistoryBadgeImport", {
      count: totalImportObservedWindows,
      netCount: totalNetImportWindows,
    });
  } else if (totalMixedExportWindows > 0) {
    badgeTone = "warn";
    badgeText = t("samplingZeroHeroHistoryBadgeMixed", { count: totalMixedExportWindows });
  } else if (totalNoDataWindows > 0) {
    badgeTone = "warn";
    badgeText = t("samplingZeroHeroHistoryBadgeNoData", { count: totalNoDataWindows });
  }
  badgeEl.className = `sampling-zero-hero-badge${badgeTone ? ` ${badgeTone}` : ""}`;
  badgeEl.textContent = badgeText;

  const anchorDayLabel = formatSamplingZeroHeroHistoryDayLabel({
    date_local: payload?.anchor_day_local || items[0]?.date_local || getSamplingFocusLocalDay(),
    is_today: (payload?.anchor_day_local || items[0]?.date_local || "") === getLocalDateText(),
  });
  const startHour = String(Math.max(0, Math.min(23, Number(payload?.window_start_hour || 18)))).padStart(2, "0");
  const endHour = String(Math.max(0, Math.min(23, Number(payload?.window_end_hour || 20)))).padStart(2, "0");
  metaEl.textContent = `${t("samplingZeroHeroHistoryMetaText", {
    day: anchorDayLabel,
    start: `${startHour}:00`,
    end: `${endHour}:00`,
    count: Number(payload?.window_count_per_day || 0),
  })} · ${formatUpdatedAt(payload?.updated_at || null)}`;

  if (!items.length) {
    bodyEl.innerHTML = `<div class="sampling-zero-hero-empty">${escapeHtml(t("samplingZeroHeroHistoryAwaiting"))}</div>`;
    return;
  }

  bodyEl.innerHTML = items
    .map((dayItem) => {
      const summary = dayItem?.summary || {};
      const windows = Array.isArray(dayItem?.windows) ? dayItem.windows : [];
      return `
        <section class="sampling-zero-hero-history-day">
          <div class="sampling-zero-hero-history-day-head">
            <div>
              <p class="sampling-zero-hero-history-day-title">${escapeHtml(formatSamplingZeroHeroHistoryDayLabel(dayItem))}</p>
              <p class="sampling-zero-hero-history-day-summary">${escapeHtml(
                t("samplingZeroHeroHistoryDaySummary", {
                  importCount: Number(summary.import_windows || 0),
                  mixedCount: Number(summary.mixed_export_windows || 0),
                  exportCount: Number(summary.export_windows || 0),
                  idleCount: Number(summary.idle_windows || 0),
                  noDataCount: Number(summary.no_data_windows || 0),
                  pendingCount: Number(summary.pending_windows || 0),
                  partialCount: Number(summary.partial_windows || 0),
                }),
              )}</p>
            </div>
            <p class="sampling-zero-hero-history-day-energy">${escapeHtml(
              t("samplingZeroHeroHistoryDayEnergy", {
                import: formatSamplingZeroHeroEnergyWh(summary.import_wh),
                export: formatSamplingZeroHeroEnergyWh(summary.export_wh),
              }),
            )}</p>
          </div>
          <div class="sampling-zero-hero-history-grid">
            ${windows
              .map((windowItem) => {
                const stateClass = `is-${String(windowItem?.state || "idle").replaceAll("_", "-")}`;
                const partialClass = windowItem?.has_partial_coverage ? " is-partial" : "";
                const energyText = windowItem?.is_future
                  ? "-"
                  : t("samplingZeroHeroHistoryEnergyText", {
                      import: formatSamplingZeroHeroEnergyWh(windowItem?.grid_import_wh),
                      export: formatSamplingZeroHeroEnergyWh(windowItem?.grid_export_wh),
                    });
                const rangeText = formatSamplingZeroHeroHistoryWindowRange(windowItem);
                const stateText = formatSamplingZeroHeroHistoryState(windowItem);
                const coverageText = formatSamplingZeroHeroHistoryCoverage(windowItem);
                const tooltipLines = [
                  rangeText,
                  stateText,
                  energyText,
                  coverageText,
                ].filter((line) => line && line !== "-");
                const tooltipText = tooltipLines.join(" · ");
                return `
                  <article
                    class="sampling-zero-hero-history-window ${stateClass}${partialClass}"
                    tabindex="0"
                    aria-label="${escapeHtml(tooltipText || rangeText)}"
                  >
                    <div class="sampling-zero-hero-history-window-tooltip" role="tooltip">
                      <p class="sampling-zero-hero-history-window-tooltip-title">${escapeHtml(rangeText)}</p>
                      <p>${escapeHtml(stateText)}</p>
                      <p>${escapeHtml(energyText)}</p>
                      <p>${escapeHtml(coverageText)}</p>
                    </div>
                  </article>
                `;
              })
              .join("")}
          </div>
        </section>
      `;
    })
    .join("");
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

function formatDailyCostAud(value, { signed = false } = {}) {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  const sign = n < 0 ? "-" : (signed && n > 0 ? "+" : "");
  return `${sign}$${formatTrimmedDecimal(Math.abs(n), 2)}`;
}

function formatDailyCostDeltaPercent(delta, actual) {
  const deltaNumber = Number(delta);
  const actualNumber = Number(actual);
  if (!Number.isFinite(deltaNumber) || !Number.isFinite(actualNumber) || Math.abs(actualNumber) < 0.0001) {
    return "-";
  }
  const percent = (deltaNumber / Math.abs(actualNumber)) * 100;
  const sign = percent > 0 ? "+" : "";
  return `${sign}${formatTrimmedDecimal(percent, Math.abs(percent) >= 10 ? 1 : 2)}%`;
}

function formatDailyCostDeltaAudWithPercent(delta, actual) {
  const deltaText = formatDailyCostAud(delta, { signed: true });
  const percentText = formatDailyCostDeltaPercent(delta, actual);
  return percentText === "-" ? deltaText : `${deltaText} (${percentText})`;
}

function dailyCostVariantTotal(cost, variantKey, fallbackKey) {
  const variants = cost?.pricing_variants && typeof cost.pricing_variants === "object" ? cost.pricing_variants : {};
  const variant = variants[variantKey] && typeof variants[variantKey] === "object" ? variants[variantKey] : {};
  const variantTotal = Number(variant.total_aud);
  if (Number.isFinite(variantTotal)) return variantTotal;
  const fallback = Number(cost?.[fallbackKey]);
  return Number.isFinite(fallback) ? fallback : null;
}

function formatDailyCostFormulaPart(label, value, unit = "kWh") {
  const n = Number(value);
  if (!Number.isFinite(n) || Math.abs(n) < 0.0001) return "";
  const valueText = unit === "AUD" ? formatDailyCostAud(n) : `${formatTrimmedDecimal(n, 2)} ${unit}`;
  return `${label} ${valueText}`;
}

function formatDailyCostRate(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `$${formatTrimmedDecimal(n, 4)}/kWh`;
}

function formatDailyCostKwhRatePart(label, kwh, rate) {
  const kwhNumber = Number(kwh);
  if (!Number.isFinite(kwhNumber) || Math.abs(kwhNumber) < 0.0001) return "";
  return `${label} ${formatTrimmedDecimal(kwhNumber, 2)} kWh x ${formatDailyCostRate(rate)}`;
}

function joinDailyCostFormula(parts) {
  const filtered = parts.filter(Boolean);
  return filtered.length ? filtered.join(" · ") : "-";
}

function formatDailyCostKwh(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${formatTrimmedDecimal(n, 2)} kWh`;
}

function dailyCostZeroHeroStatusText(item) {
  const status = String(item?.status || "");
  if (item?.qualified === true || status === "qualified") return t("reportDailyCostQualified");
  if (status === "not_qualified") return t("reportDailyCostNotQualified");
  if (status === "insufficient_data") return t("reportDailyCostInsufficient");
  if (status === "not_available") return t("reportDailyCostNotAvailable");
  return "-";
}

function formatApiErrorMessage(err) {
  const raw = String(err?.message || err || "");
  try {
    const parsed = JSON.parse(raw);
    if (typeof parsed?.detail === "string") return parsed.detail;
  } catch (_err) {
    // Use the original message.
  }
  return raw;
}

function setSamplingDailyCostHidden(hidden) {
  const container = document.getElementById("samplingDailyCost");
  if (!container) return;
  container.classList.toggle("hidden", Boolean(hidden));
  container.hidden = Boolean(hidden);
  container.setAttribute("aria-hidden", hidden ? "true" : "false");
  if (hidden) container.innerHTML = "";
}

function renderSamplingDailyCost(dailyCost, billActualCost = null) {
  const container = document.getElementById("samplingDailyCost");
  if (!container) return;
  const dailyCostMap = dailyCost && typeof dailyCost === "object" ? dailyCost : {};
  const billActualMap = billActualCost && typeof billActualCost === "object" ? billActualCost : {};
  const hasEstimatedCost = Number.isFinite(Number(dailyCostMap.total_daily_cost_aud));
  const hasBillActual = Number.isFinite(Number(billActualMap.actual_daily_cost_aud));
  if (currentTab !== "report" || (!hasEstimatedCost && !hasBillActual)) {
    setSamplingDailyCostHidden(true);
    return;
  }

  setSamplingDailyCostHidden(false);
  const total = hasEstimatedCost ? Number(dailyCostMap.total_daily_cost_aud) : null;
  const estimatedDailyCapTotal = hasEstimatedCost ? dailyCostVariantTotal(dailyCostMap, "daily_cap", "total_daily_cost_aud") : null;
  const estimatedNoDailyCapTotal = hasEstimatedCost ? dailyCostVariantTotal(dailyCostMap, "no_daily_cap", "total_daily_cost_aud") : null;

  const lineItems = dailyCostMap.line_items || {};
  const fixed = lineItems.fixed || {};
  const importItem = lineItems.import || {};
  const exportItem = lineItems.export || {};
  const zeroHero = lineItems.zerohero || {};
  const rates = dailyCostMap.rates && typeof dailyCostMap.rates === "object" ? dailyCostMap.rates : {};
  const fixedAud = hasEstimatedCost ? Number(fixed.total_aud || 0) : null;
  const importAud = hasEstimatedCost ? Number(importItem.charge_aud || 0) : null;
  const exportCreditAud = hasEstimatedCost ? Number(exportItem.credit_aud || 0) : null;
  const fitCreditAud = hasEstimatedCost ? Number(exportItem.fit_credit_aud || 0) : null;
  const superExportCreditAud = hasEstimatedCost ? Number(exportItem.super_export_credit_aud || 0) : null;
  const zeroHeroCreditAud = hasEstimatedCost ? Number(zeroHero.credit_aud || 0) : null;
  const planName = dailyCostMap.plan?.display_name || dailyCostMap.plan?.id || billActualMap.plan_name || billActualMap.plan_id || "-";
  const basis = dailyCostMap.price_basis || billActualMap.price_basis || "inc GST";
  const samples = Number(dailyCostMap.samples || billActualMap.samples || 0);
  const actualTotal = hasBillActual ? Number(billActualMap.actual_daily_cost_aud) : null;
  const actualDailyCapTotal = hasBillActual ? dailyCostVariantTotal(billActualMap, "daily_cap", "actual_daily_cost_aud") : null;
  const actualNoDailyCapTotal = hasBillActual ? dailyCostVariantTotal(billActualMap, "no_daily_cap", "actual_daily_cost_aud") : null;
  const actualItems = hasBillActual && billActualMap.line_items && typeof billActualMap.line_items === "object"
    ? billActualMap.line_items
    : {};
  const actualFixed = Number(actualItems.fixed_aud || 0);
  const actualImport = Number(actualItems.import_aud || 0);
  const actualExport = Number(actualItems.solar_feed_in_aud || 0) + Number(actualItems.super_export_aud || 0);
  const actualZeroHero = Number(actualItems.zerohero_aud || 0);
  const actualUsageKwh = actualItems.usage_kwh && typeof actualItems.usage_kwh === "object" ? actualItems.usage_kwh : {};
  const zeroHeroRate = Number(dailyCostMap.rates?.zerohero_credit_aud_per_qualified_day || 0);
  const zeroHeroStatus = String(zeroHero.status || "");
  const hasZeroHeroCostLine =
    zeroHeroRate > 0 ||
    Math.abs(Number(zeroHeroCreditAud || 0)) > 0.0001 ||
    Math.abs(actualZeroHero) > 0.0001 ||
    (zeroHeroStatus && zeroHeroStatus !== "not_available");
  const invoiceText = hasBillActual
    ? `${billActualMap.invoice_number || "-"} · ${t("reportDailyCostBillAllocated")}`
    : t("reportDailyCostNoBill");
  const zeroHeroCoverage = Number(zeroHero.coverage_ratio);
  const zeroHeroCoverageText = Number.isFinite(zeroHeroCoverage)
    ? `${formatTrimmedDecimal(zeroHeroCoverage * 100, 0)}%`
    : "-";
  const zeroHeroRedWindows = Math.max(0, Math.round(Number(zeroHero.net_import_windows || 0)));
  const zeroHeroYellowWindows = Math.max(0, Math.round(Number(zeroHero.mixed_export_windows || 0)));
  const deltaTone = (value) => {
    if (value === null || value === undefined || !hasBillActual || !Number.isFinite(Number(value))) return "is-muted";
    if (Math.abs(Number(value)) < 0.01) return "is-even";
    return Number(value) > 0 ? "is-higher" : "is-lower";
  };
  const rows = [
    {
      key: "fixed",
      label: t("reportDailyCostFixed"),
      estimatedDetail: joinDailyCostFormula([
        formatDailyCostFormulaPart("supply", rates.daily_charge_aud_per_day, "AUD"),
        formatDailyCostFormulaPart("metering", rates.metering_charge_aud_per_day, "AUD"),
      ]),
      actualDetail: joinDailyCostFormula([
        formatDailyCostFormulaPart("supply", rates.daily_charge_aud_per_day, "AUD"),
        formatDailyCostFormulaPart("metering", rates.metering_charge_aud_per_day, "AUD"),
      ]),
      estimated: fixedAud,
      actual: actualFixed,
    },
    {
      key: "import",
      label: t("reportDailyCostImport"),
      estimatedDetail: joinDailyCostFormula([
        formatDailyCostKwhRatePart("peak", importItem.peak_kwh, rates.peak_usage_aud_per_kwh),
        formatDailyCostKwhRatePart("shoulder", importItem.shoulder_kwh, rates.shoulder_usage_aud_per_kwh),
        formatDailyCostKwhRatePart("free", importItem.free_kwh, rates.off_peak_first_50_kwh_per_day_aud_per_kwh),
        formatDailyCostKwhRatePart(t("reportDailyCostOverCap"), importItem.off_peak_balance_kwh, rates.off_peak_balance_aud_per_kwh),
      ]),
      actualDetail: joinDailyCostFormula([
        formatDailyCostKwhRatePart("peak", actualUsageKwh.peak_kwh, rates.peak_usage_aud_per_kwh),
        formatDailyCostKwhRatePart("shoulder", actualUsageKwh.shoulder_kwh, rates.shoulder_usage_aud_per_kwh),
        formatDailyCostKwhRatePart("free", actualUsageKwh.free_kwh, rates.off_peak_first_50_kwh_per_day_aud_per_kwh),
        formatDailyCostKwhRatePart(t("reportDailyCostOverCap"), actualUsageKwh.off_peak_balance_kwh, rates.off_peak_balance_aud_per_kwh),
      ]),
      estimated: importAud,
      actual: actualImport,
    },
    {
      key: "credits",
      label: t("reportDailyCostCredits"),
      estimatedDetail: joinDailyCostFormula([
        formatDailyCostKwhRatePart("FIT 4-11", exportItem.fit_evening_kwh, rates.solar_feed_in_evening_aud_per_kwh),
        formatDailyCostKwhRatePart("FIT other", exportItem.fit_other_kwh, rates.solar_feed_in_other_aud_per_kwh),
        formatDailyCostKwhRatePart("super export", exportItem.super_export_billable_kwh, rates.super_export_top_up_aud_per_kwh),
      ]),
      actualDetail: joinDailyCostFormula([
        formatDailyCostKwhRatePart("FIT 4-11", actualUsageKwh.fit_evening_kwh, rates.solar_feed_in_evening_aud_per_kwh),
        formatDailyCostKwhRatePart("FIT other", actualUsageKwh.fit_other_kwh, rates.solar_feed_in_other_aud_per_kwh),
        formatDailyCostKwhRatePart("super export", actualUsageKwh.super_export_billable_kwh, rates.super_export_top_up_aud_per_kwh),
      ]),
      estimated: exportCreditAud === null ? null : -exportCreditAud,
      actual: actualExport,
    },
  ];
  if (hasZeroHeroCostLine) {
    rows.push({
      key: "zerohero",
      label: t("reportDailyCostZeroHero"),
      estimatedDetail: t("reportDailyCostZeroHeroDetail", {
        status: dailyCostZeroHeroStatusText(zeroHero),
        redWindows: Number.isFinite(zeroHeroRedWindows) ? String(zeroHeroRedWindows) : "-",
        yellowWindows: Number.isFinite(zeroHeroYellowWindows) ? String(zeroHeroYellowWindows) : "-",
        coverage: zeroHeroCoverageText,
      }),
      actualDetail: billActualMap?.zerohero && typeof billActualMap.zerohero === "object"
        ? t("reportDailyCostZeroHeroDetail", {
            status: dailyCostZeroHeroStatusText(billActualMap.zerohero),
            redWindows: Number.isFinite(Number(billActualMap.zerohero.net_import_windows)) ? String(Math.round(Number(billActualMap.zerohero.net_import_windows))) : "-",
            yellowWindows: Number.isFinite(Number(billActualMap.zerohero.mixed_export_windows)) ? String(Math.round(Number(billActualMap.zerohero.mixed_export_windows))) : "-",
            coverage: Number.isFinite(Number(billActualMap.zerohero.coverage_ratio)) ? `${formatTrimmedDecimal(Number(billActualMap.zerohero.coverage_ratio) * 100, 0)}%` : "-",
          })
        : "-",
      estimated: zeroHeroCreditAud === null ? null : -zeroHeroCreditAud,
      actual: actualZeroHero,
    });
  }
  const rowHtml = rows
    .map((row) => {
      const rowDelta = hasEstimatedCost && hasBillActual ? row.estimated - row.actual : null;
      return `
        <div class="sampling-daily-cost-row" role="row">
          <div class="sampling-daily-cost-component" role="cell">
            <span class="sampling-daily-cost-component-name">${escapeHtml(row.label)}</span>
          </div>
          <div class="sampling-daily-cost-formula" role="cell">${escapeHtml(row.estimatedDetail)}</div>
          <div class="sampling-daily-cost-amount" role="cell">${escapeHtml(formatDailyCostAud(row.estimated))}</div>
          <div class="sampling-daily-cost-formula ${hasBillActual ? "" : "is-muted"}" role="cell">${escapeHtml(hasBillActual ? row.actualDetail : "-")}</div>
          <div class="sampling-daily-cost-amount ${hasBillActual ? "" : "is-muted"}" role="cell">${escapeHtml(hasBillActual ? formatDailyCostAud(row.actual) : "-")}</div>
          <div class="sampling-daily-cost-amount sampling-daily-cost-delta ${deltaTone(rowDelta)}" role="cell">${escapeHtml(hasEstimatedCost && hasBillActual ? formatDailyCostDeltaAudWithPercent(rowDelta, row.actual) : "-")}</div>
        </div>
      `;
    })
    .join("");

  container.innerHTML = `
    <div class="sampling-daily-cost-head">
      <div>
        <p class="sampling-daily-cost-title">${escapeHtml(t("reportDailyCostTitle"))}</p>
        <p class="sampling-daily-cost-meta">${escapeHtml(t("reportDailyCostMeta", { plan: planName, basis, samples }))}</p>
      </div>
      <p class="sampling-daily-cost-bill-chip ${hasBillActual ? "has-bill" : "no-bill"}">${escapeHtml(invoiceText)}</p>
    </div>
    <div class="sampling-daily-cost-summary">
      <div class="sampling-daily-cost-metric">
        <span class="sampling-daily-cost-label">${escapeHtml(t("reportDailyCostSystemNoDailyCap"))}</span>
        <strong class="sampling-daily-cost-metric-value">${escapeHtml(formatDailyCostAud(estimatedNoDailyCapTotal))}</strong>
        <span class="sampling-daily-cost-metric-note">${escapeHtml(t("reportDailyCostEstimated"))}</span>
      </div>
      <div class="sampling-daily-cost-metric">
        <span class="sampling-daily-cost-label">${escapeHtml(t("reportDailyCostSystemDailyCap"))}</span>
        <strong class="sampling-daily-cost-metric-value">${escapeHtml(formatDailyCostAud(estimatedDailyCapTotal))}</strong>
        <span class="sampling-daily-cost-metric-note">${escapeHtml(t("reportDailyCostEstimated"))}</span>
      </div>
      <div class="sampling-daily-cost-metric ${hasBillActual ? "" : "is-muted"}">
        <span class="sampling-daily-cost-label">${escapeHtml(t("reportDailyCostUsageNoDailyCap"))}</span>
        <strong class="sampling-daily-cost-metric-value">${escapeHtml(hasBillActual ? formatDailyCostAud(actualNoDailyCapTotal) : "-")}</strong>
        <span class="sampling-daily-cost-metric-note">${escapeHtml(t("reportDailyCostBillActual"))}</span>
      </div>
      <div class="sampling-daily-cost-metric ${hasBillActual ? "" : "is-muted"}">
        <span class="sampling-daily-cost-label">${escapeHtml(t("reportDailyCostUsageDailyCap"))}</span>
        <strong class="sampling-daily-cost-metric-value">${escapeHtml(hasBillActual ? formatDailyCostAud(actualDailyCapTotal) : "-")}</strong>
        <span class="sampling-daily-cost-metric-note">${escapeHtml(t("reportDailyCostBillActual"))}</span>
      </div>
    </div>
    <div class="sampling-daily-cost-table" role="table" aria-label="${escapeHtml(t("reportDailyCostBreakdown"))}">
      <div class="sampling-daily-cost-row sampling-daily-cost-row-head" role="row">
        <div role="columnheader">${escapeHtml(t("reportDailyCostComponent"))}</div>
        <div role="columnheader">${escapeHtml(t("reportDailyCostEstimated"))} ${escapeHtml(t("reportDailyCostFormula"))}</div>
        <div role="columnheader">${escapeHtml(t("reportDailyCostEstimated"))}</div>
        <div role="columnheader">${escapeHtml(t("reportDailyCostBill"))} ${escapeHtml(t("reportDailyCostFormula"))}</div>
        <div role="columnheader">${escapeHtml(t("reportDailyCostBill"))}</div>
        <div role="columnheader">${escapeHtml(t("reportDailyCostEstimateMinusBill"))}</div>
      </div>
      ${rowHtml}
    </div>
  `;
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

function normalizeSamplingFocusSeries(system) {
  const chartSeriesMeta = getSamplingChartSeriesMeta(system);
  if (samplingChartFocusSeries && !chartSeriesMeta.some((meta) => meta.key === samplingChartFocusSeries)) {
    samplingChartFocusSeries = null;
  }
  return chartSeriesMeta;
}

function getSamplingSeriesColor(seriesKey, fallback = "#6b7f72") {
  return SAMPLING_SERIES_META_BY_KEY[seriesKey]?.color || fallback;
}

function getSamplingTotalBaseColor(seriesKey, fallback = "#6b7f72") {
  return getSamplingSeriesColor(seriesKey, fallback);
}

function getSamplingWindowSegmentColor(seriesKey, fallback = "#2fb36f") {
  if (!seriesKey) return fallback;
  if (seriesKey === "grid_import_w") return "#60a5fa";
  if (seriesKey === "grid_export_w") return "#22d3ee";
  return fallback;
}

function setSamplingFocusSeries(nextKey) {
  samplingChartFocusSeries = samplingChartFocusSeries === nextKey ? null : nextKey;
  if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
  if (stateCache.lastSamplingUsageBySystem) {
    const range = currentTab === "report"
      ? getDayInfo(reportCalendarState.selectedDay || getLocalDateText(-1))
      : getSamplingRange();
    const system = getSelectedSamplingSystem();
    renderSamplingTotals(stateCache.lastSamplingUsageBySystem, system, range.label);
  }
}

function normalizeGridWindowBreakdown(rawBreakdown, totalValue) {
  const total = Math.max(Number(totalValue || 0), 0);
  const windowValue = Math.max(0, Math.min(total, Number(rawBreakdown?.window || 0)));
  return {
    windowValue,
    otherValue: Math.max(0, total - windowValue),
    totalValue: total,
  };
}

function formatSamplingWindowSplit(windowValue, totalValue) {
  const total = Math.max(Number(totalValue || 0), 0);
  if (total <= 0) return "-";
  return `⏱ ${formatEnergyKwhText(Math.max(Number(windowValue || 0), 0))}  📊 ${formatEnergyKwhText(total)}`;
}

function formatSamplingGridImportCostSplit(freeValue, totalValue) {
  const total = Math.max(Number(totalValue || 0), 0);
  if (total <= 0) return "-";
  const free = Math.max(0, Math.min(total, Number(freeValue || 0)));
  const paid = Math.max(0, total - free);
  return `${t("samplingTotalsWindowFree")} ${formatTrimmedDecimal(free, 2)} + ${t("samplingTotalsWindowPaid")} ${formatTrimmedDecimal(paid, 2)} / ${t("samplingTotalsWindowTotal")} ${formatTrimmedDecimal(total, 2)} kWh`;
}

function buildSamplingGridImportCostParts(freeValue, totalValue) {
  const total = Math.max(Number(totalValue || 0), 0);
  const free = Math.max(0, Math.min(total, Number(freeValue || 0)));
  return [
    { label: t("samplingTotalsWindowFree"), value: free },
    { label: t("samplingTotalsWindowPaid"), value: Math.max(total - free, 0) },
    { label: t("samplingTotalsWindowTotal"), value: total },
  ];
}

function formatSamplingPointsCompact(samples) {
  const sampleCount = Math.max(0, Math.round(Number(samples || 0)));
  return t("samplingTotalsPointsCompact", { samples: sampleCount });
}

function formatSamplingValueWithPoints(valueText, samples) {
  const normalized = String(valueText || "").trim();
  if (!normalized || normalized === "-") return normalized || "-";
  return `${normalized} · ${formatSamplingPointsCompact(samples)}`;
}

function combineSamplingUsageForOverall(usageBySystem) {
  const combinedUsage = usageBySystem?.combined || null;
  const sajUsage = usageBySystem?.saj || null;
  const solplanetUsage = usageBySystem?.solplanet || null;
  const combinedEnergy = combinedUsage?.energy_kwh || {};
  const sajEnergy = sajUsage?.energy_kwh || {};
  const solplanetEnergy = solplanetUsage?.energy_kwh || {};
  const combinedBreakdown = combinedUsage?.breakdown_kwh || {};
  const sajBreakdown = sajUsage?.breakdown_kwh || {};
  const solplanetBreakdown = solplanetUsage?.breakdown_kwh || {};
  const combinedHasData = Number(combinedUsage?.samples || 0) >= 2;
  const sajHasData = Number(sajUsage?.samples || 0) >= 2;
  const solplanetHasData = Number(solplanetUsage?.samples || 0) >= 2;
  return {
    system: "overall",
    samples: combinedHasData ? Number(combinedUsage?.samples || 0) : 0,
    energy_kwh: {
      home_load: combinedHasData ? Number(combinedEnergy.home_load || 0) : 0,
      solar_generation: combinedHasData ? Number(combinedEnergy.solar_generation || 0) : 0,
      grid_import: combinedHasData ? Number(combinedEnergy.grid_import || 0) : 0,
      grid_export: combinedHasData ? Number(combinedEnergy.grid_export || 0) : 0,
      battery_charge:
        (sajHasData ? Number(sajEnergy.battery_charge || 0) : 0) + (solplanetHasData ? Number(solplanetEnergy.battery_charge || 0) : 0),
      battery_discharge:
        (sajHasData ? Number(sajEnergy.battery_discharge || 0) : 0) +
        (solplanetHasData ? Number(solplanetEnergy.battery_discharge || 0) : 0),
    },
    breakdown_kwh: {
      grid_import: {
        window: combinedHasData ? Number(combinedBreakdown?.grid_import?.window || 0) : 0,
      },
      grid_export: {
        window: combinedHasData ? Number(combinedBreakdown?.grid_export?.window || 0) : 0,
      },
    },
  };
}

function buildSamplingTotalsSummary(usageBySystem, selectedSystem) {
  const combinedUsage = usageBySystem?.combined || null;
  const sajUsage = usageBySystem?.saj || null;
  const solplanetUsage = usageBySystem?.solplanet || null;
  const combinedEnergy = combinedUsage?.energy_kwh || {};
  const sajEnergy = sajUsage?.energy_kwh || {};
  const solplanetEnergy = solplanetUsage?.energy_kwh || {};
  const combinedBreakdown = combinedUsage?.breakdown_kwh || {};
  const sajBreakdown = sajUsage?.breakdown_kwh || {};
  const solplanetBreakdown = solplanetUsage?.breakdown_kwh || {};
  const combinedHasData = Number(combinedUsage?.samples || 0) >= 2;
  const sajHasData = Number(sajUsage?.samples || 0) >= 2;
  const solplanetHasData = Number(solplanetUsage?.samples || 0) >= 2;

  if (selectedSystem === "overall") {
    const totalGridImport = combinedHasData ? Number(combinedEnergy.grid_import || 0) : 0;
    const totalGridExport = combinedHasData ? Number(combinedEnergy.grid_export || 0) : 0;
    const gridImportBreakdown = normalizeGridWindowBreakdown(combinedBreakdown?.grid_import, totalGridImport);
    const gridExportBreakdown = normalizeGridWindowBreakdown(combinedBreakdown?.grid_export, totalGridExport);
    const cards = [
      {
        title: t("samplingOverallMetricPv"),
        value: combinedHasData ? Number(combinedEnergy.solar_generation || 0) : 0,
        kind: "pv",
        seriesKey: "pv_total_w",
        scope: t("samplingTotalsScopeOverall"),
        layout: "single",
        samples: combinedHasData ? Number(combinedUsage?.samples || 0) : 0,
      },
      {
        title: t("samplingTotalsGridTitle"),
        leftLabel: t("samplingOverallMetricGridImport"),
        leftValue: totalGridImport,
        leftKind: "import",
        leftSeriesKey: "grid_import_w",
        leftWindowValue: gridImportBreakdown.windowValue,
        leftWindowLabel: SAMPLING_GRID_IMPORT_WINDOW.label,
        leftWindowCostSplit: true,
        rightLabel: t("samplingOverallMetricGridExport"),
        rightValue: totalGridExport,
        rightKind: "export",
        rightSeriesKey: "grid_export_w",
        rightWindowValue: gridExportBreakdown.windowValue,
        rightWindowLabel: SAMPLING_GRID_EXPORT_WINDOW.label,
        scope: t("samplingTotalsScopeOverall"),
        layout: "dual",
        samples: combinedHasData ? Number(combinedUsage?.samples || 0) : 0,
      },
      {
        title: t("samplingTotalsScopeHomeLoad"),
        value: combinedHasData ? Number(combinedBreakdown?.home_load?.tesla || combinedEnergy.tesla_load || 0) : 0,
        kind: "load",
        seriesKey: "tesla_charge_power_w",
        rowKey: "home_load_tesla",
        scope: t("samplingTotalsScopeOverall"),
        layout: "single",
        showGroupHeader: true,
        samples: combinedHasData ? Number(combinedUsage?.samples || 0) : 0,
        rowLabel: t("samplingOverallMetricHomeLoadTesla"),
      },
      {
        title: t("samplingTotalsScopeHomeLoad"),
        value: combinedHasData ? Number(combinedBreakdown?.home_load?.pool_pump || combinedEnergy.pool_pump_load || 0) : 0,
        kind: "load",
        seriesKey: "pool_pump_power_w",
        rowKey: "home_load_pool_pump",
        scope: t("samplingTotalsScopeOverall"),
        layout: "single",
        samples: combinedHasData ? Number(combinedUsage?.samples || 0) : 0,
        rowLabel: t("samplingOverallMetricHomeLoadPoolPump"),
      },
      {
        title: t("samplingTotalsScopeHomeLoad"),
        value: combinedHasData ? Number(combinedBreakdown?.home_load?.other || combinedEnergy.other_home_load || 0) : 0,
        kind: "load",
        seriesKey: "load_w",
        rowKey: "home_load_other",
        scope: t("samplingTotalsScopeOverall"),
        layout: "single",
        samples: combinedHasData ? Number(combinedUsage?.samples || 0) : 0,
        rowLabel: t("samplingOverallMetricHomeLoadOther"),
      },
      {
        title: t("samplingTotalsScopeSajBattery"),
        leftLabel: t("samplingOverallMetricBatteryCharge"),
        leftValue: sajHasData ? Number(sajEnergy.battery_charge || 0) : 0,
        leftKind: "charge",
        leftSeriesKey: "saj_battery_charge_w",
        rightLabel: t("samplingOverallMetricBatteryDischarge"),
        rightValue: sajHasData ? Number(sajEnergy.battery_discharge || 0) : 0,
        rightKind: "discharge",
        rightSeriesKey: "saj_battery_discharge_w",
        scope: t("samplingTotalsScopeOverall"),
        layout: "dual",
        showGroupHeader: true,
        samples: sajHasData ? Number(sajUsage?.samples || 0) : 0,
      },
      {
        title: t("samplingTotalsScopeSolplanetBattery"),
        leftLabel: t("samplingOverallMetricBatteryCharge"),
        leftValue: solplanetHasData ? Number(solplanetEnergy.battery_charge || 0) : 0,
        leftKind: "charge",
        leftSeriesKey: "solplanet_battery_charge_w",
        rightLabel: t("samplingOverallMetricBatteryDischarge"),
        rightValue: solplanetHasData ? Number(solplanetEnergy.battery_discharge || 0) : 0,
        rightKind: "discharge",
        rightSeriesKey: "solplanet_battery_discharge_w",
        scope: t("samplingTotalsScopeOverall"),
        layout: "dual",
        showGroupHeader: true,
        samples: solplanetHasData ? Number(solplanetUsage?.samples || 0) : 0,
      },
    ];
    return { cards };
  }

  const selectedUsage = selectedSystem === "solplanet" ? solplanetUsage : sajUsage;
  const selectedEnergy = selectedUsage?.energy_kwh || {};
  const selectedBreakdown = selectedUsage?.breakdown_kwh || {};
  const selectedHasData = Number(selectedUsage?.samples || 0) >= 2;
  const selectedLabel = formatSamplingSystemLabel(selectedSystem);
  const totalGridImport = selectedHasData ? Number(selectedEnergy.grid_import || 0) : 0;
  const totalGridExport = selectedHasData ? Number(selectedEnergy.grid_export || 0) : 0;
  const gridImportBreakdown = normalizeGridWindowBreakdown(selectedBreakdown?.grid_import, totalGridImport);
  const gridExportBreakdown = normalizeGridWindowBreakdown(selectedBreakdown?.grid_export, totalGridExport);
  return {
    cards: [
      {
        title: t("samplingOverallMetricPv"),
        value: selectedHasData ? Number(selectedEnergy.solar_generation || 0) : 0,
        kind: "pv",
        seriesKey: "pv_w",
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "single",
        samples: selectedHasData ? Number(selectedUsage?.samples || 0) : 0,
      },
      {
        title: t("samplingTotalsGridTitle"),
        leftLabel: t("samplingOverallMetricGridImport"),
        leftValue: totalGridImport,
        leftKind: "import",
        leftSeriesKey: "grid_w",
        leftWindowValue: gridImportBreakdown.windowValue,
        leftWindowLabel: SAMPLING_GRID_IMPORT_WINDOW.label,
        leftWindowCostSplit: true,
        rightLabel: t("samplingOverallMetricGridExport"),
        rightValue: totalGridExport,
        rightKind: "export",
        rightSeriesKey: "grid_w",
        rightWindowValue: gridExportBreakdown.windowValue,
        rightWindowLabel: SAMPLING_GRID_EXPORT_WINDOW.label,
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "dual",
        samples: selectedHasData ? Number(selectedUsage?.samples || 0) : 0,
      },
      {
        title: t("samplingOverallMetricBatteryCharge"),
        value: selectedHasData ? Number(selectedEnergy.battery_charge || 0) : 0,
        kind: "charge",
        seriesKey: "battery_w",
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "single",
        samples: selectedHasData ? Number(selectedUsage?.samples || 0) : 0,
      },
      {
        title: t("samplingOverallMetricBatteryDischarge"),
        value: selectedHasData ? Number(selectedEnergy.battery_discharge || 0) : 0,
        kind: "discharge",
        seriesKey: "battery_w",
        scope: t("samplingTotalsScopeSystem", { system: selectedLabel }),
        layout: "single",
        samples: selectedHasData ? Number(selectedUsage?.samples || 0) : 0,
      },
    ],
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
      const ts = new SiteDate(item.sampled_at_utc || "").getTime();
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
        tesla_charge_power_w: Math.max(Number(item.tesla_charge_power_w || 0), 0),
        pool_pump_power_w: Math.max(Number(item.pool_pump_power_w || 0), 0),
        load_w: Math.max(
          Number(item.load_w || 0) - Math.max(Number(item.tesla_charge_power_w || 0), 0) - Math.max(Number(item.pool_pump_power_w || 0), 0),
          0,
        ),
        [`${system}_battery_charge_w`]: Math.max(-battery, 0),
        [`${system}_battery_discharge_w`]: Math.max(battery, 0),
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.ts - b.ts);
}

function deriveOverallSeriesGapMs(items, fallbackIntervalSeconds) {
  const safeFallbackMs = Math.max(Number(fallbackIntervalSeconds || 0) * 2500, 0);
  const normalized = Array.isArray(items) ? items : [];
  if (normalized.length < 2) return safeFallbackMs;
  const deltas = [];
  for (let index = 1; index < normalized.length; index += 1) {
    const dt = Number(normalized[index].ts) - Number(normalized[index - 1].ts);
    if (Number.isFinite(dt) && dt > 0) deltas.push(dt);
  }
  if (!deltas.length) return safeFallbackMs;
  deltas.sort((a, b) => a - b);
  const median = deltas[Math.floor(deltas.length / 2)];
  return Math.max(safeFallbackMs, median * 2.5);
}

function buildOverallSeriesPayload(seriesBySystem, status, range) {
  const combinedItems = normalizeOverallSeriesItems(seriesBySystem?.combined?.items, "combined");
  const sajItems = normalizeOverallSeriesItems(seriesBySystem?.saj?.items, "saj");
  const solplanetItems = normalizeOverallSeriesItems(seriesBySystem?.solplanet?.items, "solplanet");
  const tsSet = new Set([...combinedItems.map((item) => item.ts), ...sajItems.map((item) => item.ts), ...solplanetItems.map((item) => item.ts)]);
  const timestamps = Array.from(tsSet).sort((a, b) => a - b);
  const combinedGapMs = deriveOverallSeriesGapMs(combinedItems, Number(status?.saj_sample_interval_seconds || 5));
  const sajGapMs = deriveOverallSeriesGapMs(sajItems, Number(status?.saj_sample_interval_seconds || 5));
  const solplanetGapMs = deriveOverallSeriesGapMs(solplanetItems, Number(status?.solplanet_sample_interval_seconds || 60));
  let combinedIndex = 0;
  let sajIndex = 0;
  let solplanetIndex = 0;
  let latestCombined = null;
  let latestSaj = null;
  let latestSolplanet = null;

  const items = timestamps.map((ts) => {
    while (combinedIndex < combinedItems.length && combinedItems[combinedIndex].ts <= ts) {
      latestCombined = combinedItems[combinedIndex];
      combinedIndex += 1;
    }
    while (sajIndex < sajItems.length && sajItems[sajIndex].ts <= ts) {
      latestSaj = sajItems[sajIndex];
      sajIndex += 1;
    }
    while (solplanetIndex < solplanetItems.length && solplanetItems[solplanetIndex].ts <= ts) {
      latestSolplanet = solplanetItems[solplanetIndex];
      solplanetIndex += 1;
    }
    const pvTotal = pickActiveOverallValue(latestCombined, ts, combinedGapMs, "pv_total_w");
    const gridImport = pickActiveOverallValue(latestCombined, ts, combinedGapMs, "grid_import_w");
    const gridExport = pickActiveOverallValue(latestCombined, ts, combinedGapMs, "grid_export_w");
    return {
      sampled_at_utc: new SiteDate(ts).toISOString(),
      pv_total_w: pvTotal,
      grid_import_w: gridImport,
      grid_export_w: gridExport,
      tesla_charge_power_w: pickActiveOverallValue(latestCombined, ts, combinedGapMs, "tesla_charge_power_w"),
      pool_pump_power_w: pickActiveOverallValue(latestCombined, ts, combinedGapMs, "pool_pump_power_w"),
      load_w: pickActiveOverallValue(latestCombined, ts, combinedGapMs, "load_w"),
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

function buildBatteryTimelinePayload(seriesBySystem, range) {
  const systems = [
    { key: "saj_battery_soc_percent", source: "saj", labelKey: "samplingBatteryTimelineSeriesSaj" },
    { key: "solplanet_battery_soc_percent", source: "solplanet", labelKey: "samplingBatteryTimelineSeriesSolplanet" },
    {
      key: "tesla_battery_soc_percent",
      source: "combined",
      valueKey: "tesla_battery_soc_percent",
      labelKey: "samplingBatteryTimelineSeriesTesla",
    },
  ];
  const batterySocSeries = systems.map((meta) => {
    const payload = seriesBySystem?.[meta.source];
    const items = Array.isArray(payload?.items) ? payload.items : [];
    const data = items
      .map((item) => [new SiteDate(item.sampled_at_utc || "").getTime(), Number(item[meta.valueKey || "battery_soc_percent"])])
      .filter((pair) => Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
    return {
      key: meta.key,
      labelKey: meta.labelKey,
      color: getSamplingSeriesColor(meta.key),
      data,
    };
  });
  const seriesByKey = Object.fromEntries(batterySocSeries.map((item) => [item.key, item]));
  const totalBatteryUsableData = buildTotalBatteryUsableData(
    seriesByKey.saj_battery_soc_percent?.data,
    seriesByKey.solplanet_battery_soc_percent?.data,
  );
  const series = [
    ...batterySocSeries,
    {
      key: "total_battery_usable_kwh",
      labelKey: "samplingBatteryTimelineSeriesTotalRemaining",
      color: getSamplingSeriesColor("total_battery_usable_kwh"),
      data: totalBatteryUsableData,
    },
  ];
  return {
    count: batterySocSeries.reduce((sum, item) => sum + item.data.length, 0),
    start_at_utc: range.startUtc,
    end_at_utc: range.endUtc,
    series,
  };
}

function buildTeslaFreeChargePayload(combinedSeriesPayload, dayText) {
  const parts = localDateParts(dayText);
  if (!parts) return { day: dayText || "", hasData: false, charged_percent: null, used_percent: null, net_percent: null };
  const windowStart = new SiteDate(parts.year, parts.monthIndex, parts.day, SAMPLING_GRID_IMPORT_WINDOW.startHour, 0, 0, 0);
  const windowEnd = new SiteDate(parts.year, parts.monthIndex, parts.day, SAMPLING_GRID_IMPORT_WINDOW.endHour, 0, 0, 0);
  const startMs = windowStart.getTime();
  const endMs = windowEnd.getTime();
  const items = (Array.isArray(combinedSeriesPayload?.items) ? combinedSeriesPayload.items : [])
    .map((item) => {
      const ts = new SiteDate(item?.sampled_at_utc || "").getTime();
      const soc = Number(item?.tesla_battery_soc_percent);
      return { ts, soc };
    })
    .filter((item) => Number.isFinite(item.ts) && Number.isFinite(item.soc) && item.ts >= startMs && item.ts <= endMs)
    .sort((a, b) => a.ts - b.ts);
  if (items.length < 2) {
    return {
      day: dayText || "",
      start_at_utc: windowStart.toISOString(),
      end_at_utc: windowEnd.toISOString(),
      hasData: false,
      sample_count: items.length,
      charged_percent: null,
      used_percent: null,
      net_percent: null,
    };
  }
  let chargedPercent = 0;
  let usedPercent = 0;
  for (let index = 1; index < items.length; index += 1) {
    const delta = items[index].soc - items[index - 1].soc;
    if (delta > 0) chargedPercent += delta;
    else if (delta < 0) usedPercent += Math.abs(delta);
  }
  const startSample = items[0];
  const endSample = items[items.length - 1];
  return {
    day: dayText || "",
    start_at_utc: windowStart.toISOString(),
    end_at_utc: windowEnd.toISOString(),
    hasData: true,
    sample_count: items.length,
    start_soc_percent: startSample.soc,
    end_soc_percent: endSample.soc,
    start_sampled_at_utc: new SiteDate(startSample.ts).toISOString(),
    end_sampled_at_utc: new SiteDate(endSample.ts).toISOString(),
    charged_percent: chargedPercent,
    used_percent: usedPercent,
    net_percent: endSample.soc - startSample.soc,
  };
}

function getFreeGridImportTargetKwh(usageBySystem) {
  const target = Number(usageBySystem?.combined?.breakdown_kwh?.grid_import?.window);
  return Number.isFinite(target) && target >= 0 ? target : null;
}

function buildFreeEnergyUsePayload(overallSeriesPayload, dayText, targetFreeGridImportKwh = null) {
  const parts = localDateParts(dayText);
  const targetKwh = Number(targetFreeGridImportKwh);
  const normalizedTargetKwh = Number.isFinite(targetKwh) && targetKwh >= 0 ? targetKwh : null;
  if (!parts) {
    return {
      day: dayText || "",
      hasData: false,
      tesla_kwh: null,
      home_load_kwh: null,
      battery_charge_kwh: null,
      total_kwh: normalizedTargetKwh,
    };
  }
  const windowStart = new SiteDate(parts.year, parts.monthIndex, parts.day, SAMPLING_GRID_IMPORT_WINDOW.startHour, 0, 0, 0);
  const windowEnd = new SiteDate(parts.year, parts.monthIndex, parts.day, SAMPLING_GRID_IMPORT_WINDOW.endHour, 0, 0, 0);
  const startMs = windowStart.getTime();
  const endMs = windowEnd.getTime();
  const rawItems = Array.isArray(overallSeriesPayload?.items) ? overallSeriesPayload.items : [];
  const items = rawItems
    .map((item) => ({
      ts: new SiteDate(item?.sampled_at_utc || "").getTime(),
      gridImportW: Math.max(Number(item?.grid_import_w || 0), 0),
      teslaW: Math.max(Number(item?.tesla_charge_power_w || 0), 0),
      homeLoadW: Math.max(Number(item?.load_w || 0), 0) + Math.max(Number(item?.pool_pump_power_w || 0), 0),
      batteryChargeW:
        Math.max(Number(item?.saj_battery_charge_w || 0), 0) +
        Math.max(Number(item?.solplanet_battery_charge_w || 0), 0),
    }))
    .filter((item) => Number.isFinite(item.ts) && item.ts >= startMs && item.ts <= endMs)
    .sort((a, b) => a.ts - b.ts);
  if (items.length < 2) {
    return {
      day: dayText || "",
      start_at_utc: windowStart.toISOString(),
      end_at_utc: windowEnd.toISOString(),
      hasData: false,
      sample_count: items.length,
      tesla_kwh: null,
      home_load_kwh: null,
      battery_charge_kwh: null,
      total_kwh: null,
    };
  }

  const deltas = [];
  for (let index = 1; index < items.length; index += 1) {
    const dt = items[index].ts - items[index - 1].ts;
    if (Number.isFinite(dt) && dt > 0) deltas.push(dt);
  }
  deltas.sort((a, b) => a - b);
  const medianDt = deltas[Math.floor(deltas.length / 2)] || 0;
  const maxDt = Math.max(medianDt * 2.5, 0);
  let teslaKwh = 0;
  let homeLoadKwh = 0;
  let batteryChargeKwh = 0;
  let totalKwh = 0;
  for (let index = 0; index < items.length - 1; index += 1) {
    const current = items[index];
    let nextTs = Math.min(items[index + 1].ts, endMs);
    if (Number.isFinite(maxDt) && maxDt > 0) nextTs = Math.min(nextTs, current.ts + maxDt);
    const dtHours = Math.max(nextTs - Math.max(current.ts, startMs), 0) / 3600000;
    if (dtHours <= 0 || current.gridImportW <= 0) continue;
    const demandW = current.teslaW + current.homeLoadW + current.batteryChargeW;
    if (demandW <= 0) continue;
    const importKwh = (current.gridImportW * dtHours) / 1000;
    totalKwh += importKwh;
    teslaKwh += importKwh * (current.teslaW / demandW);
    homeLoadKwh += importKwh * (current.homeLoadW / demandW);
    batteryChargeKwh += importKwh * (current.batteryChargeW / demandW);
  }
  if (normalizedTargetKwh !== null && totalKwh > 0) {
    const scale = normalizedTargetKwh / totalKwh;
    teslaKwh *= scale;
    homeLoadKwh *= scale;
    batteryChargeKwh *= scale;
    totalKwh = normalizedTargetKwh;
  }
  return {
    day: dayText || "",
    start_at_utc: windowStart.toISOString(),
    end_at_utc: windowEnd.toISOString(),
    hasData: totalKwh > 0,
    sample_count: items.length,
    tesla_kwh: teslaKwh,
    home_load_kwh: homeLoadKwh,
    battery_charge_kwh: batteryChargeKwh,
    total_kwh: totalKwh,
  };
}

function formatTeslaFreeChargePercent(value, { signed = false } = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  const sign = signed && n > 0 ? "+" : "";
  return `${sign}${formatTrimmedDecimal(n, 1)}%`;
}

function formatFreeEnergyUseKwh(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${formatTrimmedDecimal(n, 2)} kWh`;
}

function renderTeslaFreeCharge(payload, freeEnergyUsePayload = stateCache.lastSamplingFreeEnergyUse) {
  const el = document.getElementById("samplingTeslaFreeCharge");
  if (!el) return;
  const day = payload?.day || "-";
  const hasData = Boolean(payload?.hasData);
  const valueText = hasData ? formatTeslaFreeChargePercent(payload.charged_percent, { signed: true }) : "-";
  const teslaKwhText = formatFreeEnergyUseKwh(freeEnergyUsePayload?.tesla_kwh);
  const startTs = new SiteDate(payload?.start_sampled_at_utc || "").getTime();
  const endTs = new SiteDate(payload?.end_sampled_at_utc || "").getTime();
  const detailText = hasData
    ? t("samplingTeslaFreeChargeDetail", {
        kwh: teslaKwhText,
        used: formatTeslaFreeChargePercent(payload.used_percent),
        net: formatTeslaFreeChargePercent(payload.net_percent, { signed: true }),
        startTime: Number.isFinite(startTs) ? formatSamplingClock(startTs, false) : "-",
        endTime: Number.isFinite(endTs) ? formatSamplingClock(endTs, false) : "-",
      })
    : t("samplingTeslaFreeChargeNoData");
  el.innerHTML = `
    <div>
      <p class="sampling-tesla-free-charge-title">${escapeHtml(t("samplingTeslaFreeChargeTitle"))}</p>
      <p class="sampling-tesla-free-charge-meta">${escapeHtml(t("samplingTeslaFreeChargeMeta", { day }))}</p>
    </div>
    <div class="sampling-tesla-free-charge-result">
      <strong>${escapeHtml(valueText)}</strong>
      <span>${escapeHtml(detailText)}</span>
    </div>
  `;
}

function renderFreeEnergyUse(payload) {
  const el = document.getElementById("samplingFreeEnergyUse");
  if (!el) return;
  const day = payload?.day || "-";
  const hasData = Boolean(payload?.hasData);
  const rows = [
    { label: t("samplingFreeEnergyUseTesla"), value: payload?.tesla_kwh },
    { label: t("samplingFreeEnergyUseHomeLoad"), value: payload?.home_load_kwh },
    { label: t("samplingFreeEnergyUseBatteryCharge"), value: payload?.battery_charge_kwh },
  ];
  el.innerHTML = `
    <div>
      <p class="sampling-free-energy-use-title">${escapeHtml(t("samplingFreeEnergyUseTitle"))}</p>
      <p class="sampling-free-energy-use-meta">${escapeHtml(t("samplingFreeEnergyUseMeta", { day }))}</p>
    </div>
    ${
      hasData
        ? `<div class="sampling-free-energy-use-list">
            ${rows
              .map(
                (row) => `
                  <div class="sampling-free-energy-use-row">
                    <span>${escapeHtml(row.label)}</span>
                    <strong>${escapeHtml(formatFreeEnergyUseKwh(row.value))}</strong>
                  </div>
                `,
              )
              .join("")}
          </div>`
        : `<p class="sampling-free-energy-use-empty">${escapeHtml(t("samplingFreeEnergyUseNoData"))}</p>`
    }
  `;
}

function getBatteryUsableKwhValue(system, socPercent) {
  const capacityKwh = BATTERY_CAPACITY_KWH[system];
  const minDischargeSoc = BATTERY_MIN_DISCHARGE_SOC[system] ?? 0;
  const soc = Number(socPercent);
  if (!Number.isFinite(capacityKwh) || !Number.isFinite(soc)) return null;
  const usableSoc = Math.max(0, Math.min(100, soc) - minDischargeSoc);
  return (capacityKwh * usableSoc) / 100;
}

function buildTotalBatteryUsableData(sajData, solplanetData) {
  const normalize = (data) =>
    (Array.isArray(data) ? data : [])
      .map(([ts, soc]) => ({ ts: Number(ts), soc: Number(soc) }))
      .filter((item) => Number.isFinite(item.ts) && Number.isFinite(item.soc))
      .sort((a, b) => a.ts - b.ts);
  const sajItems = normalize(sajData);
  const solplanetItems = normalize(solplanetData);
  const timestamps = Array.from(new Set([...sajItems.map((item) => item.ts), ...solplanetItems.map((item) => item.ts)])).sort(
    (a, b) => a - b,
  );
  const sajGapMs = deriveOverallSeriesGapMs(sajItems, 5);
  const solplanetGapMs = deriveOverallSeriesGapMs(solplanetItems, 60);
  let sajIndex = 0;
  let solplanetIndex = 0;
  let latestSaj = null;
  let latestSolplanet = null;
  const data = [];

  for (const ts of timestamps) {
    while (sajIndex < sajItems.length && sajItems[sajIndex].ts <= ts) {
      latestSaj = sajItems[sajIndex];
      sajIndex += 1;
    }
    while (solplanetIndex < solplanetItems.length && solplanetItems[solplanetIndex].ts <= ts) {
      latestSolplanet = solplanetItems[solplanetIndex];
      solplanetIndex += 1;
    }
    if (!latestSaj || !latestSolplanet || ts - latestSaj.ts > sajGapMs || ts - latestSolplanet.ts > solplanetGapMs) continue;
    const sajKwh = getBatteryUsableKwhValue("saj", latestSaj.soc);
    const solplanetKwh = getBatteryUsableKwhValue("solplanet", latestSolplanet.soc);
    if (Number.isFinite(sajKwh) && Number.isFinite(solplanetKwh)) data.push([ts, sajKwh + solplanetKwh]);
  }
  return data;
}

function findNearestSeriesSampleAt(seriesPayload, targetMs, toleranceMs = SAMPLING_PHASE_BATTERY_SAMPLE_TOLERANCE_MS) {
  if (!Number.isFinite(targetMs)) return null;
  const items = Array.isArray(seriesPayload?.items) ? seriesPayload.items : [];
  let best = null;
  let bestDelta = Infinity;
  for (const item of items) {
    const ts = new SiteDate(item?.sampled_at_utc || "").getTime();
    if (!Number.isFinite(ts)) continue;
    const delta = Math.abs(ts - targetMs);
    if (delta > toleranceMs || delta >= bestDelta) continue;
    best = item;
    bestDelta = delta;
  }
  return best;
}

function buildSamplingPhaseBatteryPayload(seriesBySystem, dayText) {
  const parts = localDateParts(dayText);
  if (!parts) return { day: dayText || "", items: [] };
  const items = SAMPLING_PHASE_BATTERY_ENDS.map((stage) => {
    const target = new SiteDate(parts.year, parts.monthIndex, parts.day, stage.endHour, 0, 0, 0);
    const targetMs = target.getTime();
    const sajSample = findNearestSeriesSampleAt(seriesBySystem?.saj, targetMs);
    const solplanetSample = findNearestSeriesSampleAt(seriesBySystem?.solplanet, targetMs);
    const sajKwh = getBatteryUsableKwhValue("saj", sajSample?.battery_soc_percent);
    const solplanetKwh = getBatteryUsableKwhValue("solplanet", solplanetSample?.battery_soc_percent);
    const values = [sajKwh, solplanetKwh].filter((value) => Number.isFinite(value));
    return {
      id: stage.id,
      titleKey: stage.titleKey,
      endHour: stage.endHour,
      endTime: `${String(stage.endHour).padStart(2, "0")}:00`,
      target_at_utc: target.toISOString(),
      total_kwh: values.length ? values.reduce((sum, value) => sum + value, 0) : null,
      saj_kwh: sajKwh,
      solplanet_kwh: solplanetKwh,
      saj_soc_percent: Number.isFinite(Number(sajSample?.battery_soc_percent)) ? Number(sajSample.battery_soc_percent) : null,
      solplanet_soc_percent: Number.isFinite(Number(solplanetSample?.battery_soc_percent)) ? Number(solplanetSample.battery_soc_percent) : null,
    };
  });
  return { day: dayText, items };
}

function normalizeSamplingPhaseBatteryPayload(rawPayload, fallbackDay = "") {
  if (!rawPayload || typeof rawPayload !== "object") return null;
  const rawItems = Array.isArray(rawPayload.items) ? rawPayload.items : [];
  if (!rawItems.length) return null;
  const stageById = new Map(SAMPLING_PHASE_BATTERY_ENDS.map((stage) => [stage.id, stage]));
  const numberOrNull = (value) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };
  const items = rawItems.map((rawItem, index) => {
    const item = rawItem && typeof rawItem === "object" ? rawItem : {};
    const stage = stageById.get(String(item.id || "")) || SAMPLING_PHASE_BATTERY_ENDS[index] || {};
    const endHour = numberOrNull(item.endHour ?? item.end_hour ?? stage.endHour);
    return {
      id: item.id || stage.id || `stage_${index}`,
      titleKey: item.titleKey || item.title_key || stage.titleKey || "",
      endHour,
      endTime: item.endTime || item.end_time || (Number.isFinite(endHour) ? `${String(endHour).padStart(2, "0")}:00` : "-"),
      target_at_local: item.target_at_local || "",
      target_at_utc: item.target_at_utc || "",
      total_kwh: numberOrNull(item.total_kwh),
      saj_kwh: numberOrNull(item.saj_kwh),
      solplanet_kwh: numberOrNull(item.solplanet_kwh),
      saj_soc_percent: numberOrNull(item.saj_soc_percent),
      solplanet_soc_percent: numberOrNull(item.solplanet_soc_percent),
      saj_sampled_at_utc: item.saj_sampled_at_utc || "",
      solplanet_sampled_at_utc: item.solplanet_sampled_at_utc || "",
    };
  });
  return {
    day: rawPayload.day || fallbackDay || "",
    timezone_name: rawPayload.timezone_name || "",
    tolerance_seconds: numberOrNull(rawPayload.tolerance_seconds),
    items,
  };
}

function formatPhaseBatteryKwh(value) {
  const n = Number(value);
  return Number.isFinite(n) ? `${formatTrimmedDecimal(n, 1)} kWh` : "-";
}

function renderSamplingPhaseBatteryRemaining(payload, rangeLabel = "") {
  const metaEl = document.getElementById("samplingPhaseBatteryMeta");
  const bodyEl = document.getElementById("samplingPhaseBatteryBody");
  if (!metaEl || !bodyEl) return;
  const day = payload?.day || rangeLabel || "-";
  metaEl.textContent = t("samplingPhaseBatteryMeta", { day });
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const hasData = items.some((item) => Number.isFinite(Number(item?.total_kwh)));
  if (!hasData) {
    bodyEl.innerHTML = `<div class="sampling-phase-battery-empty">${escapeHtml(t("samplingPhaseBatteryNoData"))}</div>`;
    return;
  }
  bodyEl.innerHTML = items
    .map((item) => {
      const hasItemData = Number.isFinite(Number(item?.total_kwh));
      const totalText = hasItemData ? formatPhaseBatteryKwh(item.total_kwh) : t("samplingPhaseBatteryUnavailable");
      const detailText = t("samplingPhaseBatteryDetail", {
        saj: formatPhaseBatteryKwh(item?.saj_kwh),
        solplanet: formatPhaseBatteryKwh(item?.solplanet_kwh),
      });
      const stageTitle = item.titleKey ? t(item.titleKey) : String(item.id || "");
      const titleText = `${stageTitle} ${t("samplingPhaseBatteryEndAt", { time: item.endTime })}\n${totalText}\n${detailText}`;
      return `
        <article class="sampling-phase-battery-item${hasItemData ? "" : " is-empty"}" title="${escapeHtml(titleText)}">
          <p class="sampling-phase-battery-stage">${escapeHtml(stageTitle)}</p>
          <p class="sampling-phase-battery-time">${escapeHtml(t("samplingPhaseBatteryEndAt", { time: item.endTime }))}</p>
          <p class="sampling-phase-battery-value">${escapeHtml(totalText)}</p>
          <p class="sampling-phase-battery-detail">${escapeHtml(detailText)}</p>
        </article>
      `;
    })
    .join("");
}

function buildPumpStatusTimelinePayload(combinedSeriesPayload, range) {
  const items = Array.isArray(combinedSeriesPayload?.items) ? combinedSeriesPayload.items : [];
  const data = items
    .map((item) => {
      const ts = new SiteDate(item.sampled_at_utc || "").getTime();
      const availableRaw = item?.pool_pump_available;
      let pumpAvailable = null;
      if (typeof availableRaw === "boolean") pumpAvailable = availableRaw;
      else if (typeof availableRaw === "number") pumpAvailable = availableRaw > 0;
      else if (typeof availableRaw === "string") {
        const normalized = availableRaw.trim().toLowerCase();
        if (["true", "1", "yes", "on"].includes(normalized)) pumpAvailable = true;
        else if (["false", "0", "no", "off"].includes(normalized)) pumpAvailable = false;
      }
      if (pumpAvailable === null) {
        const unavailableReason = String(item?.pool_pump_unavailable_reason || "").trim();
        if (unavailableReason) pumpAvailable = false;
      }
      if (pumpAvailable === false) {
        return [ts, PUMP_STATUS_UNAVAILABLE];
      }
      const power = Number(item.pool_pump_power_w || 0);
      const staleRaw = item?.pool_pump_power_stale;
      let powerStale = false;
      if (typeof staleRaw === "boolean") powerStale = staleRaw;
      else if (typeof staleRaw === "number") powerStale = staleRaw > 0;
      else if (typeof staleRaw === "string") {
        const normalized = staleRaw.trim().toLowerCase();
        if (normalized === "true" || normalized === "1" || normalized === "yes") powerStale = true;
      }
      if (!powerStale) {
        const powerLastUpdatedTs = new SiteDate(item?.pool_pump_power_last_updated || "").getTime();
        if (Number.isFinite(ts) && Number.isFinite(powerLastUpdatedTs)) {
          powerStale = (ts - powerLastUpdatedTs) > (POOL_PUMP_POWER_STALE_SECONDS * 1000);
        }
      }
      return [ts, (!powerStale && power > POWER_FLOW_ACTIVE_THRESHOLD_W) ? PUMP_STATUS_ON : PUMP_STATUS_OFF];
    })
    .filter((pair) => Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
  let dayOnMs = 0;
  let nightOnMs = 0;
  const nextBoundaryAfter = (ts) => {
    const current = new SiteDate(ts);
    const candidates = [];
    for (const offsetDays of [0, 1]) {
      const candidate = new SiteDate(current);
      candidate.setDate(candidate.getDate() + offsetDays);
      for (const hour of [11, 23]) {
        const boundary = new SiteDate(candidate);
        boundary.setHours(hour, 0, 0, 0);
        if (boundary.getTime() > ts) candidates.push(boundary.getTime());
      }
    }
    return candidates.length ? Math.min(...candidates) : ts;
  };
  const periodForTs = (ts) => {
    const hour = new SiteDate(ts).getHours();
    return hour >= 11 && hour < 23 ? "day" : "night";
  };
  const rangeStartMs = new SiteDate(range.startUtc || "").getTime();
  const rangeEndMs = new SiteDate(range.endUtc || "").getTime();
  const observedDurationMs = Number.isFinite(rangeStartMs) && Number.isFinite(rangeEndMs) && rangeEndMs > rangeStartMs
    ? rangeEndMs - rangeStartMs
    : 0;
  let startCount = 0;
  let longestOnMs = 0;
  for (let index = 0; index < data.length; index += 1) {
    const current = data[index];
    const next = data[index + 1];
    let cursor = Number(current?.[0]);
    const end = Number(next?.[0] ?? new SiteDate(range.endUtc || "").getTime());
    const isOn = Number(current?.[1]) === PUMP_STATUS_ON;
    if (!isOn || !Number.isFinite(cursor) || !Number.isFinite(end) || end <= cursor) continue;
    const previous = data[index - 1];
    if (Number(previous?.[1]) !== PUMP_STATUS_ON) startCount += 1;
    longestOnMs = Math.max(longestOnMs, end - cursor);
    while (cursor < end) {
      const boundary = nextBoundaryAfter(cursor);
      const segmentEnd = Math.min(end, boundary);
      const durationMs = Math.max(segmentEnd - cursor, 0);
      if (periodForTs(cursor) === "day") dayOnMs += durationMs;
      else nightOnMs += durationMs;
      cursor = segmentEnd;
    }
  }
  return {
    count: data.length,
    start_at_utc: range.startUtc,
    end_at_utc: range.endUtc,
    label: range.label || "",
    data,
    total_on_hours: (dayOnMs + nightOnMs) / 3600000,
    day_on_hours: dayOnMs / 3600000,
    night_on_hours: nightOnMs / 3600000,
    start_count: startCount,
    longest_on_hours: longestOnMs / 3600000,
    duty_percent: observedDurationMs > 0 ? ((dayOnMs + nightOnMs) / observedDurationMs) * 100 : null,
  };
}

function ensureSamplingChart() {
  if (samplingChart) return samplingChart;
  const canvas = document.getElementById("samplingChartCanvas");
  if (!canvas) return null;
  if (typeof window.echarts === "undefined") return null;
  samplingChart = window.echarts.init(canvas, null, { renderer: "canvas" });
  if (SAMPLING_CHART_CLICK_INTERACTION_ENABLED) {
    bindSamplingBrushGlobalHandlers();
  }
  if (!samplingChartHandlersBound) {
    if (SAMPLING_CHART_CLICK_INTERACTION_ENABLED) {
      const zr = samplingChart.getZr();
      if (zr) {
        zr.on("mousedown", (event) => {
          if (Number(event?.button ?? 0) !== 0) return;
          if (!event?.event?.shiftKey) {
            clearSamplingBrushState({ clearChart: false });
            return;
          }
          samplingBrushState.active = true;
          samplingBrushState.pendingRange = null;
          samplingChart.dispatchAction({
            type: "takeGlobalCursor",
            key: "brush",
            brushOption: {
              brushType: "rect",
              brushMode: "single",
            },
          });
        });
      }
      samplingChart.on("click", (params) => {
        if (params?.componentType !== "series") return;
        const picked = params.seriesName || params.name;
        if (!picked) return;
        setSamplingFocusSeries(picked);
      });
    }
    if (SAMPLING_CHART_LEGEND_SELECT_ENABLED) {
      samplingChart.on("legendselectchanged", (params) => {
        if (samplingLegendSyncing) return;
        const picked = params?.name;
        if (!picked) return;
        setSamplingFocusSeries(picked);
      });
    }
    samplingChart.on("restore", () => {
      samplingChartFocusSeries = null;
      if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
      if (stateCache.lastSamplingUsageBySystem) {
        const range = currentTab === "report"
          ? getDayInfo(reportCalendarState.selectedDay || getLocalDateText(-1))
          : getSamplingRange();
        const system = getSelectedSamplingSystem();
        renderSamplingTotals(stateCache.lastSamplingUsageBySystem, system, range.label);
      }
    });
    if (SAMPLING_CHART_CLICK_INTERACTION_ENABLED) {
      const applyFromAreas = (areas) => {
        stageSamplingBrushRange(areas);
      };
      samplingChart.on("brushEnd", (event) => {
        applyFromAreas(event?.areas);
      });
      samplingChart.on("brushSelected", (event) => {
        const batch = Array.isArray(event?.batch) ? event.batch : [];
        applyFromAreas(batch[0]?.areas);
      });
    }
    samplingChartHandlersBound = true;
  }
  return samplingChart;
}

function ensureSamplingBatteryTimelineChart(key) {
  if (samplingBatteryTimelineCharts[key]) return samplingBatteryTimelineCharts[key];
  const idMap = {
    combined: "samplingBatteryTimelineCombinedCanvas",
    remaining: "samplingBatteryTimelineRemainingCanvas",
    tesla: "samplingBatteryTimelineTeslaCanvas",
  };
  const canvas = document.getElementById(idMap[key] || "");
  if (!canvas) return null;
  if (typeof window.echarts === "undefined") return null;
  samplingBatteryTimelineCharts[key] = window.echarts.init(canvas, null, { renderer: "canvas" });
  return samplingBatteryTimelineCharts[key];
}

function ensureSamplingPumpStatusChart() {
  if (samplingPumpStatusChart) return samplingPumpStatusChart;
  const canvas = document.getElementById("samplingPumpStatusCanvas");
  if (!canvas) return null;
  if (typeof window.echarts === "undefined") return null;
  samplingPumpStatusChart = window.echarts.init(canvas, null, { renderer: "canvas" });
  return samplingPumpStatusChart;
}

function setSamplingChartLoading(loading) {
  const chart = ensureSamplingChart();
  if (!chart) return;
  if (loading) {
    chart.showLoading("default", {
      text: "",
      color: "#4f8a6b",
      maskColor: "rgba(246, 250, 247, 0.62)",
      lineWidth: 2,
    });
    return;
  }
  chart.hideLoading();
}

function setSamplingBatteryTimelineLoading(loading) {
  for (const key of ["combined", "remaining", "tesla"]) {
    const chart = ensureSamplingBatteryTimelineChart(key);
    if (!chart) continue;
    if (loading) {
      chart.showLoading("default", {
        text: "",
        color: "#4f8a6b",
        maskColor: "rgba(246, 250, 247, 0.62)",
        lineWidth: 2,
      });
      continue;
    }
    chart.hideLoading();
  }
}

function setSamplingPumpStatusLoading(loading) {
  const chart = ensureSamplingPumpStatusChart();
  if (!chart) return;
  if (loading) {
    chart.showLoading("default", {
      text: "",
      color: "#4f8a6b",
      maskColor: "rgba(246, 250, 247, 0.62)",
      lineWidth: 2,
    });
    return;
  }
  chart.hideLoading();
}

function resetSamplingChartHoverState(chart, { resize = false } = {}) {
  if (!chart) return;
  try {
    chart.dispatchAction({ type: "hideTip" });
    if (resize) chart.resize();
    const zr = chart.getZr?.();
    zr?.refreshHover?.();
    zr?.refreshImmediately?.();
  } catch (_err) {
    // Ignore transient chart reset issues.
  }
}

function resetSamplingChartsHoverState({ resize = false } = {}) {
  resetSamplingChartHoverState(samplingChart, { resize });
  for (const chart of Object.values(samplingBatteryTimelineCharts)) {
    resetSamplingChartHoverState(chart, { resize });
  }
  resetSamplingChartHoverState(samplingPumpStatusChart, { resize });
}

function scheduleSamplingChartsHoverReset({ resize = false } = {}) {
  if (currentTab !== "sampling" && currentTab !== "report") return;
  samplingHoverRepairNeedsResize = samplingHoverRepairNeedsResize || Boolean(resize);
  if (samplingHoverRepairRafId) return;
  samplingHoverRepairRafId = window.requestAnimationFrame(() => {
    samplingHoverRepairRafId = 0;
    const needResize = samplingHoverRepairNeedsResize;
    samplingHoverRepairNeedsResize = false;
    resetSamplingChartsHoverState({ resize: needResize });
  });
}

function getSamplingSmoothConfig() {
  const mode = document.getElementById("samplingSmoothModeSelect")?.value || "smooth";
  if (mode === "detail") {
    return { targetCount: 420, window1: 5, window2: 1, smooth: 0.35 };
  }
  return { targetCount: 180, window1: 15, window2: 11, smooth: 0.85 };
}

function buildSamplingChartLegendRows(chartSeriesMeta) {
  const availableKeys = new Set(chartSeriesMeta.map((item) => item.key));
  const isOverall = availableKeys.has("pv_total_w") || availableKeys.has("grid_import_w");
  const rows = isOverall
    ? [
        { titleKey: "samplingLegendGroupSolar", keys: ["pv_total_w"] },
        { titleKey: "samplingLegendGroupGrid", keys: ["grid_import_w", "grid_export_w"] },
        { titleKey: "samplingLegendGroupLoad", keys: ["tesla_charge_power_w", "load_w"] },
        { titleKey: "samplingLegendGroupSajBattery", keys: ["saj_battery_charge_w", "saj_battery_discharge_w"] },
        {
          titleKey: "samplingLegendGroupSolplanetBattery",
          keys: ["solplanet_battery_charge_w", "solplanet_battery_discharge_w"],
        },
      ]
    : [
        { titleKey: "samplingLegendGroupSolar", keys: ["pv_w"] },
        { titleKey: "samplingLegendGroupGrid", keys: ["grid_w"] },
        { titleKey: "samplingLegendGroupBattery", keys: ["battery_w"] },
        { titleKey: "samplingLegendGroupLoad", keys: ["load_w"] },
      ];
  return rows
    .map((row) => ({ ...row, keys: row.keys.filter((key) => availableKeys.has(key)) }))
    .filter((row) => row.keys.length > 0);
}

function formatSamplingChartLegendLabel(seriesKey, chartSeriesMeta) {
  const isOverall = chartSeriesMeta.some((item) => item.key === "pv_total_w" || item.key === "grid_import_w");
  const compactLabelKey = isOverall ? SAMPLING_POWER_LEGEND_LABEL_KEYS[seriesKey] : "";
  if (compactLabelKey) return t(compactLabelKey);
  const hit = chartSeriesMeta.find((item) => item.key === seriesKey);
  return hit ? t(hit.labelKey) : String(seriesKey);
}

function buildSamplingChartLegendLayout(chartSeriesMeta, legendSelected) {
  const rows = buildSamplingChartLegendRows(chartSeriesMeta);
  const legendLeft = 14;
  const legendTop = 48;
  const legendWidth = 134;
  const gridLeft = 182;
  const groupTitleHeight = 17;
  const legendItemRowHeight = 19;
  const groupGap = 9;
  let cursorTop = legendTop;
  const legend = [];
  const graphic = [];
  for (const row of rows) {
    graphic.push({
      type: "text",
      left: legendLeft,
      top: cursorTop,
      silent: true,
      style: {
        text: t(row.titleKey),
        fill: "#20372d",
        fontSize: 12,
        fontWeight: 800,
      },
    });
    cursorTop += groupTitleHeight;
    legend.push({
      type: "plain",
      top: cursorTop,
      left: legendLeft,
      width: legendWidth,
      height: row.keys.length * legendItemRowHeight,
      orient: "vertical",
      selectedMode: SAMPLING_CHART_LEGEND_SELECT_ENABLED,
      selected: legendSelected,
      data: row.keys,
      icon: "roundRect",
      itemWidth: 12,
      itemHeight: 8,
      itemGap: 5,
      formatter: (name) => formatSamplingChartLegendLabel(name, chartSeriesMeta),
      textStyle: { color: "#56675d", fontSize: 12 },
    });
    cursorTop += row.keys.length * legendItemRowHeight + groupGap;
  }
  return {
    legend,
    graphic,
    gridLeft,
    gridBottom: 58,
  };
}

function renderSamplingChart(seriesPayload) {
  samplingChartLastPayload = seriesPayload;
  const chart = ensureSamplingChart();
  if (!chart) return;
  chart.hideLoading();
  const items = Array.isArray(seriesPayload?.items) ? seriesPayload.items : [];
  const chartSeriesMeta = normalizeSamplingFocusSeries(seriesPayload?.system || getSelectedSamplingSystem());
  const startMs = new SiteDate(seriesPayload?.start_at_utc || "").getTime();
  const endMs = new SiteDate(seriesPayload?.end_at_utc || "").getTime();
  const xMin = Number.isFinite(startMs) ? startMs : null;
  const xMax = Number.isFinite(endMs) ? endMs : null;
  const timeAxisConfig = buildSamplingTimeAxisConfig(xMin, xMax);

  const smoothCfg = getSamplingSmoothConfig();
  const preparedSeries = chartSeriesMeta.map((meta) => {
    const focused = !samplingChartFocusSeries || samplingChartFocusSeries === meta.key;
    const color = focused ? meta.color : "#b8c2bc";
    const lineOpacity = focused ? 0.95 : 0.35;
    const lineWidth = focused ? 2.8 : 1.4;
    const rawData = items
      .map((item) => [new SiteDate(item.sampled_at_utc || "").getTime(), Number(item[meta.key])])
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
  const legendSelected = Object.fromEntries(
    chartSeriesMeta.map((item) => [item.key, !samplingChartFocusSeries || samplingChartFocusSeries === item.key]),
  );
  const legendLayout = buildSamplingChartLegendLayout(chartSeriesMeta, legendSelected);
  const chartGraphic = items.length
    ? legendLayout.graphic
    : [
        ...legendLayout.graphic,
        {
          type: "text",
          left: "center",
          top: "middle",
          style: { text: t("samplingChartNoData"), fill: "#6b7f72", fontSize: 14 },
        },
      ];

  chart.setOption(
    {
      animation: false,
      grid: { left: legendLayout.gridLeft, right: 18, top: 58, bottom: legendLayout.gridBottom },
      legend: legendLayout.legend,
      graphic: chartGraphic,
      tooltip: {
        show: true,
        trigger: "axis",
        triggerOn: "mousemove",
        alwaysShowContent: false,
        enterable: false,
        axisPointer: {
          type: "cross",
          snap: true,
          animation: false,
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
        ...timeAxisConfig,
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
      brush: SAMPLING_CHART_CLICK_INTERACTION_ENABLED
        ? {
            toolbox: false,
            xAxisIndex: 0,
            brushMode: "single",
            throttleType: "debounce",
            throttleDelay: 200,
          }
        : {
            toolbox: false,
          },
      series,
    },
    true,
  );
  if (SAMPLING_CHART_CLICK_INTERACTION_ENABLED) {
    chart.dispatchAction({
      type: "takeGlobalCursor",
      key: "brush",
      brushOption: {
        brushType: false,
      },
    });
  }
}

function renderSamplingBatteryTimeline(payload) {
  samplingBatteryTimelineLastPayload = payload;
  const rangeSeries = Array.isArray(payload?.series) ? payload.series : [];
  const startMs = new SiteDate(payload?.start_at_utc || "").getTime();
  const endMs = new SiteDate(payload?.end_at_utc || "").getTime();
  const xMin = Number.isFinite(startMs) ? startMs : null;
  const xMax = Number.isFinite(endMs) ? endMs : null;
  const timeAxisConfig = buildSamplingTimeAxisConfig(xMin, xMax);
  const miniTimeAxisConfig = (() => {
    const oneHourMs = 60 * 60 * 1000;
    const durationMs = Number.isFinite(xMin) && Number.isFinite(xMax) ? Math.max(0, xMax - xMin) : 0;
    const is24HourView = durationMs >= 20 * oneHourMs && durationMs <= 28 * oneHourMs;
    if (!is24HourView) {
      return {
        splitNumber: 4,
        axisLabel: {
          color: "#6b7f72",
          hideOverlap: true,
          fontSize: 11,
          rotate: 45,
          formatter: (value) => formatSamplingClock(value, false),
        },
      };
    }
    return {
      splitNumber: 6,
      minInterval: 4 * oneHourMs,
      maxInterval: 4 * oneHourMs,
      axisLabel: {
          color: "#6b7f72",
          hideOverlap: true,
          fontSize: 11,
          rotate: 45,
          formatter: (value) => {
            const dt = new SiteDate(value);
            if (Number.isNaN(dt.getTime())) return "";
            return `${String(dt.getHours()).padStart(2, "0")}:00`;
        },
      },
    };
  })();
  const seriesByKey = Object.fromEntries(rangeSeries.map((item) => [item.key, item]));
  const pickBatteryTimelineValueAt = (data, ts) => {
    if (!Array.isArray(data) || !data.length || !Number.isFinite(ts)) return null;
    let low = 0;
    let high = data.length - 1;
    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const midTs = Number(data[mid]?.[0]);
      if (midTs === ts) return Number(data[mid]?.[1]);
      if (midTs < ts) low = mid + 1;
      else high = mid - 1;
    }
    const candidates = [];
    if (high >= 0) candidates.push(data[high]);
    if (low < data.length) candidates.push(data[low]);
    if (!candidates.length) return null;
    let best = null;
    for (const candidate of candidates) {
      const candidateTs = Number(candidate?.[0]);
      const candidateValue = Number(candidate?.[1]);
      if (!Number.isFinite(candidateTs) || !Number.isFinite(candidateValue)) continue;
      const distance = Math.abs(candidateTs - ts);
      if (!best || distance < best.distance) {
        best = { distance, value: candidateValue };
      }
    }
    return best ? best.value : null;
  };
  const renderBatteryChart = ({ chartKey, seriesKeys, unit, yAxisMax, legend = false }) => {
    const chart = ensureSamplingBatteryTimelineChart(chartKey);
    if (!chart) return;
    chart.hideLoading();
    const sources = seriesKeys.map((seriesKey) => {
      const source = seriesByKey[seriesKey] || null;
      const sampledData = downsampleByBucket(Array.isArray(source?.data) ? source.data : [], 220);
      return {
        seriesKey,
        source,
        smoothedData: movingAverage(movingAverage(sampledData, 5), 5),
        color: source?.color || getSamplingSeriesColor(seriesKey, "#6b7f72"),
      };
    });
    const series = sources
      .filter(({ smoothedData }) => smoothedData.length)
      .map(({ seriesKey, source, smoothedData, color }) => ({
          name: source ? t(source.labelKey) : seriesKey,
          type: "line",
          smooth: 0.55,
          showSymbol: false,
          connectNulls: false,
          data: smoothedData,
          lineStyle: { width: 2.8, opacity: 0.95, color, cap: "round", join: "round" },
          itemStyle: { color },
          emphasis: { focus: "series" },
          animation: false,
        }));
    chart.setOption(
      {
        animation: false,
        grid: { left: unit === "kwh" ? 52 : 46, right: 12, top: legend ? 34 : 18, bottom: 36 },
        legend: legend
          ? {
              top: 6,
              left: "center",
              itemWidth: 10,
              itemHeight: 8,
              textStyle: { color: "#52695d", fontSize: 11 },
            }
          : undefined,
        tooltip: {
          trigger: "axis",
          triggerOn: "mousemove",
          alwaysShowContent: false,
          enterable: false,
          axisPointer: {
            type: "cross",
            animation: false,
            lineStyle: { type: "dashed", color: "#7d9588" },
            crossStyle: { color: "#7d9588" },
          },
          formatter: (params) => {
            if (!Array.isArray(params) || !params.length) return "";
            const hoveredTs = Number(params[0].axisValue);
            const values = sources.map(({ seriesKey, source, smoothedData }) => {
              const value = pickBatteryTimelineValueAt(smoothedData, hoveredTs);
              const formattedValue = Number.isFinite(value)
                ? `${formatTrimmedDecimal(value, 1)}${unit === "kwh" ? " kWh" : "%"}`
                : "-";
              return `${source ? t(source.labelKey) : seriesKey}: ${formattedValue}`;
            });
            return [formatSamplingClock(hoveredTs, true), ...values].join("<br/>");
          },
        },
        xAxis: {
          type: "time",
          min: xMin,
          max: xMax,
          axisLine: { lineStyle: { color: "#adc2b5" } },
          splitLine: { lineStyle: { color: "#eef4ee" } },
          ...timeAxisConfig,
          ...miniTimeAxisConfig,
        },
        yAxis: {
          type: "value",
          min: 0,
          max: yAxisMax,
          axisLine: { lineStyle: { color: "#adc2b5" } },
          splitLine: { lineStyle: { color: "#dbe7df", type: "dashed" } },
          axisLabel: {
            color: "#6b7f72",
            formatter: (v) => (unit === "kwh" ? `${Number(v).toFixed(0)} kWh` : `${Number(v).toFixed(0)}%`),
          },
        },
        series,
        graphic: series.length
          ? []
          : [
              {
                type: "text",
                left: "center",
                top: "middle",
                style: { text: t("samplingBatteryTimelineNoData"), fill: "#6b7f72", fontSize: 12 },
              },
            ],
      },
      true,
    );
    chart.resize();
  };
  renderBatteryChart({
    chartKey: "combined",
    seriesKeys: ["saj_battery_soc_percent", "solplanet_battery_soc_percent"],
    unit: "percent",
    yAxisMax: 100,
    legend: true,
  });
  renderBatteryChart({
    chartKey: "remaining",
    seriesKeys: ["total_battery_usable_kwh"],
    unit: "kwh",
    yAxisMax: 60,
  });
  renderBatteryChart({ chartKey: "tesla", seriesKeys: ["tesla_battery_soc_percent"], unit: "percent", yAxisMax: 100 });
}

function ensureSamplingTotalsChart() {
  if (samplingTotalsChart) return samplingTotalsChart;
  const canvas = document.getElementById("samplingTotalsChartCanvas");
  if (!canvas) return null;
  samplingTotalsChart = {
    __isCustomBars: true,
    resize() {},
    showLoading() {
      const el = document.getElementById("samplingTotalsChartCanvas");
      if (el) el.classList.add("eb-loading");
    },
    hideLoading() {
      const el = document.getElementById("samplingTotalsChartCanvas");
      if (el) el.classList.remove("eb-loading");
    },
  };
  return samplingTotalsChart;
}

function setSamplingTotalsChartLoading(loading) {
  ensureSamplingTotalsChart();
  const el = document.getElementById("samplingTotalsChartCanvas");
  if (!el) return;
  if (loading) {
    el.classList.add("eb-loading");
  } else {
    el.classList.remove("eb-loading");
  }
}

function renderSamplingTotalsChartOverlay(chart, rows, hasData) {
  // No-op: overlay is now rendered inline in the HTML bars
}

function buildSamplingTotalsChartRows(cards) {
  const rows = [];
  for (const card of Array.isArray(cards) ? cards : []) {
    if (card.showGroupHeader) {
      rows.push({ type: "group-header", label: card.title });
    }
    if (card.layout === "stacked") {
      const leftVal = Math.max(Number(card.leftValue || 0), 0);
      const rightVal = Math.max(Number(card.rightValue || 0), 0);
      const totalVal = leftVal + rightVal;
      rows.push({
        type: "stacked",
        label: card.title,
        rowKey: card.rowKey || null,
        totalValue: totalVal,
        segments: [
          {
            label: card.leftLabel || "",
            value: leftVal,
            color: getSamplingTotalBaseColor(card.leftSeriesKey, "#6b7f72"),
            seriesKey: card.leftSeriesKey || null,
          },
          {
            label: card.rightLabel || "",
            value: rightVal,
            color: getSamplingTotalBaseColor(card.rightSeriesKey, "#6b7f72"),
            seriesKey: card.rightSeriesKey || null,
          },
        ],
        valueLabel:
          totalVal > 0
            ? `⚡ ${formatTrimmedDecimal(leftVal, 2)}  🔋 ${formatTrimmedDecimal(rightVal, 2)} kWh`
            : null,
        valueParts:
          totalVal > 0
            ? [
                { icon: "⚡", value: leftVal },
                { icon: "🔋", value: rightVal },
              ]
            : null,
      });
    } else if (card.layout === "dual") {
      const suffix = card.showGroupHeader
        ? ""
        : card.title === t("samplingTotalsGridTitle")
          ? ""
          : card.title === t("samplingTotalsScopeSajBattery")
            ? " (SAJ)"
            : card.title === t("samplingTotalsScopeSolplanetBattery")
              ? " (Solplanet)"
              : "";
      rows.push({
        label: `${card.leftLabel || ""}${suffix}`.trim(),
        rowKey: card.leftRowKey || null,
        value: Math.max(Number(card.leftValue || 0), 0),
        overlayValue: Math.max(Number(card.leftWindowValue || 0), 0),
        overlayLabel: card.leftWindowLabel || "",
        windowCostSplit: Boolean(card.leftWindowCostSplit),
        valueLabel:
          Number(card.leftValue || 0) > 0
            ? (card.leftWindowCostSplit
                ? formatSamplingGridImportCostSplit(card.leftWindowValue, card.leftValue)
                : card.leftWindowLabel
                ? formatSamplingWindowSplit(card.leftWindowValue, card.leftValue)
                : formatEnergyKwhText(card.leftValue))
            : null,
        valueParts:
          Number(card.leftValue || 0) > 0 && card.leftWindowCostSplit
            ? buildSamplingGridImportCostParts(card.leftWindowValue, card.leftValue)
            : Number(card.leftValue || 0) > 0 && card.leftWindowLabel
            ? [
                { icon: "⏱", value: Number(card.leftWindowValue || 0) },
                { icon: "📊", value: Number(card.leftValue || 0) },
              ]
            : null,
        kind: card.leftKind,
        seriesKey: card.leftSeriesKey || null,
      });
      rows.push({
        label: `${card.rightLabel || ""}${suffix}`.trim(),
        rowKey: card.rightRowKey || null,
        value: Math.max(Number(card.rightValue || 0), 0),
        overlayValue: Math.max(Number(card.rightWindowValue || 0), 0),
        overlayLabel: card.rightWindowLabel || "",
        windowCostSplit: Boolean(card.rightWindowCostSplit),
        valueLabel:
          Number(card.rightValue || 0) > 0
            ? (card.rightWindowCostSplit
                ? formatSamplingGridImportCostSplit(card.rightWindowValue, card.rightValue)
                : card.rightWindowLabel
                ? formatSamplingWindowSplit(card.rightWindowValue, card.rightValue)
                : formatEnergyKwhText(card.rightValue))
            : null,
        valueParts:
          Number(card.rightValue || 0) > 0 && card.rightWindowCostSplit
            ? buildSamplingGridImportCostParts(card.rightWindowValue, card.rightValue)
            : Number(card.rightValue || 0) > 0 && card.rightWindowLabel
            ? [
                { icon: "⏱", value: Number(card.rightWindowValue || 0) },
                { icon: "📊", value: Number(card.rightValue || 0) },
              ]
            : null,
        kind: card.rightKind,
        seriesKey: card.rightSeriesKey || null,
      });
    } else {
      rows.push({
        label: card.rowLabel || card.title,
        rowKey: card.rowKey || null,
        value: Math.max(Number(card.value || 0), 0),
        valueLabel:
          Number(card.value || 0) > 0
            ? formatEnergyKwhText(card.value)
            : null,
        kind: card.kind,
        seriesKey: card.seriesKey || null,
      });
    }
  }
  return rows;
}

function buildEbValueHtml(row) {
  if (row.valueParts && row.valueParts.length === 3) {
    const [free, paid, total] = row.valueParts;
    const parts = [
      formatTrimmedDecimal(Number(free.value || 0), 2),
      "+",
      formatTrimmedDecimal(Number(paid.value || 0), 2),
      "/",
      `${formatTrimmedDecimal(Number(total.value || 0), 2)} kWh`,
    ];
    return `<span class="eb-value eb-value-cost-split">${parts.map((part) => `<span>${escapeHtml(part)}</span>`).join("")}</span>`;
  }
  if (row.valueParts && row.valueParts.length === 2) {
    const [a, b] = row.valueParts;
    const compactText = `${formatTrimmedDecimal(Number(a.value || 0), 2)} / ${formatTrimmedDecimal(Number(b.value || 0), 2)} kWh`;
    return `<span class="eb-value">${escapeHtml(compactText)}</span>`;
  }
  const text = row.valueLabel || (row.totalValue != null ? `${formatTrimmedDecimal(row.totalValue, 2)} kWh` : (Number(row.value) ? `${formatTrimmedDecimal(Number(row.value), 2)} kWh` : "—"));
  return `<span class="eb-value">${escapeHtml(text)}</span>`;
}

function energyBreakdownEmojiForRow(row) {
  const label = String(row?.label || "").toLowerCase();
  const title = String(row?.title || "").toLowerCase();
  const seriesKey = String(row?.seriesKey || "").toLowerCase();
  const kind = String(row?.kind || "").toLowerCase();

  if (label.includes("pv") || title.includes("pv") || seriesKey === "pv_w" || seriesKey === "pv_total_w") return "☀️";
  if (label.includes("tesla") || seriesKey === "tesla_charge_power_w") return "🚗";
  if (label.includes("pool pump") || label.includes("水泵") || seriesKey === "pool_pump_power_w") return "💧";
  if (label.includes("home load") || label.includes("家庭用电") || label.includes("other home")) return "🏠";
  if (label.includes("grid") || title.includes("grid") || kind === "import" || kind === "export" || seriesKey === "grid_w") return "⚡";
  if (label.includes("battery") || title.includes("battery") || kind === "charge" || kind === "discharge" || seriesKey.includes("battery")) return "🔋";
  return "•";
}

function formatEnergyBreakdownLabel(row) {
  const text = String(row?.label || "").trim();
  const emoji = energyBreakdownEmojiForRow(row);
  if (!text) return emoji;
  return `${emoji} ${text}`;
}

function renderSamplingTotalsChart(cards) {
  ensureSamplingTotalsChart();
  const container = document.getElementById("samplingTotalsChartCanvas");
  if (!container) return;
  container.classList.remove("eb-loading");

  const rows = buildSamplingTotalsChartRows(cards);
  const hasData = rows.some((row) =>
    row.type === "group-header" ? false : row.type === "stacked" ? row.totalValue > 0 : Number(row.value || 0) !== 0,
  );

  if (!hasData) {
    container.innerHTML = `<div class="eb-no-data">${escapeHtml(t("samplingTotalsNoData"))}</div>`;
    return;
  }

  const maxAbs = Math.max(
    1,
    ...rows.map((row) =>
      row.type === "stacked" ? row.totalValue : Math.abs(Number(row.value || 0)),
    ),
  );

  // Track whether each row is inside a group (follows a group-header)
  const rowInGroup = rows.map((_, i) => {
    for (let j = i - 1; j >= 0; j--) {
      if (rows[j].type === "group-header") return true;
      break;
    }
    return false;
  });
  // Propagate: any row after a group-header (until the next group-header or non-grouped row) is in the group
  let inGroupState = false;
  const rowInGroupFinal = rows.map((row) => {
    if (row.type === "group-header") { inGroupState = true; return false; }
    return inGroupState;
  });

  const html = rows
    .map((row, i) => {
      if (row.type === "group-header") {
        return `<div class="eb-group-header">${escapeHtml(formatEnergyBreakdownLabel(row))}</div>`;
      }
      const inGroup = rowInGroupFinal[i];
      if (row.type === "stacked") {
        const total = row.totalValue;
        const pct = Math.min(100, (total / maxAbs) * 100);
        const rightPct = (100 - pct).toFixed(2);
        const dimmed =
          samplingChartFocusSeries &&
          !row.segments.some((s) => s.seriesKey === samplingChartFocusSeries);
        const labelHtml = escapeHtml(formatEnergyBreakdownLabel(row));
        const valueHtml = buildEbValueHtml(row);

        const titleLines = row.segments
          .map((s) => `${s.label}: ${formatEnergyKwhText(s.value)}`)
          .join("\n");
        const titleText = `${row.label}\n${titleLines}`;

        const segmentsHtml = row.segments
          .filter((s) => s.value > 0)
          .map((s) => {
            const segPct = total > 0 ? ((s.value / total) * 100).toFixed(2) : "0";
            return `<div class="eb-segment" style="width:${segPct}%;background:${s.color};"></div>`;
          })
          .join("");

        return `<div class="eb-row${dimmed ? " eb-dimmed" : ""}${inGroup ? " eb-row-grouped" : ""}" data-row-key="${escapeHtml(row.rowKey || "")}" title="${escapeHtml(titleText)}">
  <div class="eb-track">
    <div class="eb-fill eb-fill-stacked" style="width:${pct.toFixed(2)}%;">${segmentsHtml}</div>
  </div>
  <div class="eb-content eb-content-light" style="clip-path:inset(0 ${rightPct}% 0 0)">
    <span class="eb-label">${labelHtml}</span>
    ${valueHtml}
  </div>
  <div class="eb-content eb-content-dark" style="clip-path:inset(0 0 0 ${pct.toFixed(2)}%)">
    <span class="eb-label">${labelHtml}</span>
    ${valueHtml}
  </div>
</div>`;
      }

      const val = Math.max(Number(row.value || 0), 0);
      const pct = Math.min(100, (val / maxAbs) * 100);
      const overlayVal = Math.min(Math.max(Number(row.overlayValue || 0), 0), val);
      const overlayPct = overlayVal > 0 && val > 0 ? Math.min(100, (overlayVal / val) * 100) : 0;
      const dimmed = samplingChartFocusSeries && samplingChartFocusSeries !== row.seriesKey;
      const color = getSamplingTotalBaseColor(row.seriesKey, "#6b7f72");
      const overlayColor = getSamplingWindowSegmentColor(row.seriesKey, getSamplingSeriesColor(row.seriesKey, "#6b7f72"));
      const valueText = row.valueLabel || (val ? `${formatTrimmedDecimal(val, 2)} kWh` : "—");

      let titleText = `${row.label}: ${formatEnergyKwhText(val)}`;
      if (row.overlayLabel && overlayVal > 0) {
        titleText += `\n${row.overlayLabel}: ${formatEnergyKwhText(overlayVal)}`;
        titleText += `\n${t("samplingTotalsWindowOther")}: ${formatEnergyKwhText(Math.max(val - overlayVal, 0))}`;
        titleText += `\n${t("samplingTotalsWindowTotal")}: ${formatEnergyKwhText(val)}`;
      }

      const overlayHtml =
        overlayPct > 0
          ? `<div class="eb-overlay" style="width:${overlayPct.toFixed(2)}%;background:${overlayColor};"></div>`
          : "";

      const rightPct = (100 - pct).toFixed(2);
      const labelHtml = escapeHtml(formatEnergyBreakdownLabel(row));
      const valueHtml = buildEbValueHtml(row);
      const costSplitClass = row.windowCostSplit ? " eb-row-cost-split" : "";
      if (row.overlayLabel && row.windowCostSplit) {
        titleText = `${row.label}: ${formatEnergyKwhText(val)}`;
        titleText += `\n${t("samplingTotalsWindowFree")} (${row.overlayLabel}): ${formatEnergyKwhText(overlayVal)}`;
        titleText += `\n${t("samplingTotalsWindowPaid")}: ${formatEnergyKwhText(Math.max(val - overlayVal, 0))}`;
        titleText += `\n${t("samplingTotalsWindowTotal")}: ${formatEnergyKwhText(val)}`;
      }
      return `<div class="eb-row${dimmed ? " eb-dimmed" : ""}${inGroup ? " eb-row-grouped" : ""}${costSplitClass}" data-row-key="${escapeHtml(row.rowKey || "")}" data-series-key="${escapeHtml(row.seriesKey || "")}" title="${escapeHtml(titleText)}">
  <div class="eb-track">
    <div class="eb-fill" style="width:${pct.toFixed(2)}%;background:${color};">${overlayHtml}</div>
  </div>
  <div class="eb-content eb-content-light" style="clip-path:inset(0 ${rightPct}% 0 0)">
    <span class="eb-label">${labelHtml}</span>
    ${valueHtml}
  </div>
  <div class="eb-content eb-content-dark" style="clip-path:inset(0 0 0 ${pct.toFixed(2)}%)">
    <span class="eb-label">${labelHtml}</span>
    ${valueHtml}
  </div>
</div>`;
    })
    .join("");

  container.innerHTML = html;
}

function renderSamplingTotals(usageBySystem, selectedSystem, rangeLabel, options = {}) {
  normalizeSamplingFocusSeries(selectedSystem);
  const systemText = formatSamplingSystemLabel(selectedSystem);
  const summary = buildSamplingTotalsSummary(usageBySystem, selectedSystem);
  const cards = Array.isArray(summary?.cards) ? summary.cards : [];
  const hasData = cards.some((card) =>
    card.layout === "dual" ? Number(card.leftValue || 0) > 0 || Number(card.rightValue || 0) > 0 : Number(card.value || 0) > 0,
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
    return;
  }
  if (!hasData) {
    renderSamplingTotalsChart([]);
    return;
  }
  renderSamplingTotalsChart(cards);
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
      updated_at: payload?.updated_at || new SiteDate().toISOString(),
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
  setText("sajControlMeta", sajControlApiRemovedMessage());
}

async function applySajWorkingMode(modeCodeArg = null, summaryKey = "sajControlPopupWorkingModeSummary", purposeKey = "sajDebugPurposeSetMode") {
  void modeCodeArg;
  void summaryKey;
  void purposeKey;
  const errorText = sajControlApiRemovedMessage();
  setText("sajControlMeta", errorText);
  showSajActionError(errorText, []);
}

async function applySajInverterModeTarget() {
  const modeCode = Number(document.getElementById("sajInverterModeCodeInput")?.value || "0");
  await applySajWorkingMode(modeCode, "sajControlPopupInverterModeSummary", "sajDebugPurposeSetInverterModeTarget");
}

async function applySajSlot(kind) {
  void kind;
  const errorText = sajControlApiRemovedMessage();
  setText("sajControlMeta", errorText);
  showSajActionError(errorText, []);
}

async function applySajToggles() {
  const errorText = sajControlApiRemovedMessage();
  setText("sajControlMeta", errorText);
  showSajActionError(errorText, []);
}

async function applySajEnableMasksOnly() {
  const errorText = sajControlApiRemovedMessage();
  setText("sajControlMeta", errorText);
  showSajActionError(errorText, []);
}

async function applySajLimits() {
  const errorText = sajControlApiRemovedMessage();
  setText("sajControlMeta", errorText);
  showSajActionError(errorText, []);
}

async function applySajManualDischargePower(powerW) {
  const payload = await fetchJson("/api/saj/manual-discharge-power", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ power_w: powerW }),
    timeoutMs: 15000,
  });
  stateCache.lastSajControl = payload;
  return payload;
}

async function applySajManualChargePower(powerW) {
  const payload = await fetchJson("/api/saj/manual-charge-power", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ power_w: powerW }),
    timeoutMs: 15000,
  });
  stateCache.lastSajControl = payload;
  return payload;
}

async function applySajSelfConsumption() {
  const payload = await fetchJson("/api/saj/self-consumption", {
    method: "PUT",
    timeoutMs: 15000,
  });
  stateCache.lastSajControl = payload;
  return payload;
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

function renderSamplingPumpStatusTimeline(payload) {
  samplingPumpStatusLastPayload = payload;
  const chart = ensureSamplingPumpStatusChart();
  if (!chart) return;
  chart.hideLoading();
  const data = Array.isArray(payload?.data) ? payload.data : [];
  const startMs = new SiteDate(payload?.start_at_utc || "").getTime();
  const endMs = new SiteDate(payload?.end_at_utc || "").getTime();
  const xMin = Number.isFinite(startMs) ? startMs : null;
  const xMax = Number.isFinite(endMs) ? endMs : null;
  const timeAxisConfig = buildSamplingTimeAxisConfig(xMin, xMax);
  const segments = [];
  for (let index = 0; index < data.length; index += 1) {
    const current = data[index];
    const next = data[index + 1];
    const segmentStart = Number(current?.[0]);
    const segmentEnd = Number(next?.[0] ?? xMax);
    const segmentValue = Number(current?.[1]);
    if (!Number.isFinite(segmentStart) || !Number.isFinite(segmentEnd) || segmentEnd <= segmentStart) continue;
    const normalizedStatus = segmentValue === PUMP_STATUS_UNAVAILABLE
      ? PUMP_STATUS_UNAVAILABLE
      : (segmentValue === PUMP_STATUS_ON ? PUMP_STATUS_ON : PUMP_STATUS_OFF);
    segments.push([segmentStart, segmentEnd, normalizedStatus]);
  }
  chart.setOption(
    {
      animation: false,
      grid: { left: 58, right: 18, top: 24, bottom: 54 },
      tooltip: {
        trigger: "item",
        triggerOn: "mousemove",
        formatter: (params) => {
          const value = Array.isArray(params?.value) ? params.value : [];
          const segmentStart = Number(value[0]);
          const segmentEnd = Number(value[1]);
          const statusValue = Number(value[2]);
          const statusText = statusValue === PUMP_STATUS_ON
            ? t("samplingPumpStatusOn")
            : (statusValue === PUMP_STATUS_UNAVAILABLE ? t("poolPumpStateUnavailable") : t("samplingPumpStatusOff"));
          return [
            `${formatSamplingClock(segmentStart, true)} - ${formatSamplingClock(segmentEnd, true)}`,
            `${t("samplingPumpStatusTitle")}: ${statusText}`,
          ].join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        min: xMin,
        max: xMax,
        axisLine: { lineStyle: { color: "#adc2b5" } },
        splitLine: { lineStyle: { color: "#eef4ee" } },
        ...timeAxisConfig,
      },
      yAxis: {
        type: "category",
        data: [t("samplingPumpStatusTitle")],
        axisLine: { lineStyle: { color: "#adc2b5" } },
        splitLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: "#6b7f72" },
      },
      series: segments.length
        ? [
            {
              name: "pool_pump_status_band",
              type: "custom",
              data: segments,
              renderItem: (params, api) => {
                const startCoord = api.coord([api.value(0), 0]);
                const endCoord = api.coord([api.value(1), 0]);
                const categorySize = api.size([0, 1])[1];
                const rect = window.echarts.graphic.clipRectByRect(
                  {
                    x: startCoord[0],
                    y: startCoord[1] - categorySize * 0.28,
                    width: Math.max(endCoord[0] - startCoord[0], 1),
                    height: Math.max(categorySize * 0.56, 10),
                  },
                  {
                    x: params.coordSys.x,
                    y: params.coordSys.y,
                    width: params.coordSys.width,
                    height: params.coordSys.height,
                  },
                );
                if (!rect) return null;
                const statusValue = Number(api.value(2));
                const isUnavailable = statusValue === PUMP_STATUS_UNAVAILABLE;
                const isOn = statusValue === PUMP_STATUS_ON;
                return {
                  type: "rect",
                  shape: rect,
                  style: {
                    fill: isUnavailable ? "#fbbf24" : (isOn ? "#0ea5e9" : "#dbe7df"),
                    stroke: isUnavailable ? "#d97706" : (isOn ? "#0284c7" : "#c7d4cc"),
                    lineWidth: 1,
                  },
                };
              },
              animation: false,
            },
          ]
        : [],
      graphic: segments.length
        ? []
        : [
            {
              type: "text",
              left: "center",
              top: "middle",
              style: { text: t("samplingPumpStatusNoData"), fill: "#6b7f72", fontSize: 14 },
            },
          ],
    },
    true,
  );
}

function formatPumpStatusHours(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${formatTrimmedDecimal(Math.max(n, 0), 2)}h`;
}

function formatPumpStatusDuty(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return formatTrimmedDecimal(Math.max(0, Math.min(100, n)), 0);
}

function renderSamplingPumpStatusSummary(currentPayload, recentPayload = null) {
  const container = document.getElementById("samplingPumpStatusSummary");
  if (!container) return;
  const current = currentPayload && typeof currentPayload === "object" ? currentPayload : {};
  const recent = recentPayload && typeof recentPayload === "object" ? recentPayload : {};
  const recentRange = recent.label || t("samplingPumpStatusRecent48h");
  const metrics = [
    {
      label: t("samplingPumpStatusCurrentRange"),
      value: formatPumpStatusHours(current.total_on_hours),
      detail: t("samplingPumpStatusDayNightDetail", {
        dayHours: formatTrimmedDecimal(Number(current.day_on_hours || 0), 2),
        nightHours: formatTrimmedDecimal(Number(current.night_on_hours || 0), 2),
      }),
    },
    {
      label: t("samplingPumpStatusRecent48h"),
      value: formatPumpStatusHours(recent.total_on_hours),
      detail: t("samplingPumpStatusDutyDetail", { duty: formatPumpStatusDuty(recent.duty_percent) }),
    },
    {
      label: t("samplingPumpStatusStarts"),
      value: Number.isFinite(Number(recent.start_count)) ? String(Math.max(0, Math.round(Number(recent.start_count)))) : "-",
      detail: t("samplingPumpStatusStartsDetail", { range: recentRange }),
    },
    {
      label: t("samplingPumpStatusLongestRun"),
      value: formatPumpStatusHours(recent.longest_on_hours),
      detail: t("samplingPumpStatusLongestDetail", { range: recentRange }),
    },
  ];
  container.innerHTML = metrics
    .map((item) => `
      <article class="sampling-pump-status-metric">
        <p class="sampling-pump-status-metric-label">${escapeHtml(item.label)}</p>
        <p class="sampling-pump-status-metric-value">${escapeHtml(item.value)}</p>
        <p class="sampling-pump-status-metric-detail">${escapeHtml(item.detail)}</p>
      </article>
    `)
    .join("");
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

function setSolplanetPinModalVisible(visible) {
  const modal = document.getElementById("solplanetPinModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function setTeslaCurrentModalVisible(visible) {
  const modal = document.getElementById("teslaCurrentModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !visible);
}

function setTeslaCurrentModalBusy(busy) {
  const input = document.getElementById("teslaCurrentModalInput");
  const applyBtn = document.getElementById("teslaCurrentModalApplyBtn");
  const closeBtn = document.getElementById("teslaCurrentModalCloseBtn");
  if (input) input.disabled = Boolean(busy);
  if (applyBtn) applyBtn.disabled = Boolean(busy);
  if (closeBtn) closeBtn.disabled = Boolean(busy);
}

function renderTeslaCurrentModal(teslaInfo = null) {
  const minA = toFiniteNumber(teslaInfo?.minCurrentA);
  const maxA = toFiniteNumber(teslaInfo?.maxCurrentA);
  const stepA = toFiniteNumber(teslaInfo?.currentStepA) || 1;
  const fallbackValue = toFiniteNumber(teslaInfo?.configuredCurrentA) ?? toFiniteNumber(teslaInfo?.currentA) ?? minA ?? 0;
  const draftValue = teslaCurrentDraftA === null ? fallbackValue : teslaCurrentDraftA;
  const clampedValue = minA === null || maxA === null ? draftValue : Math.max(minA, Math.min(maxA, draftValue));
  const input = document.getElementById("teslaCurrentModalInput");
  if (input instanceof HTMLInputElement) {
    if (minA !== null) input.min = String(minA);
    if (maxA !== null) input.max = String(maxA);
    input.step = String(stepA);
    input.value = String(Math.round(clampedValue));
  }
  setText("teslaCurrentModalMeta", t("teslaCurrentModalIntro"));
  setText("teslaCurrentModalStatus", t("teslaCurrentModalStatusReady", { value: `${Math.round(clampedValue)}A` }));
  setTeslaCurrentModalBusy(teslaCurrentControlBusy);
}

function openTeslaCurrentModal() {
  const teslaInfo = teslaInfoFromCombinedFlow(stateCache.lastSummary?.combinedFlow || {});
  if (!teslaInfo?.controlAvailable) return;
  if (teslaCurrentDraftA === null) {
    teslaCurrentDraftA = teslaInfo?.configuredCurrentA ?? teslaInfo?.currentA ?? teslaInfo?.minCurrentA ?? null;
  }
  renderTeslaCurrentModal(teslaInfo);
  setTeslaCurrentModalVisible(true);
}

function setSolplanetPinModalBusy(busy) {
  solplanetPinModalBusy = Boolean(busy);
  const ids = [
    "solplanetPinApplyBtn",
  ];
  for (const id of ids) {
    const button = document.getElementById(id);
    if (button) button.disabled = solplanetPinModalBusy;
  }
  for (const id of [
    "solplanetPinTargetInput",
    "solplanetPoutTargetInput",
    "solplanetChargeStartInput",
    "solplanetChargeEndInput",
    "solplanetDischargeStartInput",
    "solplanetDischargeEndInput",
  ]) {
    const input = document.getElementById(id);
    if (input) input.disabled = solplanetPinModalBusy;
  }
}

function solplanetEncodeSlot(hour, minute, durationMinutes, mode) {
  return ((((hour & 0xff) << 24) | ((minute & 0xff) << 16) | ((durationMinutes & 0xff) << 8) | (mode & 0xff)) >>> 0);
}

function solplanetParseTimeInput(id, fallback = "00:00") {
  const raw = String(document.getElementById(id)?.value || fallback).trim();
  const match = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return solplanetParseTimeText(fallback);
  return {
    hour: Math.max(0, Math.min(23, Number(match[1]))),
    minute: Math.max(0, Math.min(59, Number(match[2]))),
  };
}

function solplanetParseTimeText(value, fallback = "00:00") {
  const raw = String(value || fallback).trim();
  const match = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return { hour: 0, minute: 0 };
  return {
    hour: Math.max(0, Math.min(23, Number(match[1]))),
    minute: Math.max(0, Math.min(59, Number(match[2]))),
  };
}

function solplanetDurationMinutes(start, end) {
  const startMinutes = (start.hour * 60) + start.minute;
  const endMinutes = (end.hour * 60) + end.minute;
  const duration = (endMinutes - startMinutes + (24 * 60)) % (24 * 60);
  return Math.max(0, Math.min(255, duration));
}

function solplanetSetInputValue(id, value) {
  const input = document.getElementById(id);
  if (input) input.value = String(value);
}

function renderSolplanetPinModalState(controlPayload = null) {
  const state = controlPayload?.control_state || controlPayload?.state || null;
  const currentPin = Number(state?.limits?.pin);
  const currentPout = Number(state?.limits?.pout);
  if (Number.isFinite(currentPin)) solplanetSetInputValue("solplanetPinTargetInput", Math.round(currentPin));
  if (Number.isFinite(currentPout)) solplanetSetInputValue("solplanetPoutTargetInput", Math.round(currentPout));

  const dayState = state?.days?.Mon || Object.values(state?.days || {})[0] || {};
  const decoded = Array.isArray(dayState?.decoded_slots) ? dayState.decoded_slots : [];
  const chargeSlot = decoded.find((item) => Number(item?.mode) === 2) || decoded[0] || {};
  const dischargeSlot = decoded.find((item) => Number(item?.mode) === 3) || decoded[1] || {};
  if (chargeSlot?.time_text) solplanetSetInputValue("solplanetChargeStartInput", chargeSlot.time_text);
  if (chargeSlot?.end_time_text) solplanetSetInputValue("solplanetChargeEndInput", chargeSlot.end_time_text);
  if (dischargeSlot?.time_text) solplanetSetInputValue("solplanetDischargeStartInput", dischargeSlot.time_text);
  if (dischargeSlot?.end_time_text) solplanetSetInputValue("solplanetDischargeEndInput", dischargeSlot.end_time_text);

  setText("solplanetPinModalStatus", t("solplanetDashboardPinReady"));
}

async function openSolplanetPinModal() {
  setSolplanetPinModalVisible(true);
  setSolplanetPinModalBusy(true);
  setText("solplanetPinModalStatus", t("solplanetDashboardPinLoading"));
  try {
    const payload = await fetchJson("/api/solplanet/control/state", { timeoutMs: 15000 });
    stateCache.lastSolplanetControl = payload;
    renderSolplanetPinModalState(payload);
  } catch (err) {
    setText("solplanetPinModalStatus", t("solplanetControlLoadFailed", { error: String(err) }));
  } finally {
    setSolplanetPinModalBusy(false);
  }
}

async function applySolplanetDashboardPin() {
  if (solplanetPinModalBusy) return;
  const pin = Math.max(0, Math.min(20000, Math.trunc(Number(document.getElementById("solplanetPinTargetInput")?.value || "0"))));
  const pout = Math.max(0, Math.min(20000, Math.trunc(Number(document.getElementById("solplanetPoutTargetInput")?.value || "0"))));
  const chargeStart = solplanetParseTimeInput("solplanetChargeStartInput", "11:00");
  const chargeEnd = solplanetParseTimeInput("solplanetChargeEndInput", "15:00");
  const dischargeStart = solplanetParseTimeInput("solplanetDischargeStartInput", "18:00");
  const dischargeEnd = solplanetParseTimeInput("solplanetDischargeEndInput", "21:00");
  const chargeDuration = solplanetDurationMinutes(chargeStart, chargeEnd);
  const dischargeDuration = solplanetDurationMinutes(dischargeStart, dischargeEnd);
  setSolplanetPinModalBusy(true);
  setText("solplanetPinModalStatus", t("solplanetDashboardPinApplying"));
  try {
    let payload = await fetchJson("/api/solplanet/control/limits", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pin, pout }),
      timeoutMs: 45000,
    });
    const state = payload?.state || payload?.control_state || stateCache.lastSolplanetControl?.state || stateCache.lastSolplanetControl?.control_state || {};
    for (const day of SOLPLANET_SCHEDULE_DAYS) {
      const existingSlots = Array.isArray(state?.days?.[day]?.encoded_slots)
        ? [...state.days[day].encoded_slots]
        : [0, 0, 0, 0, 0, 0];
      while (existingSlots.length < 6) existingSlots.push(0);
      existingSlots[0] = solplanetEncodeSlot(chargeStart.hour, chargeStart.minute, chargeDuration, 2);
      existingSlots[1] = solplanetEncodeSlot(dischargeStart.hour, dischargeStart.minute, dischargeDuration, 3);
      payload = await fetchJson(`/api/solplanet/control/day-schedule/${day}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ slots: existingSlots.slice(0, 6) }),
        timeoutMs: 45000,
      });
    }
    stateCache.lastSolplanetControl = payload;
    if (payload?.state) {
      renderSolplanetPinModalState(payload);
    }
    const readbackError = String(payload?.readback_error || "").trim();
    setText(
      "solplanetPinModalStatus",
      readbackError
        ? `${t("solplanetDashboardPinApplied", { pin, pout })} (${readbackError})`
        : t("solplanetDashboardPinApplied", { pin, pout }),
    );
    void loadSummary();
  } catch (err) {
    setText("solplanetPinModalStatus", t("solplanetControlApplyFailed", { error: String(err) }));
  } finally {
    setSolplanetPinModalBusy(false);
  }
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
      requested_at: new SiteDate().toISOString(),
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
  clearTeslaControlFeedback();
  setTeslaControlFeedback("requesting", targetEnabled);
  renderTeslaControlButton(teslaInfo);
  try {
    await fetchJson("/api/tesla/control/charging", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: targetEnabled }),
      timeoutMs: 10000,
    });
    setTeslaControlFeedback("awaiting_vehicle", targetEnabled);
    await waitForTeslaControlConfirmation(targetEnabled);
  } catch (err) {
    setTeslaControlFeedback("failed", targetEnabled);
    scheduleTeslaControlFeedbackClear(6000);
    window.alert(t("teslaControlApplyFailed", { error: String(err) }));
  } finally {
    teslaControlBusy = false;
    renderTeslaControlButton(teslaInfoFromCombinedFlow(stateCache.lastSummary?.combinedFlow || {}));
  }
}

async function applyTeslaChargeCurrent() {
  if (teslaControlBusy || teslaCurrentControlBusy) return;
  const teslaInfo = teslaInfoFromCombinedFlow(stateCache.lastSummary?.combinedFlow || {});
  const minA = toFiniteNumber(teslaInfo?.minCurrentA);
  const maxA = toFiniteNumber(teslaInfo?.maxCurrentA);
  const fallbackValue = toFiniteNumber(teslaInfo?.configuredCurrentA) ?? toFiniteNumber(teslaInfo?.currentA);
  const requested = teslaCurrentDraftA ?? fallbackValue;
  if (requested === null || minA === null || maxA === null) return;
  const amps = Math.round(Math.max(minA, Math.min(maxA, requested)));
  teslaPendingCurrentSourceA = fallbackValue ?? amps;
  teslaPendingCurrentTargetA = amps;
  teslaCurrentControlBusy = true;
  renderTeslaCurrentControl(teslaInfo);
  renderTeslaControlButton(teslaInfo);
  renderTeslaCurrentModal(teslaInfo);
  try {
    await fetchJson("/api/tesla/control/current", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ amps }),
      timeoutMs: 12000,
    });
    teslaCurrentDraftA = amps;
    await loadSummary();
    setTeslaCurrentModalVisible(false);
  } catch (err) {
    teslaPendingCurrentSourceA = null;
    teslaPendingCurrentTargetA = null;
    setText("teslaCurrentModalStatus", t("teslaCurrentApplyFailed", { error: String(err) }));
    window.alert(t("teslaCurrentApplyFailed", { error: String(err) }));
  } finally {
    teslaCurrentControlBusy = false;
    const updatedTeslaInfo = teslaInfoFromCombinedFlow(stateCache.lastSummary?.combinedFlow || {});
    renderTeslaCurrentControl(updatedTeslaInfo);
    renderTeslaControlButton(updatedTeslaInfo);
    renderTeslaCurrentModal(updatedTeslaInfo);
  }
}

// ---------------------------------------------------------------------------
// Weather forecast widget — hourly scroll strip
// ---------------------------------------------------------------------------

function _solarBarColor(w) {
  // 0 W/m² = grey, 200+ = orange-yellow gradient
  if (!w || w <= 0) return "#ccd7ce";
  const ratio = Math.min(1, w / 800);
  // interpolate #f9c74f → #f3722c as ratio grows
  const r = Math.round(249 + (243 - 249) * ratio);
  const g = Math.round(199 + (114 - 199) * ratio);
  const b = Math.round(79 + (44 - 79) * ratio);
  return `rgb(${r},${g},${b})`;
}

function renderWeatherHourly(hours, days) {
  const container = document.getElementById("weatherHourlyScroll");
  if (!container) return;
  container.innerHTML = "";
  container.scrollLeft = 0;

  // Build day-label map for separator rendering
  const dayLabels = {};
  days.forEach((d) => {
    if (d.is_past) dayLabels[d.date] = t("weatherYesterday");
    else if (!dayLabels[d.date]) {
      // first non-past = today, second = tomorrow
      const nonPast = days.filter((x) => !x.is_past);
      if (d.date === nonPast[0]?.date) dayLabels[d.date] = t("weatherToday");
      else dayLabels[d.date] = t("weatherTomorrow");
    }
  });

  let lastDate = null;
  const maxSolar = Math.max(1, ...hours.map((h) => h.solar_w_m2 || 0));
  let nowChip = null;

  hours.forEach((hour) => {
    // Day separator pill
    if (hour.date && hour.date !== lastDate) {
      lastDate = hour.date;
      const sep = document.createElement("div");
      sep.className = "weather-day-sep" + (hour.is_past ? " weather-day-sep-past" : "");
      sep.textContent = dayLabels[hour.date] || hour.date;
      container.appendChild(sep);
    }

    const isPast = Boolean(hour.is_past);
    const isNow = Boolean(hour.is_now);
    let cls = "weather-hour-chip";
    if (isNow) cls += " weather-hour-now";
    else if (isPast) cls += " weather-hour-past";

    const chip = document.createElement("div");
    chip.className = cls;

    const solar = hour.solar_w_m2 ?? 0;
    const barH = Math.round((solar / maxSolar) * 28);
    const barColor = _solarBarColor(solar);
    const tempStr = hour.temp != null ? `${hour.temp}°` : "-";
    const solarStr = solar > 0 ? `${solar}W` : "—";

    chip.innerHTML = `
      <span class="wh-time">${hour.hour_label}</span>
      <span class="wh-icon">${hour.icon}</span>
      <span class="wh-temp">${tempStr}</span>
      <span class="wh-solar-bar-wrap">
        <span class="wh-solar-bar" style="height:${barH}px;background:${barColor}"></span>
      </span>
      <span class="wh-solar-val">${solarStr}</span>
    `;
    container.appendChild(chip);
    if (isNow) nowChip = chip;
  });

  // Keep the current-hour chip centered after each render/refresh.
  if (nowChip) {
    requestAnimationFrame(() => {
      nowChip.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
        inline: "center",
      });
    });
  }
}

async function loadWeatherForecast() {
  const errorEl = document.getElementById("weatherError");
  const widgetEl = document.getElementById("weatherWidget");
  if (isMobileDashboardLayout()) {
    if (widgetEl) widgetEl.classList.add("hidden");
    return;
  }
  if (widgetEl) widgetEl.classList.remove("hidden");
  if (errorEl) errorEl.classList.add("hidden");
  try {
    const data = await fetchJson("/api/weather/forecast", { timeoutMs: 12000 });
    renderWeatherHourly(data.hours || [], data.days || []);
    const fetched = data.fetched_at ? new SiteDate(data.fetched_at).toLocaleTimeString() : "-";
    setText("weatherUpdatedAt", fetched);
  } catch (err) {
    if (errorEl) {
      errorEl.textContent = t("weatherLoadError", { error: String(err.message || err) });
      errorEl.classList.remove("hidden");
    }
  }
}

async function loadHomePlan() {
  const planMeta = document.getElementById("planMeta");
  if (planMeta) planMeta.textContent = t("planNoData");
  try {
    const payload = await fetchJson("/api/home-plan/electricity-plans", { timeoutMs: 6000 });
    stateCache.lastHomePlan = payload;
    renderHomePlan(payload);
  } catch (err) {
    stateCache.lastHomePlan = null;
    setText("planMeta", t("planLoadFailed", { error: String(err) }));
    renderHomePlan(null);
  }
}

async function loadSummary() {
  const requestId = ++summaryRequestId;
  const summary = stateCache.lastSummary || {
    combinedFlow: { metrics: {} },
    collectorStatus: null,
    poolPump: null,
  };
  stateCache.lastSummary = summary;
  const cachedCombinedFlow = summary?.combinedFlow || { metrics: {} };
  const hasCachedCombined = hasCombinedSnapshot(cachedCombinedFlow);
  setSystemLoadMeta("combined", {
    phase: hasCachedCombined ? "done" : "loading",
    updatedAt: cachedCombinedFlow?.updated_at || null,
    refreshing: hasCachedCombined,
  });
  renderSummary(summary);
  renderSajProfilePanel();
  const previousCombinedFlow = summary.combinedFlow;
  const combinedRequest = fetchJson("/api/energy-flow/combined", { timeoutMs: 30000 });
  void loadSajProfile();

  void combinedRequest
    .then((combinedFlow) => {
      if (requestId !== summaryRequestId) return;
      summary.combinedFlow = { ...combinedFlow, __load_error: false };
      summary.poolPump = pickLatestPoolPumpDevice(summary.poolPump, combinedFlow?.pool_pump);
      setSystemLoadMeta("combined", {
        phase: "done",
        updatedAt: combinedFlow?.updated_at || new SiteDate().toISOString(),
        refreshing: false,
      });
      writeCachedDashboardSummary(summary);
      renderSummary(summary);
    })
    .catch(() => {
      if (requestId !== summaryRequestId) return;
      if (hasCombinedSnapshot(previousCombinedFlow)) {
        summary.combinedFlow = { ...previousCombinedFlow, __load_error: false };
        setSystemLoadMeta("combined", {
          phase: "done",
          updatedAt: previousCombinedFlow?.updated_at || null,
          refreshing: false,
        });
      } else {
        summary.combinedFlow = { metrics: {}, __load_error: true };
        setSystemLoadMeta("combined", {
          phase: "failed",
          updatedAt: previousCombinedFlow?.updated_at || null,
          quality: "failed",
          count: 0,
          refreshing: false,
        });
      }
      renderSummary(summary);
    });

  const baseResults = await Promise.allSettled([
    fetchJson("/api/collector/status", { timeoutMs: 6000 }),
    fetchJson("/api/dashboard/notifications", { timeoutMs: 6000 }),
  ]);
  void loadWeatherForecast();
  if (requestId !== summaryRequestId) return;

  if (baseResults[0].status === "fulfilled") {
    summary.collectorStatus = baseResults[0].value;
    stateCache.lastCollectorStatus = baseResults[0].value;
  }
  summary.dashboardNotifications = baseResults[1].status === "fulfilled" ? baseResults[1].value : null;
  renderSummary(summary);
}

async function loadNotificationMatrix() {
  const notificationSummary = { combinedFlow: { metrics: {} }, collectorStatus: null };
  stateCache.lastNotificationSummary = notificationSummary;
  renderNotificationMatrix();

  const [collectorResult, timeWindowResult, combinedResult] = await Promise.allSettled([
    fetchJson("/api/collector/status", { timeoutMs: 6000 }),
    fetchJson("/api/time-window-rules", { timeoutMs: 8000 }),
    fetchJson("/api/energy-flow/combined", { timeoutMs: 30000 }),
  ]);

  if (collectorResult.status === "fulfilled") {
    notificationSummary.collectorStatus = collectorResult.value;
    stateCache.lastCollectorStatus = collectorResult.value;
  }
  if (timeWindowResult.status === "fulfilled") {
    stateCache.lastTimeWindowRules = timeWindowResult.value;
  }
  if (combinedResult.status === "fulfilled") {
    notificationSummary.combinedFlow = { ...combinedResult.value, __load_error: false };
  }

  stateCache.lastNotificationSummary = notificationSummary;
  renderNotificationMatrix();
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
      last_requested_at: new SiteDate().toISOString(),
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
        updated_at: response?.updated_at || new SiteDate().toISOString(),
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
        updated_at: new SiteDate().toISOString(),
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

function setDatabaseActionBusy(busy, exporting = false, importing = false, backingUp = false) {
  const exportBtn = document.getElementById("databaseExportBtn");
  const importBtn = document.getElementById("databaseImportBtn");
  const backupBtn = document.getElementById("databaseBackupNowBtn");
  if (exportBtn) {
    exportBtn.disabled = busy;
    exportBtn.textContent = exporting ? t("databaseExporting") : t("databaseExportBtn");
  }
  if (importBtn) {
    importBtn.disabled = busy;
    importBtn.textContent = importing ? t("databaseImporting") : t("databaseImportBtn");
  }
  if (backupBtn) {
    backupBtn.disabled = busy;
    backupBtn.textContent = backingUp ? t("databaseBackupRunning") : t("databaseBackupNowBtn");
  }
}

function dragEventHasFiles(event) {
  const types = event?.dataTransfer?.types;
  return Boolean(types && [...types].includes("Files"));
}

function setBillsDropActive(active) {
  const card = document.getElementById("billsCard");
  const overlay = document.getElementById("billsDropOverlay");
  if (card) card.classList.toggle("is-dragging-file", Boolean(active));
  if (overlay) {
    overlay.classList.toggle("hidden", !active);
    overlay.setAttribute("aria-hidden", active ? "false" : "true");
  }
}

function resetBillsDropState() {
  billsDropDepth = 0;
  setBillsDropActive(false);
}

async function exportDatabaseFile() {
  setDatabaseActionBusy(true, true, false);
  try {
    setDatabaseTransferState({
      title: t("databaseTransferExportTitle"),
      message: t("databaseTransferExportStarting"),
      ratio: 0.04,
      status: "loading",
    });
    startDatabaseTransferAutoProgress(0.18, 0.01, 220);
    const xhr = await xhrRequest("/api/database/export.sqlite3", {
      method: "GET",
      responseType: "blob",
      onDownloadProgress: (event, req) => {
        const totalHeader = Number(req.getResponseHeader("Content-Length") || 0);
        const total = event.lengthComputable ? event.total : totalHeader;
        if (total > 0) {
          const ratio = Math.max(0, Math.min(1, event.loaded / total));
          setDatabaseTransferState({
            title: t("databaseTransferExportTitle"),
            message: t("databaseTransferDownloaded", {
              loaded: formatBinarySize(event.loaded),
              total: formatBinarySize(total),
            }),
            ratio,
            status: "loading",
          });
          return;
        }
        setDatabaseTransferState({
          title: t("databaseTransferExportTitle"),
          message: t("databaseTransferExportStreaming"),
          ratio: Math.max(Number(databaseTransferState.ratio || 0), 0.2),
          status: "loading",
        });
        startDatabaseTransferAutoProgress(0.82, 0.015, 220);
      },
    });
    stopDatabaseTransferAutoProgress();
    if (xhr.status < 200 || xhr.status >= 300) {
      throw new Error(xhr.statusText || `HTTP ${xhr.status}`);
    }
    const blob = xhr.response;
    setDatabaseTransferState({
      title: t("databaseTransferExportTitle"),
      message: t("databaseTransferExportFinishing"),
      ratio: 1,
      status: "done",
    });
    const filenameMatch = (xhr.getResponseHeader("Content-Disposition") || "").match(/filename=\"([^\"]+)\"/);
    const filename = filenameMatch?.[1] || "wattimize.sqlite3";
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    window.setTimeout(() => {
      clearDatabaseTransferState();
    }, 1200);
  } catch (err) {
    stopDatabaseTransferAutoProgress();
    setDatabaseTransferState({
      title: t("databaseTransferExportTitle"),
      message: String(err),
      percentText: t("databaseTransferFailed"),
      ratio: 1,
      status: "failed",
    });
    window.alert(t("databaseExportFailed", { error: String(err) }));
  } finally {
    setDatabaseActionBusy(false, false, false);
  }
}

async function importDatabaseFile(file) {
  if (!file) return;
  if (!window.confirm(t("databaseImportConfirmReplace"))) return;
  setDatabaseActionBusy(true, false, true);
  try {
    const formData = new FormData();
    formData.append("file", file);
    setDatabaseTransferState({
      title: t("databaseTransferImportTitle"),
      message: t("databaseTransferImportUploading"),
      ratio: 0.02,
      status: "loading",
    });
    startDatabaseTransferAutoProgress(0.12, 0.008, 220);
    const xhr = await xhrRequest("/api/database/import.sqlite3", {
      method: "POST",
      body: formData,
      responseType: "text",
      onUploadProgress: (event) => {
        if (event.lengthComputable && event.total > 0) {
          const ratio = Math.max(0, Math.min(0.9, (event.loaded / event.total) * 0.9));
          setDatabaseTransferState({
            title: t("databaseTransferImportTitle"),
            message: t("databaseTransferUploaded", {
              loaded: formatBinarySize(event.loaded),
              total: formatBinarySize(event.total),
            }),
            ratio,
            status: "loading",
          });
          return;
        }
        setDatabaseTransferState({
          title: t("databaseTransferImportTitle"),
          message: t("databaseTransferImportUploading"),
          ratio: Math.max(Number(databaseTransferState.ratio || 0), 0.18),
          status: "loading",
        });
        startDatabaseTransferAutoProgress(0.78, 0.012, 200);
      },
      onUploadComplete: () => {
        stopDatabaseTransferAutoProgress();
        setDatabaseTransferState({
          title: t("databaseTransferImportTitle"),
          message: t("databaseTransferImportApplying"),
          ratio: 0.9,
          status: "loading",
        });
        startDatabaseTransferAutoProgress(0.96, 0.004, 280);
      },
    });
    if (xhr.status < 200 || xhr.status >= 300) {
      let message = xhr.responseText || `HTTP ${xhr.status}`;
      try {
        const parsed = JSON.parse(xhr.responseText || "{}");
        if (typeof parsed?.detail === "string") message = parsed.detail;
      } catch (_err) {
        // Keep raw response text.
      }
      throw new Error(message);
    }
    JSON.parse(xhr.responseText || "{}");
    stopDatabaseTransferAutoProgress();
    setDatabaseTransferState({
      title: t("databaseTransferImportTitle"),
      message: t("databaseTransferImportRefreshing"),
      ratio: 0.95,
      status: "loading",
    });
    startDatabaseTransferAutoProgress(0.98, 0.003, 240);
    databasePager.page = 1;
    samplingPager.page = 1;
    stateCache.lastDatabaseTables = null;
    stateCache.lastDatabasePage = null;
    stateCache.lastDatabaseBackups = null;
    stateCache.lastSampling = null;
    stateCache.lastSamplingSeries = null;
    stateCache.lastSamplingBatteryTimeline = null;
    stateCache.lastSamplingPhaseBattery = null;
    stateCache.lastSamplingTeslaFreeCharge = null;
    stateCache.lastSamplingFreeEnergyUse = null;
    stateCache.lastSamplingPumpStatus = null;
    stateCache.lastStorageStatus = null;
    await loadDatabase();
    await loadSampling();
    stopDatabaseTransferAutoProgress();
    setDatabaseTransferState({
      title: t("databaseTransferImportTitle"),
      message: t("databaseTransferImportDone"),
      ratio: 1,
      status: "done",
    });
    window.alert(t("databaseImportDone"));
    window.setTimeout(() => {
      clearDatabaseTransferState();
    }, 1200);
  } catch (err) {
    stopDatabaseTransferAutoProgress();
    setDatabaseTransferState({
      title: t("databaseTransferImportTitle"),
      message: String(err),
      percentText: t("databaseTransferFailed"),
      ratio: 1,
      status: "failed",
    });
    window.alert(t("databaseImportFailed", { error: String(err) }));
  } finally {
    setDatabaseActionBusy(false, false, false);
    const input = document.getElementById("databaseImportFileInput");
    if (input) input.value = "";
  }
}

async function runDatabaseBackupNow() {
  setDatabaseActionBusy(true, false, false, true);
  try {
    setDatabaseTransferState({
      title: t("databaseTransferBackupTitle"),
      message: t("databaseTransferBackupRunning"),
      ratio: 0.04,
      status: "loading",
    });
    startDatabaseTransferAutoProgress(0.92, 0.006, 300);
    const result = await fetchJson("/api/database/backups/run?force=true", {
      method: "POST",
      timeoutMs: 900000,
    });
    stopDatabaseTransferAutoProgress();
    const statusPayload = result?.backup_status || result;
    stateCache.lastDatabaseBackups = statusPayload;
    renderDatabaseBackupStatus(statusPayload);
    setDatabaseTransferState({
      title: t("databaseTransferBackupTitle"),
      message: result?.created ? t("databaseTransferBackupDone") : t("databaseBackupSkipped"),
      ratio: 1,
      status: "done",
    });
    window.alert(result?.created ? t("databaseBackupDone") : t("databaseBackupSkipped"));
    window.setTimeout(() => {
      clearDatabaseTransferState();
    }, 1200);
  } catch (err) {
    stopDatabaseTransferAutoProgress();
    setDatabaseTransferState({
      title: t("databaseTransferBackupTitle"),
      message: String(err),
      percentText: t("databaseTransferFailed"),
      ratio: 1,
      status: "failed",
    });
    window.alert(t("databaseBackupFailed", { error: String(err) }));
  } finally {
    setDatabaseActionBusy(false, false, false, false);
  }
}

async function loadDailyReport({ generateIfMissing = false } = {}) {
  const day = reportCalendarState.selectedDay || getLocalDateText();
  const today = getLocalDateText();
  if (day === today) {
    reportCalendarState.statusText = t("reportTodayLiveMeta", { day });
    setReportLoadingModal(true, reportCalendarState.statusText);
    renderReportCalendar();
    try {
      await loadSampling({ forceRaw: true, reportDay: day });
    } finally {
      setReportLoadingModal(false);
      renderReportCalendar();
    }
    return;
  }

  if (samplingLoadInFlight) return;
  samplingLoadInFlight = true;
  const range = getDayInfo(day);
  setSamplingZeroHeroCurrentVisible(false);
  setSamplingZeroHeroHistoryVisible(false);
  setSamplingSamplesSectionVisible(false);
  setSamplingRangeControlsDisabled(true);
  setSamplingTotalsChartLoading(true);
  setSamplingChartLoading(true);
  setSamplingBatteryTimelineLoading(true);
  setSamplingPumpStatusLoading(true);
  try {
    if (day > today || !isReportGeneratableDay(day)) {
      const message = t("reportNotReady");
      reportCalendarState.statusText = message;
      renderReportCalendar();
      renderDailyReportEmpty(message, range.label);
      return;
    }

    reportCalendarState.loadingDay = day;
    reportCalendarState.statusText = t("reportLoadingMeta", { day });
    setReportLoadingModal(true, reportCalendarState.statusText);
    renderReportCalendar();
    let payload = await fetchDailyReportPayload(day, { timeoutMs: 10000 });
    if (!payload && generateIfMissing) {
      reportCalendarState.statusText = t("reportGeneratingMeta", { day });
      setReportLoadingModal(true, reportCalendarState.statusText);
      renderReportCalendar();
      const generated = await fetchJson(`/api/reports/daily/generate?day_local=${encodeURIComponent(day)}`, {
        method: "POST",
        timeoutMs: 180000,
      });
      reportCalendarState.availableDays.set(day, generated || { report_day_local: day });
      payload = await fetchDailyReportPayload(day, { timeoutMs: 10000 });
    }
    if (!payload) {
      const message = t("reportUnavailable", { day });
      if (generateIfMissing) reportCalendarState.availableDays.delete(day);
      reportCalendarState.statusText = message;
      renderReportCalendar();
      renderDailyReportEmpty(message, range.label);
      return;
    }
    let displayDailyCost = payload.daily_cost || null;
    const hasDisplayDailyCost = displayDailyCost && Number.isFinite(Number(displayDailyCost.total_daily_cost_aud));
    if (!hasDisplayDailyCost) {
      try {
        displayDailyCost = await fetchDailyCostPayload(day, {
          startUtc: payload.period_start_utc || range.startUtc,
          endUtc: payload.period_end_utc || range.endUtc,
        });
      } catch (_err) {
        displayDailyCost = null;
      }
    }
    const billActualMeta = getReportBillActualMeta({
      daily_cost: displayDailyCost || null,
      bill_actual_cost: payload.bill_actual_cost || null,
    });
    reportCalendarState.availableDays.set(day, {
      report_day_local: payload.report_day_local || day,
      timezone_name: payload.timezone_name || "",
      generated_at_utc: payload.generated_at_utc || "",
      sample_count: payload.samples?.total || 0,
      report_status: payload.report_status || "",
      has_bill_actual_cost: billActualMeta.hasBillActual,
      bill_actual_invoice_number: billActualMeta.invoice,
    });
    reportCalendarState.statusText = "";
    const usageBySystem = payload.usage_by_system || {};
    const seriesBySystem = payload.series_by_system || {};
    const combinedSeries = seriesBySystem.combined || null;
    const sajSeries = seriesBySystem.saj || null;
    const solplanetSeries = seriesBySystem.solplanet || null;
    const overallSeries = buildOverallSeriesPayload(
      { combined: combinedSeries, saj: sajSeries, solplanet: solplanetSeries },
      null,
      range,
    );

    stateCache.lastSamplingUsageBySystem = usageBySystem;
    stateCache.lastSamplingDaily = combineSamplingUsageForOverall(usageBySystem);
    renderSamplingUsage(stateCache.lastSamplingDaily, range.label);
    renderSamplingTotals(usageBySystem, "overall", range.label);
    renderSamplingDailyCost(displayDailyCost || null, payload.bill_actual_cost || null);

    stateCache.lastSamplingZeroHeroHistory = payload.zero_hero_history || null;
    renderSamplingZeroHeroHistory(stateCache.lastSamplingZeroHeroHistory);

    const samples = payload.samples || { items: [], page: 1, page_size: SAMPLING_PAGE_SIZE };
    samplingPager.hasNext = Boolean(samples.has_next);
    samplingPager.hasPrev = Boolean(samples.has_prev);
    stateCache.lastSamplingPage = samples;
    renderSamplingPage(samples);

    stateCache.lastSamplingSeries = overallSeries;
    setText(
      "samplingChartMeta",
      t("samplingChartMeta", {
        system: formatSamplingSystemLabel("overall"),
        range: range.label,
        count: overallSeries.count || 0,
      }),
    );
    renderSamplingChart(overallSeries);

    const batteryTimelinePayload = buildBatteryTimelinePayload(
      { combined: combinedSeries, saj: sajSeries, solplanet: solplanetSeries },
      range,
    );
    stateCache.lastSamplingBatteryTimeline = batteryTimelinePayload;
    const phaseBatteryPayload =
      normalizeSamplingPhaseBatteryPayload(payload.phase_battery, day) ||
      buildSamplingPhaseBatteryPayload({ saj: sajSeries, solplanet: solplanetSeries }, day);
    stateCache.lastSamplingPhaseBattery = phaseBatteryPayload;
    const teslaFreeChargePayload = buildTeslaFreeChargePayload(combinedSeries, day);
    stateCache.lastSamplingTeslaFreeCharge = teslaFreeChargePayload;
    const freeEnergyUsePayload = buildFreeEnergyUsePayload(overallSeries, day, getFreeGridImportTargetKwh(usageBySystem));
    stateCache.lastSamplingFreeEnergyUse = freeEnergyUsePayload;
    setText(
      "samplingBatteryTimelineMeta",
      t("samplingBatteryTimelineMeta", { range: range.label, count: batteryTimelinePayload.count || 0 }),
    );
    renderSamplingBatteryTimeline(batteryTimelinePayload);
    renderTeslaFreeCharge(teslaFreeChargePayload, freeEnergyUsePayload);
    renderFreeEnergyUse(freeEnergyUsePayload);
    renderSamplingPhaseBatteryRemaining(phaseBatteryPayload, range.label);

    const pumpStatusPayload = buildPumpStatusTimelinePayload(combinedSeries, range);
    const recentPumpRange = getRecentPumpStatusRange(range);
    let recentPumpStatusPayload = null;
    try {
      const recentPumpSeries = await fetchJson(
        `/api/storage/series?system=combined&start_utc=${encodeURIComponent(recentPumpRange.startUtc)}&end_utc=${encodeURIComponent(recentPumpRange.endUtc)}&max_points=5000`,
        { timeoutMs: 12000 },
      );
      recentPumpStatusPayload = buildPumpStatusTimelinePayload(recentPumpSeries, recentPumpRange);
    } catch (_err) {
      recentPumpStatusPayload = null;
    }
    stateCache.lastSamplingPumpStatus = pumpStatusPayload;
    stateCache.lastSamplingRecentPumpStatus = recentPumpStatusPayload;
    setText(
      "samplingPumpStatusMeta",
      t("samplingPumpStatusMeta", {
        range: range.label,
        count: pumpStatusPayload.count || 0,
      }),
    );
    renderSamplingPumpStatusSummary(pumpStatusPayload, recentPumpStatusPayload);
    renderSamplingPumpStatusTimeline(pumpStatusPayload);
    const noDataReport = getDailyReportStatus(payload) === "no_data";
    setSamplingUpdatedAt(
      t(noDataReport ? "reportNoDataMeta" : "reportGeneratedMeta", {
        day: payload.report_day_local || day,
        time: formatSamplingDateTime(payload.generated_at_utc),
      }),
      { noDataReport },
    );
  } catch (err) {
    const message = generateIfMissing
      ? t("reportGenerateFailed", { day, error: String(err) })
      : t("loadFailed", { error: String(err) });
    reportCalendarState.statusText = message;
    renderDailyReportEmpty(message, range.label);
  } finally {
    reportCalendarState.loadingDay = "";
    renderReportCalendar();
    setReportLoadingModal(false);
    setSamplingTotalsChartLoading(false);
    setSamplingChartLoading(false);
    setSamplingBatteryTimelineLoading(false);
    setSamplingPumpStatusLoading(false);
    setSamplingRangeControlsDisabled(false);
    samplingLoadInFlight = false;
  }
}

function setReportUsageUploadBusy(busy) {
  const button = document.getElementById("reportUsageChooseBtn");
  const input = document.getElementById("reportUsageFileInput");
  if (button) button.disabled = Boolean(busy);
  if (input) input.disabled = Boolean(busy);
}

function setBillsUploadBusy(busy) {
  const button = document.getElementById("billsUploadBtn");
  const input = document.getElementById("billsFileInput");
  if (button) button.disabled = Boolean(busy);
  if (input) input.disabled = Boolean(busy);
}

async function uploadBillFile(file, { statusElementId, noFileKey, uploadingKey, uploadedKey, failedKey, afterUpload } = {}) {
  if (!file) {
    setText(statusElementId, t(noFileKey));
    return;
  }
  setText(statusElementId, t(uploadingKey));
  const formData = new FormData();
  formData.append("file", file);
  try {
    const payload = await fetchJson("/api/bills/upload", {
      method: "POST",
      body: formData,
      timeoutMs: 120000,
    });
    const invoice = payload?.bill?.invoice_number || "-";
    setText(statusElementId, t(uploadedKey, { invoice }));
    reportBillsState.loaded = false;
    stateCache.lastBills = null;
    if (typeof afterUpload === "function") await afterUpload(payload);
  } catch (err) {
    setText(statusElementId, t(failedKey, { error: formatApiErrorMessage(err) }));
  }
}

async function uploadReportUsageFile(file) {
  if (!file) {
    setText("reportUsageUploadStatus", t("reportUsageNoFile"));
    return;
  }
  setReportUsageUploadBusy(true);
  setText("reportUsageUploadStatus", t("reportUsageUploading"));
  const formData = new FormData();
  formData.append("file", file);
  try {
    const payload = await fetchJson("/api/usage/upload", {
      method: "POST",
      body: formData,
      timeoutMs: 120000,
    });
    const name = payload?.usage?.source_filename || file.name || "-";
    const updatedDays = Number(payload?.daily_report_update?.updated_daily_reports || 0);
    setText("reportUsageUploadStatus", t("reportUsageUploaded", { name, days: updatedDays }));
    await loadReportIndexForVisibleMonth({ force: true });
    await loadDailyReport({ generateIfMissing: false });
  } catch (err) {
    setText("reportUsageUploadStatus", t("reportUsageUploadFailed", { error: formatApiErrorMessage(err) }));
  } finally {
    setReportUsageUploadBusy(false);
    const input = document.getElementById("reportUsageFileInput");
    if (input) input.value = "";
  }
}

async function uploadBillsFile(file) {
  setBillsUploadBusy(true);
  try {
    await uploadBillFile(file, {
      statusElementId: "billsUploadStatus",
      noFileKey: "billsNoFile",
      uploadingKey: "billsUploading",
      uploadedKey: "billsUploaded",
      failedKey: "billsUploadFailed",
      afterUpload: async () => {
        await loadBills({ force: true });
        await loadReportBillsMenu({ force: true });
      },
    });
  } finally {
    setBillsUploadBusy(false);
    const input = document.getElementById("billsFileInput");
    if (input) input.value = "";
  }
}

async function uploadDroppedBillsFile(file) {
  if (!file) return;
  if (!window.confirm(t("billsDropConfirmUpload", { name: file.name || "-" }))) return;
  await uploadBillsFile(file);
}

async function loadReportView({ generateIfMissing = true } = {}) {
  setReportCalendarVisible(true);
  setReportCalendarMonthFromDay(reportCalendarState.selectedDay || getLocalDateText(-1));
  renderReportCalendar();
  void loadReportBillsMenu();
  await loadReportIndexForVisibleMonth();
  await loadDailyReport({ generateIfMissing });
}

async function loadSampling({ forceRaw = false, reportDay = "" } = {}) {
  if (currentTab === "report" && !forceRaw) {
    await loadDailyReport();
    return;
  }
  if (samplingLoadInFlight) return;
  samplingLoadInFlight = true;
  const system = getSelectedSamplingSystem();
  const overallMode = system === "overall";
  const normalizedReportDay = localDateParts(reportDay) ? reportDay : "";
  const reportRawMode = currentTab === "report" && forceRaw;
  const range = normalizedReportDay ? getDayInfo(normalizedReportDay) : getSamplingRange();
  setSamplingSamplesSectionVisible(!reportRawMode);
  setSamplingZeroHeroCurrentVisible(!reportRawMode);
  setSamplingZeroHeroHistoryVisible(!reportRawMode);
  if (reportRawMode) {
    setSamplingUpdatedAt(t("reportTodayLiveMeta", { day: normalizedReportDay || getLocalDateText() }));
  } else {
    renderSamplingZeroHeroWindow();
  }
  if (!reportRawMode) renderSamplingZeroHeroHistory();
  setSamplingRangeControlsDisabled(true);
  setSamplingTotalsChartLoading(true);
  setSamplingChartLoading(true);
  setSamplingBatteryTimelineLoading(true);
  setSamplingPumpStatusLoading(true);
  try {
    if (!range.startUtc || !range.endUtc || range.invalid) {
      const invalidMessage = range.invalidReason || "Invalid time range";
      stateCache.lastSamplingDaily = null;
      stateCache.lastSamplingUsageBySystem = null;
      stateCache.lastSamplingPage = null;
      stateCache.lastSamplingSeries = null;
      stateCache.lastSamplingBatteryTimeline = null;
      stateCache.lastSamplingPhaseBattery = null;
      stateCache.lastSamplingTeslaFreeCharge = null;
      stateCache.lastSamplingFreeEnergyUse = null;
      stateCache.lastSamplingPumpStatus = null;
      stateCache.lastSamplingRecentPumpStatus = null;
      setText("samplingDailyMeta", t("loadFailed", { error: invalidMessage }));
      setText("samplingChartMeta", t("loadFailed", { error: invalidMessage }));
      setText("samplingBatteryTimelineMeta", t("loadFailed", { error: invalidMessage }));
      setText("samplingPumpStatusMeta", t("loadFailed", { error: invalidMessage }));
      renderSamplingRows([]);
      renderSamplingChart({ items: [] });
      renderSamplingBatteryTimeline({ series: [] });
      renderSamplingPhaseBatteryRemaining(null, range.label);
      renderTeslaFreeCharge(null);
      renderFreeEnergyUse(null);
      renderSamplingPumpStatusSummary(null, null);
      renderSamplingPumpStatusTimeline({ data: [] });
      renderSamplingTotals(null, system, range.label, { metaText: t("loadFailed", { error: invalidMessage }) });
      renderSamplingDailyCost(null);
      return;
    }
    const sajUsageUrl =
      `/api/storage/usage-range?system=saj&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}`;
    const solplanetUsageUrl =
      `/api/storage/usage-range?system=solplanet&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}`;
    const combinedUsageUrl =
      `/api/storage/usage-range?system=combined&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}`;
    const sajSeriesUrl =
      `/api/storage/series?system=saj&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}&max_points=500`;
    const solplanetSeriesUrl =
      `/api/storage/series?system=solplanet&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}&max_points=500`;
    const combinedSeriesUrl =
      `/api/storage/series?system=combined&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}&max_points=500`;
    const selectedSeriesUrl = overallMode ? combinedSeriesUrl : system === "solplanet" ? solplanetSeriesUrl : sajSeriesUrl;
    const recentPumpRange = getRecentPumpStatusRange(range);
    const recentPumpSeriesUrl =
      `/api/storage/series?system=combined&start_utc=${encodeURIComponent(recentPumpRange.startUtc)}&end_utc=${encodeURIComponent(recentPumpRange.endUtc)}&max_points=5000`;
    const zeroHeroHistoryDay = normalizedReportDay || getSamplingFocusLocalDay();
    const zeroHeroHistoryUrl =
      `/api/storage/zero-hero-window-history?system=combined&days=1&day_local=${encodeURIComponent(zeroHeroHistoryDay)}`;
    const liveDailyCostUrl =
      `/api/storage/daily-cost?system=combined&report_day_local=${encodeURIComponent(zeroHeroHistoryDay)}&start_utc=${encodeURIComponent(range.startUtc)}&end_utc=${encodeURIComponent(range.endUtc)}`;

    const [collectorResult, statusResult, sajUsageResult, solplanetUsageResult, combinedUsageResult, samplesResult, seriesResult, overallSajSeriesResult, overallSolplanetSeriesResult, combinedPumpSeriesResult, recentPumpSeriesResult, zeroHeroHistoryResult, liveDailyCostResult] = await Promise.allSettled([
      fetchJson("/api/collector/status", { timeoutMs: 6000 }),
      fetchJson("/api/storage/status", { timeoutMs: 6000 }),
      fetchJson(sajUsageUrl, { timeoutMs: 6000 }),
      fetchJson(solplanetUsageUrl, { timeoutMs: 6000 }),
      overallMode ? fetchJson(combinedUsageUrl, { timeoutMs: 6000 }) : Promise.resolve(null),
      fetchJson(buildSamplingUrl(range), { timeoutMs: 6000 }),
      fetchJson(selectedSeriesUrl, { timeoutMs: 6000 }),
      overallMode ? fetchJson(sajSeriesUrl, { timeoutMs: 6000 }) : Promise.resolve(null),
      overallMode ? fetchJson(solplanetSeriesUrl, { timeoutMs: 6000 }) : Promise.resolve(null),
      overallMode ? Promise.resolve(null) : fetchJson(combinedSeriesUrl, { timeoutMs: 6000 }),
      fetchJson(recentPumpSeriesUrl, { timeoutMs: 12000 }),
      fetchJson(zeroHeroHistoryUrl, { timeoutMs: 12000 }),
      reportRawMode ? fetchJson(liveDailyCostUrl, { timeoutMs: 12000 }) : Promise.resolve(null),
    ]);

    if (collectorResult.status === "fulfilled") {
      stateCache.lastCollectorStatus = collectorResult.value;
      if (stateCache.lastSummary && typeof stateCache.lastSummary === "object") {
        stateCache.lastSummary.collectorStatus = collectorResult.value;
      }
    }
    if (!reportRawMode) renderSamplingZeroHeroWindow();
    if (!reportRawMode && zeroHeroHistoryResult.status === "fulfilled") {
      stateCache.lastSamplingZeroHeroHistory = zeroHeroHistoryResult.value;
      renderSamplingZeroHeroHistory(zeroHeroHistoryResult.value);
    } else if (!reportRawMode) {
      stateCache.lastSamplingZeroHeroHistory = null;
      const zeroHeroHistoryBadge = document.getElementById("samplingZeroHeroHistoryBadge");
      if (zeroHeroHistoryBadge) {
        zeroHeroHistoryBadge.className = "sampling-zero-hero-badge";
        zeroHeroHistoryBadge.textContent = "-";
      }
      setText("samplingZeroHeroHistoryMeta", t("loadFailed", { error: String(zeroHeroHistoryResult.reason) }));
      const zeroHeroHistoryBody = document.getElementById("samplingZeroHeroHistoryBody");
      if (zeroHeroHistoryBody) {
        zeroHeroHistoryBody.innerHTML = `<div class="sampling-zero-hero-empty">${escapeHtml(
          t("loadFailed", { error: String(zeroHeroHistoryResult.reason) }),
        )}</div>`;
      }
    }

    if (statusResult.status === "fulfilled") {
      stateCache.lastSamplingStatus = statusResult.value;
      renderSamplingStatus(statusResult.value);
    } else {
      setText("samplingStorageMeta", t("loadFailed", { error: String(statusResult.reason) }));
    }

    const usageBySystem = {
      combined: combinedUsageResult.status === "fulfilled" ? combinedUsageResult.value : null,
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
      const selectedUsageError =
        system === "overall"
          ? combinedUsageResult.reason
          : system === "solplanet"
            ? solplanetUsageResult.reason
            : sajUsageResult.reason;
      setText("samplingDailyMeta", t("loadFailed", { error: String(selectedUsageError) }));
    }
    renderSamplingTotals(usageBySystem, system, range.label);
    renderSamplingDailyCost(reportRawMode && liveDailyCostResult.status === "fulfilled" ? liveDailyCostResult.value : null);

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
      overallMode &&
      (seriesResult.status === "fulfilled" || overallSajSeriesResult.status === "fulfilled" || overallSolplanetSeriesResult.status === "fulfilled");
    if (seriesResult.status === "fulfilled" || overallHasAnySeries) {
      let payload = seriesResult.status === "fulfilled" ? seriesResult.value : { items: [], count: 0 };
      if (overallMode) {
        const combinedSeries = seriesResult.status === "fulfilled" ? seriesResult.value : null;
        const sajSeries = overallSajSeriesResult.status === "fulfilled" ? overallSajSeriesResult.value : null;
        const solplanetSeries = overallSolplanetSeriesResult.status === "fulfilled" ? overallSolplanetSeriesResult.value : null;
        payload = buildOverallSeriesPayload({ combined: combinedSeries, saj: sajSeries, solplanet: solplanetSeries }, stateCache.lastSamplingStatus, range);
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

    const batteryTimelinePayload = buildBatteryTimelinePayload(
      {
        combined: seriesResult.status === "fulfilled" ? seriesResult.value : null,
        saj: overallSajSeriesResult.status === "fulfilled" ? overallSajSeriesResult.value : null,
        solplanet: overallSolplanetSeriesResult.status === "fulfilled" ? overallSolplanetSeriesResult.value : null,
      },
      range,
    );
    stateCache.lastSamplingBatteryTimeline = batteryTimelinePayload;
    setText(
      "samplingBatteryTimelineMeta",
      t("samplingBatteryTimelineMeta", {
        range: range.label,
        count: batteryTimelinePayload.count || 0,
      }),
    );
    renderSamplingBatteryTimeline(batteryTimelinePayload);
    const teslaFreeChargePayload = buildTeslaFreeChargePayload(
      seriesResult.status === "fulfilled" ? seriesResult.value : null,
      normalizedReportDay || getSamplingFocusLocalDay(),
    );
    stateCache.lastSamplingTeslaFreeCharge = teslaFreeChargePayload;
    const freeEnergyUsePayload = buildFreeEnergyUsePayload(
      stateCache.lastSamplingSeries?.system === "overall" ? stateCache.lastSamplingSeries : null,
      normalizedReportDay || getSamplingFocusLocalDay(),
      overallMode ? getFreeGridImportTargetKwh(usageBySystem) : null,
    );
    stateCache.lastSamplingFreeEnergyUse = freeEnergyUsePayload;
    renderTeslaFreeCharge(teslaFreeChargePayload, freeEnergyUsePayload);
    renderFreeEnergyUse(freeEnergyUsePayload);
    const phaseBatteryPayload = buildSamplingPhaseBatteryPayload(
      {
        saj: overallSajSeriesResult.status === "fulfilled" ? overallSajSeriesResult.value : null,
        solplanet: overallSolplanetSeriesResult.status === "fulfilled" ? overallSolplanetSeriesResult.value : null,
      },
      normalizedReportDay || getSamplingFocusLocalDay(),
    );
    stateCache.lastSamplingPhaseBattery = phaseBatteryPayload;
    renderSamplingPhaseBatteryRemaining(phaseBatteryPayload, range.label);

    const combinedSeriesForPump = overallMode
      ? (seriesResult.status === "fulfilled" ? seriesResult.value : null)
      : (combinedPumpSeriesResult.status === "fulfilled" ? combinedPumpSeriesResult.value : null);
    const pumpStatusPayload = buildPumpStatusTimelinePayload(combinedSeriesForPump, range);
    const recentPumpStatusPayload = recentPumpSeriesResult.status === "fulfilled"
      ? buildPumpStatusTimelinePayload(recentPumpSeriesResult.value, recentPumpRange)
      : null;
    stateCache.lastSamplingPumpStatus = pumpStatusPayload;
    stateCache.lastSamplingRecentPumpStatus = recentPumpStatusPayload;
    setText(
      "samplingPumpStatusMeta",
      t("samplingPumpStatusMeta", {
        range: range.label,
        count: pumpStatusPayload.count || 0,
      }),
    );
    renderSamplingPumpStatusSummary(pumpStatusPayload, recentPumpStatusPayload);
    renderSamplingPumpStatusTimeline(pumpStatusPayload);
    if (reportRawMode) {
      setSamplingUpdatedAt(t("reportTodayLiveMeta", { day: normalizedReportDay || getLocalDateText() }));
    }
  } finally {
    setSamplingTotalsChartLoading(false);
    setSamplingChartLoading(false);
    setSamplingBatteryTimelineLoading(false);
    setSamplingPumpStatusLoading(false);
    setSamplingRangeControlsDisabled(false);
    samplingLoadInFlight = false;
  }
}

async function loadDatabase() {
  await loadDatabaseBackupStatus();
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
  renderSamplingZeroHeroWindow();
  renderSamplingZeroHeroHistory();
  if (stateCache.lastSamplingStatus) renderSamplingStatus(stateCache.lastSamplingStatus);
  if (stateCache.lastSamplingDaily) {
    const range = getSamplingRange();
    renderSamplingUsage(stateCache.lastSamplingDaily, range.label);
  }
  if (stateCache.lastSamplingUsageBySystem) {
    const range = getSamplingRange();
    const system = getSelectedSamplingSystem();
    renderSamplingTotals(stateCache.lastSamplingUsageBySystem, system, range.label);
  }
  if (stateCache.lastSamplingPage) renderSamplingPage(stateCache.lastSamplingPage);
  if (stateCache.lastSamplingSeries) {
    const range = getSamplingRange();
    const payload = stateCache.lastSamplingSeries;
    setText(
      "samplingChartMeta",
      t("samplingChartMeta", {
        system: formatSamplingSystemLabel(payload.system || getSelectedSamplingSystem()),
        range: range.label,
        count: payload.count || 0,
      }),
    );
    renderSamplingChart(payload);
  }
  if (stateCache.lastSamplingBatteryTimeline) {
    const range = getSamplingRange();
    setText(
      "samplingBatteryTimelineMeta",
      t("samplingBatteryTimelineMeta", {
        range: range.label,
        count: stateCache.lastSamplingBatteryTimeline.count || 0,
      }),
    );
    renderSamplingBatteryTimeline(stateCache.lastSamplingBatteryTimeline);
  }
  if (stateCache.lastSamplingPhaseBattery) {
    const range = currentTab === "report"
      ? getDayInfo(reportCalendarState.selectedDay || getLocalDateText(-1))
      : getSamplingRange();
    renderSamplingPhaseBatteryRemaining(stateCache.lastSamplingPhaseBattery, range.label);
  }
  if (stateCache.lastSamplingTeslaFreeCharge) {
    renderTeslaFreeCharge(stateCache.lastSamplingTeslaFreeCharge, stateCache.lastSamplingFreeEnergyUse);
  }
  if (stateCache.lastSamplingFreeEnergyUse) {
    renderFreeEnergyUse(stateCache.lastSamplingFreeEnergyUse);
  }
  if (stateCache.lastSamplingPumpStatus) {
    const range = getSamplingRange();
    setText(
      "samplingPumpStatusMeta",
      t("samplingPumpStatusMeta", {
        range: range.label,
        count: stateCache.lastSamplingPumpStatus.count || 0,
      }),
    );
    renderSamplingPumpStatusSummary(stateCache.lastSamplingPumpStatus, stateCache.lastSamplingRecentPumpStatus);
    renderSamplingPumpStatusTimeline(stateCache.lastSamplingPumpStatus);
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

async function loadOperationTimeline() {
  try {
    const payload = await fetchJson(`/api/operations/timeline?limit=300&lang=${encodeURIComponent(currentLang)}`, { timeoutMs: 10000 });
    renderOperationTimelinePage(payload);
  } catch (err) {
    stateCache.lastOperationTimeline = null;
    setText("operationTimelineCount", t("loadFailed", { error: String(err) }));
    setText("operationTimelineUpdatedAt", formatUpdatedAt(null));
    renderOperationTimelineRows([]);
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


async function loadTabWithGuard(tab, fromAutoRefresh = false) {
  const tabKey = ALL_TABS.includes(tab) ? tab : "dashboard";
  if (!configReady && tabKey !== "plan") return false;
  if (tabKey === "notificationMatrix" && fromAutoRefresh) return false;
  const slot = tabLoadState[tabKey];
  if (slot?.inFlight) return false;
  if (slot) slot.inFlight = true;
  try {
    if (tabKey === "plan") {
      await loadHomePlan();
      return true;
    }
    if (tabKey === "notificationMatrix") {
      await loadNotificationMatrix();
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
    if (tabKey === "report") {
      if (fromAutoRefresh && reportCalendarState.selectedDay !== getLocalDateText()) {
        return false;
      }
      await loadReportView({ generateIfMissing: !fromAutoRefresh });
      return true;
    }
    if (tabKey === "monthlyReport") {
      await loadMonthlyReportIndexForVisibleYear();
      await loadMonthlyReport();
      return true;
    }
    if (tabKey === "bills") {
      await loadBills();
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
    if (tabKey === "operationTimeline") {
      await loadOperationTimeline();
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
    if (tabKey === "notificationMatrix") {
      renderNotificationMatrix();
    }
  }
}

function runAutoRefreshRound() {
  if (!configReady) return;
  void loadTabWithGuard(currentTab, true);
}

function setAutoRefresh(seconds) {
  const safeSeconds = isMobileViewport() ? 0 : (AUTO_REFRESH_OPTIONS.includes(seconds) ? seconds : 30);
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

function forceRefreshCurrentView() {
  // Re-render the UI from cached state only, without fetching fresh data.
  applyTranslations();
  setActiveTab(currentTab, false);
  refreshMobileCombinedFlowDiagram();
  if (currentTab === "sampling" || currentTab === "report") {
    scheduleSamplingChartsHoverReset({ resize: true });
  } else if (currentTab === "monthlyReport") {
    renderMonthlyReport();
  }
}

function setActiveTab(tab, load = true) {
  const previousTab = currentTab;
  currentTab = normalizeAppTab(tab) || "dashboard";
  const samplingRangeModeSelect = document.getElementById("samplingRangeModeSelect");
  if (currentTab === "notificationMatrix" && previousTab !== "notificationMatrix") {
    resetNotificationMatrixCollapseState();
  }
  localStorage.setItem("activeTab", currentTab);
  location.hash = currentTab;

  const dashboardView = document.getElementById("dashboardView");
  const planView = document.getElementById("planView");
  const notificationMatrixView = document.getElementById("notificationMatrixView");
  const rawDataView = document.getElementById("rawDataView");
  const solplanetControlView = document.getElementById("solplanetControlView");
  const samplingView = document.getElementById("samplingView");
  const monthlyReportView = document.getElementById("monthlyReportView");
  const billsView = document.getElementById("billsView");
  const databaseView = document.getElementById("databaseView");
  const workerLogsView = document.getElementById("workerLogsView");
  const operationTimelineView = document.getElementById("operationTimelineView");
  const workerFailureLogView = document.getElementById("workerFailureLogView");
  const tabDashboard = document.getElementById("tabDashboard");
  const tabPlan = document.getElementById("tabPlan");
  const tabNotificationMatrix = document.getElementById("tabNotificationMatrix");
  const tabRawData = document.getElementById("tabRawData");
  const tabSolplanetControl = document.getElementById("tabSolplanetControl");
  const tabReport = document.getElementById("tabReport");
  const tabMonthlyReport = document.getElementById("tabMonthlyReport");
  const tabBills = document.getElementById("tabBills");
  const tabDatabase = document.getElementById("tabDatabase");
  const tabWorkerLogs = document.getElementById("tabWorkerLogs");
  const tabOperationTimeline = document.getElementById("tabOperationTimeline");
  const tabWorkerFailureLog = document.getElementById("tabWorkerFailureLog");

  const dashboardActive = currentTab === "dashboard";
  const planActive = currentTab === "plan";
  const notificationMatrixActive = currentTab === "notificationMatrix";
  const rawDataActive = currentTab === "rawData";
  const solplanetControlActive = currentTab === "solplanetControl";
  const samplingActive = currentTab === "sampling" || currentTab === "report";
  const reportActive = currentTab === "report";
  const monthlyReportActive = currentTab === "monthlyReport";
  const billsActive = currentTab === "bills";
  const databaseActive = currentTab === "database";
  const workerLogsActive = currentTab === "workerLogs";
  const operationTimelineActive = currentTab === "operationTimeline";
  const workerFailureLogActive = currentTab === "workerFailureLog";
  if (dashboardView) dashboardView.classList.toggle("hidden", !dashboardActive);
  if (planView) planView.classList.toggle("hidden", !planActive);
  if (notificationMatrixView) notificationMatrixView.classList.toggle("hidden", !notificationMatrixActive);
  if (rawDataView) rawDataView.classList.toggle("hidden", !rawDataActive);
  if (solplanetControlView) solplanetControlView.classList.toggle("hidden", !solplanetControlActive);
  if (samplingView) samplingView.classList.toggle("hidden", !samplingActive);
  if (monthlyReportView) monthlyReportView.classList.toggle("hidden", !monthlyReportActive);
  if (billsView) billsView.classList.toggle("hidden", !billsActive);
  if (databaseView) databaseView.classList.toggle("hidden", !databaseActive);
  if (workerLogsView) workerLogsView.classList.toggle("hidden", !workerLogsActive);
  if (operationTimelineView) operationTimelineView.classList.toggle("hidden", !operationTimelineActive);
  if (workerFailureLogView) workerFailureLogView.classList.toggle("hidden", !workerFailureLogActive);
  if (tabDashboard) tabDashboard.classList.toggle("active", dashboardActive);
  if (tabPlan) tabPlan.classList.toggle("active", planActive);
  if (tabNotificationMatrix) tabNotificationMatrix.classList.toggle("active", notificationMatrixActive);
  if (tabRawData) tabRawData.classList.toggle("active", rawDataActive);
  if (tabSolplanetControl) tabSolplanetControl.classList.toggle("active", solplanetControlActive);
  if (tabReport) tabReport.classList.toggle("active", reportActive);
  if (tabMonthlyReport) tabMonthlyReport.classList.toggle("active", monthlyReportActive);
  if (tabBills) tabBills.classList.toggle("active", billsActive);
  if (tabDatabase) tabDatabase.classList.toggle("active", databaseActive);
  if (tabWorkerLogs) tabWorkerLogs.classList.toggle("active", workerLogsActive);
  if (tabOperationTimeline) tabOperationTimeline.classList.toggle("active", operationTimelineActive);
  if (tabWorkerFailureLog) tabWorkerFailureLog.classList.toggle("active", workerFailureLogActive);
  [
    [tabDashboard, dashboardActive],
    [tabPlan, planActive],
    [tabNotificationMatrix, notificationMatrixActive],
    [tabRawData, rawDataActive],
    [tabSolplanetControl, solplanetControlActive],
    [tabReport, reportActive],
    [tabMonthlyReport, monthlyReportActive],
    [tabBills, billsActive],
    [tabDatabase, databaseActive],
    [tabWorkerLogs, workerLogsActive],
    [tabOperationTimeline, operationTimelineActive],
    [tabWorkerFailureLog, workerFailureLogActive],
  ].forEach(([tab, active]) => {
    if (!tab) return;
    tab.setAttribute("aria-selected", active ? "true" : "false");
    tab.tabIndex = active ? 0 : -1;
  });
  syncRawDataSystemUi();
  const samplingTitle = document.getElementById("samplingTitle");
  if (samplingTitle) {
    const titleKey = reportActive ? "reportTitle" : "samplingTitle";
    samplingTitle.dataset.i18n = titleKey;
    samplingTitle.textContent = t(titleKey);
  }
  if (samplingRangeModeSelect) samplingRangeModeSelect.disabled = false;
  setReportCalendarVisible(reportActive);
  setReportUsageUploadVisible(reportActive);
  setSamplingZeroHeroCurrentVisible(samplingActive && !reportActive);
  setSamplingZeroHeroHistoryVisible(samplingActive && !reportActive);
  setSamplingSamplesSectionVisible(samplingActive && !reportActive);
  if (!reportActive) setReportLoadingModal(false);
  if (currentTab === "sampling") {
    renderSamplingRangeInputContainer();
  } else if (currentTab === "report") {
    renderReportCalendar();
  } else if (currentTab === "monthlyReport") {
    renderMonthlyReport();
  } else if (currentTab === "bills") {
    renderBills();
  } else if (currentTab === "plan") {
    renderHomePlan();
  }
  if (dashboardActive) {
    window.requestAnimationFrame(() => {
      refreshFlowDiagrams();
    });
  }
  if (samplingActive) {
    window.requestAnimationFrame(() => {
      if (samplingChart) samplingChart.resize();
      Object.values(samplingBatteryTimelineCharts).forEach((chart) => chart?.resize?.());
      if (samplingPumpStatusChart) samplingPumpStatusChart.resize();
      if (samplingTotalsChart) samplingTotalsChart.resize();
    });
  } else if (currentTab === "monthlyReport") {
    window.requestAnimationFrame(() => monthlyReportDailyTrendChart?.resize?.());
  }

  if (load) {
    void loadCurrentTab();
  }
}

bindChangeIfPresent("langSelect", (event) => {
  const nextLang = event.target.value === "zh" ? "zh" : "en";
  currentLang = nextLang;
  localStorage.setItem("lang", nextLang);
  applyTranslations();
  refreshFlowDiagrams();
  if (currentTab === "report") renderReportCalendar();
  else if (currentTab === "monthlyReport") renderMonthlyReport();
  else if (currentTab === "bills") renderBills();
  else renderSamplingRangeInputContainer();
  syncWorkerLogsFilters();
  if (stateCache.lastWorkerLogsPage) renderWorkerLogsPage(stateCache.lastWorkerLogsPage);
  if (stateCache.lastOperationTimeline) renderOperationTimelinePage(stateCache.lastOperationTimeline);
  if (stateCache.lastHomePlan) renderHomePlan(stateCache.lastHomePlan);
  if (stateCache.lastOperationTimeline || currentTab === "operationTimeline") {
    void loadOperationTimeline();
  }
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
bindClickIfPresent("combinedReserveFloorSaveBtn", () => {
  void saveDashboardReserveFloor();
});
bindKeydownIfPresent("combinedReserveFloorInput", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    void saveDashboardReserveFloor();
  }
});
bindChangeIfPresent("combinedReserveFloorInput", () => {
  if (!isMobileViewport()) return;
  void saveDashboardReserveFloor();
});

bindClickIfPresent("tabDashboard", () => {
  setActiveTab("dashboard");
});
bindClickIfPresent("tabPlan", () => {
  setActiveTab("plan");
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

bindClickIfPresent("tabReport", () => {
  setActiveTab("report");
});
bindClickIfPresent("reportUsageChooseBtn", () => {
  const input = document.getElementById("reportUsageFileInput");
  if (input) input.click();
});
bindChangeIfPresent("reportUsageFileInput", (event) => {
  const file = event?.target?.files?.[0] || null;
  void uploadReportUsageFile(file);
});
bindClickIfPresent("tabMonthlyReport", () => {
  setActiveTab("monthlyReport");
});
bindClickIfPresent("tabBills", () => {
  setActiveTab("bills");
});
bindClickIfPresent("billsUploadBtn", () => {
  const input = document.getElementById("billsFileInput");
  if (input) input.click();
});
bindChangeIfPresent("billsFileInput", (event) => {
  const file = event?.target?.files?.[0] || null;
  void uploadBillsFile(file);
});
bindChangeIfPresent("billsPeriodSelect", (event) => {
  billsState.selectedInvoice = String(event?.target?.value || "");
  renderBills();
});
bindClickIfPresent("tabDatabase", () => {
  setActiveTab("database");
});
bindClickIfPresent("tabWorkerLogs", () => {
  setActiveTab("workerLogs");
});
bindClickIfPresent("tabOperationTimeline", () => {
  setActiveTab("operationTimeline");
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

bindChangeIfPresent("samplingRangeModeSelect", async () => {
  if (currentTab !== "sampling") return;
  renderSamplingRangeInputContainer();
  samplingPager.page = 1;
  await loadSampling();
});
bindClickIfPresent("samplingRangeJumpBtn", async () => {
  if (currentTab !== "sampling") return;
  const mode = document.getElementById("samplingRangeModeSelect")?.value || "day";
  if (mode === "day") {
    samplingRangeState.day = getLocalDateText();
  } else if (mode === "week") {
    samplingRangeState.week = getLocalDateText();
  } else if (mode === "month") {
    const now = new SiteDate();
    samplingRangeState.monthYear = now.getFullYear();
    samplingRangeState.month = String(now.getMonth() + 1);
  } else {
    return;
  }
  renderSamplingRangeInputContainer();
  samplingPager.page = 1;
  await loadSampling();
});

bindClickIfPresent("monthlyReportPrevBtn", async () => {
  shiftMonthlyReportPickerYear(-1);
  await loadMonthlyReportIndexForVisibleYear();
});
bindClickIfPresent("monthlyReportNextBtn", async () => {
  shiftMonthlyReportPickerYear(1);
  await loadMonthlyReportIndexForVisibleYear();
});
{
  const view = document.getElementById("monthlyReportView");
  if (view) {
    view.addEventListener("click", (event) => {
      const monthButton = event.target?.closest?.("[data-monthly-report-month]");
      if (monthButton) {
        const monthKey = monthButton.getAttribute("data-monthly-report-month") || "";
        if (!setMonthlyReportMonthFromKey(monthKey)) return;
        monthlyReportState.payload = null;
        void loadMonthlyReport();
        return;
      }
      const anomalyDayButton = event.target?.closest?.("[data-monthly-anomaly-day]");
      if (anomalyDayButton) {
        const day = anomalyDayButton.getAttribute("data-monthly-anomaly-day") || "";
        if (!localDateParts(day) || !monthlyReportState.payload) return;
        monthlyReportState.selectedAnomalyDay = day;
        renderMonthlyAnomalies(monthlyReportState.payload);
        return;
      }
      const button = event.target?.closest?.("[data-monthly-report-day]");
      if (!button) return;
      const day = button.getAttribute("data-monthly-report-day") || "";
      if (!localDateParts(day)) return;
      reportCalendarState.selectedDay = day;
      setReportCalendarMonthFromDay(day);
      setActiveTab("report");
    });
  }
}

bindChangeIfPresent("samplingSmoothModeSelect", () => {
  if (samplingChartLastPayload) renderSamplingChart(samplingChartLastPayload);
});
bindClickIfPresent("databaseExportBtn", () => {
  void exportDatabaseFile();
});
bindClickIfPresent("databaseImportBtn", () => {
  const fileInput = document.getElementById("databaseImportFileInput");
  if (!fileInput) return;
  if (typeof fileInput.showPicker === "function") {
    fileInput.showPicker();
    return;
  }
  fileInput.click();
});
bindClickIfPresent("databaseBackupNowBtn", () => {
  void runDatabaseBackupNow();
});

{
  const input = document.getElementById("databaseImportFileInput");
  if (input) {
    input.addEventListener("change", (event) => {
      const target = event.target;
      const file = target?.files?.[0];
      if (!file) return;
      void importDatabaseFile(file);
    });
  }
}

{
  const billsView = document.getElementById("billsView");
  if (billsView) {
    document.addEventListener("dragenter", (event) => {
      if (currentTab !== "bills" || !dragEventHasFiles(event)) return;
      event.preventDefault();
      billsDropDepth += 1;
      setBillsDropActive(true);
    });
    document.addEventListener("dragover", (event) => {
      if (currentTab !== "bills" || !dragEventHasFiles(event)) return;
      event.preventDefault();
      event.dataTransfer.dropEffect = "copy";
      setBillsDropActive(true);
    });
    document.addEventListener("dragleave", (event) => {
      if (currentTab !== "bills" || !dragEventHasFiles(event)) return;
      event.preventDefault();
      billsDropDepth = Math.max(0, billsDropDepth - 1);
      if (billsDropDepth === 0) setBillsDropActive(false);
    });
    document.addEventListener("drop", (event) => {
      if (currentTab !== "bills" || !dragEventHasFiles(event)) return;
      event.preventDefault();
      const file = event.dataTransfer?.files?.[0] || null;
      resetBillsDropState();
      if (file) void uploadDroppedBillsFile(file);
    });
    window.addEventListener("blur", resetBillsDropState);
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
  const modal = document.getElementById("solplanetPinModal");
  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target === modal) setSolplanetPinModalVisible(false);
    });
  }
}
{
  const modal = document.getElementById("teslaCurrentModal");
  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target === modal && !teslaCurrentControlBusy) setTeslaCurrentModalVisible(false);
    });
  }
}
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
  const solplanetPinModal = document.getElementById("solplanetPinModal");
  if (solplanetPinModal && !solplanetPinModal.classList.contains("hidden")) {
    setSolplanetPinModalVisible(false);
  }
  const teslaCurrentModal = document.getElementById("teslaCurrentModal");
  if (teslaCurrentModal && !teslaCurrentModal.classList.contains("hidden") && !teslaCurrentControlBusy) {
    setTeslaCurrentModalVisible(false);
  }
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

function bindKeydownIfPresent(id, handler) {
  const el = document.getElementById(id);
  if (el) el.addEventListener("keydown", handler);
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
bindChangeIfPresent("sajDashboardProfileSelect", () => {
  renderSajTouControls();
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
bindClickIfPresent("solplanetPinModalCloseBtn", () => {
  setSolplanetPinModalVisible(false);
});
bindClickIfPresent("teslaCurrentModalCloseBtn", () => {
  if (teslaCurrentControlBusy) return;
  setTeslaCurrentModalVisible(false);
});
bindClickIfPresent("solplanetPinApplyBtn", () => {
  void applySolplanetDashboardPin();
});
bindInputIfPresent("teslaCurrentModalInput", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) return;
  teslaCurrentDraftA = Number(target.value);
  setText("teslaCurrentModalStatus", t("teslaCurrentModalStatusReady", { value: `${Math.round(Number(target.value || 0))}A` }));
});
bindClickIfPresent("teslaCurrentModalApplyBtn", () => {
  void applyTeslaChargeCurrent();
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
        configReady = isConfigComplete(result);
        if (configReady) {
          setConfigModalVisible(false);
          if (saveMsg) saveMsg.textContent = t("configSaved");
          void loadCurrentTab();
        } else if (saveMsg) {
          saveMsg.textContent = t("configMustSaveFirst");
        }
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

bindClickIfPresent("configAutoDiscoverBtn", async () => {
  const saveMsg = document.getElementById("configSaveMsg");
  if (saveMsg) saveMsg.textContent = t("configAutoDiscovering");
  try {
    const result = await fetchJson("/api/config/solplanet/discover", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        solplanet_dongle_host: document.getElementById("cfgDongleHost").value.trim(),
      }),
      timeoutMs: 15000,
    });
    fillConfigForm(result);
    configReady = isConfigComplete(result);
    if (saveMsg) saveMsg.textContent = t("configAutoDiscovered");
  } catch (err) {
    configReady = false;
    if (saveMsg) saveMsg.textContent = t("configAutoDiscoverFailed", { error: String(err) });
  }
});

bindClickIfPresent("configPruneRetentionBtn", async () => {
  if (!window.confirm(t("configPruneRetentionConfirm"))) return;
  const saveMsg = document.getElementById("configSaveMsg");
  const button = document.getElementById("configPruneRetentionBtn");
  if (button) button.disabled = true;
  if (saveMsg) saveMsg.textContent = t("configPruningRetention");
  try {
    const savedConfig = await fetchJson("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildConfigPayloadFromForm()),
      timeoutMs: 10000,
    });
    fillConfigForm(savedConfig);
    configReady = isConfigComplete(savedConfig);
    const result = await fetchJson("/api/storage/retention/prune", {
      method: "POST",
      timeoutMs: 180000,
    });
    const reclaimed = Number(result?.retention?.storage_compaction?.reclaimed_bytes || 0);
    if (saveMsg) saveMsg.textContent = t("configPruneRetentionDone", { size: formatBinarySize(reclaimed) });
    await loadCurrentTab();
  } catch (err) {
    if (saveMsg) saveMsg.textContent = t("configPruneRetentionFailed", { error: String(err) });
  } finally {
    if (button) button.disabled = false;
  }
});

window.addEventListener("resize", () => {
  if (samplingChart) samplingChart.resize();
  Object.values(samplingBatteryTimelineCharts).forEach((chart) => chart?.resize?.());
  if (samplingPumpStatusChart) samplingPumpStatusChart.resize();
  if (samplingTotalsChart) samplingTotalsChart.resize();
  if (monthlyReportDailyTrendChart) monthlyReportDailyTrendChart.resize();
  refreshFlowDiagrams();
  refreshMobileCombinedFlowDiagram();
});

document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  if (!target.closest("#reportBillMenu") && !target.closest("#reportBillListBtn")) {
    setReportBillMenuVisible(false);
  }
  const mobileActionNode = target.closest("[data-mobile-action]");
  const mobileAction = mobileActionNode?.getAttribute("data-mobile-action");
  if (mobileAction === "solplanet-pin") {
    void openSolplanetPinModal();
    return;
  }
  if (mobileAction === "saj-profile") {
    toggleSajDashboardProfilePopover();
    return;
  }
  if (target.closest(".efd-node-id-combined-inverter2Node")) {
    void openSolplanetPinModal();
    return;
  }
  if (target.closest("#combined-inverter1State")) {
    toggleSajDashboardProfilePopover();
    return;
  }
  const insidePopover = target.closest("#sajDashboardProfilePopover");
  if (!insidePopover) setSajDashboardProfilePopoverVisible(false);
  if (currentTab === "report") {
    scheduleSamplingChartsHoverReset({ resize: false });
  }
});

window.addEventListener("focus", () => {
  scheduleSamplingChartsHoverReset({ resize: true });
});

document.addEventListener("visibilitychange", () => {
  if (document.hidden) return;
  scheduleSamplingChartsHoverReset({ resize: true });
});

initFlowDiagrams();
bindClickIfPresent("combined-teslaChargingToggleBtn", () => {
  void toggleTeslaCharging();
});
bindClickIfPresent("combined-poolPumpToggleBtn", () => {
  void togglePoolPump();
});
applyTranslations();
samplingRangeState.day = getLocalDateText();
samplingRangeState.week = samplingRangeState.day;
samplingRangeState.day = getLocalDateText();
samplingRangeState.week = samplingRangeState.day;
samplingRangeState.monthYear = new SiteDate().getFullYear();
samplingRangeState.month = String(new SiteDate().getMonth() + 1);
samplingRangeState.relative = "-6h";
samplingRangeState.endDate = samplingRangeState.day;
samplingRangeState.startDate = getLocalDateText(-1);
{
  const now = new SiteDate();
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

function renderSiteClock() {
  const clock = document.getElementById("siteClock");
  if (!clock) return;
  clock.textContent = `${new SiteDate().toLocaleString(currentLang === "zh" ? "zh-CN" : "en-AU", {hour12: false, timeZoneName: "short"})} · ${SITE_TIME_ZONE}`;
}
renderSiteClock();
window.setInterval(renderSiteClock, 1000);
