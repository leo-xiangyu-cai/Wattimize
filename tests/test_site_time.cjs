const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { test } = require('node:test');
const context = vm.createContext({document:{querySelector:()=>({content:'Australia/Sydney'})}, Date, Intl,
  t:(key)=>key, monthLabel:String, NOTIFICATION_MATRIX_WINDOWS:[], currentLang:'en'});
for (const file of ['app/static/vendor/date-fns-tz.js','app/static/site-time.js']) vm.runInContext(fs.readFileSync(file,'utf8'),context);
const app = fs.readFileSync('app/static/app.js','utf8');
for(const name of ['localDateParts','toUtcIsoFromDateOnly','toUtcIsoFromDateEndExclusive','toUtcIsoFromDateTimeLocal','toLocalDateTimeInputValueFromMs','getLocalDateText','getWeekInfo','getMonthInfo','getNextWindowEndLocalTime','isLocalTimeInHourWindow','applyRelativeOffset']) {
 const match = app.match(new RegExp(`^function ${name}\\([^]*?^}`, 'm'));
 assert.ok(match, name); vm.runInContext(match[0],context);
}
const run=(source)=>vm.runInContext(source,context);
test('Sydney 11am is independent of browser timezone in both seasons',()=>{
 for(const [utc,offset] of [['2026-10-09T00:00:00Z',-660],['2026-07-09T01:00:00Z',-600]]) {
  assert.equal(run(`new SiteDate('${utc}').getHours()`),11);
  assert.equal(run(`new SiteDate('${utc}').getTimezoneOffset()`),offset);
  assert.equal(run(`new SiteDate('${utc}').toLocaleTimeString('en-GB',{hour12:false})`),'11:00:00');
  assert.equal(run(`isLocalTimeInHourWindow(new SiteDate('${utc}'),11,15)`),true);
  assert.equal(run(`isLocalTimeInHourWindow(new SiteDate(Date.parse('${utc}')-1),11,15)`),false);
 }
});
test('day ranges include 23 or 25 real hours across DST',()=>{
 for(const [day,hours] of [['2026-10-04',23],['2026-04-05',25],['2027-04-04',25],['2026-10-09',24]]) {
  assert.equal(run(`(Date.parse(toUtcIsoFromDateEndExclusive('${day}'))-Date.parse(toUtcIsoFromDateOnly('${day}')))/3600000`),hours);
 }
});
test('week and month boundaries stay on site midnight',()=>{
 assert.equal(run("getWeekInfo('2026-10-04').startUtc"),'2026-09-27T14:00:00.000Z');
 assert.equal(run("getWeekInfo('2026-10-04').endUtc"),'2026-10-04T13:00:00.000Z');
 assert.equal(run("getWeekInfo('2026-10-09').week"),41);
 assert.equal(run('getMonthInfo(10,2026).startUtc'),'2026-09-30T14:00:00.000Z');
 assert.equal(run('getMonthInfo(10,2026).endUtc'),'2026-10-31T13:00:00.000Z');
});
test('wall time inputs and UTC serialization round trip',()=>{
 assert.equal(run("toUtcIsoFromDateTimeLocal('2026-10-09T11:00')"),'2026-10-09T00:00:00.000Z');
 assert.equal(run("toLocalDateTimeInputValueFromMs(Date.parse('2026-10-09T00:00:00Z'))"),'2026-10-09T11:00');
 assert.equal(run("new SiteDate('2026-10-09T00:00:00Z').toISOString()"),'2026-10-09T00:00:00.000Z');
 assert.equal(run("Number.isNaN(new SiteDate('bad').getTime())"),true);
});
test('overnight target accounts for real DST duration; elapsed hours stay elapsed',()=>{
 assert.equal(run("(getNextWindowEndLocalTime(5,new SiteDate('2026-10-03T23:00'))-new SiteDate('2026-10-03T23:00'))/3600000"),5);
 assert.equal(run("(getNextWindowEndLocalTime(5,new SiteDate('2026-04-04T23:00'))-new SiteDate('2026-04-04T23:00'))/3600000"),7);
 assert.equal(run("(new SiteDate('2026-10-04T03:30')-applyRelativeOffset(new SiteDate('2026-10-04T03:30'),{unit:'h',amount:2}))/3600000"),2);
});
