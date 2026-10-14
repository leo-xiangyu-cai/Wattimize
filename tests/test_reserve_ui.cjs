const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {test} = require('node:test');
const source = fs.readFileSync('app/static/app.js', 'utf8');
const html = fs.readFileSync('app/static/index.html', 'utf8');
function setup() {
  const elements = new Map();
  const document = {activeElement: null, getElementById(id) {
    if (!elements.has(id)) elements.set(id, {value:'',disabled:false,checked:false,textContent:'',innerHTML:''});
    return elements.get(id);
  }};
  const context = vm.createContext({document, Date, Boolean, String, Number, console,
    t: (key, fields) => key+JSON.stringify(fields || {}),
    setText: (id,value) => {document.getElementById(id).textContent=value;},
    formatLocalDateTime: s=>s, escapeHtml:s=>s.replaceAll('<','&lt;')});
  vm.runInContext(source.slice(source.indexOf('let reserveForecastData ='),source.indexOf('async function loadReserveForecast()')),context);
  return {document,context};
}
test('auto locks manual input, keeps independent values, unchecking restores editing',()=>{
  const {document,context}=setup();
  context.data={automatic:true,manual_kwh:15,effective_kwh:12,source:'automatic',prediction:{recommended_kwh:12}};
  vm.runInContext('renderReserveForecast(data)',context);
  assert.equal(document.getElementById('combinedReserveFloorInput').disabled,true);
  assert.equal(document.getElementById('combinedReserveFloorInput').value,15);
  assert.equal(document.getElementById('combinedReservePrediction').value,12);
  context.data.automatic=false;context.data.source='manual';context.data.effective_kwh=15;
  vm.runInContext('renderReserveForecast(data)',context);
  assert.equal(document.getElementById('combinedReserveFloorInput').disabled,false);
  assert.equal(document.getElementById('combinedReserveAuto').checked,false);
  assert.match(html,/id="combinedReservePrediction"[^>]*readonly/);
});
test('polling does not overwrite a manual value being edited',()=>{
  const {document,context}=setup();
  document.activeElement=document.getElementById('combinedReserveFloorInput');
  document.activeElement.value='18';
  context.data={automatic:false,manual_kwh:15,effective_kwh:15,source:'manual',prediction:{}};
  vm.runInContext('renderReserveForecast(data)',context);
  assert.equal(document.activeElement.value,'18');
});
test('empty evaluation set does not display a fabricated accuracy',()=>{
  const {document,context}=setup();
  context.data={automatic:false,manual_kwh:15,effective_kwh:15,source:'manual',prediction:{},stats:{evaluated_nights:0,mae_kwh:null,coverage_percent:null}};
  vm.runInContext('renderReserveForecast(data)',context);
  assert.match(document.getElementById('reserveForecastStats').textContent,/"coverage":"—"/);
});
