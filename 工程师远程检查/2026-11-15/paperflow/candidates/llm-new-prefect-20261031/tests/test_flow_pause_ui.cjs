const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'flow_app.js'), 'utf8');
const start = source.indexOf('  function integer(');
const end = source.indexOf('  function setStatus(', start);
assert.ok(start >= 0 && end > start);
const pausedStatus = vm.runInNewContext(`(() => { ${source.slice(start, end)}; return pausedStatus; })()`,
  { number: new Intl.NumberFormat('zh-CN') });

test('pause waits for admitted requests and shows unknown outcomes', () => {
  assert.equal(pausedStatus({inflight_requests:2,unknown_requests:0})[0], '正在暂停');
  assert.equal(pausedStatus({inflight_requests:0,unknown_requests:1})[0], '待核对');
  assert.equal(pausedStatus({inflight_requests:0,unknown_requests:0})[0], '已暂停');
  assert.equal(pausedStatus({})[0], '状态待核对');
});
