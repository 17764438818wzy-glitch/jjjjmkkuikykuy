const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const script = path.join(__dirname, '..', 'workbuddy-dashboard.js');

function browser(replies) {
  assert.equal(fs.existsSync(script), true, 'WorkBuddy dashboard script must exist');
  const nodes = new Map();
  function getNode(id) {
    if (!nodes.has(id)) nodes.set(id, {
      textContent: '', value: '', disabled: false, dataset: {}, handlers: {},
      innerHTML: '', open: false,
      addEventListener(name, fn) { this.handlers[name] = fn; },
      setAttribute() {},
      showModal() { this.open = true; }, close() { this.open = false; }, focus() {},
    });
    return nodes.get(id);
  }
  const document = {
    getElementById: getNode,
    createElement() { return getNode('workbuddy-accounts-dialog'); },
    body: { append() {} },
  };
  const calls = [];
  let interval;
  const context = { document, AbortSignal, Date,
    fetch: async (url, options = {}) => {
      calls.push({ url, options });
      const item = replies.shift();
      if (item instanceof Error) throw item;
      return { ok: true, json: async () => item };
    },
    setInterval: (fn, ms) => { interval = { fn, ms }; },
  };
  vm.runInNewContext(fs.readFileSync(script, 'utf8'), context);
  return { nodes, calls, get interval() { return interval; } };
}

test('refreshes every second and never invents numbers or sends a model request', async () => {
  const page = browser([{ connected: true, authorized: false, accounts_ready: 4, accounts_total: 6 }]);
  await new Promise(setImmediate);
  assert.equal(page.interval.ms, 1000);
  assert.equal(page.nodes.get('workbuddy-input-tokens').textContent, '—');
  assert.equal(page.nodes.get('workbuddy-inflight').textContent, '—');
  assert.equal(page.nodes.get('workbuddy-test-button').disabled, true);
  assert.equal(page.calls.length, 1);
  assert.equal(page.calls[0].options.method, undefined);
});

test('renders actual usage while leaving unavailable concurrency unknown', async () => {
  const page = browser([{ connected: true, authorized: true, usage_available: true, recent_model: 'sample-model', recent_realm: 'intl',
    input_tokens: 1200, output_tokens: 50, successful_requests: 3, failed_requests: 0,
    accounts_ready: 4, accounts_total: 6, credit_balance: 90, credits_partial: true,
    prefect_active_slots: 0, concurrency_limit: 50 }]);
  await new Promise(setImmediate);
  assert.equal(page.nodes.get('workbuddy-input-tokens').textContent, '1,200');
  assert.equal(page.nodes.get('workbuddy-failed').textContent, '0');
  assert.equal(page.nodes.get('workbuddy-inflight').textContent, '—');
  assert.equal(page.nodes.get('workbuddy-prefect-slots').textContent, '0 / 50');
  assert.equal(page.nodes.get('workbuddy-session').textContent, '用量已连接');
  assert.equal(page.nodes.get('workbuddy-test-button').disabled, false);
});

test('does not claim usage is connected when authenticated statistics fail', async () => {
  const page = browser([{ connected: true, authorized: true, usage_available: false }]);
  await new Promise(setImmediate);
  assert.equal(page.nodes.get('workbuddy-session').textContent, '用量暂不可用');
});

test('the test request is sent only after the user clicks and lists all accounts', async () => {
  const page = browser([{ connected: true, authorized: true, recent_model: 'sample-model', recent_realm: 'intl' },
    { status: 'available', accounts: [
      { uid: 'a', nickname: '甲', realm: 'intl', enabled: true, status: 'untested' },
      { uid: 'b', nickname: '乙', realm: 'cn', enabled: false, status: 'disabled' },
    ] },
    { status: 'success', accounts: [
      { uid: 'a', nickname: '甲', realm: 'intl', enabled: true, status: 'success', model: 'sample-model' },
      { uid: 'b', nickname: '乙', realm: 'cn', enabled: false, status: 'disabled' },
    ] }]);
  await new Promise(setImmediate);
  assert.equal(page.calls.length, 1);
  await page.nodes.get('workbuddy-test-button').handlers.click();
  assert.equal(page.calls.length, 3);
  assert.equal(page.calls[1].options.method, undefined);
  assert.equal(page.calls[2].options.method, 'POST');
  assert.match(page.calls[2].url, /\/accounts\/test$/);
  assert.equal(page.nodes.get('workbuddy-test-result').textContent, '成功');
  assert.equal(page.nodes.get('workbuddy-test-result').dataset.state, 'success');
  assert.equal(page.nodes.get('workbuddy-accounts-dialog').open, true);
  assert.match(page.nodes.get('workbuddy-account-rows').innerHTML, /甲/);
  assert.match(page.nodes.get('workbuddy-account-rows').innerHTML, /乙/);
  assert.match(page.nodes.get('workbuddy-account-rows').innerHTML, /已禁用/);
  assert.equal(page.calls.filter(call => /\/disable$/.test(call.url)).length, 0);
});

test('shows failures in red but leaves unknown results distinct', async () => {
  for (const [reply, expectedText, expectedState] of [
    [{ status: 'failed', accounts: [] }, '失败', 'failed'],
    [{ status: 'unknown', accounts: [] }, '结果未知', 'unknown'],
  ]) {
    const page = browser([{ connected: true, authorized: true, recent_model: 'sample-model', recent_realm: 'intl' },
      { status: 'available', accounts: [] }, reply]);
    await new Promise(setImmediate);
    await page.nodes.get('workbuddy-test-button').handlers.click();
    assert.equal(page.nodes.get('workbuddy-test-result').textContent, expectedText);
    assert.equal(page.nodes.get('workbuddy-test-result').dataset.state, expectedState);
  }
});

test('disables only the account the user selected and escapes error text', async () => {
  const account = { uid: 'a', nickname: '甲<script>', realm: 'intl', enabled: true,
    status: 'failed', error: '<bad> 403' };
  const page = browser([
    { connected: true, authorized: true },
    { status: 'available', accounts: [account] },
    { status: 'failed', accounts: [account] },
    { status: 'disabled' },
  ]);
  await new Promise(setImmediate);
  await page.nodes.get('workbuddy-test-button').handlers.click();
  const rows = page.nodes.get('workbuddy-account-rows');
  assert.match(rows.innerHTML, /甲&lt;script&gt;/);
  assert.match(rows.innerHTML, /&lt;bad&gt; 403/);
  const button = { disabled: false, dataset: { wbDisable: 'a' } };
  await rows.handlers.click({ target: { closest: () => button } });
  assert.match(page.calls[3].url, /\/accounts\/disable$/);
  assert.equal(page.calls[3].options.body, '{"uid":"a"}');
  assert.match(rows.innerHTML, /已禁用/);
});
