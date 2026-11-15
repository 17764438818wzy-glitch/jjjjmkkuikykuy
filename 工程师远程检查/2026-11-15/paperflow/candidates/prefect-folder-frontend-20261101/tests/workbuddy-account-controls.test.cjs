const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const tick = () => new Promise(setImmediate);
const account = (uid, enabled = false) => ({ uid, nickname: `账号${uid}`, realm: 'cn', enabled,
  status: enabled ? 'untested' : 'disabled' });

function browser(initial) {
  const nodes = new Map();
  const listeners = {};
  const get = id => {
    if (!nodes.has(id)) nodes.set(id, { textContent: '', disabled: false, dataset: {}, handlers: {},
      innerHTML: '', open: false, addEventListener(name, fn) { this.handlers[name] = fn; },
      setAttribute() {}, showModal() { this.open = true; }, close() { this.open = false; } });
    return nodes.get(id);
  };
  const state = { accounts: initial, calls: [], batches: [], onRefresh: null, enableResult: null };
  const document = { hidden: false, getElementById: get, body: { append() {} },
    createElement: () => get('workbuddy-accounts-dialog'), addEventListener: (name, fn) => { listeners[name] = fn; } };
  let interval;
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', 'workbuddy-dashboard.js'), 'utf8'), {
    document, AbortSignal, Date, setInterval(fn) { interval = fn; },
    fetch: async (url, options = {}) => {
      const endpoint = new URL(url).pathname;
      state.calls.push({ endpoint, options });
      let value;
      if (endpoint.endsWith('/summary')) value = { connected: true, authorized: true, domestic_only_test: true };
      else if (endpoint.endsWith('/accounts')) value = { status: 'available', accounts: state.accounts.map(a => ({ ...a })) };
      else if (endpoint.endsWith('/accounts/test')) value = { status: 'success', accounts: state.accounts.map(a => ({ ...a })) };
      else if (endpoint.endsWith('/accounts/enable')) {
        const { uid } = JSON.parse(options.body);
        const a = state.accounts.find(a => a.uid === uid);
        if (state.enableResult) value = state.enableResult;
        else { a.enabled = true; value = { status: 'enabled', account: { ...a } }; }
      } else if (endpoint.endsWith('/accounts/refresh')) {
        const { uids } = JSON.parse(options.body);
        state.batches.push(uids);
        if (state.onRefresh) value = await state.onRefresh(uids);
        else {
          let enabled_count = 0;
          value = { status: 'success', accounts: uids.map(uid => {
            const a = state.accounts.find(a => a.uid === uid);
            if (!a.enabled) enabled_count += 1;
            a.enabled = true;
            return { ...a, status: 'success' };
          }) };
          value.enabled_count = enabled_count;
        }
      } else throw new Error(`Unexpected endpoint: ${endpoint}`);
      return { ok: true, json: async () => value };
    },
  });
  return { state, get, listeners, poll: () => interval() };
}

async function open(page) {
  await tick();
  await page.get('workbuddy-test-button').handlers.click();
}

test('summary and visibility refresh never test or enable accounts automatically', async () => {
  const p = browser([account('off')]);
  await tick();
  await p.poll();
  await p.listeners.visibilitychange();
  await tick();
  assert.ok(p.state.calls.every(c => c.endpoint.endsWith('/summary')));
  await open(p);
  assert.match(p.get('workbuddy-accounts-dialog').innerHTML, /workbuddy-account-refresh/);
  assert.match(p.get('workbuddy-account-rows').innerHTML, /data-wb-enable="off"/);
  assert.equal(p.state.batches.length, 0);
});

test('Enable changes only the selected account and leaves its connectivity untested', async () => {
  const p = browser([account('one'), account('two')]);
  await open(p);
  await p.get('workbuddy-account-rows').handlers.click({ target: { closest: () =>
    ({ disabled: false, dataset: { wbEnable: 'one' } }) } });
  const calls = p.state.calls.filter(c => c.endpoint.endsWith('/accounts/enable'));
  assert.equal(calls.length, 1);
  assert.deepEqual(JSON.parse(calls[0].options.body), { uid: 'one' });
  assert.match(p.get('workbuddy-account-rows').innerHTML, /data-wb-disable="one"/);
  assert.match(p.get('workbuddy-account-rows').innerHTML, /data-wb-enable="two"/);
  assert.match(p.get('workbuddy-account-rows').innerHTML, /未测试/);
  assert.equal(p.state.batches.length, 0);
});

test('Refresh reloads newly added accounts and processes them in batches of four with final progress', async () => {
  const p = browser([account('old')]);
  await open(p);
  p.state.accounts.push(...Array.from({ length: 6 }, (_, i) => account(`new-${i}`)));
  await p.get('workbuddy-account-refresh').handlers.click();
  assert.deepEqual(p.state.batches.map(a => a.length), [4, 3]);
  assert.deepEqual(p.state.batches.flat(), ['old', 'new-0', 'new-1', 'new-2', 'new-3', 'new-4', 'new-5']);
  assert.match(p.get('workbuddy-account-message').textContent, /7\s*\/\s*7/);
  assert.match(p.get('workbuddy-account-message').textContent, /启用\s*7/);
  assert.match(p.get('workbuddy-account-rows').innerHTML, /data-wb-disable="new-5"/);
  assert.equal(p.get('workbuddy-account-refresh').disabled, false);
});

test('failed and unknown results retain the account switch and show escaped reasons', async () => {
  const p = browser([account('failed'), account('network', true)]);
  await open(p);
  p.state.onRefresh = async () => ({ status: 'failed', enabled_count: 0, accounts: [
    { ...account('failed'), status: 'failed', error: '<quota exhausted>' },
    { ...account('network', true), status: 'unknown', error: 'network unavailable' },
  ] });
  await p.get('workbuddy-account-refresh').handlers.click();
  const html = p.get('workbuddy-account-rows').innerHTML;
  assert.match(html, /data-wb-enable="failed"/);
  assert.match(html, /data-wb-disable="network"/);
  assert.match(html, /&lt;quota exhausted&gt;/);
  assert.equal(p.get('workbuddy-test-result').dataset.state, 'failed');
  assert.equal(p.state.calls.filter(c => c.endpoint.endsWith('/accounts/enable')).length, 0);
});

test('a pending refresh blocks a duplicate refresh and row mutation', async () => {
  const p = browser([account('only')]);
  await open(p);
  let release;
  p.state.onRefresh = () => new Promise(resolve => { release = resolve; });
  const pending = p.get('workbuddy-account-refresh').handlers.click();
  await tick();
  assert.equal(p.get('workbuddy-account-refresh').disabled, true);
  await p.get('workbuddy-account-refresh').handlers.click();
  await p.get('workbuddy-account-rows').handlers.click({ target: { closest: () =>
    ({ disabled: false, dataset: { wbEnable: 'only' } }) } });
  assert.equal(p.state.batches.length, 1);
  assert.equal(p.state.calls.filter(c => c.endpoint.endsWith('/accounts/enable')).length, 0);
  release({ status: 'success', enabled_count: 1, accounts: [{ ...account('only', true), status: 'success' }] });
  await pending;
  assert.equal(p.get('workbuddy-account-refresh').disabled, false);
});

test('a later batch timeout keeps completed results and is not replayed', async () => {
  const p = browser(Array.from({ length: 5 }, (_, i) => account(`a-${i}`)));
  await open(p);
  p.state.onRefresh = async uids => {
    if (uids.includes('a-4')) throw new Error('timeout');
    return { status: 'success', enabled_count: 4, accounts: uids.map(uid => ({ ...account(uid, true), status: 'success' })) };
  };
  await p.get('workbuddy-account-refresh').handlers.click();
  assert.equal(p.state.batches.length, 2);
  assert.match(p.get('workbuddy-account-rows').innerHTML, /data-wb-disable="a-0"/);
  assert.match(p.get('workbuddy-account-rows').innerHTML, /待核对/);
  assert.equal(p.get('workbuddy-test-result').dataset.state, 'unknown');
  await p.poll();
  assert.equal(p.state.batches.length, 2);
});

test('an unknown enable response is not treated as enabled or automatically resent', async () => {
  const p = browser([account('pending')]);
  p.state.enableResult = { status: 'unknown', error: '启用结果待核对' };
  await open(p);
  await p.get('workbuddy-account-rows').handlers.click({ target: { closest: () =>
    ({ disabled: false, dataset: { wbEnable: 'pending' } }) } });
  assert.match(p.get('workbuddy-account-message').textContent, /待核对/);
  assert.equal(p.state.calls.filter(c => c.endpoint.endsWith('/accounts/enable')).length, 1);
  assert.match(p.get('workbuddy-account-rows').innerHTML, /data-wb-enable="pending"/);
});
