const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

async function renderWith(summary) {
  const elements = new Map();
  const element = (id) => {
    if (!elements.has(id)) elements.set(id, { textContent: '', disabled: false, title: '', dataset: {}, addEventListener() {} });
    return elements.get(id);
  };
  const context = {
    document: { hidden: false, getElementById: element, addEventListener() {} },
    fetch: async () => ({ ok: true, json: async () => summary }),
    AbortSignal: { timeout: () => undefined },
    setInterval() {},
  };
  vm.runInNewContext(fs.readFileSync('workbuddy-dashboard.js', 'utf8'), context);
  await new Promise((resolve) => setImmediate(resolve));
  return element('workbuddy-test-button');
}

test('old monitor cannot offer the account test until domestic-only support is loaded', async () => {
  const connected = { connected: true, authorized: true };
  const oldButton = await renderWith(connected);
  assert.equal(oldButton.disabled, true);
  assert.match(oldButton.title, /等待国内账号测试服务更新/);
  const currentButton = await renderWith({ ...connected, domestic_only_test: true });
  assert.equal(currentButton.disabled, false);
});
