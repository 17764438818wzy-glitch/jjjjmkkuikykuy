const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '..', 'site');

class Element {
  constructor() { this.children = []; this.dataset = {}; this.attributes = {}; this.listeners = {}; this._text = ''; }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map((child) => child.textContent).join(''); }
  set innerHTML(_) { throw new Error('content must not be interpreted as HTML'); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  append(...items) { this.children.push(...items); }
  replaceChildren(...items) { this._text = ''; this.children = items; }
  addEventListener(name, callback) { this.listeners[name] = callback; }
  click() { if (!this.disabled) this.listeners.click?.(); }
}

test('three folder result categories expand, show pending evidence, paginate and close', async () => {
  const ids = ['passed-count', 'failed-count', 'unrecognized-count', 'results-hint', 'list-panel', 'list-title',
    'list-count', 'list-message', 'group-list', 'pagination', 'page-info', 'previous-page', 'next-page', 'close-list'];
  const nodes = new Map(ids.map((id) => ['postprocessing-' + id, new Element()]));
  const buttons = ['passed', 'failed', 'unrecognized'].map((filter) => {
    const button = new Element(); button.dataset.postprocessingFilter = filter; return button;
  });
  const calls = [], listeners = {}, revision = 'a'.repeat(64), as_of = '2026-11-01T00:00:00Z';
  let verified = true, mixedPending = false;
  const context = { URL, Intl, AbortSignal, setTimeout() { return 1; }, clearTimeout() {},
    document: { getElementById: (id) => nodes.get(id), querySelectorAll: () => buttons,
      createElement: () => new Element() },
    window: { addEventListener: (name, fn) => { listeners[name] = fn; } },
    fetch: async (url) => {
      const query = new URL(url).searchParams;
      calls.push(query);
      const filter = query.get('postprocessing_result_filter');
      const page = Number(query.get('postprocessing_result_page'));
      const result = { available: true, basis: 'postprocessing-registered-ledger/1', as_of, partial: false,
        counts: { passed: 21, failed: null, unrecognized: null },
        list: { filter, page, page_size: 20, total: filter === 'passed' && !mixedPending ? 21 : null,
          has_next: filter === 'passed' && page === 1,
          items: [{ group_id: `folder-${page}`, task_name: '合成大工单', title_status: '已提取',
            paper_title: '<img src=x>', reason: '已登记结果' },
            ...(mixedPending ? [{group_id: 'pending-folder', verification_pending: true,
              reason: '已登记 · 证据待核对'}] : [])] } };
      return { ok: true, json: async () => ({ postprocessing_observation_remaining_ms: 10000, postprocessing_registered_ledger: { available: true,
        basis: 'postprocessing-registered-ledger/1', current_delivery_verified: false, revision, as_of,
        results: result, server_observation: { available: verified,
          basis: 'paperflow-server-receipt-observation/1', as_of, observation_revision: revision } } }) };
    } };
  vm.runInNewContext(fs.readFileSync(path.join(root, 'postprocessing-results-server-20261101.js'), 'utf8'), context);
  const flush = () => new Promise(setImmediate);
  buttons[0].click(); await flush();
  assert.equal(nodes.get('postprocessing-list-panel').hidden, false);
  assert.match(nodes.get('postprocessing-group-list').textContent, /合成大工单/);
  assert.match(nodes.get('postprocessing-group-list').textContent, /<img src=x>/);
  assert.match(nodes.get('postprocessing-group-list').textContent, /登记已通过/);
  nodes.get('postprocessing-next-page').click(); await flush();
  assert.equal(calls.at(-1).get('postprocessing_result_page'), '2');
  assert.equal(calls.at(-1).get('postprocessing_result_revision'), revision);
  verified = false;
  buttons[0].click(); await flush();
  assert.match(nodes.get('postprocessing-group-list').textContent, /已登记 · 待核验/);
  assert.doesNotMatch(nodes.get('postprocessing-group-list').textContent, /登记已通过/);
  verified = true;
  buttons[0].click(); await flush();
  assert.match(nodes.get('postprocessing-group-list').textContent, /登记已通过/);
  mixedPending = true;
  buttons[0].click(); await flush();
  assert.match(nodes.get('postprocessing-group-list').textContent, /登记已通过/);
  assert.match(nodes.get('postprocessing-group-list').textContent, /已登记 · 待核验/);
  mixedPending = false;
  for (const [index, filter] of [[1, 'failed'], [2, 'unrecognized']]) {
    buttons[index].click(); await flush();
    assert.equal(calls.at(-1).get('postprocessing_result_filter'), filter);
    assert.match(nodes.get('postprocessing-group-list').textContent, /证据待核验/);
    assert.equal(nodes.get(`postprocessing-${filter}-count`).textContent, '—');
  }
  nodes.get('postprocessing-close-list').click();
  assert.equal(nodes.get('postprocessing-list-panel').hidden, true);
});

test('candidate keeps three result controls in the postprocessing card', () => {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  const card = html.split('aria-labelledby="postprocessing-title"')[1]
    .split('aria-labelledby="waiting-rag-upload-title"')[0];
  for (const filter of ['passed', 'failed', 'unrecognized']) {
    assert.match(card, new RegExp(`data-postprocessing-filter="${filter}"`));
  }
});

test('pending list retries stop when the user switches filters', async () => {
  const ids = ['passed-count', 'failed-count', 'unrecognized-count', 'results-hint', 'list-panel', 'list-title',
    'list-count', 'list-message', 'group-list', 'pagination', 'page-info', 'previous-page', 'next-page', 'close-list'];
  const nodes = new Map(ids.map((id) => ['postprocessing-' + id, new Element()]));
  const buttons = ['passed', 'failed', 'unrecognized'].map((filter) => {
    const button = new Element(); button.dataset.postprocessingFilter = filter; return button;
  });
  const timers = [], calls = [], revision = 'a'.repeat(64), as_of = '2026-11-01T00:00:00Z';
  const context = { URL, Intl, AbortSignal, setTimeout: (fn, delay) => { timers.push({ fn, delay }); return timers.length; }, clearTimeout() {},
    document: { getElementById: (id) => nodes.get(id), querySelectorAll: () => buttons,
      createElement: () => new Element() }, window: { addEventListener() {} },
    fetch: async (url) => {
      const filter = new URL(url).searchParams.get('postprocessing_result_filter');
      calls.push(filter);
      if (calls.length === 1 || calls.length === 3) return { ok: false, status: 503 };
      const result = { available: true, basis: 'postprocessing-registered-ledger/1', as_of, partial: false,
        counts: { passed: 1, failed: null, unrecognized: null },
        list: { filter, page: 1, page_size: 20, total: filter === 'passed' ? 1 : null,
          has_next: false, items: [] } };
      return { ok: true, json: async () => ({ postprocessing_observation_remaining_ms: 10000, postprocessing_registered_ledger: { available: true,
        basis: 'postprocessing-registered-ledger/1', current_delivery_verified: false, revision, as_of,
        results: result, server_observation: { available: true,
          basis: 'paperflow-server-receipt-observation/1', as_of, observation_revision: revision } } }) };
    } };
  vm.runInNewContext(fs.readFileSync(path.join(root, 'postprocessing-results-server-20261101.js'), 'utf8'), context);
  const flush = () => new Promise(setImmediate);
  buttons[0].click(); await flush();
  assert.equal(calls.length, 1);
  buttons[1].click(); await flush();
  timers.splice(timers.findIndex((timer) => timer.delay === 1000), 1)[0].fn(); await flush();
  assert.deepEqual(calls, ['passed', 'failed']);
  buttons[0].click(); await flush();
  assert.equal(calls.length, 3);
  timers.splice(timers.findIndex((timer) => timer.delay === 1000), 1)[0].fn(); await flush();
  assert.deepEqual(calls, ['passed', 'failed', 'passed', 'passed']);
});

test('an open list clears verified rows when its observation expires', async () => {
  const ids = ['passed-count', 'failed-count', 'unrecognized-count', 'results-hint', 'list-panel', 'list-title',
    'list-count', 'list-message', 'group-list', 'pagination', 'page-info', 'previous-page', 'next-page', 'close-list'];
  const nodes = new Map(ids.map((id) => ['postprocessing-' + id, new Element()]));
  const buttons = ['passed', 'failed', 'unrecognized'].map((filter) => {
    const button = new Element(); button.dataset.postprocessingFilter = filter; return button;
  });
  const timers = [], listeners = {}, revision = 'a'.repeat(64), as_of = '2026-11-01T00:00:00Z';
  let now = 1000;
  class FakeDate extends Date { constructor(...args) { super(...(args.length ? args : [now])); } static now() { return now; } }
  const context = { URL, Intl, AbortSignal, Date: FakeDate, performance: { now: () => now },
    setTimeout: (fn, delay) => { timers.push({ fn, delay }); return timers.length; }, clearTimeout() {},
    document: { getElementById: (id) => nodes.get(id), querySelectorAll: () => buttons,
      createElement: () => new Element() }, window: { addEventListener: (name, fn) => { listeners[name] = fn; } },
    fetch: async () => ({ ok: true, json: async () => ({ postprocessing_observation_remaining_ms: 40,
      postprocessing_registered_ledger: { available: true, basis: 'postprocessing-registered-ledger/1',
        current_delivery_verified: false, revision, as_of,
        results: { available: true, basis: 'postprocessing-registered-ledger/1', as_of, partial: false,
          counts: { passed: 1, failed: null, unrecognized: null },
          list: { filter: 'passed', page: 1, page_size: 20, total: 1, has_next: false,
            items: [{ group_id: 'folder-one' }] } },
        server_observation: { available: true, basis: 'paperflow-server-receipt-observation/1',
          as_of, observation_revision: revision } } }) }) };
  vm.runInNewContext(fs.readFileSync(path.join(root, 'postprocessing-results-server-20261101.js'), 'utf8'), context);
  buttons[0].click(); await new Promise(setImmediate);
  assert.match(nodes.get('postprocessing-group-list').textContent, /登记已通过/);
  assert.ok(timers.length, 'the list must have an expiration timer');
  now += 41; timers.shift().fn();
  assert.doesNotMatch(nodes.get('postprocessing-group-list').textContent, /登记已通过/);
  assert.equal(nodes.get('postprocessing-passed-count').textContent, '—');
});
