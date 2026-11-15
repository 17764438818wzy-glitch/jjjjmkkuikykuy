const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '..');

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
  const context = { URL, Intl, AbortSignal,
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
        list: { filter, page, page_size: 20, total: filter === 'passed' ? 21 : null,
          has_next: filter === 'passed' && page === 1,
          items: [{ group_id: `folder-${page}`, task_name: '合成大工单', title_status: '已提取',
            paper_title: '<img src=x>', reason: '已登记结果' }] } };
      return { ok: true, json: async () => ({ postprocessing_registered_ledger: { available: true,
        basis: 'postprocessing-registered-ledger/1', current_delivery_verified: false, revision, as_of,
        results: result, server_observation: { available: true,
          basis: 'paperflow-server-receipt-observation/1', as_of, observation_revision: revision } } }) };
    } };
  vm.runInNewContext(fs.readFileSync(path.join(root, 'postprocessing-results.js'), 'utf8'), context);
  const flush = () => new Promise(setImmediate);
  buttons[0].click(); await flush();
  assert.equal(nodes.get('postprocessing-list-panel').hidden, false);
  assert.match(nodes.get('postprocessing-group-list').textContent, /合成大工单/);
  assert.match(nodes.get('postprocessing-group-list').textContent, /<img src=x>/);
  nodes.get('postprocessing-next-page').click(); await flush();
  assert.equal(calls.at(-1).get('postprocessing_result_page'), '2');
  assert.equal(calls.at(-1).get('postprocessing_result_revision'), revision);
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
