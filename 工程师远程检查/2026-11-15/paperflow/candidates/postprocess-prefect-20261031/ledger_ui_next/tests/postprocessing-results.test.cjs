const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const script = path.join(root, 'postprocessing-results.js');

class Element {
  constructor(tag = 'div') {
    this.tag = tag;
    this.children = [];
    this.dataset = {};
    this.attributes = {};
    this.listeners = {};
    this.hidden = false;
    this.disabled = false;
    this._text = '';
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map((child) => child.textContent).join(''); }
  set innerHTML(_) { throw new Error('untrusted input must not be rendered as HTML'); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this._text = ''; this.children = [...children]; }
  addEventListener(name, fn) { this.listeners[name] = fn; }
  click() { if (!this.disabled && this.listeners.click) this.listeners.click(); }
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

function registeredResult(filter, page, revision = 'a'.repeat(64)) {
  const results = result(filter, page, [{ reason: `已登记第 ${page} 页` }], 21).postprocessing_results;
  results.basis = 'postprocessing-registered-ledger/1';
  return { postprocessing_registered_ledger: { available: true,
    basis: 'postprocessing-registered-ledger/1', as_of: '2026-11-01T00:00:00Z',
    revision, current_delivery_verified: false, results } };
}

function harness(fetchImpl) {
  const ids = ['passed-count', 'failed-count', 'unrecognized-count', 'results-hint',
    'list-panel', 'list-title', 'list-count', 'list-message', 'group-list',
    'pagination', 'page-info', 'previous-page', 'next-page', 'close-list'];
  const nodes = new Map(ids.map((name) => ['postprocessing-' + name, new Element()]));
  nodes.get('postprocessing-list-panel').hidden = true;
  const buttons = ['passed', 'failed', 'unrecognized'].map((filter) => {
    const button = new Element('button');
    button.dataset.postprocessingFilter = filter;
    return button;
  });
  const listeners = {};
  const context = {
    document: {
      getElementById: (id) => {
        assert.ok(nodes.has(id), `unknown element ${id}`);
        return nodes.get(id);
      },
      querySelectorAll: (selector) => {
        assert.equal(selector, '[data-postprocessing-filter]');
        return buttons;
      },
      createElement: (tag) => new Element(tag),
    },
    window: { addEventListener: (name, fn) => { listeners[name] = fn; } },
    fetch: fetchImpl,
    URL,
    Intl,
    AbortSignal,
  };
  vm.runInNewContext(fs.readFileSync(script, 'utf8'), context, { filename: script });
  return { nodes, buttons, listeners };
}

function result(filter, page, items, total = items.length) {
  return { postprocessing_results: {
    available: true,
    counts: { passed: 21, failed: 1, unrecognized: null },
    partial: false,
    as_of: '2026-11-01T00:00:00Z',
    list: { filter, page, page_size: 20, total, items, message: '暂无资料' },
  } };
}

test('HTML adds the three result controls only to the postprocessing card', () => {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  const post = html.split('aria-labelledby="postprocessing-title"')[1].split('aria-labelledby="waiting-rag-upload-title"')[0];
  const rag = html.split('aria-labelledby="waiting-rag-upload-title"')[1].split('aria-labelledby="rag-processing-title"')[0];
  assert.match(html, /src="postprocessing-results\.js\?/);
  for (const filter of ['passed', 'failed', 'unrecognized']) {
    assert.match(post, new RegExp(`data-postprocessing-filter="${filter}"`));
  }
  assert.match(post, /id="postprocessing-list-panel"/);
  assert.doesNotMatch(rag, /data-postprocessing-filter=/);
});

test('summary shows verified counts, partial counts and unknowns without inventing zeros', () => {
  const { nodes, listeners } = harness(async () => { throw new Error('unexpected fetch'); });
  listeners['paperflow:queue-summary']({ detail: { postprocessing_results: {
    available: true, counts: { passed: 5, failed: 0, unrecognized: null }, partial: true,
  } } });
  assert.equal(nodes.get('postprocessing-passed-count').textContent, '≥5');
  assert.equal(nodes.get('postprocessing-failed-count').textContent, '≥0');
  assert.equal(nodes.get('postprocessing-unrecognized-count').textContent, '—');
  listeners['paperflow:queue-summary']({ detail: null });
  for (const name of ['passed', 'failed', 'unrecognized']) {
    assert.equal(nodes.get(`postprocessing-${name}-count`).textContent, '—');
  }
});

test('results label the full verification time', () => {
  const { nodes, listeners } = harness(async () => { throw new Error('unexpected fetch'); });
  listeners['paperflow:queue-summary']({ detail: result(null, 1, []) });
  assert.match(nodes.get('postprocessing-results-hint').textContent, /完整核验于/);
});

test('registered results are labeled as historical observations, not full verification', () => {
  const h = harness(async () => { throw new Error('unexpected fetch'); });
  h.listeners['paperflow:queue-summary']({ detail: registeredResult(null, 1) });
  assert.equal(h.nodes.get('postprocessing-passed-count').textContent, '21');
  assert.match(h.nodes.get('postprocessing-results-hint').textContent, /登记/);
  assert.match(h.nodes.get('postprocessing-results-hint').textContent, /截至/);
  assert.match(h.nodes.get('postprocessing-results-hint').textContent, /2026/);
  assert.doesNotMatch(h.nodes.get('postprocessing-results-hint').textContent, /完整核验/);
});

test('registered empty list does not claim that no unrecognized work exists', async () => {
  const h = harness(async () => {
    const payload = registeredResult('unrecognized', 1);
    payload.postprocessing_registered_ledger.results.list.total = null;
    payload.postprocessing_registered_ledger.results.list.items = [];
    return { ok: true, json: async () => payload };
  });
  h.buttons[2].click();
  await flush();
  assert.match(h.nodes.get('postprocessing-list-message').textContent, /暂无已登记/);
  assert.match(h.nodes.get('postprocessing-list-message').textContent, /待核对/);
  assert.equal(h.nodes.get('postprocessing-unrecognized-count').textContent, '—');
});

test('registered items without an identified title show that it is pending', async () => {
  const h = harness(async () => {
    const payload = registeredResult('passed', 1);
    payload.postprocessing_registered_ledger.results.list.items[0].title_status = '暂不可用';
    return { ok: true, json: async () => payload };
  });
  h.buttons[0].click();
  await flush();
  assert.match(h.nodes.get('postprocessing-group-list').textContent, /题目待核对/);
});

test('registered results send their first page revision on later pages', async () => {
  const calls = [];
  const h = harness(async (url) => {
    const q = new URL(url).searchParams;
    calls.push(q);
    return { ok: true, json: async () => registeredResult(q.get('postprocessing_result_filter'),
      Number(q.get('postprocessing_result_page'))) };
  });
  h.buttons[0].click();
  await flush();
  assert.equal(calls[0].get('postprocessing_result_revision'), null);
  h.nodes.get('postprocessing-next-page').click();
  await flush();
  assert.equal(calls[1].get('postprocessing_result_revision'), 'a'.repeat(64));
  assert.match(h.nodes.get('postprocessing-list-count').textContent, /登记/);
  assert.doesNotMatch(h.nodes.get('postprocessing-list-count').textContent, /核验结果/);
});

test('a changed ledger revision clears stale rows and returns to the first page', async () => {
  const calls = [];
  const h = harness(async (url) => {
    const q = new URL(url).searchParams;
    const page = Number(q.get('postprocessing_result_page'));
    calls.push(page);
    if (page === 2) return { ok: true, json: async () => ({ postprocessing_registered_ledger: {
      available: false, basis: 'postprocessing-registered-ledger/1', reason: 'snapshot_changed',
      results: { available: false, message: '登记结果已更新' } } }) };
    return { ok: true, json: async () => registeredResult('passed', 1, calls.length === 1 ? 'a'.repeat(64) : 'b'.repeat(64)) };
  });
  h.buttons[0].click();
  await flush();
  h.nodes.get('postprocessing-next-page').click();
  await flush();
  await flush();
  assert.deepEqual(calls, [1, 2, 1]);
  assert.match(h.nodes.get('postprocessing-page-info').textContent, /第 1/);
  assert.doesNotMatch(h.nodes.get('postprocessing-group-list').textContent, /第 2 页/);
});

test('three filters open real paginated results, safe diagnostics and close', async () => {
  const calls = [];
  const item = { task_id: 'task-1', group_id: 'group-1', task_name: '合成任务', folder_label: '合成文件夹',
    title_status: '已提取', paper_title: '<img src=x onerror=alert(1)>', reason: '合成失败原因',
    order_note: '需核对', gaps: [{ message: '合成缺口' }], warnings: ['合成警告'],
    diagnostics: ['合成诊断'] };
  const h = harness(async (url) => {
    const parsed = new URL(url);
    calls.push(parsed);
    const filter = parsed.searchParams.get('postprocessing_result_filter');
    const page = Number(parsed.searchParams.get('postprocessing_result_page'));
    assert.equal(parsed.searchParams.get('postprocessing_result_page_size'), '20');
    return { ok: true, json: async () => result(filter, page, [item], filter === 'passed' ? 21 : 1) };
  });
  for (const [index, filter] of ['passed', 'failed', 'unrecognized'].entries()) {
    h.buttons[index].click();
    await flush();
    assert.equal(h.nodes.get('postprocessing-list-panel').hidden, false);
    assert.equal(calls.at(-1).searchParams.get('postprocessing_result_filter'), filter);
    const text = h.nodes.get('postprocessing-group-list').textContent;
    for (const expected of ['合成任务', '合成文件夹', '<img src=x onerror=alert(1)>', '合成失败原因', '合成缺口', '合成警告', '合成诊断']) {
      assert.ok(text.includes(expected), `missing ${expected}`);
    }
  }
  h.buttons[0].click();
  await flush();
  assert.equal(h.nodes.get('postprocessing-pagination').hidden, false);
  h.nodes.get('postprocessing-next-page').click();
  await flush();
  assert.equal(calls.at(-1).searchParams.get('postprocessing_result_page'), '2');
  assert.equal(h.nodes.get('postprocessing-next-page').disabled, true);
  h.nodes.get('postprocessing-previous-page').click();
  await flush();
  assert.equal(calls.at(-1).searchParams.get('postprocessing_result_page'), '1');
  h.nodes.get('postprocessing-close-list').click();
  assert.equal(h.nodes.get('postprocessing-list-panel').hidden, true);
  assert.ok(h.buttons.every((button) => button.attributes['aria-pressed'] === 'false'));
});

test('unavailable lists do not show stale rows', async () => {
  const h = harness(async () => ({ ok: true, json: async () => ({ postprocessing_results: {
    available: false, message: '暂时无法核验', list: { filter: 'passed', page: 1, page_size: 20, total: 1,
      items: [{ task_name: '不得显示' }] },
  } }) }));
  h.buttons[0].click();
  await flush();
  assert.equal(h.nodes.get('postprocessing-group-list').children.length, 0);
  assert.equal(h.nodes.get('postprocessing-list-message').textContent, '暂时无法核验');
  assert.equal(h.nodes.get('postprocessing-passed-count').textContent, '—');
});

test('a mismatched filter, page or page size cannot be presented as the requested result', async () => {
  for (const change of [{ filter: 'failed' }, { page: 2 }, { page_size: 5 }]) {
    const h = harness(async () => {
      const payload = result('passed', 1, [{ task_name: '不得显示' }], 1);
      Object.assign(payload.postprocessing_results.list, change);
      return { ok: true, json: async () => payload };
    });
    h.buttons[0].click();
    await flush();
    assert.equal(h.nodes.get('postprocessing-group-list').children.length, 0);
    assert.equal(h.nodes.get('postprocessing-list-count').textContent, '暂不可用');
  }
});

test('an unknown total can still page through explicitly confirmed results', async () => {
  const calls = [];
  const h = harness(async (url) => {
    const query = new URL(url).searchParams;
    const page = Number(query.get('postprocessing_result_page'));
    calls.push(page);
    const payload = result('unrecognized', page, [{ reason: `第 ${page} 页` }], null);
    payload.postprocessing_results.list.has_next = page === 1;
    return { ok: true, json: async () => payload };
  });
  h.buttons[2].click();
  await flush();
  assert.equal(h.nodes.get('postprocessing-pagination').hidden, false);
  assert.equal(h.nodes.get('postprocessing-next-page').disabled, false);
  h.nodes.get('postprocessing-next-page').click();
  await flush();
  assert.deepEqual(calls, [1, 2]);
  assert.equal(h.nodes.get('postprocessing-next-page').disabled, true);
  assert.equal(h.nodes.get('postprocessing-previous-page').disabled, false);
});

test('late replies cannot reopen a closed list or replace a newer selection', async () => {
  const pending = [];
  const h = harness((url) => new Promise((resolve) => pending.push({ url: new URL(url), resolve })));
  h.buttons[0].click();
  h.buttons[1].click();
  pending[1].resolve({ ok: true, json: async () => result('failed', 1, [{ reason: '新结果' }], 1) });
  await flush();
  pending[0].resolve({ ok: true, json: async () => result('passed', 1, [{ reason: '过期结果' }], 1) });
  await flush();
  assert.ok(h.nodes.get('postprocessing-group-list').textContent.includes('新结果'));
  assert.ok(!h.nodes.get('postprocessing-group-list').textContent.includes('过期结果'));
  h.buttons[2].click();
  h.nodes.get('postprocessing-close-list').click();
  pending[2].resolve({ ok: true, json: async () => result('unrecognized', 1, [{ reason: '关闭后结果' }], 1) });
  await flush();
  assert.equal(h.nodes.get('postprocessing-list-panel').hidden, true);
  assert.ok(!h.nodes.get('postprocessing-group-list').textContent.includes('关闭后结果'));
});
