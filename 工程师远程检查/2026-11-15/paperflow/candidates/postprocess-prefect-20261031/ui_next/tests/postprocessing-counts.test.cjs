const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const script = path.join(root, 'prefect-ticket-counts.js');

function payload(basis = undefined) {
  return { prefect_ticket_summary: { source: 'prefect', scope: 'all_tasks', available: true,
    postprocessing_basis: basis,
    counts: { waiting_preprocessing: 300, preprocessing_in_progress: 2, waiting_llm: 4,
      llm_attachment_in_progress: 1, llm_body_in_progress: 5,
      waiting_postprocessing: 8, postprocessing_in_progress: 3, waiting_rag_upload: 6,
      postprocessing_held: 1, postprocessing_unresolved: 2 } } };
}

test('postprocessing numbers require the verified stage basis', () => {
  const read = require(script).readTicketCounts;
  const missing = read(payload());
  assert.equal(missing[5], null);
  assert.equal(missing[9], null);
  assert.equal(missing[10], null);
  const verified = read(payload('folder-stage-receipts/1'));
  assert.equal(verified[5], 6);
  assert.equal(verified[9], 8);
  assert.equal(verified[10], 3);
  assert.deepEqual(verified.slice(0, 5), [300, 2, 4, 1, 5]);
  const invalid = payload('folder-stage-receipts/1');
  invalid.prefect_ticket_summary.counts.postprocessing_in_progress = null;
  assert.equal(read(invalid)[10], null);
});

test('refresh preserves other stage behavior, diagnostics, and clears on failure', async () => {
  const ids = ['waiting-preprocessing-count', 'waiting-preprocessing-hint', 'preprocessing-count', 'preprocessing-hint',
    'waiting-llm-count', 'waiting-llm-hint', 'llm-attachment-count', 'llm-body-count', 'llm-processing-hint',
    'waiting-postprocessing-count', 'waiting-postprocessing-hint', 'postprocessing-count', 'postprocessing-hint',
    'waiting-rag-upload-count', 'waiting-rag-upload-hint', 'rag-processing-count', 'rag-processing-hint',
    'waiting-nas-archive-count', 'waiting-nas-archive-hint', 'completed-count', 'completed-hint'];
  const nodes = new Map(ids.map((id) => [id, { textContent: '', setAttribute() {} }]));
  const response = payload('folder-stage-receipts/1');
  Object.assign(response.prefect_ticket_summary, { llm_body_in_progress_partial: true,
    llm_processing_as_of: '2026-11-01T00:00:00Z', postprocessing_as_of: '2026-11-01T00:00:00Z' });
  const replies = [response, new Error('offline')];
  let repeat;
  const events = [];
  const context = { document: { getElementById: (id) => nodes.get(id) || null }, AbortSignal,
    window: { dispatchEvent: (event) => events.push(event) },
    CustomEvent: class { constructor(type, init) { this.type = type; this.detail = init.detail; } },
    fetch: async () => { const reply = replies.shift(); if (reply instanceof Error) throw reply;
      return { ok: true, json: async () => reply }; },
    setInterval: (callback) => { repeat = callback; },
  };
  vm.runInNewContext(fs.readFileSync(script, 'utf8'), context);
  await new Promise(setImmediate);
  assert.equal(nodes.get('waiting-postprocessing-count').textContent, '8');
  assert.equal(nodes.get('postprocessing-count').textContent, '3');
  assert.equal(nodes.get('waiting-rag-upload-count').textContent, '6');
  assert.match(nodes.get('waiting-postprocessing-hint').textContent, /已登记等待/);
  assert.match(nodes.get('waiting-postprocessing-hint').textContent, /实际处理前复核原回答/);
  assert.match(nodes.get('waiting-postprocessing-hint').textContent, /交接材料完整核验于/);
  assert.equal(nodes.get('llm-body-count').textContent, '≥5');
  assert.match(nodes.get('llm-processing-hint').textContent, /其他工单或请求待核对/);
  assert.match(nodes.get('postprocessing-hint').textContent, /暂存待核对 1 组/);
  assert.match(nodes.get('postprocessing-hint').textContent, /状态待核对 2 组/);
  for (const name of ['waiting-postprocessing-hint', 'postprocessing-hint', 'waiting-rag-upload-hint']) {
    assert.match(nodes.get(name).textContent, /完整核验于/);
  }
  assert.equal(events.at(-1).type, 'paperflow:queue-summary');
  await repeat();
  for (const name of ['waiting-postprocessing-count', 'postprocessing-count', 'waiting-rag-upload-count']) {
    assert.equal(nodes.get(name).textContent, '—');
  }
});

test('candidate HTML changes the count-script cache version', () => {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  assert.match(html, /prefect-ticket-counts\.js\?v=20261101-postprocessing-3/);
  assert.match(html, /llm-results\.js\?v=20261031-folder-details-1/);
});
