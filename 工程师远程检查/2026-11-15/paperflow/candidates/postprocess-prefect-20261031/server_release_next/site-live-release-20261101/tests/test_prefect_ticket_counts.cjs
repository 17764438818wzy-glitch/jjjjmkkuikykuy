const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const script = path.join(__dirname, '..', 'prefect-ticket-counts.js');

function reader() {
  assert.ok(fs.existsSync(script), 'Prefect ticket count reader has not been implemented');
  return require(script).readTicketCounts;
}

function snapshot(waiting, processing, llm) {
  return { prefect_ticket_summary: { source: 'prefect', scope: 'all_tasks', available: true,
    counts: { waiting_preprocessing: waiting, preprocessing_in_progress: processing, waiting_llm: llm } } };
}

function firstThree(waiting, processing, llm) {
  return [waiting, processing, llm, ...Array(8).fill(null)];
}

test('uses one backend snapshot for the three Prefect ticket stages', () => {
  const read = reader();
  assert.deepEqual(read(snapshot(309, 0, 0)), firstThree(309, 0, 0));
  assert.deepEqual(read(snapshot(308, 1, 0)), firstThree(308, 1, 0));
  assert.deepEqual(read(snapshot(308, 0, 1)), firstThree(308, 0, 1));
});

test('does not use legacy local queue numbers as Prefect ticket counts', () => {
  const read = reader();
  assert.deepEqual(read({ waiting_available: true, waiting_count: 309, in_progress_count: 0 }), Array(11).fill(null));
  assert.deepEqual(read({ prefect_ticket_summary: { source: 'prefect', scope: 'all_tasks', available: false,
    counts: { waiting_preprocessing: 309, preprocessing_in_progress: 0, waiting_llm: 0 } } }), Array(11).fill(null));
});

test('distinguishes a verified zero from missing or invalid counts', () => {
  const read = reader();
  assert.deepEqual(read(snapshot(0, null, -1)), firstThree(0, null, null));
  assert.deepEqual(read(snapshot('0', false, 1.5)), Array(11).fill(null));
});

test('reads later stages including postprocessing from the same Prefect snapshot', () => {
  const read = reader();
  const payload = snapshot(301, 1, 2);
  Object.assign(payload.prefect_ticket_summary.counts, {
    llm_attachment_in_progress: 3,
    llm_body_in_progress: 2,
    waiting_rag_upload: 1,
    rag_in_progress: 4,
    waiting_nas_archive_delete: 5,
    completed: 6,
    waiting_postprocessing: 7,
    postprocessing_in_progress: 9,
  });
  assert.deepEqual(read(payload), [301, 1, 2, 3, 2, 1, 4, 5, 6, 7, 9]);
});

test('renders every stage from ticket data and clears stale values on failure', async () => {
  const ids = [
    'waiting-preprocessing-count', 'waiting-preprocessing-hint', 'preprocessing-count', 'preprocessing-hint',
    'waiting-llm-count', 'waiting-llm-hint', 'llm-attachment-count', 'llm-body-count', 'llm-processing-hint',
    'waiting-postprocessing-count', 'waiting-postprocessing-hint', 'postprocessing-count', 'postprocessing-hint',
    'waiting-rag-upload-count', 'waiting-rag-upload-hint', 'rag-processing-count', 'rag-processing-hint',
    'waiting-nas-archive-count', 'waiting-nas-archive-hint', 'completed-count', 'completed-hint',
    'preprocessing-passed-count', 'preprocessing-failed-count', 'preprocessing-unrecognized-count',
    'preprocessing-results-hint', 'passed-count',
  ];
  const nodes = new Map(ids.map((id) => [id, { textContent: '', setAttribute() {} }]));
  nodes.get('passed-count').textContent = '309';
  nodes.get('preprocessing-passed-count').textContent = '8';
  nodes.get('preprocessing-results-hint').textContent = '独立结果';
  const payload = snapshot(301, 1, 2);
  Object.assign(payload.prefect_ticket_summary.counts, {
    llm_attachment_in_progress: 3, llm_body_in_progress: 2, waiting_rag_upload: 1,
    rag_in_progress: 4, waiting_nas_archive_delete: 5, completed: 6,
    waiting_postprocessing: 7, postprocessing_in_progress: 9,
  });
  const replies = [{ waiting_count: 309, in_progress_count: 0 }, payload, new Error('offline')];
  let repeat;
  const events = [];
  const context = {
    document: { getElementById: (id) => nodes.get(id) || null }, AbortSignal,
    window: { dispatchEvent: (event) => events.push(event) },
    CustomEvent: class { constructor(type, init) { this.type = type; this.detail = init.detail; } },
    fetch: async () => {
      const reply = replies.shift();
      if (reply instanceof Error) throw reply;
      return { ok: true, json: async () => reply };
    },
    setInterval: (callback) => { repeat = callback; },
  };
  vm.runInNewContext(fs.readFileSync(script, 'utf8'), context);
  await new Promise(setImmediate);
  assert.equal(nodes.get('waiting-preprocessing-count').textContent, '—');
  assert.equal(nodes.get('rag-processing-count').textContent, '—');
  await repeat();
  for (const [id, expected] of [
    ['waiting-preprocessing-count', '301'], ['preprocessing-count', '1'], ['waiting-llm-count', '2'],
    ['llm-attachment-count', '3'], ['llm-body-count', '2'], ['waiting-rag-upload-count', '1'],
    ['rag-processing-count', '4'], ['waiting-nas-archive-count', '5'], ['completed-count', '6'],
    ['waiting-postprocessing-count', '7'], ['postprocessing-count', '9'],
  ]) assert.equal(nodes.get(id).textContent, expected, id);
  assert.equal(nodes.get('preprocessing-results-hint').textContent, '独立结果');
  assert.equal(events.at(-1).type, 'paperflow:queue-summary');
  assert.equal(nodes.get('passed-count').textContent, '309');
  await repeat();
  assert.equal(nodes.get('completed-count').textContent, '—');
  assert.equal(nodes.get('waiting-postprocessing-count').textContent, '—');
  assert.equal(nodes.get('postprocessing-count').textContent, '—');
  assert.equal(nodes.get('passed-count').textContent, '309');
});
