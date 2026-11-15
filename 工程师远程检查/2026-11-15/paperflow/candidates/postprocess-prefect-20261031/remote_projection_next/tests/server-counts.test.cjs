const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const script = path.resolve(__dirname, '..', 'prefect-ticket-counts.js');

function payload(verified = true) {
  const revision = 'a'.repeat(64);
  const as_of = '2026-11-01T00:00:00Z';
  return { postprocessing_registered_ledger: { available: true,
    basis: 'postprocessing-registered-ledger/1', current_delivery_verified: false,
    revision, as_of,
    stage: { available: true, basis: 'postprocessing-registered-ledger/1', as_of, partial: false,
      counts: { waiting_postprocessing: 2, postprocessing_in_progress: 99, waiting_rag_upload: 1 } },
    server_observation: { available: verified, basis: 'paperflow-server-receipt-observation/1',
      observation_revision: revision, as_of, waiting_rag_upload: verified ? 1 : null } } };
}

test('RAG waiting count requires a matching server observation and never uses local PID', () => {
  const read = require(script).readTicketCounts;
  const good = payload();
  assert.equal(read(good)[5], 1);
  assert.equal(read(good)[10], null);
  const unavailable = payload(false);
  assert.equal(read(unavailable)[5], null);
  const noProof = payload();
  delete noProof.postprocessing_registered_ledger.server_observation;
  assert.equal(read(noProof)[5], null);
  assert.equal(read(noProof)[9], null, 'old registration does not prove that its source is still available');
  const mismatched = payload();
  mismatched.postprocessing_registered_ledger.server_observation.observation_revision = 'b'.repeat(64);
  assert.equal(read(mismatched)[5], null);
  const legacy = { prefect_ticket_summary: { source: 'prefect', scope: 'all_tasks', available: true,
    postprocessing_basis: 'folder-stage-receipts/1', counts: { waiting_rag_upload: 9,
      postprocessing_in_progress: 8, waiting_postprocessing: 7 } } };
  assert.equal(read(legacy)[5], null, 'the server page must not fall back to a local receipt projection');
  assert.equal(read(legacy)[10], null, 'the server page must not fall back to a local PID');
});

test('queue hints distinguish server observation from RAG acceptance', async () => {
  const ids = ['waiting-postprocessing-count', 'waiting-postprocessing-hint', 'postprocessing-count',
    'postprocessing-hint', 'waiting-rag-upload-count', 'waiting-rag-upload-hint'];
  const nodes = new Map(ids.map((id) => [id, { textContent: '', setAttribute() {} }]));
  const context = { document: { getElementById: (id) => nodes.get(id) || null }, AbortSignal,
    window: { dispatchEvent() {} }, CustomEvent: class {},
    fetch: async () => ({ ok: true, json: async () => payload() }), setInterval() {} };
  vm.runInNewContext(fs.readFileSync(script, 'utf8'), context);
  await new Promise(setImmediate);
  assert.equal(nodes.get('waiting-rag-upload-count').textContent, '1');
  assert.equal(nodes.get('postprocessing-count').textContent, '—');
  assert.match(nodes.get('waiting-rag-upload-hint').textContent, /服务器/);
  assert.match(nodes.get('waiting-rag-upload-hint').textContent, /截至/);
  assert.doesNotMatch(nodes.get('waiting-rag-upload-hint').textContent, /已上传|已入库/);
});
