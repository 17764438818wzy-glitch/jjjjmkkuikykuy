const assert = require('node:assert/strict');
const path = require('node:path');
const test = require('node:test');
const script = path.resolve(__dirname, '..', 'site', 'postprocessing-results-server-20261101.js');

function payload(withServer = true) {
  const revision = 'a'.repeat(64), as_of = '2026-11-01T00:00:00Z';
  const ledger = { available: true, basis: 'postprocessing-registered-ledger/1',
    current_delivery_verified: false, revision, as_of,
    results: { available: true, basis: 'postprocessing-registered-ledger/1', as_of, partial: false,
      counts: { passed: 2, failed: 1, unrecognized: 0 },
      list: { filter: 'failed', page: 1, page_size: 20, total: 1, has_next: false,
        items: [{ reason: '已登记未通过' }] } } };
  if (withServer) ledger.server_observation = { available: true,
    basis: 'paperflow-server-receipt-observation/1', as_of, observation_revision: revision };
  return { postprocessing_registered_ledger: ledger };
}

test('result counts cannot fall back to old local evidence or unverified registration', () => {
  const resultFrom = require(script).resultFrom;
  assert.equal(resultFrom({ postprocessing_results: { available: true, counts: { passed: 8 } } }).available, false);
  const pending = resultFrom(payload(false));
  assert.equal(pending.counts.passed, null);
  assert.equal(pending.counts.failed, null);
  assert.equal(pending.counts.unrecognized, null);
  assert.equal(pending.list.total, null);
  assert.match(pending.list.items[0].reason, /待核验/);
  const verified = resultFrom(payload(true));
  assert.equal(verified.counts.passed, 2);
  assert.equal(verified.counts.failed, null);
});
