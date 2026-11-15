const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

test('the page shows a folder only at its later observed stage', async () => {
  const now = new Date().toISOString();
  const folder = { task_id: 'task-1', group_id: 'folder-1' };
  const summary = { source: 'prefect', scope: 'all_tasks', available: true,
    counts: { waiting_llm: 0, llm_processing_folders: 1, llm_body_in_progress: 1, llm_attachment_in_progress: 0 },
    llm_processing_as_of: now, waiting_llm_partial: false, llm_processing_folders_partial: false,
    llm_body_in_progress_partial: false, llm_attachment_in_progress_partial: false,
    llm_folder_positions: [{ ...folder, stage: 'llm_processing', body: true, attachment: false }] };
  const positions = { available: true, basis: 'prefect-folder-stage-positions/1', as_of: now,
    counts: { waiting_postprocessing: 1, postprocessing_in_progress: 0, waiting_rag_upload: 0 },
    partial: false, unresolved_count: 0,
    folder_positions: [{ ...folder, stage: 'waiting_postprocessing' }] };
  const nodes = new Map(['waiting-llm-count', 'waiting-llm-hint', 'llm-folder-count', 'llm-body-count',
    'llm-attachment-count', 'llm-processing-hint', 'waiting-postprocessing-count', 'waiting-postprocessing-hint']
    .map((id) => [id, { textContent: '', setAttribute() {} }]));
  const context = {
    document: { getElementById: (id) => nodes.get(id) || null },
    window: { dispatchEvent() {} }, CustomEvent: class {}, AbortSignal,
    fetch: async (url) => ({ ok: true, json: async () =>
      url.includes(':14205') ? { prefect_ticket_summary: summary } :
      url.endsWith('/postprocessing/positions') ? positions :
      url.endsWith('/postprocessing/waiting') ? { available: false } :
        { postprocessing_observation_remaining_ms: 0, postprocessing_registered_ledger: { available: false } } }),
    setInterval() {}, setTimeout() { return 1; }, clearTimeout() {},
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, 'three-card-positions.js'), 'utf8'), context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, 'prefect-ticket-counts.js'), 'utf8'), context);
  await new Promise(setImmediate);
  await new Promise(setImmediate);
  assert.equal(nodes.get('llm-folder-count').textContent, '0');
  assert.equal(nodes.get('llm-body-count').textContent, '0');
  assert.equal(nodes.get('waiting-postprocessing-count').textContent, '1');
});
