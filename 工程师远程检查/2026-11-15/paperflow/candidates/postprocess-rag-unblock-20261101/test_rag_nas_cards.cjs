const assert = require('node:assert/strict');
const { readTicketCounts } = require('./prefect-ticket-counts-server-20261101.js');
const { mergeThreeCardPositions } = require('./three-card-positions-20261101.js');

const counts = { waiting_postprocessing: 0, postprocessing_in_progress: 0, waiting_rag_upload: 1,
  rag_in_progress: 2, waiting_nas_archive_delete: 3, nas_in_progress: 4, completed: 5 };
const positions = { available: true, basis: 'prefect-folder-stage-positions/1', partial: false,
  unresolved_count: 0, downstream_available: true, as_of: '2026-11-01T21:00:01Z', counts,
  folder_positions: [{ task_id: 'task', group_id: 'group', stage: 'rag_in_progress' }] };
const summary = { source: 'prefect', scope: 'all_tasks', available: true,
  llm_processing_as_of: '2026-11-01T21:00:00Z', llm_folder_positions: [
    { task_id: 'task', group_id: 'group', stage: 'waiting_llm' }],
  waiting_llm_partial: false, llm_processing_folders_partial: false,
  llm_body_in_progress_partial: false, llm_attachment_in_progress_partial: false };

let actual = readTicketCounts({ prefect_ticket_summary: summary, prefect_postprocessing_positions: positions });
assert.deepEqual(actual.slice(5, 9), [1, 2, 7, 5]);
assert.equal(mergeThreeCardPositions(summary, positions).waiting_llm, 0);
actual = readTicketCounts({ prefect_ticket_summary: summary,
  prefect_postprocessing_positions: { ...positions, downstream_available: false } });
assert.deepEqual(actual.slice(6, 9), [null, null, null]);
