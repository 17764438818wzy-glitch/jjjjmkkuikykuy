const assert = require('node:assert/strict');
const test = require('node:test');
const { mergeThreeCardPositions } = require('./three-card-positions.js');

const item = (stage, body = false, attachment = false) =>
  ({ task_id: 'task-1', group_id: 'folder-1', stage, body, attachment });
const llm = (items, asOf = '2026-11-01T10:00:00Z') => ({
  source: 'prefect', scope: 'all_tasks', available: true, llm_processing_as_of: asOf,
  llm_folder_positions: items, waiting_llm_partial: false, llm_processing_folders_partial: false,
  llm_body_in_progress_partial: false, llm_attachment_in_progress_partial: false,
});
const post = (items, asOf = '2026-11-01T10:00:02Z') => ({
  available: true, basis: 'prefect-folder-stage-positions/1', partial: false, as_of: asOf,
  folder_positions: items,
});

test('a folder at the LLM handoff is counted once in LLM', () => {
  const result = mergeThreeCardPositions(llm([item('llm_processing')]), post([item('upstream')]));
  assert.equal(result.llm_processing_folders, 1);
  assert.equal(result.waiting_llm, 0);
  assert.equal(result.waiting_postprocessing, 0);
});

test('a folder moved to waiting postprocessing leaves LLM', () => {
  const result = mergeThreeCardPositions(llm([item('llm_processing', true)]), post([item('waiting_postprocessing')]));
  assert.equal(result.llm_processing_folders, 0);
  assert.equal(result.llm_body_in_progress, 0);
  assert.equal(result.waiting_postprocessing, 1);
});

test('two request types on one folder produce one processing ticket', () => {
  const result = mergeThreeCardPositions(llm([item('llm_processing', true, true)]), post([]));
  assert.equal(result.llm_processing_folders, 1);
  assert.equal(result.llm_body_in_progress, 1);
  assert.equal(result.llm_attachment_in_progress, 1);
});

test('an unknown request is separate from the known running ticket', () => {
  const result = mergeThreeCardPositions(llm([item('llm_processing', null, false)]), post([]));
  assert.equal(result.llm_processing_folders, 1);
  assert.equal(result.llm_body_in_progress, null);
  assert.equal(result.llm_body_in_progress_partial, true);
});

test('an upstream handoff not yet in the LLM snapshot still shows its ticket', () => {
  const result = mergeThreeCardPositions(llm([]), post([item('upstream')]));
  assert.equal(result.llm_processing_folders, 1);
  assert.equal(result.llm_body_in_progress_partial, true);
  assert.equal(result.llm_attachment_in_progress_partial, true);
});

test('a later retry waiting in LLM overrides an older handoff observation', () => {
  const result = mergeThreeCardPositions(llm([item('waiting_llm')], '2026-11-01T10:00:04Z'), post([item('upstream')]));
  assert.equal(result.waiting_llm, 1);
  assert.equal(result.llm_processing_folders, 0);
});

test('an older handoff followed by an absent newer LLM ticket does not claim an empty postprocessing queue', () => {
  const result = mergeThreeCardPositions(llm([], '2026-11-01T10:00:04Z'), post([item('upstream')]));
  assert.equal(result.waiting_postprocessing, null);
  assert.equal(result.waiting_postprocessing_partial, true);
});

test('a newer LLM retry moves a ticket out of an older waiting-postprocessing snapshot', () => {
  const result = mergeThreeCardPositions(llm([item('llm_processing', true)], '2026-11-01T10:00:04Z'),
    post([item('waiting_postprocessing')]));
  assert.equal(result.llm_processing_folders, 1);
  assert.equal(result.waiting_postprocessing, 0);
});

test('missing snapshots never imply a confirmed zero', () => {
  const result = mergeThreeCardPositions(null, null);
  assert.equal(result.waiting_llm, null);
  assert.equal(result.llm_processing_folders, null);
  assert.equal(result.waiting_postprocessing, null);
});

test('an incomplete LLM status contract does not imply an empty queue', () => {
  const snapshot = llm([]);
  delete snapshot.llm_processing_folders_partial;
  const result = mergeThreeCardPositions(snapshot, post([]));
  assert.equal(result.waiting_llm, null);
  assert.equal(result.llm_processing_folders, null);
});

test('partial empty observations and unrecognized positions are not shown as zero', () => {
  const partial = { ...llm([]), llm_processing_folders_partial: true, waiting_llm_partial: true };
  const unresolved = { ...post([]), partial: true };
  const result = mergeThreeCardPositions(partial, unresolved);
  assert.equal(result.waiting_llm, null);
  assert.equal(result.llm_processing_folders, null);
  assert.equal(result.waiting_postprocessing, null);
  assert.equal(mergeThreeCardPositions(llm([]), post([item('unexpected')])).waiting_postprocessing, null);
});
