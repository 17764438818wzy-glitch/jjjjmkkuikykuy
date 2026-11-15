/* Merge read-only folder positions so a ticket contributes to one stage. */
(() => {
  'use strict';

  function mergeThreeCardPositions(summary, post) {
    const key = (item) => `${item.task_id}\u0000${item.group_id}`;
    const validItem = (item) => item && typeof item.task_id === 'string' && item.task_id &&
      typeof item.group_id === 'string' && item.group_id && typeof item.stage === 'string';
    const unique = (items) => Array.isArray(items) && items.every(validItem) &&
      new Set(items.map(key)).size === items.length;
    const llmTime = Date.parse(summary?.llm_processing_as_of);
    const postTime = Date.parse(post?.as_of);
    const llmValid = summary?.available === true && summary.source === 'prefect' && summary.scope === 'all_tasks' &&
      Number.isFinite(llmTime) && unique(summary.llm_folder_positions) &&
      ['waiting_llm_partial', 'llm_processing_folders_partial', 'llm_body_in_progress_partial',
        'llm_attachment_in_progress_partial'].every((name) => typeof summary[name] === 'boolean') &&
      summary.llm_folder_positions.every((item) => ['waiting_llm', 'llm_processing'].includes(item.stage));
    const postStages = new Set(['waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload',
      'upstream', 'held', 'failed', 'unresolved']);
    const postValid = post?.available === true && post.basis === 'prefect-folder-stage-positions/1' &&
      Number.isFinite(postTime) && typeof post.partial === 'boolean' && unique(post.folder_positions) &&
      post.folder_positions.every((item) => postStages.has(item.stage));
    const postById = new Map((postValid ? post.folder_positions : []).map((item) => [key(item), item]));
    const llmById = new Map((llmValid ? summary.llm_folder_positions : []).map((item) => [key(item), item]));
    const downstream = new Set(['waiting_postprocessing', 'postprocessing_in_progress',
      'waiting_rag_upload', 'held', 'failed']);
    let waiting = 0, processing = 0, body = 0, attachment = 0;
    let waitingPost = postValid ? post.folder_positions.filter((item) => item.stage === 'waiting_postprocessing').length : null;
    let waitingPostPartial = !postValid || post.partial;
    let waitingPartial = !llmValid || summary?.waiting_llm_partial === true;
    let processingPartial = !llmValid || summary?.llm_processing_folders_partial === true;
    let bodyPartial = !llmValid || summary?.llm_body_in_progress_partial === true;
    let attachmentPartial = !llmValid || summary?.llm_attachment_in_progress_partial === true;

    function addProcessing(item) {
      processing += 1;
      if (item.body === true) body += 1;
      else if (item.body !== false) bodyPartial = true;
      if (item.attachment === true) attachment += 1;
      else if (item.attachment !== false) attachmentPartial = true;
    }

    for (const [id, item] of llmById) {
      const later = postById.get(id);
      if (later?.stage === 'waiting_postprocessing' && llmTime > postTime) {
        waitingPost -= 1;
      } else if (later && downstream.has(later.stage)) {
        continue;
      }
      if (later?.stage === 'unresolved' && postTime > llmTime) {
        waitingPartial = processingPartial = true;
        continue;
      }
      if (later?.stage === 'upstream' && postTime >= llmTime && item.stage !== 'llm_processing') {
        addProcessing({});
      } else if (item.stage === 'waiting_llm') {
        waiting += 1;
      } else {
        addProcessing(item);
      }
    }
    for (const [id, item] of postById) {
      if (item.stage !== 'upstream' || llmById.has(id)) continue;
      if (llmValid && llmTime > postTime) {
        processingPartial = true;
        waitingPostPartial = true;
        continue;
      }
      addProcessing({});
    }

    const known = (value, partial) => partial && value === 0 ? null : value;
    return {
      waiting_llm: llmValid ? known(waiting, waitingPartial) : null,
      waiting_llm_partial: waitingPartial,
      llm_processing_folders: llmValid || processing ? known(processing, processingPartial) : null,
      llm_processing_folders_partial: processingPartial,
      llm_body_in_progress: llmValid || processing ? known(body, bodyPartial) : null,
      llm_body_in_progress_partial: bodyPartial,
      llm_attachment_in_progress: llmValid || processing ? known(attachment, attachmentPartial) : null,
      llm_attachment_in_progress_partial: attachmentPartial,
      waiting_postprocessing: waitingPost === null ? null : known(waitingPost, waitingPostPartial),
      waiting_postprocessing_partial: waitingPostPartial,
    };
  }

  if (typeof module !== 'undefined' && module.exports) module.exports = { mergeThreeCardPositions };
  if (typeof globalThis !== 'undefined') globalThis.paperflowMergeThreeCardPositions = mergeThreeCardPositions;
})();
