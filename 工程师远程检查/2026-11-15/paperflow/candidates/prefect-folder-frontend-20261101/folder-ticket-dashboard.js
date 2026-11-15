(() => {
  'use strict';

  const stages = {
    waiting_preprocessing: 'waiting-preprocessing-count',
    preprocessing_in_progress: 'preprocessing-count',
    waiting_llm: 'waiting-llm-count',
    llm_in_progress: 'llm-folder-count',
    waiting_postprocessing: 'waiting-postprocessing-count',
    postprocessing_in_progress: 'postprocessing-count',
    waiting_rag_upload: 'waiting-rag-upload-count',
    rag_in_progress: 'rag-processing-count',
    completed: 'completed-count',
  };
  const hints = [
    'waiting-preprocessing-hint', 'preprocessing-hint', 'waiting-llm-hint',
    'llm-processing-hint', 'waiting-postprocessing-hint', 'postprocessing-hint',
    'waiting-rag-upload-hint', 'rag-processing-hint', 'waiting-nas-archive-hint', 'cleanup-hint', 'completed-hint',
  ];
  const sections = {
    preprocessing: '前处理',
    llm: 'LLM',
    postprocessing: '后处理',
    rag: 'RAG',
    nas: 'NAS 归档',
    cleanup: '本机清理',
  };
  const downstreamSections = new Set(['rag', 'nas', 'cleanup']);
  const resultLabels = { passed: '已通过', failed: '失败', unrecognized: '未识别' };
  const classificationLabels = { ...resultLabels, unresolved: '待核对' };
  const failedStages = { preprocessing: '前处理', llm: 'LLM', postprocessing: '后处理', rag: 'RAG', nas: 'NAS', cleanup: '清理' };
  const retryStatuses = { scheduled: '排队中', running: '处理中', completed: '已完成', failed: '失败',
    dispatch_unknown: '派发待核对', unresolved: '状态待核对' };
  const taskStates = {
    created: '未开始', waiting: '等待中', running: '运行中', paused: '已暂停', pausing: '暂停中',
    completed: '已完成', cancelled: '已结束', failed: '存在失败', unrecognized: '存在未识别', unresolved: '状态待核对',
  };
  const checkLabels = { prefect_deployment: 'Prefect 工单部署', paper_deployment: '论文小工单部署', prefect_storage: 'Prefect 记录服务',
    project_config: '项目配置', execution_enabled: '项目执行开关', executor: '执行进程', global_limits: '共享资源额度',
    local_scripts: '处理脚本与配置', nimble: '第一步服务', model_gateway: '模型路由', gateway_seats: '出口席位配置', upstream_accounts: '上游账号连通测试', rag: 'RAG', nas: 'NAS', pipeline: '处理流程接线' };
  const stageNames = new Set([...Object.keys(stages), 'waiting_nas_archive_delete', 'nas_in_progress', 'waiting_cleanup', 'cleanup_in_progress', 'failed', 'unresolved']);
  const resultNames = new Set([null, ...Object.keys(resultLabels)]);
  const lists = Object.fromEntries(Object.keys(sections).map((section) => [section, { filter: null, page: 1, rows: [], requestVersion: 0 }]));
  const element = (id) => document.getElementById(id);
  const number = new Intl.NumberFormat('zh-CN');
  let snapshot = null;
  let snapshotComplete = false;
  let updateFailed = false;
  let apiUrl = '';
  let tasksApiUrl = '';
  let controlApiUrl = '';
  let queryApiUrl = document.documentElement?.dataset.queryMode === 'summary' ? 'http://127.0.0.1:8826' : '';
  let detailFolders = [];
  let failedDetailFolders = [];
  let deleteUnindexedAllowed = false;
  let automaticGatewayCapacity = false;
  let selectedTask = null;
  let selectionVersion = 0;
  let folderRequestVersion = 0;
  let folderReadPending = false;
  let lastPollAt = 0;
  let classificationRequestVersion = 0;
  let preflightReady = false;
  let classifications = null;
  let classificationComplete = false;
  let classificationUpdateFailed = false;
  const classificationList = { filter: null, page: 1 };
  const pendingCreateKey = 'paperflow-pending-task-create';
  const lastTaskKey = 'paperflow-last-selected-task-v1';
  const lastConfigKey = 'paperflow-last-api-config-v1';
  const deletedTaskIds = new Set();
  const retryMessages = new Map();
  const retryChecks = new Map();
  let storageFailed = false;
  const queryStore = globalThis.PaperflowQuery.createQueryStore(localStorage, () => {
    storageFailed = true;
    element('save-state').textContent = '本机记录未保存或无法恢复';
  });

  function sourceId() { return controlApiUrl || tasksApiUrl || apiUrl; }
  function queryKey(kind, taskId = '') { return ['paperflow', sourceId(), kind, taskId]; }
  function knownOffline() { return typeof navigator !== 'undefined' && navigator.onLine === false; }

  function saveLocal(key, value) {
    try { localStorage.setItem(key, JSON.stringify(value)); return true; }
    catch (_) {
      storageFailed = true;
      element('save-state').textContent = '本机记录未保存或无法恢复';
      return false;
    }
  }

  function readLocal(key) {
    try { return JSON.parse(localStorage.getItem(key) || 'null'); } catch (_) { return null; }
  }

  function controlKey(taskId) {
    return `paperflow-control:${encodeURIComponent(controlApiUrl)}:${taskId}`;
  }

  function readControlState(taskId) {
    const raw = localStorage.getItem(controlKey(taskId));
    if (!raw) return { intent_seq: 0, pending: [] };
    const value = JSON.parse(raw);
    if (!Number.isSafeInteger(value.intent_seq) || value.intent_seq < 0 || !Array.isArray(value.pending) ||
        value.pending.some((entry) => !entry || typeof entry.request_id !== 'string' ||
          !['start', 'pause', 'delete'].includes(entry.action) || !Number.isSafeInteger(entry.intent_seq))) {
      throw new Error('操作记录无法读取');
    }
    return value;
  }

  function writeControlState(taskId, state) {
    if (deletedTaskIds.has(taskId)) return;
    localStorage.setItem(controlKey(taskId), JSON.stringify(state));
  }

  function pendingControls(taskId) {
    try { return readControlState(taskId).pending; } catch (_) { return null; }
  }

  async function checkControlOperation(taskId, operation) {
    if (operation.action === 'delete') return checkTaskDeletion(taskId, operation);
    function invalidatePauseAcknowledgment() {
      if (operation.action !== 'pause') return;
      try {
        const state = readControlState(taskId);
        const pending = state.pending.find((entry) => entry.action === 'pause' &&
          entry.request_id === operation.request_id && entry.intent_seq === operation.intent_seq);
        if (!pending || pending.pause_state !== 'pausing') return;
        pending.pause_state = 'unresolved';
        writeControlState(taskId, state);
        if (selectedTask?.task_id === taskId && snapshot) render();
      } catch (_) { /* An unavailable local record never authorizes another POST. */ }
    }
    try {
      const response = await fetch(`${controlApiUrl}/operations/${encodeURIComponent(operation.request_id)}`, {
        cache: 'no-store', signal: AbortSignal.timeout(8000),
      });
      if (!response.ok) { invalidatePauseAcknowledgment(); return; }
      const value = await response.json();
      const targetId = typeof value.target === 'string' ? value.target : value.target?.task_id;
      if (value.request_id !== operation.request_id || value.action !== operation.action || targetId !== taskId ||
          value.intent_seq !== operation.intent_seq || value.found !== true) {
        invalidatePauseAcknowledgment();
        return;
      }
      const state = readControlState(taskId);
      if (operation.action === 'pause' && value.resolved === false) {
        const pending = state.pending.find((entry) => entry.request_id === operation.request_id &&
          entry.intent_seq === operation.intent_seq);
        if (pending) {
          pending.pause_state = value.status === 'accepted' && value.pause_state === 'pausing' ? 'pausing' : 'unresolved';
          writeControlState(taskId, state);
          if (selectedTask?.task_id === taskId && snapshot) render();
        }
      }
      if (value.status === 'accepted' && value.resolved === false) {
        if (selectedTask?.task_id === taskId && state.intent_seq === operation.intent_seq && typeof value.message === 'string') {
          element('control-message').textContent = value.message;
        }
        return;
      }
      if (value.resolved !== true || !['confirmed', 'rejected', 'superseded'].includes(value.status)) {
        invalidatePauseAcknowledgment();
        return;
      }
      state.pending = state.pending.filter((entry) => entry.request_id !== operation.request_id);
      if (state.intent_seq === operation.intent_seq) {
        state.guard = { action: operation.action, intent_seq: operation.intent_seq, status: value.status };
        if (value.status === 'confirmed' && operation.action === 'pause') state.confirmed = { ...operation };
        else if (operation.action === 'start') delete state.confirmed;
      }
      writeControlState(taskId, state);
      if (selectedTask?.task_id === taskId && state.intent_seq === operation.intent_seq) {
        const label = operation.action === 'start' ? '开始' : '暂停';
        const failed = Array.isArray(value.failed_checks)
          ? value.failed_checks.map((check) => `${checkLabels[check.name] || check.name}：${check.detail}`).join('；') : '';
        const reason = failed ? `依赖自检未通过：${failed}` : value.reason;
        const confirmedMessage = operation.action === 'start' && typeof value.message === 'string' && value.message.trim()
          ? value.message : `${label}已确认`;
        element('control-message').textContent = value.status === 'confirmed' ? confirmedMessage :
          value.status === 'superseded' ? `${label}已被后续操作取代` : `${label}未被接受${reason ? `：${reason}` : ''}`;
      }
      if (selectedTask?.task_id === taskId) {
        updateFailed = true;
        if (snapshot) render();
        await queryStore.cancel(queryKey(queryApiUrl ? 'summary' : 'folders', taskId));
        if (selectedTask?.task_id === taskId && apiUrl) {
          await refresh();
          return true;
        }
      }
    } catch (_) {
      // A failed lookup leaves the original request identity pending.
      invalidatePauseAcknowledgment();
    }
  }

  async function checkPendingControls(taskId) {
    const pending = pendingControls(taskId);
    if (!pending) return;
    await Promise.all(pending.map((operation) => checkControlOperation(taskId, operation)));
  }

  function updateNotice() {
    element('notice-text').textContent = !controlApiUrl ? '任务控制接口待接入。'
      : knownOffline() ? (snapshot ? '连接中断，仅显示上次记录，暂不能提交操作。' : '连接中断，暂无已保存记录，暂不能提交操作。')
      : updateFailed ? (snapshot ? '任务状态更新失败，当前显示上次记录。' : '任务状态暂不可用。')
      : '任务控制接口已配置，操作状态以后台为准。';
  }

  function updateControls() {
    updateNotice();
    let state;
    try { state = selectedTask ? readControlState(selectedTask.task_id) : { pending: [] }; } catch (_) { state = null; }
    const pending = state?.pending;
    const guard = state?.guard;
    const startPending = pending?.some((entry) => entry.action === 'start') ||
      (guard?.action === 'start' && guard.status === 'confirmed');
    const latestStart = pending?.some((entry) => entry.action === 'start' && entry.intent_seq === state.intent_seq) ||
      (guard?.action === 'start' && guard.status === 'confirmed' && guard.intent_seq === state.intent_seq);
    const pausePending = pending?.some((entry) => entry.action === 'pause');
    const available = !knownOffline() && controlApiUrl && selectedTask && snapshot && !updateFailed && pending;
    const actions = snapshot?.actions || (deleteUnindexedAllowed ? {delete:{allowed:true}} : undefined);
    const deletePending = pending?.some((entry) => entry.action === 'delete');
    element('delete-task-button').disabled = !(!knownOffline() && controlApiUrl && selectedTask && pending &&
      !deletePending && (actions?.delete?.allowed === true || state?.deleteFailed));

    element('start-button').disabled = !(available && automaticGatewayCapacity && !guard && !startPending && !pausePending && !deletePending && actions?.start.allowed);
    element('pause-button').disabled = !(!knownOffline() && controlApiUrl && selectedTask && snapshot && pending && !pausePending && !deletePending &&
      ((available && !guard && actions?.pause.allowed) || latestStart));
    element('control-availability').textContent = !controlApiUrl || !selectedTask || !snapshot ? ''
      : !pending ? '操作记录无法读取，暂不可提交'
      : knownOffline() ? '当前离线，暂不可提交操作'
      : updateFailed ? '任务状态更新失败，操作暂不可用'
      : startPending || pausePending || guard ? '操作结果待核对'
      : !automaticGatewayCapacity ? '等待国内账号自动检测服务更新'
      : !actions ? '操作许可待后端返回'
      : ['start', 'pause'].filter((action) => !actions[action].allowed)
        .map((action) => `${action === 'start' ? '开始' : '暂停'}：${actions[action].reason || '暂不可用'}`).join(' · ');
  }

  function showUnavailable(message, label) {
    for (const id of [...Object.values(stages), 'llm-body-count', 'llm-attachment-count', 'waiting-nas-archive-count', 'cleanup-count']) {
      element(id).textContent = label;
      element(id).setAttribute('aria-label', label);
    }
    for (const section of Object.keys(sections)) {
      for (const result of Object.keys(resultLabels)) element(`${section}-${result}-count`).textContent = label;
      element(`${section}-results-hint`).textContent = message;
      document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => { button.disabled = true; });
    }
    for (const id of hints) element(id).textContent = message;
    element('folder-ticket-overview').textContent = message;
    element('task-detail').hidden = true;
    element('task-detail').textContent = '';
    const hasPendingRetries = Boolean(selectedTask && pendingRetryFolders().length);
    element('failed-folders-button').hidden = !hasPendingRetries;
    element('failed-folders-button').disabled = !hasPendingRetries;
    if (hasPendingRetries) {
      element('failed-folders-button').textContent = '查看失败及待核对工单';
      element('failed-folders-title').textContent = '当前任务的失败及待核对工单';
      if (!element('failed-folders-panel').hidden) renderFailures();
    } else {
      element('failed-folders-panel').hidden = true;
      element('failed-folders-list').replaceChildren();
    }
  }

  function showClassificationsUnavailable(message, label) {
    for (const id of ['passed-count', 'failed-count', 'unrecognized-count', 'unknown-count']) element(id).textContent = label;
    element('unknown-stat').hidden = true;
    element('classification-results-hint').textContent = message;
    element('list-panel').hidden = true;
    element('group-list').replaceChildren();
    document.querySelectorAll('[data-filter]').forEach((button) => {
      button.disabled = true;
      button.setAttribute('aria-pressed', 'false');
    });
  }

  function renderClassificationList() {
    if (!classificationList.filter) return;
      const matching = classifications.items.filter((item) => item.status === classificationList.filter);
    const pages = Math.max(1, Math.ceil(matching.length / 20));
    classificationList.page = Math.min(classificationList.page, pages);
    const rows = matching.slice((classificationList.page - 1) * 20, classificationList.page * 20).map((item) => {
      const row = document.createElement('li');
      const body = document.createElement('div');
      const title = document.createElement('span');
      const folder = document.createElement('p');
      const group = document.createElement('p');
      const status = document.createElement('span');
      body.className = 'entry-content';
      title.className = 'entry-title';
      title.textContent = item.paper_title || '题目待核对';
      folder.className = 'entry-detail';
      folder.textContent = `文件夹：${item.folder_name}`;
      group.className = 'entry-detail';
      group.textContent = `资料组：${item.group_id}`;
      body.append(title, folder, group);
      if (item.reason && !queryApiUrl) {
        const reason = document.createElement('p');
        reason.className = 'entry-reason';
        reason.textContent = item.reason;
        body.append(reason);
      }
      status.className = `entry-status ${item.status === 'unresolved' ? 'unknown' : item.status}`;
      status.textContent = classificationLabels[item.status];
      row.append(body, status);
      return row;
    });
    element('list-title').textContent = `第一步${classificationLabels[classificationList.filter]}`;
    element('list-count').textContent = `${classificationComplete ? '共' : '已确认'} ${number.format(matching.length)} 个文件夹`;
    element('group-list').replaceChildren(...rows);
    element('list-message').hidden = matching.length > 0;
    element('list-message').textContent = classificationComplete ? '暂无资料' : '暂无已确认资料，其他结果待核对';
    element('pagination').hidden = pages <= 1;
    element('page-info').textContent = `第 ${classificationList.page} / ${pages} 页`;
    element('previous-page').disabled = classificationList.page <= 1;
    element('next-page').disabled = classificationList.page >= pages;
  }

  function renderClassifications() {
    const counts = classifications.counts || { passed: 0, failed: 0, unrecognized: 0, unresolved: 0 };
    if (!classifications.counts) for (const item of classifications.items) counts[item.status] += 1;
    const display = (value) => classifications.counts ? value == null ? '待核对' : number.format(value)
      : classificationComplete ? number.format(value) : value ? `≥${number.format(value)}` : '待核对';
    for (const [status, id] of Object.entries({ passed: 'passed-count', failed: 'failed-count', unrecognized: 'unrecognized-count', unresolved: 'unknown-count' })) {
      element(id).textContent = display(counts[status]);
    }
    element('unknown-stat').hidden = classificationComplete && !counts.unresolved;
    const stamp = classifications.as_of ? new Date(classifications.as_of).toLocaleString('zh-CN', { hour12: false }) : '尚无已保存记录';
    const quality = classificationComplete ? '第一步分类结果' : '已确认第一步结果 · 统计不完整';
    element('classification-results-hint').textContent = `当前任务 · ${quality} · ${classificationUpdateFailed ? '更新失败 · 上次数据' : '数据截至'} ${stamp}`;
    document.querySelectorAll('[data-filter]').forEach((button) => { button.disabled = false; });
    renderClassificationList();
  }

  function pendingRetryFolders() {
    const prefix = `paperflow-pending-retry:${selectedTask.task_id}:`;
    const folders = [];
    for (let i = 0; i < localStorage.length; i += 1) {
      const key = localStorage.key(i);
      if (key.startsWith(prefix)) folders.push(key.slice(prefix.length));
    }
    return folders;
  }

  function checkPendingRetry(taskId, folderId, folderName = folderId) {
    const retryKey = `paperflow-pending-retry:${taskId}:${folderId}`;
    if (retryChecks.has(retryKey)) return retryChecks.get(retryKey);
    const version = selectionVersion;
    const check = (async () => {
      let marker;
      try { marker = JSON.parse(localStorage.getItem(retryKey)); } catch (_) { /* Preserve legacy markers. */ }
      let message = retryMessages.get(retryKey) || '重试结果仍待核对';
      if (!marker || typeof marker.request_id !== 'string' || marker.task_id !== taskId ||
          marker.folder_id !== folderId || typeof marker.ticket_id !== 'string') {
        message = '原重试记录缺少可核对的请求身份，结果待核对';
      } else {
        try {
          const response = await fetch(`${controlApiUrl}/operations/${encodeURIComponent(marker.request_id)}`, {
            cache: 'no-store', signal: AbortSignal.timeout(8000),
          });
          const value = await response.json();
          if (deletedTaskIds.has(taskId)) return;
          const target = value.target;
          if (response.ok && value.request_id === marker.request_id && value.action === 'retry' &&
              target?.task_id === taskId && target?.folder_id === folderId &&
              (!target.ticket_id || target.ticket_id === marker.ticket_id) && value.found === true && value.resolved === true) {
            if (['rejected', 'superseded'].includes(value.status)) {
              localStorage.removeItem(retryKey);
              if (value.status === 'superseded') message = '重试请求已被取代';
              else if (message === '重试结果仍待核对') message = '重试请求未被接受';
            } else if (value.status === 'confirmed' && typeof value.retry_id === 'string' &&
                       (!marker.retry_id || marker.retry_id === value.retry_id)) {
              marker.retry_id = value.retry_id;
              localStorage.setItem(retryKey, JSON.stringify(marker));
              if (version === selectionVersion && selectedTask?.task_id === taskId) {
                await queryStore.cancel(queryKey(queryApiUrl ? 'summary' : 'folders', taskId));
                await refresh();
                if (queryApiUrl) await loadFolderDetails({ folder_id: folderId });
              }
              const current = version === selectionVersion && !updateFailed && snapshot?.folders.find((item) =>
                item.folder_id === marker.folder_id && item.ticket_id === marker.ticket_id);
              if (current?.last_retry?.retry_id === marker.retry_id &&
                  ['scheduled', 'running', 'completed', 'failed'].includes(current.last_retry.status)) {
                localStorage.removeItem(retryKey);
                message = `已核对：本次重试${retryStatuses[current.last_retry.status]}（编号 ${marker.retry_id}）`;
              } else message = `已确认重试请求，等待工单显示本次重试 ${marker.retry_id}`;
            }
          }
        } catch (_) { /* Retain the original identity for a later lookup. */ }
      }
      if (deletedTaskIds.has(taskId)) return;
      retryMessages.set(retryKey, message);
      if (version === selectionVersion && selectedTask?.task_id === taskId) {
        element('failed-folders-message').textContent = `文件夹 ${folderName}：${message}`;
        if (snapshot) render();
        else renderFailures();
      }
    })();
    retryChecks.set(retryKey, check);
    check.finally(() => retryChecks.delete(retryKey));
    return check;
  }

  function checkPendingRetries(taskId) {
    if (selectedTask?.task_id !== taskId) return;
    for (const folderId of pendingRetryFolders()) {
      const key = `paperflow-pending-retry:${taskId}:${folderId}`;
      if (retryMessages.get(key) !== '正在提交重试请求') checkPendingRetry(taskId, folderId);
    }
  }

  function renderFailures() {
    const pendingFolders = new Set(pendingRetryFolders());
    const snapshotFolders = queryApiUrl ? failedDetailFolders : snapshot?.folders || [];
    const presentFolders = new Set(snapshotFolders.map((folder) => folder.folder_id));
    const folders = snapshotFolders.filter((folder) => folder.stage === 'failed' || pendingFolders.has(folder.folder_id));
    for (const folderId of pendingFolders) {
      if (!presentFolders.has(folderId)) folders.push({ task_id: selectedTask.task_id, folder_id: folderId,
        folder_name: folderId, ticket_id: null, paper_title: null, stage: 'unresolved' });
    }
    const rows = folders.map((folder) => {
      const row = document.createElement('li');
      const body = document.createElement('div');
      const title = document.createElement('span');
      const folderLine = document.createElement('p');
      const idLine = document.createElement('p');
      const reason = document.createElement('p');
      const retry = document.createElement('button');
      body.className = 'entry-content';
      title.className = 'entry-title';
      title.textContent = folder.paper_title || '题目待核对';
      folderLine.className = 'entry-detail';
      folderLine.textContent = `${folder.ticket_id ? '文件夹' : '文件夹 ID'}：${folder.folder_name}`;
      idLine.className = 'entry-detail';
      idLine.textContent = `小工单：${folder.ticket_id || '待核对'}`;
      reason.className = 'entry-reason';
      reason.textContent = queryApiUrl && folder.stage === 'failed' ? failedStages[folder.failed_stage] : folder.stage === 'failed'
        ? `${failedStages[folder.failed_stage]} · ${folder.failure_detail || folder[`${folder.failed_stage}_detail`] || '失败原因待接入'}`
        : folder.ticket_id ? '工单状态已变化，重试结果待核对' : '当前快照未返回该工单';
      retry.className = 'control-button';
      retry.type = 'button';
      const retryKey = `paperflow-pending-retry:${selectedTask.task_id}:${folder.folder_id}`;
      const pending = Boolean(localStorage.getItem(retryKey));
      const busy = retryMessages.get(retryKey) === '正在提交重试请求';
      const hasRetryHistory = Object.hasOwn(folder, 'last_retry');
      retry.disabled = pending ? !apiUrl || busy : !controlApiUrl || folder.retryable !== true || !hasRetryHistory || updateFailed;
      retry.textContent = pending ? busy ? '正在处理' : '核对重试结果' : !controlApiUrl || !hasRetryHistory
        ? '手动重试（待接入）' : folder.retryable === false ? '暂不可重试' : folder.retryable === true ? '手动重试' : '手动重试（待接入）';
      body.append(title, folderLine, idLine, reason);
      const retryReason = document.createElement('p');
      retryReason.className = 'entry-detail';
      retryReason.textContent = retryMessages.get(retryKey) || (pending ? '重试结果待核对，请点击核对' : folder.retry_reason ? `重试状态：${folder.retry_reason}` : '');
      body.append(retryReason);
      retry.addEventListener('click', async () => {
        if (localStorage.getItem(retryKey)) {
          retry.disabled = true;
          retryReason.textContent = '正在核对重试结果';
          await checkPendingRetry(selectedTask.task_id, folder.folder_id, folder.folder_name);
          return;
        }
        if (knownOffline()) {
          retryReason.textContent = '当前离线，暂不可提交重试';
          return;
        }
        if (!confirm(`确认从失败位置重试“${folder.folder_name}”吗？`)) return;
        const version = selectionVersion;
        const marker = { request_id: crypto.randomUUID(), task_id: folder.task_id, folder_id: folder.folder_id,
          ticket_id: folder.ticket_id, previous_retry_id: folder.last_retry?.retry_id || null, retry_id: null };
        try {
          localStorage.setItem(retryKey, JSON.stringify(marker));
        } catch (_) {
          retryReason.textContent = '本机操作记录未保存，无法提交';
          return;
        }
        retry.disabled = true;
        retryReason.textContent = '正在提交重试请求';
        retryMessages.set(retryKey, retryReason.textContent);
        try {
          const response = await fetch(`${controlApiUrl}/tasks/${encodeURIComponent(folder.task_id)}/folders/${encodeURIComponent(folder.folder_id)}/retry`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ request_id: marker.request_id }), signal: AbortSignal.timeout(120000),
          });
          const value = await response.json();
          if (deletedTaskIds.has(folder.task_id)) return;
          if (response.ok && value.accepted === true && value.request_id === marker.request_id &&
              (value.task_id == null || value.task_id === marker.task_id) &&
              (value.folder_id == null || value.folder_id === marker.folder_id) &&
              (value.ticket_id == null || value.ticket_id === marker.ticket_id)) {
            if (typeof value.retry_id === 'string') {
              marker.retry_id = value.retry_id;
              localStorage.setItem(retryKey, JSON.stringify(marker));
            }
            retryReason.textContent = '重试请求已提交，结果待核对';
          } else if (response.ok) {
            retryReason.textContent = '重试结果待核对，请联系后端确认';
          } else if (response.status === 409 && value.detail?.ready === false && Array.isArray(value.detail.checks)) {
            retryReason.textContent = `依赖自检未通过：${value.detail.checks.filter((check) => !check.ready).map((check) => `${checkLabels[check.name] || check.name}：${check.detail}`).join('；')}`;
          } else if (response.status === 409 && value.detail && typeof value.detail.code === 'string' && typeof value.detail.message === 'string') {
            retryReason.textContent = `${value.detail.message}；请核对重试结果`;
          } else if (response.status === 409 && typeof value.detail === 'string') {
            retryReason.textContent = `${value.detail}；请核对重试结果`;
          } else if (response.status >= 400 && response.status < 500 && typeof value.detail === 'string') {
            retryReason.textContent = `${value.detail}；请核对重试结果`;
          } else {
            retryReason.textContent = '重试结果待核对，请联系后端确认';
          }
        } catch (_) {
          retryReason.textContent = '重试结果待核对，请联系后端确认';
        } finally {
          if (!deletedTaskIds.has(folder.task_id)) {
            retryMessages.set(retryKey, retryReason.textContent);
            if (version === selectionVersion) {
              if (!element('failed-folders-panel').hidden) renderFailures();
              await checkPendingRetry(folder.task_id, folder.folder_id, folder.folder_name);
            }
          }
        }
      });
      row.append(body, retry);
      return row;
    });
    element('failed-folders-list').replaceChildren(...rows);
  }

  function renderList(section) {
    const state = lists[section];
    if (!state.filter) return;
    const complete = snapshotComplete && !(snapshot.summary ? snapshot.summary.uncertain_results[section] : snapshot.folders.some((folder) => uncertainResult(folder, section)));
    const resultLabel = (section === 'postprocessing' || downstreamSections.has(section)) && state.filter === 'failed' ? '未通过' : resultLabels[state.filter];
    const matching = (snapshot.summary ? state.rows : snapshot.folders).filter((folder) => folder[`${section}_result`] === state.filter);
    const total = snapshot.summary ? snapshot.summary.results[section][state.filter] : matching.length;
    const pages = Math.max(1, Math.ceil(total / 20));
    state.page = Math.min(state.page, pages);
    const visible = snapshot.summary ? matching.slice(0,20) : matching.slice((state.page - 1) * 20, state.page * 20);
    const rows = visible.map((folder) => {
      const row = document.createElement('li');
      const body = document.createElement('div');
      const title = document.createElement('span');
      const folderLine = document.createElement('p');
      const idLine = document.createElement('p');
      const result = document.createElement('span');
      body.className = 'entry-content';
      title.className = 'entry-title';
      title.textContent = folder.paper_title || '题目待核对';
      folderLine.className = 'entry-detail';
      folderLine.textContent = `文件夹：${folder.folder_name}`;
      idLine.className = 'entry-detail';
      idLine.textContent = `小工单：${folder.ticket_id}`;
      result.className = `entry-status ${state.filter}`;
      result.textContent = resultLabel;
      body.append(title, folderLine, idLine);
      if (folder.task_name) {
        const taskLine = document.createElement('p');
        taskLine.className = 'entry-detail';
        taskLine.textContent = `所属任务：${folder.task_name}`;
        body.append(taskLine);
      }
      const detail = folder[`${section}_detail`];
      if (detail) {
        const detailLine = document.createElement('p');
        detailLine.className = 'entry-reason';
        detailLine.textContent = detail;
        body.append(detailLine);
      } else if (state.filter !== 'passed') {
        const detailLine = document.createElement('p');
        detailLine.className = 'entry-detail';
        detailLine.textContent = '结果详情暂不可用';
        body.append(detailLine);
      }
      row.append(body, result);
      return row;
    });
    element(`${section}-list-title`).textContent = `${sections[section]}${resultLabel}`;
    element(`${section}-list-count`).textContent = `${complete ? '共' : '已确认'} ${number.format(total)} 个文件夹`;
    element(`${section}-group-list`).replaceChildren(...rows);
    element(`${section}-list-message`).hidden = matching.length > 0;
    element(`${section}-list-message`).textContent = complete ? '暂无资料' : '暂无已确认资料，其他工单待核对';
    element(`${section}-pagination`).hidden = pages <= 1;
    element(`${section}-page-info`).textContent = `第 ${state.page} / ${pages} 页`;
    element(`${section}-previous-page`).disabled = state.page <= 1;
    element(`${section}-next-page`).disabled = state.page >= pages;
  }

  function uncertainResult(folder, section) {
    const stage = folder.stage_snapshots?.[section];
    return downstreamSections.has(section) ? !stage || stage.final !== true || stage.result == null
      : Boolean(stage && stage.final !== true && stage.pending_request_count !== 0);
  }

  function render() {
    if (snapshot.summary && snapshot.classification) {
      classifications = { ...snapshot.classification, task_id: selectedTask.task_id, complete: snapshot.complete, items: classifications?.items || [] };
      classificationComplete = snapshot.complete;
      classificationUpdateFailed = updateFailed;
      renderClassifications();
    }
    let counts = Object.fromEntries([...Object.keys(stages), 'waiting_nas_archive_delete', 'nas_in_progress', 'waiting_cleanup', 'cleanup_in_progress', 'failed', 'unresolved'].map((stage) => [stage, 0]));
    let results = Object.fromEntries(Object.keys(sections).map((section) => [section, { passed: 0, failed: 0, unrecognized: 0 }]));
    let uncertainResults = Object.fromEntries(Object.keys(sections).map((section) => [section, false]));
    let failures = Object.fromEntries(Object.keys(failedStages).map((stage) => [stage, 0]));
    let body = 0, attachment = 0, bodyUnknown = false, attachmentUnknown = false;
    for (const folder of snapshot.summary ? [] : snapshot.folders) {
      counts[folder.stage] += 1;
      if (folder.stage === 'failed') failures[folder.failed_stage] += 1;
      else if (!localStorage.getItem(`paperflow-pending-retry:${selectedTask.task_id}:${folder.folder_id}`)) {
        retryMessages.delete(`paperflow-pending-retry:${selectedTask.task_id}:${folder.folder_id}`);
      }
      if (folder.llm_body_running === true) body += 1;
      if (folder.llm_attachment_running === true) attachment += 1;
      if (folder.stage !== 'llm_in_progress' && (folder.llm_body_running === true || folder.llm_attachment_running === true)) {
        counts.llm_in_progress += 1;
      }
      if (folder.stage === 'llm_in_progress' || folder.stage === 'failed') {
        if (folder.llm_body_running === null) bodyUnknown = true;
        if (folder.llm_attachment_running === null) attachmentUnknown = true;
      }
      for (const section of Object.keys(sections)) {
        const result = folder[`${section}_result`];
        if (result) results[section][result] += 1;
        if (uncertainResult(folder, section)) uncertainResults[section] = true;
      }
    }
    if (snapshot.summary) {
      ({ counts, results, failures, body, attachment } = snapshot.summary);
      uncertainResults = snapshot.summary.uncertain_results;
      bodyUnknown = snapshot.summary.body_unknown;
      attachmentUnknown = snapshot.summary.attachment_unknown;
    }
    const folderTotal = snapshot.summary ? snapshot.summary.folder_total : snapshot.folders.length;
    const complete = snapshotComplete;
    const display = (value, knownComplete = complete) => knownComplete ? number.format(value) : value ? `≥${number.format(value)}` : '待核对';
    for (const [stage, id] of Object.entries(stages)) {
      element(id).textContent = display(counts[stage]);
      element(id).setAttribute('aria-label', `${display(counts[stage])} 个文件夹`);
    }
    element('llm-body-count').textContent = display(body, complete && !bodyUnknown);
    element('llm-attachment-count').textContent = display(attachment, complete && !attachmentUnknown);
    element('waiting-nas-archive-count').textContent = display(counts.waiting_nas_archive_delete + counts.nas_in_progress);
    element('cleanup-count').textContent = display(counts.waiting_cleanup + counts.cleanup_in_progress);
    element('llm-body-count').setAttribute('aria-label', `正文识别 ${display(body, complete && !bodyUnknown)} 个文件夹`);
    element('llm-attachment-count').setAttribute('aria-label', `附件识别 ${display(attachment, complete && !attachmentUnknown)} 个文件夹`);
    element('waiting-nas-archive-count').setAttribute('aria-label', `NAS 归档 ${display(counts.waiting_nas_archive_delete + counts.nas_in_progress)} 个文件夹`);
    element('cleanup-count').setAttribute('aria-label', `本机清理 ${display(counts.waiting_cleanup + counts.cleanup_in_progress)} 个文件夹`);
    element('task-total').textContent = snapshot.summary ? number.format(folderTotal) : display(folderTotal);
    element('discovered').textContent = complete ? '当前任务 · 按文件夹小工单统计' : '当前任务 · 工单统计不完整';
    const stillProcessing = ['preprocessing_in_progress', 'llm_in_progress', 'postprocessing_in_progress',
      'rag_in_progress', 'nas_in_progress', 'cleanup_in_progress'].some((stage) => counts[stage] > 0);
    const controls = pendingControls(selectedTask.task_id);
    let guard, controlSequence;
    try {
      const state = readControlState(selectedTask.task_id);
      guard = state.guard;
      controlSequence = state.intent_seq;
    } catch (_) { /* Controls stay disabled. */ }
    const pausePending = controls?.some((entry) => entry.action === 'pause') || guard?.action === 'pause';
    const pauseDraining = controls?.some((entry) => entry.action === 'pause' &&
      entry.intent_seq === controlSequence && entry.pause_state === 'pausing');
    const taskState = pausePending ? (pauseDraining ? '暂停中（正在等待文件夹停稳）' : '暂停请求待核对') : snapshot.task_state === 'pausing' ? '暂停中（正在等待文件夹停稳）' : snapshot.desired_state === 'paused' && (snapshot.task_state === 'running' || (snapshot.task_state === 'paused' && stillProcessing))
      ? '暂停中（当前仍在处理）'
      : snapshot.task_state === 'completed' && !complete ? '状态待核对' : (taskStates[snapshot.task_state] || '状态待核对');
    element('task-state').textContent = updateFailed ? `${taskState}（上次数据）` : taskState;
    element('task-detail').hidden = !snapshot.stop_reason;
    element('task-detail').textContent = snapshot.stop_reason ? `${updateFailed ? '上次记录：' : ''}${snapshot.stop_reason}` : '';

    const stamp = snapshot.as_of ? new Date(snapshot.as_of).toLocaleString('zh-CN', { hour12: false }) : '尚无已保存记录';
    const quality = complete ? '按文件夹工单统计' : '已确认数量 · 统计不完整';
    const freshness = updateFailed ? '更新失败 · 上次数据' : '数据截至';
    const hint = `当前任务 · ${quality} · ${freshness} ${stamp}`;
    for (const id of hints) element(id).textContent = hint;
    element('waiting-nas-archive-hint').textContent = `等待 ${display(counts.waiting_nas_archive_delete)} · 处理中 ${display(counts.nas_in_progress)} · ${quality} · ${freshness} ${stamp}`;
    element('cleanup-hint').textContent = `等待 ${display(counts.waiting_cleanup)} · 处理中 ${display(counts.cleanup_in_progress)} · ${quality} · ${freshness} ${stamp}`;
    const pending = counts.unresolved ? ` · 待核对 ${number.format(counts.unresolved)}` : '';
    const breakdown = Object.keys(failedStages).filter((stage) => failures[stage]).map((stage) => `${failedStages[stage]} ${number.format(failures[stage])}`).join('、');
    const failed = counts.failed ? ` · 失败 ${number.format(counts.failed)}（${breakdown}）` : '';
    const detail = snapshot.detail ? ` · ${snapshot.detail}` : '';
    const total = complete ? `共 ${number.format(folderTotal)} 张文件夹工单`
      : folderTotal ? `已读取 ${number.format(folderTotal)} 张文件夹工单` : '工单数量待核对';
    element('folder-ticket-overview').textContent = `${total}${pending}${failed} · ${quality} · ${freshness} ${stamp}${detail}`;
    const pendingRetries = pendingRetryFolders();
    element('failed-folders-button').hidden = !counts.failed && !pendingRetries.length;
    element('failed-folders-button').disabled = !counts.failed && !pendingRetries.length;
    element('failed-folders-button').textContent = pendingRetries.length ? '查看失败及待核对工单' : '查看失败工单';
    element('failed-folders-title').textContent = pendingRetries.length ? '当前任务的失败及待核对工单' : '当前任务的失败工单';
    if (!element('failed-folders-panel').hidden) renderFailures();

    for (const section of Object.keys(sections)) {
      for (const result of Object.keys(resultLabels)) element(`${section}-${result}-count`).textContent = display(results[section][result], complete && !uncertainResults[section]);
      const resultQuality = uncertainResults[section]
        ? downstreamSections.has(section) ? '已确认阶段结果 · 当前状态待核对' : '上次阶段结果 · 当前请求待核对'
        : complete ? '阶段结果' : '已确认阶段结果 · 统计不完整';
      element(`${section}-results-hint`).textContent = `当前任务 · ${resultQuality} · ${freshness} ${stamp}`;
      document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => { button.disabled = false; });
      renderList(section);
    }
    updateControls();
  }

  function normalizeDownstreamResults(folder) {
    const normalized = { ...folder };
    for (const section of downstreamSections) {
      const stage = folder.stage_snapshots?.[section];
      // Downstream results are supplied by stage snapshots; flat fields may come from our saved display.
      const result = stage?.final === true ? stage.result ?? null
        : stage?.previous_result?.result ?? folder[`${section}_result`] ?? null;
      if (!resultNames.has(result)) throw new Error('阶段结果格式不正确');
      normalized[`${section}_result`] = result;
      normalized[`${section}_detail`] = stage?.final !== true && result
        ? '上次已确认结果，当前阶段状态待核对'
        : folder.failed_stage === section ? folder.failure_detail : null;
    }
    return normalized;
  }

  function validateSnapshot(value, taskId) {
    if (value?.summary) validateSummary(value.summary);
    if (value?.summary && (!value.classification?.counts || Object.keys(classificationLabels).some((key)=>
        value.classification.counts[key] !== null && (!Number.isSafeInteger(value.classification.counts[key]) || value.classification.counts[key] < 0)))) throw new Error('分类汇总格式不正确');
    if (!value || value.task_id !== taskId || typeof value.task_state !== 'string' || typeof value.desired_state !== 'string' ||
        (value.stop_reason != null && typeof value.stop_reason !== 'string') || (value.detail != null && typeof value.detail !== 'string') ||
        (value.control_intent_seq != null && (!Number.isSafeInteger(value.control_intent_seq) || value.control_intent_seq < 0)) ||
        (value.actions != null && ['start', 'pause'].some((action) => !value.actions[action] ||
          typeof value.actions[action].allowed !== 'boolean' || (value.actions[action].reason != null && typeof value.actions[action].reason !== 'string'))) ||
        !Array.isArray(value.folders) || typeof value.complete !== 'boolean' ||
        !(value.as_of === null || Number.isFinite(Date.parse(value.as_of))) ||
        value.folders.some((folder) => !folder || typeof folder.ticket_id !== 'string' || !folder.ticket_id ||
          folder.task_id !== taskId || typeof folder.folder_id !== 'string' ||
          typeof folder.folder_name !== 'string' || !(folder.paper_title === null || typeof folder.paper_title === 'string') ||
          !stageNames.has(folder.stage) || !(folder.llm_body_running === null || typeof folder.llm_body_running === 'boolean') ||
          !(folder.llm_attachment_running === null || typeof folder.llm_attachment_running === 'boolean') ||
          (folder.last_retry != null && (typeof folder.last_retry.retry_id !== 'string' || !folder.last_retry.retry_id ||
            !Object.hasOwn(retryStatuses, folder.last_retry.status) ||
            (folder.last_retry.stage != null && typeof folder.last_retry.stage !== 'string'))) ||
          (folder.stage === 'failed' && !Object.hasOwn(failedStages, folder.failed_stage)) ||
          Object.keys(sections).some((section) => !downstreamSections.has(section) && !resultNames.has(folder[`${section}_result`])))) {
      throw new Error('工单数据格式不正确');
    }
    return { ...value, folders: value.folders.map(normalizeDownstreamResults) };
  }

  function validateClassifications(value, taskId) {
    if (!value || value.task_id !== taskId || typeof value.complete !== 'boolean' ||
        !(value.as_of === null || Number.isFinite(Date.parse(value.as_of))) ||
        !Array.isArray(value.items) || value.items.some((item) => !item || typeof item.group_id !== 'string' ||
          typeof item.folder_name !== 'string' || !(item.paper_title === null || typeof item.paper_title === 'string') ||
          !Object.hasOwn(classificationLabels, item.status) || (item.reason != null && typeof item.reason !== 'string'))) {
      throw new Error('第一步分类结果格式不正确');
    }
    return value;
  }

  async function validateControlSnapshot(value, taskId, signal) {
    const state = readControlState(taskId);
    const confirmed = state.confirmed;
    const expected = confirmed?.action === 'start' ? 'running' : 'paused';
    if (!confirmed || confirmed.intent_seq !== state.intent_seq || value.desired_state === expected) return value;
    const response = await fetch(`${controlApiUrl}/operations/${encodeURIComponent(confirmed.request_id)}`, {
      cache: 'no-store', signal: AbortSignal.any([signal, AbortSignal.timeout(8000)]),
    });
    if (!response.ok) throw new Error('操作状态暂不可核对');
    const operation = await response.json();
    const targetId = typeof operation.target === 'string' ? operation.target : operation.target?.task_id;
    if (operation.request_id !== confirmed.request_id || operation.action !== confirmed.action ||
        targetId !== taskId || operation.intent_seq !== confirmed.intent_seq || operation.found !== true ||
        operation.resolved !== true || operation.status !== 'superseded') {
      throw new Error('工单快照尚未反映已确认操作');
    }
    const latest = readControlState(taskId);
    if (latest.confirmed?.request_id === confirmed.request_id) {
      delete latest.confirmed;
      writeControlState(taskId, latest);
    }
    return value;
  }

  function keepStageResults(value, previous) {
    if (!previous || previous.task_id !== value.task_id || !Array.isArray(previous.folders)) return value;
    if (value.query_status === 'stale' && Number.isFinite(Date.parse(previous.as_of)) &&
        (!Number.isFinite(Date.parse(value.as_of)) || Date.parse(previous.as_of) >= Date.parse(value.as_of))) {
      return { ...previous, folders: previous.folders.map(normalizeDownstreamResults), query_status: 'stale' };
    }
    const previousFolders = new Map(previous.folders.map((folder) => [folder.ticket_id, normalizeDownstreamResults(folder)]));
    const folders = value.folders.map((folder) => {
      const before = previousFolders.get(folder.ticket_id);
      if (!before || before.folder_id !== folder.folder_id || !before.stage_snapshots || !folder.stage_snapshots) return folder;
      let next = folder;
      for (const stage of Object.keys(sections)) {
        const oldStage = before.stage_snapshots[stage];
        const newStage = folder.stage_snapshots[stage];
        if (!oldStage || !newStage || !oldStage.processing_attempt_id || !newStage.processing_attempt_id) continue;
        const oldTime = Date.parse(oldStage.recorded_at);
        const newTime = Date.parse(newStage.recorded_at);
        const older = Number.isFinite(oldTime) && Number.isFinite(newTime) && newTime < oldTime;
        if (older) {
          next = { ...next, stage_snapshots: { ...next.stage_snapshots, [stage]: oldStage },
            [`${stage}_result`]: before[`${stage}_result`], [`${stage}_detail`]: before[`${stage}_detail`] };
        } else if (oldStage.processing_attempt_id === newStage.processing_attempt_id && oldStage.final === true &&
                   newStage.final !== true && !next[`${stage}_result`] && before[`${stage}_result`]) {
          next = { ...next, [`${stage}_result`]: before[`${stage}_result`],
            [`${stage}_detail`]: '保留上次阶段结果，当前状态待核对' };
        }
      }
      return next;
    });
    return { ...value, folders };
  }

  async function refresh() {
    const taskId = selectedTask.task_id;
    lastPollAt = Date.now();
    const version = selectionVersion;
    const requestVersion = ++folderRequestVersion;
    folderReadPending = true;
    try {
      const key = queryKey(queryApiUrl ? 'summary' : 'folders', taskId);
      const value = await queryStore.fetch(key, async ({ signal }) => {
        const url = new URL(queryApiUrl ? `${queryApiUrl}/summary` : apiUrl);
        url.searchParams.set('task_id', taskId);
        const response = await fetch(url.toString(), { cache: 'no-store', signal: AbortSignal.any([signal, AbortSignal.timeout(120000)]) });
        if (!response.ok) {
          if (queryApiUrl && response.status===409) {
            const rejection=await response.json();
            if (version===selectionVersion && rejection.detail?.code==='index_required' && rejection.detail.task_id===taskId) deleteUnindexedAllowed=rejection.detail.delete_allowed===true;
          }
          throw new Error('工单接口暂不可用');
        }
        const fresh = await validateControlSnapshot(validateSnapshot(await response.json(), taskId), taskId, signal);
        return keepStageResults(fresh, queryStore.get(key));
      });
      if (version !== selectionVersion || requestVersion !== folderRequestVersion) return;
      snapshot = queryApiUrl ? { ...value, folders: detailFolders } : value;
      snapshotComplete = value.complete && value.as_of !== null && value.query_status !== 'preparing_history' &&
        (value.summary ? value.summary.counts.unresolved===0 : !value.folders.some((folder) => folder.stage === 'unresolved'));
      updateFailed = value.query_status === 'stale';
      if (!updateFailed) {
        try {
          const state = readControlState(taskId);
          if (Number.isSafeInteger(value.control_intent_seq) && value.control_intent_seq > state.intent_seq) {
            state.intent_seq = value.control_intent_seq;
            writeControlState(taskId, state);
          }
          const guard = state.guard;
          const expected = guard?.action === 'start' ? 'running' : 'paused';
          if (guard && (guard.status !== 'confirmed' || guard.action === 'start' || value.desired_state === expected)) {
            delete state.guard;
            writeControlState(taskId, state);
          }
        } catch (_) { updateFailed = true; }
      }
      render();
    } catch (_) {
      if (version !== selectionVersion || requestVersion !== folderRequestVersion) return;
      updateFailed = true;
      if (snapshot) render();
      else {
        showUnavailable('当前任务的文件夹工单数据暂不可用', '暂不可用');
        element('task-total').textContent = '暂不可用';
        element('task-state').textContent = '状态待核对';
        element('discovered').textContent = '当前任务数据暂不可用';
        updateControls();
      }
    } finally {
      if (version === selectionVersion && requestVersion === folderRequestVersion) folderReadPending = false;
    }
  }

  async function refreshClassifications() {
    if (queryApiUrl) return;
    const taskId = selectedTask.task_id;
    const version = selectionVersion;
    const requestVersion = ++classificationRequestVersion;
    try {
      const value = await queryStore.fetch(queryKey('classifications', taskId), async ({ signal }) => {
        const response = await fetch(`${controlApiUrl}/tasks/${encodeURIComponent(taskId)}/classifications`, {
          cache: 'no-store', signal: AbortSignal.any([signal, AbortSignal.timeout(120000)]),
        });
        if (!response.ok) throw new Error('第一步分类接口暂不可用');
        return validateClassifications(await response.json(), taskId);
      });
      if (version !== selectionVersion || requestVersion !== classificationRequestVersion) return;
      classifications = value;
      classificationComplete = value.complete && value.as_of !== null && value.query_status !== 'preparing_history' && !value.items.some((item) => item.status === 'unresolved');
      classificationUpdateFailed = value.query_status === 'stale';
      renderClassifications();
    } catch (_) {
      if (version !== selectionVersion || requestVersion !== classificationRequestVersion) return;
      classificationUpdateFailed = true;
      if (classifications) renderClassifications();
      else showClassificationsUnavailable('第一步分类结果暂不可用', '暂不可用');
    }
  }

  function renderPreflight(value) {
    if (typeof value.ready !== 'boolean' || !Array.isArray(value.checks) || !Number.isFinite(Date.parse(value.checked_at)) ||
        value.checks.some((check) => !check || typeof check.name !== 'string' || typeof check.ready !== 'boolean' || typeof check.detail !== 'string')) {
      throw new Error('自检结果格式不正确');
    }
    preflightReady = value.ready;
    const rows = value.checks.map((check) => {
      const row = document.createElement('li');
      row.textContent = `${checkLabels[check.name] || check.name}：${check.ready ? '已就绪' : '未就绪'} · ${check.detail}`;
      return row;
    });
    element('preflight-checks').replaceChildren(...rows);
    const pending = localStorage.getItem(pendingCreateKey);
    element('check-create-result').hidden = !pending;
    element('create-task-confirm').disabled = !preflightReady || Boolean(pending);
    element('new-task-message').textContent = pending
      ? `上次创建结果待核对，请核查请求编号 ${pending}`
      : preflightReady ? '依赖自检通过，可以创建任务' : '依赖自检未通过，请处理未就绪项';
  }

  async function runPreflight() {
    preflightReady = false;
    element('create-task-confirm').disabled = true;
    element('repeat-preflight').disabled = true;
    element('new-task-message').textContent = '正在检查依赖及上游账号连通性；符合条件时会尝试启动本机第一步服务，请稍候';
    element('preflight-checks').replaceChildren();
    element('check-create-result').hidden = !localStorage.getItem(pendingCreateKey);
    if (knownOffline()) {
      element('new-task-message').textContent = '当前离线，暂不可创建任务';
      element('repeat-preflight').disabled = false;
      return;
    }
    const monitors = [
      ['WorkBuddy 网关', 'http://127.0.0.1:8797/api/workbuddy/summary', (data) => {
        if (!data.connected) return '网关未连接';
        const missing = [];
        if (!data.authorized || !data.usage_available) missing.push('用量');
        if (!Number.isSafeInteger(data.prefect_active_slots) || !Number.isSafeInteger(data.concurrency_limit)) missing.push('Prefect 名额');
        if (!Number.isSafeInteger(data.in_flight)) missing.push('实际在途');
        return missing.length ? `已连接，${missing.join('、')}暂无数据` : '状态和用量已连接';
      }],
      ['服务器资源', 'http://127.0.0.1:8798/api/server-resources', (data) => {
        const fields = [['CPU', data.cpu_percent], ['GPU', data.gpu_percent],
          ['显存', data.gpu_memory_used_mib], ['内存', data.memory_percent]];
        const missing = fields.filter(([, value]) => typeof value !== 'number' || !Number.isFinite(value)).map(([name]) => name);
        return missing.length === fields.length ? '暂不可用' : missing.length ? `已连接，${missing.join('、')}暂无数据` : 'CPU、GPU、显存、内存已连接';
      }],
      ['NAS 状态', 'http://127.0.0.1:8799/api/nas-status', (data) => {
        if (data.status !== 'connected') return data.status === 'unavailable' ? '归档目录不可访问' : '状态待核对';
        const missing = [];
        if (!Number.isFinite(data.storage_total_bytes) || !Number.isFinite(data.storage_free_bytes)) missing.push('存储空间');
        if (!Number.isFinite(data.archive_write_bytes_per_second)) missing.push('归档写入速度');
        return missing.length ? `目录已连接，${missing.join('、')}暂无数据` : '目录、存储空间和写入速度已连接';
      }],
    ];
    const monitoring = Promise.all(monitors.map(async ([name, url, describe]) => {
      try {
        const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(5000) });
        if (!response.ok) throw new Error('监控接口不可用');
        return `${name}：${describe(await response.json())}`;
      } catch (_) {
        return `${name}：监控接口暂不可用`;
      }
    }));
    try {
      const response = await fetch(`${controlApiUrl}/preflight`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}', signal: AbortSignal.timeout(600000),
      });
      if (!response.ok) throw new Error('自检接口不可用');
      renderPreflight(await response.json());
    } catch (_) {
      const pending = localStorage.getItem(pendingCreateKey);
      element('new-task-message').textContent = pending
        ? `创建结果待核对，请核查请求编号 ${pending}；依赖自检暂不可用`
        : '依赖自检暂不可用，不能创建任务';
    } finally {
      const rows = ['以下为监控显示检查，不改变任务开始许可', ...await monitoring].map((message) => {
        const row = document.createElement('li');
        row.textContent = message;
        return row;
      });
      element('preflight-checks').append(...rows);
      element('repeat-preflight').disabled = false;
    }
  }

  element('new-task-button').addEventListener('click', () => {
    element('new-task-dialog').showModal();
    runPreflight();
  });
  element('repeat-preflight').addEventListener('click', runPreflight);
  element('cancel-new-task').addEventListener('click', () => element('new-task-dialog').close());
  element('check-create-result').addEventListener('click', async () => {
    const requestId = localStorage.getItem(pendingCreateKey);
    if (!requestId) return;
    const version = selectionVersion;
    element('check-create-result').disabled = true;
    element('new-task-message').textContent = `正在核查请求编号 ${requestId}`;
    try {
      const response = await fetch(`${controlApiUrl}/tasks/by-request/${encodeURIComponent(requestId)}`, {
        cache: 'no-store', signal: AbortSignal.timeout(120000),
      });
      const value = await response.json();
      if (!response.ok) {
        if (response.status >= 400 && response.status < 500 && typeof value.detail === 'string') {
          element('new-task-message').textContent = `创建结果待核对：${value.detail}（请求编号 ${requestId}）`;
          return;
        }
        throw new Error('核查暂不可用');
      }
      if (value.request_id !== requestId || typeof value.found !== 'boolean' || typeof value.resolved !== 'boolean') throw new Error('核查结果不正确');
      if (value.found && value.resolved) {
        if (!value.task || typeof value.task.task_id !== 'string' || typeof value.task.task_name !== 'string') throw new Error('任务结果不正确');
        localStorage.removeItem(pendingCreateKey);
        element('check-create-result').hidden = true;
        element('new-task-dialog').close();
        if (version === selectionVersion) selectTask(value.task);
        else element('control-message').textContent = `任务“${value.task.task_name}”已创建，可从任务列表打开`;
      } else {
        element('new-task-message').textContent = `创建结果仍待核对，保留请求编号 ${requestId}`;
      }
    } catch (_) {
      element('new-task-message').textContent = `创建结果待核对，暂时无法核查请求编号 ${requestId}`;
    } finally {
      element('check-create-result').disabled = false;
    }
  });
  element('new-task-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    if (!preflightReady || localStorage.getItem(pendingCreateKey)) return;
    if (knownOffline()) {
      element('new-task-message').textContent = '当前离线，暂不可创建任务';
      return;
    }
    const taskName = element('new-task-name').value.trim();
    const sourcePaths = element('new-task-paths').value.split(/\r?\n/).map((path) => path.trim()).filter(Boolean);
    if (!taskName || !sourcePaths.length) {
      element('new-task-message').textContent = '请填写任务名称及资料文件夹路径';
      return;
    }
    const requestId = crypto.randomUUID();
    const version = selectionVersion;
    try { localStorage.setItem(pendingCreateKey, requestId); }
    catch (_) {
      element('new-task-message').textContent = '本机操作记录未保存，无法创建任务';
      return;
    }
    element('create-task-confirm').disabled = true;
    element('new-task-message').textContent = '正在创建任务';
    try {
      const response = await fetch(`${controlApiUrl}/tasks`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: requestId, task_name: taskName, source_paths: sourcePaths }),
        signal: AbortSignal.timeout(600000),
      });
      const value = await response.json();
      if (response.status === 409 && value.detail && typeof value.detail === 'object' && value.detail.ready === false) {
        localStorage.removeItem(pendingCreateKey);
        renderPreflight(value.detail);
        return;
      }
      if (response.status === 422) {
        localStorage.removeItem(pendingCreateKey);
        element('create-task-confirm').disabled = false;
        element('new-task-message').textContent = '任务名称或资料路径无效，请核对后再创建';
        return;
      }
      if (!response.ok || value.request_id !== requestId || value.accepted !== true || value.status !== 'confirmed' ||
          typeof value.task_id !== 'string' || typeof value.task_name !== 'string') throw new Error('创建结果待核对');
      localStorage.removeItem(pendingCreateKey);
      element('new-task-dialog').close();
      if (version === selectionVersion) selectTask(value);
      else element('control-message').textContent = `任务“${value.task_name}”已创建，可从任务列表打开`;
    } catch (_) {
      preflightReady = false;
      element('new-task-message').textContent = `创建结果待核对，请联系后端核查请求编号 ${requestId}`;
      element('check-create-result').hidden = false;
    }
  });

  function selectTask(task, { refreshNow = true, remember = true } = {}) {
    selectedTask = task;
    selectionVersion += 1;
    folderReadPending = false;
    snapshot = null;
    detailFolders = [];
    failedDetailFolders = [];
    deleteUnindexedAllowed = false;
    snapshotComplete = false;
    classifications = null;
    classificationList.filter = null;
    classificationUpdateFailed = false;
    updateFailed = false;
    element('task-name').textContent = task.task_name;
    if (remember) saveLocal(lastTaskKey, { task_id: task.task_id, task_name: task.task_name, source: sourceId() });
    element('save-state').textContent = storageFailed ? '本机记录未保存或无法恢复' : `当前任务：${task.task_name}`;
    element('task-total').textContent = apiUrl ? '读取中' : '待接入';
    element('task-state').textContent = apiUrl ? '读取中' : '待接入';
    element('discovered').textContent = apiUrl ? '正在读取当前任务工单' : '当前任务工单接口待接入';
    element('control-message').textContent = controlApiUrl ? '请确认当前任务状态后操作' : '任务操作等待后端接口';
    element('failed-folders-message').textContent = '';
    element('failed-folders-panel').hidden = true;
    for (const section of Object.keys(sections)) {
      lists[section].filter = null;
      lists[section].rows = [];
      lists[section].requestVersion += 1;
      element(`${section}-list-panel`).hidden = true;
      document.querySelectorAll(`[data-${section}-filter]`).forEach((item) => item.setAttribute('aria-pressed', 'false'));
    }
    showUnavailable(apiUrl ? '正在读取当前任务工单' : '当前任务工单接口待接入', apiUrl ? '读取中' : '待接入');
    showClassificationsUnavailable(controlApiUrl ? '正在读取当前任务第一步结果' : '第一步分类结果接口待接入', controlApiUrl ? '读取中' : '待接入');
    try {
      const saved = queryStore.get(queryKey(queryApiUrl ? 'summary' : 'folders', task.task_id));
      if (saved) {
        snapshot = validateSnapshot(saved, task.task_id);
        snapshotComplete = saved.complete && saved.as_of !== null && saved.query_status !== 'preparing_history' &&
          (saved.summary ? saved.summary.counts.unresolved===0 : !saved.folders.some((folder) => folder.stage === 'unresolved'));
        updateFailed = true;
        render();
      }
      const savedClassifications = queryApiUrl ? null : queryStore.get(queryKey('classifications', task.task_id));
      if (savedClassifications) {
        classifications = validateClassifications(savedClassifications, task.task_id);
        classificationComplete = savedClassifications.complete && savedClassifications.as_of !== null && savedClassifications.query_status !== 'preparing_history' &&
          !savedClassifications.items.some((item) => item.status === 'unresolved');
        classificationUpdateFailed = true;
        renderClassifications();
      }
    } catch (_) {
      storageFailed = true;
      element('save-state').textContent = '本机记录未保存或无法恢复';
    }
    updateControls();
    element('refresh-task-button').disabled = !apiUrl && !controlApiUrl;
    if (refreshNow && apiUrl) refresh();
    if (refreshNow && controlApiUrl) {
      refreshClassifications();
      checkPendingControls(task.task_id);
      checkPendingRetries(task.task_id);
    }
  }

  async function sendTaskControl(action) {
    const taskId = selectedTask.task_id;
    const version = selectionVersion;
    if (knownOffline()) {
      element('control-message').textContent = '当前离线，暂不可提交操作';
      updateControls();
      return;
    }
    let operation;
    try {
      const state = readControlState(taskId);
      if (state.pending.some((entry) => entry.action === action) ||
          (action === 'start' && state.pending.length)) return;
      operation = { request_id: crypto.randomUUID(), action, intent_seq: state.intent_seq + 1 };
      state.intent_seq = operation.intent_seq;
      state.pending.push(operation);
      writeControlState(taskId, state);
    } catch (_) {
      element('control-message').textContent = '本机操作记录未保存，无法提交';
      updateControls();
      return;
    }
    updateControls();
    if (snapshot) render();
    element('control-message').textContent = action === 'start' ? '正在提交开始请求' : '正在提交暂停请求';
    try {
      const response = await fetch(`${controlApiUrl}/tasks/${encodeURIComponent(taskId)}/${action}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: operation.request_id, intent_seq: operation.intent_seq }),
        signal: AbortSignal.timeout(action === 'start' ? 120000 : 8000),
      });
      const value = await response.json();
      if (version !== selectionVersion || readControlState(taskId).intent_seq !== operation.intent_seq) return;
      if (response.ok && value.accepted === true && value.request_id === operation.request_id) {
        element('control-message').textContent = `${action === 'start' ? '开始' : '暂停'}请求已提交，结果待核对`;
      } else if (response.status === 409 && value.detail && typeof value.detail === 'object' && Array.isArray(value.detail.checks)) {
        const failed = value.detail.checks.filter((check) => !check.ready).map((check) => `${checkLabels[check.name] || check.name}：${check.detail}`).join('；');
        element('control-message').textContent = `依赖自检未通过：${failed}`;
      } else if (response.status >= 400 && response.status < 500 && typeof value.detail === 'string') {
        element('control-message').textContent = value.detail;
      } else {
        element('control-message').textContent = '操作结果待核对，请查看当前任务状态';
      }
    } catch (_) {
      if (version === selectionVersion) {
        try {
          if (readControlState(taskId).intent_seq === operation.intent_seq) element('control-message').textContent = '操作结果待核对，请查看当前任务状态';
        } catch (_) { /* Keep the original record for recovery. */ }
      }
    } finally {
      const refreshed = await checkControlOperation(taskId, operation);
      if (!refreshed && version === selectionVersion) {
        await queryStore.cancel(queryKey(queryApiUrl ? 'summary' : 'folders', taskId));
        if (version === selectionVersion) await refresh();
      }
    }
  }
  async function finishTaskDeletion(taskId) {
    deletedTaskIds.add(taskId);
    if (selectedTask?.task_id === taskId) {
      selectionVersion += 1;
      folderRequestVersion += 1;
      classificationRequestVersion += 1;
    }
    await queryStore.removeTask(sourceId(), taskId);
    localStorage.removeItem(controlKey(taskId));
    const remembered = readLocal(lastTaskKey);
    if (remembered?.task_id === taskId && remembered.source === sourceId()) localStorage.removeItem(lastTaskKey);
    for (let i = localStorage.length - 1; i >= 0; i -= 1) {
      const key = localStorage.key(i);
      if (key?.startsWith("paperflow-pending-retry:" + taskId + ":")) localStorage.removeItem(key);
    }
    for (const key of retryMessages.keys()) {
      if (key.startsWith("paperflow-pending-retry:" + taskId + ":")) retryMessages.delete(key);
    }
    if (selectedTask?.task_id === taskId) {
      selectedTask = null;
      snapshot = null;
      classifications = null;
      element('task-detail').hidden = true;
      element('task-detail').textContent = '';
      snapshotComplete = classificationComplete = false;
      folderReadPending = false;
      element('task-name').textContent = '请选择任务';
      element('save-state').textContent = '任务已删除';
      showUnavailable('任务已删除', '已删除');
      showClassificationsUnavailable('任务已删除', '已删除');
      element('control-message').textContent = '任务已删除，原件和 NAS/RAG 已有成果已保留';
      updateControls();
    }
  }
  function deletionFailed(taskId, operation, reason) {
    const state = readControlState(taskId);
    state.pending = state.pending.filter((item) => item.request_id !== operation.request_id);
    state.deleteFailed = true;
    state.retryDelete = operation;
    state.guard = { action: 'delete', status: 'failed', intent_seq: operation.intent_seq };
    writeControlState(taskId, state);
    if (selectedTask?.task_id === taskId) {
      element('control-message').textContent = '删除未完成：' + reason;
      updateControls();
    }
  }
  async function checkTaskDeletion(taskId, operation) {
    try {
      const response = await fetch(`${controlApiUrl}/tasks/${encodeURIComponent(taskId)}/deletion`, {
        cache: 'no-store', signal: AbortSignal.timeout(8000),
      });
      if (!response.ok) return;
      const value = await response.json();
      if (value.task_id !== taskId) return;
      if (value.deleted === true) return await finishTaskDeletion(taskId);
      if (value.status === 'failed' || value.status === 'present') deletionFailed(taskId, operation, value.reason || '请重试清理');
    } catch (_) {
      // Keep the original delete identity pending; checking never resends a POST.
    }
  }
  async function sendTaskDeletion() {
    if (!selectedTask || !controlApiUrl || knownOffline()) return;
    const taskId = selectedTask.task_id;
    let state;
    let operation;
    try {
      state = readControlState(taskId);
      if (state.pending.some((item) => item.action === 'delete')) return;
      if (!confirm('删除当前任务的全部工单、快照、索引、缓存和中间产物？源文件原件及 NAS/RAG 已有成果会保留。')) return;
      operation = state.retryDelete?.intent_seq === state.intent_seq ? state.retryDelete : { request_id: crypto.randomUUID(), action: 'delete', intent_seq: state.intent_seq + 1 };
      state.intent_seq = operation.intent_seq;
      state.pending.push(operation);
      state.deleteFailed = false;
      writeControlState(taskId, state);
    } catch (_) {
      element('control-message').textContent = '本机操作记录未保存，无法提交删除';
      return;
    }
    updateControls();
    element('control-message').textContent = '正在停止并删除本任务';
    try {
      const response = await fetch(`${controlApiUrl}/tasks/${encodeURIComponent(taskId)}/delete`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: operation.request_id, intent_seq: operation.intent_seq }),
        signal: AbortSignal.timeout(30000),
      });
      const value = await response.json();
      if (response.ok && value.task_id === taskId && value.request_id === operation.request_id && value.deleted === true) {
        await finishTaskDeletion(taskId);
        return;
      }
      if (response.status === 409) {
        await checkTaskDeletion(taskId, operation);
        return;
      }
    } catch (_) {
      if (selectedTask?.task_id === taskId) element('control-message').textContent = '删除结果待核对，正在查询原请求';
    }
    await checkTaskDeletion(taskId, operation);
    updateControls();
  }
  element('delete-task-button').addEventListener('click', sendTaskDeletion);

  element('start-button').addEventListener('click', () => sendTaskControl('start'));
  element('pause-button').addEventListener('click', () => sendTaskControl('pause'));
  element('refresh-task-button').addEventListener('click', async () => {
    if (!selectedTask) return;
    const taskId = selectedTask.task_id;
    const version = selectionVersion;
    await Promise.all([queryStore.cancel(queryKey(queryApiUrl ? 'summary' : 'folders', taskId)), queryStore.cancel(queryKey('classifications', taskId))]);
    if (version !== selectionVersion) return;
    if (apiUrl) refresh();
    if (controlApiUrl) {
      refreshClassifications();
      checkPendingControls(selectedTask.task_id);
      checkPendingRetries(selectedTask.task_id);
    }
  });

  function refreshVisibleTask() {
    if (!selectedTask || document.visibilityState === 'hidden') return;
    if (apiUrl && !folderReadPending) refresh();
    if (controlApiUrl) {
      refreshClassifications();
      checkPendingControls(selectedTask.task_id);
      checkPendingRetries(selectedTask.task_id);
    }
  }

  if (typeof document.addEventListener === 'function') {
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') refreshVisibleTask();
    });
  }
  if (typeof globalThis.addEventListener === 'function') {
    globalThis.addEventListener('offline', updateControls);
    globalThis.addEventListener('online', () => { updateControls(); refreshVisibleTask(); });
  }

  function validateTasks(value) {
    if (!value || !Array.isArray(value.tasks) || typeof value.complete !== 'boolean' ||
        !(value.as_of === null || Number.isFinite(Date.parse(value.as_of))) ||
        value.tasks.some((task) => !task || typeof task.task_id !== 'string' || !task.task_id || typeof task.task_name !== 'string')) {
      throw new Error('任务数据格式不正确');
    }
    return value;
  }

  function renderTasks(value, stale) {
      const rows = value.tasks.filter((task) => !deletedTaskIds.has(task.task_id)).map((task) => {
        const row = document.createElement('div');
        const button = document.createElement('button');
        row.className = 'task-row';
        button.className = 'task-select';
        button.type = 'button';
        button.dataset.taskId = task.task_id;
        button.textContent = task.task_name;
        button.addEventListener('click', () => {
          element('open-task-dialog').close();
          selectTask(task);
        });
        row.append(button);
        return row;
      });
      element('task-list').replaceChildren(...rows);
      const message = value.complete ? (rows.length ? '请选择任务' : '暂无任务') : '任务列表不完整，请稍后刷新';
      element('task-list-message').textContent = stale ? `更新失败 · 上次任务列表 · ${message}` : message;
  }

  element('open-task-button').addEventListener('click', async () => {
    element('open-task-dialog').showModal();
    element('task-list-message').textContent = '正在读取任务列表';
    element('task-list').replaceChildren();
    const key = queryKey('tasks');
    const source = sourceId();
    let saved;
    try {
      saved = queryStore.get(key);
      if (saved) renderTasks(validateTasks(saved), true);
    } catch (_) { saved = null; }
    try {
      const value = await queryStore.fetch(key, async ({ signal }) => {
        const response = await fetch(queryApiUrl ? `${queryApiUrl}/tasks` : tasksApiUrl, { cache: 'no-store', signal: AbortSignal.any([signal, AbortSignal.timeout(120000)]) });
        if (!response.ok) throw new Error('任务列表暂不可用');
        return validateTasks(await response.json());
      });
      if (source !== sourceId()) return;
      renderTasks(value, value.query_status === 'stale');
    } catch (_) {
      if (source === sourceId()) {
        if (saved) renderTasks(saved, true);
        else element('task-list-message').textContent = '任务列表暂不可用';
      }
    }
  });
  element('close-open-task').addEventListener('click', () => element('open-task-dialog').close());
  element('failed-folders-button').addEventListener('click', async () => {
    element('failed-folders-panel').hidden = false;
    if (queryApiUrl && !await loadFolderDetails({ failed: 'true' })) return;
    renderFailures();
  });
  element('close-failed-folders').addEventListener('click', () => { element('failed-folders-panel').hidden = true; });

  document.querySelectorAll('[data-filter]').forEach((button) => {
    button.addEventListener('click', async () => {
      if (!classifications) return;
      if (queryApiUrl && !await loadClassificationDetails()) return;
      classificationList.filter = button.dataset.filter;
      classificationList.page = 1;
      document.querySelectorAll('[data-filter]').forEach((item) => item.setAttribute('aria-pressed', String(item === button)));
      element('list-panel').hidden = false;
      renderClassificationList();
    });
  });
  element('close-list').addEventListener('click', () => {
    classificationList.filter = null;
    element('list-panel').hidden = true;
    document.querySelectorAll('[data-filter]').forEach((button) => button.setAttribute('aria-pressed', 'false'));
  });
  element('previous-page').addEventListener('click', () => {
    if (classificationList.page > 1) { classificationList.page -= 1; renderClassificationList(); }
  });
  element('next-page').addEventListener('click', () => {
    classificationList.page += 1;
    renderClassificationList();
  });

  for (const section of Object.keys(sections)) {
    document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => {
      button.addEventListener('click', async () => {
        if (!snapshot) return;
        if (queryApiUrl && !await loadFolderDetails({ section, result: button.dataset[`${section}Filter`] })) return;
        lists[section].filter = button.dataset[`${section}Filter`];
        lists[section].page = 1;
        document.querySelectorAll(`[data-${section}-filter]`).forEach((item) => {
          item.setAttribute('aria-pressed', String(item === button));
        });
        element(`${section}-list-panel`).hidden = false;
        renderList(section);
      });
    });
    element(`${section}-close-list`).addEventListener('click', () => {
      lists[section].filter = null;
      lists[section].rows = [];
      lists[section].requestVersion += 1;
      element(`${section}-list-panel`).hidden = true;
      document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => button.setAttribute('aria-pressed', 'false'));
    });
    element(`${section}-previous-page`).addEventListener('click', async () => {
      if (lists[section].page > 1) {
        const page=lists[section].page-1;
        if (queryApiUrl && !await loadFolderDetails({section,result:lists[section].filter,offset:(page-1)*20,limit:20})) return;
        lists[section].page=page;renderList(section);
      }
    });
    element(`${section}-next-page`).addEventListener('click', async () => {
      const page=lists[section].page+1;
      if (queryApiUrl && !await loadFolderDetails({section,result:lists[section].filter,offset:(page-1)*20,limit:20})) return;
      lists[section].page=page;renderList(section);
    });
  }

  showUnavailable('等待后端接入 Prefect 文件夹工单接口', '待接入');
  showClassificationsUnavailable('第一步分类结果接口待接入', '待接入');
  element('open-task-button').disabled = true;
  element('new-task-button').disabled = true;
  function applyConfig(config, fresh = false) {
    if (typeof config.tasks_api_url !== 'string' || typeof config.folder_tickets_api_url !== 'string' || typeof config.control_api_url !== 'string') throw new Error('接口配置格式不正确');
    tasksApiUrl = config.tasks_api_url;
    apiUrl = config.folder_tickets_api_url;
    controlApiUrl = config.control_api_url.replace(/\/$/, '');
    automaticGatewayCapacity = fresh && config.automatic_gateway_capacity === true;
    updateNotice();
    element('open-task-button').disabled = !tasksApiUrl;
    element('new-task-button').disabled = !controlApiUrl;
    if (tasksApiUrl && !selectedTask) showUnavailable('请先选择任务', '请选择');
  }

  async function initialize() {
    await queryStore.restore();
    queryApiUrl = readLocal('paperflow-read-api-v1') || queryApiUrl;
    try {
      const response = await fetch('query-config.json', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
      if (response.ok) {
        const config = await response.json();
        if (typeof config.read_api_url !== 'string' || !/^https?:\/\//.test(config.read_api_url)) throw new Error('查询接口配置无效');
        queryApiUrl = config.read_api_url.replace(/\/$/, '');
        saveLocal('paperflow-read-api-v1', queryApiUrl);
      }
    } catch (_) { /* Keep the read endpoint; never fall back to heavy history on a summary page. */ }
    if (queryApiUrl) queryStore.compact?.();
    const savedConfig = readLocal(lastConfigKey);
    const savedTask = readLocal(lastTaskKey);
    try {
      if (savedConfig) applyConfig(savedConfig);
      if (savedTask && typeof savedTask.task_id === 'string' && typeof savedTask.task_name === 'string' && savedTask.source === sourceId()) {
        selectTask(savedTask, { refreshNow: false, remember: false });
      }
    } catch (_) { /* A malformed saved configuration is not an endpoint. */ }
    try {
      const response = await fetch('api-config.json', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
      if (!response.ok) throw new Error('接口配置暂不可用');
      const config = await response.json();
      const previousSource = sourceId();
      applyConfig(config, true);
      saveLocal(lastConfigKey, config);
      if (selectedTask && previousSource !== sourceId()) {
        selectionVersion += 1;
        selectedTask = null;
        snapshot = null;
        classifications = null;
        showUnavailable('接口来源已变化，请重新选择任务', '请选择');
        showClassificationsUnavailable('接口来源已变化，请重新选择任务', '请选择');
        element('task-name').textContent = '请重新选择任务';
        updateControls();
      }
    } catch (_) {
      if (!savedConfig) showUnavailable('前端接口配置暂不可用', '暂不可用');
    }
    refreshVisibleTask();
    if (apiUrl || controlApiUrl) setInterval(() => {
      if (!selectedTask || document.visibilityState === 'hidden') return;
      const operations = pendingControls(selectedTask.task_id);
      let guard;
      try { guard = readControlState(selectedTask.task_id).guard; } catch (_) { /* Keep polling on unreadable state. */ }
      const pending = !snapshot || snapshot.has_pending !== false || !operations || operations.length || guard || pendingRetryFolders().length;
      const failed = updateFailed || classificationUpdateFailed;
      if (!pending && !failed) return;
      if (Date.now() - lastPollAt < (failed ? 30000 : 10000)) return;
      refreshVisibleTask();
    }, 10000);
    if (typeof navigator !== 'undefined' && navigator.serviceWorker && globalThis.isSecureContext) {
      navigator.serviceWorker.register('service-worker.js').catch(() => {});
    }
  }
  initialize();

  function validateSummary(value) {
    const integer = (number) => Number.isSafeInteger(number) && number >= 0;
    if (!integer(value.folder_total) || !value.counts || !value.results || !value.failures || !value.uncertain_results ||
        [...Object.keys(stages),'waiting_nas_archive_delete','nas_in_progress','waiting_cleanup','cleanup_in_progress','failed','unresolved'].some((key)=>!integer(value.counts[key])) ||
        Object.keys(sections).some((key)=>!value.results[key] || Object.keys(resultLabels).some((result)=>!integer(value.results[key][result])) || typeof value.uncertain_results[key] !== 'boolean') ||
        Object.keys(failedStages).some((key)=>!integer(value.failures[key])) || !integer(value.body) || !integer(value.attachment) ||
        typeof value.body_unknown !== 'boolean' || typeof value.attachment_unknown !== 'boolean') throw new Error('汇总格式不正确');
  }

  async function loadFolderDetails(criteria) {
    const taskId=selectedTask.task_id, version=selectionVersion;
    const list=criteria.section ? lists[criteria.section] : null;
    const requestVersion=list ? ++list.requestVersion : null;
    try {
      const url=new URL(`${queryApiUrl}/folder-tickets`);url.searchParams.set('task_id',taskId);
      for (const [key,value] of Object.entries(criteria)) url.searchParams.set(key,value);
      const response=await fetch(url.toString(),{cache:'no-store',signal:AbortSignal.timeout(15000)});
      if (!response.ok) throw new Error('工单详情暂不可用');
      const value=validateSnapshot(await response.json(),taskId);
      if (version!==selectionVersion || (list && requestVersion!==list.requestVersion)) return false;
      if (list) list.rows=value.folders;
      if (criteria.failed) failedDetailFolders=value.folders;
      if (criteria.folder_id) {
        failedDetailFolders=[...failedDetailFolders.filter((folder)=>folder.folder_id!==criteria.folder_id),...value.folders];
      }
      const replaced=(folder)=>criteria.folder_id ? folder.folder_id===criteria.folder_id
        : criteria.failed ? folder.stage==='failed'
        : criteria.section ? folder[`${criteria.section}_result`]===criteria.result : true;
      const rows=new Map(detailFolders.filter((folder)=>!replaced(folder)).map((folder)=>[folder.ticket_id,folder]));
      for (const folder of value.folders) rows.set(folder.ticket_id,folder);
      detailFolders=[...rows.values()];snapshot={...snapshot,folders:detailFolders};
      return true;
    } catch (_) {
      if (version===selectionVersion) element('failed-folders-message').textContent='工单列表暂不可用，请稍后再试';
      return false;
    }
  }

  async function loadClassificationDetails() {
    const taskId=selectedTask.task_id,version=selectionVersion;
    try {
      const response=await fetch(`${queryApiUrl}/tasks/${encodeURIComponent(taskId)}/classifications`,{cache:'no-store',signal:AbortSignal.timeout(15000)});
      if (!response.ok) throw new Error('分类列表暂不可用');
      const value=validateClassifications(await response.json(),taskId);
      if (version!==selectionVersion) return false;
      classifications={...classifications,items:value.items};
      return true;
    } catch (_) {
      if (version===selectionVersion) element('list-message').textContent='分类列表暂不可用，请稍后再试';
      return false;
    }
  }
})();
