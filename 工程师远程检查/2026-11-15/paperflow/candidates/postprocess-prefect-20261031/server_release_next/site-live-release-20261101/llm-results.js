(() => {
  'use strict';

  // Use the same folder-order snapshot and interaction as preprocessing.
  const base = 'http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary';
  const retryBase = 'http://127.0.0.1:14207/api/v1/llm-retries';
  const retryStorageKey = 'paperflow.llm-retry.pending.v1';
  const retryHistoryKey = 'paperflow.llm-retry.history.v1';
  const labels = { passed: '已通过', failed: '失败', unrecognized: '未识别' };
  const ids = { passed: 'llm-passed-count', failed: 'llm-failed-count', unrecognized: 'llm-unrecognized-count' };
  const number = new Intl.NumberFormat('zh-CN');
  const state = { filter: null, page: 1, pageSize: 20, pages: 1, sequence: 0, items: [], detail: null, partial: true };
  const retry = { pending: null, feedback: '', rows: {}, latestItems: [], latestStarted: 0, latestRequestId: null };
  const monitoredRetries = new Set();
  let loadingRetryHistory = false;
  let renderedRows = null;
  const element = (id) => document.getElementById(id);
  const validCount = (value) => Number.isSafeInteger(value) && value >= 0 ? value : null;

  function savePending(value) {
    retry.pending = value;
    try {
      if (value) localStorage.setItem(retryStorageKey, JSON.stringify(value));
      else localStorage.removeItem(retryStorageKey);
    } catch (_) { /* The durable server-side request ID remains authoritative. */ }
    refreshRetryUI();
  }

  function folderKey(item) { return `${item.task_id}:${item.group_id}`; }

  function retryMessage(result) {
    const counts = { queued: 0, pending: 0, submitted: 0, completed: 0,
      not_sent: 0, paused: 0, failed: 0, needs_review: 0, reconciling: 0, already_running: 0 };
    for (const item of result.items || []) {
      if (Object.hasOwn(counts, item.state)) counts[item.state] += 1;
    }
    const parts = [`已入队 ${counts.queued} 个`, `已提交或处理中 ${counts.submitted} 个`, `待核对 ${counts.needs_review} 个`];
    if (counts.completed) parts.push(`已完成 ${counts.completed} 个`);
    if (counts.not_sent) parts.push(`本次未提交 ${counts.not_sent} 个`);
    if (counts.paused) parts.push(`已暂停 ${counts.paused} 个`);
    if (counts.reconciling) parts.push(`工单恢复核对中 ${counts.reconciling} 个`);
    if (counts.failed) parts.push(`再次失败 ${counts.failed} 个`);
    if (counts.pending) parts.push(`提交中 ${counts.pending} 个`);
    if (counts.already_running) parts.push(`已有重试 ${counts.already_running} 个`);
    return (result.state === 'failed' || result.state === 'interrupted' ?
      `${result.message || '重试状态待核对'} · ` : '') + parts.join(' · ');
  }

  function showRetryResult(result, started) {
    for (const item of result.items || []) {
      const key = folderKey(item);
      const previous = retry.rows[key];
      if (!previous || previous.requestId === result.request_id || previous.started <= started) {
        retry.rows[key] = { started: Math.max(started, previous && previous.requestId === result.request_id ? previous.started : 0),
          requestId: result.request_id, message: item.message || '状态待核对', item };
      }
    }
    if (result.request_id === retry.latestRequestId || started >= retry.latestStarted) {
      retry.latestStarted = Math.max(retry.latestStarted, started);
      retry.latestRequestId = result.request_id;
      retry.feedback = retryMessage(result);
      retry.latestItems = [...(result.items || [])];
    }
    refreshRetryUI();
  }

  function rememberRetry(entry) {
    try {
      const history = JSON.parse(localStorage.getItem(retryHistoryKey) || '[]');
      const prior = Array.isArray(history) ? history.filter((item) => item.request_id !== entry.request_id) : [];
      localStorage.setItem(retryHistoryKey, JSON.stringify([...prior, entry].slice(-10)));
    } catch (_) { /* The server still retains the authoritative record. */ }
  }

  async function monitorRetry(entry) {
    if (monitoredRetries.has(entry.request_id)) return;
    monitoredRetries.add(entry.request_id);
    try {
    while (true) {
      try {
        const response = await fetch(`${retryBase}/${encodeURIComponent(entry.request_id)}`,
          { cache: 'no-store', signal: AbortSignal.timeout(8000) });
        if (!response.ok) throw new Error('status unavailable');
        const result = await response.json();
        showRetryResult(result, entry.started);
        if (!['queued', 'running'].includes(result.state) &&
            !(result.items || []).some((item) => ['queued', 'pending', 'submitted', 'reconciling'].includes(item.state))) {
          if (state.filter) loadList();
          return;
        }
      } catch (_) {
        if (entry.request_id === retry.latestRequestId || entry.started >= retry.latestStarted) {
          retry.feedback = '重试状态暂不可用，正在查询';
          refreshRetryUI();
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
    } finally {
      monitoredRetries.delete(entry.request_id);
    }
  }

  async function loadRetryHistory() {
    if (loadingRetryHistory) return;
    loadingRetryHistory = true;
    try {
      let offset = 0;
      let total = null;
      while (total === null || offset < total) {
        const response = await fetch(`${retryBase}?limit=100&offset=${offset}`,
          { cache: 'no-store', signal: AbortSignal.timeout(8000) });
        if (!response.ok) throw new Error('history unavailable');
        const page = await response.json();
        if (!Number.isSafeInteger(page.total) || page.total < 0 || !Array.isArray(page.commands)) {
          throw new Error('invalid history');
        }
        if (total === null) total = page.total;
        if (!page.commands.length) break;
        for (const result of page.commands) {
          if (typeof result.request_id !== 'string' || !Number.isSafeInteger(result.started_ms)) continue;
          const entry = { request_id: result.request_id, started: result.started_ms };
          retry.latestStarted = Math.max(retry.latestStarted, entry.started);
          showRetryResult(result, entry.started);
          if (['queued', 'running'].includes(result.state) ||
              (result.items || []).some((item) => ['queued', 'pending', 'submitted', 'reconciling'].includes(item.state))) {
            monitorRetry(entry);
          }
        }
        offset += page.commands.length;
      }
    } catch (_) {
      if (!retry.feedback) retry.feedback = '历史重试记录暂不可用；不会自动提交';
      refreshRetryUI();
    } finally {
      loadingRetryHistory = false;
    }
  }

  function refreshRetryUI() {
    const bulk = element('llm-retry-all');
    const message = element('llm-retry-message');
    if (!bulk || !message) return;
    bulk.hidden = state.filter !== 'failed';
    bulk.disabled = Boolean(retry.pending) || state.partial;
    message.hidden = state.filter !== 'failed' || !retry.feedback;
    const latestTime = retry.latestStarted ? new Date(retry.latestStarted).toLocaleTimeString('zh-CN', { hour12: false }) : '';
    message.textContent = `${latestTime ? `最近一次重试（${latestTime}）：` : ''}${retry.feedback}`;
    const details = element('llm-retry-details');
    const detailList = element('llm-retry-detail-list');
    if (details && detailList) {
      details.hidden = state.filter !== 'failed' || !retry.latestItems.length;
      detailList.replaceChildren();
      for (const item of retry.latestItems) {
        const row = document.createElement('li');
        const name = item.paper_title || item.folder_label || item.group_id;
        row.append(document.createTextNode(`${name}：${item.message || '状态待核对'}`));
        if (item.state === 'not_sent') {
          const button = document.createElement('button');
          button.type = 'button';
          button.className = 'text-button';
          button.textContent = '重新请求';
          button.disabled = Boolean(retry.pending);
          button.addEventListener('click', () => requestRetry('one', item));
          row.append(' ', button);
        }
        detailList.append(row);
      }
    }
    renderedRows = null;
    if (state.filter === 'failed') renderRows();
  }

  async function pollRetry(pending, started) {
    while (retry.pending?.request_id === pending.request_id) {
      try {
        const response = await fetch(`${retryBase}/${encodeURIComponent(pending.request_id)}`,
          { cache: 'no-store', signal: AbortSignal.timeout(8000) });
        if (response.status === 404) {
          // Loading the page or losing a response must never issue another
          // retry command. Preserve the original ID while its outcome is
          // uncertain and continue read-only observation.
          retry.feedback = '原重试操作尚未确认，正在核对；未自动重新提交';
          refreshRetryUI();
          await new Promise((resolve) => setTimeout(resolve, 2500));
          continue;
        }
        if (!response.ok) throw new Error('status unavailable');
        const result = await response.json();
        if (['finished', 'failed', 'interrupted'].includes(result.state)) {
          const entry = { request_id: pending.request_id, started };
          rememberRetry(entry);
          showRetryResult(result, started);
          savePending(null);
          if (state.filter) loadList();
          if ((result.items || []).some((item) => ['queued', 'pending', 'submitted', 'reconciling'].includes(item.state))) {
            monitorRetry(entry);
          }
          return;
        }
        retry.feedback = result.message || '正在核对原请求';
        refreshRetryUI();
      } catch (_) {
        retry.feedback = '提交或查询状态暂不可用，正在保留请求编号，避免重复提交';
        refreshRetryUI();
      }
      await new Promise((resolve) => setTimeout(resolve, 2500));
    }
  }

  async function requestRetry(scope, item = null) {
    if (retry.pending || (scope === 'all' && state.partial) || state.filter !== 'failed') return;
    const request = { request_id: crypto.randomUUID(), scope };
    const started = Date.now();
    retry.latestStarted = started;
    retry.latestRequestId = request.request_id;
    retry.latestItems = [];
    if (scope === 'one') {
      for (const key of ['task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id']) request[key] = item[key];
    }
    retry.feedback = '正在提交核对请求';
    savePending(request);
    try {
      const response = await fetch(retryBase, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request), signal: AbortSignal.timeout(8000) });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        if (response.status >= 400 && response.status < 500 && response.status !== 408 && response.status !== 409) {
          retry.feedback = payload.detail || '请求未被接受';
          savePending(null);
          return;
        }
        throw new Error('submission outcome unknown');
      }
    } catch (_) {
      retry.feedback = '提交结果待核对，正在按原请求编号查询';
      refreshRetryUI();
    }
    pollRetry(request, started);
  }

  function renderCounts(result) {
    const counts = result?.available === true ? result.counts || {} : {};
    for (const [key, id] of Object.entries(ids)) {
      const value = validCount(counts[key]);
      const target = element(id);
      const partial = value !== null && result?.partial === true;
      target.textContent = value === null ? '—' : `${partial ? '≥' : ''}${number.format(value)}`;
      target.setAttribute('aria-label', `LLM ${labels[key]}：${value === null ? '暂不可用' : `${partial ? '至少 ' : ''}${number.format(value)} 个文件夹`}`);
    }
    const hint = element('llm-results-hint');
    if (result?.available !== true) {
      hint.textContent = '全部任务 · 结果暂不可用';
      return;
    }
    const time = new Date(result.as_of);
    const stamp = Number.isNaN(time.getTime()) ? '' : ` · ${time.toLocaleTimeString('zh-CN', { hour12: false })}`;
    const parts = [result.partial ? '部分工单待核对' : '按文件夹统计'];
    const pending = validCount(result.unconfirmed_folders);
    if (pending > 0) parts.push(`${result.partial ? '至少 ' : ''}${number.format(pending)} 个文件夹的工单或请求状态待核对`);
    if (validCount(counts.unrecognized) === null) parts.push('未识别结果暂不可统计');
    parts.push('通过指原始回复已完成登记，尚未解析');
    hint.textContent = parts.join(' · ') + stamp;
  }

  function line(className, text) {
    const item = document.createElement('p');
    item.className = className;
    item.textContent = text;
    return item;
  }

  function closeDetail() {
    state.detail = null;
  }

  function isDetailFor(view, item) {
    return view && view.taskId === item.task_id && view.groupId === item.group_id &&
      view.paperId === item.paper_id && view.receiptId === item.receipt_id;
  }

  function renderFolderDetail(item) {
    const panel = document.createElement('div');
    panel.className = 'entry-files';
    panel.append(line('entry-files-message', '以下文件名来自已核验的前处理交接记录，不代表每个文件已发送、识别完成或已生成 LLM 输出。'));
    const files = item.input_files;
    if (!files || !Array.isArray(files.body) || !Array.isArray(files.attachment)) {
      panel.append(line('entry-files-message', '文件明细暂不可用，请稍后再查看。'));
      return panel;
    }
    for (const [key, label] of [['body', '正文'], ['attachment', '附件']]) {
      const section = document.createElement('div');
      section.className = 'entry-file-section';
      const heading = document.createElement('strong');
      heading.textContent = label;
      section.append(heading);
      if (!files[key].length) section.append(line('entry-file-empty', '无'));
      for (const file of files[key]) {
        const name = typeof file?.name === 'string' && file.name ? file.name : '文件名暂不可用';
        section.append(line('entry-file-name', name));
      }
      panel.append(section);
    }
    return panel;
  }

  function toggleFolderDetail(item) {
    const same = isDetailFor(state.detail, item);
    closeDetail();
    if (!same) state.detail = { taskId: item.task_id, groupId: item.group_id,
      paperId: item.paper_id, receiptId: item.receipt_id };
    renderRows();
  }

  function renderRow(item) {
    const row = document.createElement('li');
    row.dataset.taskId = item.task_id || '';
    row.dataset.groupId = item.group_id || '';
    const body = document.createElement('div');
    body.className = 'entry-content';
    const title = document.createElement('span');
    title.className = 'entry-title';
    const hasTitle = item.title_status === '已提取' && typeof item.paper_title === 'string' && item.paper_title.trim();
    const hasCandidate = item.title_status === '待核对' && typeof item.paper_title_candidate === 'string' && item.paper_title_candidate.trim();
    title.textContent = hasTitle ? item.paper_title : item.folder_label || item.group_id || '资料名称暂不可用';
    const expanded = isDetailFor(state.detail, item);
    const toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'entry-toggle';
    toggle.disabled = !item.task_id || !item.group_id || !item.paper_id || !item.receipt_id;
    toggle.setAttribute('aria-expanded', String(Boolean(expanded)));
    toggle.append(title);
    const action = document.createElement('span');
    action.className = 'entry-toggle-label';
    action.textContent = expanded ? '收起明细' : '查看明细';
    toggle.append(action);
    toggle.addEventListener('click', () => toggleFolderDetail(item));
    body.append(toggle);
    if (hasTitle && item.folder_label) body.append(line('entry-detail', `文件夹：${item.folder_label}`));
    if (hasCandidate) body.append(line('entry-detail', `候选题目：${item.paper_title_candidate}`));
    if (item.task_name) body.append(line('entry-detail', `所属任务：${item.task_name}`));
    if (state.filter === 'passed') body.append(line('entry-detail', '正文及所需附件的原始回复已完成登记，内容尚未解析'));
    if (item.reason) body.append(line('entry-reason', String(item.reason)));
    if (item.order_note) body.append(line('entry-reason', String(item.order_note)));
    if (state.filter === 'failed') {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'text-button';
      button.textContent = '重新请求';
      button.style.marginTop = '7px';
      button.disabled = Boolean(retry.pending) ||
        !item.task_id || !item.group_id || !item.paper_id || !item.receipt_id || !item.attempt_id;
      button.setAttribute('aria-label', `重新请求：${title.textContent}`);
      button.addEventListener('click', () => requestRetry('one', item));
      body.append(button);
      const previousRetry = retry.rows[folderKey(item)];
      if (previousRetry && previousRetry.requestId === retry.latestRequestId) {
        const when = new Date(previousRetry.started).toLocaleTimeString('zh-CN', { hour12: false });
        const message = previousRetry.message.replace('大任务当前已暂停', '大任务当时已暂停');
        body.append(line('entry-detail', `重试记录（${when}）：${message}`));
      }
    }
    if (expanded) body.append(renderFolderDetail(item));
    const status = document.createElement('span');
    status.className = `entry-status ${state.filter}`;
    status.textContent = labels[state.filter] || '';
    row.append(body, status);
    return row;
  }

  function renderRows() {
    if (state.detail && !state.items.some((item) => isDetailFor(state.detail, item))) closeDetail();
    const view = state.detail;
    const signature = JSON.stringify([state.items, view && [view.taskId, view.groupId, view.paperId, view.receiptId],
      Boolean(retry.pending), retry.rows, state.partial]);
    if (signature === renderedRows) return;
    const list = element('llm-group-list');
    const active = document.activeElement;
    const focusedRow = active?.classList.contains('entry-toggle') ? active.closest('li') : null;
    const focusedTask = focusedRow?.dataset.taskId;
    const focusedGroup = focusedRow?.dataset.groupId;
    const scrollTop = list.scrollTop;
    list.replaceChildren(...state.items.map(renderRow));
    renderedRows = signature;
    list.scrollTop = scrollTop;
    if (focusedRow && focusedTask && focusedGroup) {
      const replacement = [...list.children].find((row) => row.dataset.taskId === focusedTask && row.dataset.groupId === focusedGroup);
      replacement?.querySelector('.entry-toggle')?.focus({ preventScroll: true });
    }
  }

  function showMessage(message) {
    closeDetail();
    state.items = [];
    renderedRows = null;
    element('llm-list-message').hidden = false;
    element('llm-list-message').textContent = message;
    element('llm-group-list').replaceChildren();
    element('llm-pagination').hidden = true;
  }

  async function loadList() {
    if (!state.filter) return;
    const sequence = ++state.sequence;
    const filter = state.filter;
    const page = state.page;
    element('llm-list-count').textContent = '列表获取中';
    element('llm-previous-page').disabled = true;
    element('llm-next-page').disabled = true;
    const url = new URL(base);
    url.searchParams.set('llm_result_filter', filter);
    url.searchParams.set('llm_result_page', String(page));
    url.searchParams.set('llm_result_page_size', String(state.pageSize));
    try {
      const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error('结果暂不可用');
      const payload = await response.json();
      if (sequence !== state.sequence || filter !== state.filter || page !== state.page) return;
      const result = payload?.llm_results;
      renderCounts(result);
      state.partial = result?.available !== true || result.partial !== false;
      refreshRetryUI();
      const list = result?.list;
      if (!list || list.filter !== filter || list.page !== page || !Array.isArray(list.items)) {
        element('llm-list-count').textContent = '暂不可用';
        showMessage(result?.message || '结果暂不可用，请稍后再试');
        return;
      }
      const total = validCount(list.total);
      const pages = total === null ? 1 : Math.max(1, Math.ceil(total / state.pageSize));
      state.pages = pages;
      if (total !== null && page > pages) {
        closeDetail();
        state.page = pages;
        loadList();
        return;
      }
      element('llm-list-count').textContent = total === null ? '数量暂不可用' :
        `${result.partial ? '至少 ' : '共 '}${number.format(total)} 个文件夹 · 列表实时更新`;
      state.items = list.items;
      renderRows();
      element('llm-list-message').hidden = list.items.length > 0;
      element('llm-list-message').textContent = list.message || '暂无资料';
      element('llm-pagination').hidden = total === null ? page <= 1 : pages <= 1;
      element('llm-page-info').textContent = total === null ? `第 ${page} 页 · 总页数待核对` : `第 ${page} / ${pages} 页`;
      element('llm-previous-page').disabled = page <= 1;
      element('llm-next-page').disabled = total === null || page >= pages;
    } catch (_) {
      if (sequence !== state.sequence) return;
      element('llm-list-count').textContent = '暂不可用';
      showMessage('结果暂不可用，请稍后再试');
    }
  }

  function select(filter) {
    closeDetail();
    state.filter = filter;
    state.page = 1;
    state.pages = 1;
    document.querySelectorAll('[data-llm-filter]').forEach((button) => {
      button.setAttribute('aria-pressed', String(button.dataset.llmFilter === filter));
    });
    element('llm-list-panel').hidden = false;
    element('llm-list-title').textContent = `LLM ${labels[filter]}`;
    refreshRetryUI();
    if (filter === 'failed') loadRetryHistory();
    showMessage('正在获取列表…');
    loadList();
  }

  const retryActions = document.createElement('div');
  retryActions.style.display = 'flex';
  retryActions.style.gap = '16px';
  retryActions.style.alignItems = 'center';
  const retryAll = document.createElement('button');
  retryAll.id = 'llm-retry-all';
  retryAll.type = 'button';
  retryAll.className = 'text-button';
  retryAll.textContent = '全部重新请求';
  retryAll.hidden = true;
  retryAll.addEventListener('click', () => requestRetry('all'));
  const closeList = element('llm-close-list');
  closeList.parentNode.insertBefore(retryActions, closeList);
  retryActions.append(retryAll, closeList);
  const retryNotice = document.createElement('p');
  retryNotice.id = 'llm-retry-message';
  retryNotice.className = 'entry-detail';
  retryNotice.setAttribute('role', 'status');
  retryNotice.hidden = true;
  retryActions.closest('.list-header').insertAdjacentElement('afterend', retryNotice);
  const retryDetails = document.createElement('details');
  retryDetails.id = 'llm-retry-details';
  retryDetails.hidden = true;
  const retryDetailsTitle = document.createElement('summary');
  retryDetailsTitle.textContent = '查看最近一次重试明细';
  const retryDetailList = document.createElement('ul');
  retryDetailList.id = 'llm-retry-detail-list';
  retryDetailList.className = 'entry-detail';
  retryDetailList.style.maxHeight = '240px';
  retryDetailList.style.overflowY = 'auto';
  retryDetails.append(retryDetailsTitle, retryDetailList);
  retryNotice.insertAdjacentElement('afterend', retryDetails);

  document.querySelectorAll('[data-llm-filter]').forEach((button) => {
    button.addEventListener('click', () => select(button.dataset.llmFilter));
  });
  element('llm-close-list').addEventListener('click', () => {
    closeDetail();
    state.filter = null;
    state.sequence += 1;
    element('llm-list-panel').hidden = true;
    document.querySelectorAll('[data-llm-filter]').forEach((button) => button.setAttribute('aria-pressed', 'false'));
  });
  element('llm-previous-page').addEventListener('click', () => {
    if (state.page > 1) { closeDetail(); state.page -= 1; loadList(); }
  });
  element('llm-next-page').addEventListener('click', () => {
    if (state.page < state.pages) { closeDetail(); state.page += 1; loadList(); }
  });
  window.addEventListener('paperflow:queue-summary', (event) => {
    renderCounts(event.detail?.llm_results);
    if (state.filter) loadList();
  });
  try {
    const history = JSON.parse(localStorage.getItem(retryHistoryKey) || '[]');
    if (Array.isArray(history)) {
      for (const entry of history.slice(-10)) {
        if (typeof entry.request_id === 'string' && Number.isSafeInteger(entry.started)) {
          retry.latestStarted = Math.max(retry.latestStarted, entry.started);
          monitorRetry(entry);
        }
      }
    }
  } catch (_) { /* A bad history entry never starts a new request. */ }
  try {
    const previous = JSON.parse(localStorage.getItem(retryStorageKey) || 'null');
    if (previous && typeof previous.request_id === 'string' && ['one', 'all'].includes(previous.scope)) {
      retry.feedback = '正在核对上次重试请求';
      savePending(previous);
      retry.latestStarted = Date.now();
      pollRetry(previous, retry.latestStarted);
    }
  } catch (_) { /* A bad local entry never starts a new request. */ }
})();
