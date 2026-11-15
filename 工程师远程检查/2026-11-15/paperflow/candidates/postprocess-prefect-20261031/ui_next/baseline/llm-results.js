(() => {
  'use strict';

  // Use the same folder-order snapshot and interaction as preprocessing.
  const base = 'http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary';
  const labels = { passed: '已通过', failed: '失败', unrecognized: '未识别' };
  const ids = { passed: 'llm-passed-count', failed: 'llm-failed-count', unrecognized: 'llm-unrecognized-count' };
  const number = new Intl.NumberFormat('zh-CN');
  const state = { filter: null, page: 1, pageSize: 20, pages: 1, sequence: 0, items: [], detail: null };
  let renderedRows = null;
  const element = (id) => document.getElementById(id);
  const validCount = (value) => Number.isSafeInteger(value) && value >= 0 ? value : null;

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
    const signature = JSON.stringify([state.items, view && [view.taskId, view.groupId, view.paperId, view.receiptId]]);
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
    showMessage('正在获取列表…');
    loadList();
  }

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
})();
