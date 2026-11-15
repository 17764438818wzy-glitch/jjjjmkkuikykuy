(() => {
  'use strict';

  // This uses the same polling event and list interaction as preprocessing.
  const base = 'http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary';
  const labels = { passed: '已通过', failed: '未通过', unrecognized: '未识别' };
  const ids = { passed: 'postprocessing-passed-count', failed: 'postprocessing-failed-count',
    unrecognized: 'postprocessing-unrecognized-count' };
  const number = new Intl.NumberFormat('zh-CN');
  const state = { filter: null, page: 1, pageSize: 20, pages: 1, hasNext: false, sequence: 0 };
  const element = (id) => document.getElementById(id);
  const validCount = (value) => Number.isSafeInteger(value) && value >= 0 ? value : null;

  function renderCounts(result) {
    const counts = result?.available === true ? result.counts || {} : {};
    for (const [key, id] of Object.entries(ids)) {
      const value = validCount(counts[key]);
      const partial = value !== null && result?.partial === true;
      const target = element(id);
      target.textContent = value === null ? '—' : `${partial ? '≥' : ''}${number.format(value)}`;
      target.setAttribute('aria-label', `后处理${labels[key]}：${value === null ? '暂不可用' :
        `${partial ? '至少 ' : ''}${number.format(value)} 个文件夹`}`);
    }
    const hint = element('postprocessing-results-hint');
    if (result?.available !== true) {
      hint.textContent = '全部任务 · 结果暂不可用';
      return;
    }
    const time = new Date(result.as_of);
    const stamp = Number.isNaN(time.getTime()) ? '' : ` · ${time.toLocaleTimeString('zh-CN', { hour12: false })}`;
    const parts = [result.partial ? '全部任务 · 部分结果待核对' : '全部任务 · 后处理结果'];
    if (validCount(counts.unrecognized) === null) parts.push('未识别结果暂不可统计');
    hint.textContent = parts.join(' · ') + stamp;
  }

  function line(className, value) {
    const item = document.createElement('p');
    item.className = className;
    item.textContent = value;
    return item;
  }

  function addNotes(body, value, label) {
    if (!Array.isArray(value) || !value.length) return;
    for (const note of value.slice(0, 5)) {
      const message = typeof note === 'string' ? note :
        typeof note?.message === 'string' ? note.message :
          typeof note?.reason === 'string' ? note.reason : null;
      if (message) body.append(line('entry-detail', `${label}：${message}`));
    }
    if (value.length > 5) body.append(line('entry-detail', `${label}还有其他记录，请核对工单`));
  }

  function renderRow(item) {
    const row = document.createElement('li');
    const body = document.createElement('div');
    body.className = 'entry-content';
    const title = document.createElement('span');
    title.className = 'entry-title';
    const hasTitle = item.title_status === '已提取' && typeof item.paper_title === 'string' && item.paper_title.trim();
    const hasCandidate = item.title_status === '待核对' && typeof item.paper_title_candidate === 'string' && item.paper_title_candidate.trim();
    title.textContent = hasTitle ? item.paper_title : item.folder_label || item.group_id || '资料名称暂不可用';
    body.append(title);
    if (item.folder_label && item.folder_label !== title.textContent) body.append(line('entry-detail', `文件夹：${item.folder_label}`));
    if (item.task_name) body.append(line('entry-detail', `所属任务：${item.task_name}`));
    else if (item.task_id) body.append(line('entry-detail', `所属任务编号：${item.task_id}`));
    if (item.group_id) body.append(line('entry-detail', `资料编号：${item.group_id}`));
    if (hasCandidate) body.append(line('entry-detail', `候选题目：${item.paper_title_candidate}`));
    if (item.reason) body.append(line('entry-reason', String(item.reason)));
    if (item.order_note) body.append(line('entry-reason', String(item.order_note)));
    addNotes(body, item.gaps, '识别缺口');
    addNotes(body, item.warnings, '警告');
    addNotes(body, item.diagnostics, '诊断');
    const status = document.createElement('span');
    status.className = `entry-status ${state.filter}`;
    status.textContent = labels[state.filter] || '';
    row.append(body, status);
    return row;
  }

  function showMessage(message) {
    element('postprocessing-list-message').hidden = false;
    element('postprocessing-list-message').textContent = message;
    element('postprocessing-group-list').replaceChildren();
    element('postprocessing-pagination').hidden = true;
  }

  async function loadList() {
    if (!state.filter) return;
    const sequence = ++state.sequence;
    const filter = state.filter;
    const page = state.page;
    element('postprocessing-list-count').textContent = '列表获取中';
    element('postprocessing-previous-page').disabled = true;
    element('postprocessing-next-page').disabled = true;
    const url = new URL(base);
    url.searchParams.set('postprocessing_result_filter', filter);
    url.searchParams.set('postprocessing_result_page', String(page));
    url.searchParams.set('postprocessing_result_page_size', String(state.pageSize));
    try {
      const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error('结果暂不可用');
      const payload = await response.json();
      if (sequence !== state.sequence || filter !== state.filter || page !== state.page) return;
      const result = payload?.postprocessing_results;
      renderCounts(result);
      const list = result?.list;
      if (result?.available !== true || !list || list.filter !== filter || list.page !== page ||
          list.page_size !== state.pageSize || !Array.isArray(list.items) ||
          list.items.some((item) => !item || typeof item !== 'object' || Array.isArray(item))) {
        element('postprocessing-list-count').textContent = '暂不可用';
        showMessage(result?.message || '结果暂不可用，请稍后再试');
        return;
      }
      const total = validCount(list.total);
      const pages = total === null ? 1 : Math.max(1, Math.ceil(total / state.pageSize));
      state.pages = pages;
      state.hasNext = total === null ? list.has_next === true : page < pages;
      if (total !== null && page > pages) {
        state.page = pages;
        loadList();
        return;
      }
      element('postprocessing-list-count').textContent = total === null ? '数量暂不可用' :
        `${result.partial ? '至少 ' : '共 '}${number.format(total)} 个文件夹 · 列表实时更新`;
      element('postprocessing-group-list').replaceChildren(...list.items.map(renderRow));
      element('postprocessing-list-message').hidden = list.items.length > 0;
      element('postprocessing-list-message').textContent = list.message || '暂无资料';
      element('postprocessing-pagination').hidden = total === null ? page <= 1 && !state.hasNext : pages <= 1;
      element('postprocessing-page-info').textContent = total === null ? `第 ${page} 页 · 总页数待核对` : `第 ${page} / ${pages} 页`;
      element('postprocessing-previous-page').disabled = page <= 1;
      element('postprocessing-next-page').disabled = !state.hasNext;
    } catch (_) {
      if (sequence !== state.sequence) return;
      element('postprocessing-list-count').textContent = '暂不可用';
      showMessage('结果暂不可用，请稍后再试');
    }
  }

  function select(filter) {
    state.filter = filter;
    state.page = 1;
    state.pages = 1;
    state.hasNext = false;
    document.querySelectorAll('[data-postprocessing-filter]').forEach((button) => {
      button.setAttribute('aria-pressed', String(button.dataset.postprocessingFilter === filter));
    });
    element('postprocessing-list-panel').hidden = false;
    element('postprocessing-list-title').textContent = `后处理${labels[filter]}`;
    showMessage('正在获取列表…');
    loadList();
  }

  document.querySelectorAll('[data-postprocessing-filter]').forEach((button) => {
    button.addEventListener('click', () => select(button.dataset.postprocessingFilter));
  });
  element('postprocessing-close-list').addEventListener('click', () => {
    state.filter = null;
    state.sequence += 1;
    element('postprocessing-list-panel').hidden = true;
    document.querySelectorAll('[data-postprocessing-filter]').forEach((button) => button.setAttribute('aria-pressed', 'false'));
  });
  element('postprocessing-previous-page').addEventListener('click', () => {
    if (state.page > 1) { state.page -= 1; loadList(); }
  });
  element('postprocessing-next-page').addEventListener('click', () => {
    if (state.hasNext) { state.page += 1; loadList(); }
  });
  window.addEventListener('paperflow:queue-summary', (event) => {
    renderCounts(event.detail?.postprocessing_results);
    if (state.filter) loadList();
  });
})();
