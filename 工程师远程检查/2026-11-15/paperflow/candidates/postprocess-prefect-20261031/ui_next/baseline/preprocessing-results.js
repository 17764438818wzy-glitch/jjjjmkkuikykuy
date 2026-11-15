(() => {
  'use strict';

  const base = 'http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary';
  const labels = { passed: '已通过', failed: '失败', unrecognized: '未识别' };
  const ids = { passed: 'preprocessing-passed-count', failed: 'preprocessing-failed-count',
    unrecognized: 'preprocessing-unrecognized-count' };
  const number = new Intl.NumberFormat('zh-CN');
  const state = { filter: null, page: 1, pageSize: 20, pages: 1, sequence: 0 };
  const element = (id) => document.getElementById(id);

  function validCount(value) {
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
  }

  function renderCounts(result) {
    const counts = result?.available === true ? result.counts || {} : {};
    for (const [key, id] of Object.entries(ids)) {
      const value = validCount(counts[key]);
      const target = element(id);
      const partial = value !== null && result?.partial === true;
      target.textContent = value === null ? '—' : `${partial ? '≥' : ''}${number.format(value)}`;
      target.setAttribute('aria-label', `前处理${labels[key]}：${value === null ? '暂不可用' : `${partial ? '至少 ' : ''}${number.format(value)} 个文件夹`}`);
    }
    const hint = element('preprocessing-results-hint');
    if (result?.available !== true) {
      hint.textContent = '全部任务 · 结果暂不可用';
      return;
    }
    const time = new Date(result.as_of);
    const stamp = Number.isNaN(time.getTime()) ? '' : ` · ${time.toLocaleTimeString('zh-CN', { hour12: false })}`;
    const parts = [result.partial ? '全部任务 · 部分结果待核对' : '全部任务 · 前处理结果'];
    if (validCount(result.order_failures) > 0) parts.push(`其中 ${number.format(result.order_failures)} 张文件夹工单后续状态需核对`);
    if (validCount(counts.unrecognized) === null) parts.push('未识别结果暂不可统计');
    hint.textContent = parts.join(' · ') + stamp;
  }

  function line(className, text) {
    const item = document.createElement('p');
    item.className = className;
    item.textContent = text;
    return item;
  }

  function renderRow(item) {
    const row = document.createElement('li');
    const body = document.createElement('div');
    body.className = 'entry-content';
    const title = document.createElement('span');
    title.className = 'entry-title';
    const hasTitle = item.title_status === '已提取' && typeof item.paper_title === 'string' && item.paper_title.trim();
    const hasCandidate = item.title_status === '待核对' && typeof item.paper_title_candidate === 'string' && item.paper_title_candidate.trim();
    title.textContent = hasTitle ? item.paper_title : item.title_status === '待核对' ? '题目待核对' :
      item.title_status === '未提取' ? '题目未识别' : '题目暂不可用';
    body.append(title);
    if (item.folder_label) body.append(line('entry-detail', `文件夹：${item.folder_label}`));
    if (item.task_name) body.append(line('entry-detail', `所属任务：${item.task_name}`));
    if (hasCandidate) body.append(line('entry-detail', `候选题目：${item.paper_title_candidate}`));
    if (!hasTitle && item.group_id) body.append(line('entry-detail', `资料编号：${item.group_id}`));
    if (state.filter === 'passed') body.append(line('entry-detail', '前处理交接已完成，局部审核以资料记录为准'));
    if (item.reason) body.append(line('entry-reason', String(item.reason)));
    if (item.order_note) body.append(line('entry-reason', String(item.order_note)));
    const gaps = Array.isArray(item.gaps) ? item.gaps : [];
    if (gaps.length) {
      body.append(line('entry-detail', `局部识别缺口：${number.format(gaps.length)} 处`));
      for (const gap of gaps.slice(0, 5)) {
        body.append(line('entry-detail', typeof gap?.message === 'string' ? gap.message : '内容需要核对'));
      }
      if (gaps.length > 5) body.append(line('entry-detail', '还有其他位置需要核对'));
    }
    const status = document.createElement('span');
    status.className = `entry-status ${state.filter}`;
    status.textContent = labels[state.filter] || '';
    row.append(body, status);
    return row;
  }

  function showMessage(message) {
    element('preprocessing-list-message').hidden = false;
    element('preprocessing-list-message').textContent = message;
    element('preprocessing-group-list').replaceChildren();
    element('preprocessing-pagination').hidden = true;
  }

  async function loadList() {
    if (!state.filter) return;
    const sequence = ++state.sequence;
    const filter = state.filter;
    const page = state.page;
    element('preprocessing-list-count').textContent = '列表获取中';
    element('preprocessing-previous-page').disabled = true;
    element('preprocessing-next-page').disabled = true;
    const url = new URL(base);
    url.searchParams.set('result_filter', filter);
    url.searchParams.set('result_page', String(page));
    url.searchParams.set('result_page_size', String(state.pageSize));
    try {
      const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error('结果暂不可用');
      const payload = await response.json();
      if (sequence !== state.sequence || filter !== state.filter || page !== state.page) return;
      const result = payload?.preprocessing_results;
      renderCounts(result);
      const list = result?.list;
      if (!list || list.filter !== filter || list.page !== page || !Array.isArray(list.items)) {
        element('preprocessing-list-count').textContent = '暂不可用';
        showMessage(result?.message || '结果暂不可用，请稍后再试');
        return;
      }
      const total = validCount(list.total);
      const pages = total === null ? 1 : Math.max(1, Math.ceil(total / state.pageSize));
      state.pages = pages;
      if (total !== null && page > pages) {
        state.page = pages;
        loadList();
        return;
      }
      element('preprocessing-list-count').textContent = total === null ? '数量暂不可用' :
        `${result.partial ? '至少 ' : '共 '}${number.format(total)} 个文件夹 · 列表实时更新`;
      const groupList = element('preprocessing-group-list');
      groupList.replaceChildren(...list.items.map(renderRow));
      element('preprocessing-list-message').hidden = list.items.length > 0;
      element('preprocessing-list-message').textContent = list.message || '暂无资料';
      element('preprocessing-pagination').hidden = total === null ? page <= 1 : pages <= 1;
      element('preprocessing-page-info').textContent = total === null ? `第 ${page} 页 · 总页数待核对` : `第 ${page} / ${pages} 页`;
      element('preprocessing-previous-page').disabled = page <= 1;
      element('preprocessing-next-page').disabled = total === null || page >= pages;
    } catch (_) {
      if (sequence !== state.sequence) return;
      element('preprocessing-list-count').textContent = '暂不可用';
      showMessage('结果暂不可用，请稍后再试');
    }
  }

  function select(filter) {
    state.filter = filter;
    state.page = 1;
    state.pages = 1;
    document.querySelectorAll('[data-preprocessing-filter]').forEach((button) => {
      button.setAttribute('aria-pressed', String(button.dataset.preprocessingFilter === filter));
    });
    element('preprocessing-list-panel').hidden = false;
    element('preprocessing-list-title').textContent = `前处理${labels[filter]}`;
    showMessage('正在获取列表…');
    loadList();
  }

  document.querySelectorAll('[data-preprocessing-filter]').forEach((button) => {
    button.addEventListener('click', () => select(button.dataset.preprocessingFilter));
  });
  element('preprocessing-close-list').addEventListener('click', () => {
    state.filter = null;
    state.sequence += 1;
    element('preprocessing-list-panel').hidden = true;
    document.querySelectorAll('[data-preprocessing-filter]').forEach((button) => button.setAttribute('aria-pressed', 'false'));
  });
  element('preprocessing-previous-page').addEventListener('click', () => {
    if (state.page > 1) { state.page -= 1; loadList(); }
  });
  element('preprocessing-next-page').addEventListener('click', () => {
    if (state.page < state.pages) { state.page += 1; loadList(); }
  });
  window.addEventListener('paperflow:queue-summary', (event) => {
    renderCounts(event.detail?.preprocessing_results);
    if (state.filter) loadList();
  });
})();
