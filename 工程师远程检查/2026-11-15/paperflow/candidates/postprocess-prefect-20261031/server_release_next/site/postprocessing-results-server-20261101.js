(() => {
  'use strict';

  // This uses the same polling event and list interaction as preprocessing.
  const base = 'http://127.0.0.1:14206/api/v1/local-intake/preprocessing/summary';
  const labels = { passed: '已通过', failed: '未通过', unrecognized: '未识别' };
  const ids = { passed: 'postprocessing-passed-count', failed: 'postprocessing-failed-count',
    unrecognized: 'postprocessing-unrecognized-count' };
  const ledgerBasis = 'postprocessing-registered-ledger/1';
  const number = new Intl.NumberFormat('zh-CN');
  const state = { filter: null, page: 1, pageSize: 20, pages: 1, hasNext: false, sequence: 0,
    basis: null, revision: null };
  let listTimer = null, listExpirySequence = 0;
  const monotonicNow = () => typeof performance !== 'undefined' ? performance.now() : Date.now();
  const element = (id) => document.getElementById(id);
  const validCount = (value) => Number.isSafeInteger(value) && value >= 0 ? value : null;
  const hasLedger = (payload) => Object.prototype.hasOwnProperty.call(payload || {}, 'postprocessing_registered_ledger');
  function resultFrom(payload) {
    if (!hasLedger(payload)) return { available: false, message: '服务器结果暂不可核验' };
    const ledger = payload.postprocessing_registered_ledger;
    const result = ledger?.results;
    if (ledger?.available !== true || ledger.basis !== ledgerBasis || ledger.current_delivery_verified !== false ||
        !/^[0-9a-f]{64}$/.test(ledger.revision || '') || !Number.isFinite(Date.parse(ledger.as_of)) ||
        result?.available !== true || result.basis !== ledgerBasis || result.as_of !== ledger.as_of) {
      return { available: false, message: result?.message || '登记结果暂不可用' };
    }
    const remote = ledger.server_observation;
    const verified = remote?.available === true && remote.basis === 'paperflow-server-receipt-observation/1' &&
      remote.as_of === ledger.as_of && remote.observation_revision === ledger.revision;
    const safe = { ...result, counts: { ...result.counts,
      passed: verified ? result.counts?.passed : null, failed: null, unrecognized: null } };
    if (result.list) {
      const pending = result.list.filter !== 'passed' || !verified;
      safe.list = { ...result.list, total: pending ? null : result.list.total,
        items: Array.isArray(result.list.items) ? result.list.items.map((item) => pending && item ?
          { ...item, verification_pending: true, reason: `${item.reason || '已登记'} · 证据待核验` } : item) : result.list.items };
    }
    return safe;
  }

  if (typeof module !== 'undefined' && module.exports) module.exports = { resultFrom };
  if (typeof document === 'undefined') return;

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
    const registered = result.basis === ledgerBasis;
    const stamp = Number.isNaN(time.getTime()) ? '' :
      ` · ${registered ? '截至' : '完整核验于'} ${registered ? time.toLocaleString('zh-CN', { hour12: false }) : time.toLocaleTimeString('zh-CN', { hour12: false })}`;
    const parts = [registered ? (result.partial ? '已登记结果 · 部分状态待核对' : '已登记结果') :
      (result.partial ? '全部任务 · 部分结果待核对' : '全部任务 · 后处理结果')];
    if (registered) parts.push('交付材料需另行复核');
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
    else if (state.basis === ledgerBasis && !hasTitle) body.append(line('entry-detail', '题目待核对'));
    if (item.reason) body.append(line('entry-reason', String(item.reason)));
    if (item.order_note) body.append(line('entry-reason', String(item.order_note)));
    addNotes(body, item.gaps, '识别缺口');
    addNotes(body, item.warnings, '警告');
    addNotes(body, item.diagnostics, '诊断');
    const status = document.createElement('span');
    status.className = `entry-status ${item.verification_pending ? 'pending' : state.filter}`;
    status.textContent = item.verification_pending ? '已登记 · 待核验' :
      `${state.basis === ledgerBasis ? '登记' : ''}${labels[state.filter] || ''}`;
    row.append(body, status);
    return row;
  }

  function showMessage(message) {
    element('postprocessing-list-message').hidden = false;
    element('postprocessing-list-message').textContent = message;
    element('postprocessing-group-list').replaceChildren();
    element('postprocessing-pagination').hidden = true;
  }

  function clearListObservation() {
    listExpirySequence += 1;
    if (listTimer !== null) clearTimeout(listTimer);
    listTimer = null;
    state.sequence += 1;
    state.revision = null;
    renderCounts({ available: false });
    if (state.filter) {
      element('postprocessing-list-count').textContent = '暂不可用';
      showMessage('观察已过期，等待重新核验');
    }
  }

  async function loadList(attempt = 0) {
    if (!state.filter) return;
    const sequence = ++state.sequence;
    const filter = state.filter;
    const page = state.page;
    const startedWall = Date.now(), startedMonotonic = monotonicNow();
    element('postprocessing-list-count').textContent = '列表获取中';
    element('postprocessing-previous-page').disabled = true;
    element('postprocessing-next-page').disabled = true;
    const url = new URL(base);
    url.searchParams.set('postprocessing_result_filter', filter);
    url.searchParams.set('postprocessing_result_page', String(page));
    url.searchParams.set('postprocessing_result_page_size', String(state.pageSize));
    if (state.basis === ledgerBasis && page > 1 && state.revision) {
      url.searchParams.set('postprocessing_result_revision', state.revision);
    }
    try {
      const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(5000) });
      if (response.status === 503 && attempt < 4 && sequence === state.sequence &&
          filter === state.filter && page === state.page) {
        showMessage('正在核验列表，请稍候…');
        setTimeout(() => {
          if (sequence === state.sequence && filter === state.filter && page === state.page) loadList(attempt + 1);
        }, 1000);
        return;
      }
      if (!response.ok) throw new Error('结果暂不可用');
      const payload = await response.json();
      if (sequence !== state.sequence || filter !== state.filter || page !== state.page) return;
      const remaining = payload?.postprocessing_observation_remaining_ms;
      if (!Number.isSafeInteger(remaining) || remaining <= 0 || remaining > 30000 ||
          Date.now() >= startedWall + remaining || monotonicNow() >= startedMonotonic + remaining) {
        throw new Error('列表观察已过期');
      }
      const ledger = payload?.postprocessing_registered_ledger;
      if (page > 1 && state.basis === ledgerBasis && ledger?.available === false &&
          ledger.reason === 'snapshot_changed') {
        state.page = 1;
        state.revision = null;
        showMessage('登记结果已更新，正在返回第一页…');
        loadList();
        return;
      }
      const result = resultFrom(payload);
      renderCounts(result);
      const list = result?.list;
      if (result?.available !== true || !list || list.filter !== filter || list.page !== page ||
          list.page_size !== state.pageSize || !Array.isArray(list.items) ||
          list.items.some((item) => !item || typeof item !== 'object' || Array.isArray(item)) ||
          (page > 1 && state.basis === ledgerBasis &&
            (result.basis !== ledgerBasis || !state.revision || ledger?.revision !== state.revision))) {
        element('postprocessing-list-count').textContent = '暂不可用';
        showMessage(result?.message || '结果暂不可用，请稍后再试');
        return;
      }
      state.basis = result.basis || null;
      state.revision = state.basis === ledgerBasis ? ledger.revision : null;
      if (listTimer !== null) clearTimeout(listTimer);
      const expirySequence = ++listExpirySequence;
      listTimer = setTimeout(() => {
        if (expirySequence === listExpirySequence) clearListObservation();
      }, Math.max(0, Math.min(startedWall + remaining - Date.now(),
                              startedMonotonic + remaining - monotonicNow())));
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
        `${result.partial ? '至少 ' : '共 '}${number.format(total)} 个文件夹 · ${state.basis === ledgerBasis ? '已登记结果' : '按核验结果更新'}`;
      element('postprocessing-group-list').replaceChildren(...list.items.map(renderRow));
      element('postprocessing-list-message').hidden = list.items.length > 0;
      element('postprocessing-list-message').textContent = state.basis === ledgerBasis ?
        (result.partial || filter === 'unrecognized' ? '暂无已登记的本类结果；其他状态待核对' : '暂无已登记的本类结果') :
        (list.message || '暂无资料');
      element('postprocessing-pagination').hidden = total === null ? page <= 1 && !state.hasNext : pages <= 1;
      element('postprocessing-page-info').textContent = total === null ? `第 ${page} 页 · 总页数待核对` : `第 ${page} / ${pages} 页`;
      element('postprocessing-previous-page').disabled = page <= 1;
      element('postprocessing-next-page').disabled = !state.hasNext;
    } catch (_) {
      if (sequence !== state.sequence) return;
      clearListObservation();
    }
  }

  function select(filter) {
    state.filter = filter;
    state.page = 1;
    state.pages = 1;
    state.hasNext = false;
    state.basis = null;
    state.revision = null;
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
    listExpirySequence += 1;
    if (listTimer !== null) clearTimeout(listTimer);
    listTimer = null;
    element('postprocessing-list-panel').hidden = true;
    document.querySelectorAll('[data-postprocessing-filter]').forEach((button) => button.setAttribute('aria-pressed', 'false'));
  });
  element('postprocessing-previous-page').addEventListener('click', () => {
    if (state.page > 1) { state.page -= 1; loadList(); }
  });
  element('postprocessing-next-page').addEventListener('click', () => {
    if (state.hasNext) { state.page += 1; loadList(); }
  });
  window.addEventListener('paperflow:postprocessing-summary', (event) => {
    if (!event.detail) { clearListObservation(); return; }
    renderCounts(resultFrom(event.detail));
    if (state.filter) loadList();
  });
})();
