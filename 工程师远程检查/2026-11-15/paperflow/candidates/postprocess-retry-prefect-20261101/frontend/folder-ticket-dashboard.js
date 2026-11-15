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
    'waiting-rag-upload-hint', 'rag-processing-hint', 'waiting-nas-archive-hint', 'completed-hint',
  ];
  const sections = {
    preprocessing: '前处理',
    llm: 'LLM',
    postprocessing: '后处理',
  };
  const resultLabels = { passed: '已通过', failed: '失败', unrecognized: '未识别' };
  const failedStages = { preprocessing: '前处理', llm: 'LLM', postprocessing: '后处理', rag: 'RAG', nas: 'NAS' };
  const stageNames = new Set([...Object.keys(stages), 'waiting_nas_archive_delete', 'nas_in_progress', 'failed', 'unresolved']);
  const resultNames = new Set([null, ...Object.keys(resultLabels)]);
  const lists = Object.fromEntries(Object.keys(sections).map((section) => [section, { filter: null, page: 1 }]));
  const element = (id) => document.getElementById(id);
  const number = new Intl.NumberFormat('zh-CN');
  let snapshot = null;
  let updateFailed = false;
  let lastSuccessAt = 0;
  let apiUrl = '';

  function showUnavailable(message, label) {
    for (const id of [...Object.values(stages), 'llm-body-count', 'llm-attachment-count', 'waiting-nas-archive-count']) {
      element(id).textContent = label;
    }
    for (const section of Object.keys(sections)) {
      for (const result of Object.keys(resultLabels)) element(`${section}-${result}-count`).textContent = label;
      element(`${section}-results-hint`).textContent = message;
      document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => { button.disabled = true; });
      element(`${section}-list-panel`).hidden = true;
    }
    for (const id of hints) element(id).textContent = message;
    element('folder-ticket-overview').textContent = message;
  }

  function renderList(section) {
    const state = lists[section];
    if (!state.filter) return;
    const resultLabel = section === 'postprocessing' && state.filter === 'failed' ? '未通过' : resultLabels[state.filter];
    const matching = snapshot.folders.filter((folder) => folder[`${section}_result`] === state.filter);
    const pages = Math.max(1, Math.ceil(matching.length / 20));
    state.page = Math.min(state.page, pages);
    const rows = matching.slice((state.page - 1) * 20, state.page * 20).map((folder) => {
      const row = document.createElement('li');
      const body = document.createElement('div');
      const title = document.createElement('span');
      const folderLine = document.createElement('p');
      const idLine = document.createElement('p');
      const result = document.createElement('span');
      body.className = 'entry-content';
      title.className = 'entry-title';
      title.textContent = folder.paper_title || (folder.paper_title_candidate ? `${folder.paper_title_candidate}（候选题目待核对）` : '题目待核对');
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
    element(`${section}-list-count`).textContent = `${snapshot.complete ? '共' : '已确认'} ${number.format(matching.length)} 个文件夹`;
    element(`${section}-group-list`).replaceChildren(...rows);
    element(`${section}-list-message`).hidden = matching.length > 0;
    element(`${section}-list-message`).textContent = snapshot.complete ? '暂无资料' : '暂无已确认资料，其他工单待核对';
    element(`${section}-pagination`).hidden = pages <= 1;
    element(`${section}-page-info`).textContent = `第 ${state.page} / ${pages} 页`;
    element(`${section}-previous-page`).disabled = state.page <= 1;
    element(`${section}-next-page`).disabled = state.page >= pages;
  }

  function render() {
    const counts = Object.fromEntries([...Object.keys(stages), 'waiting_nas_archive_delete', 'nas_in_progress', 'failed', 'unresolved'].map((stage) => [stage, 0]));
    const results = Object.fromEntries(Object.keys(sections).map((section) => [section, { passed: 0, failed: 0, unrecognized: 0 }]));
    const failures = Object.fromEntries(Object.keys(failedStages).map((stage) => [stage, 0]));
    let body = 0, attachment = 0;
    for (const folder of snapshot.folders) {
      counts[folder.stage] += 1;
      if (folder.stage === 'failed') failures[folder.failed_stage] += 1;
      if (folder.stage === 'llm_in_progress') {
        if (folder.llm_body_running) body += 1;
        if (folder.llm_attachment_running) attachment += 1;
      }
      for (const section of Object.keys(sections)) {
        const result = folder[`${section}_result`];
        if (result) results[section][result] += 1;
      }
    }
    const complete = snapshot.complete && !counts.unresolved;
    const display = (value) => complete || value ? number.format(value) : '—';
    for (const [stage, id] of Object.entries(stages)) {
      element(id).textContent = display(counts[stage]);
      element(id).setAttribute('aria-label', `${display(counts[stage])} 个文件夹`);
    }
    element('llm-body-count').textContent = display(body);
    element('llm-attachment-count').textContent = display(attachment);
    element('waiting-nas-archive-count').textContent = display(counts.waiting_nas_archive_delete + counts.nas_in_progress);
    element('llm-body-count').setAttribute('aria-label', `正文识别 ${display(body)} 个文件夹`);
    element('llm-attachment-count').setAttribute('aria-label', `附件识别 ${display(attachment)} 个文件夹`);
    element('waiting-nas-archive-count').setAttribute('aria-label', `NAS 归档及删除 ${display(counts.waiting_nas_archive_delete + counts.nas_in_progress)} 个文件夹`);

    const stamp = new Date(snapshot.as_of).toLocaleString('zh-CN', { hour12: false });
    const quality = complete ? '按文件夹工单统计' : '已确认数量 · 统计不完整';
    const freshness = updateFailed ? '更新失败 · 上次数据' : '数据截至';
    const hint = `全部任务 · ${quality} · ${freshness} ${stamp}`;
    for (const id of hints) element(id).textContent = hint;
    element('waiting-nas-archive-hint').textContent = `等待 ${display(counts.waiting_nas_archive_delete)} · 处理中 ${display(counts.nas_in_progress)} · ${quality} · ${freshness} ${stamp}`;
    const pending = counts.unresolved ? ` · 待核对 ${number.format(counts.unresolved)}` : '';
    const breakdown = Object.keys(failedStages).filter((stage) => failures[stage]).map((stage) => `${failedStages[stage]} ${number.format(failures[stage])}`).join('、');
    const failed = counts.failed ? ` · 失败 ${number.format(counts.failed)}（${breakdown}）` : '';
    element('folder-ticket-overview').textContent = `${snapshot.complete ? '共' : '已读取'} ${number.format(snapshot.folders.length)} 张文件夹工单${pending}${failed} · ${quality} · ${freshness} ${stamp}`;

    for (const section of Object.keys(sections)) {
      for (const result of Object.keys(resultLabels)) element(`${section}-${result}-count`).textContent = display(results[section][result]);
      const resultQuality = complete ? '阶段结果' : '已确认阶段结果 · 统计不完整';
      const waitingValidation = section === 'llm' ? snapshot.folders.filter((folder) =>
        folder.llm_response_saved && folder.llm_result === null).length : 0;
      const returned = waitingValidation ? ` · 已返回待校验 ${number.format(waitingValidation)}` : '';
      element(`${section}-results-hint`).textContent = `全部任务 · ${resultQuality}${returned} · ${freshness} ${stamp}`;
      document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => { button.disabled = false; });
      renderList(section);
    }
  }

  async function refresh() {
    try {
      const response = await fetch(apiUrl, { cache: 'no-store', signal: AbortSignal.timeout(8000) });
      if (!response.ok) throw new Error('工单接口暂不可用');
      const value = await response.json();
      if (!Array.isArray(value.folders) || typeof value.complete !== 'boolean' || !Number.isFinite(Date.parse(value.as_of)) ||
          value.folders.some((folder) => !folder || typeof folder.ticket_id !== 'string' || !folder.ticket_id ||
            typeof folder.task_id !== 'string' || typeof folder.folder_id !== 'string' ||
            typeof folder.folder_name !== 'string' || !(folder.paper_title === null || typeof folder.paper_title === 'string') ||
            !stageNames.has(folder.stage) || typeof folder.llm_body_running !== 'boolean' ||
            typeof folder.llm_attachment_running !== 'boolean' ||
            (folder.stage === 'failed' && !Object.hasOwn(failedStages, folder.failed_stage)) ||
            Object.keys(sections).some((section) => !resultNames.has(folder[`${section}_result`])))) {
        throw new Error('工单数据格式不正确');
      }
      snapshot = value;
      lastSuccessAt = Date.now();
      updateFailed = false;
      render();
    } catch (_) {
      updateFailed = true;
      if (snapshot && Date.now() - lastSuccessAt <= 30000) render();
      else {
        snapshot = null;
        showUnavailable('文件夹工单数据暂不可用', '—');
      }
    }
  }

  for (const section of Object.keys(sections)) {
    document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => {
      button.addEventListener('click', () => {
        if (!snapshot) return;
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
      element(`${section}-list-panel`).hidden = true;
      document.querySelectorAll(`[data-${section}-filter]`).forEach((button) => button.setAttribute('aria-pressed', 'false'));
    });
    element(`${section}-previous-page`).addEventListener('click', () => {
      if (lists[section].page > 1) { lists[section].page -= 1; renderList(section); }
    });
    element(`${section}-next-page`).addEventListener('click', () => {
      lists[section].page += 1;
      renderList(section);
    });
  }

  showUnavailable('等待后端接入 Prefect 文件夹工单接口', '待接入');
  fetch('api-config.json', { cache: 'no-store' }).then((response) => response.json()).then((config) => {
    apiUrl = config.folder_tickets_api_url;
    if (!apiUrl) return;
    refresh();
    setInterval(refresh, 10000);
  }).catch(() => showUnavailable('前端接口配置暂不可用', '暂不可用'));
})();
