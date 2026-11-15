(() => {
  'use strict';

  const $ = (id) => document.getElementById(id);
  // An already-open preview can have the previous HTML cached when this script updates.
  if (!$('unknown-stat')) {
    const button = document.createElement('button');
    button.id = 'unknown-stat';
    button.className = 'stat';
    button.type = 'button';
    button.dataset.filter = 'unknown';
    button.setAttribute('aria-pressed', 'false');
    button.hidden = true;
    const title = document.createElement('span');
    title.textContent = '结果未知';
    const count = document.createElement('strong');
    count.id = 'unknown-count';
    count.className = 'unknown';
    count.textContent = '0';
    button.append(title, count);
    document.querySelector('.stats').append(button);
  }
  if (!$('registration-failed-stat')) {
    const button = document.createElement('button');
    button.id = 'registration-failed-stat';
    button.className = 'stat';
    button.type = 'button';
    button.dataset.filter = 'registration_failed';
    button.setAttribute('aria-pressed', 'false');
    button.hidden = true;
    const title = document.createElement('span');
    title.textContent = '登记失败';
    const count = document.createElement('strong');
    count.id = 'registration-failed-count';
    count.className = 'failed';
    count.textContent = '0';
    button.append(title, count);
    document.querySelector('.stats').append(button);
  }
  const number = new Intl.NumberFormat('zh-CN');
  const labels = { passed: '已通过', failed: '失败', unrecognized: '未识别', unknown: '结果未知', registration_failed: '登记失败' };
  const pendingCreateKey = 'first-step-pending-create';
  let pendingCreate = null;
  try {
    const saved = JSON.parse(localStorage.getItem(pendingCreateKey) || 'null');
    if (saved && typeof saved.name === 'string' && typeof saved.request_id === 'string' && typeof saved.api_base === 'string') pendingCreate = saved;
  } catch (_) { /* 页面仍可读取已有任务 */ }
  const state = { demo: false, complete: false, resultsAvailable: false, filter: null, page: 1, pageSize: 20, apiBase: '', tusUrl: '', taskId: null, payload: null, request: 0,
    requireTaskSelection: false, taskReadError: false, controlsReadError: false, configReadError: false,
    taskStatus: null, followTask: false, controlsReady: false, controlBusy: '', controlMessage: '', uncertainAction: '',
    startPending: false, pausePending: false, controlSequence: 0, controlRevision: null, startAttemptRevision: null,
    startNotSubmittedMessage: '', taskName: '',
    workspaceCapabilities: { create: false, createLookup: false, list: false, save: false, upload: false }, workspacePending: '', tasksPage: 1,
    createError: pendingCreate ? '上次创建结果未确认，可核对同一个请求。' : '', createUncertain: Boolean(pendingCreate), pendingCreate,
    uploader: null, uploadBusy: false, uploadOpening: false, uploadGeneration: 0, ungroupedSignature: '' };
  let classificationView = null;
  let renderedListSignature = null;
  let configurationLoaded = false;

  function demoHash(index) {
    let value = index + 1;
    let hash = '';
    for (let i = 0; i < 8; i += 1) {
      value = (Math.imul(value ^ (i * 2654435761), 1664525) + 1013904223) >>> 0;
      hash += value.toString(16).padStart(8, '0');
    }
    return hash;
  }

  const demoItems = Array.from({ length: 176 }, (_, index) => {
    const status = index < 157 ? 'passed' : index < 159 ? 'failed' : 'unrecognized';
    const titleStatus = ['已提取', '待核对', '未提取'][index % 3];
    const displayNumber = String(index + 1).padStart(3, '0');
    return {
      group_id: demoHash(index).slice(0, 32),
      display_hash: demoHash(index),
      status,
      title_status: titleStatus,
      paper_title: titleStatus === '已提取' ? `示例论文 ${displayNumber}：体育教育相关研究` : null,
      paper_title_candidate: titleStatus === '待核对' ? `示例备选题目 ${displayNumber}：体育教育相关研究` : null,
      reason: status === 'failed' ? '示例：文件读取失败' : status === 'unrecognized' ? '示例：无法确定正文或资料分类' : null,
    };
  });

  function integer(value) {
    return Number.isInteger(value) && value >= 0 ? value : null;
  }

  function setStatus(text, style = '') {
    $('task-state').textContent = text;
    $('task-state').className = `status${style ? ` is-${style}` : ''}`;
  }

  function setNotice(text, toggleText = null) {
    $('notice-text').textContent = text;
    $('demo-toggle').hidden = toggleText === null;
    if (toggleText !== null) $('demo-toggle').textContent = toggleText;
  }

  function renderControls() {
    const available = state.controlsReady && !state.demo;
    const busy = Boolean(state.controlBusy) || state.startPending || state.pausePending || Boolean(state.workspacePending);
    const status = state.taskStatus;
    const uploadReady = state.workspaceCapabilities.upload && Boolean(state.tusUrl) && !state.demo;
    $('upload-button').disabled = !uploadReady || busy || !state.taskId || !['draft', 'uploading'].includes(status);
    $('local-folder-button').disabled = !state.workspaceCapabilities.local || state.demo || busy || !state.taskId || status !== 'draft';
    $('delete-task-button').disabled = !state.workspaceCapabilities.delete || state.demo || busy || !state.taskId ||
      ['starting', 'running', 'pausing'].includes(status);
    $('delete-task-button').textContent = ['delete_failed', 'deleting'].includes(status) ? '重试删除' : '删除任务';
    $('start-button').disabled = !available || busy || !state.taskId || integer(state.controlRevision) === null ||
      Boolean(state.uncertainAction) || !['ready', 'paused'].includes(status);
    const canPause = ['starting', 'running', 'unknown'].includes(status) || state.startPending || state.uncertainAction === 'start';
    $('pause-button').disabled = !available || Boolean(state.controlBusy) || state.pausePending || !state.taskId || !canPause;
    const messages = {
      ready: '资料已就绪，点击开始处理', starting: '任务正在启动', running: '任务正在处理',
      pausing: '正在暂停，等待当前工作停止', paused: '已暂停，点击开始继续',
      needs_review: '部分资料需要核对',
      unknown: '任务状态暂未确认', complete: '处理完成', completed: '处理完成',
      delete_failed: '删除未完成，可再次点击删除重试', deleting: '正在删除任务和产物',
    };
    const connectionLost = state.taskReadError || state.controlsReadError || state.configReadError;
    $('control-message').textContent = connectionLost ? '后端连接暂不可用，正在重试' : state.controlMessage ||
      (!available ? '上传、开始和暂停尚未接入' : messages[status] || '选择资料后开始');
    $('task-name').textContent = state.taskName || (state.taskId ? `任务 ${String(state.taskId).slice(0, 8)}` : '尚未选择任务');
    $('new-task-button').disabled = state.startPending || state.pausePending || state.workspacePending === 'create';
    $('open-task-button').disabled = state.startPending || state.pausePending || state.workspacePending === 'create';
    $('cancel-new-task').disabled = state.workspacePending === 'create';
    $('save-task-button').disabled = !state.workspaceCapabilities.save || !state.taskId || Boolean(state.workspacePending);
    const recoverableCreate = state.createUncertain && state.pendingCreate && state.workspaceCapabilities.createLookup && state.pendingCreate.api_base === state.apiBase;
    $('new-task-name').disabled = Boolean(state.pendingCreate);
    $('create-task-confirm').textContent = recoverableCreate ? '核对并创建' : '创建';
    $('create-task-confirm').disabled = !state.workspaceCapabilities.create || state.workspacePending === 'create' || (state.createUncertain && !recoverableCreate);
    $('new-task-message').textContent = state.createError || (state.workspaceCapabilities.create ?
      (uploadReady || state.workspaceCapabilities.local ? '创建后选择本机资料，任务不会自动开始。' : '创建后任务不会自动开始；资料入口尚未接入。') : '后端新建任务接口准备中。');
    if (!state.taskId && !state.workspacePending) $('save-state').textContent = '未选择任务';
  }

  async function checkControls() {
    const apiBase = state.apiBase;
    try {
      const response = await fetch(new URL('/openapi.json', apiBase), { cache: 'no-store', signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error('接口不可用');
      const schema = await response.json();
      if (state.apiBase !== apiBase) return;
      state.controlsReadError = false;
      const tusReady = Boolean(schema.paths?.['/tus/files/']?.post) &&
        Boolean(schema.paths?.['/tus/files/{tus_id}']?.head) && Boolean(schema.paths?.['/tus/files/{tus_id}']?.patch);
      state.tusUrl = tusReady ? new URL('/tus/files/', apiBase).href : '';
      const required = ['/api/v1/first-step/tasks/{task_id}/start', '/api/v1/first-step/tasks/{task_id}/pause'];
      state.controlsReady = required.every((path) => Boolean(schema.paths?.[path]?.post));
      state.workspaceCapabilities = {
        create: Boolean(schema.paths?.['/api/v1/first-step/tasks']?.post) &&
          Boolean(schema.paths?.['/api/v1/first-step/tasks/by-request/{request_id}']?.get),
        createLookup: Boolean(schema.paths?.['/api/v1/first-step/tasks/by-request/{request_id}']?.get),
        list: Boolean(schema.paths?.['/api/v1/first-step/tasks']?.get),
        save: Boolean(schema.paths?.['/api/v1/first-step/tasks/{task_id}/save']?.post),
        local: Boolean(schema.paths?.['/api/v1/first-step/tasks/{task_id}/local-source']?.post),
        browse: Boolean(schema.paths?.['/api/v1/first-step/local-folder-dialog']?.post),
        delete: Boolean(schema.paths?.['/api/v1/first-step/tasks/{task_id}']?.delete),
        upload: Boolean(schema.paths?.['/api/v1/first-step/tasks/{task_id}/uploads']?.post) &&
          Boolean(schema.paths?.['/api/v1/first-step/uploads/{upload_id}']?.get) &&
          ['/manifest', '/seal', '/finish'].every((suffix) => Boolean(schema.paths?.[`/api/v1/first-step/uploads/{upload_id}${suffix}`]?.post)),
      };
    } catch (_) {
      if (state.apiBase !== apiBase) return;
      state.controlsReadError = true;
      state.controlsReady = false;
      state.tusUrl = '';
      state.workspaceCapabilities = { create: false, createLookup: false, list: false, save: false, upload: false };
    }
    renderControls();
  }

  async function controlRequest(path, body, multipart = false, timeoutMs = 15000) {
    const options = { method: 'POST', cache: 'no-store', body: multipart ? body : JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs) };
    if (!multipart) options.headers = { 'Content-Type': 'application/json' };
    const response = await fetch(new URL(path, state.apiBase), options);
    let data = {};
    try { data = await response.json(); } catch (_) { /* 服务未返回 JSON */ }
    if (!response.ok) {
      const detail = typeof data.detail === 'string' ? data.detail : '后端未接受请求';
      const error = new Error(detail);
      error.status = response.status;
      throw error;
    }
    return data;
  }

  async function selectTask(taskId, name = '') {
    if (state.startPending || state.pausePending) return;
    closeClassification();
    state.request += 1;
    state.taskId = taskId;
    state.taskName = name;
    state.followTask = true;
    state.taskStatus = 'unknown';
    state.controlRevision = null;
    state.startAttemptRevision = null;
    state.startNotSubmittedMessage = '';
    state.uncertainAction = '';
    state.controlMessage = '';
    state.payload = null;
    showUngrouped([]);
    state.filter = null;
    state.page = 1;
    $('list-panel').hidden = true;
    $('save-state').textContent = '正在读取任务';
    showProcessing(null, '状态未知', '正在读取任务状态。');
    setNotice('正在读取所选任务。');
    renderControls();
    await refreshLive();
  }

  async function createTask(event) {
    event.preventDefault();
    const name = $('new-task-name').value.trim();
    const recoverable = state.createUncertain && state.pendingCreate && state.workspaceCapabilities.createLookup && state.pendingCreate.api_base === state.apiBase;
    if (!name || !state.workspaceCapabilities.create || state.workspacePending || state.startPending || state.pausePending || (state.createUncertain && !recoverable)) return;
    state.workspacePending = 'create';
    state.createError = '';
    renderControls();
    $('new-task-message').textContent = '正在创建任务';
    try {
      let result;
      if (state.workspaceCapabilities.createLookup) {
        if (!state.pendingCreate) {
          const pending = { name, request_id: crypto.randomUUID(), api_base: state.apiBase };
          localStorage.setItem(pendingCreateKey, JSON.stringify(pending));
          state.pendingCreate = pending;
        }
        if (recoverable) {
          const lookup = await fetch(new URL(`/api/v1/first-step/tasks/by-request/${encodeURIComponent(state.pendingCreate.request_id)}`, state.apiBase),
            { cache: 'no-store', signal: AbortSignal.timeout(5000) });
          if (lookup.ok) result = await lookup.json();
          else if (lookup.status !== 404) throw new Error('暂时无法核对创建结果');
        }
        if (!result) result = await controlRequest('/api/v1/first-step/tasks', { name, request_id: state.pendingCreate.request_id });
      } else {
        result = await controlRequest('/api/v1/first-step/tasks', { name });
      }
      if (!result.task_id) throw new Error('后端未返回任务编号');
      localStorage.removeItem(pendingCreateKey);
      state.pendingCreate = null;
      state.createUncertain = false;
      $('new-task-dialog').close();
      if (state.startPending || state.pausePending) {
        $('save-state').textContent = '任务已创建，可在“打开任务”中查看';
      } else {
        await selectTask(result.task_id, result.name || name);
      }
    } catch (error) {
      state.createUncertain = !error.status || error.status >= 500;
      if (!state.createUncertain && state.pendingCreate) {
        localStorage.removeItem(pendingCreateKey);
        state.pendingCreate = null;
      }
      state.createError = !state.createUncertain ? `创建未成功：${error.message}` : (state.pendingCreate ? '创建结果未确认，可核对同一个请求。' : '创建结果未确认，请关闭窗口并在“打开任务”中核对。');
    } finally {
      state.workspacePending = '';
      renderControls();
    }
  }

  async function loadTasks() {
    const list = $('task-list');
    list.replaceChildren();
    $('previous-task-page').hidden = true;
    $('next-task-page').hidden = true;
    if (!state.workspaceCapabilities.list) {
      $('task-list-message').textContent = '后端任务列表接口准备中。';
      return;
    }
    $('task-list-message').textContent = '正在读取任务列表';
    try {
      const url = new URL('/api/v1/first-step/tasks', state.apiBase);
      url.searchParams.set('page', String(state.tasksPage));
      url.searchParams.set('page_size', '30');
      const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error('无法读取任务列表');
      const data = await response.json();
      const tasks = Array.isArray(data) ? data : (data.items || data.tasks);
      if (!Array.isArray(tasks)) throw new Error('任务列表格式尚未确认');
      if (!tasks.length && state.tasksPage > 1) { state.tasksPage -= 1; await loadTasks(); return; }
      const names = { draft: '等待选择资料', uploading: '上传中或未完成', ready: '等待开始', starting: '正在启动', running: '处理中',
        pausing: '正在暂停', paused: '已暂停', complete: '已完成', completed: '已完成',
        failed: '处理中断', needs_review: '待核对', unknown: '状态未知', delete_failed: '删除未完成', deleting: '正在删除' };
      for (const task of tasks) {
        if (!task || !task.task_id) continue;
        const row = document.createElement('div');
        row.className = 'task-row';
        const select = document.createElement('button');
        select.type = 'button';
        select.className = 'task-select';
        const title = document.createElement('span');
        title.textContent = task.name || task.task_name || `任务 ${String(task.task_id).slice(0, 8)}`;
        const status = document.createElement('span');
        status.textContent = names[task.task_status || task.status] || '状态未知';
        select.append(title, status);
        select.addEventListener('click', async () => {
          if (state.startPending || state.pausePending || state.workspacePending === 'create') {
            $('task-list-message').textContent = '请等待当前操作确认后再切换任务。';
            return;
          }
          $('open-task-dialog').close();
          if (!state.pendingCreate) {
            state.createUncertain = false;
            state.createError = '';
          }
          await selectTask(task.task_id, title.textContent);
        });
        const remove = document.createElement('button');
        remove.type = 'button';
        remove.className = 'task-delete';
        remove.textContent = ['delete_failed', 'deleting'].includes(task.task_status) ? '重试删除' : '删除';
        remove.disabled = !state.workspaceCapabilities.delete || Boolean(state.workspacePending) ||
          ['starting', 'running', 'pausing'].includes(task.task_status);
        remove.addEventListener('click', () => deleteTask(task.task_id, title.textContent));
        row.append(select, remove);
        list.append(row);
      }
      $('task-list-message').textContent = list.children.length ? '' : '暂无可打开的任务。';
      const total = integer(data.total) ?? integer(data.list_total);
      $('previous-task-page').hidden = state.tasksPage <= 1;
      $('next-task-page').hidden = total !== null ? state.tasksPage * 30 >= total : tasks.length < 30;
    } catch (_) {
      $('task-list-message').textContent = '任务列表暂时无法读取。';
    }
  }

  async function saveTask() {
    if (!state.taskId || !state.workspaceCapabilities.save || state.workspacePending) return;
    const taskId = state.taskId;
    state.followTask = true;
    state.request += 1;
    state.workspacePending = 'save';
    $('save-state').textContent = '正在保存';
    renderControls();
    try {
      const result = await controlRequest(`/api/v1/first-step/tasks/${encodeURIComponent(taskId)}/save`, {});
      if (result.task_id && result.task_id !== taskId) throw new Error('任务编号不一致');
      if (state.taskId === taskId) $('save-state').textContent = result.saved_at ? '已保存' : '保存结果未确认';
    } catch (error) {
      if (state.taskId === taskId) $('save-state').textContent = error.status && error.status < 500 ? '保存失败' : '保存结果未确认';
    } finally {
      state.workspacePending = '';
      renderControls();
    }
  }

  async function deleteTask(taskId, name) {
    if (!taskId || !state.workspaceCapabilities.delete || state.workspacePending) return;
    if (!window.confirm(`确定删除任务“${name || taskId}”吗？\n该任务的交付结果、工作副本和中间产物会被清理，原始资料保留。`)) return;
    state.workspacePending = 'delete';
    let deleted = false;
    renderControls();
    $('task-list-message').textContent = '正在删除任务和产物，请稍候。';
    try {
      const response = await fetch(new URL(`/api/v1/first-step/tasks/${encodeURIComponent(taskId)}`, state.apiBase),
        { method: 'DELETE', cache: 'no-store', signal: AbortSignal.timeout(600000) });
      const result = await response.json();
      if (!response.ok || !result.deleted) throw new Error(result.detail || '删除未完成，请重试');
      deleted = true;
      if (state.taskId === taskId) {
        state.request += 1;
        state.taskId = null;
        state.taskName = '';
        state.taskStatus = null;
        state.controlRevision = null;
        state.followTask = false;
        state.payload = null;
        state.filter = null;
        state.page = 1;
        $('list-panel').hidden = true;
        showUngrouped([]);
      }
      await refreshLive();
      const retained = [];
      if (result.shared_groups_retained || result.shared_conversion_caches_retained) retained.push('共用产物已保留');
      if (result.unassigned_conversion_caches_retained) retained.push(`${result.unassigned_conversion_caches_retained} 项旧缓存归属不明，已保留`);
      if (result.unknown_history_retained) retained.push(`${result.unknown_history_retained} 条未知请求状态已保留`);
      $('save-state').textContent = retained.length ? `任务已删除；${retained.join('，')}` : '任务及其产物已删除';
    } catch (error) {
      const message = error.message || '删除结果未确认，请查看任务列表';
      $('task-list-message').textContent = message;
      state.controlMessage = message;
      await refreshLive();
    } finally {
      state.workspacePending = '';
      renderControls();
      if (deleted && $('open-task-dialog').open) await loadTasks();
    }
  }

  async function browseLocalFolder() {
    if (!state.workspaceCapabilities.browse || state.workspacePending) return;
    state.workspacePending = 'browse';
    $('browse-local-folder').disabled = true;
    $('confirm-local-folder').disabled = true;
    $('cancel-local-folder').disabled = true;
    $('local-folder-message').textContent = '请在弹出的窗口中选择文件夹。';
    try {
      const result = await controlRequest('/api/v1/first-step/local-folder-dialog', {}, false, 610000);
      if (result.path) $('local-folder-path').value = result.path;
      $('local-folder-message').textContent = '选择或输入路径后，程序会先统计资料组。';
    } catch (error) {
      $('local-folder-message').textContent = error.message || '无法打开选择窗口，请输入路径。';
    } finally {
      state.workspacePending = '';
      $('browse-local-folder').disabled = !state.workspaceCapabilities.browse;
      $('confirm-local-folder').disabled = false;
      $('cancel-local-folder').disabled = false;
      renderControls();
    }
  }

  async function chooseLocalFolder(event) {
    event.preventDefault();
    if (!state.taskId || state.workspacePending || !state.workspaceCapabilities.local) return;
    const taskId = state.taskId;
    const path = $('local-folder-path').value.trim();
    if (!path) return;
    state.workspacePending = 'local';
    $('browse-local-folder').disabled = true;
    $('confirm-local-folder').disabled = true;
    $('cancel-local-folder').disabled = true;
    $('local-folder-message').textContent = '正在扫描文件夹，不会复制或启动处理。';
    renderControls();
    try {
      const result = await controlRequest(`/api/v1/first-step/tasks/${encodeURIComponent(taskId)}/local-source`, { path }, false, 300000);
      if (result.task_id !== taskId) throw new Error('选择结果未确认');
      $('local-folder-dialog').close();
      state.followTask = true;
      state.controlMessage = '';
      await refreshLive();
    } catch (error) {
      $('local-folder-message').textContent = error.message || '扫描结果未确认，请查看任务状态。';
      await refreshLive();
    } finally {
      state.workspacePending = '';
      $('browse-local-folder').disabled = !state.workspaceCapabilities.browse;
      $('confirm-local-folder').disabled = false;
      $('cancel-local-folder').disabled = false;
      renderControls();
    }
  }

  async function openUploader() {
    if ($('upload-button').disabled || state.uploadOpening || state.uploader) return;
    const taskId = state.taskId;
    state.followTask = true;
    state.request += 1;
    state.uploadOpening = true;
    try {
      const { createWorkspaceUploader } = await import('./workspace-uploader.mjs?v=20261030-4');
      if (state.taskId !== taskId || $('upload-button').disabled) return;
      $('upload-message').textContent = '请选择一个资料文件夹。';
      const generation = ++state.uploadGeneration;
      state.uploader = createWorkspaceUploader({ taskId, apiBase: state.apiBase, tusUrl: state.tusUrl,
        target: $('uppy-dashboard'), folderHost: $('selected-folders'), isActive: () => state.uploadGeneration === generation,
        onStatus: (message) => { if (state.uploadGeneration === generation) $('upload-message').textContent = message; },
        onBusy: (busy) => { if (state.uploadGeneration === generation) { state.uploadBusy = busy; $('close-upload').disabled = busy; } },
        onReady: () => { if (state.uploadGeneration === generation && state.taskId === taskId) refreshLive(); },
      });
      $('upload-dialog').showModal();
    } catch (_) {
      state.controlMessage = '上传组件无法加载，请稍后再试';
      renderControls();
    } finally {
      state.uploadOpening = false;
    }
  }

  function closeUploader() {
    if (state.uploadBusy) return;
    state.uploadGeneration += 1;
    if (state.uploader) state.uploader.destroy();
    state.uploader = null;
    $('selected-folders').hidden = true;
    $('upload-dialog').close();
    refreshLive();
  }

  async function changeTask(action) {
    if (!state.taskId || state.controlBusy || state.pausePending || (action === 'start' && state.startPending)) return;
    if (action === 'start' && integer(state.controlRevision) === null) return;
    const taskId = state.taskId;
    state.followTask = true;
    state.request += 1;
    const sequence = ++state.controlSequence;
    if (action === 'start') {
      state.startAttemptRevision = state.controlRevision;
      state.startNotSubmittedMessage = '';
    }
    state[`${action}Pending`] = true;
    state.controlMessage = action === 'start' ? '正在请求开始' : '正在请求暂停';
    renderControls();
    try {
      const body = action === 'start' ? { expected_revision: state.controlRevision } : {};
      const result = await controlRequest(`/api/v1/first-step/tasks/${encodeURIComponent(taskId)}/${action}`, body);
      if (state.taskId !== taskId || sequence !== state.controlSequence) return;
      if (result.task_id && result.task_id !== taskId) throw new Error('任务编号不一致');
      const revision = integer(result.control_revision);
      if (revision === null) throw new Error('后端未确认控制版本');
      if (state.controlRevision !== null && revision < state.controlRevision) return;
      state.controlRevision = revision;
      state.followTask = true;
      state.taskStatus = result.task_status || 'unknown';
      state.uncertainAction = '';
      state.controlMessage = '';
    } catch (error) {
      if (sequence === state.controlSequence) {
        if (action === 'start' && error.status === 503 && error.message.includes('任务未启动')) {
          state.startNotSubmittedMessage = error.message;
        }
        state.uncertainAction = !error.status || error.status >= 500 ? action : '';
        state.controlMessage = state.uncertainAction ? `${action === 'start' ? '开始' : '暂停'}结果未确认，正在核对状态` : `${action === 'start' ? '开始' : '暂停'}未成功：${error.message}`;
      }
    } finally {
      state[`${action}Pending`] = false;
      renderControls();
    }
    await refreshLive();
  }

  function showProcessing(discovered, status = '处理中', hint = '正在识别和分流资料，请稍候。') {
    state.complete = false;
    state.resultsAvailable = false;
    setStatus(status, status === '处理中' ? '' : 'warning');
    $('task-total').textContent = ['处理中', '结果汇总中', '等待上传', '等待开始', '正在暂停', '已暂停'].includes(status) ? '统计中' : '暂未确定';
    $('discovered').replaceChildren();
    if (integer(discovered) !== null) {
      $('discovered').append('已发现 ');
      const value = document.createElement('strong');
      value.textContent = number.format(discovered);
      $('discovered').append(value, ' 组 ');
      const note = document.createElement('span');
      note.textContent = ['等待上传', '等待开始', '正在暂停', '已暂停', '待核对'].includes(status) ? '（总数尚未确定）' : '（还在统计）';
      $('discovered').append(note);
    } else {
      const messages = { '暂无任务数据': '新建任务以开始使用', '未选择任务': '新建任务或打开已有任务', '数据暂不可用': '暂时无法读取任务数据', '处理中断': '任务未正常结束', '等待上传': '上传资料后开始统计', '等待开始': '任务尚未开始处理', '状态未知': '暂时无法确认任务状态' };
      $('discovered').textContent = messages[status] || '已发现组数正在统计';
    }
    $('processing').hidden = !['处理中', '结果汇总中'].includes(status);
    $('processing').classList.toggle('is-idle', status !== '处理中');
    $('processing').classList.remove('is-determinate');
    const progress = $('processing').querySelector('[role="progressbar"]');
    progress.setAttribute('aria-label', status === '处理中' ? '工作正在处理，任务总数尚未确定' : status);
    ['aria-valuemin', 'aria-valuemax', 'aria-valuenow', 'aria-valuetext'].forEach((attribute) => progress.removeAttribute(attribute));
    progress.style.removeProperty('--progress-value');
    $('progress-caption').textContent = status === '处理中' ? '正在处理' : status;
    $('progress-hint').textContent = hint;
    $('results').hidden = true;
    $('list-panel').hidden = true;
  }

  function showUngrouped(items) {
    const files = Array.isArray(items) ? items.filter((item) => typeof item === 'string') : [];
    const signature = JSON.stringify(files);
    if (state.ungroupedSignature === signature) return;
    state.ungroupedSignature = signature;
    $('ungrouped').hidden = !files.length;
    $('ungrouped-count').textContent = `${number.format(files.length)} 个文件`;
    const list = $('ungrouped-list');
    list.replaceChildren();
    for (const path of files) {
      const item = document.createElement('li');
      item.textContent = path;
      list.append(item);
    }
  }

  function showComplete(total, passed, failed, unrecognized) {
    state.complete = true;
    state.resultsAvailable = true;
    setStatus('处理结束', 'finished');
    $('task-total').replaceChildren(document.createTextNode(number.format(total)));
    const unit = document.createElement('small');
    unit.textContent = '组';
    $('task-total').append(unit);
    $('discovered').textContent = '全部资料已处理完毕';
    $('processing').hidden = true;
    $('results').hidden = false;
    $('passed-count').textContent = number.format(passed);
    $('failed-count').textContent = number.format(failed);
    $('unrecognized-count').textContent = number.format(unrecognized);
    $('unknown-stat').hidden = true;
    if (state.filter === 'unknown') {
      state.filter = null;
      state.page = 1;
      $('list-panel').hidden = true;
      document.querySelectorAll('.stat').forEach((button) => button.setAttribute('aria-pressed', 'false'));
    }
    if (state.filter) renderList();
  }

  function showLiveDetails(payload, taskStatus) {
    if (state.complete) return;
    const total = integer(payload.total_groups);
    const processed = integer(payload.processed_groups);
    const unknown = integer(payload.unknown_groups);
    const counts = [payload.passed_groups, payload.failed_groups, payload.unrecognized_groups].map(integer);
    $('unknown-stat').hidden = unknown === null || unknown === 0;
    $('unknown-count').textContent = number.format(unknown || 0);
    if (state.filter === 'unknown' && $('unknown-stat').hidden) {
      state.filter = null;
      state.page = 1;
      $('list-panel').hidden = true;
      document.querySelectorAll('.stat').forEach((button) => button.setAttribute('aria-pressed', 'false'));
    }
    if (total !== null) {
      $('task-total').replaceChildren(document.createTextNode(number.format(total)));
      const unit = document.createElement('small');
      unit.textContent = '组';
      $('task-total').append(unit);
      $('discovered').textContent = '任务总数已确认';
    }
    if (taskStatus === 'running' && total !== null && total > 0 && processed !== null && processed <= total) {
      const progress = $('processing').querySelector('[role="progressbar"]');
      $('processing').classList.add('is-determinate');
      progress.style.setProperty('--progress-value', `${processed / total * 100}%`);
      progress.setAttribute('aria-label', '工作处理进度');
      progress.setAttribute('aria-valuemin', '0');
      progress.setAttribute('aria-valuemax', String(total));
      progress.setAttribute('aria-valuenow', String(processed));
      progress.setAttribute('aria-valuetext', `已处理 ${number.format(processed)} / ${number.format(total)} 组`);
      $('progress-caption').textContent = `已处理 ${number.format(processed)} / ${number.format(total)} 组`;
    }
    if (processed !== null && (processed > 0 || unknown > 0) && counts.every((value) => value !== null) &&
        counts.reduce((sum, value) => sum + value, 0) === processed) {
      state.resultsAvailable = true;
      $('results').hidden = false;
      $('passed-count').textContent = number.format(counts[0]);
      $('failed-count').textContent = number.format(counts[1]);
      $('unrecognized-count').textContent = number.format(counts[2]);
      if (state.filter) renderList();
    }
  }

  function line(className, text) {
    const element = document.createElement('p');
    element.className = className;
    element.textContent = text;
    return element;
  }

  function classificationSummary(item) {
    const classification = item.classification_state;
    const counts = item.confirmed_counts;
    const unresolved = integer(item.unresolved_count);
    if (!['complete', 'partial'].includes(classification) || !counts || unresolved === null ||
        ['body', 'attachment', 'trash'].some((key) => integer(counts[key]) === null) || counts.body > 1 ||
        (classification === 'complete' && unresolved !== 0)) return '分类数据暂不可用';
    if (classification === 'complete') {
      return `正文 ${counts.body ? number.format(counts.body) : '未识别'}  ·  附件 ${number.format(counts.attachment)}  ·  垃圾 ${number.format(counts.trash)}`;
    }
    const partialCount = (name, value) => `${name} ${value ? `已确认 ${number.format(value)}` : '待识别'}`;
    return `${partialCount('正文', counts.body)}  ·  ${partialCount('附件', counts.attachment)}  ·  ${partialCount('垃圾', counts.trash)}` +
      (unresolved ? `  ·  待确认 ${number.format(unresolved)}` : '  ·  分类未完成');
  }

  function classificationSignature(item) {
    return JSON.stringify([item.classification_state, item.confirmed_counts, item.unresolved_count]);
  }

  function closeClassification() {
    if (classificationView) renderedListSignature = null;
    if (classificationView?.controller) classificationView.controller.abort();
    classificationView = null;
  }

  function classificationIsCurrent(view) {
    return classificationView === view && state.taskId === view.taskId && state.apiBase === view.apiBase &&
      state.filter === view.filter && state.page === view.page && !state.demo;
  }

  async function loadClassification(view) {
    if (!classificationIsCurrent(view)) return;
    const controller = new AbortController();
    view.controller = controller;
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const url = new URL(`/api/v1/first-step/tasks/${encodeURIComponent(view.taskId)}`, view.apiBase);
      url.searchParams.set('group_id', view.groupId);
      url.searchParams.set('include_files', 'true');
      url.searchParams.set('page_size', '1');
      const response = await fetch(url, { cache: 'no-store', signal: controller.signal });
      if (!response.ok) throw new Error('无法读取分类明细');
      const payload = await response.json();
      const detail = payload?.group_detail;
      if (payload?.task_id !== view.taskId || detail?.task_id !== view.taskId || detail?.group_id !== view.groupId ||
          !['complete', 'partial', 'unavailable'].includes(detail.classification_state)) throw new Error('分类明细未确认');
      if (detail.classification_state !== 'unavailable' && (!detail.files ||
          ['body', 'attachment', 'trash', 'unresolved'].some((key) => !Array.isArray(detail.files[key])) ||
          detail.files.body.length > 1)) throw new Error('分类明细不完整');
      if (!classificationIsCurrent(view) || view.controller !== controller) return;
      view.status = detail.classification_state === 'unavailable' ? 'unavailable' : 'ready';
      view.detail = detail;
      renderList(true);
    } catch (_) {
      if (!classificationIsCurrent(view) || view.controller !== controller) return;
      view.status = 'unavailable';
      view.detail = null;
      renderList(true);
    } finally {
      clearTimeout(timeout);
      if (view.controller === controller) view.controller = null;
    }
  }

  function toggleClassification(item) {
    if (!state.taskId || !state.apiBase || !item.group_id || state.demo) return;
    const same = classificationView && classificationView.taskId === state.taskId && classificationView.groupId === item.group_id;
    closeClassification();
    if (!same) {
      classificationView = { taskId: state.taskId, apiBase: state.apiBase, groupId: item.group_id,
        filter: state.filter, page: state.page, signature: classificationSignature(item), status: 'loading', detail: null, controller: null };
    }
    const view = classificationView;
    renderList(true);
    const target = [...$('group-list').children].find((row) => row.dataset.groupId === item.group_id)?.querySelector('.entry-toggle');
    if (target) target.focus({ preventScroll: true });
    if (view) loadClassification(view);
  }

  function classificationFiles(view) {
    const panel = document.createElement('div');
    panel.className = 'entry-files';
    if (view.status === 'loading') {
      panel.append(line('entry-files-message', '正在读取分类明细…'));
      return panel;
    }
    if (view.status !== 'ready') {
      panel.append(line('entry-files-message', '分类数据暂不可用'));
      return panel;
    }
    const detail = view.detail;
    for (const [key, label] of [['body', '正文'], ['attachment', '附件'], ['trash', '垃圾'], ['unresolved', '待确认']]) {
      const files = detail.files[key];
      if (key === 'unresolved' && files.length === 0) continue;
      const section = document.createElement('div');
      section.className = 'entry-file-section';
      const heading = document.createElement('strong');
      heading.textContent = label;
      section.append(heading);
      if (!files.length) {
        const empty = key === 'body' ? (detail.classification_state === 'partial' ? '待识别' : '未识别') :
          detail.classification_state === 'partial' ? '待识别' : '无';
        section.append(line('entry-file-empty', empty));
      }
      for (const file of files) {
        const name = typeof file?.relative_path === 'string' && file.relative_path ? file.relative_path : '文件名暂不可用';
        const reason = key === 'unresolved' && typeof file?.reason === 'string' && file.reason ? `（${file.reason}）` : '';
        section.append(line('entry-file-name', name + reason));
      }
      panel.append(section);
    }
    return panel;
  }

  function appendItem(item) {
    const row = document.createElement('li');
    if (typeof item.group_id === 'string') row.dataset.groupId = item.group_id;
    const content = document.createElement('div');
    content.className = 'entry-content';
    const title = document.createElement('span');
    const confirmed = item.title_status === '已提取' && typeof item.paper_title === 'string' && item.paper_title.trim();
    title.className = `entry-title${confirmed ? '' : ' is-hash'}`;
    title.textContent = confirmed ? item.paper_title : (item.display_hash || item.group_id || item.group || '标识暂不可用');
    const expanded = Boolean(classificationView && classificationView.taskId === state.taskId && classificationView.groupId === item.group_id);
    const toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'entry-toggle';
    toggle.disabled = !item.group_id || !state.taskId || state.demo;
    toggle.setAttribute('aria-expanded', String(expanded));
    toggle.append(title);
    const action = document.createElement('span');
    action.className = 'entry-toggle-label';
    action.textContent = expanded ? '收起' : '查看分类';
    toggle.append(action);
    toggle.addEventListener('click', () => toggleClassification(item));
    content.append(toggle);
    if (!confirmed && item.title_status === '待核对' && item.paper_title_candidate) {
      content.append(line('entry-detail', `候选题目：${item.paper_title_candidate}`));
    } else if (!confirmed) {
      content.append(line('entry-detail', '题目未识别'));
    }
    content.append(line('entry-classification', classificationSummary(item)));
    if (item.registration_status === 'registered') content.append(line('entry-detail', '已登记'));
    if (item.registration_status === 'failed') content.append(line('entry-reason', `登记失败：${item.registration_reason || '原因未确认'}`));
    if (item.reason) content.append(line('entry-reason', String(item.reason)));
    if (expanded) content.append(classificationFiles(classificationView));
    const status = document.createElement('span');
    const itemStatus = Object.hasOwn(labels, item.status) ? item.status : state.filter;
    status.className = `entry-status ${itemStatus}`;
    status.textContent = labels[itemStatus] || '';
    row.append(content, status);
    $('group-list').append(row);
  }

  function renderList(force = false) {
    if (!state.resultsAvailable || !state.filter) return;
    const selected = state.filter;
    document.querySelectorAll('.stat').forEach((button) => button.setAttribute('aria-pressed', String(button.dataset.filter === selected)));
    $('list-panel').hidden = false;
    $('list-title').textContent = labels[selected];
    let total;
    let items;
    if (state.demo) {
      const all = demoItems.filter((item) => item.status === selected);
      total = all.length;
      items = all.slice((state.page - 1) * state.pageSize, state.page * state.pageSize);
    } else if (state.payload && state.payload.filter === selected && state.payload.page === state.page && Array.isArray(state.payload.items)) {
      total = integer(state.payload.list_total);
      items = state.payload.items;
    } else {
      total = null;
      items = [];
    }
    if (classificationView) {
      const current = items.find((item) => item.group_id === classificationView.groupId);
      if (!classificationIsCurrent(classificationView) || !current) {
        closeClassification();
      } else if (classificationSignature(current) !== classificationView.signature) {
        const previous = classificationView.controller;
        classificationView.controller = null;
        if (previous) previous.abort();
        classificationView.signature = classificationSignature(current);
        classificationView.status = 'loading';
        classificationView.detail = null;
        const view = classificationView;
        queueMicrotask(() => loadClassification(view));
      }
    }
    $('list-count').textContent = total === null ? '列表获取中' :
      `共 ${number.format(total)} 组${state.demo ? ' · 演示数据' : state.complete ? '' : ' · 列表实时更新'}`;
    $('list-message').hidden = items.length > 0;
    $('list-message').textContent = total === null ? '正在获取列表…' : '暂无资料';
    const signature = JSON.stringify([state.apiBase, state.taskId, state.demo, selected, state.page, items]);
    if (force || signature !== renderedListSignature) {
      const focusedGroup = document.activeElement?.closest?.('#group-list > li')?.dataset.groupId;
      const scrollTop = $('group-list').scrollTop;
      $('group-list').replaceChildren();
      items.forEach(appendItem);
      renderedListSignature = signature;
      if (focusedGroup) {
        const target = [...$('group-list').children].find((row) => row.dataset.groupId === focusedGroup)?.querySelector('.entry-toggle');
        if (target) target.focus({ preventScroll: true });
      }
      $('group-list').scrollTop = scrollTop;
    }
    const pages = total === null ? 1 : Math.max(1, Math.ceil(total / state.pageSize));
    $('pagination').hidden = total === null || pages <= 1;
    $('page-info').textContent = `第 ${state.page} / ${pages} 页`;
    $('previous-page').disabled = state.page <= 1;
    $('next-page').disabled = state.page >= pages;
  }

  function showDemo() {
    $('registration-failed-stat').hidden = true;
    if (state.complete) {
      showComplete(176, 157, 2, 17);
      setNotice('当前为演示数据，尚未接入实际任务。', '返回处理状态 →');
    } else {
      showProcessing(28);
      setNotice('当前为演示数据，尚未接入实际任务。', '查看完成状态 →');
    }
  }

  async function refreshLive() {
    if (!state.apiBase || state.demo) return;
    if (state.requireTaskSelection && !state.taskId) {
      state.taskReadError = false;
      showProcessing(null, '未选择任务', '新建任务或打开已有任务。');
      setNotice('请选择工作间。');
      renderControls();
      return;
    }
    const request = ++state.request;
    const requestedFilter = state.filter || 'passed';
    const requestedPage = state.page;
    try {
      const base = new URL(state.apiBase, location.href);
      const requestedTaskId = state.followTask && state.taskId ? state.taskId : null;
      const task = requestedTaskId ? encodeURIComponent(requestedTaskId) : 'current';
      const url = new URL(`/api/v1/first-step/tasks/${task}`, base);
      url.searchParams.set('filter', requestedFilter);
      url.searchParams.set('page', String(requestedPage));
      url.searchParams.set('page_size', String(state.pageSize));
      const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(5000) });
      if (request === state.request) state.taskReadError = false;
      if (response.status === 404) {
        const error = await response.json();
        if (request !== state.request) return;
        if (error.detail === '第一步任务统计不存在') {
          if (state.followTask) {
            state.taskStatus = 'unknown';
            showUngrouped([]);
            showProcessing(null, '状态未知', '暂时无法确认任务状态。');
            setNotice('任务状态尚未确认。');
            renderControls();
            return;
          }
          state.taskId = null;
          state.taskStatus = null;
          state.taskName = '';
          state.controlRevision = null;
          state.payload = null;
          showUngrouped([]);
          state.filter = null;
          state.page = 1;
          showProcessing(null, '暂无任务数据', '新建任务以开始使用。');
          setNotice('暂无任务，请新建任务。');
          renderControls();
          return;
        }
        throw new Error('task not found');
      }
      if (!response.ok) throw new Error('interface unavailable');
      const payload = await response.json();
      if (request !== state.request) return;
      if (!payload || !payload.task_id) {
        state.taskId = null;
        state.taskStatus = null;
        state.taskName = '';
        state.controlRevision = null;
        showUngrouped([]);
        showProcessing(null, '暂无任务数据', '等待第一步任务统计生成。');
        setNotice('已连接接口，尚无可显示的任务统计。');
        renderControls();
        return;
      }
      if (requestedTaskId && payload.task_id !== requestedTaskId) throw new Error('任务编号不一致');
      if (state.taskId && payload.task_id !== state.taskId) {
        state.filter = null;
        state.page = 1;
        $('list-panel').hidden = true;
        document.querySelectorAll('.stat').forEach((button) => button.setAttribute('aria-pressed', 'false'));
        state.controlRevision = null;
        state.taskName = '';
      }
      const revision = integer(payload.control_revision);
      if (state.controlRevision !== null && (revision === null || revision < state.controlRevision)) return;
      if (revision !== null) state.controlRevision = revision;
      state.taskId = payload.task_id;
      if (!requestedTaskId) state.followTask = true;
      state.taskName = payload.name || payload.task_name || state.taskName;
      state.payload = payload;
      const registrationFailed = integer(payload.registration_failed_groups);
      $('registration-failed-stat').hidden = registrationFailed === null || registrationFailed === 0;
      $('registration-failed-count').textContent = number.format(registrationFailed || 0);
      if (state.filter === 'registration_failed' && $('registration-failed-stat').hidden) {
        state.filter = null;
        state.page = 1;
        $('list-panel').hidden = true;
      }
      showUngrouped(payload.ungrouped_files);
      const taskStatus = payload.task_status || payload.status;
      state.taskStatus = taskStatus || 'unknown';
      if (state.workspacePending !== 'save') $('save-state').textContent = payload.saved_at ? '已保存' : '保存状态未确认';
      const pausedAfterStart = state.uncertainAction === 'start' && taskStatus === 'paused' &&
        revision !== null && state.startAttemptRevision !== null && revision > state.startAttemptRevision;
      if ((state.uncertainAction === 'start' && ['starting', 'running', 'pausing', 'complete', 'failed'].includes(taskStatus)) ||
          pausedAfterStart || (state.uncertainAction === 'pause' && ['pausing', 'paused', 'complete', 'failed'].includes(taskStatus))) {
        state.uncertainAction = '';
        state.controlMessage = pausedAfterStart && state.startNotSubmittedMessage ? `开始未成功：${state.startNotSubmittedMessage}` : '';
        state.startNotSubmittedMessage = '';
      }
      if (['complete', 'completed', 'finished'].includes(taskStatus)) {
        const values = [payload.total_groups, payload.passed_groups, payload.failed_groups, payload.unrecognized_groups].map(integer);
        if (values.every((value) => value !== null) && values[0] === values[1] + values[2] + values[3]) {
          showComplete(...values);
          setNotice('显示第一步任务的实际结果。');
        } else {
          showProcessing(payload.discovered_groups, '结果汇总中', '正在等待完整的处理结果。');
          setNotice('任务结果尚未完整返回。');
        }
      } else if (['failed', 'crashed', 'cancelled', 'incomplete'].includes(taskStatus)) {
        showProcessing(payload.discovered_groups, '处理中断', '任务未正常结束，请查看后端记录。');
        setNotice('显示第一步任务的实际状态。');
      } else if (taskStatus === 'running') {
        showProcessing(payload.discovered_groups);
        setNotice('显示第一步任务的实际状态。');
      } else if (taskStatus === 'pausing') {
        showProcessing(payload.discovered_groups, '正在暂停', '等待当前工作停止。');
        setNotice('显示第一步任务的实际状态。');
      } else if (taskStatus === 'paused') {
        showProcessing(payload.discovered_groups, '已暂停', '点击开始可继续处理。');
        setNotice('显示第一步任务的实际状态。');
      } else if (taskStatus === 'needs_review') {
        const hasUngrouped = Array.isArray(payload.ungrouped_files) && payload.ungrouped_files.length > 0;
        const hasUnknown = integer(payload.unknown_groups) > 0;
        showProcessing(payload.discovered_groups, '待核对', hasUngrouped && hasUnknown ? '有资料尚未归组，部分结果待确认。' :
          hasUnknown ? '部分结果待确认，请查看下方列表。' : '有资料尚未归组，请查看下方列表。');
        setNotice(hasUngrouped && hasUnknown ? '部分资料尚未归组，部分结果未知。' :
          hasUnknown ? '结果未知的资料组不计入已处理。' : '部分资料需要核对，任务总数尚未确定。');
      } else if (taskStatus === 'draft' || taskStatus === 'uploading') {
        showProcessing(payload.discovered_groups, '等待上传', '上传资料后等待你点击开始。');
        setNotice('显示第一步任务的实际状态。');
      } else if (taskStatus === 'starting') {
        showProcessing(payload.discovered_groups, '正在启动', '任务正在启动，可随时请求暂停。');
        setNotice('显示第一步任务的实际状态。');
      } else if (['ready', 'pending', 'scheduled'].includes(taskStatus)) {
        showProcessing(payload.discovered_groups, '等待开始', '等待第一步任务开始。');
        setNotice('显示第一步任务的实际状态。');
      } else {
        showProcessing(payload.discovered_groups, '状态未知', '暂时无法确认任务状态。');
        setNotice('任务状态尚未确认。');
      }
      showLiveDetails(payload, taskStatus);
      renderControls();
    } catch (_) {
      if (request !== state.request) return;
      state.taskReadError = true;
      state.taskStatus = 'unknown';
      showUngrouped([]);
      showProcessing(null, '数据暂不可用', '正在等待后端接口连接。');
      setNotice('未能读取实际任务，请稍后再试。');
      renderControls();
    }
  }

  async function checkConfiguration() {
    try {
      const response = await fetch('api-config.json', { cache: 'no-store' });
      if (!response.ok) throw new Error('接口配置不可用');
      const config = await response.json();
      state.configReadError = false;
      const apiBase = typeof config.api_base_url === 'string' ? config.api_base_url.trim() : '';
      state.requireTaskSelection = config.open_current === false;
      configurationLoaded = true;
      if (apiBase && apiBase !== state.apiBase) {
        if (state.apiBase && (state.startPending || state.pausePending)) return;
        closeClassification();
        state.request += 1;
        state.apiBase = apiBase;
        state.demo = false;
        state.filter = null;
        state.page = 1;
        state.taskId = null;
        state.taskName = '';
        state.taskStatus = null;
        state.followTask = false;
        state.controlRevision = null;
        state.startAttemptRevision = null;
        state.startNotSubmittedMessage = '';
        state.controlsReady = false;
        state.taskReadError = false;
        state.controlsReadError = false;
        state.tusUrl = '';
        state.workspaceCapabilities = { create: false, createLookup: false, list: false, save: false, upload: false };
        state.payload = null;
        state.uncertainAction = '';
        state.controlMessage = '';
        showUngrouped([]);
        renderControls();
        await refreshLive();
      } else if (apiBase && !state.demo) {
        await refreshLive();
      } else if (!apiBase) {
        showProcessing(null, '数据暂不可用', '等待后端接口配置。');
        setNotice('尚未配置第一步接口。');
      }
      if (apiBase && !state.demo) await checkControls();
    } catch (_) {
      state.configReadError = true;
      state.controlsReady = false;
      state.tusUrl = '';
      state.workspaceCapabilities = { create: false, createLookup: false, list: false, save: false, upload: false };
      state.taskStatus = 'unknown';
      showUngrouped([]);
      showProcessing(null, '数据暂不可用', '正在等待后端接口连接。');
      setNotice('未能读取实际任务，请稍后再试。');
      renderControls();
    }
  }

  $('demo-toggle').addEventListener('click', () => {
    if (!state.demo) return;
    state.complete = !state.complete;
    state.filter = null;
    state.page = 1;
    document.querySelectorAll('.stat').forEach((button) => button.setAttribute('aria-pressed', 'false'));
    $('list-panel').hidden = true;
    showDemo();
  });

  $('new-task-button').addEventListener('click', () => {
    if (!state.createUncertain) state.createError = '';
    $('new-task-name').value = state.pendingCreate?.name || '';
    renderControls();
    $('new-task-dialog').showModal();
  });
  $('cancel-new-task').addEventListener('click', () => $('new-task-dialog').close());
  $('new-task-dialog').addEventListener('cancel', (event) => {
    if (state.workspacePending === 'create') event.preventDefault();
  });
  $('new-task-form').addEventListener('submit', createTask);
  $('open-task-button').addEventListener('click', () => {
    state.tasksPage = 1;
    $('open-task-dialog').showModal();
    loadTasks();
  });
  $('close-open-task').addEventListener('click', () => $('open-task-dialog').close());
  $('previous-task-page').addEventListener('click', () => {
    if (state.tasksPage > 1) { state.tasksPage -= 1; loadTasks(); }
  });
  $('next-task-page').addEventListener('click', () => { state.tasksPage += 1; loadTasks(); });
  $('save-task-button').addEventListener('click', saveTask);
  $('local-folder-button').addEventListener('click', () => {
    if ($('local-folder-button').disabled) return;
    $('local-folder-path').value = '';
    $('local-folder-message').textContent = '选择或输入路径后，程序会先统计资料组。';
    $('browse-local-folder').disabled = !state.workspaceCapabilities.browse;
    $('local-folder-dialog').showModal();
  });
  $('browse-local-folder').addEventListener('click', browseLocalFolder);
  $('local-folder-form').addEventListener('submit', chooseLocalFolder);
  $('cancel-local-folder').addEventListener('click', () => {
    if (!state.workspacePending) $('local-folder-dialog').close();
  });
  $('local-folder-dialog').addEventListener('cancel', (event) => {
    if (state.workspacePending) event.preventDefault();
  });
  $('delete-task-button').addEventListener('click', () => deleteTask(state.taskId, state.taskName));
  $('upload-button').addEventListener('click', openUploader);
  $('close-upload').addEventListener('click', closeUploader);
  $('upload-dialog').addEventListener('cancel', (event) => {
    event.preventDefault();
    closeUploader();
  });
  $('start-button').addEventListener('click', () => changeTask('start'));
  $('pause-button').addEventListener('click', () => changeTask('pause'));

  document.querySelectorAll('.stat').forEach((button) => button.addEventListener('click', () => {
    state.filter = button.dataset.filter;
    state.page = 1;
    renderList();
    if (!state.demo) refreshLive();
  }));
  $('close-list').addEventListener('click', () => {
    closeClassification();
    state.filter = null;
    $('list-panel').hidden = true;
    document.querySelectorAll('.stat').forEach((button) => button.setAttribute('aria-pressed', 'false'));
  });
  $('previous-page').addEventListener('click', () => {
    if (state.page > 1) { state.page -= 1; renderList(); if (!state.demo) refreshLive(); }
  });
  $('next-page').addEventListener('click', () => {
    state.page += 1;
    renderList();
    if (!state.demo) refreshLive();
  });

  showProcessing(null, '连接中', '正在读取接口配置。');
  setNotice('正在连接后端。');
  checkConfiguration();
  setInterval(checkConfiguration, 5000);
})();
