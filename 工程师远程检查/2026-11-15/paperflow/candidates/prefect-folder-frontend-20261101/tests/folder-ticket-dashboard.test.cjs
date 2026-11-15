const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const script = path.join(__dirname, '..', 'folder-ticket-dashboard.js');
const queryBundle = path.join(__dirname, '..', 'vendor', 'query-runtime.js');

class Element {
  constructor() {
    this.textContent = '—';
    this.hidden = false;
    this.disabled = false;
    this.dataset = {};
    this.attributes = {};
    this.children = [];
    this.listeners = {};
    this.open = false;
    this.value = '';
  }
  setAttribute(name, value) { this.attributes[name] = value; }
  addEventListener(name, handler) { this.listeners[name] = handler; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  click() { if (!this.disabled) return this.listeners.click?.(); }
  showModal() { this.open = true; }
  close() { this.open = false; }
  submit() { return this.listeners.submit?.({ preventDefault() {} }); }
}

function makePage(snapshot, refreshResult, configured = true, options = {}) {
  const nodes = new Map();
  const buttons = new Map();
  const timers = [];
  let elapsed = 0;
  class TestDate extends Date { static now() { return Date.now() + elapsed; } }
  const element = (id) => {
    if (!nodes.has(id)) nodes.set(id, new Element());
    return nodes.get(id);
  };
  for (const section of ['preprocessing', 'llm', 'postprocessing']) {
    buttons.set(section, ['passed', 'failed', 'unrecognized'].map((filter) => {
      const button = new Element();
      button.dataset[`${section}Filter`] = filter;
      return button;
    }));
  }
  buttons.set('classification', ['passed', 'failed', 'unrecognized', 'unresolved'].map((filter) => {
    const button = new Element();
    button.dataset.filter = filter;
    return button;
  }));
  let calls = 0;
  const requests = [];
  const readRequests = [];
  const controlRequests = [];
  const storage = options.storage || new Map();
  const tasks = options.tasks || [{ task_id: 'task-1', task_name: '任务一' }];
  const tasksUrl = options.apiRoot ? `${options.apiRoot}/tasks` : 'https://example.test/tasks';
  const foldersUrl = options.apiRoot ? `${options.apiRoot}/folder-tickets` : 'https://example.test/folders';
  const controlRoot = options.apiRoot || 'https://example.test/control';
  const document = {
    getElementById: element,
    querySelectorAll(selector) {
      if (selector === '[data-filter]') return buttons.get('classification');
      const match = selector.match(/^\[data-(preprocessing|llm|postprocessing)-filter\]$/);
      return match ? buttons.get(match[1]) : [];
    },
    createElement() { return new Element(); },
  };
  const context = {
    document, Intl, Date: TestDate, URL, AbortController, navigator: options.navigator || { onLine: true },
    AbortSignal: { timeout: (milliseconds) => ({ milliseconds }), any: ([signal, timeout]) => ({ ...timeout, signal }) },
    crypto: { randomUUID: options.randomUUID || (() => '11111111-1111-4111-8111-111111111111') },
    confirm: () => options.confirmRetry !== false,
    localStorage: {
      get length() { return storage.size; },
      key: (index) => [...storage.keys()][index] ?? null,
      getItem: (key) => storage.get(key) ?? null,
      setItem: (key, value) => storage.set(key, value),
      removeItem: (key) => storage.delete(key),
    },
    fetch: async (url, requestOptions) => {
      if (url === 'api-config.json') {
        if (options.configFailure) throw new Error('configuration unavailable');
        return { ok: true, json: async () => options.apiConfig || ({
          tasks_api_url: configured ? tasksUrl : '',
          folder_tickets_api_url: configured ? foldersUrl : '',
          control_api_url: options.controls ? controlRoot : '',
          automatic_gateway_capacity: options.gatewayReady !== false,
        }) };
      }
      if (!requestOptions?.method || requestOptions.method === 'GET') readRequests.push({ url: String(url), options: requestOptions });
      if (url === tasksUrl && requestOptions?.method !== 'POST') {
        if (options.taskListFailure) throw new Error('task list unavailable');
        return { ok: true, json: async () => options.taskListResponse || ({ as_of: '2026-11-01T12:00:00Z', complete: true, tasks }) };
      }
      if (String(url).endsWith('/classifications')) {
        const taskId = decodeURIComponent(String(url).split('/').at(-2));
        const value = options.classificationResponse
          ? await options.classificationResponse(taskId)
          : { task_id: taskId, as_of: '2026-11-01T12:00:00Z', complete: false, items: [] };
        return { ok: true, json: async () => value };
      }
      if (String(url).startsWith(`${controlRoot}/`) && !String(url).startsWith(`${foldersUrl}?`)) {
        controlRequests.push({ url, options: requestOptions });
        return options.controlResponse(url, requestOptions);
      }
      const request = new URL(url);
      assert.equal(request.origin + request.pathname, foldersUrl);
      const taskId = request.searchParams.get('task_id');
      requests.push(taskId);
      calls += 1;
      if (calls > 1 && refreshResult instanceof Error) throw refreshResult;
      const selected = options.snapshotResponse ? await options.snapshotResponse(taskId, calls)
        : options.snapshots ? await options.snapshots[taskId] : (calls > 1 && refreshResult ? refreshResult : snapshot);
      const value = { task_id: taskId, task_state: 'running', desired_state: 'running', ...selected };
      if (!options.rawSnapshot && !Object.hasOwn(value, 'actions')) value.actions = {
        start: { allowed: value.desired_state === 'paused' && value.task_state !== 'completed', reason: null },
        pause: { allowed: value.desired_state === 'running', reason: null },
      };
      return { ok: true, json: async () => value };
    },
    setInterval(handler, interval) { timers.push(() => { elapsed += interval; return handler(); }); }, setTimeout, clearTimeout,
  };
  vm.createContext(context);
  if (fs.existsSync(queryBundle)) vm.runInContext(fs.readFileSync(queryBundle, 'utf8'), context);
  // DOM unit checks use the supplied in-memory storage. Native IndexedDB is
  // exercised by query-recovery.test.cjs in a real isolated browser profile.
  const createQueryStore = context.PaperflowQuery.createQueryStore;
  context.PaperflowQuery.createQueryStore = (storage, onError) => createQueryStore(storage, onError, storage);
  if (fs.existsSync(script)) vm.runInContext(fs.readFileSync(script, 'utf8'), context);
  return {
    element, buttons, timers, requests, readRequests, controlRequests, storage,
    async selectTask(taskId = 'task-1') {
      element('open-task-button').click();
      await new Promise(setImmediate);
      const row = element('task-list').children.find((item) => item.children[0].dataset.taskId === taskId);
      assert.ok(row, `task ${taskId} is available`);
      row.children[0].click();
      await new Promise(setImmediate);
    },
  };
}

const folder = (ticket_id, stage, extra = {}) => ({
  ticket_id, task_id: 'task-1', folder_id: `folder-${ticket_id}`,
  folder_name: `资料-${ticket_id}`, paper_title: null, stage,
  llm_body_running: false, llm_attachment_running: false,
  preprocessing_result: null, llm_result: null, postprocessing_result: null,
  last_retry: null,
  ...extra,
});
const flush = () => new Promise(setImmediate);

test('shows waiting for integration rather than a zero before an endpoint is configured', async () => {
  const page = makePage(null, null, false);
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '待接入');
  assert.match(page.element('folder-ticket-overview').textContent, /等待后端接入/);
});

test('a single candidate API root provides task, folder and control routes', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'running', desired_state: 'running', folders: [folder('1', 'waiting_llm')] }, null, true, {
    apiRoot: 'https://example.test/api/v1/prefect', controls: true,
    controlResponse: async () => reply({ ok: true }),
  });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-llm-count').textContent, '1');
  assert.equal(page.element('passed-count').textContent, '待核对');
  await page.element('pause-button').click();
  assert.equal(page.controlRequests[0].url, 'https://example.test/api/v1/prefect/tasks/task-1/pause');
});

test('the unified server configuration loads the page with its own task routes',
  { skip: !process.env.PAPERFLOW_UNIFIED_FIXTURE }, async () => {
    const config = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_UNIFIED_FIXTURE, 'utf8'));
    assert.equal(config.tasks_api_url, `${config.control_api_url}/tasks`);
    assert.equal(config.folder_tickets_api_url, `${config.control_api_url}/folder-tickets`);
    const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
      folders: [folder('1', 'waiting_llm')] }, null, true, {
      apiRoot: config.control_api_url, apiConfig: config, controls: true,
      controlResponse: async () => reply({ ok: true }),
    });
    await flush(); await page.selectTask();
    assert.equal(page.element('waiting-llm-count').textContent, '1');
    await page.element('pause-button').click();
    assert.equal(page.controlRequests[0].url, `${config.control_api_url}/tasks/task-1/pause`);
  });

test('counts the current stage of each folder and retains stage results', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'waiting_preprocessing'),
    folder('2', 'preprocessing_in_progress'),
    folder('3', 'llm_in_progress', { llm_body_running: true, preprocessing_result: 'passed' }),
    folder('4', 'llm_in_progress', { llm_attachment_running: true, preprocessing_result: 'passed' }),
    folder('5', 'waiting_postprocessing', { preprocessing_result: 'passed', llm_result: 'passed' }),
    folder('6', 'rag_in_progress', { postprocessing_result: 'passed' }),
    folder('7', 'nas_in_progress', { postprocessing_result: 'passed' }),
    folder('8', 'completed', { postprocessing_result: 'passed' }),
  ] });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '1');
  assert.equal(page.element('preprocessing-count').textContent, '1');
  assert.equal(page.element('llm-folder-count').textContent, '2');
  assert.equal(page.element('llm-body-count').textContent, '1');
  assert.equal(page.element('llm-attachment-count').textContent, '1');
  assert.equal(page.element('waiting-postprocessing-count').textContent, '1');
  assert.equal(page.element('rag-processing-count').textContent, '1');
  assert.equal(page.element('waiting-nas-archive-count').textContent, '1');
  assert.equal(page.element('completed-count').textContent, '1');
  assert.equal(page.element('preprocessing-passed-count').textContent, '3');
  assert.equal(page.element('postprocessing-passed-count').textContent, '3');
});

test('a folder processing its body and attachments at the same time is one ticket', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'llm_in_progress', { llm_body_running: true, llm_attachment_running: true }),
  ] });
  await flush(); await page.selectTask();
  assert.equal(page.element('llm-folder-count').textContent, '1');
  assert.equal(page.element('llm-body-count').textContent, '1');
  assert.equal(page.element('llm-attachment-count').textContent, '1');
  assert.match(page.element('folder-ticket-overview').textContent, /共 1 张文件夹工单/);
});

test('queue counts and registered total stay separate during incomplete classification', async () => {
  let current = { as_of: '2026-11-01T12:00:00Z', complete: true,
    folders: Array.from({ length: 10 }, (_, i) => folder(String(i + 1), 'waiting_preprocessing')) };
  const page = makePage(null, null, true, { controls: true, snapshotResponse: async () => current,
    classificationResponse: async (taskId) => ({ task_id: taskId, as_of: '2026-11-01T12:00:00Z', complete: false, items: [] }) });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '10');
  assert.equal(page.element('preprocessing-count').textContent, '0');
  assert.equal(page.element('task-total').textContent, '10');
  current = { ...current, as_of: '2026-11-01T12:00:05Z',
    folders: current.folders.map((item, i) => i < 7 ? item : { ...item, stage: 'preprocessing_in_progress' }) };
  page.timers[0](); await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '7');
  assert.equal(page.element('preprocessing-count').textContent, '3');
  assert.equal(page.element('task-total').textContent, '10');
  assert.match(page.element('folder-ticket-overview').textContent, /共 10 张文件夹工单/);
  assert.equal(page.element('passed-count').textContent, '待核对');
  current = { ...current, as_of: '2026-11-01T12:00:10Z',
    folders: [...current.folders, folder('11', 'unresolved')] };
  page.timers[0](); await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '≥7');
  assert.equal(page.element('preprocessing-count').textContent, '≥3');
  assert.equal(page.element('task-total').textContent, '≥11');
  assert.match(page.element('waiting-preprocessing-hint').textContent, /统计不完整/);
  assert.match(page.element('waiting-nas-archive-hint').textContent, /统计不完整/);
  page.buttons.get('preprocessing')[0].click();
  assert.match(page.element('preprocessing-list-message').textContent, /其他工单待核对/);
  assert.equal(page.controlRequests.length, 0);
});

test('a complete list with an unresolved ticket does not display a completed task', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, task_state: 'completed',
    folders: [folder('1', 'unresolved')] });
  await flush(); await page.selectTask();
  assert.equal(page.element('task-total').textContent, '≥1');
  assert.equal(page.element('waiting-preprocessing-count').textContent, '待核对');
  assert.equal(page.element('task-state').textContent, '状态待核对');
  assert.match(page.element('folder-ticket-overview').textContent, /待核对 1/);
});

test('a partial snapshot does not present unknown totals as zero', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '待核对');
  assert.match(page.element('waiting-preprocessing-hint').textContent, /已确认|统计不完整/);
});

test('a folder with an unresolved position is reported as awaiting review', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false,
    folders: [folder('1', 'unresolved')] });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '待核对');
  assert.match(page.element('folder-ticket-overview').textContent, /待核对 1/);
  assert.match(page.element('waiting-preprocessing-hint').textContent, /统计不完整/);
});

test('a failed folder identifies the stage where it stopped', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    folders: [folder('1', 'failed', { failed_stage: 'rag' })] });
  await flush(); await page.selectTask();
  assert.match(page.element('folder-ticket-overview').textContent, /失败 1/);
  assert.match(page.element('folder-ticket-overview').textContent, /RAG 1/);
  assert.equal(page.element('rag-processing-count').textContent, '0');
});

test('a failed refresh retains the last snapshot and marks it outdated', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    folders: [folder('1', 'waiting_preprocessing')] }, new Error('offline'));
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '1');
  page.timers[0]();
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '1');
  assert.match(page.element('folder-ticket-overview').textContent, /更新失败|上次/);
});

test('result list shows the matching folder from the same snapshot', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'waiting_llm', { paper_title: '论文甲', preprocessing_result: 'passed' }),
    folder('2', 'waiting_preprocessing'),
  ] });
  await flush(); await page.selectTask();
  page.buttons.get('preprocessing')[0].click();
  assert.equal(page.element('preprocessing-list-panel').hidden, false);
  assert.equal(page.element('preprocessing-group-list').children.length, 1);
  assert.equal(page.element('preprocessing-group-list').children[0].children[0].children[0].textContent, '论文甲');
});

test('postprocessing result details use the same label as the card', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'postprocessing', postprocessing_result: 'failed' }),
  ] });
  await flush(); await page.selectTask();
  page.buttons.get('postprocessing')[1].click();
  assert.equal(page.element('postprocessing-list-title').textContent, '后处理未通过');
  assert.equal(page.element('postprocessing-group-list').children[0].children[1].textContent, '未通过');
});

test('task selection scopes counts and lists, and late responses cannot replace another task', async () => {
  let resolveA;
  const delayedA = new Promise((resolve) => { resolveA = resolve; });
  const page = makePage(null, null, true, {
    tasks: [{ task_id: 'A', task_name: '任务甲' }, { task_id: 'B', task_name: '任务乙' }],
    snapshots: {
      A: delayedA,
      B: { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
        folder('b1', 'waiting_llm', { task_id: 'B', paper_title: '论文乙', preprocessing_result: 'passed' }),
        folder('b2', 'waiting_llm', { task_id: 'B' }),
      ] },
    },
  });
  await flush();
  assert.deepEqual(page.requests, []);
  await page.selectTask('A');
  await page.selectTask('B');
  assert.equal(page.element('waiting-llm-count').textContent, '2');
  page.buttons.get('preprocessing')[0].click();
  assert.equal(page.element('preprocessing-group-list').children[0].children[0].children[0].textContent, '论文乙');
  resolveA({ as_of: '2026-11-01T11:59:00Z', complete: true, folders: [
    folder('a1', 'waiting_preprocessing', { task_id: 'A' }),
  ] });
  await flush();
  assert.deepEqual(page.requests, ['A', 'B']);
  assert.equal(page.element('waiting-llm-count').textContent, '2');
  assert.equal(page.element('waiting-preprocessing-count').textContent, '0');
  assert.match(page.element('task-name').textContent, /任务乙/);
});

test('a slow previous task does not block or restart the selected task read', async () => {
  let finishA, finishB;
  const delayedA = new Promise((resolve) => { finishA = resolve; });
  const delayedB = new Promise((resolve) => { finishB = resolve; });
  const page = makePage(null, null, true, {
    tasks: [{ task_id: 'A', task_name: '任务甲' }, { task_id: 'B', task_name: '任务乙' }],
    snapshotResponse: async (taskId) => taskId === 'A' ? delayedA : delayedB,
  });
  await flush(); await page.selectTask('A'); await page.selectTask('B');
  page.timers[0](); await flush();
  assert.deepEqual(page.requests, ['A', 'B']);
  finishA({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [] });
  await flush(); page.timers[0](); await flush();
  assert.deepEqual(page.requests, ['A', 'B']);
  finishB({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('b1', 'waiting_llm', { task_id: 'B' }),
  ] });
  await flush();
  assert.equal(page.element('waiting-llm-count').textContent, '1');
  page.timers[0](); await flush();
  assert.deepEqual(page.requests, ['A', 'B', 'B']);
});

test('slow folder reads complete without repeated polling and then refresh normally', async () => {
  let finishRead;
  const delayed = new Promise((resolve) => { finishRead = resolve; });
  const first = { as_of: '2026-11-01T12:00:00Z', complete: true,
    folders: Array.from({ length: 176 }, (_, i) => folder(String(i + 1), 'waiting_llm')) };
  const page = makePage(null, null, true, { snapshotResponse: async (_, call) => call === 1 ? delayed : first });
  await flush(); await page.selectTask();
  for (let i = 0; i < 7; i += 1) page.timers[0]();
  await flush();
  assert.deepEqual(page.requests, ['task-1']);
  assert.equal(page.readRequests.find((request) => request.url.includes('/folders?')).options.signal.milliseconds, 120000);
  assert.equal(page.readRequests.find((request) => request.url === 'https://example.test/tasks').options.signal.milliseconds, 120000);
  finishRead(first); await flush();
  assert.equal(page.element('waiting-llm-count').textContent, '176');
  page.timers[0](); await flush();
  assert.deepEqual(page.requests, ['task-1', 'task-1']);
});

test('an older refresh or error cannot overwrite a newer snapshot after task control', async () => {
  let resolveOlder, rejectOlder;
  const older = new Promise((resolve) => { resolveOlder = resolve; });
  const olderError = new Promise((_, reject) => { rejectOlder = reject; });
  const first = { as_of: '2026-11-01T12:00:00Z', complete: true, task_state: 'paused', desired_state: 'paused',
    folders: [folder('1', 'waiting_llm')] };
  const newer = { as_of: '2026-11-01T12:01:00Z', complete: true,
    task_state: 'running', desired_state: 'running',
    folders: [folder('1', 'waiting_llm'), folder('2', 'waiting_llm')] };
  const newest = { ...newer, as_of: '2026-11-01T12:02:00Z', task_state: 'paused', desired_state: 'paused' };
  const page = makePage(null, null, true, { controls: true, controlResponse: async () => reply({ ok: true }), snapshotResponse: async (_, call) =>
    call === 5 ? newest :
    call === 1 ? first : call === 2 ? older : call === 4 ? olderError : newer });
  await flush(); await page.selectTask();
  page.timers[0](); await flush();
  await page.element('start-button').click();
  assert.equal(page.element('waiting-llm-count').textContent, '2');
  const newestHint = page.element('folder-ticket-overview').textContent;
  resolveOlder(first); await flush();
  assert.equal(page.element('waiting-llm-count').textContent, '2');
  assert.equal(page.element('folder-ticket-overview').textContent, newestHint);
  page.timers[0](); await flush();
  await page.element('pause-button').click();
  const afterPauseHint = page.element('folder-ticket-overview').textContent;
  rejectOlder(new Error('old request failed')); await flush();
  assert.equal(page.element('folder-ticket-overview').textContent, afterPauseHint);
});

test('LLM passed stays passed when postprocessing fails', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'postprocessing', llm_result: 'passed', postprocessing_result: 'failed' }),
  ] });
  await flush(); await page.selectTask();
  assert.equal(page.element('llm-passed-count').textContent, '1');
  assert.equal(page.element('postprocessing-failed-count').textContent, '1');
});

test('cleanup failures are shown at the correct stage', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'cleanup' }),
  ] });
  await flush(); await page.selectTask();
  assert.match(page.element('folder-ticket-overview').textContent, /清理 1/);
  assert.equal(page.element('completed-count').textContent, '0');
});

test('local cleanup is displayed separately from NAS archiving and completion', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'cleanup_in_progress'),
  ] });
  await flush(); await page.selectTask();
  assert.equal(page.element('cleanup-count').textContent, '1');
  assert.equal(page.element('waiting-nas-archive-count').textContent, '0');
  assert.equal(page.element('completed-count').textContent, '0');
});

test('switching tasks clears the previous result while the next task is loading', async () => {
  let resolveB;
  const delayedB = new Promise((resolve) => { resolveB = resolve; });
  const page = makePage(null, null, true, {
    tasks: [{ task_id: 'A', task_name: '任务甲' }, { task_id: 'B', task_name: '任务乙' }],
    snapshots: {
      A: { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
        folder('a1', 'waiting_llm', { task_id: 'A', preprocessing_result: 'passed' }),
      ] },
      B: delayedB,
    },
  });
  await flush(); await page.selectTask('A');
  page.buttons.get('preprocessing')[0].click();
  assert.equal(page.element('preprocessing-list-panel').hidden, false);
  await page.selectTask('B');
  assert.equal(page.element('waiting-llm-count').textContent, '读取中');
  assert.equal(page.element('preprocessing-list-panel').hidden, true);
  assert.equal(page.buttons.get('preprocessing')[0].disabled, true);
  resolveB({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [] });
  await flush();
  assert.equal(page.element('waiting-llm-count').textContent, '0');
});

test('a response containing a folder from another task is unavailable instead of mixed', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'waiting_llm', { task_id: 'another-task' }),
  ] });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-llm-count').textContent, '暂不可用');
  assert.match(page.element('folder-ticket-overview').textContent, /暂不可用/);
});

test('incomplete snapshots show known lower bounds and cannot display a completed task', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, task_state: 'completed', folders: [
    folder('1', 'waiting_llm'),
  ] });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-llm-count').textContent, '≥1');
  assert.equal(page.element('completed-count').textContent, '待核对');
  assert.equal(page.element('task-state').textContent, '状态待核对');
});

test('an incomplete backend snapshot explains why counts are unavailable', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false,
    detail: '论文工单读取尚未接线', folders: [] });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-llm-count').textContent, '待核对');
  assert.match(page.element('folder-ticket-overview').textContent, /工单数量待核对/);
  assert.match(page.element('folder-ticket-overview').textContent, /论文工单读取尚未接线/);
});

test('unknown body and attachment activity remains visible as awaiting review', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false,
    detail: '阶段结果尚未接线', folders: [folder('1', 'unresolved', {
      llm_body_running: null, llm_attachment_running: null,
    })] });
  await flush(); await page.selectTask();
  assert.equal(page.element('llm-body-count').textContent, '待核对');
  assert.equal(page.element('llm-attachment-count').textContent, '待核对');
  assert.match(page.element('folder-ticket-overview').textContent, /待核对 1/);
  assert.match(page.element('folder-ticket-overview').textContent, /阶段结果尚未接线/);
});

test('unknown activity is not displayed as zero when the ticket list is complete', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'llm_in_progress', { llm_body_running: null, llm_attachment_running: true }),
  ] });
  await flush(); await page.selectTask();
  assert.equal(page.element('llm-folder-count').textContent, '1');
  assert.equal(page.element('llm-body-count').textContent, '待核对');
  assert.equal(page.element('llm-attachment-count').textContent, '1');
});

test('a failed RAG folder can be located, while retry remains disabled until the new API is available', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', paper_title: '论文甲', failure_detail: '入库失败',
      retryable: null, retry_reason: '失败点尚未接线' }),
  ] });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  assert.equal(page.element('failed-folders-panel').hidden, false);
  const row = page.element('failed-folders-list').children[0];
  assert.equal(row.children[0].children[0].textContent, '论文甲');
  assert.match(row.children[0].children[3].textContent, /RAG.*入库失败/);
  assert.match(row.children[0].children[4].textContent, /失败点尚未接线/);
  assert.equal(row.children[1].disabled, true);
});

test('a backend-declared non-retryable failure remains disabled and shows its reason', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [
    folder('1', 'failed', { failed_stage: 'postprocessing', retryable: false, retry_reason: '手动恢复尚未接线' }),
  ] }, null, true, { controls: true });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  const row = page.element('failed-folders-list').children[0];
  assert.equal(row.children[1].disabled, true);
  assert.equal(row.children[1].textContent, '暂不可重试');
  assert.match(row.children[0].children[4].textContent, /手动恢复尚未接线/);
  assert.equal(page.controlRequests.length, 0);
});

test('a requested pause remains visible while already running work is finishing', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [
      folder('1', 'llm_in_progress', { llm_body_running: true }),
    ] });
  await flush(); await page.selectTask();
  assert.match(page.element('task-state').textContent, /暂停/);
  assert.match(page.element('task-state').textContent, /处理/);
  assert.equal(page.element('llm-folder-count').textContent, '1');
});

test('the failed folder uses its stage detail when no overall failure detail is provided', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'llm', llm_result: 'failed', llm_detail: '请求失败的实际原因' }),
  ] });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  assert.match(page.element('failed-folders-list').children[0].children[0].children[3].textContent, /请求失败的实际原因/);
});

test('a shared dependency failure is shown as a task reason instead of a paper failure', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'failed', desired_state: 'paused', stop_reason: '模型服务不可用', folders: [] });
  await flush(); await page.selectTask();
  assert.equal(page.element('task-detail').hidden, false);
  assert.match(page.element('task-detail').textContent, /模型服务不可用/);
  assert.equal(page.element('task-total').textContent, '0');
  assert.equal(page.element('failed-folders-button').hidden, true);
});

const reply = (body, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => body });
const preflight = (ready) => ({ ready, checked_at: '2026-11-02T10:00:00Z', checks: [
  { name: 'rag', ready, detail: ready ? '可用' : 'RAG 未就绪' },
] });

test('dependency preflight shows the failure and blocks task creation', async () => {
  const page = makePage(null, null, true, { controls: true,
    controlResponse: async () => reply(preflight(false)) });
  await flush();
  assert.equal(page.element('new-task-button').disabled, false);
  page.element('new-task-button').click();
  await flush();
  assert.equal(page.element('new-task-dialog').open, true);
  assert.match(page.element('preflight-checks').children[0].textContent, /RAG 未就绪/);
  assert.equal(page.element('create-task-confirm').disabled, true);
  await page.element('new-task-form').submit();
  assert.equal(page.controlRequests.length, 1);
  assert.equal(page.controlRequests[0].options.signal.milliseconds, 600000);
});

test('preflight waits for first-step recovery and shows the backend result', async () => {
  let finishPreflight;
  const response = new Promise((resolve) => { finishPreflight = resolve; });
  const page = makePage(null, null, true, { controls: true, controlResponse: async () => response });
  await flush(); page.element('new-task-button').click(); await flush();
  assert.match(page.element('new-task-message').textContent, /尝试启动/);
  assert.equal(page.element('create-task-confirm').disabled, true);
  assert.equal(page.controlRequests[0].options.signal.milliseconds, 600000);
  finishPreflight(reply({ ready: true, checked_at: '2026-11-02T10:00:00Z', checks: [
    { name: 'nimble', ready: true, detail: '第一步服务已自动启动并复检通过，所需模型已注册，未发送推理' },
    { name: 'paper_deployment', ready: true, detail: '论文子工单部署可读取' },
  ] }));
  await flush();
  assert.equal(page.element('create-task-confirm').disabled, false);
  assert.match(page.element('preflight-checks').children[0].textContent, /第一步服务：已就绪/);
  assert.match(page.element('preflight-checks').children[0].textContent, /已自动启动并复检通过/);
  assert.match(page.element('preflight-checks').children[1].textContent, /论文小工单部署：已就绪/);

  const failed = makePage(null, null, true, { controls: true, controlResponse: async () => reply({
    ready: false, checked_at: '2026-11-02T10:00:00Z', checks: [
      { name: 'nimble', ready: false, detail: '第一步服务自动启动失败：等待启动超过 25 秒' },
    ],
  }) });
  await flush(); failed.element('new-task-button').click(); await flush();
  assert.equal(failed.element('create-task-confirm').disabled, true);
  assert.match(failed.element('preflight-checks').children[0].textContent, /自动启动失败：等待启动超过 25 秒/);
});

test('a ready preflight creates one task with an explicit request identity and source path', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] }, null, true, {
    controls: true,
    controlResponse: async (url) => url.endsWith('/preflight') ? reply(preflight(true)) : reply({
      request_id: '11111111-1111-4111-8111-111111111111', accepted: true, status: 'confirmed',
      task_id: 'new-task', task_name: '测试任务', task_state: 'created', desired_state: 'paused',
    }, 201),
  });
  await flush(); page.element('new-task-button').click(); await flush();
  page.element('new-task-name').value = '测试任务';
  page.element('new-task-paths').value = 'F:\\模拟资料';
  await page.element('new-task-form').submit(); await flush();
  const sent = page.controlRequests.find((request) => request.url.endsWith('/tasks'));
  assert.ok(sent);
  assert.equal(sent.options.signal.milliseconds, 600000);
  const body = JSON.parse(sent.options.body);
  assert.equal(body.request_id, '11111111-1111-4111-8111-111111111111');
  assert.deepEqual(body.source_paths, ['F:\\模拟资料']);
  assert.equal(page.storage.has('paperflow-pending-task-create'), false);
  assert.match(page.element('task-name').textContent, /测试任务/);
});

test('an unknown create outcome retains its identity across reload and is never resent automatically', async () => {
  const storage = new Map();
  const options = { controls: true, storage, controlResponse: async (url) => {
    if (url.endsWith('/preflight')) return reply(preflight(true));
    throw new Error('connection lost');
  } };
  const page = makePage(null, null, true, options);
  await flush(); page.element('new-task-button').click(); await flush();
  page.element('new-task-name').value = '测试任务';
  page.element('new-task-paths').value = 'F:\\模拟资料';
  await page.element('new-task-form').submit();
  assert.match(page.element('new-task-message').textContent, /待核对/);
  assert.equal(storage.get('paperflow-pending-task-create'), '11111111-1111-4111-8111-111111111111');
  await page.element('new-task-form').submit();
  assert.equal(page.controlRequests.filter((request) => request.url.endsWith('/tasks')).length, 1);
  const reopened = makePage(null, null, true, options);
  await flush(); reopened.element('new-task-button').click(); await flush();
  assert.equal(reopened.element('create-task-confirm').disabled, true);
  assert.match(reopened.element('new-task-message').textContent, /11111111-1111-4111-8111-111111111111/);
  assert.equal(reopened.controlRequests.filter((request) => request.url.endsWith('/tasks')).length, 0);
});

test('a pending creation keeps its request identity until the backend finds the task', async () => {
  const requestId = '11111111-1111-4111-8111-111111111111';
  const storage = new Map([['paperflow-pending-task-create', requestId]]);
  let found = false;
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] }, null, true, {
    controls: true, storage,
    controlResponse: async (url) => url.endsWith('/preflight') ? reply(preflight(true)) : reply(found
      ? { request_id: requestId, found: true, resolved: true, task: { task_id: 'task-2', task_name: '已创建任务' } }
      : { request_id: requestId, found: false, resolved: false }),
  });
  await flush(); page.element('new-task-button').click(); await flush();
  assert.equal(page.element('check-create-result').hidden, false);
  await page.element('check-create-result').click();
  assert.equal(page.controlRequests.find((request) => request.url.endsWith(`/tasks/by-request/${requestId}`)).options.signal.milliseconds, 120000);
  assert.equal(storage.get('paperflow-pending-task-create'), requestId);
  assert.equal(page.element('create-task-confirm').disabled, true);
  assert.match(page.element('new-task-message').textContent, /待核对/);
  found = true;
  await page.element('check-create-result').click(); await flush();
  assert.equal(storage.has('paperflow-pending-task-create'), false);
  assert.equal(page.element('task-name').textContent, '已创建任务');
  assert.equal(page.controlRequests.filter((request) => request.url.endsWith('/tasks')).length, 0);
  assert.ok(page.controlRequests.some((request) => request.url.endsWith(`/tasks/by-request/${requestId}`)));
});

test('an unverified creation lookup never clears the original request identity', async () => {
  const requestId = '11111111-1111-4111-8111-111111111111';
  const storage = new Map([['paperflow-pending-task-create', requestId]]);
  const page = makePage(null, null, true, { controls: true, storage, controlResponse: async (url) =>
    url.endsWith('/preflight') ? reply(preflight(true)) : reply({ request_id: 'different', found: true, resolved: true,
      task: { task_id: 'task-2', task_name: '其他任务' } }) });
  await flush(); page.element('new-task-button').click(); await flush();
  await page.element('check-create-result').click();
  assert.equal(storage.get('paperflow-pending-task-create'), requestId);
  assert.equal(page.element('create-task-confirm').disabled, true);
  assert.match(page.element('new-task-message').textContent, /待核对/);
  assert.ok(page.controlRequests.some((request) => request.url.endsWith(`/tasks/by-request/${requestId}`)));
});

test('creation lookup displays a backend conflict while retaining the original request identity', async () => {
  const requestId = '11111111-1111-4111-8111-111111111111';
  const storage = new Map([['paperflow-pending-task-create', requestId]]);
  const page = makePage(null, null, true, { controls: true, storage, controlResponse: async (url) =>
    url.endsWith('/preflight') ? reply(preflight(true)) : reply({ detail: '同一创建身份对应多个工单，需核对' }, 409) });
  await flush(); page.element('new-task-button').click(); await flush();
  await page.element('check-create-result').click();
  assert.equal(storage.get('paperflow-pending-task-create'), requestId);
  assert.equal(page.element('create-task-confirm').disabled, true);
  assert.match(page.element('new-task-message').textContent, /同一创建身份对应多个工单/);
});

test('task action buttons use backend permissions and display the reasons', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false,
    task_state: 'unresolved', desired_state: 'paused', folders: [], actions: {
      start: { allowed: false, reason: '任务已结束' }, pause: { allowed: false, reason: '任务已结束' },
    } }, null, true, { controls: true });
  await flush(); await page.selectTask();
  assert.equal(page.element('start-button').disabled, true);
  assert.equal(page.element('pause-button').disabled, true);
  assert.match(page.element('control-availability').textContent, /任务已结束/);
});

test('start stays disabled until the updated gateway control service is loaded', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true,
  { controls: true, gatewayReady: false, controlResponse: async () => reply({}) });
  await flush(); await page.selectTask();
  assert.equal(page.element('start-button').disabled, true);
  assert.match(page.element('control-availability').textContent, /等待.*国内账号/);
  await page.element('start-button').click();
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 0);
});

test('missing action permissions do not prevent displaying counts and do not enable controls', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', actions: null, folders: [folder('1', 'waiting_llm')] }, null, true, { controls: true });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-llm-count').textContent, '1');
  assert.equal(page.element('start-button').disabled, true);
  assert.equal(page.element('pause-button').disabled, true);
  assert.match(page.element('control-availability').textContent, /待后端返回/);
});

test('start and pause address only the selected parent task', async () => {
  const current = { as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] };
  const operations = new Map();
  let sequence = 0;
  const page = makePage(current, null, true, { controls: true,
    randomUUID: () => `request-${++sequence}`,
    controlResponse: async (url, options) => {
      if (url.includes('/operations/')) return reply(operations.get(url.split('/').at(-1)));
      if (url.endsWith('/start')) Object.assign(current, { task_state: 'running', desired_state: 'running' });
      if (url.endsWith('/pause')) Object.assign(current, { task_state: 'paused', desired_state: 'paused' });
      const request = JSON.parse(options.body);
      const operation = { ...request, action: url.split('/').at(-1), target: { task_id: 'task-1' },
        accepted: true, found: true, resolved: true, status: 'confirmed' };
      operations.set(request.request_id, operation);
      return reply(operation);
    } });
  await flush(); await page.selectTask();
  assert.equal(page.readRequests.find((request) => request.url.endsWith('/classifications')).options.signal.milliseconds, 120000);
  assert.equal(page.element('start-button').disabled, false);
  assert.equal(page.element('pause-button').disabled, true);
  await page.element('start-button').click();
  assert.equal(page.element('start-button').disabled, true);
  assert.equal(page.element('pause-button').disabled, false);
  await page.element('pause-button').click();
  const posts = page.controlRequests.filter((r) => r.options?.method === 'POST');
  assert.deepEqual(posts.map((r) => r.url), [
    'https://example.test/control/tasks/task-1/start',
    'https://example.test/control/tasks/task-1/pause',
  ]);
  assert.equal(posts[0].options.signal.milliseconds, 120000);
  assert.equal(posts[1].options.signal.milliseconds, 8000);
  assert.equal(page.element('start-button').disabled, false);
});

test('start shows failed dependency details returned by the backend', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, { controls: true,
    controlResponse: async () => reply({ detail: preflight(false) }, 409) });
  await flush(); await page.selectTask();
  await page.element('start-button').click();
  assert.match(page.element('control-message').textContent, /RAG 未就绪/);
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
});

test('a rejected start recovered after a timeout displays its saved preflight failure', async () => {
  const requestId = '11111111-1111-4111-8111-111111111111';
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, { controls: true,
    controlResponse: async (_url, options) => {
      if (options?.method === 'POST') throw new Error('response timed out');
      return reply({ request_id: requestId, action: 'start', target: { task_id: 'task-1' }, intent_seq: 1,
        found: true, resolved: true, status: 'rejected',
        failed_checks: [{ name: 'rag', ready: false, detail: 'RAG 未就绪' }] });
    } });
  await flush(); await page.selectTask();
  await page.element('start-button').click();
  assert.match(page.element('control-message').textContent, /RAG 未就绪/);
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
});

test('an unknown control outcome is not resent and is reported for review', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, { controls: true,
    controlResponse: async () => { throw new Error('connection lost'); } });
  await flush(); await page.selectTask();
  await page.element('start-button').click();
  assert.match(page.element('control-message').textContent, /待核对/);
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
});

test('a pending start displays backend account testing progress without posting again', async () => {
  let finishStart;
  const pendingStart = new Promise((resolve) => { finishStart = resolve; });
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, { controls: true,
    controlResponse: async (url) => url.endsWith('/start') ? pendingStart : reply({
      request_id: '11111111-1111-4111-8111-111111111111', action: 'start', target: { task_id: 'task-1' },
      intent_seq: 1, found: true, resolved: false, status: 'accepted',
      message: '正在测试国内账号，本次尚未开始处理',
    }),
  });
  await flush(); await page.selectTask();
  const start = page.element('start-button').click();
  await flush(); await page.element('refresh-task-button').click(); await flush();
  assert.match(page.element('control-message').textContent, /正在测试国内账号/);
  assert.equal(page.element('start-button').disabled, true);
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
  finishStart(reply({ accepted: true, request_id: '11111111-1111-4111-8111-111111111111' }));
  await start;
});

test('a pending start keeps its identity and permits a later pause with a higher intent sequence', async () => {
  let finishStart;
  const pendingStart = new Promise((resolve) => { finishStart = resolve; });
  let serial = 0;
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, { controls: true,
    randomUUID: () => `request-${++serial}`,
    controlResponse: async (url, options) => {
      if (url.endsWith('/start')) return pendingStart;
      if (url.endsWith('/pause')) return reply({ accepted: true, request_id: JSON.parse(options.body).request_id });
      return reply({ found: false, resolved: false, status: 'unknown' });
    },
  });
  await flush(); await page.selectTask();
  const start = page.element('start-button').click();
  await flush();
  assert.equal(page.element('pause-button').disabled, false, 'pause remains available while start is unresolved');
  await page.element('pause-button').click();
  const posts = page.controlRequests.filter((request) => request.options?.method === 'POST');
  assert.equal(posts.length, 2);
  const first = JSON.parse(posts[0].options.body);
  const second = JSON.parse(posts[1].options.body);
  assert.equal(first.request_id, 'request-1');
  assert.equal(second.request_id, 'request-2');
  assert.ok(second.intent_seq > first.intent_seq);
  finishStart(reply({ accepted: true, request_id: first.request_id }));
  await start;
  assert.match(page.element('control-message').textContent, /暂停/, 'late start must not overwrite the newer pause');
});

test('after reopening, an unknown control is checked by its original request id and never posted again', async () => {
  const storage = new Map();
  const snapshot = { as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] };
  const first = makePage(snapshot, null, true, { controls: true, storage,
    controlResponse: async () => { throw new Error('connection lost'); },
  });
  await flush(); await first.selectTask();
  await first.element('start-button').click();
  const requestId = JSON.parse(first.controlRequests.find((request) => request.options?.method === 'POST').options.body).request_id;
  const second = makePage(snapshot, null, true, { controls: true, storage,
    controlResponse: async (url) => reply({ request_id: requestId, found: false, resolved: false, status: 'unknown' }),
  });
  await flush(); await second.selectTask(); await flush();
  assert.ok(second.readRequests.some((request) => request.url.endsWith(`/operations/${requestId}`)));
  assert.equal(second.element('start-button').disabled, true);
  assert.equal(second.controlRequests.filter((request) => request.options?.method === 'POST').length, 0);
});

test('a confirmed start may already have paused before the next fresh snapshot', async () => {
  let current = { as_of: '2026-11-01T12:00:00Z', complete: true, has_pending: true,
    task_state: 'paused', desired_state: 'paused', folders: [] };
  const page = makePage(null, null, true, { controls: true,
    snapshotResponse: async () => current,
    controlResponse: async (_url, options) => options?.method === 'POST'
      ? reply({ accepted: true, request_id: '11111111-1111-4111-8111-111111111111' })
      : reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'start',
        target: { task_id: 'task-1' }, intent_seq: 1, found: true, resolved: true, status: 'confirmed' }),
  });
  await flush(); await page.selectTask(); await page.element('start-button').click();
  assert.equal(page.element('start-button').disabled, false, 'the fresh backend pause permits an explicit later start');
  assert.equal(page.element('pause-button').disabled, true);
  current = { ...current, task_state: 'running', desired_state: 'running', as_of: '2026-11-01T12:00:10Z' };
  page.element('refresh-task-button').click(); await flush(); await flush();
  assert.equal(page.element('start-button').disabled, true);
  assert.equal(page.element('pause-button').disabled, false);
});

test('a confirmed start can later become paused after a dependency failure', async () => {
  let current = { as_of: '2026-11-01T12:00:00Z', complete: true, has_pending: true,
    task_state: 'paused', desired_state: 'paused', folders: [] };
  const page = makePage(null, null, true, { controls: true,
    snapshotResponse: async () => current,
    controlResponse: async (_url, options) => {
      if (options?.method === 'POST') {
        current = { ...current, task_state: 'running', desired_state: 'running' };
        return reply({ accepted: true, request_id: '11111111-1111-4111-8111-111111111111' });
      }
      return reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'start',
        target: { task_id: 'task-1' }, intent_seq: 1, found: true, resolved: true, status: 'confirmed' });
    },
  });
  await flush(); await page.selectTask(); await page.element('start-button').click();
  assert.equal(page.element('task-state').textContent, '运行中');
  current = { ...current, task_state: 'paused', desired_state: 'paused', stop_reason: '共享服务认证失败' };
  await page.element('refresh-task-button').click(); await flush();
  assert.equal(page.element('task-state').textContent, '已暂停');
  assert.match(page.element('task-detail').textContent, /共享服务认证失败/);
  assert.equal(page.element('start-button').disabled, false);
});

test('a confirmed later pause cannot be submitted again because an older start is unresolved', async () => {
  let finishStart;
  const slowStart = new Promise((resolve) => { finishStart = resolve; });
  let serial = 0;
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, has_pending: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, { controls: true,
    randomUUID: () => `request-${++serial}`,
    controlResponse: async (url, options) => {
      if (url.endsWith('/start')) return slowStart;
      if (url.endsWith('/pause')) return reply({ accepted: true, request_id: 'request-2' });
      if (url.endsWith('/operations/request-2')) return reply({ request_id: 'request-2', action: 'pause',
        target: { task_id: 'task-1' }, intent_seq: 2, found: true, resolved: true, status: 'confirmed' });
      return reply({ request_id: 'request-1', action: 'start', target: { task_id: 'task-1' },
        intent_seq: 1, found: true, resolved: false, status: 'accepted' });
    },
  });
  await flush(); await page.selectTask();
  const start = page.element('start-button').click(); await flush();
  await page.element('pause-button').click();
  assert.equal(page.element('pause-button').disabled, true);
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 2);
  finishStart(reply({ accepted: true, request_id: 'request-1' })); await start;
  assert.equal(page.element('pause-button').disabled, true);
});

test('accepted pause shows folder drainage until the backend confirms every folder settled', async () => {
  let settled = false;
  let current = { as_of: '2026-11-04T12:00:00Z', complete: true, has_pending: true,
    task_state: 'running', desired_state: 'running', folders: [] };
  const page = makePage(null, null, true, { controls: true,
    snapshotResponse: async () => current,
    controlResponse: async (url, options) => {
      const requestId = options?.method === 'POST' ? JSON.parse(options.body).request_id : url.split('/').pop();
      return reply({ request_id: requestId, action: 'pause', target: { task_id: 'task-1' }, intent_seq: 1,
        found: true, accepted: true, status: settled ? 'confirmed' : 'accepted', resolved: settled,
        pause_state: settled ? 'paused' : 'pausing' });
    },
  });
  await flush(); await page.selectTask();
  current = { ...current, desired_state: 'paused', task_state: 'pausing' };
  await page.element('pause-button').click(); await flush();
  assert.match(page.element('task-state').textContent, /暂停中/);
  assert.doesNotMatch(page.element('task-state').textContent, /已暂停/);
  settled = true;
  current = { ...current, task_state: 'paused' };
  await page.element('refresh-task-button').click(); await flush();
  assert.equal(page.element('task-state').textContent, '已暂停');
  assert.equal(page.controlRequests.filter((item) => item.options?.method === 'POST').length, 1);
});

test('a control request is not sent if its identity cannot be stored', async () => {
  class RefusingStorage extends Map {
    set(key, value) {
      if (key.startsWith('paperflow-control:')) throw new Error('storage unavailable');
      return super.set(key, value);
    }
  }
  const storage = new RefusingStorage();
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, { controls: true, storage,
    controlResponse: async () => reply({ accepted: true }),
  });
  await flush(); await page.selectTask();
  await page.element('start-button').click();
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 0);
  assert.match(page.element('control-message').textContent, /未保存.*无法提交/);
});

test('known offline state blocks a new pause even while an earlier start is pending', async () => {
  let finishStart;
  const pendingStart = new Promise((resolve) => { finishStart = resolve; });
  const network = { onLine: true };
  let serial = 0;
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    task_state: 'paused', desired_state: 'paused', folders: [] }, null, true, {
    controls: true, navigator: network, randomUUID: () => `request-${++serial}`,
    controlResponse: async (url) => url.endsWith('/start') ? pendingStart : reply({ found: false, resolved: false }),
  });
  await flush(); await page.selectTask();
  const start = page.element('start-button').click(); await flush();
  network.onLine = false;
  await page.element('pause-button').click();
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
  assert.match(page.element('control-message').textContent, /离线|连接不可用/);
  finishStart(reply({ accepted: true, request_id: 'request-1' })); await start;
});

test('a delayed pre-pause running snapshot cannot undo a confirmed later pause', async () => {
  let finishStart;
  let serial = 0;
  const slowStart = new Promise((resolve) => { finishStart = resolve; });
  let current = { as_of: '2026-11-01T12:00:00Z', complete: true, has_pending: true,
    task_state: 'paused', desired_state: 'paused', folders: [] };
  const page = makePage(null, null, true, { controls: true, randomUUID: () => `request-${++serial}`,
    snapshotResponse: async () => current,
    controlResponse: async (url) => {
      if (url.endsWith('/start')) return slowStart;
      if (url.endsWith('/pause')) return reply({ accepted: true, request_id: 'request-2' });
      if (url.endsWith('/operations/request-2')) return reply({ request_id: 'request-2', action: 'pause',
        target: { task_id: 'task-1' }, intent_seq: 2, found: true, resolved: true, status: 'confirmed' });
      return reply({ request_id: 'request-1', action: 'start', target: { task_id: 'task-1' },
        intent_seq: 1, found: true, resolved: true, status: 'superseded' });
    },
  });
  await flush(); await page.selectTask();
  const start = page.element('start-button').click(); await flush();
  await page.element('pause-button').click();
  current = { ...current, task_state: 'running', desired_state: 'running' };
  finishStart(reply({ accepted: true, request_id: 'request-1' })); await start; await flush();
  assert.match(page.element('task-state').textContent, /暂停/);
  assert.equal(page.element('pause-button').disabled, true);
  assert.equal(page.element('start-button').disabled, true);
});

test('a stale HTTP 200 retains the original result time and shows that refresh failed', async () => {
  const saved = { as_of: '2026-11-01T12:00:00Z', complete: true, has_pending: true,
    query_status: 'updated', folders: [folder('1', 'waiting_llm')] };
  const page = makePage(saved, { ...saved, query_status: 'stale' });
  await flush(); await page.selectTask();
  const originalHint = page.element('folder-ticket-overview').textContent;
  page.timers[0](); await flush();
  const staleHint = page.element('folder-ticket-overview').textContent;
  assert.equal(page.element('waiting-llm-count').textContent, '1');
  assert.match(staleHint, /更新失败.*上次数据/);
  assert.match(staleHint, /2026/);
  assert.ok(originalHint.includes(staleHint.split('上次数据 ')[1]));
});

test('preparing history with no saved timestamp is shown as pending rather than zero or invalid data', async () => {
  const page = makePage({ as_of: null, complete: false, has_pending: true,
    query_status: 'preparing_history', detail: '正在准备历史记录', folders: [] });
  await flush(); await page.selectTask();
  assert.match(page.element('folder-ticket-overview').textContent, /正在准备历史记录/);
  assert.notEqual(page.element('task-total').textContent, '0');
  assert.doesNotMatch(page.element('folder-ticket-overview').textContent, /Invalid Date/);
});

test('a newly created task with updated or reused status may have no recorded timestamp', async () => {
  const page = makePage({ as_of: null, complete: false, has_pending: true,
    query_status: 'updated', task_state: 'created', desired_state: 'paused',
    folders: [folder('1', 'waiting_preprocessing')] }, null, true, { controls: true,
    classificationResponse: async (taskId) => ({ task_id: taskId, as_of: null, complete: false,
      has_pending: true, query_status: 'reused', items: [] }),
  });
  await flush(); await page.selectTask();
  assert.equal(page.element('task-state').textContent, '未开始');
  assert.equal(page.element('waiting-preprocessing-count').textContent, '≥1');
  assert.match(page.element('folder-ticket-overview').textContent, /尚无已保存记录/);
  assert.match(page.element('classification-results-hint').textContent, /尚无已保存记录/);
  assert.notEqual(page.element('task-total').textContent, '0');
});

test('an accepted retry retains its request identity until that exact operation is confirmed', async () => {
  const storage = new Map();
  const key = 'paperflow-pending-retry:task-1:folder-1';
  let savedBeforePost;
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true,
      last_retry: { retry_id: 'old-retry', stage: 'rag', status: 'failed' } }),
  ] }, null, true, { controls: true, storage,
    controlResponse: async (url, options) => {
      if (options?.method === 'POST') {
        savedBeforePost = storage.get(key);
        return reply({ accepted: true, request_id: '11111111-1111-4111-8111-111111111111',
          task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1', retry_id: 'new-retry', status: 'scheduled' });
      }
      return reply({ request_id: '11111111-1111-4111-8111-111111111111', found: false, resolved: false, status: 'unknown' });
    },
  });
  await flush(); await page.selectTask(); page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  const post = page.controlRequests.find((request) => request.options?.method === 'POST');
  assert.equal(JSON.parse(post.options.body).request_id, '11111111-1111-4111-8111-111111111111');
  assert.equal(JSON.parse(savedBeforePost).request_id, '11111111-1111-4111-8111-111111111111');
  assert.ok(storage.has(key));
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.ok(page.readRequests.some((request) => request.url.endsWith('/operations/11111111-1111-4111-8111-111111111111')));
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
  assert.ok(storage.has(key));
});

test('retry confirmation requires the same retry id in a fresh ticket snapshot', async () => {
  const key = 'paperflow-pending-retry:task-1:folder-1';
  let reportedRetry = 'another-retry';
  let current = { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true,
      last_retry: { retry_id: 'old-retry', stage: 'rag', status: 'failed' } }),
  ] };
  const page = makePage(null, null, true, { controls: true, snapshotResponse: async () => current,
    controlResponse: async (_url, options) => options?.method === 'POST'
      ? reply({ accepted: true, request_id: '11111111-1111-4111-8111-111111111111', retry_id: 'new-retry' })
      : reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'retry',
        target: { task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1' }, found: true, resolved: true,
        status: 'confirmed', retry_id: reportedRetry }),
  });
  await flush(); await page.selectTask(); page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.ok(page.storage.has(key), 'another retry cannot confirm this request');
  reportedRetry = 'new-retry';
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.ok(page.storage.has(key), 'an old failure snapshot cannot release the new retry');
  current = { ...current, folders: [folder('1', 'waiting_rag_upload', {
    last_retry: { retry_id: 'new-retry', stage: 'rag', status: 'running' },
  })] };
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(page.storage.has(key), false);
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
});

test('reopening a pending retry checks its original operation without another submission', async () => {
  const key = 'paperflow-pending-retry:task-1:folder-1';
  const storage = new Map([[key, JSON.stringify({ request_id: 'retry-request', task_id: 'task-1',
    folder_id: 'folder-1', ticket_id: '1', previous_retry_id: 'old-retry', retry_id: null })]]);
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true }),
  ] }, null, true, { controls: true, storage,
    controlResponse: async () => reply({ request_id: 'retry-request', found: false, resolved: false, status: 'unknown' }),
  });
  await flush(); await page.selectTask(); await flush();
  assert.ok(page.readRequests.some((request) => request.url.endsWith('/operations/retry-request')));
  assert.ok(storage.has(key));
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 0);
});

test('an older attempt and a late interim event cannot overwrite a saved final stage result', async () => {
  const finalStage = { processing_attempt_id: 'attempt-new', result_version: 'artifact-1',
    recorded_at: '2026-11-01T12:02:00Z', final: true, pending_request_count: 0, result: 'passed' };
  let current = { as_of: '2026-11-01T12:02:00Z', complete: true, has_pending: true,
    folders: [folder('1', 'waiting_llm', { preprocessing_result: 'passed', stage_snapshots: { preprocessing: finalStage } })] };
  const page = makePage(null, null, true, { snapshotResponse: async () => current });
  await flush(); await page.selectTask();
  assert.equal(page.element('preprocessing-passed-count').textContent, '1');
  current = { ...current, as_of: '2026-11-01T12:03:00Z', folders: [folder('1', 'waiting_llm', {
    preprocessing_result: null, stage_snapshots: { preprocessing: { ...finalStage, result_version: 'artifact-2',
      recorded_at: '2026-11-01T12:01:30Z', final: false, pending_request_count: 1, result: null } },
  })] };
  page.element('refresh-task-button').click(); await flush();
  assert.equal(page.element('preprocessing-passed-count').textContent, '1', 'same-attempt interim state cannot replace final');
  current = { ...current, folders: [folder('1', 'waiting_llm', {
    preprocessing_result: 'failed', stage_snapshots: { preprocessing: { ...finalStage, processing_attempt_id: 'attempt-old',
      result_version: 'artifact-99', recorded_at: '2026-11-01T12:01:00Z', result: 'failed' } },
  })] };
  page.element('refresh-task-button').click(); await flush();
  assert.equal(page.element('preprocessing-passed-count').textContent, '1', 'older attempt cannot replace the current result');
  assert.equal(page.element('preprocessing-failed-count').textContent, '0');
});

test('new pending evidence does not erase a prior result or turn the pending count into zero', async () => {
  const finalStage = { processing_attempt_id: 'attempt-1', result_version: 'artifact-1',
    recorded_at: '2026-11-01T12:00:00Z', final: true, pending_request_count: 0, result: 'passed' };
  let current = { as_of: '2026-11-01T12:00:00Z', complete: true, has_pending: true,
    folders: [folder('1', 'waiting_llm', { preprocessing_result: 'passed', stage_snapshots: { preprocessing: finalStage } })] };
  const page = makePage(null, null, true, { snapshotResponse: async () => current });
  await flush(); await page.selectTask();
  current = { ...current, as_of: '2026-11-01T12:01:00Z', folders: [folder('1', 'waiting_llm', {
    preprocessing_result: null, stage_snapshots: { preprocessing: { ...finalStage, result_version: 'artifact-2',
      recorded_at: '2026-11-01T12:01:00Z', final: false, pending_request_count: 1, result: null } },
  })] };
  await page.element('refresh-task-button').click(); await flush();
  await new Promise((resolve) => setTimeout(resolve, 20));
  const persisted = JSON.parse(page.storage.get('paperflow-query-cache-v1'));
  const detail = persisted.clientState.queries.find((query) => query.queryKey[2] === 'folders').state.data;
  assert.equal(detail.folders[0].stage_snapshots.preprocessing.pending_request_count, 1);
  assert.equal(detail.folders[0].stage_snapshots.preprocessing.final, false);
  assert.equal(page.element('preprocessing-passed-count').textContent, '≥1');
  assert.match(page.element('preprocessing-results-hint').textContent, /待核对/);
});

test('reopening without configuration or API restores the selected task and its last successful result', async () => {
  const storage = new Map();
  const snapshot = { as_of: '2026-11-01T12:00:00Z', complete: true, has_pending: true,
    query_status: 'updated', folders: [folder('1', 'waiting_llm')] };
  const first = makePage(snapshot, null, true, { controls: true, storage });
  await flush(); await first.selectTask(); await new Promise((resolve) => setTimeout(resolve, 20));
  const second = makePage(null, null, true, { controls: true, storage, configFailure: true,
    snapshotResponse: async () => { throw new Error('offline'); },
    classificationResponse: async () => { throw new Error('offline'); },
  });
  await flush(); await flush(); await flush();
  assert.equal(second.element('task-name').textContent, '任务一');
  assert.equal(second.element('waiting-llm-count').textContent, '1');
  assert.match(second.element('folder-ticket-overview').textContent, /上次数据/);
  assert.match(second.element('folder-ticket-overview').textContent, /2026/);
});

test('when the task list cannot be refreshed the last successful list is still available', async () => {
  const storage = new Map();
  const first = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [] }, null, true, { storage });
  await flush(); await first.selectTask(); await new Promise((resolve) => setTimeout(resolve, 20));
  const reopened = makePage(null, null, true, { storage, taskListFailure: true, configFailure: true,
    snapshotResponse: async () => { throw new Error('offline'); },
  });
  await flush(); await flush();
  await reopened.element('open-task-button').click(); await flush();
  assert.equal(reopened.element('task-list').children[0].children[0].dataset.taskId, 'task-1');
  assert.match(reopened.element('task-list-message').textContent, /上次|未更新/);
});

test('an unreadable persisted cache is not removed or overwritten by later queries', async () => {
  const original = '{not-json';
  const storage = new Map([['paperflow-query-cache-v1', original]]);
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [] }, null, true, { storage });
  await flush(); await page.selectTask(); await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(storage.get('paperflow-query-cache-v1'), original);
  assert.match(page.element('save-state').textContent, /无法恢复|未保存/);
});

test('a late creation result does not replace a task selected afterward', async () => {
  let finishCreate;
  const delayed = new Promise((resolve) => { finishCreate = resolve; });
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] }, null, true, {
    controls: true, tasks: [{ task_id: 'task-1', task_name: '任务一' }, { task_id: 'task-2', task_name: '任务二' }],
    controlResponse: async (url) => url.endsWith('/preflight') ? reply(preflight(true)) : delayed,
  });
  await flush(); page.element('new-task-button').click(); await flush();
  page.element('new-task-name').value = '新建任务';
  page.element('new-task-paths').value = 'F:\\fake';
  const creation = page.element('new-task-form').submit(); await flush();
  await page.selectTask('task-2');
  finishCreate(reply({ request_id: '11111111-1111-4111-8111-111111111111',
    accepted: true, status: 'confirmed', task_id: 'new-task', task_name: '新建任务' }));
  await creation; await flush();
  assert.equal(page.element('task-name').textContent, '任务二');
  assert.equal(page.storage.has('paperflow-pending-task-create'), false);
});

test('task creation does not send when its request identity cannot be saved', async () => {
  class RefusingStorage extends Map {
    set(key, value) {
      if (key === 'paperflow-pending-task-create') throw new Error('storage unavailable');
      return super.set(key, value);
    }
  }
  const page = makePage(null, null, true, { controls: true, storage: new RefusingStorage(),
    controlResponse: async (url) => url.endsWith('/preflight') ? reply(preflight(true)) : reply({ accepted: true }),
  });
  await flush(); page.element('new-task-button').click(); await flush();
  page.element('new-task-name').value = '隔离测试';
  page.element('new-task-paths').value = 'F:\\fake';
  await page.element('new-task-form').submit();
  assert.equal(page.controlRequests.filter((request) => request.url.endsWith('/tasks')).length, 0);
  assert.match(page.element('new-task-message').textContent, /未保存.*无法创建/);
});

test('first-step classifications include folders without tickets and keep unknown totals distinct', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] }, null, true, {
    controls: true,
    classificationResponse: async (taskId) => ({ task_id: taskId, as_of: '2026-11-01T12:00:00Z', complete: false, items: [
      { group_id: 'group-1', folder_name: '资料甲', paper_title: '论文甲', status: 'passed', reason: null },
      { group_id: 'group-2', folder_name: '资料乙', paper_title: null, status: 'failed', reason: '缺少正文' },
      { group_id: 'group-3', folder_name: '资料丙', paper_title: null, status: 'unresolved', reason: null },
    ] }),
  });
  await flush(); await page.selectTask();
  assert.equal(page.element('passed-count').textContent, '≥1');
  assert.equal(page.element('failed-count').textContent, '≥1');
  assert.equal(page.element('unrecognized-count').textContent, '待核对');
  assert.equal(page.element('unknown-count').textContent, '≥1');
  page.buttons.get('classification')[1].click();
  assert.equal(page.element('list-panel').hidden, false);
  const row = page.element('group-list').children[0];
  assert.match(row.children[0].children[1].textContent, /资料乙/);
  assert.match(row.children[0].children[3].textContent, /缺少正文/);
});

test('late first-step results cannot overwrite the selected task', async () => {
  let resolveA;
  const delayedA = new Promise((resolve) => { resolveA = resolve; });
  const page = makePage(null, null, true, {
    controls: true,
    tasks: [{ task_id: 'A', task_name: '任务甲' }, { task_id: 'B', task_name: '任务乙' }],
    snapshots: {
      A: { as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] },
      B: { as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] },
    },
    classificationResponse: async (taskId) => taskId === 'A' ? delayedA : ({
      task_id: 'B', as_of: '2026-11-01T12:00:00Z', complete: true,
      items: [{ group_id: 'b1', folder_name: '乙', paper_title: null, status: 'failed', reason: null }],
    }),
  });
  await flush(); await page.selectTask('A'); await page.selectTask('B');
  assert.equal(page.element('failed-count').textContent, '1');
  resolveA({ task_id: 'A', as_of: '2026-11-01T12:00:00Z', complete: true,
    items: [{ group_id: 'a1', folder_name: '甲', paper_title: null, status: 'passed', reason: null }] });
  await flush();
  assert.equal(page.element('failed-count').textContent, '1');
  assert.equal(page.element('passed-count').textContent, '0');
});

test('an older first-step classification refresh or error cannot overwrite a newer one for the same task', async () => {
  let resolveOlder, rejectOlder;
  const older = new Promise((resolve) => { resolveOlder = resolve; });
  const olderError = new Promise((_, reject) => { rejectOlder = reject; });
  let calls = 0;
  const first = { task_id: 'task-1', as_of: '2026-11-01T12:00:00Z', complete: true,
    items: [{ group_id: 'g1', folder_name: '甲', paper_title: null, status: 'passed', reason: null }] };
  const newer = { ...first, as_of: '2026-11-01T12:01:00Z', items: [...first.items,
    { group_id: 'g2', folder_name: '乙', paper_title: null, status: 'passed', reason: null }] };
  const page = makePage({ as_of: first.as_of, complete: true, folders: [] }, null, true, {
    controls: true, classificationResponse: async () => ++calls === 1 ? first : calls === 2 ? older : calls === 4 ? olderError : newer,
  });
  await flush(); await page.selectTask();
  page.timers[0](); page.timers[0](); await flush();
  assert.equal(calls, 2, 'the polling reads share the in-flight classification request');
  await page.element('refresh-task-button').click(); await flush();
  assert.equal(page.element('passed-count').textContent, '2');
  const newestHint = page.element('classification-results-hint').textContent;
  resolveOlder(first); await flush();
  assert.equal(page.element('passed-count').textContent, '2');
  assert.equal(page.element('classification-results-hint').textContent, newestHint);
  page.timers[0](); await flush();
  await page.element('refresh-task-button').click(); await flush();
  rejectOlder(new Error('old classification request failed')); await flush();
  assert.equal(page.element('classification-results-hint').textContent, newestHint);
});

test('a backend-approved failed folder is retried only after user confirmation', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true, retry_reason: null }),
  ] }, null, true, { controls: true, controlResponse: async (_url, options) => options?.method === 'POST'
    ? reply({ accepted: true, request_id: '11111111-1111-4111-8111-111111111111',
      task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1', retry_id: 'retry-1' })
    : reply({ request_id: '11111111-1111-4111-8111-111111111111', found: false, resolved: false, status: 'unknown' }) });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  const retry = page.element('failed-folders-list').children[0].children[1];
  assert.equal(retry.disabled, false);
  await retry.click();
  const posts = page.controlRequests.filter((request) => request.options?.method === 'POST');
  assert.equal(posts.length, 1);
  assert.match(posts[0].url, /\/tasks\/task-1\/folders\/folder-1\/retry$/);
  assert.equal(posts[0].options.signal.milliseconds, 120000);
  assert.ok(page.storage.has('paperflow-pending-retry:task-1:folder-1'));

  const declined = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('2', 'failed', { failed_stage: 'rag', retryable: true }),
  ] }, null, true, { controls: true, confirmRetry: false, controlResponse: async () => reply({ ok: true }) });
  await flush(); await declined.selectTask();
  declined.element('failed-folders-button').click();
  await declined.element('failed-folders-list').children[0].children[1].click();
  assert.equal(declined.controlRequests.length, 0);
});

test('an unknown retry outcome remains available for lookup without automatic resending', async () => {
  const storage = new Map();
  const snapshot = { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true }),
  ] };
  const options = { controls: true, storage, controlResponse: async () => { throw new Error('connection lost'); } };
  const page = makePage(snapshot, null, true, options);
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
  assert.equal(JSON.parse(storage.get('paperflow-pending-retry:task-1:folder-1')).ticket_id, '1');
  page.timers[0](); await flush();
  assert.equal(page.element('failed-folders-list').children[0].children[1].textContent, '核对重试结果');
  const reopened = makePage(snapshot, null, true, options);
  await flush(); await reopened.selectTask();
  reopened.element('failed-folders-button').click();
  assert.equal(reopened.element('failed-folders-list').children[0].children[1].textContent, '核对重试结果');
  assert.equal(reopened.controlRequests.filter((request) => request.options?.method === 'POST').length, 0);
});

test('a successful retry clears the displayed failure when the same ticket advances', async () => {
  const current = { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'postprocessing', postprocessing_result: 'failed', retryable: true }),
  ] };
  const page = makePage(current, null, true, { controls: true, controlResponse: async (_url, options) => {
    if (options?.method === 'POST') {
      current.folders = [folder('1', 'waiting_rag_upload', { llm_result: 'passed', postprocessing_result: 'passed',
        last_retry: { retry_id: 'retry-1', stage: 'postprocessing', status: 'running' } })];
      return reply({ accepted: true, request_id: '11111111-1111-4111-8111-111111111111', retry_id: 'retry-1' });
    }
    return reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'retry',
      target: { task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1' }, found: true, resolved: true,
      status: 'confirmed', retry_id: 'retry-1' });
  } });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  page.timers[0](); await flush();
  assert.equal(page.element('failed-folders-button').hidden, true);
  assert.equal(page.element('failed-folders-list').children.length, 0);
  assert.equal(page.element('waiting-rag-upload-count').textContent, '1');
  assert.equal(page.element('postprocessing-passed-count').textContent, '1');
  assert.equal(page.element('task-total').textContent, '1');
});

test('an uncertain retry dispatch response retains the pending marker and shows the reason', async () => {
  const storage = new Map();
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true }),
  ] }, null, true, { controls: true, storage, controlResponse: async () =>
    reply({ detail: '原论文工单重试派发结果待核对' }, 409) });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  const row = page.element('failed-folders-list').children[0];
  await row.children[1].click();
  assert.equal(JSON.parse(storage.get('paperflow-pending-retry:task-1:folder-1')).ticket_id, '1');
  assert.match(row.children[0].children[4].textContent, /派发结果待核对/);
  page.timers[0](); await flush();
  assert.equal(page.element('failed-folders-list').children[0].children[1].textContent, '核对重试结果');
});

test('a retry success response for a different ticket does not clear the pending marker', async () => {
  const storage = new Map();
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true }),
  ] }, null, true, { controls: true, storage, controlResponse: async () => reply({
    task_id: 'task-1', folder_id: 'folder-1', ticket_id: 'another-ticket', retry_id: 'retry-1', status: 'scheduled',
  }) });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  const row = page.element('failed-folders-list').children[0];
  await row.children[1].click();
  assert.equal(JSON.parse(storage.get('paperflow-pending-retry:task-1:folder-1')).ticket_id, '1');
  assert.match(row.children[0].children[4].textContent, /待核对/);
});

test('a rejected retry preflight displays the failure and permits a later user decision', async () => {
  const storage = new Map();
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true }),
  ] }, null, true, { controls: true, storage, controlResponse: async (_url, options) => options?.method === 'POST'
    ? reply({ detail: preflight(false) }, 409)
    : reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'retry',
      target: { task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1' }, found: true, resolved: true, status: 'rejected' }) });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  const row = page.element('failed-folders-list').children[0];
  await row.children[1].click();
  assert.equal(storage.has('paperflow-pending-retry:task-1:folder-1'), false);
  assert.match(row.children[0].children[4].textContent, /RAG 未就绪/);
});

test('a retry preflight failure remains visible after a polling refresh while it was pending', async () => {
  let resolveRetry;
  const retryResponse = new Promise((resolve) => { resolveRetry = resolve; });
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true }),
  ] }, null, true, { controls: true, controlResponse: async () => retryResponse });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  const request = page.element('failed-folders-list').children[0].children[1].click();
  page.timers[0](); await flush();
  resolveRetry(reply({ detail: preflight(false) }, 409));
  await request; await flush();
  assert.match(page.element('failed-folders-list').children[0].children[0].children[4].textContent, /RAG 未就绪/);
  page.timers[0](); await flush();
  assert.match(page.element('failed-folders-list').children[0].children[0].children[4].textContent, /RAG 未就绪/);
});

test('retry lookup retains an old success and only clears after the same ticket has a new confirmed retry', async () => {
  const storage = new Map();
  let confirmed = false;
  const current = { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true,
      last_retry: { retry_id: 'old-retry', stage: 'rag', status: 'completed' } }),
  ] };
  const page = makePage(current, null, true, { controls: true, storage, controlResponse: async (_url, options) => {
    if (options?.method === 'POST') throw new Error('timeout');
    return confirmed ? reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'retry',
      target: { task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1' }, found: true, resolved: true,
      status: 'confirmed', retry_id: 'new-retry' }) : reply({ found: false, resolved: false, status: 'unknown' });
  } });
  await flush(); await page.selectTask();
  page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(JSON.parse(storage.get('paperflow-pending-retry:task-1:folder-1')).previous_retry_id, 'old-retry');
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(JSON.parse(storage.get('paperflow-pending-retry:task-1:folder-1')).previous_retry_id, 'old-retry');
  assert.match(page.element('failed-folders-message').textContent, /待核对/);
  current.folders = [folder('1', 'llm_in_progress', { llm_body_running: true,
    last_retry: { retry_id: 'new-retry', stage: 'rag', status: 'running' } })];
  page.timers[0](); await flush();
  assert.equal(page.element('failed-folders-button').hidden, false);
  confirmed = true;
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(storage.has('paperflow-pending-retry:task-1:folder-1'), false);
  assert.match(page.element('failed-folders-message').textContent, /已核对.*处理中/);
  assert.equal(page.controlRequests.filter((request) => request.options?.method === 'POST').length, 1);
});

test('retry lookup keeps an unknown result and does not match another ticket or a legacy marker', async () => {
  const storage = new Map([['paperflow-pending-retry:task-1:folder-1', '1|old-retry|']]);
  const current = { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: false,
      last_retry: { retry_id: 'new-retry', stage: 'rag', status: 'dispatch_unknown' } }),
  ] };
  const page = makePage(current, null, true, { controls: true, storage });
  await flush(); await page.selectTask(); page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.ok(storage.has('paperflow-pending-retry:task-1:folder-1'));
  assert.equal(page.requests.length, 1, 'legacy marker cannot be matched without its original request id');
  current.folders[0].ticket_id = 'another-ticket';
  current.folders[0].last_retry.status = 'completed';
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.ok(storage.has('paperflow-pending-retry:task-1:folder-1'));
  storage.set('paperflow-pending-retry:task-1:folder-1', '1');
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(storage.get('paperflow-pending-retry:task-1:folder-1'), '1');
  assert.equal(page.controlRequests.length, 0);
});

test('a pending retry remains visible for lookup even if its folder is absent from the current snapshot', async () => {
  const storage = new Map([['paperflow-pending-retry:task-1:folder-1', '1|old-retry|']]);
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] }, null, true, { controls: true, storage });
  await flush(); await page.selectTask();
  assert.equal(page.element('failed-folders-button').hidden, false);
  page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(storage.get('paperflow-pending-retry:task-1:folder-1'), '1|old-retry|');
  assert.match(page.element('failed-folders-message').textContent, /未返回|待核对/);
  assert.equal(page.controlRequests.length, 0);
});

test('a pending retry remains visible and read-only when the entire snapshot is unavailable', async () => {
  const storage = new Map([['paperflow-pending-retry:task-1:folder-1', '1|old-retry|']]);
  const page = makePage(null, null, true, { controls: true, storage,
    snapshotResponse: async () => { throw new Error('offline'); } });
  await flush(); await page.selectTask();
  assert.equal(page.element('failed-folders-button').hidden, false);
  page.element('failed-folders-button').click();
  await page.element('failed-folders-list').children[0].children[1].click();
  assert.equal(storage.get('paperflow-pending-retry:task-1:folder-1'), '1|old-retry|');
  assert.match(page.element('failed-folders-message').textContent, /读取失败|待核对/);
  assert.equal(page.requests.length, 1, 'legacy marker does not trigger an unbound read');
  assert.equal(page.controlRequests.length, 0);
});

test('a complete folder snapshot can show exact counts when only the parent status is unresolved', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, task_state: 'unresolved',
    folders: [folder('1', 'waiting_llm')] });
  await flush(); await page.selectTask();
  assert.equal(page.element('waiting-llm-count').textContent, '1');
  assert.equal(page.element('task-state').textContent, '状态待核对');
});

test('structured retry rejection displays its reason while an uncertain dispatch retains its retry identity', async () => {
  const snapshot = { as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'rag', retryable: true }),
  ] };
  const denied = makePage(snapshot, null, true, { controls: true, controlResponse: async (_url, options) => options?.method === 'POST'
    ? reply({ detail: { code: 'retry_not_allowed', message: '失败证据未确认', last_retry: null } }, 409)
    : reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'retry',
      target: { task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1' }, found: true, resolved: true, status: 'rejected' }) });
  await flush(); await denied.selectTask(); denied.element('failed-folders-button').click();
  await denied.element('failed-folders-list').children[0].children[1].click();
  assert.equal(denied.storage.has('paperflow-pending-retry:task-1:folder-1'), false);
  assert.match(denied.element('failed-folders-list').children[0].children[0].children[4].textContent, /失败证据未确认/);
  let confirmed = false;
  const unknown = makePage(snapshot, null, true, { controls: true, controlResponse: async (_url, options) => {
    if (options?.method === 'POST') return reply({ detail: {
      code: 'retry_dispatch_unknown', message: '派发结果待核对', retry_id: 'this-retry',
    } }, 409);
    return confirmed ? reply({ request_id: '11111111-1111-4111-8111-111111111111', action: 'retry',
      target: { task_id: 'task-1', folder_id: 'folder-1', ticket_id: '1' }, found: true, resolved: true,
      status: 'confirmed', retry_id: 'this-retry' }) : reply({ found: false, resolved: false, status: 'unknown' });
  } });
  await flush(); await unknown.selectTask(); unknown.element('failed-folders-button').click();
  await unknown.element('failed-folders-list').children[0].children[1].click();
  assert.equal(JSON.parse(unknown.storage.get('paperflow-pending-retry:task-1:folder-1')).request_id, '11111111-1111-4111-8111-111111111111');
  assert.match(unknown.element('failed-folders-list').children[0].children[0].children[4].textContent, /派发结果待核对/);
  snapshot.folders[0].last_retry = { retry_id: 'another-retry', stage: 'rag', status: 'running' };
  await unknown.element('failed-folders-list').children[0].children[1].click();
  assert.ok(unknown.storage.has('paperflow-pending-retry:task-1:folder-1'));
  snapshot.folders[0].last_retry.retry_id = 'this-retry';
  confirmed = true;
  await unknown.element('failed-folders-list').children[0].children[1].click();
  assert.equal(unknown.storage.has('paperflow-pending-retry:task-1:folder-1'), false);
});

test('actual isolated candidate API responses render as incomplete and block creation when preflight fails',
  { skip: !process.env.PAPERFLOW_CANDIDATE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_CANDIDATE_FIXTURE, 'utf8'));
    const page = makePage(null, null, true, {
      controls: true,
      rawSnapshot: true,
      taskListResponse: fixture.tasks,
      snapshots: { [fixture.created.task_id]: fixture.folders },
      classificationResponse: async () => fixture.classifications,
      controlResponse: async () => reply(fixture.preflight),
    });
    await flush();
    page.element('new-task-button').click(); await flush();
    assert.equal(page.element('create-task-confirm').disabled, true);
    assert.match(page.element('preflight-checks').children[0].textContent, /未就绪/);
    page.element('cancel-new-task').click();
    await page.selectTask(fixture.created.task_id);
    assert.equal(page.element('waiting-preprocessing-count').textContent, '待核对');
    assert.equal(page.element('passed-count').textContent, '待核对');
    assert.match(page.element('folder-ticket-overview').textContent, /统计不完整/);
    assert.match(page.element('task-list-message').textContent, /列表不完整/);
  });

test('actual candidate API lookup responses retain or resolve the original creation request',
  { skip: !process.env.PAPERFLOW_CANDIDATE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_CANDIDATE_FIXTURE, 'utf8'));
    const requestId = fixture.lookup_found.request_id;
    const storage = new Map([['paperflow-pending-task-create', requestId]]);
    let found = false;
    const page = makePage(null, null, true, {
      controls: true, rawSnapshot: true, storage,
      snapshots: { [fixture.created.task_id]: fixture.folders },
      controlResponse: async (url) => url.endsWith('/preflight') ? reply(fixture.preflight)
        : reply(found ? fixture.lookup_found : fixture.lookup_missing),
    });
    await flush(); page.element('new-task-button').click(); await flush();
    await page.element('check-create-result').click();
    assert.equal(storage.get('paperflow-pending-task-create'), requestId);
    found = true;
    await page.element('check-create-result').click(); await flush();
    assert.equal(storage.has('paperflow-pending-task-create'), false);
    assert.equal(page.element('task-name').textContent, fixture.created.task_name);
    assert.equal(page.element('start-button').disabled, !fixture.folders.actions.start.allowed);
    assert.equal(page.element('pause-button').disabled, !fixture.folders.actions.pause.allowed);
  });

test('candidate HTTP queue snapshot separates waiting and registration before classification completes',
  { skip: !process.env.PAPERFLOW_STAGE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_STAGE_FIXTURE, 'utf8'));
    const snapshot = fixture.queue_before_classification;
    assert.equal(snapshot.complete, true);
    assert.equal(fixture.queue_classification.complete, false);
    const page = makePage(snapshot, null, true, {
      controls: true, rawSnapshot: true, tasks: [{ task_id: snapshot.task_id, task_name: snapshot.task_name }],
      classificationResponse: async () => fixture.queue_classification,
    });
    await flush(); await page.selectTask(snapshot.task_id);
    assert.equal(page.element('waiting-preprocessing-count').textContent, '7');
    assert.equal(page.element('preprocessing-count').textContent, '3');
    assert.equal(page.element('task-total').textContent, '10');
    assert.match(page.element('folder-ticket-overview').textContent, /累计已登记 10/);
    assert.equal(page.element('passed-count').textContent, '待核对');
    assert.equal(page.controlRequests.length, 0);
  });

test('current candidate HTTP snapshots show one ticket through parallel work, pause, retry and completion',
  { skip: !process.env.PAPERFLOW_STAGE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_STAGE_FIXTURE, 'utf8'));
    const snapshots = [fixture.both_running, fixture.paused_draining, fixture.failed, fixture.recovered, fixture.completed];
    const taskId = snapshots[0].task_id;
    const page = makePage(null, null, true, {
      controls: true, rawSnapshot: true, tasks: [{ task_id: taskId, task_name: snapshots[0].task_name }],
      snapshotResponse: async (_, call) => snapshots[call - 1],
    });
    await flush(); await page.selectTask(taskId);
    assert.equal(page.element('task-total').textContent, '1');
    assert.equal(page.element('llm-folder-count').textContent, '1');
    assert.equal(page.element('llm-body-count').textContent, '1');
    assert.equal(page.element('llm-attachment-count').textContent, '1');
    page.timers[0](); await flush();
    assert.match(page.element('task-state').textContent, /暂停中.*处理/);
    page.timers[0](); await flush();
    assert.equal(page.element('postprocessing-failed-count').textContent, '1');
    page.element('failed-folders-button').click();
    assert.equal(page.element('failed-folders-list').children[0].children[1].disabled, false);
    page.timers[0](); await flush();
    assert.equal(page.element('failed-folders-button').hidden, true);
    assert.equal(page.element('failed-folders-list').children.length, 0);
    assert.equal(page.element('postprocessing-passed-count').textContent, '1');
    assert.equal(page.element('waiting-rag-upload-count').textContent, '1');
    page.timers[0](); await flush();
    assert.equal(page.element('completed-count').textContent, '1');
    assert.equal(page.element('task-total').textContent, '1');
  });

test('current candidate HTTP snapshots preserve unknown activity and show the task stop reason',
  { skip: !process.env.PAPERFLOW_STAGE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_STAGE_FIXTURE, 'utf8'));
    const taskId = fixture.unknown_body.task_id;
    const page = makePage(null, null, true, {
      rawSnapshot: true, tasks: [{ task_id: taskId, task_name: fixture.unknown_body.task_name }],
      snapshotResponse: async (_, call) => call === 1 ? fixture.unknown_body : fixture.stopped_by_dependency,
    });
    await flush(); await page.selectTask(taskId);
    assert.equal(page.element('llm-body-count').textContent, '待核对');
    assert.equal(page.element('llm-attachment-count').textContent, '1');
    page.timers[0](); await flush();
    assert.match(page.element('task-detail').textContent, /合成模型服务不可用/);
  });

test('current candidate HTTP snapshot does not present an unknown dispatch as zero tickets',
  { skip: !process.env.PAPERFLOW_STAGE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_STAGE_FIXTURE, 'utf8'));
    const snapshot = fixture.unknown_dispatch;
    assert.equal(snapshot.complete, false);
    assert.equal(snapshot.task_state, 'unresolved');
    const page = makePage(snapshot, null, true, {
      rawSnapshot: true, tasks: [{ task_id: snapshot.task_id, task_name: snapshot.task_name }],
    });
    await flush(); await page.selectTask(snapshot.task_id);
    assert.equal(page.element('task-total').textContent, '待核对');
    assert.equal(page.element('waiting-preprocessing-count').textContent, '待核对');
    assert.match(page.element('folder-ticket-overview').textContent, /待核对/);
  });

test('current candidate HTTP snapshot counts a still-running body after an attachment fails',
  { skip: !process.env.PAPERFLOW_STAGE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_STAGE_FIXTURE, 'utf8'));
    const snapshot = fixture.failed_parallel;
    assert.equal(snapshot.folders[0].stage, 'failed');
    assert.equal(snapshot.folders[0].llm_body_running, true);
    const page = makePage(snapshot, null, true, {
      rawSnapshot: true, tasks: [{ task_id: snapshot.task_id, task_name: snapshot.task_name }],
    });
    await flush(); await page.selectTask(snapshot.task_id);
    assert.equal(page.element('task-total').textContent, '1');
    assert.equal(page.element('llm-folder-count').textContent, '1');
    assert.equal(page.element('llm-body-count').textContent, '1');
    assert.equal(page.element('llm-attachment-count').textContent, '0');
    assert.match(page.element('task-state').textContent, /暂停中.*处理/);
    assert.equal(page.element('failed-folders-button').hidden, false);
  });

test('current candidate HTTP retry snapshots resolve only the new retry on the original ticket',
  { skip: !process.env.PAPERFLOW_STAGE_FIXTURE }, async () => {
    const fixture = JSON.parse(fs.readFileSync(process.env.PAPERFLOW_STAGE_FIXTURE, 'utf8'));
    const taskId = fixture.failed.task_id;
    const ticket = fixture.failed.folders[0];
    let current = fixture.failed;
    const page = makePage(null, null, true, {
      controls: true, rawSnapshot: true, tasks: [{ task_id: taskId, task_name: fixture.failed.task_name }],
      snapshotResponse: async () => current, controlResponse: async () => { throw new Error('timeout'); },
    });
    await flush(); await page.selectTask(taskId); page.element('failed-folders-button').click();
    await page.element('failed-folders-list').children[0].children[1].click();
    const key = `paperflow-pending-retry:${taskId}:${ticket.folder_id}`;
    assert.equal(page.storage.get(key), `${ticket.ticket_id}||`);
    current = fixture.recovered;
    page.timers[0](); await flush();
    assert.equal(page.element('failed-folders-button').hidden, false);
    await page.element('failed-folders-list').children[0].children[1].click();
    assert.equal(page.storage.has(key), false);
    assert.match(page.element('failed-folders-message').textContent, /已核对.*处理中/);
    assert.equal(page.controlRequests.length, 1);
  });


for (const [count, available] of [[8, true], [0, false]]) {
  test(`creation account check shows ${count} connected accounts and ${count * 2} LLM slots`, async () => {
    const value = {
      ready: available, checked_at: '2026-11-04T10:00:00Z',
      successful_accounts: count, llm_limit: count * 2,
      checks: [{ name: 'upstream_accounts', ready: available,
        detail: `本次实际联通 ${count} 个国内账号，预计 LLM 并发 ${count * 2}；未联通账号不计入。` }],
    };
    const page = makePage(null, null, true, { controls: true,
      controlResponse: async () => reply(value) });
    await flush(); page.element('new-task-button').click(); await flush();
    assert.equal(page.element('new-task-dialog').open, true);
    const row = page.element('preflight-checks').children[0].textContent;
    assert.match(row, /上游账号连通测试/);
    assert.ok(row.includes(`联通 ${count} 个`));
    assert.ok(row.includes(`LLM 并发 ${count * 2}`));
    assert.equal(page.element('create-task-confirm').disabled, !available);
    assert.equal(page.controlRequests[0].options.signal.milliseconds, 600000);
  });
}


test('deletion removes the selected task and its persisted views while keeping other task data', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [],
    actions: { start: { allowed: false }, pause: { allowed: true }, delete: { allowed: true } } }, null, true, {
    controls: true,
    tasks: [{ task_id: 'task-1', task_name: 'A' }, { task_id: 'task-2', task_name: 'B' }],
    controlResponse: async (url, options) => {
      const request = JSON.parse(options.body);
      return reply({ task_id: 'task-1', request_id: request.request_id, deleted: true });
    },
  });
  await flush(); await page.selectTask(); await flush();
  page.storage.set('paperflow-pending-retry:task-1:folder-A', '{}');
  page.storage.set('paperflow-pending-retry:task-2:folder-B', '{}');
  await page.element('delete-task-button').click(); await flush(); await flush();
  assert.equal(page.controlRequests.filter((r) => r.options?.method === 'POST').length, 1);
  assert.match(page.controlRequests[0].url, /\/tasks\/task-1\/delete$/);
  assert.equal(page.storage.has('paperflow-last-selected-task-v1'), false);
  assert.equal(page.storage.has('paperflow-pending-retry:task-1:folder-A'), false);
  assert.equal(page.storage.has('paperflow-pending-retry:task-2:folder-B'), true);
  const saved = JSON.parse(page.storage.get('paperflow-query-cache-v1'));
  assert.equal(saved.clientState.queries.some((q) => q.queryKey[3] === 'task-1'), false);
  assert.equal(page.element('delete-task-button').disabled, true);
});

test('a timed out deletion is checked without replaying its POST', async () => {
  let checked = false;
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [],
    actions: { start: { allowed: false }, pause: { allowed: true }, delete: { allowed: true } } }, null, true, {
    controls: true,
    controlResponse: async (url, options) => {
      if (options?.method === 'POST') throw new Error('response lost');
      if (String(url).endsWith('/deletion')) {
        checked = true;
        return reply({ task_id: 'task-1', deleted: true });
      }
      return reply({ found: false, resolved: false });
    },
  });
  await flush(); await page.selectTask();
  await page.element('delete-task-button').click(); await flush(); await flush();
  assert.equal(checked, true);
  assert.equal(page.controlRequests.filter((r) => r.options?.method === 'POST').length, 1);
  assert.equal(page.storage.has('paperflow-last-selected-task-v1'), false);
});


test('an unregistered deletion can be retried manually with its original identity', async () => {
  let posts = 0;
  const identities = [];
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [],
    actions: { start: { allowed: false }, pause: { allowed: true }, delete: { allowed: true } } }, null, true, {
    controls: true,
    controlResponse: async (url, options) => {
      if (options?.method === 'POST') {
        identities.push(JSON.parse(options.body));
        posts += 1;
        if (posts === 1) throw new Error('request did not arrive');
        return reply({ task_id: 'task-1', request_id: identities[0].request_id, deleted: true });
      }
      return reply({ task_id: 'task-1', deleted: false, status: 'present' });
    },
  });
  await flush(); await page.selectTask();
  await page.element('delete-task-button').click(); await flush();
  assert.equal(posts, 1);
  assert.equal(page.element('delete-task-button').disabled, false);
  await page.element('delete-task-button').click(); await flush();
  assert.equal(posts, 2);
  assert.deepEqual(identities[1], identities[0]);
  assert.equal(page.storage.has('paperflow-last-selected-task-v1'), false);
});

test('a late control lookup cannot recreate records for a deleted task', async () => {
  let release;
  const deferred = new Promise((resolve) => { release = resolve; });
  const storageKey = 'paperflow-control:https%3A%2F%2Fexample.test%2Fcontrol:task-1';
  const storage = new Map([[storageKey, JSON.stringify({ intent_seq: 1,
    pending: [{ action: 'start', request_id: 'old-start', intent_seq: 1 }] })]]);
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [],
    actions: { start: { allowed: false }, pause: { allowed: true }, delete: { allowed: true } } }, null, true, {
    controls: true, storage,
    controlResponse: async (url, options) => {
      if (String(url).includes('/operations/old-start')) return deferred;
      const request = JSON.parse(options.body);
      return reply({ task_id: 'task-1', request_id: request.request_id, deleted: true });
    },
  });
  await flush(); await page.selectTask();
  await page.element('delete-task-button').click(); await flush();
  assert.equal(storage.has(storageKey), false);
  release(reply({ request_id: 'old-start', action: 'start', target: { task_id: 'task-1' },
    intent_seq: 1, found: true, resolved: true, status: 'superseded' }));
  await flush(); await flush();
  assert.equal(storage.has(storageKey), false);
});

test('a manual deletion after a newer intent uses a new request and a higher sequence', async () => {
  const identities = [];
  let serial = 0;
  const storageKey = 'paperflow-control:https%3A%2F%2Fexample.test%2Fcontrol:task-1';
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [],
    actions: { start: { allowed: false }, pause: { allowed: true }, delete: { allowed: true } } }, null, true, {
    controls: true, randomUUID: () => `delete-${++serial}`,
    controlResponse: async (url, options) => {
      if (options?.method === 'POST') {
        identities.push(JSON.parse(options.body));
        if (identities.length === 1) throw new Error('request did not arrive');
        return reply({ task_id: 'task-1', request_id: identities.at(-1).request_id, deleted: true });
      }
      return reply({ task_id: 'task-1', deleted: false, status: 'present' });
    },
  });
  await flush(); await page.selectTask();
  await page.element('delete-task-button').click(); await flush();
  const state = JSON.parse(page.storage.get(storageKey));
  state.intent_seq += 1;
  page.storage.set(storageKey, JSON.stringify(state));
  await page.element('delete-task-button').click(); await flush();
  assert.equal(identities.length, 2);
  assert.notEqual(identities[1].request_id, identities[0].request_id);
  assert.ok(identities[1].intent_seq > state.intent_seq);
});
