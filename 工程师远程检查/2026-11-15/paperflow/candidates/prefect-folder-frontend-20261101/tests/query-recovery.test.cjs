const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const { test } = require('node:test');
const { chromium } = require('playwright');
const { QueryClient } = require('@tanstack/query-core');
const { persistQueryClientSave } = require('@tanstack/query-persist-client-core');

const root = path.resolve(__dirname, '..');
const browserPath = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const task = { task_id: 'fake-task', task_name: '隔离假任务' };
const folder = { ticket_id: 'fake-ticket', task_id: 'fake-task', folder_id: 'fake-folder',
  folder_name: '合成文件夹', paper_title: null, stage: 'waiting_llm',
  llm_body_running: false, llm_attachment_running: false, preprocessing_result: 'passed',
  llm_result: null, postprocessing_result: null, last_retry: null };

async function fixtureServer() {
  const state = { reads: 0, classifications: 0, posts: 0, fail: false, pending: true, paths: [],
    holdReads: false, openReads: 0, abortedReads: 0, payloadBytes: 0,
    classificationDelayMs: 0, classificationsReturned: 0 };
  let origin;
  const server = http.createServer(async (request, response) => {
    const url = new URL(request.url, origin);
    state.paths.push(url.pathname);
    const json = (value, status = 200) => {
      response.writeHead(status, { 'content-type': 'application/json' });
      response.end(JSON.stringify(value));
    };
    if (request.method !== 'GET') { state.posts += 1; return json({ detail: 'no writes in this fixture' }, 405); }
    if (url.pathname === '/api-config.json') return json({
      tasks_api_url: `${origin}/api/v1/prefect/tasks`,
      folder_tickets_api_url: `${origin}/api/v1/prefect/folder-tickets`,
      control_api_url: `${origin}/api/v1/prefect`,
    });
    if (url.pathname === '/api/v1/prefect/tasks') return json({ as_of: '2026-11-01T12:00:00Z', complete: true,
      query_status: 'updated', has_pending: state.pending, tasks: [task] });
    if (url.pathname === '/api/v1/prefect/folder-tickets') {
      state.reads += 1;
      if (state.holdReads) {
        state.openReads += 1;
        response.on('close', () => {
          if (!response.writableEnded) { state.abortedReads += 1; state.openReads -= 1; }
        });
        return;
      }
      if (state.fail) return json({ query_status: 'unavailable' }, 503);
      return json({ task_id: task.task_id, task_state: state.pending ? 'running' : 'completed',
        desired_state: 'running', as_of: '2026-11-01T12:00:00Z', complete: true,
        query_status: 'updated', has_pending: state.pending, folders: [folder],
        fixture_padding: 'x'.repeat(state.payloadBytes),
        actions: { start: { allowed: false, reason: null }, pause: { allowed: true, reason: null } } });
    }
    if (url.pathname.endsWith('/classifications')) {
      state.classifications += 1;
      if (state.classificationDelayMs) await new Promise((resolve) => setTimeout(resolve, state.classificationDelayMs));
      if (state.fail) return json({ query_status: 'unavailable' }, 503);
      json({ task_id: task.task_id, as_of: '2026-11-01T12:00:00Z', complete: true,
        query_status: 'updated', has_pending: state.pending, items: [] });
      state.classificationsReturned += 1;
      return;
    }
    const relative = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    const filename = path.resolve(root, relative);
    if (!filename.startsWith(`${root}${path.sep}`)) { response.writeHead(404).end(); return; }
    try {
      const data = await fs.readFile(filename);
      const type = filename.endsWith('.html') ? 'text/html' : filename.endsWith('.js') ? 'text/javascript' :
        filename.endsWith('.css') ? 'text/css' : 'application/octet-stream';
      response.writeHead(200, { 'content-type': type });
      response.end(data);
    } catch (_) { response.writeHead(404).end(); }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
  return { origin, state, close: () => new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); }) };
}

async function waitUntil(condition, description, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (!(await condition()) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 20));
  assert.ok(await condition(), description);
}

async function isolateNetwork(context, origin) {
  await context.route('**/*', (route) => {
    if (new URL(route.request().url()).origin === origin) return route.continue();
    return route.abort();
  });
}

async function selectTask(page) {
  await page.locator('#open-task-button').click();
  await page.locator('[data-task-id="fake-task"]').click();
  await page.waitForFunction(() => document.querySelector('#waiting-llm-count').textContent === '1');
}

async function committedQueryCache(page) {
  return page.evaluate(() => new Promise((resolve, reject) => {
    const request = indexedDB.open('paperflow-query-cache-v1');
    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains('queries')) { db.close(); resolve(null); return; }
      const tx = db.transaction('queries', 'readonly');
      const read = tx.objectStore('queries').get('paperflow-query-cache-v1');
      tx.oncomplete = () => { db.close(); resolve(read.result ?? null); };
      tx.onerror = () => { db.close(); reject(tx.error); };
    };
  }));
}

test('a slow successful classification GET updates the page without a timeout failure or business POST', async (t) => {
  const fixture = await fixtureServer();
  fixture.state.pending = false;
  fixture.state.classificationDelayMs = 9000;
  t.after(() => fixture.close());
  const browser = await chromium.launch({ headless: true, executablePath: browserPath });
  t.after(() => browser.close());
  const context = await browser.newContext();
  await isolateNetwork(context, fixture.origin);
  const page = await context.newPage();
  await page.goto(fixture.origin);
  await selectTask(page);
  await waitUntil(() => fixture.state.classificationsReturned === 1, 'slow classification returned', 12000);
  await page.waitForFunction(() => document.querySelector('#classification-results-hint').textContent.includes('数据截至'), undefined, { timeout: 1500 });
  assert.doesNotMatch(await page.locator('#classification-results-hint').textContent(), /更新失败|上次数据|暂不可用/);
  assert.equal(fixture.state.classifications, 1);
  assert.equal(fixture.state.posts, 0);
});

test('large query cache migrates and reopens offline without changing original control records', async (t) => {
  const fixture = await fixtureServer();
  fixture.state.payloadBytes = 6 * 1024 * 1024;
  const profilePrefix = path.join(os.tmpdir(), 'paperflow-large-cache-');
  const profile = await fs.mkdtemp(profilePrefix);
  let context = await chromium.launchPersistentContext(profile, { headless: true, executablePath: browserPath });
  t.after(async () => {
    if (context) await context.close();
    await fixture.close();
    if (!path.resolve(profile).startsWith(path.resolve(profilePrefix))) throw new Error('invalid test profile path');
    await fs.rm(profile, { recursive: true, force: true });
  });
  await isolateNetwork(context, fixture.origin);
  const controlRoot = `${fixture.origin}/api/v1/prefect`;
  const originals = {
    [`paperflow-control:${encodeURIComponent(controlRoot)}:${task.task_id}`]: JSON.stringify({
      intent_seq: 7, pending: [{ request_id: 'original-unknown-request', action: 'start', intent_seq: 7 }],
    }),
    'paperflow-pending-task-create': 'original-create-request',
    [`paperflow-pending-retry:${task.task_id}:fake-folder`]: JSON.stringify({ request_id: 'original-retry-request' }),
  };
  const legacyClient = new QueryClient();
  const queryKey = ['paperflow', controlRoot, 'folders', task.task_id];
  await legacyClient.fetchQuery({ queryKey, queryFn: async () => ({ task_id: task.task_id,
    task_state: 'paused', desired_state: 'paused', as_of: '2026-11-01T12:00:00Z', complete: false,
    has_pending: null, query_status: 'updated', folders: [folder], fixture_padding: 'y'.repeat(4 * 1024 * 1024) }) });
  let legacy;
  await persistQueryClientSave({ queryClient: legacyClient, buster: 'paperflow-v1',
    persister: { persistClient: async (value) => { legacy = JSON.stringify(value); } } });
  legacyClient.clear();
  await context.addInitScript(({ originals, legacy, task }) => {
    for (const [key, value] of Object.entries(originals)) localStorage.setItem(key, value);
    localStorage.setItem('paperflow-query-cache-v1', legacy);
    localStorage.setItem('paperflow-last-selected-task-v1', JSON.stringify(task));
  }, { originals, legacy, task });
  let page = await context.newPage();
  await page.goto(fixture.origin);
  await selectTask(page);
  let cache;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if ((await page.locator('#save-state').textContent()).includes('本机记录未保存')) break;
    cache = await committedQueryCache(page);
    if (cache?.length > 6 * 1024 * 1024) break;
    await new Promise((resolve) => setTimeout(resolve, 30));
  }
  assert.doesNotMatch(await page.locator('#save-state').textContent(), /本机记录未保存/);
  assert.ok(cache?.length > 6 * 1024 * 1024, 'large cache transaction must commit');
  const stored = await page.evaluate(() => Object.fromEntries(Object.entries(localStorage)));
  for (const [key, value] of Object.entries(originals)) assert.equal(stored[key], value);
  assert.equal(stored['paperflow-query-cache-v1'], legacy, 'old query cache is read-only');
  const parsed = JSON.parse(cache).clientState.queries.find((query) => query.queryKey[2] === 'folders').state.data;
  assert.equal(parsed.as_of, '2026-11-01T12:00:00Z');
  await page.evaluate(() => navigator.serviceWorker.ready);
  await context.close();
  context = await chromium.launchPersistentContext(profile, { headless: true, executablePath: browserPath });
  await isolateNetwork(context, fixture.origin);
  await context.setOffline(true);
  page = await context.newPage();
  await page.goto(fixture.origin, { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => document.querySelector('#waiting-llm-count').textContent === '1');
  assert.match(await page.locator('#notice-text').textContent(), /连接中断.*上次记录.*不能提交/);
  assert.ok((await committedQueryCache(page)).length > 6 * 1024 * 1024);
  const reopened = await page.evaluate(() => Object.fromEntries(Object.entries(localStorage)));
  for (const [key, value] of Object.entries(originals)) assert.equal(reopened[key], value);
  assert.equal(fixture.state.posts, 0);
});

for (const failure of ['read', 'write']) {
  test(`IndexedDB ${failure} failure preserves legacy cache and original pending identity`, async (t) => {
    const fixture = await fixtureServer();
    t.after(() => fixture.close());
    const browser = await chromium.launch({ headless: true, executablePath: browserPath });
    t.after(() => browser.close());
    const context = await browser.newContext();
    await isolateNetwork(context, fixture.origin);
    const controlKey = `paperflow-control:${encodeURIComponent(`${fixture.origin}/api/v1/prefect`)}:${task.task_id}`;
    const original = JSON.stringify({ intent_seq: 9,
      pending: [{ request_id: 'preserved-unknown', action: 'start', intent_seq: 9 }] });
    const legacy = JSON.stringify({ timestamp: Date.now(), buster: 'paperflow-v1',
      clientState: { queries: [], mutations: [] } });
    await context.addInitScript(({ failure, controlKey, original, legacy }) => {
      localStorage.setItem(controlKey, original);
      localStorage.setItem('paperflow-query-cache-v1', legacy);
      if (failure === 'read') indexedDB.open = () => { throw new DOMException('fixture read unavailable', 'UnknownError'); };
      else IDBObjectStore.prototype.put = () => { throw new DOMException('fixture write unavailable', 'QuotaExceededError'); };
    }, { failure, controlKey, original, legacy });
    const page = await context.newPage();
    await page.goto(fixture.origin);
    await selectTask(page);
    await page.waitForFunction(() => document.querySelector('#save-state').textContent.includes('本机记录未保存'));
    assert.equal(await page.evaluate((key) => localStorage.getItem(key), controlKey), original);
    assert.equal(await page.evaluate(() => localStorage.getItem('paperflow-query-cache-v1')), legacy);
    assert.equal(fixture.state.posts, 0);
  });
}

test('polling follows visible activity, failure backoff, visibility and terminal state', async (t) => {
  const fixture = await fixtureServer();
  t.after(() => fixture.close());
  const browser = await chromium.launch({ headless: true, executablePath: browserPath });
  t.after(() => browser.close());
  const context = await browser.newContext();
  await isolateNetwork(context, fixture.origin);
  const page = await context.newPage();
  await page.clock.install({ time: new Date('2026-11-02T12:00:00Z') });
  await page.goto(fixture.origin);
  await selectTask(page);
  assert.match(await page.locator('#notice-text').textContent(), /已配置/);
  assert.equal(fixture.state.reads, 1);
  await page.clock.fastForward(10000);
  await page.waitForFunction(() => document.querySelector('#waiting-llm-count').textContent === '1');
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 2);
  fixture.state.fail = true;
  await page.clock.fastForward(10000);
  await page.waitForFunction(() => document.querySelector('#folder-ticket-overview').textContent.includes('更新失败'));
  assert.equal(fixture.state.reads, 3);
  await page.clock.fastForward(20000);
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 3, 'failed connection waits for 30 seconds');
  await page.clock.fastForward(10000);
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 4);
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' });
    document.dispatchEvent(new Event('visibilitychange'));
  });
  await page.clock.fastForward(60000);
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 4, 'hidden page does not poll');
  fixture.state.fail = false;
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' });
    document.dispatchEvent(new Event('visibilitychange'));
  });
  await page.waitForFunction(() => !document.querySelector('#folder-ticket-overview').textContent.includes('更新失败'));
  assert.equal(fixture.state.reads, 5, 'becoming visible reads again');
  fixture.state.pending = null;
  await page.locator('#refresh-task-button').click();
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 6);
  await page.clock.fastForward(10000);
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 7, 'unknown pending status keeps polling');
  fixture.state.pending = false;
  await page.locator('#refresh-task-button').click();
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 8);
  await page.clock.fastForward(30000);
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(fixture.state.reads, 8, 'only explicitly non-pending data stops polling');
  assert.equal(fixture.state.posts, 0, 'manual refresh is read-only');
});

test('manual refresh aborts obsolete slow HTTP requests', async (t) => {
  const fixture = await fixtureServer();
  t.after(() => fixture.close());
  const browser = await chromium.launch({ headless: true, executablePath: browserPath });
  t.after(() => browser.close());
  const context = await browser.newContext();
  await isolateNetwork(context, fixture.origin);
  const page = await context.newPage();
  await page.goto(fixture.origin);
  await selectTask(page);
  fixture.state.holdReads = true;
  await page.locator('#refresh-task-button').click();
  await waitUntil(() => fixture.state.openReads === 1, 'first slow read started');
  await page.locator('#refresh-task-button').click();
  await waitUntil(() => fixture.state.reads === 3, 'second slow read started');
  await waitUntil(() => fixture.state.abortedReads === 1, 'first slow read was aborted');
  await page.locator('#refresh-task-button').click();
  await waitUntil(() => fixture.state.reads === 4, 'third slow read started');
  await waitUntil(() => fixture.state.abortedReads === 2, 'second slow read was aborted');
  assert.equal(fixture.state.openReads, 1);
  assert.equal(fixture.state.posts, 0);
});

test('an installed page reopens offline in a new browser process with its saved task and time', async (t) => {
  const fixture = await fixtureServer();
  const profilePrefix = path.join(os.tmpdir(), 'paperflow-d-playwright-');
  const profile = await fs.mkdtemp(profilePrefix);
  let context = await chromium.launchPersistentContext(profile, { headless: true, executablePath: browserPath });
  t.after(async () => {
    if (context) await context.close();
    await fixture.close();
    if (!path.resolve(profile).startsWith(path.resolve(profilePrefix))) throw new Error('invalid test profile path');
    await fs.rm(profile, { recursive: true, force: true });
  });
  await isolateNetwork(context, fixture.origin);
  let page = await context.newPage();
  const errors = [];
  context.on('console', (message) => { if (message.type() === 'error') errors.push(`context: ${message.text()}`); });
  context.on('serviceworker', (worker) => {
    worker.on('console', (message) => errors.push(`worker: ${message.text()}`));
    worker.on('close', () => errors.push('worker closed'));
  });
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') errors.push(message.text()); });
  await page.goto(fixture.origin);
  await selectTask(page);
  const ready = await page.evaluate(async () => Promise.race([
    navigator.serviceWorker.ready.then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 5000)),
  ]));
  if (!ready) {
    const state = await page.evaluate(async () => {
      const registration = await navigator.serviceWorker.getRegistration();
      return { secure: isSecureContext, registration: Boolean(registration),
        installing: registration?.installing?.state, waiting: registration?.waiting?.state, active: registration?.active?.state };
    });
    assert.fail(`service worker failed to install: ${JSON.stringify(state)}; paths: ${fixture.state.paths.join(', ')}; ${errors.join('; ')}`);
  }
  await waitUntil(async () => ((await committedQueryCache(page)) || '').includes('fake-task'), 'query cache transaction committed');
  await context.close();
  context = await chromium.launchPersistentContext(profile, { headless: true, executablePath: browserPath });
  await isolateNetwork(context, fixture.origin);
  await context.setOffline(true);
  page = await context.newPage();
  await page.goto(fixture.origin, { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => document.querySelector('#waiting-llm-count').textContent === '1');
  assert.equal(await page.locator('#task-name').textContent(), '隔离假任务');
  assert.match(await page.locator('#folder-ticket-overview').textContent(), /上次数据/);
  assert.match(await page.locator('#folder-ticket-overview').textContent(), /2026/);
  assert.match(await page.locator('#notice-text').textContent(), /连接中断.*上次记录.*不能提交/);
  assert.equal(fixture.state.posts, 0);
});
