const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const script = path.join(__dirname, '..', 'folder-ticket-dashboard.js');

class Element {
  constructor() {
    this.textContent = '—';
    this.hidden = false;
    this.disabled = false;
    this.dataset = {};
    this.attributes = {};
    this.children = [];
    this.listeners = {};
  }
  setAttribute(name, value) { this.attributes[name] = value; }
  addEventListener(name, handler) { this.listeners[name] = handler; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  click() { this.listeners.click?.(); }
}

function makePage(snapshot, refreshResult, configured = true) {
  const nodes = new Map();
  const buttons = new Map();
  const timers = [];
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
  let calls = 0;
  let now = Date.now();
  class PageDate extends Date { static now() { return now; } }
  const document = {
    getElementById: element,
    querySelectorAll(selector) {
      const match = selector.match(/^\[data-(preprocessing|llm|postprocessing)-filter\]$/);
      return match ? buttons.get(match[1]) : [];
    },
    createElement() { return new Element(); },
  };
  const context = {
    document, Intl, Date: PageDate, AbortSignal,
    fetch: async (url) => {
      if (url === 'api-config.json') {
        return { ok: true, json: async () => ({ folder_tickets_api_url: configured ? 'https://example.test/folders' : '' }) };
      }
      assert.equal(url, 'https://example.test/folders');
      calls += 1;
      if (calls > 1 && refreshResult instanceof Error) throw refreshResult;
      return { ok: true, json: async () => calls > 1 && refreshResult ? refreshResult : snapshot };
    },
    setInterval(handler) { timers.push(handler); },
  };
  if (fs.existsSync(script)) vm.runInNewContext(fs.readFileSync(script, 'utf8'), context);
  return { element, buttons, timers, advance(ms) { now += ms; } };
}

const folder = (ticket_id, stage, extra = {}) => ({
  ticket_id, task_id: 'task-1', folder_id: `folder-${ticket_id}`,
  folder_name: `资料-${ticket_id}`, paper_title: null, stage,
  llm_body_running: false, llm_attachment_running: false,
  preprocessing_result: null, llm_result: null, postprocessing_result: null,
  ...extra,
});
const flush = () => new Promise(setImmediate);

test('shows waiting for integration rather than a zero before an endpoint is configured', async () => {
  const page = makePage(null, null, false);
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '待接入');
  assert.match(page.element('folder-ticket-overview').textContent, /等待后端接入/);
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
  await flush();
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
  await flush();
  assert.equal(page.element('llm-folder-count').textContent, '1');
  assert.equal(page.element('llm-body-count').textContent, '1');
  assert.equal(page.element('llm-attachment-count').textContent, '1');
  assert.match(page.element('folder-ticket-overview').textContent, /共 1 张文件夹工单/);
});

test('saved LLM answers awaiting validation are visible without claiming they passed', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'waiting_postprocessing', { llm_response_saved: true, llm_detail: '原始回答已保存；画像校验在后处理进行。' }),
  ] });
  await flush();
  assert.equal(page.element('llm-passed-count').textContent, '0');
  assert.match(page.element('llm-results-hint').textContent, /已返回待校验 1/);
});

test('a partial snapshot shows unknown counts with an incomplete-data notice', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false, folders: [] });
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '—');
  assert.equal(page.element('postprocessing-passed-count').textContent, '—');
  assert.match(page.element('waiting-preprocessing-hint').textContent, /已确认|统计不完整/);
});

test('a folder with an unresolved position is reported as awaiting review', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: false,
    folders: [folder('1', 'unresolved')] });
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '—');
  assert.match(page.element('folder-ticket-overview').textContent, /待核对 1/);
  assert.match(page.element('waiting-preprocessing-hint').textContent, /统计不完整/);
});

test('a failed folder identifies the stage where it stopped', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    folders: [folder('1', 'failed', { failed_stage: 'rag' })] });
  await flush();
  assert.match(page.element('folder-ticket-overview').textContent, /失败 1/);
  assert.match(page.element('folder-ticket-overview').textContent, /RAG 1/);
  assert.equal(page.element('rag-processing-count').textContent, '0');
});

test('a failed refresh retains the last snapshot and marks it outdated', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true,
    folders: [folder('1', 'waiting_preprocessing')] }, new Error('offline'));
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '1');
  page.timers[0]();
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '1');
  assert.match(page.element('folder-ticket-overview').textContent, /更新失败|上次/);
  page.advance(31000);
  page.timers[0]();
  await flush();
  assert.equal(page.element('waiting-preprocessing-count').textContent, '—');
  assert.match(page.element('folder-ticket-overview').textContent, /暂不可用/);
});

test('result list shows the matching folder from the same snapshot', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'waiting_llm', { paper_title: '论文甲', preprocessing_result: 'passed' }),
    folder('2', 'waiting_preprocessing'),
  ] });
  await flush();
  page.buttons.get('preprocessing')[0].click();
  assert.equal(page.element('preprocessing-list-panel').hidden, false);
  assert.equal(page.element('preprocessing-group-list').children.length, 1);
  assert.equal(page.element('preprocessing-group-list').children[0].children[0].children[0].textContent, '论文甲');
});

test('postprocessing result details use the same label as the card', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'failed', { failed_stage: 'postprocessing', postprocessing_result: 'failed' }),
  ] });
  await flush();
  page.buttons.get('postprocessing')[1].click();
  assert.equal(page.element('postprocessing-list-title').textContent, '后处理未通过');
  assert.equal(page.element('postprocessing-group-list').children[0].children[1].textContent, '未通过');
});

test('a registered success retains the ticket and explains that warnings are in the package', async () => {
  const page = makePage({ as_of: '2026-11-01T12:00:00Z', complete: true, folders: [
    folder('1', 'waiting_rag_upload', { paper_title_candidate: '候选论文', postprocessing_result: 'passed',
      postprocessing_detail: '已登记通过；缺口与警告以交接封包为准，页面未展开详情。' }),
  ] });
  await flush();
  page.buttons.get('postprocessing')[0].click();
  const entry = page.element('postprocessing-group-list').children[0].children[0].children;
  assert.match(entry[0].textContent, /候选论文.*待核对/);
  assert.match(entry[2].textContent, /小工单：1/);
  assert.match(entry[3].textContent, /缺口与警告/);
});
