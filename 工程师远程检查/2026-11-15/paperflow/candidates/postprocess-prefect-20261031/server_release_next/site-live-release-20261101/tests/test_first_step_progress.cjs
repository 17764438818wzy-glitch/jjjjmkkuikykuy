const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function setup(taskId) {
  const source = fs.readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8');
  const start = source.indexOf('  function showProcessing(');
  const end = source.indexOf('\n  function showUngrouped(', start);
  assert.ok(start >= 0 && end > start, 'first-step progress renderer must exist');
  const classes = new Set();
  const progress = {
    attributes: {},
    setAttribute(name, value) { this.attributes[name] = value; },
    removeAttribute(name) { delete this.attributes[name]; },
    style: { removeProperty() {} },
  };
  const fill = { hidden: false };
  const nodes = new Map();
  for (const id of ['task-total', 'discovered', 'progress-caption', 'progress-hint', 'results', 'list-panel']) {
    nodes.set(id, { textContent: '', hidden: false, replaceChildren() {}, append() {} });
  }
  const processing = {
    hidden: false,
    classList: {
      toggle(name, on) { if (on) classes.add(name); else classes.delete(name); },
      remove(name) { classes.delete(name); },
    },
    querySelector(selector) { return selector === '.progress-fill' ? fill : progress; },
  };
  nodes.set('processing', processing);
  const context = {
    $: (id) => nodes.get(id),
    state: { taskId, taskName: '', payload: null },
    setStatus() {},
    integer: (value) => Number.isInteger(value) && value >= 0 ? value : null,
    number: new Intl.NumberFormat('zh-CN'),
    document: { createElement: () => ({ textContent: '' }) },
  };
  const showProcessing = vm.runInNewContext(`(() => { ${source.slice(start, end)}; return showProcessing; })()`, context);
  return { showProcessing, processing, progress, fill, nodes, state: context.state };
}

test('keeps an unavailable progress area for a selected task without inventing progress', () => {
  const page = setup('task-1');
  page.showProcessing(null, '数据暂不可用', '正在等待后端接口连接。');
  assert.equal(page.processing.hidden, false);
  assert.equal(page.fill.hidden, true);
  assert.equal(page.progress.attributes['aria-valuenow'], undefined);
  assert.equal(page.nodes.get('progress-caption').textContent, '数据暂不可用');
});

test('does not show a task progress area when no task was selected', () => {
  const page = setup(null);
  page.showProcessing(null, '数据暂不可用', '正在等待后端接口连接。');
  assert.equal(page.processing.hidden, true);
});

test('does not restore a progress area for a task already known to be complete', () => {
  const page = setup('task-1');
  page.state.payload = { task_id: 'task-1', task_status: 'complete' };
  page.showProcessing(null, '数据暂不可用', '正在等待后端接口连接。');
  assert.equal(page.processing.hidden, true);
});

test('restores the normal progress fill when task data becomes available', () => {
  const page = setup('task-1');
  page.showProcessing(null, '数据暂不可用', '正在等待后端接口连接。');
  page.showProcessing(null, '处理中', '正在识别和分流资料。');
  assert.equal(page.processing.hidden, false);
  assert.equal(page.fill.hidden, false);
});
