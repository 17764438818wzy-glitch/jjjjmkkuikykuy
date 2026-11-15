const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const path = require('node:path');

const text = fs.readFileSync(path.join(__dirname, '..', 'llm-results.js'), 'utf8');
const begin = text.indexOf('  function showRetryResult(result, started) {');
const end = text.indexOf('  function rememberRetry(entry) {', begin);
assert(begin >= 0 && end > begin);
const context = {
  retry: { rows: {}, latestItems: [], latestStarted: 0, latestRequestId: null },
  folderKey: (item) => `${item.task_id}:${item.group_id}`,
  retryMessage: (result) => result.message,
  refreshRetryUI: () => {},
};
vm.createContext(context);
vm.runInContext(text.slice(begin, end), context);

const oldItems = [{ task_id: 't', group_id: 'a', message: '大任务当前已暂停' },
                  { task_id: 't', group_id: 'b', message: '大任务当前已暂停' }];
context.retry.latestStarted = 100;
context.showRetryResult({ request_id: 'old', items: oldItems, message: '旧批量' }, 100);
context.retry.latestStarted = 200;
context.showRetryResult({ request_id: 'new', items: [], state: 'failed', message: '本次读取名单失败' }, 200);
assert.equal(context.retry.latestItems.length, 0, '空结果的新操作不能显示旧逐项明细');
assert.equal(context.retry.feedback, '本次读取名单失败');
context.showRetryResult({ request_id: 'old', items: oldItems, message: '旧批量' }, 100);
assert.equal(context.retry.latestItems.length, 0, '较旧的异步响应不能覆盖最新操作');
context.retry.latestStarted = 300;
const item = { task_id: 't', group_id: 'a', message: '原请求结果未确认' };
context.showRetryResult({ request_id: 'single', items: [item], message: '最新单项' }, 300);
assert.equal(context.retry.latestItems.length, 1, '单项操作的明细只显示该单项');
assert.equal(context.retry.latestItems[0].message, item.message);
context.showRetryResult({ request_id: 'single', items: [item], message: '服务端历史' }, 305);
context.showRetryResult({ request_id: 'single', items: [{...item, message: '等待后处理'}], message: '已完成' }, 300);
assert.equal(context.retry.feedback, '已完成', '同一操作的实时状态不能被时间戳差异挡住');
assert.equal(context.retry.latestItems[0].message, '等待后处理');
assert.equal(context.retry.rows['t:a'].message, '等待后处理');
console.log('latest retry display tests passed');
