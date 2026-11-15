const task = await taskSpace(88);
const page = task.page('p1');
await page.click('[data-postprocessing-filter="passed"]');
await page.waitForFunction(() => document.getElementById('postprocessing-group-list').textContent.includes('合成文件夹 1'));
await page.click('#postprocessing-next-page');
await page.waitForFunction(() => document.getElementById('postprocessing-group-list').textContent.includes('合成文件夹 2'));
const pageTwo = await page.evaluate(() => ({
  page: document.getElementById('postprocessing-page-info').textContent,
  revision: window.fixtureCalls.findLast((call) => call.filter === 'passed' && call.page === 2)?.revision,
  hint: document.getElementById('postprocessing-list-count').textContent,
}));
if (pageTwo.revision !== 'a'.repeat(64) || !pageTwo.page.includes('2')) throw new Error('分页快照未绑定');
await page.click('[data-postprocessing-filter="failed"]');
await page.waitForFunction(() => document.getElementById('postprocessing-list-title').textContent.includes('未通过'));
const failed = await page.evaluate(() => document.getElementById('postprocessing-group-list').textContent);
if (!failed.includes('合成登记记录')) throw new Error('未通过列表未展开');
await page.click('[data-postprocessing-filter="unrecognized"]');
await page.waitForFunction(() => document.getElementById('postprocessing-list-title').textContent.includes('未识别'));
const unrecognized = await page.evaluate(() => ({
  count: document.getElementById('postprocessing-unrecognized-count').textContent,
  list: document.getElementById('postprocessing-group-list').textContent,
  panelHidden: document.getElementById('postprocessing-list-panel').hidden,
}));
if (unrecognized.count !== '—' || unrecognized.list !== '' || unrecognized.panelHidden) throw new Error('未识别状态错误');
console.log({ pageTwo, failed, unrecognized });
await page.click('#postprocessing-close-list');
console.log(await page.snapshot({ scope: 'full_page' }));
await task.finish({ keep: [] });
