const task = await taskSpace(88);
const page = task.page('p1');
await page.click('#postprocessing-next-page');
await page.waitForFunction(() => document.getElementById('postprocessing-group-list').textContent.includes('合成文件夹 2'));
console.log(await page.evaluate(() => ({
  page: document.getElementById('postprocessing-page-info').textContent,
  revision: window.fixtureCalls.findLast((call) => call.filter === 'passed' && call.page === 2)?.revision,
  hint: document.getElementById('postprocessing-list-count').textContent,
})));
console.log(await page.snapshot({ scope: 'full_page' }));
