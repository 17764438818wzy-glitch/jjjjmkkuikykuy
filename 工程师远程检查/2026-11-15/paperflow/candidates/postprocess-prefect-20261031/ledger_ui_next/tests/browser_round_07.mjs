const task = await taskSpace(88);
const page = task.page('p1');
await page.reload();
await page.waitForFunction(() => document.getElementById('postprocessing-passed-count').textContent === '≥21');
await page.click('[data-postprocessing-filter="failed"]');
await page.waitForFunction(() => document.getElementById('postprocessing-group-list').textContent.includes('正文画像校验未通过'));
console.log({ failed: await page.evaluate(() => document.getElementById('postprocessing-group-list').textContent) });
console.log(await page.snapshot({ scope: 'full_page' }));
