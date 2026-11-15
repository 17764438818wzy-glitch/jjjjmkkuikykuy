const task = await taskSpace(88);
const page = task.page('p1');
await page.click('[data-postprocessing-filter="unrecognized"]');
await page.waitForFunction(() => document.getElementById('postprocessing-list-message').textContent.includes('暂无已登记'));
console.log(await page.snapshot({ scope: 'full_page' }));
await page.click('#postprocessing-close-list');
console.log({ closed: await page.evaluate(() => document.getElementById('postprocessing-list-panel').hidden) });
await task.finish({ keep: [] });
