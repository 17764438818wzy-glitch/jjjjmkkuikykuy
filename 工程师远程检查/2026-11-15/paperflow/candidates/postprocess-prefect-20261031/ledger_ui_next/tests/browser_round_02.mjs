const task = await taskSpace(88);
const page = task.page('p1');
await page.reload();
await page.waitForFunction(() => document.getElementById('postprocessing-passed-count').textContent === '≥21');
console.log(await page.snapshot({ scope: 'full_page' }));
