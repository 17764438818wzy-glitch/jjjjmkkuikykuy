const task = await taskSpace(42);
const page = task.page('p1');
await page.click('#start-button');
await page.waitForFunction(() => document.querySelector('#passed-count')?.textContent.trim() === '1', undefined, { timeout: 20000 });
console.log('FIXTURE', await page.evaluate(async () => (await fetch('/fixture-info')).json()));
console.log(await page.snapshot({ scope: 'full_page' }));
