const task = await taskSpace(88);
const page = task.page('p1');
console.log(await page.snapshot({ scope: 'full_page' }));
console.log(await page.evaluate(() => ({ calls: window.fixtureCalls, viewport: [innerWidth, innerHeight] })));
