const task = await taskSpace('paperflow-ledger-ui-20261101');
const page = task.page('p1');
await page.goto('http://127.0.0.1:18796/ledger_ui_next/tests/browser_fixture.html');
await page.waitForFunction(() => document.getElementById('waiting-postprocessing-count').textContent !== '—');
console.log({ spaceId: task.spaceId, page: page.label });
console.log(await page.snapshot({ scope: 'full_page' }));
