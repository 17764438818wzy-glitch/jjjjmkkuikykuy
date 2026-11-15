const task = await taskSpace(42);
const page = task.page('p1');
const receipt = await page.click('#delete-task-button');
console.log('CONFIRM', Boolean(receipt.dialog));
await page.acceptDialog();
await page.waitForFunction(async () => {
  const taskList = await (await fetch('/api/v1/first-step/tasks')).json();
  const fixture = await (await fetch('/fixture-info')).json();
  return taskList.total === 0 && fixture.artifact_files === 0;
}, undefined, { timeout: 20000 });
console.log('AFTER', await page.evaluate(async () => (await fetch('/fixture-info')).json()));
console.log(await page.snapshot({ scope: 'full_page' }));
await task.finish({ keep: [] });
