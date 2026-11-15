const task = await taskSpace(88);
const page = task.page('p1');
console.log(await page.evaluate(() => {
  const e = document.querySelector('#postprocessing-next-page');
  const r = e.getBoundingClientRect();
  return { y: r.y, x: r.x, width: r.width, height: r.height, scrollY, heightWindow: innerHeight,
    hidden: e.offsetParent === null, disabled: e.disabled, overflow: getComputedStyle(document.body).overflowY };
}));
