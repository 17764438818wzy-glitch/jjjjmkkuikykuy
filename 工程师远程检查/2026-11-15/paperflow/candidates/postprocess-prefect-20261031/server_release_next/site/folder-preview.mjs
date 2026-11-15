// Uppy keeps the original File objects and performs the upload. This view only
// groups its selected files by the first folder inside the chosen root.
export function bindFolderPreview(uppy, host, isActive, isLocked) {
  if (!host) return () => {};
  const list = host.querySelector('#selected-folders-list');
  const count = host.querySelector('#selected-folders-count');
  const pagination = host.querySelector('#folder-pagination');
  const pageInfo = host.querySelector('#folder-page-info');
  const previous = host.querySelector('#previous-folder-page');
  const next = host.querySelector('#next-folder-page');
  const perPage = 50;
  const filePageSize = 100;
  const collator = new Intl.Collator('zh-CN', { numeric: true });
  let page = 1;
  let scheduled = false;

  function showFiles(details, folder) {
    if (!details.open || details.dataset.loaded) return;
    details.dataset.loaded = 'true';
    const files = document.createElement('ul');
    files.className = 'folder-files';
    const more = document.createElement('button');
    more.type = 'button';
    more.textContent = '显示更多文件';
    let shown = 0;
    function appendNext() {
      const fragment = document.createDocumentFragment();
      for (const item of folder.files.slice(shown, shown + filePageSize)) {
        const row = document.createElement('li');
        const name = document.createElement('span');
        name.textContent = item.name;
        const remove = document.createElement('button');
        remove.type = 'button';
        remove.textContent = '移出';
        remove.setAttribute('aria-label', `从上传列表移出 ${item.name}`);
        remove.disabled = isLocked();
        remove.addEventListener('click', () => {
          if (isActive() && !isLocked()) uppy.removeFile(item.id);
        });
        row.append(name, remove);
        fragment.append(row);
      }
      shown = Math.min(shown + filePageSize, folder.files.length);
      files.append(fragment);
      more.hidden = shown >= folder.files.length;
    }
    more.addEventListener('click', appendNext);
    details.append(files, more);
    appendNext();
  }

  function render() {
    scheduled = false;
    if (!isActive()) return;
    const files = uppy.getFiles();
    const grouped = new Map();
    for (const file of files) {
      const path = typeof file.meta?.relativePath === 'string' ? file.meta.relativePath : '';
      const parts = path.split('/').filter(Boolean);
      const folder = parts.length > 2 ? parts.slice(0, 2).join('/') : parts[0] || '未归入文件夹';
      const name = (parts.length > 2 ? parts.slice(2) : parts.slice(1)).join('/') || file.name;
      if (!grouped.has(folder)) grouped.set(folder, { path: folder, files: [] });
      grouped.get(folder).files.push({ id: file.id, name });
    }
    const folders = Array.from(grouped.values()).sort((a, b) => collator.compare(a.path, b.path));
    const pages = Math.max(1, Math.ceil(folders.length / perPage));
    page = Math.min(page, pages);
    host.hidden = files.length === 0;
    count.textContent = `${folders.length.toLocaleString('zh-CN')} 个文件夹 · ${files.length.toLocaleString('zh-CN')} 个文件`;
    list.replaceChildren();
    const fragment = document.createDocumentFragment();
    for (const folder of folders.slice((page - 1) * perPage, page * perPage)) {
      const row = document.createElement('li');
      row.dataset.folderPath = folder.path;
      const details = document.createElement('details');
      const summary = document.createElement('summary');
      summary.append(document.createTextNode(folder.path));
      const itemCount = document.createElement('span');
      itemCount.textContent = `${folder.files.length} 个文件`;
      summary.append(itemCount);
      details.append(summary);
      details.addEventListener('toggle', () => showFiles(details, folder));
      row.append(details);
      fragment.append(row);
    }
    list.append(fragment);
    pagination.hidden = pages <= 1;
    pageInfo.textContent = `第 ${page} / ${pages} 页`;
    previous.disabled = page <= 1;
    next.disabled = page >= pages;
  }

  function schedule() {
    if (scheduled || !isActive()) return;
    scheduled = true;
    setTimeout(render, 0);
  }
  previous.onclick = () => { if (page > 1) { page -= 1; render(); } };
  next.onclick = () => { page += 1; render(); };
  for (const event of ['files-added', 'file-removed', 'cancel-all', 'upload', 'complete', 'error', 'upload-error']) {
    uppy.on(event, schedule);
  }
  return schedule;
}
