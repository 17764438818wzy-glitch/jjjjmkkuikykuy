const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'host-resources.js'), 'utf8');

function pageWithReplies(replies, serverReplies = null, nasReplies = null) {
  const nodes = new Map();
  const document = {
    getElementById(id) {
      if (id === 'server-resources-title' && !serverReplies) return null;
      if (id === 'nas-status-title' && !nasReplies) return null;
      if (!nodes.has(id)) nodes.set(id, { textContent: '', hidden: false, style: {} });
      return nodes.get(id);
    },
  };
  const repeats = [];
  const context = {
    document,
    AbortSignal,
    fetch: async (url) => {
      const reply = url.includes('8798') ? serverReplies.shift()
        : url.includes('8799') ? nasReplies.shift() : replies.shift();
      if (reply instanceof Error) throw reply;
      return { ok: true, json: async () => reply };
    },
    setInterval: (callback, ms) => { repeats.push({ callback, ms }); },
  };
  vm.runInNewContext(source, context);
  return { nodes, repeat: () => repeats[0].callback(), repeatServer: () => repeats[1].callback(),
    repeatNas: () => repeats[serverReplies ? 2 : 1].callback(), repeats };
}

const sample = {
  cpu_percent: 31.5,
  gpu_percent: 0,
  gpu_memory_used_mib: 1024,
  gpu_memory_total_mib: 8192,
  memory_percent: 50,
  memory_used_bytes: 16 * 1024 ** 3,
  memory_total_bytes: 32 * 1024 ** 3,
};

test('renders valid values, including an idle GPU and binary memory units', async () => {
  const page = pageWithReplies([sample]);
  await new Promise(setImmediate);
  assert.equal(page.nodes.get('host-cpu').textContent, '31.5%');
  assert.equal(page.nodes.get('host-gpu').textContent, '0%');
  assert.equal(page.nodes.get('host-gpu-memory').textContent, '显存 1.0 / 8.0 GiB');
  assert.equal(page.nodes.get('host-memory-size').textContent, '已用 16.0 / 32.0 GiB');
  assert.equal(page.nodes.get('host-resources-state').textContent, '已连接');
});

test('removes stale metrics when the monitor becomes unavailable', async () => {
  const page = pageWithReplies([sample, new Error('offline')]);
  await new Promise(setImmediate);
  await page.repeat();
  assert.equal(page.nodes.get('host-cpu').textContent, '—');
  assert.equal(page.nodes.get('host-gpu').textContent, '—');
  assert.equal(page.nodes.get('host-memory').textContent, '—');
  assert.equal(page.nodes.get('host-cpu-bar').hidden, true);
  assert.equal(page.nodes.get('host-resources-state').textContent, '暂不可用');
});

test('shows server resources independently and clears stale server data on failure', async () => {
  const server = { ...sample, cpu_percent: 12.4, gpu_percent: 36, memory_percent: 25 };
  const page = pageWithReplies([sample], [server, new Error('offline')]);
  await new Promise(setImmediate);
  assert.equal(page.repeats[1].ms, 1000);
  assert.equal(page.nodes.get('server-cpu').textContent, '12.4%');
  assert.equal(page.nodes.get('server-gpu').textContent, '36%');
  assert.equal(page.nodes.get('server-gpu-memory').textContent, '显存 1.0 / 8.0 GiB');
  assert.equal(page.nodes.get('server-memory').textContent, '25%');
  await page.repeatServer();
  assert.equal(page.nodes.get('server-cpu').textContent, '—');
  assert.equal(page.nodes.get('server-resources-state').textContent, '暂不可用');
  assert.equal(page.nodes.get('host-cpu').textContent, '31.5%');
});

test('shows real NAS capacity and clears it when the monitor is unavailable', async () => {
  const nas = { status: 'connected', storage_total_bytes: 2 * 1024 ** 4,
    storage_used_bytes: 0.5 * 1024 ** 4, storage_free_bytes: 1.5 * 1024 ** 4,
    archive_write_bytes_per_second: null };
  const page = pageWithReplies([sample], null, [nas, new Error('offline')]);
  await new Promise(setImmediate);
  assert.equal(page.repeats[1].ms, 1000);
  assert.equal(page.nodes.get('nas-status-state').textContent, '已连接');
  assert.equal(page.nodes.get('nas-storage').textContent, '25%');
  assert.equal(page.nodes.get('nas-storage-size').textContent, '已用 512.0 GiB / 2.0 TiB');
  assert.equal(page.nodes.get('nas-storage-free').textContent, '可用 1.5 TiB');
  assert.equal(page.nodes.get('nas-archive-speed').textContent, '—');
  await page.repeatNas();
  assert.equal(page.nodes.get('nas-status-state').textContent, '状态未知');
  assert.equal(page.nodes.get('nas-storage').textContent, '—');
  assert.equal(page.nodes.get('nas-storage-bar').hidden, true);
  assert.equal(page.nodes.get('host-cpu').textContent, '31.5%');
});
