(() => {
  'use strict';

  const element = (id) => document.getElementById(id);
  const isNumber = (value) => typeof value === 'number' && Number.isFinite(value) && value >= 0;
  const isPercent = (value) => isNumber(value) && value <= 100;

  function showPercent(prefix, name, value) {
    const label = element(`${prefix}-${name}`);
    const bar = element(`${prefix}-${name}-bar`);
    const available = isPercent(value);
    label.textContent = available ? `${Math.round(value * 10) / 10}%` : '—';
    bar.hidden = !available;
    bar.style.width = available ? `${value}%` : '0%';
    return available;
  }

  function sizes(used, total, divisor) {
    if (!isNumber(used) || !isNumber(total) || total === 0 || used > total) return '—';
    return `${(used / divisor).toFixed(1)} / ${(total / divisor).toFixed(1)} GiB`;
  }

  function showResources(prefix, data) {
    const available = [
      showPercent(prefix, 'cpu', data?.cpu_percent),
      showPercent(prefix, 'gpu', data?.gpu_percent),
      showPercent(prefix, 'memory', data?.memory_percent),
    ];
    element(`${prefix}-gpu-memory`).textContent = `显存 ${sizes(data?.gpu_memory_used_mib, data?.gpu_memory_total_mib, 1024)}`;
    element(`${prefix}-memory-size`).textContent = `已用 ${sizes(data?.memory_used_bytes, data?.memory_total_bytes, 1024 ** 3)}`;
    element(`${prefix}-resources-state`).textContent = available.every(Boolean) ? '已连接' : available.some(Boolean) ? '部分不可用' : '暂不可用';
    if (prefix === 'server') element('server-resources-scope').textContent = data?.gpu_count > 1
      ? '整机占用 · 首块 GPU · 每 1 秒更新' : '整机占用 · 每 1 秒更新';
  }

  function formatBytes(value) {
    if (!isNumber(value)) return '—';
    const divisor = value >= 1024 ** 4 ? 1024 ** 4 : 1024 ** 3;
    return `${(value / divisor).toFixed(1)} ${divisor === 1024 ** 4 ? 'TiB' : 'GiB'}`;
  }

  function showNasResources(_prefix, data) {
    const connected = data?.status === 'connected';
    const state = data?.status === 'unavailable' ? '目录不可访问' : connected ? '已连接' : '状态未知';
    element('nas-status-state').textContent = state;
    const total = data?.storage_total_bytes;
    const used = data?.storage_used_bytes;
    const free = data?.storage_free_bytes;
    const valid = connected && isNumber(total) && total > 0 && isNumber(used) && isNumber(free) && used <= total && free <= total;
    showPercent('nas', 'storage', valid ? 100 * used / total : null);
    element('nas-storage-size').textContent = valid ? `已用 ${formatBytes(used)} / ${formatBytes(total)}` : '已用 —';
    element('nas-storage-free').textContent = valid ? `可用 ${formatBytes(free)}` : '可用 —';
    const speed = connected ? data?.archive_write_bytes_per_second : null;
    element('nas-archive-speed').textContent = isNumber(speed) ? `${(speed / 1024 ** 2).toFixed(1)} MiB/s` : '—';
  }

  function startPanel(prefix, url, render = showResources) {
    let inFlight = false;
    async function refreshResources() {
      if (inFlight || document.hidden) return;
      inFlight = true;
      try {
        const response = await fetch(url, { signal: AbortSignal.timeout(prefix === 'nas' ? 2000 : 4000) });
        if (!response.ok) throw new Error('资源数据不可用');
        render(prefix, await response.json());
      } catch (_) {
        render(prefix, null);
      } finally {
        inFlight = false;
      }
    }
    refreshResources();
    document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshResources(); });
    setInterval(refreshResources, 1000);
  }

  startPanel('host', 'http://127.0.0.1:8796/api/host-resources');
  if (element('server-resources-title')) startPanel('server', 'http://127.0.0.1:8798/api/server-resources');
  if (element('nas-status-title')) startPanel('nas', 'http://127.0.0.1:8799/api/nas-status', showNasResources);
})();
