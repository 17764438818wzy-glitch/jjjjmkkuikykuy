(() => {
  'use strict';

  const element = (id) => document.getElementById(id);
  const isNumber = (value) => typeof value === 'number' && Number.isFinite(value) && value >= 0;
  const isPercent = (value) => isNumber(value) && value <= 100;

  function showPercent(name, value) {
    const label = element(`host-${name}`);
    const bar = element(`host-${name}-bar`);
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

  function showResources(data) {
    const available = [
      showPercent('cpu', data?.cpu_percent),
      showPercent('gpu', data?.gpu_percent),
      showPercent('memory', data?.memory_percent),
    ];
    element('host-gpu-memory').textContent = `显存 ${sizes(data?.gpu_memory_used_mib, data?.gpu_memory_total_mib, 1024)}`;
    element('host-memory-size').textContent = `已用 ${sizes(data?.memory_used_bytes, data?.memory_total_bytes, 1024 ** 3)}`;
    element('host-resources-state').textContent = available.every(Boolean) ? '已连接' : available.some(Boolean) ? '部分不可用' : '暂不可用';
  }

  let inFlight = false;
  async function refreshResources() {
    if (inFlight) return;
    inFlight = true;
    try {
      const response = await fetch('http://127.0.0.1:8796/api/host-resources', { signal: AbortSignal.timeout(4000) });
      if (!response.ok) throw new Error('本机资源不可用');
      showResources(await response.json());
    } catch (_) {
      showResources(null);
    } finally {
      inFlight = false;
    }
  }

  refreshResources();
  setInterval(refreshResources, 1000);
})();
