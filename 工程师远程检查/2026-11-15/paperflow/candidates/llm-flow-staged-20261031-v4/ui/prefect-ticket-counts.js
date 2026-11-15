(() => {
  'use strict';

  const metrics = [
    ['waiting_preprocessing', 'waiting-preprocessing-count', 'waiting-preprocessing-hint', '等待前处理队列'],
    ['preprocessing_in_progress', 'preprocessing-count', 'preprocessing-hint', '前处理中'],
    ['waiting_llm', 'waiting-llm-count', 'waiting-llm-hint', '等待llm处理队列'],
    ['llm_attachment_in_progress', 'llm-attachment-count', 'llm-processing-hint', 'llm附件识别'],
    ['llm_body_in_progress', 'llm-body-count', 'llm-processing-hint', 'llm正文识别'],
    ['waiting_rag_upload', 'waiting-rag-upload-count', 'waiting-rag-upload-hint', '等待rag上传队列'],
    ['rag_in_progress', 'rag-processing-count', 'rag-processing-hint', 'rag处理中'],
    ['waiting_nas_archive_delete', 'waiting-nas-archive-count', 'waiting-nas-archive-hint', '等待nas归档以及删除'],
    ['completed', 'completed-count', 'completed-hint', '运行完成'],
    ['waiting_postprocessing', 'waiting-postprocessing-count', 'waiting-postprocessing-hint', '等待后处理队列'],
    ['postprocessing_in_progress', 'postprocessing-count', 'postprocessing-hint', '后处理中'],
  ];

  function readTicketCounts(payload) {
    const summary = payload?.prefect_ticket_summary;
    if (summary?.source !== 'prefect' || summary?.scope !== 'all_tasks' || summary?.available !== true) {
      return metrics.map(() => null);
    }
    return metrics.map(([stage]) => {
      // The user wants this card to remain a visual placeholder for now.
      if (stage === 'waiting_postprocessing') return null;
      const value = summary.counts?.[stage];
      return Number.isSafeInteger(value) && value >= 0 ? value : null;
    });
  }

  if (typeof module !== 'undefined' && module.exports) module.exports = { readTicketCounts };
  if (typeof document === 'undefined') return;

  const number = new Intl.NumberFormat('zh-CN');
  function render(counts, summary = null) {
    const hints = new Map();
    metrics.forEach(([, countId, hintId, label], index) => {
      const count = document.getElementById(countId);
      if (!count) return;
      const value = counts[index];
      count.textContent = value === null ? '—' : number.format(value);
      count.setAttribute('aria-label', `${label}数量：${value === null ? '暂不可用' : `${number.format(value)} 组`}`);
      if (!hints.has(hintId)) hints.set(hintId, []);
      hints.get(hintId).push(value);
    });
    hints.forEach((values, hintId) => {
      const hint = document.getElementById(hintId);
      if (!hint) return;
      if (hintId === 'waiting-postprocessing-hint') {
        hint.textContent = '全部任务 · 数量暂不显示';
        return;
      }
      const available = values.filter((value) => value !== null).length;
      if (hintId === 'preprocessing-hint' && available > 0 && summary?.preprocessing_in_progress_as_of) {
        const time = new Date(summary.preprocessing_in_progress_as_of);
        const timeLabel = Number.isNaN(time.getTime()) ? '' : ` · ${time.toLocaleTimeString('zh-CN', { hour12: false })}`;
        hint.textContent = `本地前处理中 · 不含领取核对${timeLabel}`;
        return;
      }
      if (hintId === 'waiting-llm-hint' && available > 0 && summary?.waiting_llm_as_of) {
        const time = new Date(summary.waiting_llm_as_of);
        const timeLabel = Number.isNaN(time.getTime()) ? '' : ` · ${time.toLocaleTimeString('zh-CN', { hour12: false })}`;
        hint.textContent = `${summary.waiting_llm_partial ? '已核对工单 · 部分资料待核对' : '本地交接及工单已核对'}${timeLabel}`;
        return;
      }
      if (available > 0 && summary?.basis === 'local_registered_orders' &&
          ['waiting-preprocessing-hint', 'preprocessing-hint', 'waiting-llm-hint'].includes(hintId)) {
        const time = new Date(summary.as_of);
        const timeLabel = Number.isNaN(time.getTime()) ? '' : ` · ${time.toLocaleTimeString('zh-CN', { hour12: false })}`;
        const verifiedLlm = hintId === 'waiting-llm-hint' && summary.waiting_llm_prefect_verified === true;
        const label = verifiedLlm ? (summary.partial ? '至少此数量已核对 · 其他资料待核对' : 'Prefect 工单已核对') :
          (summary.partial ? '已登记工单 · 可能还有资料未核对' : '本地已登记工单');
        hint.textContent = `${label}${timeLabel}`;
        return;
      }
      hint.textContent = available === values.length ? '全部任务 · 按工单统计' :
        available === 0 ? '全部任务 · 工单数据未就绪' : '全部任务 · 部分工单数据未就绪';
    });
  }

  let inFlight = false;
  async function refresh() {
    if (inFlight) return;
    inFlight = true;
    try {
      const response = await fetch('http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary', {
        cache: 'no-store', signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error('工单数据不可用');
      const payload = await response.json();
      render(readTicketCounts(payload), payload?.prefect_ticket_summary);
      window.dispatchEvent(new CustomEvent('paperflow:queue-summary', { detail: payload }));
    } catch (_) {
      render(metrics.map(() => null));
      window.dispatchEvent(new CustomEvent('paperflow:queue-summary', { detail: null }));
    } finally {
      inFlight = false;
    }
  }

  render(metrics.map(() => null));
  refresh();
  setInterval(refresh, 5000);
})();
