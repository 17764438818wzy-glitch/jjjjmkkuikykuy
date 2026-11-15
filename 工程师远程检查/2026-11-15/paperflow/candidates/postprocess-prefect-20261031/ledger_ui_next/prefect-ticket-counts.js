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

  const ledgerBasis = 'postprocessing-registered-ledger/1';
  const ledgerStages = ['waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload'];
  const hasLedger = (payload) => Object.prototype.hasOwnProperty.call(payload || {}, 'postprocessing_registered_ledger');
  function registeredStage(payload) {
    const ledger = payload?.postprocessing_registered_ledger;
    const stage = ledger?.stage;
    if (ledger?.available !== true || ledger.basis !== ledgerBasis || ledger.current_delivery_verified !== false ||
        !/^[0-9a-f]{64}$/.test(ledger.revision || '') || !Number.isFinite(Date.parse(ledger.as_of)) ||
        stage?.available !== true || stage.basis !== ledgerBasis || stage.as_of !== ledger.as_of ||
        typeof stage.partial !== 'boolean' || !stage.counts || typeof stage.counts !== 'object') return null;
    return stage;
  }

  function readTicketCounts(payload) {
    const summary = payload?.prefect_ticket_summary;
    const validSummary = summary?.source === 'prefect' && summary?.scope === 'all_tasks' && summary?.available === true;
    return metrics.map(([stage]) => {
      if (ledgerStages.includes(stage) && hasLedger(payload)) {
        if (stage === 'postprocessing_in_progress') return null;
        const value = registeredStage(payload)?.counts?.[stage];
        return Number.isSafeInteger(value) && value >= 0 ? value : null;
      }
      if (!validSummary || (ledgerStages.includes(stage) && summary.postprocessing_basis !== 'folder-stage-receipts/1')) return null;
      const value = summary.counts?.[stage];
      return Number.isSafeInteger(value) && value >= 0 ? value : null;
    });
  }

  if (typeof module !== 'undefined' && module.exports) module.exports = { readTicketCounts };
  if (typeof document === 'undefined') return;

  const number = new Intl.NumberFormat('zh-CN');
  function postprocessingTime(summary, label = '完整核验于') {
    const time = summary?.postprocessing_as_of ? new Date(summary.postprocessing_as_of) : null;
    return !time || Number.isNaN(time.getTime()) ? '' :
      ` · ${label} ${label === '截至' ? time.toLocaleString('zh-CN', { hour12: false }) : time.toLocaleTimeString('zh-CN', { hour12: false })}`;
  }
  function appendPostprocessingDiagnostics(hint, summary) {
    if (summary?.source !== 'prefect' || summary?.scope !== 'all_tasks' || summary?.available !== true ||
        summary?.postprocessing_basis !== 'folder-stage-receipts/1') return;
    const diagnostics = [];
    for (const [key, label] of [['postprocessing_held', '暂存待核对'], ['postprocessing_unresolved', '状态待核对']]) {
      if (!Object.prototype.hasOwnProperty.call(summary.counts || {}, key)) continue;
      const value = summary.counts[key];
      if (Number.isSafeInteger(value) && value > 0) diagnostics.push(`${label} ${number.format(value)} 组`);
      else if (value === null) diagnostics.push(`${label} —`);
    }
    if (diagnostics.length) hint.textContent += ` · ${diagnostics.join(' · ')}`;
  }

  function render(counts, summary = null, payload = null) {
    const hints = new Map();
    const registered = registeredStage(payload);
    const observation = registered ? postprocessingTime({ postprocessing_as_of: registered.as_of }, '截至') : '';
    metrics.forEach(([stage, countId, hintId, label], index) => {
      const count = document.getElementById(countId);
      if (!count) return;
      const value = counts[index];
      const partial = value !== null && (hasLedger(payload) && ledgerStages.includes(stage) ?
        registered?.partial === true : summary?.[`${stage}_partial`] === true);
      count.textContent = value === null ? '—' : `${partial ? '≥' : ''}${number.format(value)}`;
      count.setAttribute('aria-label', `${label}数量：${value === null ? '暂不可用' : `${partial ? '至少 ' : ''}${number.format(value)} 组`}`);
      if (!hints.has(hintId)) hints.set(hintId, []);
      hints.get(hintId).push(value);
    });
    hints.forEach((values, hintId) => {
      const hint = document.getElementById(hintId);
      if (!hint) return;
      if (hintId === 'waiting-postprocessing-hint') {
        if (hasLedger(payload)) {
          hint.textContent = registered && values[0] !== null ?
            `已登记等待 · 实际处理前复核原回答${observation}` : '登记状态暂不可用';
          return;
        }
        if (values[0] === null) {
          hint.textContent = '全部任务 · 工单数据未就绪';
        } else {
          hint.textContent = `已登记等待 · 实际处理前复核原回答${postprocessingTime(summary, '交接材料完整核验于')}`;
        }
        appendPostprocessingDiagnostics(hint, summary);
        return;
      }
      if (hasLedger(payload) && hintId === 'waiting-rag-upload-hint') {
        hint.textContent = registered && values[0] !== null ?
          `已登记等待 · 交付材料需复核${observation}` : '登记状态暂不可用';
        return;
      }
      if (hasLedger(payload) && hintId === 'postprocessing-hint') {
        hint.textContent = registered ? `实际处理中数量暂不可用${observation}` : '登记状态暂不可用';
        return;
      }
      if (hintId === 'llm-processing-hint' &&
          (summary?.llm_body_in_progress_partial || summary?.llm_attachment_in_progress_partial)) {
        const time = new Date(summary?.llm_processing_as_of);
        const timeLabel = Number.isNaN(time.getTime()) ? '' : `核对于 ${time.toLocaleTimeString('zh-CN', { hour12: false })} · `;
        hint.textContent = `${timeLabel}已核对的文件夹 · 其他工单或请求待核对`;
        return;
      }
      if (hintId === 'llm-processing-hint' && summary?.llm_processing_as_of) {
        const time = new Date(summary.llm_processing_as_of);
        const timeLabel = Number.isNaN(time.getTime()) ? '' : `核对于 ${time.toLocaleTimeString('zh-CN', { hour12: false })} · `;
        hint.textContent = `${timeLabel}按文件夹工单统计`;
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
      if (['postprocessing-hint', 'waiting-rag-upload-hint'].includes(hintId)) {
        if (available > 0) hint.textContent += postprocessingTime(summary);
        appendPostprocessingDiagnostics(hint, summary);
      }
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
      render(readTicketCounts(payload), payload?.prefect_ticket_summary, payload);
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
