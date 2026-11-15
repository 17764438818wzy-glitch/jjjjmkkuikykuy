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
    ['waiting_nas_archive_delete', 'waiting-nas-archive-count', 'waiting-nas-archive-hint', 'NAS归档及删除'],
    ['completed', 'completed-count', 'completed-hint', '运行完成'],
    ['waiting_postprocessing', 'waiting-postprocessing-count', 'waiting-postprocessing-hint', '等待后处理队列'],
    ['postprocessing_in_progress', 'postprocessing-count', 'postprocessing-hint', '后处理中'],
  ];

  const ledgerBasis = 'postprocessing-registered-ledger/1';
  const ledgerStages = ['waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload'];
  const positionStages = [...ledgerStages, 'rag_in_progress', 'waiting_nas_archive_delete', 'completed'];
  const hasLedger = (payload) => Object.prototype.hasOwnProperty.call(payload || {}, 'postprocessing_registered_ledger');
  function ticketPositions(payload) {
    const value = payload?.prefect_postprocessing_positions;
    return value?.available === true && value.basis === 'prefect-folder-stage-positions/1' &&
      typeof value.partial === 'boolean' && Number.isSafeInteger(value.unresolved_count) &&
      value.unresolved_count >= 0 && Number.isFinite(Date.parse(value.as_of)) &&
      ledgerStages.every((stage) => Number.isSafeInteger(value.counts?.[stage]) && value.counts[stage] >= 0) ? value : null;
  }
  function localWaiting(payload) {
    const value = payload?.local_postprocessing_waiting;
    return value?.available === true && Number.isSafeInteger(value.count) && value.count >= 0 &&
      typeof value.partial === 'boolean' && Number.isFinite(Date.parse(value.as_of)) ? value : null;
  }
  function registeredStage(payload) {
    const ledger = payload?.postprocessing_registered_ledger;
    const stage = ledger?.stage;
    if (ledger?.available !== true || ledger.basis !== ledgerBasis || ledger.current_delivery_verified !== false ||
        !/^[0-9a-f]{64}$/.test(ledger.revision || '') || !Number.isFinite(Date.parse(ledger.as_of)) ||
        stage?.available !== true || stage.basis !== ledgerBasis || stage.as_of !== ledger.as_of ||
        typeof stage.partial !== 'boolean' || !stage.counts || typeof stage.counts !== 'object') return null;
    return stage;
  }

  function serverObservationMatches(payload) {
    const ledger = payload?.postprocessing_registered_ledger;
    const stage = registeredStage(payload);
    const remote = ledger?.server_observation;
    const value = stage?.counts?.waiting_rag_upload;
    return !!(stage && remote?.available === true && remote.basis === 'paperflow-server-receipt-observation/1' &&
        remote.as_of === ledger.as_of && remote.observation_revision === ledger.revision &&
        remote.waiting_rag_upload === value);
  }

  function serverRagCount(payload) {
    const value = registeredStage(payload)?.counts?.waiting_rag_upload;
    if (!serverObservationMatches(payload)) return null;
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
  }

  function readTicketCounts(payload) {
    const summary = payload?.prefect_ticket_summary;
    const validSummary = summary?.source === 'prefect' && summary?.scope === 'all_tasks' && summary?.available === true;
    return metrics.map(([stage]) => {
      if (stage === 'waiting_postprocessing' && Object.prototype.hasOwnProperty.call(payload || {}, 'three_card_positions')) {
        return payload.three_card_positions?.waiting_postprocessing ?? null;
      }
      if (['rag_in_progress', 'waiting_nas_archive_delete', 'completed'].includes(stage)) {
        const positions = ticketPositions(payload);
        if (positions?.downstream_available !== true) return null;
        const value = positions.counts?.[stage];
        if (!Number.isSafeInteger(value) || value < 0) return null;
        if (stage !== 'waiting_nas_archive_delete') return value;
        const active = positions.counts?.nas_in_progress;
        return Number.isSafeInteger(active) && active >= 0 ? value + active : null;
      }
      if (ledgerStages.includes(stage) && Object.prototype.hasOwnProperty.call(payload || {}, 'prefect_postprocessing_positions')) {
        return ticketPositions(payload)?.counts?.[stage] ?? null;
      }
      if (stage === 'waiting_postprocessing' && Object.prototype.hasOwnProperty.call(payload || {}, 'local_postprocessing_waiting')) {
        return localWaiting(payload)?.count ?? null;
      }
      if (ledgerStages.includes(stage) && !hasLedger(payload)) return null;
      if (ledgerStages.includes(stage) && hasLedger(payload)) {
        if (stage === 'postprocessing_in_progress') return null;
        if (stage === 'waiting_rag_upload') return serverRagCount(payload);
        if (!serverObservationMatches(payload)) return null;
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
    const positions = ticketPositions(payload);
    const registered = registeredStage(payload);
    const local = localWaiting(payload);
    const observation = registered ? postprocessingTime({ postprocessing_as_of: registered.as_of }, '截至') : '';
    const total = document.getElementById('llm-folder-count');
    if (total) {
      const value = summary?.counts?.llm_processing_folders;
      const partial = summary?.llm_processing_folders_partial === true;
      const visible = Number.isSafeInteger(value) && value >= 0 && !(partial && value === 0) ? value : null;
      total.textContent = visible === null ? '—' : `${partial ? '≥' : ''}${number.format(visible)}`;
      total.setAttribute('aria-label', `LLM 处理中工单数量：${visible === null ? '暂不可用' : `${partial ? '至少 ' : ''}${number.format(visible)} 组`}`);
    }
    metrics.forEach(([stage, countId, hintId, label], index) => {
      const count = document.getElementById(countId);
      if (!count) return;
      const value = counts[index];
      const partial = value !== null && (stage === 'waiting_postprocessing' && payload?.three_card_positions ? payload.three_card_positions.waiting_postprocessing_partial === true :
        positionStages.includes(stage) && Object.prototype.hasOwnProperty.call(payload || {}, 'prefect_postprocessing_positions') ? positions?.partial === true :
        stage === 'waiting_postprocessing' && local ? local.partial :
        hasLedger(payload) && ledgerStages.includes(stage) ?
        registered?.partial === true : summary?.[`${stage}_partial`] === true);
      const visible = value === null || (partial && value === 0) ? null : value;
      count.textContent = visible === null ? '—' : `${partial ? '≥' : ''}${number.format(visible)}`;
      count.setAttribute('aria-label', `${label}数量：${visible === null ? '暂不可用或待核对' : `${partial ? '至少 ' : ''}${number.format(visible)} 组`}`);
      if (!hints.has(hintId)) hints.set(hintId, []);
      hints.get(hintId).push(value);
    });
    hints.forEach((values, hintId) => {
      const hint = document.getElementById(hintId);
      if (!hint) return;
      if (['waiting-postprocessing-hint', 'postprocessing-hint', 'waiting-rag-upload-hint',
           'rag-processing-hint', 'waiting-nas-archive-hint', 'completed-hint'].includes(hintId) &&
          Object.prototype.hasOwnProperty.call(payload || {}, 'prefect_postprocessing_positions')) {
        if (!positions || values[0] === null) {
          hint.textContent = 'Prefect 小工单状态暂不可用';
        } else {
          const stamp = postprocessingTime({ postprocessing_as_of: positions.as_of }, '截至');
          const pending = positions.unresolved_count ? ` · ${number.format(positions.unresolved_count)} 个工单状态待核对` : '';
          const handoff = hintId === 'waiting-postprocessing-hint' && Number.isSafeInteger(positions.upstream_count) && positions.upstream_count > 0 ?
            ` · LLM 收尾交接中 ${number.format(positions.upstream_count)} 个` : '';
          const label = hintId === 'waiting-rag-upload-hint' ? '已登记等待上传' :
            hintId === 'waiting-nas-archive-hint' ?
              `等待 ${number.format(positions.counts.waiting_nas_archive_delete)} · 处理中 ${number.format(positions.counts.nas_in_progress)}` :
            hintId === 'completed-hint' ? 'RAG、NAS 和约定清理已完成' : '按 Prefect 小工单统计';
          hint.textContent = `${label}${handoff}${pending}${stamp}`;
        }
        return;
      }
      if (hintId === 'waiting-postprocessing-hint') {
        if (Object.prototype.hasOwnProperty.call(payload || {}, 'local_postprocessing_waiting')) {
          const when = local ? postprocessingTime({ postprocessing_as_of: local.as_of }, '截至') : '';
          hint.textContent = local ? `已登记等待 · 按文件夹统计${local.partial ? ' · 部分状态待核对' : ''}${when}` : '登记状态暂不可用';
          return;
        }
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
          `服务器回执已核验 · 上传前仍需复核${observation}` : '服务器回执暂不可核验';
        return;
      }
      if (hasLedger(payload) && hintId === 'postprocessing-hint') {
        hint.textContent = registered ? `实际处理中数量暂不可用${observation}` : '登记状态暂不可用';
        return;
      }
      if (hintId === 'llm-processing-hint' &&
          (summary?.llm_processing_folders_partial || summary?.llm_body_in_progress_partial || summary?.llm_attachment_in_progress_partial)) {
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

  // Keep upstream, local waiting folders and remote receipts independent.
  let upstream = null, postprocessing = null, localPostprocessing = null, positionsSnapshot = null;
  let upstreamFlight = false, postprocessingFlight = false, localPostprocessingFlight = false, positionsFlight = false;
  let positionsTimer = null, positionsExpirySequence = 0, positionsGeneration = 0, positionsRefreshPending = false;
  let postprocessingGeneration = 0, postprocessingTimer = null, postprocessingRefreshPending = false;
  let postprocessingExpirySequence = 0;
  let expiresWall = 0, expiresMonotonic = 0;
  const monotonicNow = () => typeof performance !== 'undefined' ? performance.now() : Date.now();
  function clearPostprocessing() {
    postprocessingGeneration += 1;
    postprocessingExpirySequence += 1;
    if (postprocessingTimer !== null) clearTimeout(postprocessingTimer);
    postprocessingTimer = null;
    postprocessing = null;
    expiresWall = expiresMonotonic = 0;
    renderCurrent();
    window.dispatchEvent(new CustomEvent('paperflow:postprocessing-summary', { detail: null }));
  }
  function renderCurrent() {
    if (postprocessing && (Date.now() >= expiresWall || monotonicNow() >= expiresMonotonic)) {
      clearPostprocessing();
      return;
    }
    const previousSummary = upstream?.prefect_ticket_summary;
    const merged = globalThis.paperflowMergeThreeCardPositions(previousSummary, positionsSnapshot);
    const summary = { ...(previousSummary || {}), ...merged,
      counts: { ...(previousSummary?.counts || {}), ...merged } };
    const payload = { ...(upstream || {}), prefect_ticket_summary: summary, three_card_positions: merged, prefect_postprocessing_positions: positionsSnapshot,
      local_postprocessing_waiting: localPostprocessing, postprocessing_registered_ledger:
      postprocessing?.postprocessing_registered_ledger || { available: false } };
    render(readTicketCounts(payload), summary, payload);
  }
  async function refreshUpstream() {
    if (upstreamFlight) return;
    upstreamFlight = true;
    try {
      const response = await fetch('http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary', {
        cache: 'no-store', signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error('工单数据不可用');
      upstream = await response.json();
    } catch (_) {
      upstream = null;
    } finally {
      renderCurrent();
      window.dispatchEvent(new CustomEvent('paperflow:queue-summary', { detail: upstream }));
      upstreamFlight = false;
    }
  }
  async function refreshPostprocessing() {
    if (postprocessingFlight) { postprocessingRefreshPending = true; return; }
    postprocessingFlight = true;
    const generation = postprocessingGeneration;
    const startedWall = Date.now(), startedMonotonic = monotonicNow();
    try {
      const response = await fetch('http://127.0.0.1:14206/api/v1/local-intake/preprocessing/summary', {
        cache: 'no-store', signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error('服务器后处理观察不可用');
      const value = await response.json();
      if (generation !== postprocessingGeneration) return;
      const remaining = value?.postprocessing_observation_remaining_ms;
      if (!Number.isSafeInteger(remaining) || remaining <= 0 || remaining > 30000 ||
          Date.now() >= startedWall + remaining || monotonicNow() >= startedMonotonic + remaining) {
        throw new Error('服务器观察已过期');
      }
      if (postprocessingTimer !== null) clearTimeout(postprocessingTimer);
      postprocessing = value;
      expiresWall = startedWall + remaining;
      expiresMonotonic = startedMonotonic + remaining;
      const expirySequence = ++postprocessingExpirySequence;
      postprocessingTimer = setTimeout(() => {
        if (generation === postprocessingGeneration && expirySequence === postprocessingExpirySequence) clearPostprocessing();
      }, Math.max(0, Math.min(expiresWall - Date.now(), expiresMonotonic - monotonicNow())));
    } catch (_) {
      if (generation === postprocessingGeneration) clearPostprocessing();
    } finally {
      if (generation === postprocessingGeneration) {
        renderCurrent();
        window.dispatchEvent(new CustomEvent('paperflow:postprocessing-summary', { detail: postprocessing }));
      }
      postprocessingFlight = false;
      if (postprocessingRefreshPending) {
        postprocessingRefreshPending = false;
        refreshPostprocessing();
      }
    }
  }

  async function refreshLocalPostprocessing() {
    if (localPostprocessingFlight) return;
    localPostprocessingFlight = true;
    try {
      const response = await fetch('http://127.0.0.1:14206/api/v1/local-intake/postprocessing/waiting', {
        cache: 'no-store', signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error('本地等待队列不可用');
      localPostprocessing = await response.json();
    } catch (_) {
      localPostprocessing = null;
    } finally {
      localPostprocessingFlight = false;
      renderCurrent();
    }
  }

  async function refreshPositions() {
    if (positionsFlight) { positionsRefreshPending = true; return; }
    positionsFlight = true;
    const generation = positionsGeneration;
    try {
      const response = await fetch('http://127.0.0.1:14206/api/v1/local-intake/postprocessing/positions', {
        cache: 'no-store', signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error('工单位置不可用');
      const value = await response.json();
      if (generation !== positionsGeneration) return;
      const remaining = Date.parse(value?.as_of) + 30000 - Date.now();
      if (!ticketPositions({ prefect_postprocessing_positions: value }) ||
          !Number.isFinite(remaining) || remaining <= 0) throw new Error('工单位置暂不可用');
      positionsSnapshot = value;
      if (positionsTimer !== null) clearTimeout(positionsTimer);
      const sequence = ++positionsExpirySequence;
      positionsTimer = setTimeout(() => {
        if (sequence !== positionsExpirySequence) return;
        positionsSnapshot = null;
        renderCurrent();
        window.dispatchEvent(new CustomEvent('paperflow:ticket-positions', { detail: null }));
      }, Math.min(12000, remaining));
    } catch (_) {
      if (generation !== positionsGeneration) return;
      // Keep the last timed snapshot visible until its existing expiry.
    } finally {
      positionsFlight = false;
      if (generation === positionsGeneration) {
        renderCurrent();
        window.dispatchEvent(new CustomEvent('paperflow:ticket-positions', { detail: positionsSnapshot }));
      }
      if (positionsRefreshPending) {
        positionsRefreshPending = false;
        refreshPositions();
      }
    }
  }

  function resumePostprocessing() {
    clearPostprocessing();
    positionsGeneration += 1;
    positionsExpirySequence += 1;
    if (positionsTimer !== null) clearTimeout(positionsTimer);
    positionsTimer = null;
    positionsSnapshot = null;
    renderCurrent();
    window.dispatchEvent(new CustomEvent('paperflow:ticket-positions', { detail: null }));
    refreshPostprocessing();
    refreshLocalPostprocessing();
    refreshPositions();
  }
  if (typeof document.addEventListener === 'function') {
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') resumePostprocessing();
    });
  }
  if (typeof window.addEventListener === 'function') {
    window.addEventListener('pageshow', resumePostprocessing);
    window.addEventListener('focus', resumePostprocessing);
  }

  renderCurrent();
  refreshUpstream();
  refreshPostprocessing();
  refreshLocalPostprocessing();
  refreshPositions();
  setInterval(refreshUpstream, 5000);
  setInterval(refreshPostprocessing, 5000);
  setInterval(refreshLocalPostprocessing, 5000);
  setInterval(refreshPositions, 5000);
})();
