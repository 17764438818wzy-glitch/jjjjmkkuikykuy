(() => {
  'use strict';

  const api = 'http://127.0.0.1:8797/api/workbuddy';
  const el = (id) => document.getElementById(id);
  const count = (v) => Number.isSafeInteger(v) && v >= 0 ? v.toLocaleString('zh-CN') : '—';
  let state = null;
  let refreshing = false;
  let testing = false;
  let accountDialog = null;
  let accountRows = [];

  function render(data) {
    state = data || null;
    const connected = data?.connected === true;
    const authorized = connected && data?.authorized === true;
    el('workbuddy-state').textContent = connected ? '网关已连接' : '网关暂不可用';
    el('workbuddy-session').textContent = authorized
      ? (data?.usage_available === true ? '用量已连接' : '用量暂不可用')
      : connected ? '用量暂不可用' : '等待连接';
    el('workbuddy-input-tokens').textContent = count(data?.input_tokens);
    el('workbuddy-output-tokens').textContent = count(data?.output_tokens);
    el('workbuddy-inflight').textContent = count(data?.in_flight);
    const slots = count(data?.prefect_active_slots);
    const limit = count(data?.concurrency_limit);
    el('workbuddy-prefect-slots').textContent = slots !== '—' && limit !== '—' ? `${slots} / ${limit}` : '—';
    el('workbuddy-success').textContent = count(data?.successful_requests);
    el('workbuddy-failed').textContent = count(data?.failed_requests);
    const stamp = data?.as_of ? new Date(data.as_of) : null;
    el('workbuddy-updated').textContent = stamp && !Number.isNaN(stamp.getTime())
      ? `每 1 秒刷新 · ${stamp.toLocaleTimeString('zh-CN', { hour12: false })}` : '每 1 秒刷新 · 等待数据';
    el('workbuddy-test-button').disabled = !authorized || testing || data?.domestic_only_test !== true;
    if (accountDialog) el('workbuddy-account-refresh').disabled = !authorized || testing;
    el('workbuddy-test-button').title = data?.domestic_only_test === true ? '' : '等待国内账号测试服务更新';
  }

  function showTestResult(status) {
    const labels = { testing: '测试中', success: '成功', failed: '失败', unknown: '结果未知', unavailable: '不可用' };
    const result = el('workbuddy-test-result');
    result.dataset.state = Object.hasOwn(labels, status) ? status : 'unknown';
    result.textContent = labels[result.dataset.state];
  }

  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    })[character]);
  }

  function renderAccounts(accounts) {
    accountRows = Array.isArray(accounts) ? accounts : [];
    const labels = { success: '成功', failed: '失败', unknown: '未知', disabled: '已禁用（未测试）', untested: '未测试' };
    const colors = { success: '#238367', failed: '#d35757', unknown: '#b37a25', disabled: '#91a0b3', untested: '#91a0b3' };
    el('workbuddy-account-rows').innerHTML = accountRows.length ? accountRows.map((account) => {
      const status = Object.hasOwn(labels, account.status) ? account.status : 'unknown';
      const realm = account.realm === 'cn' ? '国内版' : account.realm === 'intl' ? '国际版' : '—';
      const detail = account.error || (account.model ? `模型：${account.model}` : '');
      const button = account.enabled === true
        ? `<button type="button" class="control-button" data-wb-disable="${escapeHtml(account.uid)}"${testing ? ' disabled' : ''}>禁用</button>`
        : `<button type="button" class="control-button" data-wb-enable="${escapeHtml(account.uid)}"${testing ? ' disabled' : ''}>启用</button>`;
      return `<tr><td><strong>${escapeHtml(account.nickname)}</strong><div class="wb-muted">${realm}</div></td>`
        + `<td><span style="color:${colors[status]}">${labels[status]}</span>`
        + (detail ? `<div class="wb-detail">${escapeHtml(detail)}</div>` : '')
        + `</td><td class="wb-action">${button}</td></tr>`;
    }).join('') : '<tr><td colspan="3" class="wb-muted">暂无账号</td></tr>';
  }

  function setAccountBusy(busy) {
    testing = busy;
    renderAccounts(accountRows);
    render(state);
  }

  async function readAccounts() {
    const response = await fetch(`${api}/accounts`, { cache: 'no-store', signal: AbortSignal.timeout(12000) });
    const listing = await response.json();
    if (!response.ok || listing?.status !== 'available' || !Array.isArray(listing.accounts)) {
      throw new Error('账号列表暂不可用，请稍后核对');
    }
    return listing.accounts;
  }

  async function refreshAccounts() {
    if (testing || el('workbuddy-account-refresh').disabled) return;
    setAccountBusy(true);
    showTestResult('testing');
    let completed = 0;
    let enabled = 0;
    let pending = [];
    try {
      el('workbuddy-account-message').textContent = '正在读取最新账号…';
      renderAccounts(await readAccounts());
      const uids = accountRows.map(account => account.uid);
      for (let index = 0; index < uids.length; index += 4) {
        pending = uids.slice(index, index + 4);
        el('workbuddy-account-message').textContent = `正在检测 ${completed} / ${uids.length} · 本轮自动启用 ${enabled} 个`;
        // Each request covers at most four probes, not an unbounded whole round.
        const response = await fetch(`${api}/accounts/refresh`, { method: 'POST',
          headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ uids: pending }),
          signal: AbortSignal.timeout(120000) });
        const result = await response.json();
        const rows = result?.accounts;
        if (!response.ok || !Array.isArray(rows) || rows.length !== pending.length
            || new Set(rows.map(account => account.uid)).size !== pending.length
            || rows.some(account => !pending.includes(account.uid) || typeof account.enabled !== 'boolean'
              || !['success', 'failed', 'unknown'].includes(account.status))
            || !Number.isSafeInteger(result.enabled_count) || result.enabled_count < 0 || result.enabled_count > pending.length) {
          throw new Error(result?.error || '本批检测结果待核对');
        }
        const updates = new Map(rows.map(account => [account.uid, account]));
        renderAccounts(accountRows.map(account => updates.has(account.uid) ? { ...account, ...updates.get(account.uid) } : account));
        completed += pending.length;
        enabled += result.enabled_count;
        pending = [];
      }
      const statuses = new Set(accountRows.map(account => account.status));
      showTestResult(statuses.has('failed') ? 'failed' : statuses.has('unknown') ? 'unknown' : uids.length ? 'success' : 'unavailable');
      el('workbuddy-account-message').textContent = `已检测 ${completed} / ${uids.length} · 本轮自动启用 ${enabled} 个`
        + (uids.length ? '。未通过的账号请查看原因。' : '。暂无账号，请先在网关导入。');
    } catch (error) {
      renderAccounts(accountRows.map(account => pending.includes(account.uid)
        ? { ...account, status: 'unknown', error: '本批检测或启用结果待核对，请刷新列表后确认' } : account));
      showTestResult('unknown');
      el('workbuddy-account-message').textContent = `已检测 ${completed} / ${accountRows.length} · 本轮自动启用 ${enabled} 个。`
        + (error?.message || '本批结果待核对') + '；未自动重试。';
    } finally {
      setAccountBusy(false);
    }
  }

  function createAccountDialog() {
    if (accountDialog) return accountDialog;
    accountDialog = document.createElement('dialog');
    accountDialog.id = 'workbuddy-accounts-dialog';
    accountDialog.className = 'workspace-dialog upload-dialog';
    accountDialog.setAttribute('aria-labelledby', 'workbuddy-account-title');
    accountDialog.innerHTML = `
      <style>
        #workbuddy-accounts-dialog { max-height: min(80vh, 760px); }
        #workbuddy-accounts-dialog .wb-table-wrap { overflow: auto; max-height: 52vh; }
        #workbuddy-accounts-dialog table { width: 100%; border-collapse: collapse; font-size: 13px; }
        #workbuddy-accounts-dialog th, #workbuddy-accounts-dialog td { padding: 12px 6px; border-bottom: 1px solid #edf2f8; text-align: left; vertical-align: top; }
        #workbuddy-accounts-dialog th, #workbuddy-accounts-dialog .wb-muted { color: #91a0b3; font-size: 12px; font-weight: normal; }
        #workbuddy-accounts-dialog .wb-detail { color: #71859e; margin-top: 5px; font-size: 12px; overflow-wrap: anywhere; }
        #workbuddy-accounts-dialog .wb-action { text-align: right; white-space: nowrap; }
        #workbuddy-accounts-dialog .wb-action button { min-width: 0; padding: 5px 10px; color: #d35757; }
        #workbuddy-accounts-dialog .wb-action button[data-wb-enable] { color: #2b67bd; }
        #workbuddy-accounts-dialog .wb-refresh-help { margin: 12px 0 0; color: #71859e; font-size: 12px; }
        #workbuddy-accounts-dialog .dialog-actions { flex-wrap: wrap; }
      </style>
      <h2 id="workbuddy-account-title">账号线路测试</h2>
      <p class="dialog-description">基础测试只检测启用的国内账号；开始前会按论文线路重新检测并设置并发。</p>
      <p id="workbuddy-account-message" class="dialog-description" role="status">正在读取账号…</p>
      <div class="wb-table-wrap"><table><thead><tr><th>账号</th><th>测试结果</th><th class="wb-action">操作</th></tr></thead>
        <tbody id="workbuddy-account-rows"></tbody></table></div>
      <p class="wb-refresh-help">刷新账号会拉取最新列表并分批检测全部账号，接通的禁用账号将自动启用。</p>
      <div class="dialog-actions"><button id="workbuddy-account-refresh" type="button" class="control-button">刷新账号</button>
        <button id="workbuddy-account-close" type="button" class="control-button">关闭</button></div>`;
    document.body.append(accountDialog);
    el('workbuddy-account-close').addEventListener('click', () => accountDialog.close());
    el('workbuddy-account-refresh').addEventListener('click', refreshAccounts);
    el('workbuddy-account-rows').addEventListener('click', async (event) => {
      const button = event.target.closest('button[data-wb-disable], button[data-wb-enable]');
      if (!button || button.disabled || testing) return;
      const enabling = Object.hasOwn(button.dataset, 'wbEnable');
      const uid = enabling ? button.dataset.wbEnable : button.dataset.wbDisable;
      setAccountBusy(true);
      el('workbuddy-account-message').textContent = enabling ? '正在启用账号…' : '正在禁用账号…';
      try {
        const response = await fetch(`${api}/accounts/${enabling ? 'enable' : 'disable'}`, { method: 'POST',
          headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ uid }),
          signal: AbortSignal.timeout(45000) });
        const result = await response.json();
        if (!response.ok) throw new Error('unavailable');
        if (result?.status === (enabling ? 'enabled' : 'disabled')) {
          renderAccounts(accountRows.map(account => account.uid === uid
            ? { ...account, enabled: enabling, status: enabling ? 'untested' : 'disabled', error: '' } : account));
          el('workbuddy-account-message').textContent = enabling ? '账号已启用，线路尚未重新测试。' : '账号已禁用';
        } else {
          el('workbuddy-account-message').textContent = result?.error || (enabling ? '启用结果待核对' : '禁用结果待核对');
        }
      } catch (_) {
        el('workbuddy-account-message').textContent = enabling ? '启用结果待核对，请刷新账号状态后确认' : '禁用结果待核对，请刷新账号状态后确认';
      } finally {
        setAccountBusy(false);
      }
    });
    render(state);
    return accountDialog;
  }


  async function refresh() {
    if (refreshing || document.hidden) return;
    refreshing = true;
    try {
      const response = await fetch(`${api}/summary`, { cache: 'no-store', signal: AbortSignal.timeout(7000) });
      if (!response.ok) throw new Error('unavailable');
      render(await response.json());
    } catch (_) {
      render(null);
    } finally {
      refreshing = false;
    }
  }

  el('workbuddy-test-button').addEventListener('click', async () => {
    if (testing || el('workbuddy-test-button').disabled) return;
    testing = true;
    el('workbuddy-test-button').disabled = true;
    showTestResult('testing');
    const dialog = createAccountDialog();
    if (!dialog.open) dialog.showModal();
    renderAccounts([]);
    el('workbuddy-account-message').textContent = '正在读取账号…';
    try {
      const accountResponse = await fetch(`${api}/accounts`, { cache: 'no-store', signal: AbortSignal.timeout(12000) });
      const listing = await accountResponse.json();
      if (!accountResponse.ok || listing?.status !== 'available') throw new Error('accounts unavailable');
      renderAccounts(listing.accounts);
      el('workbuddy-account-message').textContent = '正在测试启用的国内账号，国际及已禁用账号不参与测试。';
      const response = await fetch(`${api}/accounts/test`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: '{}', signal: AbortSignal.timeout(110000) });
      const result = await response.json();
      if (!response.ok) throw new Error('test unavailable');
      if (Array.isArray(result?.accounts) && result.accounts.length) renderAccounts(result.accounts);
      el('workbuddy-account-message').textContent = result?.error || '这是基础测试；开始前会按论文线路重新检测并设置并发。';
      showTestResult(result?.status);
    } catch (_) {
      showTestResult('unknown');
      renderAccounts(accountRows.map((account) => account.enabled
        ? { ...account, status: 'unknown', error: '未收到测试结果' } : account));
      el('workbuddy-account-message').textContent = '无法获取完整测试结果，请稍后核对。';
    } finally {
      testing = false;
      renderAccounts(accountRows);
      render(state);
    }
  });

  refresh();
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
  setInterval(refresh, 1000);
})();
