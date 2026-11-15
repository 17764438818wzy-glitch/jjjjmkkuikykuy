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
    el('workbuddy-test-button').disabled = !authorized || testing;
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
        : '<span class="wb-muted">已禁用</span>';
      return `<tr><td><strong>${escapeHtml(account.nickname)}</strong><div class="wb-muted">${realm}</div></td>`
        + `<td><span style="color:${colors[status]}">${labels[status]}</span>`
        + (detail ? `<div class="wb-detail">${escapeHtml(detail)}</div>` : '')
        + `</td><td class="wb-action">${button}</td></tr>`;
    }).join('') : '<tr><td colspan="3" class="wb-muted">暂无账号</td></tr>';
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
      </style>
      <h2 id="workbuddy-account-title">账号线路测试</h2>
      <p class="dialog-description">只测试启用账号；测试可能更新 WorkBuddy 的账号冷却状态。</p>
      <p id="workbuddy-account-message" class="dialog-description" role="status">正在读取账号…</p>
      <div class="wb-table-wrap"><table><thead><tr><th>账号</th><th>测试结果</th><th class="wb-action">操作</th></tr></thead>
        <tbody id="workbuddy-account-rows"></tbody></table></div>
      <div class="dialog-actions"><button id="workbuddy-account-close" type="button" class="control-button">关闭</button></div>`;
    document.body.append(accountDialog);
    el('workbuddy-account-close').addEventListener('click', () => accountDialog.close());
    el('workbuddy-account-rows').addEventListener('click', async (event) => {
      const button = event.target.closest('button[data-wb-disable]');
      if (!button || button.disabled || testing) return;
      button.disabled = true;
      const uid = button.dataset.wbDisable;
      el('workbuddy-account-message').textContent = '正在禁用账号…';
      try {
        const response = await fetch(`${api}/accounts/disable`, { method: 'POST',
          headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ uid }),
          signal: AbortSignal.timeout(20000) });
        const result = await response.json();
        if (!response.ok) throw new Error('unavailable');
        if (result?.status === 'disabled') {
          renderAccounts(accountRows.map((account) => account.uid === uid
            ? { ...account, enabled: false, status: 'disabled', error: '' } : account));
          el('workbuddy-account-message').textContent = '账号已禁用';
        } else {
          el('workbuddy-account-message').textContent = result?.error || '未确认禁用，请核对账号状态';
          button.disabled = false;
        }
      } catch (_) {
        el('workbuddy-account-message').textContent = '未确认禁用，请核对账号状态';
        button.disabled = false;
      }
    });
    return accountDialog;
  }

  async function refresh() {
    if (refreshing) return;
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
      el('workbuddy-account-message').textContent = '正在测试启用的账号，已禁用账号不参与测试。';
      const response = await fetch(`${api}/accounts/test`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: '{}', signal: AbortSignal.timeout(110000) });
      const result = await response.json();
      if (!response.ok) throw new Error('test unavailable');
      if (Array.isArray(result?.accounts) && result.accounts.length) renderAccounts(result.accounts);
      el('workbuddy-account-message').textContent = result?.error || '已禁用账号未参与测试。点击“禁用”可手动停用账号。';
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
  setInterval(refresh, 1000);
})();
