# 前端联调：等待前处理数量

## 接口

```http
GET http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary
```

这是单独启动的本机候选接口，只适合前端与接口运行在同一台电脑上联调；不是已经部署的生产服务，也不会随电脑重启自动恢复。跨机器访问需要由部署方提供合适的后端代理，不能把浏览器所在机器的 `127.0.0.1` 当作本机服务地址。

计数单位是资料组。只有本地已经核验通过、持久记录接收，且尚未从该候选等待记录中移出的组才计数；不直接把上游登记数作为本地接收数。正文和附件保持对应，垃圾不进入可用输入清单。

```json
{
  "scope": "all_tasks",
  "waiting_count": null,
  "waiting_available": false,
  "as_of": "2026-10-30T18:00:00+00:00",
  "upstream_scan_completed_at": null,
  "oldest_verified_at": null,
  "needs_review_count": 0,
  "preprocessing_handoff_connected": false
}
```

上面是未扫描时的示例，不是实际数量。`waiting_available` 为 `true` 时，`waiting_count` 是非负整数；为 `false` 时它是 `null`。上游未确认、扫描失败或不完整、结果过期、相关资料失效或身份冲突待核时都可能不可用。`scope` 是全部被扫描的第一步任务，不能单凭它证明服务连接到了预期实例。

建议的显示逻辑：只有 HTTP 请求成功、`waiting_available === true` 且 `waiting_count` 是非负整数时显示数字；其他情况显示 `—`。本地状态库读取失败返回 HTTP 503，数量同样是 `null`。接口响应带 `Cache-Control: no-store`，允许本机 `localhost` 和 `127.0.0.1` 来源的浏览器 GET。

```javascript
async function getWaitingDisplay() {
  try {
    const response = await fetch(
      'http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary',
      { cache: 'no-store' },
    );
    if (!response.ok) return '—';
    const data = await response.json();
    return data.waiting_available === true &&
      Number.isInteger(data.waiting_count) && data.waiting_count >= 0
      ? data.waiting_count : '—';
  } catch {
    return '—';
  }
}
```

## 当前边界

- 候选接口已启动，但没有开启对真实上游的自动扫描，也没有真实接收数字；联调初始值是 `—`。扫描能力及其使用方式见本目录 `README.md`。
- `preprocessing_handoff_connected` 目前固定为 `false`。下一阶段实际开始处理后，还不会自动从该候选等待记录扣数；这部分要等消费端接线后再完善。
- 这次不提供“前处理中”数量字段或接口；后续按该阶段的实际状态单独整理。
- 上游删除任务可能移除交付副本和回执；目前没有跨阶段删除联锁。扫描调用的上游任务详情 GET 可能刷新上游异常任务状态，真实接线前需要核对。
