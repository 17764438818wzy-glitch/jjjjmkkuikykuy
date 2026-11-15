# 等待前处理队列：前端接口约定

本机前端已接入以下只读接口；当前运行状态应实时查询，不能仅凭本文档判断：

```http
GET http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary
```

响应样例中的数字只是说明字段，不是实时数据：

```json
{
  "scope": "all_tasks",
  "waiting_count": 2,
  "waiting_available": true,
  "receiver_status": "running",
  "receiver_heartbeat_at": "2026-10-31T12:00:00+00:00",
  "upstream_scan_status": "complete",
  "needs_review_count": 0,
  "preprocessing_handoff_connected": false
}
```

- `waiting_count` 按本地成功核验并取得上游 `accepted` 确认的等待资料**组**计数；正文和附件属于同一组，不额外累加。上游未登记、未识别或核验失败的组不作为正常等待组。
- 只有 `waiting_available === true` 且 `waiting_count` 是非负整数时显示数字；否则显示 `—`。请求失败也显示 `—`，不要以 0 或上游注册数量代替。
- `receiver_status` 为 `stopped` 或 `interrupted` 时提示“同步中断”；为 `running` 只说明接收进程心跳正常，不说明所有资料都已接收。
- `needs_review_count` 为正整数时可提示“有待核对”。`upstream_scan_status=incomplete` 可表示仍在扫描，不应单独翻译为接收进程已中断。
- `preprocessing_handoff_connected=false` 表示实际下游领取尚未接通。前端不得自行递减；当前默认没有开放领取写接口。“前处理中”不由这个接口提供。

`Cache-Control: no-store`；状态库不可读时返回 503。系统首次接收、接收进程停止或保护状态待核等实际边界，参见同目录 README。
