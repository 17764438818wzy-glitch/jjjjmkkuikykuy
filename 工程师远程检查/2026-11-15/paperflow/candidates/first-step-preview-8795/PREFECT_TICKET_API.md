# 页面工单计数接口约定

当前页面继续读取本机 `GET /api/v1/local-intake/preprocessing/summary`。请后端在同一响应中增加 `prefect_ticket_summary`；现有字段可保留。下面的数字仅是格式示例，不代表当前运行状态：

```json
{
  "prefect_ticket_summary": {
    "source": "prefect",
    "scope": "all_tasks",
    "available": true,
    "as_of": "2026-10-31T21:00:00Z",
    "counts": {
      "waiting_preprocessing": 308,
      "preprocessing_in_progress": 1,
      "waiting_llm": 0,
      "llm_attachment_in_progress": null,
      "llm_body_in_progress": null,
      "waiting_postprocessing": null,
      "postprocessing_in_progress": null,
      "waiting_rag_upload": null,
      "rag_in_progress": null,
      "waiting_nas_archive_delete": null,
      "completed": null,
      "preprocessing_passed": null,
      "preprocessing_failed": null,
      "preprocessing_unrecognized": null
    }
  }
}
```

- 当前页面的队列卡片展示**全部任务**，故 `scope` 为 `all_tasks`。不得将某个工作间的结果标作全部任务。
- 每个通过筛选、进入前处理的资料组只对应一张有持久身份的 Prefect 工单；正文和附件属于同一工单，计数单位为文件夹 / 组。
- 三项分别是等待前处理、正在前处理、前处理完成且确实交接进入等待 LLM 队列。后端按同一份可信状态快照对唯一工单分组计数，不从第一步分类统计或文件数量推算。
- `llm_attachment_in_progress` 与 `llm_body_in_progress` 分别是附件、正文正在处理的去重工单数。两条分支如同时处理同一工单，两项可以各计一次，不能把两项相加当作工单总数；同组多个附件也不能重复计入附件数量。
- `waiting_postprocessing` 和 `postprocessing_in_progress` 分别是等待后处理、正在后处理的去重工单数量。
- `waiting_rag_upload`、`rag_in_progress`、`waiting_nas_archive_delete` 和 `completed` 分别对应页面后续四个框。`completed` 须由后端确认工单要求的整个流程已完成，包括按实际要求核对 RAG、NAS 及收尾回执，不能仅凭某个 Prefect 子运行的完成状态判断。
- `preprocessing_passed`、`preprocessing_failed`、`preprocessing_unrecognized` 是前处理结果中去重后的工单数量，统计口径应由后端明确；它们不是第一步筛选结果，也不是当前在途数量。
- 结果未能确认、快照过期或无法核验 Prefect 连接时，`available` 为 `false`；仅个别步骤不可用时，其计数为 `null`。只有后端能确认实际为零时才返回数字 `0`。页面在无字段、不可用、接口失败或计数无效时显示 `—`。
- 失败、待核对、状态未知及其他步骤不能被悄悄归入这三个数或自动视为零。仅有前处理完成记录不代表已经成功交接到等待 LLM。
- 后续步骤也在同一工单状态快照上扩展计数；正文和附件即使分开处理，也不能把一次资料组重复算成多张工单。
- 读取接口只负责查询，不创建工单、不启动处理。工单创建、持久身份、状态流转、交接确认及开始 / 暂停控制由后端实现。

14205 的旧 `waiting_count` 是本地接收台账数，不能直接当作工单数量；`prefect_ticket_summary` 中尚未返回或暂不可核实的步骤显示 `—`。
