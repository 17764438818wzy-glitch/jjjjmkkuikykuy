# 后处理独立 Prefect 阶段候选

本目录只提供候选代码。导入不会注册、派发或启动任何任务；不会打开任一门禁、调用模型或上传 RAG。每个后处理阶段是现有文件夹 FlowRun 对应的一条独立 Prefect TaskRun，不创建新的文件夹小工单。`task_run_id` 是上游历史字段，仍指原文件夹 FlowRun；后处理 TaskRun 自己使用 `job_id`。

## 预约与计数

一张待处理的文件夹工单通过门禁、上游原回答完整性和数据库身份校验后，只能预约一次。阶段表以 `(task_id, group_id)`、原文件夹 FlowRun 和 `job_id` 分别约束唯一性；重启或代次变化不能自动重做相同文件夹。阶段记录一旦出现，就从上游“等待后处理”扣除，即使结果台账此时尚未建立。`job_id` 由原文件夹 FlowRun 确定性派生；每单 `task_key` 含该 job_id，仅订阅指定 key，避免领取历史任务。

阶段状态为 `reserved → submitting → submitted → executing → finished`。`reserved/submitting/submitted` 表示阶段已预约或等待执行；`executing` 仅表示已持久领取，展示“后处理中”还须批量核对真实 Prefect TaskRun 处于 RUNNING、阶段 key/tag/dynamic_key/原文件夹身份一致、owner PID 与启动时间仍然存活。观察不可用、TaskRun 结束但记录未收尾、owner 丢失或状态冲突均应显示未知，不能当作等待或自动重试。

提交结果未知会保留 `submission_unknown`；执行结果未知会保留 `execution_unknown`；只有收到业务侧明确且有证据的 `ConfirmedFailure` 才记录 `failed`。这些状态不属于等待，也不允许自动重新预约。`finished` 只代表阶段已结束，需结合独立结果台账和经过验证的交接回执判断“通过”或“等待 RAG”。不能把结果台账的预约本身当作完成。

`Coordinator.jobs_for_task(task_id)` 可一次读取该大工单的阶段预约及 PID、启动时间，供展示层与上游、结果台账按 `(task_id, group_id)` 互斥关联；它不查询 Prefect，也不证明执行活性。Prefect 的 TaskRun 和本机进程活性必须另行核验；任何来源不可核验时应如实显示未知。

## 必须注入的真实边界

- `task_gate(task_id)` 使用与任务暂停/删除相同的短期准入锁；`folder_gate(item)` 使用上游同一文件夹工单的 `folder-order-{attempt_id}.lock`。重活不占用短期任务锁。
- `postprocess_is_open()` 与 `rag_is_held()` 均须明确返回 `True`；异常、缺失、`None` 均拒绝预约或领取。持久门禁应在同一控制数据库内建立，且开放后处理时事务性确认 RAG 门禁仍持有。检查发生在预约、提交和执行领取边界，候选不会自行开放门禁。
- `verify_upstream(identity, generation)` 必须真实复用上游接收器会话、已登记回执、整个文件夹的原回答完整性及 SHA256 等校验，并返回同一身份、代次、冻结清单 SHA256 与 `raw_complete/session_current/not_deleted` 均为 `True` 的证据。回调不是独立证明；未完成真实实现前不能开放此阶段。
- `verify_execution(item, task_context)` 必须调用 `validate_prefect_stage` 检查真实 Prefect TaskRun，且仍需通过上游 `UpstreamFlowBinding` 检查原文件夹和大工单身份。`StageBoundHandoff` 保留上游材料核验并添加当前阶段 TaskRun 核验，以适配原文件夹已挂起的事实。
- `slot()` 应接入同目录上级已有的 `prefect_slot.postprocessing_slot(expected_api_url)`，其严格核对 Prefect 独立并发上限为 8。业务线程即使被取消也必须完成收尾后才释放名额，框架复用 `paperflow_prefect.resources.drain_thread`。合成测试中的 slot 是替身，尚不证明真实并发吞吐。
- `results.reserve(identity, generation, manifest_sha256)` 必须在业务解析之前写入持久结果意图并返回结果编号；之后通过 `success(identity, result_id, result, validate_receipt)`、`failure(identity, result_id, error)`、`unknown(identity, result_id, error)` 回写。适配已有 `ResultsStore` 时，通过必须关联真实阶段 claim 和独立回执复核，不能直接返回真值。
- `process_folder(identity, generation, item, accepted_hook, context_task)` 注入已核验的后处理桥接；只能返回带原身份的 `waiting_rag` 或 `receipt_held`，且必须调用一次进程内的 `accepted_hook(stage_result, validate_receipt)`，不能发送 LLM 请求或上传 RAG。验证闭包不进入 Prefect 持久化结果。

当前测试使用临时 SQLite、合成身份和 Prefect 运行对象替身；未连接真实 Prefect，未启动工作进程，也未处理任何真实论文。生产启用之前还需要将上述回调、任务暂停删除及前端展示与当前运行版本整合验证。
