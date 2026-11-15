# 单篇处理候选交接

状态：单篇处理候选已接入 W1 原生论文子流程和 W3 单篇收尾入口。合成单篇正常链及模型明确失败后的人工重试整链已通过；尚未部署或加载验收版本，真实论文、实际模型、正式 RAG、正式 NAS、真实来源清理和用户验收均未执行。所有用户验收结论为“待验收”。

## 2026-11-02 模型结果复用补充

- 后处理失败后重试同一子工单时，已成功的单段正文和附件在核对来源、请求和响应后直接复用，不再进入模型名额或重复保存 `llm_response_saved`；分段正文及已保存但尚未写全文判定完成回执的全文判定也保留同样的响应复用规则。人工重试和暂停恢复继续沿用原请求身份及回执。
- 服务器仅有 started 或存在 unknown 历史时不能作为未发送依据；包括本地已有日志的 not_sent 恢复，保留 unknown 后续不得自动重发的保护。历史核验在等待模型名额前执行；本轮按单子工单单执行者运行，由调度确认旧进程退出并避免重复启动。
- 合成阶段全套 `85 passed`；补充 not_sent 旧 unknown 保护后，相关模型请求、重试、分段和全文判定检查 `51 passed`。W1 的 `tests/control/test_full_candidate_chain.py::test_api_to_real_paper_entries_and_server_cleanup[postprocessing]` 为 `1 passed`：合成后处理失败再重试同一子工单后，模型 HTTP、模型名额进入和成功响应凭据各为 1 次。均为隔离样例，不代表真实论文实测验收。

## 实际入口与归属

paperflow_v3.stages.pipeline.process_paper(*, identity, registered, settings, connection, admission, limits, catalog, paperflow_root, counter_factory, output_root, retry_id=None, opener=None) 接收 W1 已登记的同一篇资料，执行前处理、正文和附件 LLM、必要时全文专业判断以及正文/附件后处理；正文和附件可在同一小工单内并行。返回 paperflow-postprocessing-receipt/1，其中 claim_id 是该篇实际子 FlowRun ID；W3 根据同一子工单的服务器凭据执行入库、归档和清理。

identity 为 task_id, group_id, paper_id, receipt_id, attempt_id, parent_run_id, task_run_id；registered 包含资料身份及 receipt_path, receipt_sha256, library_root, paper。W1 在调用前使用 control.workspace.folder_settings 限定该篇独占工作根，output_root 位于该根内；正文、附件和阶段文件均归属于同一篇。stage_work_inventory Artifact 保存 identity 和 files，返回值也含 stage_work_files；W3 在最终清理前另合并第一步和自身新增的实际文件。前处理、正文、附件和后处理使用 W1 提供的共享 Prefect 资源名及大工单准入。

本窗口复用 UnifiedBackend.receive、Backend.prepare、paperflow_prefect.llm_queue.raw_request/run_partition_raw、原 preextract_request.request_once 和既有正文/附件后处理、封包脚本；未启动旧扫描器、旧本地等待队列或旧阶段消费者。W1 已使原模型起始间隔及退避记录继续使用共享配置根，处理文件仍按篇隔离。

## 阶段回执、失败与恢复

- 前处理取得共享名额并完成最后准入后写 preprocessing_started。只有登记身份、交接和必要文件 SHA256、容量状态及原 expected_components 分段契约均核对通过，才写 preprocessing_complete。服务器和本机交接均完好的成功前处理直接复用。
- preprocessing_failed 带 task_id, group_id, paper_id, receipt_id, attempt_id, processing_attempt_id, failure_reason, retryable, reason, error_type；计划不可用时另带 plan_status。仅本地操作明确抛出 OSError 且登记回执、正文、全部附件身份可核对时，标记 local_io、retryable=true；用户修复后可在原小工单人工重试。BadZipFile/UnicodeDecodeError 为 local_parse、retryable=false；计划不可用、历史或交接身份待核对均不可原样重试。暂停不记失败，不自动修复材料或重规划。W1 候选已消费这些状态。
- 每个模型处理项先写 llm_request_intent；最后准入成功后、实际传输前写 llm_request_started，字段含 component_id, request_id, kind, request_sha256, model_dir，随后以 llm_response_saved/llm_request_failed/llm_request_unknown/llm_request_not_sent 记录实际结果。开始回执表示进入已获准的发送边界，不单独证明上游收到请求。暂停前已获准的一次请求可收尾，下一次操作重新准入。
- 认证或参数的完整 HTTP 拒绝为 http_rejection、retryable=true；明确临时 HTTP 拒绝为 explicit_transient_response、retryable=true。HTTP 413 和输出长度截断分别为 http_payload_too_large、output_length_requires_changed_plan，均为 retryable=false，需调整材料或处理计划，不能原样重发。超时或没有完整拒绝证据的结果保持 llm_request_unknown。
- 明确失败的人工重试需要同一子 FlowRun 中已有 manual_retry Artifact，其 retry_id 与调用参数一致、状态为 scheduled 或 running。单段正文、各正文分段、附件和全文排名均使用同一 component_id、新的稳定 request_id，新事件记录 retry_of 和 retry_id；原失败日志保留，请求内容摘要不变，成功项不重做。若同一尝试再次明确失败，需新的人工重试 ID。
- 暂停时若本机已写日志但可证明 http_invoked=false、请求次数为 0 且没有响应，恢复后可在独立目录继续；新事件以 retry_of 关联原请求并带 resume_reason=not_sent、retry_id=null。未知结果不会自动重发。
- llm_response_saved.data.response_text 在 Prefect Artifact 保存完整原始响应；本机响应与服务器内容的 SHA256 须一致。单段正文及全部附件响应完整后，或分段正文、全部附件和全文专业判断响应完整后，才写 llm_folder_complete，带 status=raw_complete、ranking_complete=true。格式或字段无效属于后处理失败；完整但不合格的回答不会让已完成的 LLM 阶段倒退。
- 后处理逐组件保存成功、失败和结果；任何必需附件未完成时不向 W3 交整篇。人工重试只处理明确失败的后处理项，成功正文和附件复用。各阶段原始请求、响应和失败尝试在本次工作根中保留到满足 W3 收尾条件。

## 全文专业判断

单段论文使用自身 profile.routing_candidates；分段论文使用全部完整分段处理结果形成一份全文排序。容量不足时按组整理并保留覆盖、原始回复及合并后的学习单元映射；最终排名按全文关联度输出，不按段号、专业代码或出现次数排序。该步骤使用独立的排序/整理指令，复用原请求封装、容量计数和请求日志，不与旧完整预提取模板的最多三项要求冲突。

原排序及依据保留，后处理按顺序选择最多三个；additional_related_specialties 不作为投递目标，零有效专业不虚构目标。分段 unit_id_mapping 复用原 _map_part 的合并编号；选择项引用无法对应已合并学习单元时保留原回答并让后处理失败，不猜测局部编号。合成检查证明输入及身份覆盖，真实模型是否正确理解和排序须用户验收。

## 已执行检查与实际限制

- 上一次完整阶段检查：..\..\prefect-paperflow\.venv\Scripts\python.exe -X utf8 -m pytest -q tests\stages --tb=short，71 passed in 26.37s。随后增加交接材料存在性及身份核验，相关增量检查 test_preprocessing_boundary.py、test_paper_pipeline.py、test_llm_retry.py、test_partitioned_pipeline.py 为 27 passed in 9.24s。
- 最新完整合成重试链：tests/control/test_full_candidate_chain.py::test_api_to_real_paper_entries_and_server_cleanup[True]，1 passed, 1 warning in 124.67s。通过候选任务入口、临时 Prefect、同一原生父子 FlowRun、合成 HTTP 503、人工重试、实际 W2/W3 入口和服务器清理完成回执。分类/模型为合成 HTTP，RAG 为内存替身，NAS 和被清理资料均在临时目录；不是正式服务或真实资料验收。此前重试开始时页面仍显示旧失败的问题已由 W1 修正，并在此检查中通过。
- 阶段合成检查另覆盖单段、附件、首段、后段、全文排名及中间整理请求的明确失败重试，未发送暂停恢复，401/参数拒绝、408/504 未知、输出截断、已开始正文收尾和后续附件不启动。分段论文使用非空且不同的合并后单元引用，并把重试后的凭据交 W3 server_handoff.read_complete_handoff 检查。
- test_server_response_receipt.py 使用临时 Prefect，验证本机合成响应暂不可用时，仍可从服务器回执还原原始响应字节。正式 Prefect 的长响应容量及完整凭据仍需实际核验。

## 验收对应情况

| 标准 | 当前证据 | 仍需用户验收 |
| --- | --- | --- |
| G01、A01、A03、A05 | 候选原生父子流程调用本窗口和 W3；单篇合成链贯通 | 实际选择资料、部署入口、一次开始后的正式全流程 |
| A02、A24、A25 | 最后准入、已发请求收尾、未发送恢复及共享资源有隔离检查 | 实际暂停、重启、多工单共享资源和未知请求状态 |
| A06、A09、A15 | 前处理明确失败、正文/附件/分段/全文排名重试和成功项复用已做合成检查；503 原小工单重试整链通过 | 实际失败原因、人工操作及其他论文独立推进 |
| A14 | 完整 LLM 响应与后处理状态分开，缺段/缺附件/未完整结束不通过 | 实际服务器和页面观察后处理等待或失败 |
| A16、A21 | 本阶段独占工作文件清单与原始响应服务器回执已接 W3 | 正式服务器凭据、正式 NAS 和该篇真实文件清理范围 |
| A23、A26 | 一篇一份全文排名、最多前三及合并单元引用已做合成检查 | 实际模型判断、正式专业库映射及排名依据 |
| E01～E06 | 修改限 stages/、tests/stages/ 和本交接，沿用原处理原语及既有共享控制 | 合并版本按实际链路审查；用户验收尚未进行 |

最终验收入口、加载版本和逐项结果由调度窗口统一记录；本窗口不代用户勾选通过。
