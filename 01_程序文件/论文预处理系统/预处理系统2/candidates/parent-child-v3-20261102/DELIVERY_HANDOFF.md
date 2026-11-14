# 入库、归档与清理交接（候选；用户验收待进行）

## 当前结论

本窗口已实现单篇收尾入口：读取本篇服务器处理凭据，使用正式配置连接正文和附件发布器，保存一份 NAS 原文及附件，并在全部条件成立后按明确文件清单清理。W1 已在 control/flows.py 的 _delivery_step、run_paper 写入调用；跨窗口完整父子流程、页面重试和完成状态投影仍待联调，不能将候选代码或隔离检查说成 G01 全链路已完成。

本窗口没有对真实论文、模型、正式 RAG、真实 NAS 或真实来源做写入和删除，也没有部署、重启或迁移历史任务。后续真实任务加载和观察由调度按用户授权统一执行。

本轮最终共享交接：`delivery/runtime.py` 及本窗口测试已在共享候选中，前端未改。额外的完整目标重复核验已撤回，保留入库阶段完成状态判断及原发布器、清理器各自的既有校验；归档、清理依次重新使用同一 NAS 名额。上一轮交付模块全量隔离结果为 `111 passed in 79.45s`；本次未扩大回归，仅针对阶段投影、资源释放、部分清理恢复和未知结果重新检查，`17 passed in 2.38s`。W1 仍在核对 `flows._delivery_step` 的取消语义，调度应待共同修复完成后统一加载并观察真实任务，以上结果不是已启动或实际业务验收证明。

## 单篇阶段名额审计（2026-11-02）

- 调度确认没有独立 cleanup 全局额度。候选入口现按 `RAG 名额 → 等待 NAS → NAS 归档名额 → 等待清理 → NAS 清理名额` 顺序推进；归档名额退出后才进入清理等待，清理重新申请既有 `nas` 名额，不新增或修改额度。已有部分 cleanup 回执时，恢复清理也须先申请 `nas`。RAG 的远端请求及其结果核验属于 RAG 本阶段，等待这些结果时仍占本阶段名额；不声称已验证真实服务吞吐。
- 已确认两个恢复路径原先会额外等待已完成阶段名额：RAG 已完成、NAS 未完成时仍重取 RAG；RAG 和 NAS 归档都完成、清理尚未开始时仍重新经过入库和归档两个阶段。候选入口现按服务器保存的当前论文及正式目标完成状态跳过已完成的入库和归档，清理仍按新约定申请一次 `nas` 名额；身份不符时仍走原检查，未知请求仍阻断，不改原有通道数或增加工作流锁。
- 定向检查 `66 passed in 28.31s`，覆盖入库和归档等待凭据顺序、RAG/NAS 名额释放、清理排队与占用 NAS、部分清理后恢复时仍要排队、阶段凭据保存失败时不开始下一操作，以及身份不符、已知失败和未知结果。临时 Prefect 服务上的共享名额（每项 1）验证 A 归档时 B 可入库，而 A 清理占用 NAS 时 C 归档必须等待。另将真实 `folder_status` 接到合成阶段凭据，核对等待入库、等待归档、等待清理及清理中的投影；W1 的 `tests/control/test_folder_status.py` 为 `9 passed in 0.05s`。论文、发布、归档和删除均为临时文件或替身；没有触碰真实 RAG、NAS、模型、来源或运行服务。
- 收尾全量首次回归为 `101 passed, 4 failed`：四项均是合成交接样本未附 W2 新要求的 attempt_id 和登记回执，更新本窗口测试样本并隔离 `llm_operations` 的服务器读取后，受影响四项定向通过。W2 随后合入 `counter_factory`，原直接调用其分段合成辅助函数的三个样本又因参数签名变化出现 TypeError；按 W2 已合入的四参数接口和可关闭合成计数器适配后，`test_manual_retry_handoff.py` 与 `test_processing_handoff_integration.py` 共同 `8 passed in 5.55s`。最终重新运行 `tests/delivery`，结果 `111 passed in 79.45s`；均为隔离验证。
- 队列前只新增与真实调度有关的文件状态检查：例如上次 NAS 重命名或清理删除后中断，旧文件回执仍为 `in_flight`，原入口会先写“等待归档/清理”、等名额之后才在原脚本发现结果未知。现根据已保存状态在写等待凭据前停止，已明确失败且未人工重试亦不进入队列；原归档和清理脚本保留各自保护。正文和附件已由原发布器核验后才写 complete，不新增重复的完整目标校验。
- 改动仅在本窗口候选 runtime.py 和自身隔离测试；没有重启或主动加载，不能据此宣称当前运行中的真实工单已验证。

## 给 W1 的实际接口

阶段投影对接：W3 在同一子 FlowRun 保存 `kind=delivery_stage`，`data` 为 `{"identity": 当前完整论文身份, "stage": 阶段值}`。阶段值依次为 `waiting_rag_upload`、`rag_in_progress`、`waiting_nas_archive`、`nas_in_progress`、`waiting_cleanup`、`cleanup_in_progress`。等待归档凭据只在 RAG 名额释放且当前论文全部目标完成后保存；归档进行中凭据只在取得 NAS 名额后保存；等待清理凭据只在 NAS 名额释放并有当前论文归档完成状态后保存；清理进行中凭据只在重新取得 NAS 名额后保存。清理逐文件完成事实仍使用现有 `cleanup` 回执，不另建清理队列或名额。W1 已按相同完整身份接收最新阶段凭据，且有合成投影检查；阶段凭据只用于状态投影，不代替实际业务完成及清理条件校验。恢复时已完成阶段不会重新写回早期等待状态；已有部分 cleanup 回执的恢复也根据最新等待/运行凭据区分排队与实际取得名额。此前 W1 提议的 `rag_complete` 不再另写一份，避免两套阶段凭据。

入口：paperflow_v3.delivery.runtime.deliver_paper(*, identity, registered, settings, admission, limits, first_step_work_root, retry_failed=False)。

- 这是同步函数，在当前论文子 FlowRun 中通过 await drain_thread(lambda: deliver_paper(...)) 调用。入口在需要原有脚本前调用 legacy.bind(settings)，可以在 native 路径绑定前先导入模块。
- settings 传原始配置的 Settings；入口自行调用 control.workspace.folder_settings 得到本篇工作根，不要传已 scoped 的 settings。W2 的 output_root 必须在这个工作根内。
- limits 传现有全局 Prefect 资源名，至少包含 rag、nas；admission 是该大工单的 Admission。每次真实 RAG 请求及每份 NAS 文件、每次清理都遵循最后一次准入。暂停前已获准的单次操作允许结束并保存，不增加额外原子同步。
- first_step_work_root 传 W1 本大工单调用 classify_and_register 的实际 work_root。它须位于配置的 data_root 或 classification_root 下，路径包含 task_id，并按 <work_root>/<group_id>/registration/receipts/<receipt_id>.json 登记。不同大工单使用各自根；不同论文使用各自 group 根。入口会读取父 FlowRun 的 registration 并核对身份、登记回执和论文清单。
- 输入是 W1 已登记、含完整 paper 的 registered，不需要调用方手工构造客户端或保存回调。入口自动读取同一个子 FlowRun 的服务器回执、正式 catalog、现有 RAG 配置和 NAS 目录；不创建测试专业库。成功返回 status=complete、cleanup_status=complete。
- 用户明确选择已知失败项重试时才传 retry_failed=True；已有成功请求、目标和文件不重做。结果未知的请求、NAS 重命名或删除仍停下核对，不自动重发。页面 retry 和精确失败项的展示由 W1/W4 接线，尚未完成实际用户验收。
- 此前发现附件原件目录创建被明确拒绝时可能只有 rag_effect_failed、没有 rag-attachments 快照；W1 现已在 control/retry.py 将该明确失败事件纳入重试判定，并增加对应候选检查。本窗口已有同一目录原 intent 的精确恢复逻辑；跨窗口实际联调仍待完成。
- 如果服务器已有 cleanup 记录，W1 应从父 FlowRun 的 registration 取回 registered 并直接调用此入口；不要重新运行依赖已删除本机资料的 W2。本入口依服务器保存的原清单继续尚未处理的文件，未知结果不重复删除。

## 凭据、路由和清理边界

入口从本子 FlowRun 核对 preprocessing_complete、全部必要 llm_response_saved、llm_folder_complete、postprocessing_component_complete、postprocessing_complete 和 stage_work_inventory。分段论文还须有覆盖全部正文分段的 llm_whole_ranking_complete。处理窗口的新交接格式已做合成联调。完整后处理交接在入口核对一次；正文、附件、归档和清理内部只检查各自的正式目标、完成状态与身份条件。

人工重试回执收口：W3 的 server_handoff 现将 W2 的 llm_request_failed 识别为该次请求的明确终态；旧 intent 和失败记录继续保留，同一组件后续由 W2 产生的成功响应按原交接校验。llm_request_unknown 或尚未确认的 intent 仍逐请求阻断，不能因为同组件另有成功响应而忽略。针对 W2 的实际 process_paper 人工恢复入口，使用合成传输分别覆盖单段正文、附件、两次连续失败后恢复、正文第一段、正文第二段和全文排名；W3 再使用实际封包校验、正文及附件发布原语向内存 RAG 替身投递。另用 W2 实际 run_raw_component 生成超时未知回执，验证 W3 阻断、W2 拒绝人工重发且只发出一次合成请求。上述检查未调用真实模型或 RAG。

正式专业使用配置中的 catalog_path 和原有 paperflow.package.resolve_routes；保持 W2 提供的全文排名顺序，最多三个，不创建库。正文使用 preextract_batch_delivery.delivery_publisher 和原 RagPublisher；附件使用原 _Delivery、原资料库和 Files 目录逻辑。attachment_delivery.py 为已有单次发送原语增加可恢复的明确失败/未发送标记，继续使用其原有 FileLock，没有新增任务锁。

ServerEffects 在 Prefect Artifact 保存同一子工单的逐请求 intent、not_sent、returned 或明确 failed。已确认的 HTTP 400、401、403、404、405、413、415、422 拒绝可在用户明确重试后再次发送原失败项；其他结果如连接中断、超时或无法确定的错误保持未知。成功目标的服务器证据不会重置；若还有失败或未知请求，不进入归档和清理。附件原有本机 intent 只有关联服务器明确失败凭据或可证明尚未发送时才恢复，并保留原记录；遇到无法确认的本机或服务器结果仍等待核对。

A02 补充：正式 RagflowClient 的 GET 在连接失败或部分 5xx 时内部最多尝试三次。TaskCheckedClient 现在复用该原生重试方法，并在每次底层 _request_once 前检查大工单准入；为本次读取浅复制客户端，不改原客户端实例或线上旧文件，也不新增锁。隔离测试使用真实 RagflowClient 及模拟网络 opener，覆盖连接错误、HTTP 503 后暂停、运行中继续原重试，以及已获准请求在暂停后完成。此检查不代表正式服务已加载或真实请求验收通过。

NAS 保存目标是配置 nas_destination 下该大工单/资料组的一个目录，逐一保存接收清单中的原文及应保存附件；不增加 NAS 文件指纹、哈希或下载回读。复制失败记录 failed 并在用户重试时只处理本次所属片段；远端重命名结果或服务器回执不明时保持待核对。

清理逐文件处理论文原始来源、交付副本、该组第一步独占工作区和该篇处理工作区。清理前校验 W2 已存服务器的 stage_work_inventory 仍属于本篇并在本机完整存在，另收集本窗口产生的工作记录；每个删除意图和结果先后保存在 Prefect。来源和 NAS 不得与工作根重叠，来源混杂目录不递归删除，未归档垃圾保留。W1 的 ReceiptStore 为每个 group 创建独占 registration/receipts.sqlite3，事务 finally 关闭连接；它与该篇其他独占记录一同清理。根外共享数据库、程序及其他论文不在清单内。

## 原有实现及兼容范围

复用了 paperflow.package.validate_package/resolve_routes、preextract_batch_delivery.delivery_publisher、paperflow.rag.RagPublisher、paperflow_prefect.attachment_publish._Delivery 和 ensure_original_folder、unified_attachments.receipt_root_for、prefect-paperflow 的 Settings/legacy，以及 W1 的 Admission、共享资源和服务器回执。旧扫描器、旧本机任务队列和旧阶段调度未作为新流入口。

旧 NAS ArchiveReader 依赖历史内容寻址对象和索引，本次按用户要求写入明确目录且不写旧索引。已检查本次候选任务页没有发现旧 NAS 原件浏览或下载入口；不能因此宣称旧 ArchiveReader 已兼容新归档，历史归档未修改。

## 隔离检查

候选根目录执行：..\\..\\prefect-paperflow\\.venv\\Scripts\\python.exe -m pytest tests/delivery -q --tb=short

先前完整交付模块检查为 81 passed in 54.91s；其后人工重试接口检查为 18 passed in 14.02s。阶段名额修改后的最新定向结果见上方“单篇阶段名额审计”，没有将旧结果冒充修改后的全量检查。人工重试检查结束后临时 Prefect 服务器关闭时出现一次日志输出流已关闭的提示；当时只读检查该临时端口已无监听。检查使用合成论文、临时文件和内存 RAG 替身；test_isolated_delivery_chain 使用临时 Prefect Artifact API 和原发布器；test_runtime 覆盖自动入口，但将 RAG、服务器、资源准入和封包依赖替换为隔离对象；test_processing_handoff_integration 将 W2 的实际合成处理结果交给 W3 服务器交接校验；test_runtime_import 在新进程检查先导入、后绑定。另在只读条件下按本机配置完成一次 native 源文件绑定检查。这些均不代表真实服务加载、线上处理、正式 RAG/NAS 写入或用户验收。

## 按标准交接（全部用户验收待进行）

| 编号 | 本窗口候选状态和证据 | 尚需联调或用户观察 |
| --- | --- | --- |
| G01 | 单篇收尾入口和处理交接已有隔离检查 | W1 将创建入口、父子流、W2、W3、前端和最终完成状态真正接通；由用户完成真实验收 |
| A01 | 子 FlowRun 身份贯穿服务器交接和收尾 | 核对实际一篇一小工单及提前逐篇推进 |
| A02 | 每请求、NAS 文件和删除按最后准入检查；暂停恢复有隔离检查 | 用户按实际暂停、恢复和共享资源观察 |
| A03 | 复用原业务脚本，不复用旧扫描/队列调度 | W1 核对实际父子调用链 |
| A04 | 服务器记录绑定子 FlowRun | 原生父子关系由 W1 实际联调 |
| A05 | deliver_paper 可连续运行收尾 | W1 接入后核对一次开始自动贯通 |
| A06 | 单篇异常保留失败或未知，原样向上抛出 | W1 投影真实失败点及其他篇独立推进 |
| A07 | 清理完成才返回本篇 complete | W1 父工单汇总和 W4 页面待联调 |
| A08 | 本次未写历史任务 | 交付时核对新旧任务实际基线 |
| A09 | 明确拒绝和明确文件失败可人工重试；未知不重发 | W1/W4 接入用户选择与原小工单恢复 |
| A10 | 不属于本窗口页面实现 | W1/W4 核对当前大工单展示 |
| A11 | 不属于本窗口第一步登记 | W1 核对暂停和恢复登记 |
| A12 | 实际入口读取配置的正式 catalog、RAG 和 NAS | 用户核对实际库 ID 与归档目录 |
| A13 | 不属于本窗口新建前自检 | W1 核对本次配置依赖 |
| A14 | 本入口要求必要 LLM 完成回执 | W2/W1/W4 核对模型与后处理独立状态 |
| A15 | 不完整正文或附件交接不能进入收尾 | 在实际失败场景验证保留与重试 |
| A16 | 清理原件、副本和两处独占工作根，含独占登记数据库 | W1/W2 接入后对照本篇清单及其他论文 |
| A17 | 不属于本窗口创建入口 | W1/W4 核对拒绝创建与待核对 |
| A18 | 本窗口清理清单包含 source_path 和交付 path | 用用户验收资料观察原位置及 NAS |
| A19 | 每个应保存文件成功才记 NAS 完成；不增加 NAS 指纹或回读 | 验证真实 NAS 成功与失败 |
| A20 | 收尾按篇执行，不等待其他篇 | W1 实际多篇调度验证立即清理 |
| A21 | 服务器记录上游阶段、请求身份、目标、NAS 和逐文件清理；校验 W2 工作清单 | 查看真实 Prefect 数据及本机记录确已清理 |
| A22 | 来源目录只删清单文件；排除未归档垃圾 | 验证混杂目录中的垃圾保留 |
| A23 | 以 W2 全文排序映射正式库；NAS 每篇一份 | 验证实际专业 ID、附件链接、归档位置 |
| A24 | 收尾每项读取 W1 准入，成功项及未知状态保留 | W1 验证服务重启后手动开始 |
| A25 | 收尾使用 W1 指定 rag、nas 全局 Prefect 资源名 | W1 验证多任务共享和独立暂停 |
| A26 | 原顺序正式映射 1/2/3/更多取前三；分段核对全文排名覆盖 | 用户验收不同篇的实际全文排名和库数 |
| A27 | 全部正文、附件目标及 NAS 完成后才清理；失败目标人工补做 | 验证三目标中一个失败时其余不重传 |
| E01 | 沿用原附件 FileLock 和 W1 的 Prefect 资源；本窗口未新增工作流锁 | 汇总最终代码时复核 |
| E02 | 完整交接在入口验证一次；各阶段保留不同业务前提 | 复核 W1 集成后实际调用 |
| E03 | 仅为原单次发送增加实际需要的恢复适配；未重建旧调度 | 复核最终跨窗口接线及复用 |
| E04 | 仅在暂停、明确拒绝及文件失败边界记录后抛出 | 验证未知结果未吞掉或显示成功 |
| E05 | 修改本窗口交付目录、测试目录及本交接 | W1/W2/W4 按自身目录完成 |
| E06 | 规则按论文身份、正式目标、事件状态和文件归属适用；没有样本或论文 ID 特判 | 用不同资料、失败和未知场景验收 |
