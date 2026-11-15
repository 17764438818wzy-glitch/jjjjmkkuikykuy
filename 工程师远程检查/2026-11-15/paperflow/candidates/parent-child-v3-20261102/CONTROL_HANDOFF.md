# 工单与控制交接：当前候选

更新时间：2026-11-02。候选目录为 `candidates/parent-child-v3-20261102/`。调度已将第一步服务自检最终修复加载到 8796，并完成现场自检复核；真实论文和业务验收仍由用户进行。未由本窗口发送真实模型请求、处理真实论文或写正式 RAG/NAS。

## 最新结论：本候选可交调度按既定方案加载

- W2 成功项复用修复合入后，W1 重跑此前失败的 `tests/control/test_full_candidate_chain.py::test_api_to_real_paper_entries_and_server_cleanup[postprocessing]`：`1 passed, 1 warning in 88.76s`。该断言实际确认同一原生子工单完成明确后处理失败的人工重试，`(模型 HTTP 次数, LLM 名额进入次数, llm_response_saved 条数) = (1, 1, 1)`；原响应未改变，合成 RAG、NAS 和清理链完成。使用临时 Prefect、合成论文/HTTP、内存 RAG 和临时 NAS，不代表真实业务验收。
- 修复后的最新控制定向复核为 `24 passed in 1.87s`；此前任务 API/工单快照 `7 passed in 62.61s`，父工单并发与同 ID 恢复组合 `6 passed in 157.29s`。W2 报告阶段全套 85 项、追加未知历史保护关联 51 项通过；W3 报告收尾全套 111 项及最后定向 17 项通过。所核对的控制、处理和收尾共 9 个业务文件在该次联合测试开始和结束时哈希前缀一致；本窗口业务修改冻结。
- W1 没有加载、启动或重试真实任务。调度可在复核部署、依赖、暂停和原父子身份后，按已有 `start` 重排 CRASHED 的原父子工单，对 10 个 FAILED 逐篇重新核验 `retry_decision` 后使用既有单篇重试。配置的通道数、隔离测试通过和候选可加载均不代表真实任务、模型、RAG/NAS 或长期并发已验收。

## 同一工单安全加载与续跑：供调度核对

- **调度已报告的执行进展**：在 fresh paused、七类名额均为 0、第一步 85 个请求最新均 complete、41 篇模型结果已保存且无 LLM unknown 的前提下，调度核对 PID/创建时间/命令后停止了 34 棵原父子引擎树；旧 worker 自然报告原父 CRASHED、原 33 子 CRASHED，原 10 FAILED 不变，所核对的 325 个旧归属进程均退出。旧 worker 树已停止，旧 8796 API 暂保留查询；调度未人工修改 FlowRun 状态、未新建工单。以上为调度报告，本窗口不重复执行停止。
- 不能假定关闭旧执行器后 33 个 RUNNING 子工单会自然变为 CRASHED。现有 `stop_process` 先终止子进程、紧接着终止 worker，worker 可能来不及向 Prefect 报告；实际安装的 Prefect 代码只有在 worker 观察到子进程非零退出后才主动建议 CRASHED。资源占用为零也不能单独证明旧进程已退出。
- 调度提出的先停明确的 `prefect.engine`、保留旧 worker 回报、再关闭启动器的顺序已按上方报告执行；此前要求逐篇已准入操作和第一步 Nimble 已核对收尾，逐个对应原 FlowRun 与实际进程身份，并核对整个专属子树退出。停止 worker 后需等待其心跳失效，再由统一入口加载新候选及配置。
- **本机 Supervisor 次序**：只读核对现场 34 个 RUNNING 父子 FlowRun，`infrastructure_pid` 是旧 worker 下的 `cmd.exe` 包装进程，**不是** `prefect.engine` 本身；抽样树为 `cmd.exe → workspace_supervisor 两层 Python → prefect.engine 两层 Python`，子工单下还有专属 WSL 辅助进程。当前安装的 Supervisor 在 Windows 等待 engine 返回并把退出码向上传递，worker 则在观察到非零退出后上报 CRASHED。应优先让已核对的叶子 engine 及其专属辅助进程退出，保留其 Supervisor、cmd 包装和旧 worker 等待回报；核对整棵对应子树均退出且服务器读回原 ID 的 CRASHED 后，再关旧启动器。不可先杀包装进程就把原子树视为已退出；状态未转或进程仍在时停下核对。此为代码与现场只读推断，实际退出和上报须逐个验证。
- 优先在任务保持暂停、已准入业务收尾后，逐个核对原父子 FlowRun 的 `infrastructure_pid`、所属旧 worker 进程树及实际进程退出；不要仅凭 PID 已不存在判断未被复用或忽略可能脱离进程树的 WSL/辅助进程。在旧 worker 仍能处理回报时结束已确认属于旧工单的执行进程并等待 Prefect 状态更新；停掉旧 worker 后仍为 RUNNING 的，必须再次核对其实际执行进程及其相关操作均已停止，再由调度用 Prefect 官方客户端对**该原 FlowRun ID**提出 `Crashed` 状态并读取结果；不得把 FAILED 改成 CRASHED，也不得掩盖模型/RAG/文件操作的未知结果。若身份或退出状态不清楚，该工单保留原状、停止续跑核对。
- 现有 `start` 只重排当时已是 CRASHED 的原父/子 FlowRun，ID 和原父子关系不变；仍显示 RUNNING 的不会被它重排。若某子工单随后才变为 CRASHED，现有接口允许再次调用开始；W2/W3 合并、登记历史路径修复和相关合成联合检查已完成。10 个 FAILED 必须逐篇用 `retry_decision` 复核确为允许的后处理失败，才走已有单篇重试；未知不得重发。
- 补充验证：在临时 Prefect 创建有 Deployment 的合成 FlowRun，不使用 `force`，依次提出 RUNNING、CRASHED、SCHEDULED，三次均 ACCEPT 且读回相应状态；这只验证接口状态转换，不证明现场任一原进程已经退出。临时测试服务器退出时 Windows 提示临时数据库仍被占用，状态验证不受影响。
- 2026-11-02 12:41 UTC 前后 W1 只读复核：父工单 RUNNING，43 个子 FlowRun ID 与调度基线完全一致、资料组互不重复，其中 33 RUNNING、10 FAILED；当前 10 个 FAILED 经现有 `retry_decision` 全部返回允许、阶段为 postprocessing，41 篇有原始模型与全文排名完成凭据。33 个 RUNNING 子工单记录的 infrastructure PID 和父工单 PID 此时均存在；这不单独证明进程身份或之后的状态，不能提前重排。实际关闭、加载和重试前须重新逐项读取。
- 同次只读核对：项目配置 `limits.local=10`、`limits.package=10`；Prefect 的 local/package 均 active、limit=10、active_slots=0；total_llm=50、body_llm=50、attachment_llm=20、rag=20、nas=2，均 active、当前占用 0。8796 的运行进程仍须统一加载新配置；数字不表示真实吞吐已通过。

## 10 组并发与队列接线：候选已冻结，尚未加载

- 用户最终目标：分类 10、前处理 10、后处理 10、RAG 20、NAS 2；模型仍为总 50 / 正文 50 / 附件 20。调度报告已通过原配置入口将 `local`、`package` 设为 10，Prefect 两项均读回 active 且占用为 0；当前 8796 仍缓存旧配置，待统一加载。W1 仅把分类并发设为 10，其余沿用动态 Prefect 共享名额。配置值不代表真实持续并发已验收。
- W1 的 `_process_step` 传 `counter_factory`，不再在控制线程创建或关闭离线计数器；共享候选的 W2 `pipeline.py` 与 `postprocessing.py` 已使用同一接口。W2 在实际后处理工作线程创建、使用并关闭计数器；最新跨阶段后处理重试检查见顶部。
- W3 最新约定为 `delivery_stage`，携带完整论文 `identity` 和 `stage`；W1 已对接该接口并完成定向隔离检查。`waiting_nas_archive` 投影为页面沿用的 `waiting_nas_archive_delete`；身份不符及结果未知不猜成排队，失败和实际完成凭据保留。最新后处理失败重试整链检查见顶部。
- 本轮父工单并发、等待前处理和快照口径已完成所列隔离检查；不修改前端，真实任务仍由调度统一安排加载及续跑。
- 登记历史根修复已加入候选：若当前组登记目录位于原历史根内，仅排除当前登记输出并保留其他历史清单及不可访问的历史根；当前输出目录若意外含历史清单则停下核对。每组保留其启动时的来源绑定，暂停后同组重入仍校验原摘要。隔离父工单测试将工作根本身作为历史根，使用真实第一步完成两组登记和原生派发。
- 本轮控制候选隔离结果：状态投影、等待前处理、登记历史、来源绑定、重试判定与收尾线程等最新定向检查 `24 passed in 1.87s`；父工单两组真实第一步、十组并发补位与暂停、API、原父子 ID 恢复合计 `6 passed, 1 warning in 157.29s`；正常及明确模型失败重试的单篇跨阶段全链 `2 passed, 1 warning in 219.99s`，这两项是在改用 `drain_thread` 之前通过的。W2 合入后最新的现场对应整链已按顶部结果复测。
- 任务总体状态现与子工单快照使用同一完整性判定；该最后一项控制修改后，`tests/control/test_api.py tests/control/test_preprocessing_queue.py` 隔离复核为 `7 passed, 1 warning in 62.61s`。
- **修复前失败证据，已由本文件顶部的最新整链通过结果替代**：`test_full_candidate_chain.py[postprocessing]` 使用临时 Prefect，先保存一次合成模型响应，再模拟后处理计数器失败，以原子工单 ID 经现有 API 人工重试并完成。修复前的重跑结果为 `(模型 HTTP 次数, LLM 名额进入次数, llm_response_saved 条数) = (1, 2, 2)`，期望 `(1, 1, 1)`，`1 failed in 95.16s`。因此当时没有重发模型 HTTP，但确实重占一次 LLM 名额并重复保存一条成功凭据；原响应未删除。全部均为合成测试，不涉及真实模型、论文或服务。
- 此测试的最终断言是 `assert (len(model_calls), len(llm_slot_entries), len(final_responses)) == (1, prior_llm_slots, 1)`，其中 `prior_llm_slots == 1`；修复前实际为 `(1, 2, 2)`，修复后断言通过。测试的临时目录、临时 Prefect、本机 HTTP、内存 RAG、临时 NAS 和合成 DOCX 均隔离；模拟 `postprocessing.process_body` 首次抛错、第二次调用真实后处理。它先校验失败可人工重试和子 ID 不变，再核对实际请求/名额/凭据次数。
- 另一次只读统计显示当前 10 个 FAILED 每篇均有 1 个已保存正文响应、0 个附件响应、无全文排名事件；这只描述当前十篇，不能替代分段与附件的通用恢复检查。
- 修复前问题位于 `stages/llm_operations.py:run_single_body` / `run_attachment` 对原成功请求的恢复，以及 `stages/llm_request.py:run_raw_component`。W2 已完成处理并报告同口径覆盖分段与全文排名；W1 已按上述现场对应整链复测，结果见顶部。
- W3 建议的取消收尾已核对并改用项目已有 `paperflow_prefect.resources.drain_thread`：原 `asyncio.to_thread` 的 awaiter 被取消后会立即结束，而业务线程还在运行；定向合成测试先复现该现象，再验证改后等待线程结束才传播取消，`tests/control/test_delivery_thread.py` 与计数器传递检查合计 `2 passed in 1.84s`。这只覆盖协程取消；强制结束整个进程不保证线程完成，仍必须先暂停并等待已准入操作收尾。若收尾线程在暂停状态下等待后续资源，取消可能一直等待恢复或线程退出，不能将其当成进程已结束。
- W2 另报告共享候选 stages 定向 `38 passed`、W2→W3 合成交接 `8 passed`，以及三篇合成慢单名额补位通过；仅为其隔离检查，实际配置、真实模型和真实吞吐未由此验收。

## 现场自检修复：已加载并复核

- 调度现场确认：项目实际配置的 Nimble 地址为 `http://127.0.0.1:11434/v1/systemone`；请求自检时 11434 无监听，其他 12 项通过，底层为连接拒绝 `WinError 10061`，本地模型 manifest 存在。原自检把底层连接异常统一显示为 `NimbleUnavailable`，丢失了可操作的中文原因。
- 用户已授权自检修复已确认的依赖未启动。当前候选仅在上述本机受管地址明确拒绝连接时，调用既有 `F:\nimble\Start-Nimble.ps1 -Quiet`，再用原 `nimble_dependency.probe_service` 读取版本与模型列表；二次确认成功才通过，并提示“已自动启动并复检通过”。模型未注册、非受管地址、服务响应异常、启动失败和超时仍显示未就绪，不下载模型、不更换地址、不发送推理、不创建工单。脚本调用有 25 秒上限。
- 调度对上一版现场复核：Nimble 已启动，版本及所需模型可读，但原 POST 直到客户端 120 秒超时仍未返回；启动它的 PowerShell 父进程已退出。该现象与后台进程继承捕获管道一致。隔离 PowerShell 回归复现旧调用等待后台子进程 `7.23` 秒；修改为普通持久日志文件后，同一组 3 项检查 `3 passed in 1.75s`，不等待后台进程退出。
- 当前候选已取消 `capture_output`，标准输入使用 `DEVNULL`，启动脚本输出保存到候选 `runtime/nimble-start-logs/nimble-preflight-*.log`；脚本失败时显示简短原因及日志路径，超时显示服务状态待核对及日志路径。未修改原启动脚本。新一轮 `test_preflight_nimble.py` 为 `12 passed in 13.06s`，`test_preflight.py` 为 `3 passed in 24.24s`；均未由本窗口启动或停止实际 Nimble。
- 调度加载最终版后，在实际 11434 无监听时发送 `POST /api/v1/prefect/preflight`：11.43 秒返回，Nimble 项为 true，提示“已自动启动并复检通过”；该次 NAS 项暂未确认。随后只读核对 NAS 目录可访问，再次完整自检为 13 项全部 true。两次原始结果分别保存在 `runtime/desktop/preflight-auto-repair.json` 和 `runtime/desktop/preflight-after-auto-repair.json`；首次管道卡住的失败证据保存在 `runtime/desktop/preflight-auto-repair-first-attempt.json`，不将该次冒充通过。
- 现场复核时 8796 API PID 为 2612、Nimble PID 为 14592；工单数为 0，`/api/ps` 显示已加载模型数为 0，此次 Nimble 日志中 POST 请求数为 0。实际页面已返回包含 120 秒等待和自动启动提示的新前端脚本。此证据仅覆盖依赖自检与恢复，不代表真实论文、模型推理、正式 RAG/NAS 写入或用户业务验收通过。

## 实际入口和边界

- `paperflow_v3/control/api.py:create_app` 提供依赖检查、创建、查询、开始、暂停和单篇人工重试。`server.py:create_configured_app` 从明确的环境配置创建应用。
- `flows.py:parent_order` 按已选本机目录调用原第一步的 `discover_groups`、`process_one_group`、`receive_handoff`，将发现计划与每组分类/登记凭据保存到 Prefect Artifact；一组合格即由 `parent.py:schedule_registered_paper` 创建一张真正的 Prefect 部署式子 FlowRun，不等待整批分类。
- `flows.py:paper_order` 校验原生父子关系和父工单登记凭据，以同一子 FlowRun ID 贯穿 W2 `stages.pipeline.process_paper`、W3 `delivery.runtime.deliver_paper` 和服务器清理回执。父工单只有分类结束、所有应处理子工单均完成且各自有清理完成凭据才完成。
- 每篇处理根为 `<配置 data_root>/paperflow-v3/<task_id>/<group_id>`，第一步根为同一任务下的 `first-step/<group_id>`；历史记录指向其他工作根时拒绝直接使用。W2 工作文件及 W3 逐文件清理由各自候选接口处理；未经完整 RAG、NAS 和服务器凭据核对不进入删除。
- 处理工作根按篇隔离，但模型连接从原项目 `Settings` 创建：原有 `request_start_root` 与 `dispatch_backoff_root` 仍位于配置的共享 `data_root`，不会因每篇工作根而变成每篇独立。当前配置的模型启动间隔由原模型配置决定；Prefect 名额使用同一组 `paperflow-connected-*`。
- 请求以最后一次准入为界。已获准的单次请求允许收尾，后续操作重新检查；未知结果保留原身份。第一步的同源未知记录、旧分类索引中的未知记录、模型/RAG/NAS/清理未确认结果不会因新工单或重试而自动重发。
- `worker.py` 是候选执行器启动入口：先核对项目和 Prefect 地址并使旧开始意图失效，再启动原 Prefect process worker，不自动创建缺失工作池。控制 API 启动也使旧开始意图失效；两个服务均就绪后才由用户手动开始。已被 Prefect 标为 `Crashed` 的原父/子 FlowRun 在手动开始时重排，同一 ID 和父级不变；业务失败仍须人工重试。若子工单的崩溃状态晚于开始操作出现，界面可再次手动开始。
- 明确的模型认证失败直接暂停所属任务；分类、模型、RAG 连接异常及 NAS 目标故障须经相应只读服务/目录复核确认共同依赖不可用才暂停。单篇内容错误不会因此暂停其他论文；暂停原因记录在父工单 `dependency_failure` Artifact 并通过 `stop_reason` 返回。

## HTTP 字段与页面约定

- `POST /api/v1/prefect/preflight` 返回 `ready, checks, checked_at`。`pipeline` 检查本候选第一步、W2/W3、父子部署名称、实际入口文件和工作目录；`prefect_deployment`、`paper_deployment`、`executor`、共享名额、项目配置、脚本、Nimble、模型列表、RAG 认证/库列表及 NAS 目录另列检查。Nimble 满足前述明确条件时会自动启动并复检；检查不发送推理、不写 RAG/NAS。Artifact 可读不证明可写，模型列表不证明账号额度或实际推理，NAS 系统可写标志不证明真实写入。创建、开始、重试由后端重新自检。
- `POST /api/v1/prefect/tasks` 使用 `{request_id: UUID, task_name, source_paths:[绝对本机文件夹]}`；创建为暂停状态，同一 `request_id` 绑定 Prefect 幂等键。`GET /api/v1/prefect/tasks/by-request/{request_id}` 找到时返回 `{found:true,resolved:true,task}`；查不到返回 `{found:false,resolved:false}`，不能据此换身份自动再建。
- `GET /api/v1/prefect/tasks` 的 `complete` 只表示列表快照读取完整；每项含 `task_id, task_name, parent_flow_run_id, desired_state, task_state, stop_reason` 和 `actions.start/pause={allowed,reason}`。`start.allowed` 只表示工单状态许可，开始时仍须通过自检。
- `GET /api/v1/prefect/folder-tickets?task_id=...` 只返回该父工单下的原生论文子工单。`complete=true` 要求分类计划已结束、没有未归组文件、分类回执齐全，且每个通过分类、完整交付并已登记的资料组恰有一张子工单；派发意图/未知但尚无子工单时为 false。快照完整不等于业务完成。
- 每篇 `stage` 使用 `preprocessing_in_progress, waiting_llm, llm_in_progress, waiting_postprocessing, postprocessing_in_progress, waiting_rag_upload, rag_in_progress, nas_in_progress, waiting_cleanup, cleanup_in_progress, completed, failed, unresolved` 等约定值；`failed_stage` 可为 preprocessing、llm、postprocessing、rag、nas、cleanup。前处理及模型活动必须有真实 started 回执；仅有 intent 不算运行。`llm_body_running` 与 `llm_attachment_running` 在未确认时为 `null`，不得当作 false。完整必要模型响应及全文专业判定保存后 `llm_result=passed`，不等待后处理。
- 运行中实际回执投影示例：前处理开始后 `stage=preprocessing_in_progress`；正文请求完成最后准入并开始后 `stage=llm_in_progress, llm_body_running=true`。两者由完整合成链中间快照检查；暂停、连接中断或服务器记录缺失时仍可为 unresolved/null。
- `GET /api/v1/prefect/tasks/{task_id}/classifications` 返回该父工单分类列表与 counts。分类结束、无未归组文件且记录齐全时 `complete=true` 并允许确认零；否则空类别为 `null` 或已观测下界。
- `POST /api/v1/prefect/tasks/{task_id}/folders/{folder_id}/retry` 只对已确认失败点调度原小工单，成功返回 `task_id, folder_id, ticket_id, retry_id, status=scheduled`。模型拒绝须有完整失败凭据且 `retryable=true`；413/输出长度需调整计划，不能原样重试。前处理仅 W2 明确标识的可恢复 `local_io` 可重试；身份、历史或结果未知不重发。
- 每篇 `last_retry` 为 `null` 或 `{retry_id,stage,status}`，status 为 scheduled、running、completed、failed、dispatch_unknown、unresolved。页面在重试前记住旧 retry_id；超时后只读查询，出现新的已确认身份可核对本次派发；没有新记录、读取失败或结果未知时继续待核对。409 `detail` 为结构化 `{code,message,last_retry?,retry_id?}`：`retry_dispatch_unknown`、`retry_already_dispatched`、`retry_not_allowed`、`paper_not_stopped`。依赖检查失败仍返回 `ready=false` 的检查详情。
- 依赖检查逐项有界等待。前端对 preflight、创建、开始、重试四类 POST 使用 120 秒超时；结果不明仍保留身份，不自动重发。普通查询超时按页面现有约定处理。
- “删除整个任务”语义尚未定案，本候选没有新增该接口；不能把成功后的逐篇清理当作任务删除授权。

## 唯一候选启动入口

调度已在现有 Windows 用户账户下使用此入口加载候选；后续加载或重启按调度安排执行，不应重复启动：

```powershell
Set-Location -LiteralPath 'F:\论文预处理系统\预处理系统2\candidates\parent-child-v3-20261102'
& 'F:\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe' -m paperflow_v3.control.launch
```

固定访问 `http://127.0.0.1:8796/`。入口在 8796 同时提供 W4 候选静态页面、API 和动态同源 `api-config.json`，无需修改 W4 的配置文件；任务弹窗填写当前电脑可读取的绝对文件夹路径。保留现有 8795 页面，并复用原本机资源采样函数提供 `/api/host-resources`，原 8795 页面可继续跨源读取。资源、服务器、NAS 和网关的其他独立展示服务仍使用各自现有地址，和工单就绪并非同一个判断。

默认直接读取现有 `F:\论文预处理系统\预处理系统2\prefect-paperflow\config.json`，其中实际 Prefect 地址为 `http://192.168.1.120:4200/api`；第一步取 `candidates\first-step-control-20261029`，页面取 `candidates\prefect-folder-frontend-20261101`。项目配置指定的正式 catalog、模型配置、NAS 目标和 Windows 凭据读取方式沿用现状，不复制或输出密钥，也不改这些文件。该配置当前允许业务执行，因此实际加载后仍须在页面自检并由用户明确创建、开始任务；自检和启动不等于用户验收通过。

该命令实际执行时会先检查页面、脚本和 8796 端口，随后在所选 Prefect 服务器更新控制会话以取消旧开始意图；若不存在则创建专用 process 工作池 `paperflow-v3-parent-child`，在其中创建或更新 `paperflow-v3-parent/parent` 与 `paperflow-v3-paper/paper` 两个候选 Deployment。入口自动取得两者 UUID 并传给 API 和该候选 worker，不需要用户填写。它不更改现有 `paperflow-unified-preprocessing` 池、旧 worker 或 `paperflow-connected-*` 全局名额。专用池及其默认队列必须未暂停且不设池级/队列级并发上限，以免等待子流程的父流程占满所有执行位；已有不明在线 worker 时拒绝启动，避免被其他执行器领单。

入口在当前进程运行候选 API，并启动一个通过 `control.worker` 清会话入口的 process worker。关闭终端会停止该 worker 的进程树；异常退出时 worker 也会检测到启动进程消失并停止。应先从页面暂停并等待已准入操作收尾，再关闭终端；强制中断正在进行的操作可能留下待核对结果。若异常退出后 Prefect 暂时仍显示旧 worker 在线，需等其心跳过期后再启动，不会替用户结束其他进程。服务重启后必须重新自检并手动开始，未知请求仍不能重发。工作池在线、前端文件可读和服务已启动均不证明模型实际输出、正式 RAG/NAS 写入或长期并发。

调度报告现场 8796 候选服务、专用池、两个 Deployment 和第一步自检最终修复均已加载。下方隔离测试证据使用临时 Prefect、临时文件和合成请求。

## 隔离检查与限制

- `tests/control/test_full_candidate_chain.py[False]`：临时 Prefect、合成 DOCX、合成模型 HTTP、内存 RAG 和临时 NAS，经候选创建/开始、原生父子 FlowRun、真实 W2/W3 入口到服务器 cleanup 与 API completed；中间快照验证实际前处理和正文模型运行。最近一次为 `1 passed, 1 warning in 92.96s`；测试通过 `run_flow` 执行原生 FlowRun，不是长期 worker，且是一篇单段、无附件的合成资料。
- 同文件 `[True]`：合成 HTTP 503 的明确模型失败经界面原小工单人工重试、保留原请求、重新保存响应并进入真实 W3 收尾；W2 前处理最新变更后重新通过，`1 passed, 1 warning in 112.21s`。该检查同样使用临时服务、内存 RAG 和临时 NAS。
- 控制模块其余测试在临时 Prefect 回归 `46 passed, 1 warning in 359.78s`；另有实际 process worker 与候选部署入口检查 `3 passed in 86.61s`。这是隔离子进程，不是正式服务或电脑重启验收。
- 统一入口与工作线程相关检查共 `6 passed in 125.70s`；随后加入真实页面文件、同源任务列表、8795 跨源资源接口核对的统一入口检查再次通过，`1 passed in 44.05s`。使用临时 Prefect 实际启动候选服务及专用 process worker，检查自动生成的父子 Deployment 身份传入工作进程、旧开始意图失效、占用/限额工作池的拒绝，以及启动进程异常退出后 worker 停止。页面只检查了 HTTP 和静态文件，未做用户浏览器功能验收。
- 隔离检查完成时，曾只读查询到专用池及 Deployment 不存在、8796 无监听；之后调度已实际加载候选，此旧观察不代表当前状态。
- `tests/control/test_resume_child.py` 在临时 Prefect 强制父、子 FlowRun 均为 Crashed，手动开始后同一父子 ID 重排并继续；覆盖子崩溃晚报告后再次手动开始，`1 passed, 1 warning in 32.75s`。
- `tests/control/test_parent_pipeline.py`：两组合成资料使用原第一步分类/交付/登记，第二组开始前第一篇已生成真实父子关系，`1 passed in 18.96s`。
- `tests/control/test_shared_failure.py`、`test_retry_decision.py`、`test_folder_status.py`、`test_api.py` 分别覆盖明确公共故障、未知/明确失败、前处理及模型实时投影、重复请求和原 FlowRun 手动重试。结果均只代表隔离检查；最新整组结果见下方回归记录。
- W2/W3 各自交接包含多段、附件、全文排名、目标库、逐文件归档清理等独立合成检查。正式账号权限、实际模型输出、正式专业库、NAS、真实来源删除、长期并发和服务/电脑重启均未验收。

## W1 逐项交接

| 标准 | 当前实现及检查 | 未完成或用户验收证据 |
| --- | --- | --- |
| G01 | API、第一步、原生父子、W2/W3 真实入口在一篇合成单段流程贯通；最新前处理代码下人工重试整链回归通过 | 真实论文、正式目标、服务器记录由用户验收 |
| A01 | `classify_and_dispatch` 按组登记派生；两组合成派发检查 | 多附件真实论文的同一工单及逐篇推进 |
| A02 | `Admission`、共享名额及每次业务边界准入；合成暂停检查 | 真实暂停、已获准请求收尾、下一操作不开始 |
| A03 | `parent_order/paper_order` 调用第一步及 W2/W3 原单次脚本 | Prefect 运行记录核对未启旧扫描/旧队列 |
| A04 | `arun_deployment(as_subflow=True)` 和实际父 TaskRun；隔离检查 | Prefect 页面展开父子树及重试后的同一 ID |
| A05 | 父子入口自动调用 W2/W3；合成完整链 | 用户一次开始后真实全链自动推进 |
| A06 | 单篇失败留在本篇；共同依赖经实际错误和只读复核暂停所属任务 | 多篇隔离及真实服务故障原因 |
| A07 | `task_state` 与 `_wait_for_completion` 要求各篇清理完成 | 失败、未识别、未知及全部完成四种界面状态 |
| A08 | 新任务限候选 Deployment 和独占根，旧任务不迁移 | 交付前后的真实历史任务基线 |
| A09 | 明确失败按同一子 FlowRun 人工重试，未知阻断；最新代码合成 503 恢复 | 正文/附件/目标库真实恢复 |
| A10 | `folder_status` 读取真实开始、终态及清理回执；合成中间快照 | 两个任务切换、正文与附件同时活动及不完整数据 |
| A11 | 暂停时分类可收尾但登记/派发需再准入；同组幂等 | 分类过程中暂停、恢复一次登记 |
| A12 | W3 使用配置 catalog 和 NAS；测试使用内存 RAG/临时目录 | 实际正式专业库与 NAS 路径 |
| A13 | 自检逐项核对，已确认的本机 Nimble 未启动时自动启动并复检；现场后续自检 13 项通过 | 真实写入、实际推理和长期运行仍由用户验收 |
| A14 | 必要原响应与全文排名完成后独立投影 LLM | 后处理等待/失败时页面及服务器凭据 |
| A15 | W2 完整交接才进入 W3；工单失败单独展示 | 必需附件失败时该篇不发布 |
| A17 | 创建前后端重查依赖；创建身份可只读核对 | 断开依赖时不建父/子工单及页面原因 |
| A20 | 每张子工单独立完成 W3，不等待其他篇 | A 已清理、B 仍处理的实际状态 |
| A21 | Prefect Artifact 保存身份、请求、阶段与清理凭据；独占工作根可清理 | 正式服务器凭据完整性与该篇本机清单 |
| A22 | 第一环节分组保留未归组；W3 只删清单成员 | 原目录垃圾保留、正文附件已清理 |
| A24 | 控制/执行器会话重置，手动开始重排原 Crashed 父子；隔离真实 worker 检查 | 实际服务/电脑重启及未知请求阻断 |
| A25 | 任务独立准入，共用配置的 Prefect 全局资源 | 两任务同时运行、暂停 A 而 B 继续 |
| A27 | 子工单完成依赖 W3 全部目标、NAS、清理凭据 | 三目标一项失败时不清理，重试不重传成功目标 |
| E01 | 沿用现有第一步资源锁、Prefect 共享名额和最后准入，无新增工作流锁 | 合并后核对边界 |
| E02 | 保留外部边界与身份核验 | 跨 W2/W3 检查是否重复校验 |
| E03 | 复用原处理与收尾脚本，新增控制入口 | 检查不调用旧消费者 |
| E04 | 明确失败、未知和暂停分别记录并向上抛出 | 实际异常原因与原身份 |
| E05 | 本窗口仅改 `control/`、`tests/control/`、本交接 | 最终文件清单 |
| E06 | 按资料组、工单身份和回执状态通用处理，不针对论文 ID | 多种资料及失败/未知场景 |

所有用户验收结论目前为“待验收”。

## 2026-11-02 客户端复用性能候选（隔离回归通过，现场待验收）

- **W1 结论：定向检查和独立复核完成，可交调度按既定流程加载此候选；加载不等于真实速度、暂停恢复或业务验收通过。** 本次性能版 W1 运行代码范围仅 `paperflow_v3/control/api.py`、`parent.py`、`flows.py`；对应测试为 `tests/control/test_client_contexts.py`、`test_delivery_thread.py`、`test_parent_concurrency.py`。WPS 规则不在本次性能版范围。
- 正式现场只读堆栈抽样：8796 读取任务准入时反复构造异步 Prefect 客户端并加载系统证书；多个已暂停子工单的交付业务线程停在 `admitted_sync_slot -> Admission.running_sync -> get_client -> ssl.create_default_context`。暂停时仍逐次读取状态是原有行为，反复构造客户端是实证开销。
- 候选代码使用 Prefect 原生 `AsyncClientContext` 按 API 请求复用；独立的 preflight 不包裹，创建任务先完成原有参数验证与依赖检查，通过后才进入上下文。分类和交付各在线程内使用 `SyncClientContext`；不同线程不共用客户端，不缓存准入状态，不改轮询周期、名额、服务地址、认证或 TLS 校验。
- 父子 Flow 主协程的变量读取使用 Prefect 现有 Flow 客户端：隔离父子 Flow 在两次真实 `Admission.running()` 前后取得的是同一个已推断客户端，故未再给主 Flow 增加第二套客户端；此结论不包含 Prefect 全局名额内部的客户端。
- 纯内存模拟下同一文件夹列表请求创建的客户端由 9 个降为 1 个，响应除时间戳外一致；两次读取当时的动态真实数据时，原代码和原生请求上下文均在 50 秒超时，只观察到证书上下文创建由 8 次降为 1 次，不能据此宣称真实页面延迟已解决。`test_client_contexts.py` 与 `test_delivery_thread.py` 共 3 项通过；`test_parent_concurrency.py` 1 项通过，覆盖 10 个相互独立的分类线程及父子 Flow 客户端复用。`test_api.py`、`test_parent_pipeline.py`、`test_resume_child.py` 和完整合成链的后处理失败重试用例共 `6 passed, 1 warning in 112.59s`；均在临时 Prefect 和模拟外部目标上执行。
- 代码文件位于当前共享候选目录；没有由本窗口重启服务、开始或重试任何工单。已运行进程是否加载旧版、新建子进程是否读到新版，须由现场按进程和导入时点核实，不能把磁盘修改等同于整体加载或验收。
- 独立复核发现安装版 Prefect 原生上下文的一个异常边界：进入上下文时若其版本检查抛错，内部已打开客户端未主动关闭；同步及异步情况均仅在纯内存模拟复现，尚未观察到现场泄漏。进入成功后的正常返回、业务异常和请求取消均已在隔离检查中确认关闭。候选未修改 Prefect 版本检查或 TLS 校验。
- 上述回归对应控制文件 SHA-256：`api.py` 为 `7eba502aa5c5491dd64e25efbd914db1494410eeb473afb7db5d70aa4f93a37a`，`parent.py` 为 `38c3a364e75b6b90be1e455061c7051c63686feb56f17a904fe7853a3d10f18b`，`flows.py` 为 `62b39e3ccbd42db2060ca92182d8ac7077d95284954ede9f1b75541c630a7137`。

## 2026-11-02 第一环节 DOCX 缓存接口及旧失败核对（候选完成，可交调度加载）

- **W1 结论：本轮控制对接和定向隔离检查完成，可交调度统一加载。** W1 已在 `flows.run_paper -> _process_step -> process_paper` 透传可选参数 `first_step_work_root`。实参是 `data_root/paperflow-v3/<task_id>/first-step`，不是某一资料组目录；该组转换目录按当前 `group_id` 选择。W2 的接收和身份校验已接入；`paper.body` 和 `archive_files` 保持原始 `.doc`。此对接改变了 `flows.py`，上一节的性能版哈希只描述接口对接前版本。
- 6 个 WPS 失败工单逐篇只读核对：每篇服务器存在 `preprocessing_started`、`preprocessing_failed(preprocessing_unconfirmed, retryable=false)`、`child_error` 三条；没有模型、后处理、RAG effect 或 NAS 回执。6 份登记回执及各自原始 `.doc`、交付副本和归档清单对应文件的 SHA-256 均匹配。
- 停止后的一次进程只读快照仅见 1 个早于 6 个工单全部启动时间的既有 `wps.exe`，没有可见 `office_wps.py --worker`；进程枚举无拒绝访问。原转换日志处于处理时的临时目录，已随临时目录移除，不能据此还原每次转换的 PID 身份或声称已有逐次退出回执。
- 原工单重试仍有两道保护：`control.retry_decision` 拒绝未确认失败，`stages.pipeline.process_paper` 也拒绝旧的 `retryable=false`。新的缓存存在本身不改变任何旧回执，不单凭错误文字或工单 ID 放开，不自动重试；证据驱动的追加核对记录及两道门槛如何协同仍待明确并验证。
- W2 当前 `_first_step_cached_docx` 只读核验结果：6 篇均找到同组、源 SHA-256 绑定且内容完整性通过的 DOCX；6 篇当前独立工作根下均未发现 `model`、`request-state.json`、`partition-state.json`、`first-request.json` 或 `material.json`。实时 Prefect 复核 6 个原子工单仍为 FAILED、登记身份未变、各 3 条原回执、均无 `manual_retry`。这只证明这些工单及当前工作根的已观察状态，不等于跨历史来源从未有模型请求。
- 6 篇登记资料的模型历史策略仍为 `history_lookup_required`、`allow_new_request=false`；后续若恢复到模型阶段，必须继续使用现有历史核查和最后准入，不得因本次前处理核对改成允许新请求。
- W1 对接口的隔离检查 `test_processing_handoff.py`、`test_paper_entry.py`、`test_shared_failure.py` 共 `6 passed in 34.01s`。W2 报告阶段回归 `96 passed`；本窗口未执行真实论文转换或发送请求。

### 历史恢复办法（候选实现中，尚未写入现场）

1. 不更改旧 `preprocessing_failed`。在现有 Prefect 回执中追加 `preprocessing_reviewed` 核对事实，绑定原失败 `event_id`、原父子/论文/登记身份和处理尝试，记录原稿及交付哈希、有效同组缓存、该子工单的无下游请求证据、进程观察及核对时间。该回执只证明已核对，不代表已重试或已派发。独立复核发现若把 `reviewed` 混进 `manual_retry`，现有消费者会把它当成派发并可能遮住未确认的授权；因此核对事实使用独立回执类型，实际授权仍使用原 `manual_retry`，不新增服务、队列或恢复框架。
2. `retry_decision` 只认可与当前失败精确绑定的核对事实，且本子工单没有任何下游模型、后处理或交付活动；原有 LLM/RAG unknown、未结束的派发等阻断保持优先。用户通过原重试入口时，生成现有 `authorized/running` 的 `manual_retry`，携带核对回执和原失败的引用，并只对原子 FlowRun 调度。
3. W2 原有第二道前处理门槛只接受与本次 `manual_retry` 和原失败匹配的已核对依据，其余 `retryable=false` 仍阻断。执行时沿用统一转换规则：同组有效 DOCX 存在则复用，不存在则走现有转换。原文件和历史请求的既有边界继续生效；不新增队列或批量绕过。实际派发前由调度确认原子工单状态及转换进程观察仍有效。

**证据及执行边界：** 原临时转换日志没有保存，缺少每次 WPS 转换进程的 PID/创建时间和逐次退出记录；当前只有进程全量只读快照支持“当前未见本次转换进程”。六份核对事实尚未写入服务器；跨历史模型请求仍须由既有历史检查判断。

W1/W2 共用的核对回执字段约定：`kind=preprocessing_reviewed`，外层 `event_id` 为该核对的唯一身份；`data` 包含 `failure_event_id`、`processing_attempt_id`、`identity={task_id,group_id,paper_id,receipt_id,attempt_id}`、`receipt_sha256`、按正文后附件顺序的 `source_sha256` 与 `delivery_sha256` 列表、按原归档清单顺序的 `archive_sha256` 列表、`cached_docx_sha256`、`observed_at`、`conversion_observation=no_owned_process_observed`、`local_request_history=none_observed`、`downstream_events=none_observed`。核对时实际分别哈希来源和交付文件；审计字段只陈述观察，不能编造历史转换 PID。真正的 `manual_retry authorized/running` 增加 `review_event_id`、`failure_event_id` 两个引用。

控制端 `retry_decision` 调用 `reviewed_preprocessing` 校验核对事实与当前子工单、登记资料及失败的绑定，并把两条引用直接返回给 API，API 将其写入 `manual_retry authorized`。子 Flow 已有的 `authorized -> running` 保留所有字段。W2 仅消费授权：要求 retry_id 一致、状态为 `scheduled/running`、stage 为 preprocessing、review 引用非空且 failure 引用等于当前失败事件，不重复扫描人工核对内容；缓存复用与缺失时原转换规则不变。前处理边界、控制重试及接口共 `30 passed in 3.71s`；临时 Prefect 合成整链 `test_full_candidate_chain.py[preprocessing_reviewed]` 为 `1 passed, 1 warning in 27.35s`，实际检查原失败保留、核对后原子工单重试以及 `authorized/running` 两个引用。真实回执尚未写入。

按调度要求保留原有最近一次 `manual_retry` 判断。独立模拟中，较早一笔仍为 `authorized`、较新另一笔为 `running` 的混合历史不由本轮改动覆盖；本次 6 篇只读核对均无历史 `manual_retry`，不应把本次结论推广为所有复杂重试历史均已核清。

以下是传给现有 `save_receipt` 的**虚构**核对示例，所有身份、摘要和时间都必须替换为对应实际核对结果；`child_flow_run_id` 应作为 UUID 传入。`cached_docx_sha256` 是核对当时有效缓存的摘要，并不要求之后缓存永久存在。

```json
{
  "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "folder_id": "example-group",
  "child_flow_run_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  "event_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  "kind": "preprocessing_reviewed",
  "data": {
    "failure_event_id": "example-original-failure",
    "processing_attempt_id": "initial",
    "identity": {
      "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      "group_id": "example-group",
      "paper_id": "example-paper",
      "receipt_id": "example-receipt",
      "attempt_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    },
    "receipt_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "source_sha256": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
    "delivery_sha256": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
    "archive_sha256": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
    "cached_docx_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    "observed_at": "2026-11-02T13:30:00+00:00",
    "conversion_observation": "no_owned_process_observed",
    "local_request_history": "none_observed",
    "downstream_events": "none_observed"
  }
}
```

W1 冻结文件 SHA-256：`control/api.py` `a1abf9ffb81ea9c80607ec3960b5cac5146c4417e2401659e78b11cdf643377b`；`control/parent.py` `38c3a364e75b6b90be1e455061c7051c63686feb56f17a904fe7853a3d10f18b`；`control/flows.py` `9d7d036ac52fb104d5fb7a5141f346d4104df6323c55d04a851d88f11c840e29`；`control/retry.py` `6273dff5dc83ae085e7c55c46b1195eabe763b2674894e75d7ec87d1e5a6a8ab`。本轮加载及真实重试由调度执行，不由 W1 操作。

## 2026-11-02 文件夹工单列表重复读取优化（候选隔离验证通过，未加载）

- 本轮运行代码只改 `control/api.py`：在一次 `/api/v1/prefect/folder-tickets` 请求中复用 `describe` 已读取的父回执、完整原生子工单与子回执，不做跨请求缓存。其他任务查询、开始、暂停和重试的既有读取路径保持不变。任务总状态仍按全部子工单及其原始回执计算，明细按原身份过滤，`complete` 仍检查完整子工单集合和未知派发。
- 纯内存的同一份数据下，修改前父工单读取 1 次、子工单集合 2 遍、父回执 2 遍、子回执 2 遍；修改后分别为 1 次、1 遍、1 遍、1 遍。超过 200 条时每一遍仍需要多次分页请求；模拟 201 个子工单及多页回执时，每页只读一次，回执仍按 `CREATED_DESC` 请求。分页不是原子快照，运行中新增数据可能到下一次请求才显示。
- 修改前采集的 6 组合成完整 JSON 与修改后的字段和值一致，覆盖未知模型请求及授权后未确认重试、身份不符回执或子工单、重复组、未知派发、未完成分类。`tests/control/test_folder_snapshot_reuse.py` 共 `12 passed`，另覆盖同一应用的下次请求看到新子工单，以及父/子回执或子工单分页中途失败返回错误而非空数据。现有 `test_api.py` 与 `test_client_contexts.py` 共 `5 passed, 1 warning`（FastAPI/Starlette 弃用警告）；均是临时 Prefect 或无网络合成测试。
- `control/api.py` SHA-256：`05e0bad5054a7a861dbcd930f55181f6901ac6100815dbeaed798f2fb951bd5b`。文件在共享候选目录；本窗口未重启服务或操作真实工单，不能将磁盘改动或隔离测试视为现场加载、页面延迟改善或业务验收。
- 调度已澄清，先前“暂不改阈值”和后续“统一改为 10 秒”是同一调度窗口的先后指令，后者明确替代前者；此前本窗口提出的确认问题不再需要答复。对应候选修改和验证见下节。

## 2026-11-02 Prefect 只读自检等待调整（候选隔离验证通过，未加载）

- `control/preflight.py` 中 6 项 Prefect 只读检查的 `wait_for` 从 3 秒改为 10 秒：父部署、子部署、Artifact、worker、全局名额、父子 Flow 信息。worker、全局名额和 Flow 原有并发集合各共享该项的 10 秒等待；6 项依次执行，整次自检并非 10 秒总限。仅延长等待，未增加重试、缓存或跳过检查；失败、异常、超时及名额不匹配仍按原逻辑未就绪，创建、开始和重试仍须通过原自检。
- 测试先在旧代码复现：模拟全局名额 3.2 秒正常返回时，3 秒预算将其判为未就绪；修改后该检查通过。模拟 10.5 秒未返回时仍按 `TimeoutError` 未就绪，直接读取异常及名额配置不符同样未就绪；7 个名额每个仅读取一次。为避免连接真实依赖，其他自检项目在这些合成用例中故意未配置，不能把模拟结果当成现场整体自检通过。
- `test_preflight_prefect_wait.py`、`test_folder_snapshot_reuse.py`、现有 `test_preflight.py` 合计 `19 passed in 44.55s`，使用内存模拟或临时 Prefect，未接触真实工单、模型、RAG 或 NAS。将修改后的 6 个 `timeout=10` 在内存还原为 `timeout=3` 后，文件 SHA-256 与修改前完全一致，证明 `preflight.py` 除这 6 处数值外无其他字节变化。
- 候选 `control/preflight.py` SHA-256：`9e42559bf4f4ac96ebaea3becfdaae66002c94d81ebc1453d91a6592078a3f6c`；`control/api.py` 仍为 `05e0bad5054a7a861dbcd930f55181f6901ac6100815dbeaed798f2fb951bd5b`。本窗口没有重启或加载服务，现场实际自检和性能效果待调度统一加载后核对。

## 2026-11-02 列表进度读取、NAS 等待及控制动作顺序（候选隔离验证通过，未加载）

- **交接结论：代码与隔离验证已完成；真实 176 篇结果对照、实际页面速度和加载后验证由调度在已暂停现场完成。** 本窗口没有启动、暂停、重试或删除真实工单，没有重启服务，也没有调用真实论文、模型或 RAG 写入。候选只改 `control/api.py`、`receipts.py`、`preflight.py` 和相应 `tests/control/`；未改前端或新建数据库、缓存、投影服务。
- 调度现场只读测到一页 200 条中，RAG 正文进度快照约 2.94 MB，成功 returned 约 0.21 MB；占比最大的负担是反复保存的完整 `rag-body` 快照。列表现在对每篇 `rag-body`、`rag-attachments`、`nas`、`cleanup` 分别只用于投影该种最新回执，其他影响原 `folder_status`、`retry_decision`、`last_retry` 的请求、结果、未发送、失败及重试回执按原逻辑读取。**按照调度最新要求，returned 本轮仍完整读取**，并非早期草案中的跳过 returned；真正的重试 POST 仍使用原完整历史。
- 运行中工单即便有尚未返回的 RAG intent，只要原阶段投影为 `rag_in_progress`，会保留该阶段和原“结果待核对、不能重发”原因，不为此重新下载旧快照。原生失败、崩溃以及投影为 failed/unresolved、身份不符或有 `child_error` 的论文回退原完整历史；未发送和未知不会被当作成功。已完成、父任务暂停但原生子工单 RUNNING、已失败和重试派发未知均有合成全量对照。
- Prefect 的 key `like_` 在服务端会自动两端加 `%`。为了取得无后缀的 UUID4 手动重试授权，使用版本位和 variant 位四种筛选；它们**可能顺带下载少量较长 key 的旧进度快照**，投影时仍只保留该类最新一份。纯内存同一批数据的完整 JSON 一致；7 篇健康/在途论文的 560 条旧正文快照，完整读取返回 560 条，筛选版实际收到 17 条（含上述偶然匹配），另 1 篇失败工单完整回退 80 条。这是合成数据下的读取量，不是现场速度或所有数据的固定比例。
- 按调度最终指示，先读取四类最新进度快照，再读取完整请求和重试事件；没有增加逐篇时间水位、跨页事务、锁或缓存。独立复核构造的并发时序是：intent 筛选完成后新 intent 才写入，其他筛选随后完成，则本次可能漏掉该 intent；旧全量读取本身也会分页，新事件在第一页读完后写入时同样不保证原子快照。先读快照再读事件可覆盖快照读取之后、相应事件筛选之前出现的新请求，但不能保证整个响应是某一时刻的原子快照。并发查询最多 16 项，按 200 篇分组；单项失败时取消并等待同组其他请求。
- 已知精确性边界：Prefect 原有排序只有 `created DESC`。独立纯内存反例表明，同一子工单若两个相关回执 `created` 完全相同，分别筛选后合并的先后次序可能与一次全读的同时间戳次序不同，进而影响是否显示待核对；尚未证实现场存在该碰撞。本轮按调度要求没有为所有同时间戳回执新增全量回退。真实 176 篇逐项对照应包含阶段、失败/未知、`retry_reason` 和 `last_retry`，若有差异须先查明，不能宣称等价。
- 已知规模边界：每篇正常快读需 4 次最新快照请求，另有 15 类 key 筛选及分页；176 篇时前者为 704 次请求。各页面请求的并发上限分别生效，论文数或同时打开的页面增加时可能再次造成延迟；目前没有千篇规模或现场速度实测。请调度实测暂停状态列表响应，再决定是否加载交付；不得将合成下载量减少等同于真实加速。
- 开始/暂停的准入条件复用原 `_task_actions` 判断，修改 Admission 前不再等待完整论文投影；开始仍保留原自检、原父/子工单恢复，动作后的响应仍读取 `describe`。因此若动作已生效而后续投影失败，客户端必须按原工单身份核对当前状态，不能把失败响应当作动作未发生。
- NAS 预检目录检查等待由 5 秒改为 15 秒；超时显示 `NAS目录检查超过15秒，状态未确认`，`OSError`、`KeyError`、`TypeError` 仅显示异常类型，不泄露路径或凭据，不新增重试或写文件。现场独立冷读取记录中 `Path.is_dir()` 返回约 9.228 秒、原 5 秒等待已超时；另一次热读取快不代表冷启动已复验。隔离用例覆盖 9.1 秒成功、15.2 秒超时、模拟权限异常、不具写权限、缺配置，以及本机只读临时文件实际触发的系统 `PermissionError`；后者并非真实 NAS ACL 测试。
- `test_folder_ticket_fast_read.py`、`test_folder_snapshot_reuse.py`、`test_control_action_order.py`、`test_api.py`、`test_client_contexts.py` 共 `30 passed, 1 warning in 47.51s`，含独立临时 Prefect；`test_preflight_prefect_wait.py` 为 `10 passed in 39.06s`。定向测试包括最新快照读取后新增 intent、连续两次重试授权、查询异常取消及临时 Prefect 的真实 key 筛选。警告为现有 FastAPI/Starlette 弃用提示。
- 冻结文件 SHA-256：`control/api.py` `b42ebbda124c073e8665dc61daf0805331976a2558b90c35522e872c5aa58bbd`；`control/receipts.py` `f86f6c7d326e0dbe4b7166e4ad070b586978832e87ea5efc264a1f5e135b8755`；`control/preflight.py` `bede3f8bcd4994104a645c66adca71833f373b319cfa8fbc619912d76fe17f16`。

## 2026-11-02 开始/暂停通信确认与现有阶段时间（候选隔离验证通过，未加载）

- 用户明确授权“允许，只修通信问题”。调度报告实际现场开始 POST 耗时 123.354 秒、暂停 POST 耗时 109.274 秒，暂停准入已先行生效；原页面开始/暂停分别等待 120/8 秒。此前动作末尾的 `describe` 会重新读取全部论文状态。以上时间为调度报告，不是本窗口额外实测。
- 本次运行代码**仅修改 `control/api.py` 开始、暂停成功时的两处 return**，不再调用动作后的 `describe`，分别返回合法 JSON：`{"task_id":"<原任务ID>","action":"start","accepted":true}` 或 `{"task_id":"<原任务ID>","action":"pause","accepted":true}`。`accepted` 仅确认原控制步骤正常返回，**不表示暂停后已在途操作全部停止或流程已完成**；当前状态仍由原 GET 读取。复核现有页面先解析 JSON，成功分支只检查 `response.ok`，之后调用原 `refresh`，不消费成功体中的状态字段。
- 原准入条件、开始前自检、对原 paused 父 FlowRun 的恢复、原 crashed 父/子 FlowRun 的重排、错误分支和活动依赖失败回执的清除均保持原顺序。开始仍需等待这些原步骤；完整列表 GET 如果仍超过页面 8 秒等待，状态刷新仍可能待核对。没有调整 Admission 轮询、列表算法、业务流程，没有新建后台任务或服务。
- 在内存中把 API 的两处新 return 还原为原 return 后，文件 SHA-256 恰好恢复到上一节冻结值 `b42ebbda124c073e8665dc61daf0805331976a2558b90c35522e872c5aa58bbd`，说明相对该版没有其他字节变化。当前 API SHA-256 为 `5a691403140061f9e19e8183591853d499a26bca4aa67e0e590d91edba604356`；`control/admission.py` 为 `6a51ac83e27aeb963ccc022ee78f008bff502c82ce68a139730f5e80881bcdaf`，`receipts.py`、`preflight.py` 哈希与上一节一致。
- 测试同步了新的成功响应契约。修改前，投影故障用例实际返回 500、原完整响应与新契约不一致，得到 `4 failed, 8 passed`；只改两处成功 return 后，相应投影故障可在动作执行后返回 200。最终命令为 `& 'F:\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe' -m pytest tests/control/test_control_action_order.py tests/control/test_api.py::test_creation_checks_dependencies_again_and_only_creates_one_parent tests/control/test_resume_child.py -q`，结果 **`18 passed, 1 warning in 33.06s`**，使用内存替身和独立临时 Prefect。覆盖原父子 ID 恢复、父/子恢复拒绝、paused 父恢复异常、活动依赖回执保存异常、检查不通过及投影故障；警告为现有 FastAPI/Starlette 弃用提示。独立复核已核对改动与原错误分支。
- 本窗口没有加载新代码、重启服务或操作真实工单；调度负责同步加载和用户最新指定的页面开始。用户要求只记录各阶段时间、不持续监控，本窗口没有新增监控或计时框架。

### 阶段时间只读核对

- Prefect `Artifact.created` 是**回执写入服务器时间**。现有 `read_receipts()` 只返回 `row.data`，丢弃该元数据；若之后只读整理时间，应从原生 `read_artifacts` 获取完整 Artifact 并保留全部分页历史，不保存模型原文。
- 分类：`classification_plan` 在发现资料后保存；每组有 `classification` 结果，模型请求另有按 `request_id/snapshot_id` 区分的 intent、complete、failed、unknown、not_sent；`classification_complete` 是父工单分类和派发工作收尾，不能证明所有组成功。没有通用的每组分类开始回执，缓存复用时也未必有新的模型请求，不能完整还原每组分类真实起止。
- 前处理：`preprocessing_started` 与 `preprocessing_complete`/`preprocessing_failed` 可按子工单和 `processing_attempt_id` 区分初次与手动重试；开始回执在获得资源后、实际处理前写入，完成回执在清单核验后写入。中断可只有 started，已有材料复核失败可仅有 `recheck` failed；缺失不能当作零时长。
- LLM：可按 `component_id`、`request_id` 关联 intent、started、response_saved 和 not_sent/failed/unknown；人工重试另结合 `retry_id/retry_of`。`llm_folder_complete` 是整篇原始回复及全文排名的汇总凭据。started 是发送边界记录，response_saved 是回复保存时间，不是远端精确执行时间；分段、附件、排名可能并行或复用，不可简单求和。
- 后处理：`postprocessing_component_started/complete/failed/not_started` 可按 `component_id` 和组件 `attempt_id` 关联，`postprocessing_complete` 是整篇汇总。暂停时可能先有 started 再有 not_started，同一次 attempt 可再次开始；复用结果也可能没有新开始回执。所有阶段应保留失败、未知、暂停和重试间隔，回执时间差不能宣称纯处理时间。

## 2026-11-02 分类开始回执（候选隔离验证通过，未加载）

- 用户已授权记录阶段时间；按调度要求仅在 `control/parent.py` 的 `classify_group` 中，`await admission.wait()` 之后、原分类线程调用之前，复用 `save_group` 写入 `classification_started`，内部 data 只有 `group_id`，外层保留现有 `task_id` 和 `folder_id`。原 `classification` 结果回执仍是分类结果依据，没有新增计时器、监控、缓存、文件或调度分支。控制 POST 仍是上一节的短确认。
- `Artifact.created` 表示服务器保存该开始回执的时间，含义是准入已放行、即将调用分类；保存后到线程检查准入之间可能发生暂停，**不能据此断言分类已实际执行或模型请求已发送**。暂停后再次进入可产生多条 started；它们可能没有对应结果，不应将一条结果重复配给所有开始记录或算成多篇。固定 key 的各次记录要从原生普通 Artifact 全历史读取，不能只取 latest collection；具体期间若回执缺失或结果未知则继续标未知。
- 该新增回执的保存位于原分类 `try` 之前：若服务器写入失败，会阻止该次调用，并经原 `TaskGroup` 异常路径影响并发组；现有 `drain_thread` 会等待已执行的线程返回，不能宣称其他线程立刻停止。暂停后的重入、两次 started 及回执写失败未在本次定向用例中实测。
- 调度已说明当前 176 组分类完成；新增代码只对之后实际进入分类的组记录，**不会为这 176 组补造旧开始时间**，也未由本窗口写入现场记录。页面开始和实际运行由调度负责。
- 定向测试先在旧代码失败，原生 Artifact 中没有所需开始回执；加入该行后运行 `& 'F:\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe' -m pytest tests/control/test_parent_pipeline.py -q`，得到 **`1 passed in 14.43s`**。该用例在独立临时 Prefect 和合成两组资料、模拟模型接口上，核对每组一条开始回执的父/组身份、`created` 早于原 `classification` 结果，保留原并发分类与尽早派发检查；不代表真实分类或现场性能已验收。旧代码的预期失败轮次结束时还出现临时 Prefect Windows 日志/数据库占用清理提示，最终通过轮输出没有该提示。
- `control/parent.py` 当前 SHA-256：`c2e31cac26cabe601a9729c62c30d1bc0cfc99479a103013a9d929bd3721d417`。将新增行在内存移除后，SHA-256 恰好恢复为上一版 `38c3a364e75b6b90be1e455061c7051c63686feb56f17a904fe7853a3d10f18b`，确认除此一行外 parent 无其他字节变化；`control/api.py` 仍为上一节通信修改哈希 `5a691403140061f9e19e8183591853d499a26bca4aa67e0e590d91edba604356`。

## 2026-11-02 最终交付状态同步（调度报告，等待用户验收）

- 用户最后要求原任务自动运行，而非暂停交付。调度报告已统一加载后端及 v8 前端，在真实浏览器选择原任务 `d40aeff3-170f-470d-ba01-54de41372625`，只点击一次开始，页面确认请求已提交。2026-11-02 08:20:13（America/Los_Angeles）一次核对结果：原生父单 `RUNNING`、`desired_state=running`；原 176 个子单中 172 个 `RUNNING`、4 个 `COMPLETED`。这是当时的观测结果，并非整批完成或用户验收通过。
- 自动监控的自动化 ID `176` 保持 `PAUSED`；这是监控状态，不是业务父工单状态。用户要求不持续监控，后续由业务自动运行，等待用户验收；不自行重启、暂停、重试或开启监控。
- 阶段时间保留在既有 Prefect Artifact 回执；新增的 `classification_started` 仅记录之后实际进入分类的组，不为本批此前已完成的分类补造开始时间。回执时间及暂停、重入、未知的解释边界见上一节。
- 完整列表在实际负载下仍慢，调度报告最后页面显示旧数据；验收项 S09 **未宣称刷新性能通过**，页面旧数据不能替代上述时点的原生状态。完整交付事实、已知限制及逐项标准以工作区 `docs/大小工单改造-交付与逐项验收报告-20261102.md` 和 `docs/大小工单改造-任务验收标准与验收记录-20261102.md` 为准。
- 本窗口本次仅据调度报告和上述交付文档同步记录，没有查询或操作运行中的工单、加载代码、重启服务或启动监控；用户逐项验收仍待进行。
