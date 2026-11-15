# Prefect 分阶段流水线 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将论文处理改为 Prefect 原生分阶段任务流水线，使 LLM 在 RAG/NAS 较慢时持续推进，并可靠处理失败、重启和已有结果续跑。

**Architecture:** 一次提交生成冻结清单，独立 Task Worker 消费各阶段任务；实际 HTTP 使用全局并发和启动速率限制。持久业务记录负责身份、结果、交接意图及发送预算；Prefect 负责常规排队、任务运行和恢复任务的重试。一个 Prefect 核对 Flow 修复交接/执行中断，不另建常规调度器。

**Tech Stack:** Windows、现有 Python 虚拟环境、Prefect 3.9.0、异步 HTTP、原子 JSON 记录与跨进程文件锁；现有 LAN Prefect Server、RAGFlow API、NAS。

**Spec:** [2026-10-29-prefect-stage-pipeline-design.md](../specs/2026-10-29-prefect-stage-pipeline-design.md)

**Status:** 供用户审阅的实施计划。2026-10-29 仅编写计划，没有修改业务程序、启动模型测试、重试论文或切换生产部署。设计参数是本方案建议值，不冒充已验证能力。

## Global Constraints

- 版本固定 Prefect 3.9.0，第一版全部业务 Worker 在同一 Windows 主机运行。
- 正文 HTTP 全局上限 100，附件 HTTP 20，总模型 HTTP 120；本地转换 4、封包 2、RAG 交付 20、NAS 2。
- 模型启动速率初始 1 次/秒、突发容量 1；正常与恢复调用共享上述限制。
- Worker 容量：正文 128、附件 32、模型恢复 32、准备 4、封包 2、RAG 40、NAS 4、控制/核对 8。
- 正常模型任务与恢复任务使用不同 task_key 和 Worker；恢复任务等待时不得占 HTTP 名额。
- 模型总时限先保持 3600 秒；不得仅通过调大时限、增加整批并发宣称问题解决。
- 新模式不依赖 `_batches`，不持有覆盖五步全生命周期的论文名额或锁。
- 只迁移冻结范围，早上已完成 100 组不重跑；310 组按实施时证据重新分类，历史快照不当实时数据。
- 保留源文件、原请求/响应、哈希、分段、请求/文档身份、已消耗重试预算；未知结果不自动重发。
- 原桌面文件不改动；inbox 副本及缓存清理严格沿用已授权范围与整篇核验要求。
- 正文新增交付名为“论文题目.扩展名”，附件为“论文题目_附件自身名.扩展名”，同时保留原名映射。
- 业务任务结果持久化仅保存轻量引用；凭据和论文全文不得进入 Prefect 参数、普通日志或公开 artifact。

## Review Focus

1. Server 重启后 DB 中仍是 Scheduled，但后台内存交付队列丢失：任务须恢复消费；归属 Task 0、4、9。
2. 恢复 Worker 全部被长 Retry-After 占满：健康新稿仍能进入正常 Worker；归属 Task 5、9。
3. 外部已接受调用，进程却在收据落盘前退出：不重复调用/建文档；归属 Task 2、4、6、9。
4. 同步网络/文件调用挂死：仅设置 Prefect timeout 不能解决，须能终止本地执行并保留远端未知；归属 Task 3、7、9。
5. 命名变更、附件新增、已清理源副本、跨批次重复输入：不因显示名或调度版本变化重做正文；归属 Task 1、2、8、9。

---

## 文件与模块分工

以下为计划中的新增/修改路径，均相对隔离候选项目根目录，尚未实现。

| 路径 | 职责 |
|---|---|
| `paperflow_prefect/pipeline_contracts.py` | 阶段信封、稳定身份、证据引用、错误类型 |
| `paperflow_prefect/pipeline_ledger.py` | 阶段记录、交接意图、发送预算、拥有者和操作锁 |
| `paperflow_prefect/pipeline_runtime.py` | 从持久回执构造阶段运行上下文，去掉批次内存依赖 |
| `paperflow_prefect/pipeline_tasks.py` | 各阶段 Prefect task 定义及独立恢复 task_key |
| `paperflow_prefect/model_transport.py` | 可取消的异步模型 HTTP、SSE 收取、请求时间线 |
| `paperflow_prefect/stage_process.py` | 同步 RAG/NAS 子进程监督、身份核对与终止 |
| `paperflow_prefect/pipeline_dispatch.py` | 投递意图、Prefect 投递、编号确认、过期投递拦截 |
| `paperflow_prefect/pipeline_reconcile.py` | 交接遗漏、失联任务、未知结果核对 |
| `paperflow_prefect/pipeline_retry.py` | 错误分类、独立恢复任务、有限发送预算 |
| `paperflow_prefect/pipeline_controls.py` | 全局/批次控制代号、暂停、停止、部署执行权 |
| `paperflow_prefect/pipeline_status.py` | 业务状态、指标、前端 artifact |
| `paperflow_prefect/pipeline_migration.py` | 旧记录只读审计、按证据导入和兼容性检查 |
| `paperflow_prefect/pipeline_flows.py` | 提交、定期核对、控制和最终验收 Flow |
| `run_stage_worker.py`、`pipeline_admin.py` | Worker 进程入口、本机控制入口 |
| `manage.py`、`settings.py`、`config.json` | 候选配置、注册独立部署和 Worker 启动配置 |
| `backend.py`、`body.py`、`unified.py`、`profile_recovery.py` | 必要的业务阶段拆分、结果恢复、分段恢复和取消适配 |
| `tests/test_pipeline_*.py` | 真实故障边界测试；不只检查装饰器或配置数值 |
| `tests/pipeline_lab.py`、`tests/pipeline_acceptance.py` | 独立 Prefect/模拟服务及持续并发验收 |

## 执行位置与测试约定

当前工作区没有 Git 根目录。实施时创建隔离候选 `F:\论文预处理系统\预处理系统2\candidates\prefect-stage-pipeline-v2`；复制明确的程序文件与必要只读配置，运行数据使用独立目录。生成文件清单、SHA256 和修改记录，不复制整个生产 runtime，不初始化新 Git 仓库，不热改生产代码。

后续命令在候选根目录运行，Python 指向已有虚拟环境：

```powershell
$pipelinePython = 'F:\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe'
```

新测试默认阻断生产模型/RAG/NAS网络和生产数据写入；隔离 Prefect API 和模拟服务只允许 loopback。需要运行现有回归时，先核对其 fixture/配置隔离，不能让 import `flows.py` 把环境切回正式 API。

### Task 0：建立基线与 Prefect 原生机制验收

**Files:** Create `tests/pipeline_lab.py`, `tests/test_pipeline_prefect_native.py`, `tools/pipeline_baseline.py`。

**Interfaces:** `capture_baseline(scope_manifest: Path, output: Path) -> Path`；`start_lab(root: Path) -> LabEndpoints`（包含隔离 API 地址、模拟服务地址、允许网络列表）；`LabEndpoints` 在 `tests/pipeline_lab.py` 定义。

- [ ] 冻结精确输入清单，导出当前部署、SDK/Server 版本、实际 Worker 身份、额度、代码/配置摘要和逐篇证据索引；输出到 `runtime/development/pipeline-v2/baseline/`，不更改原记录。
- [ ] 编写跨进程测试：投递端退出后 Worker 仍能读取参数和结果；Worker/Server 分别重启；原生 retry 等待是否占容量；租约设 60 秒时实际回收时长。`test_server_restart_does_not_imply_queue_rehydration` 必须直接观察是否能消费，不能仅断言数据库有行。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_prefect_native.py -v`。记录实际版本和行为；首次暴露的问题进入 Task 4/7 的红灯用例，不用手工改数据库把它变绿。
- [ ] 确定共享参数/结果存储位于候选专用绝对路径，启动两种独立进程验证读写。输出 `prefect-capability.json`，失败则不进入生产接线。

**交付：** 可重复的隔离 Prefect 实验环境、基线和机制报告。此阶段 0 次真实模型请求、0 次真实 RAG 写入。

### Task 1：定义阶段依赖、名称与前端完成语义

**Files:** Create `paperflow_prefect/pipeline_contracts.py`, `tests/test_pipeline_contracts.py`；后续文档 `docs/分阶段-Prefect3-前端对接-v2.md` 先定义字段。

**Interfaces:** `ArtifactRef(path: str, sha256: str)`；`StageEnvelope(batch_id, paper_id, receipt_ref, operation_key, stage, item_id, part_id, input_refs, contract_version, control_generation, dispatch_seq, dispatch_kind)`；`StageResult(operation_key, state, evidence_refs, next_stages)`。`stage` 明确枚举 intake/prepare/profile/attachment/package/rag_body/rag_attachment/archive/cleanup/finish；dispatch_kind 为 normal/recovery；control_generation 包含 global、batch、runtime。model_recovery 是执行 task_key，恢复仍使用原 profile/attachment 的 operation_key。

- [ ] 写失败测试：`test_attachment_failure_does_not_block_body`、`test_empty_attachment_inventory_has_explicit_skip`、`test_submission_complete_is_not_paper_complete`；断言只有本篇必需依赖阻塞，不能形成跨论文/跨批次依赖。
- [ ] 写身份/命名测试：同源同处理契约跨批次生成同一 operation_key；只改显示名不改变身份；源哈希/阶段输入改变必须改变身份；附件新增不使已通过正文失效；最终命名按论文题目，碰撞加序号并保留映射。
- [ ] 实现 `operation_key(envelope_inputs: dict) -> str`、`ready_stages(evidence: dict) -> list[str]`、`paper_status(evidence: dict) -> str`，区分已提交、LLM通过、RAG全目标核验、NAS核验和已完成。题目未确认使用内部 ID 保存处理中结果，显示待补题目，不把人名冒充题目。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_contracts.py -v`；输出通过记录和契约示例。

### Task 2：持久操作记录、互斥和发送预算

**Files:** Create `paperflow_prefect/pipeline_ledger.py`, `tests/test_pipeline_ledger.py`；Modify `paperflow_prefect/records.py`（只添加新格式所需能力，不改旧证据含义）。

**Interfaces:** `StageLedger(root: Path)`；`claim(envelope: StageEnvelope, owner: dict)`（上下文管理器）；`commit_result(envelope, result: StageResult) -> None`；`reserve_send(paper_id, item_id, part_id, request_sha256, recovery: bool) -> dict`；`read_operation(operation_key) -> dict`。

- [ ] 写红灯用例：两个进程争同一操作仅一个能发送；写结果中断仍能读到完整旧版或新版；重复任务发现已验证结果时实际模型调用数为 0；派发序号过期退出。
- [ ] 写预算用例：恢复前先持久预留；预留后崩溃会消耗预算；新 task_run_id/重启不重置预算；旧 retry-intent/revision 已用次数被扣减；整篇追加 HTTP <=2。
- [ ] 实现原子文件写入和分层锁，记录 Worker instance_id、PID、创建时间、task_run_id；身份绑定锁短持有，阶段外部操作只持自身锁。证据验收须重新检查哈希，不能只信 Prefect Completed。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_ledger.py -v`；保存多进程实际发送计数证据。

### Task 3：把业务步骤改成独立、可中断的 Prefect 任务

**Files:** Create `pipeline_runtime.py`, `pipeline_tasks.py`, `model_transport.py`, `stage_process.py`（均在 `paperflow_prefect/`）、`run_stage_worker.py`, `tests/test_pipeline_execution.py`；Modify `body.py`, `backend.py`, `unified.py` 中输入构造/HTTP/结果解析的必要边界。

**Interfaces:** `load_stage_context(config_path: Path, envelope: StageEnvelope) -> StageContext`（定义在 pipeline_runtime）；`async execute_stage(envelope: StageEnvelope) -> StageResult`；`async send_model(request_ref: ArtifactRef, envelope: StageEnvelope) -> ArtifactRef`；`async run_stage_process(envelope: StageEnvelope, operation: str) -> StageResult`。

- [ ] 写失败测试：新进程只拿 envelope 就能执行；没有 `_batches` 仍能工作；RAG 阻塞不占正文 Worker；模型等待不占本地转换名额；接收绑定与源哈希不匹配时 0 次外部调用。
- [ ] 把模型阶段拆为构造请求并保存、异步 HTTP 收取并保存、离线解析校验。采用可取消 AsyncClient，连接/响应流由当前 task 生命周期拥有；流中 error、截断、DONE、最终 usage 分别记账；中断保留部分响应，不伪造完整成功。
- [ ] 给每个阶段独立 task_key。正文、附件任务与 model_recovery 不共用 Worker；配置显式 Worker 容量。设置参数/结果存储；使用返回 None 的显式 cache_key_fn 禁用自动结果命中，仍 persist_result=True，避免 TaskWorker 把 NO_CACHE 自动改成 DEFAULT 后跳过业务核验。
- [ ] RAG/NAS 的同步业务放入独立可终止子进程，父异步任务负责资源额度、控制、PID 身份及结果收据。短期进程对外只读 envelope 文件，不通过命令行传凭据；隐藏窗口；退出后校验结果文件哈希。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_execution.py -v`，包含挂死网络取消、完整响应未校验崩溃、子进程终止后的记录完整性。

### Task 4：可靠交接、Server 重启补投和任务归属恢复

**Files:** Create `pipeline_dispatch.py`, `pipeline_reconcile.py`, `pipeline_flows.py`（`paperflow_prefect/`）、`tests/test_pipeline_handoff.py`。

**Interfaces:** `dispatch_ready(envelope: StageEnvelope) -> str`（Prefect task_run_id）；`async reconcile_scope(batch_id: str) -> dict`；`submit_batch(scope_manifest: str, request_id: str) -> dict`；`reconcile_batches() -> dict`（Prefect 每 15 秒运行）。

- [ ] 写四个红灯断点：完成记录落盘后尚未投递、Prefect已接受但本地编号未写、Server重启丢队列、Pending/Running旧拥有者消失；断言无漏任务且无重复外部副作用。
- [ ] 实现“结果 → 意图 → apply_async → 编号确认”；按 operation_key、dispatch_seq 标签读回已存在运行。只投递依赖齐全的任务，不提前让大量下游任务占 Worker 等上游。
- [ ] 实现 stale Scheduled 的严格重排：同 task_key 有匹配且空闲的 Worker，排队 >120秒，无有效拥有者、无已发送证据，操作锁下递增 dispatch_seq 后重投。旧投递到达时被序号校验拦住。Prefect 查询失败、无空闲 Worker、旧拥有者还活着均不能凭超时重投。
- [ ] 对失联已发送操作转 unknown；先检查本地完整响应/回执、再核对远端；不重新创建论文或模型请求。核对 Flow 只处理登记范围，不把全磁盘历史扫成新任务。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_handoff.py -v`，并在隔离 Prefect Server 上实际重启服务验证恢复消费。

### Task 5：LLM 路由诊断、有限恢复和重试隔离

**Files:** Create `paperflow_prefect/pipeline_retry.py`, `tools/trace_model_route.py`, `tests/test_pipeline_retry.py`；Modify `profile_recovery.py`, `model_transport.py`, `pipeline_tasks.py`。

**Interfaces:** `classify_failure(evidence: dict) -> RetryDecision`；`RetryDecision(kind, reason_code, not_before, remaining_sends)`；`schedule_recovery(envelope, decision) -> str | None`；`async model_recovery(envelope, not_before) -> StageResult`；`trace_route(request_ids: list[str], evidence_dir: Path) -> Path`。

- [ ] 对照 24 次 502、20 次流断和 1 次 429，生成同请求跨路由事件表；核实实际运行容器/进程、代码和配置挂载、每跳时限与时间偏差。确切网关修复只对定位出的运行源提交补丁和前后证据；未拿到日志时交付观测缺口，不能宣布已修复。
- [ ] 写失败分类测试：429含Retry-After、502已明确拒绝与执行未知、收到部分SSE后ECONNRESET、完整响应仅校验失败、分段中仅一段失败。任何 HTTP 码都不能覆盖“已发送且结果未知”证据。
- [ ] 接线独立恢复 task_key/Worker：正常任务记录可恢复失败后结束；恢复使用原业务 operation_key，更新 dispatch_seq/kind，成功后完成原阶段并派发原下游。首次追加按30秒基准，第二次按120秒基准，加0—25%抖动，尊重Retry-After；>600秒转needs_attention并显示允许时间。只有恢复 task 配一次原生 retry，次数另受 Task 2 持久预算约束。
- [ ] 分段恢复按原请求内容和 part_id，已成功部分不再调用，整篇追加<=2；完整本地回答优先离线校验。未知核对每30秒、最多10分钟，无查询能力立即说明；不重新打开未知请求预算。
- [ ] 设置全局 HTTP 并发及1次/秒启动节奏；所有重试同样受限。接入路由冷却规则（60秒至少10个终止请求、可用性错误>=30%、冷却60秒），不杀健康在途、不额外产生计费探测请求。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_retry.py -v`；必须把恢复 Worker 全部32个容量填满长等待，再证明正常 Worker 仍消费健康新稿且 HTTP 数不超限。

### Task 6：RAG/NAS 独立续跑与整篇收录证据

**Files:** Modify `paperflow_prefect/unified.py`, `recovery.py`, `attachment_publish.py`, `nas_handoff.py`, `pipeline_tasks.py`；Create `tests/test_pipeline_delivery.py`。

**Interfaces:** `execute_stage` 对 rag_body/rag_attachment/archive/cleanup/finish 返回 Task 1 的 StageResult；`verify_whole_paper(envelope: StageEnvelope) -> StageResult` 放在 pipeline_runtime。

- [ ] 写红灯用例：RAG阻塞600秒时正文仍调度；正文已入库但附件失败时仅附件未完成；任一目标库未核验不得归档清理；NAS已复制但收据未写时按哈希续办。
- [ ] 将正常入库与恢复都绑定原 prepared manifest、document_id、receipt；已提交未知先读回，不能新建替代文档。RAG20名额只在对应交付任务占用；NAS2名额独立。
- [ ] 定义整篇验收：所有目标身份/内容/启用/检索证据、附件父正文关联、NAS逐文件哈希、授权清理记录。明确“一个专业库成功”与“全篇收录”的区别。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_delivery.py -v`，以及核对隔离后的现有 unified、stage45、cleanup 边界回归；不得对真实库制造故障进行测试。

### Task 7：全局控制、停止时限和版本切换

**Files:** Create `paperflow_prefect/pipeline_controls.py`, `pipeline_admin.py`, `tests/test_pipeline_controls.py`；Modify `manage.py`, `settings.py`, `run_stage_worker.py`, `pipeline_flows.py`, `model_transport.py`, `stage_process.py`。

**Interfaces:** `read_control(scope_id: str) -> dict`；`set_control(scope_id, desired_state, expected_generation) -> dict`；`check_dispatch(envelope) -> None`；`stop_owned_execution(owner: dict) -> dict`；`activate_runtime(candidate_manifest: Path, expected_generation: int) -> dict`。

- [ ] 写红灯用例：两批同时暂停；更新global/batch/runtime generation后旧Worker无法新派发，但已获得的成功结果仍可落盘；电脑重启不自行恢复paused；Prefect不可访问时本机紧急命令仍能阻止新外部操作。
- [ ] 正常drain不再等待整批RAG完成才释放上游；紧急停止取消异步HTTP、10秒后终止不响应的已核对身份子进程。挂死传输本地退出目标<=15秒，远端未知保持独立状态。
- [ ] 配置并验证60秒租约续租；硬杀Worker槽位恢复目标<=90秒。正常退出释放、续租失败、Server短暂失联分别测试；绝不直接清空全局额度。若目标未达到必须在验收中保留实测限制。
- [ ] 新旧执行权切换采用持久runtime generation和新入口：旧部署停止接新任务，登记在途结果/未知项，再启用新入口。旧程序不支持新的fence时须确认其进程及子进程已停止，不能只写新标志便认为它已受控。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_controls.py -v`；记录外部模拟服务看到的最后旧请求、新请求开始时间和停止耗时。

### Task 8：迁移现有结果与交付前端状态契约

**Files:** Create `paperflow_prefect/pipeline_migration.py`, `pipeline_status.py`, `tests/test_pipeline_migration.py`, `tests/test_pipeline_status.py`, `docs/分阶段-Prefect3-前端对接-v2.md`；Modify `pipeline_flows.py`。

**Interfaces:** `audit_migration(scope_manifest: Path) -> dict`；`import_verified_mapping(audit: dict) -> dict`；`build_batch_progress(batch_id: str) -> dict`；`publish_progress(batch_id: str) -> str`（Prefect artifact ID）。

- [ ] 只读重新统计早上100及当前310范围，生成逐篇动作清单：不执行/仅下游/正常未发/明确失败恢复/未知核对/材料待处理；每条动作关联可核验的证据，不按旧统计猜测。
- [ ] 写红灯用例：227已通过画像导入后0模型调用；调度代码摘要变化不失效合格画像；已有RAG ID不变；旧已清理inbox副本不触发从头重做；原始请求预算继承；超范围输入拒绝。
- [ ] 建立v2映射，不修改旧请求台账和旧接收回执。接收修订、显示命名和操作身份分离；历史远端改名不得通过删除重传实现。
- [ ] 定义 `paperflow-batch-progress/2` artifact，每15秒更新，每篇包含论文题目、阶段状态、错误码、下一次处理时间、尝试次数和Prefect链接；超30秒未更新显示过期。提交Flow完成不得显示为整批收录完成。
- [ ] 文档给出基于新部署ID的提交、暂停/停止、状态查询与错误样例；ID由注册回执产生，不虚构。前端通过已有后端/Prefect API读取artifact；若要新增Web API，单独标成后续实现，不把尚不存在的URL写成可用。
- [ ] 运行 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_migration.py -v` 和 `& $pipelinePython -m unittest discover -s tests -p test_pipeline_status.py -v`；抽查前端字段与实际证据一致。

### Task 9：压力、故障、真实小批与正式切换验收

**Files:** Create `tests/pipeline_acceptance.py`, `tests/test_pipeline_end_to_end.py`, `tools/pipeline_cutover.py`；输出 `runtime/development/pipeline-v2/acceptance/` 下带时间与版本的报告。

**Interfaces:** `run_acceptance(scenario: str, lab_root: Path) -> Path`；`prepare_cutover(scope_manifest: Path, candidate_manifest: Path) -> Path`；切换脚本显式区分检查和执行，不导入即执行。

- [ ] 先运行全套新增测试：`& $pipelinePython -m unittest discover -s tests -p 'test_pipeline_*.py' -v`，再运行已隔离的相关既有回归。失败修复后只重跑受影响测试与最终整合检查，不重复无关全量检查。
- [ ] 运行 `& $pipelinePython tests/pipeline_acceptance.py --scenario sustained-100 --lab-root runtime/lab/pipeline-v2`：400模拟论文、LLM180秒、RAG600秒、1次/秒启动；有足够eligible输入时达到100且不超过100，稳态>=95并发样本占比>=95%，95%名额补位<=5秒，第二批不等待第一批收录完成。
- [ ] 运行 `--scenario crash-matrix`：分别重启Worker和Server，注入所有交接断点、挂死传输、重复投递和32个恢复任务占满；未发送任务恢复、已成功步骤不重做、未知不盲发，整篇完成数可独立复算。
- [ ] 迁移仅导入已核验的映射。先选择5篇当前范围内确实待处理且可执行的论文做端到端，包含有附件与恢复路径；没有适合样本就说明该真实验收尚缺，不把模拟替代为真实通过。
- [ ] 使用足够待处理输入按20/50/100档观察，每档至少一个完整模型响应周期；任一档提高错误率或降低每小时成功量，记录对应路由/资源证据并处理后再升档。当前剩余eligible不足100时，报告“模拟100通过、真实仅达到N”，不得重跑已完成论文凑并发。
- [ ] 输出切换包：代码/配置manifest、部署和Worker注册回执、迁移逐篇清单、实际成功吞吐/错误率/延迟、RAG/NAS验收、停止回收实测、剩余needs_attention及原因。通过后切换入口，不需等待旧批整篇全完，但需完成Task7执行权隔离。
- [ ] 回滚演练只撤回执行版本和入口，保留业务台账；v2触达操作默认冻结，旧代码兼容性确认前不重跑。报告哪些数据可以旧版本继续、哪些只能停留待核对。

## 阶段交付与上线判定

| 里程碑 | 完成条件 | 可以对用户声明 |
|---|---|---|
| A：候选可运行 | Task0—4在隔离环境通过 | 分阶段队列/交接候选已验证，尚未真实业务验收 |
| B：恢复与控制可靠 | Task5—7故障测试通过 | 重试隔离、未知结果处理、停止/重启边界通过模拟验收 |
| C：数据与前端可接 | Task8通过，迁移动作逐篇可审阅 | 已有结果迁移清单和v2前端契约已准备好 |
| D：正式可用 | Task9真实端到端、并发及收录验收通过 | 按实际达到的并发和吞吐报告正式可用范围 |

网关根因与修复是单独的交付项；调度通过不能替代网关修复。RAG服务端40并发、GPU/嵌入优化另做测量，不在本计划中凭资源截图直接翻倍。

真实测试什么时候开始：Task0即能开始不计费隔离实验；第一组真实5篇要在独立任务、恢复、控制和迁移核对通过后开始。没有完整模型周期及入库读回证据，不提前报“全部修好”。

## 文档自检

- 范围、身份、命名和完成语义：Task1、2、8。
- 并发隔离、交接以及Server/Worker故障：Task0、3、4、9。
- 502/流断/429、分段、未知、预算：Task5。
- RAG/NAS/清理证据：Task6。
- 全局暂停、及时停止、版本切换及回滚：Task7、9。
- 前端状态、指标和真实验收：Task8、9。
- 独立设计复核提出的Server重启队列丢失、重试Worker饥饿、同步超时不可中断三项均已纳入设计与测试；没有由本文执行生产变更。
