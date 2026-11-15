# 教育论文双 DeepSeek 通道实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 学校 DeepSeek 优先、官方 DeepSeek Flash 补位，在统一输出契约和原文证据校验下提高持续有效吞吐。

**Architecture:** 保留论文的稳定队列身份和原远端 ID，将模型路由作为每次解析尝试的持久记录。两个教育 Pipeline 只在模型绑定上不同；单一队列 worker 汇总通道额度并调度，原教育批次经回执核查接入。可信节点诊断驱动有限切换，质量裁决独立处理。

**Tech Stack:** Python 3.14、SQLite WAL、现有 RagflowClient、Windows worker 文件锁、pytest；不增加运行依赖。

**Spec:** [双通道书面设计](../specs/2026-09-26-deepseek-dual-channel-design.md)

## Global Constraints

- 教育 md 库：`ecf88dc4b82b11f1a8846d29005ec004`；教育图片库：`ecfba144b82b11f1a8846d29005ec004`。
- 保留财会库 `01885874b4e311f1807c3d9794ef0978` 和 Pipeline `89ea3542b52a11f1b21a85c6855e5155`，不发出财会写入。
- 学校模型 ID `afb95246b4d311f1b74c89b9f52f3fbe`；官方 Flash 模型 ID `e7e4a8e0b01511f199856dc9cfded896`。
- 学校初始额度 6，官方验证初始额度 2；6 不是学校最高上限。
- 未确认提交只读核查，不因迁移、超时、切换或重启重复提交。
- 暂缓、剔除不自动换模型；技术和契约故障分别记录；图片仅上传。
- 不记录明文密钥、论文正文或原始模型回答到公共文档和运行日志。
- 原工作目录包含既有修改；不 reset、clean 或整体暂存，仅提交本计划产生的文件和明确修改。

## Review Focus

- 多个入口用相同论文再次入队：路由已选择也必须保持稳定身份去重，不能产生 VersionConflict 或新主文档。
- 原解析 UNKNOWN 加上学校冷却：不能转官方重复执行，UNKNOWN 仍计入保守在途额度。
- 论文内容出现 `429`、`rate limit`、`Arrearage`：不能被当成节点诊断或通道故障。
- 同一服务器的 Embedding 欠费：不能只切换生成模型继续消耗官方调用。
- 旧 worker 尚有在途或未确认写入：迁移不得清空其记录或遗漏学校占用。

## 执行方法与进度记录

在当前会话由主执行者依次实施；完成后按 requesting-code-review 技能安排一次独立审查。进度记录在忽略的 `data/private/plan_ledgers/deepseek-dual-channel/progress.md`，每个任务记基线提交、测试命令、结果及必要的裁决。

当前分支为 `codex/education-pipeline-pilot`。延续已有工作目录以包含已验证但尚未提交的教育适配模块；不新建一个缺少这些模块的 HEAD 工作树。开始前保存本次将修改文件的差异基线，仅暂存本计划的改动。

---

### Task 1: 绑定验证与可信诊断

**Files:**
- Create: `ingest_model_routes.py`, `ingest_diagnostics.py`, `education_dual_deploy.py`
- Test: `test_ingest_model_routes.py`, `test_ingest_diagnostics.py`, `test_education_dual_deploy.py`

**Interfaces:**
- `education_dual_deploy.model_only_dsl(source: dict, source_model_id: str, target_model_id: str) -> dict`：只替换已核验生成节点的模型 ID，同时校验图形表单与执行参数的一致性。
- `ingest_diagnostics.model_failure(remote: dict, diagnostic: dict | None) -> dict | None`：返回标准化 `dependency`、`kind`、`status_code`、`retry_after`；缺少可信节点身份或来源时返回 None。
- `ingest_model_routes.load_routes(path: Path, service_url: str) -> dict | None`：读取不含密钥的已验证路由，只接管配置的教育服务/目标/Pipeline；返回学校和官方绑定及容量配置。

- [ ] 写失败测试：模型之外的 DSL 差异被拒绝；仅模型绑定能变化；未知节点不能当成模型节点；正文/原始回答中的限速文字返回 None；Embedding 欠费归入共享依赖；占位 FAIL 返回不可观测。
- [ ] 运行上述三个测试文件，确认因缺少实现失败。
- [ ] 实现接口，诊断读取采用已验证接口或注入的受信任节点记录，不猜测不存在的服务器接口。没有诊断能力时继续产出可解释的不可观测状态。
- [ ] 核对 `/models` 和原教育 DSL；部署脚本提供只读检查与明确部署两个模式，公共日志只记录绑定 ID、契约哈希和错误类。
- [ ] 重跑测试通过；单独提交本任务。

### Task 2: 原子领取与持久解析尝试

**Files:**
- Modify: `ingest_queue.py`
- Test: `test_ingest_routed_queue.py`, `test_ingest_queue.py`

**Interfaces:**
- `QueueStore.claim_routed_parse(owner: str, now: float, lease_seconds: int, route: dict, total_limit: int, channel_limit: int, external_counts: dict) -> dict | None`：在同一 SQLite 写事务中检查容量、按既有手动/自动公平规则选任务、持久化路由并登记解析 intent。
- `QueueStore.parse_attempts(task_id: int) -> list[dict]`：返回尝试号、模型/供应商/通道/Pipeline、契约版本、开始/结束、结果和用量状态。
- `QueueStore.retry_on_channel(task_id: int, expected: str, route: dict, reason: str) -> bool`：仅对已确认终态的技术失败登记一次官方转交，保留 remote_id，不能转交 UNKNOWN 或质量裁决。
- `QueueStore.route_state(channel_key: str) -> dict` / `save_route_state(channel_key: str, state: dict) -> None`：持久化冷却、额度、样本窗口及预算占用。

- [ ] 写失败测试：两个并发领取者不能超额；学校满额时可领取官方；UNKNOWN 占用额度；领取后重启保持模型绑定；切换不改变身份和 remote_id；重复入队复用原任务；显式外部占用减少可用容量；最多一次自动转交。
- [ ] 运行测试确认失败。
- [ ] 保持原任务的请求路由用于身份冲突检查；单独保存实际执行绑定，对网关输出实际 Pipeline。迁移旧数据库时不更改已存来源身份。
- [ ] 为解析尝试、通道状态增加 SQLite 表和索引，数据库事务中禁止网络调用。保存字段使用白名单，拒绝密钥和原始内容。
- [ ] 重跑队列测试及既有队列回归通过；单独提交本任务。

### Task 3: 双通道补位与容量控制

**Files:**
- Create: `ingest_dual_router.py`
- Modify: `ingest_scheduler.py`, `ingest_worker.py`, `ingest_gateway.py`
- Test: `test_ingest_dual_router.py`, `test_ingest_scheduler.py`, `test_ingest_gateway.py`, `test_ingest_worker_dispatch.py`

**Interfaces:**
- `DeepSeekRouter(queue: QueueStore, routes: dict)`。
- `DeepSeekRouter.claim(owner: str, now: float, external_counts: dict) -> dict | None`：学校优先、官方补位，受总额度及独立额度约束。
- `DeepSeekRouter.observe(task: dict, remote: dict, outcome: str, diagnostic: dict | None, now: float) -> bool`：记录终态/用量和可信故障；返回是否已登记官方转交。
- `DeepSeekRouter.snapshot() -> dict`：输出容量、冷却、有效裁决、入库、技术失败、契约失败及真实/估算/未知用量，供日志与试跑读取。
- `WorkPool(..., router=None, diagnostics_reader=None, external_counts_reader=None)`：关闭路由时兼容现有行为；教育已配置任务交由 router，其他专业保持原调度。

- [ ] 写失败测试：学校优先；满额/冷却时官方补位；官方异常不停止学校；运行中不改绑定；质量裁决继续补位但不切换；RAGFlow 429 暂停服务；Embedding 故障暂停共享依赖；重启保留冷却；输出契约失败不能提升学校额度。
- [ ] 测试学校容量：有积压且实际满额的有效样本窗口才加量，明确限速快速减量；无样本或以不明失败为主时不加量。
- [ ] 测试官方预算：真实用量可汇总；缺用量明确标记未知；若配置费用限额，未知价目/无法保守预留时阻止新的官方派发，学校照常运行。
- [ ] 运行测试确认失败，随后实现路由和 WorkPool 接口。网关在重绑定前核对源身份、当前 Pipeline 与远端状态，拒绝改动 RUNNING 或未确认任务。
- [ ] worker 从工作台 data root 的路由文件加载配置，密钥仍由现有 DPAPI/RAGFlow 模型管理保存。开启路由后采用总额度配置，不能沿用六个总名额把官方容量挡住。
- [ ] 重跑相关回归通过；单独提交本任务。

### Task 4: 原批次接管与 artifacts 尝试版本

**Files:**
- Create: `education_dual_adopt.py`
- Modify: `ingest_worker.py`, `ingest_education.py`, `ingest_workbench.py`, `ingest_service.py`
- Test: `test_education_dual_adopt.py`, `test_ingest_education.py`, `test_ingest_workbench.py`, `test_ingest_service.py`, `test_ingest_entrypoints.py`

**Interfaces:**
- `education_dual_adopt.audit_batch(data_root: Path, run_id: str, client, queue: QueueStore) -> dict`：只读核验旧锁/回执/源哈希/远端 ID、名称、Pipeline 和状态，返回可迁移/不确定/冲突清单。
- `education_dual_adopt.adopt_batch(data_root: Path, run_id: str, client, queue: QueueStore, audit: dict) -> dict`：要求旧派发已停止且审计仍匹配，持久化逐篇接管和原回执投影；已完成论文不重新解析，未知论文继续核查。
- `EducationArtifacts` 接受带解析尝试绑定的任务；重解析的产物按尝试版本保存，旧来源记录保留，最新指针只能指向通过准入和来源校验的产物。

- [ ] 写失败测试：旧 worker 在运行时禁止不协调接管；未知写入不重提交；旧 DONE 不重解析；多个冲突远端回执拒绝采用；二次解析产物保留旧版本且不把失效 chunk 指针当成当前来源。
- [ ] 运行测试确认失败。
- [ ] 复用现有教育 identity normalization、规格构建和旧回执采用逻辑。禁止用“本地 fail”覆盖远端更新后的 DONE；主论文/图片角色分别采用原 ID。
- [ ] 旧 worker 退出后核实锁，再接入同一持久队列；原 manifest 仍由工作台负责。自动入队、手动入队和原批次新增完成论文共享路由及额度。
- [ ] 重跑全部入口/身份/教育回归通过；单独提交本任务。

### Task 5: 实际调用、容量验证与上线

**Files:**
- Create: `education_dual_trial.py`
- Test: `test_education_dual_trial.py`
- Report: `docs/superpowers/specs/2026-09-26-deepseek-dual-channel-verification.md`
- Private evidence: `data/private/education_dual_channel/`

**Interfaces:**
- `education_dual_trial.select_samples(items: list[dict], size: int, previous_results: dict) -> list[dict]`：按长度及方法类型选代表性已完成预处理论文，不能全部选原不明失败文章。
- `education_dual_trial.summarize(queue: QueueStore, observations: list[dict]) -> dict`：分开统计有效裁决、入库、技术/契约故障、耗时与真实远端重叠，不把 FAIL 终态吞吐当成成功吞吐。

- [ ] 写失败测试：代表性样本选择、统计口径、禁止重复主文档、试跑记录缺失不得标记通过；运行确认失败，随后实现最小试跑驱动。
- [ ] 核对并保存两条教育 Pipeline 的 DSL 差异与官方调用回执。使用八篇验证模型契约和 artifacts，使用二十篇验证双通道补位；沿用已确认原文档 ID 或使用未上传论文，不重复建主文档。
- [ ] 若现有预处理没有新增正常待处理论文，选择原已明确入库且方法/长度具有代表性的论文做明确复核试跑；记录这一替代及成本，不能用它推算全量准入率。
- [ ] 在有效输出稳定后，以学校 6、9、12 等档位实测；没有吞吐平台证据时只报告已验证容量，不宣称最高上限。
- [ ] 按测试证据设置正式额度、启用官方溢出并恢复受协调的全量 worker；若某项门槛不满足，保留已完成实现和回执，明确报告未启用的能力及原因。
- [ ] 运行相关完整回归及项目完整 pytest；既有基线失败按名称报告，不能顺手修改无关模块。代码审查后修复实质风险。
- [ ] 报告代码验证、官方真实调用、八/二十篇结果、学校容量实测和当前全量状态；仅提交代码/测试/公开报告，不提交论文或密钥。

## 完成判据

双通道路由实际运行、原文档不重复、未知写入不重复提交、质量裁决及时补位、官方/学校故障相互隔离、真实来源 artifacts 可追溯；学校容量只报告有有效输出支撑的已测档位。完整回归和独立审查记录可查。未完成实测、不可观测诊断或缺少官方凭据时不得声称全量双通道已经启用。
