# 统一设计 2.2 补充与证据记录

日期：2026-10-17（America/Los_Angeles）。对应 `E:/rag mid/CODEX_WORKFLOW_DESIGN.md`。

## 本次修改范围

按用户最新要求，先修正设计确认前的准入漏洞，并完成针对候选切片可见性的隔离前置核验。当前仍是第 1 阶段，已完成复用检查与接口契约，尚未编写台账/封包交接业务代码，不能标记第 1 阶段通过。没有推进第 2—8 阶段实现，没有启动正式调度或入库。

| 条款 | 本次补充 |
| --- | --- |
| §4.3、§5.1 | 即时停止与 SQLite 双重约束；管理进程 AF_PIPE 逐动作批准；request_action ticket 不能执行，begin_action 最终确认；gate_lock 串行化暂停与最终批准；锁外等待 DB/IPC；epoch、一次性许可、失联拒绝及最小台账契约。冻结/期望/服务/所有权的变化也须先失效 scope 的 admission_generation，避免查库后再变更漏放 |
| §5.3—§5.4 | 管理进程持有最后一个 Job 句柄时，退出会终止所属树；正常维护先暂停、收束保存、确认退出再重启；崩溃后核对进程和远端未决写入 |
| §5.3 工具清单 | 写明真实 dsh.cmd 包装及现有 shutdown 传播；不假定 Ctrl+Break 或交互提示；10 秒整树取消计时安排在第 2 阶段，不更换模型接入 |
| §6.3—§6.4 | 先禁用再新增切片的隔离验收成为第 4 阶段投递前置条件；当前部署失败及原生开关能力分开记录；禁止把新增后再禁用当成关闭创建窗口 |
| §11.3、§12 | 与用户确认的八阶段一致；新增场景 36—41；第 1 阶段只做契约，第 2 阶段控制专项，第 4 阶段隔离修复和复验 |

## 已有代码证据（不是运行验收）

- `paperflow/store.py:59`：现有连接 `timeout=30`、WAL、`synchronous=FULL`，异常回滚；`update/claim/mark_active` 已有 BEGIN IMMEDIATE/CAS 可复用。没有动作许可、管理 epoch 或发布交接事务。
- `paperflow/service.py:95,142,151,164` 与 `paperflow/ui.py:56`：线程池、同进程 Event、调度循环停止和等待当前任务完成。没有管理进程—业务工作进程 IPC，也没有即时取消正在运行的 Harness。AF_PIPE 是第 2 阶段选定的原生实现契约，不是现成能力。
- `paperflow/package.py:330,450,474`：现有只读封包身份/哈希检查、封包 READY 和同卷临时目录 rename。第 1 阶段应复用，补充持久意图、确定位置和原子专业任务登记。
- `paperflow/intake.py:98-128`：旧扫描器会读取 `Updated manuscript/<task>/r*/READY.json`。候选必须在独立 sealed 目录，避免未接新控制层时被旧扫描器接走。
- `paperflow/producer.py:129-149,361-397`：固定 dsh.cmd、现有 provider/model/patches、模型超时调用 close。没有将用户暂停接入 Harness，也没有工作流级 Job 包装。

## Harness 取消：代码链已核对，整树实测待完成

安装中的 Python 包为 `deepseek-harness-sdk==0.1.5rc1`。链路为：Python SDK Popen → `C:/Users/Administrator/AppData/Local/Programs/DeepSeek Harness/resources/runtime/cli/bin/dsh.cmd` → 设置 `ELECTRON_RUN_AS_NODE=1` → `DeepSeek Harness.exe --expose-internals` → `app.asar/dsh/node_modules/@deepseek-ai/dsh-desktop-host/lib/cli.js`。

已只读核对：

| 位置 | 能证明的事实 |
| --- | --- |
| `.venv/Lib/site-packages/deepseek_harness/api.py:116`、`client.py:94-125` | close 先 JSON-RPC shutdown，默认内部等待 1 秒，必要时 terminate/kill 直接 Popen 句柄 |
| app.asar 内 `dsh-sdk-jsonrpc-server/lib/index.js:164-200,271-285` | shutdown 关闭 session/LLM fiber；协议分派 initialize、session/prompt、shutdown，没有 session/cancel |
| app.asar 内 `dsh-agent-loop/lib/index.js:815-820,1677-1690` | dispose → cancel → 当前 phase AbortController.abort，并等待 idle |
| app.asar 内 `dsh-llm-pi-ai/lib/index.js:1853-1856,1886-1888,1911` | 调用者 signal 传播给流式请求，识别调用者取消 |
| app.asar 内 `dsh-win32-process/lib/index.js:537-547,659-704` 及 `dsh-subprocess-local/lib/runner.js:191-194,225-301` | Harness 的本地子进程 provider 已有 Job/终止/IPC 失联处理；不证明 Python SDK 根包装链已纳入工作流 Job |

**尚未实测**：真实生成/工具执行中的 shutdown 合作取消、取消到整树退出耗时、SDK 强杀根进程后的后代状态、10 秒 Job 兜底及与模型超时同时 close。当前没有已实现的工作流 Job 控制层；为保持第 1 阶段范围，本次不临时实施控制层或更换 CLI 来完成该测试。第 2 阶段在受管隔离单元中验收。Ctrl+Break 的有效性、是否出现交互提示均未预设结论。

## 候选切片：本轮已实测，基础隔离失败

实测证据目录：`E:/rag mid/review-isolated/workflow-supplement-20261017-033555/`。

通过 SSH 只读核验当前运行容器：

| 字段 | 值 |
| --- | --- |
| 服务 | `http://192.168.1.120:8088` |
| 容器 | `ragflow-server-ragflow-1` |
| `/ragflow/VERSION` | `v0.27.2` |
| 镜像名称 | `local/ragflow-recovery-preserved:20260923` |
| 镜像 ID | `sha256:8e5efcd41e3374317103d4d4520399d393034112a3c7b4de30d1e432f385002c` |
| 容器启动时间 | `2026-10-08T17:05:12.312141432Z` |
| 独立测试库 | `PaperFlow API acceptance 433b9827c44b4cd794a72b1fa653ac78` / `29e8a7cec96011f1976597633927b55c` |

`candidate-visibility.json` 保存实际普通查询、文档/切片完整读回和时间戳。查询只限定上述隔离库，不指定 document_ids，不使用活动文档过滤。关键事实：

1. 文档 `f921e63eca1611f1976597633927b55c` 禁用后 `status="0"`；该部署 GET 的实际字段是 status，不是输出 enabled。
2. 新增切片 `282f9b0454a79e19`，GET 为 `available=true`；普通查询连续两次命中这个确切 ID。
3. 启用后同样查询命中同一切片。直接切片接口已经产生可查询向量索引，文档 `run=UNSTART/progress=0` 不表示该切片不可检索。
4. 启用后再禁用，切片变为 available=false，普通查询不再命中。

因此“先禁用候选文档再新增切片”**不安全，验收失败**。禁用阶段实际已命中，索引未就绪不可能解释这一结果；启用后的正向对照进一步确认查询与索引有效。没有用一次空结果推定通过。

`candidate-controls.json` 为另一自有合成文档的能力核验：

- POST 新增时即传 `available=false`，仍提前可见；部署的 add_chunk 没有读取这一输入。
- 文档已经 status=0 后重复 PATCH enabled=0，仍可检索；部署的 update_document_status_only 只在文档状态改变时更新现有切片。
- 原生批量切片 PATCH `available=false` 经有界轮询、连续两次读回收敛后，切片不可用且普通检索无命中；再启用文档后相同查询能命中。该能力可以控制已存在切片，但不能消除“新增→PATCH”之间的窗口。
- 期间观察到接口写回与 GET/检索有短暂不一致；`candidate-controls-immediate-read.json` 保留最初未等待收敛的观察，不能拿它断言开关失效。后续有界轮询结果是上面的能力结论，不是创建隔离通过。

服务器只读代码指纹与缺口：

| 部署文件 | SHA256 | 相关事实 |
| --- | --- | --- |
| `/ragflow/api/apps/restful_apis/chunk_api.py` | `baae73486b9b807984554a162961ab8e344a6efcde3da42017596fd3a7057e26` | add_chunk 1082—1187 行计算向量并 insert，未初始化 available_int；1272—1273 和 1324—1358 行已有切片状态更新能力 |
| `/ragflow/api/apps/restful_apis/document_api.py` | `06364bf4f07dd070ebf0ddac610dc44b52013269a14c314549f0cef2fbc3d4b7` | 299—301 行把 enabled 交给 update_document_status_only |
| `/ragflow/api/apps/services/document_api_service.py` | `ac43792254aac46d372acaa4bf1a7fb93a4b26df47414ea109983ec7283c6512` | 151—178 行仅状态变化时把 available_int 更新到既有切片 |

后续最小修正方向：第 4 阶段先使候选切片**创建时**的可用状态与向量/索引数据一起写入；优先复用现有字段和文档状态，覆盖正文与图片。若受支持入口不能保证，再形成可审阅的服务端最小补丁并在隔离库复验；不在本轮修改服务器，不默认扩为完整检索过滤体系，不拿客户端事后关闭掩盖创建窗口。

探针只创建带唯一 PF_CANDIDATE_VISIBILITY 前缀的自有合成文档，均按持久保存的 ID 删除并读回确认不存在；隔离库原有文档保留，未删除测试库。`cleanup-reconciliation.json` 汇总本轮所有探针文档的最终核对。最初两次探针准备失败的原始记录亦保留：其中一次清理后按 name 查询触发“无该文档”，另一次检索分页超出 rerank_candidates_count；这两次不计为隔离验收结果。

历史 `runtime/acceptance-paper/probe/report.json` 的 disabled_excluded=true 只覆盖“启用命中后再禁用”。历史记录本身没有部署镜像/代码指纹，不能直接当作当前部署完整前置验收；本轮已重新核对版本与实际顺序。

## 分阶段安排与当前影响

| 阶段 | 要交付/验收 | 当前状态 |
| --- | --- | --- |
| 1 台账与封包交接 | 复用现有连接/校验/发布；交接事务、唯一身份、发布意图、专业待投递任务、CAS/引用；预留控制记录与动作意图契约，不发许可 | 复用核查及契约已补；代码实施、崩溃和并发验收尚未开始，阶段未完成 |
| 2 暂停与进程管理 | AF_PIPE、epoch、暂停/许可串行化；阻塞 DB 时三入口拒绝及旧申请拒绝；正常退出/崩溃恢复；Harness 实测取消到整树退出 | 待实现、待实测；不能用文档复核通过代替 |
| 3 图到封包 | 保留三轮与缓存，接入第 1 阶段交接并结束图；使用第 2 阶段控制接口 | 后续阶段 |
| 4 独立专业投递 | 先修复并通过候选新增切片隔离，再投递/读回/切换/未决核对；核实后端执行边界 | 已发现明确前置阻断项；尚未修复，不影响第 1 阶段隔离台账开发 |
| 5—8 | 问题稿修复、故障排队、材料清理、旧数据与小批接管 | 保持既定顺序，本轮未实施 |

文档补充完成与运行验收通过分别报告。没有真实模型调用，没有恢复正式入库；本轮允许的远端写入仅为上述独立测试库内自建探针文档及其清理。

独立只读复核检查了四项要求、八阶段范围及两份探针读回。复核指出“冻结/期望变更也会让最终许可的 DB 快照过期”，已补入 §5.1 的 scope 失效/对齐协议和场景 41；未以文档复核替代运行测试。
