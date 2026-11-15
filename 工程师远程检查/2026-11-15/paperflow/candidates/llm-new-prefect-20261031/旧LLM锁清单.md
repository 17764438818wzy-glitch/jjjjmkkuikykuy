# 旧 LLM 处理代码中的锁与并发控制

> 新接线状态更新：文件夹队列登记、进入 LLM 和进入等待后处理三处，已去掉数据库事务外围重复取得的任务文件锁；请求准入锁和工作目录防重锁保留。新的工作目录锁获取代码已统一，正文 / 附件 / 共享并发仍为 50 / 20 / 50。详见 `../llm-lock-simplification-20261031/锁简化说明.md`。下文是原旧代码清点，并不代表这些锁全部在新链路启用；本次同步后按用户要求未测试。

本清单以旧 Prefect 正文、附件识别调用链，以及它复用的桌面版请求代码为范围；另列可能影响这条链的上游工单、旧桌面批处理和网关控制。存在于代码不等于当前新 Prefect 路径已启用。后处理、RAG、NAS 和清理本身的大量锁不在本次逐项清点范围内。

代码位置中的 `prefect-paperflow` 指 `F:\论文预处理系统\预处理系统2\prefect-paperflow`；`desktop` 指 `F:\论文预处理系统\程序\updates\unified-prefect-20261029-material-r02\desktop`。

## 直接影响 LLM 处理的控制

1. **Prefect 全局并发名额**：`prefect-paperflow/paperflow_prefect/flows.py:91`、`resources.py:31`。模型请求发出时，同时占用 `paperflow-connected-total_llm` 和对应的 `body_llm` / `attachment_llm` 名额，结束后释放；这是 Prefect 服务端的租约，不是文件锁。旧配置必须单独核验，不能因为名字相同就默认新接线已正确使用它们。
2. **批次内异步信号量**：`flows.py:49`、`flows.py:156`。每个批次在自己的进程内按阶段设置 `asyncio.Semaphore`；标题还有最多 16 的独立门。它们不能单独约束所有进程的总并发。
3. **单篇附件异步信号量**：`unified.py:39`。每篇论文附件并发门为 20；这不是系统范围的附件 20 路，更不是共享 50 路的证明。
4. **正文工作目录锁 `profile-recovery.lock`**：`body.py:22`、`profile_recovery.py:53`。防止同一正文工作目录的画像执行和恢复同时进行；锁的保护范围包括检查已有历史、调用模型及后续解析。获取失败立即退出。它是按目录互斥，不是 50 路限制。
5. **附件工作目录锁 `attachment-identification.lock`**：`attachments.py:132`、`attachment_recovery.py:70`。防止同一附件工作目录同时识别或恢复。恢复路径先持附件锁、再持正文恢复锁；并行改造时需要考虑拿锁顺序。
6. **分段初始化锁 `partition-initialization.lock`**：`desktop/preextract_partition_init.py:26`。保护同一正文分段计划的初始化；只保护初始化，不表示已有请求可以重发。旧分段执行还会涉及 `operation.lock`，见第 11 项。
7. **请求开始间隔锁 `<请求起始目录>/<端点和密钥摘要>.json.lock`**：`desktop/preextract_request.py:210`。只有配置了正数最短开始间隔时才使用，协调本机跨进程的同一端点/密钥请求起始时间；不覆盖整个 HTTP 请求，不是全局在途请求数上限。
8. **限流退避锁 `<退避目录>/<端点和密钥摘要>.json.lock`**：`desktop/preextract_throttle.py:18`。保护 429 等退避信息的读写，等待发生在锁外；没有配置退避目录时不启用。它控制冷却，不是并发名额。
9. **请求记录独占创建**：`desktop/preextract_request.py:260`、`:318`、`:337` 和 `desktop/preextract_storage.py:30`。`request-state.json`、`response-events.jsonl`、`response.txt` 使用独占创建及请求状态落盘，保护同一目录下的已有请求和响应证据。这不是能自动释放的普通互斥锁；请求已发送、结果未知或响应不完整时，不能因为重启就自动再发。同一请求若换了目录，单靠这些文件不能去重。
10. **取消网络请求的线程锁**：`desktop/stop_http.py:13`。每个停止控制器保护自己的 socket 列表，方便取消，既不是全局锁，也不限制模型并发。

## 旧桌面批处理及恢复路径

11. **工作目录 `operation.lock`**：`desktop/preextract_pipeline.py:108`、`:223`、`:450`，`desktop/preextract_partition_run.py:76`，以及 `prefect-paperflow/paperflow_prefect/profile_recovery.py:299`。用于旧桌面正文工作、分段和部分恢复的同目录互斥。旧 Prefect 正常正文入口使用的 `profile-recovery.lock` 与它不是同一把锁，不能假定互相排斥。
12. **请求目录登记锁 `single-request/source-index.json.lock`**：`desktop/preextract_pipeline.py:262`、`:308`。保护源文件到请求工作目录的登记历史，并不是模型 HTTP 全程锁。
13. **旧桌面批处理 `private/native-task-body.lock`**：`desktop/native_batch_control.py:560`。限定旧批处理的正文任务控制路径；并不等于新 Prefect 使用了这把锁。
14. **旧桌面阶段准入锁和事务**：`desktop/global_task_control.py:24`、`:68`、`:124`、`:149`。包括进程内 `Lock` / `RLock`、跨进程 `admission.lock`、初始化锁以及 SQLite `BEGIN IMMEDIATE`；保护旧阶段准入和登记，作用域取决于其数据库及进程。
15. **旧桌面进程内并发门与状态锁**：`desktop/auto_pipeline.py:61`、`:107`、`:229`、`:887`、`:892`、`:933`。`_ObservedGate` 用 `BoundedSemaphore` 和线程锁控制并记录本进程并发；`_PreextractSlots` 的条件变量协调阶段名额；`_AdmissionHold` 条件变量保护绑定和阶段登记，代码注明模型发送不持此锁；另有清单、认领、进度线程锁。它们不是跨进程的 Prefect 50 路。
16. **旧解析器修复锁**：`desktop/profile_json.py:16`。进程内 `RLock` 保护 JSON 修复相关操作；属于模型返回后的解析环节，不是请求并发锁。
17. **旧代码版本绑定锁**：`prefect-paperflow/paperflow_prefect/legacy.py:10`。进程内 `threading.Lock` 保护旧 release 模块绑定，避免混用；不是业务请求互斥。

## 与工单及附件相邻、需要区分的控制

18. **人员目录锁与论文身份锁**：`prefect-paperflow/paperflow_prefect/unified_flows.py:77`、`flows.py:175`、`:319`；旧附件队列也检查这两把锁：`attachment_queue.py:126`。旧流程中这些锁可能包住较大范围的单篇处理，不能直接当作正文与附件共享 50 路，也不能不经核验就套到新工单上。
19. **旧附件分发器 `dispatch.lock`**：`attachment_queue.py:146`。防止旧附件调度器重复分发；仅在该旧独立分发路径中起作用。
20. **标题补全锁 `title-request.lock`**：`prefect-paperflow/title_llm_supplement.py:112`。保护标题补全请求；标题补全不在本次接入范围。
21. **附件卡片 `render.lock`**：`prefect-paperflow/paperflow_prefect/attachment_card.py:91`。保护附件说明渲染，属于请求后的处理；本次不执行后处理。
22. **新上游前处理锁和工单登记事务**：`prefect-paperflow/paperflow_prefect/local_preprocessing.py:76`、`:258`、`:327`、`:412`、`:504`、`:530`。包括前处理、单次尝试、资料文件夹执行锁，以及 SQLite 登记事务；它们保护上游执行和工单身份，不是 LLM 50/20 名额。
23. **上游本地阶段名额**：`prefect-paperflow/paperflow_prefect/resources.py:137`、`backend.py:75`、`:152`、`:172`。本地准备/处理有自己的资源租约；与模型实际发送时占用的 LLM 名额不同。
24. **网关进程内门和账号锁**：`E:/WorkBuddy2API-Hub/app/wb-proxy/wb_proxy.py:358`、`:22094`。代码里有进程内信号量/账号相关锁；当前部署版本和新请求是否经过它尚未在此清单中验证，不应把源码存在等同于生效。
25. **旧桌面预准入和磁盘状态线程锁**：`desktop/preextract_admission.py:19` 的条件变量与可重入锁保护旧批次分类结果的发布及等待；`desktop/preextract_disk_pressure.py:15` 的线程锁保护磁盘压力快照。它们可能影响旧桌面批次何时放行工作，但不是 Prefect 的模型并发名额。

## 重要边界

- 文件锁负责具体目录或记录的互斥，线程锁负责单进程，Prefect 租约负责对应 Prefect 服务端中的命名并发限制；三者不能互相代替。
- 请求证据只有在身份、路径与历史一致时才可用于防重；仅凭一个新的空目录不能证明没有发送过。
- 上面列的是旧代码中的控制，不能只凭这些位置推断新接线的运行结果。本次已同步的新接线复用共享并发名额及相关工作目录锁；同步后按用户要求未进行测试，真实 50 路业务负载仍待用户验证。
