# SDD ledger — plan: E:/rag mid/langgraph_workflow/docs/implementation-plan.md

- 授权：2026-10-16 用户批准完整工作流设计，技术检查不调用 LLM。
- 阶段：完整工作流候选已实现，正在真实单篇验收；未启动全量、未修改服务器配置。
- Ruling: 当前工作目录非 Git，桌面 App 仓库有大量既有未提交改动；在独立新目录实现，不创建会丢失这些改动的基于 HEAD 工作树，也不初始化/改写现有仓库。
- Ruling: 用户明确“按设计开始做”覆盖 skill 的重复方案/执行方法询问；记录计划并在当前会话直接实施。
- Pre-flight: Store -> Producer/Graph 使用同一 task/revision/path 身份；Package -> Rag 使用同一已验证 manifest 摘要；Rag -> Store 逐专业回执，完成后才切换版本；所有接口需行为测试覆盖。
- 当前风险：C 盘仅约 0.3 GiB 空闲。全部新依赖、模型状态、缓存和临时输出放 E 盘，不操作系统存储配置。
- Task 1: complete — 独立SQLite台账、来源版本关系、分池公平调度、租约续期/活动排除、稳定文件监听及旧/新交付回流，行为测试已通过。
- Task 2: implemented, live validation in progress — 原始两轮提示词已保存精确快照和哈希；现有free/deepseek-v4.1-flash接线成功，真实read_image已有工具事件及输出文件证据；不能把SDK自带旧runtime当桌面新版。
- Ruling: 本机Harness原生pwsh在restricted-token子进程初始化阶段报0xC0000142，换可用pwsh路径后仍失败；两次无改善后停止盲改，未放宽sandbox/approval策略。FS read/read_image可用，且原生无DOCX/PDF模型读取工具。因此使用电脑端确定性原文读取工具展开DOCX原生部件、脚注/尾注/图片或PDF视图，原件哈希绑定，LLM仍独自判断章节、画像、图表、脱敏、分类和切片。
- Task 3: implemented — 原生RAGFlow客户端已成功读取当前服务器；本地真实HTTP端到端验证通过，含直接切片、精确读回、检索、激活和回执。生产写入尚待真实单篇交付。
- Task 4: implemented — 原生LangGraph后继/返修边、逐节点持久化暂停、检查点/台账崩溃窗口恢复；服务错误不改稿，格式错误自动修复，首轮修复后仍须第二/第三轮，连续无进展暂存及规则变化唤醒。
- Task 5: in progress — UI烟测通过（auto_start=false），VSCode已用code.cmd --reuse-window打开本目录和graph.py，尚未启动全量。
- 独立审查：已修原生返修READY无法接回、完整包重复第三轮、连续版本更新、旧错误切片误认成功、租约重复派发、返修饿死、非法JSON错误分类、旧图片停用、缺正文切片及失败部分成果丢失。另修自身故障包反复接管绕过暂存、坏故障包阻断扫描、ZIP成员图片ID碰撞、DOCX原生图表缺失。版本切换具有持久意图、崩溃补偿、短时串行切换与实际活动修订号；补偿不启用未验证历史版。
- 质量筛选：用户追问已核对原始第一轮，材料筛选、学习价值、适用层次和局部受限用途均保留。没有新增总分、前几百名或整篇数据复算门槛；unresolved_issues是备注，只有processing_blockers表达无法交付的处理障碍。
- 最新全套测试：69 passed；P0独立65项复核通过。真实原稿r008已完整入库到120901K/120210/120202，逐库43文本+2图片；正文、学习、画像、章节树和图表说明检索以及必要图像字节读回通过，入库至完成145秒。激活表确认r008，清理自有work后原稿哈希不变，问题包已RESOLVED。证据 runtime/acceptance-paper/completion-report.json、逐目标回执及events。
- CODEX_FIX_LIST.md P0：business_stage不允许repair，旧数据回溯、未知原点保守补round2；迁移脚本保留真实round2/round3成果，后续真实publish故障仍按publish修复；代码异常包括Service外层均code_error+private traceback；基础设施20次/24小时预算。新增CAS冲突按owner释放租约；旧sources表补回版本关联。
- 生产独立测试库探针通过：10KB、100KB、1MB元数据精确读回，name/id过滤，图片写入/下载/检索，enabled=0检索排除及探针文档清理均已确认。测试库保留供后续故障注入，非用户正式专业库。
- 架构方案前置已满足，进入第2步。JSON语法在同一Harness会话最多补正2次，不新增revision、不增加业务repair_count、不生成failure包；耗尽则暂存单篇。相同session/revision和耗尽无业务返修的回归均通过。尚未改检查点归属和第三轮/专业Send子图；并发标定尚未执行。
- 索引负担：完整画像/章节树保留标准元数据供Agent读回；索引采用既有字段投影和LLM学习切片，避免把数万字诊断再做一次embedding。RAG服务器只读确认版本v0.27.2，原生enabled按整数0/1发送。


## 2026-10-16 最终实施与验收

- CODEX_FIX_LIST的13项和ARCHITECTURE_PLAN四步已实现，先完成P0正式稿后才调整架构。最新完整结论见fix-list-status.md和acceptance.md，本节覆盖前面的历史进行中状态。
- 检查点唯一内部状态、无step_seq对齐、CAS调度副本、同会话JSON修正、第三轮散列缓存、专业Send和独立回执均完成。
- 独立审查发现并修复：消费者引用旧包、汇合清重试预算、verify/activate缺新修订回执、旧reducer记录未清、control保留pending Send、维护并发绕限流、GC身份/内容证明不足。每处均有行为回归。
- 正式r008当前再次读回通过；真实模型问题稿自动返修至r004；服务器写成功后真实进程exit86并恢复无重复；新版本激活旧版检索为空；测试库旧版GC删除前后读回通过。
- 普通venv、全新verification-venv及独立审查均103 passed；UI烟测默认不运行，doctor确认883专业、依赖和RAG读接口。
- 历史10次正式稿模型调用已从私有压缩会话提取usage；明文会话、凭据及思考未导出。详细计数和秒数独立保存，未修改不可变正式交付。
- 持续服务仍未启动；有限并发标定与最终配置另有calibration-report记录。原稿和旧业务仓库未修改。

- 有限标定完成：相同已处理小稿1/2/4路各8份，吞吐4.349/4.305/4.222份每分钟，0错误；24份探针文档删除读回。默认RAG名额调整1，LLM仍2；未声称物理极限或端到端论文产量。


## 十篇真实稿有限试跑（用户追加授权）

- 固定10篇管理学原稿，6 DOCX/2 DOC/2 PDF；避开已登记内容SHA，独立home/交接、source_roots为空。只驱动既有Service.tick，不启动全量、不扫描其他批次。输入绑定与哈希由独立只读复核确认。
- 初始本机16GB内存占用95.5%，90%背压使0模型启动；曾临时单worker/97%试跑。用户随后明确授权关闭不用的应用，保存式关闭编辑器/浏览器，退出后台空闲聊天应用，保留当前对话/试跑/远程/网络；降到65%约5.5GB可用。节点保存后恢复LLM2/RAG1/内存90%，断点保留。
- 首篇PDF图表阶段3次HTTP413：历史图片base64累计约4.1MiB，Harness默认20MiB阈值未触发。复用已装原生image-offload，free路由maxRequestImageBytes设2MiB；不降原图分辨率，逐图理解后落盘。真实续跑已出现10次image/offload，无需自写预览管线或改服务器。
- 确定性4xx/原生AUTH、INVALID_REQUEST、MISSING_CREDENTIAL、INVALID_CREDENTIAL转controller，不原样重试、不作论文修复；408/429/5xx保留网络重试。新增回归先红后绿。
- 原持续run的旧验收指纹因代码修改自然失效；未伪造通过，有限十篇验收仍在运行。最终结果以本批逐篇回执、活动版、原稿SHA、临时清理及再次实际检索为准。
- 用户追加明确：脱敏按正常身份信息尺度；图片恢复description优先、确有必要才保留原图。首篇脱敏重复查看不发布的PDF页面127次，已仅终止该篇自有SDK子进程，保留检查点；脱敏/切片及相应返修采用阶段读取范围，无留图时复用原生附件插件开关关闭read_image，真实SDK初始化通过。不能把该次人为中断记为论文内容失败。
- 第2篇旧资产节点保留4张可描述的普通图/检索截图，按新增澄清从round3_assets生成r002，未编辑已发布r001；前两轮继续沿用。其余符合规则的成果不重做。最新全套120项回归通过；第3篇已完成实际入库、检索、激活和work清理，其余仍在有限试跑，尚不能报告10篇完成。
- 用户再次明确“三轮都错也选择相信，大差不差即可”：不增加模型审稿或人工语义纠错，不因分类/描述细节重跑。身份语义由LLM负责；私有脱敏辅助映射改为不含原值的备注，不再逐词否决交付，单字/空值/辅助结构差异不拦截。常规电话、邮箱、身份证、带标签学号及本地原稿路径检查保留。真实第2篇原有r002交付已按新尺度本地验证通过，未重写其内容；取消了由过严辅助检查触发的模型返修，准备沿用原成果发布。最新全套121 passed。
- 内存恢复后，本次有限试跑在检查点边界改为LLM4/RAG1，未改全量默认配置。暂停仅为载入用户要求的正常处理尺度；已有成果/逐专业回执保留，不重复两轮理解。

## 2026-10-17 按用户确认收窄交付检查（保持停止）

- 本节及 delivery-rules.md 覆盖历史记录中更严格的脱敏、辅助文件、逐类别检索和多轮返修标准。
- 内容判断采用 DeepSeek 最终结果。隐私格式匹配、空图表说明、模型疑点、辅助文件缺失/损坏、编号/关联小差异不再触发返修；保留正常脱敏和描述优先的图片规则。
- 程序仅拦截缺必需文件、最终交付正文丢失、写错目标库、未真正入库。有效专业按代码规范名；未知/缺映射专业显式暂存，其他目标保留成功回执，存在暂存目标不能整篇激活或假完成。
- 每篇额外交付补正硬上限为一次，与同阶段JSON补正共享额度；调用前耐久记账，服务异常/重启不重置。补正成功直接走后继，更新产物指针；不重新调用同业务阶段。远端故障重试原操作，不交给模型重写。
- 新规则与边界回归先红后绿；旧规则的冲突断言已更新。最终完整离线回归157 passed，0失败/错误，网络守卫记录外部连接尝试为0。独立只读复核通过。
- 验证证据：runtime/rule-change-verification-20261017/result.json、pytest.xml、paused-check.json。修改前快照：runtime/rule-change-backup-8b5e6f4913e54560b057ccc76aad48dc。
- 未启动真实模型或服务器入库，未恢复调度；STOP_TRIAL仍存在，入库进程0。既有原稿、不可变交付、检查点及逐目标回执保留。仅将核实为pytest合成输入的失败包移到验证目录，避免测试材料进入问题稿队列。

## 2026-10-17 已批准 A/B/C 规则 v2（保持停止）

- 用户批准新的 A1–A7/B1–B7/C1–C9 规则，本节及 delivery-rules.md v2 覆盖上一节的一次补正和空图表描述一律放行规定；DeepSeek 内容判断、首轮分流和描述优先仍保留。
- 每篇额外定点补交改为累计两次，与同阶段 JSON 修复共享既有持久预算；换版本、报错或重启不重置。网络另走技术重试，成功业务步骤不重跑。
- 缺章节可复用明确字段或正文标题，明确非章节附件豁免；完全无画像/适用结构或空图表说明且无现成内容才构成缺交付。同图表已有说明可直接复用，不另请模型重写。
- 明确作者字段、作者元数据学号或准确定位才参与机械脱敏；一般研究变量、公共对象、参考文献和代码示例不因此拦截。准确映射与机械标记统一处理，正文及跨切片保持一致；模型映射冲突不任选。
- 发布前复用只读封包证明核对；异常字节、任务身份冲突、不可读/缺失的既有封包证明进入 identity 暂停，不靠重算哈希或模型补交掩盖。缺普通必需交付仍走原缺项补交逻辑。
- 当前修订的真实目标回执计算“部分完成”，同时显示写入核对、可检索确认及待处理目标；不覆盖 LangGraph 调度阶段，旧版回执和未确认意图不计成功。
- 新增26个 v2 行为用例，实质问题均先复现再修复。完整离线回归183 passed、0失败/错误、外部连接尝试0；独立只读复核通过。证据：runtime/rules-v2-verification-20261017/result.json、pytest.xml、review.md。
- 停止证据 paused-check.json：STOP_TRIAL存在，running=false，automatic_resume=false，入库进程0；原始两轮提示词与修改前快照哈希一致。未恢复入库、调用真实模型/知识库、迁移或改写真实任务状态。修改前快照：runtime/rules-v2-backup-0112519900734e0dbf65d5ec37bae252。

## 2026-10-17 第三轮改为三份独立提示词（保持停止）

- 用户批准附件三份完整提示词及旧协议冲突、最终正文准确行号、按失败节点补交三个衔接点。沿用现有 assets→redact→chunk 节点，未改图结构或前两轮业务逻辑。
- 新增 prompts/round3_assets.txt、round3_redact.txt、round3_chunk.txt，与附件各段正文一致；第三轮正常调用和子节点补交直接装载对应模板，不拼旧混合协议。消费者故障改用短技术补交，保留真实 failed_stage 与恢复阶段，提示词/工具范围单独选择。
- 程序从最终脱敏正文生成私有行号视图，提供总行数和 SHA；BOM、空行、CRLF、末尾和Unicode换行口径与既有正文物化一致。模型仍决定边界，程序复制原文，不让模型重抄。
- 缓存沿用现有阶段文件校验，新增本阶段模板 SHA，修改模板不再命中旧缓存；原有累计两次补交额度和工具限制继续复用。
- 新增14项独立提示词、阶段补交、行号和缓存行为用例；一次旧提示词措辞断言随确认的附件更新。完整离线回归197 passed、0失败/错误、外部连接尝试0。证据：runtime/independent-prompts-verification-20261017/result.json、pytest.xml。
- compatibility.json：前两轮原始文件与正常/补交四个最终提示词逐字不变，三份新模板与附件一致。paused-check.json：STOP_TRIAL存在，running=false、automatic_resume=false、入库进程0；未调用真实模型/知识库、未恢复入库或迁移实际任务。修改前快照：runtime/independent-prompts-backup-942de7516fdb42f6a07c70b94aa3358d。

## 2026-10-17 统一设计 2.2 补充（当前第 1 阶段）

- 用户授权按八阶段逐步推进；第 1 阶段已完成复用核查，随后按追加要求先补准入、管理退出、Harness 取消和候选切片可见性条款。台账/交接业务代码尚未实施，阶段未完成；不能将本次文档补充当作第 1 阶段验收通过。
- CODEX_WORKFLOW_DESIGN.md 更新为 2.2：AF_PIPE 管理进程逐动作批准、epoch、ticket/最终开始确认、gate_lock 串行化暂停；SQLite 等待在锁外，停止和 scope 代次在锁内复查。独立复核指出冻结/期望等变化也要失效准入快照，已补 scope admission_generation 与场景 41。
- Job 最后句柄关闭影响、维护暂停/收束/退出/重启顺序、远端未决写入待核对均已写明。Harness 实际 dsh.cmd/SDK shutdown/AbortController 代码链已只读核对；生成中合作取消、整树退出耗时与 10 秒兜底仍待第 2 阶段实测，未更换模型接入。
- 当前 RAGFlow v0.27.2 的隔离前置实测失败：文档 status=0 后新增切片 available=true，普通检索两次命中；启用后同查询仍命中。POST available=false 未生效，重复 enabled=0 不更新已有切片；原生切片 PATCH 等状态收敛后能隐藏，但不能消除创建窗口。该缺陷登记为第 4 阶段投递启用前阻断项，本轮未改服务器。
- 证据：docs/workflow-supplement-20261017.md；E:/rag mid/review-isolated/workflow-supplement-20261017-033555/ 下的 candidate-visibility.json、candidate-controls.json、cleanup-reconciliation.json、scope-check.json。包含部署镜像/代码指纹、确切查询、逐次状态与耗时；5 个本次自建探针文档均已删除读回，测试库既有文档保留。
- 当前 STOP_TRIAL 仍存在，未发现匹配的入库执行进程；无真实模型调用、无正式库写入、无调度恢复、无真实台账迁移。只执行了用户要求的独立测试库探针与自有文档清理。文档补充、代码证据、运行失败和待验收项分别记录。

## 2026-10-17 统一设计 2.2 第 1 阶段隔离实施完成

- 用户明确授权仅实施台账与封包交接，完成相关隔离验收后停止。候选位于 `E:/rag mid/review-isolated/workflow-stage1-20261017`；原业务源码 SHA256 全部不变，未复制 config 或真实台账到候选，未部署正式程序。
- 复用 Store.db、seal/validate/publish_package，新增 opt-in PackageHandoff/HandoffLedger：稳定发布意图、全目标原子登记、映射待定保留、引用、期望 CAS、重放及锁忙有限整事务重试。候选避开 Updated manuscript；不接 graph/service/intake，不产生执行许可。
- 27 项新增行为及真实子进程验收，加 45 项相关原回归，最终 72 passed、0失败/错误/跳过、0网络尝试。真实子进程覆盖发布前/后、事务中、提交后退出，多进程同意图与版本竞争、锁等待期间冻结和有限超时。保留公开入口产生的合成包、实际 SQLite 与重复交接结果。
- 独立复核发现默认图片模式漏图库映射、已列辅助文件哈希遗漏、已有包意图修订号三项问题；4项回归先RED再GREEN，已修复并获收尾确认。新交接严格校验全部清单文件，旧入口默认辅助兼容策略保留。
- 文档：候选 `docs/stage1-report.md`、`stage1-interface.md`、`stage1-plan.md`；证据：`evidence/result.json`、`pytest.xml`、`acceptance-latest.json`、`scope-check.json`、`process-check.json`、`independent-review.md`。主设计保持2.2与八阶段范围，只更新当前进度。
- 当前 STOP_TRIAL 存在，匹配入库进程0。未调用真实模型/RAGFlow、未迁移真实状态、未恢复入库；第2阶段IPC/Job/Harness取消、实际图接线、专业投递、修复和清理继续按既定阶段实施。既有第4阶段候选切片可见性阻断保留，本轮未修服务器。
