# 预处理工作台审查修复实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 修复用户已批准的审查报告中 12 项可复现缺陷，恢复安全、准确、低并发的持续处理。

**Architecture:** 对现有执行流程做有界修复。任务取消、终态和成员身份保持统一；账本增量读取与缓存诊断索引集中在其模块；公开序列化隐藏原始媒体名，研究图形保持保守保留。不进行独立教育入库工具重构。

**Tech Stack:** Python 3.14、Tkinter、pytest、本地 JSON/JSONL、Windows、模拟 HTTP adapter。

**Spec:** data/diagnostics/skills-audit-20260927/审查报告.md 与 findings.json；用户“好的按照这个来修改把”批准该修复范围及顺序。

## Global Constraints

- 保留内网文字接口；现有 papers=4、images_per_paper=2、global_llm=4、max_retries=4。不切换文字服务商或提高并发。
- 原文件只读；不放宽证据完整性、脱敏、发布或远端交接门槛。
- 保留所有预先存在的未提交变更；仅修改指定文件，备份保存在 data/diagnostics/audit-fixes-20260927。
- 所有复现使用隔离目录与模拟 HTTP；不读取或输出密钥、不发真实模型或上传请求。
- 不提交不相关工作、不操作 Git 分支或强制结束工作台。测试完成后正常重载现有工作台并从保存进度续跑。
- max_retries 在现有程序中作为总尝试次数（1–8）执行；文字和视觉相同上限，取消等待不发送请求。保留视觉自身较小上限。
- 三个互不重叠的文件域按 dispatching-parallel-agents 技能独立处理；auto_pipeline.py、app.py、script_task.py 由主代理负责集成。

## Review Focus

1. 父批次停止发生在子任务等 gate 时：等待可取消且没有新发送，不由子任务 clear 父取消。
2. 输出报告或终态写入异常：已有结果留存，批次不挂在 running，未完成输入可恢复。
3. 相同归档成员跨停止续跑：稳定逻辑身份、输入队列完整，没有原包和成员重复重试。
4. 账本外部追加、截断、轮换及缓存重启/设置变化：增量读取不漏账、摘要新鲜、索引不能使错误响应获准。
5. 部件名、保护截图、带题注连接线：公开字段无原始名，研究证据不静默删除。

## Task 1 — 模型请求与共享熔断（S1/S4/S5/S6/Q1）

Files: agent_client.py、vision_circuit.py；新增 test_audit_agent_fixes.py。
Interface: AgentClient.call 保持原接口；gate 支持 acquire(timeout)；VisionCircuit 按路径共享协调并持续读取持久状态，半开探针有恢复租期。

- [x] 将停止等名额、备用缓存复用、3/4/8尝试次数、非正常视觉结束、两实例失败累计/单探针/探针中断恢复写成红测试。
- [x] 运行红测试，记录失败，最小修复。
- [x] 运行上述测试及 existing vision/concurrency/stage-resume/output-truncation suites，记录兼容问题。

## Task 2 — 输入保留与公开隐私（Q4/Q5/Q6）

Files: native_docx.py、auto_evidence.py、evidence.py、knowledge_package.py（只在必要处改动）；新增 test_audit_input_fixes.py。
Interface: 既有解析、脱敏、导出、发布函数接口保持；公开元数据不含 display_crop.source_part，完整性恢复不得静默跳过题注箭头。

- [x] 将公开媒体名泄露、保护截图误删除、题注箭头遗漏写成红测试。
- [x] 执行红测试，修复公开序列化、装饰豁免和连接线判定。
- [x] 运行 input/graphics/visual-privacy/package suites；保留无题注纯装饰处理的已有合理行为。

## Task 3 — 账本与缓存性能（Q2/Q3）

Files: billing.py、billing_ui.py、stage_cache.py；新增 test_audit_io_fixes.py。
Interface: Ledger.records 保持列表与过滤接口，按路径增量消费 JSONL；新增可复用摘要接口如需由消费者集成。StageCache 外部接口保持不变，只索引同源/版本/端点的诊断摘要。UI进度使用事件中已算好的账目。

- [x] 增量追加/截断/轮换、UI单事件无全史读、缓存无关源读取量写成红测试。
- [x] 修复并验证计费 begin/finish 去重、未知 usage、外部追加兼容及缓存失效。
- [x] 用合成约1万条账本/1万缓存重复先前性能探针，记录增长成本。

## Task 4 — 任务终态、取消传播与归档身份（S2/S3 与 S1集成）

Files: auto_pipeline.py、app.py、script_task.py；新增 test_audit_task_fixes.py，必要时新增小型 archive_identity.py。
Interface: 来源记录追加可选稳定 archive_member_key/member_relative_path；老记录按 input_source_path 与 /archive/ 相对成员路径兼容。重试按逻辑身份，不按每轮私有目录。取消对象仅顶层 run 清除。

- [x] 输出保存失败仍进入 failed/stopped；父停止传播；归档两个成员跨三次停止续跑计数唯一，源文件不变，待处理成员可见：先红。
- [x] 实现最小修复，保存扩展的队列及来源身份，排除原包与成员重复重试。
- [x] 运行 script/parallel/resume/retry-boundary suites，适配新的请求取消接口。

## Task 5 — 集成、独立审查、重载与续跑

- [x] 将三项审查复现期望测试复制到常规测试并验证全部变绿。
- [x] 全套运行：C:/Python314/python.exe -B -X utf8 -m pytest . -q --ignore=data --ignore=baseline --ignore=backups。
- [x] 用本轮备份生成精确差异文件，独立 reviewer 只审这12项修复；修复关键发现后做必要覆盖验证。
- [x] 校验派生正文与媒体原图保留、接口配置未变、停止后无新发送、日志读取成本降低。
- [x] 正常关闭已停止旧窗口，确认单实例恢复入口载入新版本，仅继续未完成输入；若原入口过期，重建同等入口。
- [x] 保存修复报告、测试命令/结果/性能、审查结论及真实恢复状态，更新进度记录。

## Execution record

本轮用户已经批准审查中的具体方案。按既有工作台直接修复以保留现场工作树；备份代替另起缺少未提交变更的 checkout。保留本地变更供用户审查，不进行自动 commit。
