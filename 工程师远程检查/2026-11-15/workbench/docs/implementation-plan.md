# 论文持续工作流实施计划

> **For agentic workers:** Use superpowers:executing-plans inline; independent read-only reuse and final review are required by reuse-first-dev.

**Goal:** 实现电脑端可持续运行、可恢复、自动修复回流并确认实际入库的 LangGraph 工作流。

**Architecture:** 独立包不启动旧 App；复用原生 LangGraph/SQLite、Harness SDK、专业目录、RAGFlow 请求和 DPAPI 凭据。SQLite 台账调度阶段任务，LangGraph 执行阶段节点，文件交付不可变。

**Tech Stack:** Python 3.14，LangGraph，langgraph-checkpoint-sqlite，DeepSeek Harness SDK，SQLite，Tkinter。

**Spec:** E:/rag mid/langgraph_workflow/docs/design.md

## Global Constraints

- 不改原稿、不覆盖旧代码/任务、不增加语义检查、不设置 100 篇停止。
- 仅现有管理学/法学目录；各专业独立；凭据不进代码、日志或文档。
- C 盘空间紧张：依赖、临时文件、运行状态与缓存全部配置到 E 盘。
- 用户已批准实施，主 agent 本会话执行，不再重复请求逐任务批准。

## Review Focus

1. 接管旧 READY 后原文件被改动，必须拒绝同一修订身份冒用。
2. 远端写入成功但本地未记回执，恢复时查询去重。
3. 新版本只成功一个专业，旧版仍有效，不提前完成切换。
4. 问题稿修复没有变化必须暂存；网络故障不触发内容重写。
5. 启动界面不触发旧任务恢复或全量模型请求。

## Tasks

### Task 1: 台账与发现调度

Files: `paperflow/store.py`, `paperflow/intake.py`, `paperflow/config.py`, `tests/test_store_intake.py`.

Interfaces: `Store.register_source(path, sha256) -> dict`, `Store.claim(stages, owner, now) -> dict | None`, `discover(config, store) -> dict`.

- [x] 先写行为测试：同内容去重、更新新版本、租约恢复、新稿/返修公平、旧交付接入。
- [x] 运行失败测试，最小实现后运行同组测试通过。

### Task 2: 交付协议及三轮生产适配

Files: `paperflow/package.py`, `paperflow/producer.py`, `paperflow/prompts/`, `tests/test_package.py`, `tests/test_producer.py`.

Interfaces: `validate_package(path, task, catalog) -> dict`, `HarnessProducer.run_stage(task, stage) -> dict`, `publish_package(staging, destination) -> Path`.

- [x] 先测缺文件、哈希冲突、路径越界、切片引用、图表仅描述、隐私资产缺失与不可变发布。
- [x] 快照原始两轮提示词；模型适配使用现有免费渠道的原生 SDK；真实文件完成度决定成功，不采信聊天自报。
- [x] 测试通过后再验证原生 SDK 初始化及有限单篇处理。

### Task 3: 原生直接切片入库与读回

Files: `paperflow/rag.py`, `tests/test_rag.py`.

Interfaces: `RagPublisher.publish(task, package, receipt) -> dict`, `RagPublisher.verify(task, package, receipt) -> dict`.

- [x] 本地 HTTP 服务注入写成功响应丢失、分页、多专业部分失败、同名不同内容及版本停用。
- [x] 使用实际客户端处理 API 协议，确认每个切片和图片关系；回执原子落盘。

### Task 4: LangGraph 阶段图与修复闭环

Files: `paperflow/graph.py`, `paperflow/service.py`, `tests/test_workflow.py`.

Interfaces: `Workflow.step(task) -> dict`, `Service.tick() -> dict`, `Service.run(stop_event) -> None`.

- [x] 先测失败返修新修订、基础设施退避、重复无进展暂存、重启恢复、积压限流和单实例。
- [x] 图按每篇/修订保存检查点；每次调度推进一个阶段；LLM 与 RAG 各自限流。

### Task 5: 控制入口、接管与真实验收

Files: `paperflow/__main__.py`, `paperflow/ui.py`, `launch.pyw`, `README.md`, `tests/test_service_ui.py`, `docs/acceptance.md`.

- [x] 命令行支持只读诊断/接管预览/单次推进/持续运行；界面默认静止。
- [x] 独立审查完成后，运行全套新包测试及本地真实 HTTP 验收，再做有限真实模型/知识库验证。
- [x] 记录准确边界：离线通过、真实接入、真实入库、持续运行分别报告；未经验收不启动全量。
