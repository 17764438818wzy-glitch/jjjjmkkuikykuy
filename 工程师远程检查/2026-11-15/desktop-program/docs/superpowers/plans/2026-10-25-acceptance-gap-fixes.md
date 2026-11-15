# Acceptance Gap Fixes Implementation Plan

完成记录（2026-10-26）：本轮修复、隔离验收、v29 发布和 8791 看板同步已完成。12 项正文链路独立 QA、16 项前端回归通过；15 条交付/15 条引用的旧路径元数据已纠正并只读回验。总负责已用 ego lite 核对页面及介绍卡。证据为 WORK/evidence/coordinator-final-acceptance.json。用户手动试跑尚未启动；真实 50 并发稳定性及生产清理不在已通过范围，清理/NAS 复制/释放开关保持暂停。

> User correction, 2026-10-25 15:54: confirmed papers must proceed through normal body processing/ingestion. The added body-image pending gate was withdrawn and v27 selection restored; immutable v28 evidence remains. Task 1 below now reflects the corrected behavior. New release: confirmed-body-image-20261026-v29. Frontend freshness corrections and isolated cleanup reconciliation continue.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement the owned task. Existing user-appointed chats are the implementers; do not create duplicate task chats.

**Goal:** 修复已证实的剩余缺口，小范围测试通过后同步前端，避免重复昨晚已有效的修改。

**Architecture:** 以已逐文件核对的 v27 为不可变基线，使用同一个隔离候选。各实现文件单一负责人；独立 QA 绑定候选路径与哈希，发布方使用现有成对发布协议。

**Tech Stack:** 现有 Python、SQLite、原生文件锁、pytest、Vben/Vue 与既有只读看板。

**Spec:** `F:/论文预处理系统/运维记录/团队验收修复-20261025/设计与验收边界.md`

## Global Constraints

- 用户已授权修复、小范围测试和测试后看板同步，并要求不重复已做修改；无需再询问一般性实施许可。
- WORK=`F:/论文预处理系统/运维记录/团队验收修复-20261025`；CANDIDATE=`WORK/candidate`。
- v27 清单 SHA256 `d547d96e0a9bc1dfbfbfe764385853a0466bef6d966cc5799db35a9aa5ed271c`，183 更新文件和 324 基础绑定已校验。
- 保留 v27 准入与占用索引实现；不改变 LLM 质量规则、原稿、既有成果、未决保护和生产开关。
- 测试限隔离副本，不调用新 LLM、不恢复全量业务、不运行生产删除；本轮结果不构成真实 50 路持续运行验收。
- 不在不可变发布目录修改文件，不同时覆盖同一实现；每名实现者先记录其精简子计划与去重判断。

## Review Focus

- 已确认的正文图片进入原正文流程，完整携带现有图像、如实记录内容范围及缺项，不重新分到附件、删除或新增人工待办门禁。
- 已成功交付后发生索引锁等待，不得覆盖入库成功；重试应幂等。
- 历史资料缺失不能伪造介绍卡、附件范围或清理完成证据。
- 测试必须加载候选实际文件，不能因旧脚本固定旧版而产生错误通过结论。
- 发布期间其他聊天更新版本或策略时必须保护其改动，不覆盖；诊断加载与业务加载分别报告。

## Task 1: 正文图片衔接 — 后端工程师

**Files:**
- Candidate ownership: `desktop/auto_pipeline.py`, `desktop/preextract_workbench.py`, `desktop/preextract_material.py`；实际仅最后一项有修改。
- Tests: `WORK/tests/test_body_image_admission.py`。
- Plan/evidence: `WORK/backend-plan.md`, `WORK/evidence/backend-*`。

**Interfaces:** 保留现有用途判定与 `process_item` 入口；复用 source_page 图像资产、图像适配和请求规划，不建立新分类器或正文准入门禁。

- [x] 对照最新代码与原维护者交接，记录仍存的读取能力缺口。
- [x] 通过实际流水线调用点的针对性用例暴露 JPG 进入不支持读取器的症状。
- [x] 为已确认正文图片提供现有完整像素/全部帧，保留正文属性与项目身份，正常进入材料准备和请求构造。
- [x] 后端验证真实代表副本/既有回执与普通正文、附件、表单对照；核对原稿和已有结果不变。独立 QA 另见 Task 3。
- [x] 报告确切修改文件、红绿结果及剩余支持边界：evidence/backend-image-material-summary.json。

## Task 2: 清理尾项与持久证据 — rag 知识库管理

**Files:**
- Candidate ownership: `desktop/preextract_cleanup.py`, `desktop/preextract_cleanup_scope.py`, `desktop/purpose_cache_cleanup.py`, `desktop/purpose_cleanup.py`, `desktop/preextract_delivery_queue.py`。仅修改实际必要文件。
- Tests: `WORK/tests/test_cleanup_followup.py`。
- Plan/evidence: `WORK/cleanup-plan.md`, `WORK/evidence/cleanup-*`。

**Interfaces:** 复用原生清理入口、队列 projection、持久介绍卡/身份与已有回执；不修改 NAS 准入、释放权限或正文分流文件。

- [x] 核对 v24/v25/v27 与晚间清理实现；正确暂停和证据保护不归为代码故障。
- [x] 隔离验证 source-index 锁尾项能否续办；现有行为正确则不修改实现。
- [x] 仅在红例证实缺口时做最小幂等补正，保留已成功交付状态和持久介绍卡。
- [x] 用已清理、锁尾项、缺凭据、受保护各一个代表验证。恢复只采用可验证既有资料；证据不足继续待核对。
- [x] 输出实际修改（允许为零）、受保护文件证明和不能自动恢复的历史缺口。

## Task 3: 独立候选验收 — 测试/测试开发工程师

**Files:** 仅 `WORK/tests/` 下独立 QA 入口与 `WORK/evidence/qa-*`，不修改实现。

- [x] 准备候选实际导入路径、模块哈希和基础依赖绑定，隔离数据并禁止网络/生产写入。
- [x] 收齐 Task 1/2 实际改动后，选择对应原症状与保护场景，不重复未变化的 v26/v27 准入压力测试。
- [x] 小范围验证正文图片、正常正文/附件、清理续办、缺凭据和结果保护；检查测试副作用仅在 WORK。
- [x] 对每项给出通过、失败或未测及具体命令、退出码、哈希。失败交回原文件负责人修复。

## Task 4: 一次集成与版本读回 — 运维 / 总负责

**Files:** `WORK/scripts/` 发布/读回入口，`WORK/evidence/` 发布证据；正式包 `程序/updates/confirmed-body-image-20261026-v29`（v28 已撤回，证据保留），禁止覆盖已有同名目录。

- [x] 复用已有完整成对清单、基础绑定、选择锁及原子替换逻辑，不重复数据库迁移与大库备份。
- [x] 先 dry-run；无实际修改不制造新版本，当前选择偏离 baseline 则拒绝覆盖。
- [x] 总负责人确认 Task 3 通过及差异范围后才执行一次发布选择。
- [x] 核对业务策略字节、选择清单与新诊断进程导入；不强制重启业务，不将诊断加载扩大为所有业务生效。

## Task 5: 测试后看板同步 — 前端工程师

**Files:** 以现有 `看板/dashboard`、`看板/frontend/apps/web-antd/src/views/paperflow` 与对应测试为范围；实施前列明确切需要修改的文件，已正确部分不动。

- [x] 核对原看板最新已做改动，清除重复修复项。
- [x] Task 3 通过后接收最终状态字段/原因，只补现有页面实际缺失的映射或说明。
- [x] 验证正文正常进入流程、清理待核对及过期采样不误报；配置/峰值不等于稳定容量。
- [x] 执行相关小范围展示/接口验证和必要构建，通过后同步当前 8791 观察台并读回。
- [x] 总负责人汇总修复、测试、发布、真实加载状态和未覆盖的 50 路稳定性证据。
