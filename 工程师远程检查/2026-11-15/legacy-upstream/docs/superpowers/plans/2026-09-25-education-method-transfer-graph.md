# 教育论文方法迁移 Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 从已验证的一次标注和真实正文切片构建跨论文共用方法节点，并用无结构编译的教育 v5 首轮链路验证提速。

**Architecture:** 保留 v2 与财会配置不变。新增纯函数生成每篇方法 Graph，再汇总方法索引。隔离 v5 只运行交接、标注、准入、正文索引；导入 worker 在 DONE 后写 Tree 与方法 Graph artifact。

**Tech Stack:** Python 3.14、pytest、RAGFlow REST API、现有 DPAPI 凭据加载。

**Spec:** `docs/superpowers/specs/2026-09-25-education-method-transfer-graph-design.md`

## Global Constraints

- 保持 `paper_id`、远端 doc ID、正文 chunk ID 的一致性；不重新上传既有论文。
- 方法 Graph 不保存旧论文的时间、数值、学校名和结论正文。
- 只链接有实际执行步骤和可定位正文引文的方法。
- v2、财会库和财会 Pipeline 不变；先隔离试跑，后按回执安全切换。

## Review Focus

- 模型列出某方法但只有计划：不建边，记录缺口。
- 一条引文跨切片或仅落在 `index`/`assets`：不建边。
- 同一方法被多个实际步骤证明：一个方法节点、一条论文关系，合并真实来源 ID。
- 近义方法名称与组合方法：归一到受控具体方法；组合标签不建公共节点。
- 旧 Pipeline 正在解析和欠费重试：保持原绑定，安全重启后不重复提交。

---

### Task 1: 方法 Graph 纯函数

**Files:** Create `education_method_graph.py`; create `test_education_method_graph.py`.

**Interfaces:** `build_method_graph(paper_id, source_doc_id, metadata, chunks) -> dict`；`merge_method_graphs(graphs) -> dict`。

- [ ] 写失败测试：不同研究主体共用 `问卷调查法` 方法节点，边分别携带真实 doc/chunk ID；Graph 字符串不包含样本数、年份和学校名。
- [ ] 写失败测试：计划步骤、未知方法、缺失正文引文、外来 doc/chunk 被拒或记录缺口。
- [ ] 实现受控方法归一、实际步骤匹配、原文切片定位、稳定 ID 和方法倒排索引。
- [ ] 运行 `python -m pytest -q test_education_method_graph.py`。

### Task 2: DONE 后 artifact 与回执

**Files:** Modify `education_stream_ingest.py`; modify `test_education_stream_ingest.py`.

**Interfaces:** `Stream.method_graph(item, package)` 与现有 `Stream.direct_tree` 并列；输出 `private_dir/method_graphs/<paper_id>.json`。

- [ ] 写失败测试：DONE 且入库后生成 Tree、方法 Graph；重复 tick 复用同一回执和 artifact。
- [ ] 写失败测试：远端身份不符或正文来源缺失时记录待核查，不阻断图片回执或误判论文低质。
- [ ] 实现每 tick 有限量回填、原子写 JSON、方法索引增量重建入口。
- [ ] 运行 `python -m pytest -q test_education_stream_ingest.py test_education_method_graph.py`。

### Task 3: 隔离快速 v5 Pipeline

**Files:** Create `education_fast_pipeline.py`; create `test_education_fast_pipeline.py`; add private deployment receipt under `data/private/education_pilot/`.

**Interfaces:** `fast_dsl(source_dsl) -> dict` 移除两处结构编译节点并接通 `ArtifactSources`；`deploy()` 只创建隔离 agent。

- [ ] 写失败测试：仅移除两个 Compiler，保留全文标注、准入、Profile、正文/索引切片和 Tokenizer；图节点、上下游及参数引用一致。
- [ ] 实现 DSL 克隆与远端创建后回读校验，不更新 v2。
- [ ] 用同一规范知识包在隔离 dataset 解析；核对 DONE、准入、Profile、正文/索引 chunk 和耗时。
- [ ] 运行 `python -m pytest -q test_education_fast_pipeline.py test_education_pipeline_builder.py`。

### Task 4: 方法检索试跑与切换

**Files:** Create `education_method_pilot.py`; update `data/private/education_pilot/deployment.json` only after acceptance; use existing worker logs/receipts.

**Interfaces:** 方法索引查找 `method -> paper_ids -> source_doc_ids/source_chunk_ids`，返回不同题材论文的可验证操作章节。

- [ ] 在至少两篇题材不同、方法相同的已入库论文上验证共享方法节点与正文回链。
- [ ] 验证 v5 不产生研究数据/时间/具体主体公共节点；对比同篇 v2 耗时。
- [ ] 核对 worker 进程、锁、回执与不确定状态；安全停止旧进程，原子更新新论文 Pipeline ID，旧欠费文档继续原 Pipeline，然后隐藏启动新 worker。
- [ ] 验证新待解析论文使用 v5，旧已提交论文仍按原回执完成；失败则恢复 v2 部署指向并停止新提交。
