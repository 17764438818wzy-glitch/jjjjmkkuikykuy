# Article Repository Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 复用旧论文资产页，交付服务器上的文件夹级文章查询、画像、下载和修订同步。

**Architecture:** 独立目录使用 PostgreSQL，接入已保存的新旧入库证据，不调用旧 RAGFlow。API 提供持久条目、精确专业查询、受根目录约束的 NAS 下载和显式修订同步；复用 Vue/Ant Design 页面结构和旧画像映射。新入库登记与目录读取不承担工作流控制。

**Tech Stack:** Python/FastAPI, psycopg/PostgreSQL, Vue 3/Ant Design Vue, existing Qdrant gateway.

**Spec:** work/article-repository/requirements.md

## Global Constraints

按文件夹区分身份，原始文件只读，缺失与未知分开；保留人工字段；不自动迁移旧文；服务无需处理电脑；局域网无登录。

## Review Focus

相同正文不同文件夹不得合并；同文件夹重处理保留人工修订；NAS 不可达仍可查目录；Qdrant 修订失败不得先撤销旧归属；旧数据覆盖范围不可冒充全量。

### Task 1: persistent catalog and field projection

Files: article_repository/catalog.py, profiles.py, schema.sql; tests/test_catalog.py.
- [x] 写入并执行真实隔离 PostgreSQL 测试，覆盖文件夹身份、精确专业+仅标题检索、字段修订优先、缺字段和并发版本冲突。
- [x] 实现 Catalog.register(record), search(keyword,major,page,page_size), detail(id), edit(id,revision,changes)。
- [x] 重跑上述测试并记录结果。

### Task 2: source import and NAS downloads

Files: article_repository/imports.py, files.py, api.py; tests/test_files.py, test_import_proof.py, test_export.py, test_legacy_inventory.py, test_nas_offline.py.
- [x] 测试旧目录 sightings 的文件夹还原、新完成文件夹输入、读路径约束、缺文件、单文件与 ZIP。
- [x] 实现只读来源适配和安全流下载；目录独立于 NAS 可达性。
- [x] 对实际本地来源进行只读导出，服务器隔离导入并核对数量与身份。

### Task 3: explicit Qdrant revision synchronization

Files: article_repository/sync.py; tests/test_sync.py.
- [x] 测试先写新版本并确认全部归属，才撤销旧版可见性；失败保留旧版；未迁资料不迁移。
- [x] 复用现有网关批量入库与完整画像元数据，在修订时保存版本并更新检索文本。
- [x] 隔离测试库验证标题/画像/专业修改及原始下载未变，不触及生产文章内容。

### Task 4: reused frontend and deployment

Files: frontend/ArticleRepository.vue, PaperProfile.vue, ArchiveCard.vue, main.js, vite.config.js; compose.yaml, Dockerfile.
- [x] 从已核对旧组件复用列表、Drawer、画像和下载样式；加入已确认字段、整包和修订。
- [x] 构建、浏览器验收列表组合搜索/详情/下载/编辑（隔离数据）。
- [x] 部署独立服务器服务与 PostgreSQL，导入已确认来源，现有面板增加仓库链接。
- [x] 验证服务器端查询时延、NAS实际下载、仅一次同步结果；独立审查实际diff与证据。

## Execution record

实施已获用户“没问题 你开始弄吧”授权。旧看板 git 工作区存在大量既有未提交改动；当前 zhe 是独立任务目录，不是 git 仓库。使用 work/article-repository 独立候选保存改动，避免覆盖旧看板与并行工作。

最终隔离测试 23 项通过。独立审查指出的同步失败保留旧版本、来源时间与身份绑定、并发同步、部分归档、图片关联、NAS 异常与前端迟到响应等问题已修正并验证；真实浏览器另发现并修复 Ant Design 表单未绑定 model 的提交问题。

服务器入口 http://192.168.1.120:8098/ ，已接入 3,758 个历史文件夹与 1,842 个新链路完成文件夹，共 5,600 条。历史覆盖范围不代表旧服务全量。

5 并发客户端 40 次查询 P95 364.16 ms；一次实际浏览器输入到绘制 1,768 ms。真实 NAS 原文和 183,187,736 字节 ZIP 下载及字节完整性已核对。独立 Qdrant 集合完成标题、画像、专业修改与原文保留验收；未迁旧文修改未触发迁移。证据见 outputs/article-repository-validation.json、outputs/article-repository-browser-validation.json 和 outputs/文章仓库交付说明.md。
