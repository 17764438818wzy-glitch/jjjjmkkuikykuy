# 预处理工具双库分流与图片投影实施计划

> **For agentic workers:** 采用本会话内联执行；每个任务先写失败测试，再实现最小改动并运行测试。

**Goal:** 让多专业预处理工具按专业分别选择 md 库和图片库，使用 `paper_...` 做跨批次去重，并为每张已批准图片生成可检索的 `ragflow_images.md` 投影。

**Architecture:** 保留现有单库 API 兼容层，同时新增按 manifest `role`、再按扩展名兜底的路由器。知识包生成阶段只新增 `ragflow_images.md` 和 manifest role，不改变 `ragflow_assets.md`、结构 JSON、关系 JSON 或交接块。上传阶段把每个 dataset 的主文档和辅助文档记录在独立 destination receipt 中，去重索引优先使用稳定的 `paper_id`。

**Tech Stack:** Python 3、Tkinter、pytest、现有 `Store`/RAGFlow client。

**Spec:** 用户提供的最终版 v2 任务说明。

## Global Constraints

- 只修改 `E:\uv\ragflow_upstream`；不碰 `private/`、`backups/` 或远端 RAGFlow 链路。
- 不修改会计学标注逻辑、`structure.json`/`blocks.json`/`relations.json` 字段含义和 `PAPER_PIPELINE_METADATA_V1` 内容。
- 不重新调用图像模型，不改变原图字节。
- 旧字符串 `dataset_map` 自动升级为 `{paper: old_id, image: ''}`。
- md 库缺 ID 报错；图片库缺 ID 警告并跳过图片；未知类型报错。

### Task 1: 图片投影与 manifest role

**Files:**
- Modify: `knowledge_package.py`
- Test: `test_knowledge_package.py`

- [ ] 写失败测试：批准图片生成 `ragflow_images.md`，记录 12 字段、章节号/标题、AI 描述和识别文字；未批准图片不出现；manifest 为四类上传文件写 role。
- [ ] 运行目标测试，确认新文件和字段缺失导致失败。
- [ ] 实现投影生成，复用已有 figure、structure 和 image 资源值，不调用模型；保持原 sidecar 和交接块不变。
- [ ] 运行目标测试和知识包全套测试。

### Task 2: 配置兼容与 role/extension 路由

**Files:**
- Modify: `auto_config.py`, `ragflow_client.py`
- Test: `test_upload_dedup.py`, new routing tests in `test_upload_dedup.py`

- [ ] 写失败测试：新旧 `dataset_map` 规范化、md 空值报错、图片库空值跳过、role 优先、扩展名兜底、未知类型报错。
- [ ] 运行目标测试确认失败。
- [ ] 添加配置规范化函数和明确错误/警告；实现 role 优先路由，不使用 `_IMAGE_SUFFIXES` 做路由。
- [ ] 保留旧 `upload_items` 调用形状，新增 routed upload 入口。
- [ ] 运行目标测试和既有上传协议测试。

### Task 3: paper_id 去重与双库独立回执

**Files:**
- Modify: `upload_tracking.py`, `ragflow_client.py`, `auto_ragflow.py`
- Test: `test_upload_dedup.py`, `test_automatic.py`

- [ ] 写失败测试：两个不同 AUTO ID 但同一 `paper_id` 在同一 dataset 只产生一个远端文档；md/image 两个 dataset 各自有独立记录；失败/未知状态不污染另一库。
- [ ] 运行测试确认失败。
- [ ] 让 UploadIndex 索引 `paper_id`，历史包可从 manifest/metadata 补出；每个 destination 独立保留主文档和辅助文档回执。
- [ ] 自动交接分别上传 md 与图片侧，只有 md 主文档调用 parse。
- [ ] 运行上传与自动交接测试。

### Task 4: 双下拉框 UI

**Files:**
- Modify: `auto_ui.py`, `app.py`
- Test: `test_controls.py`, `test_result_destination.py`

- [ ] 写失败测试：界面暴露 md/image 两个选择值，保存为一个专业的一对 dataset ID，旧字符串配置仍可读取。
- [ ] 运行测试确认失败。
- [ ] 在连接页添加两个下拉框；专业绑定保存 `{paper, image}`；结果页继续显示 md 目标并使用同一映射上传。
- [ ] 运行 UI/config 测试。

### Task 5: 集成验证与交付报告

**Files:**
- Modify: only tests if needed for verified behavior.

- [ ] 运行全部活动测试，排除 `backups/`。
- [ ] 对一个公开知识包验证 `ragflow_images.md`、12 字段、manifest role 和批准过滤。
- [ ] 验证 `private/`、`backups/` 无变化；报告改动文件、配置默认值、路由/去重证据和是否需要重跑。

