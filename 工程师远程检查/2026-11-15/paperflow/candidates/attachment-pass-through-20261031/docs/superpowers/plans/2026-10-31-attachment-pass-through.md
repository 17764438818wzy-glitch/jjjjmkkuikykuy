# 附件直通 Implementation Plan

> **Execution:** 复用已有接口，实施在独立候选；先写针对性离线测试，不调用真实模型。

**Goal:** 本地不解析、转换或预览附件；逐篇保留上游已登记附件、原件副本和已有证据，自动交给既有 LLM 附件说明流程。

**Architecture:** 以新版登记回执和交付副本为输入。附件清单保留完整来源、证据和历史信息；附件专用调度复用既有逐附件工作流。新路径传递 upstream-only 策略给 LLM 证据读取逻辑，缺少证据时只用已有文件事实，不触发回退预览；旧路径行为不变。

**Tech Stack:** Python, pytest, existing Paperflow receipt/workflow and attachment APIs.

**Spec:** 本次对话用户确认：本地只转交附件及上游已有信息；LLM 简单标注，信息不足不编造；一个压缩包为一个附件；原件不变。

## Global Constraints

- 只修改附件交接及附件识别相关代码，不重做分类、不修改正文行为。
- 使用登记的交付副本，不回退原始来源路径；保留已有请求结果及未知请求保护。
- 一个附件失败不阻断其他附件；不启动生产、不调用真实模型。

## Review Focus

- 同组多个附件、一个压缩包只对应一次识别。
- 有证据与无证据均不重新读取附件内容；真正的模型请求包含已有证据。
- 原上游路径不可用时仍使用已登记交付副本。
- 某附件不可用或证据有问题时其他附件继续。
- 复用历史已完成结果，未知请求不重复发出。

### Task 1: 本地附件直通

**Files:** `local_prepare.py`, `attachment_handoff.py`, `tests/test_attachment_passthrough.py`.

- [ ] 离线测试实际登记回执、原件不变、完整来源/证据透传，并让任何预览或格式探测调用直接报错；确认旧行为失败。
- [ ] 精简 `prepare_registered_attachments(item, *, receipt_root, work_dir)`，保留既有身份校验和历史，移除附件格式探测及预览；提供可复用的登记附件调度入口。
- [ ] 测试附件专用调度：有正文也只调附件，多个附件及单项失败相互独立。

### Task 2: LLM 复用上游资料

**Files:** `paperflow_prefect/attachment_input.py`, `paperflow_prefect/attachments.py`, `paperflow_prefect/backend.py`, `paperflow_prefect/workflow.py`.

- [ ] 测试副本验证、有/无上游证据与实际模型请求；确保无证据时不调用本地解析。
- [ ] 对新直通路径使用新版证据验证器和交付副本，禁用自动读取、转换及重命名副本；保留旧路径、现有提示词与请求历史行为。
- [ ] 用合成输入及离线模型回调验证逐附件交接；运行相关既有测试及候选完整测试，记录边界。
