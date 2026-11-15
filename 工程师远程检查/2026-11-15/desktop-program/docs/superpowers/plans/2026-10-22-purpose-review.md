# Purpose review implementation plan

> Execute inline with TDD; one independent final review. The user has already approved implementation.

**Goal:** 程序快速初筛、LLM 复检、三路处理；保护正文和附件，保留原审核与结果，任务保持停止。

**Architecture:** 在当前停止修复 overlay 上增量接线。复用原有单请求日志、全局模型限额、缓存、DPAPI、Office 转换和关联交付，不新增第二次全局内容扫描。

**Spec:** ../specs/2026-10-22-purpose-review.md

## Global constraints

- 不触碰生产结果/回执/已有请求或正在运行的独立上传进程。
- 仅 gpt-6.1-sol / max；密钥用 DPAPI，不写源码、提示词、报告或命令参数。
- 不启动批任务；模型调用只限隔离真实样本验收。
- 新 overlay 继承当前激活模块；切换前比较配置，不能覆盖其他窗口的更新。

## Review focus

- 图纸中的设计说明不能升级整份图纸为正文。
- 表格和公式密集的主修计算书必须保持正文身份。
- 开题报告的研究方法不能造成虚假的正文混合；真正混合文件不能删。
- 扫描页、嵌入对象未覆盖或哈希变化不能删除。
- 关联不可跨项目；旧附件交付不得偷偷增加请求。

## Tasks

1. [x] 写失败测试：无全局读取、复检缓存/请求保护、真实文种反例、删除/关联边界。
2. [x] 新增 purpose_evidence.py / purpose_review.py：一次准备、短证据、精确模型参数、缓存和保守失败；复用 request_once / invoke_once / DPAPI。
3. [x] 接入 material_roles.py、associated_materials.py、auto_pipeline.py 和 preextract_workbench.py；保留原审核提示词和封包校验。
4. [x] 加入安全成员清理与新附件计划核查，旧交付仅验证不改写。
5. [x] 离线测试与真实文件隔离验证；指定模型可用后做有限真实验收，不启动批任务。
6. [x] 独立复核、保护哈希检查、封存 overlay 和验收报告；任务仍停止。

## Execution ledger

- 2026-10-22：用户已批准此前讨论方案并补充一次读取、边读边复检的时间要求。当前独立 uploader 存在，保持不动。
- Ruling: 模型列表不含指定型号，保持指定配置，先完成离线实现并请求用户放行，不使用别的型号顶替。
- Ruling: 原始压缩包如已绑定历史回执，禁止自动替换；生成可核对清理计划，不虚报原包已删除。

- Completed: gpt-6.1-sol released; all ten isolated requests used max. 135 offline tests passed, independent review passed, actual form copies deleted safely. Sealed and selected for next safe launch; no production task started. Original uploads and results preserved.
