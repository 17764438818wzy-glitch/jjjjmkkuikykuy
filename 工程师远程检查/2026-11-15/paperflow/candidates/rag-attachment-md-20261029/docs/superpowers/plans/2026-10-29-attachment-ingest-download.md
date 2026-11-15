# 附件 MD 接收、存储和下载实施计划

> For agentic workers: 使用 superpowers:executing-plans 逐项实施；复用审查与最终审查只读。

**Goal:** 消费原附件与整合 MD，复用原生 RAG Files/文档能力完成唯一原件存储、专业关联、读回及下载，并只用一份已冻结真实附件联验。

**Architecture:** 第三状态保持已有产物和调用边界；第四状态新增显式 CLI，不覆盖正在运行的后端。接收器严格核验冻结身份、MD 固定字段和原件；发布器使用原生客户端、RagPublisher 与 _Budget，逐请求持久化 intent，未知只回查，成功复用。原件通过原生 link-to-datasets 仅链接最高专业资料库，不解析且零切片；下载按已核验回执中的唯一 file_id 复用原生 RAGFlow 下载接口，并提供 CLI。

**Tech Stack:** 现有 Python、filelock、Prefect 共享 rag 额度、原生 RagflowClient/RagPublisher、原生离线 token counter；不增加依赖。

**Spec:** ../../../../rag-stage4-20-20261028/docs/superpowers/specs/2026-10-29-attachment-handoff.md

## Global Constraints

- 原附件 + 一个完整 attachment.md；附件意图和正文模型不重复调用。
- 本轮仅一个精确冻结真实附件；未知请求不重发，成功只复用；正文不重跑、不启动批次、不改全局额度、不重启服务、不做 NAS 清理。
- 当前主源码为新基线。旧第四状态候选的并发改动保持独立，不能全目录覆盖主源码。
- 模板 attachment-card/1；not_read 仅确知未尝试读取，历史未知/读过未留存为 unavailable；文本 HTML 解码一次、percent 解码一次。
- 最高专业取原画像的有序 routing_candidates；原件只在该专业的明确 Files 目录一份，主 MD 及其他专业引用用明确的专业资料库映射。
- 若没有已核验正文远端 ID，按冻结论文编号和正文来源哈希关联，记录 paper_identity_only，不冒称已关联远端正文。
- 新增模块只在显式 CLI 被调用时执行；导入无业务请求。真实联验前须收到第三状态冻结清单并核对历史及目标。

## Review Focus

- 原件与 MD 错配、路径/归属不同、字段重复或哈希改变：上传前拒绝。
- 上传/切片/元数据/启用超时：先查询同一对象，不能重发未知请求或重建原件。
- 修改 MD、跨专业引用、并发重复调用：复用同一原件与锁，保留版本回执。
- 长目录：沿用 _Budget.split；完整 MD 保留，切片精确读回，不能截掉目录或把一份 MD 强制当一片。
- 下载内容改变或输出文件已存在：验证源哈希后交付，不能覆盖原件或其他本地文件；下载仅使用回执绑定的单项附件。

## 文件归属

第四状态新增：paperflow_prefect/attachment_ingest.py（接收）、attachment_publish.py（存储/发布）、attachment_download.py（下载）、项目根 attachment_ingest_cli.py、对应 tests/test_attachment_ingest.py、test_attachment_publish.py、test_attachment_download.py、stage4 文档。复用现有 records.py/legacy.py；不改第三状态 backend.py/workflow.py/flows.py/attachments.py/stage3_handoff.py，不改原生客户端。

## Task 1: 接收与冻结校验

- [x] 写失败测试：真实格式读入、原件哈希/归属错配拒绝、重复字段拒绝、目录计数/状态约束、特殊路径解码。
- [x] 新增 read_attachment_delivery(item) -> dict；只消费交付字典与文件，不依赖 description.json。
- [x] 用真实上游离线示例及相关测试验证，记录输出。

## Task 2: 原件与 MD 发布

- [x] 写失败测试：原件唯一、超时已写入收养、未知未找到不补发、MD 长文本覆盖、说明更新不重传原件、专业引用同一 file_id。
- [x] 新增 ingest_attachment(delivery, route, client, receipt_root, budget, *, read_only=False)；共享 filelock 与持久化请求 intent，复用原生客户端和切片预算。
- [x] 精确核验远端文件位置/字节、MD 原文、元数据、切片、启用及检索。

## Task 3: 显式入口与下载

- [x] 写失败测试：按回执 GET 下载、错误哈希拒绝、已有本地输出不覆盖、回执与服务身份必须匹配。
- [x] 新增 download_attachment(receipt_path, client, output_path)，复用 RAGFlow 原件/文档下载；CLI check/ingest/reconcile/download。
- [x] CLI 真实入库用当前 Prefect rag 全局额度，配置不符就停止，不改变额度。

## Task 4: 一份真实附件联验与交付

- [x] 等第三状态冻结真实单项，核对稳定 ID、源/MD/画像哈希与历史上传记录；只读确定专业资料库和 Files 目录。
- [ ] 新增文件独立审查并按名单合并，保留哈希清单；不覆盖其他模块、不重启现有服务。
- [ ] 仅该附件进行原件/MD/引用存储与远端读回；第二次调用证明无重复上传；下载字节 SHA256 等于原件。
- [ ] 留存真实回执和验收记录，区分原件、MD、检索、正文链接及下载状态；报告结果及边界。

## 当前证据

- 新基线候选：candidates/rag-attachment-md-20261029，默认 execution_enabled=false，RAG 指向本机禁用地址。
- 基线测试：227 tests / OK；Prefect home 在首次导入时有目录创建警告，后续显式使用候选本地目录。
- 原生复用审查完成：upload_file/download_file、RagPublisher、_Budget 可用；旧 AttachmentWorkflow 依赖另一套控制台账，不直接运行。
- 真实只读库存已保存 evidence/remote-storage-readonly.json；尚未做任何外部写入。
- Ruling: 已查到原生 associated_delivery.py 的 Files 创建、link-to-datasets 和原件下载能力；采用原生单专业原件链接及下载，不新建本机 HTTP 服务，避免重复实现认证与生命周期。用户仍可在原生库下载，CLI 可下载并校验到指定本地路径。
- Task 1: 10项接收测试先失败（模块不存在），实现后全部通过；包含原件/身份错配、目录计数、路径单次解码与 MD 更新不改变原件身份。
- 真实联验已完成原件/主MD/两专业引用及检索、下载：主源码 runtime/trials/attachment-h21-20261029/stage4/reports/completed-ingest.json、download-verified.json。只上传原件一次、MD三份、切片三片；所有16次写均返回，未知写0。
- 独立审查发现并补回归：旧版未决请求不能被新版绕过；重新核验需刷新检索且检查当前可用性。真实服务语义校正：文档启用后再核验切片available；retrieval分页沿用原RagPublisher.verify_retrieval的page_size/count规则。两次明确检查/查询失败完整保留，既有原件/主MD/切片未重发。
- 最终候选清单 evidence/owned-files-v3.json；等待主源码按清单精确同步后执行同一ingest的零写入复用验收。
