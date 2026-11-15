# Prefect 五步接入 Implementation Plan

> For agentic workers: execute inline with independent read-only review. The user explicitly authorized connection and explicitly prohibited real-paper execution in this turn. Do not ask for a second design approval.

**Goal:** 将已确认的五步业务连接至 Prefect 原生调度，在看板可见，真实执行保持关闭。

**Architecture:** 远端现有 Prefect 3.9.0 管理任务；Windows 独立 runner 访问 F 盘和已有解析依赖。直接调用固定 N59 llm140-r01 的业务函数，不实例化旧 Manager、Scheduler、AutoProcessor、清理或归档后台循环。原流程与数据目录不修改。

**Tech Stack:** Python 3.14、Prefect 3.9.0、现有正文提取/画像/封包/RAGFlow/ArchiveWriter；独立 JSON 阶段记录。

**Spec:** 本聊天用户确认的三路成果接收→本机正文准备与独立附件简单识别→正文 MD 画像→两路 RAG 写入核验→NAS 原件复制核验及处理记录；本地个人文件夹保留。正文至少100个LLM请求名额，附件另算；本轮只接入、不运行真实论文。

## Global Constraints

- 新代码和运行记录仅在 `预处理系统2/prefect-paperflow`；现有业务源码只读复用并固定来源哈希。
- 输入默认 `F:/总论文_整理后_20261028`，接收每人正文/附件目录；垃圾材料只记排除，不读取内容或送后续。
- 正文100、附件20、总LLM120，准备4、封包2、RAG4、NAS2；参数是容量配置，不能称真实性能通过。
- 真实执行本地开关为false，注册不触发业务。没有自动扫描计划、没有真实模型/RAG/NAS调用。
- 参数/日志不携带凭据；Windows DPAPI配置只在获准真实执行时按原位置读取。
- 一篇论文的成功条件为两路RAG核验+NAS完整核验+记录；附件失败不阻断正文成果持久保存。
- 超时未知写入保留身份和回执，不自动重复发送；已有封包保留原task/manifest身份。

## Review Focus

- 重复正文、不同个人同内容、不同文件同名、正文修改后的修订身份不能混淆。
- 阶段恢复不能因为封包/上传/NAS失败而重新请求模型；不完整或篡改的证据不能当完成。
- 附件说明不进入正文MD；原始ZIP保留原包，附件证据读取有界且不执行代码。
- 线程等待不能让100路正文受后续依赖占住线程；名额只覆盖对应资源阶段。
- 默认检查模式、未启动真实任务、NAS失败和缺附件必须与完成严格区分；本地不删。

## Tasks

1. 输入与记录：`settings.py`, `records.py`, `intake.py`，稳定paper_id和revision，精确文件清单，独立状态目录；先测排除、同hash正文去重、多正文冲突、留存和阶段缓存证据。
2. 业务适配：`legacy.py`, `body.py`, `attachments.py`, `delivery.py`, `archive.py`。复用正文模块、request_once、RagPublisher和RegisteredWriter；测试未知响应不重发、封包身份复用、附件精确回读、NAS清单和本地留存。只用生成夹具与内存假服务。
3. Prefect接线：`flows.py`, `manage.py`, 独立虚拟环境及启停脚本。异步每篇流程，正文附件并行，独立资源额度，批次汇总失败后仍继续其他篇；真实开关false检查在任何原稿/凭据/业务访问前。
4. 验证和登记：本地完整相关测试；生成夹具实际解析+本地NAS复制核验（临时目录）；必要的Prefect接线检查任务仅创建检查记录。登记手动deployment、无计划，保存部署回执，独立审查，不启动真实论文。

## Decisions / Progress

- 当前目录不是Git仓库，新独立子目录作为隔离边界；不改另一个整理任务的文件、不创建工作树。
- 旧run_single累计阶段有附带行为，直接调用下层函数。
- 已有NAS交付上层限定512MiB；如需适应真实完整清单，复用其流式copy/hash/manifest内核，不沿用与本流程无关的小批上限，不引入删除路径。
- 已完成独立适配、资源接线和手动部署；真实执行始终关闭。
- 已登记 deployment `3c968368-58fc-40a0-9966-0ba2fde8c92c`，默认check、无schedule；接线检查run `eb01aad5-e25d-45ca-875a-250a7691c917` 的六个检查任务Completed，deployment READY。
- 合成DOCX已实际解析、模板校验、离线精确分词封包；两个夹具模型回答，本地归档逐文件读回与原件保留通过。无真实论文/LLM/RAG/NAS业务调用。
- 独立复核修复：旧回答材料/模板绑定、附件缓存源哈希、父正文身份核验、附件截断/图像证据标识、同篇操作互斥、本机工作额度、取消时底层线程完成前不释放占用。
- 取消回归曾复现asyncio内部等待器取消导致提前释放；已改用concurrent future真实完成状态，外层/内部同时取消的夹具通过。一次夹具补丁最初未覆盖Python内部wrap_future入口，修正夹具后复现失败，再完成实现修复。
- 真实100+20并发、真实模型质量、RAG/NAS整链仍待用户后续授权试跑；不将本轮接线检查或夹具作为生产验收。
