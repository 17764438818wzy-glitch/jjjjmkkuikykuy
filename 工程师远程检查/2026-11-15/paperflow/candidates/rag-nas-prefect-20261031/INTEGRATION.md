# 本批隔离 RAG / NAS 接线交接

## 范围

`batch-309.json` 是本次大工单的配置，不含凭据。核心接线从参数读取任务和隔离目录；只能以这份配置运行本次已授权的批次。每篇论文组对应一条下游记录，正文、全部要求的专业、正文图片和附件均完成并核验后才算一组 RAG 完成。NAS 随后归档并读回，只删除本次创建的测试副本。

本候选尚未加载到主线，RAG 屏障尚未由本窗口打开。上游窗口负责将其接入现有 `serve`、更新页面投影、统一切换及恢复大工单原状态。没有第二个常驻调度进程。新建的 RAG/NAS 是单独的 Prefect TaskRun，`flow_run_id=None`，通过原七字段及后处理身份关联文件夹；它们不是原文件夹 FlowRun 的原生子 TaskRun。

## 现有服务的调用接口

在现有服务启动流程中将本候选目录加入 Python 搜索路径，读取 `batch-309.json`，调用：

```python
from rag_nas_serve import build

bridge = build(runtime, access, log, batch_config=batch_config)
```

`runtime` 和 `access` 使用现有后处理服务对象；`log(event, **fields)` 使用现有日志函数。`build` 校验两者的控制库相同，调用 `group_stage.initialize` 创建 `paperflow_rag_nas_jobs`、`paperflow_rag_nas_intervals` 两张表，并检查 Prefect `paperflow-connected-rag=20`、`paperflow-connected-nas=2`；名额不存在才创建，已存在但不匹配则停止。不要在未切换的在线进程中试调用 `build`，因为它会初始化这些控制记录。

在现有 `serve` 的每轮循环中 `await bridge.tick()`，并将 `bridge.pending()` 与原服务的活动任务一起交给 `asyncio.wait(..., return_when=asyncio.FIRST_COMPLETED)`。这些都是当前进程的协程，不要另起常驻调度器。页面观察器应在同一轮切换中使用支持 `stage=rag, state=needs_review` 的候选：它表示原后处理回执存在未读范围披露问题，不表示 RAG 已写入。

预约只选择后处理 claim 为 `waiting_rag`、stage 为 `finished`、result 为 `passed` 的组；事务内核对原七字段和 claim/job/receipt/manifest。原服务器回执和封包在实际 RAG TaskRun 中再次按该身份核对。未读披露失败仅将该组记录为 `needs_review`。创建测试库、准备副本和实际 RAG 工作都处于该组 RAG 运行状态，纳入既有删除等待；总工单暂停时不接纳新的外部效果。

不同组可并行准备；同一进程仅复用正在创建的相同专业测试库结果，不设置整批串行门。创建结果未知时保留原 intent，不自动重发。原生执行器暂时返回未就绪、请求尚未发送时，在同一个 TaskRun 中释放名额并等待，不重新上传已有文档。

## 须保持可读的源码和数据

实际源码依赖为：`rag_nas_serve.py`、`group_stage.py`、`group_prefect.py`、`task_effect_admission.py`、`effect_journal.py`、`delivery_inventory.py`、`remote_artifact_fetch.py`、`bound_handoff.py`、`batch_test_routes.py`、`isolated_targets.py`、`isolated_files_folder.py`、`isolated_copy.py`、`isolated_group_prepare.py`、`isolated_group_operation.py`、`isolated_group_rag.py`、`isolated_attachment_routes.py`、`folder_native_client.py`、`folder_native_body.py`、`folder_native_attachment.py`、`group_rag_evidence.py`、`isolated_archive.py`、`isolated_cleanup.py`。`batch_metrics.py` 是只读统计工具，不参与调度。

`batch-309.json` 的 `batch_root` 必须继续指向现有 `live-tests/batch-309-fc240b92da874cca9a05f9630aeb073e`，保留该目录下已有的 `rag-dataset-effects.sqlite3`、`group-effects.sqlite3`、测试库回执、`rag-files-folder.json` 和两个 `prefect-single-*` 全部回执。`test_root` 指向桌面 `PaperFlow-SELFTEST-309-.../prefect-groups`；两组的 `test-copy-receipt.json` 仍在该目录，测试文件本身已删除。旧回执含绝对路径，不应在切换时复制到新位置后改指空日志。

现有 `prefect-paperflow/config.json`、固定来源清单、已安装的 RAG 凭据、只读 SFTP 密钥、原生运行观测、Windows NAS 凭据和原目录均继续使用。代码复用现有业务原语，不启动旧调度器、旧接口或旧前端。

两份隔离 Prefect 完成回执各自的原七字段及后处理 job/claim/receipt SHA 均已与当前记录、实际服务器只读回执核对，所有所需回执路径存在。接续时两个新 TaskRun 只读取并验证这些历史回执，不重传、不重新归档、不再删除。它们的接续 TaskRun 耗时不代表原上传耗时。较早的人工样本 `1178d1d6c682dccde7aef1cf46b1e40e` 未满足该新回执合同；配置将其指向独立新测试库和新测试副本，不把旧记录认作完成，也不向旧测试文档重传。

## 实际验证和计量

- 带附件组 `723f1c8cac67472ff77fa7f27e8caa6d`：三个专业各 113 条正文记录，附件原件一次及三个专业的附件 Markdown，完成隔离 RAG、NAS 读回和两份测试副本删除。附件回执的脱敏状态为 `not_performed`。首次执行因分词器线程归属错误中断，核对已返回效果后修复并续作，正文和附件原件未重传。
- 带图组 `4ede358f59b08e033e7983f11aca1ff7`：三个专业各 103 条正文记录、两张图片；六次图片下载的 SHA-256 均与封包相符，各专业检索通过。NAS 读回通过，删除一份测试副本；原件和原交付副本仍存在。正文走原生批处理，图片走原程序的图片切片接口和下载回读。
- `fe235667cc2450d0fc589004b85bbd8e` 的原回执读取已实测抛出 `UnreadReviewRequired`，没有开始该组上传或清理；应待核对。
- 临时 Prefect 的合成执行器测试实际启动 20 个文件夹 TaskRun，组阶段区间峰值为 20；暂停后 20 组均停住，恢复后同一批 TaskRun 均推进到等待 NAS。此测试没有发送真实论文或写 NAS。
- 两次样本都是临时隔离 Prefect 测试，非主线批量结果。已有记录的原生 HTTP 请求峰值为 1；不能由 20/2 配置或合成测试推断真实批量峰值。

批量启动后，每个组的原生请求时间和文档运行抽样写入 `batch_root/prefect-groups-work/<group_id>/native-observations.json`。运行 `python batch_metrics.py <batch_root/prefect-groups-work>` 可汇总原生 ingest HTTP 请求的时间交叠峰值；文档运行峰值是抽样值。`group_prefect.observed_concurrency` 从控制库的阶段区间计算组数峰值，须将历史回执复用的两组与实际上传区分。统计输出缺失时不把缺失当成零。
