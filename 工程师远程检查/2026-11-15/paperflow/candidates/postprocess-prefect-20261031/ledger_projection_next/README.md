# 后处理登记状态只读投影（隔离候选）

此目录是候选，不会自行接入页面、启动任务、更改队列或上传 RAG。它提供“控制库中已登记的阶段及结果，截至某个时间”的快照，供页面适配；不是材料完整性检查，也不是实时工作状态检查。

```python
snapshot(database, result_filter=None, page=1, page_size=20,
         expected_revision=None, component_id_fn=None)
```

`database` 为现存 SQLite 控制库路径；以只读方式打开，在一个读事务中读取任务、删除记录、正文／附件 LLM 登记、后处理阶段、领取、结果和回执。打开前，调用方必须另行证明其选定的数据库是唯一权威来源，此投影无法区分真实控制库与格式相同的副本。每张表的必需列会先检查，即使该表为空，缺列也返回不可用。`result_filter` 可为 `passed`、`failed` 或 `unrecognized`；页码从 1 开始，每页不超过 100 项。请求下一页必须提交第一页的 `revision`；如果台账或投影分类、计数、顺序有变化，返回 `available=false` 和 `reason=snapshot_changed`，应重新从第一页开始。修订号摘要包括数据库行及完整投影结果，不包括页面、时间或文件完整性核验。

`component_id_fn` 必须由可信调用方注入现有 `paperflow_prefect.llm_queue.folder_completion.component_id` 函数。核验通过回执时，若有附件准入或回执附件，投影利用该上游函数比对附件编号、论文编号及来源 SHA 与所有已登记附件的 component ID，缺失、重复、错配或函数不可用均归未确认；没有附件时可以不提供。同时按现有附件交付约束检查必填字段、编号与哈希格式，并核对源文件和说明 MD 在 artifacts 中的路径／哈希引用；如有命名副本记录，还要求相应原件和证明的引用。此检查只能证明回执覆盖了已登记的附件准入，不能证明冻结清单中是否存在未登记附件。

外层以及 `stage`、`results` 均提供 `available`、`basis=postprocessing-registered-ledger/1`、`as_of`；`current_delivery_verified` 固定为 `false`。读取权限、数据库缺表、数据形状无法处理时，`available=false`，数量为 `None`。`results.list` 提供筛选、页码、每页数量、总数、是否有下一页和条目。`unrecognized` 的完整总数不可确认，显示为 `None`；可列出有证据的已登记条目，其 `registered_total` 仅是已登记条目数。

## 阶段口径

- `waiting_postprocessing`：已登记进入后处理等待，正文有原始回答登记，所有已登记正文／附件请求均为 `raw_complete`，且没有任何后处理阶段、领取或结果记录。材料和回答文件本身没有在这里读取。
- `postprocessing_registered_inflight` 和 `postprocessing_registered_executing`：通过身份／代次核对的预约／提交／执行数与登记执行数。后者包含于前者；状态组合异常时也可能与 `postprocessing_unresolved` 重叠，不能互相相加；它们不是实际存活任务数。身份／代次不一致或任务状态未知的记录只归未确认，不计入登记在途。
- `postprocessing_in_progress` 始终为 `None`，因为此投影不检查 Prefect、进程或跨机器活性。
- `waiting_rag_upload`、`postprocessing_held`：终态为完成、结果为通过，且领取及已保存回执身份一致、回执 JSON 自身 SHA256 一致的登记数量。`waiting_rag_upload` 仅表示已登记等待，不表示材料现在可上传或已经上传；`receipt_held` 另计。
- `postprocessing_unresolved`：存在未确定状态、身份／代次冲突或不完整记录的文件夹。无法关联领取的回执单独显示在 `unmapped_receipts`，不猜测所属文件夹数。存在上述问题时 `stage.partial=true`。

登记通过需要任务、小工单、阶段、领取、结果、回执的身份和代次关系一致，且阶段已经 `finished`；仅在执行中登记回执或结果还不能计为通过。正文回执需引用封包的 `manifest.json`、`READY.json` 和 `body-result.json`，且 manifest 引用 SHA 与正文登记一致；这只是回执内引用的检查，不能证明这些文件当前存在。已登记未通过必须有已持久化的证据路径和 SHA256；未识别登记同样要求证据引用。对回执中 Windows、UNC 与 POSIX 历史路径只做不依赖运行系统的绝对路径语法识别；证据和产物不在展示时重新读取或重新计算哈希。任务状态未知、删除过程中、启动尚未确认或身份不匹配时列入未确认；已暂停任务的历史记录仍可展示，但不表示允许开始工作。已删除的大工单不展示。

`results.counts` 为已登记的通过／未通过数和未知的未识别总数；`results.partial` 表示还有待处理、已登记在途或未确认记录，不能把当前结果当作全部工作已完成。`registered_unrecognized` 与 `results.list.registered_total` 仅统计已有明确登记的未识别项。

## 集成约束与限制

页面必须明确展示“已登记，截至某时”，并把未知数字显示为 `—`。此投影使用独立 `basis`，不能假冒原先逐文件核验的 `folder-stage-receipts/1`；现有页面若只接受旧 `basis`，需要显式适配新口径。读取快照不验证所有预期分段是否都已经登记、材料是否仍存在、文件哈希是否仍匹配、当前本地接收状态或远程 Prefect 状态。后处理开始和未来 RAG 上传前必须继续执行各自独立的完整身份、状态及文件校验。

表中任何一行变化都会导致 `revision` 变化并要求重新翻页，可能包括与本页无关的记录；当前仅对小规模现有控制库做过只读时延观察，未对大量记录和高频并发写入做性能压测。脱敏遵循原有 best-effort 政策，登记通过不证明没有缺口或百分之百脱敏。

仅使用合成临时数据库运行测试：

```powershell
& '.\prefect-paperflow\.venv\Scripts\python.exe' -X utf8 -B -m pytest -q 'candidates/postprocess-prefect-20261031/ledger_projection_next/tests' -p no:cacheprovider
```
