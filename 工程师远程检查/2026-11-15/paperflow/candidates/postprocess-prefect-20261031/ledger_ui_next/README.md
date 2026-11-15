# 后处理登记台账与页面接线（隔离候选）

本目录仅是隔离候选；未覆盖 8795 页面或 14205 接口，没有打开后处理、RAG 门禁，没有启动真实后处理。`index.html` 和两个 JS 文件复制自现有页面后，只修改后处理登记口径、结果翻页和脚本缓存版本；其他页面脚本和样式继续沿用现网原件。`waiting_api.py` 复制自 `api_fix_next/` 后增加可选的只读投影入口；不提供投影时保留原来的接口行为。

## 展示口径

候选 API 使用单独的 `postprocessing_registered_ledger`，其 `basis` 是 `postprocessing-registered-ledger/1`；不能把它伪装成 `folder-stage-receipts/1`。页面提示“已登记、截至某时、交付材料需复核”。等待后处理和等待 RAG 的数字只反映台账当时的登记，存在未确认记录时显示至少；真正处理和未来上传前仍需读取并重新核验实际交接材料、原始回答、回执和产物。已登记通过沿用原流程的 `learning / best_effort`，并不证明没有识别缺口或百分百脱敏。未识别总数未知显示 `—`；仅凭台账无法判断服务器进程是否实际存活，后处理中数量同样为 `—`。

点开通过、未通过和未识别结果仍使用现有页面交互。第二页起必须发送第一页的 `postprocessing_result_revision`；若台账或投影分类发生变化，接口返回 `snapshot_changed`，页面回到第一页。活动较频繁时需要重新翻页；摘要用于快照一致性，不是文件内容或实时进程证明。结果为用户安全文字时沿用 `textContent`，不把内容当 HTML 执行。

## 与迁移服务的接入条件

- `create_app(..., registered_projection=callback)` 可选择注入投影。回调接收 `result_filter`、`page`、`page_size`、`expected_revision`；正常实现是绑定 `ledger_projection_next/ledger_projection.py` 的 `snapshot`，并注入原有 `paperflow_prefect.llm_queue.folder_completion.component_id`。不得复制/猜测算法。调用方必须先证明所选控制库是唯一权威来源，投影函数无法识别旧副本；换库或换服务时必须丢弃旧缓存并重新创建服务。后台缓存最多并行读取 2 个投影，结果按观察时间标记；不是实时工作执行名额。
- 如同一权威状态服务能提供现有 `FolderOrderService._add_result_titles`，可通过 `registered_title_enricher` 显式注入，以原有身份匹配规则补论文题目；接口只接受不改变任务、文件夹、论文和回执身份的题目结果。不可核对时显示“题目待核对”。失败原因可通过 `registered_failure_messages` 注入 `results_next/postprocessing_results.py` 中的 `FAILURES` 白名单；未配置时保留通用登记原因，不读取或暴露私有报告。题目是独立的显示观察，分页修订号约束台账中的身份、分类与顺序。
- 接口不自动寻找数据库、不启动任务、不调用模型、不上传 RAG，也不判定服务端活性。旧结果字段保持不可用，以免旧页面把登记时间误标为完整核验。页面、API 和投影三者须配套；页面和 API 目前仍使用原本的本机地址，服务器架构定案后需由迁移方案提供经核实的服务路由与权限，不应直接把服务器路径或本机副本当权威来源。
- 现有控制库不能仅凭已登记 LLM 准入记录证明预期附件／分段均已登记。接入端须按真实处理准入复用冻结 manifest、接收回执与来源链；台账展示只校验可见登记及回执元数据。即便历史记录可显示登记通过，也不能据此证明当前文件仍存在、哈希仍匹配或可交付。
- 现有候选只知道 `owner_pid` 与进程启动时间，跨机器 PID 不可互认。要显示真正的后处理中数量，必须由服务器可信执行端提供带主机／实例身份和时效的活性证据；缺少时保持 `—`，不能由“登记执行”替代。RAG 门禁只约束显式检查它的未来上传入口。

## 运行候选测试

```powershell
& '.\prefect-paperflow\.venv\Scripts\python.exe' -X utf8 -B -m pytest -q 'candidates/postprocess-prefect-20261031/ledger_ui_next/tests' -p no:cacheprovider
node --test 'candidates/postprocess-prefect-20261031/ledger_ui_next/tests/postprocessing-counts.test.cjs' 'candidates/postprocess-prefect-20261031/ledger_ui_next/tests/postprocessing-results.test.cjs'
```

测试使用临时 SQLite、合成台账和替身 HTTP 客户端，不访问真实论文、模型或 RAG。页面原型尚未用服务器数据及真实浏览器端到端核验。
