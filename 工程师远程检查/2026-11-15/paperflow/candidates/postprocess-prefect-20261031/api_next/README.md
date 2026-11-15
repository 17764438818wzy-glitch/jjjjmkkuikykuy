# 后处理统计 API 隔离候选

此目录只提供被动读取和展示适配。`waiting_api.py` 基于运行代码增量修改；此目录的 `waiting_cli.py` 是原样快照，具体注入候选另见 `../projection_next/waiting_cli.py`。默认不创建工单或启动后处理，现网 API 未由本目录修改。

## 线上快照哈希

| 文件 | SHA256 |
| --- | --- |
| `local-intake-live-link-20261031/waiting_api.py` | `2D2C5E05F251BD78BBE69EFA38BF2312961C01B5795DAE52CD657C7EFE6795DB` |
| `local-intake-live-link-20261031/waiting_cli.py` | `C6E5DC5E13BCA1B15A5606F099982FFB164CB74CC26D8BECDF6ED95D3F7433C7` |

快照存于 `baseline/`。安装前必须再次核对运行版本，并做最小增量合并。

## 投影契约

`create_app(..., stage_projection=None, results_projection=None)` 两项均可不提供。不提供或来源不可确认时，三个阶段的数字和后处理结果均未知；不会使用 LLM 的累计完成数冒充等待后处理数字。

`stage_projection(snapshot)` 是只读函数，需自行核验阶段数据及所有相关来源在读取期间的一致性；已核验时返回 `available=True`、`basis='folder-stage-receipts/1'`、带时区的完整核验时间 `as_of`，及 `counts` 中的 `waiting_postprocessing`、`postprocessing_in_progress`、`waiting_rag_upload`，值只能是非负整数或 `None`。可选 `postprocessing_held`、`postprocessing_unresolved`。缺来源、时间、快照不一致或无法验证必须返回不可用。此 API 外层额外检查接收器、来源、上游扫描和调用前后快照身份；外层身份检查**不能代替**投影自身对数据库并发变化的核验。不能用接收器时间代替完整核验时间。

`results_projection(result_filter, page, page_size)` 是只读函数，可对接 `ResultsStore.summary`；结果应符合前端约定，包括计数、`partial`、时间，以及筛选请求对应的 `list.filter/page/page_size/total/items`。未识别总数可为 `None`，`has_next=True` 时可继续查看已确认结果。接口只把符合当前请求参数的结果交给前端，不生成题目或虚构诊断。

两个投影的完整验证在后台执行，避免摘要请求等待文件全量哈希。结果缓存最多 32 个键、最多两个并发完整读取；阶段缓存最多一个。每个键按来源身份、分类和分页隔离。未提供缓存见证的投影仍采用 4 秒刷新、8 秒到期；真实投影提供见证后，每次取值须复核本地业务快照、交付及产物文件属性、执行进程，另在后台复核 Prefect。后者超过 8 秒未核对或核对失败即返回未知，校验从远端请求开始时计时。完整核验在 15 秒后允许后台刷新，45 秒到期；显示的 `as_of` 始终来自完整核验，不随轻量复核重置。

属性检查用于发现变化，不等于重新计算大文件 SHA256；外部状态在两次读取之间变化仍存在观察窗口。任一业务快照或已跟踪文件变化、PID 丢失、后台 Prefect 身份或状态变化都会失效；读取错误时未知。同步轻量检查也有成本，真实 268 组实测单次本地检查约 0.75 秒；不得将“完整验证在后台”解读为接口完全不做磁盘读取。

## 核验

新功能和缓存反例使用合成数据测试，包括业务变化、进程丢失、Prefect 变化/超时、属性未变但完整核验过期、分页隔离与时间来源。2026-11-01 的进程内只读联测接入真实投影后，在摘要外壳为合成数据的条件下，跨过旧 8 秒时限仍能取得后处理和结果；不代表线上页面已加载新代码。另见 `../projection_next/README.md`。

另运行现有 `test_waiting_api.py`：2 项通过，另 2 项在 fixture 初始化时因旧测试导入已不存在的 `_MODEL_GATE` 报错；对未修改线上基线重跑得到相同结果。这两项未能完成回归，不能声称通过。
