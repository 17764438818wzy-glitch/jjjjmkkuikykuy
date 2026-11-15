# 只读页面候选交接快照

本记录是 2026-11-01 的候选与观测快照，不启动服务，不改变 8795、14205、Prefect、后处理门或 RAG 门，也不是正式发布许可。全量现场切换需要用户指令和新的真实配置核验。

## 本次已验证

- 8795 当前 HTTP 200，实际仍引用 `prefect-ticket-counts.js?v=20261101-postprocessing-4` 和 `postprocessing-results.js?v=20261101-results-2`。14206 本机当时没有可连接的监听。
- 修复 SSH helper 的 UTF-8 中文输入后，固定服务器身份 probe 成功；仅指定三张小工单的真实聚合为 `35f35=verified`、`bf190=pending`、`12ef=pending`，已核验子集计数 1，等待 RAG 数为未知。两张 pending 在本次来源检查先暂停；未定位未读问题分别由独立回执检查与服务器窗口复核。其后只读来源诊断三张均通过，原因当时未留证，不能断定先前暂不可用的根因。详见 `ALIGNMENT-20261101.md` 和两个指定观察 JSON。
- 项目 Python 下候选测试 187 项通过，1 条 Starlette 依赖弃用提示；网页脚本测试 9 项通过。40 张模拟工单轮转测试覆盖所有工单，观察线程最多 2；这不是 8 个后处理执行并发的实际压测。
- 截至本次核对，`release-inventory-three-receipt-probe-20261101-06.json` 覆盖 256 个已列本机源码/配置文件且 `ReleaseGuard.verify()` 成功，SHA256 `b1235269a03b29c97b60c3f88c6e4b1a385573505b5cb0ff5d6c7511a307ed6e`；它是**三单观察的临时快照，不是全量发布清单**。服务器代码、解释器、第三方包文件和远端服务配置须另行核验。
- `site-inventory-candidate-20261101-v2.json` 的 33 个候选静态文件与本目录 `site` 当前匹配，SHA256 `59e4a24ad16e4c9444e3631f5e9086db412dc26320a3cd6cf82f98c795b5fcd6`；它是候选目录清单，不能代替未来实际发布目录的清单。

## 页面展示能力和限制

当前候选的计数单位是一张原始文件夹小工单；正文切片、图片和每份附件 MD 不另计。通过、未通过、未识别三个入口可以展开并分页。“通过”在逐单服务器证据、来源和 Prefect 身份核验后显示已核验子集，标识“至少”；未确认的登记结果标为待核验。没有可核验通过结果时显示 `—`，不冒充 0。

本候选目前没有足够的服务器证据稳定统计“未通过”“未识别”总数，显示 `—`；等待 RAG 总数也显示 `—`，没有把它伪装成 0。两张已知未读限制问题不会计入已核验通过，但当前列表只能保守显示待核验，不能保证每次都展示具体未读原因。30 秒有效期、频繁写库造成缓存失效和真实多单 IO 负载仍须现场验证。

## 14206 启动参数快照：只供复核，不执行

受控启动脚本是本目录 `start_verified_page.ps1`；其调用关系和停启顺序见 `CUTOVER.md`。下表是此次读取和核验的实际候选值：

| 参数 | 值 |
| --- | --- |
| `PythonExe` | `F:\论文预处理系统\预处理系统2\prefect-first-step-independent\.venv\Scripts\pythonw.exe` |
| `RunnerPath` | 本目录 `run_waiting_service.py` |
| `RunnerSha256` | `5472cceaf870f66e2a6580787fe8a12d3a1d6565e86585ee0ffb345c09385eae` |
| `CliSha256` | `16029ba07b4dcf2f425176295f1e5b6d28d00fec30f6ef4a2ae462a4db45b9a7` |
| `GuardSha256` | `9343b0d7367926fd4365a4672ee51617b60d4e77f30e7f2642e481dbb7e8d2be` |
| `LayoutSha256` | `76e10389f88c377746112fd63f7959e09be1f36179aab9ebdb57b001a002b26a` |
| `QueueDatabase` | `F:\论文预处理系统\预处理系统2\local-intake-runtime\waiting-pool.sqlite3` |
| `Log` | 建议使用独立的新后处理观察日志，启动会写日志，路径须切换时确定 |
| `Port` / `PartialObservation` | `14206` / 显式启用 |
| `ReleaseManifest` / `ReleaseManifestSha256` | 此次三单观察值为本目录 `release-inventory-three-receipt-probe-20261101-06.json` / 上述 `b123...ed6e`；**正式运行须在共享后端稳定后重新生成并复核** |
| `HistoryRegistry` / `HistoryRegistrySha256` | 本目录 `history-registry-candidate-bb7d5d9ae45b.json` / `7fc35333f9d59094d388c4d54b30aed3db8b2b2e96b79dd5bf7d7da76a93ccb2` |
| `HistoryAccess` / `HistoryAccessSha256` | 本目录 `history-access-candidate-ed0ef92b075f.json` / `e1f3444842540ab5fb8f48a6c45ec8fc503600f42ef066f680fe8bff1ea31599` |
| `HistoryUnreadScript` / `HistoryUnreadScriptSha256` | `/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v3/server_unread_scope_query.py` / `f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521` |
| `GuardBundle` / `GuardBundleSha256` | 本目录 `guard-bundle-candidate-20261101-v2.json` / `9b304d0f21f8650e8534b9de2c1067a778f81f82d473cfc09962e936f511db13` |
| `RemoteConfig` / `RemoteConfigSha256` | 此次指定范围观察使用 `F:\论文预处理系统\预处理系统2\prefect-paperflow\runtime\remote-postprocessing\selected-runtime-7f8749a7d178468e82cd877859bbbde2.json` / `63b03ebfc547244562b6b754451c0bbeac73c90adc6b984c5092ebb2092b86dc`；**这是选定范围配置，不能据此授权常驻全量观察**。全量配置、身份、有效范围及 SHA 尚需指定和复核。 |

外层受控计划任务需要先独立核对并执行 `start_verified_page.ps1` 的同一份源码字节；本次脚本 SHA256 为 `59011d24edf63a4388c4919193d0b802cafa195f35119fe8564098ef8cde72ed`。不要从待校验文件自行推导期望值；具体受控启动模板见 `CUTOVER.md`。现有 14205 与接收扫描器保持独立，14206 故障不能替代或中断前处理 API。

六个历史 app 中有一个访问引用目前不可用；新的服务器执行 app 不在这六版清单中。未来若启用新版本，必须先补充并核验对应配置、源码和只读访问，再承认其结果，未知版本不得套用旧版本通过结论。

## 8795 合并，而非覆盖

这次递归比对中，现网站点有 55 个文件，候选 `site` 有 33 个；同名文件只有 `index.html` 不同，候选独有两个服务器观察脚本，现网另有后端工具、测试和缓存文件。因此切换时必须从当时现网重新复制完整目录，只加入以下两项，并对当时的 `index.html` 做三方合并：

- `site/postprocessing-results-server-20261101.js`
- `site/prefect-ticket-counts-server-20261101.js`

当时的 index 差异是去掉旧 `prefect-ticket-counts.js` 和旧 `postprocessing-results.js` 引用，依次加入上述两个新脚本；保留前处理、LLM 手动重试和其他现网脚本。切换前重新比较现网变化，针对实际的新版本站点生成单独静态清单；确认 14206 的实际数据、预期的未知状态、恢复旧站点的方法和进程退出后再考虑重启 8795。此处没有执行上述切换。
