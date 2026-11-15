# 教育解析：失败原因与标注校验修复

## 修复前核查

2026-09-27 核查通用工作池中的 542 篇主文档：310 篇已入库，232 篇远端显示 FAIL。按文档 ID、Pipeline、解析开始时间、上传文件哈希与解析后正文逐一绑定执行记录后，232 篇分为：

| 实际结果 | 数量 |
| --- | ---: |
| 完成标注后判暂缓 | 188 |
| 完成标注后判剔除 | 2 |
| 标注字段或引文校验失败 | 38 |
| 预处理专业为思想政治教育，被候选专业清单拦截 | 3 |
| 长文 ArtifactBody 输出列表截断，返回类型错误 | 1 |

原先工作池只读文档 metadata 和 progress_msg；程序节点通过异常返回的裁决没有写入 metadata，progress_msg 又可能停在排队文案，因此大量裁决被混成故障。

## 已落实的修改

1. 从 RAGFlow 持久化 ingestion 执行记录读取失败节点与最终异常行。校验文档、Pipeline、轮次对应的开始时间、文件内容和正文后才采信；保存简短原因与执行 ID，不保存完整错误堆栈到工作台状态。
2. 分开展示暂缓、剔除、技术失败；无法读取诊断时释放解析名额，只做有限次数的只读补查。旧轮次结果不得覆盖新轮次状态。
3. 修复正文短章节切片只保留前两行而丢失真实研究对象描述的问题；保留完整内容及正文／辅助材料角色边界。
4. 合并同名研究方法、规范已知同义名，并保留每段不同的实施说明。
5. Profile、研究步骤和完整引文保留在专门的索引数据中，正文每个 chunk 不再重复这些大字段，降低长文输出膨胀。
6. 提示词按文献／理论、教学设计、经验案例等实际类型判断完成的工作；不统一强制问卷或实验数据。思想政治教育进入候选范围，仍须证明实际教育研究属性。
7. 全文哈希、coverage_token、四个核心证据字段及原文引文定位继续校验。

教育学校与官方两条 Pipeline 同步部署；模型绑定保持为两路 DeepSeek，并发仍为学校 6、官方 2。财会 Pipeline 修改前后配置哈希一致。

## 验证

- 离线回放原执行输出：38 篇标注失败中，26 篇通过入库校验、4 篇恢复出质量裁决、8 篇仍缺少合格引文。1 篇长文输出失败通过；原先成功的 40 篇仍通过。
- 本次针对性测试：61 项通过，另有 3 个子测试通过，含旧诊断覆盖新轮次的回归测试。
- 全部根目录测试：1374 项通过、11 个子测试通过；1 项 Tk 初始化环境错误，单独重跑通过。此前同一轮修改的全套测试为 1372 项通过。
- 第一轮线上 8 篇：5 篇入库、3 篇暂缓，0 技术失败；约 125 秒完成。5 篇入库文档的 Profile、研究步骤、Tree、Graph 与来源关联均已检查。
- 扩展至累计 20 篇：12 篇入库、5 篇暂缓、3 篇剔除，0 技术失败，0 原因不明。新增的 12 篇约 219 秒完成。全部 12 篇入库 artifacts、其余 8 篇的执行诊断和轮次均已核验。

## 查看失败原因

原一轮 232 篇的完整原因在本机私有目录 `data/private/education_failure_repair/failure_reasons.md`。动态状态由通用工作池保存；已打开的旧工作台窗口需要重开后才能加载新的原因显示代码。RAGFlow 原生 FAIL 仍是节点运行状态，工作池显示经核验的实际裁决。

本次优先重跑受影响的失败／暂缓论文；原已成功的 310 篇未整体重新解析。新模型输出仍可能出现字段或证据问题，不能把一次小样本通过解释为后续必定零失败。

执行日志接口和节点输出结构的核对依据包括本机实际响应，以及 [RAGFlow 日志服务](https://github.com/infiniflow/ragflow/blob/main/api/db/services/pipeline_operation_log_service.py) 与 [Pipeline 执行器](https://github.com/infiniflow/ragflow/blob/main/rag/flow/pipeline.py)。

## 全量补跑恢复

2026-09-27 01:12（北京时间），剩余 210 篇已按原文档 ID 各排入一次补跑：25 篇旧技术失败、185 篇旧暂缓。原已确认的 2 篇剔除及本次 20 篇试跑结果保持当前裁决。后台工作进程 PID 20508，学校 DeepSeek 6 个名额、官方 DeepSeek 2 个名额，继续接收已接管预处理批次的新完成论文。本次没有创建 Codex 定时自动化。

截至恢复时，主文档已入库总数从 310 增至 322；余下补跑属于运行中的工作，后续不能以试跑的零技术失败推断全量结果。

私有核验记录：

- `data/private/education_failure_repair/bound_diagnostics.json`：原 232 篇诊断与来源绑定。
- `data/private/education_failure_repair/offline_replay.json`：272 个原执行结果回放。
- `data/private/education_failure_repair/trial_eight.json`、`trial_twenty.json`：线上试跑。
- `data/private/education_failure_repair/verification_twenty.json`：20 篇结果及 artifacts 核验。
- `data/private/education_failure_repair/resumed.json`：210 篇补跑清单。
- `data/private/education_failure_repair/worker_restart.json`：新进程与日志位置。
