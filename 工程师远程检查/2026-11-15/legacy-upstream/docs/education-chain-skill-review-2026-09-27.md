# 教育链路审查：代码规范与设计实现

日期：2026-09-27。审查来自用户提供的 `C:/Users/Administrator/Downloads/skills-main.zip`，使用包内 `engineering/code-review` 的双轴审查和 `engineering/diagnosing-bugs` 的隔离复现方法。

## 范围与证据

- 比较基准：`548d3e781e0e7599fca900110d7fc93c50f980db`；HEAD：`c6d98766673add0de4e324432055933396500430`。
- 检查 `git diff 548d3e7...HEAD`，并纳入当前尚未提交的教育模块、失败修复、诊断读取器和通用工作池。
- 原技能偏向 issue 与提交审查；本库没有 `docs/agents/issue-tracker.md`、独立的编码规范文件或 AGENTS.md。本次使用用户已确认的本地设计文档作为需求与可靠性规则，没有要求额外安装或初始化工具。
- 线上只读核对确认两条教育 Pipeline 均为一个生成模型节点，两路仍使用 DeepSeek，同一契约与本次修复已部署，并发配置为 6＋2；教育附属文件任务中 `parse_required=1` 的数量为 0。
- 两个独立审查分别核对 Standards 和 Spec；主审另外核对只读运行快照、产物与隔离复现。以下按轴保留结果，不把样式偏好当成故障。
- 本次没有修改产品代码、线上配置、队列任务或运行进程。复现仅使用临时队列／模拟输入；运行数据只读。

## Standards

### S1 · P1：重复入队与过期查询保护互相冲突

[调度器](E:/uv/ragflow_upstream/ingest_scheduler.py:102) 对比 `updated_at`，有变化就丢弃查询结果；[QueueStore.enqueue](E:/uv/ragflow_upstream/ingest_queue.py:185) 即使任务内容完全没变，也会更新它。[worker](E:/uv/ragflow_upstream/ingest_worker.py:158) 先扫描批次入队，再收取查询结果。

没有旧批次接管标记的新教育批次，会重复扫描相同文件。这种情况下，即使远端 DONE，完成的查询也可每次被丢弃，本地持续停在 submitted/running，占用名额。

**隔离证据：**三轮 DONE 查询前分别重复提交相同任务，结果均停留 submitted；去掉重复入队操作，同样的查询成为 accepted。违反设计中“明确终态释放容量”的规则。修正方向是无实质变化的入队不改变执行修订号，而非去掉轮次保护。当前已接管旧批次有跳过已有任务的分支，不据此宣称它正在整体卡死。

### S2 · P2：旧终态可能确认新一轮不确定提交

[reconcile_write](E:/uv/ragflow_upstream/ingest_gateway.py:109) 只看文档和 Pipeline 一致、远端已是 RUNNING/DONE/FAIL/CANCEL，就确认提交；没有证明该执行来自本次提交。

**隔离证据：**模拟第二轮提交在到达服务端前超时，远端仍保留 9 月 1 日的 FAIL，核对函数却返回 confirmed。新尝试可能被旧结果结束。这是基准之前已有逻辑在新轮次机制下继续暴露的问题，违反“未知提交保留核查”的要求。Spec 轴另从诊断归属验证同一问题，不能重复计作独立缺陷。

### S3 · P1：查询没有公平轮转，后排任务和诊断补查会饥饿

[_schedule_polls](E:/uv/ragflow_upstream/ingest_scheduler.py:231) 每次从相同顺序取运行任务，取完以后才考虑待补查诊断。当前解析名额为 8，查询线程为 6。

**隔离证据：**8 个持续运行任务加 1 个待补查诊断，连续 20 轮只查询任务 1–6；任务 7、8 和诊断任务查询次数均为 0。已完成但尚未被查到的任务会延迟释放名额；待诊断可能一直积压到在途任务减少。

**线上证据：**01:24 快照有 9 篇 diagnostic_pending；其中两篇首次诊断缺失后已等待多分钟。主审单独只读查询其中一篇，已经能够取得明确剔除原因。应对状态查询、未知提交核查和诊断补查做公平排班，不能只增加解析并发。

### S4 · P1：远端后来变成 FAIL，本地仍计为 accepted

[post 异常处理](E:/uv/ragflow_upstream/ingest_scheduler.py:130) 统一保留 accepted，只标记 `post_status=needs_review`，并且只记录异常类型。

**线上证据：**任务 1、97、542、835 本地 accepted，远端逐篇重查为 FAIL；在全新临时产物目录回放 EducationArtifacts，四篇都报 `Education artifact source is not an admitted paper`。它们的当前 artifacts 没有发布成功。

**隔离证据：**远端 FAIL 导致后处理拒绝后，本地仍为 `{status:accepted, post_status:needs_review, error:null}`。这里须区分“远端仍 DONE、仅本地产物处理失败”和“远端已 FAIL”：前者可保留准入并独立补产物，后者应重新核对解析结论和具体失败节点。当前 accepted 汇总不能当作端到端完整成功数。本审查未推断远端由 DONE 变 FAIL 的服务端根因。

### S5 · P1（规模场景）：全库在途扫描有 10 万文档硬上限

[RagflowInFlightCounts](E:/uv/ragflow_upstream/ingest_remote_activity.py:19) 每次缓存到期都分页扫描整个目标库，每页 100 条、最多 1,000 页。它位于同步派发路径上。

**隔离证据：**模拟库中 100,000 个全部 DONE 的文档，发出 1,000 次查询后仍抛出 `Remote activity page limit exceeded`，不能进入本轮派发。未到上限时，查询开销也随整个库增长。这里的数量是远端文档数，不能等同于论文数。

需要可验证的服务端在途过滤／增量状态维护，并为外部提交保留核查入口；不应简单提高页数后继续周期性全库扫描。当前数百篇运行不受硬上限影响，但用户计划的十几万篇规模会触发。

## Spec

### Q1 · P2：artifacts 发布前缺少完整来源绑定

设计要求“对每个 artifact 检查稳定身份、原文哈希和来源 doc/chunk ID”。[EducationArtifacts](E:/uv/ragflow_upstream/ingest_education.py:142) 校验了本地文件哈希，却没有充分证明远端正文与本地原文及当前解析尝试对应，然后会写出 `latest.valid=true`。

**隔离证据：**使用不同远端正文与不匹配的远端来源哈希，仍发布有效指针，并生成一条来自不同正文的方法边。此处只是隔离输入证明的验证缺口，没有声称线上论文已普遍串文。

修正时不能直接比较上传字节 SHA 与 Markdown 解析后 SHA：已知解析器会去掉空行。应沿用经验证的非空行内容对应、解析执行身份和 chunk 来源绑定，再决定能否发布当前版本。

### Q2 · P2：旧 Pipeline 的学校调用漏计学校额度

设计要求“学校多个别名不能默认视为独立额度”。[外部在途统计](E:/uv/ragflow_upstream/ingest_remote_activity.py:43) 只通过当前两个 Pipeline ID 识别通道。

**隔离证据：**6 个未接管、使用同一学校模型的旧 Pipeline RUNNING 文档，被计为 `total=6, school=0`，随后仍可领取新的学校任务。它保护了总容量，却未保护学校容量。

影响条件是还有其他入口／未接管旧链路在并行调用同一供应商。已接管任务保存通道绑定，本审查没有证明当前现场已经超出学校额度。应按已核验的供应商实例或通道归属汇总，而非仅按当前 Pipeline ID。

### Q3 · P1：诊断还不能证明属于本轮执行

修复约定要求“旧轮次结果不得覆盖新轮次状态”。[执行诊断](E:/uv/ragflow_upstream/ingest_execution_diagnostics.py:41) 只比较日志时间和远端文档时间，随后直接附上本地当前尝试号，没有与本地持久化的本轮执行身份绑定。

**隔离证据：**9 月 26 日的旧日志被赋给 9 月 27 日的第二轮尝试，得到 quality_hold。已增加的 stale-poll 保护只能识别“查询途中本地任务变化”，不能识别“一开始拿到的远端状态就是旧执行”。这一项与 S2 同属执行轮次绑定问题，分别反映在核对与诊断两个位置。

## 产物质量观察与已知边界

- 对当时 340 篇 accepted 的本地快照检查，338 篇具有当前有效指针，另 2 篇指针尚未有效；后续快照的后处理待复核数量已增为 4，应按上面的实时运行记录看待，不能把这两个快照混算。
- 338 篇产物含 18,266 个 Tree 节点，其中 8 篇共 100 个节点没有正文 chunk 关联。抽查发现问卷题目、选项和教材条目进入了 Tree。比如“1.您所在学校”和“A.初级 B.中级 C.高级”被当作章节。当前[题目过滤](E:/uv/ragflow_upstream/education_direct_tree.py:14)只覆盖带固定“单选题／多选题”等后缀的标题，覆盖面不足。应保留这些材料正文，把论文目录和问卷条目分开处理。
- 这些产物含 994 条方法边、253 个未连方法实例，6 篇没有方法边。未连边可能是步骤名称未能可靠匹配方法；缺少证据时不造边符合设计，不能直接算解析失败。正式检索接入前应单独复核召回缺口。
- 最终四级 Wiki 导航、Agent 的正式方法索引调用、学校 9／12 档位上限测试及可靠计费，均有明确延期记录，本次不把它们报告为未告知的程序缺陷。

## 运行快照

2026-09-27 01:24:01（北京时间）：

| 状态 | 主论文数 |
| --- | ---: |
| 本地 accepted | 347 |
| 其中 artifacts done | 343 |
| 其中 artifacts needs_review | 4 |
| 结果待诊断 | 9 |
| 暂缓 | 5 |
| 剔除 | 5 |
| 运行中／已提交 | 6／2 |
| 待解析 | 168 |

worker PID 20508 仍在运行。本次没有暂停全量任务，也没有修改财会链路。

## 可重复证据

私有目录 `data/private/education_chain_skill_review/`：

- `standards/repro_current.py`：重复扫描与旧终态核对；主审已独立执行复现。
- `spec/repro.py`、`spec/repro_results.json`：轮次归属、来源绑定、旧通道容量；主审已独立执行。
- `repro_poll_starvation.py`：期望公平查询的断言失败，证实后排没有被查询。
- `repro_post_terminal.py`：期望远端 FAIL 不继续统计为完整成功的断言失败。
- `repro_inventory_limit.py`：10 万文档触发分页上限。
- `artifact_audit.json`、`post_failures.json`、`post_remote_state.json`、`final_snapshot.json`：只读现场核查。
- `skill_source.json`：用户压缩包 SHA-256、采用的技能和审查基准。

**双轴总结：Standards 5 项，最高 P1，涉及状态查询和持续派发；Spec 3 项，最高 P1，涉及本轮执行归属。S2/Q3 是同一根本问题，合计 7 类实质问题；另有 Tree 目录质量观察。**
