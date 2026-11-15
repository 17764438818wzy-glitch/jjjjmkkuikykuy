# 后处理暂停和删除控制候选

本目录是隔离候选：基于 `candidates/first-step-control-20261029/paperflow_prefect_v2/` 当前磁盘文件合并，尚未覆盖运行文件、重启服务或操作真实工单。`control_next` 只含修改的五个文件，其他相对导入仍从现有 v2 包及其复用的 `paperflow_prefect` 包取得；不是可直接替换整个目录的完整包。

## 磁盘基线

2026-11-01 当地时间核对到实际 14203 和第一步 Runner 的入口均属于 `candidates/first-step-control-20261029/`。下面是当前运行目录中的源文件 SHA256，以及本隔离候选的 SHA256；再次合并前必须重新核对源文件哈希，如有变化应重新审查差异。

| 文件 | 当前运行目录磁盘 SHA256 | 隔离候选 SHA256 |
| --- | --- | --- |
| `first_step_control.py` | `c0436deeb4e169bc57a3e2ef6ae09d74ef963601351f800c4fa0aec7fcdde0cb` | `1dc55476d1991a75eda3cd40a40c13e95f1621c55c1263bee27c657d20ae0fc6` |
| `first_step_delete.py` | `fd42a7931645cc3e2daf8e9aab567dd003ace1eb4e488cefb849496ff5b2ea01` | `b650b4755d8bf03bf7464653ec60f888a1435c6a703551988bd84a95682d07a0` |
| `first_step_scheduler.py` | `25228f7bf45946c637fc48d8a56807ded55356cd96663e69223c0e147eb5bceb` | `c031d9b62124aa26985da6041edb344369b234ff60168ad6f44e9e2080f2da3b` |
| `group_summary_store.py` | `5216000302d77ded49605482c742bf49d605171a159acc4944375df1ad5b7471` | `84740ba0131e75d85192abe524ffa6f2080032fdad136072ad1c67eb78fdf61a` |
| `llm_admission_state.py` | `57b1e43230f2b274d60f77c14c730c4c845734e9c5d9652bb1b062759f4d48f4` | `c6b2db69d1f8b99bba827c6a0e8a24890c32a9217f6f6160aa736cd6144b7bca` |

逐文件差异已经核对；哈希只证明当前磁盘字节，不能证明长驻 Python 进程当前内存里的模块版本。

## 最小差异

- `llm_admission_state.py`：保留原 LLM 准入统计，并按文件夹合并后处理阶段、领取和结果的状态，避免同一个执行阶段和领取重复计数。`finished` 须有对应成功结果、已接收阶段回执及匹配身份和回执哈希；状态、身份或表结构不可确认则显示未知。逐单 key 使用 `paperflow-folder-postprocess-stage-v1-` 加规范 job UUID 的 `.hex`，与 `generation_next/stage_runtime.py` 的 `task_key_for_job` 对齐。必须有 `source_llm_generation` 字段；每个阶段的原代次须与同身份 `llm_folder_queue` 记录一致，且为正整数、不能大于阶段代次。代次缺失、无效或上游身份不符时为未知。
- `first_step_control.py`：暂停时在已有 `preprocessing-{task_id}.lock` 和同一 SQLite 短事务内栅栏尚未执行的后处理预约；读取阶段事实失败时仍保存暂停意图，返回未知。已有开始新代次路径复用扩展后的统计，因此存在活动或未知后处理时阻止开始。
- `group_summary_store.py`：原有一致性读取事务内合并后处理状态；阶段表损坏或缺失时，暂停状态及相关计数显示未知，不能显示已暂停或零。
- `first_step_delete.py`：保留最新 LLM `stage_jobs` 栅栏、取消和回读；增加独立 `postprocess_jobs` 栅栏与传递。执行中的后处理不强行改成已停止；文件清理计划提交前以及删除最终任务记录前再次检查活动/未知状态。
- `first_step_scheduler.py`：在原 LLM 身份核验基础上，核对后处理与原文件夹关系、规范 UUID、UUID5 阶段编号、逐单 task key、dynamic key 和 tag，才取消或等待该 TaskRun；创建结果不明且远端查不到时拒绝视为已停止。原 LLM 检查及取消逻辑保留。

暂停和删除短期持有任务准入锁时不等待 `folder-order-*`；执行侧仍可按先文件夹锁、后任务短锁的顺序收尾。`reserved` 可安全栅栏为 `stopped_before_submit`；`submitting/submitted` 会变为 `cancel_fenced_*` 并显示未知，不能自动重领。当前没有针对这些未知提交状态的自动 Prefect 回读解栅栏流程，必须先核对真实 TaskRun，不能把它当作已停止。真实执行异常或未完成回执也不会自动清除。

实际启用前必须在同一控制库中先初始化 `paperflow_postprocess_stage_jobs`、`postprocessing_claims`、`postprocessing_receipts` 和 `postprocessing_results`，并确认队列门禁默认关闭。阶段表还须先通过 `generation_next` 的空表迁移增加 `source_llm_generation`；旧表非空不能自动推测并迁移。缺少这些表或字段时，本候选会拒绝把后处理当空闲；暂停意图仍保存。阶段业务结果表、回执和门禁的初始化不由此控制副本自动执行。

## 进程与加载

本次只读观察时：

- `Paperflow-FirstStep-API-14203` 的实际监听进程为 PID 49484，入口为 `managed_api_only.py → summary_api.py`。`summary_api.py` 在 v2 模式加载控制 API、`first_step_scheduler.py` 和汇总路由；控制 API/汇总进一步使用 `first_step_control.py`、`group_summary_store.py`、`llm_admission_state.py`。`first_step_delete.py` 由删除接口按需导入，当前是否已加载无法从入口代码确定。
- `Paperflow-Unified-FirstStep-Runner` 的实际 Python 进程为 PID 304，入口为 `run_unified_first_step.py → serve_controlled.py → order_v2_flow.py`。其入口直接导入 `group_summary_store.py`，工单执行时导入 `first_step_control.py` 及其调度、汇总和准入依赖。Runner 本身通常不调用删除入口；已存在的执行子进程各自的已加载模块未知。
- 第一、第二个计划任务均显示运行中。观察到 API 进程启动于 2026-10-31 23:12:52，Runner 进程启动于 17:45:49；Runner 早于上述部分源文件最后修改时间。只凭文件内容及进程启动时间，无法确认其内存加载的具体版本。

要让两个长期进程可靠读取新模块，并验证前台控制与任务执行使用一致版本，仅替换文件不够：应在确认可安全停止当前工作后分别重启 `Paperflow-FirstStep-API-14203` 与 `Paperflow-Unified-FirstStep-Runner`，核对有无已有工作子进程并分别回读。某个按需导入模块未来可能在未重启时读取新文件，但不能据此认定所有模块已同步。本次未进行服务操作。

## 合成验证

在候选根目录执行（仅临时数据库、合成身份和隔离 Prefect 测试服务）：

```powershell
& 'F:\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe' -m pytest -q tests results_next\tests generation_next\tests control_next\tests --tb=short
```

此前组合结果为 309 passed、11 subtests passed；2 条分别来自 Starlette/httpx 弃用与隔离 Prefect 日志上下文的提示。2026-11-01 修订原 LLM 代次后，控制专项 `python -m pytest -q control_next/tests/test_postprocess_control.py --tb=short` 为 31 passed；尚未重跑全部组合。测试覆盖合法和伪造逐单 key、非法/缺失/改变的原代次、提交未知、阶段及领取去重、完成回执损坏、暂停/删除期间锁顺序、删除前后复核以及新旧两种阶段任务的取消身份。测试未验证真实工单或生产服务。
