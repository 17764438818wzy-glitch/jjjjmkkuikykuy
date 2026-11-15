# 后处理代次与回执恢复隔离候选

本目录未部署，不包含自动扫描、自动恢复、模型请求或 RAG 上传入口；未修改线上运行文件、数据库、门禁和服务。依赖候选根目录已有的 `body_adapter.py`、`folder_processor.py`、`folder_state.py`、`handoff_inputs.py`、`prefect_slot.py`、`stage_gates.py` 和 `results_next/postprocessing_results.py`，也依赖原有 `prefect-paperflow` 中的业务脚本。部署集成应按实际加载路径合并，不能把此目录误当完整应用。

## 合并文件

1. `stage_runtime.py`：以现有 `runtime_next/stage_runtime.py` 为基础，增加阶段源代次、空表迁移、原 ID 的未提交预约恢复、持有回执的受控复核。
2. `live_runtime.py`：以现有 `runtime_next/live_runtime.py` 为基础，保留正文失败代码白名单及报告绑定，使用当前大工单代次并传递原 LLM 代次；提供显式的 `resume_stopped` 和 `build_runtime()['reconcile_held']`。
3. `batch_next.py`：与最新 `runtime_next/batch_next.py` 的唯一差异是等候快照选择当前大工单代次，并允许合法的较早上游 LLM 代次；`guard_prior_failures` 与预约/提交事务中的 `_gates()` 均保留。这个 guard 从独立只读连接观察已落库状态，并在写事务持锁期间再次调用，不能检测尚未落库的失败。
4. `generation_mainline_bridge.py`、`generation_attachment_adapter.py`、`generation_attachment_card.py`：分别派生自候选 `mainline_bridge.py`、`attachment_adapter.py` 和原 `paperflow_prefect/attachment_card.py`。首次附件生成仍调用原 `render_attachment_card`；复核时从原模板逻辑重算精确字节并只读现有文件，不重写 `current.json`。这三个文件需要和上述运行模块一起在 Python 导入路径中，不能只替换前两项。模板未来变化时需重新审查并运行新旧生成器一致性测试。
5. `control_next/llm_admission_state.py`：与其他四个 `control_next` 文件一起按该目录 README 的基线哈希检查和合并。控制汇总必须核验新增字段和同身份上游来源，缺失或不符显示未知。
6. 投影层需同步核验 `job.source_llm_generation == llm_folder_queue.generation` 及范围，不能再要求阶段代次等于 LLM 原代次；活动阶段仍需匹配当前大工单代次。

`generation_next/tests/` 和 `control_next/tests/` 为合成测试，不是业务运行文件。

## 表结构及迁移顺序

`paperflow_postprocess_stage_jobs.generation` 是本次阶段准入的大工单代次。新列 `source_llm_generation INTEGER` 冻结原 `llm_folder_queue.generation`，必须是正整数且不大于阶段代次；不改原 LLM 表、原回答、材料或请求历史。新建阶段表声明该列 `NOT NULL`；从旧空表迁移时 SQLite 的新增列为可空，候选读写明确拒绝空值或非法值。

人工重试候选还增加两个可空文本列：`source_retry_sha256` 固定原工单的重试记录及原始回答登记，`source_retry_prefect_sha256` 固定成功重试和历史暂停阶段已核验的 Prefect 完成状态。普通工单两列均为空；已有人工作业但缺少对应证明的旧记录不得被推断为通过。`initialize()` 仅在显式调用时添加缺失列，不回填旧记录；本次没有对生产库执行迁移。

1. 先保持后处理与 RAG 门禁关闭，禁止新的后处理预约；独立核对两个门禁及四个阶段表，备份控制库并确认无活动后处理。
2. 在同一写事务内检查阶段表结构和记录数；仅旧表缺列且 **0 行** 时执行 `ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN source_llm_generation INTEGER`。有旧记录则整次拒绝自动迁移，不推测、回填或覆盖其原代次。`Coordinator.initialize()` 实现了这个空表检查和事务迁移。该函数只设置阶段表，不开放任何门禁。
3. 合并上述全部互相依赖的文件，并按控制候选说明确保相关长驻进程加载一致版本；重启与否须依据实际部署进程和当前任务状态决定。
4. 回读新列、四张表、原 LLM 台账、两道门禁和服务状态，再按授权运行受控验证；不能把安装、哈希匹配或合成测试当成真实工单验证。

## 恢复能力及限制

`resume_stopped` 仅接受原 job 的 `stopped_before_submit`：恢复代次必须大于原阶段代次，冻结源代次和材料不变、无领取/结果记录，指定同一 Prefect API 明确报告该 TaskRun 不存在，并在任务准入锁和事务中重新检查所有条件，才复用原 job id 恢复预约；缺表、远端超时、远端已存在、提交未知、`cancel_fenced_submitting/submitted`、身份异常一律拒绝。没有后台自动重试；该 API 不能证明曾经的未知执行已经停止。

`build_runtime()['reconcile_held'](job_id, generation)` 是显式异步管理接口：复用 Prefect 的后处理全局并发名额和文件夹锁，核验原已完成 stage、已通过结果、持有领取、已存回执、全部制品及上游回答/代次，再在同一事务复核状态并只将领取从 `receipt_held` 推进为 `waiting_rag`。它不创建新工单、不重做封包、不请求模型、不上传 RAG；完成回执和原 LLM 记录不变。此管理复核使用 Prefect 并发名额，但尚未注册为新的 Prefect TaskRun，也未接入 UI 或后台自动恢复；调用前后均要求后处理门开放且 RAG 门保持关闭。验证过程中暂停、删除、来源变化、内容改变或证据不可用均保留原状态。

人工重试完成后，仍沿用原文件夹工单及原失败历史。候选入口核对重试命令、连续失败历史、暂停或未提交历史、所有正文与附件回答及 Prefect 阶段；预约、提交、执行和回执验收时复核冻结来源。实际回执结果还依赖 `folder_state.py`、`results_next/postprocessing_results.py`、`server_release_next/scope_verifier.py` 与 `postprocess-server-20261101/remote_coordinator.py`、`selected_ticket_probe.py` 的对应候选版本，不能只替换本目录。Prefect 与 SQLite 属于不同系统，外部状态观察和本地提交无法构成单一原子事务；后续 RAG 接入也必须再次核验完整来源和接收状态。页面核验是候选逻辑，未代表线上界面已更新。

## 验证

```powershell
& 'F:\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe' -m pytest -q tests results_next\tests generation_next\tests control_next\tests --tb=short
```

2026-11-01 人工重试候选组合测试为 428 passed、1 skipped、11 subtests passed；另一运行目录测试为 66 passed，页面来源候选测试为 221 passed。双方服务器候选联合回归为 214 passed；其后补充的重试对齐测试单独复跑 12 passed（与联合回归有重叠）。涵盖单段及多段正文、附件封包、连续重试、暂停恢复、来源变化、远程回执核验超时，以及原身份与不明结果保护。测试使用临时材料及隔离 Prefect，不代表已经验证真实论文、真实负载、实机暂停恢复或 RAG 接收。
