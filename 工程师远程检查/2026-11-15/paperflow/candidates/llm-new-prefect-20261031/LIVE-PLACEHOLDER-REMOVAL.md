# 运行环境：移除假的占位工单

2026-10-31 20:44，用户重申只移除假的占位工单，保留以文件夹为单位的真实执行工单。

已同步 `candidates/local-intake-live-link-20261031/prefect_orders.py` 和
`candidates/first-step-control-20261029/paperflow_prefect_v2/first_step_scheduler.py`：
后续不创建占位 TaskRun 或它的标签；注册、统计、取消改为核验真实文件夹 FlowRun、
任务队列、参数和本地绑定。被删除任务的迟到工单由取消墓碑继续核对。
原文件备份位于 `prefect-paperflow/runtime/installations/remove-placeholder-20261031-204404-e43f68f6`。

五个模拟场景和一个临时 Prefect 服务测试通过。同步后三个本地服务正常，当前无任务、
等待 LLM 为 0。此操作没有接入 LLM 执行，也没有执行模型或后处理。

注意：真实文件夹 FlowRun 已保留且正在使用；其他原生 TaskRun 隔离候选并未同步，
不能假设其与当前运行接线相同。后续接线必须核实本文件所述运行状态，
不得把真实文件夹工单当成假的占位工单删除。
