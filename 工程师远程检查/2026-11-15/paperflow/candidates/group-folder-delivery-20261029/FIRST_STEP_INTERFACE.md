# 第一步总任务汇总接口

本说明对应隔离版；尚未测试、安装或同步。

- 单次最多输入 30000 个资料组；内部每 500 组推进，前端不展示内部批次。
- 第一阶段的结果写入 `<第一步输出目录>/group-task-summaries/<task_id>.json`。
- Prefect 入口使用当前 `flow_run.id` 作为总任务 ID；独立命令行入口可传 `--task-id`（UUID），不传则自动生成，结果列表每条均包含同一个 `task_id`。
- 候选看板后端接口：`GET /api/v1/first-step/tasks/{task_id}?page=1&page_size=50`。
- `total_groups` 是总组数，`passed_groups` 是第一步通过数，`not_passed_groups` 是未通过数，包含 `failed_groups` 和 `review_groups`。`waiting_groups` 是尚未结束的数量，不计入失败。`issues` 是当前页未通过明细；每条列明资料组、失败或待核对状态以及文件和原因。
- 任务全部结束以 `status=complete` 且 `waiting_groups=0` 为准；题目待核对不会单独算作分类失败。
- `dashboard-overlay/prefect_setup.py` 是现有看板后端的隔离接线副本，需与本次第一步代码一起审查安装；不要覆盖运行中看板。默认读取配置中的 `classification_root`；独立命令行输出到其他目录时，可通过 `PAPERFLOW_FIRST_STEP_OUTPUT_ROOT` 指定该输出目录。
- 模型请求单路控制在本第一步 Python 进程内；当前已运行的 Ollama 服务需按后续操作确认切换，不能由脚本配置推断其已经生效。
