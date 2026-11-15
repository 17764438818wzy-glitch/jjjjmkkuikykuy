# 独立第一步统计接口

接口地址：`http://127.0.0.1:4203/api/v1/first-step/tasks/{task_id}?filter=passed&page=1&page_size=50`。接口已允许 `http://127.0.0.1:8795` 的页面读取。它仅查询已有记录，不读取原论文、不调用 Nimble，也不触发登记。

- `task_id=current` 表示最近开始并已进入第一步程序的任务，返回值中的 `task_id` 是实际 UUID；没有任务时返回 404。
- `filter` 支持 `passed`、`failed`、`unrecognized`；省略时显示全部已处理组。`page` 从 1 开始，`page_size` 范围为 1 至 200。
- 响应包括 `task_status`、`total_groups`、`processed_groups`、`passed_groups`、`failed_groups`、`unrecognized_groups`、`filter`、`page`、`page_size`、`list_total`、`items`。一组仅属于一种处理结果，`processed_groups` 是三类计数之和。排队和处理中不算已处理；重复输入同一路径只计一次。
- 分类和题目是不同状态：执行错误、读取/模型超时或交付错误属于 `failed`；程序正常处理但正文或文件归属仍无法确认属于 `unrecognized`；已完成分类及交付属于 `passed`。失败与未识别的 `reason` 包含已有具体原因。
- 每项返回 `group_id`、`display_hash`、`status`、`title_status`、`paper_title`、`paper_title_candidate`、`reason`，以及可用时的 `group`、`delivery_dir`、`delivery_complete` 等字段。正文存在且哈希有效时 `display_hash` 为正文 SHA-256，否则使用 `group_id`。无法建立资料组标识的无效输入，两者为 null。
- 题目 `已提取` 显示 `paper_title`；`待核对` 显示 `display_hash` 并在下方显示 `paper_title_candidate`；`未提取` 显示 `display_hash` 并在下方显示“题目未识别”。题目状态不改变分组计数。
- 每组结束后增量写入本机统计索引，计数与列表从同一数据库快照读取；`list_total` 与选中的分类数字相同。旧 JSON 台账仍按原规则写入，用于保持现有流程兼容。
- `task_status=complete` 和 Prefect 的 Completed 表示流程结束，不代表每个组都成功。开始统计前参数错误的任务会记录为 `failed`，未处理组不算进三个结果数。
- 本实例无升级前的任务汇总；该接口索引本次接入后产生的任务，不会自动将其他目录的旧 JSON 记录导入。新旧 Nimble 服务共用本机算力。

交接目录格式见 `INTERFACE.md`。下一步登记尚未接入。
