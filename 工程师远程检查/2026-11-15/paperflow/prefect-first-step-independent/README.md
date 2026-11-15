# 独立的第一步 Prefect

本实例只注册第一步本地预处理，没有自动计划。原始资料、交付结果与 Prefect 数据分别位于 `config.json` 中的 `library_root`、`classification_root` 和本目录 `runtime`。本实例每次最多 4 组资料并行，使用 `127.0.0.1:11435` 上独立的本机 Nimble 服务（`OLLAMA_NUM_PARALLEL=1`），与旧服务分开运行，复用已安装的模型文件；题目文件名请求等待预算为 10 秒。新实例的限制不约束其他程序和旧服务。

Windows 上运行 `start.ps1` 启动本实例，Prefect 地址为 `http://127.0.0.1:4201`，独立的统计接口地址为 `http://127.0.0.1:4203`。部署是手动触发的，不会自己处理资料。每次最多 30000 组，内部按每批 500 组推进。

运行时提供 `person_folders` 和可选的 `library_root`、`known_paper_ids`、`verified_new_groups`。当前分类代码对没有可复用完整交接的新处理组，要求该组出现在已核实为新资料的 `verified_new_groups` 中；仅填写 `person_folders` 可能会返回历史状态待核实。不能把已经处理过的资料假称为新资料；已处理批次的再次分析须先核对相应历史状态。

流程返回 `task_id` 和统计接口路径。`GET /api/v1/first-step/tasks/{task_id}?filter=passed&page=1&page_size=50` 同时返回组数和当前分类的资料列表；`filter` 支持 `passed`、`failed`、`unrecognized`，省略时列出全部已处理组。`task_id` 可使用 `current` 指代最近一次任务，尚未有任务时返回 404。详情见 `FIRST_STEP_INTERFACE.md`。

前端逐组读取 `display_title` 作为主标题、`display_subtitle` 作为小字：

| `title_status` | 主标题 | 小字 |
| --- | --- | --- |
| 已提取 | 已确认题目 | 空 |
| 待核对 | 正文文件 SHA-256；没有时使用资料组标识 | 候选题目 |
| 未提取 | 正文文件 SHA-256；没有时使用资料组标识 | 题目未识别 |

接口另返回 `display_hash`、`hash_type`，`file_sha256` 是文件哈希，`group_id` 是资料组标识，二者不能混淆。资料组分类状态和题目状态是不同字段。Prefect 流程显示完成只表示该流程运行结束，必须读取分组统计才能知道每组是否通过。
