# 第一步分类：前后端接口

这套工作间只做本机资料分类，不执行登记或后续处理。用户工作间与合成测试区分开，均未替换旧 Prefect。

- 页面：`http://127.0.0.1:8795/`
- 用户分类 API：`http://127.0.0.1:14203`
- 用户独立 Prefect：`http://127.0.0.1:4291`
- 本机 Nimble：`http://127.0.0.1:11435/v1/systemone`
- 用户工作数据：本目录的 `runtime/workspace/data`；分类产物：`runtime/workspace/output`；每组交付包在该输出目录的 `deliveries` 下。
- 合成测试使用 14204 / 4292 和独立数据；其测试任务不出现在用户工作间。

页面地址和 API 地址限本机访问。14203 和 4291 由本机计划任务 `Paperflow-FirstStep-Manual-14203-4291` 手动启动并保持宿主进程；该任务没有自动触发器，电脑重启后不会自动启动。它只启动服务，不提交论文任务。

宿主启动前会检查上传、模型请求和 Prefect 队列；存在未完成工作时拒绝启动，避免自动重发。宿主不自动重启退出的服务，退出信息写入 `runtime/managed-service-host.log`，需要人工核实。

## 统计与列表

`GET http://127.0.0.1:14203/api/v1/first-step/tasks/{task_id}?filter=passed&page=1&page_size=50`

页面初开使用 `current` 查询最近任务，随后使用返回的真实任务编号。`filter` 可选 `passed`、`failed`、`unrecognized`、`unknown`。对应通过、失败、未识别、请求结果未知；点击数字后用同一接口分页查询对应列表。`processed_groups` 等于前三种结果之和，未知、排队和处理中不计入已处理。各列表的 `list_total` 对应相应统计数。

题目仅控制显示：`已提取` 显示 `paper_title`；`待核对` 显示 `display_hash` 和 `paper_title_candidate`；`未提取` 显示 `display_hash` 和“题目未识别”。已有确定的正文哈希优先作 `display_hash`，否则用资料组编号。失败和未知返回 `reason`。读取列表不会请求 Nimble。

上传结束后能明确分组时返回总组数；无法归组的散文件放入 `ungrouped_files`，总组数显示为未知，不冒充完成。浏览器文件选择不包含空目录，因此空目录不会被统计。

## 工作间与上传

1. `POST /api/v1/first-step/tasks`：`{"name":"任务名","request_id":"可选 UUID"}`。
2. `POST /api/v1/first-step/tasks/{task_id}/uploads`：`{"root_name":"选择的根目录名"}`。
3. 分批 `POST /api/v1/first-step/uploads/{upload_id}/manifest`：`{"files":[{"relative_path":"根目录/子目录/文件名","size":123}]}`。
4. `POST /api/v1/first-step/uploads/{upload_id}/seal`：`{"expected_files":文件总数}`。
5. Uppy Tus 上传到 `http://127.0.0.1:14203/tus/files/`；请求带 `X-Task-ID`、`X-Folder-Upload-ID`、`X-Writer-ID`，元数据带 `task_id`、`folder_upload_id`、`relativePath`。
6. `POST /api/v1/first-step/uploads/{upload_id}/finish`：校验并还原目录；上传完成本身不会开始分类。

上传中如关闭页面，目前不能重新选取文件后接续同一上传；保留原窗口和文件对象时可以续传。资料原件不会被移动或删除。

## 开始、暂停与并发

- `GET /api/v1/first-step/tasks?page=1&page_size=50`：任务列表。
- `POST /api/v1/first-step/tasks/{task_id}/save`：`{"name":"可选名称"}`。
- `POST /api/v1/first-step/tasks/{task_id}/start`：`{"expected_revision":当前 control_revision}`，开始或恢复当前任务。
- `POST /api/v1/first-step/tasks/{task_id}/pause`：`{}`。暂停后不再派发新组和新模型请求，已发出的请求先等待结果；之后进入 `paused`。

一次最多同时处理 4 组，每 500 组为一个批次；本工作间的 Nimble 请求同时只发送 1 个。排队时间不占模型响应超时时间，题目辅助请求的响应时间上限为 10 秒。其他独立程序如果也使用本机模型，不受本工作间的并发锁管理。

必需的模型请求如果发送后没有得到确定结果，该组列为未知，不自动重发；其他组继续处理。题目辅助请求未知不会单独阻断已确认的分类。已收到明确结果的资料可以重新建立任务处理。

新工作间按所选根目录及组相对路径、请求类型和相关文件内容关联未知请求。浏览器不提供本机父目录：如果不同来源具有完全相同的所选目录名、组相对路径和文件，可能无法区分；同一批资料改选根目录名则可能无法关联。未知结果应先核实，避免人工重复发送。
