# 第一阶段到本地接收：占用与保护接口

本文件描述接口及约定。2026-10-31 08:13 已将本候选的五个接口文件切换到独立第一步 14203；此状态是快照，后续仍需按当前进程和数据核实。不要因分类通过或目录存在就认定本地已经接收。

切换后只读核验：310 组中 309 组通过并登记，另一组未识别；本地真实等待库已有 309 组 waiting 和 309 个 accepted 引用，14205 接口显示 309、待核对 0，真实前处理尚未接通。合并当时运行目录最新分类规则后，相关合成回归 92 项通过；两端候选的实际 HTTP 合成握手也已通过。未启动论文分类、模型或本地全文处理。

14203 现由独立的手动计划任务 `Paperflow-FirstStep-API-14203` 观察，任务入口为 `managed_api_only.py`；它不自动重试，也没有登录触发器。切换备份及一致性快照在运行目录 `runtime/retention-api-switch-20261031-081307`。已有引用后不得恢复旧的无保护删除接口或回滚真实状态库。

## 协议

1. 本地先持久保存 `request_id`，随后请求 `POST /api/v1/first-step/tasks/{task_id}/local-retentions`，发送 `group_id`、`paper_id`、`receipt_id`、`consumer_id=local-intake`、UUID `request_id`。成功返回相同身份、`retention_id`、`state=reserved` 和 `created`。同一请求重放返回相同引用，不产生重复；身份冲突返回 409。
2. 本地核验正文、附件和回执后，提交权威 SQLite 的 `groups` 和 `retention_requests`；不要持有该库的写事务调用上游 HTTP。
3. 本地请求 `POST /api/v1/first-step/tasks/{task_id}/local-retentions/{retention_id}/confirm`，发送 `consumer_id` 和 `receipt_id`。上游只读服务端配置的权威队列，核对该引用及已提交的组、回执和文件指纹；通过后返回 `state=accepted`。`waiting` 和有对应处理领取记录的 `processing` 可确认；`needs_review`、身份冲突返回 409，库缺失或损坏返回 503。重复确认会重新核验。
4. `POST .../{retention_id}/release` 当前始终返回 409。登记完成但尚未被本地发现的资料也默认受删除保护；即使本地说“已经处理”，当前也不会释放。任务有保护引用或仍有已登记交付时不能删除。
5. `GET /api/v1/first-step/tasks/{task_id}/local-retentions?page=1&page_size=200` 只查显式引用，不遍历全部登记库；返回 `task_id`、`items`、`page`、`page_size`、`list_total`、`has_next`、`revision`。每项含任务、资料组、论文、回执、接收方、请求、引用编号和状态。按固定顺序分页，跨页 `revision` 或总数变化时应重新查询，不能以不完整列表判定引用消失。此接口不是删除安全判断。

## 配置和安全边界

`install_control_routes(..., local_intake_queue_db=<绝对路径>)` 用于程序或隔离测试。正式 `summary_api.py` 先读取环境变量 `PAPERFLOW_LOCAL_INTAKE_DB`，否则读取配置字段 `local_intake_queue_db`；没有配置、数据库不存在、无法读取或 schema 不匹配时确认返回 503，不会创建新库。两端应指向同一个 `paperflow-local-waiting/1` 数据库。

文件大内容由本地首次接收和未来领取时核验；上游确认采用已核验记录的文件 stat 指纹，并复核回执、交付清单和证据等小文件摘要。它不是每次重新扫描全文的哈希证明，也不保证确认之后文件不再变化。异常数据及不同状态保守保留，不能由调用方口头声明释放。

上游候选 5 个需要迁移的代码文件：`summary_api.py`，以及 `paperflow_prefect/first_step_control.py`、`first_step_control_api.py`、`first_step_delete.py`、`first_step_retention.py`。已有运行目录的 `group_classification.py` 是较新的独立改动，不能用本目录旧文件覆盖。部署、API 托管和回滚需另外核实当前无运行任务/上传，以及与本地窗口的切换顺序。
