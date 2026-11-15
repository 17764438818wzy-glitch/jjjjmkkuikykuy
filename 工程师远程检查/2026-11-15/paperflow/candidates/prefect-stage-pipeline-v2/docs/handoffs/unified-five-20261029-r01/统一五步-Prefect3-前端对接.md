# 统一五步：Prefect 3 前端对接

更新：2026-10-29。入口已切换到统一实现；配置与检查运行已读回。本文描述当前实际接口。

## 服务与版本

| 项目 | 当前值 |
|---|---|
| Prefect Server | `http://192.168.1.120:4200`，版本 `3.9.0` |
| API base | `http://192.168.1.120:4200/api` |
| Deployment ID | `3c968368-58fc-40a0-9966-0ba2fde8c92c` |
| 部署名称 | `论文五步流程（已接线）/五步接入-手动运行` |
| 实际入口 | `paperflow_prefect.unified_flows.paperflow_batch` |
| 业务契约 | `paperflow-unified-five/1` |
| 批次契约 | `paperflow-unified-batch/1` |
| 调度 | 手动提交；无自动扫描计划；同时运行 1 个批次，批内各篇独立推进 |
| 并发上限 | 正文模型 100、附件模型 20、总模型 120；RAG 20；NAS 2；本地准备 4；封包 2 |

并发数是名额上限，不代表实时请求数。RAG 正文、附件与恢复入口共用 `paperflow-connected-rag`。

## 正式目录

以下均为 Windows 执行端绝对路径。前端上传完成后，把完整个人资料组放进 inbox，再提交目录路径。

```text
输入：F:\论文预处理系统\预处理系统2\prefect-paperflow\runtime\inbox
处理记录：F:\论文预处理系统\预处理系统2\prefect-paperflow\runtime\data
接收台账：F:\论文预处理系统\预处理系统2\prefect-paperflow\runtime\receipts
分流记录：F:\论文预处理系统\预处理系统2\prefect-paperflow\runtime\classification
NAS：\\zwjy-nas\paper_ai\论文原件库\Prefect五步\统一五步正式
```

专业映射使用主工程 `catalog.json` 的 883 个专业独立论文库/资料库，已经按远端 ID、库名和嵌入模型核对。历史封包仍沿用其中冻结的库 ID；旧试跑结果不会自动改投正式库。

新输入支持平铺个人目录，或个人目录下一层的 `正文/附件/垃圾材料` 等既有分类文件夹。垃圾材料保留。分类不确定的文件会保留原因，已经接收的正文和附件仍可独立处理；整篇不完整时不能进入 NAS 清理。

## 创建运行

`POST /deployments/3c968368-58fc-40a0-9966-0ba2fde8c92c/create_flow_run`

新资料示例：

```json
{
  "name": "前端批次-001",
  "idempotency_key": "frontend-request-001",
  "parameters": {
    "run_mode": "process",
    "person_folders": ["F:\\论文预处理系统\\预处理系统2\\prefect-paperflow\\runtime\\inbox\\学校\\个人资料组"],
    "verified_new_groups": ["F:\\论文预处理系统\\预处理系统2\\prefect-paperflow\\runtime\\inbox\\学校\\个人资料组"],
    "max_papers": 100
  }
}
```

重复发送同一次前端请求必须保持同一个 `idempotency_key`；新的人工恢复操作使用新 key。保留响应中的 `id` 作为 `flow_run_id`。

`verified_new_groups` 仅用于已确认没有既有分流请求的新资料组，不应把历史目录批量标成新资料。模型调用还会独立核对原请求台账。

| 参数 | 用途 |
|---|---|
| `run_mode` | `check`（默认）、`process`、`resume-rag`、`resume-stage5` |
| `person_folders` | 待分流的完整个人目录绝对路径列表 |
| `handoff_paths` | 已有 `paperflow-group-handoff/2` 文件列表 |
| `handoff_evidence` | `{交接文件绝对路径: [附件证据 sidecar 绝对路径]}`；显式 handoff 入口一起传入证据 |
| `receipt_paths` | 已有 `paperflow-receipt/1` 文件列表；恢复必须用原回执 |
| `verified_new_groups` | 已确认首次分流的目录列表 |
| `migrations` | 按 `group_id` 提供 `previous_group_root` / `origin_group_id`；用于已有资料搬迁后的身份连续性 |
| `max_papers` | 本次输入数量上限，1–10000；不会截断后悄悄遗漏超限项 |

三个输入列表可以并用，但不能重复同一接收版本，也不能在同一批次提交同一论文的多个迁移版本。输入必须属于正式 inbox；空输入不会扫描或重跑旧批次。

已有回执处理或恢复：

```json
{
  "name": "前端恢复-001",
  "idempotency_key": "frontend-recovery-001",
  "parameters": {
    "run_mode": "resume-rag",
    "receipt_paths": ["F:\\论文预处理系统\\预处理系统2\\prefect-paperflow\\runtime\\receipts\\receipts\\实际回执ID.json"],
    "max_papers": 1
  }
}
```

`resume-rag` 沿用原正文对象，完成附件入库，再进入第五步；没有原正文回执会停下。`resume-stage5` 必须已有整篇准入绑定；它只核对 NAS 并继续清理/完成记录。已有第五步绑定的论文即使再次 `process` 也优先恢复第五步。

## 读取实际进度和结果

1. `GET /flow_runs/{flow_run_id}` 读取调度状态。
2. `POST /flow_runs/filter`，body 为 `{"flow_runs":{"parent_flow_run_id":{"any_":["批次flow_run_id"]}},"limit":200,"offset":0}`，读取逐篇子运行；多页按 offset 分页。
3. `POST /task_runs/filter`，body 为 `{"task_runs":{"flow_run_id":{"any_":["批次或单篇flow_run_id"]}},"limit":200,"offset":0}`，读取阶段任务状态。
4. 最终业务结果从下列 artifact 读取。当前 `persist_result=false`，不要依赖 Prefect state.data 得到业务 JSON。

`POST /artifacts/filter`：

```json
{
  "artifacts": {
    "flow_run_id": {"any_": ["本次批次flow_run_id"]},
    "key": {"any_": ["paperflow-unified-batch"]}
  },
  "limit": 10,
  "offset": 0
}
```

该 artifact 为 markdown 类型，`data` 是 JSON 文本，解析后字段如下：

```json
{
  "schema": "paperflow-unified-batch/1",
  "contract_version": "paperflow-unified-five/1",
  "batch_id": "本地业务批次ID",
  "total": 1,
  "complete": 1,
  "papers": [
    {
      "status": "complete",
      "paper_id": "PF_...",
      "receipt_id": "...",
      "receipt_path": "...",
      "archive_status": "verified",
      "local_originals": "cleaned",
      "cleanup": {"status": "complete"},
      "record": "实际完成回执路径"
    }
  ],
  "runtime": {"source_sha256": "实际加载代码摘要", "disk_matches_loaded": true}
}
```

判定规则：

- `check` 的 Completed 只说明配置/模块/额度检查通过。其 artifact key 是 `paperflow-unified-runtime`。
- 一篇只有 `papers[].status=complete` 才是整篇完成；RAG 单目标成功或 NAS 复制完成都不能代替它。
- `incomplete` 的 `failures` 表示停止阶段；已有结果继续保留。一个输入失败不阻断其他有效输入。
- `previous_result` 仅供历史展示，不能覆盖本次 incomplete。
- 进程在最终 artifact 生成前异常退出时，保持失败/待核对状态；不能因为查不到 artifact 推断为成功。

阶段任务名覆盖分流接收、本地正文准备、正文画像、附件识别/MD、封包、正文/附件 RAG、NAS 和清理。分流与逐项接收当前位于同一 input task 中，详细接收状态从 `receipt_paths` 指向的 H2.1 台账读取。

执行端还保存 `runtime/data/batches/<batch_id>-rag.json`（`rag-progress/1`），含 waiting、occupied、uploading、verifying、verified_papers 和 unknown。它是本地状态文件，当前没有新建 HTTP 读取接口；前端后端若需要更细 RAG 进度，可按已返回的 batch_id 读取该文件。既有看板 `/api/v1/tasks` 仍属于旧执行器接口，需要前端适配到这里的 Prefect 接口；设计稿中的 `stage45-view/1` 尚不是现有服务接口。

## 取消、下载与本地清理

取消调用 `POST /flow_runs/{id}/set_state`：`{"state":{"type":"CANCELLING","name":"Cancelling"},"force":false}`。收到的是取消请求，需继续读回终态；不把它显示成“已暂停”。原请求和未确认上传都保留，恢复不能盲目重发。

取消信号会阻止后续 RAG 写入与下一项本地删除；在途工作按原台账核对。Prefect 结束/强制终止进程后，未知远端结果仍需恢复读回。

原附件回执保留 RAG Files ID、专业资料库文档 ID、下载路径、原始 SHA256；当前 `attachment_download.py` / `attachment_ingest_cli.py download` 支持核验下载。前端后端复用这些回执与已有下载能力，RAG 凭据由服务端读取。

第五步只在整篇 RAG 证据齐全、NAS 清单和逐文件内容读回匹配后清理冻结的 inbox 原件。垃圾、后来新增文件、最终 MD、原请求/回答、封包和回执保留。可再生中间文件只有具备明确独占生产记录才允许清理；目前统一上游不会凭文件扩展名推断可删除缓存。

## 验收与回滚证据

- 380 项组合回归通过。
- 实际本机 Nimble 完成生成样例的分流，H2.1 接收正文和附件，未调用云模型。
- 883 专业的两类目标库全部按远端 ID、名称、嵌入模型核对。
- 已有真实附件的 36 次只读查询通过，0 远端写入、0 新模型请求。
- 真实 NAS 上完成生成样例 2 原件 + 1 独占缓存的复制读回、清理和删除中断恢复；这组 RAG 证据明确是隔离样例。
- 部署检查运行：`458c2e6f-9d01-4cf1-acf7-6e6773bc6522`，Completed，实际代码摘要 `d6c585eabad87e89c4a52c8ac11dcbad01a67c9e120b5c636bef72243c917dde`。
- 真实已有结果恢复链路：`a8b315c8-d4ad-4f2d-9879-dc30ba47d860`，Completed，整篇业务 complete。保留原论文号、工作目录和 RAG 文档 ID；NAS 读回后只删除 inbox 验收副本，原稿及 10 份模型/封包证据哈希不变，0 新模型请求、0 新 RAG 文档。这篇无附件，沿用原试跑封包目标；新模型首跑和含附件整链尚未做正式批量验收。
- 清理后的再次恢复：`c106e30e-4a9e-4f08-8059-dee9b779dfa1`。原件/模型请求保护与恢复读回见 `runtime/unified-live-recovery/acceptance.json`。

完整验收证据位于 `F:\论文预处理系统\预处理系统2\candidates\unified-five-20261029-r01\evidence`。部署备份位于 `runtime/backups/unified-five-1793286324011403200`，保留变更前文件、部署定义、额度及逐文件 SHA256。回滚需先确认运行器和名额空闲，恢复对应文件及旧 RAG 额度，再启动运行器；不要删除已产生的业务台账。
