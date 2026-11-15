# 分阶段 Prefect 3 前端契约 v2

本文件先固定 Task 1 的业务接口。它不表示 Worker、迁移、服务端或生产切换已完成。实现位置为 `paperflow_prefect/pipeline_contracts.py`；模块不导入 Prefect、不访问文件或网络。后续任务必须消费此契约，不能另造操作身份或把提交 Flow 的 Completed 当成整篇完成。

## 1. 阶段与轻量序列化

业务阶段枚举 `Stage`：`intake / prepare / profile / attachment / package / rag_body / rag_attachment / archive / cleanup / finish`。

`DispatchKind`：`normal / recovery`。恢复执行 task_key 为 `model_recovery`，信封的业务 stage 仍是原 `profile` 或 `attachment`，operation_key 也保留；仅 dispatch_seq/kind 和调度记录变化。

三个 dataclass 都提供 `to_dict()` 和 `from_dict(dict)`；前者产生可直接 JSON 序列化的 dict，后者恢复 ArtifactRef/枚举/tuple。构造时拒绝未知字段，防止把全文、图片或凭据字段混进普通参数或结果。此结构限制不能替代调用方的隐私审查。

| 对象 | 字段 | 约束 |
|---|---|---|
| ArtifactRef | path, sha256 | 非空路径字符串；64 位十六进制 SHA256，归一为小写；不在构造时读文件 |
| StageEnvelope | batch_id, paper_id | 非空批次登记 ID、稳定业务论文 ID |
| StageEnvelope | receipt_ref, input_refs | 单个引用和有序引用列表；构造可传 ArtifactRef 或 `{path,sha256}` |
| StageEnvelope | operation_key, stage | `op-` 加 64 位小写 SHA256；原业务阶段枚举 |
| StageEnvelope | item_id, part_id | 非空稳定材料/分段 ID 或显式 null |
| StageEnvelope | contract_version | 本阶段处理契约版本；不是 Prefect 调度代码版本 |
| StageEnvelope | control_generation | 严格包含 global、batch、runtime，值均为 >=0 整数；布尔值不作为整数 |
| StageEnvelope | dispatch_seq, dispatch_kind | >=0 整数投递序号；normal/recovery；运行身份另存 owner/runtime config |
| StageResult | operation_key, state | 原操作身份、业务状态；不使用 Prefect 任务状态代替 |
| StageResult | evidence_refs, next_stages | 已保存证据引用列表、后继业务阶段列表；后继仅为提示，投递前仍须重算依赖 |

状态值为 `pending / submitted / running / complete / passed / verified / skipped / failed / retryable_failed / unknown / needs_attention / blocked / cancelled`。普通就绪只选择缺失记录或 state=pending 的阶段/条目。submitted、running、失败、unknown 均不能通过 ready_stages 自动重发，须走后续核对/恢复规则。

## 2. operation_key 的唯一算法输入

`operation_key(envelope_inputs)` 只序列化以下字段，再以 UTF-8、JSON sort_keys=True、紧凑分隔符计算 SHA256：

```json
{
  "paper_id": "稳定业务论文ID",
  "source_sha256": "本材料源字节的SHA256",
  "stage": "profile",
  "item_id": null,
  "part_id": null,
  "input_hashes": ["本阶段实际依赖的结果SHA256，按input_refs顺序"],
  "contract_version": "body-profile/1"
}
```

调用 API 时用 `input_refs`，函数提取各引用的 sha256 生成上述 `input_hashes`。顺序有业务意义，不排序列表。源 SHA 从已冻结回执核验后取得；同论文、同源字节、同输入、同处理契约跨批次生成同一操作身份。

batch_id、receipt_ref 及回执整体哈希、路径、显示文件名、调度版本、PID、尝试次数、dispatch_seq/kind、控制代号均不进入身份。调用方必须将本阶段真正依赖的每份输入放入 input_refs；正文 profile 不混入无关附件清单或整回执哈希。这样新增附件不会使已通过的正文失效。身份函数只校验哈希语法；真实字节、原请求、原文档和处理契约的证据核验由业务适配器负责。

## 3. 一篇论文的依赖证据

ready_stages/paper_status 只接收一篇论文的快照，不接收整批/全局等待表：

```json
{
  "paper_id": "paper-1",
  "batch_id": "batch-1",
  "submission": {"state": "complete"},
  "whole_receipt_verified": false,
  "body_admitted": true,
  "stages": {
    "intake": {"state": "complete"},
    "prepare": {"state": "complete"},
    "profile": {"state": "passed"},
    "package": {"state": "pending"}
  },
  "attachments": {
    "inventory_state": "complete",
    "items": {
      "attachment-1": {
        "required": true,
        "attachment": {"state": "failed"},
        "rag_attachment": {"state": "pending"}
      }
    }
  }
}
```

此例允许 package 就绪，不能 archive。intake=complete 表示已接收子集有效，whole_receipt_verified=false 表示整回执尚有缺口；已确认正文照常 prepare/profile/package/rag_body，整回执未核验只能阻止归档和最终收尾。whole_receipt_verified 省略等同未通过，只有显式 true 才通过。

body_admitted 由 runtime 从已确认、已接收正文证据导出；只有显式 true 才允许正文 prepare/profile/package/rag_body。缺省或 false 表示尚无已确认接收正文，不是新用户审批门槛；此时附件识别仍可进行，rag_attachment 等待本篇已接收父正文及其 RAG verified，archive/final 也不能放行。即使带入旧正文成功字段，也不能覆盖当前正文尚未接收的事实。

stages 保存论文级阶段；非空附件清单以 items[item_id].attachment/rag_attachment 为权威，不能拿 stages 的汇总成功或 skipped 覆盖单项失败。required 缺省 true；显式值必须为布尔值，null、0、空字符串等非布尔值一律拒绝。只有业务核验明确非必需的条目才能标布尔 false 并排除依赖。inventory_state=complete 表示明确枚举了本篇附件清单；unknown 或缺失不能解释为无附件。已接收条目可以独立识别，即使整清单未确认，也不能阻止正文推进。

如果阶段证据带 paper_id/batch_id，必须与当前快照一致，否则拒绝；跨批次复用全局操作证据时由适配器验证身份后投影进当前快照，不直接带入旧批次包装字段。无关 other_papers/other_batches 字段不参与依赖。

| 就绪阶段 | 本篇前置条件 |
|---|---|
| intake | 尚无 intake 结果或 pending |
| prepare | intake complete/passed/verified，body_admitted=true |
| profile | intake 已接收，body_admitted=true，prepare complete/passed/verified |
| attachment | intake 已接收；至少一个已接收 item 的 attachment pending；或明确空清单待写 skip |
| package | intake 已接收，body_admitted=true，profile passed/verified |
| rag_body | intake 已接收，body_admitted=true，package complete/passed/verified |
| rag_attachment | intake 已接收，body_admitted=true，本篇 rag_body verified；至少一个 item 的 attachment passed/verified 且 rag_attachment pending |
| archive | body_admitted=true，正文画像及所有必需附件识别通过，正文及所有必需附件 RAG verified，整回执 verified |
| cleanup | 上述全部成立且 archive verified（NAS逐文件读回通过） |
| finish | 上述全部成立且 cleanup verified（授权收尾收据通过） |

ready_stages 返回阶段名字，附件 item_id 的扇出由 dispatch 执行；dispatch 必须对每个条目应用上表同一个谓词，不能因为返回 attachment/rag_attachment 就把已发送、失败或未通过的其他条目一并投递。一个附件失败不阻止同篇其他已通过附件的 RAG，也不阻止任何正文阶段。

明确空清单必须先持久写入 `stages.attachment={state:"skipped",reason:"empty_inventory"}`，再写 `stages.rag_attachment={state:"skipped",reason:"empty_inventory"}`。ready_stages 返回这些阶段名用于无 HTTP 的 skip 写入。第二个 skip 仍在本篇正文 RAG verified 后发生。缺任一个 skip，不能宣称整篇 RAG 全目标通过。

passed/verified 必须由业务验证器先核验后写：profile 包含所有分段及汇总校验；RAG verified 包含所有目标身份、内容、启用、检索和附件父正文关联；NAS verified 包含逐文件 SHA256 和读回；cleanup verified 包含授权范围和收尾记录。本模块不把裸声明变成真实验证。历史 complete 不能直接映射 profile passed 或 RAG/NAS verified。

## 4. 前端整篇状态

| paper_status | 含义 |
|---|---|
| pending | 尚无提交登记完成证据 |
| submitted | 提交登记完成，尚未推进实际处理；提交 Flow Completed 到此为止 |
| running | 已有实际处理状态，但未达到下列完整里程碑 |
| llm_passed | 正文画像及所有必需附件识别通过；不等于入库完成 |
| rag_verified | 正文与所有必需附件全部目标通过 RAG 核验；整回执缺口仍须继续处理 |
| nas_verified | 整回执已核验、全目标 RAG 通过，NAS 逐文件核验通过 |
| completed | 以上全部成立，cleanup verified，finish verified |
| needs_attention | 当前阶段/条目存在 failed/unknown/needs_attention/blocked；显示详细证据及已成功兄弟分支 |

completed 每次重核整套必需门槛，不能只读一个旧 finish 成功字段。单目标库成功不应写本篇 rag_body/rag_attachment verified。paused、draining、cancelled 等控制/批次状态由控制与进度层单独显示；不由此纯业务里程碑函数猜测。

## 5. 输出命名与原名映射

`delivery_names(paper_id,title,body_name,attachments,title_confirmed=False,existing_names=())` 返回 display_title、title_confirmed、body 和按 item_id 索引的 attachments；各文件保存 original_name/delivery_name。函数只规划名称，不改动任何文件。

已确认题目：正文 `论文题目.扩展名`，附件 `论文题目_附件自身名.扩展名`。未确认题目：显示“待补题目”，处理中成果按内部 paper_id 保存，不把原文件里的姓名推断为论文题目。body_name 可为 null，表示没有待输出正文名。

Windows 非法字符和控制字符替换为下划线，去除结尾空格/句点，CON/PRN/AUX/NUL/COM1–9/LPT1–9 等保留名加前缀下划线，碰撞按 Windows 大小写不敏感规则追加 `_2`、`_3`。调用方提供目标目录 existing_names；必须先持久保存返回映射，恢复时复用映射，不重新分配名称。超长总路径及最终写入能力由文件输出层按实际目录核验，不在本函数中操作文件。

## 6. 可执行示例与范围

`evidence/pipeline-contract-example.json` 由真实契约 API 输出，包括正常信封、保留原身份的恢复信封、轻量结果、命名映射、依赖及里程碑示例。独立 Python 子进程读取 JSON 信封并输出结果的行为由 test_pipeline_contracts.py 覆盖。此任务只验证契约，后续 Worker、账本、派发、RAG/NAS 真实性和全局限额仍须分别接入与验收。
