# 第一阶段登记到本地处理：接收接口候选

这个候选只对齐单组已登记资料的接收接口，不提交 Prefect 任务，不调用模型，不提取全文，也不改写第一步的原始回执。源码和运行中的第一步均未改动。

## 调用

`local_handoff.read_registered_item(item, *, receipt_root)` 接收第一步任务列表中一项完整的原始 `item`，以及当前第一步配置中的登记根目录。正式第一步的配置位置为 `candidates/first-step-control-20261029/runtime/workspace-config.json`，其中 `receipt_root` 才是本实例正在使用的登记位置。

输入的 `status`、`registration_status`、`delivery_complete` 必须表示分类通过、登记成功且交付完整；`group_id`、`paper_id`、`receipt_id`、`receipt_path` 和 `delivery_dir` 必须与登记回执、交付清单一致。未登记项、不同组/论文/回执混配、登记目录以外的回执不作为有效输入。

```python
from local_handoff import read_registered_item

# item 是第一步列表接口返回的一项；receipt_root 来自实际运行配置。
observed = read_registered_item(item, receipt_root=receipt_root)
if observed['body_available']:
    body = observed['paper']['body']
    attachments = observed['paper']['attachments']
```

候选通过独立 Python 命名空间复用 `first-step-control-20261029/paperflow_prefect/stage3_handoff.py` 及其同版本依赖，避免同进程已加载的旧 `paperflow_prefect` 误用为新版。运行时依赖当前该目录的代码，使用前仍须确认具体代码版本和调用环境。

## 返回与边界

返回的 `paper` 保留原论文编号、正式文件编号、文件哈希、来源路径、交付副本路径、历史与证据引用；`items` 和 `observation` 保留逐项状态及实际核验问题。它们是内存中的接收观察，不是新的登记回执。

如果附件副本或证据受损，按新版读取器返回的逐项结果报告；核验通过的正文仍可见。正文不可用时 `body_available` 为 `False`，不能仅凭 `paper` 非空判断正文可用。原回执保持不变，损坏观察使用 `paperflow-receipt-observation/1`，不冒充已修改的原回执。

**尚未创建下游对上游任务的持久引用保护。** 第一步删除任务可能使相关交付路径或回执不再存在；接收观察不能作为永久保留承诺。在后续实际使用前必须重新核验，持久接收或自动排队还需要单独解决完整来源链与删除协调。

## 本次验证

- 合成登记资料覆盖正文和附件、重复读取、未登记、组/论文/回执/目录错配、越界回执、原始目录缺失、正文或附件副本损坏、附件证据损坏，以及旧包预加载等情况。
- 当前真实第一步的两份已登记回执只读抽样：一份无附件，一份有附件；身份及副本读取一致。抽样不是全批验证。
- 该候选尚未接入运行中的 Prefect、前端或自动调度，验证范围只到单组接收观察。

## 等待前处理数量接口候选（2026-10-30）

本次仅提供“等待前处理”统计，暂不提供“前处理中”数量。入口在 `waiting_api.create_app(queue)`，独立运行入口在 `waiting_cli.py`。统计来源是 `local_queue.py` 中已经按第一步登记回执实际核验并持久记录的资料组，不直接把上游登记数当作本地已接收数。接收输出仅列正文和附件，垃圾不进入下游输入列表；不复制或移动交付文件。

```http
GET /api/v1/local-intake/preprocessing/summary
```

响应示例（字段示例，不是实时数值）：

```json
{
  "scope": "all_tasks",
  "waiting_count": 2,
  "waiting_available": true,
  "as_of": "2026-10-30T18:00:00+00:00",
  "upstream_scan_completed_at": "2026-10-30T17:59:58+00:00",
  "oldest_verified_at": "2026-10-30T17:59:40+00:00",
  "needs_review_count": 0,
  "preprocessing_handoff_connected": false
}
```

前端仅在 `waiting_available === true` 且 `waiting_count` 为数字时展示该数；否则展示 `—`。接口设置 `Cache-Control: no-store`。未扫描、扫描失败、扫描超过有效期、已接收资料失效或身份冲突待核时，`waiting_count` 为 `null`。`as_of` 是本地状态快照时间，`upstream_scan_completed_at` 是最近完成的扫描时间，`oldest_verified_at` 是等待项中最早的最近一次核验时间；它们不是文件持续受保护的证明。状态库无法读取时返回 HTTP 503，数量仍为 `null`。

`preprocessing_handoff_connected` 当前固定为 `false`：实际前处理开始时如何让该组退出等待，尚未接入。因而这个候选目前只能表达本窗口已核验接收、尚未由该候选移出的组；不能宣称已覆盖其他入口实际开始处理的情况。

### 接入前需要知道

- `waiting_cli.py scan --db <独立状态库路径> --receipt-root <第一步登记根目录>` 显式执行一次发现和核验；`waiting_cli.py watch --db <相同状态库路径> --receipt-root <第一步登记根目录>` 可显式启动循环扫描，默认一轮结束后间隔 30 秒再启动下一轮；`waiting_cli.py serve --db <相同状态库路径> --port <空闲本机端口>` 只监听 `127.0.0.1`，不自行扫描。默认扫描结果有效期为 60 秒，可用 `serve --max-age-seconds` 调整。**本次没有启用真实资料的循环扫描或生产服务。**
- 每轮扫描会逐份核验交付材料；大量材料时的耗时、磁盘负载与计数新鲜度尚未实测。扫描时间超过有效期或扫描不稳定时，接口可能持续显示不可用，不保证每组登记后立刻出现数字。
- 循环扫描的协作停止在请求和资料组之间生效；单次文件哈希、已经发出的 HTTP 请求和正在执行的数据库事务不能保证即时中止。CLI 前台可通过 Ctrl+C 中断，未配置守护或自动重启，未捕获的本地异常可能使进程退出。
- 上游任务详情 GET 在当前源码中可能刷新异常任务状态；正式启用扫描前需核对可接受性及当前运行版本。本候选开发测试没有调用真实任务接口。
- 多页扫描发生变化时，已单独核验通过的组可先被记录，但总数返回不可用；两次轮询之间被删除的上游任务可能无法发现。首次接收核验失败的资料组身份、回执位置及原因保存在独立状态库 `receive_failures`；待核项目前没有自动清除或人工处置接口。
- 空来源无法确认上游服务与登记根目录的绑定，返回不可用；即便非空并核验通过，也仅证明所见回执属于指定目录，尚不能证明该端口就是预期的第一步任务实例。正式接线前需核对运行实例和任务库身份。
- 上游删除任务仍可能删除正在等待的交付副本和回执；现有删除逻辑尚未与本地占用联锁。扫描时刻及文件核验时间不能消除删除竞态，也不能保证后续处理时文件仍存在。
- 使用现有每任务、每组、每版本的目录，不暗自修改文件布局。按用户要求，不启动前处理、LLM、RAG、NAS 或前端修改。
