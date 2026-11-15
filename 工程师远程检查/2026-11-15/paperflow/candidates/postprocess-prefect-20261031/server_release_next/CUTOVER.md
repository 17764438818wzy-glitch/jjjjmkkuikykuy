# 隔离候选与后续页面切换记录

本文件用于预先审查，不执行切换。本次已读取当前计划任务动作：

| 项目 | 当前现网动作 |
| --- | --- |
| `Paperflow-LocalIntake-Api-14205` | `prefect-first-step-independent\.venv\Scripts\pythonw.exe -X utf8 candidates\local-intake-live-link-20261031\run_waiting_service.py --log local-intake-runtime\api.log -- serve --db local-intake-runtime\waiting-pool.sqlite3 --port 14205`；实际动作中各路径均为本工作区绝对路径。 |
| `Paperflow-FirstStep-Frontend-8795-Manual` | `C:\Python314\python.exe -m http.server 8795 --bind 127.0.0.1 --directory <工作区>\candidates\first-step-preview-8795`。 |
| 独立接收扫描器 | `Paperflow-LocalIntake-Receiver-14205`；不由页面候选替换。 |

## 版本清单

`release_layout.py` 定义后端清单范围：本候选的 `.py` 和 `.ps1`、现网 local-intake Python、后处理运行依赖及各投影子目录的 Python、服务器接入所需的明确文件、第一步回执读取包和 Prefect 项目的 Python，以及明确列出的工作区配置。无关服务器批量脚本和静态站点不纳入后端运行清单。`prepare_manifest.py` 生成的 JSON 包含每一个实际绝对路径及 SHA256、Python 版本和相关包版本；`tests`、缓存和本说明文件不属于运行清单。

静态站点用 `tests\prepare_site_inventory.py` 针对实际发布目录单独记录全部文件与 SHA256，并在切换后核验浏览器取得的资源。后端清单不冻结服务器文件，也不代替服务器 service config、服务器端代码清单、SSH 身份、执行进程或 journal 的独立核验。本机清单草稿不是可供全量运行的服务器配置。

## 8795 逐项合并

1. 切换前重新读取现网 `first-step-preview-8795`，以完整副本创建一个尚不存在的独立版本目录，保留 `app.js`、`llm-results.js`、其他脚本、样式、配置及 `vendor` 的实际内容；不要从旧候选覆盖现网文件。
2. 仅添加本候选 `site\postprocessing-results-server-20261101.js` 和 `site\prefect-ticket-counts-server-20261101.js`。基于刚读取的现网 `index.html` 移除原 `prefect-ticket-counts.js` 的引用，并在原 `postprocessing-results.js` 位置依次引用上述两个新文件，保留本候选同样的版本参数。保持 `llm-results.js?v=20261101-manual-retry-1` 及其他窗口新增功能；若现网已有后续变更，人工三方合并后重新测试。
3. 对独立版本目录逐文件比较预期内容并固定权限；用该**实际绝对路径**作为 `tests\prepare_site_inventory.py --site-root`，输出到本候选内一个全新静态清单文件；另用 `prepare_manifest.py` 生成后端依赖清单。独立校验两个清单、启动文件哈希和各自依赖。不要用目标尚未建好的路径生成静态清单或沿用候选 `site` 的草稿。
4. 在允许切换的维护步骤中保存当前 8795 任务动作，将 `--directory` 改成该独立版本目录，重新启动该静态服务并核对浏览器实际收到的 `index.html` 与脚本哈希和版本参数。此步骤需要服务切换；它未在本次执行。

## 独立 14206 启动注入

1. 保留原 14205 和接收扫描器动作与进程；重新检查 14206 未被占用，并记录数据库与 journal 的真实身份。候选使用原队列的只读方法，不运行原队列建表/迁移和工作锁探测。
2. 如之后允许发布，为只读投影创建独立任务，使用与现网相同的 Python 环境、只读队列数据库和新的 14206 端口，通过 `start_verified_page.ps1` 传入以下明确参数：
   - `PythonExe`：`<工作区>\prefect-first-step-independent\.venv\Scripts\pythonw.exe`
   - `RunnerPath`：本目录 `run_waiting_service.py`；`RunnerSha256`：独立核准的该文件哈希
   - `CliSha256`、`GuardSha256`、`LayoutSha256`：分别为本目录 `waiting_cli.py`、`release_guard.py`、`release_layout.py` 的独立核准哈希
   - `QueueDatabase`：`<工作区>\local-intake-runtime\waiting-pool.sqlite3`；`Log`：一个独立的后处理观察日志；`Port`：`14206`
   - `ReleaseManifest` 与 `ReleaseManifestSha256`：独立核准的后端清单及哈希；静态站点清单单独核验，不作为 14206 启动依赖
   - 正常投影需要 `RemoteConfig`、`RemoteConfigSha256`：只有用户授权范围内、经服务器窗口确认可用于该用途的配置及其独立核准哈希。当前单单测试配置只授权该单，不能用于全量批处理或未经审查的常驻切换。
   - 逐单子集观察必须显式加 `PartialObservation`；历史路由必须同时提供 `HistoryRegistry`、`HistoryRegistrySha256`、`HistoryAccess`、`HistoryAccessSha256`、`HistoryUnreadScript`、`HistoryUnreadScriptSha256`、`GuardBundle`、`GuardBundleSha256`。据服务器窗口报告，源码固定入口、清单和受限只读命令已安装到独立测试目录。三张指定回执在修复本机编码后得到一张特定检查通过、两张待核对；它不是全量页面验收，也不构成现场切换许可。
3. 计划任务动作应由受控账户保护，并在运行前对 `start_verified_page.ps1` 的**已读取字节**校验任务动作中独立固定的 SHA256，然后执行这同一份字节；不要把期望哈希从待校验脚本或同目录侧车读取。脚本再调用 Python 对 `RunnerPath` 已读取字节校验并执行；runner、CLI 和页面装配依次校验各自独立哈希和完整清单。计划任务动作、其权限以及 Python 环境是外部信任根。
4. 保持 14205 和接收扫描器运行；只启动新的 14206 任务，检查其端口仅有预期进程、启动模式为 `server_projection`，同时确认 14205 原入口未变化。PowerShell 包装使进程树不同，停止新任务后必须核对 14206 端口和子进程实际退出，不能假设结束父进程即结束服务。
5. 检查页面实际展示和可点击列表；服务器失联、来源失联、配置漂移、过期、未知工单都不应显示为已核验通过或等待 RAG。进行任何真实业务动作仍以对应工单和既有门禁为准；页面本身不触发处理。

以下是待最终配置和哈希审核后的启动动作模板，仅供审阅；尖括号必须用独立核准的实际值替换，不应原样执行。计划任务应使用隐藏窗口，并将完整动作及哈希固定在受控任务配置中：

```powershell
$launcherPath = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-prefect-20261031\server_release_next\start_verified_page.ps1'
$expectedLauncherSha = '<启动器已核准SHA256>'
$launchParameters = @{
  PythonExe = 'F:\论文预处理系统\预处理系统2\prefect-first-step-independent\.venv\Scripts\pythonw.exe'
  RunnerPath = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-prefect-20261031\server_release_next\run_waiting_service.py'
  RunnerSha256 = '<runner已核准SHA256>'
  CliSha256 = '<waiting_cli已核准SHA256>'
  GuardSha256 = '<release_guard已核准SHA256>'
  LayoutSha256 = '<release_layout已核准SHA256>'
  QueueDatabase = 'F:\论文预处理系统\预处理系统2\local-intake-runtime\waiting-pool.sqlite3'
  Log = 'F:\论文预处理系统\预处理系统2\local-intake-runtime\postprocessing-observer.log'
  ReleaseManifest = '<后端清单绝对路径>'
  ReleaseManifestSha256 = '<后端清单已核准SHA256>'
  RemoteConfig = '<获授权的只读观察配置绝对路径>'
  RemoteConfigSha256 = '<该配置已核准SHA256>'
  PartialObservation = $true
  HistoryRegistry = '<逐版本配置清单绝对路径>'
  HistoryRegistrySha256 = '<逐版本配置清单已核准SHA256>'
  HistoryAccess = '<独立访问引用清单绝对路径>'
  HistoryAccessSha256 = '<独立访问引用清单已核准SHA256>'
  HistoryUnreadScript = '<服务器已审核并放行的未读检查脚本绝对路径>'
  HistoryUnreadScriptSha256 = '<未读检查脚本已核准SHA256>'
  GuardBundle = '<本机源码固定 bundle 绝对路径>'
  GuardBundleSha256 = '<本机 bundle 已核准SHA256>'
  Port = 14206
}
$launcherBytes = [System.IO.File]::ReadAllBytes($launcherPath)
$hasher = [System.Security.Cryptography.SHA256]::Create()
try { $actual = -join ($hasher.ComputeHash($launcherBytes) | ForEach-Object { $_.ToString('x2') }) }
finally { $hasher.Dispose() }
if ($actual -cne $expectedLauncherSha) { throw '观察启动器哈希不匹配' }
& ([scriptblock]::Create([System.Text.Encoding]::UTF8.GetString($launcherBytes))) @launchParameters
```

上述历史路由仅为候选：它按固定注册表和各自访问引用逐个调用既有 `inspect`，并通过候选固定入口核验该版本 app 源码；该服务端检查要求配置与原 `service_jobs.config_sha256` 匹配，只有完成原回执及产物映射核验再检查未读提示。失败、超时或不可访问均是未知，不能从失败推断匹配。单次 inspect 最长 10 秒且各尝试共享总时限；本窗口三张指定回执在修复编码后分别耗时 4.564、5.813、6.323 秒，不代表全量负载的响应时延。据服务器窗口报告，新增入口及受限查询命令已安装至独立目录，见 `readonly-guarded-gate-proposal-candidate-20261101-v4.md`；v2 入口和 bundle 的哈希已失效。

`history-registry-candidate-bb7d5d9ae45b.json` 和 `history-access-candidate-ed0ef92b075f.json` 是来自服务器窗口 6 版本审计记录的草稿；访问清单只保存配置路径和哈希，不复制私钥。其中 53ff 版本没有可用访问引用。它们是某次只读快照，不代表服务器状态现在未变，也不代表新接口已安装或安全信任链已固定。完成现场审核前，不能照模板启动历史路由。

显式 `PartialObservation` 已接入候选 14206 的接口装配，但未接入现网。两层缓存在下一次 HTTP 请求时用 SQLite 只读 `data_version` 检查控制库变化；显示仍有轮询及网络时间窗口。局部核验期间整个账本变化仍会保守失效，真实 8 并发稳定性未验证。

## 回退

在确认清单、入口与 Python 环境仍可信时，14206 可用相同已核准参数改为 `ClosedProjection` 启动，不传 `RemoteConfig`；也可停止独立 14206，页面会把后处理/RAG 统计显示为未知。若清单或引导信任不成立，14206 返回 503，14205 保持其独立服务。不能简单恢复旧后处理投影或把本机 PID 当服务器状态；如果静态站点需要退回，应验证实际浏览器资源与回退后旧投影的显示边界。

回退前后均保留数据库、原件、传输/启动意图、未知工单、服务器账本和所有回执；不重发、不认领，不替换任何历史版本的 service config。候选代码已实现逐版本选择并通过合成测试，但不能宣称混合版本已在真实服务器核验。所有任务变更、停启及真实工单动作均未由本说明执行。
