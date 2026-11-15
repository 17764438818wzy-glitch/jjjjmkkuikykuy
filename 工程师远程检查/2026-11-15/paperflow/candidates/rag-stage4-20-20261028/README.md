# Prefect 论文五步流程接入

当前状态：业务适配和手动部署已接好；`config.json` 中 `execution_enabled=false`。本轮没有处理真实论文、调用真实模型、写真实 RAG 或 NAS。Windows 本机执行器访问 F 盘，局域网服务器上的 Prefect 负责调度和显示。

- [五步流程部署](http://192.168.1.120:4200/v2/deployments/deployment/3c968368-58fc-40a0-9966-0ba2fde8c92c)
- [接线检查运行记录](http://192.168.1.120:4200/v2/runs/flow-run/eb01aad5-e25d-45ca-875a-250a7691c917)

默认运行参数为 `run_mode=check`。点击运行只核对模块、模板、路径配置和 Prefect 额度，不扫描原稿、不加载模型/RAG凭据、不探测NAS。没有自动扫描或定时计划。即使把网页参数改为 `process`，本机关闭开关仍会拒绝执行。

## 已接的业务

1. 接收 `F:/总论文_整理后_20261028/学校/个人/正文|附件|垃圾材料`。正文目录内完全相同的副本只处理一次，原件都保留；内容不同的多个正文报冲突。垃圾材料不进入处理。
2. 复用本机 DOCX/PDF/Office 等正文读取。附件独立做有界预览和简单说明，压缩包仅查看清单及可用README，不运行其中的程序。
3. 只有正文使用 `论文预提取_空白模板_v1.md`。复用已有完整输入、图像适配、长材料分段、回答保存和字段/引用校验。
4. 复用正文脱敏、精确离线分词、原文/画像/学习单元/必要图表切片、封包及 RAG 读回/启用/检索核验。附件仅发布简短说明，在同一目标库关联正文ID，并核对正文身份。
5. 两路 RAG 核验后，将原稿和原始附件复制到 NAS 子目录 `\\zwjy-nas\paper_ai\论文原件库\Prefect五步`。核对完整文件清单、SHA256和实际读回，保存完成记录。**本地个人文件夹及排除材料保留，没有本地释放或删除步骤。**

## 并发与恢复

| 资源 | 配置名额 |
| --- | ---: |
| 正文 LLM | 100 |
| 附件 LLM | 20 |
| LLM 合计 | 120 |
| 本机准备/图像适配/附件读取 | 4 |
| 封包及离线计数 | 2 |
| RAG 写入和核验 | 4 |
| NAS 归档 | 2 |

使用 Prefect 的全局并发额度；实际模型 dispatch 期间占用 LLM 名额，本机工作期间占用本机名额。正文与附件分别推进，附件失败时正文成果仍保存。名额配置不等于真实同时请求峰值，本轮未进行真实100＋20压测。

一个手动批次最多同时推进160篇，底层业务线程池上限256。默认批次上限100篇，可通过 `max_papers` 指定；每批最多10000篇。每篇使用一个跨进程操作锁，防止重复入口同时操作同一份记录；操作结束或进程退出后由系统释放。取消时设置停止标记，等待已有线程完成或保存未知结果后再释放同篇占用。强制结束进程的未决请求保留给后续核对。

正文缓存独立于附件修订；归档失败不会重新提取画像。原稿、模板、目录、模型及原生版本身份参与复用检查。已生成封包继续使用原task/manifest身份，不重新生成另一个入库身份。

上传超时先读回核对，不能证明不存在的写入不盲目重发。原生正文交付发生部分写入且回读不完整时，会保留未完成记录，当前适配器不会自行推测缺口并重复发送。未封完的中断封包也保留现场，需核对后恢复。

## 复用边界

直接复用 `程序/updates/unified-AB-20261026-r05-rag600-nas-20261027-c03-normal59-llm140-r01` 的业务函数，必要依赖从 `程序/desktop` 回退。原生源码固定为 `source-manifest.json` 中的哈希；发现变化会停止加载，不能悄悄混用版本。

没有启动旧 Manager、Scheduler、AutoProcessor、自动上传队列或清理后台任务。新增部分是输入身份/阶段记录、独立附件说明的发布关联，以及 Prefect 连接代码。NAS 复用原生流式复制/清单/读回内核，使用精确原件清单总字节预算，未沿用旧手动接口的512MiB批次上限。

模型和 RAG 密钥仅在真实执行获准且开关打开后，从原有 Windows DPAPI 配置读取。本工程配置不存明文密钥。

## 使用和本次验证

在本目录执行 `start-runner.ps1` 启动隐藏的本机执行器；`stop-runner.ps1` 只停止此工程的执行器，存在子进程时会要求先结束当前任务。没有设置开机自启。

```powershell
.\.venv\Scripts\python.exe -X utf8 manage.py preflight
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -X utf8 tests\native_fixture_check.py
```

- 相关测试覆盖输入分流、身份/缓存、未知写入不重复发送、父正文关联、局部失败、取消等待及关闭执行开关。
- 合成 DOCX 已实际经过原生解析、模板画像校验、真实离线counter封包、附件说明准入、临时本地归档与逐文件读回。模型返回值是夹具，不是真实LLM。
- Prefect手动检查运行已由本机runner接收，六个检查任务完成。
- 尚未验证真实论文的内容质量、真实模型额度/吞吐，以及真实RAG和NAS整链。这些需要后续明确开始真实样本验收。

证据位于 `runtime/deployment-receipt.json`、`runtime/check-run-receipt.json`、`runtime/native-fixture-receipt.json`、`runtime/test-results.txt`。处理记录位于独立的 `runtime/data`，与原稿目录分开。

Prefect并发用法参考：[官方全局并发文档](https://github.com/PrefectHQ/prefect/blob/main/docs/v3/how-to-guides/workflows/global-concurrency-limits.mdx)。
