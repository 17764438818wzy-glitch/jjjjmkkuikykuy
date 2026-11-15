# 论文持续处理工作流

电脑端负责持续调度、三轮 LLM、按需存图、脱敏切片和单篇累计最多两次定点交付补正；RAG 接收已处理内容并负责向量索引与检索。内容判断采用 DeepSeek 的结果，程序只核对实际交付：可读、内容未丢、身份一致、目标正确、必要图表和明确私人信息已处理、实际入库成功。没有 100 篇停止条件。

当前规则见 [交付检查规则](docs/delivery-rules.md)。本次改动后保持停止，未恢复入库。

## 从这里看代码

- `paperflow/graph.py`：LangGraph 阶段图、阶段转换、返修和版本切换。
- `paperflow/service.py`：持续运行、三个并发池、资源限速、暂停/停止。
- `paperflow/intake.py`：监听原稿、旧成果与问题稿接管。
- `paperflow/producer.py`：现有免费 Harness 渠道、三轮提示词和实际文件交付。
- `paperflow/prompts/round3_assets.txt`、`round3_redact.txt`、`round3_chunk.txt`：图表→脱敏→切片的独立提示词，直接采用用户确认草案，不再叠加旧混合协议。前两轮保持不变，衔接说明见 [第三轮提示词](docs/third-round-prompts.md)。
- `paperflow/reading.py`：电脑端原件读取工具，提供带原件哈希的全文、原生 XML、脚注/尾注和真实图片，不作语义判断。
- `paperflow/package.py`：纯程序技术检查、文件哈希和不可变发布。
- `paperflow/privacy.py`：明确作者字段或准确定位的机械处理，不扩大为普通号码、人名和研究数据检查。
- `paperflow/rag.py`：直接写切片、必要图片、超时对账和真实读回。
- `paperflow/store.py`：来源/修订、租约、逐目标回执与活动版本；内部进度由LangGraph检查点决定。
- `paperflow/maintenance.py`：稳定期后旧版远端文档的身份核对和清理。
- `paperflow/readiness.py`：全部验收通过且代码版本一致后，才允许启动持续服务。
- `paperflow/ui.py`：控制台，默认不自动运行。

设计依据在 `docs/design.md`，实施及验收进度在 `docs/progress.md`。
修改清单逐项证据在 `docs/fix-list-status.md`，真实验收与耗时在 `docs/acceptance.md`。

## 持续收稿入口

新入口使用受控 Manager 和 Scheduler。一次配置 `source_roots` 并启动后，程序会等待文件复制稳定，保存不可变副本，自动完成 LLM、封包和投递；空队列继续监听。每篇不需要再准备脚本。旧 Service/UI 执行入口继续封存。

```powershell
python -X utf8 -B -m paperflow --config <本次配置.json> run --root <本次运行目录> --execute --seconds 3600
```

## 诊断和验证

```powershell
.\.venv\Scripts\python.exe -X utf8 -B -m paperflow doctor --network
.\.venv\Scripts\python.exe -X utf8 -B -m paperflow status
.\.venv\Scripts\python.exe -X utf8 -B -m pytest -q
```

最新本地回归结果见 `runtime/independent-prompts-verification-20261017/result.json`；前两轮提示词兼容证明见同目录的 `compatibility.json`。每次pytest自动使用新的UUID临时目录，保留旧运行，不复用固定basetemp。历史全新 `runtime/verification-venv` 的验证记录见验收文档。

`run` 或 `managed --watch` 持续监听配置中的原稿目录；`managed --source <原稿>` 保留有限选材入口。不加 `--execute` 保持暂停。监听目录、目标库及服务器验收证据必须匹配；已有台账也必须属于本次范围，不能通过更换目录启动旧任务。当前工业工程复验仅选定两组原稿，正式全目录开关仍关闭。

旧 `.doc/.wps/.rtf/.odt` 继续复用既有 LibreOffice 转换器。格式转换使用短的私有输入、输出和 TEMP/TMP，避免深层工作进程目录触发 LibreOffice 崩溃；原稿不修改。转换进程继承原有受控进程树。临时目录有双边归属记录，管理器确认进程退出后可精确清理被强杀遗留的副本。

程序自动写 `run-timing.json`，分开记录程序启动准备、运行墙钟、每稿处理与投递；额外验收耗时另记。阅读索引另有读取和格式转换用时。开发、选材、测试准备不混入稿件速度；连续版本按 task、投递包和附件身份归属，避免重复统计。

修复程序故障后，可在原运行目录用 `run ... --execute --retry-task <task_id>` 显式接续处理未完成的暂存图作业。须先确认旧进程退出，并明确解除该运行目录的 STOP；原全量停止标记保留。该命令保留缓存、图检查点、执行历史和补正预算，不能重跑已成功材料或第一轮已排除的材料。可选修正说明不作为独立阻断条件；对于解析器准确定位的一枚独立多余闭括号，只在内存中尝试删除该符号，并要求整份标准 JSON 解析成功后才读取已有字段，不改原文件或推断名单。

配置可复制 `config.example.json` 为 `config.json`；不填写明文密钥。RAG 使用原程序 Windows DPAPI 凭据，Harness 使用现有免费 provider 的隔离运行副本。运行目录含私有资料，不能上传或作为论文成果发布。所有新增依赖、缓存和临时文件位于 E 盘。

第一轮沿用原提示词的材料筛选、学习价值和适用层次判断；排除/暂无法判断单篇暂存，保留理由。局部不足只限制相应用途，不增加总分或前几百名门槛。后续技术检查只核对交付，不新增一轮质量评审。

此程序不修改服务器配置，不导入旧 Workbench，也不启动旧批次调度。已发布材料及原稿保留；只清理本工作流拥有的可再生成临时目录。

新版稳定7天后才清理已停用旧远端文档；必须匹配完整版本身份和逐切片散列，删除前后都读回，分类移出的旧专业同样处理。UI显示近1h/24h成功、耗时、分类故障、积压、暂停名单和部分完成的待处理目标；写入核对与可检索确认分开显示。


## 8089 链路工作台

8089 工作台复用本目录程序。功能对应见 [docs/8089-workbench.md](docs/8089-workbench.md)。安装与回退记录位于 `E:/rag mid/deployments/workbench-8089-20261018`。启动保持暂停，只有登录后的明确开始或继续才执行所选批次；不自动恢复正式目录收稿。
