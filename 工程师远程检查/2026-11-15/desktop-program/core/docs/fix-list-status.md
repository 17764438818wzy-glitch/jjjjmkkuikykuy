# 修改清单及架构方案落实记录

2026-10-16。依据 CODEX_FIX_LIST.md / CODEX_ARCHITECTURE_PLAN.md。先修P0并完成正式稿入库，再执行四步架构调整，未启动持续批处理。

| 项目 | 最终实现与证据 |
|---|---|
| 1 返修位置 | business_stage禁止repair，回原失败阶段和全部后继；消费端先发布新修订，verify/activate先重建新回执。test_fix_list/test_architecture_followup。 |
| 2 正式稿 | 迁移保留真实成果，r008在3个专业库各43文本+2图像，精确读回、检索、图片字节和清理通过。runtime/acceptance-paper/final-readback.json。 |
| 3 错误分类 | 网络/服务RetryPolicy+due退避，默认20次/24h上限；内容修复；程序code_error及private traceback，不调DeepSeek。汇合转发失败不清零预算。 |
| 4 生产探针 | 独立测试库图片、name/id过滤、停用排除、10KB/100KB/1MB元数据、删除读回均通过。runtime/acceptance-paper/probe/report.json。 |
| 5 Python | reader_python或当前解释器，doctor实际导入依赖；全新venv重装后再次测试。 |
| 6 并发状态 | version乐观锁、只提交必要变化、检查点唯一内部进度；control先用原生END清旧pending再接入，避免并行重复推进。 |
| 7 凭据 | worker固定槽位、独占启动清理停用目录、runtime/凭据忽略、公开交付白名单。 |
| 8 隐私 | 程序扫描公开文本正则和私有敏感映射，展开JSON转义；命中原文只进private诊断。跨脱敏/切片映射保留。 |
| 9 旧版清理 | 当前活动新版稳定7天后，回收全部退役专业，包括已移出专业；完整manifest及每片SHA证明、停用及删除前后读回、持久意图。测试库零等待实际删除1文档，正式默认未缩短。 |
| 10 指标 | status/UI：1h/24h成功数，近24h阶段平均耗时/分类失败数，积压及parked/code_error名单；程序错误、积压满1h无成功醒目提示。 |
| 11 溯源 | 新调用记录model/provider/提示词及文件SHA/contract/reader版本、秒数及usage，进入processing/manifest；历史未知字段不捏造。 |
| 12 测试 | 每次pytest新UUID临时目录，不清理旧运行；普通环境、新venv和独立审查均103 passed。 |
| 13 耗时/JSON | 同会话最多2次纯语法补正，不增修订或业务返修数；历史zstd会话仅提取usage，10次原正式稿调用均有计数。 |

## 架构完成情况

- 调度器仅排队、名额、租约；SQLite管来源/修订/远端事实；LangGraph检查点管单篇流程。stage是调度副本，step_seq对齐已移除。
- 第三轮assets→redact→chunk，输入及输出散列一致才复用；切片故障不重看图、不重新脱敏。
- Send按专业入库、精确读回及独立回执；只重试失败专业。旧all回执仅用于历史兼容。
- 服务器停用文档无法检索，因此分支先隐身写入及精确读回，全部确认后在激活阶段验证分类检索，再停用旧版；未全部成功不报告完成。
- 分支、普通RAG节点和维护共享全局信号量。激活和GC同序加锁，避免误删当前版或并发死锁。
- LangGraph 1.2.15的Send异常处理后重抛，仅在新处理token、异常类型、完整结果及汇合pending吻合时接纳已持久处理证明；其他异常正常抛出。

## 六项验收全部满足

1. 正式稿r008完整入库：3专业各43文本+2图片，原件SHA未变，临时work清理。
2. 问题稿真实DeepSeek自动返修入库：合成教学稿r004完成；含人为破坏章节引用及模型漏写名单，共3次自动返修。后续已增加对LLM最终名单的机械同步，避免重复处理接口差异。
3. ingest真实进程退出：服务器写成功、本地未确认时os._exit(86)，重启继续检查点，13条文本无重复。
4. 同一合成原稿更新：新任务激活，旧版status=0，真实retrieval返回空。
5. 代码错误注入：相关篇code_error，不无限重试，不调用DeepSeek。
6. 全新runtime/verification-venv安装后103 passed。

证据：runtime/integration-live/restart-version-report.json、gc-report.json，及docs/acceptance.md。
首次中断测试替身曾错误传递Content-Type，产生一个隔离code_error；保留其检查点诊断，修正测试替身后，以新的合成源版本完成真实中断验收。正式原稿不受影响。
