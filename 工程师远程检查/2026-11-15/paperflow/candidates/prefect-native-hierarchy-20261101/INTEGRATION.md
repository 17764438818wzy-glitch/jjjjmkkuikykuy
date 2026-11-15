# Prefect 原生文件夹阶段归属候选

当前线上 RAG/NAS TaskRun 的 `flow_run_id` 为 None；程序通过同一控制库及原工单身份关联。此候选只调整后续新建的 RAG/NAS 阶段：创建 TaskRun 时令其 `flow_run_id` 等于原文件夹 FlowRun ID。历史 detached TaskRun 继续按原身份读取，页面同时识别历史与新阶段，不把历史记录改写成原生子任务。

- `group_prefect.py`：新提交绑定原文件夹，领取和完成检查兼容同一文件夹的原生阶段与既有 detached 历史。
- `ticket_positions.py`：允许实际父级是该原文件夹，仍读取 detached 历史，拒绝挂在别的文件夹下的阶段。
- `run_tests.py`：36 项相关检查通过，包含一个真实临时 Prefect 服务中、父文件夹 Paused 时新阶段创建、运行和推进至等待 NAS 的测试。
- `../postprocess-rag-unblock-20261101/prefect-parent-probe.py`：临时 Prefect 服务中另测过 Paused 和 Failed 的合成父文件夹，两种状态下挂载的合成 TaskRun 都完成；这是可行性测试，不是用户论文验证。

`full-stages/` 进一步准备了 LLM 和后处理阶段的同一文件夹归属，并合并删除入口对 RAG/NAS 的收尾候选。历史 detached 阶段仍可读取；新阶段只能挂在原文件夹上。25 项合成检查覆盖三个阶段的正确父级、历史归属、错误父级、LLM 页面统计、服务器回执范围和删除收尾；另有临时 Prefect 中实际创建后处理 TaskRun 的检查。删除候选不新增锁。

临时 Prefect 中，挂在 Paused 或 Failed 文件夹下的 deferred 阶段均完成，且阶段的测试日志能按 TaskRun ID 从 Prefect 读到。线上现有 detached 任务的部分普通完成日志没有进入 Prefect；这项隔离结果不能证明主线已修复或历史日志已补齐。

候选均尚未部署。原总工单和文件夹工单目前本身仍不是 Prefect 原生父子，因此候选尚不代表全链路原生层级已完成。正在运行的阶段不得强制重启以装入候选。后续切换须与同一轮 RAG 精确目标查询、状态回读优化及失败工单接续方案合并验证，并更新原页面源码清单后再装载。
