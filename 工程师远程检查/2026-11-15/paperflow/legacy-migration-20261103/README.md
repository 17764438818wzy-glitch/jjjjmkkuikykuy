# 正式旧库迁移与文章资料补接

已授权范围：仅迁移正式专业资料；SELFTEST、Prefect 测试库及其他未纳入正式专业配置的库保留原处。旧 RAGFlow/MySQL/Elasticsearch/MinIO 只读，不删除源文件或旧库，不启动论文处理任务。

复用现有 `paperflow_qdrant.Library`、官方 Haystack/Qdrant 集成、文章仓库 `Catalog`、`Files`、`RevisionSync`，不另建检索服务或调度器。网关仍为 8097，文章仓库为 <http://192.168.1.120:8098/>。本次不包含 LibreChat 仿写会话验收。

服务器目录 `/opt/paperflow-legacy-migration-20261103`：

- `input/`：一次核验的旧库清单、封包、图片及来源对照，用于可重复迁移。不是在线增量抓取器。
- `state/migration.sqlite`：每个来源文件夹的一次最终发布结果。`superseded` 是已由更明确的来源或附件修正替代的记录；`recovered` 是旧失败已有完成替代，并非再次上传成功的另一种说法。
- `state/final-coverage.json`：正式源文档覆盖、专业归属、源切片计数及目标留存覆盖的核验。
- `state/attachment-endpoint-validation.json`：通过部署后的网关实际下载原始附件并校验 SHA256。
- `state/source-reconciliation.json`：仅以完全一致的源文档集合核对生成的未知来源副本。原行保留、停止默认展示，人工修订不覆盖。
- `.migration.env`：服务器私有配置，权限 600；不复制到源码，不输出凭据。

已保存向量只有文本完全一致、维度/数值有效时复用。原件下载 URL 或文档引用改写后，只重算变动文本。旧模型与现模型同属 1024 维 BGE 系列不等于逐位相同，不用模型名称替代文本校验。

原文、画像、章节和图片由已验证后处理包或已提交的新库全文补齐。缺完整封包的旧条目保留可读取切片，`legacy_original_reconstructed` 明示全文由片段重建；旧库零切片保持零切片。未知原件或文件夹不补造。

附件只读取结构化元数据或附件说明内明确的原件编号、SHA256，不按论文标题猜归属。先复制并校验原始对象，再更新下载 URL。主文档引用只接受 `旧 dataset + 旧 document id` 唯一对应、已完成且可读的目标；归属未明的独立附件不充当论文列表项。已有正常论文只补文件，不替换正文、画像或来源指针。

阶段调用：`deploy.py build` 构建自有迁移镜像，`acceptance` 为独立集合/数据库 schema 的真实历史样本，`attachment-first` 为一个附件修复样本，`attachments` 执行已圈定附件修正，`migrate` 处理正式正文（跳过附件卡片），`references` 只补已核对唯一引用的附件。启动前检查自有 runner 已停止；脚本不会替换正在运行的 runner。

`reconcile_sources.py`、`final_audit.py`、`verify_endpoints.py`、`verify_attachments.py` 是服务器维护脚本，通过既有 SSH 维护入口执行。`reference-alias-audit.json` 经只读核对后供 `references` 使用。

目标覆盖数包括留存的完整历史发布和已停用版本，不能等同于当前检索可见数量。目录条目、专业发布文档、源切片、合并后的目标向量及原始文件是不同计数。NAS 下载是否可用单独验收，不能从迁移完成状态推断全部原件齐全。

本地相关测试使用同级 `qdrant-migration`、`article-repository` 及隔离 PostgreSQL 56164，绝不指向生产数据库。测试覆盖精确向量复用/拒绝、专业边界、全文和图片引用、附件 URL 改写、零切片、文件合并、人工修订以及中断后续跑。源码不包含环境凭据或大体积输入副本。
