# 100 篇论文迁移样例

按文章 ID 共 100 篇，覆盖 6 类研究类型。包含 104 个原文/附件、578 个内容对象、110 个版本文件夹、14,405 个原始向量点，以及对应的目录、专业关联、画像、评分与操作记录。

- [100 篇索引与选择规则](selected-articles.json)
- [文件完整性清单](INTEGRITY.json)
- [离线验收结果](offline-verification.json)
- [Release 附件地址、大小与 SHA256](assets.json)
- [SHA256SUMS](SHA256SUMS)
- [Linux 部署与恢复步骤](../07_Linux服务器迁移包/Linux服务器部署交接文档.md)

原文、对象及向量数据在同一 GitHub 仓库的 [Linux 迁移 Release](https://github.com/17764438818wzy-glitch/jjjjmkkuikykuy/releases/tag/linux-server-migration-20261114) 中。`git clone` 下载代码和清单后，还需下载 Release 附件；大数据没有作为普通 Git blob 提交。

| 附件 | 用途 |
| --- | --- |
| `paperflow-sample100-20261114.tar.gz` | 100 篇全部样例数据，解压后含 catalog.json、qdrant.jsonl.gz、nas/、objects/、schema.sql 和校验清单 |
| `linux-embedding-runtime.tar.gz` | 源服务器 Linux x86_64 Ollama 0.34.0 运行时与原许可文件 |
| `linux-embedding-model.tar.gz` | bge-m3 manifest、模型 blob 及模型许可文件 |

上表是合并后的归档名称。Release 实际附件按最大 16MiB 分卷，共 191 份（样例 25、模型 63、运行时 103），完整归档及每份分卷的 hash 都在清单中。使用 `07_Linux服务器迁移包/scripts/download_assets.py` 自动下载、逐份校验并合并，再核对完整归档 SHA256；中断后可复用已校验分卷，无需手动拼接。

解压和恢复前先运行 `sample_tools.py`；恢复脚本只接受新的空目标实例。历史 active=false、原 ID 和评分操作状态均保留，恢复不触发重新处理或评分。
