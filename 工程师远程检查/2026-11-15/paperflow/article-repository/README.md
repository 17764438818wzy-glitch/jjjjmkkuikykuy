文章仓库已部署在 http://192.168.1.120:8098/ 。现有 8796 处理页面顶部提供“文章仓库”入口。

目录使用独立 PostgreSQL，原文和附件通过服务器直接读取 NAS；检索修订复用现有 Qdrant/Haystack 网关。目录接入只读取已保存的旧资料和新链路完成证据，不控制处理任务。

一来源文件夹对应一条文章。支持标题关键词与精确专业组合查询、分页、八项基础画像、单文件和单篇 ZIP 下载、人工修订及 Qdrant 同步。相同正文的不同文件夹分别保留；人工修改优先于后续来源刷新；原文和附件只读。未知、缺失和部分可用分别呈现。未迁入 Qdrant 的旧资料仅保存修改，后续迁移时使用。

初始目录接入 5,600 个文件夹；本次另外迁入正式旧库并补接资料，最新覆盖与未找到原件的数量见 validation/legacy-migration-20261103.md。正式库迁移程序在同级 legacy-migration-20261103。旧资料初始适配代码在 export_existing.py，导出的部署输入为 legacy-records.json；重新导出前应核实该脚本引用的本地目录和匹配索引。

服务器部署目录为 /opt/paperflow-article-repository，容器为 paperflow-article-repository 和 paperflow-article-postgres。独立数据库卷为 paperflow-article-repository_article_pgdata，PostgreSQL 不发布宿主机端口。现有 Qdrant Docker 网络和网关基础镜像被复用。

服务器 .env 保持权限 600，其中包含 ARTICLE_POSTGRES_PASSWORD、QDRANT_API_KEY、ARTICLE_NAS_USER、ARTICLE_NAS_PASSWORD。凭据不包含在本源码中。当前配置和数据卷保留在服务器，更新时使用原 .env。QDRANT_URL、PREFECT_API_URL、ARTICLE_NAS_ROOT 在 compose.yaml 中配置；EMBEDDING_URL 可选。ARTICLE_IMPORT_ENABLED=false 可用于隔离实例，ARTICLE_DATABASE_SCHEMA 和 QDRANT_COLLECTION 应同时指向隔离资源。

更新时，将 article_repository、frontend/dist、Dockerfile、requirements.txt、compose.yaml、majors.json 和 legacy-records.json 放到服务器部署目录后执行：

```sh
cd /opt/paperflow-article-repository
docker compose build repository
docker compose up -d
```

构建依赖当前服务器已有的 paperflow-qdrant-20261103-gateway 基础镜像。Python 新增依赖固定在 requirements.txt，网关依赖沿用该镜像。本服务启动后每 60 秒核对新链路完成资料，独立检查 NAS 文件可用性；前端查询直接使用持久目录。

前端复用了 F:/论文预处理系统/看板/frontend 的 Vue/Ant Design 依赖和既有组件结构。此源码副本不包含 node_modules。当前电脑需要重新构建时，在 frontend 创建指向该既有 node_modules 的 Junction，然后执行 npm run build；vite.config.js 中的 Ant Design 路径也指向既有看板。服务器使用已构建的 dist，不依赖处理电脑。

API：

| 请求 | 用途 |
| --- | --- |
| GET /api/library/health | 目录存活状态和条目数量 |
| GET /api/library/majors | 精确专业选项 |
| GET /api/library/articles?keyword=&major=&page=1&page_size=30 | 标题与专业组合搜索 |
| GET /api/library/articles/{id} | 详情与当前文件可用性 |
| PATCH /api/library/articles/{id} | 携带 revision 与 changes 保存人工修订并同步 |
| POST /api/library/articles/{id}/sync | 重试该文章的显式修订同步 |
| GET /api/library/articles/{id}/files/{file_id} | 原文或附件 |
| GET /api/library/articles/{id}/bundle | 单篇整包，部分可用时附完整性说明 |

局域网按已确认需求不增加登录。没有新增上传、隐藏、删除或工作流控制接口。

测试在 tests 中。完整测试依赖 PostgreSQL、pytest，以及现有 paperflow_qdrant 网关模块；本次使用工作目录同级的 qdrant-migration 源码和 qdrant-env。PostgreSQL 测试会创建、删除自己的 article_test_* schema，并校验连接属于名称为 pg-classification-isolated-20261103 的隔离数据目录，避免误用业务库。验收结果见 validation 目录；性能值是本轮实测值，不代表任意网络或任意并发下的保证。
