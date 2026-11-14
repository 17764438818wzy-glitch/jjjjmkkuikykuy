# Linux 服务器部署交接文档

这份包用于在新的 Linux 电脑还原论文项目的服务器端，并查看、下载和检索选定的 100 篇论文。Windows 论文处理端仍使用仓库原有的 `01_程序文件`、`03_配置与启动` 和《新电脑部署交接文档.md》。

采集日期：2026-11-14。交付仓库：[17764438818wzy-glitch/jjjjmkkuikykuy](https://github.com/17764438818wzy-glitch/jjjjmkkuikykuy)。大文件：[Linux 迁移 Release](https://github.com/17764438818wzy-glitch/jjjjmkkuikykuy/releases/tag/linux-server-migration-20261114)。

## 交付内容及边界

| 目录 | 用途 |
| --- | --- |
| `services/gateway` | 正式 8097 网关源码、Haystack/Qdrant 集成、依赖锁、原始及迁移用 Dockerfile |
| `services/article-repository` | 正式 8098 文章仓库、PG schema、画像/评分/分类接口、前端源码及已构建 dist |
| `services/prefect` | 原 Prefect Compose、README 和 demo 源码；新部署配置不启用 demo |
| `services/embedding` | 实际使用的 11436 systemd 原件、模型 manifest 和运行时文件清单 |
| `deployment` | 统一的新机 Compose、空配置模板；生成的真实 `.env` 不进入 Git |
| `scripts` | 新机配置、附件下载校验、离线检查、空实例恢复和目标读回脚本 |
| `evidence` | 只读采集结果、依赖版本、源码 hash 及与运行容器比较结果 |
| `服务器硬件型号与系统配置.md` | 实际 CPU、主板、内存、磁盘、GPU、网卡和系统版本 |
| 仓库 `08_100篇样例数据` | 100 篇索引、选择规则、完整文件校验清单、大文件下载地址与 hash |

宿主部署源码与运行容器中的业务源码、前端 dist 已逐文件比较，采集时没有差异。原宿主 Compose 留作来源记录；**新机操作只使用本目录 `deployment/compose.yaml`**。

迁移用 Dockerfile 复用同一业务代码，补充显式网关父镜像名，使用运行容器实测的完整 pip 版本锁；文章镜像的历史目录占位文件为 `[]`。未迁移全部历史目录、数据库、NAS、Prefect 生产任务、模型路由账户、面板账户或真实密钥。旧 8088 RAGFlow 不属于此次正在运行的 8097/8098 核心后端。

## 100 篇数据的内容

按 article ID 计数，共 100 篇：调查/案例 17、工程/系统 17、文本/规范 17、实验/测试 17、理论/建模 16、创作/设计 16。按固定种子与 ID 排序选择，要求原件清单完整、NAS 当前文件存在且大小一致、单篇原件总量不超过 20MiB，选择时没有 claimed/sending 操作。初选有 2 篇 NAS 原件缺失，已按同样规则替换；被排除 ID 和原因在 `selected-articles.json` 中。

| 内容 | 数量 |
| --- | --- |
| 文章 | 100 |
| NAS 原文及附件 | 104 个文件，166,752,363 字节 |
| 内容对象 | 578 个文件，113,988,298 字节 |
| Qdrant folders | 110，包含版本关联 |
| Qdrant points | 14,405（14,028 chunk、157 manifest、110 binding、110 complete） |
| 专业关联 | 275 |
| 已保存评分 | 8 |
| 已观察评分画像 | 78 |
| 发现记录 | 20 |
| 原 claim 记录 | 14，保留 published / terminal_review 及原操作身份 |

JSONB 原文、原始 ID、revision、request/operation/run/version ID、状态、active 标记、原始向量及历史版本均保留。导出没有调用评分、重派发、`rating-input` 或会写画像缓存的文章详情接口。全局 `source_status` 仅作来源证据，不作为 100 篇目标库统计恢复；Prefect 生产任务库不恢复。

PostgreSQL 使用只读可重复读事务导出，Qdrant 采用原始 point/payload/vector 导出并做第二次 payload 读取比较。这不是跨 PG、Qdrant、NAS 的全局原子快照；导出后的选定数据库记录会另行比对，检查结果见验收记录。

## 1. 准备新机器

使用 x86_64 / linux/amd64 的 Linux，本次源机为 Ubuntu 24.04.5 LTS。ARM 机器不能直接复用这份 Ollama 二进制和固定镜像 digest。

先装 Git、Python 3.12 或更高版本，以及 Docker Engine、Compose 插件。Docker 的 Ubuntu 安装步骤以[官方安装文档](https://docs.docker.com/engine/install/ubuntu/)为准。确认当前账号能执行 `docker info`；没有 Docker 权限时使用管理员按目标机器的管理方式配置。

```bash
git --version
python3 --version
docker info
docker compose version
```

按下载包、解压、镜像构建和新数据库估算，建议预留至少 30GB 可用空间。Docker 镜像及 Python wheel 不包含在这次 Release 中，首次构建需要能访问相应镜像仓库和 PyPI。源机依赖精确版本见 `evidence/linux-dependency-locks.json`。

## 2. 克隆并下载附件

下列命令在新 Linux 机器执行；仓库路径可以自行选择。

```bash
git clone https://github.com/17764438818wzy-glitch/jjjjmkkuikykuy.git
cd jjjjmkkuikykuy
TASK_REPO="$PWD"
python3 "$TASK_REPO/07_Linux服务器迁移包/scripts/download_assets.py" --output "$TASK_REPO/08_100篇样例数据/downloads"
mkdir "$TASK_REPO/08_100篇样例数据/data"
tar -xzf "$TASK_REPO/08_100篇样例数据/downloads/paperflow-sample100-20261114.tar.gz" -C "$TASK_REPO/08_100篇样例数据/data"
python3 "$TASK_REPO/07_Linux服务器迁移包/scripts/sample_tools.py" "$TASK_REPO/08_100篇样例数据/data"
```

下载器会核对每个附件的字节数与 SHA256，数据离线检查会核对全部文件、100 篇身份、Qdrant 点 ID 映射、manifest/分块闭包及完成标记。预期输出 `offline_integrity: passed`、`articles: 100`、`points: 14405`、`folders: 110`。

只想先看目录和原件时，下载命令可追加 `--sample-only`，暂不安装嵌入模型。断开的 `.download` 文件不会自动覆盖；保留到其他位置后再执行下载。网络需要代理时，在新机器自行配置 `HTTPS_PROXY`，本包没有保存旧电脑的代理账户。

## 3. 创建新机配置并构建

```bash
python3 "$TASK_REPO/07_Linux服务器迁移包/scripts/configure.py" --sample-dir "$TASK_REPO/08_100篇样例数据/data"
cd "$TASK_REPO/07_Linux服务器迁移包/deployment"
docker compose config --quiet
docker compose build gateway
docker compose build repository
```

必须先构建 gateway，再构建 repository。配置脚本会生成新随机网关密钥、Qdrant 密钥和两个数据库口令，只保存在新机的 `deployment/.env`，权限设为 600；已存在 `.env` 时拒绝覆盖。密钥可由维护者在新机本地读取，供处理端或客户端配置。

默认仅绑定本机 `127.0.0.1`，便于先验收。如需从同一局域网的 Windows 电脑访问，首次生成配置时改用下面的形式，将示例地址换成新 Linux 机器的实际 IP：

```bash
python3 "$TASK_REPO/07_Linux服务器迁移包/scripts/configure.py" --sample-dir "$TASK_REPO/08_100篇样例数据/data" --public-host 192.168.1.200 --bind-host 192.168.1.200
```

上面两个 `configure.py` 命令任选一个执行。实际 IP 不是固定的 192.168.1.200；若 `.env` 已生成，在本地编辑其中的 `PUBLIC_HOST`、`BIND_HOST` 即可。

| 服务 | 默认新端口 | 说明 |
| --- | --- | --- |
| 网关 | 18097 | 需新生成的 Bearer API key |
| 文章仓库 | 18098 | 原项目为局域网应用，无登录；绑定供访问的实际局域网地址 |
| Prefect UI/API | 14200 | API 地址以 `/api` 结尾 |
| Qdrant | 本机 16333 | 默认只绑定 127.0.0.1，容器内部用 qdrant:6333 |
| 嵌入 | Docker 主机网关 IP:11436 | 不使用旧 11434 unit |

`ARTICLE_IMPORT_ENABLED=false` 为默认值，避免启动时从历史目录或 Prefect 自动登记其他论文。`ARTICLE_NAS_ROOT=/sample/nas` 使用随包的本地原件；NAS 用户、口令为空但环境变量必须存在。

## 4. 只恢复到空的新实例

**先只启动 qdrant 和 article-postgres；此时不要启动 gateway、repository 或整个 compose。** 提前启动业务容器会创建 collection 或专业数据，恢复脚本会拒绝覆盖。

```bash
cd "$TASK_REPO/07_Linux服务器迁移包/deployment"
docker compose up -d --wait qdrant article-postgres
docker compose run --rm --no-deps -T repository python /migration-tools/restore_sample.py --apply-to-empty-target
docker compose run --rm --no-deps -T repository python /migration-tools/verify_sample.py
```

恢复脚本仅允许 Compose 内部的 `qdrant` / `article-postgres` 服务地址，要求自动导入关闭、数据库各业务表为空、目标 collection 不存在、目标对象目录为空。它直接恢复原始向量，不调用嵌入模型或 `Library.ingest`，然后恢复显式数据库列并设置发现记录序列。NAS 原件保持导出时的相对路径，只读挂载；内容对象复制到新实例 state 的可写对象目录。

验证脚本将 PG 每张表所有导出列、Qdrant 全部点的 ID/payload、向量维度与数值、目标对象文件逐项读回。Cosine 向量重新存入 Qdrant 时可能有浮点归一化尾差，阈值为最大绝对误差 2e-6；归档中的原始向量数值保持导出原件。预期输出 `target_verification: passed`。

这是跨 PG、Qdrant 和对象目录的多步恢复，不具备全局回滚。若中断或失败，保留日志和当前目录；不要直接重跑或删除不明目录。使用新的空 `STATE_ROOT`（并停止此前的迁移测试容器）重新建立一份独立实例，再恢复。脚本不包含清库、删 collection 或重试业务任务功能。

## 5. 启动目录、原件与 Prefect

第 4 步读回通过之后执行：

```bash
docker compose up -d gateway repository prefect-postgres prefect
docker compose ps
curl --fail http://127.0.0.1:18097/health
curl --fail http://127.0.0.1:14200/api/health
```

如果第 3 步绑定的是局域网 IP，将上面的 `127.0.0.1` 换成该 IP。在浏览器打开 `http://新机IP:18098` 查看 100 篇目录并下载原件，打开 `http://新机IP:14200` 查看新的空 Prefect 服务。没有恢复任何生产任务，不会自动启动 demo 或旧业务。

目录、已有元数据和原件下载可先验收；语义查询还需要下一步的嵌入服务。文章详情读取可能观察并保存画像缓存，属于新实例运行行为；数据逐列验收应在打开详情页之前执行。

## 6. 安装本次携带的 Linux 嵌入服务

Release 包含源服务器实际的 Ollama 0.34.0 Linux x86_64 runtime 及 bge-m3 模型。模型为 1024 维，manifest hash 和 blob digest 在 `services/embedding/model-runtime-inventory.json`。模型和原始许可文件随附件保留。

先确认新机 11436 没被其他服务占用；有 GPU 时先确认目标 Linux 的 NVIDIA 驱动工作正常。此次仅打包运行时和模型，没有安装新机 GPU 驱动，也没有执行新机性能测试。

```bash
ss -ltn | grep ':11436'
nvidia-smi
sudo mkdir /opt/paperflow-migration-embedding
sudo mkdir /opt/paperflow-migration-embedding/runtime
sudo mkdir /opt/paperflow-migration-embedding/models
sudo tar -xzf "$TASK_REPO/08_100篇样例数据/downloads/linux-embedding-runtime.tar.gz" -C /opt/paperflow-migration-embedding/runtime
sudo tar -xzf "$TASK_REPO/08_100篇样例数据/downloads/linux-embedding-model.tar.gz" -C /opt/paperflow-migration-embedding/models
sudo chown -R "$(id -un):$(id -gn)" /opt/paperflow-migration-embedding
python3 "$TASK_REPO/07_Linux服务器迁移包/scripts/configure_embedding.py" --user "$(id -un)" --group "$(id -gn)"
sudo install -m 0644 "$TASK_REPO/07_Linux服务器迁移包/deployment/paperflow-migration-embedding.service" /etc/systemd/system/paperflow-migration-embedding.service
sudo systemctl daemon-reload
sudo systemctl enable --now paperflow-migration-embedding.service
```

`ss` 没有匹配输出表示当时未观察到监听；若有其他服务，先核对归属，不覆盖。`mkdir` 如提示目录已存在，先核对已有文件，避免解包覆盖。`nvidia-smi` 仅用于确认 GPU 驱动；没有 NVIDIA GPU 时不把该命令失败当作数据恢复失败。

unit 生成器读取新机 `docker0` IPv4，仅绑定该网关地址的 11436；避免照搬旧机器地址和用户名。若主机使用自定义 Docker host-gateway，使用 `--bridge-ip 实际网关IP`，并确保 Compose 的 `host.docker.internal` 与它一致。

只读检查模型是否在位，在 deployment 目录运行：

```bash
docker compose run --rm --no-deps -T repository python -c "import httpx; u='http://host.docker.internal:11436'; print(httpx.get(u+'/api/version',trust_env=False).json()); print(httpx.get(u+'/api/tags',trust_env=False).json())"
```

模型列表应有 `bge-m3:latest`。需要验证语义检索时，可由维护者在新机启动后输入一条查询；这会在新机计算 query embedding。现有 100 篇分块的向量已经恢复，不需要重新对 100 篇论文跑处理流程。源服务器上本次没有执行模型检索或推理。

## 7. 接入 Windows 处理端及后续业务验证

先按仓库根目录的 Windows《新电脑部署交接文档.md》安装处理端。现有 Windows 迁移启动脚本明确限制使用本机 Prefect，并要求本包本机 Prefect 进程记录；**不能仅把 URL 改为远端 Linux Prefect**。这份 Linux 包没有修改 Windows 的这个保护机制。

服务器端就绪后，通过 Windows 的 `03_配置与启动/新电脑配置.json` 修改知识库入口，再按既有初始化、离线检查及凭据保存顺序配置，避免只改派生项目文件后被下次初始化覆盖：

| 配置项 | 值或要求 |
| --- | --- |
| 项目中的 `rag_backend` | 沿用迁移模板的 `qdrant` |
| `qdrant_gateway_url` | `http://新机IP:18097` |
| Windows `prefect_api_url` | 保持 `http://127.0.0.1:14200/api`，由原 Windows 启动脚本管理 |
| 知识库 API key | 新机 `.env` 中的 `PAPERFLOW_RAG_API_KEY`，由维护者在本地填写 |
| 模型路由与处理凭据 | 在 Windows 按已有交接流程单独配置，不复用模板空值；现有校验仍绑定旧共享 8081 路由地址 |

8081 模型路由及 8789 账户服务属于外部接口，没有打包账户数据库。只还原 100 篇样例时不依赖这些账户；真正处理新论文时需要处理端自己能访问相应路由和服务。新 Linux 网关有独立 state 后，按 Windows 文档核对测试网关隔离配置，再由操作人手动选择一篇新论文进行处理和网关入库验收。

正式接入 NAS 原件时，需要在新 Linux 的 `.env` 中配置真实 `ARTICLE_NAS_ROOT`、`ARTICLE_NAS_USER`、`ARTICLE_NAS_PASSWORD`，确保 Windows 处理端归档目录与仓库相对路径一致。样例模式的 `/sample/nas` 只含这 100 篇，不会自动包含后续新论文的原件。新机可以继续使用同一局域网 NAS，或另行复制到统一的新存储根目录。

Linux 上这份 Prefect 是独立空服务，当前 Windows 本地 Prefect 的新任务不会自动出现在其中。若以后要求 Linux 统一编排或文章仓库自动登记 Windows 的新处理结果，需要另行适配 Windows 连接限制、局域网可达的 Prefect API、deployment/worker 注册及文章仓库的 `PREFECT_API_URL`，完成独立接线验收。不能仅将 `ARTICLE_IMPORT_ENABLED` 改成 true 就认为跨机器登记已经接通。

确认实际任务来源与 NAS 都匹配之后，才在新机 `.env` 手动将 `ARTICLE_IMPORT_ENABLED` 改为 `true` 并重新创建该新机 repository 容器；这会开启 bootstrap/refresh/原件状态观察。原任务、原 request/operation/claim 身份均不重放。

本次交付完成的是源文件和 100 篇数据迁移资料，不等于已在新 Linux 上跑通新论文全流程。新机端的实际构建、恢复、健康检查及论文验收结果需按以上顺序记录。

## 停止和保留

在新机 deployment 目录执行 `docker compose stop` 停止这组迁移服务；保留 `STATE_ROOT`、样例 data、`.env` 和验收日志。嵌入服务独立管理，需停止时执行 `sudo systemctl stop paperflow-migration-embedding.service`。不要对源服务器、旧库或其他共享服务执行这些新机命令。

## 已知限制与版本说明

- `services/article-repository/frontend/dist` 是当前生产容器使用的构建结果，可直接打进镜像。原前端开发配置含 Windows 本地路径，`package.json` 也不是独立 Linux 重建所需的完整工作区清单；本次未重构前端工具链，不能声称随包源码可直接 npm 重建。
- 默认 state 路径、Compose 名称与端口独立，不沿用原服务器容器名、旧外部网络和生产数据卷。
- 某些历史证据包含旧 IP 的对象 URL、源任务和 NAS 来源字段，均保留原值，以免改变身份和指纹。下载对象时使用新网关基址加原 digest；需要旧链接长期兼容时由新机另设地址映射，不能修改归档证据冒充原件。
- 归档中的全库 `points_count` 等是源库采集快照，不是选出的 100 篇数量；此次样例以 14,405 点为准。
- 已完成的打包检查与未执行的新机部署分别记录于《验收记录.md》。
