# Prefect 独立预览

此目录用于查看 Prefect 原生界面和模拟任务。未接入正式论文处理。

- 控制服务地址：<http://192.168.1.120:4200>
- 服务器目录：`/home/wzy/prefect-preview`
- Docker Compose 项目：`paperflow-prefect-preview`
- Prefect：官方 `3.9.0-python3.12` 镜像，compose 中固定镜像摘要。
- 数据库：独立 PostgreSQL 容器、账号及数据卷，不使用现有业务数据库。
- 数据库密码只保存在服务器 `.env`，权限为 `0600`。

## 演示内容

1. `Paperflow Five Steps DEMO / five-step-demo`：三篇合成论文展示接收、正文准备、正文画像、独立附件识别、模拟入库和模拟归档。无原稿读取。
2. `LLM Concurrency DEMO 100 plus 20 / body100-attachments20`：默认正文 100、附件 20 个模拟等待任务。**没有真实模型请求，不是正式并发性能验收。**

在 Deployments 页面可手动运行这两个演示。在 Runs 打开具体运行，可查看任务关系、Logs 日志和 Artifacts 演示结果；在 Concurrency 查看独立演示额度。

演示仅调用本预览 Prefect API，并写入新建的 `demo-output` 目录和预览数据卷。没有访问 LLM 路由、RAG、NAS 或真实论文资料库。没有自动运行计划。

## 已验证结果

2026-10-28 已通过浏览器页面、Prefect API、模拟结果文件和 Docker 容器状态核对：

- Prefect 服务版本 `3.9.0`，健康接口返回 `true`。
- [五步流程演示](http://192.168.1.120:4200/v2/runs/flow-run/aeca71c4-9d9a-4781-aec3-f80fef7a5f72)：3 篇合成论文，24/24 个步骤完成。
- [正文 100＋附件 20 演示](http://192.168.1.120:4200/v2/runs/flow-run/16134cac-0812-4a48-893b-1c491a806e18)：120/120 个模拟任务完成。按各任务开始/结束时间重新计算，同时等待峰值为正文 100、附件 20、合计 120；运行中的独立额度读回也一致。
- 新建三个容器正常运行，PostgreSQL 和 Prefect 服务健康。部署前记录的 56 个容器 ID 在核验时仍全部运行。
- 完整核验记录见同目录 `deployment-receipt.json`，两次运行的模拟结果也已保存。

这些结果只验证独立演示环境和模拟调度，不代表真实论文、真实 LLM、RAG 或 NAS 的生产验收。

## 服务器管理

下列命令均在服务器目录 `/home/wzy/prefect-preview` 内运行。

启动或恢复预览：

```sh
sudo docker compose --profile demo up -d
```

查看状态：

```sh
sudo docker compose --profile demo ps
```

停止预览，保留数据：

```sh
sudo docker compose --profile demo stop
```

查看服务日志：

```sh
sudo docker compose logs --tail=100 server demo-runner
```

不要将演示完成状态、模拟等待峰值当作真实论文处理或真实 LLM 并发通过。
