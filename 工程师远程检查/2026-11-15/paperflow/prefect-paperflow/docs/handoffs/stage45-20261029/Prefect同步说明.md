# Prefect 文档同步记录

版本：stage45-20261029-v1。已同步至局域网 Prefect 3.9.0 的 Artifact 文档记录；两份正文均已通过实际 artifact ID 读回，并与本地文档内容哈希核对一致。

| 文档 | Artifact key | Artifact ID |
|---|---|---|
| 统一交付汇总及代码验收清单 | `paperflow-stage45-delivery` | `50c1b48e-a829-4217-a8b0-5d34db5ce87e` |
| 前端与 Prefect 3 接线设计 | `paperflow-stage45-frontend-prefect-design` | `e18a245a-8e24-4b46-9b52-0913c31c4182` |

服务地址：`http://192.168.1.120:4200`。可在 Artifacts 中按以上 key 查找。

精确读回接口：

- [交付汇总的 API 记录](http://192.168.1.120:4200/api/artifacts/50c1b48e-a829-4217-a8b0-5d34db5ce87e)
- [接线设计的 API 记录](http://192.168.1.120:4200/api/artifacts/e18a245a-8e24-4b46-9b52-0913c31c4182)

对应原部署是“论文五步流程”的 `五步接入-手动运行`，部署 ID `3c968368-58fc-40a0-9966-0ba2fde8c92c`。文档元数据保留该部署引用；没有把文档伪装成一次论文运行，也没有修改部署说明、执行器或并发额度。

完整同步回执为 `evidence/prefect-document-sync.json`。本次共两份文档，每份内容只有一个匹配版本。首次读取最新版本时遇到集合 ID 与 artifact ID 的差异，已通过原生 latest/filter 认回正确版本，未重复创建汇总文档。

交付压缩包保留前端样例、原接口和模板、候选归属与验收来源。代码合入与生产启用属于下一次接线工作；本次真实论文启动数为 0。
