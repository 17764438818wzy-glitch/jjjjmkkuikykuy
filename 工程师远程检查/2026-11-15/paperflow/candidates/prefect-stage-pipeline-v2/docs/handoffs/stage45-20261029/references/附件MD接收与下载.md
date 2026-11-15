# 第四状态：附件 MD 接收与下载

交付单元是一个原附件和一份 `attachment.md`。MD 保留上游的身份字段、类型、简单说明、读取依据及包内目录；第四状态核验后存储，不再调用模型补写或拼接 `description.json`。内部冻结联验清单可以引用历史证据文件，接收器只校验这些证据的哈希。

## 本轮入口范围

`attachment_ingest_cli.py` 是显式单附件联验入口。它未接入正在运行的批次，不改变并发配置，不重启服务，不发布正文，也不操作 NAS。原来的第四状态 20 路候选仍独立保留。本入口加入当前 `paperflow-connected-rag` 全局额度，要求本机配置与远端一致。

- `check`：本地校验冻结清单、H2.1 回执、原件、MD、画像路由及历史；不读取凭据，不发网络请求。
- `ingest`：复用原生 Files、link-to-datasets、MD 文档、离线切片预算和检索 API。原件仅存到最高专业原件目录，并只链接该专业资料库；原件不解析、不启用、零切片。MD 可检索，其他专业只存引用同一原件 ID 的说明。
- `reconcile`：只读回和重新检索，不创建或修改远端对象。POST retrieval 属于查询。
- `download`：按已核验回执的唯一 file_id 下载，核验字节数及 SHA256，再写入新本地路径。已有不同文件不覆盖，原件和 MD 不覆盖。

每次执行指定一个新的 `--report` 路径，同时保留 `.audit.json` 调用审计。审计没有凭据和附件正文。入库回执以论文和附件身份固定，持久化每一个写入意图，原件、文档和切片超时后只查原对象。未知请求未找到时不会补发。旧 MD 版本未完成时，新版不能绕过旧回执；应先使用原冻结清单回查。

已完成的入库再次执行仍会下载读回、验证文档元数据/切片内容与可用性，并刷新检索证据。成功复用应产生零远端写入。

## 命令

先用 `python attachment_ingest_cli.py --help` 查看入口；各子命令也支持 `--help`。

```powershell
python attachment_ingest_cli.py check --handoff <冻结清单.json> --handoff-sha256 <清单SHA256> --report <新报告.json>
python attachment_ingest_cli.py ingest --config <当前配置.json> --handoff <冻结清单.json> --handoff-sha256 <清单SHA256> --targets <目标映射.json> --targets-sha256 <映射SHA256> --receipt-root <回执根目录> --report <新报告.json>
python attachment_ingest_cli.py reconcile --config <当前配置.json> --handoff <冻结清单.json> --handoff-sha256 <清单SHA256> --targets <目标映射.json> --targets-sha256 <映射SHA256> --receipt-root <同一回执根目录> --report <新报告.json>
python attachment_ingest_cli.py download --config <当前配置.json> --receipt <已核验receipt.json> --output <新的本地下载路径> --report <新报告.json>
```

原生下载地址使用 `/api/v1/files/{file_id}`，需现有认证。界面用户可在最高专业资料库的原件文档使用 RAGFlow 自带下载操作；本入口提供上述 CLI 下载。没有新增网页按钮或另建下载服务。

若正文只有部分候选画像，本轮仅用其中已保存的专业排序，不重跑画像；关联状态是 `paper_identity_only`，不表示正文已完成或已有远端正文 ID。
