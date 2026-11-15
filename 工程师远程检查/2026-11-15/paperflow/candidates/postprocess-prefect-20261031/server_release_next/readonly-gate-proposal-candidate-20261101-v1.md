# 服务器只读核验 gate 候选：审查用，未安装、未放行

本文件从本机六版本快照生成，用于给服务器窗口审查。它不授权执行或部署；权限、服务器目录不可变性、解释器与历史 app 导入依赖需另行核验。

- 本机脚本：`F:\论文预处理系统\预处理系统2\candidates\postprocess-prefect-20261031\server_release_next\server_unread_scope_query.py`
- 脚本 SHA256：`f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521`
- 建议的独立服务器目录：`/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v1`
- 脚本目标路径：`/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v1/server_unread_scope_query.py`
- 六版本注册表：`F:\论文预处理系统\预处理系统2\candidates\postprocess-prefect-20261031\server_release_next\history-registry-candidate-bb7d5d9ae45b.json`
- 注册表 SHA256：`7fc35333f9d59094d388c4d54b30aed3db8b2b2e96b79dd5bf7d7da76a93ccb2`
- 独立访问引用：`F:\论文预处理系统\预处理系统2\candidates\postprocess-prefect-20261031\server_release_next\history-access-candidate-ed0ef92b075f.json`
- 访问引用 SHA256：`e1f3444842540ab5fb8f48a6c45ec8fc503600f42ef066f680fe8bff1ea31599`

仅供目标服务器以独立受控目录接收相同字节，禁止覆盖已有版本脚本；需现场重新计算 SHA256，并固定目录所有者/权限。以下完整命令须逐条精确匹配，不能只匹配前缀、不能允许任意 `python -c` 或放宽脚本、哈希、app、配置参数。输入来自原回执关联的单项 JSON，输出不含私有正文。

旧版 inspect 命令继续使用现有 gate，不新增服务端版本查询脚本；只有既有 inspect 完整通过并校验身份后才调用以下检查。现有 inspect 命令本身不逐次验证 RPC 源码 SHA，必须由服务器侧保证固定目录及历史 RPC、依赖、解释器的完整性；仅凭这份清单不构成该保证。

此检查只覆盖特定的“无法定位的未读声明缺少公开限制”，不证明所有未读类型、完整脱敏或 RAG 接收。该查询除检查关联 JSON 外还会通过 verify_job 核验冻结输入快照。

固定 SHA 只覆盖单个 helper 的字节；`-B` 不阻止读取已存在的字节码。需独立审查导入链/字节码，禁止任意目录或软链接替换。单个 JSON 上限 4 MiB；过大或证据不完整返回未知。

## 精确候选命令

### 访问引用 `base`，配置 `2317ca51d2bb64f0702d2c987e3ffc4de5653d9f912511eca59aa8f82228dc1a`

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v1/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-service-1ed179828417/app /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-service-1ed179828417/app/server-job-service.json 2317ca51d2bb64f0702d2c987e3ffc4de5653d9f912511eca59aa8f82228dc1a
```

- `d6e032eecb0c57234395c27f4db54f532fc68e0a62fafab6086b0df8d587fe04`：访问引用 `unavailable_53ff` 当前不可用，保持未知，不生成放行命令。

### 访问引用 `base`，配置 `204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf`

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v1/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app/server-job-service.json 204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf
```

### 访问引用 `capacity`，配置 `fa58a91d78b82b39c28e97d5630e66de9b41b728a33a5248ee75e3f59cb265db`

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v1/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/app /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/app/server-job-service.json fa58a91d78b82b39c28e97d5630e66de9b41b728a33a5248ee75e3f59cb265db
```

### 访问引用 `capacity`，配置 `571c6edb56c3fc8da7d015dde64d4d39a8a3b3538e9c6dd2edb363a3b6f827ac`

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v1/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unread-920edfdd8046450d9ffe2c133c63e8cb/app /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unread-920edfdd8046450d9ffe2c133c63e8cb/app/server-job-service.json 571c6edb56c3fc8da7d015dde64d4d39a8a3b3538e9c6dd2edb363a3b6f827ac
```

### 访问引用 `capacity`，配置 `b207bb4336595b988ebb105817fc240c9317e6426d7b8753483ea87ed1c6ede0`

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v1/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unlocated-44ce6687f2ca4a6b8c9261e43d99f56b/app /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unlocated-44ce6687f2ca4a6b8c9261e43d99f56b/app/server-job-service.json b207bb4336595b988ebb105817fc240c9317e6426d7b8753483ea87ed1c6ede0
```

## 审查和验证顺序

1. 在离线环境核对脚本和清单，再由服务器窗口检查现场目录、现有命令、版本依赖和固定身份；本文件不触碰服务器。
2. 逐条审查 gate 白名单只允许上述完整命令；不得因查询失败重跑、重领工单或修改原有材料。
3. 完成受控只读试验后，分别核对历史版本匹配、正常单、缺陷单和未知单的显示；页面发布另受用户指令约束。
