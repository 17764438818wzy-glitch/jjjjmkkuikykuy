# 经源码固定的服务器只读入口：隔离审查稿

本稿只从已有只读审计快照和本机候选生成；没有安装、开放 gate、连接真实服务或认领工单。将受控 Python/venv 的启动（包括 site 和 .pth）、操作系统、账号与标准库/系统动态依赖作为外部信任边界；这些依赖没有完整逐字节固定，入口不是 Python 沙箱。六个历史 app 的静态直接依赖图未发现所列动态装载方式，但不证明运行时依赖闭包。

## 固定输入

- 本机引导脚本 `server_verified_entry.py`：`0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032` → `/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py`
- 本机复用校验器 `release_guard.py`：`812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591` → `/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py`
- 本机未读检查脚本 `server_unread_scope_query.py`：`f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521` → `/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_unread_scope_query.py`
- 六版 app 源码清单索引：`1b42e88533c6df1697e9fa5aac0bec50239a6e85ee621a7936199f3a8903cae6`；各个清单的服务器目标路径与 SHA 在本机 `guarded-app-manifests-candidate-20261101-v1/index.json` 中。索引仅供本机固定，6 份清单才需要放在其对应服务器路径。
- 本机 guard bundle：`36d7bbeb7dbe3bfaed38495e1bc4f34b86ae118f594430ea9e479551c5a668f7`；本机历史注册表：`7fc35333f9d59094d388c4d54b30aed3db8b2b2e96b79dd5bf7d7da76a93ccb2`；本机访问引用：`e1f3444842540ab5fb8f48a6c45ec8fc503600f42ef066f680fe8bff1ea31599`。

固定入口先校验 guard 源码，再校验该历史 app 清单和各源码；常规 app import 以核验后的同一份源码字节执行，未读脚本自身也先校验。入口启动前 venv/site、标准库和 Python 本身仍是可信环境。已批准代码若自行绕过普通 import 直接加载代码，不受 finder 沙箱约束。

所有服务器目标目录须独立建立并受控，不覆盖历史目录。逐文件安装后重新校验 SHA 和权限；把以下完整命令逐条精确匹配，不得放行任意 `python -c`、可变脚本、配置或清单。输入仅为现有单个原始回执绑定请求；未知/缺失不得自动重发。

运行时 Python 3.12 `-I -B` 的 venv prefix 已由服务器窗口做无业务对照，但新增 guard/importlib 是否改变 native executable 映射尚需现场独立无业务验证；哈希清单只覆盖 app .py，并非全部运行时依赖。以下为审查命令，不构成放行或页面切换。

## 每个可访问版本的精确候选命令

### 配置 `2317ca51d2bb64f0702d2c987e3ffc4de5653d9f912511eca59aa8f82228dc1a`，访问引用 `base`

**inspect**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-service-1ed179828417/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-2317ca51d2bb64f0702d2c987e3ffc4de5653d9f912511eca59aa8f82228dc1a.json c4b6c15f47b941d92b575614ba81d5b85337af98c39e08e3677304b650ceb12a /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-service-1ed179828417/app/server_rpc.py bf11c973138ffb9ded90d13efc1c6dd098684aff925ed6cb3a44fa49e653b789 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-service-1ed179828417/app/server-job-service.json 2317ca51d2bb64f0702d2c987e3ffc4de5653d9f912511eca59aa8f82228dc1a inspect
```

**unread**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-service-1ed179828417/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-2317ca51d2bb64f0702d2c987e3ffc4de5653d9f912511eca59aa8f82228dc1a.json c4b6c15f47b941d92b575614ba81d5b85337af98c39e08e3677304b650ceb12a /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-service-1ed179828417/app/server-job-service.json 2317ca51d2bb64f0702d2c987e3ffc4de5653d9f912511eca59aa8f82228dc1a unread
```

### 配置 `d6e032eecb0c57234395c27f4db54f532fc68e0a62fafab6086b0df8d587fe04`

访问引用 `unavailable_53ff` 不可用，保持未知，不生成命令。

### 配置 `204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf`，访问引用 `base`

**inspect**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf.json 214de689a6b9130a60c98a082f5ec1c26e1ab22fd3b2a8b6a145f327e3380b49 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app/server_rpc.py bf11c973138ffb9ded90d13efc1c6dd098684aff925ed6cb3a44fa49e653b789 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app/server-job-service.json 204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf inspect
```

**unread**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf.json 214de689a6b9130a60c98a082f5ec1c26e1ab22fd3b2a8b6a145f327e3380b49 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app/server-job-service.json 204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf unread
```

### 配置 `fa58a91d78b82b39c28e97d5630e66de9b41b728a33a5248ee75e3f59cb265db`，访问引用 `capacity`

**inspect**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-fa58a91d78b82b39c28e97d5630e66de9b41b728a33a5248ee75e3f59cb265db.json 91ca3e4b77a7c77590f2628cdf0432cf29b23366e1d19186a3791cae6465fe8f /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/app/server_rpc.py bf11c973138ffb9ded90d13efc1c6dd098684aff925ed6cb3a44fa49e653b789 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/app/server-job-service.json fa58a91d78b82b39c28e97d5630e66de9b41b728a33a5248ee75e3f59cb265db inspect
```

**unread**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-fa58a91d78b82b39c28e97d5630e66de9b41b728a33a5248ee75e3f59cb265db.json 91ca3e4b77a7c77590f2628cdf0432cf29b23366e1d19186a3791cae6465fe8f /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/app/server-job-service.json fa58a91d78b82b39c28e97d5630e66de9b41b728a33a5248ee75e3f59cb265db unread
```

### 配置 `571c6edb56c3fc8da7d015dde64d4d39a8a3b3538e9c6dd2edb363a3b6f827ac`，访问引用 `capacity`

**inspect**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unread-920edfdd8046450d9ffe2c133c63e8cb/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-571c6edb56c3fc8da7d015dde64d4d39a8a3b3538e9c6dd2edb363a3b6f827ac.json c6b2dc1297d7eefec5839545823b971d44fc72532c98706586038cca5c3cfe00 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unread-920edfdd8046450d9ffe2c133c63e8cb/app/server_rpc.py bf11c973138ffb9ded90d13efc1c6dd098684aff925ed6cb3a44fa49e653b789 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unread-920edfdd8046450d9ffe2c133c63e8cb/app/server-job-service.json 571c6edb56c3fc8da7d015dde64d4d39a8a3b3538e9c6dd2edb363a3b6f827ac inspect
```

**unread**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unread-920edfdd8046450d9ffe2c133c63e8cb/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-571c6edb56c3fc8da7d015dde64d4d39a8a3b3538e9c6dd2edb363a3b6f827ac.json c6b2dc1297d7eefec5839545823b971d44fc72532c98706586038cca5c3cfe00 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unread-920edfdd8046450d9ffe2c133c63e8cb/app/server-job-service.json 571c6edb56c3fc8da7d015dde64d4d39a8a3b3538e9c6dd2edb363a3b6f827ac unread
```

### 配置 `b207bb4336595b988ebb105817fc240c9317e6426d7b8753483ea87ed1c6ede0`，访问引用 `capacity`

**inspect**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unlocated-44ce6687f2ca4a6b8c9261e43d99f56b/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-b207bb4336595b988ebb105817fc240c9317e6426d7b8753483ea87ed1c6ede0.json de1020c014d7f83e18d02912c77f88eff783c3eff233665ba32e331520f3d430 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unlocated-44ce6687f2ca4a6b8c9261e43d99f56b/app/server_rpc.py bf11c973138ffb9ded90d13efc1c6dd098684aff925ed6cb3a44fa49e653b789 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unlocated-44ce6687f2ca4a6b8c9261e43d99f56b/app/server-job-service.json b207bb4336595b988ebb105817fc240c9317e6426d7b8753483ea87ed1c6ede0 inspect
```

**unread**

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unlocated-44ce6687f2ca4a6b8c9261e43d99f56b/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-b207bb4336595b988ebb105817fc240c9317e6426d7b8753483ea87ed1c6ede0.json de1020c014d7f83e18d02912c77f88eff783c3eff233665ba32e331520f3d430 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_unread_scope_query.py f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-unlocated-44ce6687f2ca4a6b8c9261e43d99f56b/app/server-job-service.json b207bb4336595b988ebb105817fc240c9317e6426d7b8753483ea87ed1c6ede0 unread
```

### 基准服务器只读 probe

```sh
/home/wzy/paperflow-postprocess/candidate-20261101/venv/bin/python -I -B -c 'import hashlib,hmac,pathlib,sys; p=pathlib.Path(sys.argv[1]); f=p.open('"'"'rb'"'"'); data=f.read(1048577); f.close(); (len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) or sys.exit('"'"'version script changed'"'"'); sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; exec(compile(data,str(p),'"'"'exec'"'"',dont_inherit=True),{'"'"'__name__'"'"':'"'"'__main__'"'"','"'"'__file__'"'"':str(p)})' /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/server_verified_entry.py 0324cfcc43cd0396cca6f93cc4fce90b8e7352a713eb8ca92b5226903d9d0032 /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/release_guard.py 812fcf5bfb606571eadd4113961ed0c04f5bca7c95cfa4668846037f510b1591 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app /home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v2/manifests/app-204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf.json 214de689a6b9130a60c98a082f5ec1c26e1ab22fd3b2a8b6a145f327e3380b49 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app/server_rpc.py bf11c973138ffb9ded90d13efc1c6dd098684aff925ed6cb3a44fa49e653b789 /home/wzy/paperflow-postprocess/candidate-20261101/postprocess-revision-cb359fad896b4ecaa62cdcf46ee7d3fe/app/server-job-service.json 204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf probe
```

## 验证顺序

1. 核对服务器历史版本源码清单、信任边界和各文件的实际哈希；核实系统解释器、venv 与 site 在该阶段可信。
2. 先在独立目录用无业务合成输入验证各版本导入来源、native 映射、超时和 gate 精确匹配；不能以此改写旧结果。
3. 需要现场回执验证时，仅作获授权的只读核对；正确单、缺陷单、历史未知单分别核验。未读检查只处理无法定位的未读声明，不证明完全脱敏或 RAG 接收。
4. 其他条件和用户指令满足前，保留现网入口及所有工单/回执/封包。
