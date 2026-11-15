# 当前受控链路的使用说明

正式受管源码位于 `E:\rag mid\langgraph_workflow`，本次小批发布记录位于 `E:\rag mid\deployments\formal-pilot-20261017`。它以已验收候选为基线，只增加正式小批的必要接线。三轮处理到封包、专业投递、修复、排队和清理复用设计 2.2 的实现；新入口为 `paperflow managed`。`FINAL_REVIEW_REPORT.md` 是切换前候选验收，不代表本次正式试跑结果；正式实测以本次发布记录为准。

## 运行环境

本机 Windows、Python 3.14 和现有虚拟环境 `E:\rag mid\langgraph_workflow\.venv` 已用于验收。依赖版本在 `pyproject.toml` 中固定。Windows Job Object、AF_PIPE、本机 DPAPI 和已登录的 DeepSeek Harness/dsh.cmd 均为实际依赖。RAG 凭据由原 `auto_config.load_key` 读取；模型继续使用原 Harness SDK 接入，未把密钥写入候选配置。

这是一套源码程序。审核包不包含论文、账户凭据、虚拟环境、运行台账或安装程序；在另一台电脑运行需配置对应环境和自己的凭据。`reference/legacy_support` 是现有复用模块的源码参考，未把它们重复改造成新的客户端。

## 显式选择 1—10 篇的受控入口

准备一个独立运行目录；配置明确指定 `home`、`handoff_root`、`catalog_path`、`delivery_acceptance_report`，并设置 `source_roots: []`。隔离试验沿用已验收 loopback API。正式地址额外要求 `production_pilot` 指向本次授权清单，绑定原稿绝对路径/哈希、目录哈希、运行根目录、实际端点和到期时间；仅改成 8088 地址不会获准执行。投递和远端清理每次核对真实部署身份，API 重启或文件漂移后旧验收失效，需要重验。该身份接口只有受鉴权 GET，不提供索引读取或执行命令能力。

下例中的路径是占位示例，不是已创建的测试库配置：

```powershell
Set-Location -LiteralPath 'E:\rag mid\review-isolated\workflow-final-review-20261017'
& 'E:\rag mid\langgraph_workflow\.venv\Scripts\python.exe' -B -X utf8 -m paperflow --config 'E:\paperflow-test\config.json' managed --root 'E:\paperflow-test' --source 'E:\samples\one.docx' --legacy-root 'E:\rag mid'
```

不带 `--execute` 时，仅登记指定原稿的独立副本并发现任务，保持暂停。需要执行这批显式选择的材料时，在同一命令后加 `--execute --seconds 3600`。重复传入 `--source` 可选择多篇；不扫描整个原稿文件夹。相同目录不能混入未选材料或悄悄替换已登记的源文件内容。

运行期间，新管理器一直持有四把既有旧执行器锁，并检查两处正式 STOP。`run/process/scan/ui` 的旧执行命令在本候选中明确拒绝，避免误入旧链路；未重做 UI。

## 停止、退出和恢复

在本次 `--root` 下建立名为 `STOP` 的空文件即可请求停止；也可使用当前终端的 Ctrl+C。即时关闭准入，模型立即取消；已发远端写按既有收束期限处理。管理进程保存暂停、确认进程树退出后才释放旧锁。正式 `STOP_TRIAL` 和 dispatcher `STOP` 保持原状。

新启动默认暂停；本次目录已有 `STOP` 时，即使传入 `--execute` 也不会恢复。需要继续时，应先确认停止原因已经处理，再人工处理本次目录的 STOP 并显式执行。旧 epoch 的许可不能复用。网络未知写仍由核对屏障保护，不能靠重启、删除标记或一次成功响应清除。

查看本次根目录的 `managed-result.json`：`manager.live_units=0`、`pause_persisted=true` 表示收束；文章的 `available`、`expected_complete`、`pending_reconcile` 和 `complete` 是不同结果。`reason=settled` 只说明队列不再有可运行项，含挂起时也会出现，不能单独当作入库成功。

## 验收资源和正式上线

本轮真实试验的自有库、临时 API 和隧道在结束时回收。保留的运行目录用于复核，不应再拿其中已删除库的旧回执继续投递。需要再次试验须创建并验收新的自有库。

正式批量保持暂停。当前只授权一次明确小批切换；具体材料与结果保存于本次发布目录。桌面 launch.pyw 仅显示暂停与操作说明，不再直接进入旧 Service。旧 config.json、历史台账与原稿保留。扩大篇数或后续批量仍需用户另行授权。

未查实的远端执行上限只影响相关未知写屏障的自动解除；保持这些目标待核对即可，不能伪造参数。第八阶段尚未读回的历史目标也不能凭本轮新稿试验标为通过。无需重新开发已经验收的八个阶段，完整检索过滤、切换完成后的自动回滚和并发提速仍属后补。

## 回归复跑

在候选根目录运行：

```powershell
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTHONPATH="$pwd\evidence\network_guard;$pwd"
$env:PAPERFLOW_OFFLINE_AUDIT="$pwd\evidence\local-retest-network"
& 'E:\rag mid\langgraph_workflow\.venv\Scripts\python.exe' -B -X utf8 -u -m pytest -p no:cacheprovider -q --tb=short
```

此钩子只限制 Python 外部网络调用，不是系统防火墙；全套使用合成服务及既有 Harness 的 loopback 夹具。真实模型/RAG 的验收记录与离线回归分别保存，不能互相替代。
