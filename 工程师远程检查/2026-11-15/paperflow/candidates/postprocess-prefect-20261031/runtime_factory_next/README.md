# 服务器入口复用工厂（隔离候选）

`runtime_next/live_runtime.py` 和 `generation_next/live_runtime.py` 的 `build_runtime()` 现在额外返回：

- `upstream_factory(prefect_client)`：原来的 `upstream` 闭包，创建原 `UpstreamFlowBinding`。调用方应在 `with runtime['client']() as client:` 的作用域内使用，不得缓存已经关闭的 client 或 binding。
- `backend_factory(verified)`：原来的 `backend_for` 闭包，使用已核验接收材料构造原 `UnifiedBackend`，传入 `invoke=None`。构造会读取配置并绑定已选业务代码，并非纯函数；这项设置本身不发送模型请求。
- `observe_run(identity)`：每次打开并关闭原 Prefect client，调用上述 binding 的 `load(identity, require_running=False)`；异常返回 `False`。该观察只用于原小工单和交接身份，不证明任务允许运行、服务器进程活性、队列门禁状态或附件完整性，也不能替代现有短事务和执行前复核。

两个入口其余业务路径保持原样；原每文件夹锁和任务门禁仍存在，没有新增全局材料/传输锁。调用 `build_runtime()` 会读取现有配置并检查路径，不能为了验证接口而直接运行真实工单。两个候选版本可能生成不同 Python 类实例；同一进程必须选择一套一致的模块来源，不能靠混用别名绕过服务器适配器的类型检查。

`tests/test_runtime_factory_exports.py` 分别对两个入口使用临时路径与替身构造，核对新工厂复用、client 生命周期、`invoke=None` 和原门禁对象；不连接真实 Prefect、服务器、模型或材料。
