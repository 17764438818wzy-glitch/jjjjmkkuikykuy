# 局域网模型接口修复（2026-09-21）

Windows 和 Linux 的 `agent_client.py` 同步使用同一地址规则：文字模型、旧版视觉设置、视觉主通道和备用通道允许 HTTP 回环地址，以及明确的 RFC1918 私有 IPv4 和 IPv6 ULA 地址。公网地址继续要求 HTTPS；不通过 DNS 解析将域名自动判定为内网。账号、查询参数、片段和缺少协议头的地址仍拒绝。

本服务填写 `http://192.168.1.120:8081/v1`。填写根地址时程序仍自动补 `/v1`。读取模型使用 `/models`，模型调用继续使用 `/chat/completions`，本次未添加 Responses 协议适配。

## 验证

- 新增 `test_lan_ai_endpoints.py`：修改前 20 失败、72 通过；修改后 92 通过。
- Windows 根目录完整测试：973 通过，1 项原有失败 `test_p0_2_fallback_config.py::test_saved_runtime_config_enables_configured_fallback_channel`。该测试要求实际配置开启备用通道，而用户设置为关闭，本次保留用户设置。
- Linux 核心：888 通过、97 按现有平台规则跳过。用容器 UID 10001 将应用复制到独立可写临时目录运行，未挂载线上数据。直接在不可写 `/app` 运行会使测试的默认断路器状态写入失败；新旧镜像都复现相同 67 项失败，改用可写测试副本后全部通过。
- Linux 网页后端：51 通过，2 项依赖弃用警告。
- 部署版本 `paper-workbench:20260921-r7-lan`；web 和 worker 均 healthy，首页与 `/healthz` 均 HTTP 200。
- 已从线上 worker 验证地址规范化和实际请求可达：用测试占位密钥访问返回预期 401，证明请求不再被本地 HTTP 校验阻止。这不是用户真实密钥或模型推理成功的验证。

## 变更与部署范围

生产代码仅新增地址判断助手并替换两处判断；预处理、AI 判断规则、重试、缓存和上传规则不变。Windows 原程序未强制重启，需用户正常关闭再打开加载修复。Linux 更新前后队列均无活动任务。

新 Linux 发布目录从服务器现有 `20260921-r6` 复制，只替换 `agent_client.py` 并加入回归测试，保留新版网页。原 worker 的 r5 与 web 的 r6 Python 源码已核对一致。原发布版本和镜像保留；环境选择备份为 `/opt/paper-workbench/deploy/.env.before-20260921-r7-lan`，可按既有运维流程回退。

本机源码备份、Windows 测试日志与部署脚本：`C:\Users\Administrator\Documents\uv\diagnostics\lan-ai-20260921`。
