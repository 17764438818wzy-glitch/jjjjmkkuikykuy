# 视觉通道主备验收记录

本次只覆盖 `agent_client.py` 的视觉请求、两组配置、分级错误处理、熔断和账单字段；RAGFlow、双库路由、上传去重、图片级断点逻辑未改动。

## 配置

`ai_settings.json` 新增 `vision_model`：

```json
{
  "enabled": true,
  "primary": {"base_url": "", "api_key": "", "model": ""},
  "fallback": {"enabled": true, "base_url": "", "api_key": "", "model": ""},
  "retry": {"max_tries": 3, "backoff_base_seconds": 2.0},
  "circuit_breaker": {
    "failure_threshold": 3,
    "cooldown_seconds": 300,
    "cooldown_multiplier": 2.0,
    "max_cooldown_seconds": 3600
  }
}
```

界面显示主通道“中科大免费通道”和备用通道“DeepSeek 官方”；地址、密钥和模型名均为空，由界面填写。密钥仍只写入 DPAPI 文件：`ai_vision_primary_key.dpapi` / `ai_vision_fallback_key.dpapi`。

视觉页的“读取可用模型”和“模型连通测试”是统一按钮：一次操作依次处理已启用的主通道和备用通道，分别回填 `vision_model` / `vision_fallback_model`，并在状态栏显示两路结果。

## 分级错误测试（fake opener）

| 输入 | 实际行为 | 证据测试 |
|---|---|---|
| 502/503/504、连接错误、超时 | 按视觉重试参数退避；仍失败计一次主熔断失败，再切备用 | `test_primary_503_retries_then_falls_back_and_records_channel` |
| 429 | 较长退避重试，失败后报限流；不切备用、不计熔断 | `test_429_retries_without_fallback` |
| 400 且含 `multimodal` / `enable-multimodal` / `多模态` | 不重试，立即切备用 | `test_multimodal_400_switches_immediately[...]` |
| 400 其他 | `[请求错误]`，停止，不切备用 | `test_fatal_configuration_errors_stop_without_switch[400-*]` |
| 401/403 | `[配置错误]`，提示 Key/权限和配置路径，停止，不切备用 | `test_fatal_configuration_errors_stop_without_switch[401/403-*]` |
| 404 | `[配置错误]`，提示模型名路径，停止，不切备用 | `test_fatal_configuration_errors_stop_without_switch[404-*]` |

## 熔断与重启

`vision_circuit.json` 与 `processing_state.json` 位于同一状态目录，使用临时文件、`fsync` 和 `os.replace` 原子替换；识别失败不会写入 `images_done`，图片失败仍由既有 `failures` 列表负责。

fake opener 的序列 `503 → 备用成功 → 503 → 备用成功` 在阈值达到后产生：

```text
primary HTTP 503  (channel=primary, error_type=service_unavailable)
fallback response (channel=fallback, ok=true)
primary HTTP 503  (channel=primary, error_type=service_unavailable)
circuit_opened    (primary, next requests skip primary)
fallback response (channel=fallback, ok=true)
```

冷却到期只放行一个 `half_open` 探测；探测成功把状态写回 `closed`。重新创建 `AgentClient` 后仍从 `vision_circuit.json` 读取 `open` 状态，直接走备用。证据测试：`test_circuit_opens_skips_primary_and_persists`、`test_cooldown_probe_success_closes_and_switches_back`、`test_circuit_state_survives_client_restart`。

## 计费字段

每个请求最终记录包含 `channel`、`model`、`tokens`/`total_tokens`、`ok`、`error_type`；探测请求走同一账本。汇总示例：

```text
本批图片识别 6400 次
主通道 6312 次 / 备用通道 88 次 / 熔断 2 次
预估费用由 usage + 模型价格快照计算
```

实际账单测试：`test_probe_is_billed`；字段与汇总测试：`test_primary_503_retries_then_falls_back_and_records_channel`。

## 验证命令

```text
pytest -q test_vision_failover.py                 # 15 passed
pytest -q --ignore=backups                         # 552 passed
```

仓库内 `backups/` 含同名历史测试副本，直接对整个树执行 pytest 会触发 pytest 的 import-file-mismatch；验收命令明确排除了它们。
