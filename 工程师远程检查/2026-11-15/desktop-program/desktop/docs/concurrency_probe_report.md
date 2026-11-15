# 并发探测报告

已使用当前工作区配置的真实中转站和合成四色图片完成探测；没有读取论文、写入知识包，也没有在报告中记录 API Key。每档 20 次，配置重试预算为 4 次（瞬时网络错误按客户端上限最多 5 次尝试），结果如下：

| 并发 | 成功 | 429 | 平均耗时 | 吞吐（次/秒） |
|---:|---:|---:|---:|---:|
| 4 | 20/20 | 0 | 8.326s | 0.428 |
| 8 | 19/20 | 1 | 15.516s | 0.304 |
| 16 | 11/20 | 1 | 21.179s | 0.274 |
| 32 | 20/20 | 0 | 28.992s | 0.362 |

按“成功率不低于 99% 且吞吐最高”选择全局 LLM 闸门 `global_llm=4`。32 路虽然本轮最终成功率达到 100%，但吞吐低于 4 路，不作为默认值。探测原始记录保存在 `concurrency_probe.json`。

`concurrency.enabled` 仍保持 `false`，这是为了保持旧批处理的默认行为；启用后默认的并发上限使用实测安全值。配置中的起点为：

| 配置 | 默认值 | 说明 |
|---|---:|---|
| `concurrency.enabled` | `false` | 保持向后兼容，确认小批量后手动打开 |
| `concurrency.papers` | `2` | 论文级线程数 |
| `concurrency.images_per_paper` | `2` | 单篇图片级线程数 |
| `concurrency.global_llm` | `4` | 实测最高吞吐且成功率不低于 99% |
| `concurrency.max_retries` | `4` | 并发模式请求重试上限 |
| `concurrency.backoff_base_seconds` | `1.0` | 退避起始秒数 |

探测脚本会用合成四色图片、每档 20 次调用，输出 4/8/16/32 并发的成功数、429 数、平均耗时和吞吐，并选择“成功率不低于 99% 且吞吐最高”的档位：

```powershell
python probe_concurrency.py --base-url https://中转站/v1 --api-key $env:VISION_KEY --model <vision-model> --output concurrency_probe.json
```

先用 `python app.py --limit 10`（或界面的扫描上限）试跑小批量，再在 Agent 模型页勾选并发预处理。若中转站状态变化，可重新运行探测脚本覆盖该报告。
