# 第三轮独立提示词（2026-10-17）

用户已批准三份草案及三个衔接点。本次复用既有节点、文件交接、正文物化和累计两次补交额度；改完保持停止，不恢复入库。

| 既有节点 | 独立模板 | 本阶段交付 |
| --- | --- | --- |
| round3_assets | paperflow/prompts/round3_assets.txt | 只更新 assets.json；描述优先，必要才留图 |
| round3_redact | paperflow/prompts/round3_redact.txt | 必要的正文、画像、树、图表文字及留图遮挡指令 |
| round3_chunk | paperflow/prompts/round3_chunk.txt | 只生成 chunks.json；body 给行范围，learning/chart 给内容 |

三份模板正文与用户附件一致。程序只填任务标识、工作目录和允许查看的必要图片路径；切片另提供准确行号，补交另附当前故障范围。第三轮直接返回对应模板，不拼旧通用协议。前两轮原始文件、正常调用和补交调用的最终提示词均与修改前逐字一致。

## 最终正文行号

切片调用前，从当前 article.redacted.md 生成 private/article.numbered.txt，并向模型提供总行数和原文件 SHA。它是最终正文的等价只读视图，每行前有行号及 TAB；编号不写回正文，不作为公开成果入库。UTF-8 BOM、空行、CRLF、末尾换行和 Unicode 换行采用与正文物化相同的 splitlines 口径。

模型提供从 1 到末行连续、无重叠的边界，既有 materialize_body_chunks 按范围复制正文。无需让模型抄写正文或自行数行。

## 按失败位置补交

直接失败于第三轮子节点时，沿用对应独立模板及其读取/工具范围；原图表、脱敏或切片步骤成功后不重复执行。既有图保持失败业务阶段和后继关系。

发布、技术检查等消费者故障使用短技术补交提示词：正文覆盖问题仅补边界/格式；明确隐私问题只落实准确定位；图表缺交付仅复用已有说明或图像定位；其他缺项优先恢复现成内容，无依据则报告缺失。提示词/工具范围与实际 failed_stage 分开，不能为选择提示词覆盖流程恢复位置。消费者补交不重新执行整套研究理解、分类或质量评审。

额外补交仍按单篇累计最多两次，JSON 修复共享额度；网络故障另行技术重试。

## 缓存及验证

阶段输入指纹增加本阶段模板 SHA。模板文字变更后旧缓存不再命中，沿用现有缓存文件证明和 prompt/provenance 记录，不新增缓存框架或依赖。

本地验证记录位于 runtime/independent-prompts-verification-20261017/：result.json、pytest.xml、compatibility.json、paused-check.json。完整离线回归197项通过，外部连接尝试为0；未调用真实模型、知识库或持续服务，未迁移真实台账/检查点。独立复核记录另见该目录 review.md。
