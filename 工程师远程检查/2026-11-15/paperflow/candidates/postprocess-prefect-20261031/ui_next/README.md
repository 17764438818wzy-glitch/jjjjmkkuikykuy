# 后处理结果 UI 候选

此目录是隔离候选。第一版结果按钮与计数脚本已由主任务合并至现网页面；本目录后续新增的是核验时间标识和脚本版本，尚待主任务合并。`index.html` 在后处理卡片增加三种结果及其展开面板，引用独立的 `postprocessing-results.js`；等待 RAG 卡片没有新增结果按钮。样式和统计事件沿用现有前处理实现；LLM 脚本和样式没有改动。

## 复制基线

从 `candidates/first-step-preview-8795` 复制的原始五个基线位于 `baseline/`。以下为当时基线，不是当前现网哈希。第一版合并后重新比对当前现网，候选仅新增时间说明及对应脚本版本；后续合并前应再次核对，避免覆盖并行工作。

| 基线文件 | SHA256 |
| --- | --- |
| `index.html` | `D1668CC3F746FFFEC53D09E53222F25C51B7DE92112FE890B17B95B9A83CEE02` |
| `preprocessing-results.js` | `E37289C1138A37897B9A82B56343F81E255CA420D86E40878A9A454C5AA64C93` |
| `llm-results.js` | `F75F45CD752FE22EA3D14335D5DBBE48CA69612A950319AE415E5848B95FC8A2` |
| `prefect-ticket-counts.js` | `4E3A2B179158979ACC5008851DA11ED962B1FD5ADFE5F10F03AD9B6DF559317A` |
| `styles.css` | `E33086C8D386F5A554287DF405673A18C205E9FC1BE0FB999A8202E602249CAD` |

## 接口与核验

监听现有 `paperflow:queue-summary` 事件中的 `postprocessing_results`，展开结果时向现有统计接口发送 `postprocessing_result_filter`、`postprocessing_result_page`、`postprocessing_result_page_size`。返回的 `list` 需包含匹配的 `filter`、`page`、`page_size`、`total`、`items`；`available` 未确认或列表不匹配时显示不可用。部分统计标为至少，未知数显示“—”。文本使用 `textContent` 呈现，列表显示工单归属与接口实际返回的原因、缺口、警告、诊断。

三个队列和结果的时间均标注完整核验时间，未提供可信时间的投影由 API 判不可用。等待后处理明确显示“已登记等待 · 实际处理前复核原回答”，时间限定为交接材料的完整核验，不把模型原回答准入作为已验证事实。先运行失败测试后实现，当前 `node --test tests/postprocessing-results.test.cjs tests/postprocessing-counts.test.cjs` 共 11 项通过。第一版曾使用合成浏览器响应核验点击、翻页和收起。合成验收不代表实际后处理任务已通过。
