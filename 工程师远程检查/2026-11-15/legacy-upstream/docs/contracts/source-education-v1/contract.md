# 来源学历交接接口：source-education/1.0

状态：两端共同实施的接口基线；当前仅完成设计与本地样例校验，不代表产品或服务器已接通。

确认依据：2026-10-10 用户确认原始学历、原文学习形式、业务归类分别保存；成人本科归为本科 / 成人教育 / 函授类；来源信息不直接决定质量和适用档位；历史文章只补元数据；验收包含完全未注明。

## 职责

| 端 | 职责 |
| --- | --- |
| 预处理 | 清理封面前从原文件取证，识别三项信息，校验原文位置，保存到新任务和续跑检查点，随新知识包交接。 |
| RAG | 校验可信交接、文件身份、结构及规则版本；保存来源事实和核验状态；接入画像、普通检索及定位检索；处理冲突和缓存。 |
| Agent | 读取统一字段选材；继续使用独立的质量评价与适用档位。 |

## 载体与版本

- 新知识包在 metadata.json、批次 item 和可信 handoff 中携带同一个 `source_education` 对象及 `source_education_revision`。
- RAG 文档 meta_fields 中 `source_education` 保存为 JSON 字符串，`source_education_revision` 保存为字符串。画像和两种检索返回解析后的相同对象及 revision。
- document_id/stable_paper_id 复用现有 handoff 外层，不在来源对象内再造另一套论文 ID；历史补丁由明确的 dataset/document 身份绑定当前来源记录。
- schema_version 固定为 `source-education/1.0`，extraction_rule_version 固定为 `source-education-extract/1.0`。成人归类规则版本固定为 `adult-to-correspondence/1.0`。
- revision 为 source_education 的确定性 JSON UTF-8 字节的 SHA256，小写十六进制。复用 knowledge_package.encoded 的规则：对象键递归按 ASCII 顺序排序，保留数组顺序，不转义中文，无多余空白；整数必须在 JavaScript 安全整数范围内，定位整数为 1..9007199254740991，不含 NaN、Infinity 或非法代理字符。Python 发送端必须使用 int，拒绝 float（包括 1.0），再执行 json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))；JavaScript 接收端检查 Number.isSafeInteger，递归排序后 JSON.stringify。JSON 文本排版不影响 revision。
- 未知 schema/rule 版本不能静默按 v1 解释、过滤字段后假装成功；保留原载荷，标为 unsupported，影响来源字段，不因此删除已有正文或质量结果。
- 原文件 SHA256、来源标注 revision、下游正文 source_version 是三个不同概念。不得互相赋值。

## 三项字段

| 字段 | 值 | 含义 |
| --- | --- | --- |
| education_level.value | associate / bachelor / master / doctor / unknown | 专科、本科、硕士、博士或未知；与下游现有学历枚举兼容。 |
| study_mode.value | correspondence / adult_education / full_time / part_time / amateur / off_job / self_study / open_education / online_education / other / unknown | 原文的学习形式或教育形式类别；必须同时保存原文 raw_labels。这里只做原文归一，不作法定教育制度认定。 |
| business_classification.value | correspondence / unknown | 函授类或尚无已约定业务归类；不表示质量或适用档位。 |

education_level 和 study_mode 均带 status、raw_labels、evidence_ids、candidates。

- status=source_explicit：有该论文来源身份的原文明示；值不为 unknown，原文标签和依据不为空，candidates 为空。
- status=unknown：值为 unknown，原文标签、依据、候选均为空。不从题目、目录路径、正文研究对象或导师学位补猜。其他未支持的目录/人工提示留在既有记录，不能升级为原文明示。
- status=conflict：有效值为 unknown；在 candidates 中保留至少两个不同值、各自的原文标签和证据。顶层 evidence_ids 为双方证据集合，不能丢弃冲突引文。

业务归类：

1. study_mode=correspondence 且 source_explicit → business_classification=correspondence，basis=source_correspondence，rule_id=explicit_correspondence。
2. study_mode=adult_education 且 source_explicit → business_classification=correspondence，basis=business_rule，rule_id=adult_education_to_correspondence。
3. study_mode 冲突 → 业务归类 unknown/conflict；即使双方都可能落到函授类，也保留待核状态。
4. 未注明或不在上述映射中的形式 → 业务归类 unknown，basis=none，rule_id=null。不能扩大成“所有本科都归函授”。
5. 学历和学习形式独立。只写“成人教育”时可以归函授类，但学历仍 unknown。

`raw_labels` 是逐字原文，例如原句“成人本科毕业论文”对应学习形式 raw_labels=["成人"]，标准显示值 adult_education 对应“成人教育”；原句仍完整保存在 evidence.quote。

v1 的最小标签归一表如下。raw_labels 应选最小明确标签片段，须逐字出现在 quote 中；比对归一表时仅允许 Unicode NFKC 和去空白，不改写原文存储。新别名必须先升级提取规则，不能由两端各自猜测。

| value | 可归一的原文标签 |
| --- | --- |
| associate / bachelor | 专科、大专 / 本科、大学本科 |
| master / doctor | 硕士、硕士研究生 / 博士、博士研究生 |
| correspondence / adult_education | 函授 / 成人、成人教育、成人高等教育 |
| full_time / part_time | 全日制 / 非全日制 |
| amateur / off_job | 业余 / 脱产 |
| self_study / open_education / online_education | 自考、自学考试 / 开放教育 / 网络教育、远程教育 |

other 只用于已有明确学习形式字段中不在表内的原文形式，保留原标签，不做函授归类；不能把表内已知标签改存为 other。标签出现仍不证明角色正确，必须排除导师、调查对象、参考文献或正文话题。

## 原始文件证据

- original_source_sha256 始终指最初输入文件的二进制校验值。
- evidence_source_sha256 指实际取证文件。PDF、DOCX 原文直接取证时两者相同。
- DOC 转 DOCX、DOCX 容器规范化后取证时，两者可以不同；source_relation 为 converted 或 normalized，必须给出 derivation 的输入/输出校验值、处理方法及可查的本地记录标识。原文件校验值不能改成转换文件校验值。
- evidence_source_format 为 pdf/docx/txt/md；每条证据只包含最小必要原文片段、字段引用和可定位位置，不携带姓名、学号、完整本地路径或整张未脱敏封面。
- PDF locator：kind=pdf_page，page 从 1 开始，以 evidence_source_sha256 所指文件为准。
- DOCX locator：kind=docx_paragraph，part=word/document.xml，paragraph_index 从 1 开始，按该 XML 原始文档顺序统计所有 w:p（含表格、文本框、空段和兼容分支）。先给全部 w:p 保留原始索引，再只从实际选择的兼容分支取证；未选择的备用分支不是可用来源。不能使用清洗后段落编号。
- TXT/MD locator：kind=text_lines，start_line/end_line 从 1 开始，按原文件解码后的行定位。
- 不能把这些位置填到现有画像正文 evidence 的 start/end。来源证据与质量证据分开保存和核验。

## 接收规则

1. schema 校验后还须检查：revision 匹配、证据 ID 唯一、引用都存在、每个依据的 supports 包含相应字段、raw_labels 确实为关联 quote 的连续片段、标签归一值符合当前提取规则、位置类型与取证格式一致、候选和状态一致、业务归类与已约定规则一致、转换链身份一致。JSON Schema 的 integer 可能接受 1.0，因此 Python 发送端不能省略严格整数检查；JavaScript 在解码后检查安全整数并按同一规则重算摘要，不需要从解析后的 Number 区分原始文本写的是 1 还是 1.0。仅引文存在和结构通过不足以证明标签语义或作者角色正确。
2. 不能仅凭载荷自称 source_explicit 就当已核实。RAG 应绑定可信预处理记录及当前论文身份，核验原文件/取证文件校验值与位置回执；原文件可读时直接复核原句。已验证的可信原文件回执可作为来源依据，不要求清洗正文再次包含封面原句。
3. 接收侧单独保存 source_education_verification 字符串（pending / verified / conflict / unsupported）、source_education_verified_revision 字符串（已核验载荷 revision；未核验为空串）和 source_education_verification_reason 字符串。这三个字段由 RAG 生成，预处理不能在普通补丁中自授 verified。只有 verified 且 verified_revision 等于当前 revision 时，才可作为已核实来源使用。无法验证来源时保留标注为待核，不伪造 verified。完整合法的“完全未注明”也可被核验；verified 不会把 unknown 变成已知值。
4. 三种字段和来源证据须进入 RAG schema、导入适配器、校验器、写入白名单、画像保存、普通检索和定位检索；不依赖未知字段碰巧透传。检索必须返回统一对象、revision、接收核验状态。
5. 原有明确学历遇到不一致的新标注时，不覆盖原值；保留既有与新载荷及各自 revision/证据，记录待裁决冲突，有效检索状态为 conflict。跨更新冲突不可把不同原文件的证据拼到同一套文件 SHA256 下，也不能只保留顶层旧学历，却在画像内部写入另一值。未注明的新标注不得擦除已核实旧值。冲突未裁决时，已存在明确值保留在历史/冲突记录中，有效学历兼容投影返回 unknown 并附冲突状态，不能用于已核实学历筛选。
6. 对旧画像兼容读取。原来只有 education.value 的画像不因此失效；尚无新字段时来源学习形式、业务归类返回 unknown。学历兼容投影必须来自已核验且无冲突的有效值，不另行从清洗正文猜一次。
7. 更新使用现有并发版本比较/CAS 机制，不覆盖其他任务刚写入的文档元数据。补标结束后回读，核对 revision 和三项返回值。

## 新文章与历史文章

- 新文章：在生成知识包时加入交接对象，由原有包生成过程计算文件哈希、包 revision 和审核包指纹。
- 已入库文章：先核对稳定论文 ID、原文件对应关系和当前正文版本，再通过元数据更新追加来源对象。不改 ingest.md/document.md、不重新上传正文、不重切片、不更换 source_version。
- 历史本地审核包和不可变检查点不直接改写。若需本地留存补标，用独立来源标注记录，关联原包指纹和原文件 SHA；不能把这种记录伪装成已重审的旧包。
- 质量复用有条件：正文、质量依据及评价规则未变时保留原质量核实。来源证据不塞入质量 evidence 集合，避免平白改变质量核实输入哈希。
- 更新来源标注 revision 后，刷新文档元数据缓存；重新检查学历/学习形式相关筛选缓存、检索快照及已保存的选材判断。既有质量评价并不能自动证明旧选材判断仍有效。
- 旧画像迁移不能因 source_version/rubric_version 未变就跳过新来源字段；应先比较 source_education_revision。

## 验收与状态

examples.json 有五类合成样例：函授本科、成人本科、仅写本科、冲突、完全未注明。样例里的 SHA 用于格式测试，不代表存在真实来源文件或已经核实。

每类都应走：预处理输出 → RAG 保存及回读 → 普通检索 → 定位检索 → Agent 选材读取。三项值、原文依据、revision 和核验状态必须一致。

还须验证：导师硕士不覆盖论文本科；正文谈函授不构成封面函授；已有明确值与新标注冲突不覆盖；未知不猜值；补标前后正文版本、切片 ID 和未受影响的质量核实保持一致；受影响选材判断被更新。

当前证据：check_examples.py 仅验证接口结构、引用、版本摘要及五类预期语义，不调用模型、RAG 或真实原文件。不能据此宣称完整链路通过。

离线复核命令（在此目录执行）：`python check_examples.py`、`node check_revision.mjs`。前者依赖已有 jsonschema；后者仅用 Node 内置模块，检查两种语言对五例计算的 revision 是否相同。

## 当前代码接入点

- 预处理复用 classification_rules.py 的原文身份取证、source_context；auto_pipeline.py 的新处理及 retry_ai 两条路径分别保存 item/prepared.identity/meta。
- evidence.py 和 knowledge_package.py 均有 handoff 输出，需统一同一载荷。
- 下游候选 research-exemplars.ts schema、ingestion.ts、rag.ts、publish-metadata.py、update-worker.mjs、rag-plugin.mjs、located-sources.mjs 分别有结构、导入、证据检查、白名单、更新和返回边界；候选源码不等于已部署版本。
