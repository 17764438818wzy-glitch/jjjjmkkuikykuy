# 第一步：按资料组交付文件夹

本说明对应新建的独立第一步实例，交付格式沿用候选版。下游页面和下游程序的接入需由各自负责；新实例尚未运行真实论文。

## 输出单位与位置

每个明确传入的资料组、每个不可变交付版本对应一个独立目录：

```text
<output-dir>/deliveries/<group_id>/<交付版本>/
  delivery.json                 # paperflow-group-delivery/1
  handoff.json                  # 原 paperflow-group-handoff/2，内容不改
  files/<原组内相对路径>         # 正文、附件、垃圾、未确认文件的可核验副本
  attachment-evidence/<证据键>/<版本>.json
```

原始资料不移动、不修改。旧分类记录和请求意图仍作为内部台账保留。
没有可靠哈希或复制失败的文件记录 copied=false 和 error，不冒充完整交付。
目录以临时目录构建，最后重命名发布；.building-* 不是可交付目录。
复制不完整时，后续只重试复制并保留旧目录，不因此重发 Nimble 请求。

## 上游返回值

- `delivery_dir`：下游应接收的资料组交付目录。
- `handoff` / `unreceived`：指向该目录里的 `handoff.json`，保持原分类完整/不完整含义。
- `evidence_handoffs`：该目录内的附件证据路径，包含完整已保存版本链。
- `delivery_complete`：整个交付包是否满足完整接收条件。
- `delivery_errors`：没有形成可核验副本的文件条目。
- `evidence_errors`：附件证据问题。无法发布目录时返回 `error`，不交付半成品路径。

## 题目输出

交付目录的 `handoff.json` 用 `title_status` 标明题目状态：

- `已提取`：`paper_title` 为从正文封面或同组独立封面确认的完整题目；`title_source` 保留依据。
- `待核对`：`paper_title_candidate` 保存未能确认完整性的题目候选和对应来源。
- `未提取`：`paper_title` 与 `paper_title_candidate` 均为空。

无明确“题目”标签时，完整的封面题目也可以确认；分行题目会根据封面文字拼接。
文件名由本地 Nimble 辅助识别，核对结果记录在 `filename_title_observation` 和
`filename_title_note`；与封面不一致时保留差异，不覆盖明确的正文封面题目。
只有文件名线索、正文尚未确认时，题目仅作为候选，资料组完整交付状态不因此改变。
旧交接没有 `title_status` 时，可按 `paper_title`、`paper_title_candidate` 是否为空推导三态。
新规则不改写已保存的交接或旧测试产物。

## 下游需要改的接口

1. 接收参数增加 `delivery_dir`，读取目录内的 `delivery.json` 和 `handoff.json`。
2. 校验 `schema`、`delivery_id`、`handoff_sha256`、组编号、快照编号和结果版本。
   `delivery_id` 是去掉该字段后对其余清单使用现有 `records.identity()` 的结果。
3. 保留 handoff 的 `group_root`、`library_root`、文件编号和版本作为原始归属身份；
   不重写 handoff，不因交付目录变化重新分配论文编号。
4. 实际读取 `delivery_dir/files/<relative>`，核对文件大小、SHA256、copied 状态和目录边界；
   handoff 内的旧绝对路径仅作来源记录。副本失败时不能悄悄回退原目录。
5. 从 `delivery.json.evidence_records` 读取包内证据及完整版本链，逐项核对哈希；
   附件来源核验应使用同哈希副本，同时保留原 handoff 的证据绑定。
   包的哈希验证不等于其中图片/文字内容已有可信来源，沿用原证据信任规则。
6. `files_complete` 只代表副本齐全；`classification_complete` 只代表分类齐全；
   `ready_for_receipt` 还要求原交接满足接收条件且无附件证据错误。
   正文等可逐项接收，但任何未确认、缺副本或证据错误不能标记为整组完整成功。
7. 接收回执保存包清单路径和哈希、原始来源路径及实际副本路径；恢复、附件证据复核、
   题目依据读取和允许读取根也应使用已校验副本。保持旧 JSON/旧回执兼容。

上游 `group_delivery.load_delivery()` 可读取和校验目录元数据；
`delivered_file()` 校验某一文件副本。它们不会接收论文、调用模型或启动下游。
完整包仍可能没有正文，不能将交付完整等同于论文业务完成。
