# 本机论文/作业数据清洗第一阶段

本项目只做文件级盘点、路径检查、重复识别和小样本复制，不读取 Word/PDF 正文，不覆盖、移动或删除原始文件。

## 目录

```text
cleaning-pipeline/
  src/paper_cleaning/
  tests/
output/
logs/
sample_small/
```

## 默认排除目录

当前项目目录和数据目录可以放在同一个文件夹里。运行 `--input .` 时，脚本会自动跳过以下工程和输出目录：

```text
cleaning-pipeline
output
logs
sample_small
.git
.venv
venv
__pycache__
.pytest_cache
```

如果还有其他不应扫描的目录，可以追加 `--exclude`：

```powershell
python -m src.paper_cleaning.inventory --input . --output output/inventory_limit20.csv --mode full --log logs/inventory_limit20.log --limit 20 --exclude raw_export temp_data
```

`--exclude` 会在默认排除目录的基础上追加，不会取消默认排除规则。

## 运行准备

在当前数据目录执行以下命令，让 Python 能找到 `cleaning-pipeline/src` 下的模块：

```powershell
$env:PYTHONPATH = "cleaning-pipeline"
```

## 常用命令

全量盘点：

```powershell
python -m src.paper_cleaning.inventory --input . --output output/inventory_full.csv --mode full --log logs/inventory_full.log
```

只盘点文档文件：

```powershell
python -m src.paper_cleaning.inventory --input . --output output/inventory_documents.csv --mode documents --log logs/inventory_documents.log
```

检查 inventory，计数时会使用同样的默认排除目录：

```powershell
python -m src.paper_cleaning.check_inventory --input . --inventory output/inventory_full.csv --report output/inventory_full_check_report.txt
```

构建最多 20 个文件的小样本，复制时也会跳过工程和输出目录：

```powershell
python -m src.paper_cleaning.sample_builder --input . --output sample_small --limit 20
```

对已经抽样并转换成 txt 的小样本文本做脱敏：

```powershell
python -m src.paper_cleaning.sanitize --input output/raw_text --output output/sanitized_text --report output/sanitize_report.csv --mapping output/sanitized_mapping.csv --limit 20
```

第一阶段建议先跑限制数量的测试命令：

```powershell
python -m src.paper_cleaning.inventory --input . --output output/inventory_limit20.csv --mode full --log logs/inventory_limit20.log --limit 20
```

运行测试：

```powershell
cd cleaning-pipeline
python -m pytest -q
```

## inventory 启动提示

`inventory.py` 启动时会打印：

```text
input 路径
实际扫描根目录
默认排除目录
用户追加排除目录（如果传了 --exclude）
前 20 个一级目录
前 20 个被扫描到的文件
```

这用于确认 `--input .` 时没有把 `cleaning-pipeline/`、`output/`、`logs/`、`sample_small/` 等工程目录扫进去。

## 字段说明

`inventory.py` 输出 CSV 至少包含：

```text
file_id, original_relative_path, sanitized_relative_path, file_name, file_ext,
file_size, sha256, is_duplicate, is_archive, archive_type, status, error_message
```

`sanitized_relative_path` 第一阶段暂时保持与原始相对路径一致，不尝试改名；后续脱敏阶段再处理姓名、学号等信息。

## 脱敏策略

`sanitize.py` 只处理 `*.txt`，不读取 Word/PDF 原文。本机测试建议只对 `output/raw_text` 或小样本转换出的 `sample_small/raw_text` 运行，不直接处理全量数据。

封面和前置信息采用“锚点前删除”：

```text
摘要
ABSTRACT
Abstract
目录
第一章
第1章
绪论
```

如果这些锚点出现在文档前 3000 个字符内，则删除锚点之前的内容，从锚点开始保留；如果找不到锚点，不盲删全文。

正文只做字段型脱敏和明确敏感信息替换：

```text
手机号 -> [PHONE]
邮箱 -> [EMAIL]
身份证号 -> [ID_CARD]
学号/学生证号/Student ID 字段 -> [STUDENT_ID]
学生姓名/姓名/作者 字段 -> [NAME]
指导教师/指导老师/导师 字段 -> [TEACHER]
班级/专业班级 字段 -> [CLASS]
QQ/微信/WeChat 字段 -> [CONTACT]
```

脚本不会把所有 2-4 个中文字符当成人名，也不会泛化删除“国内外研究现状”“文献综述”“理论基础”“参考文献”等章节里的学者姓名、作者名、公司名、品牌名或地名。

## 脱敏输出归档

脱敏后的训练文本不保留原始学校、学院、班级、姓名文件夹结构。最终只输出脱敏后的 `txt` 文件，不生成 `sanitized_docx`，不复制原始 Word/PDF。输出按 15 个固定学科大类和“专业或方向”归档：

```text
output/sanitized_text/
  经管财会类/
    财务管理/
      FM_000001.txt
  医护健康类/
    护理学/
      NUR_000001.txt
  计算机软件与数据类/
    Java管理系统/
      JAVA_000001.txt
  智能硬件与控制类/
    STM32/
      STM32_000001.txt
  未识别_其他类/
    未识别方向/
      UNK_000001.txt
```

固定学科大类：

```text
经管财会类、教育教学类、医护健康类、法学社会公共类、文学外语传媒类、艺术设计类、
计算机软件与数据类、智能硬件与控制类、机械制造与机电类、土木建筑与交通类、
电气电子与通信能源类、化工材料环境安全类、农林生物资源类、理学统计心理类、未识别/其他类
```

`未识别/其他类` 的实际目录名为 `未识别_其他类`，目录名不会包含 `/`。

识别优先级：

```text
1. 技术强关键词：STM32、51单片机、PLC、LabVIEW、Spring Boot、YOLO、微信小程序等
2. 封面/前置信息里的“专业：xxx”
3. 论文题目
4. 原始相对路径或文件名
5. 正文前 5000 字
6. 仍识别不到时归入 未识别/其他类 / 未识别方向
```

`sanitize_report.csv` 是脱敏统计报告；`sanitized_mapping.csv` 是内部追溯映射表，包含原始相对路径、脱敏后路径、题目、学科大类、专业或方向、技术路线和质量状态等信息。mapping 表不要喂给智能体训练或分析，只用于内部审计和回查。

最终归档识别由 `src/paper_cleaning/subject_taxonomy.py` 和 `src/paper_cleaning/tech_classifier.py` 维护。

本机小样本命令：

```powershell
$env:PYTHONPATH = "cleaning-pipeline"
python -m src.paper_cleaning.sanitize --input output/raw_text --output output/sanitized_text --report output/sanitize_report.csv --mapping output/sanitized_mapping.csv --limit 20
```

NAS/Docker 命令示例：

```bash
export PYTHONPATH=/app/cleaning-pipeline
python -m src.paper_cleaning.sanitize \
  --input /data/raw_text \
  --output /data/sanitized_text \
  --report /data/output/sanitize_report.csv \
  --mapping /data/output/sanitized_mapping.csv
```

## 第一阶段一键流水线

`run_stage1.py` 会按顺序运行：

```text
1. archive_extract  展开 zip/rar/7z 中的文档
2. inventory        盘点普通文档和解压后的文档
3. extract_text     提取 docx/pdf/txt 文本，doc 标记 unsupported
4. sanitize         删除封面敏感字段，按学科大类/专业或方向输出 txt
5. quality_filter   基础质量粗筛
6. subject/tech classification  回写 mapping，输出 subject 和 tech 报告
```

本机小样本命令：

```powershell
$env:PYTHONPATH = "cleaning-pipeline"
python -m src.paper_cleaning.run_stage1 --input sample_small --output output --limit 20 --batch-size 10 --log logs/stage1.log
```

NAS/Docker 运行：

```bash
docker compose run --rm stage1
```

容器内等价命令：

```bash
python -m src.paper_cleaning.run_stage1 \
  --input /data/input \
  --output /data/output \
  --log /data/logs/stage1.log \
  --batch-size 100 \
  --resume
```

NAS 长时间运行建议：

```bash
python -m src.paper_cleaning.run_stage1 \
  --input /data/input \
  --output /data/output \
  --log /data/logs/stage1.log \
  --batch-size 100 \
  --resume
```

`--batch-size` 控制每批进入 `extract_text -> text_hash 去重 -> sanitize` 的文件数，避免一次性处理全量。中断后再次执行同一条命令并保留 `--resume`，脚本会根据 `sanitized_mapping.csv` 和 `_stage1_tmp/text_hash_index.csv` 跳过已处理文件，继续后续批次。

默认情况下，压缩包解压文件、raw_text、批次目录、text_hash 状态等中间内容都放在：

```text
output/_stage1_tmp/
```

完整成功后会自动清理 `_stage1_tmp`，最终只保留脱敏文本、映射表和 CSV 报告。如果正在排查问题，可以临时加 `--keep-intermediate` 保留中间文件；NAS 正式运行不建议长期保留。

## Stage1 输出结构

```text
output/
  archive_report.csv
  inventory.csv
  inventory_unique.csv
  dedupe_report.csv
  extract_text_report.csv
  extract_text_report_unique.csv
  sanitize_report.csv
  sanitized_mapping.csv
  sanitized_text/
    经管财会类/
      财务管理/
        FM_000001.txt
    计算机软件与数据类/
      Java管理系统/
        JAVA_000001.txt
    未识别_其他类/
      未识别方向/
        UNK_000001.txt
  quality_report.csv
  subject_classification_report.csv
  tech_classification_report.csv
  tech_group_index.csv
```

`output/_stage1_tmp/` 是运行中的临时目录，成功完成后默认清理，不作为最终产物保存。

## 技术路线分类

专业或方向归档目录保持不变：

```text
output/sanitized_text/{学科大类}/{专业或方向}/xxx.txt
```

技术路线是额外的第二套分类结果，不会再复制一份 `sanitized_text`，避免浪费 NAS 空间。Stage1 会新增两个 CSV：

```text
output/subject_classification_report.csv
output/tech_classification_report.csv
output/tech_group_index.csv
```

`tech_classification_report.csv` 字段包括：

```text
file_id, detected_subject_group, detected_major_or_direction, detected_title,
tech_type, tech_subtype, tech_keywords, tech_detect_source,
tech_confidence, tech_reason, sanitized_text_path
```

`tech_group_index.csv` 是轻量索引，字段包括：

```text
tech_type, tech_subtype, file_id, sanitized_text_path,
detected_major_or_direction, detected_subject_group, detected_title
```

技术分类只处理 `quality_status = pass` 或 `review` 的文件，`reject` 文件不会进入技术路线分类。当前是规则和关键词识别，不调用大模型。词典在：

```text
src/paper_cleaning/tech_taxonomy.py
src/paper_cleaning/tech_classifier.py
```

已支持 Java 管理系统、Python 项目、微信小程序、Android App、Web 前端、单片机、PLC、LabVIEW、物联网、数据库系统等项目技术类型。

## 压缩包处理说明

`archive_extract.py` 会扫描 `.zip`、`.rar`、`.7z`。当前环境没有 rar/7z 依赖时会在 `archive_report.csv` 记录 `unsupported`，不会让流程崩溃。zip 会展开其中的 `.doc`、`.docx`、`.pdf`、`.txt`，最多递归 2 层压缩包。加密、损坏或无法读取的压缩包会记录为 `error`。

## 未识别专业

专业识别依次尝试：

```text
封面/前置信息 “专业：xxx”
原始路径或文件名中的专业关键词
正文前 3000 字中的专业关键词
未识别专业
```

识别不到时文件进入：

```text
output/sanitized_text/未识别专业/UNK_000001.txt
```

## quality_status 含义

```text
pass              基础质量正常
review            需要人工复核，例如缺少摘要或参考文献
reject            明显低质量，例如字数太少、乱码严重、没有正文
reject_duplicate  重复文件
```

## mapping 表作用

`sanitized_mapping.csv` 是内部追溯表，记录脱敏编号、来源类型、原始相对路径、压缩包路径、压缩包内部路径、raw text 路径、脱敏输出路径、识别专业和题目。它不进入训练文本，不喂给智能体，只用于审计、抽查和回查原始数据来源。
## 重复文章去重

Stage1 现在包含两层去重，且只对后续流程放行第一次出现的文章：

```text
1. 文件级去重：inventory 后按 sha256 判断完全相同文件，保留第一份，后续记录为 duplicate_skipped。
2. 文本级去重：extract_text 后按 raw_text 的规范化 text_hash 判断正文重复，保留第一份，后续记录为 duplicate_text_skipped。
```

重复文章不会再次生成 `sanitized_text`，不会进入 `quality_report.csv`，也不会进入后续分类、知识库或资料包。原始文件始终不删除、不移动、不覆盖。

去重报告：

```text
output/dedupe_report.csv
```

字段：

```text
file_id, duplicate_type, kept_file_id, original_relative_path,
archive_path, archive_inner_path, reason, status
```

其中 `duplicate_type=file` 表示 sha256 完全一致；`duplicate_type=text` 表示提取文本规范化后内容一致。`kept_file_id` 指向被保留并继续进入脱敏和质量筛选的那一份。
