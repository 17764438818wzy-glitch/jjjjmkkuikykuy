# BASELINE

冻结时间：2026-09-19 18:56:19 +08:00（补正后）

| 角色 | 绝对路径 | tag | commit | pre-fix 创建时工作区 | 版本常量 |
|---|---|---|---|---|---|
| 主程序 | E:\uv\ragflow_upstream | pre-fix | 6ed0eccd80df1c6428a9b0e1728317c43b5726fb | clean（打 tag 前已核验） | automatic-v2.6.6-knowledge / evidence-v3-media |
| 外部依赖 2 | E:\uv\src\paper_pipeline | pre-fix | dccf459ac23b1bdce2f251eec945585d2869db4b | clean（打 tag 前已核验） | automatic-v2.6.6-knowledge / evidence-v3-media |
| 外部依赖 1 | E:\dity\脱敏分类脚本\脱敏分类脚本\cleaning-pipeline | pre-fix | d921eb03741395ca6241537efe5ebf32bf73d789 | clean（打 tag 前已核验） | automatic-v2.6.6-knowledge / evidence-v3-media |

## 闸门度量工具（修复前后必须保持相同）

- 统一脚本：`baseline/诊断_通过率.py`
- 版本：`source-path-dedup-1.0.0`
- 调用：`C:\Python314\python.exe baseline\诊断_通过率.py --root "E:\uv\ragflow_upstream" --out baseline\通过率_修复前.md`
- 规则：只读扫描 `data/runs/*/manifest.json`；按 `source_path.casefold()` 去重；按 `run_id` 升序扫描，重复来源保留最后一条。修复后报告必须使用同一脚本和版本。

## 证据来源

- `通过率_修复前.md`：由统一脚本生成，按 source_path 去重，结果 90/206 = 43.7%。
- `测试_修复前.txt`：从 `pre-fix` 标签建立的干净 worktree 运行的完整逐条输出；权威结果为 561 passed、1 failed，唯一失败已归为环境 fixture 缺失。
- `raw_测试_pre-fix.txt`：上述干净检出的完整 pytest 原始输出，文件头含解释器绝对路径、版本、命令、commit 和空 status。
- `历史_测试_修复前_556通过6失败.txt`：旧的 556/6 记录，仅作历史留档，已在 `测试_修复前.txt` 中标注作废。
- `pip_freeze_修复前.txt`：当前 Python 解释器的 `python -m pip freeze`。
- `python_环境_修复前.txt`：Python 版本、解释器绝对路径和冻结时间。
- `ai_settings_脱敏.json`：`data/ai_settings.json` 的副本；所有 api_key/secret/token/password 字段均为空。
- `验证样本_10篇.md`：按 reason_kind/failure_position 分层抽样并校验源文件存在。
- `并发度_修复前.json`：`measure_run.py --json data/runs` 的只读输出。
- `requirements.txt`：由用户提供的环境信息生成；Windows 专用候选项已注释。

## 桌面版核验

- 快捷方式：`C:\Users\Administrator\Desktop\论文知识库预处理.lnk`
- 解析目标：`C:\Python314\pythonw.exe "E:\uv\ragflow_upstream\launch.pyw"`，工作目录 `E:\uv\ragflow_upstream`。
- 实际启动后 `pythonw.exe` 窗口标题为“论文知识库预处理”，进程响应正常。
- `C:\Python314\python.exe app.py --smoke-test` 通过：6 个 tab、空闲、未启动处理。
- 同一 Tk Workbench 临时扫描核验通过：6 个 tab，扫描本地 `sample.txt` 得到 1 行结果。
- `C:\Python314\tcl\tk8.6\tk.tcl` 存在，`import tkinter` 成功（Tk 8.6）；本次权威测试失败原因不是 Tk 缺失。

## 修复线保存

- `git stash list`：空。
- 修复线已转为分支：`fix/p0-1-concurrency-default`、`fix/p0-2-fallback-channel`、`fix/p0-3-schema-retry`、`fix/p0-4-repair-prompt`、`fix/p0-5-no-whole-paper-reject`，以及基线期间产生的 `fix/repair-line-after-baseline`、`fix/repair-line-after-branch-note`、`fix/repair-line-after-classification`、`fix/repair-line-after-desktop-check`、`fix/repair-line-pre-fix-snapshot`。
- 无法直接拆分的两份快照另有 tag：`repair-stash-before-pristine`、`repair-stash-after-baseline`；它们保留原 stash 对象及其未跟踪文件父对象。
