import json
from pathlib import Path

from engine import Processor, Store, scan_sources
from test_workbench import paper


def test_progress_reports_file_stages_and_persists_log(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    (source / "good.txt").write_text(paper(), encoding="utf-8")
    (source / "broken.docx").write_bytes(b"bad document")
    store = Store(tmp_path / "output")
    updates = []
    manifest = Processor(store).run(scan_sources(source), lambda state: updates.append(state.copy()))
    events = [state["progress"] for state in updates if "progress" in state]
    assert any(e["stage"] == "准备文件" and e["file"] == "good.txt" for e in events)
    assert any(e["stage"] == "提取正文" and e["file"] == "good.txt" for e in events)
    assert any(e["stage"] == "脱敏与分类" and e["file"] == "good.txt" for e in events)
    assert any(e["stage"] == "基础筛选" for e in events)
    assert any(e["level"] == "error" and "broken.docx" in e["file"] for e in events)
    assert events[-1]["stage"] == "任务结束"
    assert events[-1]["completed"] == 2
    assert manifest["status"] == "completed"
    path = store.runs / manifest["run_id"] / "progress.jsonl"
    persisted = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(persisted) == len(events)
    assert all(e["time"] and e["total"] == 2 for e in persisted)


def test_panel_updates_progress_errors_and_history(tmp_path):
    import tkinter as tk
    from app import Workbench
    root = tk.Tk()
    root.withdraw()
    try:
        app = Workbench(root, tmp_path)
        assert hasattr(app, "log_text"), "Missing persistent progress panel"
        app.show_progress({"time": "12:00:01", "stage": "格式转换", "file": "sample.doc", "message": "转换中", "level": "info", "completed": 1, "total": 3})
        assert "sample.doc" in app.progress_detail.get()
        assert float(app.progress_bar["value"]) == 1
        assert float(app.progress_bar["maximum"]) == 3
        app.show_progress({"time": "12:00:03", "stage": "提取失败", "file": "broken.docx", "message": "文件损坏", "level": "error", "completed": 1, "total": 3})
        assert "文件损坏" in app.log_text.get("1.0", "end")
        assert app.log_text.tag_ranges("error")
        app.major.set("全部专业")
        app.tabs.select(app.rag_tab)
        assert app.progress_panel.winfo_manager(), "Panel should remain outside the tabs"
        manifest = {"run_id": "history_test", "status": "completed", "message": "历史任务完成",
                    "processed_sources": 3, "source_count": 3, "started_at": "2026-09-14T12:00:00"}
        app.store.save(manifest)
        app.load_progress(manifest)
        assert "旧批次没有逐文件进度日志" in app.log_text.get("1.0", "end")
        path = app.store.runs / manifest["run_id"] / "progress.jsonl"
        path.write_text(json.dumps({"stage": "提取失败", "file": "earlier.docx", "message": "历史错误", "level": "error"}) + "\n", encoding="utf-8")
        app.load_progress(manifest)
        assert "earlier.docx" in app.log_text.get("1.0", "end")
        assert "旧批次没有" not in app.log_text.get("1.0", "end")
        assert app.log_text.tag_ranges("error")
    finally:
        root.destroy()
