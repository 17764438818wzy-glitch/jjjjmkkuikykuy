"""Bind existing paper-processing scripts to this task's own local work root."""

from pathlib import Path


def folder_settings(settings, task_id, group_id, paper):
    root = Path(settings.values["data_root"]).resolve() / "paperflow-v3" / str(task_id) / group_id
    for row in [paper["body"], *paper["attachments"]]:
        history = row.get("history") or {}
        paths = list(history.get("work_dirs", []))
        paths.extend(value for key, value in history.items() if key in ("body_work", "attachment_work") and value)
        if any(not Path(path).resolve().is_relative_to(root) for path in paths):
            raise ValueError("资料绑定了其他历史工作目录，需核对后再处理")
    return type(settings)({**settings.values, "data_root": str(root)})
