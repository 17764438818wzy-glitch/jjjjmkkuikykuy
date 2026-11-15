"""Collect concrete files from the current paper's isolated work area."""
from pathlib import Path


def stage_workspace(settings, identity, output_root):
    root = Path(settings.values['data_root']).resolve()
    if (root.name != str(identity['group_id']) or root.parent.name != str(identity['task_id'])
            or root.parent.parent.name != 'paperflow-v3'
            or not Path(output_root).resolve().is_relative_to(root)):
        raise ValueError('前处理和后处理必须使用当前小工单的独占工作目录')
    return root


def stage_work_files(root, required_paths):
    """Return file paths only; no file or directory is removed here."""
    root = Path(root).resolve(strict=True)
    for path in required_paths:
        path = Path(path)
        if not path.exists() or not path.resolve().is_relative_to(root):
            raise ValueError('本篇工作产物不在独占工作目录内，需核对')
    files = []
    for path in root.rglob('*'):
        if path.is_file():
            if not path.resolve().is_relative_to(root):
                raise ValueError('本篇工作文件指向独占工作目录之外，需核对')
            files.append(str(path.absolute()))
    return sorted(files)
