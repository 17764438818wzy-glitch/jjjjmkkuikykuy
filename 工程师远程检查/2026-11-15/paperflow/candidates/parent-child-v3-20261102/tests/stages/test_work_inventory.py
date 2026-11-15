"""List only files in one paper's isolated work area for the delivery owner."""
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_inventory_is_scoped_to_the_paper_and_lists_files_not_directories(tmp_path):
    from paperflow_v3.stages.work_inventory import stage_workspace, stage_work_files

    identity = {'task_id': 'parent-1', 'group_id': 'paper-1'}
    root = tmp_path / 'paperflow-v3' / identity['task_id'] / identity['group_id']
    body = root / 'body' / 'model' / 'response.txt'
    attachment = root / 'attachment' / 'model' / 'request.json'
    output = root / 'postprocessing' / 'publication' / 'manifest.json'
    for path in (body, attachment, output):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(path.name, encoding='utf8')
    other = root.parent / 'other-paper' / 'do-not-touch.txt'
    other.parent.mkdir()
    other.write_text('other paper', encoding='utf8')
    settings = SimpleNamespace(values={'data_root': str(root)})

    workspace = stage_workspace(settings, identity, root / 'postprocessing')
    result = stage_work_files(workspace, [body.parent, attachment.parent, output])
    assert set(map(Path, result)) == {body, attachment, output}
    assert other not in map(Path, result)
    assert all(Path(path).is_file() for path in result)


def test_inventory_rejects_unscoped_output_or_historical_work(tmp_path):
    from paperflow_v3.stages.work_inventory import stage_workspace, stage_work_files

    identity = {'task_id': 'parent-1', 'group_id': 'paper-1'}
    root = tmp_path / 'paperflow-v3' / identity['task_id'] / identity['group_id']
    root.mkdir(parents=True)
    settings = SimpleNamespace(values={'data_root': str(root)})
    with pytest.raises(ValueError, match='独占'):
        stage_workspace(SimpleNamespace(values={'data_root': str(tmp_path)}), identity, root / 'output')
    with pytest.raises(ValueError, match='独占'):
        stage_workspace(settings, identity, tmp_path / 'shared-output')
    with pytest.raises(ValueError, match='独占'):
        stage_work_files(root, [tmp_path / 'other-paper-history'])
