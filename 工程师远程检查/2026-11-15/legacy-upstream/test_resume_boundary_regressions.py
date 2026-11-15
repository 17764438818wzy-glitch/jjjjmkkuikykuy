"""Real resume boundaries: source identity and paper-only delivery."""
import pytest

from classification_rules import resolve_identity
from test_script_task_ui import ui, configure


@pytest.mark.parametrize('path', [
    r'C:\samples\工业工程\主修-机械自动化学院-工业工程-论文.docx',
    r'C:\samples\机械自动化学院\论文.docx',
    r'C:\samples\化学与工程学院\未知专业\论文.docx',
])
def test_institution_path_cannot_invent_a_source_major(path):
    result = resolve_identity('摘要\n正文', path, '论文')
    assert result['major'] == '未识别方向'
    assert result['classification']['source'] == 'unknown'


def test_declared_directory_and_real_title_still_classify():
    declared = resolve_identity('摘要\n正文',
        r'C:\samples\会计学\机械自动化学院-论文.docx', '论文')
    assert declared['major'] == '会计学'
    assert declared['classification']['locked']
    topic = resolve_identity('摘要\n正文',
        r'C:\samples\机械自动化学院\论文.docx', '基于PLC的恒压供水系统设计')
    assert topic['major'] == 'PLC'


def start_without_external_work(app, monkeypatch):
    configure(app)
    app.image_dataset.set('')
    monkeypatch.setattr('app.confirm_serial_start', lambda *args: True)
    # Stop at the asynchronous boundary; all startup/configuration code is real.
    monkeypatch.setattr(app, 'async_task', lambda *args, **kwargs: None)
    app.start_automatic([{'path': 'synthetic.txt', 'name': 'synthetic.txt'}])


def test_paper_only_start_keeps_automatic_handoff_enabled(ui, monkeypatch):
    _, app = ui
    start_without_external_work(app, monkeypatch)
    assert app.processor.config['task_datasets'] == {
        'paper': {'id': 'paper', 'name': '论文库'}, 'image': {}}
    assert app.processor.config['auto_upload']
    assert app.processor.rag_client is not None


def test_paper_only_start_still_checks_rag_credentials(ui):
    _, app = ui
    configure(app)
    app.image_dataset.set('')
    app.key.set('')
    assert app.task_setup_problem()


def test_explicit_defer_keeps_automatic_handoff_disabled(ui, monkeypatch):
    _, app = ui
    configure(app)
    app.defer_task_upload()
    monkeypatch.setattr('app.confirm_serial_start', lambda *args: True)
    monkeypatch.setattr(app, 'async_task', lambda *args, **kwargs: None)
    app.start_automatic([{'path': 'synthetic.txt', 'name': 'synthetic.txt'}])
    assert not app.processor.config['auto_upload']
    assert app.processor.rag_client is None
