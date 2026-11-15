"""External progress is read without owning or changing a processing run."""
import threading
import time

import pytest

from test_script_task_ui import ui


@pytest.mark.parametrize('value', [None, '', 0, 0.0])
def test_unknown_evidence_time_is_not_shown_as_epoch(value):
    from preextract_monitor_ui import display_time
    assert display_time(value) == '暂无记录'


def test_real_reader_snapshot_displays_local_time_and_safe_labels(ui, tmp_path):
    import re
    from test_preextract_monitor import paper, put, read
    root, _, work, _ = paper(tmp_path)
    put(work/'model/request-state.json', dict(status='request_in_flight', started_at=200))
    result = read(root)
    _, app = ui
    app._monitor_batch_id = 'trial'
    token = app._monitor_token = object()
    app.finish_preextract_monitor((token, 'trial', [], result, None))
    values = app.monitor_tree.item(result['rows'][0]['id'], 'values')
    assert '待响应' in values[4] and '未投递' in values[5]
    assert re.fullmatch(r'\d{2}-\d{2} \d{2}:\d{2}:\d{2}', values[6])


def pump(root, predicate, timeout=4):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        root.update()
        time.sleep(.01)
    assert predicate()


def snapshot(label='封包检查完成，待入库'):
    return dict(batch_id='trial', label='试跑', updated_at=time.time(),
                counts=dict(total=1, packaged=1, verified=0, attention=0, pending=0),
                rows=[dict(id='paper-1', number=1, name='论文.docx', stage='packaged',
                           label=label, attempt='原始尝试', attempts=1, model_status='回答完整',
                           delivery_status='待入库', verified_targets=0, total_targets=2,
                           updated_at=time.time(), detail='已有封包，尚无读回回执',
                           work_dir='', source_path='')])


def test_monitor_is_in_existing_history_tab_and_never_changes_run(ui, monkeypatch):
    root, app = ui
    assert hasattr(app, 'monitor_tree')
    assert len(app.tabs.tabs()) == 6
    original = app.current = {'run_id': 'local-owned'}
    app._monitor_batch_id = 'trial'
    token = app._monitor_token = object()
    app.finish_preextract_monitor((token, 'trial', [], snapshot(), None))
    assert app.monitor_tree.item('paper-1', 'values')[2] == '封包检查完成，待入库'
    assert app.current is original
    assert not app.busy and app.task_kind is None
    assert '入库并读回 0' in app.monitor_summary.get()
    assert app.store.history() == []


def test_monitor_keeps_last_rows_and_marks_stale_on_read_error(ui):
    _, app = ui
    assert hasattr(app, 'monitor_tree')
    app._monitor_batch_id = 'trial'
    token = app._monitor_token = object()
    app.finish_preextract_monitor((token, 'trial', [], snapshot(), None))
    token = app._monitor_token = object()
    app.finish_preextract_monitor((token, 'trial', [], None, 'JSONDecodeError'))
    assert app.monitor_tree.get_children() == ('paper-1',)
    assert '暂未更新' in app.monitor_health.get()
    assert '上次' in app.monitor_health.get()


def test_monitor_background_refresh_coalesces_and_keeps_selection(ui):
    root, app = ui
    assert hasattr(app, 'monitor_tree')
    entered, release = threading.Event(), threading.Event()
    main = threading.get_ident()
    calls = []
    class Reader:
        def discover(self):
            assert threading.get_ident() != main
            return [dict(id='trial', label='试跑', count=1)]
        def read(self, batch_id):
            assert threading.get_ident() != main
            calls.append(batch_id)
            entered.set()
            assert release.wait(2)
            return snapshot()
    app._monitor_reader = Reader()
    app._monitor_batch_id = 'trial'
    app.tabs.select(app.history_tab)
    for _ in range(20):
        app.request_preextract_monitor(force=True)
    assert entered.wait(1)
    assert calls == ['trial']
    release.set()
    pump(root, lambda: bool(app.monitor_tree.get_children()))
    app.monitor_tree.selection_set('paper-1')
    token = app._monitor_token = object()
    app.finish_preextract_monitor((token, 'trial', [], snapshot('全部专业读回完成'), None))
    assert app.monitor_tree.selection() == ('paper-1',)
    assert app.monitor_tree.item('paper-1', 'values')[2] == '全部专业读回完成'
    assert not app.busy


def test_late_monitor_response_cannot_replace_new_batch(ui):
    _, app = ui
    assert hasattr(app, 'monitor_tree')
    app._monitor_batch_id = 'new'
    token = app._monitor_token = object()
    app.finish_preextract_monitor((token, 'trial', [], snapshot(), None))
    assert app.monitor_tree.get_children() == ()


def test_monitor_startup_preserves_external_running_batch(tmp_path, monkeypatch):
    import json
    import tkinter as tk
    import app as module
    run = tmp_path / 'runs' / 'external'
    run.mkdir(parents=True)
    path = run / 'manifest.json'
    path.write_text(json.dumps(dict(run_id='external', status='running', started_at='test',
                                    items=[], inputs=[], events=[])), encoding='utf8')
    before = path.read_bytes()
    instances = []
    original = module.Workbench
    class Observed(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            instances.append(self)
    monkeypatch.setattr(module, 'Workbench', Observed)
    def mainloop(root):
        try:
            assert instances[0].tabs.select() == str(instances[0].history_tab)
            assert not instances[0].busy
            assert path.read_bytes() == before
        finally:
            for timer in root.tk.call('after', 'info'):
                root.after_cancel(timer)
            root.destroy()
    monkeypatch.setattr(tk.Tk, 'mainloop', mainloop)
    module.main(['--monitor', '--data-dir', str(tmp_path)])
    assert path.read_bytes() == before
