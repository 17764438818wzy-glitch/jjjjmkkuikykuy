"""Count live HTTP requests across native batches without owning their tasks."""
import json
from pathlib import Path

import pytest


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding='utf8')


def batch(root, rid, workspaces, *, status='running', private=False):
    base = root/'runs'/'parent'/'private'/'workers'/'w' if private else root
    put(base/'runs'/rid/'manifest.json', dict(run_id=rid, status=status,
        active_items=[{'preextract_work_dir':str(w)} for w in workspaces]))


def request(work, name='single', status='request_in_flight', **extra):
    path = work/'model'/name/'request-state.json'
    put(path, dict(status=status, client_http_requests=1, started_at=100,
                   http_started_at=101, **extra))
    return path


def owner(root, rid, pid=123, created=10):
    put(root/'private/model_activity/owners'/(rid+'.json'),
        dict(schema='model-activity-owner/1', run_id=rid, pid=pid, create_time=created))


def reader(root, live=lambda pid, created: True):
    from model_activity import ModelActivityReader
    return ModelActivityReader(root, process_is_same=live)


def test_combines_two_current_and_six_background_requests_without_reading_payloads(tmp_path):
    current = tmp_path/'runs/current/private/paper'
    background = tmp_path/'runs/retry/private/paper'
    for n in range(2): request(current, str(n))
    for n in range(6): request(background, str(n))
    # Large/private request and response bodies are not inputs to this reader.
    (background/'model/request.json').write_text('not JSON', encoding='utf8')
    batch(tmp_path, 'current', [current]);batch(tmp_path, 'retry', [background])
    batch(tmp_path, 'duplicate-child', [background], private=True)
    owner(tmp_path, 'current');owner(tmp_path, 'retry')
    before = {p:p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    result = reader(tmp_path).read('current')
    assert result['total'] == 8 and result['current'] == 2 and result['background'] == 6
    assert result['unconfirmed'] == 0 and len(result['batches']) == 2
    assert before == {p:p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}


def test_shared_workspace_and_alias_are_counted_once_prefer_current_batch(tmp_path):
    work=tmp_path/'private/paper';request(work, 'part-002')
    batch(tmp_path, 'current', [work, work/'..'/'paper']);batch(tmp_path, 'retry', [work])
    owner(tmp_path, 'current');owner(tmp_path, 'retry')
    result=reader(tmp_path).read('current')
    assert (result['total'],result['current'],result['background']) == (1,1,0)


def test_completed_failed_cached_and_unstarted_requests_do_not_inflate_total(tmp_path):
    work=tmp_path/'private/paper'
    for state in ('response_complete','request_failed_or_uncertain','partial_response'):
        request(work, state, state)
    request(work, 'finished-intent', finished_at=110)
    path=request(work, 'not-sent');s=json.loads(path.read_text());s.pop('http_started_at');put(path,s)
    request(work, 'active')
    batch(tmp_path,'current',[work]);owner(tmp_path,'current')
    result=reader(tmp_path).read('current')
    assert result['total'] == 1 and result['waiting_to_send'] == 1


@pytest.mark.parametrize('have_owner,live',[(False,True),(True,False)])
def test_orphan_or_dead_process_records_are_unconfirmed(tmp_path,have_owner,live):
    work=tmp_path/'private/paper';request(work)
    batch(tmp_path,'current',[work])
    if have_owner:owner(tmp_path,'current')
    result=reader(tmp_path,lambda pid,created:live).read('current')
    assert result['total'] == 0 and result['unconfirmed'] == 1


def test_terminal_and_private_worker_batches_do_not_add_requests(tmp_path):
    work=tmp_path/'private/paper';request(work)
    batch(tmp_path,'stopped',[work],status='stopped');owner(tmp_path,'stopped')
    batch(tmp_path,'private',[work],private=True)
    assert reader(tmp_path).read()['total'] == 0


def test_process_identity_binds_creation_time_and_does_not_reuse_pid(tmp_path):
    work=tmp_path/'private/paper';request(work)
    batch(tmp_path,'current',[work]);owner(tmp_path,'current',pid=22,created=1234.25)
    calls=[]
    def same(pid,created):
        calls.append((pid,created));return created==1234.0
    result=reader(tmp_path,same).read()
    assert calls == [(22,1234.25)] and result['unconfirmed']==1


def test_json_being_replaced_reports_incomplete_instead_of_an_exact_zero(tmp_path):
    from model_activity import ModelActivityReadError
    p=tmp_path/'runs/current/manifest.json';put(p,{'run_id':'current','status':'running','active_items':[]})
    r=reader(tmp_path);assert r.read()['total']==0
    p.write_text('{',encoding='utf8')
    snapshot=r.read('current')
    assert snapshot['partial'] and not snapshot['current_complete']


def test_outside_workspace_is_refused_without_opening_it(tmp_path):
    from model_activity import ModelActivityReadError
    root=tmp_path/'data';outside=tmp_path/'outside';request(outside)
    batch(root,'current',[outside]);owner(root,'current')
    with pytest.raises(ModelActivityReadError):reader(root).read()


def test_owner_registration_is_best_effort_and_uses_real_process_identity(tmp_path):
    import os
    import psutil
    from model_activity import record_run_owner
    assert record_run_owner(tmp_path,'current')
    record=json.loads((tmp_path/'private/model_activity/owners/current.json').read_text())
    assert record['pid']==os.getpid() and record['create_time']==psutil.Process().create_time()
    bad=tmp_path/'not-a-directory';bad.write_text('original',encoding='utf8')
    assert record_run_owner(bad,'current') is False
    assert bad.read_text()=='original'


def test_ui_uses_background_reader_and_marks_old_total_unavailable_on_error(tmp_path, monkeypatch):
    import threading
    import time
    import tkinter as tk
    from types import SimpleNamespace
    from model_activity_ui import install_model_activity
    root=tk.Tk();root.withdraw()
    app=SimpleNamespace(root=root,store=SimpleNamespace(root=tmp_path),current={'run_id':'current'},
                        busy=True,processor=object(),task_kind='processing')
    import tkinter.ttk as ttk
    frame=ttk.Frame(root);frame.pack();app.panes=ttk.Frame(frame);app.panes.pack()
    processor=app.processor
    panel=install_model_activity(app)
    main=threading.get_ident();calls=[]
    class Reader:
        def read(self,current):
            calls.append(threading.get_ident());assert current=='current'
            assert threading.get_ident()!=main
            if len(calls)>1:raise ValueError('private provider data must not be displayed')
            return {'total':8,'current':2,'background':6,'unconfirmed':0,'waiting_to_send':0,'batches':[]}
    panel.reader=Reader()
    def pump(predicate):
        deadline=time.monotonic()+3
        while not predicate() and time.monotonic()<deadline:root.update();time.sleep(.01)
        assert predicate()
    try:
        panel.refresh();pump(lambda:'总数 8' in panel.summary.get())
        assert '本批 2' in panel.summary.get() and '后台 6' in panel.summary.get()
        panel.refresh();pump(lambda:'暂未更新' in panel.health.get())
        assert '暂不可用' in panel.summary.get() and 'private provider' not in panel.health.get()
        assert install_model_activity(app) is panel
        assert app.busy and app.processor is processor and app.task_kind=='processing'
    finally:
        panel.destroy();root.destroy()
