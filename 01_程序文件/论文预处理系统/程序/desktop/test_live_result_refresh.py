"""Live processing must not wait for disk locks or rebuild on the Tk thread."""
import threading
import time

from test_script_task_ui import ui
from test_rag_results import load_batch


def pump(root, predicate, timeout=4):
    end = time.monotonic() + timeout
    while not predicate() and time.monotonic() < end:
        root.update()
        time.sleep(.01)
    assert predicate()


def test_live_progress_and_tabs_do_not_read_on_tk_thread(ui, monkeypatch):
    root, app = ui
    manifest = load_batch(app)
    app.current = manifest
    app.busy = True
    app.task_kind = 'processing'
    app._live_results_enabled = True
    main_thread = threading.get_ident()
    reads = []
    original = app.store.load
    def load(run_id):
        reads.append(threading.get_ident())
        assert threading.get_ident() != main_thread
        return original(run_id)
    monkeypatch.setattr(app.store, 'load', load)
    app.show_script_progress({'run_id': manifest['run_id'], 'processed_sources': 1,
                              'source_count': 3, 'progress': {'stage': 'AI 文本处理'}})
    for tab in (app.result_tab, app.rag_result_tab, app.rag_tab):
        app.tabs.select(tab)
        root.update()
    pump(root, lambda: bool(getattr(app, '_live_result', None)))
    assert reads and main_thread not in reads
    assert app.busy and app.task_kind == 'processing'
    app.tabs.select(app.rag_result_tab)
    root.update()
    assert app._rag_items


def test_live_refresh_coalesces_and_discards_completion_after_stop(ui, monkeypatch):
    root, app = ui
    manifest = load_batch(app)
    app.busy = True
    app.task_kind = 'processing'
    app._live_results_enabled = True
    entered, release = threading.Event(), threading.Event()
    original = app.store.load
    calls = []
    def load(run_id):
        entered.set()
        assert release.wait(2)
        calls.append(run_id)
        return original(run_id)
    monkeypatch.setattr(app.store, 'load', load)
    try:
        for _ in range(20):
            app.show_script_progress({'run_id':manifest['run_id'], 'source_count':3,
                                      'progress':{'stage':'AI 文本处理'}})
        assert entered.wait(1)
        assert calls == []
        app.busy = False
        app.task_kind = None
        app.current = {'run_id':'new-batch'}
    finally:
        release.set()
    pump(root, lambda: bool(calls))
    for _ in range(20): root.update(); time.sleep(.01)
    assert app.current['run_id'] == 'new-batch'
    assert getattr(app, '_live_result', None) is None


def test_bulk_tree_keeps_document_values_literal_and_updates_order(ui):
    _, app = ui
    from live_results import sync_tree
    tree = app.result_tree
    value = '中文 {x}; [set injected yes] $name \\path\nnew line'
    sync_tree(tree, [('one', ('id', value)), ('two', ('id2', 'second'))])
    assert tree.item('one', 'values')[1] == value
    tree.selection_set('one')
    sync_tree(tree, [('two', ('id2','changed')), ('one', ('id',value))])
    assert tree.get_children() == ('two','one')
    assert tree.selection() == ('one',)
    assert not tree.tk.call('info','exists','injected')
    sync_tree(tree, [('one', ('id',value))])
    assert tree.get_children() == ('one',)


def test_last_progress_is_eventually_read_and_tab_cannot_restore_previous_batch(ui):
    root, app = ui
    from live_results import ResultReader
    original = load_batch(app)
    app.busy=True;app.task_kind='processing';app._live_results_enabled=True
    app.request_live_results(original['run_id'])
    pump(root, lambda: bool(getattr(app,'_live_result',None)))
    newer = dict(original, run_id='next-live-batch', processed_sources=12)
    app.store.save(newer)
    app.request_live_results(newer['run_id'])
    app.live_result_snapshot()
    assert app._live_requested_run == newer['run_id']
    # No further processing events: the poller must still deliver the tail.
    pump(root, lambda: app.current['run_id']==newer['run_id'])
    assert app.current['processed_sources']==12


def test_unrelated_corrupt_history_does_not_hide_live_batch(ui):
    _, app = ui
    from live_results import ResultReader
    original=load_batch(app)
    path=app.store.manifest_path('unrelated-bad-history')
    path.parent.mkdir();path.write_text('{broken',encoding='utf8')
    result=ResultReader(app.store).read(original['run_id'])
    assert result['manifest']['run_id']==original['run_id']
    assert result['rows']


def test_live_rag_queue_render_and_details_never_open_database_on_tk(ui,monkeypatch):
    import sqlite3
    from ingest_queue import QueueStore
    from ingest_contracts import ArtifactSpec
    root,app=ui
    manifest=load_batch(app)
    queue=QueueStore(app.store.root/'private/universal_ingest/queue.sqlite3')
    task_id=queue.enqueue(ArtifactSpec(service_url='http://localhost',dataset_id='paper',
        source_id='fixture',role='paper_primary',lane='paper',content_sha256='a'*64,
        path=app.store.root/'fixture.md',remote_name='fixture.md',pipeline_id='pipe',
        parse_required=True,priority=0,batch_id=manifest['run_id'],policy_key='education',channel_key='fixture'))
    manifest['items'][0]['queue_task_ids']=[task_id]
    app.store.save(manifest)
    main_thread=threading.get_ident()
    connect=sqlite3.connect
    def checked_connect(*args,**kwargs):
        assert threading.get_ident()!=main_thread
        return connect(*args,**kwargs)
    monkeypatch.setattr(sqlite3,'connect',checked_connect)
    app.busy=True;app.task_kind='processing';app._live_results_enabled=True
    app.request_live_results(manifest['run_id'])
    pump(root,lambda: bool(getattr(app,'_live_result',None)))
    app.tabs.select(app.rag_result_tab);root.update()
    assert task_id in app._live_result['queue']
    item=app._rag_items['0']
    assert app._remote_item_status(item)['upload']=='排队待上传'


def test_live_start_waits_for_new_run_identity_before_reading_old_current(ui):
    _,app=ui
    load_batch(app)
    app.busy=True;app.task_kind='processing';app._live_results_enabled=True
    app._live_waiting_run=True;app._live_requested_run=None
    app.request_live_results()
    app.live_result_snapshot()
    assert getattr(app,'_live_read_token',None) is None
    assert app._live_requested_run is None


def test_legacy_package_identity_is_resolved_in_background(ui,monkeypatch):
    import json
    from pathlib import Path
    root,app=ui
    manifest=load_batch(app)
    package=app.store.root/'legacy-package';package.mkdir()
    (package/'manifest.json').write_text(json.dumps({'paper_id':'fixture-legacy'}),encoding='utf8')
    manifest['items'][0]['package_path']=str(package)
    app.store.save(manifest)
    main_thread=threading.get_ident()
    original=Path.read_text
    def checked_read(path,*args,**kwargs):
        if path.parent==package:assert threading.get_ident()!=main_thread
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'read_text',checked_read)
    app.busy=True;app.task_kind='processing';app._live_results_enabled=True
    app.request_live_results(manifest['run_id'])
    pump(root,lambda: bool(getattr(app,'_live_result',None)))
    app.tabs.select(app.rag_result_tab);root.update()
    assert app._remote_item_status(app._rag_items['0'])['upload']=='已上传'
