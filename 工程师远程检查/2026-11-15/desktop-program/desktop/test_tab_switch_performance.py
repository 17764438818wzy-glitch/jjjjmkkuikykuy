"""Real notebook switches must reuse unchanged data and preserve live updates."""
import copy
import json

from test_rag_results import load_batch
from test_script_task_ui import ui
from script_task import task_snapshot


def switch(root, app, tab):
    app.tabs.select(tab)
    root.update()


def test_repeated_tabs_do_not_read_history_or_rebuild_rows(ui, monkeypatch):
    root, app = ui
    load_batch(app)
    switch(root, app, app.result_tab)
    switch(root, app, app.rag_result_tab)
    calls = []
    for tree in (app.result_tree, app.rag_result_tree):
        for name in ('insert', 'delete'):
            original = getattr(tree, name)
            monkeypatch.setattr(tree, name, lambda *a, _name=name, _original=original, **kw:
                                (calls.append(_name), _original(*a, **kw))[1])
    for name in ('load', 'history'):
        original = getattr(app.store, name)
        monkeypatch.setattr(app.store, name, lambda *a, _name=name, _original=original, **kw:
                            (calls.append(_name), _original(*a, **kw))[1])
    app.result_tree.selection_set('0')
    app.rag_result_tree.selection_set('1')
    for tab in (app.input_tab, app.result_tab, app.rag_result_tab, app.result_tab):
        switch(root, app, tab)
    assert calls == []
    assert app.result_tree.selection() == ('0',)
    assert app.rag_result_tree.selection() == ('1',)


def test_tab_reads_updated_prior_receipt_and_current_result(ui):
    root, app = ui
    original = load_batch(app)
    retry = {**copy.deepcopy(original), 'run_id': 'retry-switch', 'retry_of': original['run_id'],
             'items': [], 'inputs': [], 'events': []}
    app.store.save(retry)
    app.current = retry
    switch(root, app, app.rag_result_tab)
    assert app._rag_states['0']['process'] == '已完成'
    switch(root, app, app.input_tab)
    original['items'][0]['uploads']['http://localhost|paper']['ragflow_status'] = 'failed'
    app.store.save(original)
    switch(root, app, app.rag_result_tab)
    assert app._rag_states['0']['process'] == '处理失败'
    switch(root, app, app.input_tab)
    retry['items'] = [{**copy.deepcopy(original['items'][0]), 'title': '更新的论文题名'}]
    app.store.save(retry)
    switch(root, app, app.result_tab)
    assert app._ai_result_items['0']['title'] == '更新的论文题名'


def test_first_result_tab_reads_current_record_saved_after_it_was_loaded(ui):
    root,app=ui
    manifest=load_batch(app)
    latest=copy.deepcopy(manifest)
    latest['items'][0]['title']='首次切换前已经保存的新题名'
    app.store.save(latest)
    switch(root,app,app.result_tab)
    assert app._ai_result_items['0']['title']=='首次切换前已经保存的新题名'


def test_search_refresh_cannot_label_old_object_as_latest_saved_version(ui):
    root,app=ui
    manifest=load_batch(app)
    switch(root,app,app.result_tab)
    switch(root,app,app.input_tab)
    latest=copy.deepcopy(manifest)
    latest['items'][0]['title']='后台已经保存的新题名'
    app.store.save(latest)
    app.ai_result_search.set('0.docx')
    switch(root,app,app.result_tab)
    assert app._ai_result_items['0']['title']=='后台已经保存的新题名'


def test_saved_version_is_captured_before_read_when_writer_changes_it(ui,monkeypatch):
    _,app=ui
    manifest=load_batch(app)
    latest=copy.deepcopy(manifest)
    latest['items'][0]['title']='并发保存的新题名'
    original_load=app.store.load
    raced=[]
    def load_and_save(run_id):
        value=original_load(run_id)
        if run_id==manifest['run_id'] and not raced:
            raced.append(True)
            app.store.save(latest)
        return value
    monkeypatch.setattr(app.store,'load',load_and_save)
    app.result_snapshot(refresh=False)
    current=app.result_snapshot(refresh=False)
    assert current['items'][0]['title']=='并发保存的新题名'


def test_failed_batch_load_clears_previous_rows_and_selection(ui):
    root,app=ui
    manifest=load_batch(app)
    switch(root,app,app.result_tab)
    app.result_tree.selection_set('0')
    switch(root,app,app.input_tab)
    app.current={**copy.deepcopy(manifest),'run_id':'broken-switch','retry_of':'missing-parent'}
    app.store.save(app.current)
    switch(root,app,app.result_tab)
    assert app.result_tree.get_children()==()
    assert app.result_tree.selection()==()
    assert app._ai_result_items=={}
    assert 'broken-switch' in app.ai_batch_label.get()
    assert app.ai_retry_selected_button.instate(['disabled'])


def test_external_receipt_reload_error_clears_rag_rows_and_reports_failure(ui):
    _,app=ui
    manifest=load_batch(app)
    app.render_rag_results()
    receipt=app.store.root/'private'/'education_full_ingest'/manifest['run_id']/'receipts.json'
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({'batch_id':manifest['run_id'],'primary':{},'images':{}}),encoding='utf-8')
    app.store.manifest_path(manifest['run_id']).write_text('{broken',encoding='utf-8')
    app.render_rag_results()
    assert app.rag_result_tree.get_children()==()
    assert app._rag_items=={}
    assert app._rag_states=={}
    assert '无法载入批次' in app.rag_result_summary.get()
    assert app.rag_import_button.instate(['disabled'])


def test_script_progress_changes_one_row_without_deleting_entire_table(ui, monkeypatch):
    root, app = ui
    original = load_batch(app)
    app.show_script_results(original)
    root.update()
    app.source_tree.selection_set('1')
    writes = []
    for name in ('delete', 'insert'):
        original_method = getattr(app.source_tree, name)
        monkeypatch.setattr(app.source_tree, name, lambda *a, _name=name, _original=original_method, **kw:
                            (writes.append(_name), _original(*a, **kw))[1])
    updated = copy.deepcopy(original)
    updated['items'][0].update(status='reject', reason_kind='technical', reason='更新后的故障说明')
    app.store.save(updated)
    app.current = updated
    app.show_script_results(updated)
    assert writes == []
    assert app.source_tree.selection() == ('1',)
    assert '更新后的故障说明' in app.source_tree.item('0', 'values')[2]


def test_unchanged_ai_progress_does_not_reload_retry_chain(ui, monkeypatch):
    _, app = ui
    original = load_batch(app)
    original.update(status='running')
    app.store.save(original)
    app.busy = True
    app.task_kind = 'processing'
    payload = {'run_id': original['run_id'], 'processed_sources': 3, 'source_count': 3,
               'ai_item': original['items'][0], 'progress': {'stage': 'AI 文本处理'}}
    app.show_script_progress(payload)
    loads = []
    original_load = app.store.load
    monkeypatch.setattr(app.store, 'load', lambda rid: (loads.append(rid), original_load(rid))[1])
    for _ in range(6):
        app.show_script_progress(payload)
    assert loads == []


def test_explicit_render_preserves_in_memory_edit_contract(ui):
    _, app = ui
    original = load_batch(app)
    app.render_ai_results()
    original['items'][0]['title'] = '尚未保存的题名修订'
    app.render_ai_results()
    assert app._ai_result_items['0']['title'] == '尚未保存的题名修订'


def test_explicit_script_render_honors_unsaved_result_edit(ui):
    _, app = ui
    manifest=load_batch(app)
    app.show_script_results(manifest)
    manifest['items'][0].update(status='reject',reason_kind='technical',reason='尚未保存的结果修订')
    app.show_script_results(manifest)
    assert '尚未保存的结果修订' in app.source_tree.item('0','values')[2]


def test_merged_snapshot_owns_nested_results_and_inputs(ui):
    _, app = ui
    first = load_batch(app)
    first['inputs'] = [{'path': 'C:/input/0.docx', 'details': {'kind': 'original'}}]
    first['metadata'] = {'label': 'original'}
    first['items'][0]['ai_steps'] = [{'reason': 'original'}]
    app.store.save(first)
    retry = {**first, 'run_id': 'copy-check', 'retry_of': first['run_id'], 'items': [], 'inputs': []}
    app.store.save(retry)
    snapshot = task_snapshot(app.store, retry)
    snapshot['items'][0]['ai_steps'][0]['reason'] = 'edited'
    snapshot['inputs'][0]['details']['kind'] = 'edited'
    snapshot['metadata']['label'] = 'edited'
    assert first['items'][0]['ai_steps'][0]['reason'] == 'original'
    assert app.store.load(first['run_id'])['inputs'][0]['details']['kind'] == 'original'
    assert retry['metadata']['label'] == 'original'
