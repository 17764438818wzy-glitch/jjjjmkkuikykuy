"""Preprocess without destinations, then explicitly route selected approved results."""
import copy
import pytest
from test_script_task_ui import ui, configure


@pytest.mark.parametrize('selection', ['none', 'paper', 'both'])
def test_start_snapshots_optional_destinations_and_never_auto_uploads_partial(ui, monkeypatch, selection):
    _, app = ui
    configure(app)
    if selection != 'both':
        # New MD batches route by final specialties, so an empty legacy picker
        # no longer disables uploading. Local-only work explicitly opts out.
        app.ai_vars['auto_upload'].set(False)
        app.image_dataset.set('')
        if selection == 'none':
            app.dataset.set('')
        app.key.set('')
        app.url.set('invalid while working locally')
    errors = []
    scheduled = []
    monkeypatch.setattr(app, 'error', lambda error: errors.append(str(error)))
    monkeypatch.setattr(app, 'async_task', lambda work, done, **kw: scheduled.append(work))
    app.start_automatic([{'path': 'fixture.txt', 'format': '.txt', 'size': 10}])
    assert not errors, errors
    assert scheduled
    config = app.processor.config
    assert bool(config['auto_upload']) == (selection == 'both')
    assert bool(app.processor.rag_client) == (selection == 'both')
    assert bool(config['task_datasets']['paper'].get('id')) == (selection != 'none')


def test_deferred_ai_retry_preserves_no_upload_even_after_new_picker_selection(ui, tmp_path, monkeypatch):
    from test_ai_resume import rejected_run, records
    _, app = ui
    configure(app)
    _, _, first = rejected_run(tmp_path, store=app.store)
    first['task_settings'] = {'destinations': {'paper': {}, 'image': {}}}
    app.store.save(first)
    app.current = first
    errors = []
    scheduled = []
    monkeypatch.setattr(app, 'error', lambda error: errors.append(str(error)))
    monkeypatch.setattr(app, 'async_task', lambda work, done, **kw: scheduled.append(work))
    app._pending_operation = 'retry_ai'
    app.start_automatic(records(first))
    assert not errors, errors
    assert scheduled
    assert not app.processor.config['auto_upload']
    assert not app.processor.rag_client


def deferred_batch(app):
    from test_rag_results import load_batch
    manifest = load_batch(app)
    manifest['task_settings']['destinations'] = {'paper': {}, 'image': {}}
    for item in manifest['items']:
        item['uploads'] = {}
    app.store.save(manifest)
    app.current = manifest
    return manifest


def test_deferred_status_is_pending_and_not_changed_by_unsaved_picker(ui):
    _, app = ui
    manifest = deferred_batch(app)
    configure(app)
    state = app._remote_item_status(manifest['items'][0])
    assert state['upload'] == '待上传'
    assert not state['target_id']
    assert '选择' in state['reason']


def test_import_uses_visible_pair_and_only_selected_rows_then_retry_uses_saved_pair(ui, monkeypatch):
    import auto_ragflow
    _, app = ui
    first = deferred_batch(app)
    configure(app)
    app.render_rag_results()
    app.rag_result_tree.selection_set('1')
    seen = []
    monkeypatch.setattr(app, 'async_task', lambda work, done, **kw: done(work()))
    monkeypatch.setattr(auto_ragflow, 'handoff', lambda *a, **kw: seen.append((a[1], copy.deepcopy(kw))))
    app.import_rag_selected()
    assert len(seen) == 1
    assert seen[0][0] == first['run_id']
    assert seen[0][1]['item_ids'] == ['1']
    assert seen[0][1]['target_datasets']['paper']['id'] == 'paper'
    assert seen[0][1]['target_datasets']['image']['id'] == 'images'
    saved = app.store.load(first['run_id'])
    assert saved['task_settings'] == first['task_settings']
    assert not saved['items'][0].get('handoff_destinations')
    assert saved['items'][1]['handoff_destinations']['paper']['id'] == 'paper'
    app.dataset.set('图片库')
    app.image_dataset.set('论文库')
    app.rag_result_tree.selection_set('1')
    app.retry_handoff()
    assert seen[-1][1]['target_datasets']['paper']['id'] == 'paper'


def test_import_requires_current_connection_and_locks_controls(ui, monkeypatch):
    _, app = ui
    deferred_batch(app)
    configure(app)
    app.render_rag_results()
    app.rag_result_tree.selection_set('0')
    app.url.set('http://another-server')
    scheduled = []
    errors = []
    monkeypatch.setattr(app, 'error', lambda error: errors.append(str(error)))
    monkeypatch.setattr(app, 'async_task', lambda *a, **kw: scheduled.append(True))
    app.import_rag_selected()
    assert errors and not scheduled
    app.busy = True
    app.update_controls()
    for widget in (app.rag_dataset_combo, app.rag_image_dataset_combo, app.rag_import_button, app.rag_connect_button):
        assert widget.instate(['disabled'])


@pytest.mark.parametrize('linked', [False, True])
def test_manual_import_runs_real_handoff_and_deduplicates_after_reload(ui, monkeypatch, linked):
    import ragflow_client
    from test_upload_dedup import processed, AssetRemote
    _, app = ui
    configure(app)
    item = processed(app.store, 'asset-deferred')
    first = app.store.load('asset-deferred')
    first['task_settings'] = {'destinations': {'paper': {}, 'image': {}}}
    app.store.save(first)
    app.current = first
    if linked:
        app.current = {'run_id': 'retry', 'retry_of': first['run_id'], 'items': [],
                       'status': 'completed', 'task_settings': copy.deepcopy(first['task_settings'])}
        app.store.save(app.current)
    app.url.set('http://localhost')
    app.connected([{'id': 'paper', 'name': '论文库'}, {'id': 'images', 'name': '图片库'}])
    app.dataset.set('论文库'); app.image_dataset.set('图片库')
    app.render_rag_results()
    app.rag_result_tree.selection_set(item['id'])
    remote = AssetRemote()
    remote.datasets = lambda: list(app.datasets)
    monkeypatch.setattr(ragflow_client, 'RagflowClient', lambda *args: remote)
    monkeypatch.setattr(app, 'async_task', lambda work, done, **kw: done(work()))
    app.import_rag_selected()
    saved = app.store.load(first['run_id'])['items'][0]
    assert saved['handoff_status'] == 'uploaded', saved.get('handoff_reason')
    assert set(saved['uploads']) == {'http://localhost|paper', 'http://localhost|images'}
    assert saved['uploads']['http://localhost|paper']['parse_status'] == 'submitted'
    assert remote.parse_calls
    posts = remote.posts
    app.current = app.store.load(app.current['run_id'])
    app.render_rag_results()
    app.rag_result_tree.selection_set(item['id'])
    app.import_rag_selected()
    assert remote.posts == posts
    if linked:
        assert app.store.load('retry')['items'] == []


def test_deferred_handoff_cannot_fall_back_to_old_mapping(tmp_path):
    from auto_ragflow import handoff
    from engine import Store
    from test_upload_dedup import processed, AssetRemote
    store = Store(tmp_path / 'data')
    processed(store, 'deferred')
    manifest = store.load('deferred')
    manifest['task_settings'] = {'destinations': {'paper': {}, 'image': {}}}
    store.save(manifest)
    remote = AssetRemote()
    handoff(store, 'deferred', remote, {'会计学': {'paper': 'accounting', 'image': ''}})
    assert remote.posts == 0


def test_reading_datasets_preserves_explicit_defer_choice(ui):
    _, app = ui
    configure(app)
    app.task_defer_button.invoke()
    app.connected(list(reversed(app.datasets)))
    assert not app.dataset.get() and not app.image_dataset.get()
    assert not app.task_setup_problem()


@pytest.mark.parametrize('partial', [False, True])
def test_retry_cannot_initiate_unassigned_deferred_import(ui, monkeypatch, partial):
    _, app = ui
    manifest = deferred_batch(app)
    if partial:
        manifest['task_settings']['destinations']['paper'] = {'id': 'paper', 'name': '论文库'}
    app.store.save(manifest)
    configure(app)
    app.render_rag_results()
    app.rag_result_tree.selection_set('0')
    app.update_rag_controls()
    scheduled = []
    monkeypatch.setattr(app, 'async_task', lambda *args, **kw: scheduled.append(True))
    app.retry_handoff()
    assert not scheduled
    assert app.rag_retry_button.instate(['disabled'])


def test_new_destination_import_does_not_restart_old_destination_assets(tmp_path):
    from auto_ragflow import handoff
    from engine import Store
    from test_upload_dedup import processed, AssetRemote
    store = Store(tmp_path / 'data')
    processed(store, 'change-destination')
    manifest = store.load('change-destination')
    manifest['items'][0]['uploads'] = {
        'http://localhost|old-images': {'dataset_id': 'old-images', 'route': 'image',
            'asset_uploads': [{'path': 'assets/old.png', 'status': 'uploaded',
                               'document_id': 'old-asset', 'ragflow_status': 'failed'}]}}
    store.save(manifest)
    remote = AssetRemote()
    targets = {'paper': {'id': 'new-paper', 'name': '新论文库'},
               'image': {'id': 'new-images', 'name': '新图片库'}}
    remote.datasets = lambda: list(targets.values())
    remote.docs.append({'id': 'old-asset', 'dataset_id': 'old-images', 'run': '4'})
    handoff(store, 'change-destination', remote, {}, target_datasets=targets,
            item_ids=['change-destination'])
    assert store.load('change-destination')['items'][0]['handoff_status'] == 'uploaded'
    assert 'old-asset' not in remote.parse_calls
