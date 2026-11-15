"""Durable delivery keeps sealed packages and model work independent of RAG."""
import copy
import json
import threading
import time
from pathlib import Path

import pytest

from auto_ragflow import handoff
from engine import Store
from test_preextract_handoff import prepared_batch, memory_client
from test_preextract_workbench_flow import flow
from test_script_task_ui import ui


MODE = {'preextract_delivery_mode': 'disk_queue_v1', 'work_pool_auto_start': False}


def test_registration_does_not_race_with_a_package_pointer_update(tmp_path):
    from filelock import FileLock
    from preextract_delivery_queue import DeliveryQueue, enqueue_items, queue_path
    store, item, prepared, work = prepared_batch(tmp_path)
    with FileLock(str(work/'operation.lock'), timeout=0):
        queue = enqueue_items(store, 'batch', [item], 'http://invalid')
        assert queue.list() == []
    queue = enqueue_items(store, 'batch', [item], 'http://invalid')
    assert len(queue.list()) == 1
    assert queue.list()[0]['payload']['manifest_sha256'] == prepared['validation']['manifest_sha256']


def test_new_mode_persists_package_without_upload_and_survives_stale_manifest(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    stale = copy.deepcopy(store.load('batch'))
    before = {str(p.relative_to(item['package_path'])): p.read_bytes()
              for p in Path(item['package_path']).rglob('*') if p.is_file()}
    handoff(store, 'batch', client, {}, upload_config=MODE)
    assert client.writes == 0, 'registering a sealed package must not synchronously upload'
    saved = Store(store.root).load('batch')['items'][0]
    assert saved['handoff_status'] == 'queued'
    assert saved['preextract_delivery_queue']['status'] == 'queued'
    stale['items'].append(dict(id='next-paper', status='pending'))
    store.save(stale)
    reloaded = Store(store.root).load('batch')
    assert len(reloaded['items']) == 2
    assert reloaded['items'][0]['handoff_status'] == 'queued'
    assert before == {str(p.relative_to(item['package_path'])): p.read_bytes()
                      for p in Path(item['package_path']).rglob('*') if p.is_file()}


def test_deferred_new_batch_upload_uses_original_saved_mode(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    manifest = store.load('batch')
    manifest['task_settings'] = {'preextract_delivery_mode': 'disk_queue_v1'}
    store.save(manifest)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, item_ids=[item['id']],
            upload_config={'work_pool_auto_start': False})
    assert client.writes == 0
    assert store.load('batch')['items'][0]['handoff_status'] == 'queued'


def test_one_missing_package_does_not_prevent_other_finished_packages_queuing(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    manifest = store.load('batch')
    missing = copy.deepcopy(item)
    missing.update(id='missing', package_path=str(Path(item['package_path']) / 'missing'))
    manifest['items'].insert(0, missing)
    store.save(manifest)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    saved = store.load('batch')['items']
    assert saved[0]['handoff_status'] == 'pending' and saved[0]['handoff_reason']
    assert saved[1]['handoff_status'] == 'queued'
    assert client.writes == 0


def test_worker_reuses_real_delivery_and_duplicate_enqueue_does_not_repeat(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path, image=True)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    from preextract_delivery_worker import drain_queue
    from preextract_delivery_queue import DeliveryQueue, queue_path
    queue = DeliveryQueue(queue_path(store.root))
    assert len(queue.list()) == 1
    drain_queue(store.root, client)
    saved = store.load('batch')['items'][0]
    assert saved['handoff_status'] == 'uploaded'
    assert saved['preextract_delivery']['status'] == 'complete'
    assert all(x['verified'] and x['retrieval_verified']
               for x in saved['preextract_delivery']['targets'].values())
    receipt = json.loads((work / 'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['status'] == 'complete'
    writes = client.writes
    handoff(store, 'batch', client, {}, upload_config=MODE)
    drain_queue(store.root, client)
    assert client.writes == writes
    assert len(queue.list()) == 1
    assert queue.list()[0]['status'] == 'complete'


def test_saved_package_queues_without_legacy_work_pool_or_waiting_for_rag(tmp_path, monkeypatch):
    from auto_pipeline import AutoProcessor
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    processor = AutoProcessor(store, dict(MODE, work_pool_enabled=False, auto_upload=True))
    processor.rag_client = client
    processor._enqueue_approved_after_save('batch', [item], lambda *args: None)
    assert client.writes == 0
    assert store.load('batch')['items'][0]['handoff_status'] == 'queued'


def _queue_records(tmp_path, count):
    from preextract_delivery_queue import DeliveryQueue, queue_path
    root = tmp_path / 'queue-fixture'
    queue = DeliveryQueue(queue_path(root))
    for i in range(count):
        work = tmp_path / f'work-{i}'
        work.mkdir()
        queue.enqueue(dict(id=f'job-{i}', service='http://localhost:9380', task={},
            manifest_sha256=f'hash-{i}', store_root=str(root), run_id=f'batch-{i // 5}',
            item=dict(id=f'paper-{i}', package_path=str(root / 'packages' / str(i)),
                      preextract_work_dir=str(work), source_hash=f'source-{i}')))
    return root, queue


def test_global_six_refills_while_first_delivery_is_still_running(tmp_path):
    from preextract_delivery_worker import drain_queue
    root, queue = _queue_records(tmp_path, 14)
    client = type('Client', (), {'base_url': 'http://localhost:9380'})()
    lock = threading.Lock()
    first_six = threading.Event()
    release_one = threading.Event()
    release_rest = threading.Event()
    seventh_started = threading.Event()
    active = 0
    peak = 0
    seen = []
    errors = []
    def deliver(q, job, client, stop):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            seen.append(job['id'])
            if len(seen) == 6:
                first_six.set()
            if job['id'] == 'job-6':
                seventh_started.set()
        try:
            assert (release_one if job['id'] == 'job-1' else release_rest).wait(5)
            q.update(job['id'], status='complete')
        finally:
            with lock:
                active -= 1
    def run():
        try:
            drain_queue(root, client, deliver=deliver)
        except BaseException as exc:
            errors.append(exc)
    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert first_six.wait(3)
        # A second batch/consumer cannot create another six delivery lanes.
        assert drain_queue(root, client, deliver=deliver) is False
        release_one.set()
        assert seventh_started.wait(3)
        assert queue.get('job-0')['status'] == 'uploading'
        assert queue.get('job-6')['status'] == 'uploading'
        assert len(queue.list()) == 14, 'RAG backlog must not reject package registration above eight'
    finally:
        release_one.set()
        release_rest.set()
        worker.join(8)
    assert not worker.is_alive()
    assert not errors
    assert peak == 6
    assert len(seen) == len(set(seen)) == 14
    assert all(row['status'] == 'complete' for row in queue.list())


def test_restart_claims_only_registered_safe_jobs_and_holds_uncertain_writes(tmp_path):
    from preextract_delivery_worker import drain_queue
    from preextract_delivery_queue import DeliveryQueue
    root, queue = _queue_records(tmp_path, 3)
    client = type('Client', (), {'base_url': 'http://localhost:9380'})()
    interrupted = queue.claim(client.base_url)
    uncertain = queue.claim(client.base_url)
    work = Path(uncertain['payload']['item']['preextract_work_dir'])
    (work / 'delivery-binding.json').write_text('{}', encoding='utf8')
    # This unrelated historical directory is never discovered or adopted.
    historical = root / 'runs' / 'old' / 'packages' / 'untouched'
    historical.mkdir(parents=True)
    (historical / 'manifest.json').write_text('{}', encoding='utf8')
    seen = []
    def deliver(q, job, client, stop):
        seen.append(job['id'])
        q.update(job['id'], status='complete')
    drain_queue(root, client, deliver=deliver)
    restarted = DeliveryQueue(queue.path)
    assert set(seen) == {interrupted['id'], 'job-2'}
    assert restarted.get(uncertain['id'])['status'] == 'needs_review'
    assert len(restarted.list()) == 3
    assert (work / 'delivery-binding.json').read_text(encoding='utf8') == '{}'


def test_stale_producer_save_cannot_erase_completed_readback(tmp_path, monkeypatch):
    from preextract_delivery_worker import drain_queue
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    stale = copy.deepcopy(store.load('batch'))
    handoff(store, 'batch', client, {}, upload_config=MODE)
    drain_queue(store.root, client)
    stale['items'].append(dict(id='later', status='pending'))
    store.save(stale)
    for manifest in (Store(store.root).load('batch'), Store(store.root).history()[0]):
        assert len(manifest['items']) == 2
        assert manifest['items'][0]['handoff_status'] == 'uploaded'
        assert manifest['items'][0]['preextract_delivery']['status'] == 'complete'


def test_changed_package_is_held_without_any_network_write(tmp_path, monkeypatch):
    from preextract_delivery_worker import drain_queue
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    article = Path(item['package_path']) / 'article.redacted.md'
    article.write_text(article.read_text(encoding='utf8') + '\nchanged', encoding='utf8')
    drain_queue(store.root, client)
    saved = store.load('batch')['items'][0]
    assert client.writes == 0
    assert saved['handoff_status'] == 'pending'
    assert saved['preextract_delivery_queue']['status'] != 'complete'


def test_explicit_retry_of_registered_complete_package_rechecks_deleted_remote(tmp_path, monkeypatch):
    from preextract_delivery_worker import drain_queue
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    drain_queue(store.root, client)
    writes = client.writes
    client.docs.clear()
    handoff(store, 'batch', client, {}, item_ids=[item['id']], upload_config=MODE)
    drain_queue(store.root, client)
    assert client.writes > writes
    assert store.load('batch')['items'][0]['handoff_status'] == 'uploaded'


def test_new_batch_reusing_completed_identity_rechecks_remote_without_new_model(tmp_path, monkeypatch):
    import shutil
    from preextract_delivery_worker import drain_queue
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    drain_queue(store.root, client)
    writes = client.writes
    next_item = copy.deepcopy(item)
    published = store.runs / 'next-batch' / 'packages' / item['id']
    shutil.copytree(item['package_path'], published)
    next_item['package_path'] = str(published)
    store.save(dict(run_id='next-batch', status='completed', items=[next_item]))
    client.docs.clear()
    handoff(store, 'next-batch', client, {}, upload_config=MODE)
    assert store.load('next-batch')['items'][0]['handoff_status'] == 'queued'
    assert client.writes == writes
    drain_queue(store.root, client)
    assert client.writes > writes
    assert store.load('next-batch')['items'][0]['preextract_delivery']['status'] == 'complete'


def test_new_verified_alias_can_use_its_package_while_keeping_original_receipt(tmp_path, monkeypatch):
    import shutil
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from preextract_delivery_worker import drain_queue
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    drain_queue(store.root, client)
    writes = client.writes
    next_item = copy.deepcopy(item)
    published = store.runs / 'next-batch' / 'packages' / item['id']
    shutil.copytree(item['package_path'], published)
    # A fresh private working directory must not replace the previous delivery
    # intent/receipt. Only the verified public package location is substituted.
    next_work = tmp_path / 'new-private-work'
    (next_work / 'private').mkdir(parents=True)
    shutil.copy2(work / 'private' / 'prepared-publication.json', next_work / 'private' / 'prepared-publication.json')
    next_item.update(package_path=str(published), preextract_work_dir=str(next_work))
    store.save(dict(run_id='next-batch', status='completed', items=[next_item]))
    Path(item['package_path']).rename(Path(item['package_path']).with_name('archived-original-package'))
    handoff(store, 'next-batch', client, {}, upload_config=MODE)
    drain_queue(store.root, client)
    assert store.load('next-batch')['items'][0]['handoff_status'] == 'uploaded'
    assert store.load('next-batch')['items'][0]['preextract_delivery']['status'] == 'complete'
    assert client.writes == writes, 'existing documents should only be read back'
    job = DeliveryQueue(queue_path(store.root)).list()[0]
    assert job['payload']['item']['package_path'] == str(published)
    assert job['payload']['item']['preextract_work_dir'] == str(work)
    assert not (next_work / 'delivery-receipt.json').exists()


def test_original_document_to_package_flow_finishes_before_rag_upload(flow, monkeypatch):
    from preextract_delivery_worker import drain_queue
    processor, opener, sources = flow
    processor.config.update(MODE, auto_upload=True)
    processor.config['concurrency'].update(papers=2, global_llm=2)
    client = memory_client(monkeypatch)
    processor.rag_client = client
    manifest = processor.run(sources)
    assert len(opener.calls) == 2
    assert client.writes == 0
    assert len(manifest['items']) == 2
    assert all(row['status'] == 'pass' for row in manifest['items'])
    assert all(row['handoff_status'] == 'queued' for row in processor.store.load(manifest['run_id'])['items'])
    drain_queue(processor.store.root, client)
    saved = processor.store.load(manifest['run_id'])
    assert all(row['handoff_status'] == 'uploaded' for row in saved['items'])
    assert len(opener.calls) == 2, 'uploading existing packages must never call the model'


def test_existing_rag_view_distinguishes_waiting_and_active_delivery(tmp_path, monkeypatch):
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from preextract_handoff import describe_preextract_item
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    view = describe_preextract_item(store.load('batch')['items'][0], client.base_url)
    from rag_status import UPLOAD_STATES, PROCESS_STATES
    assert view['upload'] in UPLOAD_STATES and view['process'] in PROCESS_STATES
    assert view['upload'] == '排队待上传' and not view['can_retry']
    DeliveryQueue(queue_path(store.root)).claim(client.base_url)
    view = describe_preextract_item(store.load('batch')['items'][0], client.base_url)
    assert view['upload'] == '上传中' and not view['can_retry']


def test_live_reader_observes_delivery_changes_without_producer_manifest_save(tmp_path, monkeypatch):
    from live_results import ResultReader
    from preextract_delivery_worker import drain_queue
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    original = store.manifest_path('batch').read_bytes()
    reader = ResultReader(store)
    queued = reader.read('batch')
    assert queued['manifest']['items'][0]['handoff_status'] == 'queued'
    drain_queue(store.root, client)
    completed = reader.read('batch')
    assert completed['manifest']['items'][0]['handoff_status'] == 'uploaded'
    assert store.manifest_path('batch').read_bytes() == original
    assert reader.read('batch') is completed


def test_original_rag_page_switch_refreshes_disk_delivery_without_new_page(ui, tmp_path, monkeypatch):
    from preextract_delivery_worker import drain_queue
    _, app = ui
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    app.store = store
    app.current = store.load('batch')
    app.url.set(client.base_url)
    app.render_rag_results(refresh=False)
    assert app._rag_states[item['id']]['upload'] == '排队待上传'
    drain_queue(store.root, client)
    app.render_rag_results(refresh=False)
    assert app._rag_states[item['id']]['process'] == '已完成'
    assert len(app.tabs.tabs()) == 6


def test_idle_original_rag_page_follows_background_delivery_without_tab_switch(ui, tmp_path, monkeypatch):
    from preextract_delivery_worker import drain_queue
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from test_live_result_refresh import pump

    root, app = ui
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    app.store = store
    app.current = store.load('batch')
    app.url.set(client.base_url)
    app.tabs.select(app.rag_result_tab)
    root.update()
    app.render_rag_results(refresh=False)
    assert not app.busy and app._rag_states[item['id']]['upload'] == '排队待上传'
    selected_tab = app.tabs.select()
    selected_writes = client.writes

    queue = DeliveryQueue(queue_path(store.root))
    job = queue.claim(client.base_url)
    pump(root, lambda: app._rag_states[item['id']]['upload'] == '上传中')
    assert client.writes == selected_writes, 'Display refresh must not submit anything'
    queue.update(job['id'], status='queued')
    drain_queue(store.root, client)
    pump(root, lambda: app._rag_states[item['id']]['process'] == '已完成')
    assert app.tabs.select() == selected_tab
    assert not app.busy and len(app.tabs.tabs()) == 6


def test_package_arriving_at_worker_idle_exit_is_not_left_without_consumer(tmp_path, monkeypatch):
    from preextract_delivery_worker import drain_queue
    from preextract_delivery_queue import DeliveryQueue
    root, queue = _queue_records(tmp_path, 1)
    client = type('Client', (), {'base_url': 'http://localhost:9380'})()
    original = DeliveryQueue.claim
    injected = False
    def claim(self, service):
        nonlocal injected
        result = original(self, service)
        if result is None and not injected and self.get('job-0')['status'] == 'complete':
            injected = True
            later = copy.deepcopy(self.get('job-0')['payload'])
            later.update(id='late-job', run_id='late-batch')
            self.enqueue(later)
        return result
    monkeypatch.setattr(DeliveryQueue, 'claim', claim)
    def deliver(q, job, client, stop):
        q.update(job['id'], status='complete')
    drain_queue(root, client, deliver=deliver)
    assert injected
    assert queue.get('late-job')['status'] == 'complete'


def test_replaced_manifest_cannot_inherit_an_old_complete_projection(tmp_path, monkeypatch):
    from preextract_delivery_worker import drain_queue
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    drain_queue(store.root, client)
    stale = store.load('batch')
    store.save(stale)
    path = Path(item['package_path']) / 'manifest.json'
    path.write_text(path.read_text(encoding='utf8') + '\n', encoding='utf8')
    saved = store.load('batch')['items'][0]
    assert saved['handoff_status'] == 'pending'
    assert saved['preextract_handoff_blocked']


def test_pending_projection_cannot_hide_a_changed_package(tmp_path, monkeypatch):
    from preextract_handoff import describe_preextract_item
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {}, upload_config=MODE)
    store.save(store.load('batch'))
    path = Path(item['package_path']) / 'manifest.json'
    path.write_text(path.read_text(encoding='utf8') + '\n', encoding='utf8')
    saved = store.load('batch')['items'][0]
    assert saved['preextract_delivery_queue']['status'] == 'needs_review'
    assert describe_preextract_item(saved, client.base_url)['upload'] == '上传待确认'


def test_queue_initialization_does_not_break_original_manifest_reads(tmp_path):
    import sqlite3
    from preextract_delivery_queue import DeliveryQueue, queue_path
    store = Store(tmp_path / 'data')
    original = dict(run_id='old-batch', status='running', items=[])
    store.save(original)
    path = queue_path(store.root)
    path.parent.mkdir(parents=True)
    # Another process can read Store after SQLite creates the file but before
    # the first queue constructor commits its tables.
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE unrelated (id INTEGER)')
    assert store.load('old-batch') == original
    DeliveryQueue(path)
    assert store.load('old-batch') == original


def test_wake_waits_for_worker_final_empty_check_and_does_not_block_producer(tmp_path, monkeypatch):
    import preextract_delivery_worker as worker
    from preextract_delivery_queue import DeliveryQueue
    from filelock import FileLock
    root, queue = _queue_records(tmp_path, 1)
    service = 'http://localhost:9380'
    client = type('Client', (), {'base_url': service})()
    (root / 'settings.json').write_text(json.dumps(dict(ragflow_url=service)), encoding='utf8')
    original = DeliveryQueue.has_work
    probe_entered, launched = threading.Event(), threading.Event()
    launched_guards = []
    injected = False
    class Child:
        def poll(self):
            return None
    def launch(*args, **kwargs):
        guard = FileLock(str(queue.path) + '.worker.lock', timeout=0, thread_local=False)
        guard.acquire()
        launched_guards.append(guard)
        launched.set()
        return Child()
    monkeypatch.setattr(worker.subprocess, 'Popen', launch)
    original_lock = worker.FileLock
    def lock(path, **kwargs):
        if str(path).endswith('.launcher.lock') and threading.current_thread().name == 'sealed-delivery-launcher':
            probe_entered.set()
        return original_lock(path, **kwargs)
    monkeypatch.setattr(worker, 'FileLock', lock)
    def has_work(self, service, **kwargs):
        nonlocal injected
        result = original(self, service, **kwargs)
        if not result and not injected:
            injected = True
            later = copy.deepcopy(self.get('job-0')['payload'])
            later.update(id='late-job', run_id='late-batch')
            self.enqueue(later)
            worker.wake_worker(root, service)
            assert probe_entered.wait(2)
            # Keep the final empty check locked while the asynchronous launcher
            # tries to enter. No production subprocess or RAG is involved.
            time.sleep(.15)
        return result
    monkeypatch.setattr(DeliveryQueue, 'has_work', has_work)
    try:
        worker.drain_queue(root, client, deliver=lambda q, job, *_: q.update(job['id'], status='complete'))
        assert launched.wait(2), 'a wake during the final empty check must start the next consumer'
    finally:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and str(queue.path) in worker._launching:
            time.sleep(.01)
        for guard in launched_guards:
            guard.release()


def test_another_wake_during_launcher_exit_is_observed(tmp_path, monkeypatch):
    import preextract_delivery_worker as worker
    root, queue = _queue_records(tmp_path, 1)
    first_entered, release, again = threading.Event(), threading.Event(), threading.Event()
    calls = []
    def ensure(*args):
        calls.append(1)
        if len(calls) == 1:
            first_entered.set()
            assert release.wait(3)
        else:
            again.set()
    monkeypatch.setattr(worker, '_ensure_worker', ensure)
    try:
        worker.wake_worker(root, 'http://localhost:9380')
        assert first_entered.wait(2)
        worker.wake_worker(root, 'http://localhost:9380')
        release.set()
        assert again.wait(2)
    finally:
        release.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and str(queue.path) in worker._launching:
            time.sleep(.01)
