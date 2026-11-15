"""Cleanup at the original verified handoff; real local packages, no network."""
import hashlib
import json
from pathlib import Path
import threading

import pytest
from filelock import FileLock

from test_preextract_workbench_flow import flow
from test_preextract_handoff import memory_client


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _policy(root, enabled=True):
    path = root/'private/preextract_delivery/cleanup-policy.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(enabled=enabled)), encoding='utf8')


def _case(flow, tmp_path, monkeypatch):
    processor, opener, sources = flow
    catalog_path = Path(processor.config['preextract_runtime_dir'])/'catalog.json'
    catalog = json.loads(catalog_path.read_text(encoding='utf8'))
    catalog['majors'].append(dict(code='080902', name='软件工程', discipline_code='08',
        discipline='工学', group_code='0809', group='计算机类',
        paper_dataset_id='test-paper-2', image_dataset_id='test-images-2'))
    catalog_path.write_text(json.dumps(catalog, ensure_ascii=False), encoding='utf8')
    opener.transform = lambda candidate: candidate['profile']['routing_candidates'].append(
        dict(code='080902', name='软件工程'))
    manifest = processor.run(sources[:1])
    item = manifest['items'][0]
    assert item['status'] == 'pass', item.get('reason')
    work = Path(item['preextract_work_dir'])
    rendered = work/'private/rendered'
    rendered.mkdir()
    (rendered/'temporary.pdf').write_bytes(b'local disposable render')
    stream = work/'model/response-events.jsonl'
    assert stream.is_file()
    _policy(processor.store.root)
    client = memory_client(monkeypatch)
    return processor, opener, sources, manifest, item, work, rendered, stream, client


def _deliver(case):
    from preextract_handoff import handoff_preextract_item
    processor, _, _, manifest, item, _, _, _, client = case
    handoff_preextract_item(processor.store, manifest['run_id'], item['id'], client)


def test_all_specialties_verified_then_cleanup_preserves_learning_originals_and_cache(flow, tmp_path, monkeypatch):
    case = _case(flow, tmp_path, monkeypatch)
    processor, opener, sources, manifest, item, work, rendered, stream, client = case
    retained = [Path(sources[0]['path']), work/'material.json', work/'private/candidate.json',
        work/'private/论文预提取.md', work/'model/response.txt', work/'model/request.json',
        work/'model/request-state.json', work/'private/prepared-publication.json']
    retained += [p for p in Path(item['package_path']).rglob('*') if p.is_file()]
    retained += [p for p in (work/'publication').rglob('*') if p.is_file()]
    before = {str(path): _sha(path) for path in retained}
    _deliver(case)
    receipt = json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['status'] == 'complete'
    assert len(receipt['targets']) == 2
    assert all(t['verified'] and t['retrieval_verified'] for t in receipt['targets'].values())
    assert not stream.exists(), 'verified delivery must remove redundant stream log'
    assert not rendered.exists(), 'unreferenced render directory must be removed'
    assert {str(path): _sha(path) for path in retained} == before
    report = json.loads((work/'cleanup-receipt.json').read_text(encoding='utf8'))
    assert report['status'] == receipt['cleanup'] == 'complete'
    assert report['removed_bytes'] > 0
    writes = client.writes
    _deliver(case)
    assert client.writes == writes
    assert json.loads((work/'cleanup-receipt.json').read_text(encoding='utf8'))['removed_bytes'] == report['removed_bytes']
    later = processor.run([dict(sources[0], prepared_checkpoint=item['prepared_checkpoint'])], operation='retry_ai')
    assert later['items'][0]['status'] == 'pass', later['items'][0].get('reason')
    assert len(opener.calls) == 1, 'cleanup must not force another model request'


def test_failed_specialty_readback_does_not_clean_anything(flow, tmp_path, monkeypatch):
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, _, work, rendered, stream, client = case
    request = client.request
    def incomplete(method, endpoint, body=None, content_type=None):
        result = request(method, endpoint, body, content_type)
        if method == 'GET' and 'test-paper-2' in endpoint and '/chunks' in endpoint:
            raise RuntimeError('second specialty readback failed')
        return result
    client.request = incomplete
    _deliver(case)
    receipt = json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['status'] != 'complete'
    assert stream.is_file() and rendered.is_dir()
    assert not (work/'cleanup-receipt.json').exists()


def test_cleanup_failure_does_not_erase_verified_ingestion(flow, tmp_path, monkeypatch):
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, _, work, _, stream, _ = case
    original_unlink = Path.unlink
    def denied(path, *args, **kwargs):
        if path == stream:
            raise PermissionError('file is held by another reader')
        return original_unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', denied)
    _deliver(case)
    item = processor.store.load(manifest['run_id'])['items'][0]
    assert item['handoff_status'] == 'uploaded'
    assert item['preextract_delivery']['status'] == 'complete'
    assert stream.is_file()
    report = json.loads((work/'cleanup-receipt.json').read_text(encoding='utf8'))
    assert report['status'] in {'pending', 'partial', 'error'}


def test_material_hash_dependency_is_not_deleted(flow, tmp_path, monkeypatch):
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, item, work, rendered, stream, _ = case
    path = work/'material.json'
    bundle = json.loads(path.read_text(encoding='utf8'))
    protected = rendered/'temporary.pdf'
    bundle['file_hashes'][str(protected)] = _sha(protected)
    path.write_text(json.dumps(bundle, ensure_ascii=False), encoding='utf8')
    item['prepared_checkpoint']['sha256'] = _sha(path)
    processor.store.save(manifest)
    _deliver(case)
    assert protected.is_file()
    assert not stream.exists()


def test_busy_work_and_parent_revision_history_are_not_cleaned(flow, tmp_path, monkeypatch):
    from preextract_cleanup import cleanup_item
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, item, work, rendered, stream, _ = case
    _policy(processor.store.root, False)
    _deliver(case)
    _policy(processor.store.root)
    with FileLock(str(work/'operation.lock'), timeout=0):
        result = cleanup_item(processor.store.root, manifest['run_id'], item)
    assert result['status'] == 'deferred'
    assert stream.exists() and rendered.exists()
    (work/'revisions').mkdir()
    (work/'revisions'/'evidence.json').write_text('{}', encoding='utf8')
    result = cleanup_item(processor.store.root, manifest['run_id'], item)
    assert result['status'] == 'deferred'
    assert stream.exists() and rendered.exists()


def test_queue_tail_cleans_only_complete_jobs_without_model_or_rag_calls(flow, tmp_path, monkeypatch):
    from preextract_cleanup import cleanup_queue_once
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from preextract_delivery_worker import drain_queue
    from auto_ragflow import handoff
    case = _case(flow, tmp_path, monkeypatch)
    processor, opener, _, manifest, item, work, rendered, stream, client = case
    _policy(processor.store.root, False)
    handoff(processor.store, manifest['run_id'], client, {},
        upload_config={'preextract_delivery_mode': 'disk_queue_v1', 'work_pool_auto_start': False})
    queue = DeliveryQueue(queue_path(processor.store.root))
    _policy(processor.store.root)
    assert cleanup_queue_once(processor.store.root)['completed'] == 0
    assert stream.is_file() and rendered.exists()
    _policy(processor.store.root, False)
    drain_queue(processor.store.root, client)
    _policy(processor.store.root)
    before = client.writes, len(opener.calls)
    result = cleanup_queue_once(processor.store.root)
    assert result['completed'] == 1
    assert not stream.exists() and not rendered.exists()
    assert (client.writes, len(opener.calls)) == before
    assert queue.list()[0]['status'] == 'complete'
    saved = processor.store.load(manifest['run_id'])['items'][0]
    assert saved['preextract_delivery']['cleanup'] == 'complete'


def test_cleanup_retries_saved_local_intent_after_file_unlock(flow, tmp_path, monkeypatch):
    case = _case(flow, tmp_path, monkeypatch)
    _, _, _, _, _, work, rendered, stream, client = case
    original_unlink = Path.unlink
    def denied(path, *args, **kwargs):
        if path == stream:
            raise PermissionError('busy reader')
        return original_unlink(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', denied)
        _deliver(case)
    assert not rendered.exists() and stream.exists()
    before = client.writes
    _deliver(case)
    assert client.writes == before
    assert not stream.exists()
    report = json.loads((work/'cleanup-receipt.json').read_text(encoding='utf8'))
    assert report['status'] == 'complete' and report['removed_files'] == 2


def test_reparse_render_path_is_rejected_before_any_deletion(flow, tmp_path, monkeypatch):
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, _, _, rendered, stream, _ = case
    original_is_junction = Path.is_junction
    monkeypatch.setattr(Path, 'is_junction', lambda path: path == rendered or original_is_junction(path))
    _deliver(case)
    assert stream.is_file() and rendered.is_dir()
    item = processor.store.load(manifest['run_id'])['items'][0]
    assert item['handoff_status'] == 'uploaded'
    assert item['preextract_cleanup']['status'] == 'error'


def test_outside_workspace_and_wrong_delivery_identity_are_not_cleaned(flow, tmp_path, monkeypatch):
    import copy
    from preextract_cleanup import cleanup_item
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, item, work, rendered, stream, _ = case
    _policy(processor.store.root, False)
    _deliver(case)
    _policy(processor.store.root)
    wrong = copy.deepcopy(item)
    wrong['preextract_work_dir'] = str(tmp_path)
    assert cleanup_item(processor.store.root, manifest['run_id'], wrong)['status'] == 'error'
    receipt_path = work/'delivery-receipt.json'
    receipt = json.loads(receipt_path.read_text(encoding='utf8'))
    receipt['task']['task_id'] = 'another-paper'
    receipt_path.write_text(json.dumps(receipt), encoding='utf8')
    assert cleanup_item(processor.store.root, manifest['run_id'], item)['status'] == 'deferred'
    assert stream.is_file() and rendered.is_dir()


def test_other_pending_delivery_reference_preserves_workspace(flow, tmp_path, monkeypatch):
    from preextract_cleanup import cleanup_item
    from preextract_delivery_queue import DeliveryQueue, queue_path
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, item, _, rendered, stream, _ = case
    _policy(processor.store.root, False)
    _deliver(case)
    _policy(processor.store.root)
    queue = DeliveryQueue(queue_path(processor.store.root))
    queue.enqueue(dict(id='other-pending-delivery', service='http://localhost:9390',
        store_root=str(processor.store.root),run_id=manifest['run_id'],item=item,task={},manifest_sha256='other'))
    result = cleanup_item(processor.store.root, manifest['run_id'], item)
    assert result['status'] == 'deferred' and result['reason'] == 'another_delivery_still_uses_workspace'
    assert stream.is_file() and rendered.is_dir()


def test_queue_projection_compare_preserves_new_upload_state(tmp_path):
    from preextract_delivery_queue import DeliveryQueue, queue_path
    queue = DeliveryQueue(queue_path(tmp_path))
    queue.enqueue(dict(id='one', service='http://localhost:9380', store_root=str(tmp_path),
        run_id='batch',item={'id':'one','package_path':str(tmp_path/'public')},task={},manifest_sha256='same'))
    queue.update('one',status='complete')
    queue.update('one',status='uploading',projection={'handoff_status':'uploading'})
    assert queue.update('one',projection={'handoff_status':'uploaded'},expected_status='complete') is False
    assert queue.get('one')['status']=='uploading'
    assert queue.get('one')['projection']['handoff_status']=='uploading'


def test_requeue_cannot_change_complete_state_while_cleanup_owns_paper(flow, tmp_path, monkeypatch):
    from filelock import Timeout
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from preextract_delivery_worker import drain_queue
    from auto_ragflow import handoff
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, item, work, _, _, client = case
    _policy(processor.store.root, False)
    handoff(processor.store, manifest['run_id'], client, {},
        upload_config={'preextract_delivery_mode':'disk_queue_v1','work_pool_auto_start':False})
    drain_queue(processor.store.root, client)
    queue = DeliveryQueue(queue_path(processor.store.root))
    job = queue.list()[0]
    assert job['status']=='complete'
    with FileLock(str(work/'operation.lock'), timeout=0):
        with pytest.raises(Timeout):
            queue.enqueue(job['payload'], retry=True)
        assert queue.get(job['id'])['status']=='complete'
        handoff(processor.store, manifest['run_id'], client, {},
            upload_config={'preextract_delivery_mode':'disk_queue_v1','work_pool_auto_start':False})
        assert processor.store.load(manifest['run_id'])['items'][0]['handoff_status']=='uploaded'


def test_old_loaded_process_blocks_activation_without_touching_queue_or_files(flow, tmp_path, monkeypatch):
    import os
    import psutil
    from preextract_cleanup import cleanup_item, cleanup_queue_once
    case = _case(flow, tmp_path, monkeypatch)
    processor, _, _, manifest, item, _, rendered, stream, _ = case
    _policy(processor.store.root, False)
    _deliver(case)
    path=processor.store.root/'private/preextract_delivery/cleanup-policy.json'
    path.write_text(json.dumps(dict(enabled=True,wait_for_processes=[dict(pid=os.getpid(),
        create_time=psutil.Process().create_time())])),encoding='utf8')
    report=cleanup_item(processor.store.root,manifest['run_id'],item)
    assert report['status']=='deferred' and report['reason']=='waiting_for_legacy_process_exit'
    assert cleanup_queue_once(processor.store.root)['waiting_for_legacy'] is True
    assert stream.is_file() and rendered.is_dir()


@pytest.mark.parametrize('failure', ['access_denied', 'missing_time'])
def test_unknown_legacy_process_state_keeps_cleanup_disabled(tmp_path, monkeypatch, failure):
    import psutil
    from preextract_cleanup import legacy_processes_alive
    path=tmp_path/'private/preextract_delivery/cleanup-policy.json'
    path.parent.mkdir(parents=True)
    owner=dict(pid=12345,create_time=1.0)
    if failure=='missing_time':owner.pop('create_time')
    path.write_text(json.dumps(dict(enabled=True,wait_for_processes=[owner])),encoding='utf8')
    if failure=='access_denied':
        def denied(pid):raise psutil.AccessDenied(pid)
        monkeypatch.setattr(psutil,'Process',denied)
    else:
        class Present:
            def is_running(self):return True
            def create_time(self):return 1.0
        monkeypatch.setattr(psutil,'Process',lambda pid:Present())
    assert legacy_processes_alive(tmp_path) is True


def test_concurrent_first_registration_cannot_bypass_original_workspace_lock(tmp_path, monkeypatch):
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from filelock import Timeout
    queue=DeliveryQueue(queue_path(tmp_path))
    first=tmp_path/'first';second=tmp_path/'second'
    first.mkdir();second.mkdir()
    def payload(work):
        return dict(id='same',service='http://localhost:9380',store_root=str(tmp_path),run_id=work.name,
            item=dict(id=work.name,preextract_work_dir=str(work),package_path=str(tmp_path/'public')),
            task={},manifest_sha256='same')
    original=queue._enqueue_locked
    entered=False
    def insert_competing_then_continue(incoming, **kwargs):
        nonlocal entered
        if not entered:
            entered=True
            queue.enqueue(payload(second))
            queue.update('same',status='complete')
            with FileLock(str(second/'operation.lock'),timeout=0):
                return original(incoming,**kwargs)
        return original(incoming,**kwargs)
    monkeypatch.setattr(queue,'_enqueue_locked',insert_competing_then_continue)
    with pytest.raises(Timeout):queue.enqueue(payload(first),retry=True)
    assert queue.get('same')['status']=='complete'
