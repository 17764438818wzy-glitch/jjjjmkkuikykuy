"""Exercise the desktop handoff with real sealed packages and an in-memory API."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import threading

from auto_ragflow import handoff
from engine import Store
from test_preextract_publish import Counter, PAPERFLOW, fixture


def prepared_batch(tmp_path, *, two_specialties=True, image=False):
    from preextract_publish import prepare_publication
    candidate, bundle, catalog = fixture()
    # A short real package keeps these delivery tests focused on receipt behavior.
    candidate['profile']['learning_units'][0]['steps'][0]['operation'] = '进行计算'
    if two_specialties:
        candidate['profile']['routing_candidates'].append(dict(code='080802', name='轨道交通信号与控制'))
        catalog['majors'].append(dict(code='080802', name='轨道交通信号与控制',
                                     paper_dataset_id='rail_ds', image_dataset_id='rail_images'))
    if image:
        import hashlib
        from PIL import Image
        path = tmp_path / 'source-image.png'
        Image.new('RGB', (12, 12), 'white').save(path)
        bundle['assets'] = [dict(id='A1', image_path=str(path),
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(), width=12, height=12)]
        candidate['assets']['items'] = [dict(id='A1', kind='figure', caption='控制结构',
            storage='image', section_ids=['S1'], unit_ids=['U1'], meaning='控制系统的关系')]
    work = tmp_path / 'single-request'
    result = prepare_publication(candidate, bundle, work / 'publication',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter())
    (work / 'private').mkdir()
    (work / 'private' / 'prepared-publication.json').write_text(
        json.dumps({k: v for k, v in result.items() if k != 'packet'}, ensure_ascii=False), encoding='utf8')
    store = Store(tmp_path / 'desktop')
    published = store.runs / 'batch' / 'packages' / 'paper1'
    shutil.copytree(result['package_path'], published)
    item = dict(id='paper1', title='控制模型研究', major='自动化', status='pass', automatic_approval=True,
        content_format='paper_preextract', preextract_work_dir=str(work), package_path=str(published),
        path=str(published / 'article.redacted.md'), source_hash=bundle['source']['source_sha256'], uploads={})
    store.save(dict(run_id='batch', status='completed', items=[item],
                    task_settings={'destinations': {'paper': {'id': 'wrong_ui_ds', 'name': '临时选择库'}}}))
    return store, item, result, work


def memory_client(monkeypatch):
    spec = importlib.util.spec_from_file_location('preextract_protocol_fake', PAPERFLOW / 'tests' / 'test_rag.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    client = module.MemoryRag()
    client.base_url = 'http://localhost:9380'
    client.api_key = 'test-current-ui-key'
    def forbidden(*args, **kwargs):
        raise AssertionError('must not use legacy routing, saved credentials, parsing, or scheduler')
    client.datasets = forbidden
    client.parse = forbidden
    import paperflow.rag as rag
    monkeypatch.setattr(rag, 'native_client', forbidden)
    monkeypatch.setattr('auto_ragflow.upload_routed_items', forbidden)
    return client


def interrupted_readback(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {})
    path = work/'delivery-receipt.json'
    receipt = json.loads(path.read_text(encoding='utf8'))
    receipt.update(status='uncertain', stage='readback', error_type='RemoteConflict')
    for target in receipt['targets'].values():
        target.pop('retrieval_verified', None)
        obj = target['body']
        client.docs[(obj['dataset_id'], obj['document_id'])]['status'] = '0'
        for chunk in client.chunks[obj['document_id']]:
            chunk['available'] = False
    path.write_text(json.dumps(receipt), encoding='utf8')
    calls = []
    request = client.request
    def guarded(method, endpoint, body=None, content_type=None):
        calls.append((method, endpoint, copy.deepcopy(body)))
        assert method == 'GET' or (method == 'POST' and endpoint == '/retrieval') or (
            method == 'PATCH' and set(body) <= {'enabled', 'available'})
        value = request(method, endpoint, body, content_type)
        if method == 'PATCH' and 'enabled' in body:
            did = endpoint.rsplit('/', 1)[-1]
            for chunk in client.chunks[did]:
                chunk['available'] = bool(body['enabled'])
        return value
    client.request = guarded
    client.upload_bytes = lambda *a, **k: (_ for _ in ()).throw(AssertionError('must not upload'))
    return store, item, work, client, calls


def test_explicit_reconciliation_finishes_existing_chunks_without_upload(tmp_path, monkeypatch):
    from preextract_handoff import handoff_preextract_item
    store, item, work, client, calls = interrupted_readback(tmp_path, monkeypatch)
    before = copy.deepcopy(client.chunks)
    handoff_preextract_item(store, 'batch', item['id'], client, reconcile=True)
    receipt = json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['status'] == 'complete'
    assert store.load('batch')['items'][0]['handoff_status'] == 'uploaded'
    assert len([c for c in calls if c[0] == 'PATCH']) == 2
    assert all(t['verified'] and t['retrieval_verified'] for t in receipt['targets'].values())
    assert list(work.glob('delivery-receipt.before-reconcile.*.json'))
    assert {did: [(c['id'], c['content']) for c in chunks] for did, chunks in client.chunks.items()} == {
        did: [(c['id'], c['content']) for c in chunks] for did, chunks in before.items()}
    calls.clear()
    handoff_preextract_item(store, 'batch', item['id'], client, reconcile=True)
    assert not [c for c in calls if c[0] == 'PATCH']


def test_reconciliation_preserves_uncertainty_when_existing_content_differs(tmp_path, monkeypatch):
    from preextract_handoff import handoff_preextract_item
    store, item, work, client, calls = interrupted_readback(tmp_path, monkeypatch)
    next(iter(client.chunks.values()))[0]['content'] += 'changed'
    before = (work/'delivery-receipt.json').read_bytes()
    handoff_preextract_item(store, 'batch', item['id'], client, reconcile=True)
    assert (work/'delivery-receipt.json').read_bytes() == before
    assert not [c for c in calls if c[0] != 'GET']
    assert store.load('batch')['items'][0]['handoff_status'] == 'pending'


def test_reconciliation_requires_unchanged_task_and_all_original_targets(tmp_path, monkeypatch):
    from preextract_handoff import handoff_preextract_item
    store, item, work, client, calls = interrupted_readback(tmp_path, monkeypatch)
    path = work/'delivery-receipt.json'
    receipt = json.loads(path.read_text(encoding='utf8'))
    receipt['task']['task_id'] = 'another-task'
    path.write_text(json.dumps(receipt), encoding='utf8')
    handoff_preextract_item(store, 'batch', item['id'], client, reconcile=True)
    assert not calls
    assert store.load('batch')['items'][0]['handoff_status'] == 'pending'


def test_existing_handoff_delivers_all_md_specialties_with_current_client(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {'自动化': {'paper': 'also_wrong'}},
            target_datasets={'paper': {'id': 'wrong_ui_ds'}},
            upload_config={'work_pool_enabled': True, 'work_pool_auto_start': True})
    saved = store.load('batch')['items'][0]
    assert saved['handoff_status'] == 'uploaded'
    assert {ds for ds, did in client.docs} == {'paper_ds', 'rail_ds'}
    assert len(saved['uploads']) == 2
    assert {row['dataset_id'] for row in saved['uploads'].values()} == {'paper_ds', 'rail_ds'}
    receipt = json.loads((work / 'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['status'] == 'complete'
    assert set(receipt['targets']) == {'080801', '080802'}
    assert all(target['verified'] and target['retrieval_verified'] for target in receipt['targets'].values())
    assert all(row['preextract_readback_verified'] for row in saved['uploads'].values())
    assert not list(store.root.rglob('queue.sqlite3'))
    before = client.writes
    handoff(store, 'batch', client, {}, upload_config={'work_pool_enabled': True})
    assert client.writes == before
    assert store.load('batch')['items'][0]['handoff_status'] == 'uploaded'


def test_preextract_handoff_honors_existing_four_paper_concurrency(tmp_path,monkeypatch):
    import preextract_handoff
    store=Store(tmp_path/'desktop')
    items=[dict(id=f'paper{i}',status='pass',automatic_approval=True,content_format='paper_preextract') for i in range(8)]
    store.save(dict(run_id='batch',status='completed',items=items))
    lock=threading.Lock();barrier=threading.Barrier(4,timeout=1);active=0;peak=0;seen=[]
    stop=threading.Event();client=object()
    def bounded_delivery(actual_store,run_id,item_id,actual_client,*,progress,stop):
        nonlocal active,peak
        assert actual_store is store and actual_client is client
        with lock:
            active+=1;peak=max(peak,active);seen.append((item_id,stop))
        try:
            try:barrier.wait()
            except threading.BrokenBarrierError:pass
            preextract_handoff._save(store,run_id,item_id,dict(handoff_status='uploaded'))
        finally:
            with lock:active-=1
    monkeypatch.setattr(preextract_handoff,'handoff_preextract_item',bounded_delivery)
    handoff(store,'batch',client,{},concurrency=dict(enabled=True,papers=4,global_llm=4),
            upload_config={'_stop_event':stop})
    assert peak==4
    assert sorted(item_id for item_id,_ in seen)==[f'paper{i}' for i in range(8)]
    assert all(event is stop for _,event in seen)
    assert all(row['handoff_status']=='uploaded' for row in store.load('batch')['items'])


def test_local_revision_activation_failure_does_not_erase_verified_remote_delivery(tmp_path,monkeypatch):
    store,item,prepared,work=prepared_batch(tmp_path)
    client=memory_client(monkeypatch)
    (work/'revision.json').write_text('{}',encoding='utf8')
    (work/'material.json').write_text(json.dumps(dict(version='preextract-material/1.0',
        source=dict(file='local-source.docx',source_sha256=prepared['task']['source_sha256']))),encoding='utf8')
    def activation_failure(*a,**kw):raise ValueError('local pointer could not be saved')
    monkeypatch.setattr('preextract_pipeline.activate_revision',activation_failure)
    handoff(store,'batch',client,{})
    saved=store.load('batch')['items'][0]
    assert saved['handoff_status']=='uploaded'
    assert saved['preextract_delivery']['status']=='complete'
    assert saved['preextract_revision_activation']['status']=='pending'
    assert json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))['status']=='complete'


def test_local_revision_status_save_failure_preserves_verified_remote_delivery(tmp_path,monkeypatch):
    import preextract_handoff
    store,item,prepared,work=prepared_batch(tmp_path)
    client=memory_client(monkeypatch)
    (work/'revision.json').write_text('{}',encoding='utf8')
    (work/'material.json').write_text(json.dumps(dict(version='preextract-material/1.0',
        source=dict(file='local-source.docx',source_sha256=prepared['task']['source_sha256']))),encoding='utf8')
    monkeypatch.setattr('preextract_pipeline.activate_revision',lambda *a,**kw:None)
    save=preextract_handoff._save
    def fail_local_status(store,run_id,item_id,fields):
        if 'preextract_revision_activation' in fields:
            raise OSError('local activation status could not be saved')
        return save(store,run_id,item_id,fields)
    monkeypatch.setattr(preextract_handoff,'_save',fail_local_status)
    handoff(store,'batch',client,{})
    saved=store.load('batch')['items'][0]
    assert saved['handoff_status']=='uploaded'
    assert saved['preextract_delivery']['status']=='complete'
    assert json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))['status']=='complete'


def test_result_view_uses_each_readback_receipt_instead_of_remote_parse_state(tmp_path, monkeypatch):
    from rag_status import describe_item
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {})
    manifest = store.load('batch')
    item = manifest['items'][0]
    # Direct chunk publication need not trigger RAGFlow's original-file parser.
    for record in item['uploads'].values():
        record['ragflow_status'] = 'unstarted'
    state = describe_item(item, manifest, client.base_url)
    assert state['upload'] == '已上传'
    assert state['process'] == '已完成'
    assert set(state['target_ids']) == {'paper_ds', 'rail_ds'}
    assert '自动化' in state['target'] and '轨道交通信号与控制' in state['target']
    assert not state['can_retry']


def test_lost_write_response_keeps_receipt_and_never_resends(tmp_path, monkeypatch):
    from rag_status import describe_item
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    client.lose_chunk_response = True
    handoff(store, 'batch', client, {})
    manifest = store.load('batch')
    saved = manifest['items'][0]
    assert saved['handoff_status'] == 'pending'
    receipt_path = work / 'delivery-receipt.json'
    receipt = json.loads(receipt_path.read_text(encoding='utf8'))
    assert receipt['status'] == 'uncertain'
    assert receipt['targets']['080801']['body']['document_id']
    assert next(iter(saved['uploads'].values()))['document_id']
    state = describe_item(saved, manifest, client.base_url)
    assert state['process'] != '已完成'
    assert not state['can_retry']
    before = (client.writes, receipt_path.read_bytes())
    handoff(store, 'batch', client, {}, item_ids=['paper1'])
    assert (client.writes, receipt_path.read_bytes()) == before


def test_sealed_copy_drift_cannot_upload(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    Path(item['path']).write_text('changed after approval', encoding='utf8')
    handoff(store, 'batch', client, {})
    assert client.writes == 0
    assert store.load('batch')['items'][0]['handoff_status'] == 'pending'
    assert not (work / 'delivery-receipt.json').exists()


def test_complete_receipt_cannot_be_reused_for_a_different_server(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {})
    receipt_before = (work / 'delivery-receipt.json').read_bytes()
    other = memory_client(monkeypatch)
    other.base_url = 'http://localhost:9381'
    handoff(store, 'batch', other, {})
    assert other.writes == 0
    saved = store.load('batch')['items'][0]
    assert saved['handoff_status'] == 'pending'
    assert '服务' in saved['handoff_reason']
    assert (work / 'delivery-receipt.json').read_bytes() == receipt_before


def test_unbound_prior_receipt_blocks_delivery_and_retry_action(tmp_path, monkeypatch):
    from rag_status import describe_item
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    receipt_path = work / 'delivery-receipt.json'
    receipt_path.write_text(json.dumps({'status': 'uncertain', 'targets': {}}), encoding='utf8')
    before = receipt_path.read_bytes()
    handoff(store, 'batch', client, {})
    manifest = store.load('batch')
    assert client.writes == 0 and receipt_path.read_bytes() == before
    assert manifest['items'][0]['handoff_status'] == 'pending'
    assert not describe_item(manifest['items'][0], manifest, client.base_url)['can_retry']


def test_required_images_get_independent_target_receipts_and_byte_readback(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path, image=True)
    client = memory_client(monkeypatch)
    checked = []
    image = Path(prepared['package_path']) / prepared['packet']['assets'][0]['file']
    def download(image_id):
        checked.append(image_id)
        return image.read_bytes()
    client.download_image = download
    handoff(store, 'batch', client, {})
    saved = store.load('batch')['items'][0]
    assert saved['handoff_status'] == 'uploaded'
    assert {ds for ds, did in client.docs} == {'paper_ds', 'image_ds', 'rail_ds', 'rail_images'}
    assert len(checked) == 2
    assert len(saved['uploads']) == 4
    assert sum(row['route'] == 'image' for row in saved['uploads'].values()) == 2


def test_partial_specialty_confirmation_cannot_report_complete(tmp_path, monkeypatch):
    from rag_status import describe_item
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    request = client.request
    def fail_last_retrieval(method, endpoint, body=None, content_type=None):
        if endpoint == '/retrieval' and body.get('dataset_ids') == ['rail_ds']:
            raise TimeoutError('retrieval readback interrupted')
        return request(method, endpoint, body, content_type)
    client.request = fail_last_retrieval
    handoff(store, 'batch', client, {})
    manifest = store.load('batch')
    saved = manifest['items'][0]
    assert saved['handoff_status'] == 'pending'
    receipt = json.loads((work / 'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['targets']['080801']['retrieval_verified']
    assert not receipt['targets']['080802'].get('retrieval_verified')
    state = describe_item(saved, manifest, client.base_url)
    assert state['upload'] == '已上传' and state['process'] != '已完成'
    assert '检索确认 1/2' in state['assets']
    before = client.writes
    handoff(store, 'batch', client, {})
    assert client.writes == before


def test_stopped_before_handoff_performs_no_remote_operation(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    stop = threading.Event()
    stop.set()
    handoff(store, 'batch', client, {}, upload_config={'_stop_event': stop})
    assert client.writes == 0
    assert not (work / 'delivery-receipt.json').exists()
    assert store.load('batch')['items'][0]['handoff_status'] != 'uploaded'


def test_no_approved_candidate_does_not_enter_legacy_upload_or_queue(tmp_path,monkeypatch):
    store,item,prepared,work=prepared_batch(tmp_path)
    manifest=store.load('batch');manifest['items'][0]['status']='reject';store.save(manifest)
    client=memory_client(monkeypatch)
    handoff(store,'batch',client,{},upload_config={'work_pool_enabled':True,'research_profile':'legacy-invalid'})
    assert client.writes==0 and not (work/'delivery-receipt.json').exists()


def test_deleted_document_is_recreated_from_existing_packet_and_old_receipt_is_retained(tmp_path, monkeypatch):
    store,item,prepared,work=prepared_batch(tmp_path)
    client=memory_client(monkeypatch)
    handoff(store,'batch',client,{})
    receipt=json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))
    missing=receipt['targets']['080802']['body']
    retained=receipt['targets']['080801']['body']
    del client.docs[(missing['dataset_id'],missing['document_id'])]
    del client.chunks[missing['document_id']]
    before=client.writes
    handoff(store,'batch',client,{})
    saved=store.load('batch')['items'][0]
    assert saved['handoff_status']=='uploaded',saved.get('handoff_reason')
    assert client.writes>before
    assert len(client.docs)==2 and (retained['dataset_id'],retained['document_id']) in client.docs
    assert len(list(work.glob('delivery-receipt.*.json')))==1
    assert json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))['remote_reconciliation']['missing']


def test_unreachable_remote_cannot_turn_old_receipt_into_current_completion(tmp_path, monkeypatch):
    from rag_status import describe_item
    store,item,prepared,work=prepared_batch(tmp_path)
    client=memory_client(monkeypatch)
    handoff(store,'batch',client,{})
    receipt=(work/'delivery-receipt.json').read_bytes();before=client.writes
    def offline(*args,**kwargs):raise TimeoutError('offline')
    client.request=offline
    handoff(store,'batch',client,{})
    manifest=store.load('batch');saved=manifest['items'][0]
    assert saved['handoff_status']=='pending' and client.writes==before
    assert (work/'delivery-receipt.json').read_bytes()==receipt
    assert describe_item(saved,manifest,client.base_url)['process']!='已完成'


def test_failed_recreation_does_not_disable_other_completed_specialties(tmp_path,monkeypatch):
    store,item,prepared,work=prepared_batch(tmp_path)
    client=memory_client(monkeypatch);handoff(store,'batch',client,{})
    receipt=json.loads((work/'delivery-receipt.json').read_text(encoding='utf8'))
    missing=receipt['targets']['080802']['body'];retained=receipt['targets']['080801']['body']
    del client.docs[(missing['dataset_id'],missing['document_id'])]
    del client.chunks[missing['document_id']]
    def offline(*args,**kwargs):raise TimeoutError('replacement upload response unknown')
    client.upload_bytes=offline
    handoff(store,'batch',client,{})
    assert store.load('batch')['items'][0]['handoff_status']=='pending'
    assert client.docs[(retained['dataset_id'],retained['document_id'])]['status']=='1'


def test_stop_after_current_write_preserves_uncertain_receipt_without_next_request(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path)
    client = memory_client(monkeypatch)
    stop = threading.Event()
    request = client.request
    def stop_after_chunk(method, endpoint, body=None, content_type=None):
        result = request(method, endpoint, body, content_type)
        if method == 'POST' and '/chunks' in endpoint:
            stop.set()
        return result
    client.request = stop_after_chunk
    handoff(store, 'batch', client, {}, upload_config={'_stop_event': stop})
    assert client.writes == 2  # one document stub and one chunk, then cooperative stop
    receipt = json.loads((work / 'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['status'] == 'uncertain' and receipt['error_type'] == 'InterruptedError'
    saved = store.load('batch')['items'][0]
    assert saved['handoff_status'] == 'pending'
    assert saved['preextract_delivery']['targets']['080801']['body']['document_id']
