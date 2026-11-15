"""The existing desktop runner owns one-request work and both result views."""
import copy
import json
from pathlib import Path

import pytest


def test_existing_desktop_has_only_its_original_tabs(ui):
    _, app = ui
    labels = [app.tabs.tab(tab, 'text').strip() for tab in app.tabs.tabs()]
    assert len(labels) == 6
    assert '选择与处理' in labels and 'AI 处理结果' in labels
    assert not any('一次提取' in label for label in labels)


from test_script_task_ui import ui


@pytest.fixture
def runner(tmp_path, monkeypatch):
    from engine import Store
    from auto_pipeline import AutoProcessor
    import preextract_pipeline
    source = tmp_path / 'paper.txt'
    source.write_text('完整研究材料。研究问题、操作、结果和限制。', encoding='utf8')
    monkeypatch.setattr(preextract_pipeline, 'ROOT', tmp_path / 'isolated-registry')
    proc = AutoProcessor(Store(tmp_path / 'data'), {
        'extraction_mode': 'paper-preextract/1.0', 'auto_upload': False,
        'concurrency': {'enabled': False}, 'resume': {'enabled': False}})
    monkeypatch.setattr(proc, '_material_screen', lambda *a, **k: pytest.fail('old semantic stages must not run'))
    return proc, source


def test_original_runner_records_program_failure_without_entering_ai(runner, monkeypatch):
    import preextract_workbench as bridge
    from processing_errors import LocalDocumentError
    from processing_outcomes import execution_outcome, ai_outcome
    proc, source = runner
    def fail(*args, **kw):
        kw['progress']('checking_request')
        raise LocalDocumentError('request_capacity', '图像总容量超限：7 张图片；材料已保留')
    monkeypatch.setattr(bridge, 'run_single', fail)
    result = proc.run([{'path': str(source), 'name': source.name}])
    item = result['items'][0]
    assert execution_outcome(item, result)[0] == '不通过'
    assert '7 张图片' in execution_outcome(item, result)[1]
    assert ai_outcome(item, result['status'])[0] == '未进入 AI'
    assert item['failure_stage'] == '检查请求容量'


def test_ai_failure_keeps_prepared_program_result(runner, monkeypatch):
    import preextract_workbench as bridge
    from processing_outcomes import execution_outcome, ai_outcome
    proc, source = runner
    def fail(*args, **kw):
        work = Path(kw['work_dir']); work.mkdir(parents=True)
        (work / 'material.json').write_text('{}', encoding='utf8')
        kw['progress']('material_prepared')
        kw['progress']('extracting_once')
        kw['progress']('模型正在输出完整提取结果')
        raise TimeoutError('synthetic secret must not be shown')
    monkeypatch.setattr(bridge, 'run_single', fail)
    result = proc.run([{'path': str(source), 'name': source.name}])
    item = result['items'][0]
    assert execution_outcome(item, result)[0] == '通过'
    assert ai_outcome(item, result['status'])[0] == '调用失败'
    assert len(item['ai_steps']) == 1
    assert 'secret' not in json.dumps(item)


def test_packaging_failure_does_not_erase_valid_ai_result(runner, monkeypatch):
    import preextract_workbench as bridge
    from processing_outcomes import execution_outcome, ai_outcome
    proc, source = runner
    def fail(*args, **kw):
        kw['progress']('extracting_once')
        kw['progress']('candidate_validated')
        kw['progress']('redacting_and_packaging')
        raise OSError('synthetic')
    monkeypatch.setattr(bridge, 'run_single', fail)
    result = proc.run([{'path': str(source), 'name': source.name}])
    item = result['items'][0]
    assert execution_outcome(item, result)[0] == '不通过'
    assert ai_outcome(item, result['status'])[0] == '通过'
    assert len(item['ai_steps']) == 1


def test_saved_incomplete_model_request_keeps_program_preparation_success(runner, monkeypatch):
    import preextract_workbench as bridge
    from processing_outcomes import execution_outcome
    proc, source = runner
    def fail(*args, **kw):
        work = Path(kw['work_dir']); work.mkdir(parents=True)
        (work / 'material.json').write_text('{}', encoding='utf8')
        kw['progress']('material_prepared')
        kw['progress']('saved_model_request_incomplete')
        raise RuntimeError('该原稿已有失败或未决模型请求，不自动重发')
    monkeypatch.setattr(bridge, 'run_single', fail)
    result = proc.run([{'path': str(source), 'name': source.name}])
    item = result['items'][0]
    assert execution_outcome(item, result)[0] == '通过'
    assert item['ai_result']['status'] == 'incomplete'
    assert len(item['ai_steps']) == 1


def test_desktop_connection_uses_selected_model_and_existing_limits(runner, monkeypatch):
    import preextract_workbench as bridge
    proc, _ = runner
    proc.config.update(base_url='http://localhost:8081/v1', text_model='chosen-model', timeout=123, max_tokens=23000,
        preextract_limits={'context_window': 300000, 'max_output_tokens': 32000, 'supports_images': True,
                           'max_total_image_bytes': 2097152})
    proc.agent.config = proc.config
    proc.agent.key = 'test-key'
    connection = bridge.model_connection(proc)
    assert connection['base_url'] == 'http://localhost:8081/v1'
    assert connection['api_key'] == 'test-key'
    assert connection['model'] == 'chosen-model'
    assert connection['timeout'] == 123 and connection['max_tokens'] == 23000


def test_original_form_preserves_bound_model_capacity(ui):
    from auto_config import load_config,save_config
    from auto_ui import read_ai_form
    _,app=ui
    config=load_config(app.store.root)
    limits=dict(model='alias-model',base_url='http://localhost:8081/v1',context_window=1000000,
                max_output_tokens=128000,supports_images=True)
    config['preextract_limits']=limits
    save_config(app.store.root,config)
    current,_=read_ai_form(app)
    assert current['preextract_limits']==limits
    app.ai_vars['max_tokens'].set(128000)
    current,_=read_ai_form(app)
    assert current['max_tokens']==128000


def test_desktop_saves_long_extraction_timeout_and_starts_original_four_lane_flow(ui, monkeypatch):
    from auto_config import load_config, load_key, save_config
    from auto_ui import save_form
    from preextract_workbench import model_connection
    from test_script_task_ui import configure

    _, app = ui
    configure(app)
    app.ai_vars['base_url'].set('http://127.0.0.1:9/v1')
    app.ai_vars['text_model'].set('verified-image-capable-model')
    app.ai_vars['timeout'].set('3600')
    app.ai_vars['max_tokens'].set('128000')
    app.ai_vars['concurrency_enabled'].set(True)
    app.ai_vars['concurrency_papers'].set('4')
    app.ai_vars['concurrency_global_llm'].set('4')
    capacity = load_config(app.store.root)
    capacity['preextract_limits'] = dict(model='verified-image-capable-model',
        base_url='http://127.0.0.1:9/v1', context_window=1000000,
        max_output_tokens=128000, supports_images=True)
    save_config(app.store.root, capacity)
    monkeypatch.setattr('urllib.request.OpenerDirector.open',
                        lambda *a, **k: pytest.fail('Settings and start preparation must not call a model'))

    save_form(app)
    saved = load_config(app.store.root)
    assert saved['timeout'] == 3600 and saved['max_tokens'] == 128000
    assert saved['concurrency']['papers'] == saved['concurrency']['global_llm'] == 4
    assert load_key(app.store.root) == 'synthetic-key'

    starts = []
    monkeypatch.setattr(app, 'async_task', lambda *a, **k: starts.append((a, k)))
    monkeypatch.setattr(app, 'error', lambda message: pytest.fail(message))
    app.start_automatic([dict(path='paper.docx', name='paper.docx')])
    assert len(starts) == 1
    assert app.processor.config['extraction_mode'] == 'paper-preextract/1.0'
    assert app.processor.config['preextract_delivery_mode'] == 'disk_queue_v1'
    assert model_connection(app.processor)['timeout'] == 3600
    assert app.processor.config['concurrency']['papers'] == 4


def test_original_defer_button_still_prevents_new_format_auto_upload(ui,monkeypatch):
    from test_script_task_ui import configure
    _,app=ui;configure(app)
    app.defer_task_upload()
    monkeypatch.setattr(app,'async_task',lambda *args,**kwargs:None)
    monkeypatch.setattr(app,'error',lambda message:pytest.fail(message))
    app.start_automatic([dict(path='paper.docx',name='paper.docx')])
    assert not app.processor.config['auto_upload'] and not app.processor.rag_client


def test_new_md_auto_upload_requires_rag_key_even_without_legacy_dataset_selection(ui):
    from test_script_task_ui import configure
    _, app = ui
    configure(app)
    app.dataset.set('')
    app.image_dataset.set('')
    app.key.set('')
    app.ai_vars['auto_upload'].set(True)
    assert 'RAGFlow' in app.task_setup_problem()
    app.defer_task_upload()
    assert app.task_setup_problem() == ''


def test_new_md_uses_final_specialties_without_blocking_on_stale_single_library(ui):
    from test_script_task_ui import configure
    _, app = ui
    configure(app)
    app.dataset.set('以前选择的旧知识库')
    app.image_dataset.set('以前选择的旧图片库')
    app.connected_url = 'http://old-service.invalid'
    assert app.task_setup_problem() == ''


def test_real_capacity_error_preserves_safe_counts_in_program_result(tmp_path):
    from preextract_request import build_request
    from preextract_workbench import _failure
    from test_preextract_request import _image_bundle
    bundle=_image_bundle(tmp_path)
    with pytest.raises(ValueError) as error:
        build_request(bundle,'模板',{},dict(model='test',max_total_image_bytes=1))
    failure=_failure(error.value)
    assert failure['code']=='request_capacity'
    assert '2 张图像' in failure['message'] and 'Base64' in failure['message']
    assert '上限 1 字节' in failure['message']


def test_original_start_passes_current_selection_to_existing_runner(ui, tmp_path, monkeypatch):
    from test_script_task_ui import configure
    _,app=ui;configure(app)
    selected=[{'path':str(tmp_path/'paper.docx'),'name':'paper.docx'}]
    captured=[]
    def async_task(fn,done,kind=None):
        assert kind=='processing'
        app.processor.run=lambda rows,callback,operation:captured.append((rows,operation,app.processor.config))
        fn()
    monkeypatch.setattr(app,'async_task',async_task)
    app.start_automatic(selected)
    assert captured and captured[0][0]==selected
    config=captured[0][2]
    assert config['text_model']=='text-fixture'
    assert config['extraction_mode']=='paper-preextract/1.0'
    assert config['task_settings']['preextract_delivery_mode']=='disk_queue_v1'
    assert len(app.tabs.tabs())==6


@pytest.mark.parametrize('operation',['retry_failed','retry_ai'])
@pytest.mark.parametrize('queued_batch',[False,True])
def test_original_retry_keeps_only_new_batch_delivery_mode(ui,monkeypatch,operation,queued_batch):
    from test_script_task_ui import configure
    _,app=ui;configure(app)
    app.current=dict(run_id='previous',task_settings=dict(destinations={'paper':{},'image':{}}))
    if queued_batch:
        app.current['task_settings']['preextract_delivery_mode']='disk_queue_v1'
    app._pending_operation=operation
    monkeypatch.setattr(app,'async_task',lambda *args,**kwargs:None)
    monkeypatch.setattr(app,'error',lambda message:pytest.fail(message))
    app.start_automatic([dict(path='paper.docx',name='paper.docx',
        prepared_checkpoint={'format':'paper-preextract-prepared/1'})])
    config=app.processor.config
    assert (config.get('preextract_delivery_mode')=='disk_queue_v1') is queued_batch
    assert (config['task_settings'].get('preextract_delivery_mode')=='disk_queue_v1') is queued_batch


def test_original_rag_import_uses_md_routes_without_reselecting_single_library(ui, monkeypatch):
    _,app=ui
    item={'id':'a','status':'pass','automatic_approval':True,'content_format':'paper_preextract'}
    manifest={'run_id':'test','status':'completed','items':[item]}
    app.store.save(manifest);app.current=manifest
    app.key.set('test-local-key');app.dataset.set('');app.image_dataset.set('')
    monkeypatch.setattr(app,'selected_rag_items',lambda:[item])
    calls=[]
    monkeypatch.setattr('auto_ragflow.handoff',lambda *args,**kw:calls.append((args,kw)))
    monkeypatch.setattr(app,'error',lambda message:pytest.fail(message))
    def async_task(fn,done,kind=None):
        assert kind=='processing'
        fn()
    monkeypatch.setattr(app,'async_task',async_task)
    app.import_rag_selected()
    assert len(calls)==1
    assert calls[0][1]['item_ids']==['a']
    assert calls[0][1]['upload_config']['_stop_event'] is app.processor.stop
    assert 'handoff_destinations' not in app.store.load('test')['items'][0]


def test_targeted_read_uses_existing_gate_ledger_and_once_client(runner, tmp_path, monkeypatch):
    import threading
    from types import SimpleNamespace
    import preextract_request
    import preextract_workbench as bridge
    proc, _ = runner
    calls=[]; gate=threading.BoundedSemaphore(1)
    proc.agent.llm_gate=gate
    proc.agent.ledger=SimpleNamespace(
        begin=lambda *args: calls.append('begin') or 'receipt',
        finish=lambda *args, **kw: calls.append(('finish', kw['ok'])))
    proc.agent.on_usage=lambda: calls.append('usage')
    def once(body, connection, model_dir, **kw):
        assert not gate.acquire(blocking=False)
        assert kw['opener'] is proc.agent.opener
        if kw.get('on_request_started'):kw['on_request_started']()
        Path(model_dir).mkdir()
        (Path(model_dir)/'request-state.json').write_text(json.dumps(
            dict(status='response_complete',usage={'completion_tokens':11})))
        calls.append('once')
        return 'saved result'
    monkeypatch.setattr(preextract_request,'request_once',once)
    assert bridge.invoke_once(proc,{},dict(model='selected'),tmp_path/'supplement')=='saved result'
    assert calls==['begin','once',('finish',True),'usage']
    assert gate.acquire(blocking=False)
    gate.release()


@pytest.mark.parametrize('same_request',[True,False])
def test_targeted_read_cache_or_rejected_reuse_never_bills_old_usage(runner,tmp_path,same_request):
    import hashlib
    from types import SimpleNamespace
    import preextract_workbench as bridge
    from preextract_material import write_json,file_sha
    proc,_=runner;model_dir=tmp_path/'saved';model_dir.mkdir()
    body=dict(model='selected',messages=[])
    (model_dir/'response.txt').write_text('saved result',encoding='utf8')
    write_json(model_dir/'request-state.json',dict(status='response_complete',
        request_sha256=hashlib.sha256(json.dumps(body,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),
        response_sha256=file_sha(model_dir/'response.txt'),usage={'completion_tokens':11}))
    calls=[]
    proc.agent.ledger=SimpleNamespace(begin=lambda *a:calls.append('begin') or 'receipt',
        finish=lambda *a,**kw:calls.append(('finish',kw['ok'])))
    proc.agent.on_usage=lambda:calls.append('usage')
    if same_request:
        assert bridge.invoke_once(proc,body,dict(model='selected'),model_dir)=='saved result'
    else:
        with pytest.raises(RuntimeError,match='已存在'):
            bridge.invoke_once(proc,dict(body,messages=['different']),dict(model='selected'),model_dir)
    assert calls==[]


def test_child_and_global_requests_share_the_original_four_slot_gate(runner,tmp_path,monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    import preextract_request
    import preextract_workbench as bridge
    from preextract_material import write_json
    proc,_=runner;proc.agent.llm_gate=threading.BoundedSemaphore(4)
    proc.agent.ledger=None;lock=threading.Lock();active=[0];peak=[0];entered=[0]
    first_four=threading.Event();release=threading.Event()
    def once(body,connection,model_dir,**kw):
        with lock:
            active[0]+=1;entered[0]+=1;peak[0]=max(peak[0],active[0])
            if active[0]==4:first_four.set()
        try:
            assert release.wait(5)
            Path(model_dir).mkdir()
            write_json(Path(model_dir)/'request-state.json',dict(status='response_complete'))
            return 'saved result'
        finally:
            with lock:active[0]-=1
    monkeypatch.setattr(preextract_request,'request_once',once)
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures=[pool.submit(bridge.invoke_once,proc,{},dict(model='selected'),tmp_path/f'{kind}-{i}')
                 for kind in ('part','global') for i in range(6)]
        try:assert first_four.wait(5)
        finally:release.set()
        assert all(f.result(timeout=5)=='saved result' for f in futures)
    assert entered[0]==12 and peak[0]==4 and active[0]==0

