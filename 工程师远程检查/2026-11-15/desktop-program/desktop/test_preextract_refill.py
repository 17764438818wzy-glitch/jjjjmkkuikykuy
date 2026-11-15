"""The original batch runner refills model requests before local work finishes.

Real DOCX preparation, request journals, validation and sealed publication are
used. Only external transport/token counting come from the offline flow fixture.
"""
import copy
import json
import threading
import hashlib
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path
import pytest

from test_preextract_workbench_flow import flow


def papers(flow, count):
    from docx import Document
    processor, opener, sources = flow
    for number in range(len(sources), count):
        source = Path(sources[0]['path']).with_name(f'paper-{number}.docx')
        doc = Document(sources[0]['path'])
        doc.add_paragraph(f'独立样本编号：{number}')
        doc.save(source)
        sources.append(dict(path=str(source), name=source.name))
    processor.config['concurrency'].update(enabled=True, papers=4, global_llm=4)
    processor.config['preextract_preparation']={'workers':2,'max_pending':8,'office_enabled':True}
    return processor, opener, sources


def test_source_registry_lookup_waits_for_writer_and_reads_committed_workspace(flow, tmp_path):
    import preextract_pipeline as pipeline
    from filelock import FileLock
    processor, _, sources = flow
    source = Path(sources[0]['path'])
    template = Path(processor.config['preextract_template'])
    old = tmp_path / 'registered-before'
    new = tmp_path / 'registered-after'
    pipeline.register_workspace(source, template, old)
    registry = pipeline.ROOT / 'data/single-request/source-index.json'
    started = threading.Event()

    def read_workspace():
        started.set()
        return pipeline.default_work_dir(source, template)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with FileLock(str(registry) + '.lock', timeout=5):
            future = pool.submit(read_workspace)
            assert started.wait(2)
            with pytest.raises(FutureTimeout):
                future.result(timeout=0.2)
            records = json.loads(registry.read_text(encoding='utf8'))
            records[next(iter(records))] = str(new.resolve())
            pipeline.write_json(registry, records)
        assert future.result(timeout=2) == new.resolve()


def test_four_complex_papers_waiting_for_office_do_not_block_later_normal_papers(flow,monkeypatch,tmp_path):
    import preextract_pipeline as pipeline
    import material_conversion as conversion
    import evidence_render
    import fitz
    processor,opener,sources=papers(flow,8)
    processor.config['preextract_preparation']={'workers':4,'max_pending':8,'office_enabled':True}
    processor.config['concurrency'].update(papers=10,global_llm=10)
    heavy={str(Path(r['path'])) for r in sources[:4]}
    ordinary={str(Path(r['path'])) for r in sources[4:]}
    original=pipeline.prepare_material
    waiting=set();ready=set();lock=threading.Lock();release=threading.Event();all_waiting=threading.Event();ordinary_ready=threading.Event()
    backend=conversion.ConversionPool(tmp_path/'conversion')
    monkeypatch.setattr(conversion,'_pool',lambda root:backend)
    monkeypatch.setattr(backend,'_available',lambda:[('lo-1','libreoffice'),('lo-2','libreoffice'),('wps','wps')])
    monkeypatch.setattr(backend,'_identity',lambda engine:'test-'+engine)
    def execute(engine,source,output,target_format,timeout,**kwargs):
        assert release.wait(15)
        output.mkdir(parents=True,exist_ok=True)
        path=output/(source.stem+'.pdf')
        with fitz.open() as pdf:
            pdf.new_page().insert_text((20,30),'Original formula evidence');pdf.save(path)
        return path
    monkeypatch.setattr(backend,'_execute',execute)
    def prepare(source,work,*args,**kwargs):
        result=original(source,work,*args,**kwargs)
        if str(Path(source)) in heavy:
            with lock:
                waiting.add(str(source))
                if len(waiting)==4:all_waiting.set()
            evidence_render.render_office_pdf(source,Path(work)/'office-proof')
        else:
            with lock:
                ready.add(str(source))
                if ready==ordinary:ordinary_ready.set()
        return result
    monkeypatch.setattr(pipeline,'prepare_material',prepare)
    snapshots=[]
    with ThreadPoolExecutor(max_workers=1) as workers:
        future=workers.submit(processor.run,sources,lambda p:snapshots.append(copy.deepcopy(p)))
        try:
            assert all_waiting.wait(10)
            assert ordinary_ready.wait(10),'ordinary material must progress while all four complex papers are blocked'
            assert not release.is_set()
        finally:release.set()
        manifest=future.result(timeout=30)
    assert len(manifest['items'])==8 and all(i['status']=='pass' for i in manifest['items'])
    assert manifest['concurrency']['preparation_peak']<=4
    assert manifest['concurrency']['llm_peak']<=10
    assert any(p.get('pipeline_concurrency',{}).get('conversion_waiting',0)>=4 for p in snapshots)


@pytest.mark.parametrize('count,stop_before_reply', [(12, False), (12, True), (24, False)])
def test_preparation_runs_ahead_of_four_blocked_models_without_building_waiting_requests(flow, monkeypatch, count, stop_before_reply):
    import preextract_pipeline as pipeline
    from ai_resume import retry_records
    processor, opener, sources = papers(flow, count)
    release = threading.Event()
    four_running = threading.Event()
    twelve_prepared = threading.Event()
    lock = threading.Lock()
    requests = plans = 0
    preparing = preparation_peak = 0
    prepared = {}
    snapshots = []
    original_open = opener.open
    original_prepare = pipeline.prepare_material
    original_plan = pipeline.plan_requests

    def slow_model(request, timeout):
        nonlocal requests
        with lock:
            requests += 1
            if requests == 4:
                four_running.set()
        assert release.wait(30), 'test did not release model responses'
        return original_open(request, timeout)

    def prepare(*args, **kwargs):
        nonlocal preparing, preparation_peak
        with lock:
            preparing += 1
            preparation_peak = max(preparation_peak, preparing)
        try:
            return original_prepare(*args, **kwargs)
        finally:
            with lock:
                preparing -= 1

    def plan(*args, **kwargs):
        nonlocal plans
        with lock:
            plans += 1
        return original_plan(*args, **kwargs)

    def progress(payload):
        item = payload.get('ai_item') or {}
        with lock:
            if item.get('prepared_checkpoint'):
                prepared[item['source_hash']] = copy.deepcopy(item)
                if len(prepared) >= 12:
                    twelve_prepared.set()
            if 'pipeline_concurrency' in payload:
                snapshots.append(copy.deepcopy(payload['pipeline_concurrency']))

    monkeypatch.setattr(opener, 'open', slow_model)
    monkeypatch.setattr(pipeline, 'prepare_material', prepare)
    monkeypatch.setattr(pipeline, 'plan_requests', plan)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(processor.run, sources, progress)
        try:
            assert four_running.wait(15)
            assert twelve_prepared.wait(15), 'papers 5-12 must prepare before any of the first four model replies'
            assert requests == plans == 4, 'waiting materials must not build image-bearing requests or call models'
            assert not future.done()
            assert preparation_peak <= 2
            # Eight pending materials include the preparation reservations.
            assert len(prepared) == 12
            if stop_before_reply:
                processor.stop.set()
        finally:
            release.set()
        manifest = future.result(timeout=60)

    assert manifest['concurrency']['llm_peak'] <= 4
    assert snapshots and max(s['postprocess_waiting'] for s in snapshots) <= 8
    if stop_before_reply:
        assert manifest['status'] == 'stopped' and len(opener.calls) == 4
        waiting = [item for item in manifest['items'] if item['ai_result']['status'] == 'not_started']
        assert len(waiting) == 8
        assert all(item['script_result']['status'] == 'completed' for item in waiting)
        assert all(not item['automatic_approval'] for item in manifest['items'])
        materials = {item['source_hash']: hashlib.sha256(Path(item['prepared_checkpoint']['path']).read_bytes()).hexdigest()
                     for item in manifest['items']}
        responses = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                     for item in manifest['items']
                     if (path := Path(item['preextract_work_dir']) / 'model/response.txt').is_file()}
        records = retry_records(manifest['items'])
        assert len(records) == 12, 'prepared but uncalled papers must resume through the original retry entry'
        manifest = processor.run(records, operation='retry_ai')
        assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == value for path, value in responses.items())
        for item in manifest['items']:
            assert hashlib.sha256(Path(item['prepared_checkpoint']['path']).read_bytes()).hexdigest() == materials[item['source_hash']]
    assert len(opener.calls) == count
    assert len(manifest['items']) == count
    assert all(item['status'] == 'pass' for item in manifest['items']), [item.get('reason') for item in manifest['items']]


def test_refill_after_durable_response_with_bounded_local_work(flow, monkeypatch):
    import preextract_pipeline as pipeline
    processor, opener, sources = papers(flow, 12)
    release = threading.Event()
    first_four = threading.Barrier(4)
    ten_requests = threading.Event()
    validate_started = threading.Event()
    lock = threading.Lock()
    active = peak = validations = 0
    original_validate = pipeline.validate_candidate
    original_open = opener.open
    requests = 0
    snapshots = []

    def open_reply(request, timeout):
        nonlocal requests
        with lock:
            requests += 1
            number = requests
        if number <= 4:
            first_four.wait(timeout=15)
        result = original_open(request, timeout)
        if number == 10:
            ten_requests.set()
        return result

    def slow_validate(*args, **kwargs):
        nonlocal active, peak, validations
        with lock:
            active += 1
            validations += 1
            peak = max(peak, active)
            if active == 2:
                validate_started.set()
        try:
            if not release.wait(timeout=30):
                raise AssertionError('test did not release local validation')
            return original_validate(*args, **kwargs)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(opener, 'open', open_reply)
    monkeypatch.setattr(pipeline, 'validate_candidate', slow_validate)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(processor.run, sources, lambda payload: snapshots.append(copy.deepcopy(payload)))
        try:
            assert validate_started.wait(15), 'two papers must reach local processing'
            assert ten_requests.wait(12), 'next papers must start while earlier validation is blocked'
            # Two local workers + eight waiting results; in-flight results are
            # reserved within this bound, rather than added on top of it.
            assert not future.done()
            assert requests == 10
            assert peak == 2
            assert max(p.get('processed_sources', 0) for p in snapshots) == 0
            observed = [p['pipeline_concurrency'] for p in snapshots if 'pipeline_concurrency' in p]
            assert observed and max(p['postprocess_waiting'] for p in observed) <= 8
            assert max(p['postprocess_reserved'] for p in observed) <= 10
            assert max(p['preparation_reserved'] for p in observed) <= 8
        finally:
            release.set()
        manifest = future.result(timeout=40)
    assert requests == 12 and validations == 12
    assert peak == 2
    assert manifest['concurrency']['llm_peak'] == 4
    assert manifest['processed_sources'] == 12
    assert len(manifest['items']) == len({i['id'] for i in manifest['items']}) == 12
    assert all(i['status'] == 'pass' for i in manifest['items']), [i.get('reason') for i in manifest['items']]
    assert manifest['billing']['requests'] == 12
    for item in manifest['items']:
        work = Path(item['preextract_work_dir'])
        state = json.loads((work / 'model/request-state.json').read_text(encoding='utf8'))
        assert state['status'] == 'response_complete' and state['client_http_requests'] == 1
        assert Path(item['package_path']).is_relative_to(processor.store.runs / manifest['run_id'])


def test_stop_while_waiting_keeps_answers_and_original_retry_finishes_without_model(flow, monkeypatch):
    import preextract_pipeline as pipeline
    from ai_resume import retry_records
    from processing_outcomes import ai_outcome
    processor, opener, sources = papers(flow, 6)
    release = threading.Event()
    all_saved = threading.Event()
    seen = set()
    original_validate = pipeline.validate_candidate

    def slow_validate(*args, **kwargs):
        assert release.wait(20)
        return original_validate(*args, **kwargs)

    def progress(payload):
        item = payload.get('ai_item') or {}
        if item.get('ai_result', {}).get('status') == 'response_saved':
            seen.add(item['source_hash'])
            if len(seen) == 6:
                all_saved.set()

    monkeypatch.setattr(pipeline, 'validate_candidate', slow_validate)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(processor.run, sources, progress)
        try:
            assert all_saved.wait(15)
            processor.stop.set()
        finally:
            release.set()
        stopped = future.result(timeout=20)
    assert stopped['status'] == 'stopped'
    assert len(opener.calls) == len(stopped['items']) == 6
    waiting = [i for i in stopped['items'] if i['ai_result']['status'] == 'response_saved']
    assert waiting and all(ai_outcome(i, 'stopped')[0] == '回答已保存' for i in waiting)
    assert not any(i.get('automatic_approval') for i in stopped['items'])
    before = {i['source_hash']: hashlib.sha256((Path(i['preextract_work_dir']) / 'model/response.txt').read_bytes()).hexdigest()
              for i in stopped['items']}
    records = retry_records(stopped['items'])
    assert len(records) == 6, 'original retry must include saved answers awaiting program work'
    completed = processor.run(records, operation='retry_ai')
    assert len(opener.calls) == 6, 'program-only recovery must reuse the original responses'
    assert all(i['status'] == 'pass' for i in completed['items']), [i.get('reason') for i in completed['items']]
    assert completed['billing']['requests'] == 0
    for item in completed['items']:
        assert hashlib.sha256((Path(item['preextract_work_dir']) / 'model/response.txt').read_bytes()).hexdigest() == before[item['source_hash']]


def test_failed_local_step_releases_capacity_and_recovery_keeps_model_content(flow, monkeypatch):
    import preextract_pipeline as pipeline
    processor, opener, sources = papers(flow, 6)
    original_validate = pipeline.validate_candidate
    failed = []
    lock = threading.Lock()

    def fail_once(candidate, *args, **kwargs):
        with lock:
            first = not failed
            if first:
                failed.append(candidate['source']['source_sha256'])
        if first:
            raise OSError('offline local validation failure')
        return original_validate(candidate, *args, **kwargs)

    monkeypatch.setattr(pipeline, 'validate_candidate', fail_once)
    first = processor.run(sources)
    assert len(opener.calls) == 6
    assert len([i for i in first['items'] if i['status'] == 'pass']) == 5
    item = next(i for i in first['items'] if i['status'] == 'reject')
    response = (Path(item['preextract_work_dir']) / 'model/response.txt').read_bytes()
    record = dict(path=item['source_path'], name=Path(item['source_path']).name, prepared_checkpoint=item['prepared_checkpoint'])
    result = processor.run([record], operation='retry_ai')
    assert result['items'][0]['status'] == 'pass', result['items'][0].get('reason')
    assert len(opener.calls) == 6
    assert (Path(item['preextract_work_dir']) / 'model/response.txt').read_bytes() == response


def test_finished_phase_notifications_cannot_reopen_a_paper_or_leak_capacity():
    from auto_pipeline import _PreextractSlots
    slots = _PreextractSlots(1, 1, 1, threading.Event(), prepare_limit=1, max_prepared=1)
    assert slots.admit('first')
    with slots.postprocess('first'):
        assert slots.admit('second')
        assert not slots.admit('third')
    slots.finish('first')
    slots._phase('first', 'post_wait')
    slots.finish('first')
    assert slots.snapshot()['outstanding'] == 1
    with slots.postprocess('second'):
        assert slots.admit('third')
    slots.finish('second')
    slots.finish('third')
    assert slots.snapshot()['outstanding'] == 0


def test_ten_models_refill_while_two_local_workers_are_still_blocked(flow, monkeypatch):
    import preextract_pipeline as pipeline
    processor, opener, sources = papers(flow, 12)
    processor.config['concurrency'].update(papers=10, global_llm=10)
    ten_running = threading.Event()
    next_running = threading.Event()
    release_one = threading.Event()
    release_models = threading.Event()
    local_started = threading.Event()
    release_local = threading.Event()
    lock = threading.Lock()
    starts = 0
    original_open, original_validate = opener.open, pipeline.validate_candidate

    def blocked_open(request, timeout):
        nonlocal starts
        with lock:
            starts += 1
            number = starts
            if number == 10: ten_running.set()
            if number == 11: next_running.set()
        if number == 1:
            assert release_one.wait(15)
        else:
            assert release_models.wait(20)
        return original_open(request, timeout)

    def blocked_local(*args, **kwargs):
        local_started.set()
        assert release_local.wait(20)
        return original_validate(*args, **kwargs)

    monkeypatch.setattr(opener, 'open', blocked_open)
    monkeypatch.setattr(pipeline, 'validate_candidate', blocked_local)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(processor.run, sources)
        try:
            assert ten_running.wait(15)
            release_one.set()
            assert local_started.wait(5)
            assert next_running.wait(3), 'a saved answer must release model admission before local work completes'
        finally:
            release_one.set(); release_models.set(); release_local.set()
        result = future.result(timeout=40)
    assert starts == 12 and result['concurrency']['llm_peak'] == 10
    assert result['concurrency']['postprocess_peak'] <= 2
    assert all(item['status'] == 'pass' for item in result['items'])


def test_archive_next_member_waits_for_model_admission_and_honors_stop():
    from auto_pipeline import _PreextractSlots
    stop = threading.Event()
    slots = _PreextractSlots(1, 1, 2, stop, prepare_limit=1, max_prepared=1)
    assert slots.admit('archive')
    with slots.postprocess('archive'):
        assert slots.admit('other-paper')
        slots.enter_model('other-paper')
    entered = threading.Event()
    def next_member():
        entered.set()
        slots.enter_prepare('archive')
        slots.enter_model('archive')
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(next_member)
        assert entered.wait(2)
        assert not future.done()
        assert not slots.admit('third-paper')
        stop.set()
        with pytest.raises(InterruptedError):
            future.result(timeout=2)
    slots.finish('archive')
    slots.finish('other-paper')


def test_archive_members_keep_original_identity_and_share_stage_limits(flow, tmp_path):
    import zipfile
    processor, opener, sources = papers(flow, 6)
    archive = tmp_path / 'papers.zip'
    with zipfile.ZipFile(archive, 'w') as output:
        for row in sources[:2]:
            output.write(row['path'], arcname=row['name'])
    selected = [dict(path=str(archive), name=archive.name), *sources[2:]]
    manifest = processor.run(selected)
    assert len(opener.calls) == 6
    assert len(manifest['items']) == 6
    assert all(i['status'] == 'pass' for i in manifest['items']), [i.get('reason') for i in manifest['items']]
    members = [i for i in manifest['items'] if i.get('archive_member_path')]
    assert {Path(i['archive_member_path']).name for i in members} == {'paper-0.docx', 'paper-1.docx'}
    assert all(i['input_source_path'] == str(archive) for i in members)
    assert manifest['concurrency']['llm_peak'] <= 4
    assert manifest['concurrency']['postprocess_peak'] <= 2
    assert manifest['billing']['requests'] == 6


def test_reopened_workbench_can_resume_response_saved_at_a_hard_interruption(flow):
    from ai_resume import retry_records
    from engine import Store
    from script_task import task_snapshot
    processor, opener, sources = flow
    captured = []
    def progress(payload):
        item = payload.get('ai_item') or {}
        if item.get('ai_result', {}).get('status') == 'response_saved':
            captured.append(copy.deepcopy(item))
    complete = processor.run(sources[:1], progress)
    assert complete['items'][0]['status'] == 'pass'
    assert captured
    # Reproduce the durable active row at the precise crash boundary, without
    # killing a production process or inventing a terminal failure reason.
    interrupted = dict(complete, status='running', processed_sources=0, items=[], active_items=[captured[0]])
    processor.store.save(interrupted)
    assert not retry_records(task_snapshot(processor.store, interrupted)['items'])
    reopened = Store(processor.store.root)
    reopened.recover()
    snapshot = task_snapshot(reopened, reopened.load(complete['run_id']))
    records = retry_records(snapshot['items'])
    assert len(records) == 1, 'saved answers must remain resumable after process death'
    result = processor.run(records, operation='retry_ai')
    assert result['items'][0]['status'] == 'pass'
    assert len(opener.calls) == 1
