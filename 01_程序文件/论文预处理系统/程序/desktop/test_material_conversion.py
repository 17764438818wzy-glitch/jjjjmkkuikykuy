import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import fitz
import pytest


def pdf(path, text='Retain full equation and figure evidence'):
    path.parent.mkdir(parents=True, exist_ok=True)
    with fitz.open() as doc:
        doc.new_page().insert_text((40, 50), text)
        doc.save(path)
    return path


def test_conversion_wait_releases_parser_but_retains_material_reservation():
    from auto_pipeline import _PreextractSlots
    slots = _PreextractSlots(10, 2, 18, threading.Event(), prepare_limit=4, max_prepared=8)
    assert slots.admit('complex-1') and slots.admit('complex-2')
    with slots.conversion('complex-1'), slots.conversion('complex-2'):
        for i in range(4):
            assert slots.admit('normal-'+str(i))
        snapshot = slots.snapshot()
        assert snapshot['preparation_active'] == 4
        assert snapshot['conversion_waiting'] == 2
        assert snapshot['preparation_reserved'] == 6
        for i in range(4):
            slots.finish('normal-'+str(i))
    assert slots.snapshot()['preparation_active'] == 2
    for name in ['complex-1', 'complex-2']:
        slots.finish(name)
    assert slots.snapshot()['outstanding'] == 0


def test_full_material_reservations_resume_without_waiting_on_themselves():
    from auto_pipeline import _PreextractSlots
    slots = _PreextractSlots(10, 2, 18, threading.Event(), prepare_limit=4, max_prepared=4)
    for i in range(4):
        assert slots.admit(str(i))
    with slots.conversion('0'), slots.conversion('1'), slots.conversion('2'), slots.conversion('3'):
        assert not slots.admit('extra')
        assert slots.snapshot()['preparation_reserved'] == 4
    assert slots.snapshot()['preparation_active'] == 4


def test_cancelled_conversion_does_not_return_to_parser_or_leak_reservations():
    from auto_pipeline import _PreextractSlots
    stop = threading.Event()
    slots = _PreextractSlots(10, 2, 18, stop, prepare_limit=4)
    assert slots.admit('complex')
    with pytest.raises(InterruptedError):
        with slots.conversion('complex'):
            stop.set()
    assert slots.snapshot()['preparation_active'] == 0
    slots.finish('complex')
    assert slots.snapshot()['outstanding'] == 0


def pool_fixture(tmp_path, monkeypatch):
    import material_conversion as module
    pool = module.ConversionPool(tmp_path/'shared')
    monkeypatch.setattr(pool, '_available', lambda: [(f'lo-{i+1}','libreoffice') for i in range(4)]+[('wps','wps')])
    monkeypatch.setattr(pool, '_identity', lambda engine: 'test-'+engine)
    return pool


def test_four_libreoffice_and_one_wps_share_work_without_exceeding_limits(tmp_path, monkeypatch):
    pool = pool_fixture(tmp_path, monkeypatch)
    active = {}; peaks = {}; lock = threading.Lock()
    three = threading.Event(); release = threading.Event(); started = []

    def execute(engine, source, output, target_format, timeout):
        with lock:
            active[engine] = active.get(engine, 0)+1
            peaks[engine] = max(peaks.get(engine, 0), active[engine])
            started.append(engine)
            if sum(active.values()) == 5: three.set()
        try:
            assert release.wait(5)
            return pdf(output/(source.stem+'.pdf'))
        finally:
            with lock: active[engine] -= 1

    monkeypatch.setattr(pool, '_execute', execute)
    sources = []
    for i in range(10):
        path = tmp_path/f'{i}.wmf';path.write_bytes(('formula-'+str(i)).encode());sources.append(path)
    with ThreadPoolExecutor(max_workers=10) as workers:
        futures = [workers.submit(pool.convert, source, tmp_path/f'out-{i}', target_format='pdf') for i, source in enumerate(sources)]
        try:
            assert three.wait(5)
            assert sorted(started) == ['libreoffice']*4+['wps']
        finally: release.set()
        assert all(f.result(timeout=5).is_file() for f in futures)
    assert peaks == {'libreoffice': 4, 'wps': 1}


def test_cache_reuses_conversion_without_dropping_occurrences_or_holding_parser(tmp_path, monkeypatch):
    pool = pool_fixture(tmp_path, monkeypatch)
    executed = []; scopes = []

    def execute(engine, source, output, target_format, timeout):
        executed.append(engine)
        return pdf(output/(source.stem+'.pdf'))

    @contextmanager
    def wait_scope():
        scopes.append('wait')
        yield
        scopes.append('resume')

    monkeypatch.setattr(pool, '_execute', execute)
    source = tmp_path/'formula.wmf'; source.write_bytes(b'identical formula bytes')
    first = pool.convert(source,tmp_path/'one',target_format='pdf',wait_scope=wait_scope)
    second = pool.convert(source,tmp_path/'two',target_format='pdf',wait_scope=wait_scope)
    assert first != second and first.read_bytes() == second.read_bytes()
    assert source.read_bytes() == b'identical formula bytes'
    assert len(executed) == 1 and scopes == ['wait','resume']


def test_context_routes_only_material_calls_and_unwinds_on_failure(tmp_path, monkeypatch):
    import engine, evidence_render, material_conversion as module
    pool = pool_fixture(tmp_path, monkeypatch); calls = []
    monkeypatch.setattr(module, '_pool', lambda root: pool)
    monkeypatch.setattr(pool, '_execute', lambda backend, source, output, target_format, timeout: calls.append((backend,target_format)) or pdf(output/(source.stem+'.pdf')))
    source=tmp_path/'figure.emf';source.write_bytes(b'original')
    assert module.current() is None
    with pytest.raises(RuntimeError,match='test exit'):
        with module.preparation_context(tmp_path):
            assert evidence_render.render_office_pdf(source,tmp_path/'pdf').is_file()
            raise RuntimeError('test exit')
    assert module.current() is None and len(calls) == 1


def test_visual_cancel_propagates_without_becoming_an_image_reading_problem(tmp_path,monkeypatch):
    import evidence_render
    source=tmp_path/'formula.wmf';source.write_bytes(b'original')
    def cancelled(*args,**kwargs):raise InterruptedError('stopped')
    monkeypatch.setattr(evidence_render,'render_office_pdf',cancelled)
    with pytest.raises(InterruptedError):evidence_render.load_visual_image(source)


def test_waiting_conversion_can_be_cancelled_without_leaking_backend(tmp_path,monkeypatch):
    pool=pool_fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(pool,'_available',lambda:[('lo-1','libreoffice')])
    entered=threading.Event();release=threading.Event();stop=threading.Event()
    def execute(engine,source,output,target_format,timeout):
        entered.set();assert release.wait(5)
        return pdf(output/(source.stem+'.pdf'))
    monkeypatch.setattr(pool,'_execute',execute)
    first=tmp_path/'one.wmf';second=tmp_path/'two.wmf'
    first.write_bytes(b'one');second.write_bytes(b'two')
    with ThreadPoolExecutor(max_workers=2) as workers:
        one=workers.submit(pool.convert,first,tmp_path/'one',target_format='pdf')
        assert entered.wait(3)
        two=workers.submit(pool.convert,second,tmp_path/'two',target_format='pdf',stop=stop)
        stop.set()
        try:
            with pytest.raises(InterruptedError):two.result(timeout=3)
        finally:release.set()
        assert one.result(timeout=3).is_file()
    assert not pool.busy and not pool.waiting


def test_corrupt_cache_is_not_accepted_as_a_conversion(tmp_path,monkeypatch):
    pool=pool_fixture(tmp_path,monkeypatch);calls=[]
    monkeypatch.setattr(pool,'_execute',lambda engine,source,output,target_format,timeout:
        calls.append(engine) or pdf(output/(source.stem+'.pdf')))
    source=tmp_path/'formula.wmf';source.write_bytes(b'formula original')
    pool.convert(source,tmp_path/'one',target_format='pdf')
    next((pool.root/'cache').rglob('content.pdf')).write_bytes(b'corruption')
    assert pool.convert(source,tmp_path/'two',target_format='pdf').is_file()
    assert len(calls)==2


def test_failed_libreoffice_keeps_previous_output_and_original(tmp_path,monkeypatch):
    import office_libreoffice as module
    from types import SimpleNamespace
    source=tmp_path/'formula.wmf';source.write_bytes(b'original')
    output=tmp_path/'out';output.mkdir();previous=output/'formula.pdf';pdf(previous)
    before=previous.read_bytes()
    monkeypatch.setattr(module,'executable',lambda:Path(__file__))
    monkeypatch.setattr(module,'_run',lambda *args:SimpleNamespace(returncode=1,stdout=b'',stderr=b'conversion failed'))
    with pytest.raises(RuntimeError,match='有效转换'):
        module.convert(source,output)
    assert source.read_bytes()==b'original' and previous.read_bytes()==before
    assert b'conversion failed' in (tmp_path/'out_conversion.log').read_bytes()


def test_preparation_config_round_trip_does_not_change_model_concurrency(tmp_path):
    from auto_config import load_config,save_config
    config=load_config(tmp_path)
    config['concurrency'].update(papers=10,global_llm=10,min_start_interval_seconds=16)
    config['preextract_preparation'].update(workers=4,max_pending=8,office_enabled=True)
    save_config(tmp_path,config)
    loaded=load_config(tmp_path)
    assert loaded['preextract_preparation']==dict(workers=4,max_pending=8,office_enabled=True)
    assert loaded['concurrency']==config['concurrency']


def test_failed_conversion_keeps_diagnostic_log_and_never_publishes(tmp_path,monkeypatch):
    pool=pool_fixture(tmp_path,monkeypatch)
    def failed(engine,source,output,target_format,timeout):
        output.parent.mkdir(parents=True,exist_ok=True)
        (output.parent/'out_conversion.log').write_text('failed conversion detail',encoding='utf8')
        raise RuntimeError('conversion failed')
    monkeypatch.setattr(pool,'_execute',failed)
    source=tmp_path/'original.wmf';source.write_bytes(b'original formula')
    with pytest.raises(RuntimeError,match='conversion failed'):
        pool.convert(source,tmp_path/'output',target_format='pdf')
    assert not (tmp_path/'output'/'original.pdf').exists()
    logs=list(pool.root.rglob('out_conversion.log'))
    assert len(logs)==1 and logs[0].read_text('utf8')=='failed conversion detail'
    assert not pool.busy and not pool.waiting


def test_wps_only_waiter_does_not_block_free_libreoffice_slots(tmp_path,monkeypatch):
    pool=pool_fixture(tmp_path,monkeypatch)
    held=threading.Event();release=threading.Event();lo_started=threading.Event()
    def execute(engine,source,output,target_format,timeout):
        if engine=='wps':held.set();assert release.wait(5)
        else:lo_started.set()
        return pdf(output/(source.stem+'.pdf'))
    monkeypatch.setattr(pool,'_execute',execute)
    sources=[]
    for name in ['first.wps','second.wps','normal.emf']:
        path=tmp_path/name;path.write_bytes(name.encode());sources.append(path)
    with ThreadPoolExecutor(max_workers=3) as workers:
        first=workers.submit(pool.convert,sources[0],tmp_path/'a',target_format='pdf')
        assert held.wait(3)
        second=workers.submit(pool.convert,sources[1],tmp_path/'b',target_format='pdf')
        import time
        deadline=time.monotonic()+2
        while not pool.waiting and time.monotonic()<deadline:threading.Event().wait(.01)
        assert pool.waiting
        third=workers.submit(pool.convert,sources[2],tmp_path/'c',target_format='pdf')
        try:assert lo_started.wait(2),'a WPS-only waiter must not block idle LibreOffice instances'
        finally:release.set()
        assert all(f.result(timeout=5).is_file() for f in [first,second,third])
