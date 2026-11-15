import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def test_serial_start_confirmation_uses_historical_estimate():
    from app import confirm_serial_start

    prompts = []
    def ask(title, message):
        prompts.append((title, message))
        return False

    assert confirm_serial_start(184, {'enabled': False}, ask=ask) is False
    assert prompts and '串行模式' in prompts[0][1]
    assert '约 8.0 小时' in prompts[0][1]


def test_enabled_start_skips_serial_confirmation():
    from app import confirm_serial_start

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError('confirmation should not run for parallel mode')

    assert confirm_serial_start(5, {'enabled': True}, ask=fail_if_called) is True


def test_concurrency_config_has_nested_defaults_and_round_trips(tmp_path):
    from auto_config import DEFAULTS, load_config, save_config

    cfg = load_config(tmp_path)
    assert cfg['concurrency'] == {
        'enabled': True,
        'papers': 2,
        'images_per_paper': 2,
        'global_llm': 4,
        'max_retries': 4,
        'backoff_base_seconds': 1.0,
        'min_start_interval_seconds': 0.0,
    }
    assert cfg['resume'] == {'enabled': True, 'state_file': 'processing_state.json'}
    save_config(tmp_path, {**DEFAULTS, 'concurrency': cfg['concurrency'], 'resume': cfg['resume']}, '')
    assert load_config(tmp_path)['concurrency']['global_llm'] == 4


def test_processing_state_is_atomic_and_tracks_each_image(tmp_path):
    from processing_state import ProcessingState

    state = ProcessingState(tmp_path, 'processing_state.json')
    state.begin_paper('AUTO_demo', 'paper_demo', ['figure_1', 'figure_2'])

    def mark(index):
        state.image_done('AUTO_demo', f'figure_{index}')

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(mark, [1, 2]))
    state.image_failed('AUTO_demo', 'figure_2', '429 Too Many Requests')
    data = json.loads((tmp_path / 'processing_state.json').read_text(encoding='utf-8'))
    record = data['papers']['AUTO_demo']
    assert record['images_done'] == ['figure_1', 'figure_2']
    assert record['stage'] == 'images_done'
    assert data['failures'][-1]['image'] == 'figure_2'
    assert json.loads((tmp_path / 'failures.json').read_text(encoding='utf-8'))[-1]['paper'] == 'AUTO_demo'


def test_processing_state_invalidates_all_stages_when_processing_version_changes(tmp_path):
    from processing_state import ProcessingState
    state = ProcessingState(tmp_path, 'processing_state.json', processing_version='pipeline-v1')
    state.begin_paper('paper', 'paper-id', ['figure'])
    state.paper_stage('paper', 'parse')
    state.image_done('paper', 'figure')
    record = state.snapshot()['papers']['paper']
    assert record['processing_version'] == 'pipeline-v1'
    assert record['completed_stages'] == ['parse']
    assert ProcessingState(tmp_path, 'processing_state.json', processing_version='pipeline-v2').snapshot()['papers'] == {}


def test_agent_llm_gate_limits_simultaneous_requests(monkeypatch):
    from agent_client import AgentClient

    gate = threading.BoundedSemaphore(2)
    client = AgentClient({'base_url': 'https://example.test/v1', 'text_model': 'test'}, 'key')
    client.llm_gate = gate
    active = 0
    peak = 0
    lock = threading.Lock()

    class Response:
        headers = {'Content-Type': 'application/json'}
        def read(self, _):
            return b'{"choices":[{"finish_reason":"stop","message":{"content":"{\\"decision\\":\\"pass\\",\\"reason\\":\\"ok\\"}"}}]}'

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class Opener:
        def open(self, request, timeout):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(.03)
            with lock:
                active -= 1
            return Response()

    client.opener = Opener()
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda _: client.call('screen', {'text': 'x'}), range(6)))
    assert peak <= 2


def test_upload_routed_items_uses_bounded_parallel_uploads(tmp_path):
    pytest.importorskip('PIL')
    from engine import Store, write_json
    from ragflow_client import upload_routed_items

    store = Store(tmp_path)
    run_id = 'parallel-upload'
    package_root = store.runs / run_id / 'packages'
    items = []
    for index in range(4):
        item_id = f'AUTO_item_{index}'
        package = package_root / item_id
        package.mkdir(parents=True)
        (package / 'ingest.md').write_text(f'# paper {index}', encoding='utf-8')
        (package / 'ragflow_assets.md').write_text('', encoding='utf-8')
        (package / 'manifest.json').write_text(json.dumps({'files': []}), encoding='utf-8')
        from engine import digest
        items.append({'id': item_id, 'major': '会计学', 'status': 'pass', 'path': str(package / 'ingest.md'),
                      'reviewed_hash': digest(package / 'ingest.md'), 'paper_id': f'paper_{index}', 'uploads': {}})
    write_json(store.runs / run_id / 'manifest.json', {'run_id': run_id, 'items': items, 'status': 'completed'})

    class Client:
        base_url = 'https://example.test/v1'
        def __init__(self):
            self.active = 0
            self.peak = 0
            self.lock = threading.Lock()

        def upload(self, dataset_id, path, remote_name=None):
            with self.lock:
                self.active += 1
                self.peak = max(self.peak, self.active)
            time.sleep(.03)
            with self.lock:
                self.active -= 1
            return f'{dataset_id}-{path.stem}'

    client = Client()
    routes = {'paper': {'id': 'paper-kb', 'name': '论文库'}, 'image': None}
    assert upload_routed_items(store, run_id, [item['id'] for item in items], client, routes,
                               concurrency={'enabled': True, 'papers': 2, 'images_per_paper': 1, 'global_llm': 2}) == 4
    assert client.peak >= 2


def test_document_scheduler_runs_papers_in_parallel(tmp_path, monkeypatch):
    import auto_pipeline
    from auto_pipeline import AutoProcessor
    from engine import Store

    class Worker:
        active = 0
        peak = 0
        lock = threading.Lock()

        def __init__(self, store, config):
            self.store = store
            self.config = config
            self.stop = threading.Event()
            self.agent = type('Agent', (), {})()
            self.stage_cache = None

        def run(self, selected, callback):
            with self.lock:
                type(self).active += 1
                type(self).peak = max(type(self).peak, type(self).active)
            time.sleep(.08)
            source = selected[0]['path']
            item_id = 'AUTO_worker_' + str(abs(hash(source)))
            rejected = self.store.runs / 'worker' / 'rejected' / (item_id + '.md')
            rejected.parent.mkdir(parents=True, exist_ok=True)
            rejected.write_text('# rejected', encoding='utf-8')
            with self.lock:
                type(self).active -= 1
            return {'run_id': 'worker', 'items': [{'id': item_id, 'title': Path(source).name,
                    'major': '会计学', 'status': 'reject', 'reason': 'fixture', 'reason_kind': 'technical',
                    'path': str(rejected), 'display_path': str(rejected), 'uploads': {}}]}

    monkeypatch.setattr(auto_pipeline, 'AutoProcessor', Worker)
    original = AutoProcessor(Store(tmp_path / 'out'), {
        'concurrency': {'enabled': True, 'papers': 2, 'images_per_paper': 1, 'global_llm': 2},
        'resume': {'enabled': True, 'state_file': 'processing_state.json'},
        'auto_upload': False,
    })
    records = [{'path': str(tmp_path / f'{index}.txt'), 'name': f'{index}.txt'} for index in range(4)]
    started = time.monotonic()
    result = original.run(records)
    elapsed = time.monotonic() - started
    assert elapsed < .6
    assert Worker.peak >= 2
    assert len(result['items']) == 4


def test_one_failed_image_does_not_cancel_other_images_and_is_checkpointed(tmp_path):
    from PIL import Image
    from auto_evidence import media_privacy, Rejected
    from processing_state import ProcessingState

    package=tmp_path/'package';(package/'_review').mkdir(parents=True);(package/'assets').mkdir()
    resources=[];blocks=[]
    for index in [1,2]:
        from PIL import ImageDraw
        path=package/'_review'/f'figure_{index}.png';figure=Image.new('RGB',(20,20),'white')
        ImageDraw.Draw(figure).line([(2,17),(8,10),(17,2)],fill='blue',width=2);figure.save(path)
        rid=f'figure_{index}';resources.append({'id':rid,'local_path':path.relative_to(package).as_posix(),
            'kind':'image','privacy_status':'pending','position':{'page':index}})
        blocks.append({'id':f'b{index}','type':'image','resource_id':rid,'caption':f'图{index}'})
    class Agent:
        def __init__(self): self.processing_metrics=[];self.stop=None;self.llm_gate=None
        def call(self,task,payload,images=()):
            if payload['id']=='figure_1': raise RuntimeError('429 Too Many Requests')
            return {'decision':'pass','reason':'ok','research':True,'sensitive_boxes':[],
                    'text':'坐标轴','description':'趋势图','coordinate_space':'normalized'}
    state=ProcessingState(tmp_path,'state.json')
    meta={'resources':resources,'blocks':blocks}
    with pytest.raises(RuntimeError):
        media_privacy(meta,package,Agent(),lambda *_:None,
                      {'enabled':True,'images_per_paper':2,'_paper_id':'paper_test'},state,'paper_test')
    snapshot=state.snapshot()['papers']['paper_test']
    assert snapshot['images_done']==['figure_2']
    assert snapshot['images_failed']==['figure_1']
    assert (package/'assets'/'figure_2.png').is_file()
