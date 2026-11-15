"""Candidate upstream handoff binding, using fake local files and claims only."""
from contextlib import contextmanager
import importlib
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
CANDIDATE = Path(__file__).resolve().parents[1] / 'upstream_candidate'
NAMESPACE = '_paperflow_segment_binding_test'
package = ModuleType(NAMESPACE)
package.__path__ = [str(CANDIDATE), str(ROOT / 'prefect-paperflow/paperflow_prefect')]
package.__package__ = NAMESPACE
sys.modules[NAMESPACE] = package
flows = ModuleType(NAMESPACE + '.flows')
flows.LIMIT_PREFIX = 'synthetic-'
sys.modules[flows.__name__] = flows
upstream = importlib.import_module(NAMESPACE + '.local_preprocessing')
contract = importlib.import_module(NAMESPACE + '.segment_plan_contract')


@pytest.fixture
def setup(tmp_path, monkeypatch):
    material_path = tmp_path / 'work' / 'material.json'
    material_path.parent.mkdir()
    source = {'source_sha256': 'a' * 64}
    material = {'source': source, 'blocks': []}
    material_path.write_text(json.dumps(material), encoding='utf8')
    person = tmp_path / 'person'
    person.mkdir()
    paper = {'paper_id': 'paper-1', 'revision': 'rev-1', 'person_dir': str(person),
             'body': {'sha256': 'a' * 64}, 'attachments': []}
    claim = {'task_id': 'task-1', 'group_id': 'group-1', 'paper_id': 'paper-1',
             'receipt_id': 'receipt-1', 'receipt_sha256': 'c' * 64,
             'receipt_path': str(tmp_path / 'receipt.json')}
    verified = {**claim, 'paper': paper}
    span = {'block_id': 'b1', 'start': 0, 'end': 3}
    plan = {'schema': 'preextract-input-plan/1', 'mode': 'single',
            'parts': [{'ranges': [span], 'asset_ids': [], 'bundle': material,
                       'body': {'model': 'fake'}, 'capacity': {}}],
            'coverage': {'complete': True, 'block_count': 1, 'character_count': 3,
                         'ranges': [span], 'asset_ids': []}}

    class Queue:
        consumer_id = 'synthetic'
        saved = None
        def completed_preprocessing(self, *_): return None
        def claim_next(self, **_): return claim
        def complete_preprocessing(self, consumer, attempt, handoff): self.saved = handoff

    class FakeBackend:
        def __init__(self, *_args, **_kwargs):
            self.root, self.settings, self.catalog = tmp_path / 'runtime', SimpleNamespace(), {}
        def receive(self, _): return paper
        def paper_root(self, _): return tmp_path / 'paper'

    @contextmanager
    def admission(*_): yield

    monkeypatch.setattr(upstream, 'UnifiedBackend', FakeBackend)
    monkeypatch.setattr(upstream, 'settings_for_verified_receipt', lambda *args: None)
    monkeypatch.setattr(upstream.Backend, 'prepare', lambda *_: {'material': str(material_path)})
    monkeypatch.setattr(upstream, 'Records', lambda _: SimpleNamespace(cached=lambda *_: None))
    monkeypatch.setattr(upstream, '_verify_folder_receipt', lambda *_: verified)
    monkeypatch.setattr(upstream, '_available_material', lambda value, _: (value, []))
    monkeypatch.setattr(upstream, '_admit_task', admission)
    monkeypatch.setattr(upstream, '_verified_aux', lambda *_: None)
    queue = Queue()
    return queue, claim, material, plan


def test_handoff_binds_exact_preprocessing_plan_and_reading_file(setup, monkeypatch):
    queue, claim, material, plan = setup
    monkeypatch.setattr(upstream, '_capacity_and_projection',
        lambda *_: (material, [], {'status': 'planned', 'mode': 'single', 'parts': 1}, plan))
    result = upstream._prepare_claim(queue, 'attempt-1', None, claim['task_id'], 1,
                                     claim['group_id'], claim['receipt_id'])
    manifest = json.loads(Path(result['manifest_path']).read_text(encoding='utf8'))
    receipt = manifest['capacity']['plan_identity']
    assert queue.saved['manifest_sha256'] == upstream.sha_file(result['manifest_path'])
    assert receipt['reading_sha256'] == manifest['reading_material']['sha256']
    contract.verify_plan(plan, receipt, source_sha256='a' * 64,
                         reading_sha256=manifest['reading_material']['sha256'])


def test_unavailable_plan_is_not_recorded_as_empty_or_single(setup, monkeypatch):
    queue, claim, material, _ = setup
    monkeypatch.setattr(upstream, '_capacity_and_projection',
        lambda *_: (material, [], {'status': 'unavailable', 'error_type': 'Synthetic'}, None))
    result = upstream._prepare_claim(queue, 'attempt-1', None, claim['task_id'], 1,
                                     claim['group_id'], claim['receipt_id'])
    manifest = json.loads(Path(result['manifest_path']).read_text(encoding='utf8'))
    assert manifest['capacity'] == {'status': 'unavailable', 'error_type': 'Synthetic'}


def test_capacity_planner_passes_its_real_result_to_the_handoff(setup, tmp_path, monkeypatch):
    _, _, material, plan = setup
    material = dict(material, parser_version='candidate-direct-clean/25')
    model_config = tmp_path / 'model-config.json'
    model_config.write_text(json.dumps({'text_model': 'fake', 'base_url': 'http://invalid.local',
        'max_tokens': 1000, 'preextract_limits': {'model': 'fake', 'base_url': 'http://invalid.local',
        'supports_images': True, 'context_window': 1000000}}), encoding='utf8')
    backend = SimpleNamespace(settings=SimpleNamespace(path=lambda key: model_config), catalog={})
    projection_module = ModuleType(NAMESPACE + '.body_clean_handoff')
    projection_module.project_for_reading = lambda value: (value, [])
    projection_module.plan_with_available_images = lambda *args: (plan, material, [])
    tokenizer = ModuleType(NAMESPACE + '.body_segment_tokenizer')
    tokenizer.with_body_token_counter = lambda value: dict(value)
    templates = ModuleType('preextract_contract')
    templates.load_template = lambda _: ('synthetic', {})
    monkeypatch.setitem(sys.modules, projection_module.__name__, projection_module)
    monkeypatch.setitem(sys.modules, tokenizer.__name__, tokenizer)
    monkeypatch.setitem(sys.modules, templates.__name__, templates)
    projection, gaps, capacity, actual_plan = upstream._capacity_and_projection(material, tmp_path, backend)
    assert projection == material and not gaps and actual_plan == plan
    assert capacity == {'status': 'planned', 'mode': 'single', 'parts': 1}
