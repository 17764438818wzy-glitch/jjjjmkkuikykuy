"""Rebuild inputs with the upstream planner only before the first send."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(ROOT / 'prefect-paperflow'), str(DESKTOP), 'F:/论文预处理系统/程序/desktop',
                str(DESKTOP.parent / 'core')]
spec = importlib.util.spec_from_file_location('body_planner', HERE / 'body_planner.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
from segment_plan_contract import describe_plan
from paperflow_prefect import body_clean_handoff, body_segment_tokenizer
import preextract_contract


def reference(path, value):
    data = json.dumps(value).encode()
    path.write_bytes(data)
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}


@pytest.fixture
def planner(tmp_path, monkeypatch):
    source = {'source_sha256': 'a' * 64}
    material = {'source': source, 'blocks': [], 'assets': []}
    reading = copy.deepcopy(material)
    material_ref = reference(tmp_path / 'material.json', material)
    reading_ref = reference(tmp_path / 'reading.json', reading)
    span = {'block_id': 'b1', 'start': 0, 'end': 3}
    plan = {'schema': 'preextract-input-plan/1', 'mode': 'single',
            'parts': [{'ranges': [span], 'asset_ids': [], 'bundle': material,
                       'body': {'model': 'fake', 'messages': []}, 'capacity': {}}],
            'coverage': {'complete': True, 'block_count': 1, 'character_count': 3,
                         'ranges': [span], 'asset_ids': []}}
    config = {'text_model': 'fake', 'base_url': 'http://127.0.0.1/fake', 'max_tokens': 1000,
              'preextract_limits': {'model': 'fake', 'base_url': 'http://127.0.0.1/fake',
                                    'supports_images': True, 'context_window': 1200000}}
    model_path, template_path = tmp_path / 'model.json', tmp_path / 'template.md'
    model_path.write_text(json.dumps(config), encoding='utf8')
    template_path.write_text('synthetic template', encoding='utf8')
    backend = SimpleNamespace(settings=SimpleNamespace(path=lambda key: {'model_config': model_path,
                                                                          'template_path': template_path}[key]),
                              catalog={})
    receipt = describe_plan(plan, source_sha256='a' * 64, reading_sha256=reading_ref['sha256'])
    manifest = {'body': {'sha256': 'a' * 64}, 'body_work': str(tmp_path), 'material': material_ref,
                'reading_material': reading_ref,
                'capacity': {'status': 'planned', 'mode': 'single', 'parts': 1, 'plan_identity': receipt}}
    seen = []
    monkeypatch.setattr(preextract_contract, 'load_template', lambda _: ('synthetic template', {}))
    monkeypatch.setattr(body_segment_tokenizer, 'with_body_token_counter', lambda value: dict(value))
    def build(projection, work, template, catalog, connection, planning):
        seen.append((copy.deepcopy(connection), copy.deepcopy(planning)))
        return copy.deepcopy(plan), copy.deepcopy(material), []
    monkeypatch.setattr(body_clean_handoff, 'plan_with_available_images', build)
    connection = {'model': 'fake', 'base_url': 'http://127.0.0.1/fake', 'api_key': 'synthetic-only'}
    return backend, manifest, tmp_path, connection, plan, material, seen


def test_real_planner_uses_upstream_projection_and_exact_frozen_identity(planner):
    backend, manifest, work, connection, plan, material, seen = planner
    template, factory = module.make_body_planner(backend, manifest, work, connection)
    result, original, adapted = factory()
    assert template == 'synthetic template' and result == plan and original == adapted == material
    assert seen[0][0]['model'] == 'fake' and seen[0][1]['context_window'] == 1000000
    assert 'api_key' not in seen[0][0]


def test_changed_model_or_reading_material_stops_before_planning(planner):
    backend, manifest, work, connection, _, _, seen = planner
    with pytest.raises(ValueError, match='模型'):
        module.make_body_planner(backend, manifest, work, dict(connection, model='other'))
    _, factory = module.make_body_planner(backend, manifest, work, connection)
    Path(manifest['reading_material']['path']).write_text('{}', encoding='utf8')
    with pytest.raises(ValueError, match='阅读材料'):
        factory()
    assert not seen
