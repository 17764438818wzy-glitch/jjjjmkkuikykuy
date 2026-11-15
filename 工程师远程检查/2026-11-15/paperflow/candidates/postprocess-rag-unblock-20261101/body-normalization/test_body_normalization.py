import importlib.util
import json
from pathlib import Path
import sys

import pytest


WORKSPACE = Path(__file__).resolve().parents[3]
TESTS = WORKSPACE / 'candidates/postprocess-prefect-20261031/tests'
sys.path.insert(0, str(TESTS))
from test_body_adapter import fixture
from preextract_image_alias import AliasContext


spec = importlib.util.spec_from_file_location('normalization_candidate', Path(__file__).with_name('body_adapter.py'))
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)


def source(tmp_path):
    evidence, catalog = fixture(tmp_path, parts=2)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    return candidate, bundle, catalog


def derive(candidate, bundle, catalog):
    return adapter._derive({'mode': 'single'}, bundle,
                           [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)


def test_real_block_uses_its_program_indexed_section(tmp_path):
    candidate, bundle, catalog = source(tmp_path)
    reference = candidate['profile']['learning_units'][0]['source_refs'][0]
    assert reference['block_id'] == 'B001'
    reference['section_id'] = 'S002'
    artifacts, _, _ = derive(candidate, bundle, catalog)
    fixed = artifacts['candidate']['profile']['learning_units'][0]['source_refs'][0]
    assert fixed['block_id'] == 'B001'
    assert fixed['section_id'] == 'S001'
    assert any(row['code'] == 'normalized_block_section'
               for row in artifacts['part_1_validation']['compatibility_report'])


def test_unknown_block_is_still_rejected(tmp_path):
    candidate, bundle, catalog = source(tmp_path)
    candidate['profile']['learning_units'][0]['source_refs'][0]['block_id'] = 'INVENTED'
    with pytest.raises(ValueError, match='unknown_source_ref|candidate_validation_failed'):
        derive(candidate, bundle, catalog)
