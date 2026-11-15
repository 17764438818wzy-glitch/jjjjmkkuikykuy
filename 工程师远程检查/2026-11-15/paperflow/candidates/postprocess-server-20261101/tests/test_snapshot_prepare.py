"""Bind an existing folder claim to only its collected synthetic inputs."""
import copy
from io import BytesIO
from pathlib import Path
import sys
from uuid import uuid4, uuid5

import pytest

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE), str(HERE / 'tests')]
from remote_jobs import NAMESPACE
from snapshot_prepare import prepare_manifest, counter_contract_from_runtime
from snapshot_wire import snapshot_sha
from snapshot_transport import write_snapshot
from test_snapshot_collect import fixture, bind_catalog

COUNTER = dict(model_sha256='1' * 64, libllama_sha256='2' * 64, cl100k_sha256='3' * 64,
               embedding_model_budget=2500, cl_limit=1800)


@pytest.mark.parametrize('different_snapshot_catalog', [False, True])
def test_prepare_binds_exact_backend_catalog_and_snapshot_contents(tmp_path, different_snapshot_catalog):
    inside = tmp_path / 'inside'
    inputs = fixture(inside)
    catalog = tmp_path / 'catalog.json'
    bind_catalog(inputs, catalog, {'synthetic': 1})
    claim = dict(identity=copy.deepcopy(inputs['identity']), claim_id=str(uuid4()), generation=1,
                 manifest_sha256=inputs['manifest_sha256'], state='processing')
    def prepare():
        return prepare_manifest(claim, inputs,
            catalog={'different': 1} if different_snapshot_catalog else {'synthetic': 1},
            code_sha256='d' * 64, paperflow_root_windows=r'F:\release\core',
            business_workspace_windows=r'F:\workspace', output_root_windows=r'F:\pfo',
            runtime_template_sha256='e' * 64, counter_contract=COUNTER,
            allowed_roots=[inside], trusted_catalog_path=catalog, defer_binary_hash=True)
    if different_snapshot_catalog:
        with pytest.raises(ValueError, match='catalog'):
            prepare()
    else:
        result = prepare()
        value = result['manifest']
        assert value['catalog'] == {'synthetic': 1}
        assert str(catalog) in {row['path'] for row in value['files']}
        assert result['trusted_files'] == [{key: next(row for row in value['files']
            if row['path'] == str(catalog))[key] for key in ('path', 'sha256')}]
        with pytest.raises(ValueError):
            write_snapshot(BytesIO(), value, allowed_roots=[inside], verify_locally=False)
        sent = write_snapshot(BytesIO(), value, allowed_roots=[inside], verify_locally=False,
                              trusted_files=result['trusted_files'])
        assert sent['snapshot_sha256'] == result['snapshot_sha256']


def test_counter_contract_comes_from_original_runtime_and_rejects_invalid_values():
    runtime = dict(embedding_model_budget=2500, embedding_admission=dict(policy=dict(
        model_sha256='1' * 64, libllama_sha256='2' * 64, cl100k_sha256='3' * 64,
        pipeline_limit=1800, adapter_limit=2000)))
    assert counter_contract_from_runtime(runtime) == COUNTER
    del runtime['embedding_model_budget']
    assert counter_contract_from_runtime(runtime)['embedding_model_budget'] == 2000
    runtime['embedding_admission']['policy']['cl100k_sha256'] = 'invalid'
    with pytest.raises(ValueError):
        counter_contract_from_runtime(runtime)


def test_original_claim_and_all_supported_inputs_are_frozen_together(tmp_path):
    inputs = fixture(tmp_path, 2)
    claim = dict(identity=copy.deepcopy(inputs['identity']), claim_id=str(uuid4()), generation=3,
                 manifest_sha256=inputs['manifest_sha256'], state='processing')
    catalog = {'synthetic': ['a']}
    result = prepare_manifest(claim, inputs, catalog=catalog, code_sha256='d' * 64,
                              paperflow_root_windows=r'F:\release\core',
                              business_workspace_windows=r'F:\workspace', output_root_windows=r'F:\pfo',
                              runtime_template_sha256='e' * 64,
                              counter_contract=COUNTER,
                              allowed_roots=[tmp_path])
    value = result['manifest']
    assert value['job_id'] == str(uuid5(NAMESPACE, inputs['identity']['task_run_id']))
    assert value['claim_id'] == claim['claim_id']
    assert value['inputs'] == inputs and value['catalog'] == catalog
    assert value['counter_contract'] == COUNTER
    assert len(value['inputs']['body']['responses']) == 2
    assert len(value['inputs']['attachments']) == 1
    assert result['snapshot_sha256'] == snapshot_sha(value)
    assert result['dependency_scope'] == 'supported_dependency_closure_only'
    inputs['identity']['paper_id'] = 'mutated-after-freeze'
    assert value['identity']['paper_id'] != 'mutated-after-freeze'


@pytest.mark.parametrize('change', ['claim_identity', 'claim_state', 'manifest', 'code', 'template'])
def test_mismatched_claim_or_code_never_forms_a_snapshot(tmp_path, change):
    inputs = fixture(tmp_path, 1)
    claim = dict(identity=copy.deepcopy(inputs['identity']), claim_id=str(uuid4()), generation=1,
                 manifest_sha256=inputs['manifest_sha256'], state='processing')
    code = 'd' * 64
    template = 'e' * 64
    if change == 'claim_identity': claim['identity']['paper_id'] = 'wrong'
    elif change == 'claim_state': claim['state'] = 'unknown'
    elif change == 'manifest': claim['manifest_sha256'] = 'e' * 64
    elif change == 'code': code = 'not-a-sha'
    else: template = 'not-a-sha'
    with pytest.raises(ValueError):
        prepare_manifest(claim, inputs, catalog={'synthetic': 1}, code_sha256=code,
                         paperflow_root_windows=r'F:\release\core', runtime_template_sha256=template,
                         business_workspace_windows=r'F:\workspace', output_root_windows=r'F:\pfo',
                         counter_contract=COUNTER,
                         allowed_roots=[tmp_path])


def test_deferred_binary_hashing_is_explicit_in_the_frozen_snapshot(tmp_path):
    inputs = fixture(tmp_path, 1)
    claim = dict(identity=copy.deepcopy(inputs['identity']), claim_id=str(uuid4()), generation=1,
                 manifest_sha256=inputs['manifest_sha256'], state='processing')
    prepared = prepare_manifest(claim, inputs, catalog={'synthetic': 1}, code_sha256='d' * 64,
                                paperflow_root_windows=r'F:\release\core',
                                business_workspace_windows=r'F:\workspace', output_root_windows=r'F:\pfo',
                                runtime_template_sha256='e' * 64,
                                counter_contract=COUNTER,
                                allowed_roots=[tmp_path], defer_binary_hash=True)
    assert prepared['dependency_scope'] == 'supported_dependency_graph_binary_hash_deferred'
    assert prepared['manifest']['binary_hash_scope'] == 'server'
