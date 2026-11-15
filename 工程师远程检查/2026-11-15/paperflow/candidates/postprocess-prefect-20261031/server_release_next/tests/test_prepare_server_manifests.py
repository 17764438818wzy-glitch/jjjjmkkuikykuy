"""Synthetic metadata only: never reads a server or a real work order."""
import copy
import importlib.util
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parents[1]


def module():
    path = HERE / 'prepare_server_app_manifests.py'
    assert path.is_file(), 'the historical source manifest converter is missing'
    spec = importlib.util.spec_from_file_location('synthetic_manifest_converter', path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


@pytest.fixture
def metadata():
    app = '/synthetic/revision/app'
    source = [{'path': app + '/server_rpc.py', 'sha256': 'a' * 64, 'bytes': 123},
              {'path': app + '/dependency.py', 'sha256': 'b' * 64, 'bytes': 42}]
    audit = {'schema': 'paperflow-app-source-inventory/1', 'versions': [dict(app=app,
        config_path=app + '/server.json', native_manifest='/synthetic/native.json',
        native_manifest_sha256='c' * 64, sources=source, unlisted_py=[], listed_py_missing=[])]}
    registry = {'schema': 'paperflow-historical-service-versions/1', 'server_identity': {'server_id': 'test'},
        'versions': [dict(canonical_sha256='d' * 64, service_config=app + '/server.json',
                          rpc_script=app + '/server_rpc.py', rpc_script_sha256='a' * 64)]}
    return audit, registry


def test_source_snapshot_yields_exact_linux_release_manifest(metadata):
    output = module().convert(*metadata)
    assert len(output) == 1
    row = output[0]
    assert row['canonical_sha256'] == 'd' * 64
    assert row['manifest'] == {'schema': 'paperflow-page-release/1', 'python': None, 'packages': {},
        'files': {'/synthetic/revision/app/server_rpc.py': 'a' * 64,
                  '/synthetic/revision/app/dependency.py': 'b' * 64}}


@pytest.mark.parametrize('change', ['unlisted', 'missing', 'outside', 'rpc_hash', 'duplicate', 'wrong_app'])
def test_ambiguous_or_incomplete_version_snapshot_is_rejected(metadata, change):
    audit, registry = copy.deepcopy(metadata)
    version = audit['versions'][0]
    if change == 'unlisted': version['unlisted_py'] = ['extra.py']
    elif change == 'missing': version['listed_py_missing'] = ['dependency.py']
    elif change == 'outside': version['sources'][1]['path'] = '/another/dependency.py'
    elif change == 'rpc_hash': registry['versions'][0]['rpc_script_sha256'] = 'f' * 64
    elif change == 'duplicate': version['sources'].append(copy.deepcopy(version['sources'][0]))
    elif change == 'wrong_app': version['app'] = '/another/app'
    with pytest.raises(ValueError):
        module().convert(audit, registry)
