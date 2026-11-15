import copy
import hashlib
import json
from pathlib import Path

import pytest

from remote_folder_config import derive_folder_config


def fixture():
    identity = dict(task_id='t', group_id='g', paper_id='p', receipt_id='r', attempt_id='a',
                    parent_run_id='parent', task_run_id='folder')
    template = dict(schema='paperflow-remote-live/1', settings_sha256='1' * 64,
        tokenizer_runtime_sha256='2' * 64, expected_catalog_path='catalog', expected_catalog_sha256='3' * 64,
        transport_config=dict(server_identity={'server_id': 'one'}, allowed_roots=['old'],
            trusted_files=[dict(path='catalog', sha256='3' * 64)], service_config_sha256='4' * 64),
        transport_config_sha256='5' * 64, snapshot_options=dict(code_sha256='6' * 64, allowed_roots=['old']))
    roots = [str(Path.cwd() / 'synthetic-new-data'), str(Path.cwd() / 'synthetic-new-receipt')]
    record = dict(schema='paperflow-selected-ticket-evidence/1', identity=copy.deepcopy(identity), generation=2,
        claim_created=False, server_execution_started=False,
        configuration_sources=dict(settings={'sha256': '1' * 64}, runtime={'sha256': '2' * 64}),
        configuration=dict(allowed_roots=roots,
                           trusted_files=[dict(path='catalog', sha256='3' * 64)]),
        counter_contract={'counter': 'approved'})
    return template, record, identity


def test_each_folder_gets_only_its_verified_material_roots():
    template, record, identity = fixture()
    before = copy.deepcopy(template)
    value = derive_folder_config(template, record, identity=identity, generation=2,
                                 expected_counter={'counter': 'approved'})
    assert template == before
    assert value['transport_config']['allowed_roots'] == record['configuration']['allowed_roots']
    assert value['snapshot_options']['allowed_roots'] == record['configuration']['allowed_roots']
    assert value['transport_config']['service_config_sha256'] == '4' * 64
    assert value['snapshot_options']['code_sha256'] == '6' * 64
    canonical = json.dumps(value['transport_config'], ensure_ascii=False, sort_keys=True,
                           separators=(',', ':'), allow_nan=False).encode('utf-8')
    assert value['transport_config_sha256'] == hashlib.sha256(canonical).hexdigest()


@pytest.mark.parametrize('change', ['identity', 'generation', 'claimed', 'started', 'settings', 'runtime',
                                    'catalog', 'counter', 'no_roots'])
def test_stale_or_unbound_evidence_never_builds_a_config(change):
    template, record, identity = fixture()
    if change == 'identity': record['identity']['group_id'] = 'other'
    elif change == 'generation': record['generation'] = 3
    elif change == 'claimed': record['claim_created'] = True
    elif change == 'started': record['server_execution_started'] = True
    elif change == 'settings': record['configuration_sources']['settings']['sha256'] = '9' * 64
    elif change == 'runtime': record['configuration_sources']['runtime']['sha256'] = '9' * 64
    elif change == 'catalog': record['configuration']['trusted_files'][0]['sha256'] = '9' * 64
    elif change == 'counter': record['counter_contract'] = {'counter': 'other'}
    else: record['configuration']['allowed_roots'] = []
    with pytest.raises(ValueError):
        derive_folder_config(template, record, identity=identity, generation=2,
                             expected_counter={'counter': 'approved'})
