import copy
import hashlib
import json
from uuid import uuid4

import pytest

from remote_jobs import _validated
from server_termination_proof import prove_unknown_termination
from test_remote_jobs import job


def case():
    item = job(); boot = str(uuid4())
    scope = {'boot_id': boot, 'scope_id': [12, 34]}
    process = {'boot_id': boot, 'pid': 4321, 'start_ticks': 55}
    parent = '/sys/fs/cgroup/paperflow-pp-' + item['job_id'] + '.service'
    launch = {'parent': parent, 'uid': 1000, 'gid': 1000, 'clock_basis': 'linux-monotonic-boot/1'}
    clock = {'boot_id': boot, 'started_monotonic': 50.0}
    cleanup = {'terminated': True, 'scope_removed': True, 'scope_identity': scope,
               'process_identity': process, 'final_scope': {**scope, 'populated': False},
               'parent': parent, 'observed_monotonic': 60.0, 'boot_id': boot}
    config = {'worker_uid': 1000, 'worker_gid': 1000}
    canonical = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    frozen = _validated(item)
    run = {'state': 'unknown', 'frozen_json': frozen, 'claim_id': item['claim_id'],
           **{k: item['identity'][k] for k in ('task_id', 'group_id', 'task_run_id')}}
    worker = {'frozen_json': frozen, 'scope_json': canonical(scope), 'process_json': canonical(process),
              'config_json': canonical(launch), 'clock_json': canonical(clock)}
    entry = {'frozen_json': frozen, 'phase': 'unknown', 'config_sha256': hashlib.sha256(
        canonical(config).encode()).hexdigest(), 'detail_json': canonical({'accounting_error': None, 'cleanup': cleanup})}
    return item, run, worker, entry, config


def test_recorded_termination_never_changes_the_unknown_result():
    item, run, worker, entry, config = case()
    before = copy.deepcopy((item, run, worker, entry, config))
    evidence = prove_unknown_termination(item, run, worker, entry, config)
    assert evidence['process_terminated'] is True and evidence['result_state'] == 'unknown'
    assert len(evidence['evidence_sha256']) == 64
    assert (item, run, worker, entry, config) == before


@pytest.mark.parametrize('change', ['state', 'claim', 'config', 'scope', 'pid', 'boot', 'populated',
                                    'not_removed', 'not_terminated', 'clock', 'accounting_error'])
def test_unproved_or_mismatched_termination_never_releases_capacity(change):
    item, run, worker, entry, config = case()
    detail = json.loads(entry['detail_json']); cleanup = detail['cleanup']
    if change == 'state': run['state'] = 'running'
    elif change == 'claim': run['claim_id'] = str(uuid4())
    elif change == 'config': entry['config_sha256'] = '0' * 64
    elif change == 'scope': cleanup['scope_identity']['scope_id'] = [1, 2]
    elif change == 'pid': cleanup['process_identity']['pid'] = 123
    elif change == 'boot': cleanup['boot_id'] = str(uuid4())
    elif change == 'populated': cleanup['final_scope']['populated'] = True
    elif change == 'not_removed': cleanup['scope_removed'] = False
    elif change == 'not_terminated': cleanup['terminated'] = False
    elif change == 'clock': cleanup['observed_monotonic'] = 1.0
    else: detail['accounting_error'] = 'uncertain'
    entry['detail_json'] = json.dumps(detail, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    with pytest.raises(ValueError):
        prove_unknown_termination(item, run, worker, entry, config)
