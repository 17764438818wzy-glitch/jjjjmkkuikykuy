"""Read-only proof that one *unknown-result* server process has terminated.

This does not release a capacity slot, change any result, retry a job, or
delete evidence. It only checks the original ledger's recorded cgroup/process
identities and final empty-scope proof. A separate reviewed reconciliation is
still needed before an unknown slot can be released.
"""
import hashlib
import json
import math
from pathlib import PurePosixPath

from remote_jobs import _uuid, _validated


def require(value, reason):
    if not value:
        raise ValueError(reason)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def prove_unknown_termination(job, run, worker, entry, config):
    try:
        frozen = _validated(job)
        identity = job['identity']
        require(isinstance(run, dict) and isinstance(worker, dict) and isinstance(entry, dict)
                and isinstance(config, dict) and run.get('state') == 'unknown'
                and entry.get('phase') == 'unknown' and run.get('claim_id') == job['claim_id']
                and all(run.get(key) == identity[key] for key in ('task_id', 'group_id', 'task_run_id'))
                and run.get('frozen_json') == worker.get('frozen_json') == entry.get('frozen_json') == frozen
                and entry.get('config_sha256') == hashlib.sha256(canonical(config).encode('utf-8')).hexdigest(),
                'original unknown job or service configuration differs')
        detail = json.loads(entry['detail_json'])
        scope, process = json.loads(worker['scope_json']), json.loads(worker['process_json'])
        launch, clock = json.loads(worker['config_json']), json.loads(worker['clock_json'])
        cleanup = detail.get('cleanup') if isinstance(detail, dict) else None
        require(isinstance(cleanup, dict) and detail.get('accounting_error') is None
                and cleanup.get('terminated') is True and cleanup.get('scope_removed') is True,
                'termination or ledger accounting is unconfirmed')
        scope_id = scope.get('scope_id') if isinstance(scope, dict) else None
        final = cleanup.get('final_scope')
        parent = launch.get('parent') if isinstance(launch, dict) else None
        require(isinstance(scope_id, list) and len(scope_id) == 2
                and all(type(value) is int and value > 0 for value in scope_id)
                and isinstance(process, dict) and type(process.get('pid')) is int and process['pid'] > 0
                and type(process.get('start_ticks')) is int and process['start_ticks'] > 0
                and isinstance(clock, dict) and isinstance(launch, dict)
                and scope.get('boot_id') == process.get('boot_id') == clock.get('boot_id') == cleanup.get('boot_id')
                and _uuid(scope.get('boot_id')) and cleanup.get('scope_identity') == scope
                and cleanup.get('process_identity') == process and isinstance(final, dict)
                and final.get('populated') is False and final.get('boot_id') == scope['boot_id']
                and final.get('scope_id') == scope_id and cleanup.get('parent') == parent
                and isinstance(parent, str) and parent.startswith('/sys/fs/cgroup/')
                and '..' not in PurePosixPath(parent).parts
                and PurePosixPath(parent).name in {'paperflow-pp-' + job['job_id'] + '.service',
                                                   'paperflow-pp-' + job['job_id'] + '.scope'}
                and launch.get('uid') == config.get('worker_uid')
                and launch.get('gid') == config.get('worker_gid')
                and launch.get('clock_basis') == 'linux-monotonic-boot/1'
                and type(cleanup.get('observed_monotonic')) in (int, float)
                and type(clock.get('started_monotonic')) in (int, float)
                and math.isfinite(cleanup['observed_monotonic']) and math.isfinite(clock['started_monotonic'])
                and cleanup['observed_monotonic'] >= clock['started_monotonic'] >= 0,
                'original process and empty removed cgroup proof differ')
        evidence = dict(job=job, config_sha256=entry['config_sha256'], scope=scope, process=process,
                        launch=launch, clock=clock, cleanup=cleanup)
        return dict(job_id=job['job_id'], result_state='unknown', process_terminated=True,
                    evidence_sha256=hashlib.sha256(canonical(evidence).encode('utf-8')).hexdigest())
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError('incomplete original termination evidence') from exc
