"""Construct per-folder remote Prefect launchers without starting a queue.

The policy reader runs in a short-lived subprocess because its read-only
Prefect safeguards are process-local. The original Prefect task controls,
folder identity, slot and separately pinned server endpoint remain in use.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from scoped_task_launcher import ScopedTaskLauncher
from snapshot_prepare import counter_contract_from_runtime


def build(config_path, config_sha256, *, admission_wait=None):
    from remote_live_launch import _config, _original_configuration, build_from_config
    from remote_scoped_job import run_exact_folder
    template = _config(config_path, config_sha256)
    tokenizer, catalog, enabled = _original_configuration(template)
    if enabled is not True:
        raise ValueError('original Prefect execution is disabled')
    runtime = build_from_config(config_path, config_sha256)
    from remote_live_runtime import build_remote_live_runtime
    policy = Path(__file__).with_name('scoped_policy_probe.py').resolve(strict=True)
    policy_digest = hashlib.sha256(policy.read_bytes()).hexdigest()

    def read_policy(identity, generation, *, resume_job_id=None, retry_job_id=None):
        if hashlib.sha256(policy.read_bytes()).hexdigest() != policy_digest:
            raise ValueError('the original policy reader changed')
        request_body = dict(identity=identity, generation=generation)
        if resume_job_id is not None:
            request_body['resume_job_id'] = resume_job_id
        if retry_job_id is not None:
            request_body['retry_job_id'] = retry_job_id
        request = json.dumps(request_body, ensure_ascii=False).encode('utf8')
        child = subprocess.run([sys.executable, '-B', str(policy)], input=request, capture_output=True,
            timeout=90, check=False, env=dict(os.environ, PYTHONUTF8='1'),
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        if child.returncode or len(child.stdout) > 1024 * 1024:
            raise ValueError('the original folder source policy is unavailable')
        try:
            return json.loads(child.stdout)
        except (ValueError, UnicodeError) as exc:
            raise ValueError('the original folder source policy is invalid') from exc

    def make_remote(config):
        return build_remote_live_runtime(original_runtime=runtime.original_runtime,
            journal_database=config['journal_database'], journal_id=config['journal_id'],
            expected_prefect_api_url=config['expected_prefect_api_url'], server_identity=config['server_identity'],
            transport_config=config['transport_config'], transport_config_sha256=config['transport_config_sha256'],
            snapshot_options=dict(config['snapshot_options'], catalog=copy.deepcopy(catalog)),
            expected_catalog_path=config['expected_catalog_path'],
            expected_catalog_sha256=config['expected_catalog_sha256'], tokenizer_runtime=tokenizer,
            max_summary_receipts=config['max_summary_receipts'], admission_wait=admission_wait)

    launcher = ScopedTaskLauncher(runtime, template, counter_contract_from_runtime(tokenizer),
        read_policy=read_policy, make_remote=make_remote, run_exact=run_exact_folder)
    return runtime, launcher
