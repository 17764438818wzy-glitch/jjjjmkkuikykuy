"""Prepare one existing paper in the isolated batch; do not send a paper to RAG."""
from contextlib import closing, contextmanager
import json
from pathlib import Path
import sqlite3
import sys

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
sys.path[:0] = [str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates' / 'postprocess-rag-unblock-20261101')]


def main(task_id, group_id, *, create_targets=False):
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    settings = Settings.load()
    bind(settings)
    from auto_config import load_key
    from ragflow_client import RagflowClient
    from preextract_batch_delivery import load_native_batch_config
    from paperflow_prefect.records import write_json
    from effect_journal import EffectJournal
    from isolated_group_prepare import prepare_group
    from task_effect_admission import task_effect_admission
    import paramiko
    import receipt_access

    if task_id != '6cb5a766-94f9-4613-aa5e-17a883be9016':
        raise ValueError('this isolated batch belongs to a different parent task')
    run_id = 'fc240b92da874cca9a05f9630aeb073e'
    batch = HERE / 'live-tests' / ('batch-309-' + run_id)
    access = receipt_access.build()
    with closing(sqlite3.connect(access.database.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        parent = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        claim = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                           (task_id, group_id)).fetchone()
        job = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        result = db.execute('SELECT * FROM postprocessing_results WHERE task_id=? AND group_id=?',
                            (task_id, group_id)).fetchone()
        receipt = db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?', (claim['claim_id'],)).fetchone()
        if (not parent or not claim or not job or not result or not receipt
                or claim['state'] != 'waiting_rag' or job['state'] != 'finished'
                or result['state'] != 'passed' or result['stage_receipt_sha256'] != receipt['receipt_sha256']):
            raise ValueError('folder postprocessing is not ready for this isolated preparation')
        stage = dict(claim)
        stage.update(postprocess_job_id=job['job_id'], source_generation=job['generation'],
                     upstream_manifest_sha256=job['manifest_sha256'],
                     postprocess_receipt_sha256=receipt['receipt_sha256'], run_generation=parent['start_generation'])

    @contextmanager
    def admit(*_):
        with task_effect_admission(access.database, task_id, stage['run_generation']) as permit:
            if create_targets:
                yield permit
                return
            class ReadOnlyPermit:
                def check_current(self): permit.check_current()
                def start_transport(self, _record):
                    raise PermissionError('preparation may only reuse existing test targets')
            yield ReadOnlyPermit()

    raw = RagflowClient(settings['rag_url'], load_key(settings.path('settings_root'), 'ragflow'))
    config = load_native_batch_config(Path(r'E:\uv\ragflow_upstream\data'), raw)
    ssh = paramiko.SSHClient()
    ssh.load_host_keys(config.memory_settings['known_hosts'])
    ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    ssh.connect(config.memory_settings['host'], port=config.memory_settings['port'],
                username=config.memory_settings['username'],
                key_filename=str(WORKSPACE / 'prefect-paperflow/runtime/remote-postprocessing/server-rag-readonly-ed25519'),
                look_for_keys=False, allow_agent=False, timeout=5, auth_timeout=5, banner_timeout=5)
    try:
        with ssh.open_sftp() as sftp:
            result = prepare_group(stage, access, sftp,
                pool_database=WORKSPACE / 'local-intake-runtime/waiting-pool.sqlite3',
                test_root=Path.home() / 'Desktop' / ('PaperFlow-SELFTEST-309-' + run_id) / 'prefect-groups',
                work_root=batch / 'prefect-preparation', catalog_path=settings.path('catalog_path'),
                catalog_sha256='6394b6dc7376cfd2cea64451e5d4b00a616999327b4424dea734f04977a9746b',
                run_id=run_id, raw=raw, dataset_journal=EffectJournal(batch / 'rag-dataset-effects.sqlite3'),
                admit=admit, dataset_receipts=batch, config=config)
    finally:
        ssh.close()
    proof = {'schema': 'paperflow-isolated-group-preparation/1', 'task_id': task_id, 'group_id': group_id,
             'run_id': run_id, 'source_generation': stage['source_generation'],
             'manifest_sha256': result['packet']['manifest_sha256'],
             'copy_receipt_path': result['copy_receipt_path'], 'specialties': len(result['plan']['routes']),
             'attachments': len(result['attachments']),
             'reported_privacy_redaction': result['reported_privacy_redaction'],
             'prefect_stage_created': False, 'paper_sent_to_rag': False}
    write_json(batch / 'prefect-preparation' / group_id / 'preparation.json', proof)
    print(json.dumps(proof, ensure_ascii=False), flush=True)
    return stage, result, access, raw, config


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise ValueError('one existing task ID and group ID are required')
    main(sys.argv[1], sys.argv[2])
