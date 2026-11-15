import json
from dataclasses import replace

import pytest

from auto_config import load_config, save_config
from ingest_contracts import ArtifactSpec, Outcome
from ingest_gateway import RagflowGateway, IdentityMismatch
from ingest_worker import classify_task
from ingest_workbench import workbench_specs
from test_ingest_workbench import prepared


def native_spec(tmp_path, configured=True):
    store, item, config = prepared(tmp_path)
    config['datasets'][0].update(pipeline_id=None, chunk_method='naive')
    if configured:
        config['dataset_native_parser_map'] = {'finance-md': 'naive'}
    specs = workbench_specs(store, 'batch-1', [item['id']], None,
                            'upload_and_parse', config, 'http://localhost:9380')
    return next(s for s in specs if s.role == 'paper_primary')


def test_explicit_native_dataset_reuses_queue_identity_and_approval(tmp_path):
    spec = native_spec(tmp_path)
    assert spec.pipeline_id is None
    assert spec.policy_key == 'native_naive'
    assert spec.parse_required
    assert spec.package_sha256
    assert spec.channel_key == 'native-naive'


def test_missing_pipeline_still_fails_without_host_opt_in(tmp_path):
    with pytest.raises(ValueError, match='Pipeline'):
        native_spec(tmp_path, configured=False)


def test_configured_native_does_not_override_pipeline(tmp_path):
    store, item, config = prepared(tmp_path)
    config['dataset_native_parser_map'] = {'finance-md': 'naive'}
    with pytest.raises(ValueError, match='native|Native'):
        workbench_specs(store, 'batch-1', [item['id']], None,
                        'upload_and_parse', config, 'http://localhost:9380')


def test_native_policy_does_not_allow_arbitrary_no_pipeline_tasks(tmp_path):
    spec = native_spec(tmp_path)
    with pytest.raises(ValueError, match='Pipeline'):
        replace(spec, policy_key='finance')
    with pytest.raises(ValueError, match='native|Native'):
        replace(spec, pipeline_id='foreign-pipeline')


class Client:
    base_url = 'http://localhost:9380'
    def __init__(self, document):
        self.document, self.writes = document, []
    def documents(self, *a, **kw):
        return [dict(self.document)]
    def request(self, *a, **kw):
        self.writes.append(('request', a))
    def parse(self, *a):
        self.writes.append(('parse', a))


def task_and_client(spec):
    task = dict(spec.__dict__, remote_id='remote-1')
    doc = {'id': 'remote-1', 'name': spec.remote_name,
           'pipeline_id': None, 'chunk_method': 'naive', 'run': 'UNSTART'}
    return task, Client(doc)


def test_native_parse_uses_original_parse_post_and_baseline(tmp_path):
    task, client = task_and_client(native_spec(tmp_path))
    baselines = []
    gateway = RagflowGateway(lambda: client,
                             baseline_recorder=lambda t, d: baselines.append(d))
    gateway.parse(task)
    assert client.writes == [('parse', ('finance-md', 'remote-1'))]
    assert baselines[0]['pipeline_id'] is None


@pytest.mark.parametrize('changes', [
    {'pipeline_id': 'foreign'}, {'chunk_method': 'qa'},
    {'run': 'RUNNING'}, {'id': 'other'}, {'name': 'other'}])
def test_native_parse_blocks_remote_drift_without_writes(tmp_path, changes):
    task, client = task_and_client(native_spec(tmp_path))
    client.document.update(changes)
    with pytest.raises(IdentityMismatch):
        RagflowGateway(lambda: client).parse(task)
    assert client.writes == []


def test_native_done_is_technical_ingestion_not_quality_admission():
    task = {'policy_key': 'native_naive'}
    assert classify_task(task, {'run': 'DONE', 'chunk_count': 3}) == Outcome.accepted
    assert classify_task(task, {'run': 'DONE', 'chunk_count': 0}) == Outcome.technical_review
    assert classify_task(task, {'run': 'FAIL', 'chunk_count': 3}) == Outcome.technical_review


def test_native_host_config_survives_normal_save(tmp_path):
    config = load_config(tmp_path)
    config['dataset_native_parser_map'] = {'finance-md': 'naive'}
    save_config(tmp_path, config)
    assert load_config(tmp_path)['dataset_native_parser_map'] == {'finance-md': 'naive'}


def test_native_queue_waits_for_metadata_and_confirms_same_execution(tmp_path):
    from threading import Event
    from ingest_queue import QueueStore
    from ingest_scheduler import WorkPool
    from test_ingest_source_metadata import source_spec, confirmed, drive
    artifact = replace(source_spec(tmp_path), policy_key='native_naive', pipeline_id=None)
    task, client = task_and_client(artifact)
    queue = QueueStore(tmp_path/'queue.db')
    key = queue.enqueue(artifact, {'status':'parse_waiting', 'remote_id':'remote-1'})
    started, release = Event(), Event()
    def receive(task, remote):
        started.set()
        release.wait(3)
        return confirmed(task, remote)
    def parse(*args):
        client.writes.append(('parse', args))
        client.document.update(run='DONE', chunk_count=4,
                               process_begin_at='2026-10-10T22:00:00')
    client.parse = parse
    pool = WorkPool(queue, lambda:RagflowGateway(lambda:client,
                    baseline_recorder=queue.record_parse_baseline),
                    classify_task, on_source_metadata=receive)
    try:
        drive(pool, started.is_set)
        assert client.writes == []
        release.set()
        drive(pool, lambda:queue.get(key)['status']=='accepted')
        row = queue.get(key)
        assert row['remote_id']=='remote-1'
        assert row['source_metadata_status']=='confirmed'
        assert row['execution_begin_at']=='2026-10-10T22:00:00'
        assert client.writes == [('parse', ('md-1', 'remote-1'))]
    finally:
        release.set()
        pool.close()


def test_native_uncertain_parse_uses_existing_execution_reconciliation(tmp_path):
    from ingest_gateway import reconcile_write
    task, client = task_and_client(native_spec(tmp_path))
    task['parse_baseline'] = {'remote_id':'remote-1', 'process_begin_at':None}
    client.document.update(run='RUNNING', process_begin_at='2026-10-10T22:00:00')
    assert reconcile_write(task, 'parse', RagflowGateway(lambda:client))=='confirmed'
    assert client.writes==[]


@pytest.mark.parametrize('method', ['qa','book',None])
def test_native_reconcile_does_not_adopt_another_parser(tmp_path, method):
    from ingest_gateway import reconcile_write
    task, client = task_and_client(native_spec(tmp_path))
    task['parse_baseline'] = {'remote_id':'remote-1', 'process_begin_at':None}
    client.document.update(run='RUNNING', process_begin_at='2026-10-10T22:00:00',chunk_method=method)
    assert reconcile_write(task, 'parse', RagflowGateway(lambda:client))=='uncertain'
    assert client.writes==[]


def test_native_baseline_accepts_omitted_null_pipeline(tmp_path):
    from ingest_queue import QueueStore
    artifact = native_spec(tmp_path)
    task, client = task_and_client(artifact)
    del client.document['pipeline_id']
    queue=QueueStore(tmp_path/'queue.db')
    key=queue.enqueue(artifact, {'status':'parse_waiting','remote_id':'remote-1'})
    queue.transition(key,'parse_waiting','parse_intent')
    RagflowGateway(lambda:client,baseline_recorder=queue.record_parse_baseline).parse(queue.get(key))
    assert json.loads(queue.get(key)['parse_baseline'])['pipeline_id'] is None
