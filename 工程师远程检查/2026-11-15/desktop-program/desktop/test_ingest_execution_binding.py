import json

from ingest_gateway import RagflowGateway, reconcile_write
from ingest_queue import QueueStore
from test_ingest_contracts import spec
from test_ingest_gateway import FakeClient


def test_lost_parse_ack_cannot_be_confirmed_by_previous_execution(tmp_path):
    queue = QueueStore(tmp_path / 'queue.db')
    key = queue.enqueue(spec(), {'status': 'parse_waiting', 'remote_id': 'doc'})
    task = queue.claim('parse', 'worker', 1, 100, 120)[0]
    remote = {'id': 'doc', 'name': task['remote_name'], 'pipeline_id': task['pipeline_id'],
              'run': 'FAIL', 'process_begin_at': '2026-09-26T01:00:00'}
    client = FakeClient([remote])
    client.base_url = task['service_key']
    def timeout(*args):
        # A lost acknowledgement before delivery must retain the old terminal.
        raise TimeoutError()
    client.parse = timeout
    gateway = RagflowGateway(lambda: client, baseline_recorder=queue.record_parse_baseline)
    try:
        gateway.parse(task)
    except TimeoutError:
        pass
    persisted = queue.get(key)
    assert json.loads(persisted['parse_baseline'])['process_begin_at'] == remote['process_begin_at']
    assert reconcile_write(persisted, 'parse', gateway) == 'uncertain'
    remote.update(process_begin_at='2026-09-27T01:00:00', run='RUNNING')
    assert reconcile_write(persisted, 'parse', gateway) == 'confirmed'


def test_old_log_cannot_be_relabelled_with_current_attempt(tmp_path):
    from ingest_execution_diagnostics import execution_diagnostic
    from test_ingest_execution_diagnostics import record
    task, remote, log = record(tmp_path)
    task.update(parse_baseline=json.dumps({'remote_id': 'doc',
        'process_begin_at': remote['process_begin_at']}))
    assert execution_diagnostic(task, remote, log) is None
