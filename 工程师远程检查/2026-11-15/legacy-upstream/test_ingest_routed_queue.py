from concurrent.futures import ThreadPoolExecutor
import pytest

from ingest_queue import QueueStore
from test_ingest_contracts import spec
from upload_tracking import service_key


def route(channel='school'):
    return {'service_key': service_key('http://localhost:9380'), 'dataset_id': 'md-1',
            'source_pipeline_id': 'pipe-1', 'pipeline_id': 'pipe-1' if channel == 'school' else 'official-pipe',
            'channel_key': channel, 'model_id': channel+'-model', 'provider': 'DeepSeek',
            'instance': channel, 'contract_version': 'contract-1'}


def waiting(queue, source='p1'):
    item = spec(source_id=source, remote_name=source+'.md')
    task_id = queue.enqueue(item, {'status': 'parse_waiting', 'remote_id': 'remote-'+source})
    return task_id, item


def claim(queue, channel='school', owner='owner', external=None, total=8, limit=1):
    return queue.claim_routed_parse(owner, 100, 120, route(channel), total, limit, external or {})


def test_atomic_claims_enforce_channel_limit_across_workers(tmp_path):
    queue = QueueStore(tmp_path/'queue.sqlite3')
    for n in range(6):
        waiting(queue, 'p'+str(n))
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda n: claim(queue, owner=str(n)), range(6)))
    assert sum(item is not None for item in results) == 1
    assert len(queue.list_statuses(('parse_intent',))) == 1


def test_atomic_fixed_two_plus_four_limits_hold_across_router_workers(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    from test_ingest_dual_router import settings
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(12): waiting(queue,'p'+str(n))
    def routed_claim(n):
        router=DeepSeekRouter(QueueStore(queue.path),settings(2,4))
        return router.claim('worker-'+str(n),100,{})
    with ThreadPoolExecutor(max_workers=12) as pool:
        results=list(pool.map(routed_claim,range(12)))
    assert sum(task is not None and task['channel_key']=='school' for task in results)==2
    assert sum(task is not None and task['channel_key']=='official' for task in results)==4
    assert len(queue.list_statuses(('parse_intent',)))==6


def test_school_full_uses_official_and_preserves_original_identity(tmp_path):
    queue = QueueStore(tmp_path/'queue.sqlite3')
    first, _ = waiting(queue, 'p1'); second, original = waiting(queue, 'p2')
    assert claim(queue)['id'] == first
    assert claim(queue) is None
    selected = claim(queue, 'official')
    assert selected['id'] == second
    assert selected['pipeline_id'] == 'official-pipe'
    assert selected['requested_pipeline_id'] == 'pipe-1'
    assert queue.enqueue(original) == second
    restored = QueueStore(queue.path).get(second)
    assert restored['pipeline_id'] == 'official-pipe'
    assert restored['remote_id'] == 'remote-p2'
    assert restored['execution_model_id'] == 'official-model'
    assert queue.parse_attempts(second)[0]['attempt_no'] == 1


def test_unknown_and_external_calls_consume_capacity(tmp_path):
    queue = QueueStore(tmp_path/'queue.sqlite3')
    first, _ = waiting(queue, 'p1'); waiting(queue, 'p2')
    claim(queue)
    queue.transition(first, 'parse_intent', 'parse_unknown')
    assert claim(queue) is None
    assert claim(queue, 'official', total=1) is None
    queue.transition(first, 'parse_unknown', 'technical_review', remote_run='FAIL')
    assert claim(queue, external={'school': 1, 'total': 1}) is None


def test_fallback_is_once_terminal_and_does_not_reupload(tmp_path):
    queue = QueueStore(tmp_path/'queue.sqlite3')
    task_id, _ = waiting(queue)
    claim(queue)
    assert not queue.retry_on_channel(task_id, 'parse_intent', route('official'), 'llm:rate_limit')
    queue.transition(task_id, 'parse_intent', 'technical_review', remote_run='FAIL')
    assert queue.retry_on_channel(task_id, 'technical_review', route('official'), 'llm:rate_limit')
    assert claim(queue) is None
    selected = claim(queue, 'official')
    assert selected['remote_id'] == 'remote-p1'
    assert selected['execution_attempt'] == 2
    queue.transition(task_id, 'parse_intent', 'technical_review', remote_run='FAIL')
    assert not queue.retry_on_channel(task_id, 'technical_review', route('official'), 'llm:rate_limit')
    assert len(queue.parse_attempts(task_id)) == 2


@pytest.mark.parametrize('status', ['quality_hold', 'quality_reject', 'parse_unknown'])
def test_quality_decisions_and_unknown_cannot_be_rerouted(tmp_path,status):
    queue = QueueStore(tmp_path/'queue.sqlite3')
    task_id, _ = waiting(queue)
    claim(queue)
    queue.transition(task_id, 'parse_intent', status, remote_run='FAIL')
    assert not queue.retry_on_channel(task_id, status, route('official'), 'llm:rate_limit')


def test_channel_state_survives_restart_and_rejects_credentials(tmp_path):
    queue = QueueStore(tmp_path/'queue.sqlite3')
    queue.save_route_state('school', {'limit': 3, 'paused_until': 200})
    assert QueueStore(queue.path).route_state('school') == {'limit': 3, 'paused_until': 200}
    with pytest.raises(ValueError):
        queue.save_route_state('school', {'api_key': '<REDACTED_SECRET>'})

def test_first_claim_preserves_imported_previous_pipeline(tmp_path):
    q=QueueStore(tmp_path/'q.sqlite3'); item=spec()
    task_id=q.enqueue(item,{'status':'parse_waiting','remote_id':'r1','execution_previous_pipeline_id':'legacy-education'})
    task=claim(q)
    assert task['execution_previous_pipeline_id']=='legacy-education'

def test_finite_budget_required_and_verified_usage_replaces_reservation(tmp_path):
    q=QueueStore(tmp_path/'q.sqlite3'); waiting(q)
    bad=dict(route('official'),daily_cny_limit=float('nan'),reservation_cny=1)
    assert q.claim_routed_parse('w',100,120,bad,8,2,{}) is None
    task=claim(q,channel='official')
    q.record_parse_usage(task['id'],task['execution_attempt'],{'input_tokens':12,'output_tokens':5,'cost_cny':0.03})
    assert q.usage_summary('official',100)['cost_cny']==0.03

def test_imported_actual_binding_reconciles_and_occupies_its_own_channel(tmp_path):
    from ingest_gateway import RagflowGateway,reconcile_write
    q=QueueStore(tmp_path/'q.sqlite3')
    item=spec(source_id='imported',remote_name='imported.md')
    task_id=q.enqueue(item,{'status':'parse_unknown','remote_id':'r1','execution_pipeline_id':'official-pipe','execution_channel_key':'official'})
    class Client:
        base_url='http://localhost:9380'
        def documents(self,*args,**kwargs): return [{'id':'r1','name':'imported.md','pipeline_id':'official-pipe','run':'DONE'}]
    task=q.get(task_id)
    assert task['requested_pipeline_id']=='pipe-1'
    # An imported provider binding alone cannot prove an uncertain parse write.
    assert reconcile_write(task,'parse',RagflowGateway(Client))=='uncertain'
    waiting(q,'other')
    assert claim(q,channel='official',total=8,limit=1) is None
