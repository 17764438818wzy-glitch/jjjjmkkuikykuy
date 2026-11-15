from ingest_queue import QueueStore
from test_ingest_contracts import spec


def test_old_unavailable_diagnostics_do_not_hide_later_pending_work(tmp_path):
    q=QueueStore(tmp_path/'queue.db')
    for n in range(3):
        key=q.enqueue(spec(source_id='p'+str(n),remote_name=f'p{n}.md'),
            {'status':'outcome_unresolved','remote_id':f'd{n}',
             'failure_kind':'diagnostic_pending' if n==2 else 'diagnostic_unavailable'})
    assert [task['id'] for task in q.diagnostics_ready(1)]==[key]
