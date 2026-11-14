from ingest_contracts import Outcome
from ingest_worker import classify_task


def test_generic_worker_keeps_finance_and_education_policies_separate():
    finance={'policy_key':'finance','source_id':'paper-1'}
    accepted={'run':'DONE','meta_fields':{
        'accounting_paper_id':'paper-1','accounting_admission':'入库'}}
    assert classify_task(finance,accepted)==Outcome.accepted
    assert classify_task(finance,{'run':'FAIL','progress_msg':'0 tasks are ahead in the queue...'})==Outcome.outcome_unresolved
    assert classify_task({'policy_key':'generic'},{'run':'DONE'})==Outcome.accepted
    assert classify_task({'policy_key':'generic'},{'run':'FAIL'})==Outcome.technical_review


def test_worker_loads_routes_from_selected_data_root(tmp_path):
    import json
    from ingest_worker import configured_router
    from ingest_queue import QueueStore
    from test_ingest_model_routes import route_config
    path=tmp_path/'private'/'education_dual_channel'/'routes.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(route_config()),encoding='utf-8')
    queue=QueueStore(tmp_path/'queue.sqlite3')
    router,reader=configured_router(queue,tmp_path,'http://localhost:9380',lambda:None)
    assert router.total_limit==8
    assert reader is not None
    absent=tmp_path/'another'
    router,reader=configured_router(queue,absent,'http://localhost:9380',lambda:None)
    assert router is None and reader is None
