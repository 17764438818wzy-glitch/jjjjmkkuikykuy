import json
from pathlib import Path
from unittest.mock import patch

import pytest

from ingest_service import ensure_work_pool_worker, validate_worker_service


def test_service_mismatch_is_rejected_before_worker_start(tmp_path):
    (tmp_path/'settings.json').write_text(json.dumps({'ragflow_url':'http://localhost:9380'}))
    validate_worker_service(tmp_path,'http://127.0.0.1:9380/')
    with patch('ingest_service.subprocess.Popen') as launched:
        with pytest.raises(ValueError):
            ensure_work_pool_worker(tmp_path,tmp_path/'queue.sqlite3','http://other:9380')
        launched.assert_not_called()


def test_running_worker_is_reused_and_no_second_process_is_started(tmp_path):
    (tmp_path/'settings.json').write_text(json.dumps({'ragflow_url':'http://localhost:9380'}))
    with patch('ingest_service._worker_has_lock',return_value=True), \
         patch('ingest_service.subprocess.Popen') as launched:
        assert ensure_work_pool_worker(tmp_path,tmp_path/'queue.sqlite3',
                                      'http://localhost:9380') is False
        launched.assert_not_called()


def test_handoff_starts_worker_only_after_queue_is_durable(tmp_path):
    from auto_ragflow import handoff
    from ingest_queue import QueueStore
    from test_ingest_workbench import prepared
    store,item,config=prepared(tmp_path)
    store.root=tmp_path
    (tmp_path/'settings.json').write_text(json.dumps({'ragflow_url':'http://localhost:9380'}))
    queue_path=tmp_path/'queue.sqlite3'
    class Client:
        base_url='http://localhost:9380'
        def datasets(self): return config['datasets']
    def started(root,path,url,**kwargs):
        task=QueueStore(path).find_identity(url,'finance-md',item['paper_id'],'paper_primary')
        assert task is not None
        assert task['status']=='upload_waiting'
        return True
    with patch('ingest_service.ensure_work_pool_worker',side_effect=started) as worker:
        handoff(store,'batch-1',Client(),config['dataset_map'],
                upload_config=dict(config,work_pool_enabled=True,
                                   work_pool_queue=str(queue_path)))
        worker.assert_called_once()

def test_verified_dual_routes_cannot_raise_explicit_worker_capacity(tmp_path):
    from test_ingest_model_routes import route_config
    value=route_config()
    (tmp_path/'settings.json').write_text(json.dumps({'ragflow_url':'http://localhost:9380'}))
    path=tmp_path/'private/education_dual_channel/routes.json'; path.parent.mkdir(parents=True)
    path.write_text(json.dumps(value))
    with patch('ingest_service._worker_has_lock',side_effect=[False,True]), patch('ingest_service.subprocess.Popen') as launched:
        ensure_work_pool_worker(tmp_path,tmp_path/'q.sqlite3','http://localhost:9380',parse_limit=1)
    args=launched.call_args.args[0]
    assert int(args[args.index('--parse-limit')+1])==1
