import tempfile
from pathlib import Path

from ingest_queue import QueueStore
from ingest_workbench import enqueue_workbench_items
from upload_tracking import queued_upload_status
from test_ingest_workbench import prepared


def test_queue_status_never_claims_upload_before_ack():
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory)
        store,item,config=prepared(root)
        queue=QueueStore(root/'queue.sqlite3')
        ids=enqueue_workbench_items(store,'batch-1',[item['id']],None,
            'upload_and_parse',queue,config,'http://localhost:9380')
        primary=queue.find_identity('http://localhost:9380','finance-md',
                                    item['paper_id'],'paper_primary')
        status=queued_upload_status(ids,queue)
        assert status['upload']=='排队待上传'
        assert status['process']=='尚未提交'
        queue.transition(primary['id'],'upload_waiting','upload_intent')
        assert queued_upload_status(ids,queue)['upload']=='上传中'
        queue.transition(primary['id'],'upload_intent','upload_unknown')
        assert queued_upload_status(ids,queue)['upload']=='上传待确认'
        queue.transition(primary['id'],'upload_unknown','parse_waiting',remote_id='remote-1')
        status=queued_upload_status(ids,queue)
        assert status['upload']=='已上传'
        assert status['process']=='排队中'
        assert status['assets_total']==len(ids)-1


def test_confirmed_upload_only_is_not_reported_as_parsed():
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory)
        store,item,config=prepared(root)
        queue=QueueStore(root/'queue.sqlite3')
        ids=enqueue_workbench_items(store,'batch-1',[item['id']],None,
            'upload_only',queue,dict(config,manual_request=True),
            'http://localhost:9380')
        primary=queue.find_identity('http://localhost:9380','finance-md',
                                    item['paper_id'],'paper_primary')
        queue.transition(primary['id'],'upload_waiting','uploaded',remote_id='remote-1')
        status=queued_upload_status(ids,queue)
        assert status['upload']=='已上传'
        assert status['process']=='尚未提交'
