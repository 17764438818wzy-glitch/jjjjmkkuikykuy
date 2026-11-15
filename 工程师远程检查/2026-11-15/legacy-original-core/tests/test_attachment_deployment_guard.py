from paperflow.attachments import AttachmentLedger
from paperflow.package import load,save
from test_attachment_delivery import case,worker
import pytest


@pytest.mark.parametrize('pilot',[False,True])
def test_changed_deployment_identity_blocks_first_attachment_write(tmp_path,pilot):
    c,s,aid,m=case(tmp_path)
    try:
        if pilot:
            c.production_pilot='offline-route-fixture';c.save(tmp_path/'config.json')
        data=load(c.root/'backend.json')
        data['deployment_identity']={'dataset_ids':['different-deployment']}
        save(c.root/'backend.json',data)
        worker(c,s,aid,m)
        assert load(c.root/'backend.json')['calls'].get('POST /files',0)==0
        assert AttachmentLedger(s).item(aid)['state']!='complete'
    finally:m.shutdown()


def test_attachment_reuses_authenticated_production_identity_route_in_offline_transport(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        c.production_pilot='offline-route-fixture';c.save(tmp_path/'config.json')
        worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']=='complete'
        assert load(c.root/'backend.json')['calls']['GET /paperflow/deployment']==1
    finally:m.shutdown()
