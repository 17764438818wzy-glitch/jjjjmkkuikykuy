import copy
from pathlib import Path

import pytest

from paperflow.package import save
from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag,flow
from test_winjob import wait_until


def test_launcher_requires_deployment_acceptance_before_a_worker(delivery_case):
    from paperflow.delivery_worker import launch_delivery
    m,c,t,d,commits,*_=delivery_case
    with pytest.raises(ValueError,match='acceptance'):launch_delivery(m,c,d,'120201')
    assert not m.executions


def test_live_deployment_fingerprint_change_blocks_all_writes(delivery_case,tmp_path):
    from paperflow.delivery import DeliveryWorkflow
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    identity={'candidate':True,'token':'old'}
    report=dict(passed=True,endpoint=client.base_url,identity=identity)
    path=tmp_path/'visibility.json';save(path,report);a.config.delivery_acceptance_report=str(path)
    a.check_deployment=lambda:DeliveryWorkflow.check_deployment(a)
    a.client.deployment_identity=lambda:{'candidate':True,'token':'new'}
    with pytest.raises(ValueError,match='fingerprint'):a.run()
    assert not client.calls and not client.docs
