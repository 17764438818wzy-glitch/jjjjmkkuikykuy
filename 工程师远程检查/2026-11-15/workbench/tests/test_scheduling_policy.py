from datetime import datetime, timezone
from email.utils import format_datetime

import pytest

from paperflow.producer import model_failure
from paperflow.rag import RemoteConflict


def policy():
    from paperflow import scheduling_policy
    return scheduling_policy


@pytest.mark.parametrize('status,category', [(400,'held'),(401,'configuration'),(403,'configuration'),
                                           (408,'transient'),(429,'transient'),(503,'transient')])
def test_model_status_survives_without_private_message(status,category):
    error=model_failure([{'type':'turn/end','data':{'reason':{'error':{'message':f'HTTP {status}: private token and manuscript'}}}}])
    result=policy().fault(error)
    assert result['category']==category and result['status']==status
    assert 'private' not in str(result)


def test_conflict_is_never_a_service_transport_failure():
    assert policy().fault(RemoteConflict('wrong document identity'))['category']=='conflict'
    assert policy().fault(ConnectionError('secret'))['category']=='transient'
    assert policy().fault(ValueError('mapping error'))['category']=='held'
    assert policy().fault(policy().ActionDeferred('resource_busy'))['category']=='deferred'


def test_backoff_jitter_cap_and_retry_after():
    p=policy()
    assert p.backoff(1,rng=lambda:.5)==20
    assert p.backoff(2,rng=lambda:.5)==40
    assert 16<=p.backoff(1,rng=lambda:0)<=24
    assert p.backoff(90,rng=lambda:1)==900
    assert p.backoff(1,retry_after=1200,rng=lambda:.5)==1200
    assert p.retry_after('31',now=100)==31
    assert p.retry_after(format_datetime(datetime.fromtimestamp(140,timezone.utc)),now=100)==40
    assert p.retry_after('not a date',now=100)==0
    assert p.retry_after('nan',now=100)==0


def test_config_code_and_internal_exception_do_not_become_content_repair():
    error=model_failure([{'type':'turn/end','data':{'reason':{'error':{'code':'MISSING_CREDENTIAL'}}}}])
    assert policy().fault(error)['category']=='configuration'
    assert policy().fault(RuntimeError('unexpected controller bug'))['category']=='controller'
