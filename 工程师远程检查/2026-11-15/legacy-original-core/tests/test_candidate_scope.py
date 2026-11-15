import importlib.util
from pathlib import Path

import pytest


def guard():
    path=Path('server-patch/serve_candidate.py')
    if not path.exists():return lambda *a,**k:False
    spec=importlib.util.spec_from_file_location('serve_candidate',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module.allowed_request


@pytest.mark.parametrize('method,path,body,allowed',[
    ('POST','/api/v1/datasets/owned/documents',None,True),
    ('PATCH','/api/v1/datasets/formal/documents/d',{'enabled':0},False),
    ('POST','/api/v1/retrieval',{'dataset_ids':['owned']},True),
    ('POST','/api/v1/retrieval',{'dataset_ids':['owned','formal']},False),
    ('POST','/api/v1/retrieval',{},False),
    ('GET','/api/v1/documents/images/owned-chunk',None,True),
    ('GET','/api/v1/documents/images/formal-chunk',None,False),
    ('POST','/api/v1/datasets',{'name':'formal'},False),
    ('POST','/api/v1/datasets/owned/documents/parse',{},False),
    ('POST','/api/v1/datasets/owned/documents/d/chunks',{'content':'x'},True),
    ('GET','/v1/user/list',None,False)])
def test_candidate_api_cannot_leave_owned_datasets(method,path,body,allowed):
    assert guard()(method,path,body,{'owned'}) is allowed
