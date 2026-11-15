"""Real paper_order handoff into the native preprocessing retry guard, without I/O."""
import asyncio,copy,importlib.util,os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID,uuid4
import pytest

from paperflow_v3.stages import pipeline
from paperflow_v3.control.stage_admission import unsettled_requests

if os.getenv('TEST_FLOW_MODULE'):
    spec=importlib.util.spec_from_file_location('preprocessing_retry_candidate',os.environ['TEST_FLOW_MODULE'])
    flows=importlib.util.module_from_spec(spec);spec.loader.exec_module(flows)
else:
    from paperflow_v3.control import flows

class PreprocessingReached(InterruptedError):pass
class UnknownRequestPreserved(InterruptedError):pass

@pytest.fixture
def case(tmp_path,monkeypatch):
    task,child_id,deployment,flow_id,parent_task=[uuid4() for _ in range(5)]
    reg={'task_id':str(task),'group_id':'group','paper_id':'paper','receipt_id':'receipt'}
    identity={**reg,'attempt_id':str(child_id),'task_run_id':str(child_id),'parent_run_id':str(task)}
    child=SimpleNamespace(id=child_id,parent_task_run_id=parent_task,deployment_id=deployment,
        flow_id=flow_id,idempotency_key='paperflow-v3-'+str(task)+'-group')
    manual={'kind':'manual_retry','data':{'retry_id':'reviewed-preprocessing','stage':'preprocessing','state':'authorized'}}
    failed={'kind':'preprocessing_failed','event_id':'old-local-failure',
        'data':{'processing_attempt_id':'initial','retryable':True,'failure_reason':'local_io'}}
    journal=[manual,failed];calls=[];reads=[];flags={}

    @asynccontextmanager
    async def context(*a,**k):yield SimpleNamespace()
    client=SimpleNamespace(read_task_run=AsyncMock(return_value=SimpleNamespace(flow_run_id=task)),
        read_deployment=AsyncMock(return_value=SimpleNamespace(flow_id=flow_id)))
    @asynccontextmanager
    async def client_context():yield client
    async def read(*a):
        reads.append(True)
        if flags.get('read_fail') and len(reads)>1:raise OSError('receipt read unconfirmed')
        return copy.deepcopy(journal)
    async def save(**kw):
        if flags.get('write_fail'):raise OSError('receipt write unconfirmed')
        journal.insert(0,{'kind':kw['kind'],'data':copy.deepcopy(kw['data'])})
        if flags.get('superseded'):
            journal.insert(0,{'kind':'manual_retry','data':{**kw['data'],'retry_id':'different-retry'}})
        if flags.get('new_unknown'):
            journal.insert(0,{'kind':'llm_request_unknown','data':{'component_id':'body','request_id':'never-replay'}})
    async def local_save(**kw):journal.insert(0,{'kind':kw['kind'],'data':copy.deepcopy(kw['data'])})
    async def local_drain(fn):return fn()
    def prepare(*a,**k):calls.append('preprocessing');raise PreprocessingReached('actual retry guard admitted preprocessing')
    async def run(real_identity,registered,settings,admission,limits,first_root,retry_id,records):
        assert real_identity==identity
        if flags.get('new_unknown') and unsettled_requests(records,'llm'):
            raise UnknownRequestPreserved('unknown request remains visible to native admission')
        return await pipeline.process_paper(identity=real_identity,registered=registered,settings=settings,
            connection=None,admission=SimpleNamespace(),limits={'preprocessing':['local'],'postprocessing':['package']},
            catalog={},paperflow_root=tmp_path,counter_factory=None,output_root=tmp_path,
            retry_id=retry_id,records=records)
    monkeypatch.setenv('PAPERFLOW_V3_CHILD_DEPLOYMENT_ID',str(deployment))
    monkeypatch.setattr(flows.FlowRunContext,'get',lambda:SimpleNamespace(flow_run=child))
    monkeypatch.setattr(flows,'get_client',client_context)
    monkeypatch.setattr('paperflow_v3.control.recovery.read_scope',AsyncMock(return_value=None))
    monkeypatch.setattr(flows,'read_receipts',read);monkeypatch.setattr(flows,'save_receipt',save)
    monkeypatch.setattr(flows,'load_runtime',lambda:(SimpleNamespace(path=lambda n:tmp_path),None))
    monkeypatch.setattr(flows,'worker_phase',context)
    monkeypatch.setattr(flows,'current_phase',lambda:SimpleNamespace(names=('local',)))
    monkeypatch.setattr(flows,'read_registration',AsyncMock(return_value=reg))
    monkeypatch.setattr(flows,'run_paper',run)
    monkeypatch.setattr(pipeline,'stage_workspace',lambda *a:tmp_path)
    monkeypatch.setattr(pipeline,'FolderStage',context);monkeypatch.setattr(pipeline,'admitted_slot',context)
    monkeypatch.setattr(pipeline,'save_receipt',local_save);monkeypatch.setattr(pipeline,'drain_thread',local_drain)
    monkeypatch.setattr(pipeline,'prepare_registered_folder',prepare)
    # Windows asyncio opens a local socket pair while creating its event loop.
    # Forbid application transports while retaining that internal loop setup.
    monkeypatch.setattr('httpx.Client.send',lambda *a,**k:pytest.fail('network forbidden'))
    monkeypatch.setattr('httpx.AsyncClient.send',lambda *a,**k:pytest.fail('network forbidden'))
    monkeypatch.setattr('urllib.request.urlopen',lambda *a,**k:pytest.fail('network forbidden'))
    monkeypatch.setattr('subprocess.Popen',lambda *a,**k:pytest.fail('subprocess forbidden'))
    return SimpleNamespace(run=lambda:asyncio.run(flows.paper_order.fn(str(task),'group',reg)),
        journal=journal,calls=calls,reads=reads,flags=flags)

def test_authorized_retry_reaches_native_preprocessing_after_running_receipt_is_saved(case):
    with pytest.raises(PreprocessingReached):case.run()
    assert case.calls==['preprocessing']
    assert next(r['data']['state'] for r in case.journal if r['kind']=='manual_retry')=='running'

def test_already_running_retry_keeps_its_existing_identity(case):
    case.journal[0]['data']['state']='running'
    with pytest.raises(PreprocessingReached):case.run()
    assert case.calls==['preprocessing']

def test_unconfirmed_running_receipt_write_never_starts_preprocessing(case):
    case.flags['write_fail']=True
    with pytest.raises(OSError,match='write unconfirmed'):case.run()
    assert case.calls==[]

def test_unconfirmed_refresh_never_starts_preprocessing(case):
    case.flags['read_fail']=True
    with pytest.raises(OSError,match='read unconfirmed'):case.run()
    assert case.calls==[]

def test_later_retry_identity_cannot_be_consumed_by_the_old_request(case):
    case.flags['superseded']=True
    with pytest.raises(RuntimeError,match='等待用户人工重试'):case.run()
    assert case.calls==[]

def test_unknown_request_arriving_during_authorization_remains_visible(case):
    case.flags['new_unknown']=True
    with pytest.raises(UnknownRequestPreserved):case.run()
    assert case.calls==[]
