"""A terminal local counter failure must not be mistaken for an unknown network write."""
import asyncio
from types import SimpleNamespace
from uuid import uuid4
import pytest
from paperflow_v3.control.retry import retry_decision
from test_folder_pause import identity


def failure(**changes):
    return {'kind':'postprocessing_component_failed','data':{
        'component_id':'body','error_type':'TimeoutError','reason':'offline counter deadline exceeded',**changes}}


def test_exact_body_counter_timeout_only_retries_postprocessing():
    records=[failure(),{'kind':'llm_response_saved','data':{'component_id':'part','request_id':'original'}}]
    decision=retry_decision(records)
    assert decision['allowed'] is True and decision['stage']=='postprocessing'


@pytest.mark.parametrize('changes',[{'reason':'request timed out'},{'reason':'offline counter deadline exceeded (network)'},
    {'error_type':'ReadTimeout'},{'component_id':'attachment:survey'}])
def test_other_timeouts_remain_unconfirmed(changes):
    assert not retry_decision([failure(**changes)])['allowed']


def test_local_failure_never_clears_model_or_delivery_unknown():
    for unknown in [{'kind':'llm_request_unknown','data':{'component_id':'part','request_id':'original'}},
                    {'kind':'rag_effect_intent','data':{'effect_id':'original-write'}}]:
        assert not retry_decision([failure(),unknown])['allowed']


def test_native_manual_retry_replaces_old_unknown_and_completes(stage_store):
    async def scenario():
        store=stage_store;ids=identity();parent=store.admission.Admission(ids['task_id'])
        await parent.start()
        old=store.stages.FolderStage(parent,ids,'postprocessing')
        store.latest[old.key]=SimpleNamespace(key=old.key,type=store.stages.UNKNOWN,
            data={'processing_attempt_id':'initial'})
        record=failure();record.update(task_id=ids['task_id'],folder_id=ids['group_id'])
        store.raw.append(record)
        assert retry_decision([record])['stage']=='postprocessing'
        retry_id=str(uuid4())
        store.raw.append({'kind':'manual_retry','task_id':ids['task_id'],'folder_id':ids['group_id'],
            'data':{'retry_id':retry_id,'stage':'postprocessing','state':'authorized'}})
        async with store.stages.FolderStage(parent,ids,'postprocessing',retry_id=retry_id) as stage:
            assert await stage.running()
            store.raw.append({'kind':'postprocessing_component_complete','folder_id':ids['group_id'],
                'data':{'component_id':'body','processing_attempt_id':retry_id}})
        assert store.history[-1].type==store.stages.SETTLED
        assert store.history[-1].data['state']=='complete'
        assert record['data']['error_type']=='TimeoutError'
    asyncio.run(scenario())
