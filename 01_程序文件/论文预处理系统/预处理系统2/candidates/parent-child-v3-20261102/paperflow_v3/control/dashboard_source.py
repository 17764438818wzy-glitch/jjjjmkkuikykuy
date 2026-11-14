"""Native Prefect reads for the existing dashboard. No control or repair calls."""
import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from uuid import UUID
from fastapi import HTTPException

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.settings import PREFECT_API_URL,temporary_settings

from .dashboard_cache import compact_folder,merge_folder
from .snapshots import load_task_snapshot,read_task_summaries
from .query_service import _owned_prefect_context

ACTIVE_STATES=('RUNNING','PENDING','PAUSED','CANCELLING')

def children_filter(task_id,**fields):
    return FlowRunFilter(parent_flow_run_id={'any_':[task_id]},tags={'all_':['paperflow-v3-paper']},**fields)


@asynccontextmanager
async def native_context(api_url):
    # Settings are bound in the owned read task, not only in the lifespan task.
    with temporary_settings({PREFECT_API_URL:api_url}):
        async with _owned_prefect_context():
            client=get_client()
            if str(client.api_url).rstrip('/')!=api_url.rstrip('/'):
                raise RuntimeError('Read client connected to another Prefect API')
            yield client


async def pages(client,selected,*,sort=FlowRunSort.ID_DESC):
    result=[]
    while True:
        rows=await client.read_flow_runs(flow_run_filter=selected,sort=sort,limit=200,offset=len(result))
        result.extend(rows)
        if len(rows)<200:return result


async def changed_children(client,task_id,saved,total,*,prime=False):
    if prime:return await pages(client,children_filter(task_id))
    active=await pages(client,children_filter(task_id,state={'type':{'any_':list(ACTIVE_STATES)}}))
    by_id={str(child.id):child for child in active}
    previous=set(saved.get('active_ids',[])) if saved else set()
    missing=previous-set(by_id)
    if missing:
        for child in await pages(client,children_filter(task_id,id={'any_':list(missing)})):by_id[str(child.id)]=child
    cursor=saved.get('end_cursor') if saved else None
    if cursor:
        after=datetime.fromisoformat(cursor)-timedelta(seconds=1)
        for child in await pages(client,children_filter(task_id,end_time={'after_':after},
                    state={'type':{'any_':['COMPLETED','FAILED','CRASHED','CANCELLED']}})):
            by_id[str(child.id)]=child
    known=set(saved.get('rows',{})) if saved else set()
    if len(known|set(by_id))<total:
        # Includes a child created and finished entirely between observations,
        # even if an old manually-created terminal state has no end_time.
        unseen=known|set(by_id)
        selected=children_filter(task_id,**({'id':{'not_any_':list(unseen)}} if unseen else {}))
        for child in await pages(client,selected):by_id[str(child.id)]=child
    return list(by_id.values())


class NativeDashboardSource:
    def __init__(self,api_url):self.api_url=api_url

    async def observe(self,task_id,saved,*,prime=False):
        from .api import _task_actions
        from .admission import Admission
        from .recovery import read_scope,actions as recovery_actions
        from .stage_admission import pause_execution_progress as pause_progress
        task_id=UUID(str(task_id))
        async with native_context(self.api_url) as client:
            run,total,crashed,running,variable,intent_variable=await asyncio.gather(
                client.read_flow_run(task_id),client.count_flow_runs(flow_run_filter=children_filter(task_id)),
                client.count_flow_runs(flow_run_filter=children_filter(task_id,state={'type':{'any_':['CRASHED']}})),
                Admission(task_id).running(),client.read_variable_by_name('paperflow_v3_classification_summary_'+str(task_id).replace('-','_')),
                client.read_variable_by_name('paperflow_v3_intent_'+str(task_id).replace('-','_')))
            if 'paperflow-v3-task' not in run.tags:raise HTTPException(404,'任务不存在')
            if saved is None and total and not prime:return {'total':total,'folders':[]}
            summary=(await read_task_summaries([task_id])).get(str(task_id),{}) if not running else {}
            control_seq=None
            stop_reason=summary.get('stop_reason')
            intent=intent_variable.value if intent_variable else None
            if (isinstance(intent,dict) and intent.get('target')=={'task_id':str(task_id)}
                    and intent.get('action') in ('start','pause','delete')
                    and type(intent.get('intent_seq')) is int and 0<=intent['intent_seq']<2**53
                    and isinstance(intent.get('request_id'),str)):
                control_seq=intent['intent_seq']
                if intent['action']=='start' and not running:
                    operation_variable=await client.read_variable_by_name('paperflow_v3_operation_'+str(UUID(intent['request_id'])).replace('-','_'))
                    operation=operation_variable.value if operation_variable else None
                    if (isinstance(operation,dict) and operation.get('status')=='rejected'
                            and (not summary or datetime.fromisoformat(summary['as_of'])<operation_variable.updated)
                            and all(operation.get(key)==intent[key] for key in ('request_id','action','target','intent_seq'))):
                        reason=operation.get('reason') or operation.get('message')
                        if not reason and isinstance(operation.get('failed_checks'),list):
                            details=[check['detail'] for check in operation['failed_checks'] if isinstance(check,dict)
                                     and isinstance(check.get('detail'),str) and check['detail']]
                            if details:reason='依赖自检未通过：'+'；'.join(details)
                        if isinstance(reason,str) and reason:stop_reason='上次开始未成功：'+reason
            children=await changed_children(client,task_id,saved,total,prime=prime)
            projection=(await load_task_snapshot(task_id,children,repair=False,include_parent=prime)
                        if children or prime else {'folders':[],'items':[],'as_of':None})
            if variable and isinstance(variable.value,dict) and variable.value.get('task_id')==str(task_id):
                classification=variable.value
            elif saved:classification=saved['classification']
            else:
                counts=Counter(row['status'] for row in projection.get('items',[]))
                classification={'task_id':str(task_id),'phase':'legacy' if total else 'open',
                    'as_of':projection.get('as_of'),'counts':{key:counts[key] for key in ('passed','failed','unrecognized','unresolved')}}
            if any(type(classification.get('counts',{}).get(key)) is not int or classification['counts'][key]<0
                   for key in ('passed','failed','unrecognized','unresolved')):raise ValueError('Invalid saved classification counters')
            combined=dict(saved['rows']) if saved else {}
            for folder in projection['folders']:
                row=compact_folder(folder);old=combined.get(row['ticket_id'])
                combined[row['ticket_id']]=merge_folder(old,row) if old else row
            old_state=saved.get('source_state',{}) if saved else {}
            items=(old_state.get('pause_items',[]) if saved else
                [{key:row.get(key) for key in ('group_id','status','registration_status')}
                 for row in projection.get('items',[]) if row.get('status')=='unresolved' or
                  row.get('status')=='passed' and row.get('registration_status')=='registered' and
                  row['group_id'] not in {child.parameters.get('group_id') for child in children}])
            pause_snapshot={'folders':list(combined.values()),'items':items,'missing':projection.get('missing',[])}
            # Owner settlement is a separate small native artifact, and can change
            # after the final stage snapshot. Read it each time; never scan raw
            # legacy receipts just to render the dashboard.
            progress=await pause_progress(task_id,snapshot=pause_snapshot,verify_legacy=False) if not running else None
            fake=SimpleNamespace(state=SimpleNamespace(is_crashed=lambda:bool(crashed)))
            actions=_task_actions(run,running,[fake])
            scope=await read_scope(task_id)
            recovery=None
            if scope is not None:
                if not (run.state.is_final() and not run.state.is_crashed()):
                    ids=list({*scope['eligible'],*scope['blocked']})
                    scoped=await pages(client,children_filter(task_id,id={'any_':ids})) if ids else []
                    actions={**recovery_actions(scope,scoped,running),'delete':{'allowed':True,'reason':None}}
                recovery={'mode':'existing_children','eligible':len(scope['eligible']),'blocked':len(scope['blocked'])}
            complete=(saved['complete'] if saved else projection.get('complete',not total))
            if classification.get('phase')=='closed':complete=classification['counts']['passed']==total
            if not running:state=(progress or {}).get('state','paused')
            elif run.state.is_scheduled() or run.state.is_pending():state='waiting'
            else:state='running'
            if run.state.is_completed():state='completed' if complete else 'unresolved'
            elif run.state.is_failed() or run.state.is_crashed():state='failed'
            elif run.state.is_cancelled():state='cancelled'
            elif (run.state.is_paused() and (progress or {}).get('state')=='paused'
                    and (progress or {}).get('active_folders')==0):
                # Native execution is paused; incomplete classification remains
                # visible in its own counters and does not turn Pause into failure.
                state='paused'
            times=[value for value in (projection.get('as_of'),classification.get('as_of'),summary.get('as_of'),saved.get('as_of') if saved else None) if value]
            as_of=max(times,key=lambda value:datetime.fromisoformat(value)) if times else run.created.isoformat()
            cursor=max([old_state.get('end_cursor') or run.created.isoformat(),
                        *(child.end_time.isoformat() for child in children if child.end_time)])
            compatibility=saved.get('compatibility',{}) if saved else {
                'missing_stage_folders':len({row['ticket_id'] for row in projection.get('missing',[])}),
                'classification_unknown':classification['counts']['unresolved'],
                'repair_started':False}
            return {'folders':projection['folders'],'total':total,'complete':complete,'classification':classification,
                'as_of':as_of,'active_ids':[str(child.id) for child in children if child.state.type.value in ACTIVE_STATES],
                'end_cursor':cursor,'compatibility':compatibility,
                'header':{'task_name':run.parameters.get('task_name',run.name),'parent_flow_run_id':str(task_id),
                          'desired_state':'running' if running else 'paused','task_state':state,'actions':actions,
                          'pause_progress':progress,'recovery':recovery,'stop_reason':stop_reason,'control_intent_seq':control_seq},
                'source_state':{'end_cursor':cursor,'pause_items':items}}

    async def details(self,task_id,ids=None):
        if ids==[]:return {'folders':[],'complete':False,'as_of':None}
        async with native_context(self.api_url) as client:
            children=await pages(client,children_filter(task_id,**({'id':{'any_':ids}} if ids is not None else {})))
            return await load_task_snapshot(UUID(str(task_id)),children,repair=False,include_parent=False)

    async def classifications(self,task_id):
        async with native_context(self.api_url):
            value=await load_task_snapshot(UUID(str(task_id)),[],repair=False)
            return {'task_id':str(task_id),'items':value['items'],'complete':value['classification_complete'],'as_of':value['as_of']}

    async def tasks(self):
        async with native_context(self.api_url) as client:
            runs=await pages(client,FlowRunFilter(tags={'all_':['paperflow-v3-task']}))
            return {'tasks':[{'task_id':str(run.id),'task_name':run.parameters.get('task_name',run.name)} for run in runs],
                    'as_of':datetime.now(timezone.utc).isoformat(),'complete':True}
