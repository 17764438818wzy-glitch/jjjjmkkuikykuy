"""Manager-owned native single-request worker. Never launches ProcessingWorkflow."""
import json
import os
from pathlib import Path
import sys
import uuid


def request_attempt_limit(config):
    # Existing UI calls this max_retries but defines it as total, including first.
    return max(1,int(config['concurrency']['max_retries']))


def launch_prepared(manager,config,task_id,*,schedule_attempt):
    adapter=manager.prepared
    descriptor=adapter.descriptor(task_id,schedule_attempt)
    path=manager.root/'prepared-descriptors'/(schedule_attempt+'.json')
    path.parent.mkdir(exist_ok=True)
    from .prepared_store import _durable_json
    _durable_json(path,descriptor,exclusive=True)
    root=Path(__file__).resolve().parents[2]
    base=Path(os.environ.get('PAPERFLOW_BASE_ROOT',str(root)))
    return manager.launch(descriptor['kind'],task_id,[sys.executable,'-m','paperflow.prepared_worker',str(path)],
        cwd=root/'core',schedule_attempt=schedule_attempt,total_timeout=86400,
        env=dict(PYTHONPATH=os.pathsep.join([str(root/'core'),str(root/'desktop'),str(base/'desktop'),str(base/'core'),*sys.path,os.environ.get('PYTHONPATH','')]),
            PAPER_UPSTREAM_DATA_DIR=descriptor['data_root'],PYTHONDONTWRITEBYTECODE='1',
            PAPERFLOW_GLOBAL_STAGE_TOKEN=schedule_attempt))


def main(path):
    from .control_ipc import WorkerClient
    from .prepared_store import PreparedRegistry
    from .prepared_protocol import PreparedObservation
    from .store import Store
    from .scheduling_policy import fault,ActionDeferred
    data=json.loads(Path(path).read_text(encoding='utf8'))
    client=WorkerClient.from_environment()
    result=None;outcome='held';control=None
    try:
        registry=PreparedRegistry(Store(data['database']))
        from auto_config import load_config,load_key
        from auto_pipeline import AutoProcessor
        from engine import Store as DesktopStore
        from preextract_request_control import RequestControl
        from preextract_scheduler_adapter import body_permit,observe_prepared
        root=Path(data['data_root']);settings=Path(data['settings_path'])
        config=load_config(settings)
        processor=AutoProcessor(DesktopStore(root),config,load_key(settings),load_key(settings,'vision'),load_key(settings,'vision_fallback'))
        processor.agent.billing_context=dict(run_id='PREPARED_SCOPE_'+data['scope_id'],item_id=data['task_id'])
        processor.stop=client.local_cancel;processor.agent.stop=processor.stop
        record=data['record'];observation=PreparedObservation.from_dict(data['observation'])
        control=RequestControl(client,registry,data['scope_id'],data['generation'],data['task_id'],record,observation,
            purpose_config=config.get('purpose_review',{}),limit=request_attempt_limit(config),timeout=float(config.get('timeout',1800)))
        processor._prepared_request_control=control
        registry.assert_running(data['scope_id'],data['generation'])
        if data['step']=='purpose':
            from purpose_review import review,scope
            with scope(processor):role=review(record,processor,Path(observation.evidence['work_dir']))
            outcome='complete';result=dict(purpose=role.get('action'))
        else:
            from preextract_scope_guard import retired_delivery
            member=next(m for m in registry.members(data['scope_id']) if m['task_id']==data['task_id'])
            retirement=retired_delivery(registry,data['scope_id'],member) if data['step']=='cleanup' else None
            if not retirement and not body_permit(record,observation,config.get('purpose_review',{}))['allowed']:
                raise ValueError('confirmed body purpose required')
            if data['step']=='cleanup':
                from preextract_prepared_delivery import cleanup_prepared
                result=client.perform('local',dict(operation='prepared_cleanup'),
                    lambda cancel:cleanup_prepared(data,registry,processor,client.local_cancel),total_timeout=600)
            elif data['step']=='delivery':
                from preextract_prepared_delivery import deliver_prepared
                result=deliver_prepared(data,registry,client,processor)
            elif data['step']=='register':
                from preextract_prepared_delivery import register_prepared
                result=client.perform('local',dict(operation='prepared_register'),
                    lambda cancel:register_prepared(data,registry,processor),total_timeout=600)
            else:
                from preextract_scheduler_recovery import run_prepared
                def execute():return run_prepared(record,decision=data,stop=client.local_cancel,progress=lambda *_:None,processor=processor,control=control)
                if data['step'] in ('adopt','postprocess','repair_local'):
                    saved=[]
                    def local(cancel):
                        saved.append(execute());return dict(stage=saved[0].get('stage'))
                    client.perform('local',dict(operation='prepared_postprocess'),local,total_timeout=3600)
                    result=saved[0]
                else:result=execute()
            outcome='complete'
    except BaseException as exc:
        from preextract_scheduler_recovery import execution_fault
        failure=execution_fault(exc)
        outcome='stopped' if client.local_cancel.is_set() else 'deferred' if failure.get('category')=='deferred' else 'failed'
        result=dict(error=type(exc).__name__,fault=failure)
    finally:
        try:
            if control:
                try:
                    control.reconcile_requests()
                    task=registry.store.get(data['task_id'])
                    if task.get('prepared_checkpoint'):record=dict(record,prepared_checkpoint=task['prepared_checkpoint'])
                    from preextract_scope_guard import retired_delivery
                    from dataclasses import replace
                    member=next(m for m in registry.members(data['scope_id']) if m['task_id']==data['task_id'])
                    retired=retired_delivery(registry,data['scope_id'],member)
                    observed=(replace(observation,phase='cleaned' if retired['status']=='complete' else 'delivery_verified',
                        next_step='none' if retired['status']=='complete' else 'cleanup',blocking_reason=None)
                        if retired else observe_prepared(record,data_root=root,now=0))
                    if observed.identity==observation.identity:registry.refresh_observation(data['scope_id'],observed)
                except (OSError,ValueError,KeyError) as exc:
                    outcome='held';result=dict(reconcile_error=type(exc).__name__)
            result_path=Path(path).with_suffix('.result.json')
            from .prepared_store import _durable_json
            _durable_json(result_path,result)
            report=dict(result_path=str(result_path))
            if isinstance(result,dict) and result.get('fault'):report['fault']=result['fault']
            client.report_execution(outcome,report)
        finally:client.close()


if __name__=='__main__':main(sys.argv[1])
