"""Narrow recovery decisions over the native append-only attempt interfaces."""
from pathlib import Path
import json
import uuid
from preextract_material import file_sha
from preextract_request_control import adopt_saved_response


def request_fault(evidence):
    status=evidence.get('http_status')
    if status in (401,403):return dict(category='configuration',status=status,family='authentication',retry_after=0)
    return dict(category='transient',status=status,family='rate_limit' if status==429 else 'unavailable',retry_after=evidence.get('retry_after',0))


def execution_fault(exc):
    from filelock import Timeout
    from paperflow.scheduling_policy import fault
    if isinstance(exc,Timeout) or (isinstance(exc,PermissionError) and getattr(exc,'winerror',None) in (32,33)):
        return dict(category='deferred',status=None,reason='local_file_lock',family='local_file_lock',retry_after=2)
    return fault(exc)


def resume_decision(observation,*,policy=None,now=None):
    return dict(action=observation.next_step,work_dir=observation.evidence.get('work_dir'),
                reason=observation.blocking_reason,identity=observation.as_dict()['identity'])


def recover_workspace(record,observation,registry,scope_id,generation,task_id):
    registry.assert_running(scope_id,generation)
    work=Path(observation.evidence['work_dir'])
    step=observation.next_step
    if step in ('review','reconcile','none'):raise ValueError('material is not eligible for automatic recovery')
    if step in ('adopt_saved_response','adopt_parts'):
        children=observation.evidence.get('children')
        if children:
            for row in children:
                if row['next_step']=='adopt_saved_response':adopt_saved_response(row['directory'])
        else:adopt_saved_response(work/'model')
    if step=='recover':
        from preextract_pipeline import create_revision
        state=json.loads((work/'model/request-state.json').read_text(encoding='utf8'))
        marker=dict(schema='prepared-scope-binding/1',database=str(registry.store.path),scope_id=scope_id,generation=generation,task_id=task_id)
        not_sent=None
        if not (work/'model/request.json').exists():
            from preextract_request_control import not_sent_proof
            not_sent=not_sent_proof(registry,task_id,work/'model')
        work=create_revision(record['path'],work,'prepared-'+uuid.uuid4().hex[:20],mode='rerun',
            reason='frozen scope automatic recovery of explicitly ended attempt',
            recover_failed=state['status']=='request_failed_or_uncertain',managed_scope=marker,not_sent_proof=not_sent)
        checkpoint=dict(format='paper-preextract-prepared/1',path=str(work/'material.json'),sha256=file_sha(work/'material.json'))
        # A failed pointer write leaves a harmless unselected revision, never a sent request.
        registry.store.update(task_id,prepared_checkpoint=checkpoint)
        record=dict(record,prepared_checkpoint=checkpoint)
    elif step=='recover_parts':
        from preextract_partition_run import create_partition_revision
        for child in observation.evidence.get('children',[]):
            if child['next_step']=='recover':
                registry.assert_running(scope_id,generation)
                state=json.loads((Path(child['directory'])/'request-state.json').read_text(encoding='utf8'))
                create_partition_revision(record['path'],work,'prepared-'+uuid.uuid4().hex[:20],child['id'],
                    reason='frozen scope recovery of this ended child only',recover_failed=state['status']=='request_failed_or_uncertain',
                    managed_scope=dict(schema='prepared-scope-binding/1',database=str(registry.store.path),scope_id=scope_id,generation=generation,task_id=task_id))
    return record,work


class ModelSaved(Exception):
    """A native response is durable; release this execution before local work."""


def run_prepared(record,*,decision,stop,progress,processor,control):
    from preextract_pipeline import run_single,DEFAULT_TEMPLATE,DEFAULT_RUNTIME,DEFAULT_PAPERFLOW
    from preextract_workbench import invoke_once,model_connection
    registry=control.registry
    local_only=decision.get('local_only') is True
    if local_only and (decision.get('step')!='postprocess' or control.observation.next_step not in ('postprocess','adopt_saved_response','adopt_parts')):
        raise ValueError('本地恢复禁止模型、交付或未决请求步骤')
    record,work=recover_workspace(record,control.observation,registry,control.scope_id,control.generation,control.task_id)
    from preextract_scheduler_adapter import observe_prepared
    control.record=record
    control.observation=observe_prepared(record,data_root=processor.store.root,now=0)
    if decision['step']=='adopt':return dict(stage='response_adopted',work_dir=str(work))
    processor._prepared_request_control=control
    if decision['step'] in ('repair_local','repair_model'):
        from preextract_content_recovery import run_repair
        return run_repair(record,work,processor=processor,control=control,model=decision['step']=='repair_model')
    def guard():
        registry.assert_running(control.scope_id,control.generation)
        if stop.is_set():raise InterruptedError('scope stopped')
    def no_model(*args,**kwargs):
        raise ValueError('本地恢复禁止模型请求')
    def saved():
        if decision['step']=='model':raise ModelSaved()
        guard()
    guard()
    try:
        return run_single(record['path'],work_dir=work,
            template_path=record.get('template_path') or processor.config.get('preextract_template') or DEFAULT_TEMPLATE,
            runtime_dir=processor.config.get('preextract_runtime_dir') or DEFAULT_RUNTIME,
            paperflow_root=processor.config.get('preextract_paperflow_root') or DEFAULT_PAPERFLOW,
            stage='package',stop=stop,progress=progress,connection=no_model if local_only else lambda:model_connection(processor),
            invoke=no_model if local_only else lambda body,connection,directory,**kw:invoke_once(processor,body,connection,directory,**kw),
            before_model=no_model if local_only else guard,before_postprocess=saved,
            managed_scope=control.marker,associated_document=record,**(dict(local_only=True) if local_only else {}))
    except ModelSaved:return dict(stage='response_saved',work_dir=str(work))
