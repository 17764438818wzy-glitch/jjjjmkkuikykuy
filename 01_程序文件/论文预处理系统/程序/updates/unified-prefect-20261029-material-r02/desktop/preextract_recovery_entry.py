"""Hand off only this just-finished batch's prepared failures to the native scope."""
import copy
from pathlib import Path


def recover_managed_local(processor,record,*,expected_binding,progress=lambda _:None):
    """Explicit single-member local recovery; never resume a scope or batch."""
    from preextract_local_recovery import recover
    return recover(processor,record,expected_binding=expected_binding,progress=progress)


def recover_failed_batch(processor,parent,callback,operation):
    if (operation!='process' or getattr(processor,'_preserve_stop',False)
            or getattr(processor,'_prepared_request_control',None) is not None
            or processor.stop.is_set() or parent.get('status') in ('stopped','interrupted')
            or processor.config.get('prepared_dispatch_enabled')
            or not processor.config.get('auto_upload',True)):
        return parent
    from ai_resume import retry_records
    items=[dict(item,_ai_run_status=parent.get('status')) for item in parent.get('items',[])
        if not item.get('automatic_approval') and item.get('status')!='deleted'
        and (item.get('prepared_checkpoint') or {}).get('format')=='paper-preextract-prepared/1']
    selected=retry_records(items)
    if not selected:return parent
    from preextract_coordinator import persisted_scope,freeze_records,run_scope
    existing=persisted_scope(processor.store.root)
    if existing and (existing.get('intent')!='completed' or existing.get('stop_pending')):
        parent['prepared_recovery']=dict(status='waiting_existing_scope',scope_id=existing['scope_id'],parent_run_id=parent['run_id'])
        processor.store.save(parent);callback(copy.deepcopy(parent));return parent
    # Bind the relationship before dispatch. No history scan or source expansion.
    from paperflow.prepared_store import scope_manifest,task_key
    try:
        rows=freeze_records(selected,processor.store.root,processor.config)
        manifest_sha=scope_manifest(rows);scope_id=manifest_sha[:32]
        parent['prepared_recovery']=dict(parent_run_id=parent['run_id'],scope_id=scope_id,
            manifest_sha256=manifest_sha,status='registered')
        indexed={str(Path(row['record']['path']).resolve()):(row,task_key(row['observation']['identity'],row['observation']['context_sha256'])) for row in rows}
        for item in parent.get('items',[]):
            found=indexed.get(str(Path(item['source_path']).resolve())) if item.get('source_path') else None
            if found and item.get('prepared_checkpoint')==found[0]['record'].get('prepared_checkpoint'):
                item['recovery_reference']=dict(scope_id=scope_id,task_id=found[1],parent_item_id=item.get('id'),
                    checkpoint_sha256=item['prepared_checkpoint']['sha256'])
        processor.store.save(parent);callback(copy.deepcopy(parent))
        if processor.stop.is_set():return parent
        result=run_scope(processor,selected,callback,expected_manifest=manifest_sha)
        parent['prepared_recovery'].update(status=result['status'],run_id=result['run_id'])
    except (OSError,ValueError,KeyError) as exc:
        # Original batch results stay intact if control/evidence needs attention.
        parent.setdefault('prepared_recovery',dict(parent_run_id=parent['run_id'])).update(
            status='needs_review',reason=type(exc).__name__)
    processor.store.save(parent);callback(copy.deepcopy(parent));return parent
