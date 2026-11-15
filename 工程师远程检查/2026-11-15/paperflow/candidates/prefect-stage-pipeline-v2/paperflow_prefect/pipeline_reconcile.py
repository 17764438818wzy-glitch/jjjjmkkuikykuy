"""Reconcile only registered scopes. Unknown sends never enter unsent requeue."""
from dataclasses import replace
import os
import time
import psutil
from filelock import Timeout
from .pipeline_contracts import StageEnvelope, ready_stages
from .pipeline_dispatch import configured_coordinator, SubmissionUncertain, _slot
from .pipeline_ledger import OperationBusy, EvidenceChanged, OutcomeUnknown
from .records import read_json, write_json


def _resolve_unknown(coordinator,env,status):
    """Local proof first, provider readback second; neither hook may send.

    Task5/7 install format/provider validators. Absent validators deliberately
    leave unknown. StageLedger verifies any returned original reservation proof.
    """
    coordinator.ledger.mark_orphan_unknown(env)
    for verifier in (coordinator.local_outcome,coordinator.remote_outcome):
        if verifier is None: continue
        result=verifier(env,status)
        if result is not None:
            owner=dict(instance_id=coordinator.instance_id,pid=os.getpid(),
                process_create_time=psutil.Process().create_time(),task_run_id='reconcile-'+env.operation_key)
            coordinator.ledger.reconcile_result(env,result,owner)
            return 'reconciled'
    return 'unknown'


def _current(coordinator,env):
    ledger=coordinator.ledger; status=ledger.dispatch_status(env)
    record=status['record']
    if record.get('dispatch_seq',env.dispatch_seq)>env.dispatch_seq:
        # Crash between the atomic operation fence and registration pointer save.
        env=replace(env,dispatch_seq=record['dispatch_seq']); coordinator._save_envelope(env)
    if status['result'] is not None:
        intent=coordinator.intent(env)
        if intent and intent['phase']!='confirmed':
            runs=coordinator.gateway.find(env)
            if runs:
                intent.update(phase='confirmed',task_run_ids=sorted(r['id'] for r in runs))
                write_json(coordinator.intent_path(env),intent)
        return 'complete'
    if status['live']: return 'owned'
    if status['sent'] or record['state']=='unknown': return _resolve_unknown(coordinator,env,status)
    intent=coordinator.intent(env)
    if intent is None:
        if record['state']!='pending': return 'unresolved_without_intent'
        coordinator.dispatch_ready(env); return 'submitted'
    runs=coordinator.gateway.find(env)
    if not runs:
        if intent['phase']!='prepared': raise SubmissionUncertain('accepted outcome cannot be excluded')
        coordinator.dispatch_ready(env); return 'submitted'
    # The accepted-before-local-ID breakpoint is repaired without another submit.
    intent.update(phase='confirmed',task_run_ids=sorted(r['id'] for r in runs))
    write_json(coordinator.intent_path(env),intent)
    if any(r['state'] not in {'SCHEDULED','PENDING','RUNNING'} for r in runs):
        return 'native_terminal_without_business_result'
    def fresh(run):
        if run['state']=='SCHEDULED': return time.time()-run['created']<=120
        # Pending/Running without a recorded, demonstrably dead owner is not enough.
        return not record.get('owner')
    if any(fresh(r) for r in runs): return 'waiting'
    availability=coordinator.gateway.availability(env,intent['task_run_ids'])
    if not availability['certain'] or availability['owned'] or not availability['idle']:
        return 'worker_not_proven_idle'
    with coordinator._lock('handoff',env.operation_key):
        coordinator.fence(env)
        # Requery under dispatch lock; any change/unknown result stops requeue.
        current=coordinator.gateway.find(env)
        if {r['id'] for r in current}!={r['id'] for r in runs} or any(
                r['state'] not in {'SCHEDULED','PENDING','RUNNING'} or fresh(r) for r in current):
            return 'native_changed'
        availability=coordinator.gateway.availability(env,intent['task_run_ids'])
        if not availability['certain'] or availability['owned'] or not availability['idle']:
            return 'worker_changed'
        new=ledger.advance_unsent_dispatch(env)
        coordinator._save_envelope(new)
    coordinator.dispatch_ready(new)
    return 'requeued_unsent'


def reconcile_registered(coordinator,batch_id):
    report=dict(batch_id=batch_id,operations={},errors=[])
    registration=coordinator._read_scope(batch_id)
    for env in coordinator.envelopes(batch_id):
        try: report['operations'][env.operation_key]=_current(coordinator,env)
        except (OperationBusy,Timeout): report['operations'][env.operation_key]='owned_or_reconciling'
        except Exception as exc:
            # Store safe error types only; provider errors may contain sensitive payloads.
            report['errors'].append(dict(operation_key=env.operation_key,error_type=type(exc).__name__))
    registration=coordinator._read_scope(batch_id)
    for paper in registration['papers']:
        try:
            snapshot,results=coordinator._snapshot(registration,paper)
            for stage in ready_stages(snapshot):
                items=([r['file_id'] for r in paper['paper']['attachments']]
                    if stage in {'attachment','rag_attachment'} and paper['paper']['attachments'] else [None])
                for item in items:
                    if not coordinator._eligible(snapshot,stage,item) or _slot(stage,item) in paper['operations']: continue
                    env=coordinator._build(registration,paper,stage,item,results)
                    coordinator._save_envelope(env)
                    try:
                        status=coordinator.ledger.dispatch_status(env)
                        if status['result'] is None: coordinator.dispatch_ready(env)
                        report['operations'][env.operation_key]='adopted' if status['result'] else 'submitted'
                    except Exception as exc:
                        report['errors'].append(dict(operation_key=env.operation_key,error_type=type(exc).__name__))
        except (OperationBusy,Timeout): pass
        except Exception as exc: report['errors'].append(dict(paper_id=paper['paper_id'],error_type=type(exc).__name__))
    write_json(coordinator.ledger.root/'reconciliations'/(coordinator._scope_path(batch_id).name),report)
    return report


async def reconcile_scope(batch_id: str) -> dict:
    return await configured_coordinator().reconcile_scope(batch_id)
