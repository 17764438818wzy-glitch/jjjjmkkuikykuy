"""Narrow native-ingest terminal evidence validator. Pure; never disposes a document.

Policy is installed by a trusted manager review, not supplied in worker evidence.
The evidence hash binds a collected bundle; it is not proof of its own truth.
No default production policy, guessed timeout or generic HTTP-400 exemption.
"""
import hashlib
import json
import math

from .handoff_store import canonical


def _require(condition, reason):
    if not condition: raise ValueError(reason)


def _number(value):
    return type(value) in (int,float) and math.isfinite(value)


def _sha(value):
    return isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value)


def _name(value):
    return isinstance(value,str) and bool(value.strip())


def _names(value):
    return (isinstance(value,list) and bool(value) and all(_name(item) for item in value)
            and len(set(value))==len(value))


def _ids(rows, field='id'):
    _require(isinstance(rows,list),'coverage list missing')
    result=[row[field] for row in rows]
    _require(all(_name(value) for value in result) and len(set(result))==len(result),
             'coverage identity missing or duplicate')
    return set(result)


def validate(action, execution, identity, evidence, policy, controller, first_window, last_window):
    """Return a bounded non-secret decision; original reconciliation owns the SQL."""
    try:
        _validate(action,execution,identity,evidence,policy,controller,first_window,last_window)
        return True,'native task termination verified'
    except (ValueError,TypeError,KeyError,IndexError,AttributeError,OverflowError):
        return False,'native task terminal evidence incomplete or mismatched'


def _validate(action,execution,identity,evidence,policy,controller,first_window,last_window):
    _require(isinstance(policy,dict) and policy.get('type')=='native_task_terminal_v1','trusted policy absent')
    proof=evidence.get('terminal_proofs',{}).get(action['action_id'])
    _require(isinstance(proof,dict) and set(proof)==
             {'type','binding','deployment','manager','observations','evidence_sha256'},'unknown proof schema')
    _require(proof['type']=='native_task_terminal_v1','wrong terminal protocol')
    unsigned={k:v for k,v in proof.items() if k!='evidence_sha256'}
    _require(_sha(proof['evidence_sha256']) and
             hashlib.sha256(canonical(unsigned).encode()).hexdigest()==proof['evidence_sha256'],'proof digest changed')
    _require(isinstance(policy.get('approved_evidence'),dict) and
             policy['approved_evidence'].get(action['action_id'])==proof['evidence_sha256'],
             'exact evidence bundle has no trusted manager approval')
    resource=json.loads(action['request'])['resource']; bound=resource['identity']
    expected={k:bound[k] for k in ('source_id','task_id','manifest_sha256','specialty_code')}
    expected.update(action_id=action['action_id'],execution_id=action['execution_id'],
        dataset_id=resource['dataset_id'],document_id=resource['document_id'],
        request_sha256=resource['body_sha256'],write_operation='native_ingest',pipeline_id=resource['pipeline_id'])
    _require(action['kind']=='rag_write' and resource.get('write_operation')=='native_ingest' and
             proof['binding']==expected,'not the exact native write')
    _require(expected['source_id']==identity['source_id'] and expected['specialty_code']==identity['specialty_code'],
             'wrong specialty owner')
    _require(policy['pipeline_id']==expected['pipeline_id'] and policy['endpoint']==evidence['endpoint'] and
             policy['configuration']==evidence['configuration'],'deployment route changed')
    deployment=policy['deployment']
    _require(set(deployment)=={'source_sha256','configuration_sha256','task_executor_sha256'} and
             all(_sha(v) for v in deployment.values()) and proof['deployment']==deployment,'deployment source changed')
    _require(controller is not None and proof['manager']==dict(epoch=evidence['clock_epoch'],
             generation=identity['generation'],control_generation=controller['generation']) and
             controller['epoch']==evidence['clock_epoch'],'manager epoch or generation changed')
    exit_fact=json.loads(execution['exit_evidence'] or '{}')
    _require(execution['state']=='exited' and exit_fact.get('all_recorded_processes_exited') is True and
             _number(exit_fact.get('confirmed_at')),'sender still possible')
    last_send=(exit_fact.get('confirmed_monotonic') if action['epoch']==evidence['clock_epoch']
               else evidence['epoch_started'])
    _require(_number(last_send),'sender monotonic anchor missing')
    contract=policy['done_contract']
    _require(contract.get('after_last_awaited_side_effect') is True and
             contract.get('after_current_tasks_removal') is True and _name(contract.get('id')) and
             contract.get('source_path')=='rag/svr/task_executor.py' and
             contract['source_sha256']==deployment['task_executor_sha256'],'done source contract unverified')
    coverage=policy['coverage']
    for name in ('producers','workers','side_effects'):
        values=coverage[name]
        _require(_names(values),'coverage not explicit')
    groups=coverage['stream_groups']
    _require(isinstance(groups,list) and groups and all(set(g)=={'stream','group','consumers'} and
             _name(g['stream']) and _name(g['group']) and _names(g['consumers'])
             for g in groups),'queue consumer coverage absent')
    _require(len({(g['stream'],g['group']) for g in groups})==len(groups),'duplicate queue coverage')
    _require(_names(policy['allowed_task_types']),'task type coverage absent')
    progress=policy['allowed_terminal_progress']
    _require(isinstance(progress,list) and progress and
             all(type(item) is int and item in (-1,1) for item in progress) and
             len(set(progress))==len(progress),'terminal progress rule invalid')
    visibility=policy['visibility']
    _require(_name(visibility['contract']) and _sha(visibility['evidence_sha256']) and
             _number(visibility['minimum_seconds']) and 0<=visibility['minimum_seconds']<86400,
             'visibility contract absent')
    _require(last_window['started']-first_window['finished']>=visibility['minimum_seconds'],
             'visibility window incomplete')
    age_limit=policy['max_observation_age_seconds']; heartbeat_limit=policy['max_worker_heartbeat_age_seconds']
    _require(_number(age_limit) and 0<age_limit<=3600 and _number(heartbeat_limit) and
             0<heartbeat_limit<=age_limit,'invalid freshness rule')
    observations=proof['observations']
    _require(isinstance(observations,list) and len(observations)==2,'two terminal observations required')
    previous_time=last_send; previous_tasks=None
    for ordinal,observation in enumerate(observations):
        collected=observation['collected_monotonic']
        _require(_number(collected) and previous_time<=collected<=first_window['started'] and
                 (ordinal==0 or previous_time<collected) and
                 0<=last_window['finished']-collected<=age_limit,'observation clock stale')
        previous_time=collected
        tasks=observation['tasks']; ids=_ids(tasks)
        _require(bool(ids),'all document task records required')
        for task in tasks:
            _require(task['doc_id']==expected['document_id'] and task['task_type'] in policy['allowed_task_types'] and
                _number(task['progress']) and task['progress'] in policy['allowed_terminal_progress'] and
                task['progress'] in (-1,1) and type(task['retry_count']) is int and task['retry_count']>=0 and
                _number(task['update_time']),'task not terminal or wrong document')
        current_tasks=sorted(tasks,key=lambda row:row['id'])
        _require(previous_tasks is None or previous_tasks==current_tasks,'late task/retry changed')
        previous_tasks=current_tasks
        receipts=observation['done_receipts'];_require(_ids(receipts)==ids,'done coverage mismatch')
        by_id={task['id']:task for task in tasks}
        for receipt in receipts:
            task=by_id[receipt['id']]
            _require(all(receipt.get(key)==task[key] for key in ('id','doc_id','retry_count','update_time')) and
                receipt['contract']==contract['id'] and receipt['source_sha256']==contract['source_sha256'] and
                _sha(receipt['log_sha256']) and receipt.get('last_awaited_side_effect_finished') is True and
                receipt.get('current_task_removed') is True,'done receipt mismatch')
        producers=observation['producers'];_require(_ids(producers)==set(coverage['producers']),'producer coverage incomplete')
        for producer in producers:
            _require(producer['request_sha256']==expected['request_sha256'] and
                     set(producer['task_ids'])==ids and producer.get('no_future_enqueue') is True,
                     'producer can still enqueue')
        queues=observation['stream_groups']
        _require(len(queues)==len(groups) and sorted([{k:q[k] for k in ('stream','group','consumers')} for q in queues],
                 key=canonical)==sorted(groups,key=canonical),'stream/group/consumer set changed')
        _require(all(q.get('pel_task_ids')==[] and q.get('undelivered_task_ids')==[] for q in queues),'queue not empty')
        workers=observation['workers'];_require(_ids(workers)==set(coverage['workers']),'worker coverage incomplete')
        _require(all(w.get('current_task_ids')==[] and _number(w.get('heartbeat_age_seconds')) and
                 0<=w['heartbeat_age_seconds']<=heartbeat_limit for w in workers),'worker busy or stale')
        effects=observation['side_effects'];_require(_ids(effects)==set(coverage['side_effects']),'side effect coverage incomplete')
        _require(all(e.get('pending_task_ids')==[] and set(e['completed_task_ids'])==ids for e in effects),
                 'asynchronous side effect still possible')
        seen=observation['visibility']
        _require(seen.get('contract')==visibility['contract'] and seen.get('evidence_sha256')==visibility['evidence_sha256'] and
                 set(seen['acknowledged_task_ids'])==ids,'backend visibility not bounded')
