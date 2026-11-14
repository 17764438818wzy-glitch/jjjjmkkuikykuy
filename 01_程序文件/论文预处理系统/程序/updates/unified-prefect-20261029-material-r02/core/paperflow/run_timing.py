"""Report program preparation and runtime separately from external test/audit work."""
import json
import math
from .instrumentation import best_effort,interruption_account
from .timing_artifacts import read_details


def job_belongs(row, task, packages):
    """Versions share source_id; execution attribution must use immutable task identity."""
    if row['kind'] in ('graph','repair'):
        return row['unit_key']==task['task_id']
    if row['kind']!='delivery':
        return False
    payload=json.loads(row.get('payload',row.get('data','{}')))
    if payload.get('attachment_id'):
        return payload['attachment_id']==task.get('attachment_id')
    return packages.get(payload.get('manifest_sha256'))==task['task_id']


def run_timing(scheduler, started, admitted, finished):
    attempts=scheduler.ledger.rows('SELECT a.*,j.kind,j.unit_key,j.article,j.created AS queued_at,j.data AS payload,history.first_started,e.epoch AS execution_epoch '
        'FROM scheduler_attempts a JOIN scheduler_jobs j ON j.job_id=a.job_id '
        'LEFT JOIN control_executions e ON e.execution_id=a.execution_id '
        'JOIN (SELECT job_id,MIN(started) AS first_started FROM scheduler_attempts GROUP BY job_id) history '
        'ON history.job_id=a.job_id WHERE a.started>=?',(admitted or finished,))
    seals={}
    for action in scheduler.ledger.rows('''SELECT a.execution_id,a.state,a.result FROM control_actions a
        JOIN scheduler_attempts s ON s.execution_id=a.execution_id WHERE s.started>=? AND a.kind='local'
        AND json_extract(a.request,'$.resource.operation') IN ('seal','seal_attachment')''',(admitted or finished,)):
        fact=json.loads(action['result'] or '{}')
        measured=(action['state']=='complete' and fact.get('completed') is True
            and all(type(fact.get(k)) in (int,float) and math.isfinite(fact[k]) and fact[k]>=0
                    for k in ('elapsed_seconds','admission_seconds')))
        seals.setdefault(action['execution_id'],[]).append(fact if measured else None)
    packages={r['manifest_sha256']:r['task_id'] for r in scheduler.ledger.rows('SELECT manifest_sha256,task_id FROM delivery_packages')}
    articles=[];tasks=scheduler.manager.store.all_tasks()
    for task in tasks:
        rows=[r for r in attempts if job_belongs(r,task,packages)]
        if not rows:continue
        intervals=[(r['started'],r['finished'] or finished) for r in rows]
        work={kind:sum(max(0,(r['finished'] or finished)-r['started']) for r in rows if r['kind']==kind)
              for kind in ('graph','repair','delivery')}
        packaging=[fact for row in rows for fact in seals.get(row['execution_id'],[])]
        measured=bool(packaging) and all(fact is not None for fact in packaging)
        first_delivery={row['job_id']:(row['queued_at'],row['first_started'])
                        for row in rows if row['kind']=='delivery'}
        queue=sum(max(0,start-created) for created,start in first_delivery.values())
        articles.append(dict(task_id=task['task_id'],stage=task['stage'],
            started_at=min(a for a,b in intervals),finished_at=max(b for a,b in intervals),
            elapsed_seconds=max(b for a,b in intervals)-min(a for a,b in intervals),
            processing_seconds=work['graph']+work['repair'],delivery_seconds=work['delivery'],
            delivery_queue_seconds=queue,packaging_timing_complete=measured,
            packaging_seconds=sum(f['elapsed_seconds'] for f in packaging) if measured else None,
            packaging_admission_seconds=sum(f['admission_seconds'] for f in packaging) if measured else None))
    measurement=measurement_report(scheduler,attempts,admitted or finished,packages=packages,tasks=tasks)
    return dict(started_at=started,runtime_started_at=admitted,finished_at=finished,measurement=measurement,
        preparation_seconds=(admitted or finished)-started,runtime_seconds=finished-admitted if admitted else 0,
        extra_audit_seconds=None,articles=articles,
        note='Runtime includes directory idle/stability/queue waits; per-article work uses actual attempts. '
             'Processing includes packaging; packaging work and its admission wait are also shown separately when measured. '
             'Delivery includes necessary readback/index/activation confirmation; times across specialties are additive. '
             'Initial delivery queue time uses each job\'s first attempt over its full history, not a later retry after restart. '
             'Developer/test preparation and additional audits are not included.')


def measurement_report(scheduler,attempts,admitted,*,packages=None,tasks=()):
    """Reuse immutable attempt identity and existing action/result JSON channels."""
    action_rows=scheduler.ledger.rows('SELECT a.action_id,a.execution_id,a.kind,a.result FROM control_actions a '
        'JOIN scheduler_attempts s ON s.execution_id=a.execution_id WHERE s.started>=? ORDER BY a.rowid',(admitted,))
    actions={}
    for row in action_rows:
        result=json.loads(row['result'] or '{}');timing=result.get('_timing') if isinstance(result,dict) else None
        actions.setdefault(row['execution_id'],[]).append(dict(action_id=row['action_id'],kind=row['kind'],timing=timing,
            metrics_complete=bool(timing and timing.get('metrics_complete'))))
    result=[];round1=[];slots=None
    for row in attempts:
        data=json.loads(row.get('data') or '{}');worker=data.get('_worker_timing') or data.get('_timing') or {}
        detail_status=worker.get('details_status','inline_legacy')
        artifact_failed_stage=None
        if worker.get('details_ref') is not None:
            payload=json.loads(row.get('payload') or '{}')
            task_id=row['unit_key'] if row['kind'] in ('graph','repair') else (packages or {}).get(payload.get('manifest_sha256'))
            if payload.get('attachment_id'):
                task_id=next((task['task_id'] for task in tasks if task.get('attachment_id')==payload['attachment_id']),None)
            expected=dict(epoch=row.get('execution_epoch'),execution_id=row.get('execution_id'),
                attempt_id=row['attempt_id'],job_id=row['job_id'],unit_key=row['unit_key'],task_id=task_id,
                execution_kind='graph' if row['kind'] in ('graph','repair') else row['kind'])
            expected.update({key:payload[key] for key in ('manifest_sha256','specialty_code') if key in payload})
            config=getattr(scheduler,'config',None)
            loaded=best_effort(lambda:read_details(config.root,worker,expected))
            detail_status=loaded['detail_status'] if loaded else 'reader_failed'
            if loaded and loaded.get('timing') is not None:
                worker=loaded['timing'];artifact_failed_stage=loaded.get('failed_stage_timing')
            else:worker=dict(worker,metrics_complete=False)
        facts=actions.get(row.get('execution_id'),[])
        ledgers={}
        def stage_key(stage):
            return stage.get('stage_invocation_id') or (stage.get('stage'),stage.get('input_fingerprint'))
        for action in facts:
            snapshot=(action.get('timing') or {}).get('stage_snapshot')
            if isinstance(snapshot,dict):ledgers[stage_key(snapshot)]=snapshot
        for stage in worker.get('stages') or []:ledgers[stage_key(stage)]=stage
        failed_stage=artifact_failed_stage or data.get('_failed_stage_timing')
        if isinstance(failed_stage,dict):ledgers[stage_key(failed_stage)]=failed_stage
        stages=list(ledgers.values())
        for stage in stages:
            round1.append(dict(stage,task_id=row['unit_key'],attempt_id=row['attempt_id'],
                input_fingerprint=stage.get('input_fingerprint'),outcome=stage.get('outcome') or row.get('outcome')))
        llm=[x['timing'] for x in facts if x['kind']=='llm']
        llm_valid=bool(llm) and all(x and type(x.get('permit_wait_seconds')) in (int,float)
            and type(x.get('client_function_seconds')) in (int,float) for x in llm)
        result.append(dict(attempt_id=row['attempt_id'],execution_id=row.get('execution_id'),kind=row['kind'],
            task_or_unit=row['unit_key'],outcome=row.get('outcome'),right_censored=row.get('finished') is None,
            timing_detail_status=detail_status,
            clock_id=worker.get('clock_id'),epoch=worker.get('epoch'),
            graph_elapsed_seconds=worker.get('graph_elapsed_seconds'),five_stage_seconds=worker.get('five_stage_seconds'),
            five_stage_elapsed_seconds=worker.get('five_stage_elapsed_seconds'),
            five_stage_elapsed_complete=worker.get('five_stage_elapsed_complete') is True,
            business_seconds=worker.get('business_seconds'),business_view_additive_to_attempt=False,
            worker_llm_call_seconds=sum(x['permit_wait_seconds']+x['client_function_seconds'] for x in llm) if llm_valid else None,
            stages=stages,actions=facts,manager_permits=data.get('_manager_permits'),
            metrics_complete=bool(worker.get('metrics_complete')) and bool(facts) and all(x['metrics_complete'] for x in facts)
                and all(stage.get('metrics_complete') is True for stage in stages)))
        if data.get('_scheduler_timing'):slots=data['_scheduler_timing']
    slots=best_effort(scheduler.slot_snapshot) if hasattr(scheduler,'slot_snapshot') else slots
    slot_complete=isinstance(slots,dict) and all(isinstance(slots.get(layer),dict) and slots[layer].get('metrics_complete') is True
        for layer in ('article_slots','llm_permits','harness_actors'))
    return dict(metrics_complete=bool(result) and all(x['metrics_complete'] for x in result) and slot_complete,
        attempts=result,slot_metrics=slots or dict(metrics_complete=False,reason='slot_events_not_recorded'),
        round1_interruption=interruption_account(round1),
        historical_comparison=dict(graph_seconds=705.61,five_stage_seconds=624.82,worker_llm_seconds=594.60,
            n=28,cohort='historical_old_epoch',prior_attempt_cold_recompute_graph_seconds=1519.75,
            historical_base_rerun_seconds=43702,historical_waste_seconds=None,
            note='Compare matched metric definitions and cohorts; no inference of pure embedding compute or model slowdown.'))
