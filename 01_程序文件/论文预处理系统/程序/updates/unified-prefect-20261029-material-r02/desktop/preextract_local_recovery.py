"""One authorized, idle, scope-owned local action over verified saved evidence."""
import copy
import json
import sqlite3
import time
import uuid
from dataclasses import replace
from pathlib import Path

from filelock import Timeout


class _Busy(Exception):
    pass


class _NoExternalActions:
    def perform(self,*args,**kwargs):
        raise ValueError('本地恢复禁止模型、交付与外部动作')


def _database_busy(exc):
    return (getattr(exc, 'sqlite_errorcode', 0) & 255) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED} or (
        'busy' in str(exc).lower() or 'locked' in str(exc).lower())


def recover(processor,record,*,expected_binding,progress):
    from preextract_scope_guard import binding,registry_for
    from preextract_scheduler_adapter import observe_prepared,body_permit,context_identity
    from preextract_recovery_evidence import candidate_evidence
    from preextract_material import file_sha
    from paperflow.scheduling_store import SchedulingLedger
    from preextract_request_control import RequestControl
    from preextract_scheduler_recovery import run_prepared

    result=dict(status='needs_review',new_model_requests=0,uploaded=False)
    ledger=job=registry=None
    outcome='held'
    try:
        checkpoint=record.get('prepared_checkpoint') or {}
        work=Path(checkpoint['path']).resolve().parent
        marker=binding(work)
        if not marker or marker!=expected_binding:
            raise ValueError('本地恢复需要明确且一致的原范围身份')
        result.update(scope_id=marker['scope_id'],task_id=marker['task_id'],work_dir=str(work))
        registry=registry_for(marker)
        def check():
            if processor.stop.is_set():raise InterruptedError('scope stopped')
            registry.assert_running(marker['scope_id'],marker['generation'])
            task=registry.store.get(marker['task_id'])
            if not task or task.get('lease_until',0)>time.time():raise _Busy('prepared task is owned')
            with registry.store.db() as c:
                tables={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if 'scheduler_sessions' in tables and c.execute(
                    'SELECT 1 FROM scheduler_sessions WHERE ended IS NULL AND last_seen>?',
                    (time.time()-30,)).fetchone():
                    raise _Busy('prepared manager is active; local recovery requires an idle scope')
            return True
        check()
        member=next((m for m in registry.members(marker['scope_id']) if m['task_id']==marker['task_id']),None)
        if not member:raise ValueError('prepared member missing')
        native=copy.deepcopy(member['record'])
        task=registry.store.get(marker['task_id'])
        active_checkpoint=task.get('prepared_checkpoint') or native.get('prepared_checkpoint')
        if checkpoint!=active_checkpoint or context_identity(record)!=member['context_sha256']:
            raise ValueError('本地恢复材料或来源上下文已改变')
        native['prepared_checkpoint']=copy.deepcopy(active_checkpoint)
        observation=observe_prepared(native,data_root=processor.store.root,now=time.time())
        if observation.as_dict()['identity']!=member['identity'] or observation.context_sha256!=member['context_sha256']:
            raise ValueError('本地恢复来源、模板或材料身份不一致')
        if not body_permit(native,observation,processor.config.get('purpose_review',{})).get('allowed'):
            raise ValueError('本地恢复需要原范围的正文用途证据')
        with registry.store.db() as c:
            if c.execute("SELECT 1 FROM prepared_http WHERE task_id=? AND state IN ('possible','unknown')",
                         (marker['task_id'],)).fetchone():
                raise ValueError('未决模型请求保留，不能自动采纳或重发')
        from preextract_pipeline import DEFAULT_TEMPLATE
        template=Path(native.get('template_path') or processor.config.get('preextract_template') or DEFAULT_TEMPLATE)
        proof=candidate_evidence(native['path'],work,template_path=template)
        result['reused_response_sha256']=file_sha(proof['candidate_path'])
        # A previous content-review decision remains recorded. This explicit
        # action derives a local postprocess view only after full proof checks.
        local_observation=replace(observation,next_step='postprocess',blocking_reason=None)
        ledger=SchedulingLedger(registry.store)
        service=ledger.ensure_service('local',dict(operation='prepared-local-recovery/1'))['service_id']
        payload=dict(execution_engine='paper_preextract',step='postprocess',scope_id=marker['scope_id'],
            scope_generation=marker['generation'],candidate_checkpoint=checkpoint,local_recovery_id=uuid.uuid4().hex)
        job=ledger.claim_local_recovery(marker['task_id'],payload,service,still_valid=check,before_commit=check)
        if job is None:raise _Busy('prepared job is already owned')
        check()
        if candidate_evidence(native['path'],work,template_path=template)!=proof:
            raise ValueError('本地恢复开始前旧模型证据已改变')
        local_processor=copy.copy(processor)
        local_processor.config=copy.deepcopy(processor.config)
        control=RequestControl(_NoExternalActions(),registry,marker['scope_id'],marker['generation'],
            marker['task_id'],native,local_observation,purpose_config=processor.config.get('purpose_review',{}))
        prepared=run_prepared(native,decision=dict(step='postprocess',local_only=True),stop=processor.stop,
            progress=progress,processor=local_processor,control=control)
        check()
        if candidate_evidence(native['path'],work,template_path=template)!=proof:
            raise ValueError('本地恢复期间旧模型证据已改变')
        if prepared.get('stage')=='package_verified':
            result.update(status='package_verified',package_path=prepared['package_path'])
            outcome='complete'
        else:
            codes=[r['code'] for r in prepared.get('errors',[])]
            result.update(status='needs_content' if set(codes)&{'learning_unusable','routing_missing'} else 'needs_review',
                          stage=prepared.get('stage'),validation_codes=codes)
        registry.refresh_observation(marker['scope_id'],observe_prepared(native,data_root=processor.store.root,now=time.time()))
    except (_Busy,Timeout) as exc:
        result.update(status='busy',reason=str(exc))
    except InterruptedError as exc:
        result.update(status='stopped',reason=str(exc))
    except (OSError,ValueError,KeyError,TypeError,StopIteration,sqlite3.OperationalError) as exc:
        reason=str(exc)
        stopped=processor.stop.is_set() or 'scope stopped' in reason or 'pending stop' in reason
        result.update(status='stopped' if stopped else 'busy' if isinstance(exc,sqlite3.OperationalError) and _database_busy(exc) else 'needs_review',
                      reason=reason,error_type=type(exc).__name__)
    finally:
        if ledger is not None and job is not None:
            # The native writer already retries its bounded transaction. A
            # short second round handles transient contention at finalization;
            # a persistent failure retains ownership and reports its exact ID.
            for attempt in range(3):
                try:
                    ledger.finish(job['attempt_id'],outcome,result)
                    break
                except sqlite3.OperationalError as exc:
                    if _database_busy(exc) and attempt < 2:
                        time.sleep(.05)
                        continue
                    result.update(local_result_status=result['status'],status='needs_review',
                        reason='本地结果已保留，但恢复作业收尾未写入；原所有权保留，需核对后收尾',
                        finalization_error=str(exc),unfinished_claim=dict(job_id=job['job_id'],attempt_id=job['attempt_id']))
                    break
    return result
