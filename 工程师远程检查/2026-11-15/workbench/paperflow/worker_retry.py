"""Bounded short attempts; outer backoff belongs to the durable queue."""
from .scheduling_policy import ActionDeferred,fault


def short_attempts(client,call,*,interval):
    for attempt in range(3):
        if client.stopped.is_set():raise ActionDeferred('stopped')
        try:return call()
        except Exception as exc:
            failure=fault(exc)
            if (failure['category']!='transient' or attempt==2 or getattr(exc,'unresolved_write',False)
                    or failure['retry_after']>interval):raise
            if client.stopped.wait(interval):raise ActionDeferred('stopped') from exc


def report_failure(client,exc):
    failure=fault(exc)
    outcome=('stopped' if client.stopped.is_set() else 'deferred' if failure['category']=='deferred' else
             'failed' if failure['category'] in ('transient','configuration') else 'held')
    data=dict(fault=failure,error=type(exc).__name__)
    # Persist only the narrow routing fact. The diagnostic may contain manuscript
    # text or private identifiers and remains in the task's private directory.
    from .phase_cache import CACHE_STAGES
    if getattr(exc,'required_stage',None) in CACHE_STAGES:data['required_stage']=exc.required_stage
    if getattr(exc,'check',None) in ('missing_files','body_loss','privacy','identity','wrong_library'):
        data['check']=exc.check
    try:client.report_execution(outcome,data)
    except Exception:pass  # Missing result is reconciled from action facts and confirmed tree exit.
