"""Independent Prefect tasks; persisted values contain only identity and refs."""
from prefect import task
from .pipeline_contracts import StageEnvelope


def no_cache(context,parameters): return None


async def execute_stage(envelope: StageEnvelope):
    from .pipeline_execution import execute_stage as execute
    return await execute(envelope)


def _business_task(name):
    async def execute(envelope: StageEnvelope):
        if isinstance(envelope,dict): envelope=StageEnvelope.from_dict(envelope)
        if name=='model_recovery':
            if envelope.dispatch_kind.value!='recovery': raise ValueError('recovery worker requires recovery envelope')
        elif envelope.stage.value!=name or envelope.dispatch_kind.value!='normal':
            raise ValueError('worker stage/envelope mismatch')
        result=await execute_stage(envelope)
        from .pipeline_reconcile import reconcile_scope
        # execute_stage has committed and released its operation claim already.
        # If handoff fails, the independent 15s reconciler reads that durable result.
        await reconcile_scope(envelope.batch_id)
        return result.to_dict()
    execute.__name__='pipeline_'+name
    execute.__qualname__='pipeline_'+name
    decorated=task(name='pipeline-'+name,cache_key_fn=no_cache,persist_result=True,
                   result_storage_key='pipeline-task-results/{task_run.id}',refresh_cache=True,
                   retries=0,log_prints=False)(execute)
    # The distinct function name survives Prefect with_options reconstruction.
    # Do not manually override task_key: workers and submitters must match.
    return decorated


TASKS={name:_business_task(name) for name in ('intake','prepare','profile','attachment','package',
    'rag_body','rag_attachment','archive','cleanup','finish','model_recovery')}
WORKERS={
    'body':dict(tasks=('profile',),limit=128),
    'attachment':dict(tasks=('attachment',),limit=32),
    'model_recovery':dict(tasks=('model_recovery',),limit=32),
    'prepare':dict(tasks=('prepare',),limit=4),
    'package':dict(tasks=('package',),limit=2),
    'rag':dict(tasks=('rag_body','rag_attachment'),limit=40),
    'nas':dict(tasks=('archive',),limit=4),
    'control':dict(tasks=('intake','cleanup','finish'),limit=8),
}
