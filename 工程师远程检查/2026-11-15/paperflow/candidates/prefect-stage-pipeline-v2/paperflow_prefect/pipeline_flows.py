"""Small control flows; never import legacy flows.py or retain paper lifetimes."""
from prefect import flow
from .pipeline_dispatch import configured_coordinator
from .records import read_json


@flow(name='pipeline-submit-batch',persist_result=True,log_prints=False)
def submit_batch_flow(scope_manifest: str, request_id: str) -> dict:
    coordinator=configured_coordinator()
    registered=coordinator.register(scope_manifest,request_id)
    return dict(registered,reconciliation=coordinator.reconcile_now(registered['batch_id']))


@flow(name='pipeline-reconcile-batches',persist_result=True,log_prints=False)
def reconcile_batches_flow() -> dict:
    coordinator=configured_coordinator(); reports=[]
    for path in sorted((coordinator.ledger.root/'scopes').glob('*.json')):
        record=read_json(path)
        if record.get('schema')!='pipeline-registration/1': continue
        reports.append(coordinator.reconcile_now(record['batch_id']))
    return dict(scopes=reports)


def _configured_flow(native_flow):
    coordinator=configured_coordinator()
    return coordinator,native_flow.with_options(
        result_storage='local-file-system/'+coordinator.gateway.pipeline['parameter_storage_block'])


def submit_batch(scope_manifest: str, request_id: str) -> dict:
    # Configuration must surround the engine, not begin inside the decorated body.
    coordinator,native=_configured_flow(submit_batch_flow)
    with coordinator.gateway.settings():
        from prefect.filesystems import LocalFileSystem
        pipeline=coordinator.gateway.pipeline
        LocalFileSystem(basepath=pipeline['storage_root']).save(pipeline['parameter_storage_block'],overwrite=True,_sync=True)
        return native(scope_manifest,request_id)


def reconcile_batches() -> dict:
    coordinator,native=_configured_flow(reconcile_batches_flow)
    with coordinator.gateway.settings(): return native()


def serve_reconciler():
    """Explicit opt-in native deployment; importing never installs or starts it."""
    coordinator,native=_configured_flow(reconcile_batches_flow)
    with coordinator.gateway.settings():
        native.serve(name='pipeline-reconcile-15s',interval=15,pause_on_shutdown=True)
