"""Explicit, inert assembly of the original page and server observation.

Callers provide the already constructed original runtime, remote runtime,
queue and observer. Building this object starts no worker, scan or request.
"""
from functools import partial
import importlib.util
import inspect
from pathlib import Path

from remote_live_launch import LoadedRemoteRuntime
from remote_live_runtime import RemoteLiveRuntime
from remote_prefect_entry import RemoteCoordinatorFactory
from remote_runtime_adapter import RemoteFolderStageStore, RemoteReceiptInspector
from remote_server_transport import RemoteServerTransport
from stage_runtime import Coordinator
import stage_runtime
from release_guard import ReleaseGuard


HERE = Path(__file__).resolve().parent
ORIGINAL_CALLBACKS = {'task_gate', 'folder_gate', 'slot', 'verify_upstream', 'verify_execution',
                      'postprocess_is_open', 'rag_is_held'}


def _load(guard, name, path):
    return guard.load_source(name, path)


def build_page(*, loaded_runtime, release_guard, queue, observer, max_age_seconds=60, partial_observation=False):
    """Build an unstarted API app and its read-only registered projection."""
    if (type(loaded_runtime) is not LoadedRemoteRuntime or type(release_guard) is not ReleaseGuard
            or type(max_age_seconds) not in (int, float) or not 0 < max_age_seconds <= 3600
            or type(partial_observation) is not bool):
        raise ValueError('the pinned runtime, release guard and source age are required')
    original_runtime, remote_runtime = loaded_runtime.original_runtime, loaded_runtime.remote
    if not isinstance(original_runtime, dict) or type(remote_runtime) is not RemoteLiveRuntime:
        raise ValueError('the two original runtime objects are required')
    base = original_runtime.get('coordinator')
    factory, stage, transport = remote_runtime.factory, remote_runtime.stage_store, remote_runtime.transport
    try:
        original_closures = inspect.getclosurevars(base.verify_upstream).nonlocals
    except (AttributeError, TypeError):
        original_closures = {}
    if (not isinstance(base, Coordinator) or type(factory) is not RemoteCoordinatorFactory
            or type(stage) is not RemoteFolderStageStore or type(transport) is not RemoteServerTransport
            or type(stage.inspect_receipt) is not RemoteReceiptInspector
            or not callable(original_runtime.get('upstream_factory'))
            or not callable(original_runtime.get('client'))
            or stage.admission_gate is not base.task_gate
            or stage.inspect_receipt.journal is not remote_runtime.journal
            or transport._journal is not remote_runtime.journal
            or transport._server != stage.inspect_receipt.server
            or getattr(stage.inspect_receipt._inspect_batch, '__self__', None) is not transport
            or getattr(stage.inspect_receipt._inspect_batch, '__func__', None) is not RemoteServerTransport.inspect_batch
            or getattr(transport.probe_server, '__func__', None) is not RemoteServerTransport.probe_server
            or getattr(remote_runtime.results, 'inspect_receipt', None) is not stage.inspect_receipt
            or getattr(remote_runtime.results, 'admission_gate', None) is not base.task_gate
            or Path(original_runtime['database']).resolve(strict=True) != Path(base.database).resolve(strict=True)
            or Path(observer.index).resolve(strict=True) != Path(base.database).resolve(strict=True)
            or Path(stage.database).resolve(strict=True) != Path(base.database).resolve(strict=True)
            or Path(factory._database).resolve(strict=True) != Path(base.database).resolve(strict=True)
            or Path(factory._journal_database).resolve(strict=True) != Path(remote_runtime.journal.database).resolve(strict=True)
            or factory._journal_id != remote_runtime.journal.journal_id
            or {'server_id': factory._server_id, 'ledger_id': factory._ledger_id} != transport._server
            or str(observer.api_url).rstrip('/') != str(original_runtime['api']).rstrip('/')
            or original_closures.get('upstream') is not original_runtime['upstream_factory']
            or original_closures.get('client') is not original_runtime['client']
            or set(factory._original_callbacks) != ORIGINAL_CALLBACKS
            or any(getattr(base, name, None) is not callback
                   for name, callback in factory._original_callbacks.items())):
        raise ValueError('original control, Prefect, gate or transport bindings changed')
    def check_release():
        release_guard.verify()
        loaded_runtime.check_configuration()
        factory._check_files()

    check_release()
    stage.inspect_receipt.journal._ready(stage)
    projections = _load(release_guard, '_release_page_existing_projections', HERE.parent / 'projection_next' / 'projections.py')
    source = _load(release_guard, '_release_page_scope_verifier', HERE / 'scope_verifier.py')
    ledger = _load(release_guard, '_release_page_registered_ledger', HERE.parent / 'ledger_projection_next' / 'ledger_projection.py')
    server = _load(release_guard, '_release_page_server_projection', HERE / 'server_projection.py')
    api = _load(release_guard, '_release_page_waiting_api', HERE / 'waiting_api.py')
    revision_module = _load(release_guard, '_release_page_revision_witness', HERE / 'revision_witness.py')
    revision = revision_module.ReadOnlyRevision(base.database)
    from paperflow_prefect.llm_queue.folder_completion import component_id

    projector = projections.Projections(queue, observer, stage_runtime, stage, remote_runtime.results,
        binding_factory=original_runtime['upstream_factory'], process_alive=lambda *_: None,
        delivery_fingerprint=None)
    scope = source.ScopeVerifier(projector, server_identity=stage.inspect_receipt.server,
                                 check_original_files=check_release, projection_helpers=projections)
    if partial_observation:
        partial_module = _load(release_guard, '_release_page_partial_projection', HERE / 'partial_projection.py')
        aggregate_module = _load(release_guard, '_release_page_partial_aggregator', HERE / 'partial_aggregator.py')
        aggregator = aggregate_module.PartialAggregator(base.database, ledger_snapshot=ledger.snapshot,
            component_id_fn=component_id, inspector=stage.inspect_receipt, stage_store=stage,
            scope=scope, server_helpers=server, partial=partial_module, check_inputs=check_release,
            revision_witness=revision.current)
        registered = aggregator.snapshot
    else:
        registered = partial(server.snapshot, base.database, ledger_snapshot=ledger.snapshot,
            component_id_fn=component_id, inspector=stage.inspect_receipt, stage_store=stage,
            verify_scope=scope, probe_server=transport.probe_server)
    app = api.create_app(queue, max_age_seconds=max_age_seconds, ticket_observer=observer, registered_projection=registered,
                         postprocess_only=True,
                         partial_observation=partial_observation,
                         registered_revision=revision.current,
                         local_waiting_projection=partial(ledger.snapshot, base.database, component_id_fn=component_id),
                         registered_title_enricher=getattr(observer, '_add_result_titles', None))
    positions = _load(release_guard, '_release_page_ticket_positions', HERE / 'ticket_positions.py')
    positions.install(app, projector, api._ProjectionCache)
    folders = _load(release_guard, '_release_page_folder_tickets', HERE / 'folder_tickets.py')
    folders.install(app, projector, positions)
    app.state.postprocessing_revision = revision
    app.state.postprocessing_observation_revision = lambda: (
        revision.current(), app.state.postprocessing_cache_revision())
    app.router.add_event_handler('shutdown', revision.close)
    if partial_observation:
        app.router.add_event_handler('shutdown', aggregator.close)
    check_release()
    return app, registered
