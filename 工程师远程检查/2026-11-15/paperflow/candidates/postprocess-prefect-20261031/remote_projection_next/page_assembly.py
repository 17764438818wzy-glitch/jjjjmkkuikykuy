"""Explicit, inert assembly of the original page and server observation.

Callers provide the already constructed original runtime, remote runtime,
queue and observer. Building this object starts no worker, scan or request.
"""
from functools import partial
import importlib.util
import inspect
from pathlib import Path

from remote_live_runtime import RemoteLiveRuntime
from remote_prefect_entry import RemoteCoordinatorFactory
from remote_runtime_adapter import RemoteFolderStageStore, RemoteReceiptInspector
from remote_server_transport import RemoteServerTransport
from stage_runtime import Coordinator
import stage_runtime


HERE = Path(__file__).resolve().parent
ORIGINAL_CALLBACKS = {'task_gate', 'folder_gate', 'slot', 'verify_upstream', 'verify_execution',
                      'postprocess_is_open', 'rag_is_held'}


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError('page candidate module unavailable')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def build_page(*, original_runtime, remote_runtime, queue, observer):
    """Build an unstarted API app and its read-only registered projection."""
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
    factory._check_files()
    stage.inspect_receipt.journal._ready(stage)
    projections = _load('_remote_page_existing_projections', HERE.parent / 'projection_next' / 'projections.py')
    source = _load('_remote_page_scope_verifier', HERE / 'scope_verifier.py')
    ledger = _load('_remote_page_registered_ledger', HERE.parent / 'ledger_projection_next' / 'ledger_projection.py')
    server = _load('_remote_page_server_projection', HERE / 'server_projection.py')
    api = _load('_remote_page_waiting_api', HERE / 'waiting_api.py')
    from paperflow_prefect.llm_queue.folder_completion import component_id

    projector = projections.Projections(queue, observer, stage_runtime, stage, remote_runtime.results,
        binding_factory=original_runtime['upstream_factory'], process_alive=lambda *_: None,
        delivery_fingerprint=None)
    scope = source.ScopeVerifier(projector, server_identity=stage.inspect_receipt.server,
                                 check_original_files=factory._check_files)
    registered = partial(server.snapshot, base.database, ledger_snapshot=ledger.snapshot,
        component_id_fn=component_id, inspector=stage.inspect_receipt, stage_store=stage,
        verify_scope=scope, probe_server=transport.probe_server)
    app = api.create_app(queue, ticket_observer=observer, registered_projection=registered,
                         registered_title_enricher=getattr(observer, '_add_result_titles', None))
    return app, registered
