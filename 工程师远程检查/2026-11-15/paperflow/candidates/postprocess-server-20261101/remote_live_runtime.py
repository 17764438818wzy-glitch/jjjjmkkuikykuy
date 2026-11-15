"""Explicitly compose the original Prefect runtime with the remote candidate.

Importing and building this object starts no worker, initializes no database,
scans no queue and performs no network request. The caller must already have an
initialized separate remote journal and must explicitly supply the original
runtime dictionary, including its upstream/backend factories and folder observer.
The bindings must all come from the same ``live_runtime.build_runtime()`` call;
callers must not reconstruct closures or substitute factories from another run.

This is an unconnected candidate, not a deployment or a RAG acceptance grant.
The authenticated transport and service identities are configuration; their
correctness and the selected business processor require independent review.
"""
from contextlib import contextmanager
import copy
from dataclasses import dataclass
import json
from pathlib import Path
import re

from live_runtime import ResultsHooks
from remote_coordinator import RemoteSubmissionJournal
from remote_jobs import _uuid
from remote_prefect_entry import RemoteCoordinatorFactory, RemotePrefectEntry
from remote_runtime_adapter import (_OPTIONS, RemoteRuntimeCoordinator,
                                    make_remote_receipt_stores, make_stage_input_loader)
from remote_server_transport import RemoteServerTransport, stage_bounded
from snapshot_collect import _Collector
from snapshot_prepare import counter_contract_from_runtime
from ssh_rpc_client import call_bounded
from stage_runtime import Coordinator
from windows_snapshot import _windows_name


@dataclass(frozen=True)
class RemoteLiveRuntime:
    entry: RemotePrefectEntry
    factory: RemoteCoordinatorFactory
    stage_store: object
    results: object
    journal: RemoteSubmissionJournal
    transport: RemoteServerTransport


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def _absolute(value, *, file=False):
    try:
        path = Path(value)
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError('an explicit absolute path is required')
        if file and not path.is_file():
            raise ValueError('the configured file must already exist')
        return path
    except (TypeError, OSError) as exc:
        raise ValueError('an explicit absolute path is required') from exc


def build_remote_live_runtime(*, original_runtime, journal_database, journal_id, expected_prefect_api_url,
                              server_identity, transport_config, transport_config_sha256,
                              snapshot_options, expected_catalog_path, expected_catalog_sha256,
                              tokenizer_runtime, max_summary_receipts, observe_activity=None,
                              stage_call=stage_bounded, rpc_call=call_bounded, admission_wait=None):
    """Construct one worker-process composition, without executing a job.

    ``original_runtime`` is the result of ONE existing ``build_runtime()``.
    Its coordinator, gates, slot and Prefect client remain authoritative.
    ``upstream_factory(client)``, ``backend_factory(verified)`` and
    ``observe_run(identity)`` are read from that same original dictionary.
    ``tokenizer_runtime`` is the explicit approved original-format runtime
    policy; each actual folder backend must reproduce its counter contract.
    The catalog path/SHA and source roots are also bound to the independently
    SHA-pinned transport configuration, and checked again while collecting.
    """
    if (not isinstance(original_runtime, dict)
            or not all(callable(f) for f in (stage_call, rpc_call))
            or (observe_activity is not None and not callable(observe_activity))
            or not isinstance(expected_prefect_api_url, str) or not expected_prefect_api_url.strip()
            or not expected_prefect_api_url.rstrip('/')
            or not _uuid(journal_id) or not _sha(transport_config_sha256)
            or not isinstance(server_identity, dict) or not isinstance(transport_config, dict)
            or not isinstance(snapshot_options, dict) or set(snapshot_options) != _OPTIONS
            or not isinstance(snapshot_options.get('catalog'), dict) or not snapshot_options['catalog']
            or not all(_sha(snapshot_options.get(key)) for key in ('code_sha256', 'runtime_template_sha256'))
            or not _sha(expected_catalog_sha256)):
        raise ValueError('explicit original bindings and complete remote configuration are required')
    try:
        base, client, api = (original_runtime['coordinator'], original_runtime['client'], original_runtime['api'])
        upstream_factory = original_runtime['upstream_factory']
        backend_factory = original_runtime['backend_factory']
        observe_run = original_runtime['observe_run']
        database = _absolute(original_runtime['database'], file=True).resolve(strict=True)
    except (KeyError, TypeError, OSError) as exc:
        raise ValueError('the original Prefect runtime is incomplete') from exc
    if (not isinstance(base, Coordinator) or isinstance(base, RemoteRuntimeCoordinator)
            or base.database != database or not callable(client) or not isinstance(api, str)
            or not all(callable(f) for f in (upstream_factory, backend_factory, observe_run))
            or api.rstrip('/') != expected_prefect_api_url.rstrip('/')):
        raise ValueError('the original Prefect control database or API identity changed')
    journal_path = _absolute(journal_database, file=True)
    catalog_path = _absolute(expected_catalog_path, file=True)
    roots = snapshot_options.get('allowed_roots')
    if not isinstance(roots, list) or not roots or any(not isinstance(root, str) for root in roots):
        raise ValueError('explicit source directories are required')
    for root in roots:
        _absolute(root)
    root, _, _ = _windows_name(snapshot_options['paperflow_root_windows'])
    workspace, _, _ = _windows_name(snapshot_options['business_workspace_windows'])
    output, _, _ = _windows_name(snapshot_options['output_root_windows'])
    if (root.drive != 'F:' or root.name != 'core' or str(root) != snapshot_options['paperflow_root_windows']
            or workspace.drive != 'F:' or str(workspace) != snapshot_options['business_workspace_windows']
            or output.drive != 'F:' or len(str(output)) > 64 or len(output.parts) < 2
            or str(output) != snapshot_options['output_root_windows']
            or output.is_relative_to(workspace) or workspace.is_relative_to(output)
            or output.is_relative_to(root) or root.is_relative_to(output)):
        raise ValueError('server paths do not meet the existing snapshot contract')
    if (transport_config.get('server_identity') != server_identity
            or transport_config.get('allowed_roots') != roots
            or transport_config.get('trusted_files') != [dict(path=str(catalog_path), sha256=expected_catalog_sha256)]):
        raise ValueError('transport server, source directories or trusted catalog differ')
    try:
        options = copy.deepcopy(snapshot_options)
        json.dumps(options, ensure_ascii=False, allow_nan=False)
        expected_counter = counter_contract_from_runtime(tokenizer_runtime)
    except (TypeError, ValueError) as exc:
        raise ValueError('the approved snapshot or tokenizer configuration is unavailable') from exc
    if (any(type(options[key]) is not int or options[key] <= 0
            for key in ('max_json_file_bytes', 'max_total_json_bytes'))
            or options['max_total_json_bytes'] < options['max_json_file_bytes']):
        raise ValueError('explicit positive metadata budgets are required')
    # Reuse the collector's bounded, link-aware evidence reader; this reads
    # only the configured catalog, never a paper or discovered folder. The
    # original collector will check it again for the individual snapshot.
    collector = _Collector(roots, defer_binary_hash=True,
        max_json_file_bytes=options['max_json_file_bytes'],
        max_total_json_bytes=options['max_total_json_bytes'], trusted_catalog_path=catalog_path)
    if collector.read(dict(path=str(catalog_path), sha256=expected_catalog_sha256),
                      'configured.catalog', json_value=True, catalog_file=True) != options['catalog']:
        raise ValueError('the configured catalog content or hash differs')

    @contextmanager
    def checked_client():
        with client() as actual:
            if str(getattr(actual, 'api_url', '')).rstrip('/') != api.rstrip('/'):
                raise ValueError('the original Prefect client identity changed')
            yield actual

    journal = RemoteSubmissionJournal(journal_path, control_database=database,
                                      admission_gate=base.task_gate, journal_id=journal_id)
    transport_type = RemoteServerTransport
    if admission_wait is not None:
        from capacity_aware_transport import CapacityAwareTransport
        transport_type = CapacityAwareTransport
    transport = transport_type(transport_config, config_sha256=transport_config_sha256,
                               journal=journal, stage_call=stage_call, rpc_call=rpc_call)
    stage_store, results = make_remote_receipt_stores(database=database, admission_gate=base.task_gate,
        journal=journal, server_identity=server_identity, observe_run=observe_run,
        observe_activity=observe_activity, inspect_batch=transport.inspect_batch,
        max_summary_receipts=max_summary_receipts)
    original_loader = make_stage_input_loader(database=database, client_factory=checked_client,
        upstream_factory=upstream_factory, backend_factory=backend_factory)

    def load_inputs(identity, generation, item, context_task):
        result = original_loader(identity, generation, item, context_task)
        if (not isinstance(result, dict) or result.get('counter_contract') != expected_counter
                or str(result.get('trusted_catalog_path', '')) != str(catalog_path)):
            raise ValueError('folder backend catalog or tokenizer differs from the approved configuration')
        return result

    # Preserve every existing gate/callback and the original local base. Only
    # the copy's results hook points to the shared authenticated inspector.
    remote_base = copy.copy(base)
    remote_base.results = ResultsHooks(results)
    parts = dict(base=remote_base, stage_store=stage_store, journal=journal,
                 load_inputs=load_inputs, snapshot_options=options)
    factory = RemoteCoordinatorFactory(server_identity=server_identity, control_database=database,
        journal_database=journal_path, journal_id=journal_id, original_base=base,
        components_factory=lambda: dict(parts), transport=transport.coordinator_callbacks(),
        admission_wait=admission_wait)
    # Validate all already-present local component bindings without acquiring a
    # stage/claim or invoking any of the network callbacks.
    factory()
    entry = RemotePrefectEntry(factory=factory, client_factory=checked_client,
                               expected_api_url=expected_prefect_api_url)
    return RemoteLiveRuntime(entry, factory, stage_store, results, journal, transport)
