"""Explicit local entry points for queue discovery and the waiting API."""

import argparse
import hashlib
import hmac
import importlib.util
import json
import math
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
CANDIDATES = HERE.parents[1]
INTAKE = CANDIDATES / 'local-intake-live-link-20261031'
SERVER = CANDIDATES / 'postprocess-server-20261101'


def _bootstrap(name, path, expected):
    """Execute bootstrap bytes only after checking an independent source hash."""
    if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected):
        raise ValueError('an independent bootstrap source hash is required')
    raw = Path(path).read_bytes()
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected):
        raise ValueError('bootstrap source differs from its independent hash')
    if name in sys.modules:
        raise ValueError('a fresh bootstrap module namespace is required')
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError('bootstrap module is unavailable')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        exec(compile(raw, str(path), 'exec', dont_inherit=True), module.__dict__)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


async def _closed_app(scope, receive, send):
    if scope.get('type') == 'lifespan':
        while True:
            event = await receive()
            if event['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            elif event['type'] == 'lifespan.shutdown':
                await send({'type': 'lifespan.shutdown.complete'})
                return
    elif scope.get('type') == 'http':
        await send({'type': 'http.response.start', 'status': 503,
                    'headers': [(b'content-type', b'application/json'), (b'cache-control', b'no-store')]})
        await send({'type': 'http.response.body', 'body': b'{"status":"release_unavailable"}'})


def _closed_projection_app(args, guard):
    guard.verify()
    from read_only_queue import open_queue
    queue = open_queue(args.db)
    observer = configured_orders(queue, guard=guard)
    api = guard.load_source('_release_fallback_waiting_api', HERE / 'waiting_api.py')
    from bounded_summary import BoundedSummary
    return BoundedSummary(api.create_app(queue, max_age_seconds=args.max_age_seconds,
                          ticket_observer=observer, registered_projection=None,
                          postprocess_only=True)), 'closed_projection'


def _history_router(args, loaded):
    names = ('history_registry', 'history_registry_sha256', 'history_access', 'history_access_sha256',
             'history_unread_script', 'history_unread_script_sha256')
    values = [getattr(args, name, None) for name in names]
    if not any(values):
        return None
    if not all(isinstance(value, str) and value for value in values) or loaded is None:
        raise ValueError('all independent historical observation pins are required')
    from read_only_rpc import ExistingInspectVersionRouter, PinnedAccessBindings, UnreadDisclosureGate
    server = loaded.remote.stage_store.inspect_receipt.server
    access = PinnedAccessBindings(args.history_access, args.history_access_sha256, server_identity=server)
    return ExistingInspectVersionRouter(args.history_registry, args.history_registry_sha256, server_identity=server,
        access_bindings=access,
        disclosure_gate=UnreadDisclosureGate(args.history_unread_script, args.history_unread_script_sha256))


def _guarded_delegate(args, history, delegate):
    bundle, digest = getattr(args, 'guard_bundle', None), getattr(args, 'guard_bundle_sha256', None)
    if not isinstance(bundle, str) or not bundle or not isinstance(digest, str) or not digest:
        raise ValueError('the independently fixed server guard bundle is required')
    from read_only_rpc import GuardedAppDelegate
    return GuardedAppDelegate(delegate, history, bundle, digest)


def create_release_app(args):
    """Build a passive page or a fail-closed fallback; never run a folder."""
    sys.path.insert(0, str(HERE))
    sys.path.insert(0, str(INTAKE))
    sys.path.insert(0, str(SERVER))
    guard = None
    pinned_imports_installed = False
    try:
        required = (args.release_manifest, args.release_manifest_sha256, args.guard_sha256, args.layout_sha256)
        if not getattr(args, 'closed_projection', False):
            required += (args.remote_config, args.remote_config_sha256)
        if not all(isinstance(value, str) and value for value in required):
            raise ValueError('pinned page and remote configurations are required')
        release = _bootstrap('release_guard', HERE / 'release_guard.py', args.guard_sha256)
        layout = _bootstrap('release_layout', HERE / 'release_layout.py', args.layout_sha256)
        roots, required_files = layout.layout()
        guard = release.ReleaseGuard(args.release_manifest, args.release_manifest_sha256,
            roots=roots, required_files=required_files, required_packages=layout.PACKAGES)
        guard.verify()
        sys.meta_path.insert(0, release.PinnedFinder(guard))
        # The existing receipt reader creates its package with an explicit
        # spec_from_file_location (which a finder cannot intercept). Preload
        # that package from approved bytes; its child imports use the finder.
        guard.load_source('_paperflow_first_step_receipt_reader',
                          CANDIDATES / 'first-step-control-20261029' / 'paperflow_prefect' / '__init__.py')
        pinned_imports_installed = True
        if getattr(args, 'closed_projection', False):
            return _closed_projection_app(args, guard)
        # build_from_config insists on a fresh business-module namespace. It
        # must run before importing page_assembly, which imports those classes.
        from read_only_rpc import ReadOnlyRPC, forbid_transfer
        from verified_ssh_transport import build as build_verified_ssh
        call_bounded = build_verified_ssh(guard, SERVER / 'ssh_rpc_client.py', SERVER / 'ssh_rpc_process.py')
        readonly_rpc = ReadOnlyRPC(call_bounded)
        import remote_live_launch
        loaded = remote_live_launch.build_from_config(args.remote_config, args.remote_config_sha256,
                                                       stage_call=forbid_transfer, rpc_call=readonly_rpc)
        history = _history_router(args, loaded)
        if history is None:
            raise ValueError('receipt-linked unread disclosure checks are not configured')
        readonly_rpc.delegate = _guarded_delegate(args, history, call_bounded)
        readonly_rpc.bind(database=loaded.original_runtime['database'],
                          inspector=loaded.remote.stage_store.inspect_receipt,
                          transport_config=loaded.remote.transport._config, version_router=history)
        from read_only_queue import open_queue
        queue = open_queue(args.db)
        observer = configured_orders(queue, guard=guard)
        import page_assembly
        app, _ = page_assembly.build_page(loaded_runtime=loaded, release_guard=guard, queue=queue,
            observer=observer, max_age_seconds=args.max_age_seconds,
            partial_observation=getattr(args, 'partial_observation', False))
        from bounded_summary import BoundedSummary
        return BoundedSummary(app, revision_witness=app.state.postprocessing_observation_revision), 'server_projection'
    except Exception as exc:
        print('服务器页面装配不可用：' + type(exc).__name__, file=sys.stderr, flush=True)
        # When all release sources remain pinned, preserve the existing
        # preprocessing/LLM API with registered_projection absent. If source
        # integrity itself is uncertain, return only 503 instead of loading it.
        if guard is not None and pinned_imports_installed:
            try:
                return _closed_projection_app(args, guard)
            except Exception:
                pass
        return _closed_app, 'closed_release'


def _verified_completion_reader(guard, path):
    """Use the existing completion contract, compiled from pinned bytes."""
    path = Path(path).resolve(strict=True)
    spec = importlib.util.spec_from_file_location('_paperflow_llm_result_contract', path)
    if spec is None or spec.loader is None:
        raise ValueError('LLM 完成登记契约不可用')
    module = importlib.util.module_from_spec(spec)
    exec(compile(guard.read_verified(path), str(path), 'exec', dont_inherit=True), module.__dict__)
    if not callable(getattr(module, 'inspect_folder', None)):
        raise ValueError('LLM 完成登记契约不可用')
    return module


def configured_orders(queue, *, guard):
    config_path = (INTAKE.parent / 'first-step-control-20261029' /
                   'runtime' / 'workspace-config.json')
    if not config_path.is_file():
        return None
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    if config.get('first_step_order_model') != 'v2':
        return None
    from prefect_orders import FolderOrderService
    index = Path(config['classification_root']) / 'group-task-summaries' / 'summary-index.sqlite3'
    observer = FolderOrderService(queue, prefect_api_url=config['prefect_api_url'],
                                  first_step_index=index, first_step_data_root=config['data_root'],
                                  first_step_receipt_root=config['receipt_root'])
    contract = HERE.parents[2] / 'prefect-paperflow' / 'paperflow_prefect' / 'llm_queue' / 'folder_completion.py'
    observer._llm_completion_reader = lambda: _verified_completion_reader(guard, contract)
    return observer


def main(argv=None):
    parser = argparse.ArgumentParser(description='本地接收等待统计候选')
    subparsers = parser.add_subparsers(dest='command', required=True)
    serve = subparsers.add_parser('serve', help='仅在本机启动等待数量接口')
    serve.add_argument('--db', required=True)
    serve.add_argument('--port', type=int, required=True)
    serve.add_argument('--max-age-seconds', type=float, default=60)
    serve.add_argument('--release-manifest')
    serve.add_argument('--release-manifest-sha256')
    serve.add_argument('--remote-config')
    serve.add_argument('--remote-config-sha256')
    serve.add_argument('--guard-sha256', required=True)
    serve.add_argument('--layout-sha256', required=True)
    serve.add_argument('--closed-projection', action='store_true')
    serve.add_argument('--partial-observation', action='store_true')
    serve.add_argument('--history-registry')
    serve.add_argument('--history-registry-sha256')
    serve.add_argument('--history-access')
    serve.add_argument('--history-access-sha256')
    serve.add_argument('--history-unread-script')
    serve.add_argument('--history-unread-script-sha256')
    serve.add_argument('--guard-bundle')
    serve.add_argument('--guard-bundle-sha256')
    args = parser.parse_args(argv)
    if (not 1 <= args.port <= 65535 or args.port == 14205
            or not math.isfinite(args.max_age_seconds) or args.max_age_seconds <= 0):
        parser.error('端口或扫描有效期无效')
    import uvicorn
    app, mode = create_release_app(args)
    print('页面只读装配模式：' + mode, flush=True)
    uvicorn.run(app,
                host='127.0.0.1', port=args.port, log_level='warning', access_log=False)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
