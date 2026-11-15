"""Explicit local entry points for queue discovery and the waiting API."""

import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sys
from threading import Event

from local_queue import LocalQueue


def configured_orders(queue):
    config_path = (Path(__file__).resolve().parent.parent / 'first-step-control-20261029' /
                   'runtime' / 'workspace-config.json')
    if not config_path.is_file():
        return None
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    if config.get('first_step_order_model') != 'v2':
        return None
    from prefect_orders import FolderOrderService
    index = Path(config['classification_root']) / 'group-task-summaries' / 'summary-index.sqlite3'
    return FolderOrderService(queue, prefect_api_url=config['prefect_api_url'],
                              first_step_index=index, first_step_data_root=config['data_root'],
                              first_step_receipt_root=config['receipt_root'])


def configured_postprocessing(queue, observer, *, projection_path=None):
    """Construct passive readers only; a missing or invalid source remains unknown."""
    if queue is None or observer is None:
        return None, None
    path = Path(projection_path) if projection_path is not None else (
        Path(__file__).resolve().parent.parent / 'postprocess-prefect-20261031' /
        'projection_next' / 'projections.py')
    try:
        if not path.is_file():
            return None, None
        spec = importlib.util.spec_from_file_location('_paperflow_postprocess_projection', path)
        if spec is None or spec.loader is None:
            return None, None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        stage, results = module.build_projections(queue, observer)
        if not callable(stage) or not callable(results):
            raise ValueError('projection callbacks unavailable')
        return stage, results
    except Exception as exc:
        print('后处理只读投影暂不可用：' + type(exc).__name__, file=sys.stderr, flush=True)
        return None, None


def configured_retry_notes(queue, observer):
    """Add verified retry notes to the existing LLM observer, or keep it intact."""
    if queue is None or observer is None:
        return observer
    candidate = Path(__file__).resolve().parent.parent / 'postprocess-prefect-20261031'
    runtime_dir = candidate / 'generation_next'
    notes_dir = candidate / 'retry_ui_next'
    sources = (
        ('stage_runtime', runtime_dir / 'stage_runtime.py',
         'da4a8ee6b31c601cb37b91356c687be3a4368d6de78ab89950aa599b0a979b32'),
        ('live_runtime', runtime_dir / 'live_runtime.py',
         '83b103617d4e216e161c7322efb89144cc3ea4de10b5727f049a107a1191b92b'),
        ('_paperflow_retry_note_live', notes_dir / 'retry_note.py',
         '360a3d8e5cf579bcb62323f1ca587e303481761df733730e5d760dee2ce37ab8'),
    )
    try:
        for path in (notes_dir, runtime_dir):
            if str(path) not in sys.path:
                sys.path.insert(0, str(path))
        modules = {}
        for name, path, expected in sources:
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != expected or name in sys.modules:
                raise ValueError('retry observer source changed or namespace already occupied')
            spec = importlib.util.spec_from_file_location(name, path)
            if spec is None or spec.loader is None:
                raise ValueError('retry observer source is unavailable')
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            try:
                exec(compile(raw, str(path), 'exec', dont_inherit=True), module.__dict__)
            except BaseException:
                sys.modules.pop(name, None)
                raise
            modules[name] = module

        runtime = modules['live_runtime'].build_runtime()
        if (Path(runtime['database']).resolve(strict=True) != Path(observer.index).resolve(strict=True)
                or str(runtime['api']).rstrip('/') != str(observer.api_url).rstrip('/')):
            raise ValueError('retry observer control or Prefect identity differs')
        queue_config = (Path(__file__).resolve().parent.parent / 'attachment-prefect-live-link-20261031'
                        / 'attachment-queue-config.json')
        configured_queue = json.loads(queue_config.read_text(encoding='utf-8-sig'))['queue_db']
        if Path(configured_queue).resolve(strict=True) != Path(queue.path).resolve(strict=True):
            raise ValueError('retry observer queue identity differs')
        wrapped = modules['_paperflow_retry_note_live'].RetryNoteObserver(
            observer, runtime['database'], verify_upstream=runtime['coordinator'].verify_upstream)
        print('人工重试状态提示已装配；未启动工单处理。', flush=True)
        return wrapped
    except Exception as exc:
        print('人工重试状态提示暂不可用，继续使用原 LLM 页面：' + type(exc).__name__,
              file=sys.stderr, flush=True)
        return observer


def main(argv=None):
    parser = argparse.ArgumentParser(description='本地接收等待统计候选')
    subparsers = parser.add_subparsers(dest='command', required=True)
    for name, help_text in (('scan', '显式执行一次上游资料发现与核验'),
                            ('watch', '显式启动循环资料发现与核验')):
        scanner = subparsers.add_parser(name, help=help_text)
        scanner.add_argument('--db', required=True)
        scanner.add_argument('--receipt-root', required=True)
        scanner.add_argument('--upstream', default='http://127.0.0.1:14203')
        if name == 'watch':
            scanner.add_argument('--interval-seconds', type=float, default=30)
    serve = subparsers.add_parser('serve', help='仅在本机启动等待数量接口')
    serve.add_argument('--db', required=True)
    serve.add_argument('--port', type=int, required=True)
    serve.add_argument('--max-age-seconds', type=float, default=60)
    args = parser.parse_args(argv)
    if args.command in ('scan', 'watch'):
        root = Path(args.receipt_root).resolve()
        if Path(args.db).resolve() == root / 'receipts.sqlite3':
            parser.error('本地状态库不能使用上游登记数据库')
        from upstream_discovery import FirstStepHTTPSource, discover_once, poll_until_stopped
        source, queue = FirstStepHTTPSource(args.upstream), LocalQueue(args.db, require_retention=True)
        orders = configured_orders(queue)
        if orders is not None:
            source.expected_index = orders.index
            source.expected_queue = queue.path
            source.expected_prefect_api = orders.api_url
        stop_event = Event()
        service_down = False
        if orders is not None:
            from prefect_orders import OrderServiceUnavailable, prefect_unavailable
            def submit_ready(task_id, group_id):
                nonlocal service_down
                if service_down:
                    raise OrderServiceUnavailable('本轮 Prefect 工单服务不可用')
                try:
                    return orders.sync_one(task_id, group_id)
                except Exception as exc:
                    if prefect_unavailable(exc):
                        service_down = True
                    raise
            queue.on_receipt_ready = submit_ready
        def report(result):
            nonlocal service_down
            if orders is not None:
                try:
                    if service_down:
                        raise OrderServiceUnavailable('本轮 Prefect 工单服务不可用')
                    result['folder_order_failures'] = orders.sync_existing(stop_event=stop_event)
                    if result['folder_order_failures']:
                        result['complete'] = False
                        queue.record_scan(complete=False, reason='部分文件夹工单未完成核对')
                except Exception as exc:
                    result['folder_order_failures'] = None
                    result['folder_order_error'] = type(exc).__name__
                    result['complete'] = False
                    try:
                        queue.record_scan(complete=False, reason='文件夹工单状态暂不可用')
                    except Exception:
                        pass
                service_down = False
            print(json.dumps(result, ensure_ascii=False), flush=True)
        if args.command == 'watch':
            if not math.isfinite(args.interval_seconds) or args.interval_seconds <= 0:
                parser.error('扫描间隔无效')
            poll_until_stopped(source, queue, receipt_root=root, stop_event=stop_event,
                               interval_seconds=args.interval_seconds,
                               retention_client=source, on_result=report)
            return 0
        result = discover_once(source, queue, receipt_root=root, stop_event=stop_event, retention_client=source)
        report(result)
        return 0 if result.get('complete') else 2
    if not 1 <= args.port <= 65535 or not math.isfinite(args.max_age_seconds) or args.max_age_seconds <= 0:
        parser.error('端口或扫描有效期无效')
    from waiting_api import create_app
    import uvicorn
    queue = LocalQueue(args.db, require_retention=True, require_live_watcher=True)
    observer = configured_orders(queue)
    stage_projection, results_projection = configured_postprocessing(queue, observer)
    observer = configured_retry_notes(queue, observer)
    app = create_app(queue, max_age_seconds=args.max_age_seconds,
                     ticket_observer=observer, stage_projection=stage_projection,
                     results_projection=results_projection)
    if callable(getattr(observer, 'close', None)):
        app.router.add_event_handler('shutdown', observer.close)
    uvicorn.run(app,
                host='127.0.0.1', port=args.port, log_level='warning', access_log=False)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
