"""Explicit local entry points for queue discovery and the waiting API."""

import argparse
import json
import math
from pathlib import Path
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
                              first_step_index=index, first_step_data_root=config['data_root'])


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
    uvicorn.run(create_app(queue, max_age_seconds=args.max_age_seconds,
                           ticket_observer=configured_orders(queue)),
                host='127.0.0.1', port=args.port, log_level='warning', access_log=False)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
