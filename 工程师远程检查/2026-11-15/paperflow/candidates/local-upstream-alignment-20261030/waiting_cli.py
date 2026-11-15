"""Explicit local entry points for queue discovery and the waiting API."""

import argparse
import json
import math
from pathlib import Path
from threading import Event

from local_queue import LocalQueue


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
        source, queue = FirstStepHTTPSource(args.upstream), LocalQueue(args.db)
        if args.command == 'watch':
            if not math.isfinite(args.interval_seconds) or args.interval_seconds <= 0:
                parser.error('扫描间隔无效')
            poll_until_stopped(source, queue, receipt_root=root, stop_event=Event(),
                               interval_seconds=args.interval_seconds,
                               on_result=lambda result: print(json.dumps(result, ensure_ascii=False), flush=True))
            return 0
        result = discover_once(source, queue, receipt_root=root)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result.get('complete') else 2
    if not 1 <= args.port <= 65535 or not math.isfinite(args.max_age_seconds) or args.max_age_seconds <= 0:
        parser.error('端口或扫描有效期无效')
    from waiting_api import create_app
    import uvicorn
    uvicorn.run(create_app(LocalQueue(args.db), max_age_seconds=args.max_age_seconds),
                host='127.0.0.1', port=args.port, log_level='warning', access_log=False)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
