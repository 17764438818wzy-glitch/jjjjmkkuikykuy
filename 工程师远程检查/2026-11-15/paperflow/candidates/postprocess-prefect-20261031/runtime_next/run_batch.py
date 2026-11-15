"""Explicit, bounded execution of one chosen task's postprocessing queue."""
import argparse
import asyncio
import json
import os
from pathlib import Path

from live_runtime import _prepare_imports, build_runtime


def main():
    parser = argparse.ArgumentParser(description='只处理指定大工单已登记等待的文件夹，不扫描或重发模型请求')
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--max-folders', type=int, required=True)
    parser.add_argument('--batch-size', type=int, default=8)
    args = parser.parse_args()
    if args.max_folders < 1 or not 1 <= args.batch_size <= 8:
        parser.error('文件夹数量必须为正，单批并发必须在 1 到 8 之间')
    _prepare_imports()
    from batch_next import run_batch, snapshot_waiting
    workspace = Path(__file__).resolve().parents[3]
    config = json.loads((workspace / 'prefect-paperflow' / 'config.json').read_text(encoding='utf-8-sig'))
    api = str(config.get('prefect_api_url', '')).rstrip('/')
    if not api:
        raise ValueError('Prefect 服务地址不可核验')
    os.environ['PREFECT_API_URL'] = api
    os.environ['PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS'] = 'false'
    runtime = build_runtime()
    if runtime['api'] != api:
        raise ValueError('Prefect 服务地址不一致')
    if not runtime['barriers'].postprocess_is_open() or not runtime['barriers'].rag_is_held():
        raise ValueError('队列门禁状态不允许处理')
    selections = snapshot_waiting(runtime['database'], args.task_id, args.max_folders)
    print(json.dumps({'selected': len(selections), 'batch_size': args.batch_size}, ensure_ascii=False), flush=True)
    result = asyncio.run(run_batch(runtime['coordinator'], selections,
        client_factory=runtime['client'], runtime_factory=lambda: build_runtime()['coordinator'],
        batch_size=args.batch_size))
    print(json.dumps({'completed': len(result['completed']), 'attempted': result['attempted'],
                      'remaining': result['remaining'], 'stopped': result['stopped'],
                      'detail': result['detail']}, ensure_ascii=False), flush=True)
    return 1 if result['stopped'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
