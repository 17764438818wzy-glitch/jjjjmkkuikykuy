"""Serve the first-step order on the configured shared Prefect server."""
import json
import os
from pathlib import Path
import runpy
import sys
import traceback


def main():
    root = Path(__file__).resolve().parent
    config_path = root / 'runtime' / 'workspace-config.json'
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    if config.get('first_step_order_model') != 'v2':
        raise RuntimeError('统一工单模式未启用')
    api = str(config.get('prefect_api_url', '')).rstrip('/')
    if not api or '127.0.0.1:4291' in api or 'localhost:4291' in api:
        raise RuntimeError('统一 Prefect 地址未配置')
    os.environ.update(PAPERFLOW_CONFIG=str(config_path), PREFECT_API_URL=api, PYTHONUTF8='1')
    log_path = root / 'runtime' / 'unified-first-step-runner.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf8', buffering=1) as log:
        sys.stdout = sys.stderr = log
        try:
            runpy.run_path(str(root / 'serve_controlled.py'), run_name='__main__')
        except BaseException:
            traceback.print_exc(file=log)
            raise


if __name__ == '__main__':
    main()
