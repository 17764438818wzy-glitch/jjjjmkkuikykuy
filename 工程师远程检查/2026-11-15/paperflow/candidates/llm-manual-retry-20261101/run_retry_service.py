"""Supervise only the local retry API; do not submit work at startup."""
import json
import os
from pathlib import Path
import sys
import traceback


def main():
    root = Path(__file__).resolve().parents[2]
    project = root / 'prefect-paperflow'
    config_path = project / 'config.json'
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    api = str(config.get('prefect_api_url', '')).rstrip('/')
    if not api or '127.0.0.1:4291' in api or 'localhost:4291' in api:
        raise RuntimeError('新 Prefect 地址未配置')
    os.environ.update(PAPERFLOW_CONFIG=str(config_path), PREFECT_API_URL=api, PYTHONUTF8='1')
    log_path = project / 'runtime' / 'llm-retry-api.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf8', buffering=1) as log:
        sys.stdout = sys.stderr = log
        try:
            import uvicorn
            from retry_service import create_app
            uvicorn.run(create_app(), host='127.0.0.1', port=14207, access_log=False)
        except BaseException:
            traceback.print_exc(file=log)
            raise


if __name__ == '__main__':
    main()
