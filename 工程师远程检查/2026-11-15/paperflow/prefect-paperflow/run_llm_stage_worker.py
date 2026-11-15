"""Start the separately supervised LLM-stage worker; never runs postprocessing."""
import asyncio
import json
import os
from pathlib import Path
import sys
import traceback


def main():
    root = Path(__file__).resolve().parent
    config_path = root / 'config.json'
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    api_url = str(config.get('prefect_api_url', '')).rstrip('/')
    if not api_url or '127.0.0.1:4291' in api_url or 'localhost:4291' in api_url:
        raise RuntimeError('新 Prefect 地址未配置')
    os.environ.update(PAPERFLOW_CONFIG=str(config_path), PREFECT_API_URL=api_url, PYTHONUTF8='1',
                      PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS='false')
    log_path = root / 'runtime' / 'llm-stage-worker.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf8', buffering=1) as log:
        sys.stdout = sys.stderr = log
        try:
            from paperflow_prefect.llm_queue.isolated_stage import serve_stage
            asyncio.run(serve_stage())
        except BaseException:
            traceback.print_exc(file=log)
            raise


if __name__ == '__main__':
    main()
