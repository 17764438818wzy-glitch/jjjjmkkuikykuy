"""Probe only a few scoped filenames with local Nimble; never replay a prior call."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
import time

from paperflow_prefect.nimble_group import NimbleJudge


root = Path(__file__).resolve().parent
data = root.parents[1] / 'prefect-paperflow/runtime/desktop100-closeout-20261029/closeout-inputs.json'
rows = {row['number']: row for row in json.loads(data.read_text(encoding='utf-8-sig'))['papers']}
samples = [(str(n), Path(rows[n]['source_original']).name) for n in (1, 3, 8, 20, 99)]
samples.append(('major_only_synthetic', '001_1908034124_张三_体育经济与管理.doc'))
directory = root / 'filename-nimble-probe-20261029'
directory.mkdir(exist_ok=False)
judge = NimbleJudge(timeout=180)
for label, filename in samples:
    intent = directory / (label + '-intent.json')
    with intent.open('x', encoding='utf8') as stream:
        json.dump({'source': label, 'filename': filename,
                   'filename_sha256': hashlib.sha256(filename.encode('utf8')).hexdigest(),
                   'status': 'started_or_unknown', 'time': datetime.now(timezone.utc).isoformat()},
                  stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    start = time.monotonic()
    try:
        result = judge.choose_filename_title(filename)
        record = {'source': label, 'result': result, 'elapsed_seconds': round(time.monotonic()-start, 2)}
    except Exception as exc:
        record = {'source': label, 'outcome': 'unknown', 'error_type': type(exc).__name__,
                  'elapsed_seconds': round(time.monotonic()-start, 2)}
    with (directory / (label + '-result.json')).open('x', encoding='utf8') as stream:
        json.dump(record, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps(record, ensure_ascii=False), flush=True)
    if record.get('outcome') == 'unknown':
        break
