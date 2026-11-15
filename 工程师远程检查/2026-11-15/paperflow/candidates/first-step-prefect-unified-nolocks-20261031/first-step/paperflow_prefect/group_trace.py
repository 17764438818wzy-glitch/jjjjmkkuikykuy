"""Optional local timing trace for group and model-request overlap."""
import json
import os
import threading
import time

_lock = threading.Lock()


def emit(event, group, **fields):
    path = os.environ.get('PAPERFLOW_GROUP_TRACE')
    if not path:
        return
    record = dict(event=event, group=str(group), timestamp=time.time(), **fields)
    with _lock:
        with open(path, 'a', encoding='utf-8') as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + '\n')
