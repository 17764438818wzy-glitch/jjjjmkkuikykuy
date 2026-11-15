"""Reproducible test of the Windows deletion primitive on synthetic bytes only."""
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())
from paperflow.original_archive.lifecycle import locked_file, verify_stream
from paperflow_prefect._release.file_identity import identity_from_stream


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    if len(sys.argv) != 2: raise ValueError('existing case ID required')
    case = (HERE / 'live-tests' / sys.argv[1]).resolve(strict=True)
    if case.parent != (HERE / 'live-tests').resolve(strict=True): raise ValueError('case escapes test root')
    report = json.loads((case / 'report.json').read_text(encoding='utf8'))
    historical_copy = case / 'input/body.doc'
    if sha(historical_copy) != report['source_sha256']: raise ValueError('historical copy changed')
    folder = case / ('delete-primitive-' + uuid.uuid4().hex)
    folder.mkdir(exist_ok=False)
    target = folder / 'synthetic-only.txt'
    data = b'isolated deletion primitive self-test, synthetic contents only\n'
    with target.open('xb') as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
    if target.resolve(strict=True).parent != folder.resolve(strict=True): raise ValueError('unsafe target')
    events_path = folder / 'events.jsonl'
    def event(name, **fields):
        with events_path.open('a', encoding='utf8', newline='\n') as out:
            out.write(json.dumps({'event': name, 'time_ns': time.time_ns(), **fields}, sort_keys=True) + '\n')
            out.flush(); os.fsync(out.fileno())
    with locked_file(target) as (stream, _):
        verify_stream(stream, hashlib.sha256(data).hexdigest(), len(data))
        file_id = identity_from_stream(stream)
        st = os.fstat(stream.fileno())
        if st.st_nlink != 1: raise ValueError('synthetic target has shared hard links')
        event('created_and_frozen', path=str(target), bytes=st.st_size,
              sha256=hashlib.sha256(data).hexdigest(), object_identity=file_id, hard_links=st.st_nlink,
              primitive='paperflow.original_archive.lifecycle.locked_file/verify_stream')
    with locked_file(target, delete=True) as (stream, remove):
        try:
            verify_stream(stream, '0' * 64, len(data))
        except ValueError as exc:
            error_type, error_message = type(exc).__name__, str(exc)
        else:
            raise AssertionError('wrong digest unexpectedly passed verification')
    if not target.is_file() or sha(target) != hashlib.sha256(data).hexdigest():
        raise ValueError('synthetic target changed after failed verification')
    event('mismatch_rejected', exception_type=error_type, exception_message=error_message,
          expected_sha256='0' * 64, target_still_exists=True, observed_sha256=sha(target))
    with locked_file(target, delete=True) as (stream, remove):
        verify_stream(stream, hashlib.sha256(data).hexdigest(), len(data))
        if identity_from_stream(stream) != file_id or os.fstat(stream.fileno()).st_nlink != 1:
            raise ValueError('synthetic target object identity changed')
        event('delete_intent', verified_object_identity=file_id, verified_sha256=hashlib.sha256(data).hexdigest())
        remove()
    if target.exists() or sha(historical_copy) != report['source_sha256']:
        raise ValueError('deletion postcondition not met')
    event('delete_verified', target_absent=True, historical_copy_present=True,
          historical_copy_sha256=sha(historical_copy), scope='synthetic primitive only, no release gate')
    print(json.dumps({'events': str(events_path), 'synthetic_target_deleted': True,
                      'historical_copy_preserved': True}, ensure_ascii=True))


if __name__ == '__main__': main()
