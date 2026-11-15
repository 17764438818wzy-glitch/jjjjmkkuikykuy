"""Receive one explicitly addressed folder into a private nonexecuting area."""
import json
import os
from pathlib import Path
import struct
import sys

from snapshot_wire import receive_snapshot
from snapshot_import import _PinnedDirectory


def _read_exact(stream, length):
    parts = []
    while length:
        data = stream.read(length)
        if not data:
            raise ValueError('incomplete transfer header')
        parts.append(data)
        length -= len(data)
    return b''.join(parts)


def receive(staging_root, input_stream, output_stream):
    root = Path(staging_root).absolute()
    with _PinnedDirectory(root) as directory:
        length = struct.unpack('!Q', _read_exact(input_stream, 8))[0]
        if not 1 <= length <= 16 * 1024 * 1024:
            raise ValueError('invalid transfer header length')
        header = json.loads(_read_exact(input_stream, length))
        if not isinstance(header, dict) or set(header) != {'manifest', 'snapshot_sha256'}:
            raise ValueError('invalid transfer header')
        manifest = header['manifest']
        # The UUID is validated by receive_snapshot before it creates this path.
        job_id = manifest.get('job_id', '') if isinstance(manifest, dict) else ''
        from remote_jobs import _uuid
        if not _uuid(job_id):
            raise ValueError('invalid job identity')
        directory.check_visible()
        record = receive_snapshot(root / job_id, manifest, header['snapshot_sha256'], input_stream,
                                  _parent=directory)
        directory.check_visible()
        output_stream.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')
        output_stream.flush()
        return record


if __name__ == '__main__':
    if len(sys.argv) != 2 or os.name != 'posix':
        raise SystemExit('one configured Linux staging directory is required')
    try:
        receive(sys.argv[1], sys.stdin.buffer, sys.stdout)
    except Exception as exc:
        print('staging failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
