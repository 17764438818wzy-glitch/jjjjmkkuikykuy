"""Hash-gated repair of SSH output collection after early exit status.

The only runtime change waits for channel EOF/close before the existing final
drain. Commands, result protocol, timeout and output-size limits are unchanged.
This installer never connects to a server or restarts a service.
"""
import argparse
import ast
import hashlib
import os
from pathlib import Path
import tempfile


ORIGINAL = '89c502493dd36ace3fc50005e8c70b5da9aa751e2864b015e37e1b1afa42b1b1'
PATCHED = 'e1b1f62c474c611ddc15bbabab86f5aef593ecdd7c735f2bd3279a71948f1cc3'
BEFORE = '            if channel.exit_status_ready():\n                break'
AFTER = ('            if channel.exit_status_ready() and (channel.eof_received or channel.closed):\n'
         '                break')


def transform(original):
    if original.count(BEFORE) != 1 or AFTER in original:
        raise ValueError('SSH output reader differs from the verified source')
    updated = original.replace(BEFORE, AFTER, 1)
    ast.parse(updated)
    return updated


def install(target):
    target = Path(target)
    current = target.read_bytes()
    digest = hashlib.sha256(current).hexdigest()
    if digest == PATCHED:
        return 'already_installed'
    if digest != ORIGINAL:
        raise ValueError('SSH provider source changed; inspect before applying output drain repair')
    updated = transform(current.decode('utf-8')).encode('utf-8')
    if hashlib.sha256(updated).hexdigest() != PATCHED:
        raise ValueError('SSH output drain repair checksum mismatch')
    compile(updated, str(target), 'exec')
    backup = target.with_name(target.name + '.output-drain-original')
    if backup.exists() and backup.read_bytes() != current:
        raise ValueError('SSH provider backup differs from the verified source')
    if not backup.exists():
        backup.write_bytes(current)
    fd, temporary = tempfile.mkstemp(prefix='.ssh-output-drain-', dir=target.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(updated)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, target.stat().st_mode)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return 'installed'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', type=Path,
                        default=Path('/ragflow/agent/sandbox/providers/ssh.py'))
    print(install(parser.parse_args().target))
