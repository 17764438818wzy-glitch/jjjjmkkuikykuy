"""Launch one intake worker or API under a Windows scheduled task with a log."""

import argparse
from datetime import datetime
import hashlib
import hmac
from pathlib import Path
import re
import sys
import traceback
from uuid import uuid4


def disable_cached_bytecode():
    """Dynamic source loaders must not silently execute an old timestamp pyc."""
    unused = Path(__file__).resolve().parent / ('.bytecode-disabled-' + uuid4().hex)
    if unused.exists():
        raise RuntimeError('private bytecode prefix already exists')
    sys.pycache_prefix = str(unused)
    sys.dont_write_bytecode = True


def verified_source(path, expected):
    if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected):
        raise ValueError('an independent CLI source hash is required')
    raw = Path(path).read_bytes()
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected):
        raise ValueError('CLI source differs from its independent hash')
    return raw


def main(argv=None):
    parser = argparse.ArgumentParser(description='本地接收服务托管入口')
    parser.add_argument('--log', required=True)
    parser.add_argument('--cli-sha256', required=True)
    parser.add_argument('service_args', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    service_args = args.service_args
    if service_args and service_args[0] == '--':
        service_args = service_args[1:]
    if not service_args or service_args[0] != 'serve':
        parser.error('只允许启动只读页面接口')
    disable_cached_bytecode()
    path = Path(args.log).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf8', buffering=1) as log:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = sys.stderr = log
        try:
            print(f'{datetime.now().isoformat(timespec="seconds")} starting {service_args[0]}', flush=True)
            # This fixed bootstrap always executes the adjacent source bytes,
            # never a previously compiled pyc; the CLI subsequently verifies
            # the complete independently pinned release before observations.
            source = Path(__file__).resolve().with_name('waiting_cli.py')
            namespace = {'__name__': '_paperflow_release_cli', '__file__': str(source)}
            exec(compile(verified_source(source, args.cli_sha256), str(source), 'exec', dont_inherit=True), namespace)
            run_service = namespace['main']
            return run_service(service_args)
        except BaseException:
            traceback.print_exc(file=log)
            raise
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr


if __name__ == '__main__':
    raise SystemExit(main())
