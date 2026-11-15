"""Launch one intake worker or API under a Windows scheduled task with a log."""

import argparse
from datetime import datetime
from pathlib import Path
import sys
import traceback


def main(argv=None):
    parser = argparse.ArgumentParser(description='本地接收服务托管入口')
    parser.add_argument('--log', required=True)
    parser.add_argument('service_args', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    service_args = args.service_args
    if service_args and service_args[0] == '--':
        service_args = service_args[1:]
    if not service_args or service_args[0] not in {'watch', 'serve'}:
        parser.error('只允许启动接收循环或只读接口')
    path = Path(args.log).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf8', buffering=1) as log:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = sys.stderr = log
        try:
            print(f'{datetime.now().isoformat(timespec="seconds")} starting {service_args[0]}', flush=True)
            from waiting_cli import main as run_service
            return run_service(service_args)
        except BaseException:
            traceback.print_exc(file=log)
            raise
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr


if __name__ == '__main__':
    raise SystemExit(main())
