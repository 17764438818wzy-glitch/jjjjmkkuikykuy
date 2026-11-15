"""Run the attachment Prefect runner with an independent local log."""
from datetime import datetime
import os
from pathlib import Path
import sys
import traceback


def main():
    root = Path(__file__).resolve().parent
    os.environ['PAPERFLOW_CONFIG'] = str(root.parent.parent/'prefect-paperflow'/'config.json')
    path = root/'runtime'/'attachment-runner.log'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf8', buffering=1) as log:
        sys.stdout = sys.stderr = log
        print(datetime.now().isoformat(timespec='seconds'), 'starting attachment Prefect runner', flush=True)
        try:
            import manage_attachment
            sys.argv = [str(root/'manage_attachment.py'), 'serve', *sys.argv[1:]]
            manage_attachment.main()
        except BaseException:
            traceback.print_exc(file=log)
            raise


if __name__ == '__main__':
    main()
