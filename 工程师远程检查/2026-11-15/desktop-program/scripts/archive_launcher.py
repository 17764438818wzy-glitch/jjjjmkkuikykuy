"""Normal desktop entry plus the independently managed original-file archive."""
from pathlib import Path
import json
import os
import runpy
import sys

sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT/'scripts'))
def main():
    # Preserve the original entry's read-only check and exception reporting.
    if sys.argv[1:] in ([],['--monitor']):
        from original_archive_worker import ensure_started,atomic,read
        try:ensure_started(ROOT)
        except Exception as exc:
            try:
                paths=read(ROOT/'config/paths.json')
                data=Path(os.environ.get('PAPER_UPSTREAM_DATA_DIR',paths['data_dir']))
                atomic(data/'private/original_archive/worker-status.json',
                    dict(schema='original-archive-worker/1',state='startup_error',error_type=type(exc).__name__))
            except Exception:pass
    runpy.run_path(str(ROOT/'scripts/unified_launcher.py'),run_name='__main__')

if __name__=='__main__':raise SystemExit(main())
