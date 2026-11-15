from pathlib import Path
import sys

here = Path(__file__).resolve().parent
workspace = here.parents[2]
server = workspace / 'candidates' / 'postprocess-server-20261101'
upstream = workspace / 'candidates' / 'postprocess-prefect-20261031'
sys.path[:0] = [str(path) for path in (here, server / 'tests', upstream / 'runtime_next',
    upstream / 'results_next', upstream, workspace / 'prefect-paperflow', server)]

import remote_coordinator
import remote_runtime_adapter
import remote_server_transport
import test_remote_coordinator
import pytest

test_remote_coordinator.coordinator = lambda: remote_coordinator
raise SystemExit(pytest.main(sys.argv[1:] or [str(here / 'test_receipt_reuse.py'), '-q']))
