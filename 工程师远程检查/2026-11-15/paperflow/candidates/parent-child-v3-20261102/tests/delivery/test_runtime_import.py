import os
from pathlib import Path
import subprocess
import sys


def test_entry_can_be_imported_before_native_release_is_bound(tmp_path):
    candidate = Path(__file__).resolve().parents[2]
    project = candidate.parents[1] / 'prefect-paperflow'
    environment = dict(os.environ, PYTHONPATH=os.pathsep.join((str(candidate), str(project))))
    result = subprocess.run([sys.executable, '-c', 'import paperflow_v3.delivery.runtime'],
                            cwd=tmp_path, env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
