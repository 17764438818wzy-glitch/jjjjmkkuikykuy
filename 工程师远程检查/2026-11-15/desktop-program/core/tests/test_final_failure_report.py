import json
from pathlib import Path
import sys

from test_control import manager
from test_winjob import wait_until


def test_required_stage_survives_real_worker_exit_without_private_diagnostic(manager):
    program="""
from paperflow.control_ipc import WorkerClient
from paperflow.package import PackageError
from paperflow.worker_retry import report_failure
c=WorkerClient.from_environment()
e=PackageError('private context must not be in public report','missing_files')
e.required_stage='round3_assets'
report_failure(c,e)
c.close()
"""
    manager.resume()
    execution=manager.launch('graph','owned-report',[sys.executable,'-B','-c',program],cwd=Path.cwd())
    wait_until(lambda:execution.exit_saved)
    rows=manager.ledger.rows('SELECT outcome,data FROM control_execution_results WHERE execution_id=?',(execution.execution_id,))
    assert len(rows)==1 and rows[0]['outcome']=='held'
    result=json.loads(rows[0]['data'])
    assert result['required_stage']=='round3_assets' and result['check']=='missing_files'
    assert 'private context' not in rows[0]['data']
