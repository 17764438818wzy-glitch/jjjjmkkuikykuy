"""Explicit source inventory for the isolated server-page release."""
from pathlib import Path


HERE = Path(__file__).resolve().parent
CANDIDATES = HERE.parents[1]
WORKSPACE = HERE.parents[2]
POSTPROCESS = HERE.parent
SERVER = CANDIDATES / 'postprocess-server-20261101'
INTAKE = CANDIDATES / 'local-intake-live-link-20261031'
PROJECT = WORKSPACE / 'prefect-paperflow'
PACKAGES = ('prefect', 'fastapi', 'uvicorn', 'psutil', 'Pillow', 'filelock', 'httpx', 'pydantic', 'paramiko')
SERVER_FILES = (
    'remote_coordinator.py', 'remote_jobs.py', 'remote_live_launch.py', 'remote_live_runtime.py',
    'remote_prefect_entry.py', 'remote_runtime_adapter.py', 'remote_server_transport.py',
    'snapshot_collect.py', 'snapshot_import.py', 'snapshot_prepare.py', 'snapshot_wire.py',
    'source_handle.py', 'ssh_rpc_client.py', 'ssh_rpc_process.py', 'windows_snapshot.py',
)


def layout(site_root=None):
    roots = [
        (HERE, ('.py', '.ps1'), False),
        (INTAKE, ('.py',), False),
        (POSTPROCESS, ('.py',), False),
        (POSTPROCESS / 'runtime_next', ('.py',), False),
        (POSTPROCESS / 'generation_next', ('.py',), False),
        (POSTPROCESS / 'results_next', ('.py',), False),
        (POSTPROCESS / 'projection_next', ('.py',), False),
        (POSTPROCESS / 'ledger_projection_next', ('.py',), False),
        (SERVER, ('.py',), False, SERVER_FILES),
        (CANDIDATES / 'first-step-control-20261029' / 'paperflow_prefect', ('.py',), True),
        (PROJECT / 'paperflow_prefect', ('.py',), True),
    ]
    required = [
        CANDIDATES / 'first-step-control-20261029' / 'runtime' / 'workspace-config.json',
        CANDIDATES / 'attachment-prefect-live-link-20261031' / 'attachment-queue-config.json',
        PROJECT / 'config.json',
        *(SERVER / name for name in SERVER_FILES),
    ]
    return roots, required
