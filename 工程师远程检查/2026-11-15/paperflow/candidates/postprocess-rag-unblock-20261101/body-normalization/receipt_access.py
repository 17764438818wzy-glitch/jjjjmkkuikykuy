"""Read the existing server artifact map for one registered folder receipt."""
from pathlib import Path
from contextlib import closing
import json
import sqlite3
import sys


WORKSPACE = Path(__file__).resolve().parents[2]
POSTPROCESS = WORKSPACE / 'candidates' / 'postprocess-prefect-20261031'
RELEASE = POSTPROCESS / 'server_release_next'
SERVER = WORKSPACE / 'candidates' / 'postprocess-server-20261101'
PREVIOUS = SERVER / 'optimization-20261101' / 'final'
HERE = Path(__file__).resolve().parent
FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def build():
    for path in reversed((POSTPROCESS / 'runtime_next', POSTPROCESS / 'results_next', POSTPROCESS,
                          WORKSPACE / 'prefect-paperflow', SERVER, RELEASE)):
        sys.path.insert(0, str(path))
    from release_guard import ReleaseGuard
    from release_layout import layout, PACKAGES
    from verified_ssh_transport import build as build_ssh
    from read_only_rpc import (PinnedAccessBindings, UnreadDisclosureGate, ExistingInspectVersionRouter,
                               GuardedAppDelegate, ReadOnlyRPC, command_for)
    from remote_coordinator import RemoteSubmissionJournal
    from remote_live_launch import _config
    from remote_runtime_adapter import RemoteReceiptInspector

    binding = json.loads((HERE / 'bindings.json').read_bytes())
    previous = binding
    config = _config(binding['remote_config'], binding['remote_config_sha256'])
    bundle = json.loads(Path(previous['bundle_path']).read_bytes())
    roots, required = layout()
    guard = ReleaseGuard(binding['release_inventory'], binding['release_inventory_sha256'],
                         roots=roots, required_files=required, required_packages=PACKAGES)
    guard.verify()
    connection = config['transport_config']['connection']
    with closing(sqlite3.connect(Path(config['journal_database']).resolve().as_uri() + '?mode=ro', uri=True)) as db:
        settings = dict(db.execute('SELECT name,value FROM journal_settings'))
    control = settings['control_database']
    journal = RemoteSubmissionJournal(config['journal_database'], control_database=control,
                                      admission_gate=lambda *_: None, journal_id=config['journal_id'])
    access = PinnedAccessBindings(RELEASE / 'history-access-mainline-20261101-v2.json',
        'dee6caa06cf74b9ceb74866da925e8b049c9219ede17c8616db1f3bf736fa3d3',
        server_identity=config['server_identity'])
    disclosure = UnreadDisclosureGate(bundle['unread_path'], bundle['unread_sha256'])
    router = ExistingInspectVersionRouter(previous['registry_path'], previous['registry_sha256'],
        server_identity=config['server_identity'], access_bindings=access, disclosure_gate=disclosure)
    transport = build_ssh(guard, SERVER / 'ssh_rpc_client.py', SERVER / 'ssh_rpc_process.py')
    rpc = ReadOnlyRPC(GuardedAppDelegate(transport, router, previous['bundle_path'], previous['bundle_sha256']))
    inspector = RemoteReceiptInspector(journal=journal, server_identity=config['server_identity'],
        inspect_batch=lambda requests: rpc(connection, command_for(config['transport_config'], 'inspect'),
            {'requests': requests}, timeout=90, max_bytes=16 * 1024 * 1024),
        max_summary_receipts=config['max_summary_receipts'])
    rpc.bind(database=control, inspector=inspector, transport_config=config['transport_config'], version_router=router)
    return ReceiptAccess(control, inspector)


class ReceiptAccess:
    def __init__(self, database, inspector):
        self.database, self.inspector = Path(database), inspector

    def row(self, task_id, group_id):
        with closing(sqlite3.connect(self.database.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT c.*,p.receipt_json,p.receipt_sha256 FROM postprocessing_claims c '
                'JOIN postprocessing_receipts p ON p.claim_id=c.claim_id WHERE c.task_id=? AND c.group_id=?',
                (task_id, group_id)).fetchone()
        if row is None:
            raise ValueError('the original folder receipt is unavailable')
        return dict(row)

    def artifact_map(self, task_id, group_id):
        row = self.row(task_id, group_id)
        identity = {key: row[key] for key in FIELDS}
        return self.inspector.inspect_many([(json.loads(row['receipt_json']), identity, row['claim_id'])],
                                           retain_maps=True)[0]
