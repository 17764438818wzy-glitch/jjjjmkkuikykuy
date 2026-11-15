"""Back up the existing files and ledgers before the paused mainline cutover."""
import ast
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from uuid import uuid4

import requests

here = Path(__file__).resolve().parent
root = here.parents[1]
parent_id = '6cb5a766-94f9-4613-aa5e-17a883be9016'
task = requests.get('http://127.0.0.1:14203/api/v1/first-step/tasks/' + parent_id, timeout=5).json()
if task['desired_state'] != 'paused' or task['inflight_requests'] != 0:
    raise ValueError('the original task is not paused and idle')

post = root / 'candidates/postprocess-prefect-20261031'
server = root / 'candidates/postprocess-server-20261101'
paperflow = root / 'prefect-paperflow'
pause = root / 'candidates/postprocess-rag-unblock-20261101/pause-boundary-20261101'
native = root / 'candidates/prefect-native-hierarchy-20261101/full-stages'
site = post / 'server_release_next/site-live-release-20261101'
mapping = []
for source, destination in [
    ('stage_runtime.py', post / 'runtime_next'), ('live_runtime.py', post / 'runtime_next'),
    ('retry_history.py', post / 'runtime_next'), ('folder_state.py', post), ('body_adapter.py', post),
    ('postprocessing_results.py', post / 'results_next'), ('projections.py', post / 'projection_next'),
    ('ticket_positions.py', post / 'server_release_next'), ('page_assembly.py', post / 'server_release_next'),
    ('folder_tickets.py', post / 'server_release_next')]:
    mapping.append((here / source, destination / source))
for name in ('remote_coordinator.py', 'remote_jobs.py', 'remote_prefect_entry.py', 'remote_runtime_adapter.py',
             'remote_scoped_job.py', 'snapshot_prepare.py', 'scoped_policy_probe.py', 'scoped_runtime_factory.py',
             'scoped_task_launcher.py', 'selected_ticket_probe.py'):
    mapping.append((here / name, server / name))
mapping.append((here / 'run_remote_postprocess_worker.py', paperflow / 'run_remote_postprocess_worker.py'))
mapping.append((root / 'candidates/postprocess-rag-unblock-20261101/body-normalization/preextract_contract.py',
                paperflow / 'paperflow_prefect/clean_overlay/preextract_contract.py'))
for name in ('first_step_control.py', 'controlled_nimble.py', 'llm_admission_state.py', 'first_step_scheduler.py'):
    mapping.append((pause / 'first-step/paperflow_prefect_v2' / name,
                    root / 'candidates/first-step-control-20261029/paperflow_prefect_v2' / name))
for name in ('flow_runtime.py', 'folder_ledger.py', 'isolated_stage.py', 'pause_admission.py'):
    mapping.append((pause / 'llm_queue' / name, paperflow / 'paperflow_prefect/llm_queue' / name))
for name in ('group_prefect.py', 'isolated_group_operation.py', 'rag_nas_serve.py', 'task_effect_admission.py'):
    mapping.append((pause / name, root / 'candidates/rag-nas-prefect-20261031' / name))
for name, destination in [('upstream_flow_binding.py', paperflow / 'paperflow_prefect/llm_queue'),
                          ('scope_verifier.py', post / 'server_release_next'),
                          ('prefect_orders.py', root / 'candidates/local-intake-live-link-20261031'),
                          ('llm_queue_summary.py', root / 'candidates/local-intake-live-link-20261031')]:
    mapping.append((native / name, destination / name))
for name in ('index.html', 'folder-ticket-dashboard.js', 'api-config.json'):
    mapping.append((here / 'frontend' / name, site / name))

prepared = json.loads((here / 'prepared-server.json').read_bytes())
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
if (sha(here / 'body_adapter.py') != prepared['body_sha256'] or
        sha(pause.parent / 'body-normalization/preextract_contract.py') != prepared['validator_sha256']):
    raise ValueError('server and local postprocessing sources differ')
for source, _ in mapping:
    if source.suffix == '.py':
        ast.parse(source.read_text(encoding='utf8'), filename=str(source))

bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_bytes())
config = json.loads(Path(bindings['remote_config']).read_bytes())
journal = Path(config['journal_database']).resolve(strict=True)
with sqlite3.connect(journal.as_uri() + '?mode=ro', uri=True) as db:
    control = Path(dict(db.execute('SELECT name,value FROM journal_settings'))['control_database']).resolve(strict=True)
with sqlite3.connect(control.as_uri() + '?mode=ro', uri=True) as db:
    if db.execute('SELECT desired_state,start_pending FROM tasks WHERE task_id=?', (parent_id,)).fetchone() != ('paused', 0):
        raise ValueError('the original control ledger is not paused')

backup = paperflow / 'runtime/installations' / ('prefect-continuation-' + uuid4().hex[:12])
backup.mkdir(parents=True)
plan = []
for source, destination in mapping:
    old_sha = sha(destination) if destination.is_file() else None
    if old_sha:
        saved = backup / 'files' / destination.relative_to(root)
        saved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(destination, saved)
    plan.append(dict(source=str(source), destination=str(destination), source_sha256=sha(source), old_sha256=old_sha))
for name, path in [('control.sqlite3', control), ('remote-intents.sqlite3', journal)]:
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as original, sqlite3.connect(backup / name) as saved:
        original.backup(saved)
(backup / 'plan.json').write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding='utf8')
(here / 'local-cutover.json').write_text(json.dumps(dict(backup=str(backup), control=str(control), journal=str(journal),
    journal_id=config['journal_id'], plan=str(backup / 'plan.json')), ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps({'backup': str(backup), 'files': len(plan), 'databases_backed_up': 2, 'live_sources_changed': False}))
