import importlib.util
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
from urllib.request import urlopen

sys.dont_write_bytecode = True
root = Path(__file__).resolve().parents[2]
release = root / 'candidates/postprocess-prefect-20261031/server_release_next'
binding = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_text(encoding='utf8'))
args = SimpleNamespace(
    release_manifest=binding['release_inventory'], release_manifest_sha256=binding['release_inventory_sha256'],
    guard_sha256='9343b0d7367926fd4365a4672ee51617b60d4e77f30e7f2642e481dbb7e8d2be',
    layout_sha256='76e10389f88c377746112fd63f7959e09be1f36179aab9ebdb57b001a002b26a',
    remote_config=binding['remote_config'], remote_config_sha256=binding['remote_config_sha256'],
    history_registry=binding['registry_path'], history_registry_sha256=binding['registry_sha256'],
    history_access=str(release / 'history-access-mainline-20261101-v2.json'),
    history_access_sha256='dee6caa06cf74b9ceb74866da925e8b049c9219ede17c8616db1f3bf736fa3d3',
    history_unread_script='/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v3/server_unread_scope_query.py',
    history_unread_script_sha256='f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521',
    guard_bundle=binding['bundle_path'], guard_bundle_sha256=binding['bundle_sha256'],
    db=str(root / 'local-intake-runtime/waiting-pool.sqlite3'), max_age_seconds=60,
    closed_projection=False, partial_observation=True)
spec = importlib.util.spec_from_file_location('diagnostic_waiting_cli', release / 'waiting_cli.py')
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)
wrapper, mode = cli.create_release_app(args)
if mode != 'server_projection':
    raise RuntimeError('the real read-only projection did not load')
route = next(row for row in wrapper.app.routes if row.path == '/api/v1/local-intake/postprocessing/positions')
closure = dict(zip(route.endpoint.__code__.co_freevars, (cell.cell_contents for cell in route.endpoint.__closure__)))
projector = closure['projector']
positions = sys.modules['_release_page_ticket_positions']
if '--compare' in sys.argv:
    path = root / 'candidates/postprocess-rag-unblock-20261101/frontend-position-simple/ticket_positions.py'
    spec = importlib.util.spec_from_file_location('simple_position_candidate', path)
    candidate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(candidate)
    for _ in range(3):
        for name, module in (('current', positions), ('candidate', candidate)):
            started = time.monotonic()
            result = module.observe(projector)
            print(json.dumps({'version': name, 'seconds': round(time.monotonic() - started, 3),
                'counts': result['counts'], 'unresolved': result['unresolved_count']}, ensure_ascii=False), flush=True)
    raise SystemExit(0)
samples, failures = 0, []
until = time.monotonic() + 115
print('read-only observer loaded', flush=True)
while time.monotonic() < until:
    started = time.monotonic()
    with urlopen('http://127.0.0.1:14206/api/v1/local-intake/postprocessing/positions', timeout=5) as response:
        value = json.load(response)
    samples += 1
    if value.get('available') is not True:
        probe = time.monotonic()
        try:
            result = positions.observe(projector)
            item = {'direct_available': result.get('available'), 'completed': result['counts']['completed'],
                    'unresolved': result['unresolved_count'], 'seconds': round(time.monotonic() - probe, 3)}
        except Exception as exc:
            item = {'error_type': type(exc).__name__, 'error': str(exc)[:180],
                    'seconds': round(time.monotonic() - probe, 3)}
        failures.append(item)
        print(json.dumps({'sample': samples, 'on_unavailable': item}, ensure_ascii=False), flush=True)
    time.sleep(max(0, 1 - (time.monotonic() - started)))
print(json.dumps({'samples': samples, 'unavailable_samples': len(failures), 'probes': failures}, ensure_ascii=False), flush=True)
