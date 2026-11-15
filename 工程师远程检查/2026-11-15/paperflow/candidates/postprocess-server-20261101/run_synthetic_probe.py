"""Execute one synthetic test on the server, with original evidence unchanged."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import socket
import subprocess
import sys
import time


base = Path('/home/wzy/paperflow-postprocess/candidate-20261101')
prefix = base / 'wine-test-prefix4'
case = sys.argv[1] if len(sys.argv) >= 2 else '1'
mode = sys.argv[2] if len(sys.argv) == 3 else ''
if mode not in {'', 'real-counter', 'mirrored', 'folder'}:
    raise ValueError('unknown synthetic probe mode')
use_real_counter = mode in {'real-counter', 'mirrored', 'folder'}
if case not in {'1', '2'}:
    raise ValueError('unknown synthetic case')
snapshot = base / ('synthetic-snapshot-utf8-' + case + ('b' if mode == 'folder' else ''))
code = prefix / 'drive_c/paperflow'


def check_entries(root, entries):
    for row in entries:
        relative = PurePosixPath(row.get('relative', row.get('path', '')))
        if relative.is_absolute() or '..' in relative.parts or not relative.parts:
            raise ValueError('invalid synthetic snapshot path')
        path = root.joinpath(*relative.parts)
        if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('synthetic snapshot path escaped')
        if hashlib.sha256(path.read_bytes()).hexdigest() != row['sha256']:
            raise ValueError('synthetic snapshot changed')
        if 'bytes' in row and path.stat().st_size != row['bytes']:
            raise ValueError('synthetic snapshot size changed')


entries = json.loads((snapshot / 'transport-manifest.json').read_text(encoding='utf8'))
sources = json.loads((code / 'source-manifest.json').read_text(encoding='utf8'))
check_entries(snapshot, entries)
check_entries(code, sources)
if mode in {'mirrored', 'folder'}:
    mirrored_sources = json.loads((snapshot / 'source-code-manifest.json').read_text(encoding='utf8'))
    check_entries(snapshot, mirrored_sources)
input_rows = [entry for entry in entries if entry['source'].endswith('server-input.json')]
if len(input_rows) != 1:
    raise ValueError('synthetic input identity is ambiguous')
env = dict(os.environ, WINEPREFIX=str(prefix), WINEDEBUG='-all', DISPLAY='', PYTHONUTF8='1',
           WINEDLLPATH=str(base / 'wine-runtime/usr/lib/x86_64-linux-gnu/wine'),
           CUDA_VISIBLE_DEVICES='')
cmd = [str(base / 'wine-runtime/usr/lib/wine/wine64'), r'C:\Python\python.exe', '-B',
       r'C:\paperflow\server_synthetic_probe.py', input_rows[0]['source'],
       'C:\\paperflow-output\\case' + case + ('-real' if use_real_counter else '')]
if mode in {'mirrored', 'folder'}:
    # The transport manifest preserves the Windows name; do not derive it from
    # the Linux spelling.  The test data lives under the existing workspace.
    from pathlib import PureWindowsPath
    windows_workspace = PureWindowsPath(input_rows[0]['source']).parents[4]
    env['PAPERFLOW_PROBE_WORKSPACE'] = str(windows_workspace)
    probe_root = windows_workspace / 'candidates/postprocess-server-20261101'
    cmd[3] = str(probe_root / ('server_full_folder_probe.py' if mode == 'folder' else 'server_synthetic_probe.py'))
    cmd[5] = (r'F:\pf-test\c' + case if mode == 'folder'
              else str(probe_root / 'synthetic-results-real' / ('case-' + case)))
if use_real_counter:
    from tokenizer_service import serve_client
    sys.path.insert(0, str(code / 'release/core'))
    from paperflow.embedding_counter import OfflineCounter
    config = json.loads((base / 'tokenizer/server-counter-config.json').read_text(encoding='utf8'))
    policy = config['policy']
    with OfflineCounter(**config['counter'], unified=True,
                        **{k: policy[k] for k in ('model_sha256', 'libllama_sha256', 'cl100k_sha256')}) as counter:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(2)
            listener.settimeout(0.2)
            secret = secrets.token_urlsafe(32)
            env.update(PAPERFLOW_COUNTER_PORT=str(listener.getsockname()[1]),
                       PAPERFLOW_COUNTER_SECRET=secret,
                       PAPERFLOW_COUNTER_IDENTITY=json.dumps(counter.identity, separators=(',', ':')),
                       PAPERFLOW_COUNTER_MODEL_BUDGET=str(config['model_budget']),
                       PAPERFLOW_COUNTER_CL_LIMIT=str(min(policy['pipeline_limit'], policy['adapter_limit'])))
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            deadline = time.monotonic() + 120
            try:
                while process.poll() is None:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('synthetic server probe exceeded its deadline')
                    try:
                        conn, address = listener.accept()
                    except TimeoutError:
                        continue
                    with conn:
                        if address[0] != '127.0.0.1':
                            raise ValueError('tokenizer connection was not local')
                        serve_client(conn, counter, secret)
                stdout, stderr = process.communicate(timeout=5)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
            run = subprocess.CompletedProcess(cmd, process.returncode,
                stdout.decode('utf8', errors='replace'), stderr.decode('utf8', errors='replace'))
            print('COUNTER_MODE', json.dumps({k: counter.runtime.get(k) for k in
                  ('vocab_only', 'context_created', 'inference_requests', 'unified')}))
else:
    run = subprocess.run(cmd, capture_output=True, text=True, encoding='utf8', errors='replace',
                         timeout=120, env=env)
print('PROBE_EXIT', run.returncode)
print('PROBE_OUTPUT', run.stdout[-4000:])
print('PROBE_ERROR', run.stderr[-4000:])
check_entries(snapshot, entries)
check_entries(code, sources)
if mode in {'mirrored', 'folder'}:
    check_entries(snapshot, mirrored_sources)
print('INPUT_AND_CODE_HASHES_UNCHANGED', True)
sys.exit(run.returncode)
