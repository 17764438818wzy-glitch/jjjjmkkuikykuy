"""Run the pinned Windows-path business entry with a server CPU tokenizer.

This is an executor primitive, not a scheduler or service installation. The
caller must supervise the whole process tree and provide one prepared prefix.
"""
import json
import os
from pathlib import Path, PureWindowsPath
import secrets
import socket
import subprocess
import sys
import threading
import time

from remote_jobs import _validated
from snapshot_wire import snapshot_sha
from server_path_guard import verify_snapshot_path
from tokenizer_service import serve_client


def _serve_connection(connection, counter, secret, *, path_verifier, snapshot_sha256):
    """A rejected local connection must not by itself kill the real worker."""
    try:
        serve_client(connection, counter, secret, path_verifier=path_verifier,
                     snapshot_sha256=snapshot_sha256)
        return True
    except (ValueError, RuntimeError, OSError):
        # The rejected connection is closed by the caller. A legitimate client
        # receiving EOF will fail its own operation instead of forging success.
        return False


def run(*, base, snapshot, prefix, timeout_seconds):
    base, snapshot, prefix = map(Path, (base, snapshot, prefix))
    manifest = json.loads((snapshot / 'transport-manifest.json').read_bytes())
    job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                           'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    _validated(job)
    if not 1 <= timeout_seconds <= 7200:
        raise ValueError('invalid worker timeout')
    core = PureWindowsPath(manifest['paperflow_root_windows'])
    workspace = PureWindowsPath(manifest['business_workspace_windows'])
    if core.drive != 'F:' or workspace.drive != 'F:':
        raise ValueError('unsupported business drive')
    core_linux = snapshot.joinpath('drives', 'f', *core.parts[1:])
    sys.path.insert(0, str(core_linux))
    from paperflow.embedding_counter import OfflineCounter
    config = json.loads((base / 'tokenizer/server-counter-config.json').read_bytes())
    policy = config['policy']
    entry = workspace / 'candidates/postprocess-server-20261101/server_wine_entry.py'
    cmd = [str(base / 'wine-runtime/usr/lib/wine/wine64'), r'C:\Python\python.exe', '-B',
           str(entry), r'F:\wire-manifest.json', r'F:\wire-result.json']
    with OfflineCounter(**config['counter'], unified=True,
                        **{k: policy[k] for k in ('model_sha256', 'libllama_sha256', 'cl100k_sha256')}) as counter:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(2)
            listener.settimeout(.2)
            env = dict(os.environ, WINEPREFIX=str(prefix), WINEDEBUG='-all', DISPLAY='', PYTHONUTF8='1',
                WINEDLLPATH=str(base / 'wine-runtime/usr/lib/x86_64-linux-gnu/wine'), CUDA_VISIBLE_DEVICES='',
                PAPERFLOW_COUNTER_PORT=str(listener.getsockname()[1]),
                PAPERFLOW_COUNTER_SECRET=secrets.token_urlsafe(32),
                PAPERFLOW_SNAPSHOT_SHA256=job['snapshot_sha256'],
                PAPERFLOW_COUNTER_IDENTITY=json.dumps(counter.identity, separators=(',', ':')),
                PAPERFLOW_COUNTER_MODEL_BUDGET=str(config['model_budget']),
                PAPERFLOW_COUNTER_CL_LIMIT=str(min(policy['pipeline_limit'], policy['adapter_limit'])))
            with (snapshot / 'service-worker.stdout').open('xb') as out, \
                 (snapshot / 'service-worker.stderr').open('xb') as err:
                process = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, env=env)
                def drain(source, target):
                    for chunk in iter(lambda: source.read(64 * 1024), b''):
                        target.write(chunk)
                        target.flush()
                threads = [threading.Thread(target=drain, args=(process.stdout, out), daemon=True),
                           threading.Thread(target=drain, args=(process.stderr, err), daemon=True)]
                for thread in threads:
                    thread.start()
                deadline = time.monotonic() + timeout_seconds
                try:
                    while process.poll() is None:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('server business worker exceeded its deadline')
                        try:
                            connection, address = listener.accept()
                        except TimeoutError:
                            continue
                        with connection:
                            if address[0] != '127.0.0.1':
                                raise ValueError('counter request is not local')
                            _serve_connection(connection, counter, env['PAPERFLOW_COUNTER_SECRET'],
                                path_verifier=lambda path: verify_snapshot_path(snapshot, path),
                                snapshot_sha256=job['snapshot_sha256'])
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                    for thread in threads:
                        thread.join(timeout=5)
                    if any(thread.is_alive() for thread in threads):
                        raise RuntimeError('server business worker output did not close')
                if process.returncode != 0:
                    raise RuntimeError('server business worker did not complete')
                if (counter.runtime.get('vocab_only') is not True
                        or counter.runtime.get('context_created') is not False
                        or counter.runtime.get('inference_requests') != 0):
                    raise RuntimeError('unexpected server tokenizer runtime')
                with (snapshot / 'tokenizer-runtime.json').open('x', encoding='utf8') as stream:
                    json.dump({key: counter.runtime.get(key) for key in
                               ('vocab_only', 'context_created', 'inference_requests', 'unified')}, stream)
                    stream.flush()
                    os.fsync(stream.fileno())


if __name__ == '__main__':
    if len(sys.argv) != 5:
        raise SystemExit('base, snapshot, private prefix and timeout are required')
    os.umask(0o077)
    run(base=sys.argv[1], snapshot=sys.argv[2], prefix=sys.argv[3], timeout_seconds=int(sys.argv[4]))
