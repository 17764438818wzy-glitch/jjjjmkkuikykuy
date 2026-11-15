"""Synthetic worker: Wine business scripts and real CPU-only server tokenizer."""
import json
import os
from pathlib import Path, PureWindowsPath
import secrets
import socket
import subprocess
import sys
import threading
import time

from tokenizer_service import serve_client


def main():
    base, prefix, snapshot, workspace = map(Path, sys.argv[1:5])
    sys.path.insert(0, str(prefix / 'drive_c/paperflow/release/core'))
    from paperflow.embedding_counter import OfflineCounter
    config = json.loads((base / 'tokenizer/server-counter-config.json').read_bytes())
    policy = config['policy']
    with OfflineCounter(**config['counter'], unified=True,
                        **{k: policy[k] for k in ('model_sha256', 'libllama_sha256', 'cl100k_sha256')}) as counter:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(2)
            listener.settimeout(.2)
            secret = secrets.token_urlsafe(32)
            env = dict(os.environ, WINEPREFIX=str(prefix), WINEDEBUG='-all', DISPLAY='', PYTHONUTF8='1',
                WINEDLLPATH=str(base / 'wine-runtime/usr/lib/x86_64-linux-gnu/wine'), CUDA_VISIBLE_DEVICES='',
                PAPERFLOW_PROBE_WORKSPACE=str(workspace), PAPERFLOW_COUNTER_PORT=str(listener.getsockname()[1]),
                PAPERFLOW_COUNTER_SECRET=secret,
                PAPERFLOW_COUNTER_IDENTITY=json.dumps(counter.identity, separators=(',', ':')),
                PAPERFLOW_COUNTER_MODEL_BUDGET=str(config['model_budget']),
                PAPERFLOW_COUNTER_CL_LIMIT=str(min(policy['pipeline_limit'], policy['adapter_limit'])))
            cmd = [str(base / 'wine-runtime/usr/lib/wine/wine64'), r'C:\Python\python.exe', '-B',
                   r'C:\paperflow\server_wire_folder_probe.py', r'F:\wire-manifest.json',
                   r'F:\pfwire', r'F:\wire-result.json']
            with (snapshot / 'worker.stdout').open('xb') as output, (snapshot / 'worker.stderr').open('xb') as error:
                # Wine's Windows Python requires pipes for these handles;
                # concurrently drain them to bounded-memory local log files.
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
                def drain(source, target):
                    for chunk in iter(lambda: source.read(64 * 1024), b''):
                        target.write(chunk)
                        target.flush()
                drains = [threading.Thread(target=drain, args=(process.stdout, output), daemon=True),
                          threading.Thread(target=drain, args=(process.stderr, error), daemon=True)]
                for thread in drains:
                    thread.start()
                deadline = time.monotonic() + 120
                try:
                    while process.poll() is None:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('synthetic worker exceeded deadline')
                        try:
                            conn, address = listener.accept()
                        except TimeoutError:
                            continue
                        with conn:
                            if address[0] != '127.0.0.1':
                                raise ValueError('tokenizer connection was not local')
                            serve_client(conn, counter, secret)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                    for thread in drains:
                        thread.join(timeout=5)
                    if any(thread.is_alive() for thread in drains):
                        raise RuntimeError('synthetic process output did not close')
                if process.returncode != 0:
                    raise RuntimeError('synthetic Wine business processing failed')
                if (counter.runtime.get('vocab_only') is not True
                        or counter.runtime.get('context_created') is not False
                        or counter.runtime.get('inference_requests') != 0):
                    raise RuntimeError('unexpected tokenizer runtime')


if __name__ == '__main__':
    main()
