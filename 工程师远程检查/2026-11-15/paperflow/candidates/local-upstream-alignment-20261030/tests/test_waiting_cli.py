import json
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.request import urlopen


def test_standalone_waiting_service_binds_loopback_and_exposes_unknown(tmp_path):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    script = Path(__file__).resolve().parents[1] / 'waiting_cli.py'
    process = subprocess.Popen(
        [sys.executable, '-B', str(script), 'serve', '--db', str(tmp_path / 'queue.sqlite3'),
         '--port', str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    try:
        deadline = time.monotonic() + 8
        while True:
            if process.poll() is not None:
                raise AssertionError('synthetic server exited: ' + process.stderr.read().decode('utf8', 'replace'))
            try:
                with urlopen(f'http://127.0.0.1:{port}/api/v1/local-intake/preprocessing/summary', timeout=1) as response:
                    data = json.loads(response.read())
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.05)
        assert data['waiting_count'] is None
        assert data['waiting_available'] is False
        assert 'processing_count' not in data
    finally:
        process.terminate()
        process.communicate(timeout=5)
