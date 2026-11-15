from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen


def test_scheduled_service_wrapper_runs_the_read_only_api(tmp_path):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    root = Path(__file__).resolve().parents[1]
    log = tmp_path / 'service.log'
    cmd = [sys.executable, str(root / 'run_waiting_service.py'), '--log', str(log), '--',
           'serve', '--db', str(tmp_path / 'waiting.sqlite3'), '--port', str(port)]
    process = subprocess.Popen(cmd, cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    try:
        deadline = time.monotonic() + 15
        while True:
            assert process.poll() is None, log.read_text(encoding='utf8') if log.exists() else 'service exited'
            try:
                with urlopen(f'http://127.0.0.1:{port}/api/v1/local-intake/preprocessing/summary', timeout=1) as r:
                    import json
                    result = json.load(r)
                break
            except (URLError, TimeoutError):
                assert time.monotonic() < deadline, 'service did not start'
                time.sleep(0.1)
        assert result['waiting_available'] is False
        assert result['waiting_count'] is None
        assert result['preprocessing_handoff_connected'] is False
    finally:
        process.terminate()
        process.wait(timeout=10)
    assert log.is_file()
