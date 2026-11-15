"""Loopback server resource display using the existing read-only SSH observer."""

import getpass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit


HOST = '192.168.1.120'
USERNAME = 'wzy'
ALLOWED_ORIGINS = {'http://127.0.0.1:8795', 'http://localhost:8795'}
OBSERVER = Path(__file__).resolve().parents[3] / '看板' / 'dashboard' / 'server_monitor.py'
NATIVE_CONFIG = Path(__file__).resolve().parents[2] / 'prefect-paperflow' / 'runtime' / 'rag-nas-test' / 'native-monitor.json'
FIELDS = ('cpu_percent', 'memory_percent', 'memory_used_bytes', 'memory_total_bytes',
          'gpu_percent', 'gpu_memory_used_mib', 'gpu_memory_total_mib')


def number(value, maximum=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value < 0 or (maximum is not None and value > maximum):
        return None
    return value


def normalize(sample):
    result = dict.fromkeys(FIELDS)
    if not isinstance(sample, dict):
        return result
    result['cpu_percent'] = number(sample.get('cpu_percent'), 100)
    total, available = number(sample.get('memory_total')), number(sample.get('memory_available'))
    if total and available is not None and available <= total:
        used = total - available
        result.update(memory_percent=round(100 * used / total, 1),
                      memory_used_bytes=used, memory_total_bytes=total)
    gpus = sample.get('gpus')
    if sample.get('gpu_status') == 'ok' and isinstance(gpus, list) and gpus and isinstance(gpus[0], dict):
        gpu = gpus[0]
        result['gpu_percent'] = number(gpu.get('utilization'), 100)
        capacity, used = number(gpu.get('total')), number(gpu.get('used'))
        if capacity and used is not None and used <= capacity:
            result['gpu_memory_used_mib'] = used / 1024**2
            result['gpu_memory_total_mib'] = capacity / 1024**2
        result['gpu_count'] = len(gpus)
    return result


class ResourceSamples:
    def __init__(self, clock=time.monotonic, max_age=3.0):
        self.clock, self.max_age = clock, max_age
        self.lock = threading.Lock()
        self.data, self.observed_at = None, None

    def update(self, sample):
        data = normalize(sample)
        with self.lock:
            self.data, self.observed_at = data, self.clock()

    def clear(self):
        with self.lock:
            self.data, self.observed_at = None, None

    def read(self):
        with self.lock:
            age = None if self.observed_at is None else self.clock() - self.observed_at
            return dict(self.data) if self.data is not None and age is not None and 0 <= age <= self.max_age else dict.fromkeys(FIELDS)


def load_observer():
    spec = importlib.util.spec_from_file_location('paperflow_existing_server_monitor', OBSERVER)
    if spec is None or spec.loader is None:
        raise RuntimeError('Server observer unavailable')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sample_loop(password, samples):
    observer = load_observer()
    client = None
    next_native = 0.0
    while True:
        started = time.monotonic()
        try:
            if client is None or client.get_transport() is None or not client.get_transport().is_active():
                client = observer.connect(HOST, USERNAME, password=password)
            samples.update(observer.observe(client))
            if time.monotonic() >= next_native:
                observer.publish_native_runtime(client, NATIVE_CONFIG, password=password, timeout=4.)
                next_native = time.monotonic() + 3.
            delay = max(0.05, 1.0 - (time.monotonic() - started))
        except Exception:
            samples.clear()
            if client is not None:
                client.close()
            client = None
            delay = 10.0
        time.sleep(delay)


SAMPLES = ResourceSamples()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        origin = self.headers.get('Origin')
        if origin not in ALLOWED_ORIGINS or urlsplit(self.path).path != '/api/server-resources':
            self.send_error(404)
            return
        body = json.dumps(SAMPLES.read(), allow_nan=False).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Access-Control-Allow-Origin', origin)
        self.send_header('Vary', 'Origin')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def launch():
    password = getpass.getpass('SSH password: ')
    if not password:
        raise SystemExit('No password supplied')
    process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--serve'],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    try:
        process.stdin.write((password + '\n').encode('utf-8'))
        process.stdin.close()
    except Exception:
        process.terminate()
        raise
    print('monitor pid=' + str(process.pid))


if __name__ == '__main__':
    if sys.argv[1:] == ['--launch']:
        launch()
    elif sys.argv[1:] == ['--serve']:
        password = sys.stdin.readline().rstrip('\r\n')
        if not password:
            raise SystemExit('No password supplied')
        threading.Thread(target=sample_loop, args=(password, SAMPLES), daemon=True).start()
        ThreadingHTTPServer(('127.0.0.1', 8798), Handler).serve_forever()
    else:
        raise SystemExit('Use --launch')
