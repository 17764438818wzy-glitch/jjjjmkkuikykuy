"""Read-only NAS destination status for the local 8795 dashboard."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import stat
import threading
import time
from urllib.parse import urlsplit

from host_resources_api import ALLOWED_ORIGINS


CONFIG = Path(__file__).resolve().parents[2] / 'prefect-paperflow' / 'config.json'


def empty(status='unknown'):
    return {'status': status, 'storage_total_bytes': None, 'storage_used_bytes': None,
            'storage_free_bytes': None, 'archive_write_bytes_per_second': None}


def read_nas_status(config_path=CONFIG):
    try:
        config = json.loads(config_path.read_text(encoding='utf-8'))
        destination = config['nas_destination']
        if not isinstance(destination, str) or not destination.strip():
            return empty()
    except (OSError, ValueError, TypeError, KeyError):
        return empty()

    try:
        if not stat.S_ISDIR(os.stat(destination).st_mode):
            return empty('unavailable')
    except OSError:
        return empty('unavailable')

    result = empty('connected')
    try:
        usage = shutil.disk_usage(destination)
        if usage.total > 0 and 0 <= usage.free <= usage.total:
            result.update(storage_total_bytes=usage.total,
                          storage_used_bytes=usage.total - usage.free,
                          storage_free_bytes=usage.free)
    except OSError:
        pass
    return result


class NasSamples:
    def __init__(self, clock=time.monotonic, max_age=3.0):
        self.clock, self.max_age = clock, max_age
        self.lock = threading.Lock()
        self.data, self.observed_at = None, None

    def update(self, sample, observed_at=None):
        with self.lock:
            self.data = sample
            self.observed_at = self.clock() if observed_at is None else observed_at

    def read(self):
        with self.lock:
            age = None if self.observed_at is None else self.clock() - self.observed_at
            return dict(self.data) if self.data is not None and age is not None and 0 <= age <= self.max_age else empty()


def sample_once(samples, reader=read_nas_status, clock=time.monotonic):
    started = clock()
    try:
        sample = reader()
    except Exception:
        sample = empty()
    elapsed = clock() - started
    samples.update(sample if 0 <= elapsed <= samples.max_age else empty(), observed_at=started)


def sample_loop(samples):
    while True:
        started = time.monotonic()
        sample_once(samples)
        time.sleep(max(0.05, 1.0 - (time.monotonic() - started)))


SAMPLES = NasSamples()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        origin = self.headers.get('Origin')
        if origin not in ALLOWED_ORIGINS or urlsplit(self.path).path != '/api/nas-status':
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


if __name__ == '__main__':
    threading.Thread(target=sample_loop, args=(SAMPLES,), daemon=True).start()
    ThreadingHTTPServer(('127.0.0.1', 8799), Handler).serve_forever()
