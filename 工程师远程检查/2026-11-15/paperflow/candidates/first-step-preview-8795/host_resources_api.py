"""Read-only local host resource samples for the 8795 preview."""

import csv
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import math
import subprocess
from urllib.parse import urlsplit

import psutil


ALLOWED_ORIGINS = {"http://127.0.0.1:8795", "http://localhost:8795"}


def gpu_number(value, maximum=None):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result) or result < 0 or (maximum is not None and result > maximum):
        return None
    return result


def read_gpu():
    unavailable = {"gpu_name": None, "gpu_percent": None, "gpu_memory_used_mib": None, "gpu_memory_total_mib": None}
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
            timeout=2,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        row = next(csv.reader(io.StringIO(result.stdout), skipinitialspace=True))
        return {
            "gpu_name": row[0].strip() or None,
            "gpu_percent": gpu_number(row[1], maximum=100),
            "gpu_memory_used_mib": gpu_number(row[2]),
            "gpu_memory_total_mib": gpu_number(row[3]),
        }
    except (OSError, subprocess.SubprocessError, StopIteration, IndexError, csv.Error):
        return unavailable


def read_host_resources():
    try:
        cpu = psutil.cpu_percent(interval=0.2)
    except (OSError, RuntimeError, psutil.Error):
        cpu = None
    try:
        memory = psutil.virtual_memory()
        memory_values = {
            "memory_percent": memory.percent,
            "memory_used_bytes": memory.used,
            "memory_total_bytes": memory.total,
        }
    except (OSError, RuntimeError, psutil.Error):
        memory_values = {"memory_percent": None, "memory_used_bytes": None, "memory_total_bytes": None}
    return {"cpu_percent": cpu, **memory_values, **read_gpu()}


class HostResourcesHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if urlsplit(self.path).path != "/api/host-resources":
            self.send_error(404)
            return
        body = json.dumps(read_host_resources(), allow_nan=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        origin = self.headers.get("Origin")
        if origin in ALLOWED_ORIGINS:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 8796), HostResourcesHandler).serve_forever()
