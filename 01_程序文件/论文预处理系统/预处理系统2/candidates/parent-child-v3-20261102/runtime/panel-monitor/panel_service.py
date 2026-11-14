"""Bridge the existing monitoring adapters to the current 8796 page."""

from http.server import ThreadingHTTPServer
import os
from pathlib import Path
import sys
import threading
import time
from urllib.parse import urlsplit


WORKSPACE = Path(r"F:\论文处理端迁移包\01_程序文件\论文预处理系统\预处理系统2")
ADAPTERS = WORKSPACE / "candidates" / "first-step-preview-8795"
sys.path.insert(0, str(ADAPTERS))

import nas_status_api as nas
import server_resources_api as server
import workbuddy_monitor as workbuddy


ORIGINS = {"http://127.0.0.1:8796", "http://localhost:8796"}
for adapter in (nas, server, workbuddy):
    adapter.ALLOWED_ORIGINS.update(ORIGINS)

SERVER_PASSWORD = Path(os.environ["LOCALAPPDATA"]) / "Paperflow" / "server-monitor-8796.dpapi"
server_lock = threading.Lock()
nas_lock = threading.Lock()
observer = server.load_observer()
ssh_client = None
server_retry_at = 0.0


def load_server_password():
    import win32crypt
    return win32crypt.CryptUnprotectData(SERVER_PASSWORD.read_bytes(), None, None, None, 1)[1].decode("utf-8")


class ServerHandler(server.Handler):
    def do_GET(self):
        global ssh_client, server_retry_at
        if (self.headers.get("Origin") in ORIGINS
                and urlsplit(self.path).path == "/api/server-resources"
                and time.monotonic() >= server_retry_at
                and server_lock.acquire(blocking=False)):
            try:
                if ssh_client is None or ssh_client.get_transport() is None or not ssh_client.get_transport().is_active():
                    ssh_client = observer.connect(server.HOST, server.USERNAME, password=load_server_password())
                server.SAMPLES.update(observer.observe(ssh_client))
            except Exception:
                server.SAMPLES.clear()
                if ssh_client is not None:
                    ssh_client.close()
                ssh_client = None
                server_retry_at = time.monotonic() + 10
            finally:
                server_lock.release()
        super().do_GET()


class NasHandler(nas.Handler):
    def do_GET(self):
        if (self.headers.get("Origin") in ORIGINS
                and urlsplit(self.path).path == "/api/nas-status"
                and nas_lock.acquire(blocking=False)):
            try:
                nas.sample_once(nas.SAMPLES)
            finally:
                nas_lock.release()
        super().do_GET()


def serve():
    http_servers = [ThreadingHTTPServer(("127.0.0.1", port), handler) for port, handler in (
        (8797, workbuddy.Handler), (8798, ServerHandler), (8799, NasHandler))]
    for http_server in http_servers[1:]:
        threading.Thread(target=http_server.serve_forever, daemon=True).start()
    http_servers[0].serve_forever()


if __name__ == "__main__":
    if sys.argv[1:] == ["--save-server-password"]:
        import getpass
        import win32crypt
        password = getpass.getpass("Server password: ")
        if not password:
            raise SystemExit("Server credential is empty")
        SERVER_PASSWORD.parent.mkdir(parents=True, exist_ok=True)
        SERVER_PASSWORD.write_bytes(win32crypt.CryptProtectData(password.encode("utf-8"), None, None, None, None, 1))
    elif sys.argv[1:] == ["--serve"]:
        serve()
    else:
        raise SystemExit("Use --serve or --save-server-password")
