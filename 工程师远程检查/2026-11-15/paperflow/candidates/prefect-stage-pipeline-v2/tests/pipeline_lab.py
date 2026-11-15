"""Disposable native Prefect 3.9 subprocess laboratory; never imports app flows.

All children deny non-loopback TCP and use candidate-only absolute storage.
No SQL repair/requeue logic: adverse native behavior must remain observable.
"""
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

CANDIDATE = Path(__file__).resolve().parents[1]


def _json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2), encoding='utf-8')
    temporary.replace(path)


def _http(url, method='GET', data=None):
    body = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(url, data=body, method=method,
                                     headers={'Content-Type': 'application/json'})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=8) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def _port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _deny_external_network(*,extra_loopback_ports=()):
    from urllib.parse import urlparse
    allowed_ports = {urlparse(os.environ[key]).port for key in
                     ('PREFECT_API_URL', 'PIPELINE_FAKE_URL')}
    allowed_ports.update(extra_loopback_ports)
    # Windows asyncio socketpair connects to a loopback listener created inside
    # this same process. Such owned ephemeral ports are safe to admit.
    owned_ports = set()
    original_bind = socket.socket.bind
    original_connect, original_connect_ex = socket.socket.connect, socket.socket.connect_ex
    def check(address):
        if isinstance(address, tuple):
            if address[0] not in ('127.0.0.1', '::1', 'localhost'):
                raise PermissionError('pipeline lab denied non-loopback destination')
            if address[1] not in allowed_ports | owned_ports:
                raise PermissionError('pipeline lab denied unlisted loopback service')
    def bind(sock, address):
        if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
            raise PermissionError('pipeline lab denied non-loopback bind')
        result = original_bind(sock, address)
        if isinstance(address, tuple):
            owned_ports.add(sock.getsockname()[1])
        return result
    def connect(sock, address):
        check(address)
        return original_connect(sock, address)
    def connect_ex(sock, address):
        check(address)
        return original_connect_ex(sock, address)
    socket.socket.connect, socket.socket.connect_ex = connect, connect_ex
    socket.socket.bind = bind


@dataclass
class LabEndpoints:
    root: Path
    api_url: str
    fake_url: str
    allowed_network: list[str]
    storage_root: Path
    env: dict
    sdk_version: str
    server: object = None
    fake_server: object = None
    children: list = field(default_factory=list)
    logs: list = field(default_factory=list)
    retry_wait_sample: dict = field(default_factory=dict)

    def launch(self, mode, *args):
        log = (self.root / f'{mode}-{uuid.uuid4().hex[:8]}.log').open('w', encoding='utf-8')
        self.logs.append(log)
        token = uuid.uuid4().hex
        process = subprocess.Popen([sys.executable, '-u', str(Path(__file__).resolve()),
                                    mode, *map(str, args)], cwd=self.root,
                                   env=dict(self.env, PIPELINE_LAB_LAUNCH_TOKEN=token),
                                   stdout=log, stderr=subprocess.STDOUT,
                                   creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        self.children.append(process)
        process.lab_token = token
        return process

    def stop(self, process):
        if process.poll() is None:
            # Windows venv's python.exe launches an actual Python child.
            # Terminate only this owned subprocess tree, never a PID-name sweep.
            import psutil
            owned = psutil.Process(process.pid)
            descendants = owned.children(recursive=True)
            for child in reversed(descendants):
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            try:
                owned.kill()
            except psutil.NoSuchProcess:
                pass
            psutil.wait_procs(descendants, timeout=10)
            process.wait(timeout=15)

    def wait_file(self, path, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if Path(path).is_file():
                return json.loads(Path(path).read_text(encoding='utf-8'))
            time.sleep(.2)
        raise TimeoutError(f'No lab evidence: {path}')

    def start_server(self):
        self.server = self.launch('server', self.api_url.split(':')[-1].split('/')[0])
        deadline = time.monotonic() + 75
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                raise RuntimeError('Lab server exited; inspect server log')
            try:
                _http(self.api_url + '/health')
                return
            except Exception:
                time.sleep(.5)
        raise TimeoutError('Lab server health startup timeout')

    def restart_server(self):
        self.stop(self.server)
        self.start_server()

    def worker(self, kind):
        return self.launch('worker', kind)

    def wait_worker(self, process):
        return self.wait_file(self.root / f'worker-{process.lab_token}.json')

    def submit(self, kind, token, value):
        evidence = self.root / f'submit-{uuid.uuid4().hex}.json'
        process = self.launch('submit', kind, token, value, evidence)
        process.wait(timeout=40)
        if process.returncode:
            raise RuntimeError('Lab submission failed; inspect submit log')
        result = self.wait_file(evidence)
        return dict(result, exit_code=process.returncode)

    def task_state(self, task_run_id):
        return _http(self.api_url + '/task_runs/' + task_run_id)

    def result(self, task_run_id, timeout=60, required=True):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.task_state(task_run_id)
            if state['state_type'] == 'COMPLETED':
                path = self.root / f'read-{uuid.uuid4().hex}.json'
                reader = self.launch('read', task_run_id, path)
                reader.wait(timeout=35)
                if reader.returncode:
                    raise RuntimeError('Independent result reader failed; inspect read log')
                return self.wait_file(path)
            if state['state_type'] in ('FAILED', 'CRASHED'):
                raise AssertionError(f'Lab task failed: {state["state"]}')
            time.sleep(.4)
        if required:
            raise TimeoutError(f'Native task not consumed/completed: {task_run_id}')
        return None

    def wait_event(self, token, event):
        result = self.wait_file(self.root / f'{token}-{event}.json')
        if token == 'retry-first' and event == 'attempt-1':
            # The exception leaves the HTTP context before native AwaitingRetry.
            time.sleep(.6)
            self.retry_wait_sample = _http(self.api_url + '/v2/concurrency_limits/lab-http')
        return result

    def http_wait_slots(self):
        return {'active_slots': self.retry_wait_sample['active_slots'],
                'limit': self.retry_wait_sample['limit']}

    def fake_request(self, value):
        return _http(self.fake_url, 'POST', value)

    def measure_lease_recycle(self, lease_seconds, timeout):
        _http(self.api_url + '/v2/concurrency_limits/', 'POST',
              {'name': 'lab-crash-lease', 'limit': 1, 'active': True})
        evidence = self.root / 'lease-acquired.json'
        holder = self.launch('lease', lease_seconds, evidence)
        acquired = self.wait_file(evidence)
        slots = _http(self.api_url + '/v2/concurrency_limits/lab-crash-lease')['active_slots']
        self.stop(holder)
        started = time.monotonic()
        samples = []
        reclaimed = False
        while time.monotonic() - started < timeout:
            current = _http(self.api_url + '/v2/concurrency_limits/lab-crash-lease')['active_slots']
            elapsed = time.monotonic() - started
            samples.append({'seconds': round(elapsed, 3), 'active_slots': current})
            if current == 0:
                reclaimed = True
                break
            time.sleep(1)
        new_slots = None
        if reclaimed:
            new_lease = _http(self.api_url + '/v2/concurrency_limits/increment-with-lease',
                              'POST', {'names': ['lab-crash-lease'], 'slots': 1,
                                       'mode': 'concurrency', 'lease_duration': 60})
            new_slots = _http(self.api_url + '/v2/concurrency_limits/lab-crash-lease')['active_slots']
            _http(self.api_url + '/v2/concurrency_limits/decrement-with-lease', 'POST',
                  {'lease_id': new_lease['lease_id']})
        observation = {'lease_seconds': lease_seconds, 'reclaimed': reclaimed,
                       'holder_pid': holder.pid, 'holder_killed': holder.poll() is not None,
                       'slots_after_acquire': slots, 'new_holder_slots': new_slots,
                       'recycle_seconds': round(elapsed, 3), 'samples': samples,
                       'acquired': acquired}
        _json(self.root / 'lease-observation.json', observation)
        return observation

    def close(self):
        for process in self.children:
            self.stop(process)
        if self.fake_server:
            self.fake_server.shutdown()
            self.fake_server.server_close()
        for log in self.logs:
            log.close()


def start_lab(root: Path) -> LabEndpoints:
    root = root.resolve()
    if not root.is_relative_to(CANDIDATE / 'runtime/development/pipeline-v2'):
        raise ValueError('Lab writes must remain under candidate development root')
    # Each execution is new; preserves all earlier evidence instead of deleting.
    root = root / time.strftime('%Y%m%d-%H%M%S')
    root.mkdir(parents=True, exist_ok=False)
    api_url, fake_url = f'http://127.0.0.1:{_port()}/api', f'http://127.0.0.1:{_port()}'
    storage = root / 'shared-parameters-results'
    storage.mkdir()
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('PREFECT_') and key.upper() not in
           ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY')}
    env.update(PREFECT_API_URL=api_url, PREFECT_HOME=str(root / 'prefect-home'),
               PREFECT_PROFILES_PATH=str(root / 'profiles.toml'),
               PREFECT_LOCAL_STORAGE_PATH=str(storage),
               PREFECT_RESULTS_LOCAL_STORAGE_PATH=str(storage),
               PREFECT_RESULTS_PERSIST_BY_DEFAULT='true',
               PREFECT_TASKS_SCHEDULING_DEFAULT_STORAGE_BLOCK='local-file-system/lab-shared',
               PREFECT_SERVER_DATABASE_CONNECTION_URL='sqlite+aiosqlite:///' +
               str(root / 'prefect.db').replace('\\', '/'),
               PREFECT_SERVER_ANALYTICS_ENABLED='false',
               PREFECT_SERVER_ALLOW_EPHEMERAL_MODE='false',
               PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS='false',
               PREFECT_LOGGING_LEVEL='INFO', PREFECT_API_ENABLE_HTTP2='false',
               PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW='ignore',
               PREFECT_SERVER_UI_ENABLED='false', NO_PROXY='127.0.0.1,localhost',
               PIPELINE_LAB_ROOT=str(root), PIPELINE_FAKE_URL=fake_url,
               PYTHONDONTWRITEBYTECODE='1', PYTHONUTF8='1')
    import importlib.metadata
    lab = LabEndpoints(root, api_url, fake_url, [api_url, fake_url], storage, env,
                       importlib.metadata.version('prefect'))
    if lab.sdk_version != '3.9.0':
        raise RuntimeError('Only Prefect 3.9.0 is admitted')
    class FakeHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            payload = json.dumps(dict(body, kind='fake-model-rag-storage')).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            _json(root / f'fake-request-{uuid.uuid4().hex}.json', {'body': body, 'at': time.time()})
        def log_message(self, *_args):
            pass
    lab.fake_server = ThreadingHTTPServer(('127.0.0.1', int(fake_url.rsplit(':', 1)[1])), FakeHandler)
    threading.Thread(target=lab.fake_server.serve_forever, daemon=True).start()
    try:
        lab.start_server()
        setup = lab.launch('storage')
        setup.wait(timeout=40)
        if setup.returncode:
            raise RuntimeError('Shared storage registration failed; inspect storage log')
        _http(api_url + '/v2/concurrency_limits/', 'POST',
              {'name': 'lab-http', 'limit': 1, 'active': True})
    except BaseException:
        lab.close()
        raise
    _json(root / 'lab-endpoints.json', {'api_url': api_url, 'fake_url': fake_url,
          'allowed_network': lab.allowed_network, 'storage_root': str(storage),
          'sdk_version': lab.sdk_version, 'server_version': _http(api_url + '/admin/version')})
    return lab


def _tasks():
    from prefect import task
    from prefect.cache_policies import NO_CACHE
    from prefect.concurrency.sync import concurrency
    root = Path(os.environ['PIPELINE_LAB_ROOT'])
    storage = 'local-file-system/lab-shared'
    @task(name='lab-echo', persist_result=True, result_storage=storage, cache_policy=NO_CACHE)
    def echo(token, value):
        reply = _http(os.environ['PIPELINE_FAKE_URL'], 'POST', {'reference': token})
        return {'token': token, 'value': value * 2, 'worker_pid': os.getpid(), 'fake': reply['kind']}
    @task(name='lab-retry', retries=1, retry_delay_seconds=12, persist_result=True,
          result_storage=storage, cache_policy=NO_CACHE)
    def retry(token, value):
        first = root / f'{token}-attempt-1.json'
        attempt = 2 if first.exists() else 1
        with concurrency('lab-http', strict=True, lease_duration=60):
            _json(root / f'{token}-attempt-{attempt}.json', {'at': time.time(), 'pid': os.getpid()})
            if attempt == 1:
                raise ValueError('intentional isolated retry probe')
            return {'token': token, 'value': value * 2, 'worker_pid': os.getpid()}
    return {'echo': echo, 'retry': retry}


def _main():
    _deny_external_network()
    mode, args = sys.argv[1], sys.argv[2:]
    if mode == 'server':
        from prefect.settings import get_current_settings
        settings = get_current_settings()
        _json(Path(os.environ['PIPELINE_LAB_ROOT']) / 'server-native-settings.json', {
            'lease_storage': settings.server.concurrency.lease_storage,
            'repossessor_loop_seconds': settings.server.services.repossessor.loop_seconds,
            'database_connection_url': str(settings.server.database.connection_url.get_secret_value()),
            'server_pid': os.getpid()})
        from prefect.cli import app
        sys.argv = ['prefect', 'server', 'start', '--host', '127.0.0.1', '--port', args[0], '--no-ui']
        app()
    elif mode == 'storage':
        from prefect.filesystems import LocalFileSystem
        LocalFileSystem(basepath=os.environ['PREFECT_RESULTS_LOCAL_STORAGE_PATH']).save('lab-shared', overwrite=True)
    elif mode == 'worker':
        from prefect.task_worker import TaskWorker
        tasks = _tasks()
        async def run():
            worker = TaskWorker(tasks[args[0]], limit=1)
            _json(Path(os.environ['PIPELINE_LAB_ROOT']) /
                  f'worker-{os.environ["PIPELINE_LAB_LAUNCH_TOKEN"]}.json',
                  {'pid': os.getpid(), 'kind': args[0], 'task_key': tasks[args[0]].task_key})
            await worker.start()
        asyncio.run(run())
    elif mode == 'submit':
        task = _tasks()[args[0]]
        future = task.apply_async(kwargs={'token': args[1], 'value': int(args[2])})
        _json(args[3], {'task_run_id': str(future.task_run_id), 'submitter_pid': os.getpid(),
                        'task_key': task.task_key})
    elif mode == 'read':
        from prefect.client.orchestration import get_client
        from uuid import UUID
        with get_client(sync_client=True) as client:
            run = client.read_task_run(UUID(args[0]))
            result = run.state.result()
            _json(args[1], dict(result, reader_pid=os.getpid()))
    elif mode == 'lease':
        from prefect.concurrency.sync import concurrency
        with concurrency('lab-crash-lease', strict=True, lease_duration=float(args[0])):
            _json(args[1], {'pid': os.getpid(), 'at': time.time()})
            time.sleep(200)
    else:
        raise ValueError('Unknown isolated lab role')


if __name__ == '__main__':
    _main()
