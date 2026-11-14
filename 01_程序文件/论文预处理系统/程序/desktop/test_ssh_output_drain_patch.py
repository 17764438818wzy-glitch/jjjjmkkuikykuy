"""The deployed SSH reader must retain packets arriving after exit status."""
import ast
import base64
import hashlib
import importlib
import json
from types import SimpleNamespace

import pytest


DEPLOYED = '''class SSHProvider:
    def _run_remote_command(
        self,
        client: paramiko.SSHClient,
        command: str,
        timeout: int,
    ) -> tuple[str, str, int]:
        stdin, stdout_stream, stderr_stream = client.exec_command(command, timeout=timeout)
        stdin.close()
        channel = stdout_stream.channel

        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []
        deadline = time.time() + timeout

        while True:
            while channel.recv_ready():
                stdout_chunks.append(channel.recv(65536))
            while channel.recv_stderr_ready():
                stderr_chunks.append(channel.recv_stderr(65536))

            if channel.exit_status_ready():
                break
            if time.time() > deadline:
                channel.close()
                raise TimeoutError(f"Execution timed out after {timeout} seconds")
            time.sleep(0.1)

        while channel.recv_ready():
            stdout_chunks.append(channel.recv(65536))
        while channel.recv_stderr_ready():
            stderr_chunks.append(channel.recv_stderr(65536))

        exit_code = channel.recv_exit_status()
        stdout = b"".join(stdout_chunks).decode("utf-8", errors="replace")
        stderr = b"".join(stderr_chunks).decode("utf-8", errors="replace")
        return stdout, stderr, exit_code
'''


def installer():
    return importlib.import_module('server_patches.ssh_output_drain.install')


class ScheduledChannel:
    """Protocol events are independent: exit status does not imply EOF."""
    def __init__(self, stdout=b'', stderr=b'', *, exited=True, eof=False,
                 closed=False, code=0, events=()):
        self.stdout = stdout; self.stderr = stderr
        self.exited = exited; self.eof_received = eof; self.closed = closed
        self.code = code; self.events = list(events)
        self.now = 0.0; self.sleeps = 0; self.stdin_closed = False
        self.command = None

    def exec_command(self, command, *, timeout):
        self.command = (command, timeout)
        stdin = SimpleNamespace(close=lambda: setattr(self, 'stdin_closed', True))
        stream = SimpleNamespace(channel=self)
        return stdin, stream, stream

    def recv_ready(self): return bool(self.stdout)
    def recv_stderr_ready(self): return bool(self.stderr)
    def exit_status_ready(self): return self.exited or self.closed
    def recv_exit_status(self): return self.code

    def recv(self, size):
        result, self.stdout = self.stdout[:size], self.stdout[size:]
        return result

    def recv_stderr(self, size):
        result, self.stderr = self.stderr[:size], self.stderr[size:]
        return result

    def close(self): self.closed = True

    def sleep(self, seconds):
        self.sleeps += 1; self.now += seconds
        if self.events:
            event = self.events.pop(0)
            self.stdout += event.get('stdout', b'')
            self.stderr += event.get('stderr', b'')
            for name in ('exited', 'eof_received', 'closed'):
                if name in event: setattr(self, name, event[name])


def execute(channel, *, patched=True, timeout=3):
    source = installer().transform(DEPLOYED) if patched else DEPLOYED
    ns = {'time': SimpleNamespace(time=lambda: channel.now, sleep=channel.sleep),
          'paramiko': SimpleNamespace(SSHClient=object)}
    exec(compile(ast.parse(source), '<deployed-ssh-reader>', 'exec'), ns)
    return ns['SSHProvider']()._run_remote_command(channel, 'unchanged command', timeout)


def test_exit_status_before_tail_reproduces_observed_broken_result_envelope():
    record = {'present': True, 'value': [{'text': '合成教材证据' * 60000}], 'type': 'json'}
    envelope = ('__RAGFLOW_RESULT__:' + base64.b64encode(
        json.dumps(record, ensure_ascii=False, separators=(',', ':')).encode()).decode() + '\n').encode()
    boundary = 360448
    event = {'stdout': envelope[boundary:], 'eof_received': True}
    old = ScheduledChannel(envelope[:boundary], events=[event])
    broken, _, _ = execute(old, patched=False)
    assert len(broken) == boundary
    with pytest.raises(ValueError):
        base64.b64decode(broken.split(':', 1)[1], validate=True)
    fixed = ScheduledChannel(envelope[:boundary], events=[event])
    stdout, stderr, code = execute(fixed)
    assert stdout.encode() == envelope
    assert json.loads(base64.b64decode(stdout.strip().split(':', 1)[1])) == record
    assert (stderr, code) == ('', 0)
    assert fixed.command == ('unchanged command', 3) and fixed.stdin_closed


def test_stderr_and_split_unicode_arrive_after_exit_status():
    text = '全文正文必须完整。'.encode()
    channel = ScheduledChannel(text[:1], b'warning ', code=7, events=[
        {'stdout': text[1:5], 'stderr': b'first '},
        {'stdout': text[5:], 'stderr': b'last', 'eof_received': True},
    ])
    assert execute(channel) == ('全文正文必须完整。', 'warning first last', 7)


@pytest.mark.parametrize('closed', [False, True])
def test_complete_channel_has_identical_results_and_no_added_wait(closed):
    args = {'stdout': b'finance or education output', 'stderr': b'notice',
            'eof': not closed, 'closed': closed, 'code': 2}
    original = ScheduledChannel(**args); fixed = ScheduledChannel(**args)
    assert execute(fixed) == execute(original, patched=False)
    assert fixed.sleeps == original.sleeps == 0


def test_eof_without_exit_status_keeps_waiting_for_original_status():
    channel = ScheduledChannel(b'done', exited=False, eof=True, code=4,
                               events=[{}, {'exited': True}])
    assert execute(channel) == ('done', '', 4)
    assert channel.sleeps == 2


def test_status_without_eof_still_obeys_original_deadline():
    channel = ScheduledChannel(b'partial')
    with pytest.raises(TimeoutError, match='Execution timed out after 1 seconds'):
        execute(channel, timeout=1)
    assert channel.closed and channel.now < 1.2


@pytest.fixture
def gated_fixture(monkeypatch):
    candidate = installer(); before = DEPLOYED.encode()
    after = candidate.transform(DEPLOYED).encode()
    monkeypatch.setattr(candidate, 'ORIGINAL', hashlib.sha256(before).hexdigest())
    monkeypatch.setattr(candidate, 'PATCHED', hashlib.sha256(after).hexdigest())
    return candidate, before, after


def test_installer_preserves_original_and_is_idempotent(tmp_path, gated_fixture):
    candidate, before, after = gated_fixture
    target = tmp_path / 'ssh.py'; target.write_bytes(before)
    assert candidate.install(target) == 'installed'
    assert target.read_bytes() == after
    backup = target.with_name('ssh.py.output-drain-original')
    assert backup.read_bytes() == before
    assert candidate.install(target) == 'already_installed'
    assert backup.read_bytes() == before


@pytest.mark.parametrize('problem', ['source', 'backup', 'generated_hash'])
def test_failed_installer_gate_never_changes_target(tmp_path, gated_fixture, monkeypatch, problem):
    candidate, before, _ = gated_fixture
    target = tmp_path / 'ssh.py'; backup = target.with_name('ssh.py.output-drain-original')
    if problem == 'source': before += b'\n# drift\n'
    if problem == 'backup': backup.write_bytes(b'other source')
    if problem == 'generated_hash': monkeypatch.setattr(candidate, 'PATCHED', '0' * 64)
    target.write_bytes(before)
    snapshot = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with pytest.raises(ValueError): candidate.install(target)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == snapshot
