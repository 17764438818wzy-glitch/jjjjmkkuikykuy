"""Single-attempt metadata calls with no automatic retry.

``call_once`` is useful outside task gates but Paramiko's exec acknowledgement
can block past its channel timeout. ``call_bounded`` uses a dedicated child
process with a whole-call deadline and explicit host-key pin; only that form is
eligible for a task-gated start request.
"""
import json
from pathlib import Path
import subprocess
import sys
import time


class RemoteCallError(RuntimeError):
    pass


def call_once(client, fixed_command, payload, *, io_timeout, max_bytes=16 * 1024 * 1024):
    """Do not use inside the task gate: exec acknowledgement is not bounded."""
    if (not isinstance(fixed_command, str) or not fixed_command
            or '\n' in fixed_command or '\r' in fixed_command
            or type(io_timeout) not in (int, float) or not 0 < io_timeout <= 120
            or type(max_bytes) is not int or not 1 <= max_bytes <= 16 * 1024 * 1024):
        raise RemoteCallError('fixed command and bounded transport are required')
    try:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(raw) > max_bytes:
            raise RemoteCallError('metadata request is too large')
        transport = client.get_transport()
        if transport is None or not transport.is_active() or not transport.is_authenticated():
            raise RemoteCallError('authenticated host-key-verified connection is unavailable')
        channel = transport.open_session(timeout=io_timeout)
        channel.settimeout(io_timeout)
        try:
            channel.exec_command(fixed_command)
            channel.sendall(raw)
            channel.shutdown_write()
            deadline, data, error = time.monotonic() + io_timeout, bytearray(), bytearray()
            while not channel.exit_status_ready() or channel.recv_ready() or channel.recv_stderr_ready():
                if time.monotonic() >= deadline:
                    raise RemoteCallError('server request outcome is unknown')
                if channel.recv_ready():
                    data.extend(channel.recv(4096))
                if channel.recv_stderr_ready():
                    error.extend(channel.recv_stderr(4096))
                if len(data) > max_bytes or len(error) > 65536:
                    raise RemoteCallError('server response exceeded its bound')
                time.sleep(.01)
            if channel.recv_exit_status() != 0:
                raise RemoteCallError('server request was refused or its outcome is unknown')
            return json.loads(data)
        finally:
            channel.close()
    except RemoteCallError:
        raise
    except Exception as exc:
        raise RemoteCallError('server request outcome is unknown') from exc


def _run_bounded(command, request, *, timeout, max_bytes):
    raw = json.dumps(request, ensure_ascii=False, sort_keys=True,
                     separators=(',', ':'), allow_nan=False).encode('utf8')
    if len(raw) > max_bytes:
        raise RemoteCallError('metadata request is too large')
    deadline = time.monotonic() + timeout
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout)
        output, error = process.communicate(raw, timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        try:
            process.communicate(timeout=2)
        except subprocess.TimeoutExpired as kill_error:
            raise RemoteCallError('server outcome and local helper termination are unknown') from kill_error
        raise RemoteCallError('server request outcome is unknown after timeout') from exc
    if process.returncode != 0 or len(output) > max_bytes or len(error) > 65536:
        raise RemoteCallError('server request was refused or its outcome is unknown')
    try:
        return json.loads(output)
    except (ValueError, UnicodeError) as exc:
        raise RemoteCallError('server returned invalid metadata') from exc


def call_bounded(connection, fixed_command, payload, *, timeout, max_bytes=16 * 1024 * 1024):
    """One isolated SSH attempt, pinned host key, with a whole-call deadline.

    A killed helper cannot send later bytes, but a request already received by
    the server may still be admitted. Its outcome is unknown and must not be
    retried; this alone does not prove physical start happened before a pause.
    """
    import re
    required = {'host', 'port', 'username', 'private_key', 'server_key_sha256'}
    if (not isinstance(connection, dict) or set(connection) != required
            or not isinstance(connection['host'], str) or not connection['host']
            or type(connection['port']) is not int or not 1 <= connection['port'] <= 65535
            or not isinstance(connection['username'], str) or not connection['username']
            or not isinstance(connection['private_key'], str) or not Path(connection['private_key']).is_absolute()
            or not isinstance(connection['server_key_sha256'], str)
            or not re.fullmatch('[a-f0-9]{64}', connection['server_key_sha256'])
            or not isinstance(fixed_command, str) or not fixed_command or '\n' in fixed_command or '\r' in fixed_command
            or type(timeout) not in (int, float) or not 0 < timeout <= 120
            or type(max_bytes) is not int or not 1 <= max_bytes <= 16 * 1024 * 1024):
        raise RemoteCallError('pinned SSH connection and bounded request are required')
    helper = Path(__file__).with_name('ssh_rpc_process.py').resolve(strict=True)
    return _run_bounded([sys.executable, '-B', str(helper)],
        dict(connection=connection, command=fixed_command, payload=payload, io_timeout=timeout,
             max_bytes=max_bytes), timeout=timeout, max_bytes=max_bytes)
