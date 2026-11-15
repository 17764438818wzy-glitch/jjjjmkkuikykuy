"""Synthetic local SSH server exercises pinned keys and real exec-ACK timeout."""
import hashlib
import json
from pathlib import Path
import socket
import sys
import threading
import time

import paramiko
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ssh_rpc_client import RemoteCallError, call_bounded


class Endpoint(paramiko.ServerInterface):
    def __init__(self, allowed_key, *, exec_delay):
        self.allowed_key, self.exec_delay = allowed_key, exec_delay
        self.command, self.done = None, threading.Event()

    def check_auth_publickey(self, username, key):
        return (paramiko.AUTH_SUCCESSFUL if username == 'fixture' and key.asbytes() == self.allowed_key.asbytes()
                else paramiko.AUTH_FAILED)

    def get_allowed_auths(self, username): return 'publickey'
    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED if kind == 'session' else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_exec_request(self, channel, command):
        self.command = command
        if self.exec_delay:
            time.sleep(self.exec_delay)
        def handle():
            try:
                data = bytearray()
                while True:
                    chunk = channel.recv(4096)
                    if not chunk: break
                    data.extend(chunk)
                if data:
                    payload = json.loads(data)
                    channel.sendall(json.dumps({'received': payload['synthetic']}).encode())
                    channel.send_exit_status(0)
            except (EOFError, OSError):
                pass
            finally:
                channel.close(); self.done.set()
        threading.Thread(target=handle, daemon=True).start()
        return True


def start_server(host_key, allowed_key, *, exec_delay=0):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0)); listener.listen(1); listener.settimeout(5)
    endpoint = Endpoint(allowed_key, exec_delay=exec_delay)
    def serve():
        try:
            conn, _ = listener.accept()
            transport = paramiko.Transport(conn)
            transport.add_server_key(host_key)
            try:
                transport.start_server(server=endpoint)
                channel = transport.accept(5)
                if channel is not None: endpoint.done.wait(5)
            finally:
                transport.close()
        except (EOFError, OSError, paramiko.SSHException):
            pass
        finally:
            listener.close()
    thread = threading.Thread(target=serve, daemon=True); thread.start()
    return listener.getsockname()[1], endpoint, thread


@pytest.fixture(scope='module')
def keys(tmp_path_factory):
    root = tmp_path_factory.mktemp('bounded-ssh')
    host = paramiko.RSAKey.generate(2048)
    user = paramiko.RSAKey.generate(2048)
    private = root / 'synthetic-client-key'
    user.write_private_key_file(str(private))
    return host, user, private


def connection(port, host, private, *, fingerprint=None):
    return dict(host='127.0.0.1', port=port, username='fixture', private_key=str(private),
                server_key_sha256=fingerprint or hashlib.sha256(host.asbytes()).hexdigest())


def test_actual_ssh_pinned_host_and_one_metadata_request(keys):
    host, user, private = keys
    port, endpoint, thread = start_server(host, user)
    result = call_bounded(connection(port, host, private), '/synthetic/metadata',
                          {'synthetic': 'only-fixture'}, timeout=8)
    assert result == {'received': 'only-fixture'}
    assert endpoint.command == b'/synthetic/metadata'
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_actual_ssh_exec_ack_stall_obeys_whole_call_deadline(keys):
    host, user, private = keys
    port, endpoint, thread = start_server(host, user, exec_delay=2)
    started = time.monotonic()
    with pytest.raises(RemoteCallError, match='unknown'):
        call_bounded(connection(port, host, private), '/synthetic/metadata', {'synthetic': 'never-sent'}, timeout=.8)
    assert time.monotonic() - started < 2
    thread.join(timeout=5)
    assert endpoint.command == b'/synthetic/metadata'


def test_actual_ssh_rejects_wrong_host_key_before_command(keys):
    host, user, private = keys
    port, endpoint, thread = start_server(host, user)
    with pytest.raises(RemoteCallError):
        call_bounded(connection(port, host, private, fingerprint='0' * 64),
                     '/synthetic/metadata', {'synthetic': 1}, timeout=8)
    thread.join(timeout=5)
    assert endpoint.command is None
