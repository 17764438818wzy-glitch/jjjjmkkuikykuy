"""Ephemeral RPC helper; input includes the pinned host fingerprint, not secrets."""
import hashlib
import hmac
import json
import sys

import paramiko

from ssh_rpc_client import call_once


class PinnedHostKey(paramiko.MissingHostKeyPolicy):
    def __init__(self, expected):
        self.expected = expected

    def missing_host_key(self, client, hostname, key):
        actual = hashlib.sha256(key.asbytes()).hexdigest()
        if not hmac.compare_digest(actual, self.expected):
            raise paramiko.SSHException('server host key does not match the pinned identity')


def main():
    client = None
    try:
        raw = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise ValueError('request is too large')
        request = json.loads(raw)
        connection, timeout = request['connection'], request['io_timeout']
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(PinnedHostKey(connection['server_key_sha256']))
        client.connect(connection['host'], port=connection['port'], username=connection['username'],
            key_filename=connection['private_key'], look_for_keys=False, allow_agent=False,
            timeout=timeout, banner_timeout=timeout, auth_timeout=timeout, channel_timeout=timeout)
        result = call_once(client, request['command'], request['payload'],
                           io_timeout=timeout, max_bytes=request['max_bytes'])
        output = json.dumps(result, ensure_ascii=False, sort_keys=True,
                            separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(output) > request['max_bytes']:
            raise ValueError('response is too large')
        sys.stdout.buffer.write(output); sys.stdout.buffer.flush()
    except Exception as exc:
        print('isolated SSH request failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
    finally:
        if client is not None:
            client.close()


if __name__ == '__main__':
    main()
