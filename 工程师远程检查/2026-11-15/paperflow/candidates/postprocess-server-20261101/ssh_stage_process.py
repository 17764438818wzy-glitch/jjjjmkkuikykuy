"""Ephemeral pinned SSH staging helper; parent enforces the whole-call deadline."""
import json
import sys

from snapshot_transport import stage_over_ssh


def run(request):
    import paramiko
    from ssh_rpc_process import PinnedHostKey
    connection, timeout = request['connection'], request['io_timeout']
    client = paramiko.SSHClient()
    # Do not load permissive known-host policies, agents or password fallbacks.
    client.set_missing_host_key_policy(PinnedHostKey(connection['server_key_sha256']))
    try:
        client.connect(connection['host'], port=connection['port'], username=connection['username'],
            key_filename=connection['private_key'], look_for_keys=False, allow_agent=False,
            timeout=timeout, banner_timeout=timeout, auth_timeout=timeout, channel_timeout=timeout)
        return stage_over_ssh(client, request['command'], request['manifest'],
            allowed_roots=request['allowed_roots'], io_timeout=timeout, verify_locally=request['verify_locally'],
            trusted_files=request.get('trusted_files', []))
    finally:
        client.close()


def main():
    try:
        raw = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise ValueError('staging metadata exceeds its bound')
        result = run(json.loads(raw))
        output = json.dumps(result, ensure_ascii=False, sort_keys=True,
                            separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(output) > 65536:
            raise ValueError('staging ACK exceeds its bound')
        sys.stdout.buffer.write(output); sys.stdout.buffer.flush()
    except Exception as exc:
        print('isolated staging outcome unknown: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
