"""Deployment helpers. Execute in the authenticated maintenance session.

No server credentials are stored in this file. `client`, `capture` and the
read-only preflight `containers_before` are supplied by that session.
"""
import json
import pathlib
import secrets
import shlex

LOCAL = pathlib.Path('F:/论文预处理系统/预处理系统2/prefect-preview')
REMOTE = '/home/wzy/prefect-preview'


def report(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def docker_command(arguments, timeout=60):
    result = capture(
        'cd ' + shlex.quote(REMOTE) + ' && docker compose ' + arguments,
        sudo=True, timeout=timeout,
    )
    report(result)
    return result


def prepare():
    sftp = client.open_sftp()
    try:
        try:
            sftp.stat(REMOTE)
        except FileNotFoundError:
            sftp.mkdir(REMOTE, mode=0o700)
        else:
            raise RuntimeError('Existing preview directory: inspect before reusing')
        sftp.mkdir(REMOTE + '/demo-output', mode=0o700)
        sftp.put(str(LOCAL / 'compose.yaml'), REMOTE + '/compose.yaml')
        with sftp.file(REMOTE + '/.env', 'wx') as stream:
            stream.write('POSTGRES_PASSWORD=' + secrets.token_hex(24) + '\n')
        sftp.chmod(REMOTE + '/.env', 0o600)
    finally:
        sftp.close()
    baseline = [{k: row[k] for k in ('ID', 'Names', 'Image', 'State')} for row in containers_before]
    (LOCAL / 'containers-before.json').write_text(json.dumps(baseline, indent=2), encoding='utf-8')
    report({'created': REMOTE, 'existing_containers': len(baseline), 'credentials_saved_locally': False})


def upload_demo():
    sftp = client.open_sftp()
    try:
        sftp.put(str(LOCAL / 'demo.py'), REMOTE + '/demo.py')
    finally:
        sftp.close()
    report({'uploaded': 'demo.py'})


def pull():
    # The log and pid are scoped to this new deployment. No daemon settings change.
    global PULL_NAME
    PULL_NAME = 'pull-' + secrets.token_hex(4)
    result = capture(
        'cd ' + shlex.quote(REMOTE)
        + ' && nohup sh -c ' + shlex.quote('docker compose pull > ' + PULL_NAME + '.log 2>&1; printf "%s" "$?" > ' + PULL_NAME + '.exit')
        + ' </dev/null >/dev/null 2>&1 &',
        sudo=True, timeout=10,
    )
    report(dict(result, log=PULL_NAME))


def read_pull():
    sftp = client.open_sftp()
    try:
        with sftp.file(REMOTE + '/' + PULL_NAME + '.log', 'r') as stream:
            text = stream.read().decode('utf-8', 'replace')
        try:
            with sftp.file(REMOTE + '/' + PULL_NAME + '.exit', 'r') as stream:
                code = stream.read().decode().strip()
        except FileNotFoundError:
            code = None
        report({'exit_code': code, 'log_tail': text[-2000:]})
    finally:
        sftp.close()


def finish_empty_env():
    """Complete only the known empty file from the interrupted initial setup."""
    sftp = client.open_sftp()
    try:
        if sftp.stat(REMOTE + '/.env').st_size != 0:
            raise RuntimeError('Refusing to overwrite an existing database secret')
        with sftp.file(REMOTE + '/.env', 'w') as stream:
            stream.write('POSTGRES_PASSWORD=' + secrets.token_hex(24) + '\n')
        sftp.chmod(REMOTE + '/.env', 0o600)
    finally:
        sftp.close()
    baseline = [{k: row[k] for k in ('ID', 'Names', 'Image', 'State')} for row in containers_before]
    (LOCAL / 'containers-before.json').write_text(json.dumps(baseline, indent=2), encoding='utf-8')
    report({'environment_complete': True, 'existing_containers': len(baseline)})


def inventory_after():
    result = capture("docker ps --format '{{json .}}'", sudo=True)
    if result['code']:
        report(result)
        return
    rows = [json.loads(line) for line in result['out'].splitlines()]
    ids = {row['ID'] for row in rows}
    baseline = json.loads((LOCAL / 'containers-before.json').read_text('utf-8'))
    value = {
        'preview_containers': [{k: row.get(k) for k in ('ID', 'Names', 'Image', 'State', 'Status', 'Ports')} for row in rows if row['Names'].startswith('paperflow-prefect-preview-')],
        'preexisting_ids_still_running': sum(row['ID'] in ids for row in baseline),
        'preexisting_count': len(baseline),
        'preexisting_missing_ids': [row['Names'] for row in baseline if row['ID'] not in ids],
    }
    (LOCAL / 'containers-after.json').write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    report(value)
