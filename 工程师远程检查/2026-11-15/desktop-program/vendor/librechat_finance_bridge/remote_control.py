"""Run an authorized command over SSH, using a local Windows DPAPI credential."""
import argparse
import base64
from pathlib import Path
import shlex
import subprocess
import sys

import paramiko

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

parser = argparse.ArgumentParser()
parser.add_argument('--sudo', action='store_true')
parser.add_argument('--put', nargs=2, metavar=('LOCAL', 'REMOTE'))
parser.add_argument('--get', nargs=2, metavar=('REMOTE', 'LOCAL'))
parser.add_argument('--container-script', metavar='LOCAL_PY')
parser.add_argument('--container-name', default='literature-mcp')
parser.add_argument('command', nargs='?')
args = parser.parse_args()
credential = Path.home() / '.codex/tmp/librechat-ssh-credential.dpapi'
password = subprocess.check_output([
    str(Path.home() / '.cache/codex-runtimes/codex-primary-runtime/dependencies/native/powershell/pwsh.exe'),
    '-NoProfile', '-Command',
    "$ErrorActionPreference='Stop'; $s=Get-Content -LiteralPath '" + str(credential) + "' | ConvertTo-SecureString; "
    "[System.Net.NetworkCredential]::new('', $s).Password",
], text=True).strip()
client = paramiko.SSHClient()
client.load_host_keys(str(Path.home() / '.ssh/known_hosts'))
client.set_missing_host_key_policy(paramiko.RejectPolicy())
client.connect('192.168.1.120', username='wzy', password=password,
               look_for_keys=False, allow_agent=False, timeout=12)
try:
    if args.put:
        with client.open_sftp() as sftp:
            sftp.put(*args.put)
        print('Transferred:', args.put[1])
    if args.get:
        with client.open_sftp() as sftp:
            sftp.get(*args.get)
        print('Downloaded:', args.get[0])
    command = args.command
    if args.container_script:
        encoded = base64.b64encode(Path(args.container_script).read_bytes()).decode('ascii')
        program = 'import base64; exec(base64.b64decode(' + repr(encoded) + '))'
        command = 'docker exec ' + shlex.quote(args.container_name) + ' python -c ' + shlex.quote(program)
    if command:
        command = 'sudo -S -p "" sh -c ' + shlex.quote(command) if args.sudo else command
        stdin, stdout, stderr = client.exec_command(command)
        if args.sudo:
            stdin.write(password + '\n')
            stdin.flush()
        stdin.channel.shutdown_write()
        while not stdout.channel.exit_status_ready() or stdout.channel.recv_ready() or stdout.channel.recv_stderr_ready():
            if stdout.channel.recv_ready():
                print(stdout.channel.recv(16384).decode('utf-8', 'replace'), end='', flush=True)
            if stdout.channel.recv_stderr_ready():
                print(stdout.channel.recv_stderr(16384).decode('utf-8', 'replace'), end='', file=sys.stderr, flush=True)
            if not stdout.channel.recv_ready() and not stdout.channel.recv_stderr_ready():
                import time
                time.sleep(0.1)
        sys.exit(stdout.channel.recv_exit_status())
finally:
    client.close()
