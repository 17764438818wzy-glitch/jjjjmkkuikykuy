"""Reload the existing 8798 resource monitor with the native observation."""
import getpass
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import psutil

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parents[1] / 'candidates' / 'first-step-preview-8795' / 'server_resources_api.py'
BACKUP = HERE / 'server_resources_api-before-native.py'
password = getpass.getpass('') if sys.stdin.isatty() else sys.stdin.readline().rstrip('\r\n')
listeners = [item for item in psutil.net_connections(kind='tcp')
             if item.status == psutil.CONN_LISTEN and item.laddr.ip == '127.0.0.1' and item.laddr.port == 8798]
if not password or len(listeners) != 1 or listeners[0].pid is None:
    raise ValueError('existing resource monitor is unavailable')
process = psutil.Process(listeners[0].pid)
if str(SOURCE) not in process.cmdline() or '--serve' not in process.cmdline():
    raise ValueError('port belongs to another process')
if not BACKUP.exists():
    shutil.copy2(SOURCE, BACKUP)
process.terminate()
process.wait(timeout=5)
shutil.copy2(HERE / 'server_resources_api.py', SOURCE)


def launch():
    child = subprocess.Popen([r'C:\Python314\python.exe', str(SOURCE), '--serve'], stdin=subprocess.PIPE,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    child.stdin.write((password + '\n').encode('utf8'))
    child.stdin.close()
    for _ in range(30):
        if child.poll() is not None:
            return None
        if any(item.status == psutil.CONN_LISTEN and item.laddr.ip == '127.0.0.1'
               and item.laddr.port == 8798 and item.pid == child.pid for item in psutil.net_connections(kind='tcp')):
            return child.pid
        time.sleep(.2)
    return None


pid = launch()
if pid is None:
    shutil.copy2(BACKUP, SOURCE)
    restored = launch()
    print('candidate_start_failed; original_monitor_restored:', bool(restored), flush=True)
    raise SystemExit(1)
print('existing_resource_monitor_reloaded', pid, 'native_observation_enabled', flush=True)
