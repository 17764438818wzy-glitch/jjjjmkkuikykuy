"""Diagnose Wine/Python stdio combinations using synthetic code only."""
import json
import os
from pathlib import Path
import shutil
import subprocess
from uuid import uuid4


def main():
    base = Path('/home/wzy/paperflow-postprocess/candidate-20261101')
    prefix = base / ('wine-stdio-test-' + uuid4().hex)
    shutil.copytree(base / 'wine-test-prefix4', prefix, symlinks=True)
    for name in ('f:', 'z:'):
        link = prefix / 'dosdevices' / name
        if not link.is_symlink():
            raise ValueError('unexpected test prefix device')
        link.unlink()
    drive = prefix / 'synthetic-drive-f'
    drive.mkdir()
    (prefix / 'dosdevices/f:').symlink_to(drive, target_is_directory=True)
    env = dict(os.environ, WINEPREFIX=str(prefix), WINEDEBUG='-all', DISPLAY='', PYTHONUTF8='1',
               WINEDLLPATH=str(base / 'wine-runtime/usr/lib/x86_64-linux-gnu/wine'), CUDA_VISIBLE_DEVICES='')
    cmd = [str(base / 'wine-runtime/usr/lib/wine/wine64'), r'C:\Python\python.exe',
           '-c', 'print("synthetic-stdio-ok")']
    reports = []
    for stdin_kind in ('devnull', 'pipe'):
        for stdout_kind in ('file', 'pipe'):
            with (prefix / ('output-' + stdin_kind + '-' + stdout_kind)).open('wb') as output, \
                 (prefix / ('error-' + stdin_kind + '-' + stdout_kind)).open('wb') as error:
                process = subprocess.Popen(cmd, stdin=subprocess.DEVNULL if stdin_kind == 'devnull' else subprocess.PIPE,
                    stdout=output if stdout_kind == 'file' else subprocess.PIPE,
                    stderr=error if stdout_kind == 'file' else subprocess.PIPE, env=env)
                stdout, stderr = process.communicate(input=b'' if stdin_kind == 'pipe' else None, timeout=45)
            out = stdout if stdout is not None else (prefix / ('output-' + stdin_kind + '-' + stdout_kind)).read_bytes()
            err = stderr if stderr is not None else (prefix / ('error-' + stdin_kind + '-' + stdout_kind)).read_bytes()
            reports.append(dict(stdin=stdin_kind, stdout=stdout_kind, exit=process.returncode,
                                printed=b'synthetic-stdio-ok' in out,
                                invalid_handle=b'Invalid handle' in err))
    print(json.dumps(reports))


if __name__ == '__main__':
    main()
