"""Reuse the existing converter with short, private Windows temporary paths."""
import ctypes
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile

from .package import load,save


def cleanup_office_temps(config):
    """After manager shutdown, recover only positively identified dead-reader scratch."""
    import threading
    from .cleanup_paths import inventory,delete_inventory
    from .winjob import identity_alive
    short_root=Path(getattr(config,'office_temp_root',r'E:\rag mid\lo_convert')).resolve()
    removed=[]
    for marker in (config.root/'work').glob('*/private/source-view/**/office-owned.json'):
        owner=load(marker);path=Path(owner['path'])
        if (path.parent.resolve()!=short_root or not path.name.startswith('pf-')
                or owner.get('marker')!=str(marker.resolve())):
            raise ValueError('Office scratch ownership mismatch')
        if identity_alive(owner['process']):continue
        if path.exists():
            if load(path/'paperflow-owner.json')!=owner:raise ValueError('Office scratch ownership token mismatch')
            resource=inventory(path,[short_root],[])
            delete_inventory(resource,[short_root],[],threading.Event())
        marker.unlink();removed.append(str(path))
    return dict(removed=removed)


CONVERT = '''import os,sys
from pathlib import Path
sys.path.insert(0,os.environ['PAPERFLOW_LEGACY_APP'])
from engine import convert_office
convert_office(Path(sys.argv[1]),Path(sys.argv[2]))
'''


def convert_office(source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    # Inherited by descendants. A converter crash must not open a blocking dialog.
    if os.name == 'nt':
        ctypes.windll.kernel32.SetErrorMode(0x8003)
    started = time.perf_counter()
    short_root=Path(os.environ.get('PAPERFLOW_OFFICE_TEMP_ROOT',r'E:\rag mid\lo_convert')).resolve()
    short_root.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='pf-', dir=short_root) as temporary:
        temporary=Path(temporary).resolve()
        if temporary.parent!=short_root or not temporary.name.startswith('pf-'):
            raise RuntimeError('unexpected Office temporary directory')
        from .winjob import process_identity
        marker=output/'office-owned.json'
        owner=dict(path=str(temporary),marker=str(marker.resolve()),process=process_identity(os.getpid()),token=uuid.uuid4().hex)
        save(temporary/'paperflow-owner.json',owner);save(marker,owner)
        suffix=source.suffix.lower()
        if suffix in ('.docx','.docm'):suffix='.doc'
        local=temporary/('source'+suffix);shutil.copy2(source,local)
        target=temporary/'out/source.docx'
        scratch=Path(temporary)/'tmp';scratch.mkdir()
        environment=dict(os.environ,TEMP=str(scratch),TMP=str(scratch),
            PAPERFLOW_LEGACY_APP=os.environ.get('PAPERFLOW_LEGACY_APP',r'E:\uv\ragflow_upstream'))
        command=[sys.executable,'-X','utf8','-B','-c',CONVERT,str(local),str(target.parent)]
        result = subprocess.run(command, capture_output=True, timeout=135,env=environment,
                                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        log=temporary/'out_conversion.log'
        (output/'conversion.log').write_bytes(result.stdout+b'\n'+result.stderr+(log.read_bytes() if log.is_file() else b''))
        if result.returncode or not zipfile.is_zipfile(target):
            raise RuntimeError('isolated Office conversion failed; original retained; see private conversion.log')
        with zipfile.ZipFile(target) as archive:
            if 'word/document.xml' not in archive.namelist():
                raise RuntimeError('Office conversion returned no Word document part')
        final = output/'source.docx'
        shutil.copy2(target,final)
    marker.unlink()
    return final, dict(engine='existing-libreoffice', seconds=time.perf_counter()-started)
