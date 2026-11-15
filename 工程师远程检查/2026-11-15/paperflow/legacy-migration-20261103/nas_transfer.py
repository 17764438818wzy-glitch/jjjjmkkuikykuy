"""Publish a verified original once at its NAS content address."""
import hashlib
from pathlib import Path,PureWindowsPath
import uuid
import smbclient

def publish_file(source,sha,files):
    source=Path(source)
    target=files.path({'relative':'objects/'+sha[:2]+'/'+sha})
    try:
        with files.open(target,'rb') as stream:
            if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('Existing NAS object hash mismatch')
        return False
    except FileNotFoundError:pass
    smbclient.makedirs(str(PureWindowsPath(target).parent),exist_ok=True)
    temporary=target+'.migration-'+uuid.uuid4().hex
    try:
        with source.open('rb') as incoming,files.open(temporary,'wb') as output:
            while chunk:=incoming.read(256*1024):output.write(chunk)
        with files.open(temporary,'rb') as stream:
            if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('NAS readback hash mismatch')
        try:smbclient.rename(temporary,target)
        except FileExistsError:
            with files.open(target,'rb') as stream:
                if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('Concurrent NAS object hash mismatch')
    finally:
        try:smbclient.remove(temporary)
        except OSError as exc:
            if exc.errno not in (2,20):raise
    return True
