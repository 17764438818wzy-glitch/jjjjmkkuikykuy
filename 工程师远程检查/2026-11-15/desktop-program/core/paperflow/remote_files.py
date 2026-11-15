"""Bounded, hash-checked file exchange; no archive extraction outside an owned stage."""
import hashlib
import os
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import uuid

MAX_BYTES=2*1024**3
MAX_FILES=20000
WINDOWS_DEVICES={'CON','PRN','AUX','NUL','CONIN$','CONOUT$',
    *(prefix+number for prefix in ('COM','LPT') for number in '123456789¹²³')}


def relative(name):
    if (not isinstance(name,str) or not name or len(name)>1024 or '\\' in name or ':' in name
            or any(ord(c)<32 or c in '<>"|?*' for c in name) or name.startswith('/')
            or any(p in ('','..','.') or p.rstrip(' .')!=p
                   or p.split('.')[0].upper() in WINDOWS_DEVICES for p in name.split('/'))):
        raise ValueError('unsafe exchange path')
    return PurePosixPath(name)


def file_map(root,prefix=''):
    root=Path(root).resolve();result={}
    if prefix:relative(prefix)
    for path in sorted(root.rglob('*')):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError('exchange link or escaped path')
        if path.is_file():
            name='/'.join(p for p in (prefix,path.relative_to(root).as_posix()) if p)
            relative(name);result[name]=path
    if len(result)>MAX_FILES:raise ValueError('too many exchange files')
    return result


def manifest(files):
    result=[];size=0
    for name,path in sorted(files.items()):
        relative(name);h=hashlib.sha256()
        with Path(path).open('rb') as stream:
            while chunk:=stream.read(1024**2):h.update(chunk)
        length=Path(path).stat().st_size;size+=length
        result.append(dict(path=name,size=length,sha256=h.hexdigest()))
    if size>MAX_BYTES or len(result)>MAX_FILES:raise ValueError('exchange exceeds bound')
    return result


def write_archive(stream,files):
    with tarfile.open(fileobj=stream,mode='w|') as archive:
        for name,path in sorted(files.items()):
            relative(name)
            if Path(path).is_symlink():raise ValueError('exchange link')
            with Path(path).open('rb') as source:
                item=tarfile.TarInfo(name);item.size=os.fstat(source.fileno()).st_size;item.mode=0o600
                archive.addfile(item,source)


def receive_archive(stream,destination,expected,*,max_bytes=MAX_BYTES):
    if not isinstance(expected,list) or len(expected)>MAX_FILES:raise ValueError('invalid exchange manifest')
    catalog={}
    for item in expected:
        name=item['path'];relative(name)
        if (name in catalog or type(item['size']) is not int or item['size']<0
                or len(item['sha256'])!=64 or any(c not in '0123456789abcdef' for c in item['sha256'])):
            raise ValueError('invalid exchange entry')
        catalog[name]=item
    if sum(v['size'] for v in catalog.values())>max_bytes:raise ValueError('exchange exceeds byte bound')
    destination=Path(destination).resolve();destination.mkdir(parents=True,exist_ok=False)
    seen=set()
    try:
        with tarfile.open(fileobj=stream,mode='r|') as archive:
            for item in archive:
                relative(item.name);want=catalog.get(item.name)
                if (not item.isfile() or item.name in seen or not want or item.size!=want['size']):
                    raise ValueError('unexpected exchange member')
                path=destination/relative(item.name)
                path.parent.mkdir(parents=True,exist_ok=True);h=hashlib.sha256();size=0
                with archive.extractfile(item) as source,path.open('xb') as target:
                    while chunk:=source.read(min(1024**2,max_bytes-size+1)):
                        size+=len(chunk)
                        if size>want['size']:raise ValueError('exchange file exceeds bound')
                        h.update(chunk);target.write(chunk)
                if h.hexdigest()!=want['sha256']:raise ValueError('exchange hash mismatch')
                seen.add(item.name)
        if seen!=set(catalog):raise ValueError('exchange files missing')
    except BaseException:
        shutil.rmtree(destination)
        raise
    return destination


def publish_output(stage,output):
    stage,output=Path(stage),Path(output)
    backup=output.with_name(output.name+'.remote-before-'+uuid.uuid4().hex)
    if output.exists():output.rename(backup)
    try:stage.rename(output)
    except BaseException:
        if backup.exists():backup.rename(output)
        raise
    if backup.exists():shutil.rmtree(backup)


def map_text(text,mapping):
    for original,replacement in sorted(mapping.items(),key=lambda p:len(p[0]),reverse=True):
        text=text.replace(original,replacement).replace(original.replace('\\','/'),replacement)
        # Windows paths in prompts may use either separator after the mapped prefix.
        start=0
        while (start:=text.find(replacement+'\\',start))!=-1:
            end=start+len(replacement)
            while end<len(text) and text[end] not in '\n\r\t "\'，。；：、()（）':end+=1
            text=text[:start]+text[start:end].replace('\\','/')+text[end:];start=end
    return text
