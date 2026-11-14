"""Bounded RAR5 input support; headers per https://www.rarlab.com/technote.htm.

The OS archive decoder writes to stdout only. This module chooses safe output
paths itself and verifies each payload against its declared length and CRC.
"""
import os
import re
import shutil
import struct
import subprocess
import tempfile
import threading
import zlib
from pathlib import Path, PureWindowsPath

DOCUMENT_SUFFIXES = {'.doc', '.docx', '.pdf', '.txt'}
ARCHIVE_SUFFIXES = {'.rar', '.zip', '.7z'}


def _vint(data,offset):
    value=0
    for shift in range(0,70,7):
        if offset>=len(data):raise ValueError('RAR 头截断')
        byte=data[offset];offset+=1;value|=(byte&127)<<shift
        if not byte&128:return value,offset
    raise ValueError('RAR 整数过长')


def rar5_members(source):
    if Path(source).stat().st_size>512*1024**2:raise ValueError('RAR 过大')
    raw=Path(source).read_bytes()
    if raw[:8]!=b'Rar!\x1a\x07\x01\0':raise ValueError('仅支持未加密的单卷 RAR5')
    offset=8;members=[];seen=set();total=0;ended=False
    while offset<len(raw):
        start=offset
        if start+4>len(raw):raise ValueError('RAR 头截断')
        expected=struct.unpack_from('<L',raw,start)[0]
        size,offset=_vint(raw,start+4);end=offset+size
        if end>len(raw) or zlib.crc32(raw[start+4:end])!=expected:raise ValueError('RAR 头 CRC 无效')
        header=raw[offset:end];pos=0
        kind,pos=_vint(header,pos);flags,pos=_vint(header,pos)
        extra,pos=_vint(header,pos) if flags&1 else (0,pos)
        packed,pos=_vint(header,pos) if flags&2 else (0,pos)
        if flags&24 or end+packed>len(raw):raise ValueError('不支持分卷或截断 RAR')
        if kind==1:
            archive_flags,pos=_vint(header,pos)
            if archive_flags&3:raise ValueError('不支持分卷 RAR')
        elif kind==2:
            fileflags,pos=_vint(header,pos);length,pos=_vint(header,pos);attrs,pos=_vint(header,pos)
            if fileflags&8:raise ValueError('RAR 成员大小未知或过大')
            if fileflags&2:pos+=4
            crc=struct.unpack_from('<L',header,pos)[0] if fileflags&4 else None
            if fileflags&4:pos+=4
            compression,pos=_vint(header,pos);host,pos=_vint(header,pos);name_len,pos=_vint(header,pos)
            name=header[pos:pos+name_len].decode('utf-8');pos+=name_len
            if PureWindowsPath(name).suffix.lower() in DOCUMENT_SUFFIXES|ARCHIVE_SUFFIXES and length>100*1024**2:
                raise ValueError('RAR 成员大小未知或过大')
            if pos>len(header)-extra:raise ValueError('RAR 文件名截断')
            path=PureWindowsPath(name);parts=name.replace('\\','/').split('/')
            if not name or path.drive or path.root or any(p in {'','..','.'} or ':' in p or p.endswith((' ','.')) for p in parts) or any(ord(c)<32 for c in name):
                raise ValueError('RAR 含不安全路径')
            if any(os.path.isreserved(p) for p in parts):raise ValueError('RAR 含保留文件名')
            if name.casefold() in seen:raise ValueError('RAR 含重复路径')
            seen.add(name.casefold())
            directory=bool(fileflags&1)
            if host==1 and attrs&0o170000 not in ({0o040000} if directory else {0o100000}):raise ValueError('RAR 含非普通文件')
            at=len(header)-extra
            while at<len(header):
                record_size,at=_vint(header,at);record_end=at+record_size
                record_type,after_type=_vint(header,at)
                if record_end>len(header) or record_end<after_type:raise ValueError('RAR 扩展头无效')
                if record_type not in {2,3,6}:raise ValueError('RAR 含加密、链接或未知扩展')
                at=record_end
            if not directory and crc is None:raise ValueError('RAR 成员缺少 CRC')
            total+=length
            if total>512*1024**2 or len(members)>=20000:raise ValueError('RAR 内容过大')
            members.append({'name':name,'size':length,'crc':crc,'directory':directory})
        elif kind==5:
            end_flags,pos=_vint(header,pos)
            if end_flags:raise ValueError('不支持分卷 RAR')
            ended=True;break
        elif kind!=3:raise ValueError('RAR 含未知或加密头')
        # Services never become filesystem objects: tar extracts to stdout.
        offset=end+packed
    if not ended:raise ValueError('RAR 缺少结束标记')
    return members


def _sevenzip():
    return next((Path(p) for p in [shutil.which('7z') or '',r'C:\Program Files\7-Zip\7z.exe',
                    r'F:\论文处理端迁移包\02_运行环境\7zip\7z.exe'] if p and Path(p).is_file()),None)


def _parse_rar4_listing(text):
    """Accept only regular Windows members from a successful 7-Zip listing."""
    head,separator,body=text.partition('----------')
    header=dict(line.split(' = ',1) for line in head.splitlines() if ' = ' in line)
    if not separator or header.get('Type')!='Rar' or header.get('Multivolume')!='-' or header.get('Volumes')!='1':
        raise ValueError('仅支持未加密的单卷 RAR4')
    records=[];row=None
    for line in body.splitlines():
        if not line.strip():continue
        if ' = ' not in line:raise ValueError('RAR4 列表格式无效')
        key,value=line.split(' = ',1)
        if key=='Path':row={};records.append(row)
        if row is None or key in row:raise ValueError('RAR4 列表字段无效')
        row[key]=value
    members=[];seen=set();total=0
    for row in records:
        name=row['Path'];parts=name.replace('\\','/').split('/');path=PureWindowsPath(name)
        if not name or path.drive or path.root or any(p in {'','..','.'} or ':' in p or p.endswith((' ','.')) or os.path.isreserved(p) for p in parts) or any(ord(c)<32 for c in name):
            raise ValueError('RAR 含不安全路径')
        identity='/'.join(parts).casefold()
        if identity in seen:raise ValueError('RAR 含重复路径')
        seen.add(identity)
        if any(row.get(k)!='-' for k in ('Encrypted','Split Before','Split After')) or row.get('Host OS')!='Win32':
            raise ValueError('RAR4 含加密、分卷或无法确认的文件类型')
        directory=row.get('Folder')=='+'
        if row.get('Folder') not in {'+','-'} or not re.fullmatch(r'[ADRH S]*',row.get('Attributes','')) or ('D' in row.get('Attributes',''))!=directory:
            raise ValueError('RAR4 含非普通文件')
        try:size=int(row['Size'])
        except (KeyError,ValueError):raise ValueError('RAR4 成员大小无效') from None
        if size<0 or (directory and size) or (path.suffix.lower() in DOCUMENT_SUFFIXES|ARCHIVE_SUFFIXES and size>100*1024**2):
            raise ValueError('RAR 成员大小未知或过大')
        crc=row.get('CRC','')
        if not re.fullmatch('[0-9A-Fa-f]{8}',crc):raise ValueError('RAR 成员缺少 CRC')
        total+=size
        if total>512*1024**2 or len(members)>=20000:raise ValueError('RAR 内容过大')
        members.append({'name':name,'size':size,'crc':int(crc,16),'directory':directory})
    return members


def _rar4_members(source,sevenzip):
    if not sevenzip:raise ValueError('读取 RAR4 需要本机 7-Zip 工具')
    if source.stat().st_size>512*1024**2:raise ValueError('RAR 过大')
    with tempfile.TemporaryFile() as output,tempfile.TemporaryFile() as errors:
        result=subprocess.run([str(sevenzip),'l','-slt','-sccUTF-8','-p-','--',str(source)],stdin=subprocess.DEVNULL,
                              stdout=output,stderr=errors,timeout=60,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        if result.returncode:raise ValueError('RAR4 列表读取失败或文件损坏')
        output.seek(0);raw=output.read(8*1024*1024+1)
    if len(raw)>8*1024*1024:raise ValueError('RAR4 列表过大')
    return _parse_rar4_listing(raw.decode('utf8'))


def extract_rar(source,input_root,output_dir,*,include_attachments=False):
    source=Path(source).resolve()
    sevenzip=_sevenzip()
    with source.open('rb') as stream:signature=stream.read(8)
    members=_rar4_members(source,sevenzip) if signature[:7]==b'Rar!\x1a\x07\0' else rar5_members(source)
    # Validate the complete inventory above, then decode only thesis inputs.
    # Videos and courseware are attachments, including those larger than a
    # document's extraction limit. Nested archives remain explicit failures.
    members=[member for member in members if not member['directory']
             and (include_attachments or PureWindowsPath(member['name']).suffix.lower() in DOCUMENT_SUFFIXES|ARCHIVE_SUFFIXES)]
    if any(PureWindowsPath(member['name']).suffix.lower() in ARCHIVE_SUFFIXES for member in members):
        raise ValueError('请先单独展开嵌套 RAR 内容')
    if not members:return []
    unrar=Path(r'C:\Program Files\WinRAR\UnRAR.exe')
    tar=Path(os.environ.get('SystemRoot',r'C:\Windows'))/'System32'/'tar.exe'
    if not sevenzip and not unrar.is_file() and not tar.is_file():raise ValueError('缺少支持 RAR 的本地解压工具')
    base=Path(output_dir).resolve()/source.stem;rows=[]
    for member in members:
        if member['directory']:continue
        destination=(base/Path(member['name'])).resolve()
        if not destination.is_relative_to(base):raise ValueError('RAR 输出路径越界')
        if sevenzip:command=[str(sevenzip),'x','-so','-y','-bd','--',str(source),member['name']]
        elif unrar.is_file():command=[str(unrar),'p','-inul','-p-',str(source),member['name']]
        else:command=[str(tar),'-xOf',str(source),'--',member['name']]
        payload=bytearray()
        with tempfile.TemporaryFile() as errors, subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=errors,
                            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)) as process:
            timer=threading.Timer(60,process.kill);timer.start()
            try:
                while True:
                    chunk=process.stdout.read(min(65536,member['size']-len(payload)+1))
                    if not chunk:break
                    payload.extend(chunk)
                    if len(payload)>member['size']:
                        process.kill();raise ValueError('RAR 解码超过声明大小')
                code=process.wait()
            finally:timer.cancel()
        if code or len(payload)!=member['size'] or zlib.crc32(payload)!=member['crc']:
            raise ValueError('RAR 成员解码、长度或 CRC 校验失败')
        destination.parent.mkdir(parents=True,exist_ok=True)
        with destination.open('xb') as stream:stream.write(payload)
        suffix=destination.suffix.lower()
        if include_attachments or suffix in {'.doc','.docx','.pdf','.txt'}:
            rows.append({'archive_path':str(source),'inner_path':member['name'],'extracted_path':str(destination),
                         'file_ext':suffix,'status':'success','error_message':''})
        elif suffix in {'.rar','.zip','.7z'}:raise ValueError('请先单独展开嵌套 RAR 内容')
    return rows
