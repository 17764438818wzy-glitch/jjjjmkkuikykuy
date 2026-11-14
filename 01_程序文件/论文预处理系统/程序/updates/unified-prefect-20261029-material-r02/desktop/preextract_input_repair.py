"""Narrow repairs of derived reading inputs. Original files are never rewritten."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import uuid
import zipfile
import zlib


def normalize_text(value, counts=None):
    """Combine valid surrogate pairs and explicitly replace isolated code units."""
    if isinstance(value,str):
        if not re.search(r'[\ud800-\udfff]',value):return value
        pairs=len(re.findall(r'[\ud800-\udbff][\udc00-\udfff]',value))
        isolated=len(re.findall(r'[\ud800-\udfff]',value))-2*pairs
        if counts is not None:
            counts['combined_pairs']=counts.get('combined_pairs',0)+pairs
            counts['isolated_replacements']=counts.get('isolated_replacements',0)+isolated
        return value.encode('utf-16-le','surrogatepass').decode('utf-16-le','replace')
    if isinstance(value,list):return [normalize_text(v,counts) for v in value]
    if isinstance(value,tuple):return tuple(normalize_text(v,counts) for v in value)
    if isinstance(value,dict):return {k:normalize_text(v,counts) for k,v in value.items()}
    return value


def repair_media_crc(source, directory, *, stop=None):
    """Rebuild a private ZIP only when every CRC-failing image fully decodes.

    Bypassing a CRC here recovers bytes, not their original integrity. Every
    recovered member is recorded with both CRCs and its exact content hash.
    XML, relationships, attachments and other non-media parts remain strict.
    """
    from PIL import Image
    from processing_stop import check_stop
    from preextract_material import file_sha,write_json
    source=Path(source);directory=Path(directory)
    directory.mkdir(parents=True,exist_ok=True)
    original_hash=file_sha(source)
    target=directory/('media-crc-'+uuid.uuid4().hex+'.docx')
    temporary=target.with_suffix('.tmp')
    repairs=[];members=[];total=0
    try:
        with zipfile.ZipFile(source) as archive,zipfile.ZipFile(temporary,'x') as output:
            names=set()
            for info in archive.infolist():
                check_stop(stop)
                if info.filename in names:raise ValueError('DOCX 部件重名，保留原稿待核对')
                names.add(info.filename)
                if info.file_size>128*1024**2 or total+info.file_size>512*1024**2:
                    raise ValueError('DOCX 修复副本超过容量限制，保留原稿')
                try:
                    blob=archive.read(info)
                except zipfile.BadZipFile as exc:
                    if (not info.filename.startswith('word/media/') or
                            not str(exc).startswith('Bad CRC-32 for file ')):
                        raise
                    # Only this one stream skips the stored CRC. Full deflate
                    # decoding, output size, and image decoding stay required.
                    with archive.open(info) as stream:
                        stream._expected_crc=None
                        blob=stream.read(info.file_size+1)
                    if len(blob)!=info.file_size:raise ValueError('DOCX 图片长度不符，保留原稿')
                    try:
                        with Image.open(io.BytesIO(blob)) as image:image.verify()
                        with Image.open(io.BytesIO(blob)) as image:image.load()
                    except Exception as image_error:
                        raise ValueError('DOCX 内嵌图片实际不可解码，保留原稿；需完整原文件或人工修复') from image_error
                    repairs.append(dict(member=info.filename,stored_crc32=info.CRC,
                        recovered_crc32=zlib.crc32(blob)&0xffffffff,
                        sha256=hashlib.sha256(blob).hexdigest(),bytes=len(blob),
                        integrity='crc_mismatch_original_integrity_unknown',image_decode='complete'))
                total+=len(blob)
                output.writestr(copy.copy(info),blob)
                members.append(dict(member=info.filename,sha256=hashlib.sha256(blob).hexdigest(),bytes=len(blob)))
            output.comment=archive.comment
        if not repairs:raise ValueError('未找到可定向修复的 DOCX 图片 CRC 问题')
        if file_sha(source)!=original_hash:raise ValueError('DOCX 原稿在修复期间发生变化')
        os.replace(temporary,target)
        receipt=target.with_suffix('.repair.json')
        write_json(receipt,dict(schema='docx-media-crc-copy/1',source=str(source),source_sha256=original_hash,
            derived=str(target),derived_sha256=file_sha(target),members=members,repairs=repairs,
            original_unchanged=True,missing_members=[]))
        return target,receipt,repairs
    finally:
        temporary.unlink(missing_ok=True)
