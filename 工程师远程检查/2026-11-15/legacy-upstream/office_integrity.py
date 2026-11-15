"""Conservative recovery of missing ZIP checksums in a separate DOCX copy."""
import io
import struct
import zipfile
import zlib
from pathlib import Path
from xml.etree import ElementTree as ET

from PIL import Image


def recover_zero_crc_media(source, destination):
    """Patch only zero checksums for validated PNG/JPEG members; return names.

    All uncompressed payload bytes stay identical. ZIP metadata is reserialized
    for importers that reject the original nonstandard container layout.
    Actual corruption, nonzero CRC mismatches, and unknown image formats remain
    errors. Never replace an original or an existing destination.
    """
    source=Path(source);destination=Path(destination)
    if source.resolve()==destination.resolve():raise ValueError('原文件不能作为恢复目标')
    if destination.exists():raise FileExistsError(destination)
    raw=source.read_bytes();patched=bytearray(raw);changes=[]
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members=archive.infolist()
        if len({m.filename for m in members})!=len(members):
            raise ValueError('DOCX 含重名 ZIP 成员，不能安全恢复')
        central=archive.start_dir
        for member in members:
            if raw[central:central+4]!=b'PK\x01\x02':raise ValueError('ZIP 中央目录无效')
            name_size,extra_size,comment_size=struct.unpack_from('<3H',raw,central+28)
            try:payload=archive.read(member)
            except zipfile.BadZipFile:
                if member.CRC!=0 or not member.filename.startswith('word/media/'):
                    raise
                if Path(member.filename).suffix.lower() not in {'.png','.jpg','.jpeg'}:
                    raise
                if member.flag_bits & 1:raise ValueError('加密成员不能恢复')
                header=struct.unpack_from('<4s5H3L2H',raw,member.header_offset)
                if header[0]!=b'PK\x03\x04' or header[6]!=0:
                    raise ValueError('本地 CRC 不是缺失值')
                start=member.header_offset+30+header[-2]+header[-1]
                compressed=raw[start:start+member.compress_size]
                if member.compress_type==zipfile.ZIP_STORED:payload=compressed
                elif member.compress_type==zipfile.ZIP_DEFLATED:
                    decoder=zlib.decompressobj(-15);payload=decoder.decompress(compressed)
                    payload+=decoder.flush()
                    if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
                        raise ValueError('媒体压缩数据不完整')
                else:raise ValueError('未知压缩方式不能恢复')
                if len(payload)!=member.file_size:raise ValueError('媒体长度不匹配')
                with Image.open(io.BytesIO(payload)) as image:
                    if image.format not in {'PNG','JPEG'}:raise ValueError('媒体格式不匹配')
                    image.verify()
                with Image.open(io.BytesIO(payload)) as image:image.load()
                crc=zlib.crc32(payload)
                struct.pack_into('<L',patched,member.header_offset+14,crc)
                struct.pack_into('<L',patched,central+16,crc)
                if member.flag_bits & 8:
                    descriptor=start+member.compress_size
                    if raw[descriptor:descriptor+4]==b'PK\x07\x08':descriptor+=4
                    old_crc,packed_size,unpacked_size=struct.unpack_from('<3L',raw,descriptor)
                    if old_crc!=0 or packed_size!=member.compress_size or unpacked_size!=member.file_size:
                        raise ValueError('ZIP 数据描述符不匹配')
                    struct.pack_into('<L',patched,descriptor,crc)
                changes.append(member.filename)
            if member.filename.endswith(('.xml','.rels')):ET.fromstring(payload)
            # Also decode unaffected raster evidence; do not conceal unrelated damage.
            if member.filename.startswith('word/media/') and Path(member.filename).suffix.lower() in {'.png','.jpg','.jpeg'}:
                with Image.open(io.BytesIO(payload)) as image:image.verify()
                with Image.open(io.BytesIO(payload)) as image:image.load()
            central+=46+name_size+extra_size+comment_size
    if not changes:return []
    normalized=io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(patched)) as verified, zipfile.ZipFile(normalized,'w',compression=zipfile.ZIP_DEFLATED) as output:
        for member in verified.infolist():
            # A filename alone makes writestr insert the current wall-clock time.
            # Stable container metadata keeps exact-source PDF cache keys reusable.
            info=zipfile.ZipInfo(member.filename,date_time=(1980,1,1,0,0,0))
            info.create_system=0
            info.external_attr=0x10 if member.is_dir() else 0x20
            output.writestr(info,verified.read(member),compress_type=zipfile.ZIP_DEFLATED,compresslevel=6)
    with zipfile.ZipFile(io.BytesIO(normalized.getvalue())) as verified:
        for member in verified.infolist():verified.read(member)
    destination.parent.mkdir(parents=True,exist_ok=True)
    with destination.open('xb') as stream:stream.write(normalized.getvalue())
    return changes
