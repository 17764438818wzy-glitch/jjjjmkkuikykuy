import struct
import zlib
import pytest


def vint(n):
    out=bytearray()
    while n>127:out.append((n&127)|128);n>>=7
    out.append(n);return bytes(out)


def block(data,payload=b''):
    header=vint(len(data))+data
    return struct.pack('<L',zlib.crc32(header))+header+payload


def archive(path,name='paper.txt',payload=b'original evidence',extra=b''):
    name=name.encode();flags=2|(1 if extra else 0)
    data=vint(2)+vint(flags)+(vint(len(extra)) if extra else b'')+vint(len(payload))
    data+=vint(4)+vint(len(payload))+vint(32)+struct.pack('<L',zlib.crc32(payload))
    data+=vint(0)+vint(0)+vint(len(name))+name+extra
    path.write_bytes(b'Rar!\x1a\x07\x01\0'+block(b'\x01\x00\x00')+block(data,payload)+block(b'\x05\x00\x00'))
    return path


def test_rar_extracts_verified_bytes_to_private_copy(tmp_path):
    from archive_input import extract_rar
    source=archive(tmp_path/'source.rar');before=source.read_bytes()
    rows=extract_rar(source,tmp_path,tmp_path/'out')
    assert len(rows)==1 and rows[0]['status']=='success'
    from pathlib import Path
    assert Path(rows[0]['extracted_path']).read_bytes()==b'original evidence'
    assert source.read_bytes()==before


@pytest.mark.parametrize('name',['../escape.txt','C:/escape.txt','/escape.txt','a:stream.txt'])
def test_rar_rejects_unsafe_paths_before_output(tmp_path,name):
    from archive_input import extract_rar
    source=archive(tmp_path/'source.rar',name=name)
    with pytest.raises(ValueError):extract_rar(source,tmp_path,tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_rar_rejects_redirection_and_bad_header_crc(tmp_path):
    from archive_input import extract_rar
    source=archive(tmp_path/'source.rar',extra=b'\x01\x05')
    with pytest.raises(ValueError):extract_rar(source,tmp_path,tmp_path/'out')
    source=archive(source);raw=bytearray(source.read_bytes());raw[10]^=1;source.write_bytes(raw)
    with pytest.raises(ValueError):extract_rar(source,tmp_path,tmp_path/'out')
