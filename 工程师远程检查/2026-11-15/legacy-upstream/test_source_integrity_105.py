import io
import struct
import zipfile

import pytest
from PIL import Image


def sample(path, *, corrupt='word/media/image1.png', crc=0, local_crc=None, damaged=False):
    image=io.BytesIO();Image.new('RGB',(7,5),'red').save(image,format='PNG')
    payloads={'[Content_Types].xml':b'<Types/>','word/document.xml':b'<document/>',
              'word/media/image1.png':b'broken image' if damaged else image.getvalue()}
    with zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for name,value in payloads.items():z.writestr(name,value)
    data=bytearray(path.read_bytes())
    with zipfile.ZipFile(path) as z:
        pos=z.start_dir
        for member in z.infolist():
            if member.filename==corrupt:
                struct.pack_into('<L',data,member.header_offset+14,crc if local_crc is None else local_crc)
                struct.pack_into('<L',data,pos+16,crc)
            name,extra,comment=struct.unpack_from('<3H',data,pos+28);pos+=46+name+extra+comment
    path.write_bytes(data)
    return payloads


def test_zero_media_crc_recovers_exact_payloads_without_changing_original(tmp_path):
    from office_integrity import recover_zero_crc_media
    source=tmp_path/'original.docx';target=tmp_path/'recovered.docx'
    expected=sample(source);before=source.read_bytes()
    assert recover_zero_crc_media(source,target)==['word/media/image1.png']
    assert source.read_bytes()==before
    with zipfile.ZipFile(target) as z:
        assert z.testzip() is None
        assert {name:z.read(name) for name in z.namelist()}==expected


@pytest.mark.parametrize('changes',[{'damaged':True},{'crc':123},
                                  {'local_crc':123},{'corrupt':'word/document.xml'}])
def test_unsafe_crc_or_actual_damage_is_not_recovered(tmp_path,changes):
    from office_integrity import recover_zero_crc_media
    source=tmp_path/'original.docx';target=tmp_path/'recovered.docx'
    sample(source,**changes);before=source.read_bytes()
    with pytest.raises((ValueError,zipfile.BadZipFile,OSError)):
        recover_zero_crc_media(source,target)
    assert source.read_bytes()==before
    assert not target.exists()


def test_recovery_cannot_replace_source_or_existing_destination(tmp_path):
    from office_integrity import recover_zero_crc_media
    source=tmp_path/'original.docx';sample(source);before=source.read_bytes()
    with pytest.raises(ValueError):recover_zero_crc_media(source,source)
    target=tmp_path/'existing.docx';target.write_bytes(b'existing')
    with pytest.raises(FileExistsError):recover_zero_crc_media(source,target)
    assert source.read_bytes()==before and target.read_bytes()==b'existing'


def test_recovery_bytes_are_identical_across_clock_changes(tmp_path,monkeypatch):
    import hashlib
    from office_integrity import recover_zero_crc_media
    source=tmp_path/'original.docx';expected=sample(source);before=source.read_bytes()
    first=tmp_path/'first.docx';second=tmp_path/'second.docx'
    with monkeypatch.context() as clock:
        clock.setattr(zipfile.time,'localtime',lambda *args:(2026,1,2,3,4,6,4,2,0))
        recover_zero_crc_media(source,first)
    with monkeypatch.context() as clock:
        clock.setattr(zipfile.time,'localtime',lambda *args:(2027,5,6,7,8,10,3,126,0))
        recover_zero_crc_media(source,second)
    assert hashlib.sha256(first.read_bytes()).digest()==hashlib.sha256(second.read_bytes()).digest()
    assert source.read_bytes()==before
    for path in [first,second]:
        with zipfile.ZipFile(path) as z:
            assert z.testzip() is None
            assert {n:z.read(n) for n in z.namelist()}==expected
