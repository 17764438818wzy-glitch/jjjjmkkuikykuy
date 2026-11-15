import hashlib
import io
import json
from pathlib import Path
import tarfile

import pytest

from paperflow.remote_files import file_map, manifest, receive_archive, write_archive, publish_output, map_text,relative


def test_stream_unicode_files_and_replace_only_after_complete(tmp_path):
    source=tmp_path/'源 稿';source.mkdir();(source/'图表').mkdir()
    (source/'正文.txt').write_text('完整原稿\n',encoding='utf-8')
    (source/'图表/图片.bin').write_bytes(bytes(range(256))*5)
    files=file_map(source);expected=manifest(files);stream=io.BytesIO()
    write_archive(stream,files);stream.seek(0)
    stage=tmp_path/'staging';receive_archive(stream,stage,expected)
    output=tmp_path/'output';output.mkdir();(output/'old').write_text('old')
    publish_output(stage,output)
    assert not (output/'old').exists()
    assert (output/'正文.txt').read_text(encoding='utf-8')=='完整原稿\n'
    assert manifest(file_map(output))==expected


@pytest.mark.parametrize('name',['../outside','/absolute','C:/absolute','a\\escape','a/../escape'])
def test_archive_traversal_rejected(tmp_path,name):
    stream=io.BytesIO()
    with tarfile.open(fileobj=stream,mode='w') as archive:
        item=tarfile.TarInfo(name);item.size=1;archive.addfile(item,io.BytesIO(b'x'))
    stream.seek(0)
    with pytest.raises(ValueError):receive_archive(stream,tmp_path/'stage',[dict(path=name,size=1,sha256=hashlib.sha256(b'x').hexdigest())])
    assert not (tmp_path/'outside').exists()


def test_changed_file_never_qualifies(tmp_path):
    source=tmp_path/'source';source.mkdir();(source/'x').write_bytes(b'old')
    files=file_map(source);expected=manifest(files);(source/'x').write_bytes(b'new')
    stream=io.BytesIO();write_archive(stream,files);stream.seek(0)
    with pytest.raises(ValueError,match='hash'):receive_archive(stream,tmp_path/'stage',expected)


def test_archive_link_and_duplicate_rejected(tmp_path):
    expected=[dict(path='a',size=1,sha256=hashlib.sha256(b'x').hexdigest())]
    for link in (True,False):
        stream=io.BytesIO()
        with tarfile.open(fileobj=stream,mode='w') as archive:
            item=tarfile.TarInfo('a');item.size=1
            if link:item.type=tarfile.SYMTYPE;item.linkname='../outside'
            archive.addfile(item,io.BytesIO(b'x'))
            if not link:archive.addfile(item,io.BytesIO(b'x'))
        stream.seek(0)
        with pytest.raises(ValueError):receive_archive(stream,tmp_path/('link' if link else 'dup'),expected)


def test_file_map_rejects_symlink(tmp_path):
    root=tmp_path/'root';root.mkdir();outside=tmp_path/'outside';outside.write_text('secret')
    try:(root/'link').symlink_to(outside)
    except OSError:pytest.skip('symlink permission unavailable')
    with pytest.raises(ValueError):file_map(root)


def test_path_mapping_prefers_deepest_prefix_and_keeps_identity():
    mapping={r'E:\run':'/job/output',r'E:\run\reading':'/job/input/reading'}
    text='身份 source_id=original-id; 读取 E:\\run\\reading/source.txt，输出 E:\\run/profile.json'
    result=map_text(text,mapping)
    assert 'source_id=original-id' in result
    assert '/job/input/reading/source.txt' in result
    assert '/job/output/profile.json' in result


@pytest.mark.parametrize('name',['CON','a/aux.txt','Lpt1.bin','a/.. /outside','a./b','a /b','a/<bad>.txt'])
def test_linux_output_must_be_a_safe_windows_file_name(name):
    with pytest.raises(ValueError):relative(name)
