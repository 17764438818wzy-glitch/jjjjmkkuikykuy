import hashlib,os
from pathlib import Path
import pytest
from article_repository.files import Files
import nas_transfer

@pytest.fixture
def storage(tmp_path,monkeypatch):
    root=tmp_path/'nas';root.mkdir()
    monkeypatch.setattr(nas_transfer.smbclient,'makedirs',os.makedirs)
    monkeypatch.setattr(nas_transfer.smbclient,'rename',os.rename)
    monkeypatch.setattr(nas_transfer.smbclient,'remove',os.remove)
    return Files(str(root))

def test_atomic_publish_survives_temp_name_disappearing_and_replays(storage,tmp_path):
    source=tmp_path/'original.pdf';source.write_bytes(b'original bytes')
    sha=hashlib.sha256(source.read_bytes()).hexdigest()
    assert nas_transfer.publish_file(source,sha,storage) is True
    assert nas_transfer.publish_file(source,sha,storage) is False
    target=Path(storage.path({'relative':'objects/'+sha[:2]+'/'+sha}))
    assert target.read_bytes()==source.read_bytes()
    assert [p for p in target.parent.iterdir()]==[target]

def test_same_size_corrupt_nas_object_is_never_accepted(storage,tmp_path):
    source=tmp_path/'original.pdf';source.write_bytes(b'original bytes')
    sha=hashlib.sha256(source.read_bytes()).hexdigest()
    target=Path(storage.path({'relative':'objects/'+sha[:2]+'/'+sha}));target.parent.mkdir(parents=True)
    target.write_bytes(b'wrongxxx bytes')
    assert source.stat().st_size==target.stat().st_size
    with pytest.raises(ValueError,match='hash mismatch'):nas_transfer.publish_file(source,sha,storage)
    assert target.read_bytes()==b'wrongxxx bytes'
