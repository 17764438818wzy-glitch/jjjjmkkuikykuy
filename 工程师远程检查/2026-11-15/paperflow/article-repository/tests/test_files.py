import importlib
import hashlib
import zipfile

import pytest

def service(tmp_path):
    assert importlib.util.find_spec('article_repository.files'), 'NAS file service is not implemented'
    from article_repository.files import Files
    return Files(str(tmp_path))

def test_original_bytes_and_partial_zip_are_preserved(tmp_path):
    files=service(tmp_path)
    (tmp_path/'原文.docx').write_bytes(b'original paper')
    row={'id':'body','name':'原文.docx','relative':'原文.docx','sha256':hashlib.sha256(b'original paper').hexdigest(),'bytes':14,'kind':'原文'}
    missing={'id':'attachment','name':'附件.docx','relative':'missing','kind':'附件'}
    result=files.observe([row,missing])
    assert [f['availability'] for f in result]==['available','missing']
    path=files.download(row)
    assert path.read_bytes()==b'original paper'
    path.unlink()
    path=files.bundle({'title':'论文','files':[row,missing],'inventory_complete':True})
    with zipfile.ZipFile(path) as archive:
        assert archive.read('原文.docx')==b'original paper'
        assert '不完整' in archive.read('资料完整性.txt').decode('utf8')
    path.unlink()

def test_untrusted_paths_and_wrong_hash_are_rejected(tmp_path):
    files=service(tmp_path)
    with pytest.raises(ValueError): files.observe([{'relative':'../secret','id':'x'}])
    (tmp_path/'body').write_bytes(b'body')
    with pytest.raises(ValueError): files.download({'relative':'body','sha256':'a'*64})
    assert files.observe([{'id':'none','relative':''}])[0]['availability']=='unknown'
