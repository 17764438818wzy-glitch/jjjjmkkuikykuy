import sys
from types import SimpleNamespace
import pytest

@pytest.mark.parametrize('error',[ValueError('TCP connection failed'),OSError('NAS offline')])
def test_unreachable_nas_keeps_registered_files_unknown(monkeypatch,error):
    from article_repository.files import Files
    def offline(*args,**kwargs): raise error
    monkeypatch.setitem(sys.modules,'smbclient',SimpleNamespace(register_session=offline))
    files=Files(r'\\nas\share',username='reader',password='test')
    result=files.observe([{'id':'original','relative':'folder/body.docx'}])
    assert result[0]['availability']=='unknown'
    with pytest.raises(OSError): files.download({'relative':'folder/body.docx'})

def test_smb_errno_missing_is_distinguished_from_network_unknown(monkeypatch):
    import errno
    from article_repository.files import Files
    class MissingOnSMB(OSError): pass
    def missing(path): raise MissingOnSMB(errno.ENOENT,'object not found')
    monkeypatch.setitem(sys.modules,'smbclient',SimpleNamespace(register_session=lambda *a,**k:None,stat=missing))
    service=Files(r'\\nas\share',username='reader',password='test')
    assert service.observe([{'id':'x','relative':'missing.docx'}])[0]['availability']=='missing'
