import zlib
import pytest
from pathlib import Path
import archive_input


def listing(name='论文/paper.docx',**changes):
    row={'Path':name,'Folder':'-','Size':'3','Encrypted':'-','Split Before':'-',
         'Split After':'-','CRC':f'{zlib.crc32(b"abc"):08X}','Host OS':'Win32','Attributes':'A'}
    row.update(changes)
    return ('Type = Rar\nMultivolume = -\nVolumes = 1\n----------\n'+
            '\n'.join(f'{k} = {v}' for k,v in row.items()))


def test_rar4_listing_accepts_bounded_regular_document():
    assert archive_input._parse_rar4_listing(listing())==[{'name':'论文/paper.docx','size':3,'crc':zlib.crc32(b'abc'),'directory':False}]


@pytest.mark.parametrize('change',[{'Path':'../escape.docx'},{'Path':'C:/escape.docx'},
    {'Path':'CON.docx'},{'Path':'x:ads'},{'Encrypted':'+'},{'Split After':'+'},
    {'Attributes':'l'},{'Host OS':'Unix'},{'Size':'104857601'},{'CRC':'bad'}])
def test_rar4_listing_rejects_unsafe_members(change):
    with pytest.raises(ValueError):archive_input._parse_rar4_listing(listing(**change))


def test_rar4_listing_rejects_duplicate_and_multivolume():
    one=listing()
    for value in [one.replace('Multivolume = -','Multivolume = +'),one+'\n'+one.split('----------\n')[1]]:
        with pytest.raises(ValueError):archive_input._parse_rar4_listing(value)
