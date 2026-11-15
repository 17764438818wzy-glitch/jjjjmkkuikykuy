import zipfile

from auto_pipeline import lightweight
from content_rules import detect_title


def source(tmp_path, requirement='wps', repeat=False):
    title='BCZY公司存货管理研究'
    xml=f'''<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
      xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
      xmlns:wps="http://schemas.microsoft.com/office/word/2010/wordprocessingShape"
      xmlns:future="urn:unknown"><w:body>
      <w:p><w:r><w:t>本科毕业设计（论文）</w:t></w:r></w:p>
      <w:p><w:r><mc:AlternateContent><mc:Choice Requires="{requirement}">
        <w:r><w:t>{title if requirement=='wps' else 'unsupported representation'}</w:t></w:r>
      </mc:Choice><mc:Fallback><w:r><w:t>{title}</w:t></w:r></mc:Fallback></mc:AlternateContent></w:r></w:p>
      {('<w:p><w:r><w:t>'+title+'</w:t></w:r></w:p>') if repeat else ''}
      <w:p><w:r><w:t>摘要</w:t></w:r></w:p>
      </w:body></w:document>'''
    path=tmp_path/'paper.docx'
    with zipfile.ZipFile(path,'w') as archive:archive.writestr('word/document.xml',xml)
    return path,title


def test_choice_and_fallback_are_not_both_counted_in_lightweight_text(tmp_path):
    path,title=source(tmp_path)
    before=path.read_bytes();text,_=lightweight(path)
    assert text.count(title)==1
    assert detect_title(text)==title
    assert path.read_bytes()==before


def test_unsupported_choice_uses_fallback(tmp_path):
    path,title=source(tmp_path,'future')
    text,_=lightweight(path)
    assert title in text and 'unsupported representation' not in text


def test_real_repeated_paragraphs_are_not_heuristically_deleted(tmp_path):
    path,title=source(tmp_path,repeat=True)
    text,_=lightweight(path)
    assert text.count(title)==2
