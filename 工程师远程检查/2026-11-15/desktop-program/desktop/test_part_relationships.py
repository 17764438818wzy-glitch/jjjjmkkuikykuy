import io
import zipfile
import pytest
from PIL import Image
from native_docx import parse_docx
from test_graphics_recovery import NS,chart_xml


def image_bytes(color):
    stream=io.BytesIO();Image.new('RGB',(4,3),color).save(stream,format='PNG');return stream.getvalue()


def make_docx(path, note='endnotes', target='media/note.png', external=False):
    note_tag=note[:-1];body=image_bytes('red');note_image=image_bytes('blue')
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('word/document.xml',f'<w:document {NS}><w:body><w:p><w:r><w:drawing><a:blip r:embed="same"/></w:drawing></w:r></w:p></w:body></w:document>')
        z.writestr('word/_rels/document.xml.rels','<Relationships><Relationship Id="same" Target="media/body.png"/><Relationship Id="onlyBody" Target="media/body.png"/></Relationships>')
        z.writestr(f'word/{note}.xml',f'<w:{note} {NS}><w:{note_tag} w:id="20"><w:p><w:r><w:drawing><a:blip r:embed="same"/></w:drawing></w:r></w:p></w:{note_tag}></w:{note}>')
        mode=' TargetMode="External"' if external else ''
        z.writestr(f'word/_rels/{note}.xml.rels',f'<Relationships><Relationship Id="same" Target="{target}"{mode}/></Relationships>')
        z.writestr('word/media/body.png',body);z.writestr('word/media/note.png',note_image)
        z.writestr('assets/note image.png',note_image)
        # A literal escaping name must never become evidence even if inside ZIP.
        z.writestr('../escape.png',note_image)
    return body,note_image


def parse_resources(source):
    found=[];issues=[]
    def resource(rid,data,extension,kind,position):
        found.append({'data':data,'position':dict(position),'extension':extension});return {'id':rid}
    parse_docx(source,lambda s:s,resource,lambda code,message,position:issues.append(code))
    return found,issues


@pytest.mark.parametrize('part',['endnotes','footnotes'])
def test_note_rids_are_scoped_to_their_own_part(tmp_path,part):
    path=tmp_path/'source.docx';body,note=make_docx(path,note=part)
    found,issues=parse_resources(path)
    assert [r['data'] for r in found]==[body,note]
    assert found[1]['position']=={'part':f'word/{part}.xml','id':'20'}
    assert 'image_missing' not in issues


@pytest.mark.parametrize('target',['/word/media/note.png','../assets/note%20image.png'])
def test_note_relationship_resolves_safe_root_and_relative_package_uris(tmp_path,target):
    path=tmp_path/'source.docx';body,note=make_docx(path,target=target)
    assert [r['data'] for r in parse_resources(path)[0]]==[body,note]


@pytest.mark.parametrize('target,external',[
    ('https://example.test/image.png',True),('https://example.test/image.png',False),
    ('../../escape.png',False),('../%2e%2e/escape.png',False),('//server/image.png',False)])
def test_unsafe_note_relationship_never_falls_back_to_body_rid(tmp_path,target,external):
    path=tmp_path/'source.docx';body,_=make_docx(path,target=target,external=external)
    found,issues=parse_resources(path)
    assert [r['data'] for r in found]==[body]
    assert 'image_missing' in issues


def test_missing_note_relationship_does_not_reuse_main_document_rid(tmp_path):
    path=tmp_path/'source.docx';body,_=make_docx(path)
    with zipfile.ZipFile(path) as z:entries={n:z.read(n) for n in z.namelist()}
    entries.pop('word/_rels/endnotes.xml.rels')
    with zipfile.ZipFile(path,'w') as z:
        for name,data in entries.items():z.writestr(name,data)
    found,issues=parse_resources(path)
    assert [r['data'] for r in found]==[body] and 'image_missing' in issues


def test_note_chart_uses_its_part_relationship_and_real_native_data(tmp_path,monkeypatch):
    import native_chart
    path=tmp_path/'source.docx';make_docx(path)
    with zipfile.ZipFile(path) as z:entries={n:z.read(n) for n in z.namelist()}
    entries['word/endnotes.xml']=f'<w:endnotes {NS}><w:endnote w:id="20"><w:p><w:r><w:drawing><c:chart r:id="same"/></w:drawing></w:r></w:p></w:endnote></w:endnotes>'.encode()
    entries['word/_rels/endnotes.xml.rels']=b'<Relationships><Relationship Id="same" Target="charts/note.xml"/></Relationships>'
    entries['word/charts/note.xml']=chart_xml().encode()
    with zipfile.ZipFile(path,'w') as z:
        for name,data in entries.items():z.writestr(name,data)
    original=native_chart.chart_table;resolved=[]
    def record(archive,part,position,redact):
        result=original(archive,part,position,redact);resolved.append(result);return result
    monkeypatch.setattr(native_chart,'chart_table',record)
    _,issues=parse_resources(path)
    assert len(resolved)==1
    assert resolved[0]['native_chart']['series'][0]['name']=='净利润率'
    assert 'native_chart_visual' in issues
