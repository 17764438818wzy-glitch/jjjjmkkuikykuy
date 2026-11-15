import hashlib

import pytest


def test_large_text_attachment_has_contiguous_bounded_preview_segments(tmp_path):
    from attachment_preview import preview_attachment
    source = tmp_path / 'attachment.txt'
    original = ('abcdefghijklmnopqrstuvwxyz0123456789' * 1400).encode('utf8')
    source.write_bytes(original)
    preview = preview_attachment(source, 'txt', limits={
        'text_bytes': 60000, 'text_chars': 30000, 'segment_chars': 5000})
    assert preview['status'] == 'preview' and preview['truncated'] is True
    assert len(preview['segments']) == 6
    rebuilt = ''.join(preview['text'][part['start']:part['end']] for part in preview['segments'])
    assert rebuilt == original[:30000].decode('utf8')
    assert all(part['end'] - part['start'] <= 5000 for part in preview['segments'])
    assert hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(original).digest()


def test_long_word_preview_is_segmented_without_cutting_or_copying_original(tmp_path):
    from docx import Document
    from attachment_preview import preview_attachment
    source = tmp_path / 'attachment.docx'
    doc = Document(); doc.add_paragraph('研究资料' * 4000); doc.save(source)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    preview = preview_attachment(source, 'docx', limits={'text_chars': 20000, 'segment_chars': 4000})
    assert preview['status'] == 'preview' and preview['truncated'] is False
    assert len(preview['segments']) == 4
    assert ''.join(preview['text'][x['start']:x['end']] for x in preview['segments']) == '研究资料' * 4000
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest


def test_text_continuation_preserves_multibyte_boundaries_and_detects_source_change(tmp_path):
    from attachment_preview import preview_attachment
    source = tmp_path/'long.txt'; original = '中A文🙂' * 1200
    source.write_text(original, encoding='utf8')
    limits={'text_bytes': 1001, 'text_chars': 300, 'segment_chars': 256}
    cursor = None; pieces = []; first_cursor = None
    for _ in range(100):
        row = preview_attachment(source, 'txt', limits=limits, cursor=cursor)
        assert row['status'] == 'preview'
        pieces.append(row['text']); cursor = row['next_cursor']
        if first_cursor is None:first_cursor = cursor
        if not cursor:break
    else:pytest.fail('text continuation failed to advance')
    assert ''.join(pieces) == original
    source.write_text(original + 'changed', encoding='utf8')
    assert preview_attachment(source, 'txt', limits=limits, cursor=first_cursor)['status'] == 'unavailable'
    from attachment_preview import preview_attachment_chunks
    failed = preview_attachment_chunks(source, 'txt', limits=limits, cursor=first_cursor, max_chunks=4)
    assert failed['chunks_processed'] == 1 and failed['next_cursor_after_pass'] == first_cursor
    assert failed['continuation_error']


@pytest.mark.parametrize('encoding', ['utf-16', 'utf-16-be', 'gb18030'])
def test_text_resume_preserves_bom_and_multibyte_encoding(tmp_path, encoding):
    from attachment_preview import preview_attachment
    import codecs
    source=tmp_path/'attachment.txt';original='中A文' * 800
    raw=original.encode(encoding)
    if encoding=='utf-16-be':raw=codecs.BOM_UTF16_BE + raw
    source.write_bytes(raw);cursor=None;pieces=[]
    for _ in range(100):
        row=preview_attachment(source,'txt',limits={'text_bytes':1001,'text_chars':300},cursor=cursor)
        assert row['status']=='preview'
        pieces.append(row['text']);cursor=row['next_cursor']
        if not cursor:break
    else:pytest.fail('encoded text continuation failed to advance')
    assert ''.join(pieces)==original and source.read_bytes()==raw


def test_gb18030_after_an_ascii_prefix_can_continue_without_loss(tmp_path):
    from attachment_preview import preview_attachment
    source=tmp_path/'late-chinese.txt';original='a'*3000 + '中文研究'*200
    source.write_bytes(original.encode('gb18030'))
    cursor=None;pieces=[]
    for _ in range(20):
        row=preview_attachment(source,'txt',limits={'text_bytes':1000,'text_chars':1000},cursor=cursor)
        assert row['status']=='preview'
        pieces.append(row['text']);cursor=row['next_cursor']
        if not cursor:break
    else:pytest.fail('GB18030 continuation failed to advance')
    assert ''.join(pieces)==original


def test_gb18030_with_utf8_compatible_intermediate_bytes_is_not_misdecoded(tmp_path):
    from attachment_preview import preview_attachment
    source=tmp_path/'ambiguous.txt';raw=bytes.fromhex('4141c2a9d6d0');source.write_bytes(raw)
    cursor=None;pieces=[]
    for _ in range(5):
        row=preview_attachment(source,'txt',limits={'text_bytes':2,'text_chars':2},cursor=cursor)
        assert row['status']=='preview'
        pieces.append(row['text']);cursor=row['next_cursor']
        if not cursor:break
    assert ''.join(pieces)==raw.decode('gb18030')


def test_pdf_and_word_continuations_cover_original_preview_text(tmp_path):
    import fitz
    from docx import Document
    from attachment_preview import preview_attachment
    pdf_path=tmp_path/'a.pdf'; pdf=fitz.open()
    expected=[]
    for i in range(5):
        page=pdf.new_page(); value=f'Page {i} research content and additional preview text.'
        page.insert_text((40,80),value); expected.append(value)
    pdf.save(pdf_path); pdf.close()
    cursor=None; pages=[]
    for _ in range(20):
        row=preview_attachment(pdf_path,'pdf',limits={'pdf_pages':2,'text_chars':35},cursor=cursor)
        pages.extend(row['pages']);cursor=row['next_cursor']
        if not cursor:break
    else:pytest.fail('PDF continuation failed to advance')
    assembled={n:''.join(p['text'] for p in pages if p['page']==n) for n in range(1,6)}
    assert [assembled[n] for n in range(1,6)] == expected

    doc_path=tmp_path/'a.docx';doc=Document();doc.add_paragraph('研究材料'*4000);doc.save(doc_path)
    cursor=None;pieces=[]
    for _ in range(10):
        row=preview_attachment(doc_path,'docx',limits={'text_chars':6000},cursor=cursor)
        pieces.append(row['text']);cursor=row['next_cursor']
        if not cursor:break
    assert ''.join(pieces) == '研究材料'*4000


def test_archive_directory_can_resume_without_unpacking(tmp_path):
    import zipfile
    from attachment_preview import preview_attachment
    source=tmp_path/'a.zip'
    with zipfile.ZipFile(source,'w') as archive:
        for n in range(10):archive.writestr(f'{n}.txt', f'value {n}')
    cursor=None;names=[]
    for _ in range(10):
        row=preview_attachment(source,'zip',limits={'archive_members':3},cursor=cursor)
        names.extend(x['name'] for x in row['members']);cursor=row['next_cursor']
        if not cursor:break
    assert names == [f'{n}.txt' for n in range(10)]
    assert sorted(p.name for p in tmp_path.iterdir()) == ['a.zip']


def test_large_zip_allows_bounded_directory_preview_without_unpacking(tmp_path):
    import zipfile
    from attachment_preview import preview_attachment
    source=tmp_path/'large.zip'
    with source.open('wb') as stream:stream.truncate(65*1024*1024)
    with zipfile.ZipFile(source,'a') as archive:
        for n in range(3):archive.writestr(f'{n}.txt', 'small')
    row=preview_attachment(source,'zip',limits={'archive_members':1})
    assert row['status']=='preview' and row['members'][0]['name']=='0.txt'
    assert row['next_cursor'] and len(list(tmp_path.iterdir()))==1


def test_pdf_failed_ocr_page_is_recorded_and_later_pages_continue(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    from attachment_preview import preview_attachment_chunks
    picture=Image.new('RGB',(200,200),'white'); stream=io.BytesIO();picture.save(stream,format='PNG')
    source=tmp_path/'scan.pdf';pdf=fitz.open();page=pdf.new_page()
    page.insert_image(page.rect,stream=stream.getvalue());page.insert_text((40,80),'TITLE');page=pdf.new_page()
    page.insert_text((40,80),'Later page continues with useful research text.');pdf.save(source);pdf.close()
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:[])
    row=preview_attachment_chunks(source,'pdf',limits={'pdf_pages':5})
    assert row['pages'][0]['text']=='TITLE'
    assert row['pages'][1]['text'].startswith('Later page')
    assert row['unread_pages'][0]['page']==1 and row['unread_pages'][0]['retry_cursor']['page']==0
    assert row['truncated'] is True
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:[{'text':'OCR-RECOVERED-TEXT'}])
    retry=preview_attachment_chunks(source,'pdf',limits={'pdf_pages':1,'text_chars':8},
                                    cursor=row['unread_pages'][0]['retry_cursor'])
    parts=[retry]+retry['continuations']
    assert all(p['pages'][0]['page']==1 for p in parts)
    assert ''.join(p['pages'][0]['text'] for p in parts)=='TITLE\nOCR-RECOVERED-TEXT'
    assert retry['next_cursor_after_pass'] is None
    assert retry['retry_scope']=={'mode':'replace_page','page':1}


def test_attachment_preview_automatically_continues_with_bounded_chunks(tmp_path):
    from attachment_preview import preview_attachment_chunks
    source=tmp_path/'long.txt';source.write_text('x'*28000,encoding='utf8')
    result=preview_attachment_chunks(source,'txt',limits={'text_chars':8000},max_chunks=2)
    assert result['chunks_processed'] == 2
    assert len(result['text']) == len(result['continuations'][0]['text']) == 8000
    assert result['next_cursor_after_pass'] is not None
    resumed=preview_attachment_chunks(source,'txt',limits={'text_chars':8000},max_chunks=2,
                                      cursor=result['next_cursor_after_pass'])
    assert resumed['chunks_processed'] == 2 and resumed['next_cursor_after_pass'] is None
    assert len(resumed['text']) == 8000 and len(resumed['continuations'][0]['text']) == 4000
    assert resumed['first_chunk_truncated'] is True and resumed['truncated'] is False


def test_chunked_preview_does_not_rehash_whole_file_for_every_chunk(tmp_path, monkeypatch):
    import attachment_preview
    source=tmp_path/'long.txt';source.write_text('x'*60000,encoding='utf8')
    original=attachment_preview._sha256; calls=[]
    def tracked(path): calls.append(path); return original(path)
    monkeypatch.setattr(attachment_preview,'_sha256',tracked)
    result=attachment_preview.preview_attachment_chunks(source,'txt',limits={'text_chars':1000},max_chunks=4)
    assert result['chunks_processed']==4 and len(calls)<=2


def test_repeated_registered_attachment_handoff_does_not_read_either_content(tmp_path, monkeypatch):
    import json
    import attachment_preview
    import direct_source
    import local_prepare
    delivery=tmp_path/'delivery'; (delivery/'files').mkdir(parents=True)
    receipt=tmp_path/'receipt.json'; receipt.write_text('{}',encoding='utf8')
    items=[]
    for n in (1,2):
        source=delivery/'files'/f'{n}.txt'; source.write_text(f'item{n}-contents',encoding='utf8')
        items.append({'file_id':f'f{n}','kind':'attachment','state':'已接收','path':str(source),
                      'bytes':source.stat().st_size,'sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
                      'detected_format':'txt'})
    observed={'paper_id':'p','group_id':'g','receipt_id':'r','receipt_path':str(receipt),
              'receipt_sha256':hashlib.sha256(receipt.read_bytes()).hexdigest(),
              'delivery_dir':str(delivery),'items':items}
    monkeypatch.setattr(local_prepare,'_observe_registered',lambda item,root:observed)
    def forbidden(*args,**kwargs): raise AssertionError('附件不应被解析')
    monkeypatch.setattr(direct_source,'detect_format',forbidden)
    monkeypatch.setattr(attachment_preview,'preview_attachment_chunks',forbidden)
    first=local_prepare.prepare_registered_attachments({},receipt_root=tmp_path,work_dir=tmp_path/'work')
    second=local_prepare.prepare_registered_attachments({},receipt_root=tmp_path,work_dir=tmp_path/'work')
    inv=json.loads(open(first['inventory_path'],encoding='utf8').read())
    assert first==second
    assert {row['file_id'] for row in inv['items']}=={'f1','f2'}
    assert all(row['content_status']=='upstream_only' and 'preview' not in row for row in inv['items'])

def test_contended_local_lock_retries_and_releases(tmp_path, monkeypatch):
    import filelock
    from local_prepare import _retrying_file_lock
    events = []
    class FakeLock:
        def __init__(self, path, timeout): self.path = path
        def acquire(self):
            events.append('try')
            if events.count('try') < 3: raise filelock.Timeout(self.path)
        def release(self): events.append('release')
    monkeypatch.setattr(filelock, 'FileLock', FakeLock)
    with _retrying_file_lock(tmp_path/'test.lock', timeout=.01, attempts=3):
        events.append('held')
    assert events == ['try', 'try', 'try', 'held', 'release']


def test_local_lock_does_not_swallow_stop_or_retry_forever(tmp_path, monkeypatch):
    import filelock
    from local_prepare import _retrying_file_lock
    events = []
    class FakeLock:
        def __init__(self, path, timeout): self.path = path
        def acquire(self): events.append('try'); raise filelock.Timeout(self.path)
        def release(self): events.append('release')
    monkeypatch.setattr(filelock, 'FileLock', FakeLock)
    with pytest.raises(filelock.Timeout):
        with _retrying_file_lock(tmp_path/'test.lock', timeout=.01, attempts=3): pass
    assert events == ['try'] * 3

