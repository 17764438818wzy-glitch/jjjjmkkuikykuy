"""One bounded native reading for purpose review; never edits the source."""
from __future__ import annotations

import hashlib
import json
import shutil
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

VERSION = 'purpose-evidence/1'
READER_REVISION = '2'
TEXT_LIMIT = 24000
IMAGE_LIMIT = 12


def _check(stop):
    if stop is not None and stop.is_set():
        raise InterruptedError('用途读取已停止，原件保留')


def _sha(path):
    result=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):
            result.update(chunk)
    return result.hexdigest()


def _image(source, destination):
    from PIL import Image, ImageOps
    with Image.open(source) as image:
        if getattr(image,'n_frames',1)!=1:
            raise ValueError('多帧图片尚未逐帧读取')
        image=ImageOps.exif_transpose(image).convert('RGB')
        image.thumbnail((1600,1600))
        image.save(destination,'PNG')


def collect(record, destination, *, stop=None, processor=None, source_sha256=None):
    source=Path(record['path'])
    destination=Path(destination)
    destination.mkdir(parents=True,exist_ok=True)
    _check(stop)
    before=source.stat()
    source_sha256=source_sha256 or _sha(source)
    pages=[];images=[];complete=True;objects=False;reader=source
    suffix=source.suffix.lower()
    if suffix in {'.doc','.rtf','.wps','.odt'}:
        from engine import convert_office
        from material_conversion import preparation_context
        from contextlib import nullcontext
        root=getattr(processor,'_resume_root',getattr(getattr(processor,'store',None),'root',destination))
        with preparation_context(root,enabled=True,stop=stop,
                                 wait_scope=getattr(processor,'_preextract_conversion',nullcontext)):
            reader=convert_office(source,destination/'converted')
        suffix=reader.suffix.lower()
    if suffix=='.pdf':
        import fitz
        with fitz.open(reader) as document:
            if document.needs_pass or len(document)>1500:
                raise ValueError('加密或超出用途读取范围的 PDF，原件保留')
            count=len(document)
            objects=bool(document.embfile_count())
            visual=set(range(count)) if count<=6 else {0,1,2,count//2,count-1}
            drawing_hint='图' in (record.get('name') or source.name)
            for number,page in enumerate(document):
                _check(stop)
                text=page.get_text()
                page_images=bool(page.get_images())
                if any(annot.type[1] in {'FileAttachment','Sound','Movie','Screen','RichMedia','3D'}
                       for annot in (page.annots() or [])):
                    objects=True
                entry=dict(ref=f'p{number+1}',text=text,width=page.rect.width,height=page.rect.height)
                pages.append(entry)
                needs_visual=len(text.strip())<100 or page_images or drawing_hint and number in visual
                if needs_visual:
                    if len(images)<IMAGE_LIMIT and (number in visual or len(text.strip())<100):
                        path=destination/f'p{number+1}.png'
                        scale=min(1.7,1600/max(page.rect.width,page.rect.height))
                        page.get_pixmap(matrix=fitz.Matrix(scale,scale),alpha=False).save(str(path))
                        images.append(dict(ref=entry['ref'],path=str(path.resolve())))
                    else:
                        complete=False
            complete=complete and bool(count)
    elif suffix=='.docx':
        with zipfile.ZipFile(reader) as archive:
            info=archive.getinfo('word/document.xml')
            if info.file_size>256*1024*1024:
                raise ValueError('正文 XML 超出用途读取范围；原件保留')
            paragraphs=[]
            unsafe_tags={'{http://schemas.openxmlformats.org/wordprocessingml/2006/main}altChunk',
                         '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}subDoc',
                         '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}object'}
            with archive.open(info) as stream:
                for event,node in ET.iterparse(stream,events=('end',)):
                    if node.tag in unsafe_tags:objects=True
                    if node.tag=='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p':
                        text=''.join(e.text or '' for e in node.iter()
                                     if e.tag in {'{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t',
                                                   '{http://schemas.openxmlformats.org/officeDocument/2006/math}t'})
                        if text.strip():paragraphs.append(text)
                        node.clear()
                        if len(paragraphs)%200==0:_check(stop)
            for index in range(0,len(paragraphs),40):
                pages.append(dict(ref=f'block{index//40+1}',text='\n'.join(paragraphs[index:index+40])))
            media=[n for n in archive.namelist() if n.startswith('word/media/') and not n.endswith('/')]
            objects=objects or any(n.startswith('word/embeddings/') or
                n.startswith('word/afchunk') for n in archive.namelist())
            # These parts can contain real text that the main document reader
            # did not cover. They are never treated as safely empty.
            for name in archive.namelist():
                if name.startswith(('word/header','word/footer','word/footnotes','word/endnotes','word/comments')) and name.endswith('.xml'):
                    extra=ET.fromstring(archive.read(name))
                    if any((e.text or '').strip() for e in extra.iter()
                           if e.tag.endswith('}t')):objects=True
                if name.startswith('word/') and name.endswith('.rels'):
                    relations=ET.fromstring(archive.read(name))
                    if any(e.get('TargetMode')=='External' and not
                           e.get('Type','').endswith('/hyperlink') for e in relations):
                        objects=True
            for index,name in enumerate(media):
                _check(stop)
                if index>=IMAGE_LIMIT:
                    complete=False
                    break
                if archive.getinfo(name).file_size>20*1024*1024:
                    complete=False
                    continue
                raw=destination/f'raw-{index}{Path(name).suffix}'
                raw.write_bytes(archive.read(name))
                path=destination/f'image-{index}.png'
                try:
                    _image(raw,path)
                    images.append(dict(ref=f'image{index+1}',path=str(path.resolve())))
                except (OSError,ValueError):
                    complete=False
                finally:
                    raw.unlink(missing_ok=True)
            complete=complete and not objects
    elif suffix in {'.png','.jpg','.jpeg','.webp','.bmp','.tif','.tiff','.gif'}:
        path=destination/'image.png'
        _image(reader,path)
        images=[dict(ref='p1',path=str(path.resolve()))]
        pages=[dict(ref='p1',text='')]
    elif suffix in {'.txt','.md'}:
        if reader.stat().st_size>16*1024*1024:
            raise ValueError('用途文本过大，保留待确认')
        raw=reader.read_bytes()
        for encoding in ('utf-8-sig','utf-16','gb18030'):
            try:
                text=raw.decode(encoding)
                break
            except UnicodeError:continue
        else:raise ValueError('用途文本无法解码')
        pages=[dict(ref='text',text=text)]
    else:
        raise ValueError('此格式须由原生文件规则处理')
    _check(stop)
    after=source.stat()
    if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
        raise ValueError('用途读取期间文件变化')
    text='\n'.join(p['text'] for p in pages)
    complete=complete and not objects and len(text)<=TEXT_LIMIT and bool(text.strip() or images)
    # Short review projection, with explicit incompleteness. Full reading is kept
    # once for a directed second look; it is never silently claimed as all read.
    brief=[]
    if len(text)<=TEXT_LIMIT:
        brief=pages
    else:
        for number,page in enumerate(pages):
            limit=2200 if number in {0,1,len(pages)//2,len(pages)-1} else 180
            brief.append({**page,'text':page['text'][:limit],
                          'original_characters':len(page['text'])})
            if sum(len(p['text']) for p in brief)>TEXT_LIMIT:
                brief=brief[:-1]
                break
    result=dict(schema=VERSION,source_sha256=source_sha256,
                source_name=record.get('name') or source.name,
                archive_member_path=record.get('archive_member_path',''),
                context=[p for p in source.parts[-5:-1]],page_count=len(pages),
                pages=brief,images=images,refs=list(dict.fromkeys(
                    [p['ref'] for p in brief]+[i['ref'] for i in images])),
                text='\n'.join(p['text'] for p in brief),
                complete_for_deletion=complete,embedded_objects=objects,
                prepared_seconds=None,reader_path=str(reader))
    (destination/'full-reading.json').write_text(json.dumps(pages,ensure_ascii=False),encoding='utf-8')
    (destination/'evidence.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    return result
