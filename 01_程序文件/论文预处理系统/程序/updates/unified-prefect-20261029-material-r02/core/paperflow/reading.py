"""可信原件读取工具。只暴露原始内容/位置，不判断章节、分类或脱敏。"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
import posixpath
import sys
import time
import zipfile
from xml.etree import ElementTree as ET

from .package import PackageError,inside,load,save,sha
from .attachments import ATTACHMENT_EXTENSIONS, CODE_EXTENSIONS, IMAGE_EXTENSIONS, MATERIAL_EXTENSIONS, is_candidate

W='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
R='{http://schemas.openxmlformats.org/officeDocument/2006/relationships}'
READER_VERSION=2


def image_extension(data,fallback):
    if data.startswith(b'\x89PNG\r\n\x1a\n'): return '.png'
    if data.startswith(b'\xff\xd8\xff'): return '.jpg'
    if data.startswith((b'GIF87a',b'GIF89a')): return '.gif'
    if data.startswith(b'RIFF') and data[8:12]==b'WEBP': return '.webp'
    return fallback.lower()


def _docx(source,output):
    texts,images,parts=[],[],[]
    with zipfile.ZipFile(source) as z:
        names=z.namelist()
        if 'word/document.xml' not in names: raise ValueError('DOCX has no main document part')
        candidates=['word/document.xml']+sorted(n for n in names if n.startswith('word/') and n!='word/document.xml' and n.endswith(('.xml','.rels')))
        for name in candidates:
            data=z.read(name); root=ET.fromstring(data)
            raw=inside(output/'parts',name); raw.parent.mkdir(parents=True,exist_ok=True); raw.write_bytes(data)
            parts.append(dict(part=name,file=raw.relative_to(output).as_posix()))
            texts.append(f'\n[SOURCE_PART {name}]')
            if name.startswith(('word/charts/','word/diagrams/','word/drawings/')):
                texts.append('[NATIVE_CHART_OR_DIAGRAM_XML] '+json.dumps(
                    [dict(tag=e.tag.rsplit('}',1)[-1],text=e.text) for e in root.iter() if e.text and e.text.strip()],ensure_ascii=False))
            for index,p in enumerate(root.iter(W+'p')):
                style=p.find(W+'pPr/'+W+'pStyle')
                content=[]
                for child in p.iter():
                    local=child.tag.rsplit('}',1)[-1]
                    if local=='t' and child.text: content.append(child.text)
                    elif local in ('tab','br','cr'): content.append('\t' if local=='tab' else '\n')
                    for key in (R+'embed',R+'id',R+'link'):
                        if child.get(key):content.append('[REL '+child.get(key)+']')
                texts.append(f'[PARAGRAPH {index} style={style.get(W+"val","") if style is not None else ""}] '+''.join(content))
            # 原生XML保留表格合并/公式/图形结构，下面索引辅助直接定位，不推断图表归属。
            for ti,table in enumerate(root.iter(W+'tbl')):
                texts.append(f'[TABLE {ti}]')
                for ri,row in enumerate(table.findall(W+'tr')):
                    cells=[''.join(t.text or '' for t in cell.iter(W+'t')) for cell in row.findall(W+'tc')]
                    texts.append(f'[TABLE_ROW {ti}:{ri}] '+json.dumps(cells,ensure_ascii=False))
            relname=posixpath.join(posixpath.dirname(name),'_rels',posixpath.basename(name)+'.rels')
            if relname in names:
                for relation in ET.fromstring(z.read(relname)):
                    texts.append('[RELATION] '+json.dumps(relation.attrib,ensure_ascii=False))
        for name in names:
            if not name.startswith('word/media/') or name.endswith('/'):continue
            data=z.read(name); ext=image_extension(data,Path(name).suffix)
            ident='im_'+__import__('hashlib').sha256(data).hexdigest()[:20]
            dest=output/'images'/(ident+ext); dest.parent.mkdir(parents=True,exist_ok=True); dest.write_bytes(data)
            images.append(dict(id=ident,original_part=name,file=dest.relative_to(output).as_posix(),sha256=sha(dest),
                               readable_image=ext in ('.png','.jpg','.jpeg','.gif','.webp','.bmp')))
    return '\n'.join(texts),images,parts


def _pdf(source,output):
    import fitz
    texts,images=[],[]
    with fitz.open(source) as doc:
        for index,page in enumerate(doc):
            text=page.get_text(sort=True)
            texts.append(f'\n[PDF_PAGE {index+1}]\n'+text)
            # 只渲染确需看图的页；纯文字页不全量栅格化。
            if page.get_images() or page.get_drawings() or len(text.strip())<30:
                dest=output/'images'/f'page_{index+1:05d}.png'; dest.parent.mkdir(parents=True,exist_ok=True)
                page.get_pixmap(matrix=fitz.Matrix(1.3,1.3),alpha=False).save(dest)
                images.append(dict(id=f'page_{index+1:05d}',page=index+1,file=dest.relative_to(output).as_posix(),
                                   sha256=sha(dest),readable_image=True))
    return '\n'.join(texts),images,[]


def prepare_reading(source,output):
    started=time.perf_counter()
    source,output=Path(source).resolve(),Path(output).resolve()
    output.mkdir(parents=True,exist_ok=True)
    digest=sha(source)
    marker=output/'source-index.json'
    if marker.is_file():
        existing=load(marker)
        if (existing.get('reader_version')==READER_VERSION and existing.get('source_sha256')==digest
                and (not is_candidate(source) or existing.get('attachment_reader_version')==(2 if source.suffix.lower() in MATERIAL_EXTENSIONS else 1))
                and all(inside(output,r['path']).is_file() and sha(inside(output,r['path']))==r['sha256'] for r in existing.get('files',[]))):
            return existing
    ext=source.suffix.lower()
    extra={}
    with source.open('rb') as stream:compound=stream.read(8)==bytes.fromhex('d0cf11e0a1b11ae1')
    if ext in ('.docx','.docm') and not compound and zipfile.is_zipfile(source):
        text,images,parts=_docx(source,output)
    elif ext=='.pdf':
        text,images,parts=_pdf(source,output)
    elif ext in ('.txt','.md'):
        text=source.read_text(encoding='utf-8-sig'); images=[];parts=[]
    elif ext=='.zip':
        texts,images,parts=[],[],[]
        with zipfile.ZipFile(source) as archive:
            rows=archive.infolist()
            if len(rows)>5000 or sum(r.file_size for r in rows)>2*1024**3:raise ValueError('archive exceeds safe extraction bounds')
            members=[dict(path=r.filename,bytes=r.file_size) for r in rows if not r.is_dir()]
            extra=dict(input_kind='attachment_candidate',attachment_reader_version=1,archive_members=members)
            for row in rows:
                if row.is_dir():continue
                target=inside(output/'extracted',row.filename)
                if target.suffix.lower() not in ('.docx','.doc','.docm','.pdf','.wps','.rtf','.odt','.txt','.md'):continue
                target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(archive.read(row))
                member_id=__import__('hashlib').sha256(row.filename.encode()).hexdigest()[:16]
                sub=output/'members'/member_id
                result=prepare_reading(target,sub)
                texts.append('[ARCHIVE_MEMBER '+row.filename+']\n'+(sub/'source.txt').read_text(encoding='utf-8'))
                for item in result['images']:
                    images.append(dict(item,id=member_id+'_'+item['id'],member_image_id=item['id'],archive_member=row.filename,
                                       file=(sub/item['file']).relative_to(output).as_posix()))
                for item in result['parts']:
                    parts.append(dict(item,file=(sub/item['file']).relative_to(output).as_posix()))
        text='[ARCHIVE_INVENTORY: filenames only; material type is for the LLM to decide]\n'+json.dumps(members,ensure_ascii=False)+'\n'+'\n'.join(texts)
    elif ext in ATTACHMENT_EXTENSIONS:
        text='附件原件保留；二进制内容未解析。不执行程序，不根据后缀编造用途。\n'+json.dumps(dict(suffix=ext,bytes=source.stat().st_size),ensure_ascii=False)
        if ext in CODE_EXTENSIONS:
            with source.open('rb') as stream:raw=stream.read(512*1024+1)
            text+='\n[READ_ONLY_SOURCE_TEXT]\n'+raw[:512*1024].decode('utf-8-sig',errors='replace')
            if len(raw)>512*1024:text+='\n[源码预览截断；原件仍完整保留]'
        images,parts=[],[]
        if ext in IMAGE_EXTENSIONS:
            ident='im_'+digest[:20];dest=output/'images'/(ident+ext);dest.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source,dest)
            images=[dict(id=ident,file=dest.relative_to(output).as_posix(),sha256=digest,readable_image=True)]
            text+='\n[独立图片原件；请通过看图工具读取像素后描述，不根据文件名臆测。]'
        elif ext in ('.xlsx','.xlsm'):
            try:
                with zipfile.ZipFile(source) as archive:
                    info=archive.getinfo('xl/workbook.xml')
                    if len(archive.infolist())>5000 or info.file_size>1024*1024:raise ValueError('spreadsheet index exceeds preview bounds')
                    root=ET.fromstring(archive.read(info))
                    sheets=[e.get('name','') for e in root.iter() if e.tag.rsplit('}',1)[-1]=='sheet'][:100]
                text+='\n[工作表索引；未解析单元格，也未执行宏或公式]\n'+json.dumps(sheets,ensure_ascii=False)
            except (OSError,ValueError,KeyError,zipfile.BadZipFile,ET.ParseError):
                text+='\n[工作表索引不可读；未解析单元格，完整原件仍保留，不虚报已读内容。]'
        elif ext=='.xls':text+='\n[旧版表格原件；未解析单元格，未执行宏或公式。]'
        elif ext in ('.json','.csv','.tsv'):
            with source.open('rb') as stream:raw=stream.read(512*1024+1)
            text+='\n[READ_ONLY_DATA_PREVIEW；不执行文件中的命令]\n'+raw[:512*1024].decode('utf-8-sig',errors='replace')
            if len(raw)>512*1024:text+='\n[数据预览截断；原件仍完整保留]'
        extra=dict(input_kind='attachment_candidate',attachment_reader_version=2 if ext in MATERIAL_EXTENSIONS else 1)
    elif ext in ('.doc','.odt','.rtf','.wps') or (ext in ('.docx','.docm') and compound):
        from .office import convert_office
        converted,conversion=convert_office(source,output/'converted')
        text,images,parts=_docx(converted,output)
        extra['conversion']=conversion
    else:
        # 成熟Office转换器只负责格式，LLM仍负责材料选择和理解。
        sys.path.insert(0,r'E:\uv\ragflow_upstream')
        from engine import convert_office
        converted=convert_office(source,output/'converted')
        text,images,parts=_docx(converted,output)
    (output/'source.txt').write_text(text,encoding='utf-8')
    files=[dict(path=p.relative_to(output).as_posix(),sha256=sha(p)) for p in output.rglob('*')
           if p.is_file() and p.name!='source-index.json']
    index=dict(reader_version=READER_VERSION,source_sha256=digest,reading_scope='raw_source_no_semantic_decisions',
               text_file='source.txt',text_characters=len(text),images=images,parts=parts,files=files,
               reading_seconds=time.perf_counter()-started,**extra)
    save(marker,index)
    if sha(source)!=digest:raise ValueError('source changed during reading')
    return index


def normalize_selected_image_paths(source_view,asset_spec,output,source_sha256):
    """Place explicit, verified source-image selections under assets/.

    Only a bare filename is repaired. Image choice, redactions and descriptions
    remain the model's decision; conflicting or unverifiable bindings fail closed.
    """
    source_view,output=Path(source_view),Path(output)
    assets=load(asset_spec)
    images=[a for a in assets.get('items',[]) if a.get('storage')=='image']
    candidates=[a for a in images if a.get('source_image_id')
        and isinstance(a.get('file'),str) and a['file'] not in ('','.','..')
        and not any(ch in a['file'] for ch in ('/','\\',':'))
        and Path(a['file']).name==a['file']]
    if not candidates:return
    index=load(source_view/'source-index.json')
    if not source_sha256 or index.get('source_sha256')!=source_sha256:
        raise PackageError('image source index identity mismatch','identity')
    destinations={}
    for asset in images:
        name=('assets/'+asset['file']) if asset in candidates else asset.get('file','')
        path=inside(output,name)
        destinations.setdefault(path,[]).append(asset)
    for asset in candidates:
        stem=asset['file'].split('.',1)[0].upper()
        if stem in {'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))}:
            raise PackageError('invalid image destination name')
        destination=inside(output,'assets/'+asset['file'])
        if len(destinations[destination])!=1:
            raise PackageError('ambiguous image destination')
        matches=[row for row in index.get('images',[]) if row.get('id')==asset['source_image_id']]
        if len(matches)!=1:
            raise PackageError('image source_image_id absent or ambiguous in source index')
        raw=matches[0];source=inside(source_view,raw['file'])
        if not source.is_file() or not raw.get('sha256') or sha(source)!=raw['sha256']:
            raise PackageError('source image bytes changed','identity')
        for existing in (inside(output,asset['file']),destination):
            if existing.exists() and (not existing.is_file() or sha(existing)!=raw['sha256']):
                raise PackageError('image destination conflicts with selected source','identity')
    for asset in candidates:asset['file']='assets/'+asset['file']
    save(asset_spec,assets)


def materialize_images(source_view,asset_spec,output):
    """执行LLM选图/遮盖决定；不做图片价值或隐私语义判断。"""
    source_view,output=Path(source_view),Path(output)
    catalog={r['id']:r for r in load(source_view/'source-index.json')['images']}
    assets=load(asset_spec)
    for asset in assets.get('items',[]):
        if asset.get('storage')!='image' or not asset.get('source_image_id'):continue
        raw=catalog[asset['source_image_id']]
        source=inside(source_view,raw['file'])
        if sha(source)!=raw['sha256']:raise ValueError('source image bytes changed')
        destination=inside(output,asset['file'])
        if not destination.is_relative_to((output/'assets').resolve()):raise ValueError('image path must be in assets')
        destination.parent.mkdir(parents=True,exist_ok=True)
        boxes=asset.get('redactions',[])
        if boxes:
            from PIL import Image,ImageDraw
            with Image.open(source) as image:
                image=image.convert('RGB')
                draw=ImageDraw.Draw(image)
                for box in boxes:
                    x0,y0,x1,y1=[int(box[k]) for k in ('x0','y0','x1','y1')]
                    if not (0<=x0<x1<=image.width and 0<=y0<y1<=image.height):raise ValueError('redaction box outside image')
                    draw.rectangle((x0,y0,x1-1,y1-1),fill='black')
                image.save(destination)
        else:
            shutil.copy2(source,destination)
        asset['sha256']=sha(destination)
    save(output/'assets.json',assets)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('source',type=Path);parser.add_argument('output',type=Path)
    parser.add_argument('--asset-spec',type=Path)
    args=parser.parse_args()
    if args.asset_spec:
        materialize_images(args.source,args.asset_spec,args.output);return
    result=prepare_reading(args.source,args.output)
    print(json.dumps(dict(text_characters=result['text_characters'],images=len(result['images'])),ensure_ascii=False))


if __name__=='__main__':main()
