"""Private, deterministic reading material. No model, privacy or upload calls."""
from __future__ import annotations

import copy
import hashlib
import json
import re
import shutil
from pathlib import Path

VERSION = 'preextract-material/1.0'
PARSER_VERSION = '1.5'
# Existing, request-bound material remains reusable. New material records this
# additive policy version without invalidating a completed extraction.
CLEANUP_VERSION = 'bounded-frontmatter/1.0'


def file_sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    from preextract_storage import write_new_generated_json
    if write_new_generated_json(path,value):return
    from atomic_json import write_json as persist
    persist(path,value)


def _cover_table(block):
    """Only a vertical administrative form, never a research data table."""
    if block.get('type')!='table':return False
    from knowledge_tables import table_data
    rows=table_data(block,block.get('id','cover_detection'))['rows']
    if any(len(row)!=2 for row in rows):return False
    if block.get('caption'):return False
    labels=[]
    allowed={'二级学院','学院','学校','院系','系别','专业','班级','学号','姓名','学生姓名',
             '学号姓名','指导教师','指导老师','导师','作者','日期','年级','完成时间','职称或学位','职称','学位'}
    for row in rows:
        if not any(str(v).strip() for v in row):continue
        label=re.sub(r'\s+','',str(row[0])).strip(':：')
        # A mixed form containing a title, measurements or unknown rows stays.
        if label not in allowed:return False
        labels.append(label)
    keys=set(labels)
    return bool(keys&{'学号','姓名','学生姓名','学号姓名','作者'}
                and keys&{'指导教师','指导老师','导师'}
                and keys&{'学院','二级学院','学校','院系','系别','专业','班级','年级'})


def clean_blocks(blocks):
    """Remove only bounded nonresearch sections; ambiguous material stays readable."""
    from content_rules import FRONT_END, FIELD, DATE_ONLY, generic_title, NEXT_SECTION
    # Native tables have cells/grid but often no text. Project for detection only;
    # the original research table objects, values and source positions stay intact.
    texts=[_block_text(b).strip() for b in blocks]
    compact=lambda s:re.sub(r'\s+','',s).strip('#')
    excluded=re.compile(r'^(?:致谢|致謝|Acknowledg(?:e)?ments?|(?:学术诚信|诚信|原创性|独创性|授权)声明(?:书)?|(?:论文)?(?:使用)?授权书|保证书)$',re.I)
    numbered_heading=re.compile(r'^(?:第[一二三四五六七八九十百零〇\d]+[章节]|\d+(?:[.．]\d+)*[.、．]?\s+\S|[一二三四五六七八九十]+[、．]\s*\S)')
    def research_heading(b,t):
        return bool(FRONT_END.match(t) or numbered_heading.match(t)
                    or b.get('heading_level') and t and not generic_title(t)
                    and not FIELD.match(t) and not excluded.fullmatch(compact(t)))
    research_start=next((i for i,t in enumerate(texts) if research_heading(blocks[i],t)),0)
    def boundary(b,index):
        text=str(b.get('text') or '').strip()
        # Some templates style the repeated author/supervisor fields as headings
        # between a declaration and the abstract. They do not start research.
        if (index<research_start and b.get('type')=='paragraph' and len(text)<160
                and FIELD.match(text) and 'bounded_cover_before_abstract' in removed.values()):return False
        return bool(b.get('heading_level') or FRONT_END.match(text) or NEXT_SECTION.match(text)
                    or re.match(r'^(?:附\s*[图表]|第[一二三四五六七八九十\d]+章|\d+(?:\.\d+)*\s+\S)',text,re.I)
                    or b.get('type') in {'table','image','formula','equation','unresolved_object'}
                    or b.get('resource_ids') or b.get('resource_id') or b.get('unresolved_object')
                    or '[[' in text
                    or (b.get('position') or {}).get('part') in {'word/footnotes.xml','word/endnotes.xml'})
    removed={}
    research_caption=re.compile(r'^(?:[图表]\s*[\d一二三四五六七八九十]|(?:Table|Fig(?:ure)?)[.\s]*\d)',re.I)
    front=next((i for i,t in enumerate(texts) if research_heading(blocks[i],t)
                or excluded.fullmatch(compact(t)) or research_caption.match(t)
                or blocks[i].get('type')=='table' and not _cover_table(blocks[i])),None)
    if front is not None and front<80:
        degree=next((i for i,t in enumerate(texts[:front]) if any(
            generic_title(line) and re.search(r'毕业|学位',line) for line in t.splitlines())),None)
        cover_tables={i for i,b in enumerate(blocks[:front]) if _cover_table(b)}
        labelled_identity=any(re.match(r'^(?:姓\s*名|学\s*号|指导教师)\s*[:：]\s*\S',t)
                              for t in texts[:front])
        if degree is not None and degree<8 and (cover_tables or labelled_identity):
            for i,t in enumerate(texts[:front]):
                if blocks[i].get('type')=='table':
                    if i in cover_tables:removed[i]='bounded_cover_before_abstract'
                    continue
                # Never delete the entire prefix. In particular, an experimental
                # table or a title sharing a paragraph with the degree is retained.
                if (not t or generic_title(t) or (len(t)<160 and FIELD.match(t))
                        or DATE_ONLY.fullmatch(re.sub(r'\s+','',t))
                        or (i<degree and re.fullmatch(r'\[\[IMAGE:[^\]]+\]\]',t))):
                    removed[i]='bounded_cover_before_abstract'
    for i,t in enumerate(texts):
        if i in removed or blocks[i].get('type')!='paragraph' or not excluded.fullmatch(compact(t)):continue
        end=next((j for j in range(i+1,len(blocks)) if boundary(blocks[j],j)),len(blocks))
        # An explicit heading bounds the section. A final acknowledgement is also
        # bounded by EOF, but unlabelled long content could contain research.
        if end==len(blocks) and (len(blocks)-i>35 or any(len(t)>1500 for t in texts[i+1:])):continue
        for j in range(i,end):removed[j]='bounded_nonresearch_section'
    return ([b for i,b in enumerate(blocks) if i not in removed],
            [{'source_order':i,'reason':reason,'block':copy.deepcopy(blocks[i])} for i,reason in sorted(removed.items())])


def _block_text(block):
    if block['type']=='table':
        from knowledge_tables import table_data
        data=table_data(block,block.get('id','table_detection'))
        return '\n'.join('\t'.join(row) for row in data['rows'])
    return str(block.get('text') or block.get('latex') or '')


def _position_within(position,parent):
    """Exact native parent/child identity; page-only warnings are not objects."""
    if not position or not parent:return False
    if 'bbox' in parent:
        if position.get('page')!=parent.get('page') or not position.get('bbox'):return False
        import fitz
        return fitz.Rect(position['bbox']) in fitz.Rect(parent['bbox'])
    if not any(k in parent for k in ('body_index','line','note_id')):return False
    return all(position.get(k)==v for k,v in parent.items())


def remove_repeated_margins(blocks, page_heights):
    candidates={}
    for i,b in enumerate(blocks):
        pos=b.get('position',{});page=pos.get('page');box=pos.get('bbox');height=page_heights.get(page)
        text=str(b.get('text') or '').strip()
        if b.get('type')!='paragraph' or not text or len(text)>100 or not height or not box:continue
        margin='top' if box[3]<height*.08 else 'bottom' if box[1]>height*.92 else None
        if not margin:continue
        label='<page_number>' if re.fullmatch(r'(?:第\s*)?\d+(?:\s*页)?',text) else text
        candidates.setdefault((margin,label),[]).append((i,page))
    removed={i for rows in candidates.values() if len({p for _,p in rows})>=3 for i,_ in rows}
    return [b for i,b in enumerate(blocks) if i not in removed],[dict(source_order=i,reason='repeated_page_margin',block=copy.deepcopy(blocks[i])) for i in sorted(removed)]


def metadata_inventory(source):
    """Only actual standard document properties; selectors bind private values."""
    rows=[];source=Path(source)
    if source.suffix.lower()=='.docx':
        import zipfile
        from xml.etree import ElementTree as ET
        with zipfile.ZipFile(source) as archive:
            for part in ('docProps/core.xml','docProps/app.xml'):
                if part not in archive.namelist():continue
                root=ET.fromstring(archive.read(part))
                for child in root:
                    if child.text and child.text.strip():
                        rows.append(dict(selector=dict(file_ref='main',part=part,selector=child.tag,attribute=None),value=child.text))
    elif source.suffix.lower()=='.pdf':
        import fitz
        with fitz.open(source) as pdf:
            for key,value in pdf.metadata.items():
                if isinstance(value,str) and value:rows.append(dict(selector=dict(file_ref='main',part='document.info',selector=key,attribute=None),value=value))
    return rows


def prepare_material(source, work_dir, catalog_path, render_missing=True, max_pages=160, stop=None,
                     associated_document=None, associated_records=None):
    from native_docx import parse_docx
    from evidence import parse_pdf, attach_context
    from knowledge_package import outline
    from PIL import Image
    from preextract_input_repair import normalize_text,repair_media_crc

    source=Path(source).resolve();work=Path(work_dir).resolve();catalog_path=Path(catalog_path).resolve()
    original_sha=file_sha(source);catalog_sha=file_sha(catalog_path)
    work.mkdir(parents=True,exist_ok=True); manifest=work/'material.json'
    if manifest.exists():
        cached=json.loads(manifest.read_text(encoding='utf-8'))
        if (cached.get('version')==VERSION and cached.get('parser_version')==PARSER_VERSION and cached['source']['source_sha256']==original_sha
                and cached['source']['catalog_sha256']==catalog_sha
                and cached.get('render_missing')==render_missing
                and all(Path(p).is_file() and file_sha(p)==h for p,h in cached.get('file_hashes',{}).items())):
            if cached.get('associated_materials_sha256'):
                from associated_materials import inventory_hash
                if inventory_hash(cached.get('attachments',[]))!=cached['associated_materials_sha256']:
                    raise ValueError('已有材料的关联资料清单发生变化；原请求保留')
            return cached
        raise ValueError('已有材料版本或文件发生变化；请使用新的单篇工作目录，原产物保留')
    private=work/'private';private.mkdir(exist_ok=True)
    original=private/('original'+source.suffix.lower())
    if original.exists() and file_sha(original)!=original_sha:raise ValueError('工作目录已绑定另一原稿')
    if not original.exists():shutil.copyfile(source,original)
    if file_sha(original)!=original_sha or file_sha(source)!=original_sha:raise ValueError('原稿复制期间发生变化')
    normalized=original
    if source.suffix.lower() in {'.doc','.rtf','.wps','.odt'}:
        from engine import convert_office
        normalized=convert_office(original,private/'converted')
    elif source.suffix.lower()=='.docx':
        from office_input import normalize_docx_container
        normalized=normalize_docx_container(original,private/'normalized')
    resources=[];issues=[];input_repairs=[];unicode_repairs={};asset_dir=private/'images';asset_dir.mkdir(exist_ok=True)
    reading_text=lambda value:normalize_text(value,unicode_repairs)
    def check_stop():
        if stop is not None and stop.is_set():raise InterruptedError('已停止，材料保留')
    def issue(code,message,position):
        issues.append(dict(code=code,message=message,position=position))
    def resource(rid,blob,extension,kind,position):
        check_stop()
        found=next((r for r in resources if r['id']==rid),None)
        if found:return found
        extension=extension.lower() if re.fullmatch(r'\.[a-zA-Z0-9]{1,6}',extension) else '.bin'
        path=asset_dir/(re.sub(r'[^\w.-]','_',rid)+extension);path.write_bytes(blob)
        row=dict(id=rid,kind=kind,position=position,image_path=str(path),local_path=str(path.relative_to(work)),sha256=file_sha(path),width=None,height=None)
        try:
            supported=False
            if extension not in {'.wmf','.emf'}:
                with Image.open(path) as im:
                    row.update(width=im.width,height=im.height)
                    supported=im.format in {'PNG','JPEG','GIF','WEBP'}
                    if getattr(im,'n_frames',1)>1:raise ValueError('多帧图像需要原页面证据')
            if not supported:
                # Reuse the tested lossless raster/font-aware vector renderer.
                # Keep the original bytes and both rendition identities private.
                from evidence_render import load_visual_image
                converted=path.with_name(path.stem+'.model.png')
                with load_visual_image(path) as im:
                    im.save(converted,'PNG');row.update(width=im.width,height=im.height)
                row.update(original_image_path=str(path),original_sha256=row['sha256'],
                    image_path=str(converted),local_path=str(converted.relative_to(work)),
                    sha256=file_sha(converted),rendition_method='existing_visual_renderer')
        except InterruptedError:raise
        except Exception:
            row.update(original_image_path=str(path),original_sha256=row['sha256'],image_path=None)
            issue('image_format_unreadable','图像格式无法直接提供给模型，须页面渲染',position)
        resources.append(row);return row
    check_stop()
    if normalized.suffix.lower()=='.docx':
        import zipfile
        try:
            raw,images,formulas=parse_docx(normalized,reading_text,resource,issue)
        except zipfile.BadZipFile as exc:
            if not str(exc).startswith("Bad CRC-32 for file 'word/media/"):raise
            normalized,receipt,repaired=repair_media_crc(normalized,private/'repaired-input',stop=stop)
            resources.clear();issues.clear()
            raw,images,formulas=parse_docx(normalized,reading_text,resource,issue)
            input_repairs.append(dict(kind='docx_media_crc',receipt_path=str(receipt),members=repaired))
            issue('docx_media_crc_recovered','内嵌图片 CRC 不符；已保留原字节并从可完整解码的副本读取，原始完整性未知，未删除图片',{})
    elif normalized.suffix.lower()=='.pdf':raw,images,formulas=parse_pdf(normalized,reading_text,resource,issue)
    elif normalized.suffix.lower() in {'.png','.jpg','.jpeg','.webp','.bmp','.tif','.tiff','.gif'}:
        # Confirmed body images use the existing source-page/pixel request path.
        # No OCR transcription or completeness/quality decision is invented here.
        raw=[];images=[];formulas=[]
        with Image.open(normalized) as picture:
            frame_count=getattr(picture,'n_frames',1)
            for number in range(frame_count):
                check_stop();position={'page':number+1}
                if frame_count==1:
                    blob=normalized.read_bytes();extension=normalized.suffix.lower()
                else:
                    import io
                    picture.seek(number)
                    with io.BytesIO() as output:
                        picture.copy().save(output,format='PNG')
                        blob=output.getvalue()
                    extension='.png';position['frame']=number+1
                r=resource(f'page_{number+1:04d}',blob,extension,'source_page',position)
                block=dict(type='image',position=position,resource_id=r['id'],caption='',inline=False,
                           source='original',ocr=dict(status='not_run',text='',verified=False))
                raw.append(block);images.append(block)
        issue('image_body_pixels','正文以所选原图的完整像素或全部帧提供；未生成文字层，按实际图像读取并如实记录缺项。',{})
    elif normalized.suffix.lower() in {'.txt','.md'}:
        raw=[dict(type='paragraph',text=t,position={'line':i+1}) for i,t in enumerate(normalized.read_text(encoding='utf-8-sig').splitlines())]
        images=[];formulas=[]
        if any(re.search(r'!\[|<img\b',r['text']) for r in raw):issue('external_images_missing','文本引用的外部图片尚未取得',{})
    else:raise ValueError('一次提取支持 DOCX、PDF、Office、TXT、MD 和正文图片；附件不会执行')
    raw,images,formulas=normalize_text((raw,images,formulas),unicode_repairs)
    if unicode_repairs:
        input_repairs.append(dict(kind='unicode_surrogates',**unicode_repairs))
        issue('unicode_surrogate_normalized','已合并 Unicode 代理对；无法还原的孤立码元以替代符标记，原稿保留',dict(**unicode_repairs))
    for i,b in enumerate(raw):b.setdefault('id',f'src_{i+1:05d}')
    kept,removed=clean_blocks(raw)
    if normalized.suffix.lower()=='.pdf':
        import fitz
        with fitz.open(normalized) as pdf:heights={i+1:p.rect.height for i,p in enumerate(pdf)}
        kept,margins=remove_repeated_margins(kept,heights);removed.extend(margins)
    from knowledge_outline import analyze_blocks
    toc=[b for b,info in zip(kept,analyze_blocks(kept)) if info['role']=='table_of_contents']
    removed.extend(dict(source_order=raw.index(b),reason='confirmed_table_of_contents',block=copy.deepcopy(b)) for b in toc)
    kept=[b for b in kept if b not in toc]
    def excluded_position(position):
        return (any(_position_within(position,r['block'].get('position')) for r in removed)
                and not any(_position_within(position,b.get('position')) for b in kept))
    excluded_issues=[i for i in issues if excluded_position(i.get('position'))]
    issues=[i for i in issues if i not in excluded_issues]
    images=[im for im in images if not excluded_position(im.get('position'))]
    formulas=[f for f in formulas if not excluded_position(f.get('position'))]
    attach_context(kept,images)
    for b in kept:b['text']=_block_text(b)
    paper='paper_'+original_sha[:24]
    sections,blocks=outline({'blocks':kept},paper)
    for i,s in enumerate(sections):s['order']=i
    by_old={b['source_block_id']:b for b in blocks}
    assets=[]
    for original_block in kept:
        b=by_old[original_block['id']]
        if original_block['type']=='table':
            from knowledge_tables import table_data
            table=table_data(original_block,original_block['id'])
            # Full values occur once in the body; structure preserves cell offsets.
            assets.append(dict(id='asset_'+b['id'],kind='table',caption=original_block.get('caption',''),block_ids=[b['id']],section_ids=[b['section_id']],position=b['position'],image_path=None,sha256=None,width=None,height=None,text=b['text'],structure={k:table[k] for k in ('cells','grid','header_rows','units','notes')}))
        for r in resources:
            if r['id'] in original_block.get('resource_ids',[]) or r['id']==original_block.get('resource_id'):
                if any(a['id']==r['id'] for a in assets):continue
                assets.append(dict(r,caption=original_block.get('caption',''),block_ids=[b['id']],section_ids=[b['section_id']],text=''))
    # Resources embedded in paragraph runs may not be direct block fields.
    for r in resources:
        if any(a['id']==r['id'] for a in assets):continue
        matches=[b for b in blocks if b['position'] and all(r['position'].get(k)==v for k,v in b['position'].items())]
        if not matches and excluded_position(r['position']):continue
        im=next((im for im in images if im.get('resource_id')==r['id']),{})
        assets.append(dict(r,caption=im.get('caption',''),block_ids=[b['id'] for b in matches],section_ids=list(dict.fromkeys(b['section_id'] for b in matches)),text=''))
    # Every uncertain visual object receives actual source pixels. DOCX object
    # positions are structural rather than page-based, so keep all rendered pages
    # when exact page mapping is unavailable. Never guess a page or drop a tail.
    visual_issue_codes={'embedded_object_review','native_chart_visual','body_container','scanned_page','image_missing','formula_review','image_format_unreadable','pdf_vector_review','pdf_table_review'}
    visual_issues=[i for i in issues if i['code'] in visual_issue_codes]
    unresolved=[b for b in kept if b.get('type')=='unresolved_object' or b.get('unresolved_object')]
    needs_pages=bool(visual_issues or unresolved)
    page_cleanup=None
    for item in issues:
        if item['code'] in visual_issue_codes or item['code']=='pdf_layout_review':
            item['page_evidence_ids']=[]
            item['page_evidence_status']='not_rendered_general_note' if item['code']=='pdf_layout_review' else 'not_rendered'
    if needs_pages and render_missing and normalized.suffix.lower() in {'.docx','.pdf'}:
        try:
            from preextract_page_cleanup import PageCleanup
            page_cleanup=PageCleanup(kept,removed) if removed else None
            if normalized.suffix.lower()=='.pdf':
                # PDF parser positions are original 1-based page numbers. The
                # generic layout reminder on every page is not a missing object.
                import fitz
                with fitz.open(normalized) as pdf, fitz.open(normalized) as render_pdf:
                    valid_page=lambda p:isinstance(p,int) and not isinstance(p,bool) and 1<=p<=len(pdf)
                    wanted=[(row.get('position') or {}).get('page') for row in visual_issues+unresolved]
                    fallback=any(not valid_page(p) for p in wanted)
                    selected=list(range(1,len(pdf)+1)) if fallback else sorted(set(wanted))
                    if len(selected)>max_pages:raise ValueError(f'必要页图数量 {len(selected)} 超过上限 {max_pages}；未截断，原稿保留')
                    pages=[]
                    for page_number in selected:
                        check_stop()
                        page=pdf[page_number-1]
                        if page_cleanup is not None and not page_cleanup(page):continue
                        blob=render_pdf[page_number-1].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png')
                        if page_cleanup is not None:blob=page_cleanup.apply(page_number,blob)
                        pages.append((page_number,blob))
                    known_pages=set(range(1,len(pdf)+1))
            else:
                from auto_evidence import source_pages
                if page_cleanup is not None:
                    pages=source_pages(normalized,private/'rendered',max_pages=max_pages,
                                       page_filter=page_cleanup,page_transform=page_cleanup.apply,return_records=True)
                else:
                    pages=list(enumerate(source_pages(normalized,private/'rendered',max_pages=max_pages),start=1))
                fallback=True;known_pages=set()
            evidence={}
            for page_number,blob in pages:
                check_stop();r=resource(f'page_{page_number:04d}',blob,'.png','source_page',{'page':page_number})
                matches=[b for b in blocks if (b.get('position') or {}).get('page')==page_number]
                changed=page_cleanup is not None and any(p['page']==page_number and p['action']=='mask_nonresearch_regions' for p in page_cleanup.records)
                caption=f'原稿第 {page_number} 页（阅读证据'+('；已遮除明确非研究内容' if changed else '')+'）'
                assets.append(dict(r,caption=caption,block_ids=[b['id'] for b in matches],section_ids=list(dict.fromkeys(b['section_id'] for b in matches)),text=''))
                evidence[page_number]=r['id']
            for item in issues:
                page=(item.get('position') or {}).get('page')
                is_known=isinstance(page,int) and not isinstance(page,bool) and page in known_pages
                if item['code'] in visual_issue_codes and fallback and not is_known:
                    item['page_evidence_ids']=list(evidence.values())
                    item['page_evidence_status']='provided_full_document_fallback'
                elif isinstance(page,int) and not isinstance(page,bool) and page in evidence:
                    item['page_evidence_ids']=[evidence[page]]
                    item['page_evidence_status']='provided'
                elif page_cleanup is not None and any(p['page']==page and p['action']=='omit_nonresearch_page' for p in page_cleanup.records):
                    item['page_evidence_status']='omitted_nonresearch_page'
        except Exception as exc:
            if isinstance(exc,InterruptedError):raise
            for item in visual_issues:item['page_evidence_status']='render_failed'
            issue('page_render_failed',str(exc),{})
    education={}
    try:
        from source_education import extract_source_education
        education=extract_source_education(normalized) if normalized==original else extract_source_education(normalized,original_source=original,source_relation='converted',derivation_method='existing_office_converter',derivation_record_id='single_request_prepare')
    except Exception as exc:issue('cover_fields_unavailable',str(exc),{})
    education=normalize_text(education,unicode_repairs)
    text_path=private/'body.txt';index_path=private/'index.json'
    text_path.write_text('\n\n'.join(b['text'] for b in blocks),encoding='utf-8')
    metadata=normalize_text(metadata_inventory(normalized),unicode_repairs)
    cleanup=dict(version=CLEANUP_VERSION,input_repairs=input_repairs,excluded_reading_issues=excluded_issues,
                 page_evidence=page_cleanup.report() if page_cleanup is not None else None)
    index=dict(blocks=blocks,sections=sections,assets=assets,formulas=formulas,removed=removed,cover_facts=education,metadata=metadata,cleanup=cleanup)
    write_json(index_path,index)
    facts=dict(source_id='source_'+original_sha[:24],file=str(source),source_sha256=original_sha,text_file=str(text_path),text_sha256=file_sha(text_path),index_file=str(index_path),index_sha256=file_sha(index_path),catalog_sha256=catalog_sha,reading_scope=dict(read_refs=[],unread_refs=[],extraction_issues=[i['message'] for i in issues],notes='程序材料已准备，尚未提交模型阅读'))
    bundle=dict(version=VERSION,parser_version=PARSER_VERSION,cleanup=cleanup,render_missing=render_missing,source=facts,blocks=blocks,sections=sections,assets=assets,issues=issues,attachments=[],cover_facts=education,removed=removed,formulas=formulas,file_ref='main',metadata=metadata,original_filename=source.name)
    bundle['file_hashes']={str(p):file_sha(p) for p in [original,normalized,text_path,index_path,catalog_path]+[Path(a['image_path']) for a in assets if a.get('image_path')]}
    bundle['file_hashes'].update({row['receipt_path']:file_sha(row['receipt_path']) for row in input_repairs if row.get('receipt_path')})
    bundle['file_hashes'].update({a['original_image_path']:file_sha(a['original_image_path']) for a in assets if a.get('original_image_path')})
    if associated_records:
        from associated_materials import prepare_inventory,inventory_hash
        attachments=prepare_inventory(associated_document or dict(path=str(source)),associated_records,
                                      private/'associated',stop=stop)
        bundle['attachments']=attachments
        bundle['associated_materials_sha256']=inventory_hash(attachments)
        for attachment in attachments:
            bundle['file_hashes'].update(attachment['retained_file_hashes'])
    write_json(manifest,bundle)
    if file_sha(source)!=original_sha:raise ValueError('原稿在处理期间发生变化；候选不继续')
    return bundle
