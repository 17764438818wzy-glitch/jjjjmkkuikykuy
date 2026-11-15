"""Private, deterministic reading material. No model, privacy or upload calls."""
from __future__ import annotations

import copy
import hashlib
import json
import re
import shutil
import tempfile
from pathlib import Path

VERSION = 'preextract-material/1.0'
PARSER_VERSION = 'candidate-direct-clean/25'
# Existing, request-bound material remains reusable. New material records this
# additive policy version without invalidating a completed extraction.
CLEANUP_VERSION = 'reading-scope/25'


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


def remove_repeated_margins(blocks, page_heights, *, scope_known=False):
    from reading_cleanup import heading_kind
    candidates={}
    for i,b in enumerate(blocks):
        pos=b.get('position',{});page=pos.get('page');box=pos.get('bbox');height=page_heights.get(page)
        text=str(b.get('text') or '').strip()
        if b.get('type')!='paragraph' or not text or len(text)>100 or not height or not box:continue
        margin='top' if box[3]<height*.10 else 'bottom' if box[1]>height*.90 else None
        if not margin:continue
        label='<page_number>' if re.fullmatch(r'(?:(?:第\s*)?\d+(?:\s*页)?|[IVXLC]{1,8})',text,re.I) else text
        candidates.setdefault((margin,label),[]).append((i,page))
    removed=set()
    for rows in candidates.values():
        count=len({p for _, p in rows})
        if count < 2:continue
        if count == 2:
            positions=[blocks[i].get('position') or {} for i,_ in rows]
            boxes=[pos.get('bbox') for pos in positions]
            extreme=all((box[3] < page_heights[pos['page']]*.05 or
                         box[1] > page_heights[pos['page']]*.95)
                        for pos,box in zip(positions,boxes))
            aligned=max(box[0] for box in boxes)-min(box[0] for box in boxes) <= 4
            if not extreme or not aligned or any(blocks[i].get('heading_level') for i,_ in rows):continue
        first=min(rows, key=lambda row: (row[1], row[0]))[0]
        text=str(blocks[first].get('text') or '')
        is_boundary=any(heading_kind(line) for line in text.splitlines())
        if is_boundary and not scope_known:continue
        removed.update(i for i, _ in rows if not is_boundary or i != first)
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
                     associated_document=None, associated_records=None, detected_format=None,
                     expected_sha256=None):
    # The converter output may contain the whole original document. It must
    # exist only while local parsing is active, including on failure.
    with tempfile.TemporaryDirectory(prefix='paperflow_conversion_') as conversion_dir:
        return _prepare_material(source, work_dir, catalog_path, render_missing, max_pages, stop,
                                 associated_document, associated_records, detected_format,
                                 expected_sha256, Path(conversion_dir))


def _prepare_material(source, work_dir, catalog_path, render_missing, max_pages, stop,
                      associated_document, associated_records, detected_format,
                      expected_sha256, conversion_dir):
    from reading_cleanup import clean_body_blocks, normalize_layout, ScopeUncertain, contains_inline_image
    from native_docx import parse_docx
    from evidence import parse_pdf, attach_context
    from knowledge_package import outline
    from PIL import Image
    from preextract_input_repair import normalize_text,repair_media_crc

    source=Path(source).resolve();work=Path(work_dir).resolve();catalog_path=Path(catalog_path).resolve()
    original_sha=file_sha(source);catalog_sha=file_sha(catalog_path)
    if expected_sha256 is not None and original_sha != expected_sha256:
        raise ValueError('原正文哈希与登记记录不一致')
    if source.is_relative_to(work):raise ValueError('原正文不能位于处理结果目录内')
    work.mkdir(parents=True,exist_ok=True); manifest=work/'material.json'
    if manifest.exists():
        cached=json.loads(manifest.read_text(encoding='utf-8'))
        hashes=cached.get('file_hashes') or {}
        required={str(source),str(catalog_path),str(work/'private'/'body.txt'),str(work/'private'/'index.json')}
        required.update(str(a['image_path']) for a in cached.get('assets',[]) if a.get('image_path'))
        if (required.issubset(hashes) and cached.get('version')==VERSION and cached.get('parser_version')==PARSER_VERSION and cached['source']['source_sha256']==original_sha
                and cached['source']['catalog_sha256']==catalog_sha
                and cached.get('render_missing')==render_missing
                and cached.get('cleanup',{}).get('version')==CLEANUP_VERSION
                and all(Path(p).is_file() and file_sha(p)==h for p,h in hashes.items())):
            if cached.get('associated_materials_sha256'):
                from associated_materials import inventory_hash
                if inventory_hash(cached.get('attachments',[]))!=cached['associated_materials_sha256']:
                    raise ValueError('已有材料的关联资料清单发生变化；原请求保留')
            return cached
        raise ValueError('已有材料版本或文件发生变化；请使用新的单篇工作目录，原产物保留')
    private=work/'private';private.mkdir(exist_ok=True)
    original=source
    from direct_source import prepare_readable_source
    normalized, parse_kind = prepare_readable_source(source, private, detected_format, conversion_dir)
    conversion_sha=file_sha(normalized) if normalized!=source else None
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
    visual_source = normalized
    image_page_mapping = None
    if parse_kind=='docx':
        import zipfile
        try:
            raw,images,formulas=parse_docx(normalized,reading_text,resource,issue)
        except zipfile.BadZipFile as exc:
            if not str(exc).startswith("Bad CRC-32 for file 'word/media/"):raise
            raise ScopeUncertain('DOCX 内嵌图片校验失败，保留原稿并等待人工核验') from exc
    elif parse_kind=='pdf':
        resource_mark, issue_mark = len(resources), len(issues)
        try:
            raw,images,formulas=parse_pdf(normalized,reading_text,resource,issue)
        except (InterruptedError, MemoryError, PermissionError):
            raise
        except Exception:
            # Reuse the full parser first; on a local layout failure, fall back
            # to page-local native text and explicitly mark reduced coverage.
            del resources[resource_mark:]
            del issues[issue_mark:]
            from pdf_fallback import parse_pdf_pages
            raw,images,formulas=parse_pdf_pages(normalized,reading_text,resource,issue,check_stop)
    elif parse_kind in {'png','jpg','jpeg','webp','bmp','tif','tiff','gif'}:
        # Create a transient page coordinate system in memory only. OCR and
        # cropped derivative pixels are checked exactly like scanned PDFs.
        import io
        import fitz
        from PIL import ImageOps
        raw=[];images=[];formulas=[]
        image_page_mapping=[]
        with fitz.open() as pages, Image.open(normalized) as picture:
            frame_count=getattr(picture,'n_frames',1)
            if frame_count > max_pages:raise ScopeUncertain('正文图片帧数超出本地处理上限，需人工核验')
            for number in range(frame_count):
                check_stop();picture.seek(number)
                original_pixels=list(picture.size)
                orientation=picture.getexif().get(274,1)
                if orientation not in range(1,9):raise ScopeUncertain('正文图片方向标记无效，需人工核验')
                oriented=ImageOps.exif_transpose(picture.copy()).convert('RGBA')
                canvas=Image.new('RGBA',oriented.size,(255,255,255,255))
                canvas.alpha_composite(oriented)
                frame=canvas.convert('RGB');width,height=frame.size
                scale=min(.5,1440/max(width,height))
                page=pages.new_page(width=max(1,width*scale),height=max(1,height*scale))
                with io.BytesIO() as buffer:
                    frame.save(buffer,format='PNG')
                    page.insert_image(page.rect,stream=buffer.getvalue())
                image_page_mapping.append({'page':number+1,'frame':number+1,'pixels':[width,height],
                                           'original_pixels':original_pixels,'exif_orientation':orientation,
                                           'transparent_background':'white',
                                           'page_size':[page.rect.width,page.rect.height]})
            visual_source=pages.tobytes()
    elif parse_kind in {'txt','md'}:
        raw=[dict(type='paragraph',text=t,position={'line':i+1}) for i,t in enumerate(normalized.read_text(encoding='utf-8-sig').splitlines())]
        images=[];formulas=[]
        if any(re.search(r'!\[|<img\b',r['text']) for r in raw):issue('external_images_missing','文本引用的外部图片尚未取得',{})
    else:raise ValueError('一次提取支持 DOCX、PDF、Office、TXT、MD 和正文图片；附件不会执行')
    raw,images,formulas=normalize_text((raw,images,formulas),unicode_repairs)
    if unicode_repairs:
        input_repairs.append(dict(kind='unicode_surrogates',**unicode_repairs))
        issue('unicode_surrogate_normalized','已合并 Unicode 代理对；无法还原的孤立码元以替代符标记，原稿保留',dict(**unicode_repairs))
    for i,b in enumerate(raw):b.setdefault('id',f'src_{i+1:05d}')
    blank_pages=[]; scan_pages=set()
    heights={}
    if parse_kind=='pdf' or image_page_mapping is not None:
        import fitz
        from pdf_visual import prepare_pages
        scan_pages,blank_pages=prepare_pages(visual_source,raw,images,resources,issues,
                                            continue_on_uncertainty=True)
        issues=[item for item in issues if not (item.get('code')=='scanned_page' and
                (item.get('position') or {}).get('page') in blank_pages)]
        if blank_pages:
            raw=[b for b in raw if (b.get('position') or {}).get('page') not in blank_pages]
            images=[b for b in images if (b.get('position') or {}).get('page') not in blank_pages]
            resources=[r for r in resources if (r.get('position') or {}).get('page') not in blank_pages]
    if parse_kind=='pdf' or image_page_mapping is not None:
        from pdf_visual import open_pdf
        with open_pdf(visual_source) as pdf:heights={i+1:p.rect.height for i,p in enumerate(pdf)}
        scope_raw,margins=remove_repeated_margins(raw,heights)
    else:
        scope_raw,margins=raw,[]
    unknown_pages = sorted({(i.get('position') or {}).get('page') for i in issues
                           if i.get('code') in {'ocr_page_incomplete','pdf_page_incomplete'}
                           and isinstance((i.get('position') or {}).get('page'), int)})
    quarantined = []
    # Keep known text from failed pages available to the scope parser: a native
    # "References" heading must still exclude later citations.  The projection
    # withholds the uncertain page itself using the OCR readiness item.
    try:
        kept,removed,scope_decisions=clean_body_blocks(scope_raw, continue_on_uncertainty=True)
    except ScopeUncertain as scope_error:
        # Recover each readable span independently. Only the block whose
        # boundary cannot be resolved is recorded as a gap; later blocks are
        # processed again with the last known body/reference/acknowledgement state.
        from reading_cleanup import scope_heading_events
        kept, removed = [], []
        scope_decisions = None
        offset, phase, in_appendix, seed_number = 0, 'before_abstract', False, 0
        while offset < len(scope_raw):
            seed_number += 1
            seed_id = f'__scope_recovery_seed_{seed_number}'
            seed_text = {'body':'摘要\n附录' if in_appendix else '摘要', 'references':'摘要\n参考文献',
                         'acknowledgements':'摘要\n致谢'}.get(phase)
            seed = [{'id':seed_id, 'type':'paragraph', 'text':seed_text, 'position':{}}] if seed_text else []
            remaining = len(scope_raw) - offset
            recovered = None
            consumed = 0
            for count in range(remaining, 0, -1):
                try:
                    recovered = clean_body_blocks(seed + scope_raw[offset:offset+count], continue_on_uncertainty=True)
                    consumed = count
                    break
                except ScopeUncertain:
                    continue
            if recovered is not None:
                part_kept, part_removed, decision = recovered
                def is_seed(block):
                    return block.get('id') == seed_id or (block.get('source_scope') or {}).get('block_id') == seed_id
                kept.extend(block for block in part_kept if not is_seed(block))
                removed.extend(row for row in part_removed if not is_seed(row['block']))
                if scope_decisions is None:
                    if seed:
                        decision.update(abstract_source_id=None, start_source_id=scope_raw[offset]['id'],
                                        start_kind='unverified', fallback_evidence=None)
                    scope_decisions = decision
                else:
                    for key in ('transition_visuals', 'transition_fragments', 'tail_ocr_ambiguities', 'missing_note_labels'):
                        scope_decisions.setdefault(key, []).extend(decision.get(key, []))
                phase = decision.get('end_phase', phase)
                in_appendix = decision.get('end_in_appendix', in_appendix)
                offset += consumed
            if offset >= len(scope_raw):break
            broken = scope_raw[offset]
            events = scope_heading_events(broken.get('text') or '')
            if (phase == 'before_abstract' and events and events[0] == (0, 'appendix')
                    and (broken.get('heading_level') or re.fullmatch(
                        r'\s*附\s*录(?:\s*[A-Za-z0-9一二三四五六七八九十]+)?\s*[:：]?\s*',
                        str(broken.get('text') or ''), re.I))):
                phase, in_appendix = 'body', True
                issue('scope_incomplete', '未能确定摘要起点，已从明确的附录继续处理。',
                      copy.deepcopy(broken.get('position') or {}))
                continue
            quarantined.append(broken)
            issue('scope_incomplete', '局部正文边界无法确定，已记录此处缺口；其他内容继续处理。原因：' + str(scope_error)[:300],
                  copy.deepcopy(broken.get('position') or {}))
            # Clear end-section headings inside the failed block still inform
            # where the next independently readable span begins.
            if phase != 'before_abstract':
                for _, tag in events:
                    if tag in {'references', 'acknowledgements'}:
                        phase, in_appendix = tag, False
                    elif tag == 'appendix':
                        phase, in_appendix = 'body', True
            offset += 1
        if scope_decisions is None:
            scope_decisions = {'abstract_source_id': None, 'start_source_id': None,
                               'start_kind': 'unverified', 'version': CLEANUP_VERSION,
                               'missing_note_labels': []}
        # A note and its reference can fall on opposite sides of a local gap.
        # Resolve them after all readable spans have been brought together.
        references = set()
        for block in kept:
            if not block.get('note_label'):
                references.update(re.findall(r'\[\^((?:footnoteReference|endnoteReference)\d+)\]',
                                             str(block.get('text') or '')))
        note_rows = [row for row in removed if row['reason'] == 'note_outside_reading_scope']
        restored = [row for row in note_rows if row['block'].get('note_label') in references]
        kept.extend(row['block'] for row in restored)
        removed = [row for row in removed if row not in restored]
        present = {block.get('note_label') for block in kept if block.get('note_label')}
        present.update(row['block'].get('note_label') for row in note_rows if row['block'].get('note_label'))
        scope_decisions['missing_note_labels'] = sorted(references - present)
        scope_decisions['end_phase'] = phase
        scope_decisions['end_in_appendix'] = in_appendix
        scope_decisions['incomplete'] = True
    if unknown_pages:
        scope_decisions['ocr_incomplete_pages'] = unknown_pages
        scope_decisions['incomplete'] = True
        issue('post_ocr_scope_unverified',
              '识别不完整的页面已记录缺口，后续可读取文字继续处理；后续范围可能含未识别的参考文献或致谢，建议核对。',
              {'page': unknown_pages[0]})
    if quarantined:
        removed.extend({'source_order': 0, 'reason': 'unverified_local_scope', 'block': copy.deepcopy(b)}
                       for b in quarantined)
    if scope_decisions.get('missing_note_labels'):
        issue('missing_note', '正文有脚注或尾注未找到；其余正文继续处理，建议核对原稿对应注释。',
              {'labels': scope_decisions['missing_note_labels']})
    if scope_decisions.get('start_kind') == 'first_chapter_no_abstract':
        issue('abstract_missing', '未找到摘要，已从唯一明确的第一章或绪论开始；建议核对原稿前部。', {})
    if scope_decisions.get('duplicate_abstract_count'):
        issue('duplicate_abstract_heading', '已跳过重复的独立摘要标题，周围正文继续保留。', {})
    raw_order={b['id']:i for i,b in enumerate(raw)}
    for row in removed:
        origin=row['block'].get('source_scope',{}).get('block_id',row['block']['id'])
        row['source_order']=raw_order[origin]
    removed.extend(margins)
    if parse_kind=='pdf' or image_page_mapping is not None:
        from pdf_visual import check_native_visuals, preserve_scans, preserve_vectors, guard_scope_columns, preserve_table_page_evidence
        kept,late_margins=remove_repeated_margins(kept,heights,scope_known=True)
        for row in late_margins:
            origin=(row['block'].get('source_scope') or {}).get('block_id') or row['block']['id']
            row['source_order']=raw_order.get(origin)
        removed.extend(late_margins)
        guard_scope_columns(visual_source,raw,kept,removed,issues)
        check_native_visuals(visual_source,raw,kept,removed,images,scan_pages,issues,
                             continue_on_uncertainty=True)
        preserve_scans(visual_source,kept,removed,scan_pages,resource,images,
                       issues=issues,continue_on_uncertainty=True)
        preserve_vectors(visual_source,kept,removed,issues,resource,images,
                         continue_on_uncertainty=True)
        preserve_table_page_evidence(visual_source,kept,removed,resources,images,resource,issue)
        kept.sort(key=lambda b: ((b.get('position') or {}).get('page',0),
                  ((b.get('position') or {}).get('bbox') or [0,0])[1],
                  ((b.get('position') or {}).get('bbox') or [0,0])[0]))
    elif parse_kind=='docx':
        from word_visual import recover_word_visuals
        recover_word_visuals(normalized,conversion_dir,kept,removed,issues,resource,images)
    # Text can itself contain Markdown/HTML/data-URI images. Hide only the
    # image expression when it can be delimited, and preserve surrounding prose.
    inline_pattern = re.compile(
        r'!\s*\[[^\]\n]*\]\([^\n)]*\)|<(?:svg|picture)\b[^>]*>.*?</(?:svg|picture)\s*>'
        r'|<(?:img|image)\b[^>]*>|data\s*:\s*image\s*/[^\s"\'<>]+', re.I | re.S)
    def mask_inline_text(value):
        if not contains_inline_image(value):
            return value, []
        matches = list(inline_pattern.finditer(value))
        if not matches:
            return '[图片待核]', [(0, len(value), '[图片待核]')]
        pieces, spans, cursor = [], [], 0
        for match in matches:
            if cursor < match.start():
                pieces.append(value[cursor:match.start()]);spans.append((cursor,match.start(),value[cursor:match.start()]))
            pieces.append('[图片待核]');spans.append((match.start(),match.end(),'[图片待核]'))
            cursor=match.end()
        if cursor < len(value):
            pieces.append(value[cursor:]);spans.append((cursor,len(value),value[cursor:]))
        masked = ''.join(pieces)
        if contains_inline_image(masked):
            return '[图片待核]', [(0, len(value), '[图片待核]')]
        return masked, spans
    def mask_inline_value(value):
        if isinstance(value, str):
            if contains_inline_image(value):
                return mask_inline_text(value)[0], True
            return value, False
        if isinstance(value, list):
            pairs = [mask_inline_value(v) for v in value]
            return [v for v, _ in pairs], any(changed for _, changed in pairs)
        if isinstance(value, dict):
            pairs = {k:mask_inline_value(v) for k, v in value.items()}
            return {k:v for k, (v, _) in pairs.items()}, any(changed for _, changed in pairs.values())
        return value, False
    for block in kept:
        text = str(block.get('text') or '')
        if block.get('type') == 'table':
            if any(cell.get('continuation_text') for cell in block.get('cells', [])):
                issue('table_continuation_review', '合并单元格含续格文字，需核对文字及表格归属。', block.get('position') or {})
            changed = False
            for key in ('cells', 'caption', 'units', 'notes', 'native_chart'):
                if key in block:
                    block[key], altered = mask_inline_value(block[key])
                    changed = changed or altered
            if changed:
                block['unresolved_object'] = True
                block['inline_image_unresolved'] = True
                block['inline_image_kind'] = 'table'
                issue('inline_image_review', '表格字段含有尚未恢复的内联图片，需核对并补全受影响内容。', block.get('position') or {})
            continue
        if block.get('type') != 'paragraph' or not contains_inline_image(text):
            continue
        source_scope = block.get('source_scope') or {}
        source_id = source_scope.get('block_id') or block['id']
        source_start = int(source_scope.get('start', 0))
        masked, spans = mask_inline_text(text)
        segments=[];target=0
        for start,end,piece in spans:
            if piece:
                segments.append({'target_start':target,'target_end':target+len(piece),
                    'source_block_id':source_id,'source_start':source_start+start,
                    'source_end':source_start+end,'position':copy.deepcopy(block.get('position') or {})})
                target += len(piece)
        block['source_segments'] = segments
        block.pop('source_scope', None)
        block['text'] = masked
        block['unresolved_object'] = True
        block['inline_image_unresolved'] = True
        issue('inline_image_review', '内嵌图片尚未恢复，周围正文已保留；建议核对原图。', block.get('position') or {})
    kept,layout_changes=normalize_layout(kept,page_heights=heights)
    from knowledge_outline import analyze_blocks
    toc=[b for b,info in zip(kept,analyze_blocks(kept)) if info['role']=='table_of_contents']
    source_orders={b['id']:i for i,b in enumerate(raw)}
    removed.extend(dict(source_order=source_orders.get(b['id'], source_orders.get((b.get('source_scope') or {}).get('block_id'))),
                        reason='confirmed_table_of_contents',block=copy.deepcopy(b)) for b in toc)
    kept=[b for b in kept if b not in toc]
    def excluded_position(position):
        return (any(_position_within(position,r['block'].get('position')) for r in removed)
                and not any(_position_within(position,b.get('position')) for b in kept))
    excluded_issues=[i for i in issues if i.get('code') not in {'scope_incomplete','ocr_page_incomplete','pdf_page_incomplete'}
                     and excluded_position(i.get('position'))]
    issues=[i for i in issues if i not in excluded_issues]
    for transition in scope_decisions.get('transition_visuals', []):
        issue('toc_transition_visual', '目录与正文之间的图形位置已记录，其他内容继续处理。',
              transition.get('position') or {})
    for transition in scope_decisions.get('transition_fragments', []):
        issue('toc_transition_text', '目录与正文之间有未确认的短文字，后续正文继续处理；建议核对原文。',
              transition.get('position') or {})
    for ambiguity in scope_decisions.get('tail_ocr_ambiguities', []):
        issue('appendix_ocr_ambiguity', '附录内低置信 OCR 文字可能是章节标题，相关范围待核；已保留可确认的正文。',
              ambiguity.get('position') or {})
    images=[im for im in images if not excluded_position(im.get('position'))]
    # Formulas in a split paragraph share its position; use their textual
    # markers to distinguish retained and excluded slices of that paragraph.
    scoped_kept = [b for b in kept if b.get('source_scope')]
    scoped_removed = [r['block'] for r in removed if r['block'].get('source_scope')]
    mixed_ids = {b['source_scope']['block_id'] for b in scoped_kept} & {
        b['source_scope']['block_id'] for b in scoped_removed}
    retained_formulas = []
    for formula in formulas:
        position = formula.get('position') or {}
        mixed_kept = [b for b in scoped_kept if b['source_scope']['block_id'] in mixed_ids
                      and _position_within(position, b.get('position'))]
        mixed_removed = [b for b in scoped_removed if b['source_scope']['block_id'] in mixed_ids
                         and _position_within(position, b.get('position'))]
        if mixed_kept and mixed_removed:
            token = '$' + str(formula.get('latex') or '') + '$'
            matches = [b for b in mixed_kept if token in str(b.get('text') or '')]
            in_kept = bool(matches)
            in_removed = any(token in str(b.get('text') or '') for b in mixed_removed)
            if len(matches) == 1 and not in_removed:
                retained = copy.deepcopy(formula)
                retained['_scope_source_block'] = matches[0]['id']
                retained_formulas.append(retained)
            elif in_kept == in_removed:
                issue('formula_position_unresolved', '同一段落中的公式无法准确对应清理范围；其他内容继续处理。', position)
            elif len(matches) > 1:
                issue('formula_position_unresolved', '同一段落中的重复公式无法准确对应位置；其他内容继续处理。', position)
        elif not excluded_position(position):
            retained_formulas.append(formula)
    formulas = retained_formulas
    attach_context(kept,images)
    for b in kept:b['text']=_block_text(b)
    paper='paper_'+original_sha[:24]
    sections,blocks=outline({'blocks':kept},paper)
    original_by_id={b['id']:b for b in kept}
    for b in blocks:
        origin=original_by_id[b['source_block_id']]
        if origin.get('position'):
            b['source_position']=copy.deepcopy(origin['position'])
        scope = origin.get('source_scope') or {}
        if scope.get('block_id') in mixed_ids:
            b['position'] = {**(b.get('position') or {}), 'scope_start': scope['start'], 'scope_end': scope['end']}
        for key in ('source_scope','source_segments','layout_mapping'):
            if origin.get(key):b[key]=copy.deepcopy(origin[key])
        if origin.get('source') == 'cpu_ocr' or any(
                (part.get('position') or {}).get('ocr') for part in origin.get('source_segments',[])):
            pos = copy.deepcopy(origin.get('position') or {})
            pos.update(block_id=b['id'], confidence=origin.get('ocr_confidence'))
            issue('ocr_block_review','此文本块包含 CPU OCR 内容，识别结果未经人工核对。',pos)
    for i,s in enumerate(sections):s['order']=i
    by_old={b['source_block_id']:b for b in blocks}
    for formula in formulas:
        source_block = formula.pop('_scope_source_block', None)
        if source_block in by_old:
            formula['position'] = copy.deepcopy(by_old[source_block]['position'])
    # Word image markers carry character-level placement even when a cover,
    # references and retained text share one source paragraph and position.
    marker = re.compile(r'\[\[IMAGE:([^\]]+)\]\]')
    raw_marker_ids = {ident for row in raw for ident in marker.findall(_block_text(row))}
    ambiguous_resource_ids = set()
    for row in raw:
        if row['id'] in mixed_ids:
            ambiguous_resource_ids.update(ident for ident in row.get('resource_ids', []) if ident not in raw_marker_ids)
            if row.get('resource_id') and row['resource_id'] not in raw_marker_ids:
                ambiguous_resource_ids.add(row['resource_id'])
    for r in resources:
        if r['id'] in ambiguous_resource_ids:
            issue('image_position_unresolved', '同一段落中的图片缺少可定位标记；其他内容继续处理。',
                  r.get('position') or {})
    marker_blocks = {}
    for row in kept:
        for ident in marker.findall(_block_text(row)):
            marker_blocks.setdefault(ident, []).append(by_old[row['id']])
    assets=[]
    for original_block in kept:
        b=by_old[original_block['id']]
        if original_block['type']=='table':
            from knowledge_tables import table_data
            table=table_data(original_block,original_block['id'])
            # Full values occur once in the body; structure preserves cell offsets.
            assets.append(dict(id='asset_'+b['id'],kind='table',caption=original_block.get('caption',''),block_ids=[b['id']],section_ids=[b['section_id']],position=b['position'],image_path=None,sha256=None,width=None,height=None,text=b['text'],structure={k:table[k] for k in ('cells','grid','header_rows','units','notes')}))
        for r in resources:
            if r['id'] in ambiguous_resource_ids:continue
            if r['id'] in original_block.get('resource_ids',[]) or r['id']==original_block.get('resource_id'):
                if any(a['id']==r['id'] for a in assets):continue
                assets.append(dict(r,caption=original_block.get('caption',''),block_ids=[b['id']],section_ids=[b['section_id']],text=''))
    # Resources embedded in paragraph runs may not be direct block fields.
    for r in resources:
        if r['id'] in ambiguous_resource_ids:continue
        if any(a['id']==r['id'] for a in assets):continue
        if r['id'] in raw_marker_ids:
            matches = marker_blocks.get(r['id'], [])
            if not matches:continue
        else:
            matches=[b for b in blocks if b['position'] and all(r['position'].get(k)==v for k,v in b['position'].items())]
        if not matches and excluded_position(r['position']):continue
        if not matches:
            issue('image_position_unresolved', '图片已提取，但未能对应到正文位置；其他内容继续处理。',
                  r.get('position') or {})
            continue
        im=next((im for im in images if im.get('resource_id')==r['id']),{})
        assets.append(dict(r,caption=im.get('caption',''),block_ids=[b['id'] for b in matches],section_ids=list(dict.fromkeys(b['section_id'] for b in matches)),text=''))
    if parse_kind == 'pdf':
        # PDF image streams can contain pixels outside the image's visible
        # page placement. Reuse the original page rendering for that region.
        from partial_projection import pdf_image_clip_requirements
        import fitz
        clip_checks = pdf_image_clip_requirements({
            'source': {'file': str(visual_source), 'parsed_view': {'format': 'pdf'}}, 'assets': assets})
        by_id = {a['id']: a for a in assets}
        if clip_checks:
            with fitz.open(visual_source) as pdf:
                for check in clip_checks:
                    asset = by_id.get(check.get('asset_id'))
                    position = (asset or {}).get('position') or {}
                    try:
                        page_number = position['page']
                        page = pdf[page_number - 1]
                        box = fitz.Rect(position['bbox'])
                        if box.is_empty or box.is_infinite:
                            raise ValueError('invalid image placement')
                        conflicts = [r for r in removed if r.get('reason') != 'repeated_page_margin'
                                     and (r['block'].get('position') or {}).get('page') == page_number
                                     and len((r['block'].get('position') or {}).get('bbox') or []) == 4
                                     and (box & fitz.Rect(r['block']['position']['bbox'])).get_area() > 0]
                        if conflicts:
                            raise ValueError('placement overlaps excluded content')
                        pix = page.get_pixmap(clip=box * page.rotation_matrix, matrix=fitz.Matrix(2, 2),
                                              alpha=False, annots=False)
                        rendered = resource(asset['id'] + '__visible_page', pix.tobytes('png'), '.png',
                                            'pdf_visible_region', position)
                        source_image_path, source_image_sha = asset['image_path'], asset['sha256']
                        for key in ('image_path', 'local_path', 'sha256', 'width', 'height'):
                            asset[key] = rendered[key]
                        asset.update(original_image_path=source_image_path, original_sha256=source_image_sha,
                                     image_rendering='pdf_page_visible_region')
                    except (KeyError, IndexError, TypeError, ValueError, RuntimeError, OSError):
                        issue('pdf_image_clip_review', '图片可见范围无法生成页图；保留已提取图片并记录该处。', position)
    # Every uncertain visual object receives actual source pixels. DOCX object
    # positions are structural rather than page-based, so keep all rendered pages
    # when exact page mapping is unavailable. Never guess a page or drop a tail.
    visual_issue_codes={'embedded_object_review','native_chart_visual','body_container','scanned_page','image_missing','formula_review','image_format_unreadable','pdf_vector_review','pdf_table_review','pdf_visual_incomplete'}
    visual_issues=[i for i in issues if i['code'] in visual_issue_codes]
    unresolved=[b for b in kept if b.get('type')=='unresolved_object' or b.get('unresolved_object')]
    needs_pages=bool(visual_issues or unresolved)
    if needs_pages and parse_kind in {'docx','pdf'}:
        write_json(private/'pending-visuals.json', {'issues':visual_issues,
                   'objects':[{'id':b.get('id'),'position':b.get('position'),
                               'type':b.get('type')} for b in unresolved]})
    # Located images are handed on as images; OCR inside each image is not
    # required to preserve them or their original position.
    from review_requirements import build_readiness
    readiness = build_readiness(assets,issues,scope_decisions,unresolved)
    # Cover-derived facts and Office/PDF properties are not part of the LLM view.
    education={}
    text_path=private/'body.txt';index_path=private/'index.json'
    text_path.write_text('\n\n'.join(b['text'] for b in blocks),encoding='utf-8')
    metadata=[]
    cleanup=dict(version=CLEANUP_VERSION,input_repairs=input_repairs,excluded_reading_issues=excluded_issues,
                 page_evidence=None,
                 blank_pages=blank_pages,scope=scope_decisions,layout_changes=layout_changes)
    source_blocks=[{k:copy.deepcopy(b[k]) for k in ('id','type','text','position','source','ocr_confidence') if k in b} for b in raw]
    source_view=dict(basis='original_image_memory_pages' if image_page_mapping is not None else
                     'temporary_office_conversion' if conversion_sha else 'direct_original',
                     format=parse_kind,sha256=conversion_sha or original_sha)
    if image_page_mapping is not None:source_view['page_mapping']=image_page_mapping
    index=dict(blocks=blocks,sections=sections,assets=assets,formulas=formulas,removed=removed,readiness=readiness,
               cover_facts=education,metadata=metadata,cleanup=cleanup,
               source_blocks=source_blocks,source_view=source_view)
    write_json(index_path,index)
    facts=dict(source_id='source_'+original_sha[:24],file=str(source),source_sha256=original_sha,conversion_sha256=conversion_sha,parsed_view=source_view,text_file=str(text_path),text_sha256=file_sha(text_path),index_file=str(index_path),index_sha256=file_sha(index_path),catalog_sha256=catalog_sha,reading_scope=dict(read_refs=[],unread_refs=[],extraction_issues=[i['message'] for i in issues],notes='程序材料已准备，尚未提交模型阅读'))
    bundle=dict(version=VERSION,parser_version=PARSER_VERSION,cleanup=cleanup,readiness=readiness,render_missing=render_missing,source=facts,blocks=blocks,sections=sections,assets=assets,issues=issues,attachments=[],cover_facts=education,removed=removed,formulas=formulas,file_ref='main',metadata=metadata,original_filename='main.'+parse_kind)
    bundle['file_hashes']={str(p):file_sha(p) for p in [original,text_path,index_path,catalog_path]+[Path(a['image_path']) for a in assets if a.get('image_path')]}
    if needs_pages and (private/'pending-visuals.json').is_file():
        bundle['file_hashes'][str(private/'pending-visuals.json')]=file_sha(private/'pending-visuals.json')
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
