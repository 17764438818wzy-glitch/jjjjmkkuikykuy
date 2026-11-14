"""Small local previews needed for sorting, never attachment descriptions."""
from pathlib import Path
import hashlib
import os
import re
import subprocess
import threading
import time
import zipfile
from xml.etree import ElementTree
from .attachment_evidence_handoff import _member_path


SEVEN_ZIP = Path(r'F:\论文处理端迁移包\02_运行环境\7zip\7z.exe')
ARCHIVES = {'.zip', '.7z', '.rar', '.gz', '.tar', '.bz2', '.xz'}
MAX_MEMBERS = 80
MAX_LIST_BYTES = 256 * 1024
MAX_DOCUMENT_BYTES = 32 * 1024 * 1024


def _archive_type(path):
    with path.open('rb') as stream:
        signature = stream.read(8)
    if signature.startswith(b'7z\xbc\xaf\x27\x1c'): return '7z'
    if signature.startswith(b'Rar!\x1a\x07'): return 'rar'
    if signature.startswith(b'\x1f\x8b'): return 'gz'
    if signature.startswith((b'PK\x03\x04', b'PK\x05\x06', b'PK\x07\x08')): return 'zip'
    return 'unknown'


def _limited_listing(path, seven=SEVEN_ZIP):
    if not seven.is_file():
        return b'', False, '本机压缩包目录读取器不可用'
    proc = subprocess.Popen([str(seven), 'l', '-slt', '-ba', '-sccUTF-8', '-p-', '--', str(path)],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    chunks = bytearray()
    limited = threading.Event()
    def read():
        while True:
            part = proc.stdout.read(4096)
            if not part: break
            remaining = MAX_LIST_BYTES - len(chunks)
            chunks.extend(part[:remaining])
            if len(part) > remaining:
                limited.set()
                break
    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    deadline = time.monotonic() + 20
    timed_out = False
    try:
        while reader.is_alive():
            reader.join(0.05)
            if limited.is_set() or time.monotonic() >= deadline:
                timed_out = not limited.is_set()
                proc.kill()
                break
        proc.wait(timeout=3)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        reader.join(timeout=3)
        proc.stdout.close()
    if timed_out: return bytes(chunks), True, '压缩包目录读取超时'
    if proc.returncode and not limited.is_set(): return bytes(chunks), False, '压缩包目录无法读取'
    return bytes(chunks), limited.is_set(), None


def _bounded_archive_sha(path, *, max_bytes=MAX_DOCUMENT_BYTES, seven=SEVEN_ZIP):
    """Hash streamed archive content only; do not write extracted files."""
    if not seven.is_file():
        return None
    proc = subprocess.Popen([str(seven), 'x', '-so', '-p-', '-y', '--', str(path)],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    digest, limited, failed = hashlib.sha256(), threading.Event(), threading.Event()
    def read():
        total = 0
        try:
            while True:
                part = proc.stdout.read(65536)
                if not part: break
                total += len(part)
                if total > max_bytes:
                    limited.set()
                    break
                digest.update(part)
        except OSError:
            failed.set()
    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    deadline, timed_out = time.monotonic() + 20, False
    try:
        while reader.is_alive():
            reader.join(0.05)
            if limited.is_set() or time.monotonic() >= deadline:
                timed_out = not limited.is_set()
                proc.kill()
                break
        proc.wait(timeout=3)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        reader.join(timeout=3)
        proc.stdout.close()
    if timed_out or limited.is_set() or failed.is_set() or proc.returncode:
        return None
    return digest.hexdigest()


def _unavailable_listing(reason):
    return {'evidence_id': None, 'status': 'unavailable', 'total_entries': None,
            'listed_entries': 0, 'entries': [], 'reason': reason}


def _archive(path):
    kind = _archive_type(path)
    if kind == 'unknown':
        error = '无法识别压缩包实际格式'
        return {'archive_type': kind, 'members': [], 'read_error': error,
                '_handoff': {'archive_listing': _unavailable_listing(error), 'text_samples': [], 'images': [], 'errors': [error]}}
    raw, limited, error = _limited_listing(path)
    if error and not raw:
        return {'archive_type': kind, 'members': [], 'read_error': error,
                '_handoff': {'archive_listing': _unavailable_listing(error), 'text_samples': [], 'images': [], 'errors': [error]}}
    decoded = raw.decode('utf-8', 'replace').replace('\r', '')
    blocks = decoded.split('\n\n')
    if (limited or error) and not decoded.endswith('\n\n'):
        # The final block can end halfway through a member path or metadata.
        blocks = blocks[:-1]
    members, listing_rows, unparseable = [], [], False
    for block in blocks:
        fields = dict(line.split(' = ', 1) for line in block.splitlines() if ' = ' in line)
        if 'Path' in fields and fields.get('Folder') != '+':
            members.append({'name': fields['Path'], 'size': fields.get('Size', '')})
        if 'Path' in fields:
            folder = fields.get('Folder')
            size = fields.get('Size', '')
            listing_rows.append({'path': fields['Path'], 'type': 'directory' if folder == '+' else 'file' if folder == '-' else None,
                                 'bytes': int(size) if size.isdigit() else None})
        elif fields:
            unparseable = True
    nested = any(Path(row['name']).suffix.lower() in ARCHIVES for row in members[:MAX_MEMBERS])
    safe_rows = [row for row in listing_rows if _member_path(row['path'])]
    incomplete = bool(error) or limited or unparseable or len(safe_rows) != len(listing_rows) or len(safe_rows) > MAX_MEMBERS
    incomplete = incomplete or any(row['type'] is None for row in safe_rows)
    visible = safe_rows[:MAX_MEMBERS]
    total = len(listing_rows) if not limited and not unparseable and not error else None
    if incomplete:
        reason = error or '目录截断或含无法确认的条目类型、相对路径'
        listing = ({'evidence_id': 'archive-1', 'status': 'partial', 'total_entries': total,
                    'listed_entries': len(visible), 'entries': visible, 'reason': reason}
                   if visible else _unavailable_listing(reason))
    else:
        listing = {'evidence_id': 'archive-1', 'status': 'complete', 'total_entries': len(visible),
                   'listed_entries': len(visible), 'entries': visible, 'reason': None}
    result = {'archive_type': kind, 'members': members[:MAX_MEMBERS],
              'nested': nested, 'truncated': bool(error) or limited or len(members) > MAX_MEMBERS,
              'limitations': ['仅读取目录；核验单篇重复正文时限量读取至内存，不写出或执行成员'],
              '_handoff': {'archive_listing': listing, 'text_samples': [], 'images': [], 'errors': []}}
    if error:
        result['read_error'] = error
    if not result['truncated'] and len(members) == 1:
        member = members[0]['name']
        if Path(member).suffix.lower() in {'.doc', '.docx', '.pdf', '.wps', '.rtf', '.odt'}:
            if kind == 'zip':
                try:
                    _check_zip_directory(path)
                    with zipfile.ZipFile(path) as archive:
                        files = [info for info in archive.infolist() if not info.is_dir()]
                        if len(files) == 1 and files[0].file_size <= MAX_DOCUMENT_BYTES and not files[0].flag_bits & 1:
                            digest = hashlib.sha256()
                            with archive.open(files[0]) as stream:
                                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                                    digest.update(chunk)
                            result['single_document_sha256'] = digest.hexdigest()
                        else:
                            result['read_error'] = '压缩包内正文加密或超出核验范围'
                except (OSError, ValueError, zipfile.BadZipFile, RuntimeError):
                    result['read_error'] = '压缩包内正文无法核验'
            else:
                size = members[0].get('size', '')
                if size.isdigit() and int(size) <= MAX_DOCUMENT_BYTES:
                    digest = _bounded_archive_sha(path)
                    if digest:
                        result['single_document_sha256'] = digest
                    else:
                        result['read_error'] = '压缩包内正文无法核验'
                else:
                    result['read_error'] = '压缩包内正文大小未知或超出核验范围'
    if result.get('read_error'):
        result['_handoff']['errors'].append(result['read_error'])
    return result


def _check_zip_directory(path):
    # Refuse oversized central directories before loading their entries.
    with path.open('rb') as stream:
        stream.seek(max(0, path.stat().st_size - 65557))
        tail = stream.read(65557)
    pos = tail.rfind(b'PK\x05\x06')
    if pos < 0 or len(tail) < pos + 22:
        raise ValueError('Word 结构无法读取')
    count = int.from_bytes(tail[pos + 10:pos + 12], 'little')
    size = int.from_bytes(tail[pos + 12:pos + 16], 'little')
    if count == 65535 or count > 20000 or size > 8 * 1024 * 1024:
        raise ValueError('ZIP 目录超出分类读取范围')


def _cover_table_title(root, namespace):
    """Keep the labelled title column separate from adjacent cover cells."""
    body = root.find(namespace + 'body')
    if body is None: return None
    candidates = []
    cover_nodes = []
    for node in list(body)[:20]:
        if node.tag == namespace + 'p':
            text = ''.join(child.text or '' for child in node.iter(namespace + 't'))
            if re.fullmatch(r'\s*(?:摘\s*要|abstract|目\s*录|contents)\s*', text, re.I): break
        cover_nodes.append(node)
    for table in [x for x in cover_nodes if x.tag == namespace + 'tbl']:
        if any(x.tag in {namespace + 'gridSpan', namespace + 'vMerge'} for x in table.iter()):
            continue
        table_index = cover_nodes.index(table)
        following_paragraph = ''
        for node in cover_nodes[table_index + 1:]:
            if node.tag != namespace + 'p': break
            following_paragraph = ''.join(child.text or '' for child in node.iter(namespace + 't')).strip()
            if following_paragraph: break
        if (following_paragraph and not re.match(
                r'^(?:学院|学校|专业|班级|学号|姓名|学生姓名|指导教师|日期|摘要)\s*[:：]', following_paragraph)):
            # A title may continue outside its table; let the text evidence handle it.
            continue
        rows = []
        for row in table.findall(namespace + 'tr')[:20]:
            cells = []
            for cell in row.findall(namespace + 'tc')[:20]:
                paragraphs = [''.join(node.text or '' for node in p.iter(namespace + 't')).strip()
                              for p in cell.findall(namespace + 'p')]
                cells.append('\n'.join(value for value in paragraphs if value))
            rows.append(cells)
        for index, cells in enumerate(rows):
            for column, value in enumerate(cells):
                inline = re.match(r'^\s*(?:论文)?题\s*目\s*[:：]\s*(.+)$', value, re.S)
                label = re.fullmatch(r'\s*(?:论文)?题\s*目\s*[:：]?\s*', value)
                if inline:
                    target, parts = column, [inline.group(1)]
                elif label and column + 1 < len(cells) and cells[column + 1].strip():
                    target, parts = column + 1, [cells[column + 1]]
                else:
                    continue
                incomplete = False
                for following in rows[index + 1:index + 4]:
                    if len(following) <= target:
                        if any(v.strip() for v in following): incomplete = True
                        break
                    if any(v.strip() for v in following[:target]): break
                    part = following[target].strip()
                    if not part or re.match(r'^(?:学院|学校|专业|班级|学号|姓名|学生姓名|指导教师|日期|摘要)\s*[:：]', part): break
                    parts.append(part)
                if incomplete: continue
                lines = [line for part in parts for line in part.splitlines() if line.strip()]
                cleaned = [re.sub(r'^[\s:：\uf000-\uf8ff]+', '', part).strip() for part in lines]
                title = ''
                for part in cleaned:
                    separator = ' ' if title and re.search(r'[A-Za-z0-9]$', title) and re.match(r'[A-Za-z0-9]', part) else ''
                    title += separator + part
                if title and len(title) <= 160:
                    candidates.append({'title': title, 'excerpt': '\n'.join(parts)})
    return candidates[0] if len(candidates) == 1 else None


def _docx(path):
    _check_zip_directory(path)
    with zipfile.ZipFile(path) as archive:
        has_unread_resources = any(name.startswith(('word/media/', 'word/embeddings/',
                                                   'word/diagrams/', 'word/charts/', 'word/activeX/'))
                                   for name in archive.namelist())
        info = archive.getinfo('word/document.xml')
        if info.file_size > 16 * 1024 * 1024 or info.flag_bits & 1:
            raise ValueError('Word 正文无法在限定范围内读取')
        raw = archive.read(info)
    if b'<!DOCTYPE' in raw.upper():
        raise ValueError('Word 文本包含不支持的文档声明')
    root = ElementTree.fromstring(raw)
    main_namespace = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    unread_nodes = {main_namespace + name for name in ('drawing', 'pict', 'object', 'altChunk', 'subDoc')}
    has_unread_visual_content = has_unread_resources or any(node.tag in unread_nodes for node in root.iter())
    paragraphs = []
    for paragraph in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
        paragraphs.append(''.join((node.text or '') for node in paragraph.iter() if node.tag.endswith('}t')))
    text = '\n'.join(paragraphs)
    brief = text if len(text) <= 4000 else text[:2000] + '\n...\n' + text[-2000:]
    parts = [(text, 'word/document.xml 正文文字')] if len(text) <= 4000 else [
        (text[:2000], 'word/document.xml 正文文字开头'), (text[-2000:], 'word/document.xml 正文文字末尾')]
    samples = [{'evidence_id': f'text-{index}', 'text': value,
                'location': {'member_path': None, 'page': None, 'line': None, 'region': region},
                'extraction_method': 'docx_xml_paragraph_text', 'truncated': len(text) > 4000}
               for index, (value, region) in enumerate(parts, 1) if value]
    return {'format': 'docx', 'text': brief, 'cover_table_title': _cover_table_title(root, main_namespace),
            'truncated': len(text) > 4000,
            'text_unavailable': not bool(text.strip()),
            'has_unread_visual_content': has_unread_visual_content,
            '_handoff': {'text_samples': samples, 'images': [], 'errors': [],
                         'text_scope_partial': True, 'text_scope_reason': '只读取正文 XML 的有限文字'},
            'limitations': ['仅提取正文开头和结尾文字，不识别图片']}


def _pdf(path):
    import fitz
    if path.stat().st_size > 256 * 1024 * 1024:
        raise ValueError('PDF 超出分类读取范围')
    with fitz.open(path) as document:
        if document.needs_pass or len(document) > 1500:
            raise ValueError('PDF 加密或页数超出分类读取范围')
        indices = sorted(set(i for i in (0, 1, 2, 3, len(document) // 2, len(document) - 1)
                             if 0 <= i < len(document)))
        pages, samples = [], []
        for number in indices:
            page = document[number]
            text = page.get_text()
            pages.append({'page': number + 1, 'text': text[:700], 'text_characters': len(text.strip()),
                          'images': len(page.get_images()),
                          'width': round(page.rect.width), 'height': round(page.rect.height)})
            if text[:700]:
                samples.append({'evidence_id': f'text-{number+1}', 'text': text[:700],
                    'location': {'member_path': None, 'page': number + 1, 'line': None, 'region': None},
                    'extraction_method': 'pdf_page_text', 'truncated': len(text) > 700})
        visual_pages = sum(page['width'] > page['height'] * 1.2 and page['images'] >= 1
                           and page['text_characters'] <= 180 for page in pages)
        image_pages = sum(page['images'] >= 1 and page['text_characters'] <= 180 for page in pages)
        return {'format': 'pdf', 'pages': pages, 'page_count': len(document),
                'text_unavailable': not any(page['text'].strip() for page in pages),
                'visual_heavy': len(pages) >= 3 and visual_pages >= 3 and visual_pages * 4 >= len(pages) * 3,
                'scan_heavy': len(pages) >= 3 and image_pages >= 3 and image_pages * 4 >= len(pages) * 3,
                '_handoff': {'text_samples': samples, 'images': [], 'errors': [],
                             'text_scope_partial': len(indices) < len(document) or any(s['truncated'] for s in samples),
                             'text_scope_reason': '仅提取部分页面或部分页面文字'},
                'limitations': ['仅提取部分页面文字和页面尺寸，不识别图片']}


def read_evidence(source, *, convert_office=None):
    """Read bounded routing evidence from the original without changing it."""
    source = Path(source)
    suffix = source.suffix.lower()
    if suffix == '.docx': return _docx(source)
    if suffix == '.pdf': return _pdf(source)
    if suffix in {'.doc', '.wps', '.rtf', '.odt'}:
        if convert_office is None: raise ValueError('旧版 Word 需要本机转换后读取')
        result = _docx(Path(convert_office(source)))
        for sample in result['_handoff']['text_samples']:
            sample['extraction_method'] = 'local_office_conversion_then_docx_xml_paragraph_text'
            sample['location']['region'] = '转换后的 ' + sample['location']['region']
        return result
    if suffix in {'.xlsx', '.xlsm'}:
        return {'format': 'workbook', 'limitations': ['仅用于资料分类，工作簿内容留给后续处理']}
    if suffix in ARCHIVES: return _archive(source)
    return {'format': suffix or 'unknown', 'limitations': ['仅使用文件名和格式进行分类']}
