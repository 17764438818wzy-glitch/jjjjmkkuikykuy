"""Small local previews needed for sorting, never attachment descriptions."""
from pathlib import Path
import hashlib
import os
import re
import struct
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
        signature = stream.read(512)
    if signature.startswith(b'7z\xbc\xaf\x27\x1c'): return '7z'
    if signature.startswith(b'Rar!\x1a\x07'): return 'rar'
    if signature.startswith(b'\x1f\x8b'): return 'gz'
    if signature.startswith(b'BZh'): return 'bz2'
    if signature.startswith(b'\xfd7zXZ\x00'): return 'xz'
    if signature.startswith((b'PK\x03\x04', b'PK\x05\x06', b'PK\x07\x08')): return 'zip'
    if len(signature) >= 262 and signature[257:262] == b'ustar': return 'tar'
    return 'unknown'


def _compound_format(path):
    """Identify the Office document stream without opening or changing it."""
    with path.open('rb') as stream:
        header = stream.read(512)
        if len(header) != 512 or header[:8] != bytes.fromhex('d0cf11e0a1b11ae1'):
            raise ValueError('复合文档头无效')
        shift = struct.unpack_from('<H', header, 30)[0]
        if struct.unpack_from('<H', header, 28)[0] != 0xfffe or shift not in {9, 12}:
            raise ValueError('复合文档结构不支持')
        size = 1 << shift
        sectors = path.stat().st_size // size - 1
        fat_count = struct.unpack_from('<I', header, 44)[0]
        directory = struct.unpack_from('<I', header, 48)[0]
        difat = struct.unpack_from('<I', header, 68)[0]
        difat_count = struct.unpack_from('<I', header, 72)[0]
        if not 0 < fat_count <= 4096 or difat_count > 4096:
            raise ValueError('复合文档目录超出读取范围')

        def read_sector(number):
            if number >= sectors:
                raise ValueError('复合文档目录位置无效')
            stream.seek((number + 1) * size)
            data = stream.read(size)
            if len(data) != size:
                raise ValueError('复合文档目录不完整')
            return data

        fat_ids = [number for number in struct.unpack_from('<109I', header, 76) if number < 0xfffffffa]
        seen = set()
        for _ in range(difat_count):
            if difat in seen or difat >= 0xfffffffa:
                raise ValueError('复合文档目录链无效')
            seen.add(difat)
            values = struct.unpack('<' + str(size // 4) + 'I', read_sector(difat))
            fat_ids.extend(number for number in values[:-1] if number < 0xfffffffa)
            difat = values[-1]
        if len(fat_ids) < fat_count or len(set(fat_ids[:fat_count])) != fat_count:
            raise ValueError('复合文档分配表不完整')
        fat = []
        for number in fat_ids[:fat_count]:
            fat.extend(struct.unpack('<' + str(size // 4) + 'I', read_sector(number)))
        entries, seen = [], set()
        while directory < 0xfffffffa:
            if directory in seen or directory >= len(fat) or len(seen) >= 8192:
                raise ValueError('复合文档目录链无效')
            seen.add(directory)
            data = read_sector(directory)
            for offset in range(0, size, 128):
                entry = data[offset:offset + 128]
                length = struct.unpack_from('<H', entry, 64)[0]
                name = (entry[:length - 2].decode('utf-16le').casefold()
                        if 2 <= length <= 64 and length % 2 == 0 else '')
                left, right, child = struct.unpack_from('<III', entry, 68)
                entries.append((entry[66], name, left, right, child))
            directory = fat[directory]
        if not entries or entries[0][0] != 5:
            raise ValueError('复合文档根目录无效')
        # Only root streams identify the document; embedded objects have their
        # own streams and must not be mistaken for the surrounding document.
        streams, pending, seen = set(), [entries[0][4]], set()
        while pending:
            index = pending.pop()
            if index >= 0xfffffffa:
                continue
            if index in seen or index >= len(entries):
                raise ValueError('复合文档根目录链无效')
            seen.add(index)
            entry_type, name, left, right, _ = entries[index]
            if entry_type == 2:
                streams.add(name)
            pending.extend((left, right))
        if {'encryptioninfo', 'encryptedpackage'} <= streams: return 'encrypted_office'
        if 'worddocument' in streams: return 'doc'
        if 'workbook' in streams or 'book' in streams: return 'xls'
        if 'powerpoint document' in streams: return 'ppt'
        return 'unknown_ole'


class ZipDirectoryLimit(ValueError):
    """The ordinary archive listing can remain bounded beyond Office probing."""


def detect_format(source):
    """Prefer signatures and Office container contents over filename suffix."""
    source = Path(source)
    with source.open('rb') as stream:
        head = stream.read(1024)
    # Some old Word files also contain an embedded ZIP; the OLE header wins.
    if head.startswith(bytes.fromhex('d0cf11e0a1b11ae1')):
        return _compound_format(source), 'compound_stream'
    if head.startswith(b'{\\rtf'):
        return 'rtf', 'signature'
    kind = _archive_type(source)
    if kind == 'zip':
        try:
            _check_zip_directory(source)
        except ZipDirectoryLimit:
            # A source-code attachment may contain tens of thousands of files.
            # Keep its original package and the existing bounded listing path;
            # Office-named containers still require the normal document checks.
            if source.suffix.lower() == '.zip':
                return 'zip', 'signature_directory_limited'
            raise
        with zipfile.ZipFile(source) as archive:
            names = set(archive.namelist())
            if 'word/document.xml' in names: return 'docx', 'container'
            if 'xl/workbook.xml' in names: return 'xlsx', 'container'
            if 'ppt/presentation.xml' in names: return 'pptx', 'container'
            if 'mimetype' in names:
                info = archive.getinfo('mimetype')
                if info.file_size <= 128 and not info.flag_bits & 1:
                    mime = archive.read(info)
                    if mime == b'application/vnd.oasis.opendocument.text': return 'odt', 'container'
                    if mime == b'application/vnd.oasis.opendocument.spreadsheet': return 'ods', 'container'
                    if mime == b'application/vnd.oasis.opendocument.presentation': return 'odp', 'container'
        return 'zip', 'signature'
    if kind != 'unknown':
        return kind, 'signature'
    if head.startswith(b'\x89PNG\r\n\x1a\n'): return 'png', 'signature'
    if head.startswith(b'\xff\xd8\xff'): return 'jpg', 'signature'
    if head.startswith((b'GIF87a', b'GIF89a')): return 'gif', 'signature'
    if b'%PDF-' in head:
        return 'pdf', 'signature'
    suffix = source.suffix.lower().lstrip('.')
    if suffix in {'txt', 'md', 'csv', 'tsv', 'json', 'xml', 'log', 'wps'}:
        return suffix, 'extension'
    return 'unknown', 'unknown'


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
        raise ZipDirectoryLimit('ZIP 目录超出分类读取范围')


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
    with fitz.open(str(path), filetype='pdf') as document:
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


def _text_preview(path):
    size = path.stat().st_size
    with path.open('rb') as stream:
        first = stream.read(65536)
        if size > len(first):
            stream.seek(max(len(first), size - 65536))
            last = stream.read(65536)
        else:
            last = b''
    if first.startswith(b'\xff\xfe'):
        encoding = 'utf-16le'
    elif first.startswith(b'\xfe\xff'):
        encoding = 'utf-16be'
    else:
        try:
            first.decode('utf-8-sig')
            encoding = 'utf-8-sig'
        except UnicodeDecodeError as exc:
            if size > len(first) and exc.end == len(first) and exc.reason == 'unexpected end of data':
                encoding = 'utf-8-sig'
            else:
                encoding = 'gb18030'
    start = first.decode(encoding, errors='replace').lstrip('\ufeff')
    end = last.decode(encoding, errors='replace') if last else ''
    if '\x00' in start or sum(ord(c) < 9 or 13 < ord(c) < 32 for c in start) > len(start) // 20:
        raise ValueError('文本文件内容无法按文字读取')
    truncated = size > len(first) + len(last) or len(start) + len(end) > 4000
    text = start if not truncated else start[:2000] + '\n...\n' + (end or start)[-2000:]
    samples = []
    for number, part in enumerate(text.split('\n...\n'), 1):
        if part.strip():
            samples.append({'evidence_id': f'text-{number}', 'text': part,
                'location': {'member_path': None, 'page': None, 'line': None, 'region': '文本文件局部'},
                'extraction_method': 'bounded_text_decode', 'truncated': truncated})
    return {'format': 'text', 'text': text, 'truncated': truncated, 'text_unavailable': not bool(text.strip()),
            '_handoff': {'text_samples': samples, 'images': [], 'errors': [],
                         'text_scope_partial': truncated, 'text_scope_reason': '仅保留有限文字'},
            'limitations': ['仅提取有限文字，不执行文件内容'] if truncated else ['不执行文件内容']}


def read_evidence(source, *, convert_office=None, detected=None):
    """Read bounded routing evidence from the original without changing it."""
    source = Path(source)
    kind, basis = detected if detected is not None else detect_format(source)
    if kind == 'docx':
        result = _docx(source)
    elif kind == 'pdf':
        result = _pdf(source)
    elif kind in {'doc', 'wps', 'rtf', 'odt'}:
        if convert_office is None: raise ValueError('旧版 Word 需要本机转换后读取')
        result = _docx(Path(convert_office(source)))
        for sample in result['_handoff']['text_samples']:
            sample['extraction_method'] = 'local_office_conversion_then_docx_xml_paragraph_text'
            sample['location']['region'] = '转换后的 ' + sample['location']['region']
    elif kind in {'txt', 'md', 'csv', 'tsv', 'json', 'xml', 'log'}:
        result = _text_preview(source)
    elif kind in {'xlsx', 'xls', 'ods'}:
        result = {'format': 'workbook', 'limitations': ['未读取表格内容，保留原文件']}
    elif kind in {'ppt', 'pptx', 'odp'}:
        result = {'format': kind, 'limitations': ['未读取幻灯片内容，保留原文件']}
    elif '.' + kind in ARCHIVES:
        result = _archive(source)
    elif kind in {'encrypted_office', 'unknown_ole'}:
        raise ValueError('复合文档加密或类型无法确认')
    elif kind == 'unknown' and source.suffix.lower() in {'.doc', '.docx', '.docm', '.pdf', '.rtf', '.odt'}:
        raise ValueError('文件内容与文档扩展名不符，无法确认格式')
    else:
        result = {'format': kind, 'limitations': ['未读取文件内容，仅保留文件名和格式']}
    result['detected_format'] = kind
    result['format_detection'] = basis
    return result
