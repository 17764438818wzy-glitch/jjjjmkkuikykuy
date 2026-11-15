"""Bounded local attachment previews, without copying or executing sources."""
from pathlib import Path
import codecs
import hashlib
import zipfile

MAX_TEXT_BYTES = 256 * 1024
MAX_TEXT_CHARS = 50000
MAX_PDF_PAGES = 3
MAX_MEMBERS = 256
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_DOCX_EXPANDED_BYTES = 64 * 1024 * 1024
MAX_DOCX_XML_BYTES = 8 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 4096
MAX_DIRECTORY_BYTES = 4 * 1024 * 1024


def _sha256(source):
    digest = hashlib.sha256()
    with Path(source).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _segments(text, size):
    result = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            newline = text.rfind('\n', start + size * 3 // 4, end)
            if newline > start:
                end = newline + 1
        result.append({'number': len(result) + 1, 'start': start, 'end': end})
        start = end
    return result


def _text_encoding(source):
    """Determine a stable encoding before producing byte-based continuations."""
    with Path(source).open('rb') as stream:
        bom = stream.read(4)
        if bom.startswith(codecs.BOM_UTF8):return 3, 'utf-8', False
        if bom.startswith(codecs.BOM_UTF16_LE):return 2, 'utf-16-le', False
        if bom.startswith(codecs.BOM_UTF16_BE):return 2, 'utf-16-be', False
        stream.seek(0)
        decoders = {name:codecs.getincrementaldecoder(name)(errors='strict')
                    for name in ('utf-8','gb18030')}
        non_ascii = False
        for block in iter(lambda:stream.read(1024*1024), b''):
            non_ascii = non_ascii or not block.isascii()
            for name, decoder in list(decoders.items()):
                try:
                    if '\x00' in decoder.decode(block, final=False):del decoders[name]
                except UnicodeError:
                    del decoders[name]
            if not decoders:raise UnicodeError('文本编码无法识别')
        for name, decoder in list(decoders.items()):
            try:
                if '\x00' in decoder.decode(b'', final=True):del decoders[name]
            except UnicodeError:
                del decoders[name]
        if not decoders:raise UnicodeError('文本编码无法识别')
        encoding = 'utf-8' if 'utf-8' in decoders else 'gb18030'
        return 0, encoding, non_ascii and len(decoders)>1


def _bounded_zip_directory(source):
    # Check the central-directory footer before ZipFile allocates its entries.
    # The stdlib helper also handles ZIP64; absence of this helper fails closed.
    reader = getattr(zipfile, '_EndRecData', None)
    if reader is None:return False
    with Path(source).open('rb') as stream:
        end = reader(stream)
    return bool(end and end[zipfile._ECD_ENTRIES_TOTAL] <= MAX_ARCHIVE_MEMBERS
                and end[zipfile._ECD_SIZE] <= MAX_DIRECTORY_BYTES)


def preview_attachment(source, detected_format, *, limits=None, cursor=None, _source_sha256=None):
    defaults = {'pdf_pages': MAX_PDF_PAGES, 'text_chars': MAX_TEXT_CHARS,
                'text_bytes': MAX_TEXT_BYTES, 'archive_members': MAX_MEMBERS, 'segment_chars': 8000}
    ceilings = {'pdf_pages': 100, 'text_chars': 500000, 'text_bytes': 4*1024*1024,
                'archive_members': MAX_ARCHIVE_MEMBERS, 'segment_chars': 50000}
    if limits is not None:
        if not isinstance(limits, dict) or any(k not in defaults for k in limits):
            raise ValueError('附件预览配置无效')
        for key,value in limits.items():
            minimum = 256 if key == 'segment_chars' else 1
            if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= ceilings[key]:
                raise ValueError('附件预览配置超出本地安全范围')
        defaults.update(limits)
    source = Path(source)
    kind = str(detected_format or '').lower()
    suffix = source.suffix.lower()
    source_digest = _source_sha256
    def digest():
        nonlocal source_digest
        if source_digest is None:source_digest = _sha256(source)
        return source_digest
    def check_cursor(expected):
        if cursor is None:return {}
        if (not isinstance(cursor, dict) or cursor.get('kind') != expected
                or cursor.get('source_sha256') != digest()):
            raise ValueError('续读游标与原附件不一致')
        return cursor
    def continuation(tag, **position):
        return {'kind': tag, 'source_sha256': digest(), **position}
    try:
        if kind in {'doc', 'rtf', 'wps', 'odt'}:
            import tempfile
            from local_prepare import _native_paths
            from direct_source import prepare_readable_source
            _native_paths()
            saved_cursor = check_cursor('office_text') if cursor is not None else None
            with tempfile.TemporaryDirectory(prefix='paperflow_attachment_preview_') as temporary:
                converted, readable = prepare_readable_source(source, temporary, kind, temporary)
                converted_digest = _sha256(converted)
                inner = ({'kind': 'docx_text', 'source_sha256': converted_digest,
                          'offset': saved_cursor['offset'], 'text_sha256': saved_cursor['text_sha256']}
                         if saved_cursor else None)
                result = preview_attachment(converted, readable, limits=limits, cursor=inner,
                                            _source_sha256=converted_digest)
                if result.get('next_cursor'):
                    next_inner = result['next_cursor']
                    result['next_cursor'] = continuation('office_text', offset=next_inner['offset'],
                                                         text_sha256=next_inner['text_sha256'])
                result['conversion'] = 'temporary_office_conversion'
                result.setdefault('limitations', []).append('转换文件仅用于本地预览，原附件保留。')
                return result
        if kind == 'pdf':
            import fitz
            import visual_ocr
            position = check_cursor('pdf_text')
            start_page, offset = position.get('page', 0), position.get('offset', 0)
            if not isinstance(start_page, int) or not isinstance(offset, int) or start_page < 0 or offset < 0:
                raise ValueError('PDF 续读位置无效')
            pages = []
            unread_pages = []
            remaining = defaults['text_chars']
            next_cursor = None
            with fitz.open(source) as pdf:
                if pdf.needs_pass:
                    return {'status': 'unavailable', 'reason': 'PDF 加密，无法预览'}
                total = len(pdf)
                if start_page >= total:raise ValueError('PDF 续读页超过范围')
                stop_after = position.get('stop_after_page', total)
                replacement = position.get('replace_page')
                if (not isinstance(stop_after,int) or not start_page < stop_after <= total
                        or replacement is not None and (replacement != start_page+1 or stop_after != replacement)):
                    raise ValueError('PDF 重试范围无效')
                retry_scope = ({'stop_after_page':stop_after, 'replace_page':replacement}
                               if replacement is not None else {})
                for number in range(start_page, min(stop_after, start_page + defaults['pdf_pages'])):
                    page = pdf[number]
                    text = page.get_text().strip()
                    basis = 'native_text'
                    if len(text) < 30 and page.get_image_info():
                        try:
                            lines = visual_ocr.read_pdf_page(page)
                            recognized = '\n'.join(line['text'] for line in lines)
                            if recognized.strip():
                                native_compact = ''.join(text.split())
                                ocr_compact = ''.join(recognized.split())
                                if native_compact and native_compact not in ocr_compact:
                                    text = text if ocr_compact in native_compact else text + '\n' + recognized
                                    basis = 'native_text_and_cpu_ocr_unverified'
                                else:
                                    text = recognized
                                    basis = 'cpu_ocr_unverified'
                            else:
                                basis = 'ocr_empty'
                        except InterruptedError:
                            raise
                        except Exception:
                            basis = 'ocr_unavailable'
                        if basis in {'ocr_empty','ocr_unavailable'}:
                            unread_pages.append({'page': number+1, 'reason': basis,
                                'retry_cursor': continuation('pdf_text', page=number, offset=0,
                                                             stop_after_page=number+1, replace_page=number+1),
                                'suggested_pdf_pages': 1})
                    text_hash = hashlib.sha256(text.encode('utf8')).hexdigest()
                    begin = offset if number == start_page else 0
                    if begin > len(text) or (begin and position.get('text_sha256') != text_hash):
                        raise ValueError('PDF 页面文字与续读游标不一致')
                    piece = text[begin:begin + remaining]
                    pages.append({'page': page.number+1, 'text': piece, 'basis': basis,
                                  'segments': _segments(piece, defaults['segment_chars'])})
                    remaining -= len(piece)
                    if begin + len(piece) < len(text):
                        next_cursor = continuation('pdf_text', page=number, offset=begin+len(piece),
                                                   text_sha256=text_hash, **retry_scope)
                        break
                    if (remaining == 0 or number + 1 == start_page + defaults['pdf_pages']) and number + 1 < stop_after:
                        next_cursor = continuation('pdf_text', page=number+1, offset=0)
                        break
            result = {'status': 'preview' if any(p['text'] for p in pages) else 'unavailable',
                      'pages': pages, 'total_pages': total,
                      'truncated': next_cursor is not None or bool(unread_pages), 'next_cursor': next_cursor,
                      'unread_pages': unread_pages,
                      'limitations': ['只提供有限页数及文字预览，OCR 结果未经人工核对。']}
            if replacement is not None:result['retry_scope'] = {'mode':'replace_page', 'page':replacement}
            return result
        if kind == 'docx':
            if source.stat().st_size > MAX_ARCHIVE_BYTES or not _bounded_zip_directory(source):
                return {'status': 'unavailable', 'reason': '文档超过预览大小限制，原件保留'}
            with zipfile.ZipFile(source) as archive:
                entries = archive.infolist()
                if (len(entries) > MAX_ARCHIVE_MEMBERS or sum(e.file_size for e in entries) > MAX_DOCX_EXPANDED_BYTES
                        or any(e.filename == 'word/document.xml' and e.file_size > MAX_DOCX_XML_BYTES for e in entries)):
                    return {'status': 'unavailable', 'reason': '文档展开后超过预览限制，原件保留'}
            from docx import Document
            document = Document(source)
            text = '\n'.join(p.text for p in document.paragraphs)
            position = check_cursor('docx_text')
            offset = position.get('offset', 0)
            text_hash = hashlib.sha256(text.encode('utf8')).hexdigest()
            if (not isinstance(offset, int) or offset < 0 or offset > len(text)
                    or offset and position.get('text_sha256') != text_hash):
                raise ValueError('Word 文字与续读游标不一致')
            piece = text[offset:offset+defaults['text_chars']]
            end = offset + len(piece)
            next_cursor = continuation('docx_text', offset=end, text_sha256=text_hash) if end < len(text) else None
            return {'status': 'preview' if piece.strip() else 'unavailable',
                    'text': piece, 'segments': _segments(piece, defaults['segment_chars']),
                    'truncated': next_cursor is not None, 'next_cursor': next_cursor,
                    'limitations': ['仅提取段落预览，表格、图片和复杂对象未包含。']}
        if kind == 'zip' or suffix == '.zip':
            if not _bounded_zip_directory(source):
                return {'status': 'unavailable', 'reason': '压缩包超过目录预览大小限制，原件保留'}
            position = check_cursor('archive_directory')
            offset = position.get('offset', 0)
            if not isinstance(offset, int) or offset < 0:raise ValueError('压缩包续读位置无效')
            with zipfile.ZipFile(source) as archive:
                info = [entry for entry in archive.infolist() if not entry.is_dir()]
                if offset > len(info):raise ValueError('压缩包续读位置超出范围')
                members = [{'name': entry.filename, 'bytes': entry.file_size,
                            'encrypted': bool(entry.flag_bits & 1)} for entry in info[offset:offset+defaults['archive_members']]]
            end = offset + len(members)
            next_cursor = continuation('archive_directory', offset=end) if end < len(info) else None
            return {'status': 'preview', 'member_count': len(info), 'members': members,
                    'truncated': next_cursor is not None, 'next_cursor': next_cursor,
                    'limitations': ['只读取压缩包目录，没有解压或执行文件。']}
        if suffix in {'.txt', '.md', '.csv', '.tsv', '.json', '.xml', '.py', '.c', '.h', '.cpp'}:
            position = check_cursor('text_bytes')
            if cursor is None:
                offset, encoding, ambiguous = _text_encoding(source)
            else:
                offset, encoding = position.get('offset'), position.get('encoding')
                ambiguous = bool(position.get('encoding_uncertain'))
                if (not isinstance(offset, int) or offset < 0 or offset > source.stat().st_size
                        or encoding not in {'utf-8','utf-16-le','utf-16-be','gb18030'}):
                    raise ValueError('文本续读位置无效')
            with source.open('rb') as stream:
                stream.seek(offset)
                chunks = []
                remaining = defaults['text_bytes']
                while remaining:
                    block = stream.read(min(65536, remaining))
                    if not block:break
                    chunks.append(block);remaining -= len(block)
                raw = b''.join(chunks)
                more_bytes = bool(stream.read(1))
            for encoding in (encoding,):
                try:
                    decoder = codecs.getincrementaldecoder(encoding)(errors='strict')
                    text = decoder.decode(raw, final=not more_bytes)
                    if '\x00' not in text:
                        piece = text[:defaults['text_chars']]
                        consumed = len(piece.encode(encoding))
                        if raw[:consumed].decode(encoding) != piece:raise ValueError('文本续读偏移无法验证')
                        more = more_bytes or len(text) > len(piece) or bool(decoder.getstate()[0])
                        if more and not consumed:
                            return {'status': 'unavailable', 'reason': '读取范围不足以形成完整字符，请增大文本读取字节数。'}
                        next_cursor = continuation('text_bytes', offset=offset+consumed, encoding=encoding,
                                                   encoding_uncertain=ambiguous) if more else None
                        limitations = ['仅供附件独立处理参考，不执行源文件。']
                        if ambiguous:limitations.append('内容同时符合多种编码，按 UTF-8 预览，建议核对。')
                        return {'status': 'preview' if piece.strip() else 'unavailable', 'text': piece,
                                'segments': _segments(piece, defaults['segment_chars']),
                                'truncated': more, 'next_cursor': next_cursor,
                                'encoding': encoding, 'encoding_uncertain': ambiguous, 'limitations': limitations}
                except UnicodeError:
                    continue
            return {'status': 'unavailable', 'reason': '文本编码无法识别'}
        if kind in {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp'}:
            try:
                import visual_ocr
                lines=visual_ocr.read_image(source)
                text='\n'.join(str(line.get('text') or '') for line in lines)
                return {'status':'source_image','ocr_status':'completed_unverified',
                        'ocr_text':text[:defaults['text_chars']],
                        'truncated':len(text)>defaults['text_chars'],
                        'limitations':['本机 OCR 仅供附件预览，结果未经核对；未复制原图。']}
            except InterruptedError:
                raise
            except Exception:
                return {'status':'source_image','ocr_status':'unavailable',
                        'limitations':['已登记源图可独立查看；OCR 暂不可用，未复制原图。']}
        return {'status': 'not_supported', 'reason': '该格式尚无本地预览器'}
    except InterruptedError:
        raise
    except Exception as exc:
        return {'status': 'unavailable', 'reason': f'预览失败：{type(exc).__name__}'}


def preview_attachment_chunks(source, detected_format, *, limits=None, cursor=None, max_chunks=4,
                              _source_sha256=None):
    """Read a bounded number of preview chunks; retain a cursor for more."""
    if not isinstance(max_chunks, int) or isinstance(max_chunks, bool) or not 1 <= max_chunks <= 16:
        raise ValueError('附件单次预览分段数量无效')
    snapshot_hash = _source_sha256
    if cursor is not None and snapshot_hash is None:snapshot_hash = _sha256(source)
    first = preview_attachment(source, detected_format, limits=limits, cursor=cursor,
                               _source_sha256=snapshot_hash)
    rows = []
    next_cursor = first.get('next_cursor')
    if snapshot_hash is None and next_cursor is not None:snapshot_hash = next_cursor['source_sha256']
    if snapshot_hash is None and first.get('unread_pages'):
        snapshot_hash = first['unread_pages'][0]['retry_cursor']['source_sha256']
    failure = first.get('reason') if first.get('status') == 'unavailable' else None
    if failure and cursor is not None and next_cursor is None:
        next_cursor = cursor
    for _ in range(1, max_chunks):
        if failure or next_cursor is None:break
        requested_cursor = next_cursor
        row = preview_attachment(source, detected_format, limits=limits, cursor=requested_cursor,
                                 _source_sha256=snapshot_hash)
        rows.append(row)
        if row.get('status') == 'unavailable' and row.get('reason') and not row.get('next_cursor'):
            failure = row['reason']
            next_cursor = requested_cursor
            break
        next_cursor = row.get('next_cursor')
    if _source_sha256 is None and snapshot_hash is not None and _sha256(source) != snapshot_hash:
        return {'status':'unavailable', 'reason':'附件在续读期间发生变化，已保留原进度。',
                'continuations':[], 'chunks_processed':0, 'next_cursor_after_pass':cursor}
    unread = list(first.get('unread_pages') or [])
    for row in rows:unread.extend(row.get('unread_pages') or [])
    first['first_chunk_truncated'] = bool(first.get('truncated'))
    first['continuations'] = rows
    first['chunks_processed'] = 1 + len(rows)
    first['next_cursor_after_pass'] = next_cursor
    first['unread_pages'] = unread
    first['truncated'] = bool(next_cursor or failure or unread)
    if failure:first['continuation_error'] = failure
    return first
