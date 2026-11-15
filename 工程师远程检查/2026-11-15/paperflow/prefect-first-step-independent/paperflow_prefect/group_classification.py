"""Group-local sorting and a verifiable handoff; never dispatch downstream work."""
from pathlib import Path
import json
import os
import re
import tempfile
import unicodedata
import uuid

from .records import identity, read_json, sha_file
from .nimble_group import _preview


BODY_FORMATS = {'.doc', '.docx', '.pdf', '.wps', '.rtf', '.odt', '.txt', '.md'}
ARCHIVE_FORMATS = {'.zip', '.7z', '.rar', '.gz', '.tar', '.bz2', '.xz'}
MAX_GROUPS = 30000
GROUP_BATCH_SIZE = 500
COVER_FIELD = (r'(?:二\s*级\s*学\s*院|院\s*系|学\s*院|学\s*校|专\s*业|专业名称|班\s*级|'
               r'学\s*号|姓\s*名|学生姓名|作者姓名|论文编号|作品编号|指导教师|指导老师|'
               r'导\s*师|作\s*者|日\s*期|年\s*级|完成时间|完成日期|'
               r'摘\s*要|abstract|目\s*录|contents|第[一1]章|绪论)')
TEMPLATE_NOTE = (r'[（(]\s*(?:格式(?:全校|要求)|封面(?:可|用|需要)|'
                 r'以下(?:\d+|[一二三四五六七八九十]+)项|字体|字号)[^（）()]{0,240}[）)]')


class GroupClassificationError(ValueError):
    pass


def _group_paths(group, library):
    library = Path(os.path.abspath(library))
    group = Path(os.path.abspath(group))
    if not library.is_dir():
        raise GroupClassificationError('资料库文件夹不存在或无法读取')
    if not group.is_dir():
        raise GroupClassificationError('资料组文件夹不存在或无法读取')
    for source in (library, group):
        for path in (source, *source.parents):
            if path.is_symlink() or path.is_junction():
                raise GroupClassificationError('资料组来源含链接，不能确认归属')
    group, library = group.resolve(), library.resolve()
    if group == library:
        raise GroupClassificationError('资料组不能是整个资料库')
    if not group.is_relative_to(library):
        raise GroupClassificationError('资料组文件夹不在资料库内')
    return group, library


def _group_id(group, library):
    return identity(['source-group/1', str(library).casefold(),
                     group.relative_to(library).as_posix().casefold()])[:32]


def _group_lock_key(group):
    """Same physical group gets one local lock regardless of library root."""
    return identity(['source-group-lock/1', os.path.normcase(os.path.normpath(str(group.resolve())))])[:32]


def _files(group):
    def fail(error):
        raise GroupClassificationError('资料组目录无法完整读取') from error
    paths = []
    for current, dirs, files in os.walk(group, followlinks=False, onerror=fail):
        if Path(current) == group:
            unknown = [name for name in dirs if name not in {'正文', '附件', '垃圾', '垃圾材料'}]
            if unknown:
                raise GroupClassificationError('资料组含无法确认归属的子文件夹')
        elif dirs:
            raise GroupClassificationError('分类目录内含无法确认归属的子文件夹')
        for name in [*dirs, *files]:
            path = Path(current) / name
            if path.is_symlink() or path.is_junction():
                raise GroupClassificationError('资料组内含链接，不能确认归属')
            if not path.resolve().is_relative_to(group):
                raise GroupClassificationError('来源路径超出资料组')
        paths.extend(Path(current) / name for name in files)
    return sorted(paths, key=lambda path: path.relative_to(group).as_posix().casefold())


def _snapshot_id(group_id, rows):
    return identity(['group-handoff/1', group_id,
                     [(r['relative'], r['bytes'], r['sha256']) for r in rows]])[:32]


def _version(handoff):
    return identity({key: value for key, value in handoff.items() if key != 'result_version'})[:32]


def _evidence_text(evidence, reference=None):
    if evidence.get('format') == 'pdf':
        pages = [page for page in evidence.get('pages', []) if page.get('page', 0) <= 3]
        if reference:
            match = re.fullmatch(r'正文 PDF 第(\d+)页文字', reference)
            if match:
                pages = [page for page in pages if page.get('page') == int(match.group(1))]
            elif reference != '正文 PDF 前三页文本':
                return ''
        return '\n'.join(page.get('text', '') for page in pages)
    expected_reference = '正文文本开头' if evidence.get('format') == 'text' else '正文 Word 开头文本'
    if reference and reference != expected_reference:
        return ''
    return evidence.get('text', '').split('\n...\n', 1)[0]


def _normalized_with_offsets(value):
    result, offsets = [], []
    for index, character in enumerate(value):
        for normalized in unicodedata.normalize('NFKC', character):
            if not normalized.isspace():
                result.append(normalized)
                offsets.append(index)
    return ''.join(result), offsets


def _title_proof(evidence, title, excerpt, *, labelled, reference):
    if not isinstance(title, str) or not title.strip() or not isinstance(excerpt, str) or not excerpt.strip():
        return False
    if not isinstance(reference, str) or not reference.strip():
        return False
    if reference == '正文 Word 封面表格单元格':
        fact = evidence.get('cover_table_title')
        return (labelled and evidence.get('format') == 'docx' and isinstance(fact, dict)
                and fact.get('title') == title and fact.get('excerpt') == excerpt)
    text = _evidence_text(evidence, reference)
    normalized_text, offsets = _normalized_with_offsets(text)
    normalized_title, _ = _normalized_with_offsets(title)
    normalized_excerpt, _ = _normalized_with_offsets(excerpt)
    position = normalized_text.find(normalized_excerpt)
    if not normalized_title or normalized_title != normalized_excerpt or position < 0:
        return False
    if labelled:
        # A different cover field must never be merged into the title.
        if (re.search(r'(?:论文编号|作品编号|作者姓名|完成日期|学生姓名|指导教师)\s*[:：]', excerpt)
                or re.search(r'\n[ \t]*[\u4e00-\u9fffA-Za-z]{2,16}[ \t]*[:：]', excerpt)):
            return False
        first = offsets[position]
        if not re.search(r'题\s*目\s*[：:]?\s*$', text[max(0, first - 80):first]):
            return False
        last = offsets[position + len(normalized_excerpt) - 1] + 1
        following = text[last:]
        while True:
            note = re.match(r'^\s*' + TEMPLATE_NOTE, following)
            if not note: break
            following = following[note.end():]
        boundary = r'^\s*' + COVER_FIELD + r'(?:\s*[:：]|\s|$)'
        return bool(re.match(boundary, following, re.I))
    return True


def _is_cover_field_line(line):
    line = unicodedata.normalize('NFKC', line).strip()
    if re.match(r'^' + COVER_FIELD + r'(?:[ \t]*[:：]|[ \t]+|$)', line, re.I):
        return True
    compact = re.sub(r'\s+', '', line)
    if re.fullmatch(r'(?:姓名|学生姓名|作者姓名|指导教师|指导老师|导师|作者)[\u4e00-\u9fff·]{2,5}', compact):
        return True
    if re.fullmatch(r'(?:学号|论文编号|作品编号)\d{6,}', compact):
        return True
    if (re.fullmatch(r'(?:专业名称|专业|院系|学院|学校|班级|年级|日期|完成时间|完成日期).{1,16}', compact)
            and not re.search(r'研究|调查|分析|探究|现状|策略|方法|影响|对策', compact)):
        return True
    return False


def _ambiguous_cover_field_line(line):
    if _is_cover_field_line(line):
        return False
    compact = re.sub(r'\s+', '', unicodedata.normalize('NFKC', line))
    return bool(re.fullmatch(r'(?:专业名称|专业|院系|学院|学校|班级|年级|日期|完成时间|完成日期).{1,16}', compact)
                and re.search(r'研究|调查|分析|探究|现状|策略|方法|影响|对策', compact))


def _cover_span(evidence, title, excerpt, reference, *, allow_partial_line=False):
    """Locate unlabelled title text in the readable cover region."""
    if reference not in {'正文 Word 开头文本', '正文文本开头', '正文 PDF 第1页文字'}:
        return None
    if not _title_proof(evidence, title, excerpt, labelled=False, reference=reference):
        return None
    text = _evidence_text(evidence, reference)
    normalized, offsets = _normalized_with_offsets(text)
    wanted, _ = _normalized_with_offsets(excerpt)
    position = normalized.find(wanted)
    if position < 0:
        return None
    first, last = offsets[position], offsets[position + len(wanted) - 1] + 1
    before = text[:first]
    if re.search(r'(?im)^\s*(?:摘\s*要|abstract|目\s*录|contents|第[一1]章|绪论)(?:\s|[:：]|$)', before):
        return None
    if text[text.rfind('\n', 0, first) + 1:first].strip():
        return None
    line_remainder = text[last:].split('\n', 1)[0].strip()
    if not allow_partial_line and line_remainder and not _is_cover_field_line(line_remainder):
        return None
    lines = [line.strip() for line in excerpt.splitlines() if line.strip()]
    if not lines or len(lines) > 3 or len(title) > 160:
        return None
    generic = r'^(?:本科|专科|硕士|博士)?(?:生)?(?:毕业|学位)(?:论文|设计|设计说明书)(?:\s*\(.*\))?$'
    if (any(_is_cover_field_line(line) or re.fullmatch(generic, line, re.I) for line in lines)
            or any(_ambiguous_cover_field_line(line) for line in lines)):
        return None
    return text, first, last


def _join_cover_lines(excerpt):
    title = ''
    for line in (line.strip() for line in excerpt.splitlines() if line.strip()):
        separator = ' ' if title and re.search(r'[A-Za-z0-9]$', title) and re.match(r'[A-Za-z0-9]', line) else ''
        title += separator + line
    return title


def _complete_cover_proof(evidence, title, excerpt, reference):
    span = _cover_span(evidence, title, excerpt, reference)
    if span is None:
        return False
    text, _, last = span
    for line in text[last:].splitlines():
        if not line.strip() or re.fullmatch(TEMPLATE_NOTE, line.strip()):
            continue
        return _is_cover_field_line(line)
    return False


def _filename_cover_proof(evidence, title, excerpt, reference):
    span = _cover_span(evidence, title, excerpt, reference)
    if span is None:
        return False
    if _complete_cover_proof(evidence, title, excerpt, reference):
        return True
    text, _, last = span
    if text[last:].strip():
        return False
    if evidence.get('format') == 'pdf':
        page = next((item for item in evidence.get('pages', []) if item.get('page') == 1), None)
        return bool(page and page.get('text_characters', 0) <= len(page.get('text', '').strip()))
    return evidence.get('truncated') is False


def _complete_cover_title(evidence, candidate, excerpt, reference):
    """Join adjacent cover title lines up to the next identified cover field."""
    span = _cover_span(evidence, candidate, excerpt, reference, allow_partial_line=True)
    if span is None:
        return None
    text, first, last = span
    boundary = last
    for line in text[last:].splitlines(keepends=True):
        if _is_cover_field_line(line):
            break
        if _ambiguous_cover_field_line(line):
            return None
        boundary += len(line)
    else:
        return None
    complete_excerpt = text[first:boundary].strip()
    complete_excerpt = re.sub(r'\s*' + TEMPLATE_NOTE + r'\s*$', '', complete_excerpt).strip()
    title = _join_cover_lines(complete_excerpt)
    if title and _complete_cover_proof(evidence, title, complete_excerpt, reference):
        return title, complete_excerpt
    return None


def _filename_cover_title(evidence, filename_title, candidate=None, source=None):
    """Use a filename choice to recover only text actually present on the cover."""
    reference = (source or {}).get('reference')
    if reference not in {'正文 Word 开头文本', '正文文本开头', '正文 PDF 第1页文字'}:
        reference = ('正文 PDF 第1页文字' if evidence.get('format') == 'pdf' else
                     '正文文本开头' if evidence.get('format') == 'text' else '正文 Word 开头文本')
    text = _evidence_text(evidence, reference)
    normalized, offsets = _normalized_with_offsets(text)
    wanted, _ = _normalized_with_offsets(filename_title)
    current, _ = _normalized_with_offsets(candidate or '')
    if not wanted or (current and not wanted.startswith(current)):
        return None
    position = normalized.find(wanted)
    if position < 0 or (current and normalized.find(current) != position):
        return None
    excerpt = text[offsets[position]:offsets[position + len(wanted) - 1] + 1]
    title = _join_cover_lines(excerpt)
    if not title or not _filename_cover_proof(evidence, title, excerpt, reference):
        return None
    return title, excerpt, reference


def _filename_candidate_proof(body, title, source):
    """Bind an inferred filename candidate to the original body and choices."""
    from .filename_title import filename_title_candidates
    if not isinstance(body, dict) or not isinstance(source, dict):
        return False
    decision = source.get('decision')
    if (source.get('reference') != '正文文件名（Nimble候选）'
            or not isinstance(decision, dict) or not isinstance(decision.get('model'), str)
            or not decision['model'].strip()):
        return False
    filename = Path(body.get('relative', '')).name
    candidates = filename_title_candidates(filename)
    choices = {f'title_{i:02}': value for i, value in enumerate(candidates, 1)}
    choices['none'] = None
    choice = decision.get('choice')
    return (bool(candidates) and decision.get('candidates') == choices
            and choice in choices and choices[choice] == title and decision.get('title') == title
            and source.get('excerpt') == title and source.get('filename') == filename)


def _separate_cover_proof(group, cover, evidence, title, source):
    """Verify title evidence and student identity in one same-group cover."""
    if not isinstance(cover, dict) or not isinstance(source, dict):
        return False
    identity_match = re.fullmatch(r'([\u4e00-\u9fff·]{2,5})_(\d{6,})', Path(group).name)
    if not identity_match or '封面' not in Path(cover.get('relative', '')).stem:
        return False
    text, _ = _normalized_with_offsets(_evidence_text(evidence))
    person, number = identity_match.groups()
    identity_ok = (source.get('reference') == '同组独立封面'
                   and source.get('cover_file_id') == cover.get('file_id')
                   and source.get('cover_sha256') == cover.get('sha256')
                   and person in text and re.search(r'(?<!\d)' + re.escape(number) + r'(?!\d)', text))
    if not identity_ok:
        return False
    proof = source.get('proof_kind')
    excerpt, reference = source.get('excerpt'), source.get('cover_reference')
    if proof == 'cover_complete':
        return _complete_cover_proof(evidence, title, excerpt, reference)
    if proof == 'cover_and_filename':
        return _filename_cover_proof(evidence, title, excerpt, reference)
    if proof == 'candidate':
        return _title_proof(evidence, title, excerpt, labelled=False, reference=reference)
    return proof is None and _title_proof(evidence, title, excerpt, labelled=True, reference=reference)


def _actual_suffix(row, evidence):
    detected = evidence.get('detected_format') or row.get('detected_format')
    return '.' + detected if isinstance(detected, str) and detected else Path(row['relative']).suffix.lower()


def _explicit_process_trash(row, evidence):
    # When no body was selected, only a narrowly evidenced process document
    # can be excluded. PDFs may be slide presentations, so they remain unknown.
    if _actual_suffix(row, evidence) not in {'.doc', '.docx', '.wps', '.rtf', '.odt'}:
        return False
    stem = re.sub(r'\s+', '', Path(row['relative']).stem)
    known = {'开题报告', '任务书', '中期检查表', '过程管理手册', '答辩记录表',
             '查重版论文', '查重稿论文', '查重检测报告'}
    text = _evidence_text(evidence)
    if evidence.get('truncated') is not False or evidence.get('has_unread_visual_content'):
        return False
    if re.search(r'(?:^|\n)\s*(?:摘\s*要|abstract|关键词|参考文献|第[一1]章|绪论)', text, re.I):
        return False
    return stem in known and stem in re.sub(r'\s+', '', text[:1500])


def _decision_basis(evidence, method, decision=None):
    basis = {'method': method, 'evidence': _preview(evidence, 3500)}
    if decision is not None:
        basis['decision'] = {key: decision[key] for key in
                             ('choice', 'model', 'confidence', 'probabilities') if key in decision}
    return basis


def classify_group(group, library, judge, evidence_reader, *, existing_paper_id=None, title_reader=None,
                   evidence_capture=None):
    """Classify only files beneath one explicit group root; do not move originals."""
    group, library = _group_paths(group, library)
    group_id = _group_id(group, library)
    rows, evidence, problems, decisions = [], {}, {}, {}
    for path in _files(group):
        relative = path.relative_to(group).as_posix()
        row = {'file_id': identity([group_id, relative.casefold()])[:24],
               'path': str(path), 'relative': relative, 'bytes': None, 'sha256': None}
        try:
            before = path.stat()
            row['bytes'] = before.st_size
            row['sha256'] = sha_file(path)
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise GroupClassificationError('文件读取期间发生变化')
            if not after.st_size:
                problems[row['file_id']] = '空文件'
            else:
                preview = evidence_reader(path)
                read_after = path.stat()
                post_hash = sha_file(path)
                hash_after = path.stat()
                if ((after.st_size, after.st_mtime_ns) != (read_after.st_size, read_after.st_mtime_ns)
                        or (read_after.st_size, read_after.st_mtime_ns) != (hash_after.st_size, hash_after.st_mtime_ns)
                        or post_hash != row['sha256']):
                    row['bytes'] = hash_after.st_size
                    row['sha256'] = post_hash if (read_after.st_size, read_after.st_mtime_ns) == (hash_after.st_size, hash_after.st_mtime_ns) else None
                    problems[row['file_id']] = '读取期间文件发生变化，未采用读取证据'
                elif preview.get('text_unavailable'):
                    problems[row['file_id']] = '文档没有可用文字，无法依据图片判断'
                else:
                    if preview.get('detected_format'):
                        row['detected_format'] = preview['detected_format']
                        row['format_detection'] = preview.get('format_detection')
                    evidence[row['file_id']] = preview
        except Exception as exc:
            # Even if extraction raised, its source may have changed while it
            # ran. Preserve a fresh stable identity or explicitly leave unknown.
            try:
                current = path.stat()
                current_sha = sha_file(path)
                final = path.stat()
                row['bytes'] = final.st_size
                row['sha256'] = current_sha if (current.st_size, current.st_mtime_ns) == (final.st_size, final.st_mtime_ns) else None
            except OSError:
                row['sha256'] = None
            problems[row['file_id']] = '读取失败：' + type(exc).__name__
        rows.append(row)

    usable = [dict(r, evidence=evidence[r['file_id']]) for r in rows if r['file_id'] in evidence]
    candidates = [r for r in usable if _actual_suffix(r, r['evidence']) in BODY_FORMATS]
    body_id = None
    if candidates:
        try:
            body_id = judge.choose_body(candidates)['choice']
            if body_id is not None and body_id not in {r['file_id'] for r in candidates}:
                raise GroupClassificationError('正文判断不属于当前组候选')
            if body_id is not None and (evidence[body_id].get('visual_heavy')
                                        or evidence[body_id].get('scan_heavy')):
                problems[body_id] = '图文较多且正文证据有限，尚不能确认'
                body_id = None
        except GroupClassificationError:
            raise
        except Exception as exc:
            for row in candidates:
                problems[row['file_id']] = '正文判断失败：' + type(exc).__name__

    body_sha = next((r['sha256'] for r in rows if r['file_id'] == body_id), None)
    body, attachments, trash, unresolved = None, [], [], []
    for row in rows:
        fid = row['file_id']
        actual_suffix = _actual_suffix(row, evidence.get(fid, {}))
        if fid in problems:
            unresolved.append(dict(row, reason=problems[fid]))
        elif fid == body_id:
            body = row
        elif actual_suffix in {'.ppt', '.pptx', '.odp'}:
            attachments.append(dict(row, reason='PPT 文件按附件保留'))
        elif body_id is not None and evidence[fid].get('visual_heavy'):
            attachments.append(dict(row, reason='图文证据有限，原文件按附件保留'))
        elif body_id is not None and evidence[fid].get('scan_heavy'):
            unresolved.append(dict(row, reason='扫描图片较多，文字不足以确认分类'))
        elif (actual_suffix in ARCHIVE_FORMATS
              and body_sha and evidence[fid].get('single_document_sha256') == body_sha):
            trash.append(dict(row, reason='包内仅有与本组正文完全相同的文件，原包保留',
                              decision_basis=_decision_basis(evidence[fid], 'single_document_sha256')))
        elif (actual_suffix in ARCHIVE_FORMATS
              and (evidence[fid].get('read_error') or evidence[fid].get('nested')
                   or evidence[fid].get('truncated') or len(evidence[fid].get('members', [])) > 25)):
            directory_status = evidence[fid].get('_handoff', {}).get('archive_listing', {}).get('status')
            reason = ('压缩包目录无法读取，按附件保留' if evidence[fid].get('read_error')
                      and directory_status not in {'complete', 'partial'}
                      else '压缩包内正文无法核验，按附件保留' if evidence[fid].get('read_error')
                      and directory_status == 'complete'
                      else '压缩包未完整检查，按附件保留')
            attachments.append(dict(row, reason=reason))
        elif body_id is None and _explicit_process_trash(row, evidence[fid]):
            trash.append(dict(row, reason='文件名与正文证据明确为过程材料',
                              decision_basis=_decision_basis(evidence[fid], 'filename_and_text')))
        elif body_id is None and actual_suffix in ARCHIVE_FORMATS:
            attachments.append(dict(row, reason='正文尚未确认，原压缩包按附件保留'))
        elif body_id is None:
            unresolved.append(dict(row, reason='正文尚未确认，文件分类未完成'))
        else:
            try:
                decision = judge.choose_other(dict(row, evidence=evidence[fid]), body_id)
                decisions[fid] = decision
                choice = decision['choice']
                if choice == 'attachment':
                    attachments.append(dict(row, reason='组内判断为附件'))
                elif choice == 'trash':
                    trash.append(dict(row, reason='组内判断为垃圾',
                                      decision_basis=_decision_basis(evidence[fid], 'local_model', decision)))
                elif choice == 'uncertain' and actual_suffix in ARCHIVE_FORMATS:
                    attachments.append(dict(row, reason='压缩包判断不清，按附件保留'))
                else:
                    unresolved.append(dict(row, reason='尚不能确认分类'))
            except Exception as exc:
                unresolved.append(dict(row, reason='分类失败：' + type(exc).__name__))
    title, title_source, title_candidate, candidate_source = None, None, None, None
    title_note = '未从正文中确认论文题目'
    if body and title_reader:
        try:
            candidate, reference, excerpt, labelled = title_reader(body, evidence[body['file_id']])
            if candidate and _title_proof(evidence[body['file_id']], candidate, excerpt,
                                          labelled=labelled, reference=reference):
                source = {'file_id': body['file_id'], 'source_sha256': body['sha256'],
                          'reference': reference, 'excerpt': excerpt}
                if labelled:
                    title, title_source, title_note = candidate.strip(), source, None
                else:
                    complete = _complete_cover_title(evidence[body['file_id']], candidate, excerpt, reference)
                    if complete:
                        title, source['excerpt'] = complete
                        source['proof_kind'] = 'cover_complete'
                        title_source, title_note = source, None
                    else:
                        title_candidate, candidate_source = candidate.strip(), source
                        title_note = '正文封面候选题目，需核对完整性和准确性'
        except Exception as exc:
            title_note = '论文题目读取失败：' + type(exc).__name__
    if body and title_reader:
        covers = [row for row in rows if row['file_id'] != body['file_id']
                  and '封面' in Path(row['relative']).stem]
        if len(covers) == 1 and covers[0]['file_id'] in evidence:
            cover = covers[0]
            try:
                candidate, reference, excerpt, labelled = title_reader(cover, evidence[cover['file_id']])
                source = {'file_id': body['file_id'], 'source_sha256': body['sha256'],
                          'reference': '同组独立封面', 'excerpt': excerpt,
                          'cover_file_id': cover['file_id'], 'cover_sha256': cover['sha256'],
                          'cover_reference': reference}
                if candidate:
                    complete = None if labelled else _complete_cover_title(
                        evidence[cover['file_id']], candidate, excerpt, reference)
                    if complete:
                        candidate, source['excerpt'] = complete
                        source['proof_kind'] = 'cover_complete'
                    elif not labelled:
                        source['proof_kind'] = 'candidate'
                    if _separate_cover_proof(group, cover, evidence[cover['file_id']], candidate, source):
                        if labelled or complete:
                            previous, _ = _normalized_with_offsets(title or '')
                            replacement, _ = _normalized_with_offsets(candidate)
                            if title:
                                if previous != replacement:
                                    title_note = '正文封面与同组独立封面题目不同，独立封面为：' + candidate.strip()
                            else:
                                title, title_source, title_note = candidate.strip(), source, '题目依据为同组独立封面'
                                title_candidate, candidate_source = None, None
                        elif not title:
                            title_candidate, candidate_source = candidate.strip(), source
                            title_note = '同组封面候选题目，需核对完整性和准确性'
            except Exception:
                pass  # A missing title must not block the body flow.
    filename_observation, filename_note = None, None
    if body and title_reader and callable(getattr(judge, 'choose_filename_title', None)):
        try:
            filename = Path(body['relative']).name
            decision = judge.choose_filename_title(filename)
            if decision and decision.get('title'):
                source = {'file_id': body['file_id'], 'source_sha256': body['sha256'],
                          'reference': '正文文件名（Nimble候选）', 'filename': filename,
                          'excerpt': decision['title'], 'decision': decision}
                if _filename_candidate_proof(body, decision['title'], source):
                    filename_observation = source
                    if not title:
                        cover_source = candidate_source
                        cover_evidence = evidence[body['file_id']]
                        cover_row = None
                        if cover_source and cover_source.get('reference') == '同组独立封面':
                            cover_row = next((row for row in rows
                                              if row['file_id'] == cover_source.get('cover_file_id')), None)
                            cover_evidence = evidence.get(cover_source.get('cover_file_id'))
                        proof_source = ({'reference': cover_source.get('cover_reference')}
                                        if cover_row and cover_source else cover_source)
                        matched = (_filename_cover_title(cover_evidence, decision['title'], title_candidate,
                                                         proof_source) if cover_evidence else None)
                        if matched:
                            matched_title, matched_excerpt, matched_reference = matched
                            if cover_row:
                                matched_source = dict(cover_source, excerpt=matched_excerpt,
                                                      cover_reference=matched_reference,
                                                      proof_kind='cover_and_filename')
                                accepted = _separate_cover_proof(group, cover_row, cover_evidence,
                                                                 matched_title, matched_source)
                            else:
                                matched_source = {'file_id': body['file_id'], 'source_sha256': body['sha256'],
                                                  'reference': matched_reference, 'excerpt': matched_excerpt,
                                                  'proof_kind': 'cover_and_filename'}
                                accepted = True
                            if accepted:
                                title, title_source, title_note = matched_title, matched_source, None
                                title_candidate, candidate_source = None, None
                    current = title or title_candidate
                    if current:
                        normalized_current, _ = _normalized_with_offsets(current)
                        normalized_filename, _ = _normalized_with_offsets(decision['title'])
                        filename_note = ('文件名候选与正文题目一致' if normalized_current == normalized_filename
                                         else '文件名候选与封面题目不一致，已保留差异')
                    else:
                        title_candidate, candidate_source = decision['title'], source
                        title_note = '正文文件名候选题目，需与正文核对'
        except Exception as exc:
            filename_note = '文件名题目判断失败：' + type(exc).__name__
    if not body and title_reader and callable(getattr(judge, 'choose_filename_title', None)):
        possible = [row for row in rows if _actual_suffix(row, evidence.get(row['file_id'], {})) in BODY_FORMATS]
        unresolved_ids = {row['file_id'] for row in unresolved}
        if len(possible) == 1 and possible[0]['file_id'] in unresolved_ids:
            row = possible[0]
            try:
                filename = Path(row['relative']).name
                decision = judge.choose_filename_title(filename)
                if decision and decision.get('title'):
                    source = {'file_id': row['file_id'], 'source_sha256': row['sha256'],
                              'reference': '正文文件名（Nimble候选）', 'filename': filename,
                              'excerpt': decision['title'], 'decision': decision}
                    if _filename_candidate_proof(row, decision['title'], source):
                        title_candidate, candidate_source = decision['title'], source
                        title_note = '正文尚未确认，题目仅来自文件名，需核对'
            except Exception as exc:
                filename_note = '文件名题目判断失败：' + type(exc).__name__
    handoff = {'schema': 'paperflow-group-handoff/2', 'group_id': group_id,
               'library_root': str(library), 'group_root': str(group),
               'group_relative': group.relative_to(library).as_posix(),
               'handoff_id': _snapshot_id(group_id, rows), 'paper_id': None,
               'existing_paper_id': existing_paper_id,
               'paper_id_note': '已有编号由接收窗口核对' if existing_paper_id else None,
               'body': body, 'paper_title': title, 'title_source': title_source,
               'paper_title_candidate': title_candidate, 'title_candidate_source': candidate_source,
               'title_status': '已提取' if title else '待核对' if title_candidate else '未提取',
               'title_note': title_note, 'attachments': attachments, 'trash': trash,
               'unresolved': unresolved, 'classification_complete': not bool(unresolved),
               'ready_for_receipt': not unresolved and (body is not None or not attachments),
               'attachment_status': ('有附件' if attachments else '尚未确认' if unresolved else '空白')}
    if filename_observation is not None:
        handoff['filename_title_observation'] = filename_observation
    if filename_note is not None:
        handoff['filename_title_note'] = filename_note
    handoff['result_version'] = _version(handoff)
    if evidence_capture is not None:
        evidence_capture.update(previews=evidence, decisions=decisions)
    return handoff


def verify_handoff(handoff, *, allow_incomplete=False, evidence_reader=None):
    """Return True only for complete, verified handoffs; incomplete records return False."""
    if handoff.get('schema') != 'paperflow-group-handoff/2':
        raise GroupClassificationError('不支持的交接清单版本')
    group, library = _group_paths(handoff['group_root'], handoff['library_root'])
    group_id = _group_id(group, library)
    if handoff['group_id'] != group_id or handoff['group_relative'] != group.relative_to(library).as_posix():
        raise GroupClassificationError('交接来源不一致')
    if handoff['paper_id'] is not None:
        raise GroupClassificationError('正式论文编号由接收窗口确认')
    if handoff['classification_complete'] != (not bool(handoff['unresolved'])):
        raise GroupClassificationError('分类完成情况与清单不一致')
    ready = not handoff['unresolved'] and (handoff['body'] is not None or not handoff['attachments'])
    if handoff['ready_for_receipt'] != ready:
        raise GroupClassificationError('接收条件与清单不一致')
    if not ready and not allow_incomplete:
        raise GroupClassificationError('本组仍有未确认文件，不能作为完整交接')
    rows = ([handoff['body']] if handoff['body'] else []) + handoff['attachments'] + handoff['trash'] + handoff['unresolved']
    unresolved_ids = {row['file_id'] for row in handoff['unresolved']}
    current = {path.relative_to(group).as_posix() for path in _files(group)}
    by_relative = {}
    for row in rows:
        relative = row['relative']
        relpath = Path(relative)
        if relpath.is_absolute() or '..' in relpath.parts or relative in by_relative:
            raise GroupClassificationError('交接清单含重复或越界路径')
        path = group / relpath
        if str(path) != row['path'] or row['file_id'] != identity([group_id, relative.casefold()])[:24]:
            raise GroupClassificationError('文件不属于当前资料组')
        if row['sha256'] is None:
            if not allow_incomplete or row['file_id'] not in unresolved_ids:
                raise GroupClassificationError('文件指纹缺失，不能作为完整交接')
        else:
            if not path.is_file() or path.stat().st_size != row['bytes']:
                raise GroupClassificationError('源文件不见或已变化')
            if sha_file(path) != row['sha256']:
                raise GroupClassificationError('源文件内容已变化')
        by_relative[relative] = row
    if set(by_relative) != current:
        raise GroupClassificationError('资料组文件清单已变化')
    ordered = [by_relative[r] for r in sorted(by_relative, key=str.casefold)]
    if handoff['handoff_id'] != _snapshot_id(group_id, ordered) or handoff['result_version'] != _version(handoff):
        raise GroupClassificationError('交接清单内容已变化')
    expected = '有附件' if handoff['attachments'] else '尚未确认' if handoff['unresolved'] else '空白'
    if handoff['attachment_status'] != expected:
        raise GroupClassificationError('附件空白状态与清单不一致')
    if handoff['paper_title'] is not None and handoff['paper_title_candidate'] is not None:
        raise GroupClassificationError('论文题目不能同时确认和待核对')
    if 'title_status' in handoff:
        expected_title_status = ('已提取' if handoff['paper_title'] else
                                 '待核对' if handoff['paper_title_candidate'] else '未提取')
        if handoff['title_status'] != expected_title_status:
            raise GroupClassificationError('论文题目状态与内容不一致')
    observation = handoff.get('filename_title_observation')
    if observation is not None:
        body = handoff['body'] or {}
        if (not isinstance(observation, dict) or observation.get('file_id') != body.get('file_id')
                or observation.get('source_sha256') != body.get('sha256')
                or not _filename_candidate_proof(body, observation.get('excerpt'), observation)):
            raise GroupClassificationError('文件名题目观察与原文件不一致')
    for name, source_name, labelled in [('paper_title', 'title_source', True),
                                        ('paper_title_candidate', 'title_candidate_source', False)]:
        value, source = handoff[name], handoff[source_name]
        if value is None:
            if source is not None: raise GroupClassificationError('未提供题目不能设置来源')
            continue
        source_row = handoff['body']
        if (source_row is None and not labelled and isinstance(source, dict)
                and source.get('reference') == '正文文件名（Nimble候选）'):
            candidates = [row for row in handoff['unresolved'] if row['file_id'] == source.get('file_id')]
            source_row = candidates[0] if len(candidates) == 1 else None
        if (not source_row or not isinstance(value, str) or not value.strip()
                or not isinstance(source, dict) or source.get('file_id') != source_row['file_id']
                or source.get('source_sha256') != source_row['sha256'] or not source.get('excerpt')):
            raise GroupClassificationError('论文题目没有关联本组正文')
        if source.get('reference') == '正文文件名（Nimble候选）':
            if labelled or not _filename_candidate_proof(source_row, value, source):
                raise GroupClassificationError('文件名候选题目与原文件不一致')
            continue
        if source.get('reference') == '同组独立封面':
            covers = [row for row in rows if row['file_id'] != handoff['body']['file_id']
                      and '封面' in Path(row['relative']).stem]
            if (len(covers) != 1 or (labelled and source.get('proof_kind') == 'candidate')
                    or (not labelled and source.get('proof_kind') != 'candidate')):
                raise GroupClassificationError('同组独立封面不唯一或不能确认')
            if evidence_reader is None:
                from .group_sort_cli import _read_for_sort
                evidence_reader = _read_for_sort
            try:
                cover_evidence = evidence_reader(Path(covers[0]['path']))
                if (not _separate_cover_proof(group, covers[0], cover_evidence, value, source)
                        or sha_file(covers[0]['path']) != covers[0]['sha256']
                        or sha_file(handoff['body']['path']) != handoff['body']['sha256']):
                    raise ValueError('separate cover mismatch')
                if source.get('proof_kind') == 'cover_and_filename':
                    expected, _ = _normalized_with_offsets((observation or {}).get('excerpt', ''))
                    actual, _ = _normalized_with_offsets(value)
                    if not expected or expected != actual:
                        raise ValueError('filename and cover mismatch')
            except Exception as exc:
                raise GroupClassificationError('同组独立封面来源无法重新核验') from exc
            continue
        if evidence_reader is None:
            from .group_sort_cli import _read_for_sort
            evidence_reader = _read_for_sort
        try:
            evidence = evidence_reader(Path(handoff['body']['path']))
        except Exception as exc:
            raise GroupClassificationError('正文题目来源无法重新核验') from exc
        proof = source.get('proof_kind')
        if labelled and proof == 'cover_complete':
            valid = _complete_cover_proof(evidence, value, source['excerpt'], source.get('reference'))
        elif labelled and proof == 'cover_and_filename':
            expected, _ = _normalized_with_offsets((observation or {}).get('excerpt', ''))
            actual, _ = _normalized_with_offsets(value)
            valid = (bool(expected) and expected == actual and
                     _filename_cover_proof(evidence, value, source['excerpt'], source.get('reference')))
        elif proof is None:
            valid = _title_proof(evidence, value, source['excerpt'], labelled=labelled,
                                 reference=source.get('reference'))
        else:
            valid = False
        if not valid:
            raise GroupClassificationError('论文题目与正文原文依据不一致')
    return bool(ready)


def _write_record(destination, handoff, *, source_library=None):
    destination = Path(destination).resolve()
    library = Path(source_library if source_library is not None else handoff['library_root']).resolve()
    if destination == library or destination.is_relative_to(library):
        raise GroupClassificationError('交接记录不能写入原始资料库')
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if read_json(destination) == handoff: return destination
        raise GroupClassificationError('已有不同交接记录，不能覆盖')
    temp = destination.with_name('.' + destination.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temp.open('xb') as stream:
            stream.write(json.dumps(handoff, ensure_ascii=False, indent=2).encode('utf8'))
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temp, destination)
        except FileExistsError:
            if read_json(destination) != handoff:
                raise GroupClassificationError('已有不同交接记录，不能覆盖')
        if read_json(destination) != handoff:
            raise GroupClassificationError('交接记录读回不一致')
        return destination
    finally:
        temp.unlink(missing_ok=True)


def write_handoff(destination, handoff, *, evidence_reader=None):
    """Create one immutable, complete handoff outside the source library."""
    verify_handoff(handoff, evidence_reader=evidence_reader)
    return _write_record(destination, handoff)


def write_unreceived(destination, handoff, *, evidence_reader=None):
    """Preserve a partial result while explicitly refusing complete receipt."""
    if verify_handoff(handoff, allow_incomplete=True, evidence_reader=evidence_reader):
        raise GroupClassificationError('完整交接不能写入未接收目录')
    return _write_record(destination, handoff)


def _classification_snapshot(group, library):
    group_id = _group_id(group, library)
    rows = []
    for path in _files(group):
        relative = path.relative_to(group).as_posix()
        row = {'relative': relative, 'bytes': None, 'sha256': None}
        try:
            before = path.stat()
            row['bytes'] = before.st_size
            row['sha256'] = sha_file(path)
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise GroupClassificationError('资料组快照读取期间发生变化')
        except OSError:
            pass  # Unknown files are represented, never silently declared empty.
        rows.append(row)
    return group_id, _snapshot_id(group_id, rows)


def _saved_complete(output_dir, group, library, group_id, snapshot_id, known_id):
    paths = list(output_dir.glob(group_id + '-' + snapshot_id + '-*.json'))
    if len(paths) > 1:
        raise GroupClassificationError('历史清单存在多个版本，未重新判断')
    if not paths:
        return None
    handoff = read_json(paths[0])
    if (handoff.get('schema') != 'paperflow-group-handoff/2' or handoff.get('group_id') != group_id
            or handoff.get('handoff_id') != snapshot_id or not handoff.get('ready_for_receipt')
            or handoff.get('result_version') != _version(handoff)
            or paths[0].name != group_id + '-' + snapshot_id + '-' + handoff['result_version'] + '.json'
            or (known_id is not None and handoff.get('existing_paper_id') is not None
                and handoff.get('existing_paper_id') != known_id)):
        raise GroupClassificationError('历史清单身份不一致，未重新判断')
    try:
        if (handoff['group_root'] != str(group) or handoff['library_root'] != str(library)
                or handoff['group_relative'] != group.relative_to(library).as_posix()):
            raise ValueError('group mismatch')
        rows = ([handoff['body']] if handoff['body'] is not None else [])
        for key in ('attachments', 'trash', 'unresolved'):
            if not isinstance(handoff[key], list): raise ValueError('invalid rows')
            rows.extend(handoff[key])
        if handoff['unresolved'] or len({r['relative'].casefold() for r in rows}) != len(rows):
            raise ValueError('invalid rows')
        ready = not handoff['unresolved'] and (handoff['body'] is not None or not handoff['attachments'])
        expected_status = '有附件' if handoff['attachments'] else '尚未确认' if handoff['unresolved'] else '空白'
        if (handoff['classification_complete'] != (not bool(handoff['unresolved']))
                or handoff['ready_for_receipt'] != ready or handoff['attachment_status'] != expected_status):
            raise ValueError('invalid classification status')
        for row in rows:
            relative = row['relative']
            if (Path(relative).is_absolute() or '..' in Path(relative).parts
                    or row['path'] != str(group / Path(relative))
                    or row['file_id'] != identity([group_id, relative.casefold()])[:24]):
                raise ValueError('invalid row')
        if _snapshot_id(group_id, sorted(rows, key=lambda row: row['relative'].casefold())) != snapshot_id:
            raise ValueError('snapshot mismatch')
    except (KeyError, TypeError, ValueError) as exc:
        raise GroupClassificationError('历史清单项目与来源快照不一致，未重新判断') from exc
    return paths[0]


def _reserve_classification(output_dir, group, library, group_id, snapshot_id, *, verified_new):
    root = output_dir / 'classification-intents'
    # Any previous intent without a matching complete handoff may have sent a
    # request. It must be reconciled, including after a source revision.
    if root.is_dir():
        for prior in root.glob(group_id + '-*.json'):
            prior_snapshot = prior.stem[len(group_id) + 1:]
            if not _saved_complete(output_dir, group, library, group_id, prior_snapshot, None):
                raise GroupClassificationError('存在结果未确认的历史分类请求，未重发')
    if not verified_new:
        raise GroupClassificationError('历史状态未核实为新资料，未启动分类')
    root.mkdir(parents=True, exist_ok=True)
    intent = root / (group_id + '-' + snapshot_id + '.json')
    try:
        with intent.open('x', encoding='utf8') as stream:
            json.dump({'schema': 'group-classification-intent/1', 'group_root': str(group),
                       'group_id': group_id, 'handoff_id': snapshot_id,
                       'status': 'started_or_unknown'}, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        raise GroupClassificationError('存在结果未确认的分类请求，未重发') from None


def _saved_attachment_evidence(output_dir, handoff):
    from .attachment_evidence_handoff import validate_evidence_handoff
    found, errors, histories = [], [], {}
    for row in handoff['attachments']:
        source = [handoff['handoff_id'], handoff['result_version'], row['file_id'], row['sha256'].lower()]
        key = identity([handoff['group_id']] + source)[:32]
        root = output_dir / 'attachment-evidence' / key
        paths = sorted((p for p in root.glob('*.json') if p.stem.isdecimal()), key=lambda p: int(p.stem))
        try:
            if not paths:
                raise ValueError('历史证据未定位')
            if [int(p.stem) for p in paths] != list(range(1, len(paths) + 1)):
                raise ValueError('历史证据版本链不完整')
            for version, path in enumerate(paths, 1):
                record = read_json(path)
                if record['evidence_version'] != version:
                    raise ValueError('历史证据版本不一致')
                validate_evidence_handoff(record, handoff, verify_files=True)
            found.append(str(paths[-1]))
            histories[row['file_id']] = [str(path) for path in paths]
        except Exception as exc:
            errors.append({'file_id': row['file_id'], 'error_type': type(exc).__name__})
    return found, errors, histories


def classify_batch(groups, library, output_dir, judge_factory, evidence_reader, *, title_reader=None,
                   known_paper_ids=None, verified_new_groups=None, task_id=None):
    """Handle explicitly selected, non-overlapping groups with four workers."""
    unique_groups, seen_groups = [], set()
    for path in groups:
        try:
            key = os.path.normcase(str(Path(path).resolve()))
        except (OSError, ValueError, RuntimeError):
            key = os.path.normcase(str(path))
        if key not in seen_groups:
            seen_groups.add(key)
            unique_groups.append(path)
    groups = unique_groups
    if len(groups) > MAX_GROUPS:
        raise GroupClassificationError('单次任务最多处理 30000 个资料组')
    library = Path(library).resolve()
    output_dir = Path(output_dir).resolve()
    if output_dir == library or output_dir.is_relative_to(library):
        raise GroupClassificationError('交接记录不能写入原始资料库')
    known = {}
    verified_new = {Path(path).resolve() for path in (verified_new_groups or [])}
    for raw_path, paper_id in (known_paper_ids or {}).items():
        if not isinstance(raw_path, str) or not isinstance(paper_id, str) or not paper_id.strip():
            raise GroupClassificationError('已有论文编号映射格式不正确')
        path = Path(raw_path).resolve()
        if path in known and known[path] != paper_id:
            raise GroupClassificationError('同一资料组存在不同历史编号')
        known[path] = paper_id
    from .group_task_summary import GroupTaskSummary
    task_summary = GroupTaskSummary(output_dir, library, len(groups), task_id=task_id)
    results = [{'group_root': str(path), 'task_id': task_summary.task_id} for path in groups]
    resolved = {}
    for index, path in enumerate(groups):
        try:
            group, _ = _group_paths(path, library)
            resolved.setdefault(group, []).append(index)
        except Exception as exc:
            detail = str(exc) if isinstance(exc, GroupClassificationError) else type(exc).__name__
            results[index]['error'] = '资料组无法核对：' + detail[:200]
    overlapping = set()
    for path, indices in resolved.items():
        if len(indices) > 1:
            overlapping.update(indices)
        for parent in path.parents:
            if parent in resolved:
                overlapping.update(indices)
                overlapping.update(resolved[parent])
            if parent == library:
                break
    for index in overlapping:
        results[index]['error'] = '资料组重复或互相包含，不能确认归属'
    by_paper_id = {}
    for path, indices in resolved.items():
        if path in known:
            by_paper_id.setdefault(known[path], []).append((path, indices))
    for entries in by_paper_id.values():
        if len(entries) > 1:
            for _, indices in entries:
                for index in indices:
                    results[index]['error'] = '不同资料组提供了相同历史编号，需要核对归属'
    for index, item in enumerate(results):
        if 'error' in item:
            task_summary.record(index, item)
    def process_group(entry):
        from time import time
        from .group_trace import emit
        path, indices = entry
        for index in indices:
            if 'error' in results[index]:
                continue
            results[index]['started_at'] = time()
            emit('group_start', path)
            lock = None
            try:
                from filelock import FileLock, Timeout
                lock_root = Path(tempfile.gettempdir()) / 'paperflow-group-classification-locks'
                lock_root.mkdir(parents=True, exist_ok=True)
                lock = FileLock(str(lock_root / (_group_lock_key(path) + '.lock')))
                try:
                    lock.acquire(timeout=0)
                except Timeout:
                    lock = None
                    raise GroupClassificationError('同一资料组正在分类，未重复发送') from None
                group_id, snapshot_id = _classification_snapshot(path, library)
                existing = _saved_complete(output_dir, path, library, group_id, snapshot_id, known.get(path))
                if existing is not None:
                    saved = read_json(existing)
                    found, errors, histories = _saved_attachment_evidence(output_dir, saved)
                    results[index].update(handoff=str(existing), reused=True, evidence_handoffs=found,
                                          evidence_errors=errors, evidence_history=histories)
                    from .group_delivery import package_result
                    package_result(results[index], output_dir)
                    continue
                _reserve_classification(output_dir, path, library, group_id, snapshot_id,
                                        verified_new=path in verified_new)
                capture = {}
                judge = judge_factory()
                if hasattr(judge, '_group_trace'):
                    judge._group_trace = str(path)
                handoff = classify_group(path, library, judge, evidence_reader,
                                         existing_paper_id=known.get(path), title_reader=title_reader,
                                         evidence_capture=capture)
                if (handoff['group_id'] != group_id
                        or (handoff['handoff_id'] != snapshot_id and handoff['ready_for_receipt'])):
                    raise GroupClassificationError('分类期间资料组发生变化，请核对原请求')
                filename = handoff['group_id'] + '-' + handoff['handoff_id'] + '-' + handoff['result_version'] + '.json'
                if not handoff['ready_for_receipt']:
                    destination = write_unreceived(output_dir / 'unreceived' / filename, handoff,
                                                   evidence_reader=evidence_reader)
                    results[index]['unreceived'] = str(destination)
                    results[index]['reason'] = ('本组仍有未确认文件，未作为完整交接' if handoff['unresolved']
                                                else '附件尚未确认所属论文，未作为完整交接')
                else:
                    destination = write_handoff(output_dir / filename, handoff, evidence_reader=evidence_reader)
                    results[index]['handoff'] = str(destination)
                # Sidecars are independent per confirmed attachment. A failed
                # evidence write never removes the saved handoff or other items.
                from .attachment_evidence_handoff import make_evidence_handoff, append_evidence_handoff
                results[index]['evidence_handoffs'] = []
                results[index]['evidence_errors'] = []
                for row in handoff['attachments']:
                    try:
                        origin = {'kind': 'current_read', 'record_ref': str(destination),
                                  'run_ref': None, 'entry_ref': row['file_id'], 'record_sha256': None}
                        record = make_evidence_handoff(handoff, row,
                            preview=capture['previews'].get(row['file_id']),
                            decision=capture['decisions'].get(row['file_id']), provenance=origin)
                        evidence_path = append_evidence_handoff(record, output_dir / 'attachment-evidence',
                                                               handoff=handoff)
                        results[index]['evidence_handoffs'].append(str(evidence_path))
                    except Exception as exc:
                        results[index]['evidence_errors'].append({'file_id': row['file_id'],
                                                                   'error_type': type(exc).__name__})
                from .group_delivery import package_result
                package_result(results[index], output_dir)
            except Exception as exc:
                saved = results[index].get('handoff') or results[index].get('unreceived')
                if saved:
                    results[index]['summary_handoff'] = saved
                results[index].pop('handoff', None)
                results[index].pop('unreceived', None)
                detail = str(exc) if isinstance(exc, GroupClassificationError) else type(exc).__name__
                results[index]['error'] = '分类或交接核验失败：' + detail[:200]
            finally:
                if lock is not None:
                    lock.release()
                results[index]['finished_at'] = time()
                emit('group_finish', path, status=('error' if 'error' in results[index] else
                     'unreceived' if 'unreceived' in results[index] else 'complete'))
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix='paperflow-group') as executor:
            entries = list(resolved.items())
            for offset in range(0, len(entries), GROUP_BATCH_SIZE):
                current = entries[offset:offset + GROUP_BATCH_SIZE]
                futures = {executor.submit(process_group, entry): entry for entry in current}
                for future in as_completed(futures):
                    _, indices = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        for index in indices:
                            results[index].setdefault('error', '分类任务失败：' + type(exc).__name__)
                    for index in indices:
                        task_summary.record(index, results[index])
                task_summary.flush(force=True)
    finally:
        task_summary.finish()
    return results
