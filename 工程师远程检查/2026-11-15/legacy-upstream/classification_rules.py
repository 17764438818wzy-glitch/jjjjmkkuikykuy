"""Deterministic source-major evidence in front of the unchanged legacy catalog.

Topic/technology inference remains the legacy fallback. Explicit selections,
cover fields and exact source directories cannot be replaced by that fallback.
"""
import copy
import io
import re
import sys
import unicodedata
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from engine import LEGACY_ROOT, load_legacy_taxonomy
from archive_identity import member_path

RULES_VERSION = 'source-major-evidence/2'
MANUAL_SOURCES = {'manual', 'manual_selection', 'manual_override', 'user', 'human', 'human_verified'}
MAJOR_FIELD = re.compile(r'(?<![\u4e00-\u9fff])专[ \t]*业(?:名[ \t]*称)?[ \t]*[:：]?[ \t]*')
NEXT_FIELD = re.compile(r'(?:姓\s*名|学\s*号|年\s*级|班\s*级|学\s*院|系\s*别|指导教师|完成日期)[ \t]*[:：]')
BODY_START = re.compile(r'^(?:摘要|abstract|目录|第[一1]章|绪论)', re.I)


def _compact(value):
    return re.sub(r'\s+', '', unicodedata.normalize('NFKC', str(value))).strip(':：|_')


def _discipline_quote(value):
    # Parenthetical source annotations are not validated academic evidence and
    # can contain identities. Preserve only the exact discipline excerpt.
    return re.sub(r'[（(][^）)]*[）)]\s*$', '', str(value)).strip()


def _legacy():
    if str(LEGACY_ROOT) not in sys.path:
        sys.path.insert(0, str(LEGACY_ROOT))
    from src.paper_cleaning import subject_taxonomy
    return subject_taxonomy


def _major(value, directions):
    """Match a declared field/whole folder, never a keyword in a topic/title."""
    value = _compact(value)
    value = re.sub(r'[（(][^）)]*[）)]$', '', value).removesuffix('专业')
    aliases = {**_legacy().MAJOR_ALIASES, '法律': '法学', '法律学': '法学', '思政': '思想政治教育'}
    value = aliases.get(value, value)
    return value if value in directions else None


def _native_cover_lines(source):
    """Read cover fields omitted by lightweight direct-body extraction."""
    if source is None or Path(source).suffix.lower() != '.docx':
        return []
    from native_docx import W, select_compatibility
    with zipfile.ZipFile(source) as archive:
        xml = archive.read('word/document.xml')
    namespaces = dict(value for _, value in ET.iterparse(io.BytesIO(xml), events=('start-ns',)))
    document = ET.fromstring(xml)
    select_compatibility(document, namespaces)
    body = document.find(W+'body')
    if body is None:
        return []
    # Descendant paragraphs include original text boxes/content controls. The
    # same compatibility selection as native parsing avoids duplicate branches.
    return [''.join(t.text or '' for t in paragraph.iter(W+'t'))
            for paragraph in body.iter(W+'p')][:120]


def _cover_evidence(lines, directions, origin, directory_major):
    evidence = []
    for index, line in enumerate(lines):
        if BODY_START.match(_compact(line).lstrip('#')):
            break
        for match in MAJOR_FIELD.finditer(line):
            value = NEXT_FIELD.split(line[match.end():], maxsplit=1)[0].strip(' \t:：|')
            value_index = index
            if not value:
                for after in range(index+1, min(index+5, len(lines))):
                    candidate = lines[after].strip(' \t:：|')
                    if not candidate:
                        continue
                    if NEXT_FIELD.match(candidate):
                        break
                    value = candidate; value_index = after
                    break
            major = _major(value, directions)
            normalization = None
            # Observed source typo is only normalized with independent exact
            # directory agreement, not a general edit-distance or topic guess.
            if _compact(value) == '思想政治教教育' and directory_major == '思想政治教育':
                major = directory_major; normalization = 'original_repeated_教_with_directory_agreement'
            if major:
                evidence.append({'kind':'cover_major', 'origin':origin, 'line':index+1,
                                 'value_line':value_index+1, 'quote':_discipline_quote(value), 'major':major,
                                 **({'normalization':normalization} if normalization else {})})
    return evidence


def source_context(record, source_path):
    """Preserve original archive-parent folders when an extracted copy is used."""
    origin = str(record.get('input_source_path') or source_path).replace('\\', '/')
    member = record.get('archive_member_path') or member_path(record)
    return origin.rsplit('/', 1)[0]+'/'+str(member).replace('\\', '/') if member else str(source_path)


def _directory_evidence(source_path, directions):
    folders = re.split(r'[/\\]', str(source_path))[:-1]
    exact = [(folder, _major(folder, directions)) for folder in folders]
    exact = [(folder, major) for folder, major in exact if major]
    evidence = []
    if exact:
        folder, major = exact[-1]  # Most specific folder below broader disciplines.
        evidence.append({'kind':'source_major_directory','quote':_discipline_quote(folder),'major':major})
    for folder in folders:
        cohort = re.fullmatch(r'\d{2,4}(?:级|届)?(.+?)(?:\d+)?班', _compact(folder))
        if cohort:
            major = _major(cohort[1], directions)
            if major:
                evidence.append({'kind':'source_cohort_directory','quote':folder,'major':major})
    return evidence


def resolve_identity(raw, source_path, title, *, native_source=None, existing=None):
    taxonomy = load_legacy_taxonomy()
    directions = {major:group for group, majors in taxonomy.items() for major in majors}
    selected = (existing or {}).get('classification') or {}
    manual = selected.get('method') in MANUAL_SOURCES or selected.get('source') in MANUAL_SOURCES
    if manual:
        major = selected.get('major') or (existing or {}).get('major')
        classification = copy.deepcopy(selected)
        classification.setdefault('method', 'manual')
        classification.setdefault('source', 'manual')
        classification.setdefault('confidence', 'high')
        classification.update(major=major, title=title, rules_version=RULES_VERSION, locked=True)
        return {'major':major, 'category':directions.get(major, '未识别'), 'classification':classification,
                'conflict':major not in directions}

    evidence = _directory_evidence(source_path, directions)
    directory_majors = {entry['major'] for entry in evidence}
    directory_major = next(iter(directory_majors)) if len(directory_majors) == 1 else None
    native_lines = _native_cover_lines(native_source)
    if native_lines:
        evidence += _cover_evidence(native_lines, directions, 'native_docx_cover', directory_major)
    evidence += _cover_evidence(str(raw)[:3000].splitlines(), directions, 'original_front_matter', directory_major)
    candidates = {entry['major'] for entry in evidence}
    cover = [entry for entry in evidence if entry['kind'] == 'cover_major']
    if candidates:
        major = cover[0]['major'] if cover else evidence[0]['major']
        conflict = len(candidates) > 1
        source = 'source_conflict' if conflict else 'front_matter' if cover else 'source_directory'
        confidence = 'conflict' if conflict else 'high' if cover else 'medium'
        return {'major':major, 'category':directions[major], 'conflict':conflict,
                'classification':{'method':'original_rules','source':source,'confidence':confidence,
                                  'major':major,'title':title,'rules_version':RULES_VERSION,
                                  'locked':not conflict,'evidence':evidence}}

    # Source folders were matched as complete declared majors above. Feeding
    # the whole path into the legacy substring fallback can turn an institute
    # name (e.g. 自动化学院) into an unrelated major (化学).
    hint = _legacy().classify_subject(raw, '', title)
    major = hint.detected_major_or_direction or '未识别专业'
    return {'major':major, 'category':hint.detected_subject_group or '未识别', 'conflict':False,
            'classification':{'method':'original_rules','source':hint.subject_detect_source,
                              'confidence':hint.subject_confidence,'major':major,'title':title,
                              'rules_version':RULES_VERSION,'locked':False}}


def apply_identity(item, resolved):
    """Keep top-level identity and its provenance together for both run paths."""
    for key in ('major','category','classification'):
        item[key] = copy.deepcopy(resolved[key])
    if resolved['conflict']:
        from auto_evidence import Rejected
        raise Rejected('原文专业与来源目录冲突，或手动专业不在现有词表内；需核对专业，不能按题目关键词改专业。','classification')
