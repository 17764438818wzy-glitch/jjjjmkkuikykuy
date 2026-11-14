"""Deterministic, minimal original-file identity evidence, before cover cleaning.

This metadata has no authority over quality or business applicability. Unknown
and conflicting source labels are valid results; no filename/topic inference.
"""
import copy
import hashlib
import io
import re
import unicodedata
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from knowledge_package import encoded
from native_docx import W, select_compatibility

SCHEMA_VERSION = 'source-education/1.0'
EXTRACTION_RULE_VERSION = 'source-education-extract/1.0'
BUSINESS_RULE_VERSION = 'adult-to-correspondence/1.0'
LABELS = {
    'education_level': {'associate': ('专科','大专'), 'bachelor': ('本科','大学本科'),
                        'master': ('硕士','硕士研究生'), 'doctor': ('博士','博士研究生')},
    'study_mode': {'correspondence': ('函授',), 'adult_education': ('成人','成人教育','成人高等教育'),
                   'full_time': ('全日制',), 'part_time': ('非全日制',), 'amateur': ('业余',),
                   'off_job': ('脱产',), 'self_study': ('自考','自学考试'),
                   'open_education': ('开放教育',), 'online_education': ('网络教育','远程教育')},
}
FIELDS = {'education_level': r'(?:学历(?:层次)?|培养层次|教育层次|学位)',
          'study_mode': r'(?:学习形式|学习方式|教育形式|办学形式)'}
BODY_START = re.compile(r'^(?:摘要|内容摘要|中文摘要|英文摘要|abstract|目录|绪论|引言|前言|参考文献|第[一二三四五六七八九十\d]+章|[1一][、.．\s]+)', re.I)
EXCLUDED = re.compile(r'指导(?:教师|老师)|导师|研究对象|调查对象|调查参与|参与者|受访|被试|参考文献|问卷|教师学历|学生学历|父亲|母亲|父母|亲属|监护人|家庭成员|本文|本研究')
# Adjacent cover fields need not have punctuation (including OCR/table joins).
NEXT_FIELD = re.compile(r'(?:姓名|学号|学生|作者|指导教师|指导老师|导师|专业|班级|年级|学院|学校|日期|研究对象)\s*[:：]?')
AUTHOR = re.compile(r'^(?:论文作者|作者|本人|学生)(?:姓名|信息)?(?:[:：]|$)')
THESIS = re.compile(r'(?:(?:非全日制|全日制|成人高等教育|成人教育|成人|函授|业余|脱产|自学考试|自考|开放教育|网络教育|远程教育|大学本科|本科|专科|大专|硕士研究生|硕士|博士研究生|博士))+(?:生)?(?:毕业(?:论文|设计)|学位论文)')


def _sha(source):
    return hashlib.sha256(Path(source).read_bytes()).hexdigest()


def source_education_revision(payload):
    """Canonical JSON shared with the frozen receiver, rejecting unsafe numbers."""
    def strict(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if not isinstance(key, str): raise TypeError('source education keys must be strings')
                key.encode('utf-8'); strict(value)
        elif isinstance(node, list):
            for value in node: strict(value)
        elif isinstance(node, str): node.encode('utf-8')
        elif type(node) is int:
            if abs(node) > 9007199254740991: raise ValueError('unsafe source education integer')
        elif node is not None:
            raise TypeError('source education accepts only canonical JSON strings, integers and null')
    strict(payload)
    return hashlib.sha256(encoded(payload)).hexdigest()


def source_education_fields(payload):
    return {'source_education': copy.deepcopy(payload), 'source_education_revision': source_education_revision(payload)}


def _compact(text):
    return re.sub(r'\s+', '', unicodedata.normalize('NFKC', text))


def _compact_offsets(text):
    value=[];offsets=[]
    for index,char in enumerate(text):
        for normalized in unicodedata.normalize('NFKC',char):
            if not normalized.isspace(): value.append(normalized);offsets.append(index)
    return ''.join(value),offsets


def _cover_title(compact):
    match=THESIS.search(compact)
    if match is None: return None
    prefix=compact[:match.start()];suffix=compact[match.end():]
    # A keyword embedded in a narrative sentence is not a cover designation.
    if prefix and not (prefix.endswith(('大学','学院','学校'))
                       or prefix in {'高等学历继续教育','普通高等教育','继续教育','高等教育'}
                       or re.match(r'^(?:姓名|学号|作者|学生)[:：]',prefix)):
        return None
    if suffix and not (NEXT_FIELD.match(suffix)
                       or re.fullmatch(r'[\s（(）):：\-—_·]*(?:设计)?[\s（(）):：\-—_·]*(?:(?:独创性|原创性|诚信|学术诚信)声明(?:书)?)?',suffix)):
        return None
    return match


def _docx_rows(source):
    with zipfile.ZipFile(source) as archive: xml = archive.read('word/document.xml')
    root = ET.fromstring(xml)
    # Index all original paragraphs BEFORE compatibility selection mutates XML.
    indices = {node: index for index, node in enumerate(root.iter(W+'p'), 1)}
    namespaces = dict(value for _, value in ET.iterparse(io.BytesIO(xml), events=('start-ns',)))
    select_compatibility(root, namespaces)
    parents = {child: parent for parent in root.iter() for child in parent}
    def owner(node, tag):
        while node in parents:
            node = parents[node]
            if node.tag == tag: return node
        return None
    body = root.find(W+'body')
    if body is None: return
    for paragraph in body.iter(W+'p'):
        # Text boxes own their descendant paragraphs; don't double quote them
        # as part of the surrounding anchor paragraph.
        text = ''.join(node.text or '' for node in paragraph.iter(W+'t') if owner(node, W+'p') is paragraph)
        table_row = owner(paragraph, W+'tr')
        group = table_row if table_row is not None else body
        yield text, {'kind':'docx_paragraph', 'part':'word/document.xml',
                     'paragraph_index':indices[paragraph]}, group


def _rows(source):
    suffix = source.suffix.lower()
    if suffix == '.docx': yield from _docx_rows(source)
    elif suffix == '.pdf':
        import fitz
        with fitz.open(source) as document:
            for page in document:
                for text in page.get_text(sort=True).splitlines():
                    yield text, {'kind':'pdf_page', 'page':page.number+1}, page.number
    else:
        text = source.read_text(encoding='utf-8-sig')
        for index, line in enumerate(text.splitlines(), 1):
            yield line, {'kind':'text_lines', 'start_line':index, 'end_line':index}, 'text'


def _matches(text, name):
    # Longest labels win, so 非全日制 cannot also become 全日制. Preserve
    # the exact original spaced label, using NFKC only to recognize its value.
    alternatives = sorted(((label, value) for value, labels in LABELS[name].items()
                           for label in labels), key=lambda pair: -len(pair[0]))
    pattern = re.compile('|'.join(r'\s*'.join(re.escape(char) for char in label) for label, _ in alternatives))
    # All frozen labels are Chinese; normalization outside their original span
    # must not change source offsets. Match original text with whitespace only.
    for match in pattern.finditer(text):
        label = match.group(); compact = _compact(label)
        value = next(value for candidate, value in alternatives if candidate == compact)
        yield value, label, match.start(), match.end()


def _field(candidate_map):
    candidates = list(candidate_map.values())
    unknown = {'value':'unknown', 'status':'unknown', 'raw_labels':[], 'evidence_ids':[], 'candidates':[]}
    if not candidates: return unknown
    if len(candidates) == 1: return {**candidates[0], 'status':'source_explicit', 'candidates':[]}
    return {'value':'unknown', 'status':'conflict',
            'raw_labels':list(dict.fromkeys(label for c in candidates for label in c['raw_labels'])),
            'evidence_ids':list(dict.fromkeys(ref for c in candidates for ref in c['evidence_ids'])),
            'candidates':candidates}


def extract_source_education(source, *, original_source=None, source_relation='original',
                             derivation_method=None, derivation_record_id=None):
    source = Path(source); original = Path(original_source) if original_source is not None else source
    suffix = source.suffix.lower().lstrip('.')
    if suffix not in {'pdf','docx','txt','md'}: raise ValueError('unsupported source education evidence format')
    original_hash = _sha(original); evidence_hash = _sha(source)
    if source_relation == 'original':
        if original_hash != evidence_hash or derivation_method or derivation_record_id:
            raise ValueError('original source education identity does not match evidence')
        derivation = None
    elif source_relation in {'converted','normalized'}:
        if not original_source or not derivation_method or not derivation_record_id:
            raise ValueError('derived source education requires original file and local derivation record')
        if len(derivation_method)>120 or len(derivation_record_id)>160:
            raise ValueError('source education derivation record too long')
        derivation = {'input_sha256':original_hash, 'output_sha256':evidence_hash,
                      'method':derivation_method, 'record_id':derivation_record_id}
    else: raise ValueError('unsupported source education relation')
    maps = {name:{} for name in LABELS}; evidence = []
    pending = None;excluded_role=False
    for index, (line, locator, group) in enumerate(_rows(source)):
        # Only front matter, before any body boundary; no topic/participant
        # inference even if a source forgot to mark the beginning of its body.
        if index >= 120: break
        stripped = line.strip().lstrip('#').strip()
        normalized=unicodedata.normalize('NFKC',stripped)
        # Check token boundaries before compacting: "1 引言" must not become
        # "1引言" and disappear from the supported body-start grammar.
        if BODY_START.match(normalized) or BODY_START.match(_compact(stripped)): break
        if not stripped: continue
        compact,offsets=_compact_offsets(stripped)
        title = _cover_title(compact)
        author_declaration=title and re.search(r'(?:独创性|原创性|诚信|学术诚信)声明(?:书)?$',compact)
        # A thesis-shaped string in somebody else's section is not an author
        # reset. Only an explicit author marker or formal author declaration is.
        if AUTHOR.match(compact) or author_declaration: excluded_role=False
        if excluded_role:
            pending=None;continue
        exclusion=EXCLUDED.search(compact)
        if exclusion:
            # An advisor in a later field must not suppress a valid title before
            # it, nor allow their degree to enter the paper identity.
            excluded_role=True
            stripped = stripped[:offsets[exclusion.start()]].strip()
            compact,offsets=_compact_offsets(stripped)
            title = _cover_title(compact)
        if not stripped:
            pending = None; continue
        previous_pending = pending
        pending = None
        entries = []
        for name, field_pattern in FIELDS.items():
            # A standalone labelled table cell is an explicit identity field
            # even without punctuation. Its value must stay in the same row.
            if re.fullmatch(field_pattern+r'[:：]?',compact):
                pending=(name,group);continue
            field = re.search(field_pattern+r'\s*[:：]\s*', stripped)
            if field:
                prefix=_compact(stripped[:field.start()])
                structured_prefix=re.match(r'^(?:'+ '|'.join(FIELDS.values()) +r'|姓名|学号|论文作者|作者|学生|本人)[:：]',prefix)
                if prefix and not structured_prefix: continue
                tail = NEXT_FIELD.split(stripped[field.end():], maxsplit=1)[0].strip()
                if not tail:
                    pending = (name, group); continue
                # Only the labelled value, not later identity fields.
                tail = re.split(r'(?:学历(?:层次)?|学位|学习形式|学习方式|教育形式|办学形式)\s*[:：]', tail, maxsplit=1)[0].strip()
                found = list(_matches(tail, name))
                for value, label, start, end in found:
                    entries.append((name, value, label, tail[start:end]))
                if name == 'study_mode' and not found and re.fullmatch(r'[\u4e00-\u9fff]{1,20}', tail):
                    entries.append((name, 'other', tail, tail))
        if previous_pending:
            name, previous_group = previous_pending
            if previous_group == group:
                found = list(_matches(stripped, name))
                if found and _compact(stripped) in {label for labels in LABELS[name].values() for label in labels}:
                    entries += [(name, value, label, label) for value, label, _, _ in found]
        # A formal thesis/cover designation or a standalone education banner.
        # Names before the title stay outside the quote, and arbitrary prose
        # containing a keyword never supplies evidence.
        banner = _compact(stripped) in {'成人教育','成人高等教育','开放教育','网络教育','远程教育'}
        cohort = re.fullmatch(r'\d{4}(?:年(?:度)?|级|届)(.+)',compact)
        if cohort and cohort.group(1) in {label for labels in LABELS['study_mode'].values() for label in labels}:
            # A standalone cover cohort designation; years do not supply a
            # degree and are not stored in the minimal evidence quote.
            entries += [('study_mode',value,label,label) for value,label,_,_ in _matches(stripped,'study_mode')]
        if title or banner:
            quote = stripped[offsets[title.start()]:offsets[title.end()-1]+1] if title else stripped
            for name in LABELS:
                entries += [(name, value, label, quote) for value, label, _, _ in _matches(quote, name)]
        for name, value, label, quote in entries:
            if len(label)>80: continue
            if len(quote)>240: quote=label
            # Keep the first exact, locatable representative for each distinct
            # normalized value, before imposing schema limits. Repeated labels
            # cannot exhaust the evidence budget ahead of a later contradiction.
            # Eight represented values already prove conflict if more occur.
            if value in maps[name] or len(maps[name])>=8: continue
            existing = next((row for row in evidence if row['quote']==quote and row['locator']==locator), None)
            if existing is None:
                existing = {'id':'ev-'+str(len(evidence)+1), 'quote':quote,
                            'supports':[], 'locator':locator}
                evidence.append(existing)
            if name not in existing['supports']: existing['supports'].append(name)
            maps[name][value]={'value':value,'raw_labels':[label],'evidence_ids':[existing['id']]}
    degree = _field(maps['education_level']); mode = _field(maps['study_mode'])
    business = {'value':'unknown','status':'unknown','basis':'none','rule_id':None,
                'rule_version':BUSINESS_RULE_VERSION,'evidence_ids':[]}
    if mode['status']=='conflict': business.update(status='conflict',evidence_ids=mode['evidence_ids'].copy())
    elif mode['status']=='source_explicit' and mode['value'] in {'correspondence','adult_education'}:
        adult = mode['value']=='adult_education'
        business.update(value='correspondence',status='resolved',evidence_ids=mode['evidence_ids'].copy(),
                        basis='business_rule' if adult else 'source_correspondence',
                        rule_id='adult_education_to_correspondence' if adult else 'explicit_correspondence')
    return {'schema_version':SCHEMA_VERSION,'extraction_rule_version':EXTRACTION_RULE_VERSION,
            'original_source_sha256':original_hash,'evidence_source_sha256':evidence_hash,
            'evidence_source_format':suffix,'source_relation':source_relation,'derivation':derivation,
            'education_level':degree,'study_mode':mode,'business_classification':business,'evidence':evidence}
