"""Check native foot/endnote links without generating or guessing citations."""
import re
from collections import Counter

LABEL = r'(?:footnote|endnote)Reference-?\d+'
REFERENCE = re.compile(r'\[\^(' + LABEL + r')\](?!:)')
DEFINITION = re.compile(r'^\s*\[\^(' + LABEL + r')\]:[ \t]*(.*)$', re.M)
NOTE_PARTS = {'word/footnotes.xml', 'word/endnotes.xml'}


def block_texts(block):
    if block.get('type') == 'paragraph':
        yield block.get('text', '')
    elif block.get('type') == 'table':
        for cell in block.get('cells', []):
            yield cell.get('text', '')
            for continuation in cell.get('continuation_text', []):
                yield continuation.get('text', '')


def assert_note_integrity(blocks):
    references = set()
    definitions = Counter()
    content = set()
    for block in blocks:
        label = block.get('note_label')
        for text in block_texts(block):
            references.update(REFERENCE.findall(text))
            matches = list(DEFINITION.finditer(text))
            for match in matches:
                definitions[match[1]] += 1
                # A compact note can span several original paragraphs.
                end = next((m.start() for m in matches if m.start() > match.start()), len(text))
                if text[match.start(2):end].strip():
                    content.add(match[1])
            if label and block.get('position', {}).get('part') in NOTE_PARTS:
                if DEFINITION.sub('', text).strip() or any(m[2].strip() for m in matches):
                    content.add(label)
    problems = []
    if duplicates := sorted(k for k,v in definitions.items() if v > 1):
        problems.append('注释定义重复：' + '、'.join(duplicates))
    if missing := sorted(references - definitions.keys()):
        problems.append('注释缺少原文内容：' + '、'.join(missing))
    # Old packages can contain an empty separator definition. Only a reference
    # in retained research content makes an empty note a missing source.
    if empty := sorted((references & definitions.keys()) - content):
        problems.append('注释原文为空：' + '、'.join(empty))
    if problems:
        raise ValueError('；'.join(problems) + '；请核对原文件，程序不会补造引文')
