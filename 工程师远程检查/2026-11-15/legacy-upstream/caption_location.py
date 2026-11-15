"""Tolerate a broken Word field only inside a figure/table caption number."""
import re
import unicodedata


def normalized(text):
    return re.sub(r'[^\w\u4e00-\u9fff]','',unicodedata.normalize('NFKC',text),flags=re.UNICODE).lower()


def caption_parts(text):
    match=re.fullmatch(r'\s*(图|表|Figure|Fig\.?|Table)\s*(\d+(?:\s*[.\-]\s*\d+)*)\s*[.:：、]?\s*(.+?)\s*',
                      unicodedata.normalize('NFKC',text),re.I)
    if not match:return None
    body=normalized(match[3])
    if not 4<=len(body)<=200 or body.isdigit():return None
    return normalized(match[1]),body,re.split(r'\s*[.\-]\s*',match[2])


def broken_caption_pages(label,page_texts,normalized_pages):
    parts=caption_parts(label)
    if not parts:return set()
    prefix,body,number_parts=parts
    # Full caption lines only: neither generic prose nor an arbitrary missing
    # word/number may be substituted. Some PDFs split field results across lines.
    error=r'(?:错误引用源未找到|错误未找到引用源|错误未定义书签|errorreferencesourcenotfound|errorbookmarknotdefined)'
    surviving_suffixes=[''.join(number_parts[start:]) for start in range(len(number_parts)+1)]
    suffix='(?:'+'|'.join(map(re.escape,surviving_suffixes))+')'
    pattern=re.compile(re.escape(prefix)+error+suffix+re.escape(body))
    hits=set()
    for number,text in enumerate(page_texts):
        if not normalized_pages[number]:continue  # Contents pages excluded by caller.
        lines=[line.strip() for line in text.splitlines() if line.strip()]
        for start in range(len(lines)):
            if any(pattern.fullmatch(normalized(' '.join(lines[start:start+size]))) for size in range(1,5)):
                hits.add(number);break
    return hits


def label_pages(label,page_texts,normalized_pages):
    numeric = re.sub(r'\s+', '', unicodedata.normalize('NFKC', label))
    if re.fullmatch(r'[+-]?\d+(?:[.,]\d+)*(?:%|‰)?', numeric):
        # Removing punctuation turns 10.00%, 100.00%, and adjacent table cells
        # into overlapping digit substrings. Keep the complete value and unit.
        tokens = re.findall(r'\d+|[^\d]', numeric)
        pattern = re.compile(r'(?<![\d.,+\-])' + r'\s*'.join(map(re.escape, tokens))
                             + r'(?![\d.,%‰])')
        return {i for i, page in enumerate(page_texts)
                if normalized_pages[i] and pattern.search(unicodedata.normalize('NFKC', page))}
    needle=normalized(label)
    direct={i for i,page in enumerate(normalized_pages) if needle and needle in page}
    return direct or broken_caption_pages(label,page_texts,normalized_pages)
