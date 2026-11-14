"""Reuse privately verified source-page exports only for the exact same input."""
import hashlib
import json
from pathlib import Path

from agent_client import AgentError

from project_paths import DATA_DIR
CACHE_ROOT=DATA_DIR/'private'/'verified_pdf'


def block_signature(block):
    fields = ('id','type','position','text','locator_texts','graphic_members')
    native = {key:block[key] for key in fields if key in block}
    return hashlib.sha256(json.dumps(native,ensure_ascii=False,sort_keys=True,
                                    separators=(',',':')).encode('utf-8')).hexdigest()


def verified_issue_pages(verified_render, issue, blocks, page_count):
    """Resolve an independently inspected native object on a hash-verified PDF.

    The caller must supply metadata returned by copy_verified_pdf. Unlisted
    objects still use the normal locator; changed native objects fail closed.
    """
    records = (verified_render or {}).get('verified_issue_pages', [])
    matches = [entry for entry in records if entry.get('code') == issue.get('code')
               and entry.get('position') == issue.get('position')]
    if not matches:return None
    try:
        if len(matches) != 1:raise ValueError('duplicate object verification')
        record = matches[0]
        exact = [block for block in blocks if block.get('id') == record.get('block_id')]
        if len(exact) != 1 or block_signature(exact[0]) != record.get('block_signature_sha256'):
            raise ValueError('native object changed')
        pages = record.get('verified_pages')
        if (not isinstance(pages,list) or not 1 <= len(pages) <= 4
                or any(type(page) is not int or not 1 <= page <= page_count for page in pages)
                or len(set(pages)) != len(pages)):
            raise ValueError('verified page bounds')
        return {page-1 for page in pages}
    except (ValueError,TypeError,KeyError) as exc:
        raise AgentError('已核验对象的原生内容或页面映射已改变，未使用旧定位，请重新核验') from exc


def copy_verified_pdf(source, work):
    source=Path(source)
    if source.suffix.lower()!='.docx':return None
    source_hash=hashlib.sha256(source.read_bytes()).hexdigest()
    entry=CACHE_ROOT/source_hash
    manifest=entry/'manifest.json'
    if not manifest.is_file():return None
    try:
        if entry.resolve().parent!=CACHE_ROOT.resolve():raise ValueError('cache path')
        info=json.loads(manifest.read_text(encoding='utf-8'))
        if (info.get('source_sha256')!=source_hash or info.get('verified') is not True
                or info.get('original_unchanged') is not True):raise ValueError('source provenance')
        pdf=(entry/'source.pdf').read_bytes()
        if hashlib.sha256(pdf).hexdigest()!=info.get('pdf_sha256'):raise ValueError('PDF changed')
        import fitz
        with fitz.open(stream=pdf,filetype='pdf') as document:
            if document.needs_pass or len(document)==0 or len(document)!=info.get('page_count'):
                raise ValueError('PDF pages')
        work=Path(work);work.mkdir(parents=True,exist_ok=True)
        target=work/(source.stem+'.pdf')
        target.write_bytes(pdf)
        (work/'verified_render.json').write_text(json.dumps(info,ensure_ascii=False,indent=2),encoding='utf-8')
        return target
    except (OSError,ValueError,TypeError,KeyError) as exc:
        raise AgentError('已验证的原文页图缓存校验失败；未使用过期或被修改的页面，请重新核验原文件') from exc
