"""Bind generated Wiki pages to verified document identities and evidence IDs."""

import re


IDENTITY_MARKER = '<!-- education-wiki-identity/v1 -->'
IDENTITY_END = '<!-- /education-wiki-identity/v1 -->'
SOURCE_RE = re.compile(r'SOURCE\s+([0-9a-f]{16})')


def bind_identity(page: dict, document_info: dict[str, dict], body_chunk_doc_ids: dict[str, str]) -> str:
    """Return page text with a deterministic identity block, or fail closed.

    ``document_info`` is keyed by RAGFlow doc ID. Each value contains a
    manifest-verified stable_paper_id, title, admission, and example_tier.
    """
    text = page.get('content_md_rendered') or ''
    if not text.strip():
        raise ValueError('Wiki 页面为空')
    source_doc_ids = page.get('source_doc_ids') or []
    if not source_doc_ids or any(doc_id not in document_info for doc_id in source_doc_ids):
        raise ValueError('Wiki 页面有未知来源文档')
    cited = set(SOURCE_RE.findall(text))
    if not cited:
        raise ValueError('Wiki 页面没有来源切片')
    if not cited <= set(page.get('source_chunk_ids') or []):
        raise ValueError('Wiki 页面引用了非本页来源切片')
    if not cited <= set(body_chunk_doc_ids):
        raise ValueError('Wiki 页面引用了不可作为证据的切片')
    cited_doc_ids = {body_chunk_doc_ids[chunk_id] for chunk_id in cited}
    if not set(source_doc_ids) <= cited_doc_ids:
        raise ValueError('Wiki 页面有来源论文未被正文引文覆盖')
    entries = []
    for doc_id in source_doc_ids:
        info = document_info[doc_id]
        stable_id = info['stable_paper_id']
        if not re.fullmatch(r'paper_[0-9a-f]{24}', stable_id):
            raise ValueError('稳定论文编号格式错误')
        if info['admission'] != '入库':
            raise ValueError('Wiki 来源论文未通过准入')
        tier = info['example_tier'] or '待核实'
        if tier not in ('函授', '普通本科', '重点本科', '待核实'):
            raise ValueError('范例候选档位无效')
        other_ids = set(re.findall(r'paper_[0-9a-f]{24}', text)) - {document_info[x]['stable_paper_id'] for x in source_doc_ids}
        if other_ids:
            raise ValueError('Wiki 页面含无法对应来源的稳定论文编号')
        entries.append(f"- 《{info['title']}》：`{stable_id}`；来源文档 `{doc_id}`；范例候选档位：{tier}（待人工复核）。")
    block = '\n'.join([IDENTITY_MARKER, '**来源身份与档位（程序核验）**', *entries, IDENTITY_END])
    if IDENTITY_MARKER in text:
        pattern = re.escape(IDENTITY_MARKER) + r'.*?' + re.escape(IDENTITY_END)
        if not re.search(pattern, text, re.S):
            raise ValueError('Wiki 身份标记不完整')
        return re.sub(pattern, block, text, count=1, flags=re.S)
    if len(source_doc_ids) == 1:
        # A source-only Wiki run cannot see the stable ID; remove a generated
        # sentence that calls it unknown before adding the verified value.
        text = re.sub(r'稳定\s*paper_id[^。\n]*(?:待核实|待程序核验)[^。\n]*。', '', text, count=1)
        match = re.search(r'^####\s*L4[^\n]*$', text, re.M)
        if match:
            return text[:match.end()] + '\n\n' + block + text[match.end():]
    return text.rstrip() + '\n\n' + block + '\n'
