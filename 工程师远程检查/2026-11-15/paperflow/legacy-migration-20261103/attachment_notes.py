"""Read only the identities explicitly recorded in historical attachment cards."""
import copy,re

def enrich(doc,rows):
    doc=copy.deepcopy(doc);meta=doc.setdefault('meta',{})
    text='\n'.join(r.get('content_with_weight') or r.get('md_with_weight') or '' for r in rows)
    note=bool(re.fullmatch(r'att_[a-zA-Z0-9_-]+_note\.md',doc.get('name','')) and '# 附件说明' in text)
    if not note and not str(meta.get('paperflow_role','')).startswith('attachment'):return doc
    def field(labels,pattern):
        match=re.search(r'(?:'+labels+r')[：:]\s*('+pattern+r')(?=\s|$)',text)
        return match.group(1) if match else None
    sha=field('原件SHA256',r'[a-f0-9]{64}')
    old=field('原件文件ID|原件编号',r'[A-Za-z0-9_-]{1,128}')
    if sha and meta.get('attachment_source_sha256') and sha!=meta['attachment_source_sha256']:
        raise ValueError('Attachment note and metadata original hash disagree')
    if old and meta.get('original_file_id') and old!=meta['original_file_id']:
        raise ValueError('Attachment note and metadata original file identity disagree')
    if note and (not sha or not old):raise ValueError('Attachment note original identity is incomplete')
    if sha:meta['attachment_source_sha256']=sha
    if old:meta['original_file_id']=old
    if note:meta.update(paperflow_role='attachment-note',parent_link_status='unresolved')
    name=re.search(r'(?:原附件|图纸附件原件|数据附件原件|附件原件)[：:]\s*([^\n。]+)',text)
    if name:meta['original_source_name']=name.group(1).strip()
    return doc
