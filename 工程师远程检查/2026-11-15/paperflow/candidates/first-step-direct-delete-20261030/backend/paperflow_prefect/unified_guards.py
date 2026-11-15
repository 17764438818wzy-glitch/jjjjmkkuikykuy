"""Dispatch cancellation and read-only reconciliation of older upload journals."""
from pathlib import Path
from .records import read_json


class StopAwareClient:
    def __init__(self,client,stop):self.client,self.stop,self.base_url=client,stop,client.base_url
    def check(self):
        if self.stop.is_set():raise InterruptedError('此篇已取消，不再发送新写入')
    def request(self,method,endpoint,body=None):
        if method!='GET' and endpoint!='/retrieval':self.check()
        return self.client.request(method,endpoint,body)
    def upload_file(self,*args,**kwargs):self.check();return self.client.upload_file(*args,**kwargs)
    def upload_bytes(self,*args,**kwargs):self.check();return self.client.upload_bytes(*args,**kwargs)
    def __getattr__(self,name):return getattr(self.client,name)


def audit_legacy_uploads(backend,paper,row):
    """A prior native plan (including unknown submissions) requires explicit reuse."""
    roots=[backend.body_work(paper),backend.attachment_work(paper,row),
           *(Path(p) for p in backend.settings.values.get('rag_history_roots',[]))]
    sha=row['sha256'];body_sha=paper['body']['sha256'];seen=set()
    def contains(value,digest):
        if isinstance(value,dict):return any(contains(v,digest) for v in value.values())
        if isinstance(value,list):return any(contains(v,digest) for v in value)
        return value==digest
    for root in roots:
        if not root.exists():
            if str(root) in backend.settings.values.get('rag_history_roots',[]):raise ValueError('旧入库历史根不可用')
            continue
        candidates=set(root.rglob('associated*.json'))
        if root.name in {'associated_pending','associated-supplements'}:candidates.update(root.rglob('*.json'))
        for folder_name in ('associated_pending','associated-supplements'):
            for folder in root.rglob(folder_name):
                if folder.is_dir(): candidates.update(folder.rglob('*.json'))
        for path in candidates:
            if path in seen:continue
            seen.add(path);value=read_json(path)
            schema=str(value.get('schema',''))
            if contains(value,sha) or contains(value,body_sha):
                raise ValueError('原生附件已有计划或提交历史，须沿用原回执核对：'+str(path))
