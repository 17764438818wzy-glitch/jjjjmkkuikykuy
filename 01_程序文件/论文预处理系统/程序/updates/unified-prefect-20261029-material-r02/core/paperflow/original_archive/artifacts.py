"""Resolve only registered source evidence; downloads never execute a document."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from urllib.parse import quote


class ArtifactReader:
    def __init__(self,allowed_roots,runtime):
        self.roots=tuple(Path(p).resolve() for p in allowed_roots)
        self.runtime=Path(runtime).resolve()

    def allowed(self,path):
        p=Path(path).resolve()
        if not any(p.is_relative_to(root) for root in self.roots):raise ValueError('文件不在已登记来源目录内')
        return p

    def small_json(self,path,limit=4*1024*1024,*,with_hash=False):
        path=self.allowed(path)
        if not path.is_file():return None
        if path.stat().st_size>limit:raise ValueError('详情证据超过单次读取上限')
        before=path.stat()
        with path.open('rb') as stream:raw=stream.read(limit+1)
        if len(raw)>limit:raise ValueError('详情证据超过单次读取上限')
        value=json.loads(raw.decode('utf-8-sig'))
        after=path.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise ValueError('证据正在更新，请稍后再读')
        return (value,hashlib.sha256(raw).hexdigest()) if with_hash else value

    def inventory(self,row):
        files=[];warnings=[];detail=row.get('detail',{})
        def add(path,name,kind,sha=None,inside=None):
            if not path:return
            try:
                p=self.allowed(path)
                if inside and not p.is_relative_to(inside):raise ValueError('附件不在其论文保留目录内')
                available=p.is_file()
                if p.suffix.lower() not in {'.pdf','.doc','.docx','.txt','.md','.zip','.rar','.7z','.png','.jpg','.jpeg','.webp','.tif','.tiff','.bmp','.dwg','.dxf','.m','.py','.c','.cpp','.h','.java','.ipynb','.xls','.xlsx','.csv','.ppt','.pptx','.wps','.rtf','.odt','.html','.json','.svg'}:
                    available=False
                ident=hashlib.sha256((row['row_id']+'\0'+str(p)+'\0'+str(sha)).encode()).hexdigest()
                if any(a['id']==ident for a in files):return
                files.append({'id':ident,'path':str(p),'name':name or p.name,'kind':kind,
                    'expected_hash':sha,'available':available,'bytes':p.stat().st_size if available else None,
                    'identity':'下载时校验 SHA-256' if sha else '仅有来源路径，内容身份未核验'})
            except (OSError,ValueError):warnings.append('有文件路径超出已登记范围或不可读取')
        add(row.get('source_path'),row.get('name'),'原始来源',detail.get('source_hash'))
        container=detail.get('input_source_path')
        if container and container!=row.get('source_path'):add(container,None,'原始上传包')
        work=detail.get('work_dir')
        if work:
            try:
                work=self.allowed(work)
                for p in sorted((work/'private').glob('original.*')):
                    add(p,row.get('name') or p.name,'原稿保留副本',detail.get('source_hash'),work/'private')
                plan=self.small_json(work/'private/associated-upload-plan.json')
                for a in (plan or {}).get('files',[])[:300]:
                    add(a.get('original_path'),a.get('source_name'),'附件',a.get('sha256'),work/'private')
                # The upload plan may omit a retained file. The archive inventory remains
                # independently available; downloading it says nothing about delivery success.
                material=self.small_json(work/'material.json')
                def visit(a):
                    if len(files)>=300:return
                    add(a.get('original_path'),a.get('source_name') or Path(a.get('source_path','')).name,
                        '附件',a.get('sha256') or a.get('source_sha256'),work/'private')
                    for member in a.get('members',[]):visit(member)
                for a in (material or {}).get('attachments',[]):visit(a)
                if len(files)>=300:warnings.append('关联文件超过本次详情上限，清单尚未完整显示')
            except (ValueError,OSError,TypeError):warnings.append('部分关联清单暂不可读；不代表没有附件')
        return files,warnings

    def enrich(self,row):
        result=copy.deepcopy(row);files,warnings=self.inventory(row)
        prefix=f"/api/v1/views/{quote(row['view_id'],safe='')}/papers/{quote(row['row_id'],safe='')}/download/"
        result['artifacts']=[{k:v for k,v in a.items() if k not in {'path','expected_hash'}}|
            {'location':a['path'],'url':prefix+a['id']} for a in files]
        result['artifact_warnings']=warnings
        return result

    def resolve(self,row,identity):
        files,_=self.inventory(row)
        artifact=next((a for a in files if a['id']==identity),None)
        if not artifact:raise LookupError('未登记此文件')
        if not artifact['available']:raise FileNotFoundError('文件已不可读')
        return artifact

    def download(self,artifact,*,max_bytes=None):
        path=self.allowed(artifact['path'])
        self.runtime.mkdir(parents=True,exist_ok=True)
        # Serve the same bytes that were hashed, even if production replaces the original later.
        target=tempfile.SpooledTemporaryFile(max_size=8*1024*1024,dir=self.runtime,mode='w+b')
        sha=hashlib.sha256()
        try:
            with path.open('rb') as source:
                count=0
                for chunk in iter(lambda:source.read(1024*1024),b''):
                    count+=len(chunk)
                    if max_bytes is not None and count>max_bytes:raise ValueError('文件读取超出本次容量上限')
                    sha.update(chunk);target.write(chunk)
            if artifact.get('expected_hash') and sha.hexdigest()!=artifact['expected_hash']:
                raise ValueError('文件哈希与登记记录不符，已停止下载')
            target.seek(0)
            return target
        except BaseException:target.close();raise
