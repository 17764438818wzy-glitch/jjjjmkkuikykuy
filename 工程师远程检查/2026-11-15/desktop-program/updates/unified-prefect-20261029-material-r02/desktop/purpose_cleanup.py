"""Safe member cleanup; source archives and receipt-bound originals stay intact."""
from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path


def clean_archive(source,destination,decisions,*,replacements=None,stop=None):
    source=Path(source).absolute()
    destination=Path(destination).absolute()
    if destination==source or destination.exists():
        raise ValueError('干净包必须使用新的目标，不能覆盖原包或已有结果')
    if source.suffix.lower()!='.zip':
        raise ValueError('RAR 须先安全展开再逐成员生成干净 ZIP，原包保留待处理')
    for path in (source,*source.parents,destination,*destination.parents):
        if path.is_symlink() or path.is_junction():raise ValueError('清理路径含链接')
    source=source.resolve()
    destination=destination.resolve()
    destination.parent.mkdir(parents=True,exist_ok=True)
    temporary=destination.with_suffix(destination.suffix+'.partial')
    if temporary.exists():raise ValueError('已有清理意图，未覆盖')
    replacements=replacements or {}
    kept={};removed={};replaced={};seen=set()
    source_sha256=hashlib.sha256(source.read_bytes()).hexdigest()
    try:
        with zipfile.ZipFile(source) as old,zipfile.ZipFile(temporary,'x',compression=zipfile.ZIP_DEFLATED) as new:
            for info in old.infolist():
                if stop is not None and stop.is_set():raise InterruptedError('清理已停止，原包保留')
                name=info.filename.replace('\\','/')
                parts=Path(name).parts
                if name.startswith('/') or '..' in parts or ':' in name or name.casefold() in seen:
                    raise ValueError('包成员路径无效或重复')
                seen.add(name.casefold())
                if info.is_dir():continue
                data=old.read(info)
                sha=hashlib.sha256(data).hexdigest()
                role=decisions.get(info.filename,{})
                if role.get('action')=='delete':
                    if not (role.get('confirmed') and role.get('purpose_review_verified')
                            and role.get('evidence_sha256')==sha):
                        raise ValueError('删除成员缺少同哈希的用途复检回执')
                    removed[name]=sha
                else:
                    replacement=replacements.get(info.filename)
                    if replacement:
                        path=Path(replacement['path'])
                        proof=json.loads(path.with_suffix(path.suffix+'.receipt.json').read_text(encoding='utf-8'))
                        data=path.read_bytes()
                        cleaned_sha=hashlib.sha256(data).hexdigest()
                        if (proof.get('status')!='clean_copy_verified'
                                or proof.get('source_sha256')!=sha
                                or proof.get('clean_sha256')!=cleaned_sha
                                or replacement.get('source_sha256')!=sha):
                            raise ValueError('子压缩包清理回执不匹配')
                        replaced[name]=dict(source_sha256=sha,clean_sha256=cleaned_sha)
                        sha=cleaned_sha
                    new.writestr(info,data)
                    kept[name]=sha
        with zipfile.ZipFile(temporary) as check:
            actual={i.filename:hashlib.sha256(check.read(i)).hexdigest()
                    for i in check.infolist() if not i.is_dir()}
        if actual!=kept:raise ValueError('干净包中的正文或附件校验失败')
        temporary.replace(destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    result=dict(schema='purpose-clean-archive/1',source=str(source),path=str(destination),
                source_sha256=source_sha256,clean_sha256=hashlib.sha256(destination.read_bytes()).hexdigest(),
                source_modified=False,kept=kept,removed=removed,replaced=replaced,
                status='clean_copy_verified',original_cleanup='pending_explicit_commit')
    destination.with_suffix(destination.suffix+'.receipt.json').write_text(
        json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    return result
