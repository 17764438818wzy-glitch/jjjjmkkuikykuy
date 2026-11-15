"""Locate old inventories and requests by source ownership. No writes or imports of executors."""
import hashlib
import json
from pathlib import Path

from .records import identity, read_json, sha_file
from .receipts import ReceiptError, path_key


def discover_legacy(handoff, roots, previous_group_root=None, *, mapped_source_roots=(), lexical_source=False):
    allowed={path_key(handoff['group_root'],lexical=lexical_source)}
    if previous_group_root is not None:allowed.add(path_key(previous_group_root,lexical=lexical_source))
    allowed.update(path_key(p,lexical=lexical_source) for p in mapped_source_roots)
    inventories=[];owners={};errors=[]
    for root in roots:
        root=Path(root)
        if not root.exists():
            errors.append('历史数据位置不存在：'+str(root));continue
        paths=[root] if root.is_file() else root.rglob('inventory.json')
        for path in paths:
            try:
                value=read_json(path)
                if not isinstance(value,dict):raise ValueError('历史清单不是对象')
                if value.get('schema')!='prefect-paper/1':continue
                pid=value['paper_id'];person=path_key(value['person_dir'],lexical=lexical_source)
                owners.setdefault(pid,set()).add(person)
                if person in allowed:
                    inventories.append((Path(path).resolve(),value))
            except (OSError,ValueError,KeyError,TypeError):
                errors.append('历史清单无法核验：'+str(path))
    existing=handoff.get('existing_paper_id')
    if existing in owners and not owners[existing].issubset(allowed):
        raise ReceiptError('历史论文编号属于其他来源资料组')
    ids={v['paper_id'] for _,v in inventories}
    if len(ids)>1:raise ReceiptError('同一来源存在多个历史论文编号，需要核对')
    if any(not owners[pid].issubset(allowed) for pid in ids):
        raise ReceiptError('历史论文编号同时属于多个来源资料组，需要核对')
    if existing and ids and existing not in ids:raise ReceiptError('上游历史编号与原清单不符')
    return dict(paper_id=next(iter(ids),None),person_dir=(inventories[0][1]['person_dir'] if inventories else None),
                inventories=inventories,errors=errors)


def item_legacy(legacy, kind, row):
    matches=[]
    for path,value in legacy['inventories']:
        candidates=([value['body']] if value.get('body') else []) if kind=='body' else value.get('attachments',[])
        if not isinstance(candidates,list):raise ReceiptError('历史原件清单格式不符')
        for old in candidates:
            if not isinstance(old,dict):raise ReceiptError('历史原件条目格式不符')
            if (old.get('relative','').casefold()==row['relative'].casefold()
                    and old.get('sha256')==row['sha256']):
                matches.append((path,value,old))
    ids={old['file_id'] for _,_,old in matches}
    if len(ids)>1:raise ReceiptError('同一原件有多个历史附件编号，需要核对')
    sequences={old.get('sequence',old.get('attachment_sequence')) for _,_,old in matches
               if old.get('sequence',old.get('attachment_sequence')) is not None}
    if len(sequences)>1:raise ReceiptError('同一原件有多个历史附件序号')
    sequence=next(iter(sequences),None)
    if sequence is not None and (type(sequence) is not int or sequence<1):
        raise ReceiptError('历史附件序号无效')
    paths=[]
    for inventory_path,value,old in matches:
        known=old.get('history',{})
        if not isinstance(known,dict):raise ReceiptError('历史工作位置格式不符')
        selected=known.get('body_work' if kind=='body' else 'attachment_work')
        if isinstance(selected,str) and Path(selected).is_absolute():paths.append(Path(selected))
        for name in known.get('work_dirs',[]):
            if isinstance(name,str) and Path(name).is_absolute():paths.append(Path(name))
        try:paper_dir=inventory_path.parents[3]
        except IndexError:continue
        if kind=='body':
            signature=inventory_path.parent.name
            key=identity([old['sha256'],old['relative'],signature])[:24]
            paths.append(paper_dir/'body'/key)
        else:
            folder=paper_dir/'attachments'/old['file_id']
            if folder.is_dir():
                paths.extend(p for p in folder.iterdir() if p.is_dir() and not p.is_symlink() and not p.is_junction())
    return dict(official_id=next(iter(ids),None),sequence=sequence,work_dirs=sorted({str(p.resolve()) for p in paths}),
                inventory_refs=[dict(path=str(p),sha256=sha_file(p)) for p,_,_ in matches])


def _request(model):
    state_path=model/'request-state.json';request_path=model/'request.json';response_path=model/'response.txt'
    present=any(p.exists() for p in (state_path,request_path,response_path,model/'response-events.jsonl',model/'http-error.txt'))
    if not present:return None
    result=dict(work_dir=str(model.parent),model_dir=str(model),status='unknown',
                request_path=str(request_path) if request_path.is_file() else None,
                request_sha256=None,response_path=str(response_path) if response_path.is_file() else None,
                response_sha256=None,state_path=str(state_path) if state_path.is_file() else None,
                state_sha256=None,reason='已有请求或回答证据；需继续按原台账核对')
    try:
        state=read_json(state_path);request=read_json(request_path)
        if not isinstance(state,dict) or not isinstance(request,dict):raise ValueError('历史请求格式不符')
        signature=hashlib.sha256(json.dumps(request,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        result.update(state_sha256=sha_file(state_path),request_sha256=signature)
        if state.get('request_sha256')!=signature:
            result['reason']='历史请求校验不符';return result
        if state.get('stored_request_sha256'):
            stored=request_path.read_bytes().replace(b'\r\n',b'\n')
            if hashlib.sha256(stored).hexdigest()!=state['stored_request_sha256']:
                result['reason']='历史请求原文件校验不符';return result
        if response_path.is_file():result['response_sha256']=sha_file(response_path)
        if (state.get('status')=='response_complete' and result['response_sha256']
                and state.get('response_sha256')==result['response_sha256']):
            result.update(status='complete',reason='已有原请求及完整回答；复用原工作记录')
    except (OSError,ValueError,KeyError,TypeError):
        result['reason']='历史记录缺失或无法校验；不得按新请求处理'
    return result


def observe_history(kind, paper_id, official_id, source_sha256, paths, *, errors=()):
    records=[];descriptions=[];existing_paths=[];problems=list(errors)
    for name in sorted(set(paths)):
        work=Path(name)
        if not work.is_dir():
            problems.append('历史工作位置不存在：'+str(work));continue
        existing_paths.append(str(work.resolve()))
        models={work/'model'}
        models.update(p.parent for p in work.rglob('request-state.json'))
        models.update(p.parent for p in work.rglob('request.json'))
        for model in sorted(models):
            observed=_request(model)
            if observed:records.append(observed)
        if kind=='attachment':
            description=work/'description.json'
            if description.is_file():
                try:
                    value=read_json(description)
                    if not isinstance(value,dict):raise ValueError('历史说明不是对象')
                    if (value.get('paper_id')!=paper_id or value.get('attachment_id')!=official_id
                            or value.get('source_sha256')!=source_sha256
                            or not isinstance(value.get('kind'),str) or not value['kind']
                            or not isinstance(value.get('summary'),str) or not value['summary']):
                        problems.append('历史附件说明身份不符：'+str(description));continue
                    verified=any(r['status']=='complete' and Path(r['work_dir'])==work for r in records)
                    stage=work/'stages'/'identify.json'
                    if stage.is_file():
                        s=read_json(stage)
                        if not isinstance(s,dict):raise ValueError('历史阶段记录不是对象')
                        verified=verified or (s.get('state')=='complete' and s.get('result')==value
                            and s.get('result_sha256')==identity(value)
                            and s.get('artifacts',{}).get(str(description.resolve()))==sha_file(description))
                    if verified:
                        descriptions.append(dict(path=str(description),sha256=sha_file(description),
                                                 work_dir=str(work),source_sha256=source_sha256))
                except (OSError,ValueError,TypeError):
                    problems.append('历史附件说明无法核验：'+str(description))
    unique={x['sha256'] for x in descriptions}
    selected=descriptions[0] if len(unique)==1 else None
    complete=bool(selected) if kind=='attachment' else any(r['status']=='complete' for r in records)
    policy='reuse_complete' if complete else 'inspect_saved_request' if records else 'history_lookup_required'
    chosen=(selected['work_dir'] if selected else existing_paths[0] if len(existing_paths)==1 else None)
    if len(existing_paths)>1 and chosen is None:
        problems.append('存在多个历史工作位置，需按原台账选定，不得另建空目录')
    return dict(body_work=chosen if kind=='body' else None,
                attachment_work=chosen if kind=='attachment' else None,
                work_dirs=existing_paths,request_records=records,
                existing_identification=selected,existing_identifications=descriptions,
                policy=policy,allow_new_request=False,limitations=problems)
