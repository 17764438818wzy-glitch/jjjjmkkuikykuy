"""H2.1 group-local, per-original receipt. No classification, extraction or dispatch."""
import copy
import json
from pathlib import Path
import re

from .records import identity, read_json, sha_file
from .receipts import ReceiptError, ReceiptStore, identifier
from .receipt_history import discover_legacy, item_legacy, observe_history


RECEIVED='已接收'
UNRECEIVED='未接收'


def _load(value):
    if isinstance(value,(str,Path)):
        path=Path(value).resolve()
        h=read_json(path)
        ref=dict(path=str(path),sha256=sha_file(path),hash_kind='file-sha256')
    else:
        h=copy.deepcopy(value)
        ref=dict(path=None,sha256=identity(h),hash_kind='canonical-json-sha256')
    return h,ref


def _header(h):
    if not isinstance(h,dict) or h.get('schema')!='paperflow-group-handoff/2':
        raise ReceiptError('不支持的上游交接格式')
    for name in ('group_id','handoff_id','result_version'):
        identifier(h.get(name),name)
    if h.get('paper_id') is not None:raise ReceiptError('上游不能另行分配正式论文号')
    library=Path(h['library_root']).resolve();group=Path(h['group_root']).resolve()
    if group==library or not group.is_relative_to(library):raise ReceiptError('资料组不在来源资料库内')
    if h.get('group_relative')!=group.relative_to(library).as_posix():
        raise ReceiptError('资料组相对位置不符')
    for name in ('attachments','trash','unresolved'):
        if not isinstance(h.get(name),list):raise ReceiptError('交接清单缺少列表：'+name)
    if h.get('body') is not None and not isinstance(h['body'],dict):raise ReceiptError('正文条目无效')
    rows=([h['body']] if h.get('body') else [])+h['attachments']+h['trash']+h['unresolved']
    try:
        ordered=sorted(rows,key=lambda r:r['relative'].casefold())
        snap=identity(['group-handoff/1',h['group_id'],
                       [(r['relative'],r['bytes'],r['sha256']) for r in ordered]])[:32]
        version=identity({k:v for k,v in h.items() if k!='result_version'})[:32]
    except (KeyError,TypeError,AttributeError) as exc:
        raise ReceiptError('上游交接清单缺少来源字段') from exc
    if h['handoff_id']!=snap or h['result_version']!=version:
        raise ReceiptError('上游交接标识或结果版本与清单不符')
    return library,group


def _source(h,row):
    return dict(handoff_id=h['handoff_id'],result_version=h['result_version'],
                file_id=row.get('file_id'),source_sha256=row.get('sha256'))


def _safe_original(row,group):
    identifier(row.get('file_id'),'上游文件编号')
    relative=row.get('relative')
    if not isinstance(relative,str) or not relative or '\\' in relative:
        raise ReceiptError('原件相对路径无效')
    rel=Path(relative)
    if rel.is_absolute() or '..' in rel.parts or relative in {'.',''}:
        raise ReceiptError('原件相对路径越界')
    if not isinstance(row.get('path'),str):raise ReceiptError('原件位置缺失')
    path=Path(row['path']).resolve()
    expected=(group/rel).resolve()
    if path!=expected or not path.is_relative_to(group):raise ReceiptError('原件不属于本资料组')
    if (not isinstance(row.get('sha256'),str)
            or not re.fullmatch('[0-9a-f]{64}',row['sha256'])):
        raise ReceiptError('原件SHA256缺失或无效')
    if type(row.get('bytes')) is not int or row['bytes']<0:raise ReceiptError('原件大小无效')
    # Reject links on the lexical source path, not only the resolved destination.
    lexical=Path(row['path']).absolute()
    for source in (lexical,*lexical.parents):
        if source.is_symlink() or source.is_junction():raise ReceiptError('原件来源经过链接')
        if source==group:break
    if not path.is_file():raise ReceiptError('原件缺失')
    before=path.stat()
    if before.st_size!=row['bytes'] or sha_file(path)!=row['sha256']:
        raise ReceiptError('原件大小或SHA256与交接清单不符')
    after=path.stat()
    if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
        raise ReceiptError('原件核验期间发生变化')
    return dict(row,path=str(path),format=path.suffix.lower(),name=path.name)


def _display(title,kind,sequence,suffix):
    if not isinstance(title,str) or not title.strip():return None
    clean=re.sub(r'[<>:"/\\|?*\x00-\x1f]','_',title).strip(' .')[:160]
    if not clean:return None
    if clean.split('.',1)[0].upper() in {'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))}:
        clean='_'+clean
    return clean+('' if kind=='body' else f'_附件{sequence:02d}')+suffix


def _evidence_facts(records):
    """Fingerprint supplied sidecars and prior versions, including gaps, before replay."""
    facts=[]
    for supplied in records:
        try:
            path=Path(supplied['path'] if isinstance(supplied,dict) else supplied).resolve()
            facts.append([str(path),sha_file(path)])
            value=read_json(path)
            version=value.get('evidence_version') if isinstance(value,dict) else None
            if type(version) is int and version>1:
                history=sorted(p for p in path.parent.glob('*.json')
                    if p.stem.isdecimal() and 1<=int(p.stem)<=version and p.is_file())
                facts.append(['history',str(path.parent),version,[(p.name,sha_file(p)) for p in history]])
        except (OSError,ValueError,KeyError,TypeError):
            facts.append(['unavailable',str(supplied)])
    return sorted(facts,key=lambda row:str(row))


def _evidence_for(h,row,records,validator):
    """Reuse window 1's validator. Invalid/missing evidence never becomes validated input."""
    refs=[];issues=[]
    for supplied in records:
        try:
            path=Path(supplied['path'] if isinstance(supplied,dict) else supplied).resolve()
            value=read_json(path)
            if not isinstance(value,dict):raise ReceiptError('证据记录必须为对象')
            source=value.get('source',{})
            if not isinstance(source,dict):raise ReceiptError('证据来源必须为对象')
            if source.get('file_id')!=row['file_id']:continue
            digest=sha_file(path)
            if isinstance(supplied,dict) and supplied.get('sha256') and supplied['sha256']!=digest:
                raise ReceiptError('证据记录文件校验不符')
            if value.get('group_id')!=h['group_id'] or source!=_source(h,row):
                raise ReceiptError('证据记录未绑定本次原件和来源版本')
            if validator is None:
                try:
                    from .attachment_evidence_handoff import validate_evidence_handoff
                    validator=validate_evidence_handoff
                except ImportError as exc:
                    raise ReceiptError('共用证据验证器尚未接入，证据未确认') from exc
            chain=[(path,value)]
            version=value.get('evidence_version')
            if type(version) is int and version>1:
                if path.name!=str(version)+'.json':raise ReceiptError('后续证据必须使用标准版本路径')
                available={p.name for p in path.parent.glob('*.json') if p.is_file()
                           and p.stem.isdecimal() and 1<=int(p.stem)<=version
                           and p.name==str(int(p.stem))+'.json'}
                if len(available)!=version:raise ReceiptError('证据版本链不完整')
                from .attachment_evidence_handoff import read_evidence_history
                history=read_evidence_history(path,handoff=h)
                chain=[(path.parent/(str(v['evidence_version'])+'.json'),v) for v in history]
            checked=[]
            for current,record in chain:
                outcome=validator(record,h,verify_files=True)
                if not isinstance(outcome,dict) or outcome.get('valid') is not True:
                    raise ReceiptError('共用证据验证未通过')
                checked.append(dict(path=str(current),sha256=sha_file(current),evidence_version=record['evidence_version'],
                    group_id=record['group_id'],source=record['source'],validation=outcome,
                    _canonical_sha256=identity(record)))
            refs.extend(checked)
        except Exception as exc:
            issues.append(dict(reference=str(supplied.get('path') if isinstance(supplied,dict) else supplied),
                               reason=str(exc) if isinstance(exc,ReceiptError) else type(exc).__name__))
    unique={(r['path'],r['sha256']):r for r in refs}
    return sorted(unique.values(),key=lambda r:(r['evidence_version'],r['path'])),issues


def received_items(receipt):
    return [copy.deepcopy(row) for row in receipt['items'] if row['state']==RECEIVED]


def accepted_paper(receipt):
    if not receipt.get('paper_id'):return None
    items=received_items(receipt)
    body=next((r for r in items if r['kind']=='body'),None)
    attachments=[r for r in items if r['kind']=='attachment']
    if not items:return None
    # Source/evidence versions and garbage must not invalidate successful body work.
    revision=identity([(r['kind'],r['file_id'],r['sha256']) for r in items])[:24]
    return dict(schema='prefect-paper/1',receipt_schema='paperflow-receipt/1',
                paper_id=receipt['paper_id'],revision=revision,
                person_dir=receipt['group_root'],library_root=receipt['library_root'],
                group_id=receipt['group_id'],origin_group_id=receipt['origin_group_id'],
                body=body,body_copies=[body] if body else [],attachments=attachments,
                archive_files=items,excluded=[r.get('relative') for r in receipt['retained_trash']],
                handoff_id=receipt['handoff_id'],result_version=receipt['result_version'],
                handoff_ref=receipt['handoff_ref'])


def verify_received_file(item):
    if item.get('state')!=RECEIVED:raise ReceiptError('此项尚未接收')
    source=item.get('source',{})
    if source.get('source_sha256')!=item.get('sha256'):
        raise ReceiptError('接收项身份与原件SHA256不符')
    _safe_original(dict(item,file_id=item['upstream_file_id']),Path(item['group_root']).resolve())
    return True


def batch_summary(receipts):
    groups={r['origin_group_id']:r['state'] for r in receipts}
    received=sum(state==RECEIVED for state in groups.values())
    return dict(total=len(groups),received=received,remaining=len(groups)-received)


def receive_handoff(handoff,store,*,evidence_records=(),origin_group_id=None,previous_group_root=None):
    if not isinstance(store,ReceiptStore):raise TypeError('store must be ReceiptStore')
    h,handoff_ref=_load(handoff)
    library,group=_header(h)
    store.check_output(library)
    evidence_records=tuple(evidence_records)
    evidence_facts=_evidence_facts(evidence_records)
    with store.transaction() as db:
        legacy_error=None
        roots=store.mapped_source_roots(db,origin_group_id)
        try:legacy=discover_legacy(h,store.history_roots,previous_group_root,mapped_source_roots=roots)
        except ReceiptError as exc:
            legacy_error=str(exc)
            legacy=dict(paper_id=None,person_dir=None,inventories=[],errors=[legacy_error])
        legacy_facts=[]
        for path,_ in legacy['inventories']:
            try:legacy_facts.append([str(path),sha_file(path)])
            except OSError:legacy_facts.append([str(path),None])
        input_key=identity([h,evidence_facts,sorted(legacy_facts),legacy['errors'],legacy_error])
        canonical=store.group(db,h,legacy,origin_group_id=origin_group_id,previous_group_root=previous_group_root)
        prior=store.prior(db,canonical,input_key)
        if prior and not legacy_error:
            try:store.paper(db,canonical,h,legacy,needed=False)
            except ReceiptError as exc:legacy_error=str(exc)
        if prior and prior['state']==RECEIVED and not legacy_error:
            result=prior
        else:
            incoming=([('body',h['body'])] if h.get('body') else [])+[('attachment',r) for r in h['attachments']]
            old_items={r['source']['file_id']:r for r in (prior or {}).get('items',[]) if r['state']==RECEIVED}
            checked=[]
            ids=[r.get('file_id') for _,r in incoming]
            relative=[str(r.get('relative','')).casefold() for _,r in incoming]
            for kind,row in incoming:
                error=legacy_error
                normalized=None
                if not error:
                    try:
                        if ids.count(row.get('file_id'))>1 or relative.count(str(row.get('relative','')).casefold())>1:
                            raise ReceiptError('同一文件同时出现在多个接收条目中')
                        if row.get('file_id') in old_items:
                            normalized=dict(row,format=Path(row['path']).suffix.lower(),name=Path(row['path']).name)
                        else:normalized=_safe_original(row,group)
                    except (OSError,ValueError,KeyError,TypeError) as exc:
                        error=str(exc) if isinstance(exc,ReceiptError) else type(exc).__name__
                checked.append((kind,row,normalized,error))
            try:
                pid=store.paper(db,canonical,h,legacy,needed=any(x[2] is not None for x in checked))
            except ReceiptError as exc:
                pid=None;legacy_error=str(exc)
                checked=[(k,r,None,legacy_error) for k,r,_,_ in checked]
            historical={}
            for kind,row,normalized,error in checked:
                if normalized is not None:
                    try:
                        old=item_legacy(legacy,kind,normalized)
                        historical[row['file_id']]=old
                    except (OSError,ValueError,KeyError,TypeError) as exc:
                        historical[row['file_id']]=exc
            # Reserve historical sequences before any newly added attachment can take them.
            for kind,row,normalized,error in checked:
                old=historical.get(row.get('file_id'))
                if isinstance(old,dict) and old['sequence'] is not None:
                    try:store.reserve_file(db,canonical,kind,normalized,old['official_id'],old['sequence'])
                    except ReceiptError as exc:historical[row['file_id']]=exc
            items=[];new_items=[]
            for kind,row,normalized,error in checked:
                source=_source(h,row)
                item=dict(kind=kind,state=UNRECEIVED,paper_id=pid,attachment_id=None,file_id=None,
                          upstream_file_id=row.get('file_id'),group_id=h['group_id'],group_root=str(group),
                          source=source,source_binding=dict(group_id=h['group_id'],**source),
                          handoff_ref=handoff_ref,path=row.get('path'),relative=row.get('relative'),
                          sha256=row.get('sha256'),bytes=row.get('bytes'),error=error,
                          sequence=None,display_name=None,evidence_records=[],unverified_evidence=[])
                if normalized is not None:
                    db.execute('SAVEPOINT receipt_item')
                    try:
                        old=historical[row['file_id']]
                        if isinstance(old,Exception):raise old
                        binding,is_new=store.file(db,canonical,kind,normalized,old['official_id'],old['sequence'])
                        paths=store.history_paths(db,canonical,binding['file_key'],normalized['sha256'],kind,old['work_dirs'])
                        history=observe_history(kind,pid,binding['official_id'],normalized['sha256'],paths,errors=legacy['errors'])
                        refs,issues=_evidence_for(h,normalized,evidence_records,store.evidence_validator) if kind=='attachment' else ([],[])
                        verified_refs=[]
                        db.execute('SAVEPOINT receipt_evidence')
                        try:
                            for ref in refs:verified_refs.append(store.evidence(db,ref))
                        except ReceiptError as exc:
                            db.execute('ROLLBACK TO receipt_evidence')
                            verified_refs=[]
                            issues.append(dict(reference='evidence-version-chain',reason=str(exc)))
                        finally:db.execute('RELEASE receipt_evidence')
                        item.update(normalized,state=RECEIVED,paper_id=pid,file_id=binding['official_id'],
                                    attachment_id=binding['official_id'] if kind=='attachment' else None,
                                    sequence=binding['sequence'],history=history,error=None,
                                    display_name=_display(h.get('paper_title'),kind,binding['sequence'],Path(normalized['path']).suffix),
                                    evidence_records=verified_refs,unverified_evidence=issues,
                                    legacy_inventory_refs=old['inventory_refs'],
                                    upstream_file_id=row['file_id'],group_id=h['group_id'],group_root=str(group),
                                    source=source,source_binding=dict(group_id=h['group_id'],**source),handoff_ref=handoff_ref)
                        # normalized includes the upstream file_id; official ID is assigned last above.
                        if is_new and old['official_id'] is None:
                            new_items.append(dict(kind=kind,file_id=item['file_id'],source_sha256=item['sha256']))
                    except (OSError,ValueError,KeyError,TypeError) as exc:
                        db.execute('ROLLBACK TO receipt_item')
                        item.update(state=UNRECEIVED,error=str(exc) if isinstance(exc,ReceiptError) else type(exc).__name__)
                    finally:
                        db.execute('RELEASE receipt_item')
                items.append(item)
            unresolved=copy.deepcopy(h['unresolved'])
            all_received=all(r['state']==RECEIVED for r in items) and not unresolved and not legacy_error
            result=dict(schema='paperflow-receipt/1',state=RECEIVED if all_received else UNRECEIVED,
                        group_id=h['group_id'],origin_group_id=canonical,group_root=str(group),library_root=str(library),
                        handoff_id=h['handoff_id'],result_version=h['result_version'],handoff_ref=handoff_ref,
                        paper_id=pid,items=items,new_items=new_items,retained_trash=copy.deepcopy(h['trash']),
                        unresolved=unresolved,upstream=dict(ready_for_receipt=h.get('ready_for_receipt'),
                            classification_complete=h.get('classification_complete')),
                        error=legacy_error,progress=dict(
                            body_received=sum(r['kind']=='body' and r['state']==RECEIVED for r in items),
                            body_total=sum(r['kind']=='body' for r in items),
                            attachments_received=sum(r['kind']=='attachment' and r['state']==RECEIVED for r in items),
                            attachments_total=sum(r['kind']=='attachment' for r in items),unresolved=len(unresolved)))
            result['inventory']=accepted_paper(result)
            result=store.record(db,canonical,input_key,result)
    return store.materialize(result)
