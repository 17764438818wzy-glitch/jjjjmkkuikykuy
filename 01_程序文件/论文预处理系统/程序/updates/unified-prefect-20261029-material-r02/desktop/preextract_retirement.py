"""Retire verified uploads into the existing delivery table, without transport.

The saved inventory is a write-ahead delete intent. It stays in the same row
until every owned product has gone; complete rows retain only upload evidence.
"""
from contextlib import closing, ExitStack
import copy
import json
from pathlib import Path
import sqlite3
import threading
import time

from filelock import FileLock
from paperflow.private_files import resolve_private_path as resolved
from paperflow.cleanup_paths import inventory, delete_inventory
from preextract_cleanup_scope import Dependencies, family_workspaces, sha, read, dependency_keys, content_hashes

SCHEMA = 'uploaded-products-retirement/1'
FIELD = 'preextract_retirement'
from asset_evidence import identity,terminal
from asset_profile import capture_profile




def jobs(root, *, compact=False, terminal_only=False, source_hash=None, service=None):
    from preextract_delivery_queue import queue_path
    path = queue_path(root)
    if not path.is_file(): return []
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=5)) as db:
        projection="json_object('preextract_retirement',json_extract(projection,'$.preextract_retirement'))" if compact else 'projection'
        query='SELECT id,status,payload,'+projection+' FROM deliveries'
        clauses=[];parameters=[]
        if terminal_only:
            clauses.append("status='complete' AND json_extract(projection,'$.preextract_retirement.schema')='uploaded-products-retirement/1'")
        if source_hash is not None:
            clauses.append("json_extract(payload,'$.item.source_hash')=?");parameters.append(source_hash)
        if service is not None:
            clauses.append('service=?');parameters.append(service)
        if clauses:query+=' WHERE '+' AND '.join(clauses)
        return [dict(id=i,status=s,payload=json.loads(p),projection=json.loads(v))
                for i,s,p,v in db.execute(query,parameters)]


def terminal_matches(root, *, work=None, source_hash=None, service=None):
    matches=[]
    for job in jobs(root,terminal_only=True,source_hash=source_hash,service=service):
        if not terminal(job): continue
        p=job['payload']
        if work is not None and resolved(p['item']['preextract_work_dir']) != resolved(work): continue
        if source_hash is not None and p['item'].get('source_hash') != source_hash: continue
        if service is not None and p['service'] != service: continue
        matches.append(job)
    return matches


def find_terminal(root, **filters):
    matches=terminal_matches(root,**filters)
    # Multiple different deliveries cannot silently choose a target or revision.
    return matches[0] if len(matches)==1 else None


def parent_evidence(job):
    """Adapt verified durable facts to the existing attachment executor."""
    if not terminal(job):raise ValueError('unverified retired parent')
    record=job['projection'][FIELD];ident=record['identity']
    native=copy.deepcopy(record.get('associated_receipt'))
    prepared=dict(task=ident['task'],validation=dict(manifest_sha256=ident['manifest_sha256']),
        packet=dict(routes=record['routes'],records={}))
    if native:prepared['associated_upload_plan_sha256']=native['identity']['plan_sha256']
    receipt=dict(status='complete',task=ident['task'],manifest_sha256=ident['manifest_sha256'],targets=record['targets'])
    if native:
        from purpose_cache_cleanup import verify_associated_receipt
        if native['identity']['plan_sha256']!=record.get('associated_plan_sha256'):
            raise ValueError('retired attachment plan changed')
        verify_associated_receipt(prepared,None,dict(receipt,service=ident['service']),
            dict(files=record['attachments'],routes=record['routes']),receipt=native)
    identity=dict(**ident,routes=record['routes'],targets={r['code']:record['targets'][r['code']]['body']['document_id'] for r in record['routes']})
    return dict(identity=identity,work_dir=record['work_dir'],prepared=prepared,receipt=receipt,native=native)


def _compact_targets(targets):
    result={}
    for code, value in targets.items():
        target={k:value[k] for k in ('verified','retrieval_verified') if k in value}
        for role in ('body','images'):
            if role in value:
                target[role]={k:v for k,v in value[role].items()
                              if k in {'dataset_id','document_id','name','sha256'}}
        result[code]=target
    return result

def _attachment_originals(rows, originals, archive=None):
    """A verified intact ZIP is also the original of its extracted members."""
    import hashlib,zipfile
    for row in rows:
        external=[]
        for key in ('source_path','input_source_path'):
            value=row.get(key)
            if value and Path(value).is_absolute() and Path(value).is_file():
                path=resolved(value); originals[str(path)]=sha(path); external.append(path)
        in_archive=False
        member=row.get('archive_member_path') or row.get('source_path')
        if not external and archive and member:
            try:
                with zipfile.ZipFile(archive) as z, z.open(member) as stream:
                    in_archive=hashlib.file_digest(stream,'sha256').hexdigest()==row.get('sha256')
            except (OSError,KeyError,ValueError,zipfile.BadZipFile):pass
        if not external and not in_archive and row.get('original_path') and Path(row['original_path']).is_file():
            path=resolved(row['original_path']); originals[str(path)]=sha(path)
        original_archive=next((p for p in external if p.suffix.lower()=='.zip' and originals[str(p)]==row.get('sha256')),None)
        _attachment_originals(row.get('members',[]),originals,original_archive)


def _gate(root, job, *, verify_originals=True):
    from preextract_cleanup import _safe_path
    from preextract_publish import _paperflow
    from preextract_handoff import _receipt_complete
    from associated_delivery import load_plan
    from purpose_cache_cleanup import verify_associated_receipt, _material_rows
    payload=job['payload']; item=payload['item']; work=_safe_path(item['preextract_work_dir'],root)
    if item.get('status')!='pass' or not item.get('automatic_approval'):
        raise ValueError('paper_not_approved')
    published=_safe_path(item['package_path'],root)
    if not published.is_relative_to(root/'runs'/payload['run_id']/'packages'):
        raise ValueError('package_outside_registered_run')
    prepared=read(work/'private/prepared-publication.json')
    packet=_paperflow(prepared['paperflow_root']).validate_package(
        published,prepared['task'],prepared['catalog'],strict_seal=True)
    receipt=read(work/'delivery-receipt.json'); binding=read(work/'delivery-binding.json')
    if (not _receipt_complete(receipt,packet) or receipt.get('stage')!='verified'
        or set(receipt.get('targets',{}))!={r['code'] for r in packet['routes']}
        or binding.get('routes')!=packet['routes'] or binding.get('service')!=payload['service']
        or prepared['validation']['manifest_sha256']!=payload['manifest_sha256']
        or packet['manifest_sha256']!=payload['manifest_sha256']
        or any(receipt.get(k)!=payload[k] or binding.get(k)!=payload[k]
               for k in ('task','manifest_sha256'))):
        raise ValueError('all_specialty_readback_not_confirmed')
    prepared['packet']=packet
    plan=load_plan(prepared,work)
    bundle=read(work/'material.json')
    source=resolved(bundle['source']['file']); expected=payload['task']['source_sha256']
    if (item.get('source_hash')!=expected or bundle['source']['source_sha256']!=expected
            or verify_originals and (not source.is_file() or sha(source)!=expected)):
        raise ValueError('original_source_not_verified')
    # Inputs already consumed by a verified sealed package are not reuse
    # dependencies. In particular, historical authorized image deletion is OK.
    originals={str(source):expected}
    if verify_originals:_attachment_originals(bundle.get('attachments',[]),originals)
    from preextract_scope_guard import retirement_scope_proof,closure_path
    scope=retirement_scope_proof(work,receipt)
    if item.get('prepared_scope') and (not scope or any(
            item['prepared_scope'].get(k)!=scope['binding'].get(k) for k in ('database','scope_id','task_id'))):
        raise ValueError('managed delivery scope binding missing or changed')
    associated=None
    if scope:
        association=read(closure_path(work,scope['binding']['scope_id'])).get('association') or {}
        if association.get('receipt_path'):
            if not association.get('complete'):raise ValueError('scope attachment closure incomplete')
            plan_path=Path(association['plan_path']);plan=read(plan_path)
            if plan.get('task')!=prepared['task'] or plan.get('manifest_sha256')!=payload['manifest_sha256'] or plan.get('routes')!=packet['routes']:
                raise ValueError('scope attachment plan identity changed')
            prepared=dict(prepared,associated_upload_plan_sha256=sha(plan_path))
            associated=read(association['receipt_path'])
    if associated is None and plan and plan['files']:associated=read(work/'associated-delivery-receipt.json')
    verify_associated_receipt(prepared,work,dict(receipt,service=binding['service']),plan,receipt=associated)
    # A correction can add a sole surviving original outside the body bundle.
    # Existing externally retained originals already cover the native copies.
    represented={row.get('sha256') for row in _material_rows(bundle.get('attachments',[]))}
    if verify_originals:
        _attachment_originals([row for row in (plan or {}).get('files',[])
            if row['sha256'] not in represented and row['sha256'] not in originals.values()],originals)
    attachments=[]
    for ref,entry in (associated or {}).get('files',{}).items():
        attachments.append(dict(file_ref=ref,sha256=entry['sha256'],targets={code:{k:v for k,v in r.items()
            if k in {'dataset_id','document_id','parent_document_id','verified'}} for code,r in entry['targets'].items()}))
    record=dict(schema=SCHEMA,status='pending',delivery_id=job['id'],identity=identity(payload),
        source_path=str(source),source_name=source.name,source_sha256=expected,title=item.get('title',''),
        verified_at=receipt.get('last_remote_verified_at') or time.time(),targets=_compact_targets(receipt['targets']),
        routes=packet['routes'],attachments=attachments,work_dir=str(work),package_path=str(published),
        originals=originals,created_at=time.time(),removed_files=0,removed_bytes=0)
    record['asset_profile']=capture_profile(work,prepared,record['identity'])
    from asset_evidence import retained_attachment_inventory
    record['attachment_inventory']=retained_attachment_inventory((plan or {}).get('files',[]),bundle.get('attachments',[]))
    if scope:record['scope_proof']=scope
    if associated:
        record['associated_receipt']=associated
        record['associated_plan_sha256']=prepared['associated_upload_plan_sha256']
    return record,prepared,bundle,plan


def _purpose_scope(root,work,item,bundle,plan):
    """Reuse native exact source/context ownership; save it before body deletion."""
    from purpose_review import production_settings
    from purpose_cache_cleanup import _catalog,_generation,_select_reviews,_verified_review
    config=production_settings(root); cache=root/'private/purpose-review'
    if not config.get('enabled') or not cache.is_dir(): return [],[]
    if resolved(config['cache_root'])!=cache: raise ValueError('purpose_cache_not_migrated')
    catalog,errors=_catalog(str(cache),_generation(cache))
    if errors: raise ValueError('purpose_catalog_unreadable')
    selected,containers,covered=_container_scope(root,bundle,plan,config,cache)
    ordinary=dict(plan,files=[r for r in plan['files'] if r['file_ref'] not in covered]) if plan else plan
    selected.update(_select_reviews(root,work,item,bundle,ordinary,config,cache,catalog))
    scopes=[]; targets=list(containers)
    for path,source_hash in selected.items():
        _,model,_=_verified_review(path,source_hash,cache,root,config)
        scopes.append(dict(review=str(path),model=str(model),source_sha256=source_hash))
        targets.extend([path.parent,model])
    for index in (cache/'index').glob('*.json'):
        row=read(index)
        if row.get('receipt') and resolved(row['receipt']) in selected:
            targets.append(index)
    return scopes,targets

def _container_scope(root,bundle,plan,config,cache):
    from purpose_review import confirmed_container
    from purpose_cache_cleanup import _material_rows,_review_path
    from preextract_cleanup import _safe_path
    selected={};containers=[];covered=set()
    delivered=(plan or {}).get('files',[])
    for row in _material_rows(bundle.get('attachments',[])):
        ref=(row.get('purpose_review') or {}).get('purpose_container_receipt')
        if not ref:continue
        path=_safe_path(ref,root)
        if path.parent!=cache/'containers':raise ValueError('purpose_container_outside_cache')
        record=dict(path=row['source_path'],**{k:row[k] for k in ('name','input_source_path','archive_member_path') if row.get(k)})
        confirmed=confirmed_container(record,config)
        if (not confirmed or resolved(confirmed['purpose_container_receipt'])!=path
            or confirmed['evidence_sha256']!=row['sha256']):raise ValueError('purpose_container_identity_changed')
        proof=read(path);members=row.get('members',[])
        if len(members)!=len(proof['members']):raise ValueError('purpose_container_members_changed')
        for material,evidence in zip(members,proof['members']):
            matches=[r for r in delivered if resolved(r['original_path'])==resolved(material['original_path'])]
            if (len(matches)!=1 or matches[0]['sha256']!=evidence['sha256'] or material['sha256']!=evidence['sha256']):
                raise ValueError('purpose_container_member_not_delivered')
            selected[_review_path(evidence['receipt'],cache,root)]=evidence['sha256']
            covered.add(matches[0]['file_ref'])
        # Native /2 delivery sends the verified leaves; the intact external
        # archive remains the original, rather than being another upload.
        containers.append(path)
    return selected,containers,covered

def _conversion_scope(root,hashes,locks):
    """Reuse converter identities and its native per-key lock, also on failures."""
    from preextract_cleanup import _safe_path
    from material_conversion import VERSION
    def documents(folder):
        result=[];unreadable=[]
        paths=([folder/'identity.json'] if (folder/'identity.json').is_file() else [])+sorted((folder/'failed').glob('*/failure.json'))
        for path in paths:
            try:
                value=read(path)
                if not isinstance(value,dict):raise ValueError('conversion_proof_not_object')
                result.append(value)
            except (OSError,ValueError):unreadable.append(str(path))
        return result,unreadable
    entries=[]
    for folder in (root/'private/material-conversion/cache').glob('*/*'):
        if not folder.is_dir() or folder.parent.name not in {'libreoffice','wps'}:continue
        if len(folder.name)!=64 or any(c not in '0123456789abcdef' for c in folder.name):continue
        proof,unreadable=documents(folder)
        if not proof:continue
        source={d.get('source_sha256') if isinstance(d.get('source_sha256'),str) else None for d in proof}
        entries.append((folder,source,proof,unreadable))
    selected=[];pending=list(entries)
    while pending:
        next_round=[]
        for folder,sources,proof,unreadable in pending:
            if not sources & hashes:next_round.append((folder,sources,proof,unreadable));continue
            # An unrelated corrupt cache is retained without holding every
            # uploaded paper. Proven owners still require strict validation.
            if unreadable or len(sources)!=1 or any(d.get('engine')!=folder.parent.name or ('version' in d and d['version']!=VERSION) for d in proof):
                raise ValueError('conversion_cache_identity_changed')
            digest=next(iter(sources))
            folder=_safe_path(folder,root)
            locks.enter_context(FileLock(str(folder)+'.lock',timeout=0))
            if not folder.exists():continue
            if documents(folder)!=(proof,unreadable):raise ValueError('conversion_cache_changed_before_lock')
            output=proof[0].get('sha256')
            if output is not None and not content_hashes({'sha256':output}):raise ValueError('conversion_output_identity_invalid')
            selected.append(dict(path=str(folder),source_sha256=digest,output_sha256=output))
            if output:hashes.add(output)
        if len(next_round)==len(pending):break
        pending=next_round
    return selected


def _consumer_documents(work):
    names=('material.json','private/associated-upload-plan.json','private/candidate.json','revision.json','candidate-import.json')
    paths=[work/name for name in names]
    # Attachment copies are committed before the aggregate material manifest.
    # Those native checkpoints already own their referenced shared evidence.
    if not (work/'material.json').is_file():
        paths.extend((work/'private/associated').rglob('associated-material.json'))
    return [p for p in paths if p.is_file()]


def _references(root,record,all_jobs):
    from preextract_cleanup import _references as paths
    protected=set(map(resolved,record['originals']))
    from preextract_scope_guard import retirement_dependencies
    scoped_paths,live_hashes=retirement_dependencies(root,record)
    protected.update(scoped_paths)
    family=set(map(resolved,record['family']))
    for job in all_jobs:
        if job['id']==record['delivery_id']:continue
        native=(job.get('projection') or {}).get(FIELD) or {}
        nas_wait=terminal(job) and not native.get('original_archive')
        if terminal(job) and not nas_wait:continue
        item=job['payload']['item']; other=resolved(item['preextract_work_dir'])
        if job['status']!='complete' or nas_wait:
            live_hashes.update(content_hashes(job['payload']))
            protected.add(other)
            protected.add(resolved(item['package_path']))
            protected.update(paths(item))
            for p in _consumer_documents(other):
                protected.update(dependency_keys(p,'references'));live_hashes.update(dependency_keys(p,'content_hashes'))
    # A new/unqueued revision has no delivery row yet, but remains a consumer.
    owned=set(map(resolved,record['owned_workspaces']))
    registry=root/'single-request/source-index.json'
    if registry.is_file():
        # LLM-complete but unqueued work is also a live consumer.
        registered={}
        for key,value in read(registry).items():
            registered.setdefault(resolved(value),set()).update(content_hashes({'source_hash':key.split('|')[0]}))
        retired={resolved(j['payload']['item']['preextract_work_dir']) for j in all_jobs
                 if terminal(j) and j['projection'][FIELD].get('original_archive')}
        for other in registered.keys()-owned-retired:
            protected.add(other)
            live_hashes.update(registered[other])
            for p in _consumer_documents(other):
                protected.update(dependency_keys(p,'references'));live_hashes.update(dependency_keys(p,'content_hashes'))
    retired={resolved(j['payload']['item']['preextract_work_dir']) for j in all_jobs
             if terminal(j) and j['projection'][FIELD].get('original_archive')}
    for member in family-owned-retired:
        protected.add(member)
        for p in _consumer_documents(member):
            protected.update(dependency_keys(p,'references'));live_hashes.update(dependency_keys(p,'content_hashes'))
    pending=list(record.get('conversion',[]))
    while pending:
        remaining=[]
        for scope in pending:
            if scope['source_sha256'] not in live_hashes:remaining.append(scope);continue
            protected.add(resolved(scope['path']))
            if scope.get('output_sha256'):live_hashes.add(scope['output_sha256'])
        if len(remaining)==len(pending):break
        pending=remaining
    cache=root/'private/purpose-review'
    if record.get('purpose'):
        from purpose_cache_cleanup import _catalog,_generation,_pending_refs
        catalog,errors=_catalog(str(cache),_generation(cache))
        if errors: raise ValueError('purpose_catalog_unreadable')
        pending=_pending_refs(root)
        retired_reviews={s['review'] for j in all_jobs if terminal(j)
                         for s in j['projection'][FIELD].get('purpose',[])}
        retired_reviews.update(s['review'] for s in record['purpose'])
        for scope in record['purpose']:
            review,model=resolved(scope['review']),resolved(scope['model'])
            if str(review) in pending or 'unknown-sha:'+scope['source_sha256'] in pending or scope['source_sha256'] in live_hashes:
                protected.add(review.parent);protected.add(model)
            if any(v['model_dir']==str(model) and v['path'] not in retired_reviews for v in catalog):
                protected.add(model)
        for p in (cache/'leases').glob('*.json'):
            lease=read(p); protected.update([resolved(lease['review']).parent,resolved(lease['model_dir'])])
        for container in record.get('purpose_containers',[]):
            dependencies=Dependencies(protected)
            related=[s for s in record['purpose'] if s['review'] in container['reviews']]
            if (container['source_sha256'] in live_hashes or dependencies.overlaps(resolved(container['path']))
                or any(dependencies.overlaps(resolved(s['review']).parent) for s in related)):
                protected.add(resolved(container['path']))
                for s in related:protected.update([resolved(s['review']).parent,resolved(s['model'])])
    return Dependencies(protected)


def _make_plan(root,job,record,bundle,plan,locks=None):
    from preextract_cleanup import _safe_path
    work=resolved(record['work_dir'])
    family=family_workspaces(root,work,record['source_sha256'])
    # Only current work and its explicit ancestors are eligible automatically;
    # siblings/new revisions keep their own unuploaded results.
    owned={work}; member=work
    while (member/'revision.json').is_file():
        parent=_safe_path(read(member/'revision.json')['parent'],root)
        if parent not in family or member.parent!=parent/'revisions': raise ValueError('revision_lineage_changed')
        owned.add(parent); member=parent
    record['family']=[str(p) for p in sorted(family)]
    record['owned_workspaces']=[str(p) for p in sorted(owned)]
    record['purpose'],purpose_targets=_purpose_scope(root,work,job['payload']['item'],bundle,plan)
    record['purpose_containers']=[]
    for path in purpose_targets:
        if path.parent==root/'private/purpose-review/containers':
            proof=read(path)
            record['purpose_containers'].append(dict(path=str(path),source_sha256=proof['source_sha256'],
                reviews=[str(resolved(m['receipt'])) for m in proof['members']]))
    targets=owned|{resolved(record['package_path'])}|set(purpose_targets)
    if locks is not None:
        hashes=content_hashes(bundle)
        for parent in owned:
            if (parent/'material.json').is_file():hashes.update(dependency_keys(parent/'material.json','content_hashes'))
        record['conversion']=_conversion_scope(root,hashes,locks)
        targets.update(resolved(s['path']) for s in record['conversion'])
    from preextract_delivery_queue import queue_path
    from engine import safe_component
    with closing(sqlite3.connect(queue_path(root).as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        references=db.execute('SELECT run_id,item_id,package_path,source_hash FROM references_to_deliveries WHERE delivery_id=?',(job['id'],)).fetchall()
    for run_id,item_id,package_path,source_hash in references:
        if source_hash!=record['source_sha256']:raise ValueError('delivery_reference_changed')
        package=_safe_path(package_path,root)
        if not package.is_relative_to(root/'runs'/run_id/'packages'):raise ValueError('package_reference_outside_run')
        if package.is_dir():
            if sha(package/'manifest.json')!=record['identity']['manifest_sha256']:raise ValueError('package_reference_changed')
            targets.add(package)
        index=root/'runs'/run_id/'auto_view.json'
        if not index.is_file():continue
        view=root/'处理结果'/(safe_component(run_id)+'_全自动')
        for entry in read(index).get('files',[]):
            if entry.get('id')!=item_id:continue
            path=_safe_path(view/entry['path'],root)
            if not path.is_relative_to(view) or path.suffix.lower()!='.md':raise ValueError('browse_copy_outside_view')
            if path.is_file():
                if sha(path)!=entry.get('sha256'):raise ValueError('browse_copy_changed')
                targets.add(path)
    # Flatten exact native inventories so a shared file does not hold unrelated
    # completed products. Never delete locks or follow a reparse component.
    resources={}; directories=set()
    def collect(target):
        target=_safe_path(target,root)
        if not target.exists() or any(target==other or target.is_relative_to(other) for other in family-owned): return
        if target.is_dir():
            directories.add(str(target))
            # Native inventory is reusable on subtrees without lock anchors.
            if not any(p.name.endswith('.lock') for p in target.rglob('*')) and not any(other.is_relative_to(target) for other in family-owned):
                found=inventory(target,[root],[])
                directories.update(str(target/n) for n in found['directories'])
                for name,info in found['files'].items():
                    path=target/name
                    if str(path) not in record['originals']: resources[str(path)]=dict(path=str(path),**info)
            else:
                for child in sorted(target.iterdir()): collect(child)
        elif not target.name.endswith('.lock') and str(target) not in record['originals']:
            if not target.is_file(): raise ValueError('unsupported_cleanup_entry')
            resources[str(target)]=dict(path=str(target),sha256=sha(target),bytes=target.stat().st_size)
    for target in sorted(targets): collect(target)
    record['resources']=list(resources.values()); record['directories']=sorted(directories)


def _save(queue,job,record):
    current=queue.get(job['id'])
    if current['status']!='complete' or identity(current['payload'])!=record['identity']:
        raise InterruptedError('delivery_identity_changed')
    projection=copy.deepcopy(current['projection'])
    projection[FIELD]=record
    projection['preextract_cleanup']=dict(schema=SCHEMA,status=record['status'],
        reason=record.get('reason',''),removed_files=record['removed_files'],removed_bytes=record['removed_bytes'],
        checked_at=time.time(),next_check_at=time.time()+60)
    delivery=projection.setdefault('preextract_delivery',{})
    delivery.update(cleanup=record['status'],cleanup_receipt='queue.sqlite3#'+job['id'],
        cleanup_result=projection['preextract_cleanup'],targets=record['targets'],
        receipt_path='queue.sqlite3#'+job['id'])
    for v in projection.get('uploads',{}).values(): v.pop('readback',None)
    if not queue.update(job['id'],projection=projection,expected_status='complete'):
        raise InterruptedError('delivery_status_changed')


def _refresh_family(root,record,locks,locked):
    """Consumers can arrive between retries; inventories cannot authorize them."""
    from preextract_cleanup import _safe_path
    family=set(map(resolved,record['family']))
    for _ in range(4):
        for member in sorted(family-locked):
            if member.is_dir():locks.enter_context(FileLock(str(member/'operation.lock'),timeout=0))
            locked.add(member)
        found=set(family)
        for member in family:
            for meta_path in (member/'revisions').glob('*/revision.json'):
                child=_safe_path(meta_path.parent,root); meta=read(meta_path)
                if (meta.get('source_sha256')!=record['source_sha256'] or resolved(meta['parent'])!=member
                    or child.parent!=member/'revisions'):raise ValueError('revision_lineage_changed')
                found.add(child)
        if found==family:
            record['family']=[str(p) for p in sorted(family)]
            return
        family=found
    raise InterruptedError('revision_family_keeps_changing')


def retire(queue,job,root,*,stop=None,task_scope=None):
    # This task asks for disposable intermediates, with every answer retained.
    # The old whole-product plan deletes models/packages and expands ancestors;
    # it cannot serve as a task-scoped intermediate-cleanup fallback.
    if task_scope is None or task_scope.id!=job['id']:
        return dict(status='deferred',reason='storage_exact_task_scope_missing')
    task_scope.assert_open()
    return dict(status='deferred',reason='storage_full_product_retirement_preserves_saved_results')


def _retire_admitted(queue,job,root,*,stop=None,task_scope=None):
    """Whole-product deletion is outside the preserve-results storage lane."""
    return retire(queue,job,root,stop=stop,task_scope=task_scope)


def _cleanup_lane(projection):
    cleanup=projection.get('preextract_cleanup',{})
    if cleanup.get('checked_at') is None:return 0
    reasons={cleanup.get('reason'),projection.get(FIELD,{}).get('reason')}
    if reasons & {'waiting_for_originals_nas_verification', 'purpose receipt belongs to another source context'}:return 1
    shared={'shared_cleanup_pending','shared_unuploaded_dependency'}
    return 2 if reasons & shared else 3


def cleanup_queue(root,*,stop=None,limit=16,force=False,delivery_ids=None):
    from concurrent.futures import ThreadPoolExecutor,as_completed
    from filelock import Timeout
    from preextract_delivery_queue import DeliveryQueue,queue_path
    from preextract_cleanup import policy,legacy_processes_alive
    root=resolved(root); result=dict(completed=0,deferred=0)
    if not policy(root).get('enabled') or legacy_processes_alive(root) or not queue_path(root).is_file(): return result
    from preextract_nas_scope import selected_items,registered_items
    scopes,held=(registered_items(root,stop=stop) if delivery_ids is None
                 else selected_items(root,delivery_ids=delivery_ids,stop=stop))
    delivery_ids=list(scopes)
    result['held']=held
    if not delivery_ids:return result
    queue=DeliveryQueue(queue_path(root))
    with closing(sqlite3.connect(queue.path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        query="""SELECT id, CASE
            WHEN json_extract(projection,'$.preextract_cleanup.checked_at') IS NULL THEN 0
            WHEN COALESCE(json_extract(projection,'$.preextract_cleanup.reason'),'') IN ('waiting_for_originals_nas_verification','purpose receipt belongs to another source context')
              OR COALESCE(json_extract(projection,'$.preextract_retirement.reason'),'') IN ('waiting_for_originals_nas_verification','purpose receipt belongs to another source context') THEN 1
            WHEN COALESCE(json_extract(projection,'$.preextract_cleanup.reason'),'') IN ('shared_cleanup_pending','shared_unuploaded_dependency')
              OR COALESCE(json_extract(projection,'$.preextract_retirement.reason'),'') IN ('shared_cleanup_pending','shared_unuploaded_dependency') THEN 2
            ELSE 3 END AS cleanup_lane
            FROM deliveries WHERE status='complete' AND COALESCE(json_extract(payload,'$.kind'),'')!='associated_original'"""
        args=[]
        if not force:
            query+=" AND COALESCE(json_extract(projection,'$.preextract_retirement.status'),'')!='complete' AND COALESCE(json_extract(projection,'$.preextract_cleanup.next_check_at'),0)<=?"
            args.append(time.time())
        if delivery_ids is not None:
            if not delivery_ids:return result
            query+=' AND id IN ('+','.join('?' for _ in delivery_ids)+')';args+=list(delivery_ids)
        rows=db.execute(query+" ORDER BY cleanup_lane, updated_at LIMIT ?",(*args,limit)).fetchall()
    if not rows:return result
    all_lanes=policy(root).get('all_lanes_per_cycle') is True
    selected_lane=None if force or all_lanes else rows[0][1]
    if not force and not all_lanes:
        # Return to the native queue after this lane. Old shared work must not
        # extend a newly verified paper's mutex batch or fill a shared turn.
        rows=[row for row in rows if row[1]==selected_lane]
        if selected_lane==2:rows=rows[:1]
    candidates=[row[0] for row in rows]
    # Populate the existing stamp-keyed dependency cache BEFORE the deletion
    # mutex. A cold scan must not make a newly uploaded paper wait to register.
    _warm_dependencies(root,stop)
    # Reuse v13's bounded family scheduler; shared native locks serialize
    # overlapping source-index, purpose and conversion resources.
    configured=policy(root).get('cleanup_workers',2)
    workers=min(4,max(1,configured)) if type(configured) is int else 2
    families={}
    for ident in candidates:
        if stop is not None and stop.is_set():break
        job=queue.get(ident); record=job['projection'].get(FIELD,{})
        if not force and (record.get('status')=='complete' or
                (selected_lane is not None and _cleanup_lane(job['projection'])!=selected_lane) or
                job['projection'].get('preextract_cleanup',{}).get('next_check_at',0)>time.time()): continue
        source=job['payload'].get('task',{}).get('source_sha256')
        families.setdefault(('source',source) if source else ('row',ident),[]).append(ident)

    def clean(ident):
        if stop is not None and stop.is_set() or not policy(root).get('enabled'):
            return dict(status='deferred',reason='stopped')
        try:
            report=retire(queue,queue.get(ident),root,stop=stop,task_scope=scopes[ident])
            if report['status']=='deferred':
                from preextract_nas import storage_progress
                storage_progress(queue,ident,**dict(report,schema=SCHEMA,checked_at=time.time(),next_check_at=time.time()+60))
            return report
        except Exception as exc:
            return dict(status='deferred',error_type=type(exc).__name__)
    def family(group):return [(ident,clean(ident)) for ident in group]
    try:
        with FileLock(str(queue.path.with_name('cleanup.lock')),timeout=0,thread_local=False):
            retry=[]
            with ThreadPoolExecutor(max_workers=workers,thread_name_prefix='verified-cleanup') as pool:
                for future in as_completed([pool.submit(family,group) for group in families.values()]):
                    for ident,report in future.result():
                        if report.get('error_type')=='Timeout':retry.append(ident)
                        else:result['completed' if report['status']=='complete' else 'deferred']+=1
            # No family operation lock remains held. Recheck conflicted shared
            # resources once, using the same persisted deletion contract.
            for ident in retry:
                report=clean(ident)
                result['completed' if report['status']=='complete' else 'deferred']+=1
    except Timeout:
        result['deferred']+=sum(map(len,families.values()))
    return result


def _warm_dependencies(root,stop):
    paths={Path(j['payload']['item']['preextract_work_dir']) for j in jobs(root,compact=True) if j['status']!='complete'}
    registry=root/'single-request/source-index.json'
    if registry.is_file():paths.update(Path(p) for p in read(registry).values())
    for work in paths:
        if stop is not None and stop.is_set():return
        for p in _consumer_documents(work):
            try:
                dependency_keys(p,'references');dependency_keys(p,'content_hashes')
            except (OSError,ValueError,KeyError,TypeError):
                # A warmup grants no authority. The locked scan must still
                # read the current stamp and fail closed if it remains invalid.
                pass
