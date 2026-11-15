"""Read-only completed records. No dispatch, RAG requests, or association guesses."""
import hashlib
import json
from pathlib import Path

from .package import inside, load, sha
from .workbench_view import status


def _eligible(article):
    return article['complete'] or article['state']=='excluded'


def _images(w, article, task):
    groups={}
    for route in article['routes']:
        group=groups.setdefault(route['expected_manifest'],dict(manifest=route['expected_manifest'],codes=[],routes=[]))
        group['codes'].append(route['code']);group['routes'].append(route)
    versions=[]
    with w.store.db(timeout=.1) as db:
        for digest,group in groups.items():
            row=db.execute('SELECT package_dir,manifest_bytes FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone()
            result=dict(manifest=digest,codes=sorted(group['codes']),state='unavailable',items=[])
            versions.append(result)
            try:
                if row is None:continue
                folder=Path(row['package_dir']).resolve()
                if not folder.is_relative_to(w.root):continue
                raw=bytes(row['manifest_bytes'])
                if hashlib.sha256(raw).hexdigest()!=digest:continue
                manifest=json.loads(raw)
                entry=next(f for f in manifest['files'] if f['path']=='assets.json')
                path=inside(folder,'assets.json')
                if not path.is_file() or sha(path)!=entry['sha256']:continue
                assets=load(path)['items']
                if not isinstance(assets,list) or any(not isinstance(a,dict) for a in assets):continue
                items=[]
                for asset in assets:
                    raw_mode=asset.get('storage','image' if asset.get('file') else 'description')
                    wrapped=raw_mode if isinstance(raw_mode,dict) else {}
                    declared=wrapped.get('mode') or wrapped.get('type') or raw_mode
                    # Compatibility metadata was accepted by earlier delivery versions.
                    # Read its details without silently reinterpreting it as an upload.
                    mode=raw_mode if isinstance(raw_mode,str) and raw_mode in ('image','description','omit') else 'unknown'
                    filename=asset.get('file') or wrapped.get('file')
                    destinations=[]
                    for route in group['routes']:
                        saved=db.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',
                                         (task['source_id'],route['code'])).fetchone()
                        active=json.loads(saved[0]) if saved else {}
                        if active.get('identity',{}).get('manifest_sha256')!=digest:continue
                        receipts=active.get('receipts',{}).get(route['code'],{})
                        obj=receipts.get('images' if mode=='image' else 'body',{})
                        record=obj.get('chunks',{}).get('asset_'+str(asset.get('id','')),{})
                        if mode!='omit' and obj.get('document_id'):destinations.append(dict(code=route['code'],name=route['name'],
                            dataset_id=obj.get('dataset_id') or route.get('materials_dataset_id' if mode=='image' else 'paper_dataset_id'),
                            document_id=obj.get('document_id'),chunk_id=record.get('chunk_id'),image_id=record.get('image_id')))
                    if mode=='image' and not (destinations and all(d['image_id'] for d in destinations)):
                        mode='unknown'
                    items.append(dict(id=asset.get('id'),kind=asset.get('kind'),storage=mode,
                        declared_storage=declared if isinstance(declared,str) else None,
                        filename=Path(str(filename or '').replace('\\','/')).name or None,
                        sha256=asset.get('sha256'),caption=asset.get('caption') or asset.get('title'),
                        description=asset.get('description') or asset.get('note') or wrapped.get('content') or wrapped.get('reason'),section_id=asset.get('section_id'),
                        source_location=asset.get('source_location'),body_anchor=asset.get('body_anchor'),destinations=destinations))
                result.update(state='checked',items=items)
            except (OSError,ValueError,TypeError,KeyError,StopIteration):
                # No missing or damaged inventory becomes an asserted empty inventory.
                pass
    return versions


def _attachment(w,article,task):
    with w.store.db(timeout=.1) as db:
        row=db.execute('SELECT * FROM attachment_tasks WHERE task_id=?',(task['task_id'],)).fetchone()
    if row is None:return dict(state='not_checked',count=None,items=[])
    data=json.loads(row['data'])
    item=dict(filename=task.get('display_name') or Path(task['source_path']).name,kind=row['category'],
        bytes=data.get('bytes'),sha256=row['source_sha256'],description=data['note'].get('description'),
        note=data['note'],target=data['target'],remote=data.get('remote',{}),association=data['association'],
        verification=article['attachment']['verification'],retrieval=article['attachment']['retrieval'])
    return dict(state='registered',count=1,items=[item])


def _decorate(w,article,*,detail=False):
    task=w.store.get(article['task_id'])
    versions=_images(w,article,task)
    if article['attachment']:
        image_summary=dict(state='not_applicable',retained=None,described=None,omitted=None)
    elif not versions:
        image_summary=dict(state='not_checked',retained=None,described=None,omitted=None)
    elif any(v['state']!='checked' for v in versions):
        image_summary=dict(state='unavailable',retained=None,described=None,omitted=None)
    elif any(a['storage']=='unknown' for v in versions for a in v['items']):
        image_summary=dict(state='unconfirmed',retained=None,described=None,omitted=None,
                           inventory_items=sum(len(v['items']) for v in versions))
    else:
        image_summary=dict(state='checked',**{key:sum(a['storage']==mode for v in versions for a in v['items'])
            for key,mode in [('retained','image'),('described','description'),('omitted','omit')]})
    attachments=_attachment(w,article,task)
    with w.store.db(timeout=.1) as db:
        times=[]
        for route in article['routes']:
            row=db.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',
                           (task['source_id'],route['code'])).fetchone()
            if row:
                active=json.loads(row[0])
                if active.get('identity',{}).get('manifest_sha256')==route['expected_manifest'] and active.get('activated_at'):
                    times.append(active['activated_at'])
        times.extend(a['finished'] for a in article['recent_attempts'] if a.get('finished') and a['outcome']=='complete')
    article.update(completed_at=max(times) if times else None,image_summary=image_summary,
                   attachment_summary={k:attachments[k] for k in ('state','count')})
    if not article['attempts']:
        article.update(elapsed_seconds=None,graph_seconds=None,delivery_seconds=None)
    if detail:article['completed_details']=dict(image_versions=versions,attachments=attachments)
    return article


def completed(w, *, cursor=None, query='', limit=50):
    """Keyset pagination across all registered batches, including batches older than 100."""
    if not 1<=limit<=100 or len(query)>200:raise ValueError('无效完成记录查询')
    after=None
    if cursor:
        try:
            after=json.loads(cursor)
            if not isinstance(after,list) or len(after)!=2 or type(after[0]) not in (int,float) or not isinstance(after[1],str):
                raise ValueError()
        except (ValueError,TypeError):raise ValueError('无效完成记录游标') from None
    items=[];scanned=0;more=True
    while len(items)<limit and scanned<1000:
        with w.store.db(timeout=.1) as db:
            where='';args=[]
            if after:
                where+=' AND (t.created<? OR (t.created=? AND t.task_id<?))';args.extend([after[0],after[0],after[1]])
            if query:
                where+=' AND instr(lower(COALESCE(json_extract(t.data,\'$.display_name\'),t.task_id)),lower(?))>0';args.append(query)
            rows=db.execute('''WITH selected AS (SELECT DISTINCT j.value AS task_id FROM web_batches b,json_each(b.task_ids) j)
                SELECT t.task_id,t.created FROM tasks t JOIN selected s ON s.task_id=t.task_id
                WHERE 1=1'''+where+' ORDER BY t.created DESC,t.task_id DESC LIMIT 50',args).fetchall()
        if not rows:more=False;break
        projected={a['task_id']:a for a in status(w,task_ids=[r['task_id'] for r in rows],articles_only=True)['articles']}
        for row in rows:
            after=[row['created'],row['task_id']];scanned+=1
            article=projected.get(row['task_id'])
            if article and _eligible(article):items.append(_decorate(w,article))
            if len(items)>=limit:break
        if len(items)<limit and len(rows)<50:more=False;break
    # The cursor represents the last *scanned* task; partial pages cannot skip a record.
    return dict(items=items,next_cursor=json.dumps(after,separators=(',',':')) if more and after else None)


def completed_detail(w,task_id):
    with w.store.db(timeout=.1) as db:
        known=db.execute('SELECT 1 FROM web_batches b,json_each(b.task_ids) j WHERE j.value=? LIMIT 1',(task_id,)).fetchone()
    if not known:return None
    articles=status(w,task_ids=[task_id],articles_only=True)['articles']
    if not articles or not _eligible(articles[0]):return None
    return _decorate(w,articles[0],detail=True)
