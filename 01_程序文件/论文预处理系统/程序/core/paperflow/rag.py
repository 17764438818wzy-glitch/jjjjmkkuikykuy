"""复用现有 RAGFlow HTTP 客户端，直接发布可检索切片。"""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import quote, urlencode
from urllib.request import Request

from .package import inside,asset_storage_mode
from .instrumentation import measured


class RemoteConflict(ConnectionError):
    pass


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def native_client(config):
    # 只导入无启动副作用的底层模块；绝不导入 app/auto_pipeline。
    root = str(Path(config.legacy_app).resolve())
    if root not in sys.path:
        sys.path.insert(0, root)
    from auto_config import load_key
    from ragflow_client import RagflowClient
    key = load_key(Path(root) / 'data', 'ragflow')
    if not key:
        from .scheduling_policy import ServiceConfigurationError
        raise ServiceConfigurationError('RAGFlow DPAPI credential unavailable')
    return RagflowClient(config.rag_url, key)


class RagPublisher:
    def __init__(self, client, on_change=None):
        self.client = client
        self.on_change = on_change or (lambda receipts: None)

    def documents(self, ds, **filters):
        rows, seen = [], set()
        for page in range(1, 10001):
            data = self.client.request('GET', f'/datasets/{quote(ds, safe="")}/documents?' +
                                       urlencode(dict(page=page, page_size=100, orderby='create_time', desc='false', **filters)))
            batch = data.get('docs', []) if isinstance(data, dict) else data
            if not isinstance(batch, list):
                raise RemoteConflict('invalid document response')
            for row in batch:
                if row.get('id') in seen:
                    raise RemoteConflict('duplicate document pagination')
                seen.add(row['id'])
                rows.append(row)
            if len(batch) < 100:
                return rows
        raise RemoteConflict('document pagination exceeded')

    def chunks(self, ds, did):
        rows, seen, total = [], set(), None
        for page in range(1, 10001):
            data = self.client.request('GET', f'/datasets/{ds}/documents/{did}/chunks?page={page}&page_size=100')
            batch, count = data.get('chunks'), data.get('total')
            if not isinstance(batch, list) or not isinstance(count, int) or (total is not None and total != count):
                raise RemoteConflict('unstable chunk inventory')
            total = count
            for row in batch:
                if row.get('document_id') != did or row.get('id') in seen:
                    raise RemoteConflict('chunk document/identity mismatch')
                seen.add(row['id'])
                rows.append(row)
            if len(rows) == total:
                return rows
            if not batch or len(rows) > total:
                break
        raise RemoteConflict('incomplete chunk inventory')

    def _document_lookup(self, ds, **filters):
        try:
            return self.documents(ds, **filters)
        except RuntimeError as exc:
            if set(filters) != {'name'} or "you don't own the document" not in str(exc):
                raise
            # This deployment reports a missing name as an ownership error.
            # List every keyword match, then retain exact names. Dataset access
            # is still checked by the server before returning the filtered list.
            return [doc for doc in self.documents(ds, keywords=filters['name'])
                    if doc.get('name') == filters['name']]

    @staticmethod
    def document_names(task, packet, code, role):
        upload=f'PF_{task["source_sha256"][:16]}_{packet["manifest_sha256"][:20]}_{code}_{role}.md'
        profile=packet['profile']
        if profile.get('article',{}).get('document_name_policy')=='paper-title/1':
            from .package import article_presentation
            fields=article_presentation(profile,'article.md')
            if fields:
                display=fields['display_name']
                # Same-title papers and old/new revisions coexist in one dataset.
                label='资料' if role=='images' else '正文'
                display=display[:-3]+f'__{packet["manifest_sha256"][:20]}_{code}_{label}.md'
                return upload,display
        return upload,upload

    def _document(self, task, packet, route, role, receipt, receipts):
        ds = route['paper_dataset_id' if role == 'body' else 'image_dataset_id']
        digest = packet['manifest_sha256']
        identity = dict(paperflow_source_sha256=task['source_sha256'], paperflow_manifest_sha256=digest,
                        paperflow_revision=str(task['revision']), paperflow_specialty=route['code'],
                        paperflow_role=role, paperflow_task=task['task_id'])
        name,display_name=self.document_names(task,packet,route['code'],role)
        stub = self.document_bytes(task, packet, role, identity)
        obj = receipt.setdefault(role, dict(dataset_id=ds, name=name, chunks={}))
        if obj.get('dataset_id') != ds:
            raise RemoteConflict('receipt target library mismatch')
        obj['identity']=identity
        obj['upload_name']=name
        obj['display_name']=display_name
        self.on_change(receipts)  # durable intent before external effect
        matches = self._document_lookup(ds, **({'id': obj['document_id']} if obj.get('document_id') else {'name': name}))
        matches = [d for d in matches if d.get('name') in (name,display_name)]
        if obj.get('document_id') and not matches:
            raise RemoteConflict('bound remote document missing or renamed unexpectedly')
        if not matches and not obj.get('document_id') and display_name!=name:
            # A public title is not identity: a different paper may share it.
            matches=[d for d in self._document_lookup(ds,name=display_name)
                     if all((d.get('meta_fields') or {}).get(k)==v for k,v in identity.items())]
        if len(matches) > 1:
            raise RemoteConflict('ambiguous remote document')
        if matches:
            doc = matches[0]
            meta = doc.get('meta_fields') or {}
            if meta:
                if any(meta.get(k) != v for k, v in identity.items()):
                    raise RemoteConflict('remote source/version identity conflict')
            else:
                # Lost upload response: prove exact stub bytes before binding ownership.
                raw = self._download(ds, doc['id'])
                if raw != stub:
                    raise RemoteConflict('unbound remote document bytes conflict')
            did = doc['id']
        else:
            did = self.client.upload_bytes(ds, name, stub)
        obj['document_id'] = did
        self.on_change(receipts)
        patch={'meta_fields':identity,'enabled':0}
        if display_name!=name:patch['name']=display_name
        self.client.request('PATCH', f'/datasets/{ds}/documents/{did}', patch)
        observed = self.documents(ds, id=did)
        if (len(observed) != 1 or observed[0].get('name')!=display_name
                or any((observed[0].get('meta_fields') or {}).get(k) != v for k, v in identity.items())):
            raise RemoteConflict('metadata readback mismatch')
        obj['name']=display_name
        self.on_change(receipts)
        return obj

    def _download(self, ds, did):
        if hasattr(self.client, 'download_bytes'):
            return self.client.download_bytes(ds, did)
        request = Request(self.client.base_url + f'/api/v1/datasets/{ds}/documents/{did}',
                          headers={'Authorization': 'Bearer ' + self.client.api_key})
        with self.client.opener.open(request, timeout=30) as response:
            return response.read()

    def download_image(self,image_id):
        if hasattr(self.client,'download_image'):
            return self.client.download_image(image_id)
        request=Request(self.client.base_url+'/api/v1/documents/images/'+quote(image_id,safe=''),
                        headers={'Authorization':'Bearer '+self.client.api_key})
        with self.client.opener.open(request,timeout=30) as response:
            data=response.read()
            if not response.headers.get('Content-Type','').lower().startswith('image/'):
                raise ConnectionError('stored image did not return image bytes')
            return data

    def raw_records(self, packet):
        rows = [(r['id'], r['content'], None) for r in packet['chunks']]
        # 完整结构存于可读回元数据；索引只投影既有字段，不把几十KB诊断再送一次embedding。
        profile={k:packet['profile'][k] for k in ('task_id','source_id','source_sha256','article',
            'material_disposition','applicable_level','education_level','routing_candidates',
            'additional_related_specialties','migration_boundary') if k in packet['profile']}
        profile['learning_unit_index']=[{k:u[k] for k in ('id','title','name','method','section_ids','source_location') if k in u}
                                         for u in packet['profile'].get('learning_units',[])]
        tree=dict(nodes=[{k:n[k] for k in ('id','title','parent_id','source_location') if k in n}
                         for n in packet['tree']['nodes']])
        rows.append(('__profile',compact(profile),None))
        rows.append(('__tree',compact(tree),None))
        image_records = []
        seen_images = set()
        for a in packet['assets']:
            mode = asset_storage_mode(a)
            if mode == 'omit':
                continue
            text = compact({k: a.get(k) for k in ('id', 'kind', 'caption', 'description', 'note',
                                                  'section_id', 'source_location', 'body_anchor') if a.get(k) is not None})
            if mode == 'image':
                # 同篇重复像素只发送一次；所有资产关系仍保留在正文元数据。
                if a['sha256'] not in seen_images:
                    image_records.append(('asset_' + a['id'], text, a))
                    seen_images.add(a['sha256'])
            else:
                rows.append(('asset_' + a['id'], text, None))
        return dict(body=rows, images=image_records)

    def records(self, packet):
        from .structure_plan import plan_records
        records=self.raw_records(packet);planned=plan_records(packet)
        if planned is not None:
            records['body']=[row for record in records['body']
                             for row in planned.get(record[0],[record])]
        return records

    @staticmethod
    def render(task, packet, record):
        rid, content, asset = record
        marker = compact(dict(task=task['task_id'], source_sha256=task['source_sha256'],
                              manifest_sha256=packet['manifest_sha256'], record=rid))
        return '[PAPERFLOW]' + marker + '[/PAPERFLOW]\n' + content

    def document_bytes(self, task, packet, role, identity):
        return compact(identity).encode('utf-8')

    def publish_records(self, task, packet, role, records, obj, receipts):
        ds, did = obj['dataset_id'], obj['document_id']
        existing = self.chunks(ds, did)
        for record in records:
            rid, _, asset = record
            content = self.render(task, packet, record)
            marker = content.split('\n', 1)[0]
            matches = [c for c in existing if (c.get('content') or '').split('\n', 1)[0] == marker]
            if len(matches) > 1:
                raise RemoteConflict('duplicate remote record marker')
            if matches:
                chunk = matches[0]
                if chunk.get('content') != content:
                    raise RemoteConflict('remote chunk bytes conflict')
            else:
                obj['chunks'][rid] = dict(status='intent', content_sha256=hashlib.sha256(content.encode()).hexdigest())
                self.on_change(receipts)
                body = dict(content=content)
                if asset:
                    body['image_base64'] = base64.b64encode(inside(packet['root'], asset['file']).read_bytes()).decode('ascii')
                result = self.client.request('POST', f'/datasets/{ds}/documents/{did}/chunks', body)
                chunk = result.get('chunk', result)
                if not isinstance(chunk, dict) or not chunk.get('id'):
                    raise RemoteConflict('add chunk response missing identity')
                existing.append(dict(chunk, content=content, document_id=did))
            if asset and not chunk.get('image_id'):
                raise RemoteConflict('image storage identity missing')
            obj['chunks'][rid] = dict(status='written', chunk_id=chunk['id'], image_id=chunk.get('image_id'),
                                     content_sha256=hashlib.sha256(content.encode()).hexdigest())
            self.on_change(receipts)

    @measured('business:submission')
    def publish(self, task, packet, receipts):
        for route in packet['routes']:
            target = receipts.setdefault(route['code'], {})
            for role, records in self.records(packet).items():
                if not records:
                    continue
                obj = self._document(task, packet, route, role, target, receipts)
                self.publish_records(task, packet, role, records, obj, receipts)
            # 标准字段便于现有 Agent 获取画像/树；同时已有真实检索切片。
            obj = target['body']
            docs = self.documents(obj['dataset_id'], id=obj['document_id'])
            fields = {**docs[0].get('meta_fields', {}), 'two_pass_profile_json': compact(packet['profile']),
                      'two_pass_tree_json': compact(packet['tree']), 'rag_mid_assets_json': compact(packet['assets']),
                      'paperflow_image_associations_json': compact(target.get('images', {})),
                      'paperflow_contract': 'paperflow/1.0'}
            self.client.request('PATCH', f'/datasets/{obj["dataset_id"]}/documents/{obj["document_id"]}', {'meta_fields': fields})
            self.on_change(receipts)
        return receipts

    @measured('business:readback')
    def verify(self, task, packet, receipts):
        for route in packet['routes']:
            target = receipts.get(route['code'], {})
            for role, records in self.records(packet).items():
                if not records:
                    continue
                obj = target.get(role, {})
                ds, did = obj.get('dataset_id'), obj.get('document_id')
                if not ds or not did:
                    raise RemoteConflict('missing document receipt')
                expected_ds=route['paper_dataset_id' if role=='body' else 'image_dataset_id']
                if ds!=expected_ds:
                    raise RemoteConflict('receipt target library mismatch')
                docs = self.documents(ds, id=did)
                identity=dict(paperflow_task=task['task_id'],paperflow_source_sha256=task['source_sha256'],
                    paperflow_manifest_sha256=packet['manifest_sha256'],paperflow_revision=str(task['revision']),
                    paperflow_specialty=route['code'],paperflow_role=role)
                if len(docs) != 1 or any((docs[0].get('meta_fields') or {}).get(k)!=v for k,v in identity.items()):
                    raise RemoteConflict('document identity readback mismatch')
                if role=='body':
                    fields=docs[0].get('meta_fields') or {}
                    try:associations=json.loads(fields.get('paperflow_image_associations_json',''))
                    except (ValueError,TypeError):associations=None
                    if associations!=target.get('images',{}):
                        raise RemoteConflict('image association metadata readback mismatch')
                    if any(fields.get(k)!=compact(v) for k,v in (
                        ('two_pass_profile_json',packet['profile']),('two_pass_tree_json',packet['tree']),('rag_mid_assets_json',packet['assets']))):
                        raise RemoteConflict('structured metadata readback mismatch')
                chunks = self.chunks(ds, did)
                expected = {self.render(task, packet, record) for record in records}
                if len(chunks) != len(expected) or {c.get('content') for c in chunks} != expected:
                    raise RemoteConflict('exact chunk readback mismatch')
                if set(obj.get('chunks',{}))!={record[0] for record in records}:
                    raise RemoteConflict('record receipt set readback mismatch')
                for record in records:
                    content = self.render(task, packet, record)
                    chunk=next(c for c in chunks if c.get('content')==content)
                    receipt=obj['chunks'][record[0]]
                    if (receipt.get('status')!='written' or receipt.get('chunk_id')!=chunk['id']
                        or (receipt.get('image_id') or None)!=(chunk.get('image_id') or None)
                        or receipt.get('content_sha256')!=hashlib.sha256(content.encode()).hexdigest()):
                        raise RemoteConflict('record association readback mismatch')
                    if record[2]:
                        image_id=chunk.get('image_id')
                        if not image_id:
                            raise RemoteConflict('image association readback missing')
                        if (hasattr(self.client,'opener') or hasattr(self.client,'download_image')) and hashlib.sha256(self.download_image(image_id)).hexdigest()!=record[2]['sha256']:
                            raise RemoteConflict('image bytes readback mismatch')
            target['verified'] = True
            self.on_change(receipts)
        return receipts

    def set_enabled(self, receipts, enabled):
        for target in receipts.values():
            objects = [target.get(role) for role in ('body','images')]+target.get('legacy_images',[])
            for obj in objects:
                if not obj:
                    continue
                ds, did = obj['dataset_id'], obj['document_id']
                self.client.request('PATCH', f'/datasets/{ds}/documents/{did}', {'enabled': int(enabled)})
                docs = self.documents(ds, id=did)
                if len(docs) != 1 or str(docs[0].get('status')) != ('1' if enabled else '0'):
                    raise RemoteConflict('document availability switch not confirmed')

    @measured('business:retrieval')
    def verify_retrieval(self, task, packet, receipts):
        for route in packet['routes']:
            for role,records in self.records(packet).items():
                if not records:continue
                obj=receipts[route['code']][role]
                page_size=min(100,len(records));count=max(64,page_size)
                question=packet['profile'].get('article',{}).get('title') or records[0][1][:200]
                def search(query):
                    data=self.client.request('POST','/retrieval',dict(question=query,
                        dataset_ids=[obj['dataset_id']],document_ids=[obj['document_id']],
                        similarity_threshold=0.0,vector_similarity_weight=1.0,top_k=count,
                        rerank_candidates_count=count,page_size=page_size))
                    return {r.get('id',r.get('chunk_id')) for r in data.get('chunks',[])}
                observed=search(question)
                # 精确回读已核对全部内容；检索仅确认正文/图片索引可用，不考核各类排名。
                ids=([r['id'] for r in packet['chunks'] if r.get('kind')=='body']
                     if role=='body' else [r[0] for r in records])
                groups=[ids] if ids else [[r[0] for r in records]]
                if role=='body' and packet.get('structure_plan') is not None:
                    # A sealed new revision maps each logical structure to a
                    # child group. Visibility needs one actual child per group;
                    # exact readback above proves every part, not top-k rank.
                    for structure in packet['structure_plan']['structures'].values():
                        groups.append([r[0] for r in structure['records']])
                for group in groups:
                    ids={obj['chunks'][rid]['chunk_id'] for rid in group}
                    if not ids.intersection(observed):
                        # 标题问句的排名并不保证覆盖每个类别；用现有内容核查索引，避免误报未入库。
                        representative=next(r for r in records if r[0] in group)
                        observed.update(search(self.render(task,packet,representative)[:1000]))
                    if not ids.intersection(observed):
                        raise ConnectionError('vector retrieval categories not yet visible: '+','.join(group[:3]))
            receipts[route['code']]['retrieval_verified'] = True
            self.on_change(receipts)
