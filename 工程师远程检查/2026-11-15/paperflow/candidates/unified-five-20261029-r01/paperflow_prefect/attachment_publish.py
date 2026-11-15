"""Native Files + one primary library; journal every write before sending it."""
import copy
import hashlib
import os
from pathlib import Path
import re
import time
from urllib.parse import quote

from filelock import FileLock
from .attachment_ingest import read_attachment_delivery, HEX
from .records import identity, read_json, write_json


class PendingDelivery(RuntimeError):
    """A prior request is unresolved, or read-only reconciliation cannot advance."""


def _require(condition, message):
    if not condition: raise ValueError(message)


def _sha(data): return hashlib.sha256(data).hexdigest()


def ensure_original_folder(client, root_id, code, name, receipt_root, *, read_only=False):
    """Create a single professional Files folder; an unknown create is never replayed."""
    from associated_delivery import _files
    binding = {'service':client.base_url.rstrip('/'),'parent_id':root_id,'code':code,'name':name}
    _require(isinstance(root_id,str) and re.fullmatch(r'[A-Za-z0-9_-]{1,128}',root_id)
             and isinstance(code,str) and re.fullmatch(r'[0-9]{6}(?:T|K|TK)?',code)
             and isinstance(name,str) and name.startswith('【'+code+'】') and len(name)<200, '附件目录身份不符')
    path = Path(receipt_root).resolve() / 'folders' / (identity([binding['service'],code])+'.json')
    path.parent.mkdir(parents=True,exist_ok=True)
    with FileLock(str(path)+'.lock',timeout=0):
        state = read_json(path) if path.is_file() else {'schema':'attachment-folder/1','binding':binding}
        _require(state.get('schema') == 'attachment-folder/1' and state.get('binding') == binding, '专业目录回执归属改变')
        def find():
            rows = [r for r in _files(client,root_id,name,None) if r.get('name') == name and r.get('parent_id') == root_id]
            _require(len(rows)<=1 and all(r.get('type')=='folder' for r in rows), '专业原件目录不唯一或类型不符')
            return rows[0] if rows else None
        row = find()
        if row is None:
            if read_only or state.get('intent') or state.get('folder_id'):
                raise PendingDelivery('专业原件目录未读回，不能重发或在只读模式创建')
            payload = {'parent_id':root_id,'name':name,'type':'folder'}
            state['intent'] = {'request_sha256':identity(payload),'submitted_at':time.time()}
            write_json(path,state)
            client.request('POST','/files',payload)
            row = find()
        if row is None: raise PendingDelivery('新建目录尚未读回，不重复创建')
        _require(not state.get('folder_id') or state['folder_id']==row['id'], '专业目录 ID 改变')
        state.update(folder_id=row['id'],status='verified',verified_at=time.time()); write_json(path,state)
        return row['id']


def _validate_route(card, route):
    _require(route.get('paper_id') == card['metadata']['paper_id'], '专业路由不属于本论文')
    for key in ('body_source_sha256', 'profile_sha256'):
        _require(isinstance(route.get(key), str) and HEX.fullmatch(route[key]), '正文来源或画像证据缺失')
    _require(route.get('history_reviewed') is True, '附件历史尚未核对，不能当作首次入库')
    primary = route.get('primary') or {}; refs = route.get('references', [])
    rows = [primary, *refs]; codes = [row.get('code') for row in rows]
    _require(bool(codes[0]) and codes == route.get('routing_codes') and len(codes) == len(set(codes)),
             '主附件库必须对应画像最高专业，其他专业顺序不可重算')
    for row in rows:
        for key in ('code', 'dataset_id'):
            _require(isinstance(row.get(key), str) and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', row[key]), '专业库标识不合法')
    _require(isinstance(primary.get('parent_id'), str) and
             re.fullmatch(r'[A-Za-z0-9_-]{1,128}', primary['parent_id']), '最高专业附件原件目录未配置')
    _require(len({row['dataset_id'] for row in rows}) == len(rows), '不同专业不能误用同一目标库')


class _Delivery:
    def __init__(self, card, route, client, path, budget, read_only):
        from paperflow.rag import RagPublisher
        self.card, self.route, self.client = card, route, client
        self.path, self.budget, self.read_only = path, budget, read_only
        self.publisher = RagPublisher(client)
        primary = route['primary']
        binding = {'service': client.base_url.rstrip('/'), 'raw_identity': card['raw_identity'],
                   'paper_id': route['paper_id'], 'attachment_id': card['metadata']['attachment_id'],
                   'source_sha256': card['metadata']['source_sha256'],
                   'body_source_sha256': route['body_source_sha256'],
                   'primary': {k: primary[k] for k in ('code', 'dataset_id', 'parent_id')}}
        self.state = read_json(path) if path.is_file() else {
            'schema': 'paperflow-attachment-ingest/1', 'binding': binding,
            'status': 'pending', 'raw': {}, 'publications': {}}
        _require(self.state.get('schema') == 'paperflow-attachment-ingest/1'
                 and self.state.get('binding') == binding, '已有原件回执的身份、服务或目标改变')

    def save(self):
        self.state['updated_at'] = time.time()
        write_json(self.path, self.state)
        write_json(self.path.parent / 'history' / (str(time.time_ns()) + '.json'), self.state)

    def send_once(self, state, key, payload, call):
        if state.get(key): raise PendingDelivery('原请求尚未读回确认，不能重发：' + key)
        if self.read_only: raise PendingDelivery('只读核对不创建或修改远端对象：' + key)
        state[key] = {'request_sha256': identity(payload), 'submitted_at': time.time()}
        self.save()
        return call()

    def files(self, name):
        from associated_delivery import _files
        parent = self.route['primary']['parent_id']
        # Some deployments search names across folders even when parent_id is supplied.
        return [row for row in _files(self.client, parent, name, None)
                if row.get('parent_id') == parent and row.get('name') == name]

    def file(self, name):
        rows = self.files(name)
        _require(len(rows) <= 1, '目标目录内原件名称不唯一')
        return rows[0] if rows else None

    def document(self, ds, did):
        rows = self.publisher.documents(ds, id=did)
        _require(len(rows) == 1 and rows[0].get('id') == did, '远端文档未唯一读回')
        return rows[0]

    def metadata(self, ds, did, meta, state):
        row = self.document(ds, did); observed = row.get('meta_fields') or {}
        if all(observed.get(k) == v for k, v in meta.items()): return row
        _require(not observed, '文档已有不同或不完整的归属元数据，保留原对象等待核对')
        body = {'meta_fields': meta, 'enabled': 0}
        self.send_once(state, 'metadata_intent', body,
                       lambda: self.client.request('PATCH', f'/datasets/{quote(ds,safe="")}/documents/{quote(did,safe="")}', body))
        row = self.document(ds, did)
        _require(all((row.get('meta_fields') or {}).get(k) == v for k, v in meta.items()), '文档归属元数据读回不符')
        return row

    def verify_parents(self):
        expected_keys = {'paperflow_task', 'paperflow_source_sha256', 'paperflow_manifest_sha256',
                         'paperflow_revision', 'paperflow_specialty', 'paperflow_role'}
        for parent in self.route.get('parent_targets', []):
            meta = parent.get('identity', {})
            _require(set(meta) == expected_keys and meta['paperflow_role'] == 'body'
                     and meta['paperflow_source_sha256'] == self.route['body_source_sha256']
                     and meta['paperflow_specialty'] == parent['code'], '正文远端归属证据不符')
            row = self.document(parent['dataset_id'], parent['document_id'])
            _require(str(row.get('status')) == '1' and all((row.get('meta_fields') or {}).get(k) == v for k,v in meta.items()),
                     '关联正文未启用或身份不符')

    def common_meta(self, code, role):
        meta = self.card['metadata']
        value = {'paperflow_role': role, 'paper_id': meta['paper_id'], 'attachment_id': meta['attachment_id'],
                 'attachment_source_sha256': meta['source_sha256'], 'body_source_sha256': self.route['body_source_sha256'],
                 'paperflow_specialty': code, 'primary_specialty': self.route['primary']['code'],
                 'original_file_id': self.state['raw']['file_id'], 'parent_link_status': 'paper_identity_only'}
        parent = next((p for p in self.route.get('parent_targets', []) if p['code'] == code), None)
        if parent:
            value.update(parent_link_status='verified_remote_body', parent_body_document_id=parent['document_id'])
        return value

    def original(self):
        entry = self.state['raw']; meta = self.card['metadata']; target = self.route['primary']
        name = 'PFA_' + self.card['raw_identity'] + meta['source_format']
        if entry.get('name'): _require(entry['name'] == name, '原件远端名称改变')
        entry['name'] = name
        remote = self.file(name)
        if remote is None:
            if entry.get('file_id'): raise PendingDelivery('原件已确认过但当前未读回，不能重新上传')
            data = Path(self.card['delivery']['source_path']).read_bytes()
            _require(_sha(data) == meta['source_sha256'] and len(data) == meta['source_bytes'], '原件在提交前改变')
            payload = {'parent_id': target['parent_id'], 'name': name, 'sha256': meta['source_sha256'], 'bytes': len(data)}
            fid = self.send_once(entry, 'upload_intent', payload,
                                 lambda: self.client.upload_file(target['parent_id'], name, data))
            _require(isinstance(fid, str) and bool(fid), '原件上传没有有效文件 ID')
            entry['file_id'] = fid; self.save(); remote = self.file(name)
        _require(remote is not None and remote.get('type') != 'folder'
                 and remote.get('size') == meta['source_bytes'], '原件远端位置、类型或大小不符')
        _require(not entry.get('file_id') or entry['file_id'] == remote['id'], '原件远端 ID 改变')
        fid = remote['id']
        _require(_sha(self.client.download_file(fid)) == meta['source_sha256'], '原件下载哈希不符')
        entry.update(file_id=fid, parent_id=target['parent_id'], source_sha256=meta['source_sha256'],
                     source_name=meta['source_name'], bytes=meta['source_bytes'],
                     local_original_path=self.card['delivery']['source_path'])
        self.save()
        def links(row):
            rows = row.get('kbs_info') or []
            _require(all(isinstance(r, dict) and r.get('kb_id') and r.get('document_id') for r in rows), '原件库链接无效')
            _require(len({r['kb_id'] for r in rows}) == len(rows), '原件库链接重复')
            _require(all(r['kb_id'] == target['dataset_id'] for r in rows), '原件已链接其他专业，不能复制或覆盖')
            return {r['kb_id']: r['document_id'] for r in rows}
        attached = links(remote)
        if target['dataset_id'] not in attached:
            payload = {'file_ids': [fid], 'kb_ids': [target['dataset_id']]}
            self.send_once(entry, 'link_intent', payload,
                           lambda: self.client.request('POST', '/files/link-to-datasets?mode=add', payload))
            remote = self.file(name)
            _require(remote is not None and remote['id'] == fid, '原件链接后文件身份不符')
            attached = links(remote)
        if target['dataset_id'] not in attached: raise PendingDelivery('原件库关联尚未读回，不重复链接')
        did = attached[target['dataset_id']]
        _require(not entry.get('document_id') or entry['document_id'] == did, '原件库文档 ID 改变')
        entry.update(dataset_id=target['dataset_id'], document_id=did); self.save()
        expected = self.common_meta(target['code'], 'attachment-original')
        expected['parse_status'] = 'not_requested'
        row = self.metadata(target['dataset_id'], did, expected, entry)
        if str(row.get('status')) != '0':
            payload = {'enabled': 0}
            self.send_once(entry, 'disable_intent', payload,
                           lambda: self.client.request('PATCH', f'/datasets/{quote(target["dataset_id"],safe="")}/documents/{quote(did,safe="")}', payload))
            row = self.document(target['dataset_id'], did)
        _require(str(row.get('status')) == '0' and row.get('run', 'UNSTART') in {'UNSTART', '0', 0}
                 and not self.publisher.chunks(target['dataset_id'], did), '原件必须保持未解析、未启用、零切片')
        _require(_sha(self.publisher._download(target['dataset_id'], did)) == meta['source_sha256'], '库内原件下载哈希不符')
        entry.update(status='verified', verified_at=time.time())
        self.state['download'] = {'file_path': '/api/v1/files/' + quote(fid,safe=''),
                                 'document_path': f'/api/v1/datasets/{quote(target["dataset_id"],safe="")}/documents/{quote(did,safe="")}',
                                 'authentication_required': True, 'source_sha256': meta['source_sha256']}
        self.save()

    def publish(self, target, primary_document_id=None):
        primary = primary_document_id is None; ds = target['dataset_id']; code = target['code']
        card = self.card; md_sha = card['delivery']['md_sha256']; raw = self.state['raw']
        key = identity([md_sha, code, ds, raw['file_id'], primary_document_id, self.route['profile_sha256']])
        group = self.state['publications'].setdefault(md_sha, {})
        entry = group.setdefault(code, {'identity': key, 'dataset_id': ds, 'chunks': {}})
        _require(entry.get('identity') == key, '当前 MD 版本的专业或原件链接改变')
        name = 'PFA_MD_' + key + '.md'
        download = self.client.base_url.rstrip('/') + self.state['download']['file_path']
        pointer = (f'\n\n原附件：{card["metadata"]["source_name"]}\n唯一原件所在专业：{self.route["primary"]["code"]}'
                   f'\n原件文件ID：{raw["file_id"]}\n原件文档ID：{raw["document_id"]}'
                   f'\n原件SHA256：{card["metadata"]["source_sha256"]}\n原件下载接口（需认证）：{download}\n')
        if primary:
            raw_md = card['md_text'].encode('utf8'); projection = card['md_text'] + pointer
        else:
            text = (f'# 附件引用\n论文编号：{card["metadata"]["paper_id"]}\n附件编号：{card["metadata"]["attachment_id"]}'
                    f'\n附件类型：{card["kind"]}\n简单说明：{card["summary"]}'
                    f'\n完整MD所在资料库：{self.route["primary"]["dataset_id"]}\n完整MD文档ID：{primary_document_id}'
                    f'\nMD SHA256：{md_sha}\n读取限制：{card["limitations"]}' + pointer)
            raw_md = text.encode('utf8'); projection = text
        meta = self.common_meta(code, 'attachment-card' if primary else 'attachment-reference')
        meta.update(md_sha256=md_sha, published_bytes_sha256=_sha(raw_md), profile_sha256=self.route['profile_sha256'],
                    original_document_id=raw['document_id'], primary_dataset_id=self.route['primary']['dataset_id'])
        if not primary: meta['primary_document_id'] = primary_document_id
        rows = self.publisher._document_lookup(ds, **({'id': entry['document_id']} if entry.get('document_id') else {'name': name}))
        rows = [row for row in rows if row.get('name') == name]
        _require(len(rows) <= 1, '附件 MD 远端文档不唯一')
        if not rows:
            if entry.get('document_id'): raise PendingDelivery('原 MD 文档缺失，不能重建')
            did = self.send_once(entry, 'upload_intent', {'name':name,'dataset_id':ds,'sha256':_sha(raw_md)},
                                 lambda: self.client.upload_bytes(ds, name, raw_md))
            _require(isinstance(did, str) and bool(did), 'MD 上传没有有效文档 ID')
            entry['document_id'] = did; self.save(); row = self.document(ds, did)
        else:
            row = rows[0]; did = row['id']; entry['document_id'] = did; self.save()
        _require(row.get('name') == name and self.publisher._download(ds, did) == raw_md, 'MD 原文读回不一致')
        self.metadata(ds, did, meta, entry)
        def render(index, part): return '<!-- PFA ' + key + ' ' + str(index).zfill(6) + ' -->\n' + part
        parts = self.budget.split(projection, render)
        expected = {str(i): render(i, part) for i, part in enumerate(parts)}
        observed = self.publisher.chunks(ds, did)
        for index, content in expected.items():
            marker = content.split('\n', 1)[0]
            matches = [row for row in observed if (row.get('content') or '').split('\n',1)[0] == marker]
            _require(len(matches) <= 1, 'MD 切片标识重复')
            chunk_state = entry['chunks'].setdefault(index, {})
            if matches:
                chunk = matches[0]; _require(chunk.get('content') == content, 'MD 切片内容改变')
                _require(not chunk_state.get('id') or chunk_state['id'] == chunk['id'], 'MD 切片 ID 改变')
            else:
                if chunk_state.get('id'): raise PendingDelivery('已确认 MD 切片当前缺失，不补发')
                result = self.send_once(chunk_state, 'intent', {'content_sha256':_sha(content.encode())},
                                         lambda: self.client.request('POST', f'/datasets/{quote(ds,safe="")}/documents/{quote(did,safe="")}/chunks', {'content': content}))
                chunk = result.get('chunk', result)
                _require(isinstance(chunk, dict) and chunk.get('id'), '切片写入没有有效 ID')
                observed.append(dict(chunk, content=content))
            chunk_state.update(id=chunk['id'], content_sha256=_sha(content.encode())); self.save()
        observed = self.publisher.chunks(ds, did)
        _require(len(observed) == len(expected), 'MD 切片总数不符')
        by_id = {row['id']: row for row in observed}
        _require(all(by_id.get(entry['chunks'][i]['id'], {}).get('content') == content for i,content in expected.items()),
                 'MD 切片集合未完整读回')
        row = self.document(ds, did)
        if str(row.get('status')) != '1':
            self.send_once(entry, 'enable_intent', {'enabled': 1},
                           lambda: self.client.request('PATCH', f'/datasets/{quote(ds,safe="")}/documents/{quote(did,safe="")}', {'enabled': 1}))
        row = self.document(ds, did)
        _require(str(row.get('status')) == '1' and all((row.get('meta_fields') or {}).get(k) == v for k,v in meta.items()),
                 'MD 启用或最终归属核验失败')
        # Native new chunks inherit the disabled document state until it is enabled.
        observed = self.publisher.chunks(ds, did)
        by_id = {row['id']: row for row in observed}
        _require(len(observed) == len(expected)
                 and all(by_id.get(entry['chunks'][i]['id'],{}).get('content') == content for i,content in expected.items())
                 and all(row.get('available') in (True,1,'1') for row in observed), 'MD 启用后切片内容或可用性不符')
        # Retrieval is a query, including in reconciliation. Refresh evidence each run.
        entry['retrieval_verified'] = False; self.save()
        page_size = min(100,len(parts)); count = max(64,page_size)
        result = self.client.request('POST', '/retrieval', {'question': (card['kind']+' '+card['summary'])[:1000],
            'dataset_ids':[ds], 'document_ids':[did], 'similarity_threshold':0.0,
            'vector_similarity_weight':1.0, 'top_k':count, 'rerank_candidates_count':count, 'page_size':page_size})
        hits = [row for row in result.get('chunks', []) if row.get('document_id') == did
                and row.get('id',row.get('chunk_id')) in by_id]
        if not hits: raise PendingDelivery('MD 已入库读回，索引尚未被检索命中')
        entry['retrieval_verified'] = True
        entry['retrieval_verified_at'] = time.time()
        entry['retrieval_chunk_ids'] = [row.get('id',row.get('chunk_id')) for row in hits]
        entry.update(status='verified', md_sha256=md_sha, projection_sha256=_sha(projection.encode()),
                     chunk_count=len(parts), verified_at=time.time())
        self.save(); return did

    def run(self):
        try:
            current = self.card['delivery']['md_sha256']
            for version, publications in self.state['publications'].items():
                if version != current and any(row.get('status') != 'verified' for row in publications.values()):
                    raise PendingDelivery('旧版 MD 仍有未决交付；须先用原冻结清单回查，不能换版绕过')
            self.verify_parents(); self.original()
            md_sha = self.card['delivery']['md_sha256']
            md_path = self.path.parent / 'md' / (md_sha + '.md'); md_path.parent.mkdir(parents=True, exist_ok=True)
            data = self.card['md_text'].encode('utf8')
            if md_path.exists(): _require(md_path.read_bytes() == data, '历史 MD 副本发生改变')
            else:
                with md_path.open('xb') as stream:
                    stream.write(data); stream.flush(); os.fsync(stream.fileno())
            did = self.publish(self.route['primary'])
            for target in self.route.get('references', []): self.publish(target, did)
            self.verify_parents()
            self.state.update(status='verified', delivery=copy.deepcopy(self.card['delivery']),
                              current_md_sha256=md_sha, saved_md_path=str(md_path),
                              parent_link_status=('verified_remote_body' if self.route.get('parent_targets') else 'paper_identity_only'),
                              receipt_path=str(self.path), model_requests_added=0)
            self.state.pop('error_type', None); self.save()
            return copy.deepcopy(self.state)
        except BaseException as exc:
            self.state.update(status='pending_verification', error_type=type(exc).__name__)
            self.save(); raise


def ingest_attachment(delivery, route, client, receipt_root, budget, *, read_only=False):
    card = read_attachment_delivery(delivery); _validate_route(card, route)
    key = identity([card['metadata']['paper_id'], card['metadata']['attachment_id']])
    directory = Path(receipt_root).resolve() / key; directory.mkdir(parents=True, exist_ok=True)
    with FileLock(str(directory / 'ingest.lock'), timeout=0):
        return _Delivery(card, route, client, directory / 'receipt.json', budget, read_only).run()
