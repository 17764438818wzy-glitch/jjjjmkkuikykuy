"""Publish processed attachment cards through the existing native pipeline.

The caller supplies the already verified test-only datasets and Files folder.
The original is stored once using the existing attachment business primitive.
"""
import hashlib
from pathlib import Path
import time
from urllib.parse import urlsplit

from paperflow.rag import compact, RagPublisher
from paperflow_prefect.attachment_ingest import read_attachment_delivery
from paperflow_prefect.attachment_publish import _Delivery, _validate_route, PendingDelivery
from paperflow_prefect.records import identity

from effect_journal import EffectUnknown
from folder_native_client import FolderNativeClient


class AttachmentClient(FolderNativeClient):
    def __init__(self, raw, journal, targets, pipeline, admit, require_runtime, folder):
        super().__init__(raw, journal, targets, pipeline, admit, require_runtime)
        self.folder, self.files = folder, set()
        self.on_effect = None

    def _send(self, action, send, *, reuse_returned=False):
        digest = identity(action)
        if self.on_effect is not None:
            self.on_effect(digest)
        return super()._send(action, send, reuse_returned=reuse_returned)

    def request(self, method, endpoint, body=None):
        path = urlsplit(endpoint).path
        if method == 'GET':
            result = self.raw.request(method, endpoint, body)
            if path == '/files':
                self.files.update(row['id'] for row in result.get('files', [])
                                  if row.get('parent_id') == self.folder and row.get('type') != 'folder')
            return result
        if method == 'POST' and path == '/files/link-to-datasets':
            if not set(body['file_ids']) <= self.files or not set(body['kb_ids']) <= self.targets:
                raise ValueError('attachment link is outside the selected test folder or datasets')
            return self._send({'method': method, 'endpoint': endpoint, 'body': body},
                              lambda: self.raw.request(method, endpoint, body))
        return super().request(method, endpoint, body)

    def upload_file(self, folder, name, content):
        if folder != self.folder:
            raise ValueError('attachment original is outside the test folder')
        action = {'folder': folder, 'name': name, 'sha256': hashlib.sha256(content).hexdigest()}
        return self._send(action, lambda: self.raw.upload_file(folder, name, content))

    def download_file(self, file_id):
        if file_id not in self.files:
            raise ValueError('attachment file is outside the test folder')
        return self.raw.download_file(file_id)

    def download_bytes(self, dataset, document):
        if dataset not in self.targets:
            raise ValueError('attachment document is outside the test datasets')
        return RagPublisher(self.raw)._download(dataset, document)


class AttachmentDelivery(_Delivery):
    def __init__(self, card, route, client, path, budget):
        existed = path.is_file()
        super().__init__(card, route, client, path, budget, False)
        if existed and self.state.get('transport_contract') != 'native-isolated-journal/1':
            raise ValueError('attachment receipt has a different transport history')
        if existed and self.state.get('journal_id') != client.journal.identity():
            raise ValueError('attachment receipt belongs to a different effect journal')
        self.state['transport_contract'] = 'native-isolated-journal/1'
        self.state['journal_id'] = client.journal.identity()

    def send_once(self, state, key, payload, call):
        prior = state.get(key)
        if prior:
            if prior['request_sha256'] != identity(payload):
                raise PendingDelivery('原请求内容改变，不能重发：' + key)
            effect = prior.get('effect_id')
            if effect and self.client.journal.state(effect, effect) is not None:
                raise PendingDelivery('原请求尚未读回确认，不能重发：' + key)
        state[key] = {'request_sha256': identity(payload), 'submitted_at': time.time()}
        self.save()
        def record_effect(effect):
            state[key]['effect_id'] = effect
            self.save()
        self.client.on_effect = record_effect
        try:
            return call()
        finally:
            self.client.on_effect = None


def publish_attachment(item, route, raw, journal, admit, require_runtime, pipeline, budget, receipt_path):
    card = read_attachment_delivery(item)
    _validate_route(card, route)
    if budget.cl_limit > 2038:
        raise ValueError('attachment token budget exceeds the native pipeline limit')
    primary, references = route['primary'], route['references']
    targets = [primary, *references]
    client = AttachmentClient(raw, journal, [row['dataset_id'] for row in targets], pipeline,
                              admit, require_runtime, primary['parent_id'])
    path = Path(receipt_path)
    delivery = AttachmentDelivery(card, route, client, path, budget)
    delivery.state['status'] = 'pending_verification'
    delivery.save()
    for version, publications in delivery.state['publications'].items():
        if version != card['delivery']['md_sha256'] and any(row.get('status') != 'verified' for row in publications.values()):
            raise PendingDelivery('旧版 MD 仍有未决交付，不能换版绕过')
    delivery.verify_parents()
    delivery.original()
    md_sha, raw_entry = card['delivery']['md_sha256'], delivery.state['raw']
    publications = delivery.state['publications'].setdefault(md_sha, {})
    primary_document = None
    for target in targets:
        code, ds = target['code'], target['dataset_id']
        is_primary = target is primary
        key = identity([md_sha, code, ds, raw_entry['file_id'], primary_document, route['profile_sha256']])
        entry = publications.setdefault(code, {'identity': key, 'dataset_id': ds, 'chunks': {}})
        if entry.get('identity') != key:
            raise ValueError('attachment target or original binding changed')
        name = 'PFA_NATIVE_' + key + '.md'
        pointer = (f'\n\n原附件：{card["metadata"]["source_name"]}\n唯一原件所在专业：{primary["code"]}'
                   f'\n原件文件ID：{raw_entry["file_id"]}\n原件文档ID：{raw_entry["document_id"]}'
                   f'\n原件SHA256：{card["metadata"]["source_sha256"]}'
                   f'\n原件下载接口（需认证）：{raw.base_url.rstrip("/")}{delivery.state["download"]["file_path"]}\n')
        if is_primary:
            projection = card['md_text'] + pointer
        else:
            projection = (f'# 附件引用\n论文编号：{card["metadata"]["paper_id"]}\n附件编号：{card["metadata"]["attachment_id"]}'
                          f'\n附件类型：{card["kind"]}\n简单说明：{card["summary"]}'
                          f'\n完整MD所在资料库：{primary["dataset_id"]}\n完整MD文档ID：{primary_document}'
                          f'\nMD SHA256：{md_sha}\n读取限制：{card["limitations"]}' + pointer)
        def render(index, text):
            return '<!-- PFA ' + key + ' ' + str(index).zfill(6) + ' -->\n' + text
        parts = budget.split(projection, render)
        if len(parts) > 1000:
            raise ValueError('attachment exceeds native batch capacity')
        records = [render(index, part) for index, part in enumerate(parts)]
        payload = compact({'schema': 'pf-native-bulk-proof/1',
                           'chunks': [{'text': text} for text in records]}).encode('utf8')
        payload_sha = hashlib.sha256(payload).hexdigest()
        meta = delivery.common_meta(code, 'attachment-card' if is_primary else 'attachment-reference')
        meta.update(md_sha256=md_sha, native_payload_sha256=payload_sha,
                    profile_sha256=route['profile_sha256'], original_document_id=raw_entry['document_id'],
                    primary_dataset_id=primary['dataset_id'])
        if not is_primary:
            meta['primary_document_id'] = primary_document
        rows = delivery.publisher._document_lookup(ds, **({'id': entry['document_id']} if entry.get('document_id') else {'name': name}))
        rows = [row for row in rows if row.get('name') == name]
        if len(rows) > 1:
            raise ValueError('attachment native document is not unique')
        if not rows:
            if entry.get('document_id'):
                raise PendingDelivery('attachment native document is missing')
            did = delivery.send_once(entry, 'upload_intent', {'name': name, 'dataset_id': ds, 'sha256': payload_sha},
                                     lambda: client.upload_bytes(ds, name, payload))
            entry['document_id'] = did
            delivery.save()
        else:
            did = rows[0]['id']
            entry['document_id'] = did
        if client.download_bytes(ds, did) != payload:
            raise ValueError('attachment native payload differs from the processed Markdown')
        delivery.metadata(ds, did, meta, entry)
        native = entry.get('native_batch')
        if native and (native['pipeline_id'], native['payload_sha256']) != (pipeline, payload_sha):
            raise ValueError('attachment native pipeline or payload changed')
        if not native:
            row = delivery.document(ds, did)
            if row.get('pipeline_id') != pipeline or str(row.get('status')) != '0':
                delivery.send_once(entry, 'pipeline_intent', {'pipeline_id': pipeline},
                                   lambda: client.request('PATCH', f'/datasets/{ds}/documents/{did}',
                                                          {'pipeline_id': pipeline, 'enabled': 0}))
                row = delivery.document(ds, did)
            if row.get('pipeline_id') != pipeline or str(row.get('status')) != '0':
                raise ValueError('attachment native pipeline was not confirmed')
            native = entry['native_batch'] = {'pipeline_id': pipeline, 'payload_sha256': payload_sha, 'state': 'intent'}
            delivery.save()
        submission = client.ingest_state(ds, did, pipeline)
        if submission == 'intent' or (submission is None and native['state'] != 'intent'):
            raise EffectUnknown('attachment native submission requires reconciliation')
        if native['state'] != 'verified':
            terminal = client.ingest(ds, did, pipeline)
            native.update(state='terminal', terminal=terminal)
            delivery.save()
        observed = delivery.publisher.chunks(ds, did)
        if len(observed) != len(records) or {row['content'] for row in observed} != set(records):
            raise ValueError('attachment native chunks do not match the processed Markdown')
        by_content = {row['content']: row for row in observed}
        entry['chunks'] = {str(i): {'id': by_content[text]['id'],
                                   'content_sha256': hashlib.sha256(text.encode()).hexdigest()}
                           for i, text in enumerate(records)}
        native['state'] = 'verified'
        delivery.save()
        row = delivery.document(ds, did)
        if str(row.get('status')) != '1':
            delivery.send_once(entry, 'enable_intent', {'enabled': 1},
                               lambda: client.request('PATCH', f'/datasets/{ds}/documents/{did}', {'enabled': 1}))
        row = delivery.document(ds, did)
        observed = delivery.publisher.chunks(ds, did)
        if (str(row.get('status')) != '1' or any((row.get('meta_fields') or {}).get(k) != v for k, v in meta.items())
                or any(chunk.get('available') not in (True, 1, '1') for chunk in observed)
                or len(observed) != len(records)
                or {(chunk['id'], chunk['content']) for chunk in observed}
                   != {(entry['chunks'][str(i)]['id'], text) for i, text in enumerate(records)}):
            raise ValueError('attachment document is not enabled and readable')
        if (row.get('pipeline_id') != pipeline or str(row.get('run')) not in ('3', 'DONE')
                or float(row.get('progress', 0)) < 1):
            raise EffectUnknown('attachment native pipeline has not reached a verified terminal state')
        entry['retrieval_verified'] = False
        delivery.save()
        hits = client.request('POST', '/retrieval', {'question': (card['kind'] + ' ' + card['summary'])[:1000],
            'dataset_ids': [ds], 'document_ids': [did], 'similarity_threshold': 0.0,
            'vector_similarity_weight': 1.0, 'top_k': max(64, min(100, len(records))),
            'rerank_candidates_count': max(64, min(100, len(records))), 'page_size': min(100, len(records))})
        if not any(hit.get('id', hit.get('chunk_id')) in {row['id'] for row in observed}
                   for hit in hits.get('chunks', [])):
            raise PendingDelivery('attachment retrieval is not yet confirmed')
        entry.update(status='verified', retrieval_verified=True, md_sha256=md_sha,
                     projection_sha256=hashlib.sha256(projection.encode()).hexdigest(), chunk_count=len(records))
        delivery.save()
        if is_primary:
            primary_document = did
    delivery.verify_parents()
    delivery.state.update(status='verified', current_md_sha256=md_sha, receipt_path=str(path), model_requests_added=0)
    delivery.save()
    return delivery.state
