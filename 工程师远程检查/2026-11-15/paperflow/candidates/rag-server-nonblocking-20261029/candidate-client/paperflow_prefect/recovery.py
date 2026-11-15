"""Resume original RAG objects; no model, package generation or document creation."""
import copy
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote, unquote, urlsplit

from .records import read_json, write_json


class RecoveryPending(RuntimeError):
    """An original write is still unaccounted for; do not send it again."""


def _require_terminal_write(client, dataset, document, chunk_id, content_sha256):
    """A searchable chunk alone does not prove its old server operation ended."""
    identity = json.dumps(['paperflow-chunk-operation/1', dataset, document, chunk_id],
                          ensure_ascii=False, separators=(',', ':')).encode('utf8')
    operation_id = hashlib.sha256(identity).hexdigest()
    endpoint = '/datasets/{}/documents/{}/chunks/{}/operation'.format(
        quote(dataset, safe=''), quote(document, safe=''), quote(chunk_id, safe=''))
    try:
        operation = client.request('GET', endpoint)
    except Exception as exc:
        raise RecoveryPending('原切片服务端写入终态不可用，保留回执待核对') from exc
    if (not isinstance(operation, dict) or operation.get('state') != 'completed'
            or operation.get('dataset_id') != dataset or operation.get('document_id') != document
            or operation.get('chunk_id') != chunk_id or operation.get('operation_id') != operation_id
            or operation.get('content_sha256') != content_sha256
            or operation.get('counters_committed') is not True):
        raise RecoveryPending('原切片写入或计数终态未确认，不能启用或重发')


class OriginalObjectsClient:
    def __init__(self, client, objects, missing=()):
        self.client, self.objects, self.missing = client, set(objects), set(missing)

    def upload_bytes(self, *args, **kwargs):
        raise RecoveryPending('恢复不能新建或替换原文档')

    def download_bytes(self, dataset, document):
        if (dataset, document) not in self.objects: raise RecoveryPending('原文档范围改变')
        from paperflow.rag import RagPublisher
        return RagPublisher(self.client)._download(dataset, document)

    def download_image(self, image_id):
        from paperflow.rag import RagPublisher
        return RagPublisher(self.client).download_image(image_id)

    def request(self, method, endpoint, body=None):
        parsed = urlsplit(endpoint)
        if parsed.scheme or parsed.netloc or parsed.fragment: raise RecoveryPending('恢复请求范围改变')
        parts = [unquote(p) for p in parsed.path.strip('/').split('/')]
        if method == 'GET':
            return self.client.request(method, endpoint, body)
        if method == 'POST' and endpoint == '/retrieval':
            if not all((ds, did) in self.objects for ds in body['dataset_ids'] for did in body['document_ids']):
                raise RecoveryPending('检索不属于原文档')
            return self.client.request(method, endpoint, body)
        if len(parts) >= 4 and parts[0] == 'datasets' and parts[2] == 'documents':
            obj = parts[1], parts[3]
            if obj in self.objects:
                if method == 'PATCH' and (len(parts) == 4 or (len(parts) == 6 and parts[4] == 'chunks')):
                    return self.client.request(method, endpoint, body)
                if method == 'POST' and parts[4:] == ['chunks']:
                    key = (*obj, hashlib.sha256(body['content'].encode('utf8')).hexdigest())
                    if key not in self.missing: raise RecoveryPending('此切片已有写入意图，不得重发')
                    self.missing.remove(key)  # even a lost response consumes this permission
                    return self.client.request(method, endpoint, body)
        raise RecoveryPending('恢复只能操作原文档及明确未发送的切片')


def recover_publication(prepared, *, receipt_path, client):
    from preextract_publish import _paperflow, assert_publication_private_paths
    from paperflow.rag import RagPublisher, RemoteConflict, compact
    from paperflow.delivery import DeliveryPublisher

    receipt_path = Path(receipt_path)
    original = receipt_path.read_bytes()
    existing = read_json(receipt_path)
    task = prepared['task']
    packet = _paperflow(prepared['paperflow_root']).validate_package(
        prepared['package_path'], task, prepared['catalog'], strict_seal=True)
    assert_publication_private_paths(prepared, packet, receipt_path)
    if (existing.get('task') != task or existing.get('manifest_sha256') != packet['manifest_sha256']
            or packet['manifest_sha256'] != prepared['validation']['manifest_sha256']
            or prepared['validation']['embedding'].get('admission_verified') is not True
            or existing.get('status') not in {'pending', 'uncertain', 'complete'}):
        raise ValueError('原回执、封包、来源或准入身份不一致')
    targets = copy.deepcopy(existing.get('targets', {}))
    if set(targets) != {r['code'] for r in packet['routes']}:
        raise RecoveryPending('原目标回执不完整，不能推测并新建文档')
    if existing.get('transport') == 'native_body_batch/1':
        # Native ingestion does not yet expose a durable operation+counter
        # terminal proof. DONE or an exact chunk readback alone is insufficient.
        raise RecoveryPending('原批量写入终态与计数尚无可靠证明，保留原对象待核对')
    if existing.get('transport') != 'direct_chunks/1':
        raise RecoveryPending('未知交付通道，保留原记录待核对')

    reader = RagPublisher(client)
    records_by_role = {role: rows for role, rows in reader.records(packet).items() if rows}
    objects, missing, metadata_needed, documents = set(), set(), False, {}
    for route in packet['routes']:
        target = targets[route['code']]
        if set(target).intersection({'body', 'images'}) != set(records_by_role):
            raise RecoveryPending('原正文或图表回执不完整')
        for role, records in records_by_role.items():
            obj = target[role]
            dataset = route['paper_dataset_id' if role == 'body' else 'image_dataset_id']
            expected_identity = dict(paperflow_task=task['task_id'],
                paperflow_source_sha256=task['source_sha256'], paperflow_manifest_sha256=packet['manifest_sha256'],
                paperflow_revision=str(task['revision']), paperflow_specialty=route['code'], paperflow_role=role)
            if obj.get('dataset_id') != dataset or obj.get('identity') != expected_identity:
                raise RemoteConflict('原目标库或文档身份改变')
            name, display = reader.document_names(task, packet, route['code'], role)
            if obj.get('document_id'):
                docs = reader.documents(dataset, id=obj['document_id'])
                if len(docs) != 1 or docs[0]['id'] != obj['document_id']:
                    raise RemoteConflict('原文档缺失，恢复不能自动重建')
            else:
                docs = reader._document_lookup(dataset, name=name)
                if not docs and display != name:
                    docs = reader._document_lookup(dataset, name=display)
                if len(docs) != 1: raise RecoveryPending('原上传结果尚不能唯一读回')
            doc = docs[0]; did = doc['id']; fields = doc.get('meta_fields') or {}
            if doc.get('name') not in {name, display}: raise RemoteConflict('原文档名称改变')
            if fields:
                if any(fields.get(k) != v for k, v in expected_identity.items()):
                    raise RemoteConflict('远端文档来源或修订冲突')
            else:
                # The native stub preserves its original JSON field order.
                if reader._download(dataset, did) != reader.document_bytes(task, packet, role, obj['identity']):
                    raise RemoteConflict('上传响应丢失后的原文档内容不符')
                metadata_needed = True
            obj['document_id'] = did
            objects.add((dataset, did)); documents[route['code'], role] = doc
            expected = {reader.render(task, packet, row): row for row in records}
            chunks = reader.chunks(dataset, did)
            if len({c['content'] for c in chunks}) != len(chunks) or any(c['content'] not in expected for c in chunks):
                raise RemoteConflict('远端存在冲突或额外切片')
            observed = {c['content']: c for c in chunks}
            saved = obj.setdefault('chunks', {})
            if not set(saved) <= {row[0] for row in records}: raise RemoteConflict('回执包含未知切片')
            for content, row in expected.items():
                rid = row[0]; digest = hashlib.sha256(content.encode('utf8')).hexdigest()
                previous = saved.get(rid)
                if previous and previous.get('content_sha256') != digest:
                    raise RemoteConflict('原切片回执内容身份改变')
                remote = observed.get(content)
                if remote is None:
                    if previous is not None:
                        raise RecoveryPending('原切片已有写入意图但尚未读回，不能重发')
                    missing.add((dataset, did, digest))
                else:
                    if previous and previous.get('chunk_id') and previous['chunk_id'] != remote['id']:
                        raise RemoteConflict('原切片 ID 改变')
                    # Older "written" receipts can originate from a readback,
                    # rather than from the completion of the original request.
                    _require_terminal_write(client, dataset, did, remote['id'], digest)
                    saved[rid] = dict(status='written', chunk_id=remote['id'],
                        image_id=remote.get('image_id'), content_sha256=digest)
    for route in packet['routes']:
        target = targets[route['code']]
        fields = documents[route['code'], 'body'].get('meta_fields') or {}
        for key, value in (('two_pass_profile_json', packet['profile']), ('two_pass_tree_json', packet['tree']),
                           ('rag_mid_assets_json', packet['assets']), ('paperflow_image_associations_json', target.get('images', {}))):
            if key not in fields: metadata_needed = True
            elif fields[key] != compact(value): raise RemoteConflict('远端结构或图表关联冲突')
    if receipt_path.read_bytes() != original: raise RecoveryPending('回执在核对期间改变')
    backup = receipt_path.with_name(receipt_path.stem + '.before-resume.' + str(time.time_ns()) + '.json')
    with backup.open('xb') as stream: stream.write(original)
    state = copy.deepcopy(existing)
    state.update(status='pending', stage='readback', targets=targets,
                 recovery=dict(previous_receipt=str(backup), original_objects_only=True))
    state.pop('error_type', None)
    guarded = OriginalObjectsClient(client, objects, missing)
    def persist(receipts):
        state['targets'] = receipts; write_json(receipt_path, state)
    publisher = DeliveryPublisher(guarded, on_change=persist)
    try:
        persist(targets)
        if missing or metadata_needed:
            state['stage'] = 'publishing'; persist(targets)
            publisher.publish(task, packet, targets)
        state['stage'] = 'readback'; persist(targets)
        publisher.verify(task, packet, targets)
        state['stage'] = 'activating'; persist(targets)
        publisher.set_enabled(targets, True)
        state['stage'] = 'retrieval'; persist(targets)
        publisher.verify_retrieval(task, packet, targets)
        state.update(status='complete', stage='verified', source_retained=True,
                     cleanup='not_requested', last_remote_verified_at=time.time())
        persist(targets)
        return state
    except BaseException as exc:
        state.update(status='uncertain', error_type=type(exc).__name__); persist(targets)
        raise
