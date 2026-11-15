import copy
import hashlib

import pytest

from paperflow.package import validate_package
from paperflow.rag import RagPublisher, RemoteConflict


class MemoryRag:
    """协议替身用于故障注入；另有真实 HTTP 和现有服务器验收。"""
    def __init__(self):
        self.docs = {}
        self.chunks = {}
        self.writes = 0
        self.lose_chunk_response = False

    def upload_bytes(self, ds, name, data):
        did = f'doc{len(self.docs)+1}'
        self.docs[(ds, did)] = dict(id=did, name=name, meta_fields={}, status='1', chunk_count=0)
        self.chunks[did] = []
        self.writes += 1
        return did

    def request(self, method, endpoint, body=None, content_type=None):
        from urllib.parse import urlsplit, parse_qs
        parts = urlsplit(endpoint).path.strip('/').split('/')
        query = parse_qs(urlsplit(endpoint).query)
        if method=='POST' and parts==['retrieval']:
            return {'chunks':[copy.deepcopy(c) for rows in self.chunks.values() for c in rows
                              if c['document_id'] in body['document_ids']]}
        ds = parts[1]
        if method == 'GET' and len(parts) == 3:
            rows = [copy.deepcopy(d) for (s, _), d in self.docs.items() if s == ds]
            if 'name' in query:
                rows = [d for d in rows if d['name'] == query['name'][0]]
            if 'id' in query:
                rows = [d for d in rows if d['id'] == query['id'][0]]
            page, size = int(query.get('page', [1])[0]), int(query.get('page_size', [100])[0])
            assert size <= 100
            return {'docs': rows[(page-1)*size:page*size], 'total': len(rows)}
        did = parts[3]
        if method == 'PATCH' and len(parts) == 4:
            self.docs[(ds,did)].update(body)
            if 'enabled' in body:
                self.docs[(ds,did)]['status'] = '1' if body['enabled'] else '0'
            return copy.deepcopy(self.docs[(ds,did)])
        if method == 'GET' and parts[-1] == 'chunks':
            page = int(query.get('page', [1])[0])
            rows = self.chunks[did]
            return {'chunks': copy.deepcopy(rows[(page-1)*100:page*100]), 'total': len(rows)}
        if method == 'POST' and parts[-1] == 'chunks':
            row = dict(id=f'c{len(self.chunks[did])+1}', document_id=did, content=body['content'],
                       image_id='stored-image' if body.get('image_base64') else '')
            self.chunks[did].append(row)
            self.docs[(ds,did)]['chunk_count'] += 1
            self.writes += 1
            if self.lose_chunk_response:
                self.lose_chunk_response = False
                raise TimeoutError('response lost after successful server write')
            return {'chunk': copy.deepcopy(row)}
        if method == 'POST' and parts[-1] == 'retrieval':
            return {'chunks': [copy.deepcopy(c) for rows in self.chunks.values() for c in rows
                               if c['document_id'] in body['document_ids']]}
        raise AssertionError((method, endpoint))


def test_lost_success_response_is_reconciled_without_duplicate_chunk(package_factory):
    root, task, catalog = package_factory()
    task['revision'] = 1
    packet = validate_package(root, task, catalog)
    client = MemoryRag()
    publisher = RagPublisher(client)
    receipts = {}
    client.lose_chunk_response = True
    with pytest.raises(TimeoutError):
        publisher.publish(task, packet, receipts)
    publisher.publish(task, packet, receipts)
    assert len(client.docs) == 1
    # 正文 + 学习切片 + 画像 + 章节树 + 图表说明。
    assert len(next(iter(client.chunks.values()))) == 5
    publisher.verify(task, packet, receipts)
    assert receipts['120201']['verified'] is True


def test_name_collision_with_different_identity_is_not_reused(package_factory):
    root, task, catalog = package_factory()
    task['revision'] = 1
    packet = validate_package(root, task, catalog)
    client = MemoryRag()
    publisher = RagPublisher(client)
    publisher.publish(task, packet, {})
    next(iter(client.docs.values()))['meta_fields']['paperflow_source_sha256'] = 'f' * 64
    with pytest.raises(RemoteConflict):
        publisher.publish(task, packet, {})


def test_image_association_readback_compares_structure_after_sqlite_canonicalization(package_factory):
    import json
    from paperflow.rag import compact
    root,task,catalog=package_factory();task['revision']=1;packet=validate_package(root,task,catalog)
    client=MemoryRag();publisher=RagPublisher(client);receipts={}
    publisher.publish(task,packet,receipts)
    association={'identity':{'z':'last','a':'first'},'dataset_id':'image-ds','chunks':{}}
    receipts['120201']['images']=association
    next(iter(client.docs.values()))['meta_fields']['paperflow_image_associations_json']=compact(association)
    restored=json.loads(json.dumps(receipts,sort_keys=True))
    publisher.verify(task,packet,restored)


@pytest.mark.parametrize('field,value',[('image_id','wrong-image'),('chunk_id','wrong-chunk'),('content_sha256','f'*64)])
def test_image_association_must_match_actual_business_record(package_factory,field,value):
    import io
    from PIL import Image
    from paperflow.package import load,save,seal_package
    from paperflow.rag import compact
    root,task,catalog=package_factory();task['revision']=1
    stream=io.BytesIO();Image.new('RGB',(16,16),'white').save(stream,format='PNG');data=stream.getvalue()
    (root/'image.png').write_bytes(data)
    assets=load(root/'assets.json');assets['items'][0].update(storage='image',file='image.png',sha256=hashlib.sha256(data).hexdigest())
    save(root/'assets.json',assets);seal_package(root,task,1);packet=validate_package(root,task,catalog)
    client=MemoryRag();client.download_image=lambda image_id:data
    publisher=RagPublisher(client);receipts={};publisher.publish(task,packet,receipts)
    receipts['120201']['images']['chunks']['asset_fig1'][field]=value
    body=receipts['120201']['body'];client.docs[(body['dataset_id'],body['document_id'])]['meta_fields']['paperflow_image_associations_json']=compact(receipts['120201']['images'])
    with pytest.raises(RemoteConflict,match='record|receipt|association'):publisher.verify(task,packet,receipts)


def test_readback_requires_exact_chunk_content(package_factory):
    root, task, catalog = package_factory()
    task['revision'] = 1
    packet = validate_package(root, task, catalog)
    client = MemoryRag()
    publisher = RagPublisher(client)
    receipts = {}
    publisher.publish(task, packet, receipts)
    next(iter(client.chunks.values()))[0]['content'] = 'wrong body with correct ID'
    with pytest.raises(RemoteConflict):
        publisher.verify(task, packet, receipts)


def test_body_index_can_pass_without_each_auxiliary_category_ranking(package_factory):
    class Ranked(MemoryRag):
        def __init__(self):super().__init__();self.queries=[]
        def request(self,method,endpoint,body=None,content_type=None):
            result=super().request(method,endpoint,body,content_type)
            if endpoint=='/retrieval':
                self.queries.append(body['question'])
                rows=result['chunks']
                result['chunks']=[r for r in rows if body['question']==r['content'][:1000]] or rows[:1]
            return result
    root,task,catalog=package_factory();task['revision']=1;packet=validate_package(root,task,catalog)
    client=Ranked();publisher=RagPublisher(client);receipts={}
    publisher.publish(task,packet,receipts);publisher.verify_retrieval(task,packet,receipts)
    assert receipts['120201']['retrieval_verified'] and len(client.queries)==1


def test_missing_body_index_still_fails_after_targeted_probe(package_factory):
    class Missing(MemoryRag):
        def request(self,method,endpoint,body=None,content_type=None):
            result=super().request(method,endpoint,body,content_type)
            if endpoint=='/retrieval':result['chunks']=[]
            return result
    root,task,catalog=package_factory();task['revision']=1;packet=validate_package(root,task,catalog)
    publisher=RagPublisher(Missing());receipts={};publisher.publish(task,packet,receipts)
    with pytest.raises(ConnectionError):publisher.verify_retrieval(task,packet,receipts)


def test_large_profile_is_preserved_as_metadata_without_one_huge_embedding_record(package_factory):
    import json
    root,task,catalog=package_factory();task['revision']=1
    packet=validate_package(root,task,catalog)
    packet['profile']['source_quality_notes']=['long-note'*10000]
    client=MemoryRag();publisher=RagPublisher(client)
    indexed=json.loads(next(r[1] for r in publisher.records(packet)['body'] if r[0]=='__profile'))
    assert indexed['article']==packet['profile']['article']
    assert 'source_quality_notes' not in indexed
    publisher.publish(task,packet,{})
    stored=json.loads(next(iter(client.docs.values()))['meta_fields']['two_pass_profile_json'])
    assert stored==packet['profile']
