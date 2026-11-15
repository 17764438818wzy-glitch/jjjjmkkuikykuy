"""Local stateful RAG API double; persists writes before injected response loss."""
import base64
import copy
from urllib.parse import parse_qs, unquote, urlsplit


class LocalRag:
    base_url='http://127.0.0.1:9'
    def __init__(self):
        self.docs, self.chunks, self.images = {}, {}, {}
        self.uploads = self.posts = 0
        self.lose_upload = self.lose_chunk = self.drop_chunk = False
        self.calls = []

    def upload_bytes(self, ds, name, content):
        self.uploads += 1; did = f'doc-{self.uploads}'
        self.docs[ds, did] = dict(id=did, name=name, status='0', meta_fields={}, raw=content)
        self.chunks[ds, did] = []
        self.calls.append(('UPLOAD', ds, did))
        if self.lose_upload:
            self.lose_upload = False; raise TimeoutError('upload response lost')
        return did

    def download_bytes(self, ds, did): return self.docs[ds, did]['raw']
    def download_image(self, image_id): return self.images[image_id]

    def request(self, method, endpoint, body=None):
        self.calls.append((method, endpoint, copy.deepcopy(body)))
        url = urlsplit(endpoint); parts = [unquote(p) for p in url.path.strip('/').split('/')]
        query = parse_qs(url.query)
        if endpoint == '/retrieval':
            return {'chunks': [copy.deepcopy(c) for (ds, did), rows in self.chunks.items()
                if ds in body['dataset_ids'] and did in body['document_ids']
                and self.docs[ds, did]['status'] == '1' for c in rows if c['available']]}
        ds = parts[1]
        if len(parts) == 3 and parts[2] == 'documents' and method == 'GET':
            docs = [copy.deepcopy(d) for (dataset, _), d in self.docs.items() if dataset == ds]
            for key in ('id', 'name'):
                if key in query: docs = [d for d in docs if d[key] == query[key][0]]
            page = int(query.get('page', ['1'])[0]); size = int(query.get('page_size', ['100'])[0])
            return {'docs': docs[(page-1)*size:page*size]}
        did = parts[3]; key = ds, did
        if len(parts) == 4 and method == 'PATCH':
            self.docs[key].update(copy.deepcopy(body))
            if 'enabled' in body:
                self.docs[key]['status'] = str(body['enabled'])
                for chunk in self.chunks[key]: chunk['available'] = bool(body['enabled'])
            return {}
        if len(parts) == 5 and parts[-1] == 'chunks':
            if method == 'GET':
                rows = self.chunks[key]; page = int(query.get('page', ['1'])[0]); size = int(query.get('page_size', ['100'])[0])
                return {'chunks': copy.deepcopy(rows[(page-1)*size:page*size]), 'total': len(rows)}
            if method == 'POST':
                self.posts += 1
                if self.drop_chunk:
                    self.drop_chunk = False; raise TimeoutError('unknown chunk outcome')
                row = dict(id=f'chunk-{self.posts}', document_id=did, content=body['content'],
                           available=self.docs[key]['status'] == '1')
                if body.get('image_base64'):
                    row['image_id'] = f'image-{self.posts}'
                    self.images[row['image_id']] = base64.b64decode(body['image_base64'])
                self.chunks[key].append(row)
                if self.lose_chunk:
                    self.lose_chunk = False; raise TimeoutError('chunk response lost')
                return {'chunk': copy.deepcopy(row)}
        if len(parts) == 6 and method == 'PATCH':
            row = next(c for c in self.chunks[key] if c['id'] == parts[-1]); row.update(body); return {}
        raise AssertionError((method, endpoint))
