"""Immutable folder batches using Haystack's official Qdrant integration."""
from dataclasses import replace
from uuid import UUID

from haystack import Document
from haystack_integrations.components.retrievers.qdrant import QdrantEmbeddingRetriever
from haystack_integrations.document_stores.qdrant.converters import convert_id, convert_haystack_documents_to_qdrant_points
from qdrant_client.models import UpdateMode
from .identity import stable_id,fingerprint


def condition(field, value, operator='=='):
    return {'field': 'meta.' + field, 'operator': operator, 'value': value}


class InsertOnlyWriter:
    """Haystack's writer does not expose Qdrant's native insert_only operation."""
    def __init__(self,store): self.store=store

    def run(self,*,documents):
        for start in range(0,len(documents),self.store.write_batch_size):
            points=convert_haystack_documents_to_qdrant_points(
                documents[start:start+self.store.write_batch_size],use_sparse_embeddings=False)
            self.store._client.upsert(collection_name=self.store.index,points=points,
                update_mode=UpdateMode.INSERT_ONLY,wait=True)
        return {'documents_written':len(documents)}


class Library:
    def __init__(self, store, embedder, *, embedding_batch_size=16):
        self.store, self.embedder = store, embedder
        self.embedding_batch_size = embedding_batch_size
        self.writer = InsertOnlyWriter(store)
        self.retriever = QdrantEmbeddingRetriever(document_store=store)
        # Initialize once before worker threads; collection creation is not per folder.
        self.store.count_documents()

    def _existing(self, ids):
        result = {}
        for start in range(0, len(ids), 128):
            result.update({d.id: d for d in self.store.get_documents_by_id(ids[start:start+128])})
        return result

    def ingest(self, batch):
        folder = batch['folder_id']
        docs = batch['documents']
        if not folder or not docs or len({d['id'] for d in docs}) != len(docs):
            raise ValueError('invalid folder or duplicate documents')
        points, manifests = [], []
        for doc in docs:
            UUID(doc['id'])
            records = doc['records']
            if (not records or not doc['dataset_ids'] or
                    len({r['id'] for r in records}) != len(records)):
                raise ValueError('empty document or duplicate records')
            common = {'folder_id': folder, 'document_id': doc['id'],
                      'dataset_ids': doc['dataset_ids'], 'document_name': doc['name']}
            for order, record in enumerate(records):
                if not isinstance(record.get('content'), str) or not record['content'].strip():
                    raise ValueError('empty record content')
                point_id = stable_id(doc['id'], record['id'])
                meta = {**common, 'kind': 'chunk', 'order': order, 'record': record,
                        'fingerprint': fingerprint([common, record]), 'active': True}
                points.append(Document(id=point_id, content=record['content'], meta=meta))
            manifests.append(Document(id=stable_id(doc['id'], 'manifest'), content=doc['name'],
                meta={**common, 'kind': 'manifest', 'active': True,
                      'fingerprint': fingerprint([folder, doc]), 'document': doc}))
        all_points = points + manifests
        batch_fingerprint=fingerprint(batch)
        # Pin one immutable batch to the folder before creating any content points.
        # insert_only and readback make concurrent differing requests choose one owner.
        binding_id=stable_id(folder,'binding')
        binding=Document(id=binding_id,content=folder,embedding=[0.0]*self.store.embedding_dim,
            meta={'kind':'folder_binding','folder_id':folder,'fingerprint':batch_fingerprint})
        self.writer.run(documents=[binding])
        owner=self._existing([binding_id]).get(binding_id)
        if owner is None or owner.meta.get('fingerprint')!=batch_fingerprint:
            raise ValueError('immutable folder binding conflict')
        commit_id=stable_id(folder,'complete')
        prior_commit=self._existing([commit_id]).get(commit_id)
        if prior_commit and prior_commit.meta.get('fingerprint')!=batch_fingerprint:
            raise ValueError('immutable folder conflict')
        existing = self._existing([p.id for p in all_points])
        for point in all_points:
            old = existing.get(point.id)
            if old and old.meta.get('fingerprint') != point.meta['fingerprint']:
                raise ValueError('immutable document conflict: ' + point.id)
        missing = [p for p in points if p.id not in existing]
        embedded = {}
        for start in range(0, len(missing), self.embedding_batch_size):
            part = missing[start:start+self.embedding_batch_size]
            vectors = self.embedder.encode([p.content for p in part])
            if len(vectors) != len(part):
                raise ValueError('embedding batch returned wrong count')
            embedded.update({p.id: v for p, v in zip(part, vectors, strict=True)})
            self.writer.run(documents=[replace(p, embedding=v) for p, v in zip(part, vectors, strict=True)])
        # Reuse a real existing vector for manifest storage. Manifests are excluded from search.
        for manifest in manifests:
            if manifest.id in existing:
                continue
            first = next(p for p in points if p.meta['document_id'] == manifest.meta['document_id'])
            if first.id not in embedded:
                raw = self.store._client.retrieve(self.store.index, [convert_id(first.id)], with_vectors=True)[0]
                vector = raw.vector
                vector = vector['dense'] if isinstance(vector, dict) else vector
            else:
                vector = embedded[first.id]
            self.writer.run(documents=[replace(manifest, embedding=vector)])
        observed = self._existing([p.id for p in all_points])
        if (len(observed) != len(all_points) or
                any(observed[p.id].meta.get('fingerprint') != p.meta['fingerprint'] for p in all_points)):
            raise RuntimeError('batch readback incomplete')
        # Publish a single completion point after all records and manifests are verified.
        # Readers require this point; a multi-point Qdrant write is not an atomic folder commit.
        raw=self.store._client.retrieve(self.store.index,[convert_id(points[0].id)],with_vectors=True)[0]
        vector=raw.vector['dense'] if isinstance(raw.vector,dict) else raw.vector
        commit=Document(id=commit_id,content=folder,embedding=vector,
            meta={'kind':'folder_complete','folder_id':folder,'active':True,'fingerprint':batch_fingerprint})
        self.writer.run(documents=[commit])
        observed_commit=self._existing([commit_id]).get(commit_id)
        if observed_commit is None or observed_commit.meta.get('fingerprint')!=batch_fingerprint:
            raise RuntimeError('folder completion unconfirmed')
        return {'status': 'complete', 'backend': 'qdrant', 'folder_id': folder,
                'document_ids': [d['id'] for d in docs], 'chunk_count': len(points)}

    def _manifest(self, dataset_id, document_id):
        rows = self.store.get_documents_by_id([stable_id(document_id, 'manifest')])
        if not rows or not rows[0].meta.get('active') or dataset_id not in rows[0].meta['dataset_ids']:
            return None
        if not self._existing([stable_id(rows[0].meta['folder_id'],'complete')]):
            return None
        return rows[0].meta['document']

    def document(self, dataset_id, document_id):
        doc = self._manifest(dataset_id, document_id)
        if doc is None:
            return None
        return self._document_view(dataset_id,doc)

    @staticmethod
    def _document_view(dataset_id,doc):
        fields = {**doc['meta_fields'], **doc.get('dataset_meta_fields', {}).get(dataset_id, {})}
        return {'id': doc['id'], 'dataset_id': dataset_id, 'name': doc['name'],
                'meta_fields': fields, 'status': '1', 'chunk_count': len(doc['records']),
                'backend': 'qdrant'}

    def documents(self, dataset_id, *, name=None):
        filters=[condition('active',True),condition('kind','manifest'),condition('dataset_ids',[dataset_id],'in')]
        if name: filters.append(condition('document_name',name))
        rows=self.store.filter_documents({'operator':'AND','conditions':filters})
        complete=self._existing(list({stable_id(r.meta['folder_id'],'complete') for r in rows}))
        return [self._document_view(dataset_id,r.meta['document']) for r in rows
                if stable_id(r.meta['folder_id'],'complete') in complete]

    def original(self, dataset_id, document_id):
        doc = self._manifest(dataset_id, document_id)
        if doc is None:
            raise KeyError('document not found in dataset')
        return doc['original']

    @staticmethod
    def _chunk(point, dataset_id):
        meta = point.meta
        record = meta['record']
        return {'id': point.id, 'document_id': meta['document_id'], 'dataset_id': dataset_id,
                'document_keyword': meta['document_name'], 'content': point.content,
                'available': True, 'image_id': record.get('image_id'),
                'record_id': record['id'], 'positions': record.get('positions', []),
                'section_ids': record.get('section_ids', []), 'backend': 'qdrant'}

    def chunks(self, dataset_id, document_id, *, ids=None):
        doc = self._manifest(dataset_id, document_id)
        if doc is None:
            return []
        wanted = [stable_id(document_id, r['id']) for r in doc['records']]
        if ids is not None:
            wanted = [p for p in wanted if p in ids]
        rows = self._existing(wanted)
        return [self._chunk(rows[p], dataset_id) for p in wanted if p in rows]

    def search(self, question, dataset_ids, *, document_ids=None, limit=8):
        if not question.strip() or not dataset_ids or not 1 <= limit <= 1000:
            raise ValueError('query, datasets and valid limit are required')
        filters = [condition('active', True), condition('kind', 'chunk'),
                   condition('dataset_ids', dataset_ids, 'in')]
        if document_ids:
            filters.append(condition('document_id', document_ids, 'in'))
        vector = self.embedder.encode([question])[0]
        requested=limit
        while True:
            rows=self.retriever.run(query_embedding=vector,
                filters={'operator':'AND','conditions':filters},top_k=requested)['documents']
            complete=self._existing(list({stable_id(r.meta['folder_id'],'complete') for r in rows}))
            ready=[r for r in rows if stable_id(r.meta['folder_id'],'complete') in complete]
            if len(ready)>=limit or len(rows)<requested:
                return [self._chunk(r,next(ds for ds in dataset_ids if ds in r.meta['dataset_ids'])) for r in ready[:limit]]
            if requested>=10000:
                raise RuntimeError('too many uncommitted candidates; search cannot be completed')
            requested=min(requested*4,10000)
