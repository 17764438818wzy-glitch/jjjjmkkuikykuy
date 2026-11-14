import os
from haystack.utils import Secret
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from .api import create_app
from .embedding import OllamaEmbedder
from .library import Library


def app_factory():
    store=QdrantDocumentStore(url=os.environ['QDRANT_URL'],
        api_key=Secret.from_env_var('QDRANT_API_KEY'),
        index=os.getenv('QDRANT_COLLECTION','paperflow_v1'),embedding_dim=1024,
        write_batch_size=128,progress_bar=False,wait_result_from_api=True,
        timeout=120,on_disk_payload=True,
        payload_fields_to_index=[{'field_name':'meta.'+name,'field_schema':kind}
            for name,kind in [('dataset_ids','keyword'),('document_id','keyword'),
                              ('folder_id','keyword'),('kind','keyword'),('active','bool')]])
    embedder=OllamaEmbedder(os.getenv('EMBEDDING_URL','http://172.17.0.1:11436'),
        concurrency=int(os.getenv('EMBEDDING_CONCURRENCY','4')))
    return create_app(Library(store,embedder,embedding_batch_size=int(os.getenv('EMBEDDING_BATCH_SIZE','16'))),
        os.getenv('OBJECT_ROOT','/data/objects'),token=os.environ['PAPERFLOW_RAG_API_KEY'])
