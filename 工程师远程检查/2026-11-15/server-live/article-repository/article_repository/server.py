import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import logging
import os
from pathlib import Path

from haystack.utils import Secret
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from paperflow_qdrant.embedding import OllamaEmbedder
from paperflow_qdrant.library import Library
from .api import create_app
from .catalog import Catalog
from .files import Files
from .imports import Importer
from .sync import RevisionSync

logger=logging.getLogger('article_repository')

def app_factory():
    catalog=Catalog(os.environ['ARTICLE_DATABASE_URL'],schema=os.getenv('ARTICLE_DATABASE_SCHEMA','public'))
    catalog.initialize()
    catalog.set_majors(json.loads(Path('/app/majors.json').read_text())['majors'])
    store=QdrantDocumentStore(url=os.environ['QDRANT_URL'],api_key=Secret.from_env_var('QDRANT_API_KEY'),
        index=os.getenv('QDRANT_COLLECTION','paperflow_v1'),embedding_dim=1024,
        write_batch_size=128,progress_bar=False,wait_result_from_api=True,timeout=120,on_disk_payload=True)
    library=Library(store,OllamaEmbedder(os.getenv('EMBEDDING_URL','http://172.17.0.1:11436'),concurrency=1))
    sync=RevisionSync(library)
    files=Files(os.environ['ARTICLE_NAS_ROOT'],username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
    importer=Importer(catalog,sync,files,os.environ['PREFECT_API_URL'])

    def bootstrap():
        path=Path('/app/legacy-records.json')
        digest=hashlib.sha256(path.read_bytes()).hexdigest()
        with catalog.connect() as conn:
            previous=conn.execute("SELECT status FROM source_status WHERE name='历史目录'").fetchone()
        if not previous or previous['status'].get('sha256')!=digest:
            records=json.loads(path.read_text())
            for row in records: catalog.register(row)
            catalog.source_status('历史目录',{'folders':len(records),'sha256':digest,
                'scope':'旧看板已保存且可定位来源文件夹的核验记录，不代表旧服务全库'})

    def observe_batch(cursor):
        with catalog.connect() as conn:
            rows=conn.execute("SELECT id,base->'files' files FROM article WHERE id>%s ORDER BY id LIMIT 100",(cursor,)).fetchall()
        for row in rows:
            observed=files.observe(row['files'])
            if observed!=row['files']: catalog.observe_files(row['id'],observed)
        return rows[-1]['id'] if rows else ''

    async def refresh():
        await asyncio.to_thread(bootstrap)
        while True:
            try: await asyncio.to_thread(importer.refresh)
            except Exception as exc:
                logger.exception('Article source import failed')
                await asyncio.to_thread(catalog.source_status,'新链路',{'status':'unavailable','error':type(exc).__name__,
                    'scope':'保留已登记文章，本轮新增接入未完成'})
            await asyncio.sleep(60)

    async def refresh_files():
        cursor=''
        while True:
            try: cursor=await asyncio.to_thread(observe_batch,cursor)
            except Exception:
                logger.exception('NAS availability refresh failed')
                await asyncio.sleep(60)
            await asyncio.sleep(1 if cursor else 300)

    @asynccontextmanager
    async def lifespan(app):
        tasks=[asyncio.create_task(refresh()),asyncio.create_task(refresh_files())] if os.getenv('ARTICLE_IMPORT_ENABLED','true')=='true' else []
        yield
        for task in tasks: task.cancel()
        for task in tasks:
            try: await task
            except asyncio.CancelledError: pass

    return create_app(catalog,files,sync,site='/app/article-site',lifespan=lifespan)

