"""Explicit restore into the isolated, empty Compose instance only. No model calls."""
import argparse
import json
import os
import shutil
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from sample_tools import JSON_COLUMNS,TABLE_ORDER,header,points,safe_path,verify_offline

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--sample',default='/sample',type=Path)
    parser.add_argument('--apply-to-empty-target',action='store_true')
    args=parser.parse_args()
    if not args.apply_to_empty_target:parser.error('Explicit --apply-to-empty-target is required')
    db=os.environ['ARTICLE_DATABASE_URL'];qurl=os.environ['QDRANT_URL'].rstrip('/')
    if qurl!='http://qdrant:6333' or urlparse(db).hostname!='article-postgres':
        raise ValueError('Restore is restricted to the isolated Compose service DNS names')
    if os.getenv('ARTICLE_IMPORT_ENABLED')!='false':raise ValueError('Automatic imports must be disabled')
    objects=Path(os.environ['OBJECT_ROOT'])
    if objects!=Path('/data/objects') or not objects.is_dir() or any(objects.iterdir()):
        raise ValueError('Target objects directory must be the empty isolated mount /data/objects')
    report=verify_offline(args.sample)
    catalog=json.loads((args.sample/'catalog.json').read_text(encoding='utf-8'));info=header(args.sample)
    collection=info['collection']
    if collection!=os.environ.get('QDRANT_COLLECTION','paperflow_v1'):raise ValueError('Collection name mismatch')
    with psycopg.connect(db) as conn,httpx.Client(base_url=qurl,headers={'api-key':os.environ['QDRANT_API_KEY']},timeout=180,trust_env=False) as client:
        # Guard every existing application table before any schema or data writes.
        for name in TABLE_ORDER+['source_status']:
            if conn.execute('SELECT to_regclass(%s)',('public.'+name,)).fetchone()[0] is not None:
                n=conn.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier(name))).fetchone()[0]
                if n:raise ValueError('Target database is not empty: '+name)
        existing=client.get('/collections/'+collection)
        if existing.status_code!=404:
            existing.raise_for_status()
            raise ValueError('Target collection already exists; use a fresh state directory')
        for item in json.loads((args.sample/'object-hashes.json').read_text(encoding='utf-8'))['files']:
            source=safe_path(args.sample,item['path'])
            dest=safe_path(objects,item['path'].removeprefix('objects/'))
            dest.parent.mkdir(parents=True,exist_ok=True)
            if dest.exists():raise ValueError('Object target unexpectedly exists')
            shutil.copy2(source,dest)
        config=info['collection_info']['config'];params=config['params']
        create={k:params[k] for k in ['vectors','shard_number','replication_factor','write_consistency_factor','on_disk_payload'] if k in params}
        for key in ['hnsw_config','optimizer_config','wal_config','quantization_config','strict_mode_config']:
            if key in config and config[key] is not None:
                create['optimizers_config' if key=='optimizer_config' else key]=config[key]
        client.put('/collections/'+collection,json=create).raise_for_status()
        batch=[]
        def flush():
            if batch:
                client.put('/collections/'+collection+'/points',params={'wait':'true'},json={'points':batch}).raise_for_status()
                batch.clear()
        for p in points(args.sample):
            batch.append(p)
            if len(batch)>=64:flush()
        flush()
        for key,value in info['collection_info'].get('payload_schema',{}).items():
            schema=value.get('params') or value['data_type']
            client.put('/collections/'+collection+'/index',params={'wait':'true'},json={'field_name':key,'field_schema':schema}).raise_for_status()
        conn.execute((args.sample/'schema.sql').read_text(encoding='utf-8'))
        for name in TABLE_ORDER:
            table=catalog['tables'][name];columns=table['columns']
            statement=sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(sql.Identifier(name),
                sql.SQL(',').join(map(sql.Identifier,columns)),sql.SQL(',').join(sql.Placeholder() for _ in columns))
            values=[[Jsonb(row[k]) if k in JSON_COLUMNS and row[k] is not None else row[k] for k in columns] for row in table['rows']]
            if values:
                with conn.cursor() as cur:cur.executemany(statement,values)
        conn.execute("SELECT setval(pg_get_serial_sequence('article_rating_discovery','sequence'), coalesce(max(sequence),1), max(sequence) IS NOT NULL) FROM article_rating_discovery")
        # Global source_status statistics and Prefect task state intentionally have no restore step.
    print(json.dumps({'restored':report,'next':'Run verify_sample.py before starting repository/gateway'},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
