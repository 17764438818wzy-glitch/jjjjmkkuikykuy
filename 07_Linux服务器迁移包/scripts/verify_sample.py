"""Read back target PG rows and raw Qdrant points without invoking app/model APIs."""
import argparse
import json
import os
from pathlib import Path

import httpx
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from sample_tools import TABLE_ORDER,compact,file_hash,header,normal_id,normalized_row,points,safe_path,verify_offline

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--sample',default='/sample',type=Path);args=parser.parse_args()
    report=verify_offline(args.sample)
    catalog=json.loads((args.sample/'catalog.json').read_text(encoding='utf-8'))
    for item in json.loads((args.sample/'object-hashes.json').read_text(encoding='utf-8'))['files']:
        path=safe_path(Path(os.environ['OBJECT_ROOT']),item['path'].removeprefix('objects/'))
        if not path.is_file() or path.stat().st_size!=item['bytes'] or file_hash(path)!=item['sha256']:
            raise ValueError('Target object differs')
    with psycopg.connect(os.environ['ARTICLE_DATABASE_URL'],row_factory=dict_row) as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        for name in TABLE_ORDER:
            table=catalog['tables'][name]
            query=sql.SQL('SELECT {} FROM {}').format(sql.SQL(',').join(map(sql.Identifier,table['columns'])),sql.Identifier(name))
            actual=conn.execute(query).fetchall()
            if sorted(compact(normalized_row(r)) for r in actual)!=sorted(compact(normalized_row(r)) for r in table['rows']):
                raise ValueError('Target row mismatch: '+name)
        if conn.execute('SELECT count(*) n FROM source_status').fetchone()['n']:
            raise ValueError('Unexpected source_status rows')
    collection=header(args.sample)['collection'];maximum_error=0.0;batch=[]
    with httpx.Client(base_url=os.environ['QDRANT_URL'],headers={'api-key':os.environ['QDRANT_API_KEY']},timeout=120,trust_env=False) as client:
        def compare():
            nonlocal maximum_error
            if not batch:return
            response=client.post('/collections/'+collection+'/points',json={'ids':[p['id'] for p in batch],'with_payload':True,'with_vector':True})
            response.raise_for_status();actual={normal_id(p['id']):p for p in response.json()['result']}
            if len(actual)!=len(batch):raise ValueError('Target point missing')
            for expected in batch:
                observed=actual[normal_id(expected['id'])]
                if compact(expected['payload'])!=compact(observed['payload']):raise ValueError('Point payload mismatch')
                a,b=expected['vector'],observed['vector']
                a=a if isinstance(a,dict) else {'dense':a};b=b if isinstance(b,dict) else {'dense':b}
                if a.keys()!=b.keys() or any(len(a[k])!=len(b[k]) for k in a):raise ValueError('Vector shape mismatch')
                error=max(abs(x-y) for k in a for x,y in zip(a[k],b[k],strict=True))
                maximum_error=max(maximum_error,error)
                if error>2e-6:raise ValueError('Stored vector changed beyond normalization tolerance')
            batch.clear()
        for point in points(args.sample):
            batch.append(point)
            if len(batch)>=64:compare()
        compare()
        count=client.post('/collections/'+collection+'/points/count',json={'exact':True});count.raise_for_status()
        if count.json()['result']['count']!=report['points']:raise ValueError('Unexpected extra points')
    print(json.dumps({'target_verification':'passed','vector_max_abs_difference':maximum_error,**report},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
