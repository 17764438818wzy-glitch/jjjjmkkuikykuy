"""Offline checks shared by the explicit new-instance restore and verification."""
from collections import Counter,defaultdict
from datetime import datetime,timezone
import gzip
import hashlib
import json
import math
from pathlib import Path,PurePosixPath
from uuid import NAMESPACE_URL,UUID,uuid5

TABLE_ORDER=['major','article','article_major','article_rating','article_rating_profile',
             'article_rating_discovery','article_research_type_operation','article_rating_claim']
JSON_COLUMNS={'base','overrides','sync','source_files','payload','catalog_guard','profile_snapshot','terminal_evidence'}
TIME_COLUMNS={'created_at','updated_at','observed_at'}
# qdrant-haystack 13.1.0 converters.UUID_NAMESPACE, read from the deployed wheel.
QDRANT_ID_NAMESPACE=UUID('3896d314-1e95-4a3a-b45a-945f9f0b541d')

def compact(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

def fingerprint(value):return hashlib.sha256(compact(value).encode()).hexdigest()

def stable_id(*parts):
    return uuid5(NAMESPACE_URL,json.dumps(parts,ensure_ascii=False,separators=(',',':'))).hex

def normal_id(value):return UUID(str(value)).hex

def file_hash(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()

def safe_path(root,relative):
    rel=PurePosixPath(relative)
    if not rel.parts or rel.is_absolute() or '..' in rel.parts or '\\' in relative or ':' in relative:
        raise ValueError('Unsafe package path')
    target=root.joinpath(*rel.parts).resolve()
    if not target.is_relative_to(root.resolve()):raise ValueError('Package path escapes root')
    return target

def points(root):
    with gzip.open(root/'qdrant.jsonl.gz','rt',encoding='utf-8') as stream:
        for line in stream:
            row=json.loads(line)
            if row['type']=='point':yield row['point']

def header(root):
    with gzip.open(root/'qdrant.jsonl.gz','rt',encoding='utf-8') as stream:return json.loads(next(stream))

def verify_offline(root):
    root=Path(root).resolve()
    integrity=json.loads((root/'INTEGRITY.json').read_text(encoding='utf-8'))
    paths=set()
    for item in integrity['files']:
        relative=item['path']
        if relative in paths:raise ValueError('Duplicate integrity path')
        paths.add(relative);path=safe_path(root,relative)
        if not path.is_file() or path.stat().st_size!=item['bytes'] or file_hash(path)!=item['sha256']:
            raise ValueError('Package file checksum mismatch: '+relative)
    required={'catalog.json','schema.sql','qdrant.jsonl.gz','nas-hashes.json','object-hashes.json'}
    if not required.issubset(paths):raise ValueError('Required file missing from integrity manifest')
    catalog=json.loads((root/'catalog.json').read_text(encoding='utf-8'))
    ids=catalog['selected_article_ids'];tables=catalog['tables']
    if len(ids)!=100 or len(set(ids))!=100 or set(tables)!=set(TABLE_ORDER):raise ValueError('Expected exactly 100 unique articles')
    if {r['id'] for r in tables['article']['rows']}!=set(ids) or len(tables['article']['rows'])!=100:raise ValueError('Article ID set mismatch')
    for table in TABLE_ORDER:
        columns=tables[table]['columns']
        if 'search_text' in columns:raise ValueError('Generated columns cannot be imported')
        for row in tables[table]['rows']:
            if set(row)!=set(columns):raise ValueError('Row/column mismatch')
            if table not in ['major','article'] and row['article_id'] not in ids:raise ValueError('Foreign article in sample')
    for row in tables['article']['rows']:
        for f in row['base']['files']:
            path='nas/'+f['relative'].replace('\\','/')
            if path not in paths:raise ValueError('Original not in integrity manifest')
            item=next(x for x in integrity['files'] if x['path']==path)
            if item['sha256']!=f['sha256'] or item['bytes']!=f['bytes']:raise ValueError('Original differs from catalog')
    for manifest in ['nas-hashes.json','object-hashes.json']:
        for item in json.loads((root/manifest).read_text(encoding='utf-8'))['files']:
            path=safe_path(root,item['path'])
            if item['path'] not in paths or path.stat().st_size!=item['bytes'] or file_hash(path)!=item['sha256']:
                raise ValueError('Data submanifest mismatch')
    info=header(root);collection=info['collection_info'];params=collection['config']['params']
    vector_config=params['vectors'];vector_config=vector_config.get('dense',vector_config)
    if vector_config['size']!=1024 or vector_config['distance']!='Cosine':raise ValueError('Unexpected vector configuration')
    count=0;ids_seen=set();by_folder=defaultdict(dict);kind_counts=Counter();payload_digest=hashlib.sha256();chunk_records={};manifests=[]
    with gzip.open(root/'qdrant.jsonl.gz','rt',encoding='utf-8') as stream:
        first=json.loads(next(stream));trailer=None
        for line in stream:
            row=json.loads(line)
            if row['type']=='trailer':
                if trailer is not None:raise ValueError('Duplicate trailer')
                trailer=row;continue
            if trailer is not None or row['type']!='point':raise ValueError('Unexpected point stream order')
            p=row['point'];physical_id=normal_id(p['id']);pid=p['payload']['id'];meta=p['payload']['meta'];folder=meta['folder_id'];kind=meta['kind']
            if physical_id in ids_seen:raise ValueError('Duplicate point ID')
            if physical_id!=uuid5(QDRANT_ID_NAMESPACE,pid).hex:raise ValueError('Qdrant/Haystack ID mapping mismatch')
            ids_seen.add(physical_id);count+=1;kind_counts[kind]+=1
            if folder not in first['folders']:raise ValueError('Unexpected folder')
            payload_digest.update((compact({'id':p['id'],'payload':p['payload']})+'\n').encode())
            vector=p['vector'];vectors=vector.values() if isinstance(vector,dict) else [vector]
            if any(len(v)!=1024 or any(not math.isfinite(x) for x in v) for v in vectors):raise ValueError('Invalid stored vector')
            if kind in ['folder_binding','folder_complete']:
                if kind in by_folder[folder] or pid!=stable_id(folder,'binding' if kind=='folder_binding' else 'complete'):
                    raise ValueError('Folder marker identity mismatch')
                by_folder[folder][kind]=meta['fingerprint']
            elif kind=='chunk':
                record=meta['record'];did=meta['document_id']
                if pid!=stable_id(did,record['id']):raise ValueError('Chunk identity mismatch')
                common={k:meta[k] for k in ['folder_id','document_id','dataset_ids','document_name']}
                if fingerprint([common,record])!=meta['fingerprint']:raise ValueError('Chunk fingerprint mismatch')
                chunk_records[pid]=fingerprint(record)
            elif kind=='manifest':
                doc=meta['document']
                if pid!=stable_id(doc['id'],'manifest') or fingerprint([folder,doc])!=meta['fingerprint']:
                    raise ValueError('Manifest identity/fingerprint mismatch')
                manifests.append(doc)
            else:raise ValueError('Unexpected point kind')
    if not trailer or count!=trailer['point_count'] or payload_digest.hexdigest()!=trailer['payload_sha256']:
        raise ValueError('Qdrant stream checksum mismatch')
    if set(by_folder)!=set(info['folders']):raise ValueError('Missing folders')
    for marks in by_folder.values():
        if set(marks)!= {'folder_binding','folder_complete'} or marks['folder_binding']!=marks['folder_complete']:
            raise ValueError('Folder completion fingerprint mismatch')
    expected_chunks={}
    for doc in manifests:
        for record in doc['records']:expected_chunks[stable_id(doc['id'],record['id'])]=fingerprint(record)
    if expected_chunks!=chunk_records:raise ValueError('Manifest/chunk closure mismatch')
    return {'articles':len(ids),'tables':{t:len(tables[t]['rows']) for t in TABLE_ORDER},
            'files_hashed':len(paths),'points':count,'kinds':dict(kind_counts),'folders':len(by_folder),'offline_integrity':'passed'}

def normalized_row(row):
    out=dict(row)
    for key in TIME_COLUMNS&out.keys():
        value=out[key]
        if value is not None:
            if isinstance(value,str):value=datetime.fromisoformat(value)
            out[key]=value.astimezone(timezone.utc).isoformat()
    return out

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('sample',type=Path);args=parser.parse_args()
    print(json.dumps(verify_offline(args.sample),ensure_ascii=False,indent=2))
