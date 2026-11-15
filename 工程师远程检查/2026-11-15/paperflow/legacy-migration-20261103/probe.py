"""Read-only source inventory; print counts and field shapes, never credentials or article text."""
import collections
import json
import shlex
import subprocess

TENANT='88fd7476b00e11f199856dc9cfded896'

def run(args,timeout=60):
    result=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
    if result.returncode: raise RuntimeError('Read-only probe failed: '+result.stderr[-500:])
    return result.stdout

def mysql(query):
    command='MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -B --raw --default-character-set=utf8mb4 rag_flow -e '+shlex.quote(query)
    return run(['docker','exec','ragflow-server-mysql-1','sh','-c',command])

def es(path,payload=None):
    command='curl -fsS --max-time 45 -u "elastic:$ELASTIC_PASSWORD" '
    if payload is not None: command+='-H "Content-Type: application/json" --data '+shlex.quote(json.dumps(payload))+' '
    command+=shlex.quote('http://127.0.0.1:9200/'+path)
    return json.loads(run(['docker','exec','ragflow-recovery-es-20260923','sh','-c',command]))

print(json.dumps({'document_states':mysql('SELECT status,run,COUNT(*),SUM(chunk_num),SUM(size) FROM document GROUP BY status,run'),
                  'active_models':mysql("SELECT embd_id,COUNT(*),SUM(doc_num),SUM(chunk_num) FROM knowledgebase WHERE status='1' GROUP BY embd_id"),
                  'active_counts':mysql("SELECT COUNT(*),COUNT(DISTINCT kb_id),SUM(chunk_num),SUM(size) FROM document WHERE status='1'"),
                  'kb_columns':mysql('SHOW COLUMNS FROM knowledgebase'),
                  'doc_columns':mysql('SHOW COLUMNS FROM document'),
                  'file_columns':mysql('SHOW COLUMNS FROM file'),
                  'relation_columns':mysql('SHOW COLUMNS FROM file2document')},ensure_ascii=False))
for prefix in ('ragflow_','ragflow_doc_meta_'):
    index=prefix+TENANT
    response=es(index+'/_mapping')
    properties=response[index]['mappings'].get('properties',{})
    sample=es(index+'/_search',{'size':1,'query':{'match_all':{}}})
    shapes=[]
    for hit in sample['hits']['hits']:
        shape={}
        for key,value in hit.get('_source',{}).items():
            shape[key]= {'type':type(value).__name__,'length':len(value) if isinstance(value,(str,list,dict)) else None}
            if isinstance(value,dict): shape[key]['keys']=list(value)[:100]
        shapes.append(shape)
    print(json.dumps({'index':index,'mapping':{k:{key:v[key] for key in ('type','dims','similarity') if key in v} for k,v in properties.items()},
                      'sample_shapes':shapes,'total':sample['hits']['total']},ensure_ascii=False))
result=es('ragflow_'+TENANT+'/_search',{'size':0,'aggs':{
    'availability':{'terms':{'field':'available_int','size':10,'missing':-99}},
    'models':{'terms':{'field':'embedding_model_kwd','size':30,'missing':'(missing)'}},
    'vector_count':{'filter':{'exists':{'field':'q_1024_vec'}}},
    'image_count':{'filter':{'exists':{'field':'img_id'}}},
    'roles':{'terms':{'field':'doc_type_kwd','size':20,'missing':'(missing)'}}}})
print(json.dumps({'aggregations':result['aggregations']}))
print(json.dumps({'disk':run(['df','-h','/opt','/var/lib/docker']),'memory':run(['free','-m'])}))
