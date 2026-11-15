"""Capture the one migration inventory from source storage; no source writes."""
import json
import shlex
import subprocess

TENANT='88fd7476b00e11f199856dc9cfded896'
def run(args):
    result=subprocess.run(args,capture_output=True,text=True,timeout=90)
    if result.returncode: raise RuntimeError(result.stderr[-1000:])
    return result.stdout
def mysql(sql):
    command='MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -B --raw --default-character-set=utf8mb4 rag_flow -e '+shlex.quote(sql)
    return [json.loads(row) for row in run(['docker','exec','ragflow-server-mysql-1','sh','-c',command]).splitlines() if row]
def es(path,payload,method='POST'):
    command='curl -fsS --max-time 75 -u "elastic:$ELASTIC_PASSWORD" -H "Content-Type: application/json" -X '+method+' --data '+shlex.quote(json.dumps(payload))+' '+shlex.quote('http://127.0.0.1:9200/'+path)
    return json.loads(run(['docker','exec','ragflow-recovery-es-20260923','sh','-c',command]))

kbs=mysql("SELECT JSON_OBJECT('id',id,'tenant_id',tenant_id,'name',name,'model',embd_id,'parser',parser_id,'status',status) FROM knowledgebase WHERE status='1'")
docs=mysql("SELECT JSON_OBJECT('id',id,'kb_id',kb_id,'name',name,'location',location,'bytes',size,'type',type,'source_type',source_type,'chunk_count',chunk_num,'run',run,'parser',parser_id,'parser_config',parser_config,'updated_at',update_time) FROM document WHERE status='1'")
response=es('ragflow_doc_meta_'+TENANT+'/_search?scroll=2m',{'size':500,'sort':['_doc'],
 '_source':['id','kb_id','meta_fields.paperflow_task','meta_fields.paperflow_source_sha256','meta_fields.paperflow_manifest_sha256',
            'meta_fields.paperflow_revision','meta_fields.paperflow_specialty','meta_fields.paperflow_role','meta_fields.paperflow_contract',
            'meta_fields.parent_body_document_id'], 'query':{'match_all':{}}})
metadata={}
scroll_id=None
try:
    while True:
        hits=response['hits']['hits'];scroll_id=response.get('_scroll_id')
        if not hits: break
        for hit in hits:
            item=hit['_source'];metadata[item['id']]=item.get('meta_fields') or {}
        response=es('_search/scroll',{'scroll':'2m','scroll_id':scroll_id})
finally:
    if scroll_id: es('_search/scroll',{'scroll_id':[scroll_id]},method='DELETE')
for doc in docs: doc['meta']=metadata.get(doc['id'],{})
networks={}
for name in ('ragflow-recovery-es-20260923','ragflow-server-minio-1','paperflow-qdrant','paperflow-article-repository'):
    row=json.loads(run(['docker','inspect',name]))[0]
    networks[name]={'networks':{key:value.get('IPAddress') for key,value in row['NetworkSettings']['Networks'].items()},
                    'ports':row['NetworkSettings'].get('Ports'),
                    'mounts':[{'source':v['Source'],'destination':v['Destination']} for v in row['Mounts']]}
print(json.dumps({'tenant':TENANT,'knowledgebases':kbs,'documents':docs,'network':networks},ensure_ascii=False))
