import json,shlex,subprocess
def query(sql):
    command='MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -B --raw --default-character-set=utf8mb4 rag_flow -e '+shlex.quote(sql)
    result=subprocess.run(['docker','exec','ragflow-server-mysql-1','sh','-c',command],capture_output=True,text=True,timeout=90)
    if result.returncode:raise RuntimeError(result.stderr[-300:])
    return [json.loads(row) for row in result.stdout.splitlines() if row]
files=query("SELECT JSON_OBJECT('id',id,'bucket',parent_id,'location',location,'bytes',size,'name',name,'source_type',source_type) FROM file")
relations=query("SELECT JSON_OBJECT('document_id',document_id,'file_id',file_id) FROM file2document")
print(json.dumps({'files':files,'relations':relations},ensure_ascii=False))
