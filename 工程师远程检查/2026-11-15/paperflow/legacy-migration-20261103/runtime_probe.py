import json,subprocess
commands=[
 ['docker','exec','ragflow-server-ragflow-1','python3','-c',"import minio;print('minio_version',minio.__version__)"],
 ['docker','exec','paperflow-article-repository','python','-c',"import importlib.util;print('minio_installed',bool(importlib.util.find_spec('minio')))"],
 ['docker','exec','ragflow-server-ragflow-1','sh','-c',"grep -n -A40 'def get_storage_address' /ragflow/api/db/services/file2document_service.py; sed -n '1780,1815p' /ragflow/api/apps/restful_apis/document_api.py; grep -n -A40 'def get_image' /ragflow/api/apps/file_app.py"],
]
for command in commands:
    result=subprocess.run(command,capture_output=True,text=True,timeout=30)
    print(json.dumps({'stdout':result.stdout,'stderr':result.stderr[-200:],'exit':result.returncode}))
