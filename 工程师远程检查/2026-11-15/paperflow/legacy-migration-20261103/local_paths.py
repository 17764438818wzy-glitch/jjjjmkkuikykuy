import json,sqlite3
from pathlib import Path
root=Path(__file__).parent
items=json.loads((root.parent/'legacy-package-matches.json').read_text())['matched_packages']
sample=next(i for i in items if (Path(i['path']).parents[1]/'manifest.json').exists())
manifest=Path(sample['path']).parents[1]/'manifest.json'
value=json.loads(manifest.read_text(encoding='utf-8-sig'))
print(json.dumps({'manifest':str(manifest),'sample_task':sample['task'],'schema':value.get('schema_version')},ensure_ascii=False))
for field in ('items','inputs','discovered_inputs'):
    rows=value.get(field)
    if isinstance(rows,dict): rows=list(rows.values())
    if isinstance(rows,list):
        chosen=next((v for v in rows if sample['source_sha256'] in json.dumps(v) or sample['task'] in json.dumps(v)),rows[0] if rows else {})
        print(json.dumps({'field':field,'type':type(rows).__name__,'sample':chosen},ensure_ascii=False)[:5000])
db=Path(r'F:\论文预处理系统\看板运行数据\runtime\query.sqlite')
with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
    row=conn.execute('SELECT payload,detail FROM assets LIMIT 1').fetchone()
    payload,detail=[json.loads(r) for r in row]
    print(json.dumps({'source_path':payload.get('source_path'),'input':detail.get('input_source_path'),'task':detail.get('task'),
                      'work_dir':detail.get('work_dir'),'source_hash':detail.get('source_hash'),'package_hash':detail.get('package_hash'),
                      'attachment':detail.get('attachment')},ensure_ascii=False)[:6000])
