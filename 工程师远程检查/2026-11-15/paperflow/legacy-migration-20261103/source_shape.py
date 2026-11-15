import json
from pathlib import Path
root=Path(__file__).parent
items=json.loads((root.parent/'legacy-package-matches.json').read_text())['matched_packages']
for item in items[:2]+items[-1:]:
    package=Path(item['path']); run=package.parents[1]
    output={'run':str(run),'run_files':[p.name for p in run.iterdir() if p.is_file()],'package_task':item['task']}
    for path in (run/'manifest.json',package/'source-map.json',package/'profile.json'):
        if not path.exists():continue
        value=json.loads(path.read_text(encoding='utf-8-sig'))
        record={'keys':list(value)[:50]}
        if path.name=='manifest.json':
            for key in ('tasks','papers','sources'):
                rows=value.get(key)
                if isinstance(rows,list):
                    match=next((r for r in rows if r.get('task_id')==item['task']),rows[0] if rows else {})
                elif isinstance(rows,dict): match=rows.get(item['task']) or next(iter(rows.values()),{})
                else:continue
                record[key+'_type']=type(rows).__name__;record[key+'_match']=match if isinstance(match,dict) else type(match).__name__
        else: record['source_fields']={k:v for k,v in value.items() if k.startswith('source') or k in ('original','source_file','source_path')}
        output[path.name]=record
    print(json.dumps(output,ensure_ascii=False)[:6500])
snapshot=json.loads((root/'source-inventory.json').read_text())
majors=json.loads((root.parent/'article-repository/majors.json').read_text())['majors']
known={m[k] for m in majors for k in ('paper_dataset_id','image_dataset_id')}
formal=[d for d in snapshot['documents'] if d['kb_id'] in known]
print(json.dumps({'formal_documents':len(formal),'formal_chunks':sum(d['chunk_count'] for d in formal),
 'test_or_other_documents':len(snapshot['documents'])-len(formal)}))
