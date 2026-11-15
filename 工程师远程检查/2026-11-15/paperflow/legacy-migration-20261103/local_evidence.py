import collections,json,sqlite3
from pathlib import Path

root=Path(__file__).parent
snapshot=json.loads((root/'source-inventory.json').read_text())
majors=json.loads((root.parent/'article-repository/majors.json').read_text())['majors']
known={m[k] for m in majors for k in ('paper_dataset_id','image_dataset_id')}
groups=collections.defaultdict(list)
for doc in snapshot['documents']:
    if doc['kb_id'] in known:
        meta=doc['meta'];groups[tuple(meta.get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256'))].append(doc)
items=json.loads((root.parent/'legacy-package-matches.json').read_text())['matched_packages']
matched=[p for p in items if (p['task'],p['source_sha256'],p['manifest_sha256']) in groups]
print(json.dumps({'formal_identity_groups':len(groups),'formal_matched_packages':len(matched),
                  'formal_matched_docs':sum(len(groups[(p['task'],p['source_sha256'],p['manifest_sha256'])]) for p in matched),
                  'unknown_identity_documents':len(groups.get((None,None,None),[]))}))
chosen=next(p for p in matched if len(groups[(p['task'],p['source_sha256'],p['manifest_sha256'])])>3)
package=Path(chosen['path']);run=package.parents[1]
print(json.dumps({'chosen':chosen,'run':str(run)},ensure_ascii=False))
manifest_path=run/'manifest.json'
if manifest_path.exists():
    value=json.loads(manifest_path.read_text(encoding='utf-8-sig'))
    for field in ('items','inputs','discovered_inputs'):
        rows=value.get(field)
        if isinstance(rows,dict): rows=list(rows.values())
        if isinstance(rows,list):
            matches=[r for r in rows if isinstance(r,dict) and (chosen['task'] in json.dumps(r) or chosen['source_sha256'] in json.dumps(r))]
            print(json.dumps({'field':field,'count':len(rows),'matches':matches[:1],'first':rows[:1] if not matches else None},ensure_ascii=False)[:7000])
db=Path(r'F:\论文预处理系统\看板运行数据\runtime\query.sqlite')
with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
    conn.row_factory=sqlite3.Row
    asset=conn.execute('SELECT * FROM assets LIMIT 1').fetchone()
    paper=conn.execute('SELECT * FROM papers LIMIT 1').fetchone()
    print(json.dumps({'asset_columns':list(asset.keys()),'asset_payload_keys':list(json.loads(asset['payload'])),'asset_detail_keys':list(json.loads(asset['detail'])),
                      'paper_columns':list(paper.keys()),'paper_payload_keys':list(json.loads(paper['payload'])),'paper_detail_keys':list(json.loads(paper['detail']))},ensure_ascii=False))
