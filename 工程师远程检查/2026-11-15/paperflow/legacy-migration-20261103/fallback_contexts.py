"""Resolve historical identities without a retained package to observed physical folders."""
import collections,copy,json,sqlite3,sys
from pathlib import Path
ROOT=Path(__file__).parent
from prepare_inputs import context
snapshot=json.loads((ROOT/'source-inventory.json').read_text());majors=json.loads((ROOT.parent/'article-repository/majors.json').read_text())['majors']
known={m[k] for m in majors for k in ('paper_dataset_id','image_dataset_id')}
catalog=json.loads((ROOT/'catalog-contexts.json').read_text());records={r['source_key']:r for r in catalog}
by_pair=collections.defaultdict(dict);by_task=collections.defaultdict(dict);by_sha=collections.defaultdict(dict)
for record in catalog:
 if record.get('sha') and record['source_key'].startswith('folder:'):
  by_sha[record['sha']][record['source_key']]={k:record[k] for k in ('source_key','folder_name','files')}
for raw in (r'F:\论文预处理系统\看板运行数据\runtime\query.sqlite',r'F:\论文预处理系统\工作间\新项目-20261027-7958216247\runtime\query.sqlite',r'F:\论文预处理系统\工作间\新稿小测-20261028\runtime\query.sqlite',r'F:\论文预处理系统\看板新运行数据-20261027\query.sqlite'):
 path=Path(raw)
 if not path.exists():continue
 with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as conn:
  tables={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
  for table in ('assets','papers'):
   if table not in tables:continue
   for p,d in conn.execute('SELECT payload,detail FROM '+table):
    p,d=json.loads(p),json.loads(d);row=context(p,d)
    if not row:continue
    row.update({k:v for k,v in records.get(row['source_key'],{}).items() if k in ('files','folder_name')})
    sha=d.get('source_hash');manifest=d.get('package_hash');task=d.get('task') or {}
    if sha and manifest:by_pair[(sha,manifest)][row['source_key']]=row
    if task.get('task_id') and task.get('source_sha256'):by_task[(task['task_id'],task['source_sha256'])][row['source_key']]=row
    if sha:by_sha[sha][row['source_key']]=row
plans={tuple(p['identity']) for p in json.loads((ROOT/'package-index.json').read_text())['plans']}
supplements=json.loads((ROOT/'native-supplements.json').read_text())['payloads']
groups={tuple(d['meta'].get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256')) for d in snapshot['documents'] if d['kb_id'] in known}
mapped={};stats=collections.Counter()
for identity in groups:
 if not all(identity) or identity in plans or json.dumps(identity,separators=(',',':')) in supplements:continue
 contexts=by_pair.get(identity[1:],{}) or by_task.get(identity[:2],{})
 if not contexts and len(by_sha.get(identity[1],{}))==1:contexts=by_sha[identity[1]]
 if contexts:mapped[json.dumps(identity,separators=(',',':'))]=list(contexts.values());stats['resolved_unsealed_groups']+=1
 else:stats['unresolved_unsealed_groups']+=1
hashes={sha:list(rows.values()) for sha,rows in by_sha.items() if len(rows)==1}
(ROOT/'fallback-contexts.json').write_text(json.dumps({'identities':mapped,'unique_file_hashes':hashes},ensure_ascii=False),encoding='utf-8')
print(json.dumps(dict(stats),ensure_ascii=False))
