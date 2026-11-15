"""从私有运行证据提取计数；不导出会话正文、思考或凭据。"""
from pathlib import Path
from compression import zstd
import json,sqlite3,sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from paperflow.package import load,save
from paperflow.provenance import usage_totals

project=Path(__file__).resolve().parents[1];reports=[]
for relative in ('runtime/acceptance-paper','runtime/integration-live'):
    home=project/relative;sessions={}
    for p in (home/'private').glob('harness-*/sessions/*/*/session.v4.jsonl.zstd'):
        session=p.parent.name
        with zstd.open(p,'rt',encoding='utf-8') as stream:
            # 只保留usage字段，不把会话原文加入报告或输出。
            events=[]
            for line in stream:
                event=json.loads(line)
                if event.get('type')=='assistant/message':events.append(dict(type=event['type'],data={'usage':event.get('data',{}).get('usage')}))
        sessions[session]=usage_totals(events)
    attempts=[]
    for p in sorted((home/'private/attempts').glob('*.json')):
        obj=load(p);session=obj.get('session_id',p.stem)
        item={k:obj[k] for k in ('stage','seconds','model','provider','syntax_attempt') if k in obj}
        item['session_id']=session;item['usage']=obj.get('usage') or sessions.get(session,{'available':False})
        attempts.append(item)
    with sqlite3.connect(home/'state.sqlite3') as db:
        stages=[dict(at=r[0],task_id=r[1],**json.loads(r[2])) for r in db.execute("SELECT at,task_id,data FROM events WHERE kind='stage'")]
    report=dict(source=relative,attempts=attempts,stage_events=stages,
        usage_notes='Provider raw input/output/cache/total counters; cached tokens are not subtracted or added again. Historical missing counters remain unavailable.')
    save(home/'performance-report.json',report);reports.append(dict(source=relative,attempts=len(attempts),usage_available=sum(a['usage'].get('available',False) for a in attempts)))
print(json.dumps(reports,ensure_ascii=False))
