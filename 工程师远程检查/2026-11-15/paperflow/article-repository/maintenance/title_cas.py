"""One-off reviewed display maintenance; never a workflow or semantic edit."""
import copy
from psycopg.types.json import Jsonb

PROTECTED=('source_key','base','revision','folder_name','sync','updated_at')
TABLES=('article_rating','article_rating_profile','article_rating_discovery','article_rating_claim')

def related(conn,aid):
    return {table:conn.execute('SELECT to_jsonb(t) value FROM '+table+' t WHERE article_id=%s ORDER BY to_jsonb(t)::text',(aid,)).fetchall() for table in TABLES}

def correct(conn,plan):
    title=plan.get('title')
    if not isinstance(title,str) or not title.strip() or len(title)>1000:
        raise ValueError('invalid reviewed title')
    evidence=plan.get('evidence')
    if not isinstance(evidence,dict) or not evidence.get('kind') or not evidence.get('source_sha256')==plan.get('source_sha256'):
        raise ValueError('missing reviewed source evidence')
    if conn.autocommit:raise ValueError('maintenance requires an explicit transaction')
    row=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE',(plan['id'],)).fetchone()
    if row is None:raise ValueError('missing article')
    if 'title' in row['overrides']:return {'id':row['id'],'status':'protected_manual_title'}
    if (row['revision']!=plan['revision'] or row['title']!=plan['old_title']
            or row['source_key']!=plan['source_key'] or row['base'].get('source_sha256')!=plan['source_sha256']
            or row['overrides']!=plan['overrides']):raise ValueError('reviewed identity or revision changed')
    prior=related(conn,row['id'])
    over=copy.deepcopy(row['overrides']);over['title']=plan['title']
    after=conn.execute('UPDATE article SET overrides=%s,title=%s WHERE id=%s RETURNING *',
                       (Jsonb(over),plan['title'],row['id'])).fetchone()
    if any(after[key]!=row[key] for key in PROTECTED):raise ValueError('protected article field changed')
    if related(conn,row['id'])!=prior:raise ValueError('rating or discovery records changed')
    return {'id':row['id'],'status':'corrected','before':row,'after':after,
            'evidence':plan['evidence'],'rating_and_discovery_unchanged':True}
