"""Durable folder catalog; workflow state is never mutated here."""
import copy
import hashlib
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .profiles import display,FIELDS,reference,file_summary,details,article_research_type,article_year,retain_research_type
from .filters import search_conditions,search_order,level_condition,ATTACHMENT,YEAR_VALUE,YEAR_PATTERN
from .years import infer_year


class Catalog:
    def __init__(self,dsn,*,schema='public'):
        self.dsn,self.schema=dsn,schema

    def connect(self):
        conn=psycopg.connect(self.dsn,connect_timeout=8,row_factory=dict_row)
        conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(self.schema)))
        return conn

    def initialize(self):
        with self.connect() as conn:
            conn.execute(Path(__file__).with_name('schema.sql').read_text())

    def set_majors(self,rows):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany('INSERT INTO major VALUES(%s,%s,%s,%s) ON CONFLICT(code) DO UPDATE SET name=excluded.name,paper_dataset_id=excluded.paper_dataset_id,image_dataset_id=excluded.image_dataset_id',
                    [(r['code'],r['name'],r.get('paper_dataset_id',''),r.get('image_dataset_id','')) for r in rows])

    def majors(self):
        with self.connect() as conn: return conn.execute('SELECT * FROM major ORDER BY code').fetchall()

    @staticmethod
    def _view(row,*,include_profile=False):
        base,over=row['base'],row['overrides']
        fields=display(base.get('profile') or {}) | over.get('fields',{})
        files=base.get('files',[])
        states=[f.get('availability','unknown') for f in files]
        availability=('complete' if files and base.get('inventory_complete') and all(s=='available' for s in states)
                      else 'partial' if 'available' in states else 'missing' if states and all(s=='missing' for s in states) else 'unknown')
        original=base.get('qdrant') or {}
        corrected=(row.get('sync') or {}).get('qdrant') or {}
        qdrant={'folder_ids':list(dict.fromkeys(original.get('folder_ids',[])+corrected.get('folder_ids',[]))),
                'source_folder_ids':[original['source_folder_id']] if original.get('source_folder_id') else original.get('folder_ids',[])}
        value={'id':row['id'],'title':over.get('title',base.get('title')) or base['folder_name'],
                'folder_name':base['folder_name'],'source_key':row['source_key'],
                'majors':over.get('majors',base.get('majors',[])),'fields':fields,'files':files,
                'inventory_complete':bool(base.get('inventory_complete')),'availability':availability,
                'research_type':article_research_type(base.get('profile') or {}),
                'year':article_year(base.get('profile') or {}),
                'revision':row['revision'],'manual_fields':list(over.get('fields',{})),
                'manually_edited':bool(over),'source':base.get('source',''),
                'migration': 'complete' if qdrant.get('folder_ids') else base.get('migration','unverified'),
                'sync':row.get('sync') or {},'updated_at':row['updated_at'].isoformat(),
                'qdrant':qdrant,'reference':reference(base.get('profile') or {},over.get('fields',{})),
                'file_summary':file_summary(files,base.get('inventory_complete',False))}
        if include_profile:value['profile']=details(base.get('profile') or {},over.get('fields',{}))
        return value

    def _memberships(self,conn,aid,majors):
        conn.execute('DELETE FROM article_major WHERE article_id=%s',(aid,))
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO article_major VALUES(%s,%s)',[(aid,m) for m in sorted(set(majors))])

    @staticmethod
    def _files(prior,incoming,older):
        files=copy.deepcopy(prior)
        for candidate in incoming:
            file=copy.deepcopy(candidate)
            previous=next((f for f in files if (file.get('sha256') and file['sha256']==f.get('sha256')) or
                (f['id']==file['id'] and all(f.get(k)==file.get(k) for k in ('sha256','relative','bytes')))),None)
            if previous is not None:
                chosen=previous if older else file
                if previous.get('availability')=='available' and file.get('availability','unknown')=='unknown':chosen=previous
                if older and file.get('availability')=='available' and previous.get('availability')!='available':chosen=file
                identity=previous['id'];chosen=copy.deepcopy(chosen);previous.clear();previous.update(chosen)
                previous['id']=identity
                continue
            collision=next((f for f in files if f['id']==file['id']),None)
            if collision:
                renamed=file if older else collision
                renamed['id']+='-'+hashlib.sha256(str(renamed.get('sha256') or renamed.get('relative')).encode()).hexdigest()[:16]
            files.append(file)
        return files

    def register(self,record):
        record=copy.deepcopy(record)
        if not record.get('source_key') or not record.get('folder_name'): raise ValueError('文件夹来源身份不能为空')
        aid=hashlib.sha256(record['source_key'].encode()).hexdigest()[:32]
        with self.connect() as conn:
            created=conn.execute('INSERT INTO article(id,source_key,base,title,folder_name) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(source_key) DO NOTHING RETURNING id',
                (aid,record['source_key'],Jsonb(record),record.get('title') or record['folder_name'],record['folder_name'])).fetchone() is not None
            row=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()
            prior=row['base']
            before,after=prior.get('source_updated_at'),record.get('source_updated_at')
            older=(bool(before and after) and datetime.fromisoformat(after.replace('Z','+00:00'))<datetime.fromisoformat(before.replace('Z','+00:00')))
            older=older or prior.get('source')=='qdrant_prefect' and record.get('source','').startswith('legacy')
            folders=list(dict.fromkeys((prior.get('qdrant') or {}).get('folder_ids',[])+(record.get('qdrant') or {}).get('folder_ids',[])))
            files=self._files(prior.get('files',[]),record.get('files',[]),older)
            if older: record=copy.deepcopy(prior)
            retain_research_type(prior,record)
            if created and 'year' not in ((record.get('profile') or {}).get('article') or {}):
                year=infer_year(record)
                if year:record.setdefault('profile',{}).setdefault('article',{})['year']=year
            record['files']=files
            if folders:
                record.setdefault('qdrant',{})['folder_ids']=folders
            if row['base']!=record:
                row=conn.execute('UPDATE article SET base=%s, title=%s,folder_name=%s,revision=revision+1,updated_at=now() WHERE id=%s RETURNING *',
                    (Jsonb(record),row['overrides'].get('title',record.get('title')) or record['folder_name'],record['folder_name'],aid)).fetchone()
            self._memberships(conn,aid,row['overrides'].get('majors',record.get('majors',[])))
            return self._view(row)

    def detail(self,aid):
        with self.connect() as conn:
            row=conn.execute('SELECT * FROM article WHERE id=%s',(aid,)).fetchone()
        if not row: raise LookupError('未找到文章')
        return self._view(row,include_profile=True)

    def raw(self,aid):
        with self.connect() as conn:
            row=conn.execute('SELECT * FROM article WHERE id=%s',(aid,)).fetchone()
        if not row: raise LookupError('未找到文章')
        return row

    def edit(self,aid,revision,changes):
        if set(changes)-{'title','majors','fields'}: raise ValueError('不支持修改此字段')
        if not isinstance(changes.get('fields',{}),dict) or set(changes.get('fields',{}))-set(FIELDS): raise ValueError('画像字段无效')
        if any(not isinstance(v,str) or len(v)>50000 for v in changes.get('fields',{}).values()): raise ValueError('画像内容无效')
        if 'title' in changes and (not isinstance(changes['title'],str) or not changes['title'].strip() or len(changes['title'])>1000): raise ValueError('标题不能为空或过长')
        if 'majors' in changes:
            majors=changes['majors']
            if not isinstance(majors,list) or not majors or any(not isinstance(m,str) for m in majors): raise ValueError('至少选择一个专业')
            known={m['code'] for m in self.majors()}
            if set(majors)-known: raise ValueError('专业代码无效')
        with self.connect() as conn:
            row=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()
            if not row: raise LookupError('未找到文章')
            if row['revision']!=revision: raise ValueError('记录版本已变化，请刷新后修改')
            over=copy.deepcopy(row['overrides'])
            over['fields']=over.get('fields',{}) | changes.get('fields',{})
            over.update({k:v for k,v in changes.items() if k!='fields'})
            row=conn.execute('UPDATE article SET overrides=%s,title=%s,revision=revision+1,updated_at=now() WHERE id=%s RETURNING *',
                (Jsonb(over),over.get('title',row['title']),aid)).fetchone()
            self._memberships(conn,aid,over.get('majors',row['base'].get('majors',[])))
        return self._view(row)

    def search(self,*,keyword='',major='',level='',file_condition='',attachment_ext='',research_type='',year='',page=1,page_size=30):
        if not 1<=page<=100000 or not 1<=page_size<=100 or len(keyword)>200: raise ValueError('查询参数无效')
        where,args=search_conditions(keyword=keyword,major=major,level=level,file_condition=file_condition,attachment_ext=attachment_ext,research_type=research_type,year=year)
        order,order_args=search_order(keyword)
        with self.connect() as conn:
            total=conn.execute('SELECT count(*) AS n FROM article a WHERE '+where,args).fetchone()['n']
            rows=conn.execute('SELECT * FROM article a WHERE '+where+' ORDER BY '+order+' LIMIT %s OFFSET %s',args+order_args+[page_size,(page-1)*page_size]).fetchall()
            sources=conn.execute('SELECT * FROM source_status ORDER BY name').fetchall()
        values=[self._view(row) for row in rows]
        for row in values: row.pop('files'); row.pop('qdrant'); row.pop('source_key')
        return {'rows':values,'total':total,'page':page,'page_size':page_size,'sources':sources}

    def years(self):
        with self.connect() as conn:
            rows=conn.execute(f"SELECT DISTINCT {YEAR_VALUE} AS year FROM article a "
                f"WHERE coalesce(a.base->>'entry_kind','article')='article' AND {YEAR_VALUE} ~ %s ORDER BY year DESC",
                (YEAR_PATTERN,)).fetchall()
        return [row['year'] for row in rows]

    def statistics(self):
        bachelor,args=level_condition('bachelor')
        with self.connect() as conn:
            return conn.execute(f"""SELECT count(*) AS total,count(*) FILTER(WHERE {bachelor}) AS bachelor,
                count(*) FILTER(WHERE {ATTACHMENT}) AS attachments FROM article a
                WHERE coalesce(a.base->>'entry_kind','article')='article'""",args).fetchone()

    def source_status(self,name,status):
        with self.connect() as conn:
            conn.execute('INSERT INTO source_status VALUES(%s,%s,now()) ON CONFLICT(name) DO UPDATE SET status=excluded.status,observed_at=now()', (name,Jsonb(status)))

    def save_sync(self,aid,revision,result):
        with self.connect() as conn:
            updated=conn.execute('UPDATE article SET sync=%s WHERE id=%s AND revision=%s',(Jsonb({**result,'revision':revision}),aid,revision))
            if not updated.rowcount: raise ValueError('文章版本已变化，未覆盖同步结果')

    def synchronize(self,aid,sync):
        # Both user edits and new processed versions use this one publication boundary.
        with self.connect() as conn:
            raw=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE NOWAIT',(aid,)).fetchone()
            if not raw: raise LookupError('未找到文章')
            item=self._view(raw); item['overrides']=raw['overrides']
            if raw['sync'].get('revision')==item['revision'] and raw['sync'].get('status')=='complete': return self._view(raw)
            majors=conn.execute('SELECT * FROM major WHERE code=ANY(%s)',(item['majors'],)).fetchall()
            try: result=sync.apply(item,majors)
            except Exception:
                result={**raw['sync'],'status':'failed','message':'目录修改已保存，新库同步未确认，可再次同步'}
            value={**result,'revision':item['revision']}
            row=conn.execute('UPDATE article SET sync=%s WHERE id=%s RETURNING *',(Jsonb(value),aid)).fetchone()
        return self._view(row)

    def observe_files(self,aid,files):
        with self.connect() as conn:
            row=conn.execute('SELECT base FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()
            if not row:raise LookupError('未找到文章')
            observed={f['id']:f for f in files};current=row['base'].get('files',[])
            for file in current:
                check=observed.get(file['id'])
                if check and all(check.get(k)==file.get(k) for k in ('relative','sha256','bytes')):
                    file['availability']=check.get('availability','unknown')
            conn.execute("UPDATE article SET base=jsonb_set(base,'{files}',%s) WHERE id=%s",(Jsonb(current),aid))
