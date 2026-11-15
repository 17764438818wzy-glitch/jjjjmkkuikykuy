"""A fixed operator manifest, gradually handed to the existing managed queue."""
import hashlib
import json
from pathlib import Path
import time

from .package import sha, save


class BatchIntake:
    def __init__(self, workbench):
        self.w = workbench

    def initialize(self, db):
        db.executescript('''
            CREATE TABLE IF NOT EXISTS web_intakes (
                batch_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                epoch TEXT NOT NULL,generation INTEGER NOT NULL,deadline REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS web_intake_items (
                batch_id TEXT NOT NULL,item_id TEXT NOT NULL,position INTEGER NOT NULL,
                name TEXT NOT NULL,relative_path TEXT NOT NULL,bytes INTEGER NOT NULL,
                state TEXT NOT NULL,input_id TEXT,task_id TEXT,error TEXT,
                PRIMARY KEY(batch_id,item_id));
            CREATE INDEX IF NOT EXISTS web_intake_pending ON web_intake_items(batch_id,state);
            CREATE INDEX IF NOT EXISTS web_intake_order ON web_intake_items(batch_id,position);
            CREATE TABLE IF NOT EXISTS web_input_preparations (
                request_id TEXT NOT NULL,item_id TEXT NOT NULL,input_id TEXT NOT NULL,
                name TEXT NOT NULL,relative_path TEXT NOT NULL,bytes INTEGER NOT NULL,
                PRIMARY KEY(request_id,item_id));
        ''')

    def parse(self, manifest):
        if not isinstance(manifest,list) or not manifest:
            raise ValueError('请选择本批文件清单')
        result=[];seen=set()
        from .workbench import FORMATS, MAX_UPLOAD
        for r in manifest:
            if not isinstance(r,dict):raise ValueError('文件清单无效')
            key,name,size=r.get('id'),r.get('name'),r.get('bytes')
            if (not isinstance(key,str) or not 1<=len(key)<=100 or key in seen
                    or not isinstance(name,str) or not name or len(name)>240
                    or type(size) is not int or size<0):
                raise ValueError('文件清单编号、名称或大小无效')
            relative=self.w.check_relative_path(name,r.get('relative_path',''))
            reason=('格式暂不支持' if Path(name).suffix.lower() not in FORMATS else
                    '空文件' if not size else '超过单文件 2 GiB 上限' if size>MAX_UPLOAD else '')
            result.append(dict(id=key,name=name,relative_path=relative,bytes=size,
                               state='skipped' if reason else 'pending',error=reason))
            seen.add(key)
        return result

    @staticmethod
    def fingerprint(rows):
        values=[{k:r[k] for k in ('id','name','relative_path','bytes')} for r in rows]
        return hashlib.sha256(json.dumps(values,ensure_ascii=False,sort_keys=True).encode()).hexdigest()

    def record_preparation(self,request_id,item_id,row,name,relative):
        if request_id is None and item_id is None:return row
        if (not isinstance(request_id,str) or not 1<=len(request_id)<=100
                or not isinstance(item_id,str) or not 1<=len(item_id)<=100):
            raise ValueError('本次文件准备编号无效')
        values=(request_id,item_id,row['id'],name,relative,row['bytes'])
        with self.w.store.db() as db:
            old=db.execute('SELECT * FROM web_input_preparations WHERE request_id=? AND item_id=?',(request_id,item_id)).fetchone()
            if old and tuple(old)!=values:raise ValueError('同一清单项不能换成其他原稿')
            db.execute('INSERT OR IGNORE INTO web_input_preparations VALUES(?,?,?,?,?,?)',values)
        return row

    @staticmethod
    def verify_preparation(db,request_id,item_id,row,entry):
        p=db.execute('SELECT * FROM web_input_preparations WHERE request_id=? AND item_id=?',(request_id,item_id)).fetchone()
        if not p or (p['input_id'],p['name'],p['relative_path'],p['bytes'])!=(row['id'],entry['name'],entry['relative_path'],entry['bytes']):
            raise ValueError('准备结果不属于本次清单项，请重新选择正确原稿')

    def create_sql(self,db,batch_id,rows,first_item,input_row,task_id,generation,deadline,request_id):
        first=next((r for r in rows if r['id']==first_item),None)
        if not first or first['state']!='pending' or first['bytes']!=input_row['bytes']:
            raise ValueError('首份准备文件与本批清单不一致')
        self.verify_preparation(db,request_id,first_item,input_row,first)
        db.execute('INSERT INTO web_intakes VALUES(?,?,?,?,?)',
            (batch_id,self.fingerprint(rows),self.w.manager.epoch,generation,deadline))
        for index,r in enumerate(rows):
            ready=r['id']==first_item
            db.execute('INSERT INTO web_intake_items VALUES(?,?,?,?,?,?,?,?,?,?)',
                (batch_id,r['id'],index,r['name'],r['relative_path'],r['bytes'],
                 'ready' if ready else r['state'],input_row['id'] if ready else None,
                 task_id if ready else None,r['error']))

    def pending(self, batch_id=None):
        with self.w.store.db(timeout=.1) as db:
            if batch_id is None:
                row=db.execute('SELECT id FROM web_batches ORDER BY created DESC LIMIT 1').fetchone()
                batch_id=row['id'] if row else None
            return bool(db.execute("SELECT 1 FROM web_intake_items WHERE batch_id=? AND state='pending' LIMIT 1",(batch_id,)).fetchone())

    def summary(self,batch_id):
        with self.w.store.db(timeout=.1) as db:
            header=db.execute('SELECT * FROM web_intakes WHERE batch_id=?',(batch_id,)).fetchone()
            if not header:return None
            request=db.execute('SELECT request_id FROM web_batches WHERE id=?',(batch_id,)).fetchone()[0]
            counts={r['state']:r['n'] for r in db.execute('SELECT state,COUNT(*) n FROM web_intake_items WHERE batch_id=? GROUP BY state',(batch_id,))}
            queue_tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('scheduler_jobs','scheduler_attempts','control_executions')")}
            settled_hold=''
            if len(queue_tables)==3:
                # A queue hold can retain the last resumable task stage. Release
                # preparation capacity only after every processing owner exited;
                # this read never retries the hold or changes remote barriers.
                settled_hold=''' AND NOT (
                    EXISTS(SELECT 1 FROM scheduler_jobs j WHERE j.kind='graph'
                        AND j.unit_key=t.task_id AND j.state='held')
                    AND NOT EXISTS(SELECT 1 FROM scheduler_jobs j
                        WHERE j.kind IN ('graph','repair') AND j.unit_key=t.task_id
                        AND (j.state NOT IN ('held','complete','superseded') OR (j.state='held' AND NOT EXISTS(
                            SELECT 1 FROM scheduler_attempts a JOIN control_executions e ON e.execution_id=a.execution_id
                            WHERE a.job_id=j.job_id AND a.state='finished'
                            AND a.attempt_id=(SELECT latest.attempt_id FROM scheduler_attempts latest
                                WHERE latest.job_id=j.job_id ORDER BY latest.started DESC,latest.rowid DESC LIMIT 1)
                            AND e.unit_kind='graph' AND e.unit_key=t.task_id
                            AND e.state='exited' AND COALESCE(e.exit_evidence,'')!=''))))
                    AND NOT EXISTS(SELECT 1 FROM control_executions e
                        WHERE e.unit_kind='graph' AND e.unit_key=t.task_id
                        AND (e.state!='exited' OR
                            CASE WHEN json_valid(e.exit_evidence)
                                THEN json_extract(e.exit_evidence,'$.all_recorded_processes_exited') END IS NOT 1)))'''
            unfinished=db.execute('''SELECT COUNT(DISTINCT i.task_id) FROM web_intake_items i JOIN tasks t ON t.task_id=i.task_id
                WHERE i.batch_id=? AND t.stage NOT IN ('handed_off','attachment_ready','held','parked','superseded','complete','code_error','excluded')'''
                +settled_hold,(batch_id,)).fetchone()[0]
            unique=db.execute('SELECT COUNT(DISTINCT task_id) FROM web_intake_items WHERE batch_id=?',(batch_id,)).fetchone()[0]
        return dict(batch_id=batch_id,request_id=request,fingerprint=header['fingerprint'],epoch=header['epoch'],generation=header['generation'],
            total=sum(counts.values()),ready=counts.get('ready',0),pending=counts.get('pending',0),failed=counts.get('failed',0),
            skipped=counts.get('skipped',0),open=bool(counts.get('pending',0)),unique_tasks=unique,deadline=header['deadline'],
            preparing_limit=self.w.config.article_workers or self.w.config.llm_workers,unfinished_graphs=unfinished)

    def listing(self,batch_id,offset=0,limit=20,grouped=False):
        if type(offset) is not int or offset<0 or type(limit) is not int or not 1<=limit<=200:
            raise ValueError('清单分页参数无效')
        with self.w.store.db(timeout=.1) as db:
            query='''SELECT item_id id,name,relative_path,bytes,state,input_id,task_id,error
                FROM web_intake_items WHERE batch_id=? ORDER BY position'''
            rows=[dict(r) for r in db.execute(query if grouped else query+' LIMIT ? OFFSET ?',
                (batch_id,) if grouped else (batch_id,limit,offset))]
        main_total=None
        if grouped:
            bodies={'.pdf','.doc','.docx','.docm','.wps','.rtf','.odt','.txt','.md'}
            folders={}
            for r in rows:folders.setdefault(r['relative_path'].rpartition('/')[0],[]).append(r)
            hidden=set()
            for values in folders.values():
                papers=[r for r in values if Path(r['name']).suffix.lower() in bodies and r['state']!='skipped']
                extras=[r for r in values if Path(r['name']).suffix.lower() not in bodies and r['state']!='skipped']
                if len(papers)==1:
                    papers[0]['attachments']=extras;hidden.update(r['id'] for r in extras)
            rows=[r for r in rows if r['id'] not in hidden];main_total=len(rows);rows=rows[offset:offset+limit]
        return dict(summary=self.summary(batch_id),items=rows,offset=offset,main_total=main_total)

    def authorize_resume(self,batch_id,generation,deadline):
        from .attachments import MATERIAL_EXTENSIONS
        from .workbench import FORMATS,MAX_UPLOAD
        with self.w.store.db() as db:
            db.execute('UPDATE web_intakes SET epoch=?,generation=?,deadline=? WHERE batch_id=?',
                (self.w.manager.epoch,generation,deadline,batch_id))
            db.execute("UPDATE web_intake_items SET state='pending',error='' WHERE batch_id=? AND state='failed'",(batch_id,))
            for row in db.execute("SELECT item_id,name,bytes FROM web_intake_items WHERE batch_id=? AND state='skipped' AND error='格式暂不支持'",(batch_id,)).fetchall():
                if Path(row['name']).suffix.lower() in FORMATS and 0<row['bytes']<=MAX_UPLOAD:
                    db.execute("UPDATE web_intake_items SET state='pending',error='' WHERE batch_id=? AND item_id=?",(batch_id,row['item_id']))
            return [row['item_id'] for row in db.execute("SELECT item_id,name FROM web_intake_items WHERE batch_id=? AND state='pending'",(batch_id,))
                    if Path(row['name']).suffix.lower() in MATERIAL_EXTENSIONS]

    def _scope(self,rows,deadline):
        """Extend the verified source union without rehashing every previous file."""
        w=self.w
        if time.time()>=deadline:raise ValueError('本批执行授权已到期，请明确继续')
        item=w._check_deployment()
        if item:
            save(w.config.production_pilot,dict(mode='production-pilot',root=str(w.root),endpoint=w.config.rag_url,
                catalog_sha256=item['catalog_sha256'],expires_at=deadline,maximum_seconds=item['maximum_seconds'],
                sources=[dict(path=r['path'],sha256=r['sha256']) for r in rows]))

    def append(self,batch_id,items,epoch,generation):
        from .workbench import ControlConflict
        w=self.w
        if (not isinstance(batch_id,str) or not isinstance(items,list) or not 1<=len(items)<=32
                or any(not isinstance(r,dict) or not isinstance(r.get('item_id'),str) for r in items)
                or len({r['item_id'] for r in items})!=len(items) or type(generation) is not int):
            raise ValueError('本批追加清单无效')
        with w.command_lock:
            state=w._expected(epoch,generation)
            if state['state']!='running' or (w.root/'STOP').exists():
                raise ControlConflict('新任务已暂停，请明确继续后再准备')
            with w.store.db(timeout=.1) as db:
                header=db.execute('SELECT * FROM web_intakes WHERE batch_id=?',(batch_id,)).fetchone()
                latest=db.execute('SELECT * FROM web_batches ORDER BY created DESC LIMIT 1').fetchone()
                if (not header or not latest or latest['id']!=batch_id or header['epoch']!=epoch
                        or header['generation']!=generation or time.time()>=header['deadline']):
                    raise ControlConflict('批次或准备授权已失效，请刷新后明确继续')
                verified=[]
                for item in items:
                    entry=db.execute('SELECT * FROM web_intake_items WHERE batch_id=? AND item_id=?',(batch_id,item['item_id'])).fetchone()
                    if not entry or entry['state']=='skipped':raise ValueError('文件不在本批可处理清单中')
                    if item.get('error'):
                        if entry['state']=='ready':raise ValueError('已就绪文件不能改为准备失败')
                        verified.append((entry,None,str(item['error'])[:500]));continue
                    row=db.execute('SELECT * FROM web_inputs WHERE id=?',(item.get('input_id'),)).fetchone()
                    if not row or row['bytes']!=entry['bytes'] or (entry['input_id'] and entry['input_id']!=row['id']):
                        raise ValueError('准备副本与本批文件不一致')
                    self.verify_preparation(db,latest['request_id'],item['item_id'],row,entry)
                    if sha(Path(row['path']))!=row['sha256']:raise ValueError('准备副本已改变')
                    verified.append((entry,row,''))
            selections=json.loads(latest['selection']);tasks=json.loads(latest['task_ids'])
            # SQL and file work never hold the immediate-stop lock. A pause may
            # preserve registered progress, but the final gate below cannot open.
            with w.store.db() as db:
                control=db.execute('SELECT epoch FROM controller_state WHERE id=1').fetchone()
                if not control or control['epoch']!=epoch:raise ControlConflict('管理进程已换代，拒绝旧申请')
                for entry,row,error in verified:
                    if error:
                        db.execute("UPDATE web_intake_items SET state='failed',error=? WHERE batch_id=? AND item_id=?",(error,batch_id,entry['item_id']));continue
                    task=w.store.register_source_sql(db,row['path'],row['sha256'],display_name=entry['name'])['task_id']
                    if row['id'] not in selections:selections.append(row['id'])
                    if task not in tasks:tasks.append(task)
                    db.execute("UPDATE web_intake_items SET state='ready',input_id=?,task_id=?,error='' WHERE batch_id=? AND item_id=?",
                        (row['id'],task,batch_id,entry['item_id']))
                db.execute('UPDATE web_batches SET selection=?,task_ids=? WHERE id=?',(json.dumps(selections),json.dumps(tasks),batch_id))
            state=w._expected(epoch,generation)
            if state['state']!='running' or (w.root/'STOP').exists():raise ControlConflict('暂停优先生效，准备进度已保存')
            with w.store.db(timeout=.1) as db:
                rows=[dict(r) for r in db.execute('SELECT * FROM web_inputs WHERE id IN (SELECT value FROM json_each(?))',(json.dumps(selections),))]
            self._scope(rows,header['deadline'])
            try:w.scheduler.service.extend_selected_tasks(tasks,header['deadline'],epoch,generation)
            except ValueError as exc:raise ControlConflict('暂停优先生效，旧准备申请不能放行') from exc
            with w.store.db(timeout=.1) as db:
                accepted=[dict(r) for r in db.execute('''SELECT item_id,task_id FROM web_intake_items
                    WHERE batch_id=? AND item_id IN (SELECT value FROM json_each(?))''',(batch_id,json.dumps([i['item_id'] for i in items])))]
            return dict(batch_id=batch_id,manager=w.manager.status(),intake=self.summary(batch_id),items=accepted)
