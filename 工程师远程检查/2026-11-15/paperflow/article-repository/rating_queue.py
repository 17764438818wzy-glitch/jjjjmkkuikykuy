"""Semantic input versions and durable discovery in the existing rating journal."""
import hashlib
import json
import time
import uuid

from article_repository.ratings import canonical


def version_key(item,stage='rating'):
    value={'article_id':item['article_id'],'stage':stage,'revision':item['revision'],
        'profile_sha256':item.get('profile_sha256'),'source_files':item.get('source_files',[]) if item.get('evidence_basis')!='profile' else [],
        'research_type':item['research_type'],'rubric_sha256':item['rubric_sha256']}
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def stored_version(row,stage):
    item=json.loads(row[3])
    if (type(item.get('revision')) is not int or item.get('preparation_status')!='ready'
        or item.get('evidence_basis')=='profile' and not item.get('profile_sha256')):
        # Unfrozen legacy entries have no research version to invent. Preserve
        # their exact original row/input identity until a real packet exists.
        return 'legacy-unfrozen:'+hashlib.sha256((row[0]+':'+row[2]).encode()).hexdigest()
    return version_key(item,stage)


class RatingQueue:
    def __init__(self,runner):self.runner=runner

    def initialize(self):
        with self.runner.connect() as conn:
            conn.executescript('''CREATE TABLE IF NOT EXISTS job_versions(
                version_id TEXT PRIMARY KEY,article_id TEXT NOT NULL,stage TEXT NOT NULL,
                request_id TEXT UNIQUE NOT NULL,item TEXT NOT NULL,created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS job_history(request_id TEXT PRIMARY KEY,version_id TEXT NOT NULL,
                row_snapshot TEXT NOT NULL,status TEXT NOT NULL,archived_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS classification_jobs(article_id TEXT PRIMARY KEY,research_type TEXT NOT NULL,
                input_digest TEXT NOT NULL,item TEXT NOT NULL,request_id TEXT UNIQUE NOT NULL,operation_id TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,error TEXT NOT NULL DEFAULT '',updated_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS discovery_queue(article_id TEXT PRIMARY KEY,descriptor TEXT NOT NULL,
                status TEXT NOT NULL,error TEXT NOT NULL DEFAULT '',updated_at REAL NOT NULL,last_sequence INTEGER NOT NULL DEFAULT 0,
                next_check REAL NOT NULL DEFAULT 0,read_failures INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS acceptance_completed(research_type TEXT PRIMARY KEY,article_id TEXT NOT NULL,
                request_id TEXT NOT NULL,operation_id TEXT NOT NULL,completed_at REAL NOT NULL);''')
            columns={r[1] for r in conn.execute('PRAGMA table_info(discovery_queue)')}
            for name,definition in [('next_check','REAL NOT NULL DEFAULT 0'),('read_failures','INTEGER NOT NULL DEFAULT 0')]:
                if name not in columns:conn.execute('ALTER TABLE discovery_queue ADD COLUMN '+name+' '+definition)
            for table,stage in [('jobs','rating'),('classification_jobs','classification')]:
                for row in conn.execute('SELECT * FROM '+table).fetchall():
                    item=json.loads(row[3]);identity=stored_version(row,stage)
                    conn.execute('INSERT OR IGNORE INTO job_versions VALUES(?,?,?,?,?,?)',(identity,row[0],stage,row[4],row[3],time.time()))
            conn.execute('''INSERT OR IGNORE INTO acceptance_completed
                SELECT a.research_type,j.article_id,j.request_id,j.operation_id,j.updated_at
                FROM acceptance a JOIN jobs j ON j.article_id=a.article_id WHERE j.status='written' ''')

    def enqueue(self,item,stage='rating'):
        table='jobs' if stage=='rating' else 'classification_jobs'
        identity=version_key(item,stage);aid=item['article_id']
        with self.runner.connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            if conn.execute('SELECT 1 FROM job_versions WHERE version_id=?',(identity,)).fetchone():return 'existing_version'
            # A true unknown or explicitly parked attempt in either stage blocks
            # automatic promotion, including a profile or category revision.
            for other in ('jobs','classification_jobs'):
                prior=conn.execute('SELECT * FROM '+other+' WHERE article_id=?',(aid,)).fetchone()
                if prior and prior[6] in ('uncertain','inflight','deferred_not_dispatched','write_unknown','write_pending'):
                    return 'blocked_prior_attempt'
            prior=conn.execute('SELECT * FROM '+table+' WHERE article_id=?',(aid,)).fetchone()
            if prior:
                work=self.runner.root/'requests'/prior[4]
                if prior[6] in ('prepared','needs_input') and (work/'request-state.json').exists():return 'blocked_prior_attempt'
                if table=='jobs' and prior[6] in ('written','accepted_superseded'):
                    conn.execute('''INSERT OR IGNORE INTO acceptance_completed
                        SELECT research_type,article_id,?,?,? FROM acceptance WHERE article_id=?''',(prior[4],prior[5],time.time(),aid))
                conn.execute('INSERT OR IGNORE INTO job_history VALUES(?,?,?,?,?)',
                    (prior[4],stored_version(prior,stage),canonical(list(prior)),prior[6],time.time()))
                conn.execute('DELETE FROM '+table+' WHERE article_id=?',(aid,))
            input_digest=hashlib.sha256(canonical(item).encode()).hexdigest()
            request_id=str(uuid.uuid5(uuid.NAMESPACE_URL,self.runner.run_id+':'+aid+':'+input_digest))
            suffix=':rating-write' if stage=='rating' else ':classification-write'
            operation_id=str(uuid.uuid5(uuid.NAMESPACE_URL,request_id+suffix))
            conn.execute('INSERT INTO '+table+' VALUES(?,?,?,?,?,?,?,?,?)',
                (aid,item['research_type'],input_digest,canonical(item),request_id,operation_id,'prepared','',time.time()))
            conn.execute('INSERT INTO job_versions VALUES(?,?,?,?,?,?)',(identity,aid,stage,request_id,canonical(item),time.time()))
        if stage=='rating':self.runner.categories.add(item['research_type'])
        return 'enqueued'

    def discover(self,*,bootstrap_since=None,page_limit=100):
        """Commit descriptors and cursor together before any profile/model work."""
        with self.runner.connect() as conn:meta=dict(conn.execute('SELECT key,value FROM metadata'))
        if meta.get('watch_bootstrap_complete')!='true':
            since=meta.get('watch_bootstrap_since') or bootstrap_since
            if not since:raise ValueError('首次watch必须明确bootstrap起始时间，不能默认漏掉新入库文章')
            from urllib.parse import urlencode
            packet=self.runner._api('/api/library/rating-discovery/bootstrap?'+urlencode(
                {'since':since,'cursor':meta.get('watch_bootstrap_cursor',''),'limit':page_limit}))
            updates={'watch_bootstrap_since':since,'watch_bootstrap_cursor':packet['next_cursor']}
            if not packet['next_cursor']:updates.update(watch_bootstrap_complete='true',watch_event_after=str(packet['event_checkpoint']))
        else:
            after=int(meta.get('watch_event_after',0))
            packet=self.runner._api('/api/library/rating-discovery?after='+str(after)+'&limit='+str(page_limit))
            updates={'watch_event_after':str(packet['next_after'])}
        with self.runner.connect() as conn:
            for descriptor in packet['rows']:
                aid=descriptor['article_id'];sequence=descriptor.get('sequence',0)
                conn.execute('''INSERT INTO discovery_queue(article_id,descriptor,status,error,updated_at,last_sequence) VALUES(?,?,'pending','',?,?)
                    ON CONFLICT(article_id) DO UPDATE SET descriptor=excluded.descriptor,status='pending',error='',
                    updated_at=excluded.updated_at,last_sequence=excluded.last_sequence,next_check=0,read_failures=0
                    WHERE excluded.last_sequence>=discovery_queue.last_sequence''',(aid,canonical(descriptor),time.time(),sequence))
            for key,value in updates.items():conn.execute('INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,value))
        return {'discovered':len(packet['rows']),**updates}
