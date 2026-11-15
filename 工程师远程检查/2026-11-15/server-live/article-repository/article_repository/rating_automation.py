"""Native catalog discovery and atomic missing-category annotation."""
import base64
import copy
from datetime import datetime,timezone
import hashlib
import json
import re

from psycopg.types.json import Jsonb

from .profiles import article_research_type,RESEARCH_TYPES
from .ratings import canonical,RUBRICS


class RatingAutomation:
    def _discovery_lock(self,conn):
        # Serialize event sequence allocation through commit, preventing an
        # earlier uncommitted sequence from appearing behind a saved cursor.
        conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',('article-rating-discovery:'+self.schema,))

    def _discover(self,conn,row,profile_sha='',reason='catalog'):
        self._discovery_lock(conn)
        conn.execute('''INSERT INTO article_rating_discovery(article_id,article_revision,profile_sha256,reason)
            VALUES(%s,%s,%s,%s) ON CONFLICT(article_id,article_revision,profile_sha256) DO NOTHING''',
            (row['id'],row['revision'],profile_sha,reason))

    def rating_discovery(self,*,after=0,limit=100):
        if type(after) is not int or after<0 or type(limit) is not int or not 1<=limit<=100:raise ValueError('增量游标或页大小无效')
        with self.connect() as conn:
            rows=conn.execute('''SELECT d.sequence,d.article_id,d.article_revision revision,d.profile_sha256,d.reason,
                a.updated_at,a.base#>>'{profile,research_profile,research_type}' research_type
                FROM article_rating_discovery d JOIN article a ON a.id=d.article_id
                WHERE d.sequence>%s AND coalesce(a.base->>'entry_kind','article')='article'
                ORDER BY d.sequence LIMIT %s''',(after,limit)).fetchall()
        for row in rows:row['updated_at']=row['updated_at'].isoformat()
        return {'rows':rows,'next_after':rows[-1]['sequence'] if rows else after,'has_more':len(rows)==limit}

    def rating_bootstrap(self,since,*,cursor='',limit=100):
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('初始发现页大小无效')
        with self.connect() as conn:
            if cursor:
                try:state=json.loads(base64.urlsafe_b64decode(cursor.encode()))
                except Exception as exc:raise ValueError('初始发现游标无效') from exc
            else:
                try:start=datetime.fromisoformat(since.replace('Z','+00:00'))
                except ValueError as exc:raise ValueError('首次发现必须明确起始时间') from exc
                if start.tzinfo is None:raise ValueError('首次发现起始时间必须包含时区')
                self._discovery_lock(conn)
                mark=conn.execute('SELECT clock_timestamp() until,coalesce(max(sequence),0) checkpoint FROM article_rating_discovery').fetchone()
                state={'since':start.isoformat(),'until':mark['until'].isoformat(),'checkpoint':mark['checkpoint'],'last_time':start.isoformat(),'last_id':''}
            if set(state)!={'since','until','checkpoint','last_time','last_id'}:raise ValueError('初始发现游标字段无效')
            rows=conn.execute('''SELECT id article_id,revision,updated_at,
                base#>>'{profile,research_profile,research_type}' research_type FROM article
                WHERE coalesce(base->>'entry_kind','article')='article' AND updated_at>=%s AND updated_at<=%s AND (updated_at,id)>(%s,%s)
                ORDER BY updated_at,id LIMIT %s''',(state['since'],state['until'],state['last_time'],state['last_id'],limit)).fetchall()
        for row in rows:row['updated_at']=row['updated_at'].isoformat()
        if rows:state.update(last_time=rows[-1]['updated_at'],last_id=rows[-1]['article_id'])
        token=base64.urlsafe_b64encode(canonical(state).encode()).decode() if len(rows)==limit else ''
        return {'rows':rows,'next_cursor':token,'event_checkpoint':state['checkpoint'],'window_until':state['until']}

    @staticmethod
    def _classification_view(row,*,replayed=False):
        return {**row['payload'],'article_id':row['article_id'],'operation_id':row['operation_id'],
            'payload_digest':row['payload_digest'],'revision':row['output_revision'],'replayed':replayed,'status':'accepted'}

    def classification(self,aid,operation_id):
        with self.connect() as conn:row=conn.execute('SELECT * FROM article_research_type_operation WHERE article_id=%s AND operation_id=%s',(aid,operation_id)).fetchone()
        if not row:raise LookupError('未找到分类操作回执')
        return self._classification_view(row,replayed=True)

    def classify(self,aid,body):
        required={'operation_id','request_id','run_id','revision','profile_sha256','research_type','model'}
        if set(body)!=required or body['research_type'] not in RESEARCH_TYPES:raise ValueError('分类操作字段无效')
        if (type(body['revision']) is not int or body['revision']<1 or not re.fullmatch('[a-f0-9]{64}',body['profile_sha256'])
            or any(not isinstance(body[k],str) or not body[k].strip() or len(body[k])>200 for k in ('operation_id','request_id','run_id','model'))):raise ValueError('分类身份或输入版本无效')
        digest=hashlib.sha256(canonical({'article_id':aid,**body}).encode()).hexdigest()
        with self.connect() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',('article-research-type:'+body['operation_id'],))
            old=conn.execute('SELECT * FROM article_research_type_operation WHERE operation_id=%s',(body['operation_id'],)).fetchone()
            if old:
                if old['payload_digest']!=digest:raise ValueError('分类operation_id已绑定其他输入')
                return self._classification_view(old,replayed=True)
            row=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()
            if not row:raise LookupError('未找到文章')
            if row['base'].get('entry_kind','article')!='article':raise ValueError('此记录不是文章，不得自动分类')
            if row['revision']!=body['revision']:raise ValueError('分类时文章版本已变化')
            if article_research_type(row['base'].get('profile') or {}):raise ValueError('文章已有有效类别，不能重新分类')
            frozen=conn.execute('SELECT * FROM article_rating_profile WHERE article_id=%s',(aid,)).fetchone()
            if not frozen or frozen['article_revision']!=row['revision'] or frozen['catalog_guard']!=self._profile_guard(row) or frozen['profile_sha256']!=body['profile_sha256']:raise ValueError('分类时有效画像已变化或尚未冻结')
            base=copy.deepcopy(row['base']);profile=base.setdefault('profile',{});research=profile.setdefault('research_profile',{})
            if not isinstance(research,dict):raise ValueError('原研究画像结构不支持原子类别补充，保留待核对')
            research['research_type']=body['research_type']
            row=conn.execute('UPDATE article SET base=%s,revision=revision+1,updated_at=clock_timestamp() WHERE id=%s RETURNING *',(Jsonb(base),aid)).fetchone()
            self._discover(conn,row,reason='category')
            receipt=conn.execute('''INSERT INTO article_research_type_operation(operation_id,article_id,payload_digest,payload,output_revision)
                VALUES(%s,%s,%s,%s,%s) RETURNING *''',(body['operation_id'],aid,digest,Jsonb(body),row['revision'])).fetchone()
            self._publish_claim(conn,aid,body,stage='classification')
            return self._classification_view(receipt)

    @staticmethod
    def _publish_claim(conn,aid,body,stage='rating'):
        conn.execute("""UPDATE article_rating_claim SET state='published',updated_at=clock_timestamp()
            WHERE article_id=%s AND operation_id=%s AND request_id=%s AND run_id=%s AND stage=%s
            AND payload->>'revision'=%s AND payload->>'profile_sha256'=%s AND payload->>'model'=%s""",
            (aid,body['operation_id'],body['request_id'],body['run_id'],stage,str(body['revision']),body.get('profile_sha256'),body['model']))

    def claim_rating(self,aid,body):
        required={'operation_id','request_id','run_id','revision','profile_sha256','research_type','model','stage','rubric_sha256','request_sha256'}
        if set(body)!=required or body['stage'] not in ('rating','classification'):raise ValueError('自动评档claim字段无效')
        if (type(body['revision']) is not int or body['revision']<1
            or any(not isinstance(body.get(k),str) or not re.fullmatch('[a-f0-9]{64}',body[k]) for k in ('profile_sha256','rubric_sha256','request_sha256'))
            or any(not isinstance(body.get(k),str) or not body[k].strip() or len(body[k])>200 for k in ('operation_id','request_id','run_id','model'))):raise ValueError('claim身份或版本无效')
        if body['stage']=='classification':
            if body['research_type']!='classification':raise ValueError('分类claim不能冒充已有类别')
        elif body['research_type'] not in RUBRICS or body['rubric_sha256']!=RUBRICS[body['research_type']]['sha256']:raise ValueError('评分claim规则未固定')
        identity={k:body[k] for k in ('stage','revision','profile_sha256','research_type','rubric_sha256')}
        version_id=hashlib.sha256(canonical({'article_id':aid,'source_files':[],**identity}).encode()).hexdigest()
        with self.connect() as conn:
            row=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()
            if not row:raise LookupError('未找到文章')
            if row['base'].get('entry_kind','article')!='article':raise ValueError('此记录不是文章')
            existing=conn.execute('SELECT * FROM article_rating_claim WHERE version_id=%s',(version_id,)).fetchone()
            if existing:
                own=existing['payload']==body and existing['state']=='claimed'
                return {'granted':own,'reason':'same_owner_not_sent' if own else 'version_already_claimed','version_id':version_id,
                    'request_id':existing['request_id'],'operation_id':existing['operation_id'],'run_id':existing['run_id'],'state':existing['state']}
            active=conn.execute("SELECT * FROM article_rating_claim WHERE article_id=%s AND state IN ('claimed','sending') LIMIT 1",(aid,)).fetchone()
            if active and active['state']=='sending':return {'granted':False,'reason':'prior_unresolved','version_id':active['version_id'],
                'request_id':active['request_id'],'operation_id':active['operation_id'],'run_id':active['run_id'],'state':active['state']}
            if row['revision']!=body['revision']:raise ValueError('claim文章版本已变化')
            category=article_research_type(row['base'].get('profile') or {})
            if body['stage']=='classification' and category:return {'granted':False,'reason':'already_classified'}
            if body['stage']=='rating' and category!=body['research_type']:raise ValueError('claim文章分类已变化')
            frozen=conn.execute('SELECT * FROM article_rating_profile WHERE article_id=%s',(aid,)).fetchone()
            if not frozen or frozen['article_revision']!=row['revision'] or frozen['catalog_guard']!=self._profile_guard(row) or frozen['profile_sha256']!=body['profile_sha256']:raise ValueError('claim有效画像已变化或未冻结')
            if active:
                # This owner never obtained a send permit. Revoke only after a
                # different current semantic input has passed native CAS checks.
                conn.execute("UPDATE article_rating_claim SET state='not_sent_superseded',updated_at=clock_timestamp() WHERE version_id=%s AND state='claimed'",(active['version_id'],))
            if body['stage']=='rating':
                rated=conn.execute('''SELECT operation_id FROM article_rating WHERE article_id=%s AND article_revision=%s
                    AND research_type=%s AND payload->>'rubric_sha256'=%s AND payload->>'profile_sha256'=%s LIMIT 1''',
                    (aid,body['revision'],category,body['rubric_sha256'],body['profile_sha256'])).fetchone()
                if rated:return {'granted':False,'reason':'already_rated','operation_id':rated['operation_id']}
            if conn.execute('SELECT 1 FROM article_rating_claim WHERE request_id=%s OR operation_id=%s',(body['request_id'],body['operation_id'])).fetchone():raise ValueError('claim请求/操作身份已用于其他输入')
            conn.execute('''INSERT INTO article_rating_claim(version_id,article_id,stage,request_id,operation_id,run_id,payload,state)
                VALUES(%s,%s,%s,%s,%s,%s,%s,'claimed')''',(version_id,aid,body['stage'],body['request_id'],body['operation_id'],body['run_id'],Jsonb(body)))
            return {'granted':True,'version_id':version_id,'request_id':body['request_id'],'operation_id':body['operation_id'],'run_id':body['run_id'],'state':'claimed'}

    def begin_rating_claim(self,aid,body):
        """One irreversible send permit; owner reservation alone never bills."""
        required={'run_id','request_id','operation_id','request_sha256','model'}
        if set(body)!=required:raise ValueError('claim发送许可身份字段无效')
        with self.connect() as conn:
            article=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()
            row=conn.execute('SELECT * FROM article_rating_claim WHERE article_id=%s AND request_id=%s AND operation_id=%s AND run_id=%s FOR UPDATE',
                (aid,body['request_id'],body['operation_id'],body['run_id'])).fetchone()
            if not row or any(row['payload'].get(k)!=body[k] for k in ('request_sha256','model')):raise ValueError('claim发送许可未绑定原请求')
            granted=row['state']=='claimed'
            if granted:
                frozen=conn.execute('SELECT * FROM article_rating_profile WHERE article_id=%s',(aid,)).fetchone()
                if not article or article['revision']!=row['payload']['revision'] or not frozen or frozen['catalog_guard']!=self._profile_guard(article) or frozen['profile_sha256']!=row['payload']['profile_sha256']:
                    conn.execute("UPDATE article_rating_claim SET state='not_sent_superseded',updated_at=clock_timestamp() WHERE version_id=%s",(row['version_id'],))
                    granted=False;row['state']='not_sent_superseded'
            if granted:conn.execute("UPDATE article_rating_claim SET state='sending',updated_at=clock_timestamp() WHERE version_id=%s",(row['version_id'],))
            return {'dispatch_granted':granted,'version_id':row['version_id'],'request_id':row['request_id'],
                'operation_id':row['operation_id'],'run_id':row['run_id'],'state':'sending' if granted else row['state']}

    def finish_rating_claim(self,aid,body):
        if set(body)!={'run_id','request_id','operation_id','native_state'}:raise ValueError('claim终止证据字段无效')
        proof=body['native_state']
        if (not isinstance(proof,dict) or proof.get('status') not in ('response_complete','partial_response')
            or proof.get('finish_reason') not in ('stop','length','content_filter') or proof.get('client_http_requests')!=1
            or any(not isinstance(proof.get(k),str) or not re.fullmatch('[a-f0-9]{64}',proof[k]) for k in ('response_sha256','request_sha256'))):raise ValueError('claim没有明确的原生终止证据，不得按过期解除')
        with self.connect() as conn:
            conn.execute('SELECT id FROM article WHERE id=%s FOR UPDATE',(aid,))
            row=conn.execute('SELECT * FROM article_rating_claim WHERE article_id=%s AND request_id=%s AND operation_id=%s AND run_id=%s FOR UPDATE',
                (aid,body['request_id'],body['operation_id'],body['run_id'])).fetchone()
            if not row:raise LookupError('未找到原claim身份')
            if proof.get('request_sha256')!=row['payload']['request_sha256'] or proof.get('model')!=row['payload']['model']:raise ValueError('终止证据不属于此claim的原模型请求')
            if row['state']=='sending':conn.execute("UPDATE article_rating_claim SET state='terminal_review',terminal_evidence=%s,updated_at=clock_timestamp() WHERE version_id=%s",(Jsonb(proof),row['version_id']))
            return {'status':'accepted','version_id':row['version_id'],'automatic_same_version_resend':False}
