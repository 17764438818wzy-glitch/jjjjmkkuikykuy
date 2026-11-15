"""One bounded watch/dispatcher for native new-article classification and rating."""
from contextlib import nullcontext
import hashlib
import json
from pathlib import Path
import time
from urllib.error import HTTPError,URLError
from urllib.request import ProxyHandler,build_opener

from filelock import FileLock
from article_repository.ratings import RUBRICS,canonical,profile_sha256,profile_evidence
from rating_executor import FixedChannels,HeaderRecorder,DispatchBlocked,ClaimBlocked,NoRedirect,read_json,write_json,sha,request_once
from rating_queue import RatingQueue
from rating_admission import native


class RatingWatch:
    def __init__(self,runner,classification_registry,*,cache_root,bootstrap_since=None):
        self.runner=runner;self.queue=RatingQueue(runner);self.cache=Path(cache_root).resolve()
        self.classifier=FixedChannels(classification_registry,allowed_categories={'classification'})
        if set(self.classifier.channels)!={'classification'} or self.classifier.total!=1 or self.classifier.channels['classification']['max_concurrency']!=1:
            raise ValueError('缺类分类器必须单独固定为1并发，受主派单gate及同一执行锁管理')
        self.rules=read_json(Path(__file__).with_name('classification-rules.json'))
        if hashlib.sha256(self.rules['system'].encode()).hexdigest()!=self.rules['system_sha256']:raise ValueError('分类规则内容哈希不符')
        self.bootstrap_since=bootstrap_since;self.initialized=False

    def _initialize(self):
        self.queue.initialize()
        with self.runner.connect() as conn:
            for key,value in [('classification_binding',self.classifier.signature),('classification_rules_sha256',self.rules['system_sha256'])]:
                prior=conn.execute('SELECT value FROM metadata WHERE key=?',(key,)).fetchone()
                if prior and prior[0]!=value:raise ValueError('既存分类绑定或规则发生漂移；不能自动换key/提示词')
                conn.execute('INSERT OR IGNORE INTO metadata VALUES(?,?)',(key,value))
        self.initialized=True

    def _freeze(self,aid):
        packet=self.runner._api('/api/library/articles/'+aid+'/rating-input')
        if packet['article_id']!=aid:raise ValueError('画像文章身份不符')
        snapshot=packet['profile_snapshot'];digest=profile_sha256(snapshot)
        if digest!=packet['profile_sha256'] or profile_evidence(snapshot)!=packet['profile_evidence']:raise ValueError('画像协议或哈希不符')
        path=self.cache/(aid+'-'+digest+'.json')
        if path.exists():
            if profile_sha256(read_json(path))!=digest:raise ValueError('既存冻结画像文件发生漂移')
        else:write_json(path,snapshot)
        packet['snapshot_path']=str(path);return packet

    def _item(self,packet,*,classification=False):
        category='classification' if classification else packet['research_type']
        rubric={'version':'native-six-category/1.0','sha256':self.rules['system_sha256']} if classification else RUBRICS[category]
        return {'article_id':packet['article_id'],'revision':packet['revision'],'research_type':category,
            'stage':'classification' if classification else 'rating','evidence_basis':'profile',
            'profile_snapshot_path':packet['snapshot_path'],'profile_sha256':packet['profile_sha256'],
            'source_files':[],'rubric_version':rubric['version'],'rubric_sha256':rubric['sha256'],'preparation_status':'ready'}

    def _discovery_state(self,aid,status,error=''):
        with self.runner.connect() as conn:conn.execute('UPDATE discovery_queue SET status=?,error=?,updated_at=?,next_check=? WHERE article_id=?',
            (status,error,time.time(),time.time()+30 if status=='blocked_prior_attempt' else 0,aid))

    def _read_failure(self,aid,exc):
        transient=isinstance(exc,(URLError,TimeoutError,OSError)) or isinstance(exc,HTTPError) and exc.code in (408,409,425,429,500,502,503,504)
        if not transient:self._discovery_state(aid,'needs_input',type(exc).__name__+': '+str(exc)[:500]);return
        with self.runner.connect() as conn:
            prior=conn.execute('SELECT read_failures FROM discovery_queue WHERE article_id=?',(aid,)).fetchone()
            failures=(prior[0] if prior else 0)+1
            conn.execute("UPDATE discovery_queue SET status='read_retry',error=?,updated_at=?,next_check=?,read_failures=? WHERE article_id=?",
                (type(exc).__name__+': '+str(exc)[:300],time.time(),time.time()+min(300,10*2**min(failures-1,5)),failures,aid))

    def _classification_state(self,aid,status,error=''):
        with self.runner.connect() as conn:conn.execute('UPDATE classification_jobs SET status=?,error=?,updated_at=? WHERE article_id=?',(status,error,time.time(),aid))

    def _classify(self,row):
        if self.runner._quarantined(row[0]):return False
        aid,_,_,item_text,rid,operation_id,status,_,_=row;item=json.loads(item_text)
        work=self.runner.root/'requests'/rid;work.mkdir(parents=True,exist_ok=True)
        try:
            state_path=work/'request-state.json'
            if not state_path.exists() and status in ('inflight','write_pending','write_unknown'):
                if status in ('write_pending','write_unknown') and (work/'classification-write-intent.json').exists():
                    try:
                        payload=read_json(work/'classification-write-intent.json')
                        expected=hashlib.sha256(canonical({'article_id':aid,**payload}).encode()).hexdigest()
                        receipt=self.runner._api('/api/library/articles/'+aid+'/research-type?operation_id='+operation_id)
                        if (payload['run_id'],payload['request_id'],payload['operation_id'],payload['revision'],payload['profile_sha256'])!=(self.runner.run_id,rid,operation_id,item['revision'],item['profile_sha256']):raise ValueError('原分类写入身份不符')
                        if receipt.get('operation_id')!=operation_id or receipt.get('payload_digest')!=expected:raise ValueError('原分类回执不符')
                        write_json(work/'classification-write-receipt.json',receipt);self._classification_state(aid,'written');return True
                    except Exception:pass
                self._classification_state(aid,'uncertain','曾发送分类缺少原生回执，保留原操作，不重新派发')
                self.runner._halt(aid,'曾发送分类缺少原生回执或原写回尚未确认');return False
            if state_path.exists():raw,request,state=self.runner._complete_saved(work)
            else:
                if status!='prepared':raise ValueError('分类恢复缺少原生回执；不能重发')
                packet=self._freeze(aid)
                if packet['research_type'] in RUBRICS:raise ValueError('原生已产生有效类别，跳过分类；等待重新发现')
                if (packet['revision'],packet['profile_sha256'])!=(item['revision'],item['profile_sha256']):raise ValueError('分类发送前有效画像已变化')
                self.runner._permit();connection,headers,registry_sha=self.classifier.connection('classification')
                body={'model':self.classifier.channels['classification']['model'],'temperature':0,'max_tokens':2048,
                    'stream':True,'stream_options':{'include_usage':True},'thinking':{'type':'disabled'},'reasoning_effort':'none',
                    'messages':[{'role':'system','content':self.rules['system']},
                        {'role':'user','content':canonical({'articles':[{'id':aid,**packet['profile_evidence']}]})}]}
                if len(canonical(body).encode())>16*1024*1024:raise ValueError('分类画像超过输入容量，未截断或发送')
                self.runner._claim(row,body=body,stage='classification')
                channel=self.classifier.channels['classification']
                write_json(work/'binding.json',{'research_type':'classification','key_id':channel['key_id'],
                    'key_fingerprint_sha256_16':channel['key_fingerprint_sha256_16'],'model':body['model'],
                    'endpoint':channel['endpoint'],'registry_sha256':registry_sha})
                def permit():self.runner._permit();self.classifier.check_open();self.runner._begin_claim(row)
                opener=build_opener(ProxyHandler({}),NoRedirect(),HeaderRecorder(headers,work/'http-receipt.json'))
                self._classification_state(aid,'inflight')
                request_once(body,connection,work,opener=opener,on_request_ready=permit)
                raw,request,state=self.runner._complete_saved(work)
            data=native._json(raw)
            if (not isinstance(data,dict) or set(data)!={'items'} or not isinstance(data['items'],list) or len(data['items'])!=1
                or not isinstance(data['items'][0],dict) or set(data['items'][0])!={'id','research_type'} or data['items'][0]['id']!=aid):raise ValueError('分类输出未唯一完整覆盖原文章')
            category=data['items'][0]['research_type']
            if category is None:self._classification_state(aid,'needs_review','模型明确材料不足，原版本不重复调用');self.runner._terminal_claim(row);return False
            if category not in RUBRICS:raise ValueError('分类输出类别不在批准六类内')
            payload={'operation_id':operation_id,'request_id':rid,'run_id':self.runner.run_id,'revision':item['revision'],
                'profile_sha256':item['profile_sha256'],'research_type':category,'model':request['model']}
            path='/api/library/articles/'+aid+'/research-type'
            expected=hashlib.sha256(canonical({'article_id':aid,**payload}).encode()).hexdigest()
            def read_receipt():
                receipt=self.runner._api(path+'?operation_id='+operation_id)
                if receipt.get('operation_id')!=operation_id or receipt.get('payload_digest')!=expected:raise ValueError('分类操作回执身份不符')
                write_json(work/'classification-write-receipt.json',receipt);self._classification_state(aid,'written');return True
            self._classification_state(aid,'write_pending')
            try:
                if (work/'classification-write-intent.json').exists():
                    try:return read_receipt()
                    except HTTPError as exc:
                        if exc.code!=404:raise
                write_json(work/'classification-write-intent.json',payload)
                self.runner._api(path,payload);return read_receipt()
            except HTTPError as exc:
                if exc.code in (409,422):self._classification_state(aid,'stale','原子类别补充被拒绝，未覆盖现有记录');self.runner._terminal_claim(row);return False
                try:return read_receipt()
                except Exception:self._classification_state(aid,'write_unknown','保留分类operation原回执待确认');return False
            except Exception:
                try:return read_receipt()
                except Exception:self._classification_state(aid,'write_unknown','保留分类operation原回执待确认');return False
        except DispatchBlocked as exc:
            self._classification_state(aid,'prepared',str(exc));return False
        except ClaimBlocked as exc:
            self._classification_state(aid,'blocked_prior_claim',str(exc));return False
        except Exception as exc:
            state_path=work/'request-state.json'
            complete=state_path.exists() and read_json(state_path).get('status')=='response_complete'
            if not state_path.exists() and (isinstance(exc,(URLError,TimeoutError,OSError)) or isinstance(exc,HTTPError) and exc.code in (408,409,425,429,500,502,503,504)):
                self._classification_state(aid,'prepared','画像/接纳暂不可读；没有分类模型请求');return False
            if state_path.exists() and not complete:
                try:self.runner._defer(row,fixed=self.classifier,table='classification_jobs');return False
                except (ValueError,OSError,KeyError,TypeError):pass
                self.runner._halt(aid,'分类模型发送或结果未知，停止全队列新派发；原请求不重发')
                self._classification_state(aid,'uncertain',type(exc).__name__+': '+str(exc)[:300])
            else:
                self._classification_state(aid,'needs_review',type(exc).__name__+': '+str(exc)[:300])
                if complete:self.runner._terminal_claim(row)
            return False

    def _prepare(self,aid):
        if self.runner._quarantined(aid):
            self._discovery_state(aid,'quarantined_unknown','原未知请求保留；本文章各阶段及后续版本均不自动重发')
            return 'quarantined_unknown'
        packet=self._freeze(aid)
        if packet['research_type'] in RUBRICS:
            status=self.queue.enqueue(self._item(packet));self._discovery_state(aid,status);return status
        facts=packet['profile_evidence'];profile=facts.get('profile') or {};fields=facts.get('fields') or {}
        if not (profile.get('units') or profile.get('path') or fields.get('methods') or fields.get('path')):
            self._discovery_state(aid,'needs_input','缺少研究过程事实，等待原生画像更新；未猜类/打零分');return 'needs_input'
        status=self.queue.enqueue(self._item(packet,classification=True),'classification')
        self._discovery_state(aid,status)
        return 'classification_'+status

    def pass_once(self,*,new_wave=6,history_wave=6,discover_pages=1,only_article_ids=(),_lock_held=False,dispatch=True):
        if type(new_wave) is not int or new_wave<1 or type(history_wave) is not int or history_wave<0:raise ValueError('新稿波次必须为正、历史波次不得为负')
        with (nullcontext() if _lock_held else FileLock(str(self.runner.root/'executor.lock'),timeout=0)):
            if not self.initialized:self._initialize()
            discoveries=[self.queue.discover(bootstrap_since=self.bootstrap_since) for _ in range(discover_pages)]
            with self.runner.connect() as conn:
                pending=conn.execute("SELECT article_id FROM discovery_queue WHERE status IN ('pending','read_retry','blocked_prior_attempt') AND next_check<=? ORDER BY updated_at DESC,article_id",(time.time(),)).fetchall()
            if only_article_ids:pending=[r for r in pending if r[0] in set(only_article_ids)]
            pending=pending[:new_wave]
            prepared=[]
            for (aid,) in pending:
                try:prepared.append({'article_id':aid,'status':self._prepare(aid)})
                except Exception as exc:self._read_failure(aid,exc)
            classified=[]
            if dispatch:
                with self.runner.connect() as conn:rows=conn.execute("SELECT * FROM classification_jobs WHERE status IN ('prepared','inflight','write_pending','write_unknown') ORDER BY updated_at DESC").fetchall()
                if only_article_ids:rows=[r for r in rows if r[0] in set(only_article_ids)]
                rows=rows[:1]
                for row in rows:
                    if self._classify(row):
                        classified.append(row[0])
                        try:self._prepare(row[0])
                        except Exception as exc:self._read_failure(row[0],exc)
            priority=[r['article_id'] for r in prepared]+classified
            status=self.runner.status()
            if dispatch:
                # New articles get a bounded first wave; original historical jobs
                # get a bounded second wave. Both share one process and lock.
                status=self.runner.run(acceptance_only=False,max_jobs=new_wave,priority_ids=priority,only_article_ids=only_article_ids,_lock_held=True)
                if history_wave:status=self.runner.run(acceptance_only=False,max_jobs=history_wave,only_article_ids=only_article_ids,_lock_held=True)
            with self.runner.connect() as conn:
                counts=dict(conn.execute('SELECT status,count(*) FROM discovery_queue GROUP BY status'))
                classifications=dict(conn.execute('SELECT status,count(*) FROM classification_jobs GROUP BY status'))
            report={'discoveries':discoveries,'prepared':prepared,'classified':classified,'discovery_counts':counts,
                'classification_counts':classifications,'status':status,'single_dispatcher':True}
            write_json(self.runner.root/'watch-status.json',report);return report

    def run(self,*,poll_seconds=30,max_rounds=None,**options):
        if not 1<=poll_seconds<=60:raise ValueError('发现轮询应在1至60秒')
        with FileLock(str(self.runner.root/'executor.lock'),timeout=0):
            rounds=0
            while True:
                report=self.pass_once(_lock_held=True,**options);print(json.dumps(report,ensure_ascii=False),flush=True)
                rounds+=1
                if max_rounds is not None and rounds>=max_rounds:return report
                time.sleep(poll_seconds)
