import hashlib
import importlib.util
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path

from test_ratings import body


def save(path,value):
    path.write_text(json.dumps(value,ensure_ascii=False),encoding='utf-8')
    return hashlib.sha256(path.read_bytes()).hexdigest()


@contextmanager
def endpoints(mode='complete',rating_mutator=None):
    state={'model_calls':0,'write_calls':0,'receipts':{},'claims':{},'mode':mode,'active':0,'peak':0}
    state['profile_snapshot']={'profile':{'research_profile':{'research_type':'engineering_system','overall_path':'需求、实现和实际测试'},
        'applicable_level':{'labels':['旧重点本科'],'score':99},'article':{'author_degree':'博士','summary':'系统的实际实现'}},'manual_fields':{}}
    lock=threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def send_json(self,code,value,headers=()):
            data=json.dumps(value).encode();self.send_response(code);self.send_header('Content-Type','application/json')
            for key,text in headers:self.send_header(key,text)
            self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
        def do_GET(self):
            if self.path.endswith('/rating-input'):
                from article_repository.ratings import profile_sha256,profile_evidence
                snapshot=state['profile_snapshot']
                return self.send_json(200,{'article_id':self.path.split('/')[-2],'revision':state.get('profile_revision',1),'research_type':state.get('profile_category','engineering_system'),
                    'profile_snapshot':snapshot,'profile_sha256':profile_sha256(snapshot),'profile_evidence':profile_evidence(snapshot)})
            if '/rating?' in self.path:
                from urllib.parse import parse_qs,urlparse
                op=parse_qs(urlparse(self.path).query)['operation_id'][0]
                return self.send_json(200,state['receipts'][op]) if op in state['receipts'] else self.send_json(404,{'detail':'not found'})
            aid=self.path.rsplit('/',1)[-1]
            self.send_json(200,{'id':aid,'revision':1,'research_type':'engineering_system',
                'files':[{'id':'body','kind':'原文','sha256':'a'*64}]})
        def do_POST(self):
            value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if self.path.endswith('/rating-claim'):
                op=value['operation_id'];state['claims'].setdefault(op,{**value,'begun':False})
                return self.send_json(200,{'granted':not state['claims'][op]['begun'],**{k:value[k] for k in ('run_id','request_id','operation_id')}})
            if self.path.endswith('/rating-claim/begin'):
                old=state['claims'].get(value['operation_id']);granted=bool(old and not old['begun'])
                if old:old['begun']=True
                return self.send_json(200,{'dispatch_granted':granted,**{k:value[k] for k in ('run_id','request_id','operation_id')}})
            if self.path.endswith('/rating-claim/terminal'):return self.send_json(200,{'status':'accepted'})
            if self.path.endswith('/chat/completions'):
                with lock:
                    state['model_calls']+=1;state['active']+=1;state['peak']=max(state['peak'],state['active'])
                try:
                    import time
                    time.sleep(.04)
                    if state['mode']=='protected':return self.send_json(503,{'error':{'code':11140,'message':'region protection'}})
                    if state['mode'] in ('capacity','ordinary429','duplicate_capacity'):
                        headers=[('Retry-After','1')]
                        if state['mode']!='ordinary429':headers.append(('X-WorkBuddy-Admission','not-dispatched-v1'))
                        if state['mode']=='duplicate_capacity':headers.append(('X-WorkBuddy-Admission','not-dispatched-v1'))
                        return self.send_json(429,{'error':{'code':'workbuddy_local_capacity_not_sent','retry_after':1}},headers)
                    rating=body({'revision':1})['rating']
                    if rating_mutator:rating_mutator(rating)
                    if value['messages'][0]['content'].startswith('你只做论文主研究类型分类'):
                        state['classification_calls']=state.get('classification_calls',0)+1
                        data=json.loads(value['messages'][1]['content']);aid=data['articles'][0]['id']
                        rating={'items':[{'id':aid,'research_type':state.get('classifier_result','engineering_system')}]}
                    self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
                    event={'choices':[{'delta':{'content':json.dumps(rating,ensure_ascii=False)}}]}
                    self.wfile.write(('data: '+json.dumps(event)+'\n\n').encode());self.wfile.flush()
                    if state['mode'] in ('complete','write_disconnect','write_unavailable'):
                        self.wfile.write(b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n');self.wfile.flush()
                    if state['mode']=='length':
                        self.wfile.write(b'data: {"choices":[{"delta":{},"finish_reason":"length"}]}\n\ndata: [DONE]\n\n');self.wfile.flush()
                finally:
                    with lock:state['active']-=1
                return
            from article_repository.ratings import validate_rating,payload_digest
            if state['mode']=='write_unavailable':return self.send_json(503,{'detail':'temporary writer unavailable'})
            normalized=validate_rating(value);aid=self.path.split('/')[-2]
            receipt={'status':'accepted','operation_id':value['operation_id'],'article_id':aid,
                'payload_digest':payload_digest(aid,normalized),'rating':normalized['rating'],'current':True}
            state['write_calls']+=1;state['receipts'][value['operation_id']]=receipt
            if state['mode']=='write_disconnect':
                self.connection.close();return
            self.send_json(200,receipt)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield 'http://127.0.0.1:'+str(server.server_port),state
    finally:server.shutdown();server.server_close();thread.join()


def inputs(tmp_path,url,*,gate=True,count=1):
    from article_repository.ratings import RUBRICS
    rubric=RUBRICS['engineering_system']
    secret=tmp_path/'credential.json';save(secret,{'entries':[{'id':'test-key','key':'local-test-only'}]})
    registry=tmp_path/'channels.json'
    save(registry,{'schema_version':'test','channels':[{'research_type':'engineering_system','key_id':'test-key',
        'key_fingerprint_sha256_16':hashlib.sha256(b'local-test-only').hexdigest()[:16],
        'credential_ref':{'file':str(secret),'entry_id':'test-key','field':'key'},'realm':'cn','model':'test-model',
        'base_url':url+'/v1','endpoint':url+'/v1/chat/completions','max_concurrency':2,'headers':{'X-Realm':'cn'}}],
        'max_total_scoring_concurrency':2,'dispatch_gate':{'allow_new_scoring_requests':gate}})
    items=[]
    for i in range(count):
        messages=tmp_path/f'messages-{i}.json';evidence=tmp_path/f'evidence-{i}.json'
        msgsha=save(messages,[{'role':'user','content':'原文第2页：有实际系统方法和结果。'}])
        evsha=save(evidence,{'pages':[{'file_id':'body','page':2,'page_basis':'local PDF','text_available':True,'visual_included':False}],
            'critical_missing':[],'requires_visual':False})
        items.append({'article_id':f'article-{i}','revision':1,'research_type':'engineering_system',
            'source_files':[{'file_id':'body','sha256':'a'*64}], 'messages_path':str(messages),'messages_sha256':msgsha,
            'evidence_inventory_path':str(evidence),'evidence_inventory_sha256':evsha,'preparation_status':'ready'})
    manifest=tmp_path/'manifest.json'
    save(manifest,{'schema':'article-rating-manifest/1.0','research_type':'engineering_system',
        'rubric_version':rubric['version'],'rubric_sha256':rubric['sha256'],'items':items})
    return registry,manifest


def runner(tmp_path,registry,manifest,url):
    assert importlib.util.find_spec('rating_executor'),'unified once-only executor is missing'
    from rating_executor import RatingRunner
    return RatingRunner(registry,[manifest],tmp_path/'run',api_url=url)


def test_closed_dispatch_gate_sends_zero_business_requests(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url,gate=False)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['dispatch_blocked'] is True
        assert state['model_calls']==0
        assert state['write_calls']==0


def test_complete_response_is_written_once_and_recovered_without_resending(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['written']==1
        assert state['model_calls']==1 and state['write_calls']==1
        again=runner(tmp_path,registry,manifest,url).run()
        assert again['counts']['written']==1
        assert state['model_calls']==1 and state['write_calls']==1


def test_partial_response_remains_unknown_across_restart(tmp_path):
    with endpoints(mode='partial') as (url,state):
        registry,manifest=inputs(tmp_path,url)
        first=runner(tmp_path,registry,manifest,url).run()
        assert first['counts']['uncertain']==1
        runner(tmp_path,registry,manifest,url).run()
        assert state['model_calls']==1
        assert state['write_calls']==0


def test_model_arithmetic_is_derived_from_original_levels_before_writer(tmp_path):
    with endpoints(rating_mutator=lambda r:r.update(score=81)) as (url,state):
        registry,manifest=inputs(tmp_path,url)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['written']==1
        assert next(iter(state['receipts'].values()))['rating']['score']==80
        assert state['model_calls']==1 and state['write_calls']==1


def test_disconnected_write_is_reconciled_by_original_operation_receipt(tmp_path):
    with endpoints(mode='write_disconnect') as (url,state):
        registry,manifest=inputs(tmp_path,url)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['written']==1
        assert state['model_calls']==1 and state['write_calls']==1


def test_registry_drift_is_rejected_without_sending(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url)
        instance=runner(tmp_path,registry,manifest,url)
        changed=json.loads(registry.read_text());changed['channels'][0]['model']='different-model';save(registry,changed)
        result=instance.run()
        assert result['dispatch_blocked'] is True
        assert state['model_calls']==0 and state['write_calls']==0


def test_concurrency_respects_fixed_category_limit(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url,count=5)
        config=json.loads(registry.read_text());config['max_total_scoring_concurrency']=4;save(registry,config)
        instance=runner(tmp_path,registry,manifest,url)
        instance.run()
        result=instance.run(acceptance_only=False)
        assert result['counts']['written']==5
        assert state['model_calls']==5
        assert 1<=state['peak']<=2


def test_closed_gate_still_reconciles_a_complete_prior_response(tmp_path):
    with endpoints(mode='write_unavailable') as (url,state):
        registry,manifest=inputs(tmp_path,url)
        assert runner(tmp_path,registry,manifest,url).run()['counts']['write_unknown']==1
        config=json.loads(registry.read_text());config['dispatch_gate']['allow_new_scoring_requests']=False;save(registry,config)
        state['mode']='complete'
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['written']==1
        assert result['dispatch_blocked'] is True
        assert state['model_calls']==1


def test_missing_required_pixels_is_review_needed_without_model_dispatch(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url)
        document=json.loads(manifest.read_text())
        evidence=Path(document['items'][0]['evidence_inventory_path']);inventory=json.loads(evidence.read_text())
        inventory['requires_visual']=True;document['items'][0]['evidence_inventory_sha256']=save(evidence,inventory);save(manifest,document)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['needs_review']==1
        assert state['model_calls']==0 and state['write_calls']==0


def test_placeholder_image_data_is_not_readable_visual_evidence(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url);document=json.loads(manifest.read_text());item=document['items'][0]
        item['messages_sha256']=save(Path(item['messages_path']),[{'role':'user','content':[
            {'type':'text','text':'方法图'}, {'type':'image_url','image_url':{'url':'data:image/png;base64,not-image'}}]}])
        evidence=Path(item['evidence_inventory_path']);inventory=json.loads(evidence.read_text());inventory['requires_visual']=True
        item['evidence_inventory_sha256']=save(evidence,inventory);save(manifest,document)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['needs_review']==1
        assert state['model_calls']==0 and state['write_calls']==0


def test_acceptance_binds_first_article_across_incomplete_response_restart(tmp_path):
    with endpoints(mode='partial') as (url,state):
        registry,manifest=inputs(tmp_path,url,count=2)
        first=runner(tmp_path,registry,manifest,url).run()
        assert first['counts']['uncertain']==1
        second=runner(tmp_path,registry,manifest,url).run()
        assert second['counts']=={'prepared':1,'uncertain':1}
        assert second['dispatch_blocked'] is True
        assert state['model_calls']==1 and state['write_calls']==0


def test_protection_halts_remaining_batch_and_persists_across_restart(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url,count=5)
        config=json.loads(registry.read_text());config['max_total_scoring_concurrency']=1;save(registry,config)
        assert runner(tmp_path,registry,manifest,url).run()['counts']['written']==1
        state['mode']='protected'
        result=runner(tmp_path,registry,manifest,url).run(acceptance_only=False)
        assert result['dispatch_blocked'] is True
        assert result['counts']=={'written':1,'uncertain':1,'prepared':3}
        assert state['model_calls']==2
        assert (tmp_path/'run'/'dispatch-halt.json').exists()
        state['mode']='complete'
        again=runner(tmp_path,registry,manifest,url).run(acceptance_only=False)
        assert again['dispatch_blocked'] is True and state['model_calls']==2


def test_visual_page_inventory_cannot_claim_pixels_when_none_are_supplied(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url);document=json.loads(manifest.read_text());item=document['items'][0]
        evidence=Path(item['evidence_inventory_path']);inventory=json.loads(evidence.read_text())
        inventory.pop('requires_visual');inventory['pages'][0].update(text_available=False,visual_included=True)
        item['evidence_inventory_sha256']=save(evidence,inventory);save(manifest,document)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['needs_review']==1
        assert state['model_calls']==0 and state['write_calls']==0


def profile_inputs(tmp_path,url,state):
    from article_repository.ratings import profile_sha256
    registry,manifest=inputs(tmp_path,url);document=json.loads(manifest.read_text());document['schema']='article-rating-manifest/1.1'
    item=document['items'][0]
    for key in ('messages_path','messages_sha256','evidence_inventory_path','evidence_inventory_sha256'):item.pop(key)
    snapshot=tmp_path/'profile.json';save(snapshot,state['profile_snapshot'])
    item.update(evidence_basis='profile',profile_snapshot_path=str(snapshot),profile_sha256=profile_sha256(state['profile_snapshot']),source_files=[])
    save(manifest,document);return registry,manifest


def test_profile_executor_uses_facts_not_old_answers_or_original_pages(tmp_path):
    from rating_executor import read_json
    def refs(r):r['evidence_refs']=[{'field_path':'/profile_evidence/profile/path','note':'实际路线'}]
    with endpoints(rating_mutator=refs) as (url,state):
        registry,manifest=profile_inputs(tmp_path,url,state)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['written']==1 and state['model_calls']==1
        request=read_json(next((tmp_path/'run'/'requests').glob('*/request.json')))
        assert request['thinking']=={'type':'disabled'} and request['reasoning_effort']=='none'
        assert '旧重点本科' not in json.dumps(request,ensure_ascii=False) and '博士' not in json.dumps(request,ensure_ascii=False)
        receipt=next(iter(state['receipts'].values()))
        assert receipt['rating']['evidence_refs'][0].get('page') is None


def test_changed_effective_profile_is_blocked_before_model_dispatch(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=profile_inputs(tmp_path,url,state)
        state['profile_snapshot']['profile']['research_profile']['overall_path']='changed facts'
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['needs_review']==1 and state['model_calls']==0


def test_object_profile_priority_policy_rejects_old_original_input(tmp_path):
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url);config=json.loads(registry.read_text())
        config['grading_input_policy']={'mode':'profile_priority'};save(registry,config)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']['needs_review']==1 and state['model_calls']==0


def test_new_profile_run_preserves_terminal_prior_attempt_lineage(tmp_path):
    from rating_executor import RatingRunner,read_json
    with endpoints(mode='length') as (url,state):
        registry,manifest=inputs(tmp_path,url);runner(tmp_path,registry,manifest,url).run()
        old_root=tmp_path/'run';old_state=next((old_root/'requests').glob('*/request-state.json'));original=old_state.read_bytes()
        folder=tmp_path/'profile';folder.mkdir();new_registry,new_manifest=profile_inputs(folder,url,state)
        new_root=folder/'run'
        RatingRunner(new_registry,[new_manifest],new_root,api_url=url,supersedes_run_root=old_root)
        lineage=read_json(new_root/'supersedes.json')
        assert lineage['mappings'][0]['terminal_evidence']=='terminal_length'
        assert old_state.read_bytes()==original and state['model_calls']==1


def test_true_unknown_cannot_be_superseded_by_a_new_profile_run(tmp_path):
    import pytest
    from rating_executor import RatingRunner
    with endpoints(mode='partial') as (url,state):
        registry,manifest=inputs(tmp_path,url);runner(tmp_path,registry,manifest,url).run()
        folder=tmp_path/'profile';folder.mkdir();new_registry,new_manifest=profile_inputs(folder,url,state)
        with pytest.raises(ValueError,match='终止|未知'):
            RatingRunner(new_registry,[new_manifest],folder/'run',api_url=url,supersedes_run_root=tmp_path/'run')
        with pytest.raises(ValueError,match='终止|未知'):
            RatingRunner(new_registry,[new_manifest],folder/'run',api_url=url)
        assert state['model_calls']==1


def test_local_prepare_and_publish_reuse_original_complete_response_without_rebuilding_prompt(tmp_path,monkeypatch):
    import rating_executor
    def missing_total(r):r.pop('score');r['dimension_scores'][1]['score']=99
    with endpoints(rating_mutator=missing_total) as (url,state):
        registry,manifest=inputs(tmp_path,url);instance=runner(tmp_path,registry,manifest,url)
        with monkeypatch.context() as context:
            context.setattr(rating_executor,'normalize_result',lambda raw,category:(json.loads(raw),{}))
            assert instance.run()['counts']['needs_review']==1
        raw=next((tmp_path/'run'/'requests').glob('*/response.txt'));original=raw.read_bytes()
        monkeypatch.setattr(instance,'_body',lambda item:(_ for _ in ()).throw(AssertionError('must not rebuild a future prompt')))
        prepared=instance.reconcile(publish=False)
        assert prepared['prepared']==1 and state['write_calls']==0 and state['model_calls']==1
        written=instance.reconcile(publish=True)
        assert written['published']==1 and state['write_calls']==1 and state['model_calls']==1
        assert json.loads((tmp_path/'run'/'status.json').read_text())['counts']=={'written':1}
        assert raw.read_bytes()==original
        instance.reconcile(publish=True)
        assert state['write_calls']==1 and state['model_calls']==1


def test_local_reconcile_rejects_tampered_saved_response(tmp_path):
    with endpoints(rating_mutator=lambda r:r.pop('brief_reason')) as (url,state):
        registry,manifest=inputs(tmp_path,url);instance=runner(tmp_path,registry,manifest,url);instance.run()
        path=next((tmp_path/'run'/'requests').glob('*/response.txt'));path.write_bytes(path.read_bytes()+b' ')
        result=instance.reconcile(publish=False)
        assert result['prepared']==0 and result['needs_review']==1
        assert state['model_calls']==1 and state['write_calls']==0


def test_gate_close_after_reservation_is_resumed_only_by_original_owner(tmp_path,monkeypatch):
    def refs(r):r['evidence_refs']=[{'field_path':'/profile_evidence/profile/path','note':'实际路线'}]
    with endpoints(rating_mutator=refs) as (url,state):
        registry,manifest=profile_inputs(tmp_path,url,state);instance=runner(tmp_path,registry,manifest,url)
        original=instance._api;closed=[]
        def close_after_claim(path,body=None):
            result=original(path,body)
            if path.endswith('/rating-claim') and not closed:
                closed.append(True);value=json.loads(registry.read_text());value['dispatch_gate']['allow_new_scoring_requests']=False;save(registry,value)
            return result
        monkeypatch.setattr(instance,'_api',close_after_claim)
        first=instance.run();assert first['counts']=={'prepared':1} and state['model_calls']==0
        value=json.loads(registry.read_text());value['dispatch_gate']['allow_new_scoring_requests']=True;save(registry,value)
        again=instance.run();assert again['counts']=={'written':1} and state['model_calls']==1 and len(state['claims'])==1


