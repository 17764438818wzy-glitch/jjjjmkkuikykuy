import copy
import json
from pathlib import Path

import pytest

from article_repository.ratings import profile_sha256,profile_evidence,payload_digest,validate_rating,canonical
from test_rating_executor import endpoints,inputs,profile_inputs,runner,save


def refs(r):r['evidence_refs']=[{'field_path':'/profile_evidence/profile/path','note':'实际路线'}]


def setup_watch(tmp_path,url,state,*,category='engineering_system'):
    from rating_watch import RatingWatch
    registry,manifest=profile_inputs(tmp_path,url,state)
    instance=runner(tmp_path,registry,manifest,url)
    # Keep a completed old cohort article so discovery adds a separate article.
    instance.run()
    classifier=tmp_path/'classification-channel.json';config=json.loads(registry.read_text());config['channels'][0]['research_type']='classification'
    config['channels'][0]['max_concurrency']=1;config['max_total_scoring_concurrency']=1;save(classifier,config)
    state['profile_category']=category
    if category is None:state['profile_snapshot']['profile']['research_profile'].pop('research_type',None)
    version=state.setdefault('profile_revision',1)
    events=[{'sequence':1,'article_id':'new-article','revision':version,'research_type':category,'updated_at':'2026-11-12T12:00:00+00:00'}]
    operations={};claims={};begun=set();original=instance._api
    def api(path,body=None):
        from urllib.parse import parse_qs,urlparse
        parsed=urlparse(path);query=parse_qs(parsed.query)
        if parsed.path.endswith('/rating-claim/terminal'):return {'status':'accepted'}
        if parsed.path.endswith('/rating-claim/begin'):
            granted=body['operation_id'] not in begun;begun.add(body['operation_id'])
            return {'dispatch_granted':granted,**{k:body[k] for k in ('request_id','operation_id','run_id')}}
        if parsed.path.endswith('/rating-claim'):
            identity=(parsed.path.split('/')[-2],body['stage'],body['revision'],body['profile_sha256'],body['rubric_sha256'])
            if identity in claims:return {'granted':claims[identity]['operation_id']==body['operation_id'] and body['operation_id'] not in begun,'reason':'version_already_claimed',**claims[identity]}
            receipt={k:body[k] for k in ('request_id','operation_id','run_id')};claims[identity]=receipt
            return {'granted':True,**receipt}
        if path.startswith('/api/library/rating-discovery/bootstrap'):
            return {'rows':events.copy(),'next_cursor':'','event_checkpoint':0,'window_until':'2026-11-12T13:00:00+00:00'}
        if path.startswith('/api/library/rating-discovery?'):
            after=int(query['after'][0]);rows=[e for e in events if e['sequence']>after]
            return {'rows':rows,'next_after':max((e['sequence'] for e in rows),default=after),'has_more':False}
        if parsed.path.endswith('/research-type'):
            if body is None:
                from urllib.error import HTTPError
                if query['operation_id'][0] not in operations:raise HTTPError(path,404,'not found',{},None)
                return operations[query['operation_id'][0]]
            assert body['revision']==state['profile_revision'] and body['profile_sha256']==profile_sha256(state['profile_snapshot'])
            assert state['profile_category'] is None
            state['profile_category']=body['research_type'];state['profile_revision']+=1
            state['profile_snapshot']['profile']['research_profile']['research_type']=body['research_type']
            import hashlib
            receipt={**body,'article_id':'new-article','revision':state['profile_revision'],
                'payload_digest':hashlib.sha256(canonical({'article_id':'new-article',**body}).encode()).hexdigest(),'status':'accepted'}
            operations[body['operation_id']]=receipt;return receipt
        return original(path,body)
    instance._api=api
    watch=RatingWatch(instance,classifier,cache_root=tmp_path/'profiles',bootstrap_since='2026-11-12T00:00:00+08:00')
    return instance,watch,events


def test_new_classified_article_scores_once_without_classification_call(tmp_path):
    with endpoints(rating_mutator=refs) as (url,state):
        instance,watch,events=setup_watch(tmp_path,url,state)
        result=watch.pass_once(new_wave=1,history_wave=0)
        assert result['status']['counts']['written']==2
        assert state.get('classification_calls',0)==0 and state['model_calls']==2
        events.append({**events[0],'sequence':2})
        watch.pass_once(new_wave=1,history_wave=0)
        assert state['model_calls']==2 and state['write_calls']==2


def test_missing_category_classifies_cas_refreezes_and_scores_once(tmp_path):
    with endpoints(rating_mutator=refs) as (url,state):
        instance,watch,events=setup_watch(tmp_path,url,state,category=None)
        result=watch.pass_once(new_wave=1,history_wave=0)
        assert result['status']['counts']['written']==2
        assert state['classification_calls']==1 and state['model_calls']==3 and state['write_calls']==2
        assert state['profile_revision']==2
        watch.pass_once(new_wave=1,history_wave=0)
        assert state['model_calls']==3


def test_null_or_insufficient_category_input_parks_without_repeated_calls(tmp_path):
    with endpoints(rating_mutator=refs) as (url,state):
        instance,watch,events=setup_watch(tmp_path,url,state,category=None)
        state['classifier_result']=None
        watch.pass_once(new_wave=1,history_wave=0);watch.pass_once(new_wave=1,history_wave=0)
        assert state['model_calls']==2 and state['write_calls']==1


def test_semantic_version_ignores_local_paths_and_keeps_prior_attempt_history(tmp_path):
    from rating_queue import RatingQueue
    with endpoints(rating_mutator=refs) as (url,state):
        registry,manifest=profile_inputs(tmp_path,url,state);instance=runner(tmp_path,registry,manifest,url);instance.run()
        queue=RatingQueue(instance);queue.initialize()
        with instance.connect() as conn:old=conn.execute('SELECT * FROM jobs').fetchone()
        item=json.loads(old[3]);same={**item,'profile_snapshot_path':'a-different-local-path','preparation_note':'different note'}
        assert queue.enqueue(same)=='existing_version'
        changed={**item,'revision':2,'profile_sha256':'b'*64}
        assert queue.enqueue(changed)=='enqueued'
        with instance.connect() as conn:
            assert conn.execute('SELECT request_id FROM jobs').fetchone()[0]!=old[4]
            assert json.loads(conn.execute('SELECT row_snapshot FROM job_history').fetchone()[0])==list(old)
        assert (instance.root/'requests'/old[4]/'response.txt').exists()


@pytest.mark.parametrize('state_name',['uncertain','inflight','deferred_not_dispatched','write_unknown'])
def test_prior_unknown_or_parked_attempt_blocks_version_promotion(tmp_path,state_name):
    from rating_queue import RatingQueue
    with endpoints() as (url,state):
        registry,manifest=profile_inputs(tmp_path,url,state);instance=runner(tmp_path,registry,manifest,url)
        queue=RatingQueue(instance);queue.initialize();instance._set('article-0',state_name)
        with instance.connect() as conn:row=conn.execute('SELECT * FROM jobs').fetchone()
        item={**json.loads(row[3]),'revision':2,'profile_sha256':'b'*64}
        assert queue.enqueue(item)=='blocked_prior_attempt'
        with instance.connect() as conn:assert conn.execute('SELECT request_id,status FROM jobs').fetchone()==(row[4],state_name)


def test_legacy_unfrozen_item_is_preserved_without_inventing_a_revision(tmp_path):
    from rating_queue import RatingQueue
    with endpoints() as (url,state):
        registry,manifest=profile_inputs(tmp_path,url,state);doc=json.loads(manifest.read_text())
        original=copy.deepcopy(doc['items'][0]);missing=doc['items'][0]
        for key in ('revision','profile_snapshot_path','profile_sha256'):missing.pop(key)
        missing['preparation_status']='needs_input';save(manifest,doc)
        instance=runner(tmp_path,registry,manifest,url);queue=RatingQueue(instance);queue.initialize()
        with instance.connect() as conn:old=conn.execute('SELECT * FROM jobs').fetchone()
        current={**json.loads(old[3]),**original}
        assert queue.enqueue(current)=='enqueued'
        with instance.connect() as conn:
            saved=json.loads(conn.execute('SELECT row_snapshot FROM job_history').fetchone()[0])
            assert saved==list(old) and 'revision' not in json.loads(saved[3])


def test_classification_without_original_native_receipt_stays_unknown(tmp_path):
    with endpoints(rating_mutator=refs) as (url,state):
        instance,watch,events=setup_watch(tmp_path,url,state,category=None)
        watch.pass_once(dispatch=False,new_wave=1,history_wave=0)
        with instance.connect() as conn:conn.execute("UPDATE classification_jobs SET status='inflight'")
        result=watch.pass_once(new_wave=1,history_wave=0)
        assert result['classification_counts']=={'uncertain':1}
        assert result['status']['dispatch_halt'] and state['model_calls']==1


def test_transient_profile_read_is_retried_without_repeating_a_model_call(tmp_path):
    from urllib.error import HTTPError
    with endpoints(rating_mutator=refs) as (url,state):
        instance,watch,events=setup_watch(tmp_path,url,state);original=instance._api;failed=[]
        def flaky(path,body=None):
            if path.endswith('/new-article/rating-input') and not failed:
                failed.append(True);raise HTTPError(path,503,'temporary',{},None)
            return original(path,body)
        instance._api=flaky
        first=watch.pass_once(new_wave=1,history_wave=0)
        assert first['discovery_counts']=={'read_retry':1} and state['model_calls']==1
        with instance.connect() as conn:conn.execute('UPDATE discovery_queue SET next_check=0')
        result=watch.pass_once(new_wave=1,history_wave=0)
        assert result['status']['counts']['written']==2 and state['model_calls']==2


def test_acceptance_completed_after_initialization_survives_version_archive(tmp_path):
    from rating_queue import RatingQueue
    with endpoints(rating_mutator=refs) as (url,state):
        registry,manifest=profile_inputs(tmp_path,url,state);instance=runner(tmp_path,registry,manifest,url)
        queue=RatingQueue(instance);queue.initialize();instance.run()
        with instance.connect() as conn:row=conn.execute('SELECT * FROM jobs').fetchone()
        item={**json.loads(row[3]),'revision':2,'profile_sha256':'b'*64};assert queue.enqueue(item)=='enqueued'
        with instance.connect() as conn:
            assert conn.execute('SELECT request_id FROM acceptance_completed').fetchone()[0]==row[4]
