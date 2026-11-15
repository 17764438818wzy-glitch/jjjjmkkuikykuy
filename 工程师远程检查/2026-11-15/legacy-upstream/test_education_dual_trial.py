import json
from ingest_queue import QueueStore
from education_dual_trial import select_samples,summarize

def test_representative_selection_uses_valid_sources_and_length_method_diversity():
    items=[{'paper_id':f'p{i}','length':100+i*100,'method':str(i%3),'status':'pass'} for i in range(30)]
    prior={f'p{i}':{'outcome':'accepted','remote_id':f'r{i}'} for i in range(25)}
    result=select_samples(items,8,prior)
    assert len(result)==8
    assert len({r['method'] for r in result})==3
    assert max(r['length'] for r in result)-min(r['length'] for r in result)>1500
    assert all(r['paper_id'] in prior for r in result)

def test_trial_summary_requires_all_results_and_artifacts(tmp_path):
    q=QueueStore(tmp_path/'q.sqlite3')
    incomplete=summarize(q,[{'task_id':1,'outcome':'accepted','artifacts':False,'duration':1}])
    assert not incomplete['passed']
    rows=[{'task_id':i,'outcome':v,'artifacts':True,'duration':10,'started_at':100,'finished_at':110,'channel':'school'} for i,v in enumerate(['accepted','quality_hold','quality_reject','technical_review'])]
    report=summarize(q,rows)
    assert report['valid_decisions']==3
    assert report['accepted']==1
    assert report['technical_failures']==1
    assert not report['passed']

def test_method_list_is_valid_selection_metadata():
    rows=[{'paper_id':str(i),'status':'pass','length':i,'method':['问卷法']} for i in range(8)]
    prior={str(i):{'outcome':'accepted','remote_id':str(i)} for i in range(8)}
    assert len(select_samples(rows,8,prior))==8
