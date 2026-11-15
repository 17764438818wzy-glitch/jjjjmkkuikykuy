import copy
import importlib
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'qdrant-migration'))

def test_folder_completion_is_bound_to_source_manifest_and_registration():
    module=importlib.import_module('article_repository.imports')
    assert hasattr(module,'completion_matches'),'exact completion binding is not implemented'
    identity={'task_id':'task','group_id':'group','paper_id':'paper','receipt_id':'receipt','task_run_id':'run','attempt_id':'run'}
    run={'id':'run','parameters':{'registered':{**identity,'paper':{'body':{'sha256':'original'}}}}}
    doc={'dataset_meta_fields':{'d':{'paperflow_source_sha256':'original','paperflow_manifest_sha256':'seal'}}}
    complete={'created':'2026-11-03T12:00:00+00:00','data':{'kind':'rag_complete','folder_completion':True,'data':{'status':'complete','identity':identity}}}
    post={'created':'2026-11-03T11:00:00+00:00','data':{'kind':'postprocessing_complete','data':{'validation_status':'passed','identity':identity,'body':{'manifest_sha256':'seal'}}}}
    assert module.completion_matches(run,doc,complete,post)
    wrong=copy.deepcopy(doc); wrong['dataset_meta_fields']['d']['paperflow_source_sha256']='different-body'
    assert not module.completion_matches(run,wrong,complete,post)
    wrong=copy.deepcopy(post); wrong['data']['data']['body']['manifest_sha256']='different-seal'
    assert not module.completion_matches(run,doc,complete,wrong)
    wrong=copy.deepcopy(post); wrong['created']='2026-11-03T13:00:00+00:00'
    assert not module.completion_matches(run,doc,complete,wrong)
    wrong=copy.deepcopy(complete); wrong['data']['data']['identity']['receipt_id']='old-registration'
    assert not module.completion_matches(run,doc,wrong,post)
