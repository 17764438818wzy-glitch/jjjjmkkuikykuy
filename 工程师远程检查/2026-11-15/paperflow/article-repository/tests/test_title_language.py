import copy
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest

sys.path.insert(0,r'F:\论文预处理系统\预处理系统2\qdrant-migration')
from article_repository.imports import new_record,Importer
from paperflow_qdrant.library import stable_id

def inputs(title='长虹美菱股份有限公司成本管理问题研究'):
    sha='a'*64
    reg={'task_id':'task','group_id':'group','paper_id':'paper','receipt_id':'receipt',
         'status':'passed','registration_status':'registered','delivery_complete':True,
         'paper_title':title,'title_status':'已提取','display_hash':sha,
         'paper':{'person_dir':r'E:\论文\财务管理-测试','body':{'file_id':'original','sha256':sha},
                  'archive_files':[{'file_id':'original','relative':'original.docx','sha256':sha,'bytes':123}]}}
    run={'id':'run','created':'2026-11-12T00:00:00+00:00','parameters':{'registered':reg}}
    raw=stable_id('paperflow-unstructured-retention','body')
    profile={'article':{'title':'Study on Cost Management of Changhong Meiling Company Limited'},
             'format_status':'unstructured_retained','retained_profiles':[{'profile':{'article':{'summary':'仅分段内容'}}}]}
    body={'name':'PF_internal___unstructured_retention___body.md','dataset_ids':[raw],
          'meta_fields':{'paperflow_format_status':'unstructured_retained','two_pass_profile_json':json.dumps(profile)},
          'dataset_meta_fields':{raw:{'paperflow_source_sha256':sha,'paperflow_manifest_sha256':'b'*64}}}
    return run,body

@pytest.mark.parametrize('title',['长虹美菱股份有限公司成本管理问题研究','A Study of Metaphors in English Poetry'])
def test_confirmed_original_title_preserves_its_language_and_raw_profile(title):
    run,body=inputs(title)
    original=json.loads(body['meta_fields']['two_pass_profile_json'])
    record=new_record(run,body,[],[])
    assert record['title']==title
    assert record['profile']==original

@pytest.mark.parametrize('change',['candidate','missing','sha_mismatch','display_mismatch','missing_display_hash'])
def test_unconfirmed_or_mismatched_title_never_promotes_translation_or_internal_name(change):
    run,body=inputs();reg=run['parameters']['registered']
    if change=='candidate':reg.update(title_status='待核对',paper_title_candidate=reg.pop('paper_title'))
    if change=='missing':reg.pop('paper_title')
    if change=='sha_mismatch':next(iter(body['dataset_meta_fields'].values()))['paperflow_source_sha256']='c'*64
    if change=='display_mismatch':reg['display_hash']='c'*64
    if change=='missing_display_hash':reg.pop('display_hash')
    assert new_record(run,body,[],[])['title'].startswith('题名待核实（')

def test_restart_uses_catalog_source_folders_without_registering_or_synchronizing():
    class Catalog:
        def imported_folders(self):return {'old-folder'}
        def register(self,*args):pytest.fail('previously imported source was registered again')
        def synchronize(self,*args):pytest.fail('previously imported source was synchronized again')
        def majors(self):pytest.fail('previously imported source reached processing')
    class Sync:
        def catalog_manifests(self):
            return [SimpleNamespace(meta={'folder_id':'old-folder','document':{'meta_fields':{}}})]
    Importer(Catalog(),Sync(),object(),'http://unused.invalid').refresh()

def test_catalog_read_failure_stops_import_instead_of_replaying_old_sources():
    class Catalog:
        def imported_folders(self):raise OSError('ledger unavailable')
    class Sync:
        def catalog_manifests(self):pytest.fail('import continued without the durable ledger')
    with pytest.raises(OSError,match='ledger unavailable'):
        Importer(Catalog(),Sync(),object(),'http://unused.invalid').refresh()
