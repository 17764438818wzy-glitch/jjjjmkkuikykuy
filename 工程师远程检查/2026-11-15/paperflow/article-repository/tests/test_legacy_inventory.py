import hashlib,json
import export_existing
import pytest

def test_extra_old_article_requires_matching_folder_inventory(tmp_path):
    assert hasattr(export_existing,'record_from_legacy_package'), 'legacy inventory adapter is not implemented'
    root=tmp_path/'PF'/'body'/'work'/'publication';root.mkdir(parents=True)
    profile=json.dumps({'article':{'title':'历史文章','summary':'已有画像'}}).encode()
    (root/'profile.json').write_bytes(profile)
    manifest=json.dumps({'task_id':'task','source_sha256':'source','files':[{'path':'profile.json','sha256':hashlib.sha256(profile).hexdigest()}]}).encode()
    digest=hashlib.sha256(manifest).hexdigest()
    (root/'manifest.json').write_bytes(manifest)
    (root/'READY.json').write_text(json.dumps({'task_id':'task','source_sha256':'source','manifest_sha256':digest}))
    source=r'C:\library\张三\正文\原文.docx'
    (root.parent/'material.json').write_text(json.dumps({'source':{'file':source,'source_sha256':'source'}}))
    (root.parent/'delivery-receipt.json').write_text(json.dumps({'task':{'task_id':'task','source_sha256':'source'},
        'manifest_sha256':digest,'status':'complete','stage':'verified','targets':{'130202':{'verified':True,'retrieval_verified':True}},
        'last_remote_verified_at':'2026-11-01T12:00:00+00:00'}))
    inventory=root.parents[2]/'revisions'/'r1'/'inventory.json';inventory.parent.mkdir(parents=True)
    body={'path':source,'sha256':'source','file_id':'body','relative':'正文/原文.docx','bytes':10}
    inventory.write_text(json.dumps({'schema':'prefect-paper/1','paper_id':'PF','person_dir':r'C:\library\张三','revision':'r1',
        'body':body,'archive_files':[body]}))
    item={'path':str(root),'task':'task','source_sha256':'source','manifest_sha256':digest}
    value=export_existing.record_from_legacy_package(item)
    assert value['source_key']=='folder:c:/library/张三'
    assert value['folder_name']=='张三'
    assert value['title']=='历史文章'
    assert value['migration']=='not_migrated'
    (root.parent/'material.json').write_text(json.dumps({'source':{'file':r'C:\other\原文.docx','source_sha256':'source'}}))
    with pytest.raises(ValueError): export_existing.record_from_legacy_package(item)
