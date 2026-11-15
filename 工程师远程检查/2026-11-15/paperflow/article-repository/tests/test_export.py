import export_existing
import hashlib
import json
import pytest

def test_one_verified_asset_does_not_hide_unknown_attachments_in_same_folder():
    assert hasattr(export_existing,'inventory_complete'), 'folder inventory confirmation is not implemented'
    assert not export_existing.inventory_complete([{'status':'verified'},{'status':'partial','warnings':['附件清单尚未确认']}])
    assert not export_existing.inventory_complete([{'status':'verified'},{}])
    assert export_existing.inventory_complete([{'status':'verified'},{'status':'verified'}])


def test_saved_profile_is_bound_to_its_existing_package_bytes(tmp_path):
    assert hasattr(export_existing,'profile_from_package'), 'sealed profile reader is not implemented'
    profile={'article':{'summary':'已有画像'},'source_sha256':'source'}
    content=json.dumps(profile,ensure_ascii=False).encode()
    (tmp_path/'profile.json').write_bytes(content)
    manifest={'task_id':'task','source_sha256':'source','files':[{'path':'profile.json','sha256':hashlib.sha256(content).hexdigest()}]}
    raw=json.dumps(manifest).encode(); digest=hashlib.sha256(raw).hexdigest()
    (tmp_path/'manifest.json').write_bytes(raw)
    (tmp_path/'READY.json').write_text(json.dumps({'task_id':'task','source_sha256':'source','manifest_sha256':digest}))
    item={'task':'task','source_sha256':'source','manifest_sha256':digest,'path':str(tmp_path)}
    assert export_existing.profile_from_package(item)['article']['summary']=='已有画像'
    (tmp_path/'profile.json').write_text('{"article":{"summary":"changed"}}')
    with pytest.raises(ValueError): export_existing.profile_from_package(item)

def test_old_epoch_receipt_time_and_iso_receipt_time_are_comparable():
    assert export_existing.source_time(0)=='1970-01-01T00:00:00+00:00'
    assert export_existing.source_time('2026-11-03T00:00:00Z')=='2026-11-03T00:00:00+00:00'
    assert export_existing.source_time(None) is None
