import hashlib
import json

import pytest

from evidence import export_package
from knowledge_package import knowledge_files
from test_knowledge_package import sample


def test_pilot_copy_rebuilds_legacy_handoff_without_touching_source(tmp_path):
    from pilot_package_copy import prepare_pilot_copy

    source=tmp_path/'source';source.mkdir()
    export_package(source,sample(source))
    ingest=source/'ingest.md'
    text=ingest.read_text(encoding='utf-8')
    opening='[PAPER_PIPELINE_METADATA_V1]';closing='[/PAPER_PIPELINE_METADATA_V1]'
    before,rest=text.split(opening,1);payload,after=rest.split(closing,1)
    handoff=json.loads(payload)
    del handoff['stable_paper_id'];del handoff['document_id']
    ingest.write_text(before+opening+'\n'+json.dumps(handoff,ensure_ascii=False)+'\n'+closing+after,encoding='utf-8')
    manifest=json.loads((source/'manifest.json').read_text(encoding='utf-8'))
    next(row for row in manifest['files'] if row['path']=='ingest.md')['sha256']=hashlib.sha256(ingest.read_bytes()).hexdigest()
    manifest.pop('revision')
    manifest['revision']=hashlib.sha256(json.dumps(manifest,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    (source/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False),encoding='utf-8')
    originals={str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.is_file()}

    destination=tmp_path/'trial'
    repaired=prepare_pilot_copy(source,destination)
    assert repaired['paper_id']==manifest['paper_id']
    assert {str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.is_file()}==originals
    assert 'ingest.md' in knowledge_files(destination,require_stable_paper_id=True)
    for name in ('ingest.md', 'ragflow_assets.md'):
        text=(destination/name).read_text(encoding='utf-8')
        encoded=text.split(opening,1)[1].split(closing,1)[0]
        delivered=json.loads(encoded)
        assert delivered['paper_id']==delivered['stable_paper_id']==repaired['paper_id']
        assert delivered['document_id']==repaired['document_id']
    copied=json.loads((destination/'metadata.json').read_text(encoding='utf-8'))
    for name,digest in copied['content_hashes'].items():
        assert hashlib.sha256((destination/name).read_bytes()).hexdigest()==digest
    for name in ('ingest.md','ragflow_assets.md'):
        value=json.loads((destination/name).read_text(encoding='utf-8').split(opening,1)[1].split(closing,1)[0])
        assert value['stable_paper_id']==repaired['paper_id']
        assert value['paper_id']==repaired['paper_id']
        assert value['document_id']==repaired['document_id']
    with pytest.raises(FileExistsError):
        prepare_pilot_copy(source,destination)
