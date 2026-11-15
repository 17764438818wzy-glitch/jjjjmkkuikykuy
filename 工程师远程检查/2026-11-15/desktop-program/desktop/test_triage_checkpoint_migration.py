"""A saved pre-fix native snapshot must not resurrect omitted body text."""
import json
from pathlib import Path

from docx import Document

from ai_resume import save_prepared, restore_prepared
from engine import digest
from evidence import build_package


def test_legacy_preparation_is_rebuilt_from_immutable_native_source(tmp_path):
    work = tmp_path/'runs/old/private/item'
    work.mkdir(parents=True)
    source = work/'source.docx'
    doc = Document()
    doc.add_paragraph('研究标题与开头正文。')
    doc.add_paragraph('完整的研究方法与结果必须保留。')
    doc.add_paragraph('绪论部分，介绍了本文选题背景。')
    doc.add_paragraph('参考文献')
    doc.save(source)
    package = work/'package'
    identity = {'id':'paper', 'title':'研究标题与开头正文', 'major':'教育学', 'category':'教育教学类'}
    meta = build_package(source, package, identity, redact_content=False)
    # The old snapshot had already lost the actual method/results blocks.
    meta['blocks'] = meta['blocks'][2:]
    preparation = {'raw':'研究标题与开头正文。\n完整的研究方法与结果必须保留。\n绪论部分，介绍了本文选题背景。\n参考文献',
                   'reliable':True,'screen':{'decision':'pass'}, 'hint_title':identity['title'],
                   'hint_confidence':'high','meta':meta,'identity':identity}
    item = {'source_hash':digest(source)}
    cp = save_prepared(work,source,package,item,preparation,'test-version')
    path = Path(cp['path'])
    saved = json.loads(path.read_text(encoding='utf8'))
    saved.pop('native_rules_version',None)
    path.write_text(json.dumps(saved,ensure_ascii=False),encoding='utf8')
    cp['sha256']=digest(path)
    record={'path':str(source),'prepared_checkpoint':cp}
    destination=tmp_path/'new-work';destination.mkdir()

    _, _, restored = restore_prepared(record,tmp_path,'test-version',destination)

    assert '完整的研究方法与结果必须保留。' in '\n'.join(b.get('text','') for b in restored['meta']['blocks'])
    assert restored['_native_rebuilt'] is True
    assert json.loads(path.read_text(encoding='utf8'))['preparation']['meta']==meta


def test_current_preparation_does_not_reparse_native_input(tmp_path,monkeypatch):
    from test_ai_resume import rejected_run,records
    from ai_resume import restore_prepared
    store,_,manifest=rejected_run(tmp_path)
    work=tmp_path/'new-work';work.mkdir()
    import evidence
    def forbidden(*args,**kwargs):raise AssertionError('valid native checkpoint re-parsed')
    monkeypatch.setattr(evidence,'build_package',forbidden)
    _,_,prepared=restore_prepared(records(manifest)[0],store.root,manifest['items'][0]['processing_version'],work)
    assert not prepared.get('_native_rebuilt')


def test_migrated_checkpoint_obeys_fresh_screening_rejection(tmp_path,monkeypatch):
    from test_ai_resume import rejected_run,records
    from test_stage_resume import Transport
    import auto_pipeline
    store,cfg,first=rejected_run(tmp_path)
    selected=records(first)
    cp=selected[0]['prepared_checkpoint'];path=Path(cp['path'])
    saved=json.loads(path.read_text(encoding='utf8'));saved.pop('native_rules_version')
    path.write_text(json.dumps(saved,ensure_ascii=False),encoding='utf8');cp['sha256']=digest(path)
    original_screen=auto_pipeline.coarse_screen
    monkeypatch.setattr(auto_pipeline,'coarse_screen',lambda *args:
                        {'decision':'reject','reason':'已核实为查重报告'} if len(args)>2 else original_screen(*args))
    processor=auto_pipeline.AutoProcessor(store,cfg,'fixture-key');processor.agent.opener=Transport()
    result=processor.run(selected,operation='retry_ai')
    assert not processor.agent.opener.requests
    assert result['items'][0]['reason_kind']=='screening'
