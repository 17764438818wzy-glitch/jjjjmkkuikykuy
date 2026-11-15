"""The frozen plan covers sent tiles, while intake retains original images."""
import base64,copy,json
from pathlib import Path
import pytest
from PIL import Image
from test_body_routing import fixture,save,reference
from test_body_adapter import digest,save_upstream_plan_layout


def tiled_fixture(tmp_path):
    from paperflow_v3.stages.body_adapter import frozen_plan_evidence
    from preextract_images import prepare_request_images
    evidence,_=fixture(tmp_path,parts=2)
    image=tmp_path/'upstream/original.png';Image.new('RGB',(80,150),'blue').save(image)
    bundle=json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    bundle['assets']=[dict(id='A001',kind='image',image_path=str(image),local_path=str(image),
        sha256=reference(image)['sha256'],width=80,height=150,block_ids=['B001'],section_ids=['S001'])]
    for key in ('material','reading_material'):evidence[key]=save(Path(evidence[key]['path']),bundle)
    work=tmp_path/'upstream/body-work'
    adapted,_=prepare_request_images(bundle,work,{'max_image_side':64,'max_single_image_bytes':100000})
    assert len(adapted['assets'])>1
    plan=json.loads(Path(evidence['plan']['path']).read_text(encoding='utf8'))
    plan['coverage']['asset_ids']=[a['id'] for a in adapted['assets']]
    for index,(part,row) in enumerate(zip(plan['parts'],evidence['responses'])):
        part['bundle']['assets']=copy.deepcopy(adapted['assets']) if index==0 else []
        part['asset_ids']=[a['id'] for a in part['bundle']['assets']]
        content=[{'type':'text','text':json.dumps({'material':part['bundle']},ensure_ascii=False)}]
        content.extend({'type':'image_url','image_url':{'url':'data:image/png;base64,'+
            base64.b64encode(Path(a['image_path']).read_bytes()).decode()}} for a in part['bundle']['assets'])
        part['body']['messages'][1]['content']=content
        row['request']=save(Path(row['request']['path']),part['body'])
        state=json.loads(Path(row['state']['path']).read_text(encoding='utf8'))
        state['request_sha256']=digest(part['body'],False);row['state']=save(Path(row['state']['path']),state)
    evidence['plan']=save(Path(evidence['plan']['path']),plan)
    proof=evidence['plan_identity'];proof.update(reading_sha256=evidence['reading_material']['sha256'],
        plan_sha256=digest(plan),coverage_sha256=digest(plan['coverage']))
    proof['parts']=[dict(id=p['id'],request_sha256=digest(p['body'],False),material_sha256=digest(p['bundle']),
        scope_sha256=digest(dict(ranges=p['ranges'],asset_ids=p['asset_ids']))) for p in plan['parts']]
    save_upstream_plan_layout(tmp_path,evidence)
    adapted_ref=save(work/'model/partition-material.json',adapted)
    frozen=json.loads((work/'model/partition-plan.json').read_text(encoding='utf8'))
    frozen['material_sha256']=adapted_ref['sha256'];frozen_ref=save(work/'model/partition-plan.json',frozen)
    state=json.loads((work/'model/partition-state.json').read_text(encoding='utf8'))
    state['plan_sha256']=frozen_ref['sha256'];save(work/'model/partition-state.json',state)
    evidence['plan']=frozen_plan_evidence(work,proof)
    evidence['image_report']=reference(work/'private/request-images.json')
    return evidence,image,adapted


def test_frozen_tile_material_restores_before_plan_coverage(tmp_path):
    from paperflow_v3.stages.body_adapter import _inputs
    evidence,image,adapted=tiled_fixture(tmp_path)
    before={p:p.read_bytes() for p in (tmp_path/'upstream').rglob('*') if p.is_file()}
    plan,bundle,parts,_=_inputs(evidence,evidence['identity'])
    assert len(parts)==2
    assert bundle['assets']==adapted['assets']
    assert set(plan['coverage']['asset_ids'])=={a['id'] for a in bundle['assets']}
    assert all(p.read_bytes()==value for p,value in before.items())


@pytest.mark.parametrize('changed',['original_bytes','tile_bytes','missing_tile','duplicate_tile','crop','frozen_mismatch','request_bytes'])
def test_tiled_restoration_keeps_original_and_sent_evidence_checks(tmp_path,changed):
    from paperflow_v3.stages.body_adapter import _inputs
    evidence,image,adapted=tiled_fixture(tmp_path)
    report_path=Path(evidence['image_report']['path'])
    report=json.loads(report_path.read_text(encoding='utf8'))
    tiles=report['images'][0]['tiles']
    if changed=='original_bytes':image.write_bytes(b'changed original')
    elif changed=='tile_bytes':Path(adapted['assets'][0]['image_path']).write_bytes(b'changed tile')
    elif changed=='request_bytes':
        row=evidence['responses'][0];request=json.loads(Path(row['request']['path']).read_text(encoding='utf8'))
        request['messages'][1]['content'][1]['image_url']['url']='data:image/png;base64,Y2hhbmdlZA=='
        row['request']=save(Path(row['request']['path']),request)
    else:
        if changed=='missing_tile':tiles.pop()
        elif changed=='duplicate_tile':tiles.append(copy.deepcopy(tiles[0]))
        elif changed=='crop':tiles[0]['binding']['image_adaptation']['source_crop']=[0,0,9999,9999]
        elif changed=='frozen_mismatch':tiles[0]['caption']='not the frozen caption'
        evidence['image_report']=save(report_path,report)
    with pytest.raises(ValueError):_inputs(evidence,evidence['identity'])
