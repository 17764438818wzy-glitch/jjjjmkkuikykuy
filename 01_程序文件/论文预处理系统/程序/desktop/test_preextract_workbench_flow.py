"""Existing batch runner through real material, MD validation and publication.

Only the external model transport and embedding counter are replaced. No network.
"""
import io
import json
from pathlib import Path
import threading

import pytest

from test_preextract_pipeline import _candidate
from test_preextract_publish import Counter, PAPERFLOW


@pytest.fixture
def flow(tmp_path, monkeypatch):
    from docx import Document
    from engine import Store
    from auto_pipeline import AutoProcessor
    import preextract_pipeline as pipeline
    app=tmp_path/'app';store=Store(app/'data')
    monkeypatch.setattr(pipeline,'ROOT',app)
    monkeypatch.setattr(pipeline,'DATA_DIR',app/'data')
    runtime=tmp_path/'runtime';runtime.mkdir()
    (runtime/'config.json').write_text('{}',encoding='utf8')
    catalog={'schema':'specialty-routing/1.0','majors':[
        dict(code='080901',name='计算机科学与技术',discipline_code='08',discipline='工学',
             group_code='0809',group='计算机类',paper_dataset_id='test-paper',image_dataset_id='test-images')]}
    (runtime/'catalog.json').write_text(json.dumps(catalog,ensure_ascii=False),encoding='utf8')
    template=tmp_path/'template.md';template.write_bytes(pipeline.DEFAULT_TEMPLATE.read_bytes())
    class TestCounter(Counter):
        def close(self):pass
    monkeypatch.setattr(pipeline,'offline_counter',lambda *args:TestCounter())
    def forbidden(*args,**kw):raise AssertionError('must use existing selected connection without network')
    monkeypatch.setattr(pipeline,'load_model_connection',forbidden)
    monkeypatch.setattr('urllib.request.OpenerDirector.open',forbidden)
    config=dict(extraction_mode='paper-preextract/1.0',auto_upload=False,
        base_url='http://127.0.0.1:9/v1',text_model='selected-test-model',timeout=57,max_tokens=18000,
        preextract_limits=dict(context_window=300000,max_output_tokens=32000,supports_images=True),
        preextract_runtime_dir=str(runtime),preextract_template=str(template),preextract_paperflow_root=str(PAPERFLOW),
        concurrency=dict(enabled=True,papers=1,global_llm=1),resume={'enabled':False})
    processor=AutoProcessor(store,config,'local-test-key')
    class Reply(io.BytesIO):
        headers={'Content-Type':'application/json'}
    class Opener:
        calls=[]
        lock=threading.Lock()
        def open(self,request,timeout):
            body=json.loads(request.data)
            assert body['model']=='selected-test-model' and timeout==57
            with self.lock:self.calls.append(body)
            bundle=json.loads(body['messages'][1]['content'][0]['text'])['material']
            candidate=_candidate({'template':template},bundle)
            if hasattr(self, 'transform'):
                self.transform(candidate)
            raw=json.dumps(dict(choices=[dict(message=dict(content=json.dumps(candidate,ensure_ascii=False)),finish_reason='stop')],
                usage={'prompt_tokens':700,'completion_tokens':900}),ensure_ascii=False).encode()
            return Reply(raw)
    opener=Opener();opener.calls=[];processor.agent.opener=opener
    monkeypatch.setattr(processor.agent,'call',forbidden)
    sources=[]
    for i in range(2):
        source=tmp_path/f'paper-{i}.docx';document=Document()
        document.add_heading('1 排程研究',1)
        document.add_paragraph('本研究比较两种排程方案，目标是减少作业等待时间。')
        document.add_paragraph('材料为三个作业的到达时刻和持续时间。先按两种规则排序，再记录各作业等待时间，比较平均等待时间。')
        document.add_paragraph(f'比较得到两组等待时间。本例{i}仅说明比较过程，新问题需要重新采集作业数据。')
        document.save(source);sources.append(dict(path=str(source),name=source.name))
    return processor,opener,sources


def test_original_runner_real_one_call_publication_and_checkpoint(flow):
    from preextract_workbench import validate_prepared_material
    from processing_outcomes import execution_outcome,ai_outcome
    processor,opener,sources=flow
    manifest=processor.run(sources[:1]);item=manifest['items'][0]
    assert item['status']=='pass',item.get('reason')
    assert manifest['pipeline']=='automatic' and manifest['run_id'].startswith('AUTO_')
    assert len(opener.calls)==1
    assert manifest['concurrency']['llm_peak']==1
    assert execution_outcome(item,manifest)[0]==ai_outcome(item,manifest['status'])[0]=='通过'
    assert len(item['ai_steps'])==1 and not item['ai_steps'][0]['cache_hit']
    assert manifest['billing']['requests']==1
    assert not manifest.get('human_view',{}).get('issues')
    assert Path(item['path']).is_file()
    assert Path(item['package_path']).is_relative_to(processor.store.runs/manifest['run_id'])
    folder,bundle=validate_prepared_material(dict(sources[0],prepared_checkpoint=item['prepared_checkpoint']),processor.store.root)
    assert bundle['source']['source_sha256']==item['source_hash']
    assert (folder/'private'/'论文预提取.md').is_file()
    assert not (folder/'delivery-receipt.json').exists()


def test_best_effort_keeps_partial_ai_label_and_program_redaction_report_in_original_results(flow):
    processor,opener,sources=flow
    def partial(candidate):
        candidate['record_kind']='partial_candidate'
        candidate['redaction_plan']=[dict(id='R1',scope='generated_field',action='replace',
            expected_text='不存在的署名',replacement='研究者甲',target_pointer='/profile/article/title',
            location_status='uncertain')]
    opener.transform=partial
    manifest=processor.run(sources[:1]);item=manifest['items'][0]
    assert item['status']=='pass',item.get('reason')
    assert item['preextract_model_record_kind']=='partial_candidate'
    assert '部分提取' in item['ai_result']['reason']
    assert item['preextract_redaction']['policy']=='best_effort'
    assert item['preextract_redaction']['warnings']
    assert '基础脱敏' in item['script_result']['reason']
    assert len(opener.calls)==1


def test_existing_parallel_workers_copy_sealed_publications_to_parent(flow):
    processor,opener,sources=flow
    processor.config['concurrency']['papers']=2
    manifest=processor.run(sources)
    assert len(manifest['items'])==2
    assert all(item['status']=='pass' for item in manifest['items']),[i.get('reason') for i in manifest['items']]
    assert len(opener.calls)==2 and manifest['concurrency']['llm_peak']==1
    assert len(manifest['child_runs'])==2
    assert not manifest.get('human_view',{}).get('issues')
    for item in manifest['items']:
        assert Path(item['path']).is_file()
        assert Path(item['package_path']).is_relative_to(processor.store.runs/manifest['run_id'])


def test_existing_ai_retry_reuses_complete_response_without_new_post(flow):
    processor,opener,sources=flow
    first=processor.run(sources[:1]);item=first['items'][0]
    assert item['status']=='pass',item.get('reason')
    # A failed outer save/review must still be able to reuse the durable model
    # result, while successful papers normally use existing source deduplication.
    item.update(status='reject',automatic_approval=False,reason_kind='technical')
    processor.store.save(first)
    record=dict(sources[0],prepared_checkpoint=item['prepared_checkpoint'])
    later=processor.run([record],operation='retry_ai');saved=later['items'][0]
    assert saved['status']=='pass',saved.get('reason')
    assert len(opener.calls)==1
    assert saved['ai_steps'][0]['cache_hit']
    assert Path(saved['preextract_work_dir'])==Path(item['preextract_work_dir'])


def test_old_uploaded_fingerprint_reuses_local_result_and_still_reaches_handoff(flow):
    processor,opener,sources=flow
    first=processor.run(sources[:1]);item=first['items'][0]
    assert item['status']=='pass',item.get('reason')
    record=dict(sources[0],upload_mark='已上传',source_hash=item['source_hash'])
    later=processor.run([record]);saved=later['items'][0]
    assert saved['status']=='pass',saved.get('reason')
    assert len(opener.calls)==1 and saved['ai_steps'][0]['cache_hit']
    assert not any(event.get('status') in {'duplicate','uploaded_skip'} for event in later['events'])
