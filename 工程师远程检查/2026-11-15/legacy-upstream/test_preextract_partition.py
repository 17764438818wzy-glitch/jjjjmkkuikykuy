"""Capacity planning uses real material, without external model calls."""
import copy
import json

import pytest

from preextract_request import build_request


def material(texts):
    return dict(source={'source_sha256':'source'}, blocks=[
        dict(id=f'b{i}', section_id=f's{i//2}', type='paragraph', text=text,
             position={'body_index':i}, object_ids=[]) for i,text in enumerate(texts)],
        sections=[dict(id=f's{i}', title=f'{i+1} 章节', level=1, parent_id=None,
                       order=i, block_ids=[f'b{j}' for j in range(2*i,min(2*i+2,len(texts)))])
                  for i in range((len(texts)+1)//2)], assets=[], issues=[], attachments=[])


def config(**kw):
    return dict(model='test', context_window=100000, max_tokens=2000,
                max_output_tokens=2000, **kw)


def test_request_projection_deduplicates_diagnostics_without_changing_body():
    b=material(['原文相同。','原文相同。'])
    b['issues']=[dict(code='review',message='需读取图像',position={'body_index':i},
                      page_evidence_ids=['p1','p2']) for i in range(100)]
    b['source']['reading_scope']={'extraction_issues':['需读取图像']*100}
    before=copy.deepcopy(b)
    body,capacity=build_request(b,'模板',{},config())
    m=json.loads(body['messages'][1]['content'][0]['text'])['material']
    assert len(m['issues'])==1
    assert [r['text'] for r in m['blocks']]==['原文相同。','原文相同。']
    assert [p['body_index'] for p in m['issues'][0]['positions']]==list(range(100))
    assert b==before
    assert capacity['count_basis']=='utf8_conservative_upper_bound'


def test_request_keeps_native_list_numbers_not_present_in_text():
    b=material(['采集样本']);b['blocks'][0]['list']=dict(level=1,format='decimal',template='（%1）',value=3)
    body,_=build_request(b,'模板',{},config())
    sent=json.loads(body['messages'][1]['content'][0]['text'])['material']
    assert sent['blocks'][0].get('list')==b['blocks'][0]['list']


def planner():
    import preextract_request
    assert callable(getattr(preextract_request,'plan_requests',None)), 'automatic input planner is missing'
    return preextract_request.plan_requests


def test_small_paper_still_one_request():
    b=material(['材料、研究方法。','结果与限制。'])
    plan=planner()(b,'模板',{},config())
    assert plan['mode']=='single' and len(plan['parts'])==1
    assert plan['coverage']['complete']


def test_large_paper_partitions_by_structure_and_covers_exact_text():
    b=material([f'第{i}段。'+('按实验步骤采集材料，比较结果并说明限制。'*120) for i in range(8)])
    plan=planner()(b,'模板',{},dict(config(),context_window=24000))
    assert plan['mode']=='partitioned' and 1<len(plan['parts'])<=8
    assert plan['coverage']['complete']
    owned={r['block_id']:[] for r in plan['coverage']['ranges']}
    for r in plan['coverage']['ranges']:owned[r['block_id']].append((r['start'],r['end']))
    for block in b['blocks']:
        spans=sorted(owned[block['id']]);assert spans[0][0]==0
        assert spans[-1][1]==len(block['text'])
        assert all(a[1]==z[0] for a,z in zip(spans,spans[1:]))
    assert all(not p['capacity']['violations'] for p in plan['parts'])
    for part in plan['parts']:
        section_ids={s['id'] for s in part['bundle']['sections']}
        assert all(b.get('section_id') in section_ids for b in part['bundle']['blocks'])


def test_one_huge_paragraph_splits_on_sentences_with_original_coordinates():
    text=''.join(f'第{i}步：采集材料并记录结果。' for i in range(1800))
    b=material([text])
    plan=planner()(b,'模板',{},dict(config(),context_window=22000))
    spans=sorted(plan['coverage']['ranges'],key=lambda r:r['start'])
    assert len(spans)>1
    assert ''.join(text[r['start']:r['end']] for r in spans)==text
    assert all(text[r['end']-1]=='。' for r in spans)
    for part in plan['parts']:
        rows=json.loads(part['body']['messages'][1]['content'][0]['text'])['material']['blocks']
        assert all('source_span' in r for r in rows)


def test_unsplittable_formula_stops_before_sending():
    b=material(['x='+('a+'*18000)+'b']);b['blocks'][0]['type']='formula'
    with pytest.raises(ValueError,match='不可再分'):
        planner()(b,'模板',{},dict(config(),context_window=14000))


def test_fixed_prompt_that_cannot_fit_stops_boundedly():
    with pytest.raises(ValueError,match='固定|不可再分'):
        planner()(material(['正文']), '模板'*20000,{},dict(config(),context_window=14000))


def candidate(unit='U1',text='完整的材料、步骤和解释'):
    from preextract_contract import load_template,_blank
    _,s=load_template();c=_blank(s);c.update(schema_version=s['schema_version'],record_kind='filled_candidate')
    u=_blank(s['profile']['learning_units'][0]);u.update(id=unit,title=text,question=text,interpretation=text,
        materials=[dict(id='M1',description=text)],steps=[dict(id='T1',operation=text,input_refs=['M1'],output_refs=['O1'])],
        outputs=[dict(id='O1',content=text)],source_refs=[dict(block_id='b0__span_10_20',start=1,end=3)],limitations=[text])
    c['profile']['learning_units']=[u]
    c['gaps']=[dict(id='G1',target_pointer='/profile/learning_units/0/interpretation',description='有局部限制')]
    c['redaction_plan']=[dict(id='R1',scope='generated_field',entity_key='same',expected_text='张三',replacement='研究者甲',
                              target_pointer='/profile/learning_units/0/title')]
    return c


def test_assembly_keeps_every_unit_and_maps_source_offsets_and_pointers():
    import preextract_partition as p
    assert callable(getattr(p,'assemble_parts',None)), 'lossless part assembler is missing'
    bundle=material(['0123456789abcdefghij'])
    sub=dict(bundle,blocks=[dict(bundle['blocks'][0],id='b0__span_10_20',text='abcdefghij',source_span=dict(block_id='b0',start=10,end=20))])
    a,b=candidate(text='第一段完整学习'),candidate(text='第二段完整学习')
    before=copy.deepcopy([a,b])
    result,audit=p.assemble_parts([a,b],[sub,sub],bundle)
    units=result['profile']['learning_units']
    assert len(units)==2 and [u['title'] for u in units]==['第一段完整学习','第二段完整学习']
    assert len({u['id'] for u in units})==2
    assert units[0]['steps'][0]['input_refs']==[units[0]['materials'][0]['id']]
    assert units[1]['source_refs'][0]==dict(block_id='b0',start=11,end=13)
    assert result['gaps'][1]['target_pointer']=='/profile/learning_units/1/interpretation'
    assert result['redaction_plan'][1]['target_pointer']=='/profile/learning_units/1/title'
    assert [a,b]==before and audit['learning_units_preserved']==2


def test_cross_unit_output_reference_survives_namespacing():
    from preextract_partition import assemble_parts
    c=candidate();second=copy.deepcopy(c['profile']['learning_units'][0]);second['id']='U2'
    second['materials'][0]['id']='M2';second['steps'][0]['id']='T2';second['outputs'][0]['id']='O2'
    second['steps'][0].update(input_refs=['O1'],output_refs=['O2'])
    c['profile']['learning_units'].append(second)
    b=material(['0123456789abcdefghij'])
    result,_=assemble_parts([c],[b],b)
    units=result['profile']['learning_units']
    assert units[1]['steps'][0]['input_refs']==[units[0]['outputs'][0]['id']]


def test_unread_fragment_does_not_mark_whole_original_block_unread():
    from preextract_partition import assemble_parts
    b=material(['0123456789abcdefghij'])
    a,z=candidate(),candidate()
    a['source']['reading_scope'].update(read_refs=['b0__span_0_10'])
    z['source']['reading_scope'].update(unread_refs=['b0__span_10_20'])
    subsets=[dict(b,blocks=[dict(b['blocks'][0],id=f'b0__span_{start}_{end}',text=b['blocks'][0]['text'][start:end],
                  source_span=dict(block_id='b0',start=start,end=end),reading_role='primary')]) for start,end in [(0,10),(10,20)]]
    result,_=assemble_parts([a,z],subsets,b)
    assert 'b0' not in result['source']['reading_scope']['unread_refs']
    assert any(g.get('type')=='not_read' and any(r.get('block_id')=='b0' and r.get('start')==10 and r.get('end')==20
               for r in g.get('source_refs',[])) for g in result['gaps'])


@pytest.mark.parametrize('all_unread',[False,True])
def test_assembled_reading_intervals_reach_original_learning_admission(all_unread):
    from test_preextract_contract import fixture,empty
    from preextract_contract import validate_candidate,load_template
    from preextract_partition import assemble_parts
    a,b,catalog=fixture();b['file_ref']='custom-main'
    _,skeleton=load_template();z=empty(skeleton);z['record_kind']='partial_candidate'
    text=b['blocks'][0]['text'];subsets=[]
    for start,end in [(0,10),(10,len(text))]:
        row=dict(b['blocks'][0],id=f'b1__span_{start}_{end}',text=text[start:end],
                 source_span=dict(block_id='b1',start=start,end=end),reading_role='primary')
        subsets.append(dict(b,blocks=[row]))
    def references(value):
        if isinstance(value,dict):
            if value.get('block_id')=='b1':value['block_id']=subsets[0]['blocks'][0]['id']
            for child in value.values():references(child)
        elif isinstance(value,list):
            for child in value:references(child)
    references(a)
    a['source']['reading_scope'].update(read_refs=[] if all_unread else [subsets[0]['blocks'][0]['id']],
        unread_refs=[subsets[0]['blocks'][0]['id']] if all_unread else [])
    z['source']['reading_scope'].update(unread_refs=[subsets[1]['blocks'][0]['id']])
    assembled,_=assemble_parts([a,z],subsets,b)
    v=validate_candidate(assembled,b,catalog,redaction_policy='best_effort',admission_policy='learning')
    assert v['valid'] is not all_unread,v['errors']
    if not all_unread:
        assert v['admission']['usable_unit_ids']==['P001_U1']
        assert any(g.get('type')=='not_read' and g.get('use_limit') for g in v['candidate']['gaps'])


def test_native_table_structures_travel_with_every_owned_row():
    b=material([''])
    cells=[dict(row=r,column=c,rowspan=1,colspan=1,text=('数量\n单位kg' if r==0 and c==1 else str(r*2+c)))
           for r in range(1001) for c in range(2)]
    grid=[[2*r,2*r+1] for r in range(1001)]
    text='\n'.join('\t'.join(cells[i]['text'] for i in row) for row in grid)
    b['blocks'][0].update(type='table',text=text)
    b['assets']=[dict(id='table1',kind='table',block_ids=['b0'],section_ids=['s0'],text=text,
        structure=dict(cells=cells,grid=grid,header_rows=[0],units='kg',notes=['保留脚注']))]
    plan=planner()(b,'模板',{},dict(config(),context_window=28000))
    assert len(plan['parts'])>1
    seen=set()
    for part in plan['parts']:
        tables=[a for a in part['bundle']['assets'] if a['id']=='table1']
        assert tables,'table structure is missing from a text fragment'
        s=tables[0]['structure'];assert s['units']=='kg' and s['notes']==['保留脚注']
        assert all(i is None or 0<=i<len(s['cells']) for row in s['grid'] for i in row)
        assert 0 in s['source_row_indices']
        seen.update(s['source_row_indices'])
    assert seen==set(range(1001))


def test_table_multilevel_headers_and_merged_cells_keep_source_coordinates():
    b=material(['']);cells=[];grid=[]
    def cell(row,column,text,rowspan=1,colspan=1):
        cells.append(dict(row=row,column=column,text=text,rowspan=rowspan,colspan=colspan))
        return len(cells)-1
    title=cell(0,0,'实验材料与质量',colspan=2);grid.append([title,title])
    grid.append([cell(1,0,'组别'),cell(1,1,'质量\nkg')])
    for r in range(2,802,2):
        label=cell(r,0,f'材料组{r}',rowspan=2)
        grid.append([label,cell(r,1,str(r))]);grid.append([label,cell(r+1,1,str(r+1))])
    text='\n'.join('\t'.join(cells[i]['text'] for i in row) for row in grid)
    b['blocks'][0].update(type='table',text=text)
    b['assets']=[dict(id='t',kind='table',block_ids=['b0'],section_ids=['s0'],text=text,
        structure=dict(cells=cells,grid=grid,header_rows=[0,1],units='kg',notes=['同组两次测量']))]
    plan=planner()(b,'模板',{},dict(config(),context_window=26000))
    assert len(plan['parts'])>1
    seen=set()
    for part in plan['parts']:
        s=part['bundle']['assets'][0]['structure']
        assert s['source_header_rows']==[0,1] and s['header_rows']==[0,1]
        for local_row,source_row in enumerate(s['source_row_indices']):
            seen.add(source_row)
            for col,index in enumerate(s['grid'][local_row]):
                assert s['cells'][index]==cells[grid[source_row][col]]
    assert seen==set(range(802))


def test_all_images_have_one_primary_owner_and_actual_bytes(tmp_path):
    from test_preextract_request import _image_bundle
    b=_image_bundle(tmp_path,5)
    plan=planner()(b,'模板',{},config(max_images=2))
    assert len(plan['parts'])>=3
    assert sorted(plan['coverage']['asset_ids'])==sorted(a['id'] for a in b['assets'])
    for part in plan['parts']:
        assert len([p for p in part['body']['messages'][1]['content'] if p['type']=='image_url'])==len(part['asset_ids'])


def test_oversized_table_repeats_header_and_preserves_rows():
    text='编号\t数量（kg）\n'+''.join(f'{i}\t{i*2}\n' for i in range(8000))
    b=material([text]);b['blocks'][0]['type']='table'
    plan=planner()(b,'模板',{},dict(config(),context_window=18000))
    assert len(plan['parts'])>1
    assert all(r['text'].endswith('\n') and r['table_context']['header']=='编号\t数量（kg）'
               for p in plan['parts'] for r in p['bundle']['blocks'] if r.get('reading_role')=='primary')


def test_global_synthesis_cannot_rewrite_frozen_learning():
    import preextract_partition as p
    assert callable(getattr(p,'apply_synthesis',None)), 'bounded synthesis merger is missing'
    c=candidate();before=copy.deepcopy(c['profile']['learning_units'])
    with pytest.raises(ValueError,match='学习'):
        p.apply_synthesis(c,dict(profile={'learning_units':[dict(id='U1',title='被缩短')]}))
    assert c['profile']['learning_units']==before
    result=p.apply_synthesis(c,dict(profile={'article':{'title':'全文题名'}},unit_links=[]))
    assert result['profile']['learning_units']==before


def test_global_synthesis_retains_local_migration_and_use_limits():
    from preextract_partition import apply_synthesis
    c=candidate()
    c['profile']['migration_boundary'].update(reusable_scope='参数估计步骤',
        must_recollect=['须重新采集样本'],must_not_reuse_as_new_facts=['不能照搬样本数据'],global_limits=['只检验过常温'])
    c['profile']['applicable_level']['usage_limits']=['不能推广至未测试设备']
    merged=apply_synthesis(c,dict(profile=dict(
        migration_boundary=dict(reusable_scope='比较方法',must_recollect=[],must_not_reuse_as_new_facts=[],global_limits=['另测高温']),
        applicable_level=dict(labels=['本科'],usage_limits=[]))))
    limits=merged['profile']['migration_boundary']
    assert limits['must_recollect']==['须重新采集样本']
    assert limits['must_not_reuse_as_new_facts']==['不能照搬样本数据']
    assert limits['global_limits']==['只检验过常温','另测高温']
    assert '参数估计步骤' in limits['reusable_scope']
    assert merged['profile']['applicable_level']['usage_limits']==['不能推广至未测试设备']


@pytest.mark.parametrize('damage',['response','state','journal','retry','hierarchy','binding','budget','pending-part','pending-global'])
def test_original_pipeline_runs_parts_through_injected_invoke_and_reuses_them(tmp_path,monkeypatch,damage):
    import io
    import threading
    from pathlib import Path
    from docx import Document
    import preextract_pipeline as pipeline
    from preextract_request import request_once
    from preextract_material import write_json
    from test_preextract_pipeline import _candidate
    root=tmp_path/'app';root.mkdir();runtime=tmp_path/'runtime';runtime.mkdir()
    monkeypatch.setattr(pipeline,'ROOT',root)
    source=tmp_path/'study.docx';doc=Document()
    for i in range(6):
        doc.add_heading(f'{i+1} 比较实验',1)
        doc.add_paragraph('先按两种规则排序，记录材料与结果。'*550)
    doc.save(source)
    catalog={'majors':[dict(code='080901',name='计算机科学与技术')]}
    write_json(runtime/'config.json',{});write_json(runtime/'catalog.json',catalog)
    template=pipeline.DEFAULT_TEMPLATE
    env=dict(template=template)
    conn=dict(config(),context_window=65000,base_url='http://127.0.0.1:9/v1',api_key='test-only')
    if damage=='budget':conn['max_partition_requests']=1
    calls=[];failed=[];stop=threading.Event()
    def invoke(body,connection,model_dir,**kw):
        calls.append(Path(model_dir).name)
        material=json.loads(body['messages'][1]['content'][0]['text'])['material']
        if 'synthesis_input' in material.get('reading_context',{}):
            records=material['reading_context']['synthesis_input']
            ids=[u['id'] for r in records for u in r.get('unit_index',[])]
            reply=dict(profile={'routing_candidates':[dict(code='080901',name='计算机科学与技术',reason='比较实验',unit_ids=ids,source_refs=[])]},unit_links=[])
        else:
            reply=_candidate(env,material)
            # Source program facts remain blank; the pipeline fills these.
            reply['source']={}
            if damage=='hierarchy':
                original=reply['profile']['learning_units'][0];units=[]
                for n in range(6):
                    u=copy.deepcopy(original);u['id']=original['id'] if n==0 else f'{original["id"]}_{n}'
                    u['interpretation']=f'完整学习记录{n}：'+('材料和步骤的条件须完整保留，结果不可照搬。'*240)
                    ids={r['id']:f'{r["id"]}_{n}' for key in ('materials','steps','outputs') for r in u[key]}
                    for key in ('materials','steps','outputs'):
                        for r in u[key]:
                            r['id']=ids[r['id']]
                            for refs in ('input_refs','output_refs'):
                                if refs in r:r[refs]=[ids.get(i,i) for i in r[refs]]
                    units.append(u)
                reply['profile']['learning_units']=units
        raw=json.dumps(dict(choices=[dict(message=dict(content=json.dumps(reply,ensure_ascii=False)),finish_reason='stop')],
                            usage=dict(prompt_tokens=10,completion_tokens=20))).encode()
        class Response(io.BytesIO):headers={'Content-Type':'application/json'}
        class Opener:
            def open(self,*a,**k):
                if damage=='retry' and Path(model_dir).name=='part-002' and not failed:
                    from urllib.error import URLError
                    failed.append(True);raise URLError('controlled offline failure')
                return Response(raw)
        result=request_once(body,connection,model_dir,opener=Opener(),**kw)
        if damage=='binding':stop.set()
        return result
    if damage=='budget':
        with pytest.raises(ValueError,match='总请求预算'):
            pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=conn,invoke=invoke)
        assert calls==['part-001']
        return
    if damage=='binding':
        with pytest.raises(InterruptedError):
            pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=conn,invoke=invoke,stop=stop)
        assert calls==['part-001'];stop.clear()
        changed=dict(conn,max_request_bytes=70000000)
        with pytest.raises(ValueError,match='容量配置发生变化'):
            pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=changed,invoke=invoke,stop=stop)
        assert calls==['part-001']
        return
    if damage=='retry':
        from urllib.error import URLError
        with pytest.raises(URLError):pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=conn,invoke=invoke)
        work=pipeline.default_work_dir(source,template)
        complete=(work/'model/part-001/response.txt').read_bytes()
        with pytest.raises(RuntimeError,match='不自动重发'):
            pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=conn,invoke=invoke)
        pipeline.create_revision(source,work,'retry-first',mode='rerun',part_id='part-002',
                                 reason='explicitly authorized failed part recovery',recover_failed=True)
        result=pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=conn,invoke=invoke)
        assert result['stage']=='candidate_validated'
        assert (work/'model/part-001/response.txt').read_bytes()==complete and calls.count('part-001')==1
        state=json.loads((work/'model/partition-state.json').read_text(encoding='utf8'))
        state['status']='parts_pending';write_json(work/'model/partition-state.json',state)
        (work/'model/part-002/revisions/retry-first/request-state.json').unlink()
        before=list(calls)
        with pytest.raises(ValueError,match='缺失|变化'):
            pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=conn,invoke=invoke)
        assert calls==before
        return
    handoffs=[]
    def before_postprocess():
        saved=pipeline.default_work_dir(source,template)/'model/partition-state.json'
        assert json.loads(saved.read_text(encoding='utf8'))['status']=='assembled'
        handoffs.append(list(calls))
    result=pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=conn,invoke=invoke,
                               before_postprocess=before_postprocess)
    assert result['stage']=='candidate_validated'
    assert handoffs==[calls], 'local processing starts only after every part and synthesis is saved'
    work=Path(result['work_dir']);state=json.loads((work/'model/partition-state.json').read_text(encoding='utf8'))
    assert state['status']=='assembled' and len(calls)>=3
    assert not (work/'model/request-state.json').exists()
    def no_call(*a,**k):raise AssertionError('completed parts must not be invoked again')
    again=pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=no_call,invoke=no_call)
    assert again['stage']=='candidate_validated'
    if damage=='hierarchy':
        c=json.loads((work/'model/assembled-candidate.json').read_text(encoding='utf8'))
        part_count=sum(name.startswith('part-') for name in calls)
        assert len(c['profile']['learning_units'])==part_count*6
        assert all(len(u['interpretation'])>4000 for u in c['profile']['learning_units'])
        assert sum(name.startswith('global-01-') for name in calls)>1
        assert 'global-02-001' in calls
        return
    directory=work/'model'/calls[0]
    if damage=='response':
        response=directory/'response.txt';response.write_text(response.read_text(encoding='utf8')+' ',encoding='utf8')
    elif damage=='state':(directory/'request-state.json').unlink()
    elif damage=='journal':
        state['requests']=state['requests'][1:];write_json(work/'model/partition-state.json',state)
    elif damage.startswith('pending-'):
        state['status']='synthesis_pending' if damage=='pending-global' else 'parts_pending'
        write_json(work/'model/partition-state.json',state)
        selected=next(name for name in calls if name.startswith('global-' if damage=='pending-global' else 'part-'))
        (work/'model'/selected/'request-state.json').unlink()
    with pytest.raises(ValueError,match='变化'):
        pipeline.run_single(source,runtime_dir=runtime,stage='extract',connection=no_call,invoke=no_call)
