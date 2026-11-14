"""Portable writing units for retrieval and later skills. No network or model calls."""
import copy
import hashlib
import json
import re
import shutil
from pathlib import Path, PurePosixPath

from knowledge_tables import table_data, csv_text, table_preview
from knowledge_relations import explicit_relations, native_data_tables
from knowledge_outline import heading_info, heading_level, heading_text

SCHEMA='paper-knowledge/1.0'
IMAGE_SUFFIXES={'.png','.jpg','.jpeg','.webp','.gif','.bmp','.tif','.tiff'}


def encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
def hash_file(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def safe_path(package,name):
    if not isinstance(name,str) or not name or '\\' in name or ':' in name:
        raise ValueError('知识包文件路径无效')
    parts=PurePosixPath(name)
    if parts.is_absolute() or '..' in parts.parts or str(parts)!=name:
        raise ValueError('知识包文件路径越界')
    path=(Path(package)/name).resolve()
    if not path.is_relative_to(Path(package).resolve()):raise ValueError('知识包文件路径越界')
    return path


def write_text(package,name,value):
    path=safe_path(package,name);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(value,encoding='utf-8',newline='');temp.replace(path)


def write_json(package,name,value):
    write_text(package,name,json.dumps(value,ensure_ascii=False,indent=2)+'\n')


def source_position(value):
    keys={'page','bbox','body_index','row','column','object_ref','after_id','container_path','block_index',
          'line','part','id'}
    return {k:copy.deepcopy(v) for k,v in value.items() if k in keys}


def identifier(paper,kind,value):
    return kind+'_'+hashlib.sha256(encoded([paper,value])).hexdigest()[:20]


def paper_identity(meta):
    source_hash=meta.get('original_source_sha256') or meta.get('source_sha256')
    if not isinstance(source_hash,str) or not re.fullmatch(r'[0-9a-fA-F]{64}',source_hash):
        source_hash=hashlib.sha256(encoded([meta.get('id'),meta.get('title')])).hexdigest()
    return source_hash,'paper_'+source_hash[:24].lower()


def outline(meta,paper):
    sections=[];blocks=[];stack=[];front=None
    for order,original in enumerate(meta['blocks']):
        pos=source_position(original.get('position',{}));source_id=original.get('id',f'block_{order}')
        bid=identifier(paper,'block',[source_id,pos]);level,number=heading_info(original)
        if original['type']=='paragraph' and type(level) is int and level>0:
            while stack and stack[-1]['level']>=level:stack.pop()
            if number:
                # Missing parents stay missing; never attach 2.1 to chapter 1 or Abstract.
                while stack:
                    parent_number=stack[-1].get('number')
                    front_matter=re.sub(r'\s+','',stack[-1]['title']).lower() in {'摘要','abstract','参考文献','references','bibliography','结论','结语'}
                    if not front_matter and (parent_number is None or tuple(parent_number)==number[:len(parent_number)]):break
                    stack.pop()
            section={'id':identifier(paper,'section',[source_id,pos]),'title':heading_text(original.get('text','')),
                     'number':list(number) if number else None,
                     'level':level,'parent_id':stack[-1]['id'] if stack else None,'children':[],'block_ids':[]}
            if stack:stack[-1]['children'].append(section['id'])
            sections.append(section);stack.append(section)
        if stack:section=stack[-1]
        else:
            if front is None:
                front={'id':identifier(paper,'section','front'),'title':'正文前部','level':0,'parent_id':None,'children':[],'block_ids':[]}
                sections.append(front)
            section=front
        block={'id':bid,'source_block_id':source_id,'type':original['type'],'order':order,'position':pos,
               'section_id':section['id'],'heading_level':level,'text':original.get('text',''),
               'object_ids':[],'source':original.get('source','native_document')}
        if original.get('list'):block['list']=copy.deepcopy(original['list'])
        blocks.append(block);section['block_ids'].append(bid)
    return sections,blocks


def render_block(block,objects,figure_by_source,relative=''):
    """Render unchanged prose with explicit asset references; generated metadata is separate."""
    def image(ref):
        figure=figure_by_source.get(ref)
        if not figure:raise ValueError('正文引用的图片未进入知识包')
        return f"![{figure.get('caption','')}]({relative}{figure['image']['path']})"
    if block['type']=='paragraph':
        text=re.sub(r'\[\[IMAGE:([^\]]+)\]\]',lambda m:image(m[1]),block['text'])
        prefix='#'*block.get('heading_level',0)+' ' if block.get('heading_level') else ''
        if block.get('heading_level'):text=heading_text(text)
        if block.get('list') and not block.get('heading_level'):
            item=block['list'];prefix='  '*item.get('level',0)+(str(item.get('value',1))+'. ' if item.get('format')=='decimal' else '- ')
        return prefix+text
    obj=next((objects[i] for i in block['object_ids'] if i in objects),None)
    if obj is None:return ''
    if block['type']=='image':return image(block['source_block_id'])+'\n\n'+obj.get('recognized_text','')
    if block['type']=='table':
        def esc(text):return text.replace('|',r'\|').replace('\n','<br>')
        paragraphs=[obj.get('caption',''),f"表格编号：{obj['id']}；单位：{obj.get('units','')}（表头角色：{obj['dataset']['header_status']}）"]
        width=len(obj['rows'][0]);table_lines=['| '+' | '.join(f'原第{i+1}列' for i in range(width))+' |',
                                               '| '+' | '.join(['---']*width)+' |']
        table_lines += ['| '+' | '.join(esc(re.sub(r'\[\[IMAGE:([^\]]+)\]\]',lambda m:image(m[1]),v)) for v in row)+' |' for row in obj['rows']]
        paragraphs.append('\n'.join(table_lines));paragraphs+=list(obj.get('notes',[]))
        paragraphs.append(f"[结构化表格]({relative}{obj['path']}) · [结构预览]({relative}{obj['preview']['path']})")
        for visual in obj.get('original_visuals',[]):paragraphs.append(f"![原表参考]({relative}{visual['path']})")
        return '\n\n'.join(p for p in paragraphs if p)
    return ''


def export_knowledge(package,meta,*,canonical_paper_id=False):
    package=Path(package)
    # Older accepted packages recorded the same privacy decision as
    # ``privacy_status=ai_passed`` but did not persist the newer explicit
    # ``text_privacy_complete`` flag.  Preserve those checked packages when
    # upgrading them to the current chart/table handoff format.
    text_privacy_complete = meta.get('text_privacy_complete') or (
        meta.get('privacy_status') == 'ai_passed' and bool(meta.get('privacy_check')))
    if not text_privacy_complete or not meta.get('preprocessing_complete'):
        raise ValueError('知识包必须在完整预处理和文本脱敏之后生成')
    from note_integrity import assert_note_integrity
    assert_note_integrity(meta['blocks'])
    manifest_path=package/'manifest.json'
    if manifest_path.exists():manifest_path.unlink()
    resources={r['id']:r for r in meta.get('resources',[])}
    assets={};files=set()
    for rid,r in resources.items():
        name=r['local_path'];path=safe_path(package,name)
        if r.get('privacy_status')!='approved' or not name.startswith('assets/') or not path.is_file():
            raise ValueError('知识包包含未脱敏或缺失的资源')
        actual=hash_file(path)
        if r.get('sha256') and r['sha256']!=actual:raise ValueError('知识包原图在审核后发生变化')
        assets[rid]={'path':name,'sha256':actual,'source':r.get('source','original_document_asset'),
                     'privacy_status':'approved','position':source_position(r.get('position',{}))}
        files.add(name)
    source_hash,paper=paper_identity(meta)
    sections,blocks=outline(meta,paper);section_by_id={s['id']:s for s in sections}
    tables=[];figures=[];equations=[];objects={}
    def save(name,value):write_json(package,name,value);files.add(name)
    for source,block in zip(meta['blocks'],blocks):
        if source['type']=='table':
            oid=identifier(paper,'table',[block['source_block_id'],block['position']]);name=f'assets/tables/{oid}.json'
            table={'id':oid,'path':name,'paper_id':paper,'source_block_id':block['source_block_id'],
                   'section_id':block['section_id'],'block_id':block['id'],'position':block['position'],
                   'caption':source.get('caption',''),'source':source.get('source','native_document'),**table_data(source,oid)}
            table['original_visuals']=[{**assets[rid],'association_status':'verified' if source.get('screenshot_match_verified') else 'candidate'}
                                       for rid in source.get('screenshot_ids',[]) if rid in assets]
            preview=f'assets/tables/{oid}.svg';write_text(package,preview,table_preview(table));files.add(preview)
            table['preview']={'path':preview,'source':'reconstructed_cell_grid','original_appearance':False}
            csv_path=f'assets/tables/{oid}.csv';write_text(package,csv_path,csv_text(table['rows']));files.add(csv_path);table['csv_path']=csv_path
            if source.get('native_chart'):table['native_chart']=copy.deepcopy(source['native_chart'])
            tables.append(table);objects[oid]=table;block['object_ids'].append(oid)
        elif source['type']=='image':
            rid=source.get('resource_id')
            if rid not in assets:raise ValueError('知识包正文图片缺少资源')
            oid=identifier(paper,'figure',[block['source_block_id'],block['position']]);name=f'assets/figures/{oid}.json'
            figure={'id':oid,'path':name,'paper_id':paper,'source_block_id':block['source_block_id'],
                    'section_id':block['section_id'],'block_id':block['id'],'position':block['position'],
                    'caption':source.get('caption',''),'image':assets[rid],
                    'recognized_text':source.get('ocr',{}).get('text',''),
                    'text_provenance':source.get('ocr',{}).get('source','unknown'),
                    'description':copy.deepcopy(source.get('description',{})),
                    'data_status':'not_available','related_table_ids':[],
                    'chart_spec':copy.deepcopy(source.get('chart_spec') or resources[rid].get('chart_spec') or {'source':'unknown','chart_type':None})}
            figure['chart_spec']['dataset_id']=None
            if source.get('structured_table') or resources[rid].get('structured_table'):
                observed=copy.deepcopy(source.get('structured_table') or resources[rid]['structured_table'])
                width=len(observed['rows'][0]);rows=observed['rows']
                synthetic={'cells':[{'row':ri,'column':ci,'rowspan':1,'colspan':1,'text':v} for ri,row in enumerate(rows) for ci,v in enumerate(row)],
                           'grid':[[r*width+c for c in range(width)] for r in range(len(rows))],'columns':width,
                           'header_rows':observed.get('header_rows',[]),'header_verified':False,'units':observed.get('units',''),
                           'notes':observed.get('notes',[]),'source':'ai_visual_transcription'}
                tid=identifier(paper,'table',[oid,'visual_table']);table={**table_data(synthetic,tid),'id':tid,'path':f'assets/tables/{tid}.json',
                    'paper_id':paper,'source_block_id':block['source_block_id'],'block_id':block['id'],'section_id':block['section_id'],
                    'position':block['position'],'caption':source.get('caption',''),'source':'ai_visual_transcription',
                    'original_visuals':[assets[rid]],'preview':{**assets[rid],'original_appearance':True},'csv_path':f'assets/tables/{tid}.csv'}
                write_text(package,table['csv_path'],csv_text(table['rows']));files.add(table['csv_path'])
                tables.append(table);objects[tid]=table;block['object_ids'].append(tid)
                figure['related_table_ids']=[tid];figure['data_status']='visual_transcription'
                # A screenshot table supplies its cells, not an invented chart specification.
            figures.append(figure);objects[oid]=figure;block['object_ids'].insert(0,oid)
    relations=explicit_relations(blocks,tables,figures)
    for figure in figures:
        matched=native_data_tables(figure,meta['blocks'],tables)
        if matched:
            table=matched[0];figure['related_table_ids']=[table['id']];figure['data_status']='native_cache'
            figure['chart_spec']=copy.deepcopy(table['native_chart'].get('chart_spec') or {'source':'native_cache','chart_types':[]})
            figure['chart_spec'].update(dataset_id=table['dataset']['id'],dataset_path=table['path'],
                data_binding={'layout':'series_points','categories':'dataset.series[].categories','values':'dataset.series[].values',
                              'names':'dataset.series[].name','format':'dataset.series[].format_code'})
            relations.append({'type':'native_object_data','from_id':figure['id'],'to_id':table['id'],
                              'dataset_id':table['dataset']['id'],'source':'original_object_identity'})
    for n,formula in enumerate(meta.get('formulas',[])):
        pos=source_position(formula.get('position',{}))
        # A cell-level formula may be inside a table. Preserve its exact position.
        matches=[b for b in blocks if pos and all(b['position'].get(k)==v for k,v in pos.items() if k not in {'row','column'})]
        oid=identifier(paper,'equation',[pos,n]);eq={'id':oid,'path':f'assets/equations/{oid}.json','paper_id':paper,
            'position':pos,'block_ids':[b['id'] for b in matches],'section_ids':list(dict.fromkeys(b['section_id'] for b in matches)),
            'expression':formula.get('latex',''),'expression_source':'native_parser','omml':formula.get('omml'),
            'image':None,'visual_status':'not_separately_extracted'}
        equations.append(eq);objects[oid]=eq
        for block in matches:block['object_ids'].append(oid)
    figure_by_source={f['source_block_id']:f for f in figures}
    first_inline_owner={}
    # Record inline image membership at the real prose/table location.
    for block in blocks:
        source=meta['blocks'][block['order']]
        texts=[source.get('text','')]+[c['text'] for c in source.get('cells',[])]
        for text in texts:
            for ref in re.findall(r'\[\[IMAGE:([^\]]+)\]\]',text):
                if ref not in figure_by_source:raise ValueError('内嵌图片引用未解析')
                figure=figure_by_source[ref];fid=figure['id']
                for oid in [fid,*figure.get('related_table_ids',[])]:
                    if oid not in block['object_ids']:block['object_ids'].append(oid)
                first_inline_owner.setdefault(ref,block['section_id'])
                relations.append({'type':'inline_asset','from_id':block['id'],'to_id':fid,'source':'original_inline_marker'})
    for source,block in zip(meta['blocks'],blocks):
        if source.get('inline') and block['source_block_id'] in first_inline_owner:
            owner=first_inline_owner[block['source_block_id']]
            section_by_id[block['section_id']]['block_ids'].remove(block['id'])
            block['section_id']=owner
            figure_by_source[block['source_block_id']]['section_id']=owner
            for obj in tables:
                if obj['source_block_id']==block['source_block_id']:obj['section_id']=owner
    for obj in tables+figures+equations:save(obj['path'],obj)
    block_by_id={b['id']:b for b in blocks};units=[]
    for section in sections:
        content=[copy.deepcopy(block_by_id[bid]) for bid in section['block_ids']]
        linked={oid for b in content for oid in b['object_ids']}
        for relation in relations:
            if relation.get('from_id') in section['block_ids'] and relation.get('to_id'):linked.add(relation['to_id'])
        unit={'id':section['id'],'paper_id':paper,'title':section['title'],'parent_id':section['parent_id'],
              'children':section['children'],'content':content,
              'table_ids':[t['id'] for t in tables if t['id'] in linked],
              'figure_ids':[f['id'] for f in figures if f['id'] in linked],
              'equation_ids':[e['id'] for e in equations if e['id'] in linked],
              'objects':{oid:{'path':objects[oid]['path'],'type':oid.split('_',1)[0]} for oid in sorted(linked)},
              'paths_relative_to':'package_root','purpose':'reference_structure_and_method_use_new_task_data'}
        name=f"writing_units/{section['id']}.json";markdown=f"writing_units/{section['id']}.md"
        save(name,unit)
        unit_text='\n\n'.join(render_block(b,objects,figure_by_source,'../') for b in content)
        write_text(package,markdown,unit_text+'\n');files.add(markdown)
        units.append({'id':section['id'],'title':section['title'],'path':name,'markdown_path':markdown,
                      'parent_id':section['parent_id'],'children':section['children']})
    save('metadata/structure.json',{'paper_id':paper,'sections':sections})
    save('metadata/blocks.json',{'paper_id':paper,'blocks':blocks})
    save('metadata/relations.json',{'paper_id':paper,'relations':relations})
    # RAGFlow indexes Markdown reliably, while the JSON sidecars are primarily
    # for a downstream chart/table renderer.  Keep a deterministic, compact
    # projection of those sidecars in a separate retrieval document so the
    # knowledge base can recover chart recipes and literal table values without
    # requiring the model to open local files.
    unrecovered=copy.deepcopy(meta.get('unrecovered_objects') or [])
    completeness_status='partial' if unrecovered else 'complete'
    handoff_metadata = {'schema':'paper-handoff/1.0',
                        'paper_id':paper if canonical_paper_id else meta.get('id'),
                        'document_id':meta.get('id'),'stable_paper_id':paper,'major':meta.get('major'),
                        'classification':meta.get('classification',{}),'preprocessing_complete':True,
                        'privacy_status':meta.get('privacy_status','ai_passed'),
                        'completeness_status':completeness_status,
                        'unrecovered_object_count':len(unrecovered),
                        'unrecovered_objects_ref':'manifest.json#/unrecovered_objects'}
    if 'source_education' in meta:
        from source_education import source_education_fields
        handoff_metadata.update(source_education_fields(meta['source_education']))
    handoff_block = '[PAPER_PIPELINE_METADATA_V1]\n' + json.dumps(handoff_metadata,ensure_ascii=False) + '\n[/PAPER_PIPELINE_METADATA_V1]'
    asset_lines=[
        '# 图表/表格写作参考结构',
        f'论文编号：{paper}。以下内容由已通过隐私检查的知识包生成；只可用新任务数据替换原数据，不得把原论文数值当作新结论。',
        '', handoff_block,
        '', f'证据完整性：{"带缺陷通过" if unrecovered else "完整"}；未恢复对象数：{len(unrecovered)}。',
    ]
    for figure in figures:
        asset_lines.extend([
            '', f'## 图表 {figure["id"]}',
            f'章节编号：{figure["section_id"]}',
            f'标题：{figure.get("caption", "")}',
            f'原图资产：{figure["image"]["path"]}',
            f'数据状态：{figure.get("data_status", "not_available")}',
            f'关联表格：{", ".join(figure.get("related_table_ids", [])) or "无"}',
            '```json', json.dumps({
                'figure_id':figure['id'],
                'caption':figure.get('caption',''),
                'chart_spec':figure.get('chart_spec',{}),
                'related_table_ids':figure.get('related_table_ids',[]),
                'data_status':figure.get('data_status','not_available'),
                'image':figure['image'],
            },ensure_ascii=False,indent=2),
            '```',
        ])
    for table in tables:
        asset_lines.extend([
            '', f'## 表格 {table["id"]}',
            f'章节编号：{table["section_id"]}',
            f'标题：{table.get("caption", "")}',
            f'结构文件：{table["path"]}',
            '```json', json.dumps({
                'table_id':table['id'],
                'caption':table.get('caption',''),
                'headers':table.get('fields',[]),
                'rows':table.get('rows',[]),
                'dataset':table.get('dataset',{}),
                'source':table.get('source',''),
                'original_visuals':table.get('original_visuals',[]),
            },ensure_ascii=False,indent=2),
            '```',
        ])
    write_text(package,'ragflow_assets.md','\n'.join(asset_lines)+'\n')
    files.add('ragflow_assets.md')
    image_lines=[
        '# 图片检索投影',
        f'图片投影文档编号：{meta.get("id", "")}',
        f'图片投影论文编号：{paper}',
        '',
    ]
    for figure in figures:
        image=figure.get('image') or {}
        if image.get('privacy_status') != 'approved':
            continue
        section=section_by_id.get(figure.get('section_id'),{})
        number=section.get('number')
        section_no=('.'.join(str(value) for value in number) if isinstance(number,(list,tuple))
                    else str(number or ''))
        chart_spec=figure.get('chart_spec') or {}
        chart_type=chart_spec.get('chart_type')
        if not chart_type:
            native_types=chart_spec.get('chart_types') or []
            chart_type=native_types[0] if native_types else ''
        description=(figure.get('description') or {}).get('text','')
        recognized=figure.get('recognized_text','')
        related_tables=figure.get('related_table_ids') or []
        image_file=PurePosixPath(image.get('path','')).name
        image_lines.extend([
            f'## 图片 {figure["id"]}',
            f'document_id: {meta.get("id", "")}',
            f'paper_id: {paper}',
            f'figure_id: {figure["id"]}',
            f'caption: {figure.get("caption", "")}',
            f'section_id: {figure.get("section_id", "")}',
            f'section_no: {section_no}',
            f'section_title: {section.get("title", "")}',
            f'chart_type: {chart_type}',
            'chart_spec:',
            '```json',
            json.dumps(chart_spec,ensure_ascii=False,indent=2),
            '```',
            f'description_text: {json.dumps(description,ensure_ascii=False)}',
            f'recognized_text: {json.dumps(recognized,ensure_ascii=False)}',
            f'data_status: {figure.get("data_status", "")}',
            f'related_table_ids: {json.dumps(related_tables,ensure_ascii=False)}',
            f'image_file: {image_file}',
            '',
        ])
    projection_path=package/'ragflow_images.md'
    if len(image_lines) > 4:
        write_text(package,'ragflow_images.md','\n'.join(image_lines)+'\n')
        files.add('ragflow_images.md')
    elif projection_path.exists():
        # Do not leave an old projection behind when a later idempotent export
        # finds that no image is still approved.
        projection_path.unlink()
    # Keep the primary retrieval document self-contained as well.  The
    # supplemental file is uploaded separately for filtering/asset workflows,
    # but a normal RAGFlow text hit must still expose the recipe inline.
    retrieval=(package/'ingest.md').read_text(encoding='utf-8').rstrip()
    # The accounting Pipeline only reads handoff metadata from the beginning
    # of the primary retrieval document.  Older packages had no such block;
    # insert it immediately after the Markdown title when upgrading them.
    if '[PAPER_PIPELINE_METADATA_V1]' not in retrieval[:1200]:
        title_line, separator, remainder = retrieval.partition('\n')
        retrieval = title_line.rstrip() + '\n\n' + handoff_block + '\n\n' + remainder.lstrip('\n')
    # The standalone asset document needs its own handoff. Inline recipes share
    # the primary document's opening handoff; a second one is ambiguous to RAG.
    inline_assets='\n'.join(line for line in asset_lines if line != handoff_block)
    write_text(package,'ingest.md',retrieval+'\n\n'+inline_assets+'\n')
    # The text renderer adds short identifiers beside their content. Full block
    # inventories and paths remain in JSON, not a second catalogue for embedding.
    reading_links='\n\n---\n\n## 写作参考材料（程序生成导航）\n\n[完整知识包清单](manifest.json)\n\n'
    reading_links+='\n\n'.join(f"[{u['title']}]({u['markdown_path']}) · [结构数据]({u['path']})" for u in units)
    write_text(package,'document.md',(package/'document.md').read_text(encoding='utf-8').rstrip()+reading_links+'\n')
    meta['knowledge_package']={'schema_version':SCHEMA,'manifest_path':'manifest.json','paper_id':paper,
                               'writing_unit_count':len(units),'table_count':len(tables),'figure_count':len(figures),
                               'completeness_status':completeness_status,'unrecovered_objects':unrecovered}
    content_names=['document.md','ingest.md','ragflow_assets.md']
    if (package/'ragflow_images.md').is_file():
        content_names.append('ragflow_images.md')
    meta['content_hashes']={name:hash_file(package/name) for name in content_names}
    write_json(package,'metadata.json',meta);files.update(['document.md','ingest.md','metadata.json'])
    entrypoints={'retrieval_text':'ingest.md','reading_text':'document.md',
                             'asset_reference_text':'ragflow_assets.md','structure':'metadata/structure.json',
                              'blocks':'metadata/blocks.json','relations':'metadata/relations.json'}
    if (package/'ragflow_images.md').is_file():
        entrypoints['image_projection_text']='ragflow_images.md'
    handoff={'text_upload':'ingest.md','asset_reference_upload':'ragflow_assets.md',
             'asset_upload':'approved_images','remote_asset_locations':True}
    if (package/'ragflow_images.md').is_file():
        handoff['image_projection_upload']='ragflow_images.md'
    manifest={'schema_version':SCHEMA,'paper_id':paper,'document_id':meta.get('id'),'source_sha256':source_hash,
               'title':meta.get('title',''),'major':meta.get('major',''),'paths_relative_to':'package_root',
                'completeness_status':completeness_status,'unrecovered_objects':unrecovered,
                'entrypoints':entrypoints,
              'writing_units':units,'tables':[{k:t[k] for k in ['id','path','source_block_id','section_id']} for t in tables],
              'figures':[{k:f[k] for k in ['id','path','source_block_id','section_id']} for f in figures],
              'equations':[{k:e[k] for k in ['id','path','section_ids']} for e in equations],
               'files':[{'path':name,'sha256':hash_file(safe_path(package,name)),**({'role':(
                           'paper_primary' if name=='ingest.md' else
                           'paper_assets' if name=='ragflow_assets.md' else
                           'image_projection' if name=='ragflow_images.md' else
                           'image' if name.startswith('assets/') and Path(name).suffix.lower() in IMAGE_SUFFIXES else
                           None
                       )} if (
                           name in {'ingest.md','ragflow_assets.md','ragflow_images.md'} or
                           name.startswith('assets/') and Path(name).suffix.lower() in IMAGE_SUFFIXES
                       ) else {})} for name in sorted(files)],
                'handoff':handoff}
    manifest['revision']=hashlib.sha256(encoded(manifest)).hexdigest()
    write_json(package,'manifest.json',manifest)
    return manifest


def knowledge_files(package,require_stable_paper_id=False):
    manifest=json.loads((Path(package)/'manifest.json').read_text(encoding='utf-8'))
    if manifest.get('schema_version')!=SCHEMA:raise ValueError('知识包版本不支持')
    claimed=manifest.pop('revision',None)
    if claimed!=hashlib.sha256(encoded(manifest)).hexdigest():raise ValueError('知识包清单校验失败')
    names=[]
    for row in manifest['files']:
        name=row['path'];path=safe_path(package,name)
        if name in names or not path.is_file() or hash_file(path)!=row['sha256']:
            raise ValueError('知识包文件缺失、重复或内容变化')
        names.append(name)
    for required in ['document.md','ingest.md','metadata.json','metadata/structure.json','metadata/blocks.json','metadata/relations.json']:
        if required not in names:raise ValueError('知识包缺少必要文件')
    if require_stable_paper_id:
        opening='[PAPER_PIPELINE_METADATA_V1]'
        closing='[/PAPER_PIPELINE_METADATA_V1]'
        for name in ('ingest.md','ragflow_assets.md'):
            if name not in names:continue
            content=(Path(package)/name).read_text(encoding='utf-8')
            if opening not in content[:1200]:raise ValueError('稳定论文编号交接块缺失')
            # Source education v1 permits 32 quoted source locators. Keep a
            # finite header bound that accommodates that frozen schema.
            header=content.split(opening,1)[1][:65536]
            if closing not in header:raise ValueError('稳定论文编号交接块无效')
            try:
                handoff=json.loads(header.split(closing,1)[0])
            except (ValueError,IndexError) as exc:
                raise ValueError('稳定论文编号交接块无效') from exc
            if not isinstance(handoff,dict):
                raise ValueError('稳定论文编号交接块无效')
            if (handoff.get('stable_paper_id')!=manifest.get('paper_id') or
                    handoff.get('paper_id')!=manifest.get('paper_id') or
                    handoff.get('document_id')!=manifest.get('document_id')):
                raise ValueError('稳定论文编号与知识包清单不一致')
    return ['manifest.json']+names


def publish_knowledge(package,destination):
    """Publish only the checked inventory; private inputs never travel with it."""
    files=knowledge_files(package);destination=Path(destination)
    destination.mkdir(parents=True,exist_ok=False)
    for name in files:
        target=safe_path(destination,name);target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(safe_path(package,name),target)
    knowledge_files(destination)
