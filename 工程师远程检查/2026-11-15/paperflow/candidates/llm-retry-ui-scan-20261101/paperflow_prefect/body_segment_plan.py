"""Program-only input planning for the existing complete-MD request pipeline.

Plans own exact source intervals. Context copies never count as coverage.
No semantic material selection, new queue, or model transport lives here.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import unicodedata
from collections import Counter

from preextract_request import build_request, RequestCapacityError

VERSION='preextract-input-plan/1'
PART_RULES='''本次是程序已划定范围的局部完整提取，以下范围规则优先于整篇覆盖要求。
只负责 partition_scope 的主材料；reading_role=context 和 reading_context 是背景，不重复提取。
仍使用完整 MD 模板，局部范围内的学习内容必须完整，不能缩短材料、步骤、产出、公式、解释和限制。
本段未提供的章节或图片不能声称已经读取；章节/资产只输出本段实际提供的 ID。
分片块有 source_span 时，source_refs 引用本次块 id，start/end 使用本次 text 的局部偏移；程序会映射回原文。
跨段过程保留本段内容和明确来源，在 limitations/gaps 记录关联需求，不能推测未提供的研究结果。
本次的画像和专业是局部建议；后续统一全篇画像与关系，已经完成的详细学习内容会原样保存。
'''


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()


def _span(block,start=0,end=None):
    return dict(block_id=block['id'],start=start,end=len(block.get('text','')) if end is None else end)


def _fragment(block,span,role='primary'):
    row=copy.deepcopy(block);start,end=span['start'],span['end']
    if start or end!=len(block.get('text','')):
        row['id']=f"{block['id']}__span_{start}_{end}"
        row['position'] = {**(row.get('position') or {}), 'fragment_start': start, 'fragment_end': end}
    row.update(text=block.get('text','')[start:end],source_span=copy.deepcopy(span),reading_role=role)
    if block.get('type')=='table' and (start or end!=len(block.get('text',''))):
        text=block.get('text','')
        row['table_context']=dict(header=text.splitlines()[0] if text else '',
            first_original_row=text[:start].count('\n'),last_original_row=text[:end].count('\n'))
    return row


def _cuts(block,start,end):
    text=block.get('text','');kind=block.get('type','')
    if kind in {'formula','equation','math'}:return []
    pattern=r'\n' if kind=='table' else r'(?:[。！？；!?;]|(?<!\d)\.(?!\d))[\s]*|\n+'
    # Formula and image markers must remain whole in their source fragment.
    protected=[(m.start(),m.end()) for m in re.finditer(r'\[\[IMAGE:[^\]]+\]\]|\$\$[\s\S]*?\$\$|(?<!\$)\$[^$]+\$|\\\([\s\S]*?\\\)|\\\[[\s\S]*?\\\]',text)]
    points=[m.end() for m in re.finditer(pattern,text) if start<m.end()<end
            and not any(a<m.end()<z for a,z in protected)]
    if points or kind != 'paragraph':return points
    # A very long unpunctuated prose block can still be split without losing
    # characters. Do not split formula spans or a combining/ZWJ sequence.
    middle=(start+end)//2
    for distance in range(end-start):
        for cut in (middle-distance,middle+distance):
            if (start<cut<end and not any(a<cut<z for a,z in protected)
                    and not unicodedata.combining(text[cut])
                    and text[cut] != '\u200d' and text[cut-1] != '\u200d'):
                return [cut]
    return []


def _coverage(bundle,parts):
    spans=[r for part in parts for r in part['ranges']]
    for block in bundle.get('blocks',[]):
        rows=sorted((r for r in spans if r['block_id']==block['id']),key=lambda r:r['start'])
        cursor=0
        if not rows:raise ValueError('输入分段覆盖缺少原文块')
        for row in rows:
            if row['start']!=cursor or row['end']<cursor:raise ValueError('输入分段覆盖重叠或不连续')
            cursor=row['end']
        if cursor!=len(block.get('text','')):raise ValueError('输入分段覆盖未到原文末尾')
    assets=[a for p in parts for a in p['asset_ids']]
    if Counter(assets)!=Counter(a['id'] for a in bundle.get('assets',[])):
        raise ValueError('输入分段资产主归属缺失或重复')
    return dict(complete=True,block_count=len(bundle.get('blocks',[])),
                character_count=sum(len(b.get('text','')) for b in bundle.get('blocks',[])),
                ranges=spans,asset_ids=assets)


def _assess_capacity(capacity,config):
    """Keep an estimated context excess separate from a known limit excess."""
    capacity=copy.deepcopy(capacity)
    capacity['text_counter_source']=config.get('text_counter_source',capacity['count_basis'])
    capacity['context_window_verification']='not_verified_here'
    if 'context' in capacity['violations']:
        reserved_over=capacity['output_budget']+capacity['context_reserve_tokens']>capacity['context_window']
        text_over=(capacity['count_basis']=='model_tokenizer' and
            capacity['text_count']+capacity['output_budget']+capacity['context_reserve_tokens']>capacity['context_window'])
        if not reserved_over and not text_over:
            capacity['violations'].remove('context')
            capacity.setdefault('unconfirmed_violations',[]).append('context')
    return capacity


def plan_requests(bundle,template,catalog,config,*,request_context=None):
    """Plan locally; estimated context capacity can remain unconfirmed."""
    context=request_context or {}
    instruction=context.get('instruction','');extra=context.get('content')
    body,capacity=build_request(bundle,template,catalog,config,instruction=instruction,extra=extra,enforce=False)
    capacity=_assess_capacity(capacity,config)
    confirmed_overflow=context.get('context_overflow_confirmed') is True
    full=dict(ranges=[_span(b) for b in bundle.get('blocks',[])],asset_ids=[a['id'] for a in bundle.get('assets',[])],
              bundle=bundle,body=body,capacity=capacity)
    if not capacity['violations'] and not confirmed_overflow:
        return dict(schema=VERSION,mode='single',parts=[full],coverage=_coverage(bundle,[full]))

    # This is a target only after the full request requires partitioning. The
    # actual model window and the output reservation remain unchanged.
    available=capacity['context_window']-capacity['output_budget']-capacity['context_reserve_tokens']
    target=min(300000,max(1,available))

    blocks={b['id']:b for b in bundle.get('blocks',[])}
    if len(blocks)!=len(bundle.get('blocks',[])):raise ValueError('原文块编号重复，无法分段')
    assets={a['id']:a for a in bundle.get('assets',[])}
    if len(assets)!=len(bundle.get('assets',[])):raise ValueError('原文资产编号重复，无法分段')
    sections={s['id']:s for s in bundle.get('sections',[])}
    table_info={}
    from knowledge_tables import table_data
    for asset in assets.values():
        structure=asset.get('structure') or {}
        if asset.get('kind')!='table' or not structure.get('grid'):continue
        native=table_data(dict(structure,columns=len(structure['grid'][0])),asset['id'])
        lines=['\t'.join(row) for row in native['rows']]
        joined='\n'.join(lines)
        for bid in asset.get('block_ids',[]):
            if bid not in blocks:continue
            text=blocks[bid].get('text','')
            if text not in (joined,joined+'\n'):
                table_info[bid]=None  # A guessed row/character map is unsafe.
                continue
            cursor=0;spans=[]
            for n,line in enumerate(lines):
                end=cursor+len(line)+(1 if n<len(lines)-1 or text.endswith('\n') else 0)
                spans.append((cursor,end));cursor=end
            table_info[bid]=dict(asset_id=asset['id'],row_spans=spans,lines=lines)
    indices={bid:i for i,bid in enumerate(blocks)}
    # Native formulas share a paragraph position. Match each complete marker
    # to its original character range before dividing that paragraph.
    formula_groups = {}
    for i, formula in enumerate(bundle.get('formulas', [])):
        key = (json.dumps(formula.get('position') or {}, sort_keys=True), str(formula.get('latex') or ''))
        formula_groups.setdefault(key, []).append(i)
    formula_owners = {}
    ambiguous_formulas = set()
    formula_gaps = []
    for (encoded_position, latex), formula_ids in formula_groups.items():
        position = json.loads(encoded_position)
        if not position or not latex:continue
        marker = '$' + latex + '$'
        occurrences = [(bid, match.start(), match.end()) for bid, block in blocks.items()
                       if block.get('position') and all(position.get(k) == v for k, v in block['position'].items())
                       for match in re.finditer(re.escape(marker), block.get('text', ''))]
        if len(occurrences) == len(formula_ids):
            formula_owners.update(zip(formula_ids, occurrences))
        elif occurrences:
            ambiguous_formulas.update(formula_ids)
            formula_gaps.append({'code': 'formula_position_unresolved', 'position': position,
                                 'message': '同一段中有重复公式标记，无法确定原生公式对应哪一处；文字继续保留。'})
    owners={bid:[] for bid in blocks};orphans=[]
    for asset in assets.values():
        linked=[bid for bid in asset.get('block_ids',[]) if bid in blocks]
        if linked:owners[min(linked,key=indices.get)].append(asset['id'])
        else:orphans.append(asset['id'])
    atoms=[dict(ranges=[_span(b)],asset_ids=owners[b['id']]) for b in blocks.values()]
    atoms.extend(dict(ranges=[],asset_ids=[aid]) for aid in orphans)
    max_parts=config.get('max_input_parts')
    if max_parts is not None:
        max_parts=int(max_parts)
        if max_parts<1:raise RequestCapacityError('输入分段数量上限配置无效')
    outline=[{k:s.get(k) for k in ('id','title','parent_id','level')} for s in sections.values()]
    abstract=[b for b in blocks.values() if re.search(r'摘要|abstract',str(sections.get(b.get('section_id'),{}).get('title','')),re.I)]
    # Shared context is supplementary; every original abstract block also has
    # a primary owner, including unusually large abstracts.
    abstract=[dict(id=b['id'],text=b['text']) for b in abstract if len(b.get('text',''))<=4000]
    def root(section_id):
        seen=set()
        while section_id in sections and sections[section_id].get('parent_id') and section_id not in seen:
            seen.add(section_id);section_id=sections[section_id]['parent_id']
        return section_id

    def make(items,*,background=True):
        ranges=[r for x in items for r in x['ranges']];owned=[a for x in items for a in x['asset_ids']]
        included={r['block_id'] for r in ranges};rows=[_fragment(blocks[r['block_id']],r) for r in ranges]
        # When a single figure has more tiles than a request can carry, an
        # asset-only part still needs the nearby original marker as context.
        for aid in owned:
            asset=assets[aid]
            if not asset.get('image_path'):continue
            linked=[bid for bid in asset.get('block_ids',[]) if bid in blocks]
            if not linked:continue
            original=(asset.get('image_adaptation') or {}).get('original_asset_id',aid)
            marker='[[IMAGE:'+original+']]'
            bid=next((bid for bid in linked if marker in blocks[bid].get('text','')),linked[0])
            matching=[r for r in rows if r['source_span']['block_id']==bid]
            text=blocks[bid].get('text','');anchor=text.find(marker)
            if (anchor>=0 and any(marker in r.get('text','') for r in matching)
                    or anchor<0 and matching):continue
            start=max(0,anchor-400) if anchor>=0 else 0
            end=min(len(text),anchor+len(marker)+400) if anchor>=0 else min(len(text),800)
            rows.append(_fragment(blocks[bid],dict(block_id=bid,start=start,end=end),'context'))
        for row in rows:
            info=table_info.get(row['source_span']['block_id'])
            if info:
                asset=assets[info['asset_id']];structure=asset['structure'];span=row['source_span']
                selected=[i for i,(a,z) in enumerate(info['row_spans']) if a<span['end'] and z>span['start']]
                row['table_context']=dict(original_rows=selected,
                    header='\n'.join(info['lines'][i] for i in structure.get('header_rows',[])),
                    units=structure.get('units'),notes=structure.get('notes'))
        local_sections={blocks[r['source_span']['block_id']].get('section_id') for r in rows}
        for sid in list(local_sections):
            seen=set()
            while sid in sections and sid not in seen:
                seen.add(sid);local_sections.add(sid);sid=sections[sid].get('parent_id')
        if background:
            neighbors=set()
            existing_sources={row['source_span']['block_id'] for row in rows}
            for bid in included:
                for n in (indices[bid]-1,indices[bid]+1):
                    if 0<=n<len(bundle['blocks']):
                        other=bundle['blocks'][n]
                        if other['id'] not in existing_sources and len(other.get('text',''))<=800:neighbors.add(other['id'])
            rows.extend(_fragment(blocks[bid],_span(blocks[bid]),'context') for bid in sorted(neighbors,key=indices.get))
        # Image-only parts still need their original location, even when image
        # count or byte limits put the picture in a separate request part.
        for aid in owned:
            asset = assets[aid]
            if not asset.get('image_path'):continue
            original_id = (asset.get('image_adaptation') or {}).get('original_asset_id') or aid
            marker = '[[IMAGE:' + original_id + ']]'
            for bid in asset.get('block_ids', []):
                if bid not in blocks:continue
                source = blocks[bid]
                same = [row for row in rows if (row.get('source_span') or {}).get('block_id') == bid]
                at = source.get('text', '').find(marker)
                if (at >= 0 and any(marker in row.get('text', '') for row in same)) or (at < 0 and same):
                    continue
                span = _span(source, at, at + len(marker)) if at >= 0 else _span(source, 0, 0)
                rows.append(_fragment(source, span, 'context'))
                sid = source.get('section_id')
                seen = set()
                while sid in sections and sid not in seen:
                    seen.add(sid);local_sections.add(sid);sid=sections[sid].get('parent_id')
        available={r['source_span']['block_id'] for r in rows}
        subset={k:copy.deepcopy(v) for k,v in bundle.items() if k not in {'blocks','assets','sections','issues','formulas'}}
        # Native structure follows each table fragment as a dependency. Primary
        # ownership remains unique; context copies do not count as coverage.
        dependent=[a['id'] for a in assets.values() if a.get('kind')=='table' and set(a.get('block_ids',[])) & included]
        provided=list(dict.fromkeys(owned+dependent))
        subset.update(blocks=rows,assets=[copy.deepcopy(assets[a]) for a in provided],
            sections=[copy.deepcopy(s) for s in sections.values() if s['id'] in local_sections])
        for asset in subset['assets']:
            if not asset.get('image_path'):continue
            original_id = (asset.get('image_adaptation') or {}).get('original_asset_id') or asset['id']
            marker = '[[IMAGE:' + original_id + ']]'
            matches = [row for row in rows if (row.get('source_span') or {}).get('block_id') in asset.get('block_ids', [])]
            marked = [row for row in matches if marker in row.get('text', '')]
            asset['block_ids'] = [row['id'] for row in (marked or matches)]
        positions={json.dumps(blocks[bid].get('position'),sort_keys=True) for bid in available}
        def local(row):
            pos=row.get('position') or {}
            # Embedded object position extends its containing paragraph position.
            return any(all(pos.get(k)==v for k,v in (blocks[bid].get('position') or {}).items())
                       for bid in available) if pos else bool(ranges and ranges[0]['block_id']==next(iter(blocks),None))
        subset['issues']=[copy.deepcopy(i) for i in bundle.get('issues',[]) if local(i) or set(i.get('page_evidence_ids',[])) & set(owned)]
        subset['issues'].extend(copy.deepcopy(gap) for gap in formula_gaps if local(gap))
        subset['formulas'] = []
        for formula_index, formula in enumerate(bundle.get('formulas', [])):
            if formula_index in ambiguous_formulas:continue
            owner = formula_owners.get(formula_index)
            if owner:
                bid, start, end = owner
                candidates = [row for row in rows if (row.get('source_span') or {}).get('block_id') == bid
                              and row['source_span']['start'] <= start and row['source_span']['end'] >= end]
            elif local(formula):
                pos = formula.get('position') or {}
                candidates = [row for row in rows if (row.get('source_span') or {}).get('start') == 0
                              and (not pos or all(pos.get(k) == v for k, v in
                                   (blocks[row['source_span']['block_id']].get('position') or {}).items()))]
            else:
                candidates = []
            if candidates:
                selected = min(candidates, key=lambda row: row.get('reading_role') != 'primary')
                retained = copy.deepcopy(formula)
                retained['position'] = copy.deepcopy(selected.get('position') or {})
                subset['formulas'].append(retained)
        scope=subset['source'].get('reading_scope')
        if scope is not None:scope['extraction_issues']=list(dict.fromkeys(i.get('message','') for i in subset['issues']))
        # Table fragments keep original cell coordinates/spans. Only cells in
        # the owned row interval travel here; header text accompanies every part.
        for asset in subset['assets']:
            if asset.get('kind')!='table':continue
            fragments=[r for r in rows if r.get('table_context') and r['source_span']['block_id'] in asset.get('block_ids',[])]
            if fragments and all('original_rows' in r['table_context'] for r in fragments):
                structure=asset['structure'];original_headers=structure.get('header_rows',[])
                selected=sorted(set(original_headers)|{n for r in fragments for n in r['table_context']['original_rows']})
                grid=[structure['grid'][n] for n in selected]
                used=sorted({n for row in grid for n in row if n is not None})
                indexes={n:i for i,n in enumerate(used)}
                structure.update(cells=[structure['cells'][n] for n in used],
                    grid=[[None if n is None else indexes[n] for n in row] for row in grid],
                    source_row_indices=selected,source_header_rows=original_headers,
                    header_rows=[selected.index(n) for n in original_headers])
                asset['block_ids']=[r['id'] for r in fragments]
        context=copy.deepcopy(bundle.get('reading_context') or {})
        partial=context.get('source_completeness') == 'partial'
        context.update(outline=outline if background else [s for s in outline if s['id'] in local_sections],
            original_abstract=abstract if background else [],
            note=('目录与原文摘要仅用于定向；本次材料存在缺口，其他分段也不代表缺口已补齐。跨段依赖需明确记录。'
                  if partial else '目录与原文摘要仅用于定向；未在主范围的材料仍由其他分段完整读取。跨段依赖需明确记录，不能猜测。'))
        subset['reading_context']=context
        subset['partition_scope']=dict(primary_ranges=ranges,primary_asset_ids=owned,
            context_asset_ids=[a for a in provided if a not in owned],
            context_block_ids=[r['id'] for r in rows if r['reading_role']=='context'],
            evidence_note='读取提示中的其他页图编号属于全篇索引；本次只有 assets 内带实际图像的对象可声明 pixels。')
        b,c=build_request(subset,template,catalog,config,instruction=PART_RULES+'\n'+instruction,extra=extra,enforce=False)
        c=_assess_capacity(c,config)
        c['part_input_target']=target
        if c['text_token_upper_bound']>target:c['violations'].append('part_target')
        return dict(ranges=ranges,asset_ids=owned,bundle=subset,body=b,capacity=c)

    # A prompt/catalog that cannot fit even without research material is not
    # repaired by an unbounded splitting loop.
    empty=make([],background=False)
    if 'part_target' in empty['capacity']['violations']:
        # A large fixed prompt cannot be split. Treat the target as soft and
        # leave room for at least some source material instead of blocking it.
        target=empty['capacity']['text_token_upper_bound']+300000
        empty=make([],background=False)
    if empty['capacity']['violations']:raise RequestCapacityError('固定模板、目录及预留输出已超过容量，原文保留，未发送请求')
    if confirmed_overflow:
        counter=config.get('text_token_counter')
        source_size=sum(int(counter(b.get('text',''))) if callable(counter)
                        else len(b.get('text','').encode('utf8')) for b in blocks.values())
        target=min(target,empty['capacity']['text_token_upper_bound']+source_size//2+4096)
    leaves=[]
    def divide(items,*,force=False):
        if max_parts is not None and len(leaves)>=max_parts:raise RequestCapacityError('输入分段超过数量预算，全部材料与已定范围保留')
        part=make(items)
        if part['capacity']['violations']:
            minimal=make(items,background=False)
            fewer=set(minimal['capacity']['violations'])<=set(part['capacity']['violations'])
            if (not minimal['capacity']['violations'] or
                    fewer and minimal['capacity']['text_token_upper_bound']<part['capacity']['text_token_upper_bound']):
                part=minimal
        if not part['capacity']['violations'] and not force:
            leaves.append((items,part));return
        if len(items)>1:
            def boundary(i):
                left=items[i-1]['ranges'];right=items[i]['ranges']
                a=blocks[left[-1]['block_id']].get('section_id') if left else None
                z=blocks[right[0]['block_id']].get('section_id') if right else None
                rank=0 if root(a)!=root(z) else 1 if a!=z else 2
                return rank,abs(i-len(items)/2)
            pivot=min(range(1,len(items)),key=boundary)
            divide(items[:pivot]);divide(items[pivot:]);return
        item=items[0]
        if len(item['asset_ids'])>1 and any(v in part['capacity']['violations']
                                             for v in ('image_count','image_bytes','http_bytes')):
            originals={(assets[aid].get('image_adaptation') or {}).get('original_asset_id',aid)
                       for aid in item['asset_ids']}
            if len(originals)==1:
                mid=len(item['asset_ids'])//2
                divide([dict(ranges=item['ranges'],asset_ids=item['asset_ids'][:mid])])
                divide([dict(ranges=[],asset_ids=item['asset_ids'][mid:])]);return
        if item['ranges']:
            span=item['ranges'][0];block=blocks[span['block_id']]
            info=table_info.get(block['id'])
            points=([z for _,z in info['row_spans'] if span['start']<z<span['end']] if info else
                    [] if block['id'] in table_info else _cuts(block,span['start'],span['end']))
            if points:
                cut=min(points,key=lambda n:abs(n-(span['start']+span['end'])/2))
                left_assets, right_assets = [], []
                for ident in item['asset_ids']:
                    asset = assets[ident]
                    original_id = (asset.get('image_adaptation') or {}).get('original_asset_id') or ident
                    marker = '[[IMAGE:' + original_id + ']]'
                    at = block.get('text', '').find(marker, span['start'], span['end'])
                    (right_assets if at >= cut else left_assets).append(ident)
                divide([dict(ranges=[dict(span,end=cut)],asset_ids=left_assets)])
                divide([dict(ranges=[dict(span,start=cut)],asset_ids=right_assets)]);return
        if len(item['asset_ids'])>1:
            mid=len(item['asset_ids'])//2
            divide([dict(ranges=item['ranges'],asset_ids=item['asset_ids'][:mid])])
            divide([dict(ranges=[],asset_ids=item['asset_ids'][mid:])]);return
        if part['capacity']['violations']==['part_target']:
            # Preserve an indivisible formula/table/image which fits the real
            # window even if it is larger than the preferred part target.
            part['capacity']['violations'].remove('part_target')
            part['capacity']['part_target_exceeded']=True
            leaves.append((items,part));return
        raise RequestCapacityError('不可再分的公式、表行、图像或原文对象仍超容量；未截断或丢弃材料')
    divide(atoms,force=confirmed_overflow)
    # Pack adjacent small sections together, while retaining natural cut points.
    packed=[]
    for number,(items,part) in enumerate(leaves):
        if packed:
            merged=make(packed[-1][0]+items)
            would_recreate_full=confirmed_overflow and len(packed)==1 and number==len(leaves)-1
            if not merged['capacity']['violations'] and not would_recreate_full:
                packed[-1]=(packed[-1][0]+items,merged);continue
        packed.append((items,part))
    parts=[p for _,p in packed]
    if confirmed_overflow and len(parts)<2:
        raise RequestCapacityError('已确认超限，但当前材料无法继续分段；原文保留')
    for i,part in enumerate(parts,1):part['id']=f'part-{i:03d}'
    return dict(schema=VERSION,mode='partitioned',parts=parts,coverage=_coverage(bundle,parts),
                original_capacity=capacity,max_parts=max_parts,part_input_target=target,
                context_overflow_confirmed=confirmed_overflow,partition_gaps=formula_gaps)


def _map_part(candidate,subset,prefix):
    """Rename generated identities, not source identities or research strings."""
    data=copy.deepcopy(candidate)
    from preextract_content_recovery import scope_context_gaps
    scope_context_gaps(data,subset)
    units=data.get('profile',{}).get('learning_units',[])
    unit_map={u['id']:prefix+str(u['id']) for u in units if u.get('id')}
    all_ids={};occurrences=Counter()
    for u in units:
        for key in ('materials','steps','outputs'):
            for i,row in enumerate(u.get(key,[])):
                if row.get('id'):
                    occurrences[row['id']]+=1
                    all_ids[row['id']]=prefix+str(u.get('id'))+'_'+key+'_'+str(i+1)
    unique_ids={k:v for k,v in all_ids.items() if occurrences[k]==1}
    fragment_map={b['id']:b['source_span'] for b in subset.get('blocks',[]) if b.get('source_span')}
    def walk(value,ids):
        if isinstance(value,list):
            for child in value:walk(child,ids)
        elif isinstance(value,dict):
            for key,child in list(value.items()):
                if key in {'unit_ids','upstream_unit_ids','unit_sequence','input_refs','output_refs'} and isinstance(child,list):
                    value[key]=[ids.get(x,unit_map.get(x,x)) for x in child]
                elif key=='id' and isinstance(child,str):value[key]=ids.get(child,unit_map.get(child,child))
                else:walk(child,ids)
            span=fragment_map.get(value.get('block_id'))
            if span:
                value['block_id']=span['block_id']
                if 'offset' in value and type(value['offset']) is int:value['offset']+=span['start']
                if 'start' in value or 'end' in value:
                    for key in ('start','end'):
                        if type(value.get(key)) is int:value[key]+=span['start']
                    if value.get('start') is None and value.get('end') is None:
                        value.update(start=span['start'],end=span['end'])
    for u in units:
        ids={**unit_map,**unique_ids}
        for key in ('materials','steps','outputs'):
            for i,row in enumerate(u.get(key,[])):
                if row.get('id'):ids[row['id']]=prefix+str(u.get('id'))+'_'+key+'_'+str(i+1)
        walk(u,ids)
    # Units are already mapped. Other references use only unit identities.
    saved=data.get('profile',{}).pop('learning_units',[])
    walk(data,unit_map)
    data['profile']['learning_units']=saved
    for key in ('gaps','redaction_plan'):
        for i,row in enumerate(data.get(key,[])):row['id']=prefix+key+'_'+str(i+1)
    reading=data.get('source',{}).get('reading_scope') or {}
    for key in ('read_refs','unread_refs'):
        reading[key]=list(dict.fromkeys(fragment_map.get(v,{}).get('block_id',v) for v in reading.get(key,[])))
    return data


def _merge_value(old,new,old_path,new_path,moves,*,field=''):
    moves[old_path]=new_path
    if old is None or old==[] or old=='':return copy.deepcopy(new)
    if new is None or new==[] or new=='':return old
    if isinstance(old,dict) and isinstance(new,dict):
        for key,value in new.items():
            old[key]=_merge_value(old.get(key),value,old_path+'/'+key,new_path+'/'+key,moves,field=key)
        return old
    if isinstance(old,list) and isinstance(new,list):
        for i,value in enumerate(new):
            found=None
            if field!='learning_units':
                found=next((j for j,row in enumerate(old) if row==value or
                    (isinstance(row,dict) and isinstance(value,dict) and row.get('id') and row.get('id')==value.get('id'))),None)
            j=len(old) if found is None else found
            moves[old_path+'/'+str(i)]=new_path+'/'+str(j)
            if found is None:old.append(copy.deepcopy(value))
            else:old[j]=_merge_value(old[j],value,old_path+'/'+str(i),new_path+'/'+str(j),moves)
        return old
    if old==new:return old
    if field=='storage':return max((old,new),key=lambda v:{'omit':0,'description':1,'image':2}.get(v,-1))
    if field in {'evidence_basis','work_attribution','status','decision','kind','parent_id','order'}:
        # Preserve conflicting assertions in the part records; do not invent
        # a combined enum. Global fields are settled by the synthesis request.
        return old
    if isinstance(old,str) and isinstance(new,str):return old+'\n\n'+new
    if isinstance(old,bool) and isinstance(new,bool):return old or new
    return old


def assemble_parts(candidates,subsets,bundle):
    """Keep all detailed learning records; map pointers while merging indexes."""
    from preextract_contract import load_template,_blank
    _,skeleton=load_template();result=_blank(skeleton)
    result.update(schema_version=skeleton['schema_version'],record_kind='filled_candidate')
    before_units=0;maps=[]
    for number,(raw,subset) in enumerate(zip(candidates,subsets),1):
        part=_map_part(raw,subset,f'P{number:03d}_')
        before_units+=len(part['profile']['learning_units'])
        starts={key:len(result[key]) for key in ('gaps','redaction_plan')}
        moves={}
        for key in ('profile','tree','assets','redaction_plan','gaps','segmentation','attachments'):
            result[key]=_merge_value(result[key],part.get(key),'/'+key,'/'+key,moves,field=key)
        for key in ('gaps','redaction_plan'):
            for row in result[key][starts[key]:]:
                pointer=row.get('target_pointer')
                if isinstance(pointer,str):
                    matched=next((p for p in sorted(moves,key=len,reverse=True) if pointer==p or pointer.startswith(p+'/')),None)
                    if matched is not None:row['target_pointer']=moves[matched]+pointer[len(matched):]
        reading=part.get('source',{}).get('reading_scope') or {}
        prior=result['source']['reading_scope']
        result['source']['reading_scope']=_merge_value(prior,reading,'/source/reading_scope','/source/reading_scope',{},field='reading_scope')
        if raw.get('record_kind')=='partial_candidate':result['record_kind']='partial_candidate'
        maps.append(dict(part=number,pointer_map=moves))
    reading=result['source']['reading_scope']
    declarations={};scoped_ids=set();file_refs={v for v in (bundle.get('file_ref','main'),
        bundle['source'].get('source_id'),bundle['source'].get('file'),bundle['source'].get('text_file')) if v}
    for raw,subset in zip(candidates,subsets):
        scope=raw.get('source',{}).get('reading_scope') or {}
        yes=set(scope.get('read_refs',[]));no=set(scope.get('unread_refs',[]))
        scoped_ids.update(s['id'] for s in subset.get('sections',[]))
        for block in subset.get('blocks',[]):
            if block.get('reading_role','primary')!='primary':continue
            span=block.get('source_span') or _span(block);bid=span['block_id'];scoped_ids.add(bid)
            refs={block['id'],block.get('section_id')}|file_refs
            status='unread' if refs & no else 'read' if refs & yes else 'undeclared'
            declarations.setdefault(bid,[]).append(dict(span,status=status))
    reading['read_refs']=[r for r in reading.get('read_refs',[]) if r not in scoped_ids|file_refs]
    reading['unread_refs']=[r for r in reading.get('unread_refs',[]) if r not in scoped_ids|file_refs]
    original={b['id']:b for b in bundle.get('blocks',[])}
    from preextract_contract import SOURCE_REF
    for bid,spans in declarations.items():
        unread=[s for s in spans if s['status']=='unread']
        wholly_unread=bool(unread) and all(s['status']=='unread' for s in spans)
        if wholly_unread:reading['unread_refs'].append(bid)
        elif any(s['status']=='read' for s in spans):reading['read_refs'].append(bid)
        if unread and not wholly_unread:
            for span in unread:
                gap=_blank(skeleton['gaps'][0])
                gap.update(id=f'INPUT_READING_{len(result["gaps"])+1}',type='not_read',
                    target_pointer='/source/reading_scope',description='此原文块只有指定分段区间声明未读；其他已读区间保留。',
                    impact='不能据未读区间补充事实。',use_limit='相关区间待核，已有学习内容可按已读范围使用。',claim_status='explicit',
                    source_refs=[dict(_blank(SOURCE_REF),file_ref=bundle.get('file_ref') or bundle['source'].get('source_id') or 'main',block_id=bid,
                        section_id=original[bid].get('section_id'),start=span['start'],end=span['end'])])
                result['gaps'].append(gap)
    unread=set(reading.get('unread_refs',[]))
    reading['read_refs']=list(dict.fromkeys(r for r in reading.get('read_refs',[]) if r not in unread))
    reading['unread_refs']=list(dict.fromkeys(reading['unread_refs']))
    result['source']=copy.deepcopy(bundle['source']);result['source']['reading_scope']=reading
    entities={}
    for row in result['redaction_plan']:
        ident=(row.get('role'),row.get('expected_text'))
        if ident[1]:
            if ident not in entities:entities[ident]=(f'entity_{len(entities)+1:04d}',row.get('replacement'))
            row['entity_key'],row['replacement']=entities[ident]
    if len(result['profile']['learning_units'])!=before_units:raise ValueError('分段装配丢失学习内容')
    return result,dict(learning_units_preserved=before_units,pointer_maps=maps,
        detailed_learning_sha256=digest(result['profile']['learning_units']),reading_intervals=declarations)


def apply_synthesis(candidate,synthesis):
    """Only global fields and explicit relations may change after extraction."""
    result=copy.deepcopy(candidate)
    profile=synthesis.get('profile') or {}
    if profile.get('learning_units'):raise ValueError('全文统合不得改写详细学习内容')
    allowed=set(result['profile'])-{'learning_units'}
    if set(profile)-allowed-{'learning_units'}:raise ValueError('全文统合包含不支持的画像字段')
    if synthesis.get('assets') or synthesis.get('tree'):raise ValueError('全文统合不得改写已提取图表和章节内容')
    for key,value in profile.items():
        if key=='learning_units':continue
        if key=='migration_boundary':
            # Local evidence restrictions remain binding even when the global
            # response omits them. The synthesis can add, never erase them.
            result['profile'][key]=_merge_value(result['profile'].get(key),value,'','',{})
        else:
            prior_limits=copy.deepcopy(result['profile'].get(key,{}).get('usage_limits',[])) if key=='applicable_level' else []
            result['profile'][key]=copy.deepcopy(value)
            if key=='applicable_level' and isinstance(value,dict):
                result['profile'][key]['usage_limits']=_merge_value(prior_limits,value.get('usage_limits'),'', '',{})
    known={u['id']:u for u in result['profile']['learning_units']}
    for link in synthesis.get('unit_links',[]):
        ident=link.get('id');upstream=link.get('upstream_unit_ids',[])
        if ident not in known or any(u not in known or u==ident for u in upstream):
            raise ValueError('全文统合关系引用了未知学习单元')
        known[ident]['upstream_unit_ids']=list(dict.fromkeys(known[ident].get('upstream_unit_ids',[])+upstream))
    nodes={n['id']:n for n in result.get('tree',{}).get('nodes',[])}
    for link in synthesis.get('section_links',[]):
        if link.get('id') not in nodes or link.get('to_section_id') not in nodes:raise ValueError('全文统合章节关系无法定位')
        nodes[link['id']]['connections'].append({k:v for k,v in link.items() if k!='id'})
    for key in ('gaps','redaction_plan'):
        for row in synthesis.get(key,[]):
            row=copy.deepcopy(row);row['id']='GLOBAL_'+key+'_'+str(len(result[key])+1);result[key].append(row)
    if synthesis.get('record_kind')=='partial_candidate':result['record_kind']='partial_candidate'
    return result
