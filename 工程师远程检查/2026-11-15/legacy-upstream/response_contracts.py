"""Image coordinates are checked while the model response can still be repaired."""

def validate_privacy_spans(result,payload):
    """Response wording must point to exact source text before cache acceptance."""
    import re
    from agent_client import AgentResultError
    if result.get('decision')!='pass':return
    original=payload.get('text','')
    for index,value in enumerate(result.get('sensitive_strings',[])):
        if not isinstance(value,str) or not value or value not in original:
            raise AgentResultError(f'sensitive_strings[{index}] 脱敏位置不匹配原文：须逐字复制原文精确子串，保留原空格和换行，不得翻译、拼写修正或概括')
        if re.fullmatch(r'[\d.,%％()（）+\-−—–\s]+',value):
            raise AgentResultError(f'sensitive_strings[{index}] 不能仅为数值：须带明确身份字段上下文，不能删除研究数值')

def _validate_transcription_fields(entry,path):
    """Check required consumer fields before a response leaves the retry loop."""
    from agent_client import AgentResultError
    kind=entry.get('kind')
    if not isinstance(kind,str) or kind not in {'text','table','image'}:
        raise AgentResultError(f'{path}.kind 只允许 text、table 或 image')
    if kind=='image':
        if not isinstance(entry.get('text'),str):
            raise AgentResultError(f'{path}.text 必须为图片可见文字字符串，无文字时为空字符串')
        if not isinstance(entry.get('description'),str):
            raise AgentResultError(f'{path}.description 必须为图意字符串，不能编造')
        if not (entry['text'].strip() or entry['description'].strip()):
            raise AgentResultError(f'{path}.text 与 description 不能同时为空')
    elif kind=='text':
        if not isinstance(entry.get('text'),str) or not entry['text'].strip():
            raise AgentResultError(f'{path}.text 必须为非空原文字符串')
        try:int(entry.get('heading_level',0))
        except (ValueError,TypeError,OverflowError):
            raise AgentResultError(f'{path}.heading_level 必须为整数') from None
    else:
        rows=entry.get('rows')
        if (not isinstance(rows,list) or not rows or
                not all(isinstance(row,list) and row and all(isinstance(v,str) for v in row) for row in rows) or
                len({len(row) for row in rows})!=1 or not any(v.strip() for row in rows for v in row)):
            raise AgentResultError(f'{path}.rows 必须为完整等宽的非空字符串表格，保留原数值与空单元格')
        headers=entry.get('header_rows',[0])
        if not isinstance(headers,list) or any(type(i) is not int or not 0<=i<len(rows) for i in headers):
            raise AgentResultError(f'{path}.header_rows 必须为有效行号数组，无表头时为 []')


def _validate_visual_fields(entry, path):
    from agent_client import AgentError, AgentResultError
    from visual_knowledge import visual_fields
    for field in ('chart_spec', 'structured_table'):
        if entry.get(field) is None:
            continue
        try:
            visual_fields({field: entry[field]})
        except (AgentError, TypeError, ValueError, OverflowError):
            raise AgentResultError(f'{path}.{field} 结构无效：请按该字段约定返回合法类型和字段；'
                                   '不得增加猜测数值，表格保留原样字符串和等宽行列') from None


def validate_context_images(task,payload,image_count):
    """Accept one explicit masked-overview role, never ignore arbitrary images."""
    from agent_client import AgentResultError
    if not isinstance(payload,dict) or 'context_image_indices' not in payload:return False
    indices=payload['context_image_indices']
    if (task!='privacy_image' or payload.get('phase') not in ('verify_masked','locate_privacy_regions')
            or image_count!=2 or type(indices) is not list or len(indices)!=1
            or type(indices[0]) is not int or indices[0]!=2):
        raise AgentResultError('context_image_indices 仅允许两图隐私复核时为 [2]；第一图为唯一坐标目标，第二图仅为已遮盖语境')
    return True


def validate_visual_coordinates(task,result,image_sizes,pages,payload=None):
    from agent_client import AgentResultError
    from auto_evidence import bounds
    context_mode=validate_context_images(task,payload,len(image_sizes))
    if task not in {'repair','extract_page','privacy_image'} or result['decision']!='pass':return
    if task=='privacy_image':_validate_visual_fields(result,task)
    if not image_sizes:return
    if context_mode:
        # The second bitmap supplies scene context, not a second page to mask.
        # Pixel coordinates must still fit the first (usually smaller) crop.
        image_sizes=image_sizes[:1];pages=[1]
    if not isinstance(pages,list) or len(pages)!=len(image_sizes) or any(type(p) is not int for p in pages) or len(set(pages))!=len(pages):
        raise AgentResultError('请求页码与图片尺寸不对应')
    sizes=dict(zip(pages,image_sizes))
    unit=result.get('coordinate_space','normalized')
    def check(value,size,space,path):
        try:bounds(value,size,space)
        except (ValueError,TypeError,RuntimeError) as exc:
            raise AgentResultError('AI 响应坐标无效：'+path+'；'+str(exc)) from None
    def masks(value,page,space,path):
        if not isinstance(value,list):raise AgentResultError('AI 响应坐标数组无效：'+path)
        for n,box in enumerate(value):check(box,sizes[page],space,f'{path}[{n}]')
    if len(pages)==1:
        if result.get('page_masks'):
            raise AgentResultError('单页请将遮盖区域返回 sensitive_boxes，不使用 page_masks')
        masks(result.get('sensitive_boxes'),pages[0],unit,'sensitive_boxes')
    else:
        if result.get('sensitive_boxes'):raise AgentResultError('多页 sensitive_boxes 必须为空，遮盖区域应逐页列入 page_masks')
        groups=result.get('page_masks')
        if not isinstance(groups,list):raise AgentResultError('多页缺少 page_masks 数组')
        seen=set()
        for n,group in enumerate(groups):
            page=group.get('page') if isinstance(group,dict) else None
            if type(page) is not int or page not in sizes or page in seen:
                raise AgentResultError(f'page_masks[{n}].page 必须是已提供且未重复的页码')
            seen.add(page);masks(group.get('boxes'),page,group.get('coordinate_space',unit),f'page_masks[{n}].boxes')
        if seen!=set(pages):raise AgentResultError('page_masks 必须涵盖每张输入图片，包括没有敏感信息的页')
    field='insertions' if task=='repair' else 'blocks' if task=='extract_page' else None
    known=(payload or {}).get('known_blocks')
    targets={b['id']:b for b in known if isinstance(b,dict) and isinstance(b.get('id'),str)} if isinstance(known,list) else None
    replaced=set()
    for n,entry in enumerate(result.get(field,[]) if field else []):
        if not isinstance(entry,dict):raise AgentResultError(f'{field}[{n}] 必须是对象')
        _validate_transcription_fields(entry,f'{field}[{n}]')
        _validate_visual_fields(entry,f'{field}[{n}]')
        if task=='repair':
            operation=entry.get('operation','append')
            if not isinstance(operation,str) or operation not in {'append','replace_table'}:
                raise AgentResultError(f'{field}[{n}].operation 只允许 append 或 replace_table，省略时为 append')
            if targets is not None:
                after=entry.get('after_id')
                if not isinstance(after,str) or after not in targets:
                    raise AgentResultError(f'{field}[{n}].after_id 必须对应 known_blocks 中的 id')
                target=targets[after]
                if operation=='replace_table':
                    if entry['kind']!='table' or target.get('type')!='table' or after in replaced:
                        raise AgentResultError(f'{field}[{n}] 整表替换目标无效：replace_table 仅能替换一个原生 table 一次')
                    replaced.add(after)
                actual=entry.get('page',pages[0] if len(pages)==1 else None)
                if target.get('candidate_pages') is not None and actual not in target['candidate_pages']:
                    raise AgentResultError(f'{field}[{n}].page 必须属于该对象的 candidate_pages')
        page=entry.get('page',pages[0] if len(pages)==1 else None)
        if type(page) is not int or page not in sizes:
            raise AgentResultError(f'{field}[{n}].page 必须对应已提供的图片')
        if entry.get('kind')!='image':continue
        check(entry.get('bbox'),sizes[page],entry.get('coordinate_space',unit),f'{field}[{n}].bbox')
        if task=='repair' and targets is not None and target.get('table_candidate') and target.get('table_bbox'):
            expected=bounds(target['table_bbox'])
            actual=bounds(entry['bbox'],sizes[page],entry.get('coordinate_space',unit))
            if any(actual[i]>expected[i]+.001 for i in (0,1)) or any(actual[i]<expected[i]-.001 for i in (2,3)):
                raise AgentResultError(f'{field}[{n}].bbox 未覆盖完整候选表格：须覆盖该对象的 normalized table_bbox={expected}，包括末行；不得丢弃原图或修改研究数值')
    if task=='repair':
        if targets is not None:
            missing=[key for key,target in targets.items() if target.get('requires_table_replacement') and key not in replaced]
            if missing:
                raise AgentResultError('疑难合并表格 '+', '.join(missing)+' 须提供 kind=table、operation=replace_table 的完整原文行列；保留 cells 及 continuation_text 中的原文，不得猜测空白数值；仅截图或 nonresearch_objects 不能替代结构恢复')
        ignored=result.get('nonresearch_objects',[])
        if not isinstance(ignored,list):raise AgentResultError('nonresearch_objects 必须是数组')
        for n,entry in enumerate(ignored):
            if not isinstance(entry,dict) or not isinstance(entry.get('after_id'),str) or not isinstance(entry.get('reason'),str) or not entry['reason'].strip():
                raise AgentResultError(f'nonresearch_objects[{n}] 须提供 after_id 和非空 reason')
            if targets is not None and entry['after_id'] not in targets:
                raise AgentResultError(f'nonresearch_objects[{n}].after_id 必须对应 known_blocks 中的 id')
