"""Display-only projections of native counters and stages."""


def pipeline_text(counts, limit=None):
    if not counts:return ''
    paper=counts.get('paper_llm_active')
    purpose=counts.get('purpose_llm_active')
    total=counts.get('total_llm_active',counts.get('llm_active'))
    value='正文提取 '+(str(paper) if paper is not None else '待核实')+(('/'+str(limit)) if limit else '')
    purpose_limit=counts.get('purpose_llm_limit')
    prepared=counts.get('preparation_workers')
    text=(value+' · 用途复检 '+(str(purpose) if purpose is not None else '待核实')+('/'+str(purpose_limit) if purpose_limit else '')
        +' · 全部请求 '+(str(total) if total is not None else '待核实')
        +f" · 材料准备 {counts.get('preparation_active','待核实')}"+('/'+str(prepared) if prepared else '')
        +f" · 正文就绪 {counts.get('preparation_waiting',0)}"
        +f" · 后处理 {counts.get('postprocess_active',0)}"
        +f" · 待后处理 {counts.get('postprocess_waiting',0)}")
    if 'body_candidates' in counts:text+=f" · 已确认待准备 {counts['body_candidates']}"
    if 'classified_files' in counts:
        text+=f" · 分流完成 {counts['classified_files']} · 确认附件 {counts.get('associated_files',0)} · 用途待确认 {counts.get('purpose_pending',0)}"
    if 'formal_started' in counts:
        if not counts['formal_started'] and counts.get('classification_done'):text+=' · 扫描已结束，无新正文请求；复用与附件继续处理'
        elif not counts['formal_started']:text+=f" · 首批正文 {counts.get('confirmed_bodies',0)}/{counts.get('startup_target',50)}，等待确认"
        elif counts.get('classification_done') and counts.get('confirmed_bodies',0)<counts.get('startup_target',50):text+=' · 来源已耗尽，处理不足50篇的末批'
    return text


def display_phase(item):
    """Explain current execution without changing its admission or AI outcome."""
    stages=item.get('stages') or []
    latest=stages[-1].get('stage','') if stages and isinstance(stages[-1],dict) else ''
    result=item.get('ai_result') or {}
    if latest in {'完整回答已保存，等待程序后处理'}:return '等待后处理'
    if latest in {'程序后处理开始','校验提取结果','执行脱敏、切片与封包','封包检查完成','装配分段学习内容和来源'}:
        return '后处理'
    if latest in {'材料准备完成，等待 AI 名额','材料准备完成'} and result.get('status') in {'not_started',None}:
        return '等待模型'
    if result.get('status')=='running' and latest.startswith('AI '):return '模型处理中'
    return latest or result.get('stage','')


def delivery_label(item):
    delivery=item.get('preextract_delivery') or {}
    if delivery.get('status')=='complete' and item.get('handoff_status')=='uploaded':return '已入库'
    if delivery or item.get('handoff_status') in {'pending','unknown'}:return '入库待确认'
    if item.get('handoff_status')=='uploaded':return '已记录上传'
    if item.get('package_path') and item.get('automatic_approval'):return '待入库'
    return '尚未入库'
