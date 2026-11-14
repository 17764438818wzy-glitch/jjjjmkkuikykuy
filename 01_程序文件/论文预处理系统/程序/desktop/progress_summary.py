"""Display-only projections of native counters and stages."""


def pipeline_text(counts, limit=None):
    if not counts:return ''
    model=counts.get('llm_active')
    value=('模型请求 '+str(model)+(('/'+str(limit)) if limit else '')) if model is not None else '模型请求数待同步'
    return (value+f" · 材料准备 {counts.get('preparation_active',0)}"
        +f" · 待调用 {counts.get('preparation_waiting',0)}"
        +f" · 后处理 {counts.get('postprocess_active',0)}"
        +f" · 待后处理 {counts.get('postprocess_waiting',0)}")


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
