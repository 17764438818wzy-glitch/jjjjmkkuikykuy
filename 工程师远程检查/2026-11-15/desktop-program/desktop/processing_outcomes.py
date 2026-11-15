"""Execution and AI results are independent of the final admission decision."""
import threading
from datetime import datetime


AI_LABELS={'passed':'通过','rejected':'不通过','call_failed':'调用失败',
           'validation_failed':'结果校验失败',
           'not_started':'未进入 AI','running':'处理中','response_saved':'回答已保存',
           'incomplete':'未完成','unknown':'待核对'}
TASK_LABELS={'material_intake':'材料用途分流','screen':'文种判断','classify':'专业与题名识别','privacy_text':'文本脱敏',
             'privacy_image':'图表识别与脱敏','repair':'局部内容恢复','extract_page':'页面识别',
             'paper_preextract':'完整 MD 提取'}


def ai_outcome(item,run_status=None):
    result=item.get('ai_result') or {}
    if (item.get('content_format')=='paper_preextract' and result.get('status')=='call_failed'
            and str(result.get('reason','')).startswith(('分段结构或来源校验失败','模型回答格式校验未通过','全文统合'))):
        return AI_LABELS['validation_failed'],result.get('reason',''),result.get('stage','')
    if result.get('status') in AI_LABELS:
        status=result['status']
        if status=='running' and run_status in {'completed','stopped','interrupted','failed'}:
            return '未完成','任务已停止或中断，尚未形成 AI 最终结论',result.get('stage','')
        return AI_LABELS[status],result.get('reason',''),result.get('stage','')
    reason=item.get('reason','')
    if item.get('reason_kind')=='duplicate':
        return '未进入 AI','本次重复跳过，未重新调用 AI','去重'
    metrics=item.get('processing_metrics') or []
    stages=[s.get('stage','') for s in item.get('stages',[]) if isinstance(s,dict)]
    entered=bool(metrics) or any(s.startswith('AI ') or s=='续跑复用' for s in stages)
    stage=TASK_LABELS.get(metrics[-1].get('task'),'AI 处理') if metrics else next((s for s in reversed(stages) if s.startswith('AI ')),'')
    if item.get('status')=='pass' and item.get('automatic_approval') and (
            item.get('review_status')=='ai_passed' or item.get('redaction_status')=='ai_passed' or entered):
        return '通过',reason or '历史记录显示 AI 预处理已完成',stage
    if entered:
        if run_status=='running' and item.get('status') in {'pending','running'}:
            return '处理中','AI 处理尚未完成',stage
        if item.get('reason_kind')=='stopped':return '未完成',reason or '处理已停止',stage
        if item.get('reason_kind')=='technical' and any(m.get('outcome')=='error' for m in metrics):
            return '调用失败',reason or '模型调用或响应校验失败',stage
        if item.get('status')=='reject' and item.get('reason_kind') in {'privacy','evidence','classification','screening'}:
            return '不通过',reason or 'AI 处理后的内容未满足预处理要求',stage
        return '未完成',reason or '有 AI 处理记录，缺少最终结论',stage
    if item.get('screening',{}).get('decision')=='reject':
        return '未进入 AI',reason or '基础筛除未通过','基础筛除'
    return '待核对','旧记录未区分脚本和 AI 状态；'+(reason or '无法可靠判断 AI 处理结论'),stage


def execution_outcome(item,manifest):
    """Use new explicit execution state; keep legacy admission checks intact."""
    if item.get('status')=='deleted' and item.get('reason_kind')=='material_deleted':
        return '已删除',item.get('reason','过程表单已在 AI 前删除')
    if item.get('content_format')=='associated_material' and item.get('script_result',{}).get('status')=='completed':
        return ('待解析' if item.get('status')=='pending' else '已保存'),item.get('reason','原件已保存')
    if item.get('status')=='pending' and item.get('reason_kind')=='material_pending':
        return '待解析',item.get('reason','原件已保留，待解析')
    if item.get('content_format')=='paper_preextract':
        result=item.get('script_result') or {}
        status=result.get('status')
        if status=='completed':return '通过',result.get('reason','程序处理已完成；AI 结论见“AI 处理结果”')
        if status=='running' and manifest.get('status')=='running':return '处理中',result.get('reason','程序处理中')
        return '不通过',result.get('reason','程序处理尚未完成')
    if item.get('prepared_checkpoint') and item.get('ai_result',{}).get('status') in {'running','rejected','call_failed','incomplete'}:
        return '通过','脚本结果已保存；AI 问题请在“AI 处理结果”查看和续跑'
    result=item.get('script_result') or {}
    status=result.get('status')
    if status in {'completed','failed','rejected','stopped','running'}:
        if item.get('ai_result',{}).get('status')=='running' and manifest.get('status') in {'completed','stopped','interrupted','failed'}:
            return '不通过','处理已中断，AI 尚未形成结论，可续跑未完成部分'
        if status=='completed' and item.get('status')=='pass':
            from script_task import script_outcome
            checked=script_outcome(item,manifest)
            if checked[0]!='通过':return checked
        label='通过' if status=='completed' else '处理中' if status=='running' and manifest.get('status')=='running' else '不通过'
        return label,result.get('reason') or ('脚本执行完成，AI 结论见“AI 处理结果”' if label=='通过' else '脚本尚未完成')
    from script_task import script_outcome
    return script_outcome(item,manifest)


class PhaseTracker:
    def __init__(self,item,notify=lambda item:None,lock=None):
        self.item=item
        self.notify=notify
        self.lock=lock or threading.RLock()
        self.steps={}
        item['script_result']={'status':'running','reason':'正在执行本地脚本'}
        item['ai_result']={'status':'not_started','reason':'尚未进入 AI 处理','stage':''}
        item['ai_steps']=[]

    def observe(self,call_id,task,state,reason='',cache_hit=False):
        from evidence import redactor
        with self.lock:
            label=TASK_LABELS.get(task,task)
            if state=='running':
                step={'task':task,'stage':label,'status':state,'reason':'',
                      'started_at':datetime.now().isoformat(timespec='seconds')}
                self.steps[call_id]=step
                self.item['ai_steps'].append(step)
            else:
                self.steps[call_id].update(status=state,reason=redactor()(str(reason))[:500],
                    cache_hit=bool(cache_hit),finished_at=datetime.now().isoformat(timespec='seconds'))
            self.item['ai_result']={'status':'running','reason':'AI 处理尚未完成','stage':label}
            self.item['script_result']={'status':'completed','reason':'基础脚本已执行完成，AI 处理进行中'}
            self.notify(self.item)

    def ai_complete(self):
        self.item['ai_result']={'status':'passed','reason':'所需 AI 处理及结果校验通过',
                               'stage':self.item['ai_result'].get('stage','')}

    def finish(self,outcome,reason='',stopped=False):
        with self.lock:
            # The one-request stage maintains independent program/model states.
            # A surrounding batch save or exception must not collapse them.
            if self.item.get('content_format') in {'paper_preextract','associated_material'}:
                if self.item.get('script_result',{}).get('status')=='running':
                    self.item['script_result']={'status':'stopped' if stopped else 'failed','reason':reason}
                return
            entered=bool(self.item['ai_steps'])
            ai=self.item['ai_result']
            if outcome=='passed':
                self.ai_complete()
                status='completed'
            elif outcome=='rejected':
                status='completed' if entered else 'rejected'
                ai.update(status='rejected' if entered else 'not_started',reason=reason)
            elif outcome=='pending':
                status='completed'
                ai.update(status='incomplete' if entered else 'not_started',reason=reason)
            else:
                status='stopped' if stopped else 'failed'
                if ai['status']!='passed':
                    failed=any(s['status']=='error' for s in self.item['ai_steps'])
                    ai.update(status='not_started' if not entered else 'incomplete' if stopped or not failed else 'call_failed',reason=reason)
            self.item['script_result']={'status':status,'reason':
                '脚本执行完成；AI 结论见“AI 处理结果”' if status=='completed' else reason}
