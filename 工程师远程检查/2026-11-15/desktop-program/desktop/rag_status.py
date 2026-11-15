"""Remote observations, independent from upload acknowledgements and local approval."""
UPLOAD_STATES=('全部上传状态','待上传','排队待上传','上传中','上传待确认','已上传','上传失败','部分上传失败','部分上传待确认','记录异常','配置异常')
PROCESS_STATES=('全部处理状态','尚未提交','已提交，待确认','排队中','处理中','已完成','暂缓','剔除','处理失败','部分处理失败','已取消','部分已取消','处理状态待确认','状态暂不可获取')


def normalize_remote_doc(doc):
    # RAGFlow common/constants.py TaskStatus: 0 unstart,1 running,2 cancel,3 done,4 fail,5 schedule.
    run=str(doc.get('run','')).strip().upper()
    mapping={'0':'unstarted','UNSTART':'unstarted','UNSTARTED':'unstarted',
             '1':'running','RUNNING':'running','PROCESSING':'running','PARSING':'running',
             '2':'cancelled','CANCEL':'cancelled','CANCELLED':'cancelled','CANCELED':'cancelled',
             '3':'done','DONE':'done','COMPLETED':'done','COMPLETE':'done','SUCCESS':'done',
             '4':'failed','-1':'failed','FAIL':'failed','FAILED':'failed','ERROR':'failed',
             '5':'queued','SCHEDULE':'queued','QUEUED':'queued','SUBMITTED':'submitted'}
    status=mapping.get(run,'unknown')
    if not run:
        try:
            if float(doc.get('progress'))>=1:status='done'
        except (ValueError,TypeError):pass
    return status,str(doc.get('progress_msg') or doc.get('message') or '')[:500]


def processing_state(record):
    if record.get('query_error'):return '状态暂不可获取'
    remote=str(record.get('ragflow_status') or '').lower()
    states={'done':'已完成','completed':'已完成','complete':'已完成','success':'已完成',
            'failed':'处理失败','fail':'处理失败','error':'处理失败','cancelled':'已取消',
            'running':'处理中','processing':'处理中','parsing':'处理中','queued':'排队中',
            'submitted':'已提交，待确认','unstarted':'尚未提交','missing':'状态暂不可获取'}
    if remote in states:return states[remote]
    if remote:return '处理状态待确认'
    if record.get('parse_status')=='done':return '已完成'
    if record.get('parse_status') in {'submitted','running'}:return '已提交，待确认'
    return '处理状态待确认'


def needs_recovery(state):
    return state.get('can_retry',True) and (state['upload']!='已上传' or state['process'] not in {'已完成','处理中','排队中'}
            or '交接尚未完成' in state.get('assets',''))


def describe_item(item,manifest,service,index=None,legacy_target=None):
    if item.get('content_format')=='paper_preextract':
        from preextract_handoff import describe_preextract_item
        return describe_preextract_item(item,service)
    from rag_operations import same_service
    saved_targets=item.get('handoff_destinations')
    if saved_targets is None:saved_targets=manifest.get('task_settings',{}).get('destinations')
    targets=saved_targets or {}
    records=[record for key,record in (item.get('uploads') or {}).items() if same_service(key,service)]
    target=targets.get('paper') or {}
    receipt_owner=None
    if not target:
        papers=[r for r in records if r.get('route')!='image']
        if not papers and index is not None:
            hits=[hit for hit in index.matches(item,service,None) if hit['record'].get('route')!='image']
            ids={hit['record'].get('dataset_id') for hit in hits}
            if len(ids)==1:
                papers=[hits[-1]['record']]
        if len(papers)==1:target={'id':papers[0].get('dataset_id'),'name':papers[0].get('dataset_name')}
        elif saved_targets is None:target=legacy_target or {}
    target_id=target.get('id')
    record=next((r for r in records if r.get('dataset_id')==target_id),None)
    if record is None and index is not None and target_id:
        matches=index.matches(item,service,target_id)
        if matches:
            hit=matches[-1];record=hit['record']
            receipt_owner=(hit['manifest']['run_id'],hit['item']['id'])
            if record not in records:records.append(record)
    image_target=targets.get('image') or {}
    if not image_target:
        image_records=[r for r in records if r.get('route')=='image']
        if len(image_records)==1:
            image_target={'id':image_records[0].get('dataset_id'),'name':image_records[0].get('dataset_name')}
    state={'target':target.get('name') or target_id or '未指定知识库',
           'image_target':image_target.get('name') or image_target.get('id') or '未指定',
           'image_target_id':image_target.get('id'),
           'upload':'待上传','process':'尚未提交','reason':item.get('handoff_reason',''),
           'assets':'尚无资料上传记录','record':record,'records':records,'target_id':target_id,'receipt_owner':receipt_owner,
           'can_retry':bool(target_id and (records or saved_targets is None or
               all((targets.get(kind) or {}).get('id') for kind in ('paper','image'))))}
    if not target_id:
        state.update(reason=state['reason'] or '已通过预处理，请在本页选择知识库并点击“导入选中项”')
        if '知识库' in state['reason'] and any(token in state['reason'] for token in ('不存在','缺少','还没选','不唯一')):
            state['upload']='配置异常'
        return state
    if not record:
        state['upload']='记录异常' if records else '待上传'
        reason=state['reason']
        if '知识库' in reason and any(t in reason for t in ('不存在','缺少','还没选','不唯一')):state['upload']='配置异常'
        if not reason:
            state['reason']=('当前目标库没有对应回执' if records else '已通过预处理，等待上传'
                if state['can_retry'] else '已通过预处理，请在本页选择论文和图片知识库并点击“导入选中项”')
        return state
    status=record.get('status')
    if status in {'failed','error'}:state['upload']='上传失败'
    elif status in {'sending','unknown'}:state['upload']='上传待确认'
    elif status=='uploaded' and record.get('document_id'):
        state.update(upload='已上传',process=processing_state(record))
    else:state['upload']='记录异常'
    state['reason']='；'.join(dict.fromkeys(text for text in
        [record.get('query_error'),record.get('parse_error'),state['reason'],record.get('ragflow_reason'),record.get('error')] if text))
    if record.get('chunk_count') is not None:
        state['reason']=(state['reason']+'；' if state['reason'] else '')+f"正文分块 {record['chunk_count']}"
    # Resource receipts belong to each destination and can fail after the paper was accepted.
    target_ids={target_id,image_target.get('id')}
    resources=[(r,a) for r in records if r.get('dataset_id') in target_ids
               for a in r.get('asset_uploads',[]) if isinstance(a,dict)]
    if resources:
        done=sum(a.get('status')=='uploaded' and bool(a.get('document_id')) for _,a in resources)
        state['assets']=f'已上传 {done}/{len(resources)} 项'
        failed=[(r,a) for r,a in resources if a.get('status') in {'failed','error'}]
        waiting=[(r,a) for r,a in resources if a.get('status') in {'sending','unknown','pending'}]
        if failed or waiting:
            state['upload']='部分上传失败' if failed else '部分上传待确认'
            route,asset=(failed or waiting)[0]
            state['reason']+='；'+('图片资料' if route.get('route')=='image' else '附属资料')+'：'+str(asset.get('path',''))+' '+str(asset.get('error') or '尚未确认上传完成')
        remote_errors=[a for _,a in resources if a.get('query_error')]
        remote_failed=[a for _,a in resources if processing_state(a)=='处理失败']
        remote_cancelled=[a for _,a in resources if processing_state(a)=='已取消']
        if remote_errors or remote_failed or remote_cancelled:
            abnormal=(remote_errors or remote_failed or remote_cancelled)[0]
            state['process']='状态暂不可获取' if remote_errors else '部分处理失败' if remote_failed else '部分已取消'
            state['assets']+='；'+state['process']
            state['reason']+='；附属资料 '+str(abnormal.get('path',''))+'：'+str(abnormal.get('query_error') or abnormal.get('parse_error') or abnormal.get('ragflow_reason') or processing_state(abnormal))
    if item.get('handoff_status')=='pending' and state['upload']=='已上传' and state['reason']:
        state['assets']+='；交接尚未完成'
    return state
