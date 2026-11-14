"""Replaceable display index: one source folder per row, no paper details/history."""
from datetime import datetime

STAGES=('preprocessing','llm','postprocessing','rag','nas','cleanup')
QUEUES=('waiting_preprocessing','preprocessing_in_progress','waiting_llm','llm_in_progress',
        'waiting_postprocessing','postprocessing_in_progress','waiting_rag_upload','rag_in_progress',
        'completed','waiting_nas_archive_delete','nas_in_progress','waiting_cleanup','cleanup_in_progress','failed','unresolved')
RESULTS=('passed','failed','unrecognized')
VIEW_FIELDS=('final','result','previous_result','processing_attempt_id','recorded_at','result_version','pending_request_count')


def compact_folder(folder):
    if folder.get('stage') not in QUEUES or not folder.get('ticket_id') or not folder.get('folder_id'):
        raise ValueError('Existing folder progress is not compatible with the display index')
    row={key:folder.get(key) for key in ('ticket_id','folder_id','stage','failed_stage','llm_body_running','llm_attachment_running')}
    row['stage_snapshots']={key:{field:view[field] for field in VIEW_FIELDS if field in view}
                            for key,view in folder.get('stage_snapshots',{}).items() if key in STAGES}
    for stage in STAGES:
        view=row['stage_snapshots'].get(stage)
        value=folder.get(stage+'_result')
        if stage in ('rag','nas','cleanup') and view:
            value=view.get('result') if view.get('final') else (view.get('previous_result') or {}).get('result',value)
        if value not in (None,*RESULTS):raise ValueError('Unknown folder result')
        row[stage+'_result']=value
    return row


def merge_folder(old,new):
    if (old['ticket_id'],old['folder_id'])!=(new['ticket_id'],new['folder_id']):
        raise ValueError('Original folder identity changed')
    result={**new,'stage_snapshots':dict(new['stage_snapshots'])}
    for stage in STAGES:
        before=old['stage_snapshots'].get(stage)
        after=new['stage_snapshots'].get(stage)
        if not before:continue
        older=bool(after and before.get('recorded_at') and after.get('recorded_at') and
                   datetime.fromisoformat(after['recorded_at'])<datetime.fromisoformat(before['recorded_at']))
        if not after or older:
            result['stage_snapshots'][stage]=before
            result[stage+'_result']=old[stage+'_result']
        elif (before.get('processing_attempt_id')==after.get('processing_attempt_id') and before.get('final')
              and not after.get('final') and not result[stage+'_result']
              and not (stage=='llm' and (after.get('pending_request_count') or 0)>0)):
            result[stage+'_result']=old[stage+'_result']
    return result


def aggregate(rows):
    counts=dict.fromkeys(QUEUES,0)
    results={stage:dict.fromkeys(RESULTS,0) for stage in STAGES}
    uncertain=dict.fromkeys(STAGES,False)
    failures=dict.fromkeys(STAGES,0)
    body=attachment=0
    body_unknown=attachment_unknown=False
    for row in rows:
        stage=row['stage'];counts[stage]+=1
        if stage=='failed' and row.get('failed_stage') in failures:failures[row['failed_stage']]+=1
        body+=row['llm_body_running'] is True;attachment+=row['llm_attachment_running'] is True
        if stage!='llm_in_progress' and (row['llm_body_running'] is True or row['llm_attachment_running'] is True):counts['llm_in_progress']+=1
        if stage in ('llm_in_progress','failed'):
            body_unknown|=row['llm_body_running'] is None;attachment_unknown|=row['llm_attachment_running'] is None
        for section in STAGES:
            value=row[section+'_result']
            if value:results[section][value]+=1
            view=row['stage_snapshots'].get(section)
            unknown=(not view or not view.get('final') or view.get('result') is None) if section in ('rag','nas','cleanup') else bool(view and not view.get('final') and view.get('pending_request_count')!=0)
            uncertain[section]|=unknown
    return {'folder_total':len(rows),'counts':counts,'results':results,'uncertain_results':uncertain,
            'failures':failures,'body':body,'attachment':attachment,'body_unknown':body_unknown,'attachment_unknown':attachment_unknown}
