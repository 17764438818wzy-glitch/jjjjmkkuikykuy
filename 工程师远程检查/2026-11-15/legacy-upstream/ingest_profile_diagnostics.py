"""Interpret only a source-bound, executed professional review refusal."""
import json,re
from ingest_execution import current_execution
from research_profile import checked_envelope


def diagnostic(task,remote,log):
    try:
        if not current_execution(task,remote) or remote.get('run')!='FAIL':return None
        required={'document_id':task['remote_id'],'document_name':task['remote_name'],'kb_id':task['dataset_id'],
            'pipeline_id':task.get('execution_pipeline_id') or task['pipeline_id'],'process_begin_at':remote['process_begin_at']}
        if not all(log.get(k)==v for k,v in required.items()) or str(log.get('operation_status'))!='4' or log.get('task_type')!='Parse':return None
        try:result=_diagnostic(task,remote,log)
        except (ValueError,TypeError,KeyError,AttributeError,IndexError,OSError):result=None
        if result:return result
        dsl=log.get('dsl') or {}
        for ident in reversed(dsl.get('path') or []):
            error=dsl.get('components',{}).get(ident,{}).get('obj',{}).get('params',{}).get('outputs',{}).get('_ERROR',{}).get('value')
            if error:
                return {'source':'execution_node','component_id':ident,'dependency':'pipeline','kind':'node_error',
                    'outcome':'technical_review','reason':'画像接收节点 '+ident+' 执行失败，保留技术待查',
                    'log_id':log['id'],'remote_id':task['remote_id'],'pipeline_id':required['pipeline_id'],
                    'process_begin_at':remote['process_begin_at'],'attempt':task.get('execution_attempt',0)}
        return None
    except (ValueError,TypeError,KeyError,AttributeError,IndexError,OSError):return None


def _diagnostic(task,remote,log):
    if not current_execution(task,remote) or remote.get('run')!='FAIL':return None
    required={'document_id':task['remote_id'],'document_name':task['remote_name'],'kb_id':task['dataset_id'],
        'pipeline_id':task.get('execution_pipeline_id') or task['pipeline_id'],'process_begin_at':remote['process_begin_at']}
    if not all(log.get(k)==v for k,v in required.items()) or str(log.get('operation_status'))!='4' or log.get('task_type')!='Parse':return None
    dsl=log.get('dsl') or {};components=dsl.get('components',{})
    node=components.get('CodeExec:ProfileGate',{}).get('obj',{}).get('params',{})
    error=node.get('outputs',{}).get('_ERROR',{}).get('value','')
    match=re.fullmatch(r'RuntimeError: RESEARCH_PROFILE_ROUTE_HOLD\|(.*)',error.rstrip().splitlines()[-1] if error else '')
    if not match or 'CodeExec:ProfileGate' not in (dsl.get('path') or []):return None
    event=json.loads(match.group(1))
    if task.get('policy_key')=='research_material':
        from research_material import checked_material
        packet=checked_material(task['package_path'])
    else:packet=checked_envelope(task['package_path'])
    name=re.match(r'^RP_([^_]+)__',task['remote_name'])
    if not name or not isinstance(event,dict):return None
    code=name.group(1)
    if event.get('target_code')!=code or event.get('profile_revision')!=packet['profile_revision'] or event.get('source_sha256')!=packet['source_text_sha256']:return None
    rows=event.get('review',{}).get('routes',[])
    if len(rows)!=1 or rows[0].get('code')!=code or rows[0].get('status') not in ('insufficient','contradicted') or not rows[0].get('reason'):return None
    return {'source':'execution_node','component_id':'CodeExec:ProfileGate','dependency':'classification',
        'kind':'specialty_evidence_pending','outcome':'quality_hold','reason':'当前专业依据待核：'+rows[0]['reason'][:250],
        'log_id':log['id'],'remote_id':task['remote_id'],'pipeline_id':required['pipeline_id'],
        'process_begin_at':remote['process_begin_at'],'attempt':task.get('execution_attempt',0)}
