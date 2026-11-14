"""一次性、可复核的候选验收状态迁移；不伪造模型完成轮次。"""
from pathlib import Path
from .package import load,save

MIGRATION='20261016-acceptance-business-stage-v1'
ACCEPTANCE_TASK='PW_b34d3bcfcf782a95a1170219'


def migrate_acceptance(config,store,task_id=ACCEPTANCE_TASK):
    task=store.get(task_id)
    if not task:raise ValueError('acceptance task missing')
    with store.db() as db:
        prior=db.execute('SELECT 1 FROM events WHERE task_id=? AND kind=? AND data LIKE ?',
                         (task_id,'migration','%'+MIGRATION+'%')).fetchone()
    if prior:return task
    completed=[]
    for p in (config.root/'private/attempts').glob('*.json'):
        record=load(p)
        if record.get('task_id')==task_id and record.get('finish_reason')=='completed':
            completed.append(dict(stage=record['stage'],session_id=record.get('session_id'),seconds=record.get('seconds')))
    fields=dict(original_failed_stage='round1')
    legacy=task.get('failed_stage') in (None,'repair') or task.get('error')=='unresolved model processing issues'
    if legacy:
        fields['failed_stage']='round1'
        if task['stage'] in ('repair','parked'):
            fields.update(stage='round2',error=None,error_kind=None,due=0,
                          draft_dir=str(config.root/'work'/task_id/'r005/round2'))
    # 若后续真实轮次已完成且遇到新的 publish/validate 故障，保留真实失败位置。
    result=store.update(task_id,expected_version=task['version'],**fields)
    store.event(task_id,'migration',dict(id=MIGRATION,from_stage=task['stage'],to_stage=result['stage'],
        legacy_failed_stage=task.get('failed_stage'),original_failed_stage='round1',
        current_business_stage=result.get('failed_stage'),reused_completed_native_attempts=completed))
    save(config.root/'acceptance-status.json',result)
    return result
