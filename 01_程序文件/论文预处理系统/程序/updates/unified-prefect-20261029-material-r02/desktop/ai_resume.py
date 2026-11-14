"""Private immutable native-preparation checkpoints; never upload approvals."""
import copy
import json
import shutil
from pathlib import Path

from engine import digest, write_json
from knowledge_package import safe_path
from processing_outcomes import ai_outcome
from archive_identity import member_fields


FORMAT = 'native-preparation/1'
NATIVE_RULES_VERSION = 4  # Rebuild older native layouts with current table-image and PDF rules.
RETRY_LABELS = {'不通过', '调用失败', '结果校验失败', '未完成'}


def _comparison_path(path):
    """Compare resolved Windows paths independently of the extended prefix."""
    value = str(Path(path).resolve())
    if value.startswith('\\\\?\\UNC\\'):
        value = '\\\\' + value[8:]
    elif value.startswith('\\\\?\\'):
        value = value[4:]
    return Path(value)


def needs_ai_resume(item):
    if item.get('status')=='deleted' or item.get('reason_kind')=='material_deleted':return False
    from preextract_contract import is_preextract_artifact
    if item.get('source_path') and is_preextract_artifact(item['source_path']):return False
    if ai_outcome(item,item.get('_ai_run_status'))[0] in RETRY_LABELS:return True
    run_status=item.get('_ai_run_status',item.get('_result_run_status'))
    if ((item.get('prepared_checkpoint') or {}).get('format')=='paper-preextract-prepared/1'
            and item.get('ai_result',{}).get('status')=='not_started'
            and (run_status in {'interrupted','stopped','failed'} or item.get('reason_kind') in {'stopped','technical','batch_paused'})):
        return True
    if (item.get('prepared_checkpoint') and item.get('ai_result',{}).get('status')=='response_saved'
            and not item.get('automatic_approval') and run_status in {'interrupted','stopped','failed'}):
        return True
    return bool(item.get('prepared_checkpoint') and item.get('ai_result',{}).get('status') in {'passed','response_saved'}
                and not item.get('automatic_approval') and item.get('reason_kind') in {'stopped','technical','batch_paused'})


def retry_records(items):
    return [{'path':item['source_path'], 'name':Path(item['source_path']).name,
             **member_fields(item),
             'prepared_checkpoint':copy.deepcopy(item.get('prepared_checkpoint')),
             'previous_ai_reason':ai_outcome(item,item.get('_ai_run_status'))[1]}
            for item in items if item.get('source_path') and needs_ai_resume(item)]


def save_prepared(work, original, package, item, preparation, version):
    from source_education import source_education_fields
    fields=source_education_fields(preparation['meta']['source_education']) if 'source_education' in preparation['meta'] else {}
    preparation['identity'].update(copy.deepcopy(fields));item.update(copy.deepcopy(fields))
    folder=Path(work)/'prepared'
    folder.mkdir()
    source=folder/('source'+Path(original).suffix)
    shutil.copy2(original,source)
    shutil.copytree(package,folder/'package')
    files={p.relative_to(folder).as_posix():digest(p) for p in folder.rglob('*') if p.is_file()}
    payload={'format':FORMAT,'version':version,'native_rules_version':NATIVE_RULES_VERSION,'source_hash':item['source_hash'],
             'source_name':source.name,'files':files,'preparation':preparation,**copy.deepcopy(fields)}
    path=folder/'checkpoint.json';write_json(path,payload)
    return {'path':str(path),'sha256':digest(path)}


def validate_prepared(record, root, version):
    checkpoint=record.get('prepared_checkpoint')
    if isinstance(checkpoint,dict) and checkpoint.get('format')=='paper-preextract-prepared/1':
        from preextract_workbench import validate_prepared_material
        return validate_prepared_material(record,root)
    if not isinstance(checkpoint,dict) or not checkpoint.get('path'):
        raise ValueError('缺少脚本中间结果，不能从 AI 阶段继续。旧批次需先在第一页重新生成一次脚本结果。')
    try:
        path=Path(checkpoint['path']).resolve()
        if not _comparison_path(path).is_relative_to(_comparison_path(Path(root)/'runs')) or 'private' not in path.parts:
            raise ValueError('中间结果路径无效')
        if digest(path)!=checkpoint.get('sha256'):raise ValueError('中间结果记录已改变')
        saved=json.loads(path.read_text(encoding='utf-8'))
        if saved.get('format')!=FORMAT or saved.get('version')!=version:
            raise ValueError('中间结果版本不兼容')
        if digest(Path(record['path']))!=saved['source_hash']:
            raise ValueError('原文件已修改，请先在第一页重新生成脚本结果；本次不会使用旧内容继续 AI')
        if not saved.get('files') or saved['source_name'] not in saved['files']:
            raise ValueError('脚本中间结果不完整')
        for name,expected in saved['files'].items():
            if digest(safe_path(path.parent,name))!=expected:raise ValueError('脚本中间文件已改变')
        actual={p.relative_to(path.parent).as_posix() for p in path.parent.rglob('*') if p.is_file()}
        if actual!=set(saved['files'])|{'checkpoint.json'}:raise ValueError('脚本中间文件清单已改变')
        return path.parent,saved
    except (OSError,KeyError,TypeError,UnicodeError) as exc:
        raise ValueError('原文件或脚本中间结果缺失、不可读取；请恢复文件后重试。') from exc


def restore_prepared(record, root, version, work):
    folder,saved=validate_prepared(record,root,version)
    original=Path(work)/saved['source_name'];package=Path(work)/'package'
    shutil.copy2(safe_path(folder,saved['source_name']),original)
    shutil.copytree(folder/'package',package)
    # Validate after copying as well, so concurrent edits cannot become trusted input.
    for name,expected in saved['files'].items():
        if digest(safe_path(work,name))!=expected:raise ValueError('复制中间结果时文件发生变化，请重试')
    prepared=copy.deepcopy(saved['preparation'])
    from source_education import extract_source_education, source_education_fields, EXTRACTION_RULE_VERSION
    previous=prepared['meta'];payload=previous.get('source_education')
    if (payload is None or payload.get('extraction_rule_version')!=EXTRACTION_RULE_VERSION):
        relation='original' if digest(original)==saved['source_hash'] else (
            'converted' if Path(record['path']).suffix.lower() in {'.doc','.docm','.rtf','.odt'} else 'normalized')
        payload=extract_source_education(original,original_source=record['path'],source_relation=relation,
            derivation_method='saved_native_preparation' if relation!='original' else None,
            derivation_record_id='checkpoint-'+record['prepared_checkpoint']['sha256'] if relation!='original' else None)
    if payload['original_source_sha256']!=saved['source_hash'] or payload['evidence_source_sha256']!=digest(original):
        raise ValueError('来源学历与原文件或取证文件校验值不一致')
    fields=source_education_fields(payload)
    changed=any(node.get(key)!=value for node in (saved,prepared['identity'],previous) for key,value in fields.items())
    previous.update(copy.deepcopy(fields));prepared['identity'].update(copy.deepcopy(fields))
    if changed:
        # Metadata-only migration of the private copied package; no native
        # figure extraction, no quality/body changes, no old checkpoint edits.
        from evidence import export_package
        export_package(package,previous)
        prepared['_source_education_refreshed']=True
    if saved.get('native_rules_version') != NATIVE_RULES_VERSION:
        # Old snapshots may already have discarded body chapters. Rebuild from
        # the immutable, hash-checked native source, never from truncated blocks.
        # Keep the copied old package privately; no original/checkpoint is edited.
        from evidence import build_package
        previous=prepared['meta']
        identity={key:copy.deepcopy(previous[key]) for key in
                  ('id','title','major','category','processing_version','original_source_sha256',
                   'source_education','source_education_revision')
                  if key in previous}
        package=Path(work)/'package_refreshed'
        prepared['meta']=build_package(original,package,identity,redact_content=False)
        prepared['_native_rebuilt']=True
    return original,package,prepared
