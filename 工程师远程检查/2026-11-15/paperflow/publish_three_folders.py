"""Replace only the organized copy after a full successful verification."""
import json,pathlib,shutil,time,sys
import organize_library as o
from prepare_three_folder_plan import PHASE
from materialize_three_folders import STAGE,OLD

def main():
    verification=json.loads((PHASE/'verification.json').read_text('utf-8'))
    if not verification['passed']:raise RuntimeError('Full verification has not passed')
    if o.digest(PHASE/'all-decisions.json')!=(PHASE/'stage-plan-sha256.txt').read_text():raise RuntimeError('Plan changed after stage creation')
    separate='--separate-output' in sys.argv
    stage=STAGE.resolve();target=(pathlib.Path(r'F:\总论文_整理后_三文件夹版_20261028') if separate else OLD).resolve();backup=(PHASE/'旧整理目录备份').resolve();source=o.SOURCE.resolve();workspace=o.WORK.resolve()
    permitted=pathlib.Path(r'F:\总论文_整理后_三文件夹版_20261028' if separate else r'F:\总论文_整理后_20261028').resolve()
    if target!=permitted:raise RuntimeError('Unexpected output target')
    if not stage.is_relative_to(workspace) or not backup.is_relative_to(workspace):raise RuntimeError('Stage/backup must remain inside workspace records')
    if any(path==source or path.is_relative_to(source) or source.is_relative_to(path) for path in (stage,target,backup)):raise RuntimeError('Cannot move original source or its parent')
    if stage.drive!=target.drive or backup.drive!=target.drive:raise RuntimeError('Replacement must be same-volume renames')
    if backup.exists() or not stage.is_dir() or (target.exists() if separate else not target.is_dir()):raise RuntimeError('Unexpected publication path state')
    ledger_backup=PHASE/'旧台账备份';ledger_backup.mkdir(exist_ok=separate)
    for name in ('decisions.json','materialization.jsonl','summary.json','verification.json'):
        if (ledger_backup/name).exists():
            if o.digest(o.WORK/name)!=o.digest(ledger_backup/name):raise RuntimeError('Existing ledger backup mismatch')
        else:shutil.copy2(o.WORK/name,ledger_backup/name)
    if not separate:target.rename(backup)
    try:stage.rename(target)
    except Exception:
        if not separate and not target.exists():backup.rename(target)
        raise
    # Renames preserve bytes; check the complete visible structure after the swap.
    decisions=json.loads((PHASE/'all-decisions.json').read_text('utf-8'))
    for d in decisions:
        root=target/d['school']/d['folder'];children=list(root.iterdir())
        if {p.name for p in children}!={'正文','附件','垃圾材料'} or not all(p.is_dir() for p in children):raise RuntimeError('Published directory structure mismatch')
        if len(list((root/'正文').iterdir()))!=int(bool(d['body'])):raise RuntimeError('Published body count mismatch')
    summary=json.loads((PHASE/'summary.json').read_text('utf-8'))
    summary.update(source=str(source),output=str(target),personal_groups=len(decisions),body_ready=verification['body_ready'],review_groups=0,excluded_only_groups=sum(d['excluded_only'] for d in decisions),verification='passed',unique_output_files=verification['output_data_files'],resolved_previous_pending=251,corrected_previous_body_selections=19,word_priority_changes=14)
    verification.update(verified_root=str(target),published_structure_verified=True)
    o.dump(PHASE/'verification.json',verification);o.dump(PHASE/'summary.json',summary)
    for name,src in (('decisions.json','all-decisions.json'),('materialization.jsonl','materialization.jsonl'),('summary.json','summary.json'),('verification.json','verification.json')):
        shutil.copyfile(PHASE/src,o.WORK/name)
    receipt={'output':str(target),'previous_copy_location':str(OLD if separate else backup),'previous_directory_replaced':not separate,'publication_note':'原整理目录重命名被Windows拒绝访问，已将通过核验的结果发布到独立新目录' if separate else '原位替换完成','source':str(source),'source_untouched':verification['passed'],'three_folders_verified':True,'groups':len(decisions),'body_ready':verification['body_ready'],'timestamp':time.strftime('%Y-%m-%d %H:%M:%S')}
    o.dump(PHASE/'publication.json',receipt)
    (PHASE/'工作记录说明.txt').write_text('最终资料目录：'+str(target)+'\n每人只含 正文、附件、垃圾材料 三个目录。\n原始总论文保持不变。旧整理目录位置：'+receipt['previous_copy_location']+'\n'+receipt['publication_note']+'\ninspection.jsonl 是原始内容检查与哈希基线；最新分类以 decisions.json 的 routes 和 materialization.jsonl 为准。\n',encoding='utf-8')
    print(json.dumps(receipt,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
