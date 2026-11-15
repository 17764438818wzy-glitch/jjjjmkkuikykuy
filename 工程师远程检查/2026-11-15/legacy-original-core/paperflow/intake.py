"""持续发现原稿及旧交付；不按文件名评分决定材料价值。"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import time

from .package import load, sha
from .config import rules_fingerprint
from .attachments import ATTACHMENT_EXTENSIONS


EXTENSIONS = {'.docx','.doc','.pdf','.wps','.rtf','.odt','.docm','.zip','.rar','.7z','.txt','.md'} | ATTACHMENT_EXTENSIONS


class Intake:
    def __init__(self, config, store, *, registrar=None):
        self.config, self.store = config, store
        self.registrar = registrar or store.register_source
        self.observed = {}
        self.imported = {}

    def scan(self, now=None):
        now = time.time() if now is None else now
        stats = dict(seen=0, stable=0, errors=0)
        for root in self.config.source_roots:
            if not Path(root).is_dir():
                continue
            for folder, dirs, names in os.walk(root, followlinks=False):
                dirs[:] = [d for d in dirs if d not in ('.git','.venv','__pycache__') and not Path(folder,d).is_symlink()]
                for name in names:
                    path = Path(folder,name)
                    if path.suffix.lower() not in EXTENSIONS or name.startswith('~$') or path.is_symlink():
                        continue
                    try:
                        st=path.stat()
                        key=str(path.resolve())
                        fingerprint=(st.st_size,st.st_mtime_ns)
                        previous=self.observed.get(key)
                        stats['seen']+=1
                        if previous is None or previous[:2] != fingerprint:
                            self.observed[key]=(*fingerprint,now,None)
                            continue
                        if now-previous[2]<self.config.stable_seconds:
                            continue
                        if previous[3] is None:
                            digest=sha(path)
                            # 哈希期间仍在复制/写入的文件，下个周期重新等待。
                            after=path.stat()
                            if (after.st_size,after.st_mtime_ns)!=fingerprint:
                                self.observed.pop(key,None)
                                continue
                            self.registrar(path,digest)
                            self.observed[key]=(*fingerprint,previous[2],digest)
                            stats['stable']+=1
                    except (OSError,ValueError):
                        stats['errors']+=1
        return stats

    def import_handoffs(self):
        """旧成果只进入对账/第三轮，不因 READY 或旧成功字样直接宣告入库。"""
        root=Path(self.config.handoff_root)
        stats=dict(tasks=0,deliveries=0,problems=0,errors=0)
        fingerprint=rules_fingerprint(self.config)
        for task in self.store.all_tasks():
            if (task['stage']=='parked' and task.get('error_kind')=='content'
                    and task.get('rules_fingerprint') and task['rules_fingerprint']!=fingerprint):
                self.store.request(task['task_id'],stage=task.get('resume_stage') or 'repair',kind='repair',stagnant_count=0,
                                  rules_fingerprint=fingerprint,due=0)
        for file in sorted((root/'tasks').glob('*.json')):
            try:
                stamp=(file.stat().st_mtime_ns,file.stat().st_size)
                if self.imported.get(str(file))==stamp:
                    continue
                old=load(file)
                original=Path(old.get('input',{}).get('path',''))
                digest=old.get('source_sha256') or old.get('input',{}).get('sha256')
                if not original.is_file() or not digest:
                    continue
                task=self.store.register_source(original,digest)
                stats['tasks']+=1
                self.store.update(task['task_id'],legacy_task_id=old['task_id'],
                                  legacy_result_dir=old.get('output',{}).get('result_directory'))
                # 每次也检查交付目录，不能只依赖 task.json 的更新时间。
                self.imported[str(file)]=stamp
            except (OSError,ValueError,KeyError,TypeError):
                stats['errors']+=1
        # 对账只关注已经登记的任务；处理其他任务的活动租约时不篡改阶段。
        for task in self.store.all_tasks():
            if task.get('owner') or task['stage'] not in ('round1','parked','held','repair','complete'):
                continue
            legacy=task.get('legacy_task_id') or task['task_id']
            candidates=[]
            declared=Path(task.get('legacy_result_dir') or '__no_declared_delivery__')
            if (declared/'READY.json').is_file():
                number=int(re.sub(r'\D','',declared.name) or 0)
                candidates.append((number,(declared/'READY.json').stat().st_mtime_ns,declared))
            parents={root/'Updated manuscript'/legacy,root/'results'/legacy,
                     root/'Updated manuscript'/task['task_id'],root/'results'/task['task_id']}
            for parent in parents:
                for ready in parent.glob('*/READY.json'):
                    revision=ready.parent
                    try:
                        number=int(re.sub(r'\D','',revision.name) or 0)
                        candidates.append((number,ready.stat().st_mtime_ns,revision))
                    except OSError:
                        continue
            if candidates:
                number,_,delivery=max(candidates,key=lambda r:r[:2])
                try:
                    manifest=load(delivery/'manifest.json'); ready=load(delivery/'READY.json')
                    digest=sha(delivery/'manifest.json')
                    unchanged=(digest==task.get('manifest_sha256') and str(delivery)==task.get('package_dir')) or digest==task.get('adopted_manifest_sha256')
                    if not unchanged and task.get('adopted_revision_number',-1)>=number:
                        self.store.request(task['task_id'],stage='parked',error_kind='identity',error='published legacy revision mutated')
                        continue
                    if manifest.get('source_sha256')!=task['source_sha256'] or ready.get('manifest_sha256')!=digest:
                        self.store.request(task['task_id'],stage='parked',error_kind='identity',error='legacy manifest/source mismatch')
                        continue
                    if not unchanged:
                        receipt=root/'receipts'/legacy/(delivery.name+'-rag-ingestion.json')
                        complete_protocol=manifest.get('schema')=='paperflow/1.0' and (delivery/'chunks.json').is_file()
                        self.store.request(task['task_id'],stage='validate' if complete_protocol else ('reconcile' if receipt.is_file() else 'round3'),
                            draft_dir=str(delivery),delivery_task_id=manifest.get('task_id',legacy),
                            revision=max(number if complete_protocol else number+1,task['revision']),legacy_package_dir=str(delivery),
                            legacy_receipt=str(receipt) if receipt.is_file() else None,
                            adopted_manifest_sha256=digest,adopted_revision_number=number,kind='repair' if task['kind']=='repair' else 'new',
                            **(dict(package_dir=str(delivery),manifest_sha256=digest) if complete_protocol else {}))
                        stats['deliveries']+=1
                        continue
                except (OSError,ValueError,KeyError,TypeError):
                    stats['errors']+=1
            failures=[]
            for p in (root/'failed manuscript'/legacy).glob('*/failure.json'):
                try:
                    if not (p.parent/'RESOLVED.json').exists() and not (p.parent/'CODEX_DECISION.json').exists():
                        if task.get('failure_packet') and p.parent.resolve()==Path(task['failure_packet']).resolve():
                            continue
                        # 原生失败由图自身管理，不作为外部返修再次登记。
                        details=load(p)
                        if details.get('origin')=='paperflow':
                            continue
                        failures.append((p.stat().st_mtime_ns,p,details))
                except (OSError,ValueError,KeyError,TypeError,AttributeError):
                    stats['errors']+=1
            if failures:
                stamp,problem,details=max(failures,key=lambda row:row[0])
                failure_stamp=str(problem)+'|'+str(stamp)
                if failure_stamp==task.get('imported_failure'):
                    continue
                # 被明确撤销的语义审查不恢复为待修任务。
                if 'withdrawn' in str(details.get('status','')).lower():
                    continue
                self.store.request(task['task_id'],stage='repair',kind='repair',failure_packet=str(problem.parent),
                                  revision=task['revision']+1,repair_input_dir=str(problem.parent/'artifacts'),
                                  imported_failure=failure_stamp)
                stats['problems']+=1
        return stats
