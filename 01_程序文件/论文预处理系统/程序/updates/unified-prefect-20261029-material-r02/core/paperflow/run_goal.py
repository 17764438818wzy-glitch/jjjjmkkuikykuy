"""An explicit run goal observed by the existing workbench loop, without RPC."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time


MAX_SELECTION_SECONDS=30*86400
POLL_SECONDS=60
FIRST_REPORT_SECONDS=1800
REPORT_SECONDS=7200


class GoalIdentityError(ValueError):
    pass

# This is the expected-target availability projection from article_status, with
# explicit current identity/generation and safety checks. Closeout is separate.
ACTIVE_SQL='''SELECT DISTINCT t.task_id FROM tasks t JOIN sources s ON s.task_id=t.task_id
    WHERE s.source_id=json_extract(t.data,'$.source_id')
    AND COALESCE(json_extract(t.data,'$.disposition'),'')!='exclude'
    AND EXISTS(SELECT 1 FROM target_versions v WHERE v.task_id=t.task_id)
    AND NOT EXISTS(
      SELECT 1 FROM target_versions v
      LEFT JOIN specialty_tasks st ON st.manifest_sha256=v.manifest_sha256 AND st.specialty_code=v.specialty_code
      LEFT JOIN delivery_packages p ON p.manifest_sha256=v.manifest_sha256
      LEFT JOIN specialty_active a ON a.source_id=s.source_id AND a.specialty_code=v.specialty_code
      WHERE v.task_id=t.task_id AND (
        st.status IS NOT 'pending' OR st.mapping_issue IS NOT NULL OR p.lifecycle IS NOT 'retained'
        OR p.task_id IS NOT t.task_id OR a.manifest_sha256 IS NOT v.manifest_sha256
        OR COALESCE(json_extract(a.data,'$.state'),'active')!='active'
        OR json_extract(a.data,'$.identity.task_id') IS NOT t.task_id
        OR json_extract(a.data,'$.identity.source_id') IS NOT s.source_id
        OR json_extract(a.data,'$.identity.source_sha256') IS NOT t.source_sha256
        OR json_extract(a.data,'$.identity.manifest_sha256') IS NOT v.manifest_sha256
        OR json_extract(a.data,'$.identity.specialty_code') IS NOT v.specialty_code
        OR json_extract(a.data,'$.identity.generation') IS NOT v.generation
        OR json_extract(a.data,'$.identity.mapping_version') IS NOT st.mapping_version
        OR EXISTS(SELECT 1 FROM json_each(p.identity_json) i
            WHERE json_extract(a.data,'$.identity.'||i.key) IS NOT i.value)
        OR EXISTS(SELECT 1 FROM package_freezes f WHERE f.manifest_sha256=v.manifest_sha256
            AND f.specialty_code IN ('*',v.specialty_code))
        OR EXISTS(SELECT 1 FROM legacy_pending_scopes l WHERE l.source_id=s.source_id AND l.specialty_code=v.specialty_code)
        OR EXISTS(SELECT 1 FROM legacy_targets l WHERE l.manifest_sha256=v.manifest_sha256
            AND l.specialty_code=v.specialty_code AND l.state!='active')
        OR EXISTS(SELECT 1 FROM specialty_switches sw WHERE sw.source_id=s.source_id AND sw.specialty_code=v.specialty_code
            AND sw.phase NOT IN ('intent','enabled','committed','withdrawn'))
        OR EXISTS(SELECT 1 FROM specialty_safety f WHERE f.source_id=s.source_id AND f.specialty_code=v.specialty_code
            AND (f.state='pending' OR f.manifest_sha256=v.manifest_sha256))
      ))'''


@contextmanager
def readonly_database(path):
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=.1)
    try:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        yield db
    finally:db.close()


def active_task_ids(database):
    """Return distinct current papers whose every expected specialty is active."""
    with readonly_database(database) as db:return {row[0] for row in db.execute(ACTIVE_SQL)}


def held_summary(db,has_jobs):
    rows=db.execute('''SELECT DISTINCT t.task_id,json_extract(t.data,'$.hold_reason') FROM tasks t
        JOIN sources s ON s.task_id=t.task_id WHERE t.stage IN ('held','parked','code_error')''').fetchall()
    held={key:reason for key,reason in rows}
    if has_jobs:
        rows=db.execute('''SELECT DISTINCT t.task_id,j.state,
            (SELECT json_extract(a.data,'$.reason') FROM scheduler_attempts a WHERE a.job_id=j.job_id ORDER BY a.started DESC LIMIT 1)
            FROM tasks t JOIN sources s ON s.task_id=t.task_id JOIN scheduler_jobs j
            ON ((j.kind IN ('graph','repair') AND j.unit_key=t.task_id) OR
                (j.kind='delivery' AND EXISTS(SELECT 1 FROM target_versions v WHERE v.task_id=t.task_id
                    AND v.manifest_sha256=json_extract(j.data,'$.manifest_sha256')
                    AND v.specialty_code=json_extract(j.data,'$.specialty_code')
                    AND v.generation=json_extract(j.data,'$.generation'))))
            WHERE j.state IN ('held','held_budget','code_error')''').fetchall()
        for key,state,reason in rows:held.setdefault(key,reason or ('infrastructure_budget' if state=='held_budget' else None))
    from .safe_error_messages import SAFE_MESSAGES
    known={'classification_pending','embedding_structure_pending','embedding_correction_budget_exhausted','infrastructure_budget'}
    reasons={}
    for reason in held.values():
        safe=reason if isinstance(reason,str) and (reason in known or reason in SAFE_MESSAGES) else '未知'
        reasons[safe]=reasons.get(safe,0)+1
    return len(held),reasons


def compact_slots(value):
    result={}
    for name in ('article_slots','llm_permits'):
        item=(value or {}).get(name,{})
        window=item.get('window_seconds');occupied=item.get('occupied_slot_seconds')
        empty=item.get('empty_reason_slot_seconds') or {}
        result[name]=dict(full_rate_all=item.get('full_rate_all'),full_rate_eligible=item.get('full_rate_eligible'),
            average_used=occupied/window if isinstance(window,(int,float)) and window>0 and isinstance(occupied,(int,float)) else None,
            current_used=item.get('current_used',item.get('occupied_now')),
            main_empty_reasons=dict(sorted(empty.items(),key=lambda pair:pair[1],reverse=True)[:3]),
            metrics_complete=item.get('metrics_complete'))
    return result


class RunGoal:
    def __init__(self,path,root,*,clock=time.time):
        self.path=Path(path).resolve();self.clock=clock
        if not self.path.is_relative_to(Path(root).resolve()):raise ValueError('run goal outside runtime')
        raw=self.path.read_bytes()
        if len(raw)>16*1024**2:raise ValueError('run goal too large')
        try:spec=json.loads(raw)
        except (ValueError,UnicodeError) as exc:raise ValueError('invalid run goal') from exc
        if (not isinstance(spec,dict) or spec.get('schema')!='paperflow-run-goal/1'
                or type(spec.get('target_new_active')) is not int or spec['target_new_active']<1
                or type(spec.get('started_at')) not in (int,float) or not math.isfinite(spec['started_at'])
                or spec['started_at']<=0 or spec['started_at']>clock()+60
                or not isinstance(spec.get('baseline_active_task_ids'),list)
                or any(not isinstance(key,str) or not key or len(key)>160 for key in spec['baseline_active_task_ids'])
                or len(set(spec['baseline_active_task_ids']))!=len(spec['baseline_active_task_ids'])
                or not isinstance(spec.get('progress_path'),str) or not Path(spec['progress_path']).is_absolute()):
            raise ValueError('invalid run goal')
        self.progress=Path(spec['progress_path']).resolve()
        if self.progress==self.path or self.progress.suffix.lower()!='.txt':raise ValueError('invalid run goal progress path')
        self.sha256=hashlib.sha256(raw).hexdigest();self.baseline=frozenset(spec['baseline_active_task_ids'])
        self.target=spec['target_new_active'];self.started_at=spec['started_at']
        self.next_report=self.started_at+FIRST_REPORT_SECONDS;self.last_polled=None;self.reported_reached=False
        self.pending_report=None;self.next_report_attempt=0
        self.window_started=self.started_at;self.window_ids=set();self.new_ids=set()
        self.lock=threading.Lock()
        self.value=dict(state='waiting',goal_sha256=self.sha256,started_at=self.started_at,
            baseline_active=len(self.baseline),target_new_active=self.target,new_active=None,active_total=None,
            remaining=None,observed_at=None,queue_states=None)
        self.validate()

    def validate(self):
        try:raw=self.path.read_bytes()
        except OSError as exc:raise GoalIdentityError('run goal identity unavailable') from exc
        if hashlib.sha256(raw).hexdigest()!=self.sha256:raise GoalIdentityError('run goal changed')
        if self.clock()>=self.started_at+MAX_SELECTION_SECONDS:raise GoalIdentityError('run goal expired')

    def deadline(self,seconds):
        self.validate()
        return min(self.clock()+seconds,self.started_at+MAX_SELECTION_SECONDS)

    def snapshot(self):
        with self.lock:return dict(self.value)

    def _read(self,database):
        self.validate()
        with readonly_database(database) as db:
            active={row[0] for row in db.execute(ACTIVE_SQL)}
            has_jobs=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduler_jobs'").fetchone()
            jobs={row[0]:row[1] for row in db.execute('SELECT state,COUNT(*) FROM scheduler_jobs GROUP BY state')} if has_jobs else None
            held,reasons=held_summary(db,has_jobs)
        self.new_ids=active-self.baseline;count=len(self.new_ids);now=self.clock()
        elapsed=now-self.started_at;window=now-self.window_started
        return dict(self.value,state='reached' if count>=self.target else 'running',new_active=count,
            active_total=len(active),remaining=max(0,self.target-count),observed_at=now,queue_states=jobs,
            held_articles=held,held_reasons=reasons,statistics_error=None,
            cumulative_per_hour=count*3600/elapsed if elapsed>0 else None,
            window_new_active=len(self.new_ids-self.window_ids),
            window_per_hour=len(self.new_ids-self.window_ids)*3600/window if window>0 else None)

    def check_resume(self,database):
        """Called before the existing Manager resume gate; never opens admission."""
        with self.lock:
            self.value=self._read(database)
            if self.value['state']=='reached':raise ValueError('run goal already reached')

    def _append(self,event,manager,pressure,slots):
        value=self.value
        def shown(item):return '未知' if item is None else json.dumps(item,ensure_ascii=False,separators=(',',':'))
        slot_value=compact_slots(slots() if callable(slots) else slots)
        moment=datetime.fromtimestamp(self.clock(),timezone.utc).isoformat()
        status=manager.status()
        pressure=pressure or {}
        resources=status.get('resources') or {}
        slot_value['llm_permits']['current_used']=(resources.get('llm') or {}).get('used',slot_value['llm_permits']['current_used'])
        record=(f"\n[{moment}] {event}\n"
            f"恢复前基线={len(self.baseline)}；新增全专业 active={shown(value['new_active'])}/{self.target}；"
            f"当前全专业 active={shown(value['active_total'])}；剩余={shown(value['remaining'])}。\n"
            f"当窗新增={shown(value.get('window_new_active'))}；当窗新增/小时={shown(value.get('window_per_hour'))}；"
            f"累计新增/小时={shown(value.get('cumulative_per_hour'))}；统计采样={shown(value.get('observed_at'))}。\n"
            f"目标状态={value['state']}；管理状态={shown(status.get('state'))}；在途工作单元={shown(status.get('live_units'))}；"
            f"pending_targets={shown(pressure.get('pending_targets'))}；held文章={shown(value.get('held_articles'))}；"
            f"held原因={shown(value.get('held_reasons'))}；阻塞原因={shown(pressure.get('reasons'))}。\n"
            f"64槽全满比例、平均/当前占用及主要空槽原因={shown(slot_value)}。\n"
            "完整调用成本、token、各段耗时继续引用 B3 私有 timing 文件和现有台账；本简报未重采样的值为未知。\n")
        with self.progress.open('a',encoding='utf-8') as stream:stream.write(record);stream.flush()
        if event!='启动基线':self.window_started=self.clock();self.window_ids=set(self.new_ids)

    def _report(self,event,manager,pressure,slots,now):
        try:self._append(event,manager,pressure,slots)
        except (OSError,TypeError,ValueError):
            self.pending_report=event;self.next_report_attempt=now+POLL_SECONDS
            self.value['progress_error']='write_unavailable';return False
        self.pending_report=None;self.value['progress_error']=None
        if event=='目标达到':self.reported_reached=True
        if event=='运行观察':self.next_report+=((now-self.next_report)//REPORT_SECONDS+1)*REPORT_SECONDS
        return True

    def on_resume(self,database,manager,*,pressure=None,slots=None):
        self.poll(database,manager,pressure=pressure,slots=slots,force=True,event='启动基线')

    def poll(self,database,manager,*,pressure=None,slots=None,force=False,event=None):
        """One minute read sampling; normal pause is never undone by a goal."""
        with self.lock:
            now=self.clock()
            if manager.stopped.is_set() and not force:
                if self.pending_report and now>=self.next_report_attempt:self._report(self.pending_report,manager,pressure,slots,now)
                return dict(self.value)
            try:
                if force or self.last_polled is None or now-self.last_polled>=POLL_SECONDS:
                    self.last_polled=now
                    self.value=self._read(database)
            except GoalIdentityError:
                self.value.update(state='unavailable',new_active=None,active_total=None,remaining=None,
                    observed_at=now,queue_states=None,statistics_error='goal_identity')
                if not manager.stopped.is_set():manager.pause('run goal identity unavailable')
                return dict(self.value)
            except (OSError,ValueError,TypeError,sqlite3.Error):
                self.value.update(state='stale',statistics_error='read_unavailable')
            if self.value['state']=='reached':
                if not manager.stopped.is_set():manager.pause('run goal reached')
                if not self.reported_reached and now>=self.next_report_attempt:self._report('目标达到',manager,pressure,slots,now)
            elif event:self._report(event,manager,pressure,slots,now)
            elif self.pending_report and now>=self.next_report_attempt:self._report(self.pending_report,manager,pressure,slots,now)
            elif now>=self.next_report and now>=self.next_report_attempt:self._report('运行观察',manager,pressure,slots,now)
            return dict(self.value)
