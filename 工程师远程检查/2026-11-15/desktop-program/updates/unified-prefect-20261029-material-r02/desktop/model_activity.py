"""Passive request census across top-level batches; never runs or retries work."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import tempfile
import time


class ModelActivityReadError(ValueError):
    pass


class ModelActivityBoundaryError(ModelActivityReadError):
    pass


def run_owner_state(root, run_id):
    """Prove ownership or death; unreadable identity never permits a retry."""
    import psutil
    if not isinstance(run_id,str) or not run_id or Path(run_id).name!=run_id or any(c in run_id for c in '/\\:'):
        return 'unknown'
    try:
        owner=json.loads((Path(root)/'private/model_activity/owners'/(run_id+'.json')).read_text(encoding='utf8'))
        if (owner.get('schema')!='model-activity-owner/1' or owner.get('run_id')!=run_id
                or type(owner.get('pid')) is not int or owner['pid']<=0
                or type(owner.get('create_time')) not in (int,float)
                or not math.isfinite(owner['create_time']) or owner['create_time']<=0):
            return 'unknown'
        process=psutil.Process(owner['pid'])
        return 'alive' if process.is_running() and abs(process.create_time()-owner['create_time'])<.001 else 'dead'
    except psutil.NoSuchProcess:
        return 'dead'
    except (psutil.Error,OSError,ValueError,TypeError,AttributeError):
        return 'unknown'


def process_is_same(pid, created):
    import psutil
    try:
        process = psutil.Process(pid)
        return process.is_running() and abs(process.create_time() - created) < .001
    except (psutil.Error, OSError, ValueError):
        return False


def record_run_owner(root, run_id, *, pid=None):
    """Best-effort display metadata, independent of every processing decision."""
    temporary = None
    try:
        import psutil
        if not isinstance(run_id, str) or not run_id or Path(run_id).name != run_id or any(c in run_id for c in '/\\:'):
            return False
        process = psutil.Process(os.getpid() if pid is None else pid)
        path = Path(root)/'private/model_activity/owners'/(run_id+'.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        value = dict(schema='model-activity-owner/1', run_id=run_id,
                     pid=process.pid, create_time=process.create_time())
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.owner-', suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(value).encode('utf8'))
        temporary.replace(path)
        return True
    except Exception:
        # A display metadata problem must never stop document processing.
        return False
    finally:
        if temporary is not None:
            try:temporary.unlink(missing_ok=True)
            except OSError:pass


class ModelActivityReader:
    def __init__(self, root, *, process_is_same=process_is_same):
        self.root = Path(root).resolve()
        self.process_is_same = process_is_same
        self._manifests = {}
        self._journal_cursor=0;self._journal_pending={};self._journal_identity=None
        self._journal_head=b''

    def _inside(self, path):
        path = Path(path).resolve()
        if not path.is_relative_to(self.root):
            raise ModelActivityBoundaryError('统计路径超出本程序资料目录')
        return path

    def _json(self, path, *, max_bytes=16*1024*1024):
        path = self._inside(path)
        try:
            before = path.stat()
            if before.st_size > max_bytes:
                raise ModelActivityReadError('统计记录过大')
            with path.open('rb') as stream:
                content = stream.read(max_bytes+1)
            if len(content) > max_bytes:
                raise ModelActivityReadError('统计记录过大')
            value = json.loads(content.decode('utf-8-sig'))
            after = path.stat()
            if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size) or not isinstance(value, dict):
                raise ModelActivityReadError('统计记录正在更新')
            return value
        except (OSError, UnicodeError, ValueError) as exc:
            raise ModelActivityReadError('统计记录暂不可读') from exc

    def _manifest(self, path):
        try:
            stat = path.stat();stamp = (stat.st_mtime_ns, stat.st_size)
        except OSError as exc:
            raise ModelActivityReadError('批次记录暂不可读') from exc
        prior = self._manifests.get(path)
        if prior is None or prior[0] != stamp:
            # Whole-batch inventories can exceed the small request-state limit.
            # Keep this read bounded and cache only the display projection.
            value = self._json(path, max_bytes=128*1024*1024)
            # Never retain source names, prompts or the large input inventory.
            projected = dict(run_id=value.get('run_id'),status=value.get('status'),
                source_count=value.get('source_count'),processed_sources=value.get('processed_sources'),
                item_counts={status:sum(i.get('status')==status for i in value.get('items',[]))
                             for status in ('pass','reject','pending','associated','deleted')},
                billing_run_ids=value.get('billing_run_ids',[]),
                child_runs=[dict(run_id=c.get('run_id'),status=c.get('status')) for c in value.get('child_runs',[])],
                concurrency=value.get('concurrency',{}),
                active_items=[{key:item.get(key) for key in ('preextract_work_dir','prepared_checkpoint')}
                              for item in value.get('active_items',[]) if isinstance(item,dict)])
            prior = (stamp, projected)
            self._manifests[path] = prior
        return prior[1]

    def _journal(self):
        """Native JSONL parser, incremental and read-only; no SQLite side effects."""
        from billing import Ledger
        path=self._inside(self.root/'ai_usage.jsonl')
        try:
            with path.open('rb') as stream:
                stat=os.fstat(stream.fileno());identity=(stat.st_dev,stat.st_ino)
                head=stream.read(len(self._journal_head))
                if (identity!=self._journal_identity or stat.st_size<self._journal_cursor or head!=self._journal_head):
                    self._journal_cursor=0;self._journal_pending={}
                self._journal_identity=identity
                stream.seek(self._journal_cursor);start=self._journal_cursor
                for row,offset in Ledger._journal_rows(stream):
                    if row.get('outcome')=='awaiting_response':
                        self._journal_pending[row['id']]={key:row.get(key) for key in ('run_id','task','item_id')}
                    else:self._journal_pending.pop(row['id'],None)
                    if offset-start>=32*1024*1024:break
                self._journal_cursor=stream.tell()
                stream.seek(0);self._journal_head=stream.read(min(self._journal_cursor,512))
                partial=self._journal_cursor<stat.st_size
            return list(self._journal_pending.values()),partial
        except OSError as exc:raise ModelActivityReadError('请求回执暂不可读') from exc

    def _request_lanes(self,result,owners,current_run_id,manifests):
        rows,partial=self._journal();aliases={};counts={}
        for rid,manifest in manifests.items():
            if not owners.get(rid):continue
            closed={c['run_id'] for c in manifest.get('child_runs',[]) if c.get('status')!='running'}
            for alias in [rid,*manifest.get('billing_run_ids',[])]:
                if alias in closed:continue
                aliases.setdefault(alias,[]).append(rid)
            counts[rid]=dict(paper=0,purpose=0,other=0)
        unknown=0
        for row in rows:
            live=aliases.get(row.get('run_id'),[])
            if not live:
                unknown+=1;continue
            rid=current_run_id if current_run_id in live else live[0]
            lane={'paper_preextract':'paper','material_purpose_review':'purpose'}.get(row.get('task'),'other')
            counts[rid][lane]+=1
        total=sum(sum(v.values()) for v in counts.values())
        current=counts.get(current_run_id,dict(paper=0,purpose=0,other=0))
        concurrency=dict(manifests.get(current_run_id,{}).get('concurrency',result.get('pipeline_counts',{})))
        if result.get('current_status') not in (None,'running'):
            for key in ('preparation_active','preparation_waiting','conversion_waiting','postprocess_active','postprocess_waiting','body_candidates'):
                if key in concurrency:concurrency[key]=0
        independent=bool(concurrency.get('request_lanes'))
        current_total=sum(current.values())
        if current_run_id in manifests and not owners.get(current_run_id):
            result.update(current_complete=False,partial=True)
        result.update(total=total,current=current_total,background=total-current_total,
            paper_current=current['paper'],purpose_current=current['purpose'],other_current=current['other'],
            paper_total=sum(v['paper'] for v in counts.values()),purpose_total=sum(v['purpose'] for v in counts.values()),
            paper_limit=concurrency.get('papers',50),purpose_limit=concurrency.get('purpose_llm_limit') if independent else None,
            legacy_shared_current=bool(concurrency and not independent),pipeline_counts=concurrency,
            unconfirmed=max(result['unconfirmed'],unknown),partial=bool(result['partial'] or partial),
            journal_complete=not partial,
            batches=[dict(run_id=rid,active=sum(counts[rid].values()),owner_alive=True,**counts[rid]) for rid in sorted(counts)])
        return result

    def read(self, current_run_id=None):
        requests = {}
        owners = {}
        unreadable=set()
        incomplete_runs=set()
        seen_runs=set()
        manifests={}
        current_manifest=None
        def unavailable(path,rid):
            unreadable.add(os.path.normcase(str(path)))
            incomplete_runs.add(rid)
        for path in sorted((self.root/'runs').glob('*/manifest.json')):
            rid=path.parent.name
            try:manifest = self._manifest(path)
            except ModelActivityBoundaryError:raise
            except ModelActivityReadError:
                unavailable(path,rid);continue
            if manifest['run_id'] != rid:
                unavailable(path,rid);continue
            seen_runs.add(rid)
            if rid==current_run_id:current_manifest=manifest
            if manifest['status'] != 'running':
                continue
            manifests[rid]=manifest
            owner_path = self.root/'private/model_activity/owners'/(rid+'.json')
            try:owner = self._json(owner_path) if owner_path.exists() else {}
            except ModelActivityBoundaryError:raise
            except ModelActivityReadError:
                unavailable(owner_path,rid);owner={}
            bound = (owner.get('schema') == 'model-activity-owner/1' and owner.get('run_id') == rid
                     and type(owner.get('pid')) is int and isinstance(owner.get('create_time'), (int,float)))
            owners[rid] = bool(bound and self.process_is_same(owner['pid'],owner['create_time']))
            for item in manifest['active_items']:
                reference = item.get('prepared_checkpoint') or {}
                work = item.get('preextract_work_dir') or (str(Path(reference['path']).parent) if reference.get('path') else None)
                if not work:
                    continue
                work = self._inside(work)
                try:state_paths=list((work/'model').rglob('request-state.json'))
                except OSError:
                    unavailable(work,rid);continue
                for state_path in state_paths:
                    state_path = self._inside(state_path)
                    try:state = self._json(state_path)
                    except ModelActivityBoundaryError:raise
                    except ModelActivityReadError:
                        unavailable(state_path,rid);continue
                    if (state.get('status') != 'request_in_flight' or state.get('finished_at')
                            or not isinstance(state.get('client_http_requests'),int) or state['client_http_requests'] < 1):
                        continue
                    key = os.path.normcase(str(state_path))
                    prior = requests.setdefault(key,dict(runs=set(), sent=bool(state.get('http_started_at'))))
                    prior['runs'].add(rid)
        counts = {rid:0 for rid in owners}
        unknown = waiting = 0
        for request in requests.values():
            incomplete_runs.update(rid for rid in request['runs'] if not owners.get(rid))
            live = sorted(rid for rid in request['runs'] if owners.get(rid))
            if not live:
                unknown += 1
            elif not request['sent']:
                waiting += 1
            else:
                rid = current_run_id if current_run_id in live else live[0]
                counts[rid] += 1
        total = sum(counts.values());current = counts.get(current_run_id,0)
        if current_run_id and current_run_id not in seen_runs:incomplete_runs.add(current_run_id)
        result=dict(total=total,current=current,background=total-current,unconfirmed=unknown,
                    waiting_to_send=waiting,updated_at=time.time(),
                    partial=bool(unreadable or incomplete_runs),unreadable_records=len(unreadable),
                    current_complete=current_run_id not in incomplete_runs,
                    background_complete=not bool(incomplete_runs-{current_run_id}),
                    batches=[dict(run_id=rid,active=counts[rid],owner_alive=owners[rid]) for rid in sorted(owners)])
        if current_manifest is not None:
            result.update(current_status=current_manifest.get('status'),pipeline_counts=current_manifest.get('concurrency',{}),
                source_count=current_manifest.get('source_count'),processed_sources=current_manifest.get('processed_sources'),
                item_counts=current_manifest.get('item_counts',{}))
        if (self.root/'ai_usage.jsonl').exists():
            return self._request_lanes(result,owners,current_run_id,manifests)
        return result
