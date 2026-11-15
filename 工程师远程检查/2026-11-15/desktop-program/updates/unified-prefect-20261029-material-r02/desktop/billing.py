"""Local Agent usage ledger. Never stores prompts, images, keys or model answers."""
import json
import os
import hashlib
import sqlite3
import threading
import uuid
from contextlib import closing
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

CHECKED_ON='2026-09-15'
OFFICIAL={
    name:{'input':inp,'cached_input':cache,'output':out,'checked_on':CHECKED_ON,
          'source':'https://developers.openai.com/api/docs/models/'+name}
    for name,inp,cache,out in [
        ('gpt-5.6-sol','4','0.4','20'),('gpt-5.6-terra','2','0.2','12'),
        ('gpt-5.6-luna','0.2','0.02','1.2'),('gpt-6-astra','10','1','50'),
        ('gpt-5.5','5','0.5','30')]
}
LOCK=threading.RLock()


def exchange_rate(value):
    try:
        rate=Decimal(str(value))
        if not rate.is_finite() or not Decimal('0.01')<=rate<=1000: raise ValueError()
        return rate
    except (InvalidOperation,ValueError): raise ValueError('美元兑人民币汇率须为 0.01–1000 的有效数字') from None


def price_for(model,config):
    canonical='gpt-5.6-sol' if model=='gpt-5.6' else model
    custom=(config or {}).get('model_pricing') if isinstance(config,dict) else None
    if isinstance(custom,dict) and isinstance(custom.get(canonical),dict):
        entry=custom[canonical]
        if not all(isinstance(entry.get(k),str) for k in ('input','cached_input','output')): return None
        inp,cache,out=entry['input'],entry['cached_input'],entry['output']
        basis=entry.get('basis','user_provided');checked_on=entry.get('checked_on','未核对');source=entry.get('source','user configuration')
    elif canonical in OFFICIAL:
        entry=OFFICIAL[canonical];inp,cache,out=entry['input'],entry['cached_input'],entry['output']
        basis='official_standard';checked_on=entry['checked_on'];source=entry['source']
    else: return None
    return {'model':canonical,'input':inp,'cached_input':cache,'output':out,
            'cache_write':str(Decimal(inp)*Decimal('1.25')) if canonical!='gpt-5.5' else None,
            'currency':'USD','usd_cny':str(exchange_rate(config.get('usd_cny','7'))),
            'basis':basis,'checked_on':checked_on,'source':source,
            'long_threshold':272000,'long_input_factor':'2','long_output_factor':'1.5'}


def normalize_usage(raw):
    if not isinstance(raw,dict): return None
    inp=raw.get('prompt_tokens',raw.get('input_tokens'))
    out=raw.get('completion_tokens',raw.get('output_tokens'))
    if any(type(n) is not int or n<0 for n in [inp,out]): return None
    total=raw.get('total_tokens',inp+out)
    details=raw.get('prompt_tokens_details',raw.get('input_tokens_details')) or {}
    if not isinstance(details,dict): return None
    cached=details.get('cached_tokens',raw.get('prompt_cache_hit_tokens',0))
    writes=details.get('cache_creation_tokens',raw.get('cache_creation_input_tokens',0))
    if any(type(n) is not int or n<0 for n in [total,cached,writes]): return None
    if total!=inp+out or cached+writes>inp: return None
    return {'input_tokens':inp,'output_tokens':out,'total_tokens':total,
            'cached_tokens':cached,'cache_write_tokens':writes}


def estimate(usage,pricing):
    if usage is None or pricing is None: return None
    p=pricing;u=usage
    if u['cache_write_tokens'] and p['cache_write'] is None: return None
    long=u['input_tokens']>p['long_threshold']
    input_factor=Decimal(p['long_input_factor'] if long else '1')
    output_factor=Decimal(p['long_output_factor'] if long else '1')
    amount=((u['input_tokens']-u['cached_tokens']-u['cache_write_tokens'])*Decimal(p['input'])
            +u['cached_tokens']*Decimal(p['cached_input'])
            +u['cache_write_tokens']*Decimal(p['cache_write'] or '0'))*input_factor
    amount+=u['output_tokens']*Decimal(p['output'])*output_factor
    return str(amount*Decimal(p['usd_cny'])/1000000)


class Ledger:
    def __init__(self,root):
        self.path=Path(root).resolve()/'ai_usage.jsonl'
        self.index_path=self.path.parent/'private'/'ai_usage_index.sqlite3'

    def append(self,record):
        with LOCK:
            self.path.parent.mkdir(parents=True,exist_ok=True)
            with self.path.open('a',encoding='utf-8') as stream:
                # Leading newline isolates a partial final record after a crash.
                stream.write('\n'+json.dumps(record,ensure_ascii=False)+'\n')
                stream.flush();os.fsync(stream.fileno())

    def begin(self,model,role,task,attempt,config,context,channel='text'):
        row={'id':uuid.uuid4().hex,'time':datetime.now().isoformat(timespec='seconds'),
             'model':model,'role':role,'task':task,'attempt':attempt,
             'run_id':context.get('run_id',''),'item_id':context.get('item_id',''),
             'scope':context.get('scope','processing'),'pricing':price_for(model,config),
             'channel':channel,'usage':None,'tokens':None,'total_tokens':None,
             'ok':None,'error_type':None,'cost_cny':None,'outcome':'awaiting_response'}
        self.append(row)
        return row

    def finish(self,row,raw_usage,outcome,ok=None,error_type=None):
        usage=normalize_usage(raw_usage)
        good=(outcome=='response') if ok is None else bool(ok)
        total=usage.get('total_tokens') if usage else None
        finished_at=datetime.now().isoformat(timespec='seconds')
        try: seconds=max(0.0,(datetime.fromisoformat(finished_at)-datetime.fromisoformat(row['time'])).total_seconds())
        except (KeyError,TypeError,ValueError): seconds=None
        self.append({**row,'usage':usage,'tokens':total,'total_tokens':total,
                     'ok':good,'error_type':error_type,'cost_cny':estimate(usage,row['pricing']),'outcome':outcome,
                     'finished_at':finished_at,'seconds':seconds})

    def event(self, event_type, model='', channel='primary', context=None):
        context=context or {}
        self.append({'id':uuid.uuid4().hex,'time':datetime.now().isoformat(timespec='seconds'),
                     'model':model,'role':'vision','task':event_type,'attempt':0,
                     'run_id':context.get('run_id',''),'item_id':context.get('item_id',''),
                     'scope':context.get('scope','processing'),'pricing':None,'usage':None,
                     'tokens':None,'total_tokens':None,'ok':False,'error_type':event_type,
                     'channel':channel,'cost_cny':None,'outcome':'event','event_type':event_type})

    def records(self,run_id=None,item_id=None):
        with LOCK:
            def read(connection):
                clauses=[];parameters=[]
                for name,value in (('run_id',run_id),('item_id',item_id)):
                    if value is not None:clauses.append(name+'=?');parameters.append(str(value))
                where=' WHERE '+' AND '.join(clauses) if clauses else ''
                return [json.loads(row[0]) for row in connection.execute(
                    'SELECT row_data FROM receipts'+where+' ORDER BY rowid',parameters)]
            return self._indexed(read,lambda:[row for row in self._scan()
                if (run_id is None or row.get('run_id','')==run_id)
                and (item_id is None or row.get('item_id','')==item_id)])

    def summary(self,run_id=None,item_id=None,*,run_ids=None):
        """Fresh latest-ID totals; only appended journal bytes are reparsed.

        The private index is derived data. Its cursor and totals commit together,
        and deleting it only costs a one-time journal rebuild. An empty run_ids
        collection selects no runs rather than selecting the entire ledger.
        """
        if run_ids is not None:
            selected=list(dict.fromkeys(str(value) for value in run_ids))
            if not selected:return summarize([])
        else:selected=None
        with LOCK:
            def read(connection):
                total=summarize([])
                buckets=([('both',value,str(item_id)) if item_id is not None else ('run',value,'')
                          for value in selected] if selected is not None else
                         [('both',str(run_id),str(item_id)) if run_id is not None and item_id is not None else
                          ('run',str(run_id),'') if run_id is not None else
                          ('item',str(item_id),'') if item_id is not None else ('all','','')])
                if selected is None:
                    for bucket in buckets:
                        result=connection.execute('SELECT totals FROM totals WHERE kind=? AND first_key=? AND second_key=?',bucket).fetchone()
                        if result:self._add_summary(total,json.loads(result[0]))
                else:
                    # A large parent may select thousands of child runs. One
                    # sqlite step per run repeatedly yields the GIL while the
                    # shared progress lock is held. Fetch bounded batches, then
                    # add the SAME buckets in the SAME order as before.
                    kind='both' if item_id is not None else 'run'
                    second=str(item_id) if item_id is not None else ''
                    for start in range(0,len(selected),500):
                        batch=selected[start:start+500]
                        marks=','.join('?' for _ in batch)
                        # Fetching hundreds of separate result rows also yields
                        # the GIL for every sqlite step. One JSON object transfers
                        # the same stored strings in one row per bounded batch.
                        found=json.loads(connection.execute(
                            'SELECT json_group_object(first_key,totals) FROM totals WHERE kind=? AND second_key=? AND first_key IN ('+marks+')',
                            [kind,second,*batch]).fetchone()[0])
                        for value in batch:
                            if value in found:self._add_summary(total,json.loads(found[value]))
                total['seconds']=round(total['seconds'],3)
                return total
            return self._indexed(read,lambda:summarize(row for row in self._scan()
                if (selected is None or str(row.get('run_id','')) in selected)
                and (run_id is None or row.get('run_id','')==run_id)
                and (item_id is None or row.get('item_id','')==item_id)))

    @staticmethod
    def _add_summary(total,change,sign=1):
        for key,value in change.items():
            if key=='cost_cny':total[key]=str(Decimal(total[key])+sign*Decimal(value))
            else:total[key]+=sign*value

    @staticmethod
    def _buckets(run_id,item_id):
        return (('all','',''),('run',run_id,''),('item',item_id,''),('both',run_id,item_id))

    def _scan(self):
        records={}
        if not self.path.exists():return []
        with self.path.open('rb') as stream:
            for row,_offset in self._journal_rows(stream):records[row['id']]=row
        return list(records.values())

    @staticmethod
    def _journal_rows(stream):
        # A crash tail is left before the cursor until a following newline
        # isolates it. Bound malformed-line memory independently of log size.
        limit=8*1024*1024
        while True:
            start=stream.tell();raw=stream.readline(limit+1)
            if not raw:return
            oversized=len(raw)>limit
            while raw and not raw.endswith(b'\n') and len(raw)>limit:
                raw=stream.readline(limit+1)
            complete=raw.endswith(b'\n')
            if oversized:
                if not complete:stream.seek(start);return
                continue
            try:row=json.loads(raw)
            except (ValueError,UnicodeError):
                if not complete:stream.seek(start);return
                continue
            if isinstance(row,dict) and isinstance(row.get('id'),str):yield row,stream.tell()
            if not complete:return

    @staticmethod
    def _fingerprint(stream,offset):
        stream.seek(0);head=stream.read(min(offset,512))
        stream.seek(max(0,offset-512));tail=stream.read(min(offset,512))
        return hashlib.sha256(head+tail).hexdigest()

    def _indexed(self,operation,fallback):
        try:
            self.index_path.parent.mkdir(parents=True,exist_ok=True)
            # No retained connection or unbounded in-memory receipt map.
            for attempt in range(2):
                try:
                    with closing(sqlite3.connect(self.index_path,timeout=5)) as connection:
                        with connection:
                            connection.execute('CREATE TABLE IF NOT EXISTS journal (id INTEGER PRIMARY KEY CHECK(id=1), metadata TEXT NOT NULL)')
                            connection.execute('CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, item_id TEXT NOT NULL, row_data TEXT NOT NULL, contribution TEXT NOT NULL)')
                            connection.execute('CREATE INDEX IF NOT EXISTS receipt_run ON receipts(run_id,item_id)')
                            connection.execute('CREATE TABLE IF NOT EXISTS totals (kind TEXT NOT NULL, first_key TEXT NOT NULL, second_key TEXT NOT NULL, totals TEXT NOT NULL, PRIMARY KEY(kind,first_key,second_key))')
                            self._sync(connection)
                            return operation(connection)
                except sqlite3.DatabaseError as exc:
                    # A locked or unwritable index is not corruption. Use the
                    # authoritative journal without changing billing semantics.
                    if attempt or isinstance(exc,sqlite3.OperationalError):raise
                    self.index_path.unlink(missing_ok=True)
        except (sqlite3.Error,OSError):return fallback()

    def _sync(self,connection):
        stored=connection.execute('SELECT metadata FROM journal WHERE id=1').fetchone()
        try:metadata=json.loads(stored[0]) if stored else {}
        except (ValueError,TypeError):metadata={}
        if not isinstance(metadata,dict) or type(metadata.get('offset',0)) is not int or metadata.get('offset',0)<0:
            metadata={}
        if not self.path.exists():
            connection.execute('DELETE FROM receipts');connection.execute('DELETE FROM totals')
            connection.execute('DELETE FROM journal');return
        with self.path.open('rb') as stream:
            stat=os.fstat(stream.fileno());identity=[stat.st_dev,stat.st_ino]
            offset=metadata.get('offset',0)
            rebuild=(not metadata or metadata.get('identity')!=identity or stat.st_size<offset
                     or (stat.st_size==offset and metadata.get('mtime_ns')!=stat.st_mtime_ns)
                     or metadata.get('fingerprint')!=self._fingerprint(stream,offset))
            if rebuild:
                connection.execute('DELETE FROM receipts');connection.execute('DELETE FROM totals');offset=0
            stream.seek(offset)
            changes={}
            def add(buckets,contribution,sign):
                for bucket in buckets:
                    if bucket not in changes:
                        old=connection.execute('SELECT totals FROM totals WHERE kind=? AND first_key=? AND second_key=?',bucket).fetchone()
                        changes[bucket]=json.loads(old[0]) if old else summarize([])
                    self._add_summary(changes[bucket],contribution,sign)
            def flush():
                connection.executemany('INSERT INTO totals VALUES (?,?,?,?) ON CONFLICT(kind,first_key,second_key) DO UPDATE SET totals=excluded.totals',
                    [(*bucket,json.dumps(total,separators=(',',':'))) for bucket,total in changes.items()])
                changes.clear()
            for row,new_offset in self._journal_rows(stream):
                old=connection.execute('SELECT run_id,item_id,contribution FROM receipts WHERE id=?',(row['id'],)).fetchone()
                if old:add(self._buckets(old[0],old[1]),json.loads(old[2]),-1)
                run_id=str(row.get('run_id',''));item_id=str(row.get('item_id',''))
                contribution=summarize([row])
                connection.execute('INSERT INTO receipts VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET run_id=excluded.run_id,item_id=excluded.item_id,row_data=excluded.row_data,contribution=excluded.contribution',
                    (row['id'],run_id,item_id,json.dumps(row,ensure_ascii=False),json.dumps(contribution)))
                add(self._buckets(run_id,item_id),contribution,1)
                offset=new_offset
                if len(changes)>=256:flush()
            # Advance past complete malformed/blank lines, retaining only an
            # unfinished trailing line for the next append.
            offset=stream.tell()
            flush()
            metadata={'identity':identity,'offset':offset,'mtime_ns':stat.st_mtime_ns,
                      'fingerprint':self._fingerprint(stream,offset)}
            connection.execute('INSERT INTO journal VALUES (1,?) ON CONFLICT(id) DO UPDATE SET metadata=excluded.metadata',
                               (json.dumps(metadata),))


def summarize(rows):
    result={'requests':0,'input_tokens':0,'output_tokens':0,'total_tokens':0,'cached_tokens':0,
            'unknown_usage':0,'unpriced':0,'priced_requests':0,'cost_cny':'0',
            'primary_requests':0,'fallback_requests':0,'circuit_breaker_trips':0,'seconds':0.0}
    amount=Decimal(0)
    for row in rows:
        # The durable begin row may precede cancellation during its fsync;
        # its terminal replacement records that no request reached HTTP.
        if row.get('outcome')=='cancelled_before_send':continue
        if row.get('event_type')=='circuit_opened':
            result['circuit_breaker_trips']+=1
            continue
        result['requests']+=1
        if row.get('channel')=='primary': result['primary_requests']+=1
        elif row.get('channel')=='fallback': result['fallback_requests']+=1
        u=row.get('usage')
        if u is None: result['unknown_usage']+=1
        else:
            for key in ['input_tokens','output_tokens','total_tokens','cached_tokens']: result[key]+=u[key]
        if row.get('cost_cny') is not None:
            amount+=Decimal(row['cost_cny']);result['priced_requests']+=1
        elif u is not None: result['unpriced']+=1
        result['cost_cny']=str(amount)
        result['seconds']+=float(row.get('seconds') or 0)
    result['seconds']=round(result['seconds'],3)
    return result


def summary_text(summary):
    if summary is None: return '未记录 Token（升级前的批次无法追溯）'
    s=summary
    cost=f"¥{Decimal(s['cost_cny']):.6f}"
    pending=s['unknown_usage']+s['unpriced']
    if pending: cost=('已知部分 '+cost if s['priced_requests'] else '费用未知')+f"；{s['unknown_usage']} 次用量未知 / {s['unpriced']} 次缺单价"
    prefix='已知' if s['unknown_usage'] else ''
    channels=f"主通道 {s.get('primary_requests',0)} 次 / 备用通道 {s.get('fallback_requests',0)} 次"
    trips=f" / 熔断 {s.get('circuit_breaker_trips',0)} 次" if 'circuit_breaker_trips' in s else ''
    return prefix+f"输入 {s['input_tokens']:,} / 输出 {s['output_tokens']:,} · 合计 {s['total_tokens']:,} Token · 预估 {cost} · {channels}{trips}"
