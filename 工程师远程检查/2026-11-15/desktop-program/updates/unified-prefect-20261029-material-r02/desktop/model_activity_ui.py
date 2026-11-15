"""One read-only census panel, installable without replacing a task callback."""
import queue
import threading
import time
import tkinter as tk
from tkinter import ttk

from model_activity import ModelActivityReader


def activity_text(snapshot):
    if 'paper_current' in snapshot:
        from progress_summary import pipeline_text
        counts=dict(snapshot.get('pipeline_counts') or {})
        certain=snapshot.get('journal_complete',True) and snapshot.get('current_complete',True)
        counts.update(paper_llm_active=snapshot['paper_current'] if certain else None,
            purpose_llm_active=snapshot['purpose_current'] if certain else None,
            purpose_llm_limit=snapshot.get('purpose_limit'),total_llm_active=snapshot['total'] if certain else None)
        text=pipeline_text(counts,snapshot.get('paper_limit',50))
        status={'stopped':'已停止','completed':'已完成','failed':'执行失败'}.get(snapshot.get('current_status'))
        if status:text='本批'+status+' · '+text
        if snapshot.get('background'):text+=f" · 后台请求 {snapshot['background']}"
        if snapshot.get('legacy_shared_current'):text+=' · 当前旧批次共用50名额'
        if not snapshot.get('journal_complete',True):text+=' · 正在读取完整请求回执'
        if snapshot.get('unconfirmed'):text+=f" · 未闭合记录 {snapshot['unconfirmed']}（未计在途）"
        if snapshot.get('unreadable_records'):text+=' · 部分记录暂不可读'
        elif not snapshot.get('current_complete',True):text+=' · 本批运行者待确认'
        return text
    total=snapshot['total']
    if snapshot.get('partial') and total==0:return '模型请求数暂不可用 · 部分记录未读到，不能判断为 0'
    if total==0 and snapshot['unconfirmed']:return f"模型请求数待确认 · {snapshot['unconfirmed']} 条记录未绑定存活运行者"
    prefix='模型请求已确认至少 ' if snapshot.get('partial') else '模型请求总数 '
    def scope(label,key):
        value=snapshot[key]
        if snapshot.get(key+'_complete',True):return label+' '+str(value)
        return label+('至少 '+str(value) if value else '待确认')
    text=f"{prefix}{total}（在途） · {scope('本批','current')} · {scope('后台','background')}"
    if snapshot['unconfirmed']:text+=f" · 待确认 {snapshot['unconfirmed']}"
    if snapshot['waiting_to_send']:text+=f" · 等待发送 {snapshot['waiting_to_send']}"
    return text


class ModelActivityPanel(ttk.Frame):
    def __init__(self, parent, data_root, current_run):
        super().__init__(parent)
        self.reader = ModelActivityReader(data_root)
        self.current_run = current_run
        self.summary = tk.StringVar(master=self,value='模型请求总数：正在读取…')
        self.health = tk.StringVar(master=self,value='每 2 秒汇总本批与后台请求')
        self.label = ttk.Label(self,textvariable=self.summary,foreground='#315f8c',
                               font=('Microsoft YaHei UI',10,'bold'),wraplength=1080)
        self.label.pack(anchor='w')
        ttk.Label(self,textvariable=self.health,foreground='#617082').pack(anchor='w')
        self._events = queue.SimpleQueue()
        self._reading = False
        self._closed = False
        self._next = self.after(100,self._tick)
        self._last_snapshot = None
        self._next_read = 0
        self._local_at=0
        self._local_counts=None;self._local_limit=None
        self.bind('<Destroy>',self._destroyed,add='+')

    def refresh(self):
        if self._closed or self._reading:
            return
        self._reading = True
        current = self.current_run()
        def read():
            try:self._events.put((self.reader.read(current),None))
            except Exception as exc:self._events.put((None,type(exc).__name__))
        try:threading.Thread(target=read,name='model-activity-read',daemon=True).start()
        except Exception:
            self._reading = False
            raise

    def _tick(self):
        if self._closed:
            return
        try:
            snapshot,error = self._events.get_nowait()
        except queue.Empty:
            pass
        else:
            self._reading = False
            if error:
                if time.monotonic()-self._local_at>=3:
                    self.summary.set('模型请求数暂不可用 · 上次统计已过期')
                self.health.set('统计暂未更新，当前请求数无法确认；任务继续运行。')
            else:
                self._last_snapshot = snapshot
                self._render()
                self.health.set(('存在未确认记录 · 已显示可确认数量 · ' if snapshot.get('partial') else '每 2 秒更新 · ')
                    +'最近同步 '+time.strftime('%H:%M:%S')+' · 已复用回答不重复计数')
            self._next_read = time.monotonic()+2
        if not self._reading and time.monotonic() >= self._next_read:
            self.refresh()
        self._next = self.after(100,self._tick)

    def observe_pipeline(self,run_id,counts,limit=None):
        if run_id!=self.current_run():return
        self._local_at=time.monotonic()
        self._local_counts=dict(counts);self._local_limit=limit
        self._render()
        self.health.set('本轮队列实时计数 · '+time.strftime('%H:%M:%S')+' · 模型请求与后处理分别统计')

    def _render(self):
        from progress_summary import pipeline_text
        if (self._local_counts is not None and time.monotonic()-self._local_at<3
                and 'paper_llm_active' in self._local_counts):
            self.summary.set(pipeline_text(self._local_counts,self._local_limit))
        elif self._last_snapshot is not None:self.summary.set(activity_text(self._last_snapshot))

    def _destroyed(self,event):
        if event.widget is self:
            self._closed = True
            try:self.after_cancel(self._next)
            except tk.TclError:pass


def install_model_activity(app):
    """Add only a panel and its own timers; keep the processor and event loop."""
    prior = getattr(app,'_model_activity_panel',None)
    if prior is not None and prior.winfo_exists():
        return prior
    def current():
        return getattr(app,'_live_requested_run',None) or (app.current or {}).get('run_id')
    panel = ModelActivityPanel(app.panes.master,app.store.root,current)
    panel.pack(fill='x',before=app.panes,pady=(0,6))
    app._model_activity_panel = panel
    return panel
