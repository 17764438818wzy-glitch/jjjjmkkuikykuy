"""One read-only census panel, installable without replacing a task callback."""
import queue
import threading
import time
import tkinter as tk
from tkinter import ttk

from model_activity import ModelActivityReader


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
                self.health.set('统计暂未更新，保留上次结果；任务继续运行。')
            else:
                self._last_snapshot = snapshot
                text = f"模型请求总数 {snapshot['total']}（在途） · 本批 {snapshot['current']} · 后台 {snapshot['background']}"
                if snapshot['unconfirmed']:text += f" · 待确认 {snapshot['unconfirmed']}"
                if snapshot['waiting_to_send']:text += f" · 等待发送 {snapshot['waiting_to_send']}"
                self.summary.set(text)
                self.health.set('每 2 秒更新 · 最近同步 '+time.strftime('%H:%M:%S')+' · 已复用回答不重复计数')
            self._next_read = time.monotonic()+2
        if not self._reading and time.monotonic() >= self._next_read:
            self.refresh()
        self._next = self.after(100,self._tick)

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
