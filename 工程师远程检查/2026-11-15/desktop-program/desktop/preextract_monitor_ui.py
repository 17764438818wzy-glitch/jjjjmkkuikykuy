"""Read-only external paper progress inside the existing batch history page."""
from datetime import datetime
import threading
import time
import tkinter as tk
from tkinter import ttk

from live_results import sync_tree


def display_time(value):
    if value in (None, '', 0):
        return '暂无记录'
    try:
        return datetime.fromtimestamp(float(value)).strftime('%m-%d %H:%M:%S')
    except (TypeError, ValueError, OSError, OverflowError):
        return str(value or '暂无记录')


class PreextractMonitorView:
    def build_preextract_monitor(self):
        self._monitor_token = None
        self._monitor_batch_id = None
        self._monitor_next_read = 0
        self._monitor_reader = None
        self._monitor_batches = []
        self._monitor_snapshot = None
        self._monitor_last_success = None
        frame = ttk.LabelFrame(self.history_tab, text='论文实时进度 · 外部批次同步', padding=8)
        frame.grid(row=2, column=0, sticky='nsew', pady=(8, 0))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(3, weight=1)
        self.history_tab.rowconfigure(2, weight=5)
        self.history_tree.configure(height=3)
        toolbar = ttk.Frame(frame)
        toolbar.grid(row=0, column=0, sticky='ew')
        self.monitor_batch = tk.StringVar()
        self.monitor_batch_combo = ttk.Combobox(toolbar, textvariable=self.monitor_batch,
                                               state='readonly', width=43)
        self.monitor_batch_combo.pack(side='left')
        self.monitor_batch_combo.bind('<<ComboboxSelected>>', self.select_monitor_batch)
        ttk.Button(toolbar, text='立即同步', command=lambda: self.request_preextract_monitor(force=True),
                   style='Task.TButton').pack(side='left', padx=6)
        ttk.Button(toolbar, text='查看详情', command=self.monitor_details,
                   style='Task.TButton').pack(side='right')
        self.monitor_search = tk.StringVar()
        ttk.Entry(toolbar, textvariable=self.monitor_search, width=18).pack(side='right', padx=6)
        ttk.Label(toolbar, text='筛选论文/状态').pack(side='right')
        self.monitor_search.trace_add('write', lambda *_: self.render_preextract_monitor())
        self.monitor_summary = tk.StringVar(value='等待读取论文记录')
        ttk.Label(frame, textvariable=self.monitor_summary, font=('Microsoft YaHei UI', 10, 'bold')).grid(
            row=1, column=0, sticky='w', pady=(7, 3))
        self.monitor_health = tk.StringVar(value='本页每 2 秒同步；只读取记录，不启动或重试任务。')
        ttk.Label(frame, textvariable=self.monitor_health, foreground='#617082', wraplength=1060).grid(
            row=2, column=0, sticky='w', pady=(0, 5))
        columns = ['序号', '论文', '最新阶段', '尝试', '模型记录', '入库/读回', '记录更新时间']
        self.monitor_tree = self.tree(frame, columns, [45, 255, 205, 65, 150, 170, 145], grid_row=3)
        self.monitor_tree.configure(height=9)
        self.monitor_tree.bind('<Double-1>', lambda _: self.monitor_details())

    def show_preextract_monitor(self):
        self.tabs.select(self.history_tab)
        self.request_preextract_monitor(force=True)

    def select_monitor_batch(self, _event=None):
        index = self.monitor_batch_combo.current()
        if not 0 <= index < len(self._monitor_batches):
            return
        self._monitor_batch_id = self._monitor_batches[index]['id']
        self._monitor_snapshot = None
        self._monitor_last_success = None
        self._monitor_next_read = 0
        sync_tree(self.monitor_tree, [])
        self.monitor_summary.set('正在读取所选批次…')
        self.monitor_health.set('等待新批次记录；每 2 秒同步。')
        self.request_preextract_monitor(force=True)

    def request_preextract_monitor(self, *, force=False):
        if self._monitor_token is not None:
            return
        if not force and (self.tabs.select() != str(self.history_tab)
                          or time.monotonic() < self._monitor_next_read):
            return
        if self._monitor_reader is None:
            from preextract_monitor import PreextractMonitorReader
            self._monitor_reader = PreextractMonitorReader(self.store.root)
        reader = self._monitor_reader
        requested = self._monitor_batch_id
        token = self._monitor_token = object()
        self._monitor_next_read = time.monotonic() + 2
        events = self.events
        def read():
            batches, result, error = [], None, None
            try:
                batches = reader.discover()
                batch_id = requested or (batches[0]['id'] if batches else None)
                if batch_id is not None:
                    result = reader.read(batch_id)
            except Exception as exc:
                # Exception text can contain private source/credential content.
                error = type(exc).__name__
            events.put(('preextract_monitor', (token, requested, batches, result, error)))
        try:
            threading.Thread(target=read, daemon=True).start()
        except Exception:
            self._monitor_token = None
            raise

    def finish_preextract_monitor(self, value):
        token, requested, batches, result, error = value
        if token is not self._monitor_token:
            return
        self._monitor_token = None
        if requested != self._monitor_batch_id:
            self._monitor_next_read = 0
            return
        if error:
            prior = display_time(self._monitor_last_success) if self._monitor_last_success else '尚无成功同步'
            self.monitor_health.set(f'暂未更新（{error}），保留上次有效记录；上次同步：{prior}。稍后自动重读。')
            return
        if batches:
            self._monitor_batches = batches
            self.monitor_batch_combo.configure(values=[f"{b['label']} · {b['count']} 篇" for b in batches])
        if result is None:
            self.monitor_summary.set('暂无可同步的外部论文批次')
            self.monitor_health.set('外部批次保存论文名单后可在此查看；本页不会启动处理。')
            return
        self._monitor_batch_id = result['batch_id']
        for index, batch in enumerate(self._monitor_batches):
            if batch['id'] == result['batch_id']:
                self.monitor_batch_combo.current(index)
                break
        self._monitor_snapshot = result
        self._monitor_last_success = time.time()
        counts = result['counts']
        self.monitor_summary.set(
            f"共 {counts['total']} 篇  |  已封包 {counts['packaged']}  |  入库并读回 {counts['verified']}"
            f"  |  需核对 {counts['attention']}  |  待处理 {counts['pending']}")
        self.monitor_health.set(
            '每 2 秒同步 · 最近同步 ' + display_time(self._monitor_last_success)
            + ' · 已封包含已入库；阶段来自落盘记录，请求待响应也可能处于未决状态。')
        self.render_preextract_monitor()

    def render_preextract_monitor(self):
        if not self._monitor_snapshot:
            return
        query = self.monitor_search.get().strip().casefold()
        rows = self._monitor_snapshot['rows']
        self._monitor_rows = {str(row['id']): row for row in rows}
        shown = []
        for row in rows:
            values = (row['number'], row['name'], row['label'], row['attempts'],
                      row.get('model_label') or row['model_status'],
                      row.get('delivery_label') or row['delivery_status'], display_time(row['updated_at']))
            if not query or query in ' '.join(map(str, values)).casefold():
                shown.append((str(row['id']), values))
        sync_tree(self.monitor_tree, shown)

    def monitor_details(self):
        selection = self.monitor_tree.selection()
        if not selection:
            self.monitor_health.set('请选择一篇论文查看阶段和历史尝试。')
            return
        row = self._monitor_rows.get(selection[0])
        if row:
            detail = row.get('detail', '')
            if isinstance(detail, (list, tuple)):
                detail = '\n'.join(map(str, detail))
            self.text_window('论文实时进度 · ' + row['name'],
                             f"第 {row['number']} 篇：{row['name']}\n最新阶段：{row['label']}\n"
                              f"最新尝试：{row['attempt']}\n模型：{row.get('model_label') or row['model_status']}\n"
                              f"入库：{row.get('delivery_label') or row['delivery_status']}\n记录更新时间：{display_time(row['updated_at'])}\n\n"
                             f"{detail}\n\n工作目录：{row['work_dir']}\n\n此窗口仅显示记录，不触发任何处理。")
