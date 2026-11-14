"""Chinese Windows upstream workbench using the original cleaning scripts."""
from __future__ import annotations

import argparse
import copy
import json
import os
import queue
import threading
import time
import tkinter as tk
import webbrowser
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from engine import MAX_SCAN_FILES, Store, read_json, scan_sources, write_json
from ragflow_client import DEFAULT_RAGFLOW_URL, RagflowClient, upload_routed_items
from auto_pipeline import AutoProcessor
from auto_view import REASONS, _published, outcome, result_items
from script_task import ScriptTaskView, task_snapshot, script_outcome, source_key
from ai_results_ui import AIResultsView
from rag_results_ui import RAGResultsView
from processing_outcomes import execution_outcome
from live_results import LiveResultView
from preextract_monitor_ui import PreextractMonitorView

STATUS = {"pass": "通过", "review": "不通过", "reject": "不通过", "pending":"不通过", "incomplete":"已结束，有不通过记录", "running": "处理中", "completed": "已完成", "failed": "失败", "stopped": "已停止", "interrupted": "上次中断"}


def displayed_outcome(item, manifest):
    return script_outcome(item, manifest)


HISTORICAL_SECONDS_PER_PAPER = 156.5


def serial_estimate_hours(source_count, seconds_per_paper=HISTORICAL_SECONDS_PER_PAPER):
    return max(0, float(source_count)) * float(seconds_per_paper) / 3600


def confirm_serial_start(source_count, config, ask=None):
    options = config.get('concurrency', config) if isinstance(config, dict) else {}
    if bool(options.get('enabled')):
        return True
    hours = serial_estimate_hours(source_count)
    ask = ask or messagebox.askyesno
    message = (f'当前是串行模式，按历史平均 {HISTORICAL_SECONDS_PER_PAPER:.1f} 秒/篇，'
               f'预计需要约 {hours:.1f} 小时。确定继续？')
    return bool(ask('串行模式确认', message))

def retry_sources(manifest,scope='all'):
    """Retry failed terminal sources once; successful sources need only handoff."""
    if manifest.get('status') == 'running':
        return []
    items = result_items(manifest)
    outcome_for_item=lambda item: execution_outcome(item,manifest) if scope=='script' else displayed_outcome(item,manifest)
    from archive_identity import record_key, member_fields, ARCHIVES
    accepted = {record_key(i) for i in items
                if outcome_for_item(i)[0] in {'通过','已删除'} or i.get('reason_kind') == 'duplicate'}
    accepted.update(record_key(e) for e in manifest.get('events', [])
                    if e.get('status') in {'duplicate', 'uploaded_skip'})
    records = [{'path': i['source_path'], 'name': Path(i['source_path']).name, **member_fields(i)}
               for i in items if i.get('source_path') and i.get('reason_kind') != 'duplicate'
               and outcome_for_item(i)[0] == '不通过']
    if manifest.get('status') in {'stopped', 'interrupted', 'failed'}:
        records.extend(manifest.get('inputs', []))
        records.extend(manifest.get('discovered_inputs', []))
    expanded_archives = {source_key(row['input_source_path']) for row in [*records, *items]
                         if row.get('input_source_path') and
                         source_key(row['input_source_path']) != source_key(row.get('source_path') or row.get('path', '')) and
                         Path(row['input_source_path']).suffix.lower() in ARCHIVES}
    selected, seen = [], set(accepted)
    for record in records:
        path = str(record.get('path', ''))
        key = record_key(record)
        if path and source_key(path) not in expanded_archives and key not in seen:
            selected.append(dict(record, path=path))
            seen.add(key)
    return selected


def select_operation(manifest, operation):
    operation = {'只补上传': 'upload_only', '只重跑失败': 'retry_failed',
                 '跳过所有跑过的': 'skip_processed'}.get(operation, operation)
    items = result_items(manifest)
    if operation == 'retry_failed': return retry_sources(manifest)
    if operation == 'upload_only':
        return [item for item in items if displayed_outcome(item, manifest)[0] == '通过'
                and item.get('automatic_approval') and not any(
                    record.get('status') == 'uploaded' for record in (item.get('uploads') or {}).values())]
    if operation == 'skip_processed':
        processed = {str(item.get('source_path', '')).casefold() for item in items if item.get('source_path')}
        return [dict(record, path=str(record.get('path', ''))) for record in manifest.get('inputs', [])
                if str(record.get('path', '')).casefold() not in processed]
    raise ValueError('未知操作类型：' + str(operation))


class Workbench(PreextractMonitorView,LiveResultView,RAGResultsView,AIResultsView,ScriptTaskView):
    def __init__(self, root, data_root, *, recover=True):
        self.root = root
        self.store = Store(data_root)
        if recover:
            self.store.recover()
        self.processor = AutoProcessor(self.store)
        self.events = queue.Queue()
        self.busy = False
        self.task_kind = None
        self._active_task = None
        self._model_cancel = None
        self._dataset_read_task = None
        self.sources = []
        self.current = None
        self._script_manifest = None
        self._script_progress_stamp = None
        self.datasets = []
        self.task_started = None
        self.stage_started = None
        self.last_stage = None
        settings = read_json(self.store.root / "settings.json", {})
        self.folder = tk.StringVar(value=settings.get("input_dir", ""))
        self.limit = tk.StringVar(value=str(MAX_SCAN_FILES))
        self.url = tk.StringVar(value=settings.get("ragflow_url", DEFAULT_RAGFLOW_URL))
        from auto_config import load_key
        self.key = tk.StringVar(value=os.environ.get("RAGFLOW_API_KEY", "") or load_key(self.store.root,'ragflow'))
        self.dataset = tk.StringVar()
        # The legacy `dataset` variable remains the md/paper destination so
        # existing result-page integrations keep working.  Images have an
        # independent selection and receipt namespace.
        self.image_dataset = tk.StringVar()
        self.major = tk.StringVar(value="全部专业")
        self.result_filter=tk.StringVar(value='全部结果')
        self.state = tk.StringVar(value="选择文件夹，扫描后开始处理")
        self.connection = tk.StringVar(value="输入 API Key 可直接导入，也可以打开 RAGFlow 手动上传。")
        self.root.title("论文知识库预处理工作台")
        self.root.geometry("1200x790")
        self.root.minsize(980, 660)
        self.root.configure(bg="#f3f5f7")
        self.root.option_add("*Font", "{Microsoft YaHei UI} 10")
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background="#f3f5f7")
        style.configure("TLabel", background="#f3f5f7", foreground="#243140")
        style.configure("TButton", padding=(12, 7))
        style.configure('Task.TButton', padding=(8,3))
        style.configure('TaskAccent.TButton', padding=(8,3),background='#315f8c',foreground='#f8fafc')
        style.configure("Accent.TButton", background="#315f8c", foreground="#f8fafc")
        style.map("Accent.TButton", background=[("active", "#264d74"), ("disabled", "#cad1d8")])
        style.configure("Treeview", rowheight=29, fieldbackground="#fafbfc", background="#fafbfc", foreground="#243140")
        style.configure("Treeview.Heading", padding=(8, 6), font=("Microsoft YaHei UI", 10, "bold"))
        self.build()
        self.dataset.trace_add('write',lambda *_:self.render_rag_results())
        self.url.trace_add('write',lambda *_:self.refresh_upload_display())
        self.tabs.bind('<<NotebookTabChanged>>', self.load_result_datasets, add='+')
        self.update_controls()
        self.refresh_history()
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        self.root.after(150, self.poll)
        if recover:
            self.root.after(1000, self._resume_preextract_deliveries)

    def _resume_preextract_deliveries(self):
        # Only this explicit queue is resumed; historical packages/runs are
        # never scanned or adopted by the new delivery worker.
        from preextract_delivery_worker import resume_queue
        try:
            resume_queue(self.store.root,self.url.get())
        except (OSError,ValueError):
            self.state.set('成品上传队列暂未恢复；原包保留，可从原交接入口继续。')

    def build(self):
        frame = ttk.Frame(self.root, padding=16)
        frame.pack(fill="both", expand=True)
        header = ttk.Frame(frame)
        header.pack(fill="x", pady=(0, 8))
        ttk.Label(header, text="论文知识库预处理", font=("Microsoft YaHei UI", 19, "bold")).pack(side="left")
        ttk.Button(header, text="打开 RAGFlow ↗", command=self.open_ragflow).pack(side="right")
        ttk.Button(header, text="论文实时进度", command=self.show_preextract_monitor).pack(side="right", padx=8)
        ttk.Label(frame, text="程序准备全文与图表 → 模型完整提取 → 程序校验、脱敏、切片和封包 → RAGFlow 入库与读回", foreground="#617082").pack(anchor="w", pady=(0, 8))
        footer=ttk.Frame(frame)
        footer.pack(side="bottom",fill="x")
        ttk.Separator(footer).pack(fill="x",pady=(8,5))
        self.status_label=ttk.Label(footer,textvariable=self.state,wraplength=900)
        self.status_label.pack(anchor="w")
        footer.bind('<Configure>',lambda e:self.status_label.configure(wraplength=max(200,e.width)))
        self.panes = tk.PanedWindow(frame, orient="vertical", background="#f3f5f7", borderwidth=0, sashwidth=7)
        self.panes.pack(fill="both", expand=True)
        self.tabs = ttk.Notebook(self.panes)
        self.panes.add(self.tabs, minsize=360, stretch="always")
        self.input_tab = ttk.Frame(self.tabs, padding=6)
        self.result_tab = ttk.Frame(self.tabs, padding=14)
        self.rag_result_tab = ttk.Frame(self.tabs, padding=14)
        self.rag_tab = ttk.Frame(self.tabs, padding=18)
        self.history_tab = ttk.Frame(self.tabs, padding=14)
        for tab, name in [(self.input_tab, "  选择与处理  "), (self.result_tab, "  AI 处理结果  "), (self.rag_result_tab, "  RAGFlow处理结果  "), (self.rag_tab, "  RAGFlow 连接  "), (self.history_tab, "  批次记录  ")]:
            self.tabs.add(tab, text=name)
        self.build_input()
        self.build_results()
        self.build_rag_results()
        self.build_ragflow()
        self.build_history()
        from auto_ui import build_ai_tab
        build_ai_tab(self)
        self.build_task_settings()
        self.build_progress()
        from model_activity_ui import install_model_activity
        install_model_activity(self)

    def build_progress(self):
        self.progress_panel = ttk.Frame(self.panes, padding=(12, 6, 12, 0))
        self.panes.add(self.progress_panel, minsize=145, height=210, stretch="never")
        top = ttk.Frame(self.progress_panel)
        top.pack(fill="x")
        ttk.Label(top, text="脚本处理进展", font=("Microsoft YaHei UI", 11, "bold")).pack(side="left")
        self.elapsed = tk.StringVar(value="等待任务")
        ttk.Label(top, textvariable=self.elapsed, foreground="#617082").pack(side="left", padx=16)
        self.auto_scroll = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text="自动滚动", variable=self.auto_scroll).pack(side="right")
        ttk.Button(top, text="清空显示", command=self.clear_progress).pack(side="right", padx=8)
        self.progress_detail = tk.StringVar(value="开始处理后，这里显示当前文件、处理阶段和失败原因。")
        ttk.Label(self.progress_panel, textvariable=self.progress_detail, wraplength=1060).pack(anchor="w", pady=(5, 3))
        bar_row = ttk.Frame(self.progress_panel)
        bar_row.pack(fill="x", pady=(0, 6))
        self.progress_bar = ttk.Progressbar(bar_row, mode="determinate", maximum=1, value=0)
        self.progress_bar.pack(side="left", fill="x", expand=True)
        self.progress_count = tk.StringVar(value="本次脚本完成 0/0")
        ttk.Label(bar_row, textvariable=self.progress_count, foreground="#617082").pack(side="left", padx=(12, 0))
        from billing_ui import build_progress
        build_progress(self)
        box = ttk.Frame(self.progress_panel)
        box.pack(fill="both", expand=True)
        self.log_text = tk.Text(box, height=5, wrap="word", state="disabled", font=("Microsoft YaHei UI", 9),
                                background="#fafbfc", foreground="#243140", relief="flat", padx=8, pady=5)
        scroll = ttk.Scrollbar(box, command=self.log_text.yview)
        scroll.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scroll.set)
        self.log_text.pack(fill="both", expand=True)
        self.log_text.tag_configure("error", foreground="#b42318")
        self.log_text.tag_configure("warning", foreground="#946200")

    def clear_progress(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def show_progress(self, event):
        stage, filename = event.get("stage", "处理中"), event.get("file", "")
        if stage.startswith(('RAGFlow','AI ')) or stage=='续跑复用':
            return
        if (stage, filename) != self.last_stage:
            self.last_stage = (stage, filename)
            self.stage_started = time.monotonic()
        detail = stage + (" · " + filename if filename else "")
        self.progress_detail.set(detail)
        if "total" in event:
            completed, total = event.get("completed", 0), event["total"]
            self.progress_bar.configure(value=completed, maximum=max(1, total))
            self.progress_count.set(f"本次脚本完成 {completed}/{total}")
        stamp = event.get("time", time.strftime("%H:%M:%S"))
        message = str(event.get("message", "")).replace("\r", " ").replace("\n", " ")
        line = f"{stamp}  {detail}" + ("  |  " + message if message else "") + "\n"
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line, event.get("level", "info"))
        excess = int(self.log_text.index("end-1c").split(".")[0]) - 2001
        if excess > 0:
            self.log_text.delete("1.0", f"{excess + 1}.0")
        if self.auto_scroll.get():
            self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def load_progress(self, manifest):
        from collections import deque
        from billing_ui import update_progress
        update_progress(self,{**manifest,'billing':manifest.get('billing')})
        self.clear_progress()
        self.elapsed.set("历史记录")
        path = self.store.manifest_path(manifest["run_id"]).with_name("progress.jsonl")
        if path.exists():
            with path.open(encoding="utf-8") as stream:
                for line in deque(stream, maxlen=2000):
                    try:
                        self.show_progress(json.loads(line))
                    except (ValueError, TypeError, KeyError):
                        continue
        self.show_progress({"stage": STATUS.get(manifest["status"], manifest["status"]),
                            "message": manifest["message"] + ("（旧批次没有逐文件进度日志）" if not path.exists() else ""),
                            "completed": manifest.get("processed_sources",len(manifest.get('items',[]))), "total": manifest.get("source_count",len(manifest.get('inputs',[]))),
                            "time": manifest.get("finished_at", manifest["started_at"])[11:19]})

    def update_elapsed(self):
        if self.task_started is not None:
            seconds = int(time.monotonic() - self.task_started)
            if self.task_kind == 'model_settings':
                self.elapsed.set(f'模型读取/测试 {seconds} 秒' + (' · 已结束' if not self.busy else ''))
                return
            stage_seconds = int(time.monotonic() - (self.stage_started or self.task_started))
            self.elapsed.set(f"累计 {seconds // 60:02d}:{seconds % 60:02d}" +
                             (f" · 当前阶段 {stage_seconds} 秒" if self.busy else " · 已结束"))

    def tree(self, parent, columns, widths, grid_row=None):
        box = ttk.Frame(parent)
        if grid_row is None: box.pack(fill="both", expand=True, pady=6)
        else: box.grid(row=grid_row,column=0,sticky="nsew",pady=6)
        box.rowconfigure(0,weight=1);box.columnconfigure(0,weight=1)
        tree = ttk.Treeview(box, columns=columns, show="headings", selectmode="extended",height=3)
        for col, width in zip(columns, widths):
            tree.heading(col, text=col)
            tree.column(col, width=width, minwidth=65, stretch=True)
        scroll = ttk.Scrollbar(box, orient="vertical", command=tree.yview)
        horizontal=ttk.Scrollbar(box,orient="horizontal",command=tree.xview)
        tree.configure(yscrollcommand=scroll.set,xscrollcommand=horizontal.set)
        scroll.grid(row=0,column=1,sticky="ns")
        horizontal.grid(row=1,column=0,sticky="ew")
        tree.grid(row=0,column=0,sticky="nsew")
        tree.bind('<Control-a>',lambda e:self.select_files(tree,'all'))
        tree.bind('<Control-i>',lambda e:self.select_files(tree,'invert'))
        tree.bind('<Control-Shift-A>',lambda e:self.select_files(tree,'none'))
        return tree

    def select_files(self,tree,mode):
        if self.busy and tree==self.source_tree: return 'break'
        children=tree.get_children()
        chosen=set(tree.selection())
        tree.selection_set(children if mode=='all' else [] if mode=='none' else [i for i in children if i not in chosen])
        if tree==self.source_tree:self.update_controls()
        return 'break'

    def update_controls(self):
        count=len(self.source_tree.selection());total=len(self.source_tree.get_children())
        self.selection_count.set(f'已选 {count} / {total} 个文件')
        problem=self.task_setup_problem()
        self.start_button.configure(state='normal' if count and not self.busy and not problem and self._script_manifest is None else 'disabled',
                                    text='正在处理…' if self.busy and self.task_kind=='processing' else '开始处理')
        processing=self.busy and self.task_kind=='processing'
        stopping=processing and self.processor.stop.is_set()
        self.stop_button.configure(state='normal' if processing and not stopping else 'disabled',
                                   text='等待当前操作停止…' if stopping else '停止处理')
        for control in self.input_controls:control.configure(state='disabled' if self.busy else 'normal')
        for control in getattr(self,'processing_mode_controls',[]):
            control.configure(state='disabled' if self.busy else 'normal')
        for control in [self.select_all_button,self.select_none_button,self.invert_button]:
            control.configure(state='disabled' if self.busy or not total else 'normal')
        for control in (self.dataset_combo,self.image_dataset_combo):
            control.configure(state='readonly' if not self.busy or processing else 'disabled')
        for control in getattr(self, 'task_option_controls', []):
            control.configure(state='disabled' if self.busy else 'readonly')
        self.update_rag_controls()
        self.update_ai_retry_controls()
        failed=len(retry_sources(self._script_manifest,scope='script')) if self._script_manifest else 0
        self.resume_button.configure(text=f'继续跑未通过（{failed}）',
                                     state='normal' if failed and not self.busy and not problem else 'disabled')
        self.load_latest_button.configure(state='disabled' if self.busy else 'normal')
        if hasattr(self, 'task_hint'):
            self.task_hint.set('本次运行设置已固定' if processing else problem or
                               ('本页展示脚本执行情况；AI 结论见“AI 处理结果”。新批次请重新扫描。' if self._script_manifest else
                                '一次提取使用所选文本模型读取全文和图像；请使用支持图像的模型。'))
            if not processing and not problem and not self.dataset.get():
                self.task_hint.set('新结果按 MD 最终专业名单分别入库；自动交接使用原有 RAGFlow 连接设置。' if self.ai_vars['auto_upload'].get()
                    else '已关闭自动导入；处理后可到原“RAGFlow处理结果”页导入选中结果。')
            elif not processing and not problem and not self.image_dataset.get():
                self.task_hint.set('已启用本次自动交接；新结果按 MD 最终专业名单入库，所选单库用于旧格式结果。')
            for control in (self.task_model_settings_button, self.task_defer_button):
                control.configure(state='disabled' if self.busy else 'normal')
        for name in ('task_connect_button','rag_connection_button'):
            control=getattr(self,name,None)
            if control is not None:
                control.configure(state='normal' if self.can_read_datasets() else 'disabled')

    def build_input(self):
        self.input_tab.columnconfigure(0,weight=1);self.input_tab.rowconfigure(3,weight=1)
        row = ttk.Frame(self.input_tab)
        row.grid(row=0,column=0,sticky="ew")
        ttk.Label(row, text="输入文件夹").pack(side="left", padx=(0, 10))
        folder_entry=ttk.Entry(row, textvariable=self.folder)
        folder_entry.pack(side="left", fill="x", expand=True)
        choose_button=ttk.Button(row, text="选择…", command=self.choose)
        choose_button.pack(side="left", padx=8)
        ttk.Label(row, text="扫描上限").pack(side="left")
        limit_box=ttk.Spinbox(row, textvariable=self.limit, from_=1, to=MAX_SCAN_FILES, width=6)
        limit_box.pack(side="left", padx=8)
        self.scan_button=ttk.Button(row, text="扫描文件", command=self.scan)
        self.scan_button.pack(side="left")
        self.input_controls=[folder_entry,choose_button,limit_box,self.scan_button]
        # The Agent settings are initialized after this page is built.  Keep a
        # dedicated row ready so the concurrency switch can be mounted here,
        # where a user starts a batch, rather than hidden in a scrolled page.
        self.processing_mode_row=ttk.LabelFrame(self.input_tab,text='处理模式',padding=(6,0))
        self.processing_mode_row.grid(row=1,column=0,sticky='ew',pady=(4,0))
        self.source_tree = self.tree(self.input_tab, ["文件名", "脚本状态", "处理阶段 / 结果原因", "上次失败原因", "来源路径"], [265, 95, 280, 230, 350],grid_row=3)
        self.source_tree.heading('脚本状态',text='脚本执行状态')
        self.source_tree.master.grid_configure(pady=3)
        self.source_tree.bind('<<TreeviewSelect>>',lambda e:self.update_controls())
        self.source_tree.bind('<Double-1>', self.preview_script_row)
        actions = ttk.Frame(self.input_tab)
        actions.grid(row=2,column=0,sticky="ew")
        self.select_all_button=ttk.Button(actions,text='全选',command=lambda:self.select_files(self.source_tree,'all'))
        self.select_none_button=ttk.Button(actions,text='取消全选',command=lambda:self.select_files(self.source_tree,'none'))
        self.invert_button=ttk.Button(actions,text='反选',command=lambda:self.select_files(self.source_tree,'invert'))
        for button in [self.select_all_button,self.select_none_button,self.invert_button]:button.pack(side='left',padx=(0,6))
        self.selection_count=tk.StringVar(value='已选 0 / 0 个文件')
        ttk.Label(actions,textvariable=self.selection_count).pack(side='left',padx=5)
        self.start_button = ttk.Button(actions, text="开始处理", style="Accent.TButton", command=self.start)
        self.start_button.pack(side="right")
        self.stop_button = ttk.Button(actions, text="停止处理", command=self.stop, state="disabled")
        self.stop_button.pack(side="right", padx=10)
        resume_row=ttk.Frame(self.input_tab)
        resume_row.grid(row=4,column=0,sticky='ew',pady=(2,0))
        self.resume_button=ttk.Button(resume_row,text='继续跑未通过（0）',command=self.retry_failed)
        self.resume_button.pack(side='left')
        self.load_latest_button=ttk.Button(resume_row,text='载入最近批次',command=self.load_latest_script_results)
        self.load_latest_button.pack(side='right')
        ttk.Label(resume_row,text='显示').pack(side='left',padx=(10,4))
        self.script_filter=tk.StringVar(value='全部结果')
        self.script_filter_combo=ttk.Combobox(resume_row,textvariable=self.script_filter,state='readonly',width=10,
                                             values=['全部结果','不通过','通过','已保存','待解析','已删除','重复跳过','待处理','处理中'])
        self.script_filter_combo.pack(side='left')
        self.script_summary=tk.StringVar(value='扫描文件后选择本次处理范围')
        self.script_summary_label=ttk.Label(resume_row,textvariable=self.script_summary,foreground='#617082')
        self.script_summary_label.pack(side='left',padx=10)
        self.script_filter.trace_add('write',lambda *_:self.apply_script_filter())
        for control in (choose_button,self.scan_button,self.select_all_button,self.select_none_button,
                        self.invert_button,self.stop_button,self.resume_button,self.load_latest_button):
            control.configure(style='Task.TButton')
        self.start_button.configure(style='TaskAccent.TButton')

    def build_results(self):
        self.build_ai_results()

    def build_ragflow(self):
        # Keep every connection control reachable when the progress pane is expanded.
        container = self.rag_tab
        canvas = tk.Canvas(container, background="#f3f5f7", highlightthickness=0, height=1)
        scrollbar = ttk.Scrollbar(container, command=canvas.yview)
        scrollbar.pack(side="right", fill="y")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(fill="both", expand=True)
        content = ttk.Frame(canvas)
        item = canvas.create_window((0, 0), window=content, anchor="nw")
        content.bind("<Configure>", lambda _: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>", lambda event: canvas.itemconfigure(item, width=event.width))
        self.build_ragflow_fields(content)

    def build_ragflow_fields(self, content):
        self.rag_content=content
        ttk.Label(content, text="连接 RAGFlow", font=("Microsoft YaHei UI", 14, "bold")).pack(anchor="w", pady=(0, 12))
        for label, variable, hidden in [("服务地址", self.url, False), ("API Key（Windows 加密保存）", self.key, True)]:
            ttk.Label(content, text=label).pack(anchor="w", pady=(10, 4))
            ttk.Entry(content, textvariable=variable, show="•" if hidden else "", width=80).pack(anchor="w", fill="x")
        connection_row=ttk.Frame(content)
        connection_row.pack(fill='x',pady=14)
        self.rag_connection_button=ttk.Button(connection_row, text="连接并读取知识库", command=self.connect)
        self.rag_connection_button.pack(side='left')
        self.rag_connection_status=ttk.Label(connection_row,textvariable=self.connection,
                                             foreground='#617082',wraplength=850)
        self.rag_connection_status.pack(side='left',fill='x',expand=True,padx=(12,0))
        connection_row.bind('<Configure>',lambda event:self.rag_connection_status.configure(
            wraplength=max(180,event.width-self.rag_connection_button.winfo_reqwidth()-12)),add='+')
        ttk.Label(content, text="md 知识库（论文正文与论文侧资产）").pack(anchor="w", pady=(8, 4))
        self.dataset_combo = ttk.Combobox(content, textvariable=self.dataset, state="readonly", width=80)
        self.md_dataset_combo = self.dataset_combo
        self.paper_dataset_combo = self.dataset_combo
        self.dataset_combo.pack(anchor="w", fill="x")
        ttk.Label(content, text="图片知识库（图片与图片文本投影）").pack(anchor="w", pady=(10, 4))
        self.image_dataset_combo = ttk.Combobox(content, textvariable=self.image_dataset, state="readonly", width=80)
        self.image_dataset_combo.pack(anchor="w", fill="x")
        ttk.Button(content, text="重试交接", command=self.retry_handoff).pack(anchor="w", pady=(10, 0))
        ttk.Separator(content).pack(fill="x", pady=8)
        ttk.Label(content, text="工作分工", font=("Microsoft YaHei UI", 12, "bold")).pack(anchor="w", pady=(12, 8))
        ttk.Label(content, text="新批次：程序准备完整材料，模型按现有 MD 提取，程序执行校验、脱敏、切片、封包并落盘排队。\nRAGFlow：接收现成切片，生成向量和索引；程序逐专业核对内容、图片和检索结果，全部确认才算入库完成。\n旧格式结果保留原交接方式。停止本地处理后，已经登记的成品仍按原上传队列继续交付。", justify="left", wraplength=950).pack(anchor="w")
        ttk.Button(content, text="打开原程序文件夹", command=lambda: os.startfile(str(__import__('project_paths').ROOT))).pack(anchor="w", pady=20)

    def build_history(self):
        self.history_tab.columnconfigure(0,weight=1);self.history_tab.rowconfigure(1,weight=1)
        toolbar=ttk.Frame(self.history_tab);toolbar.grid(row=0,column=0,sticky='ew')
        ttk.Button(toolbar, text="刷新记录", command=self.refresh_history).pack(side='left')
        ttk.Button(toolbar, text="查看所选批次", command=self.load_history).pack(side='right')
        self.history_tree = self.tree(self.history_tab, ["批次", "开始时间", "状态", "文档数", "说明"], [245, 175, 85, 70, 420],grid_row=1)
        self.history_tree.bind("<Double-1>", lambda _: self.load_history())
        self.build_preextract_monitor()

    def async_task(self, fn, on_done, kind='operation'):
        if self.busy:
            self.state.set("当前操作尚未结束，请稍候。")
            return
        self.busy = True
        self.task_kind=kind
        token = object()
        self._active_task = token
        self.task_started = time.monotonic()
        self.stage_started = self.task_started
        self.update_controls()
        def worker():
            try:
                result = fn()
                self.events.put(("task_done", (token, on_done, result)))
            except Exception as exc:
                self.events.put(("task_error", (token, str(exc))))
        threading.Thread(target=worker, daemon=True).start()

    def cancel_model_task(self):
        # Only model settings requests may detach from their network worker.
        # Document processing retains its existing stop/save behaviour.
        if self.task_kind != 'model_settings':
            return
        self._active_task = None
        self.busy = False
        self.task_kind = None
        self.update_elapsed()
        self.task_started = None
        self.stage_started = None
        self.elapsed.set('模型读取/测试已停止')
        self.update_controls()

    def poll(self):
        started=time.monotonic()
        try:
            # Leave time for input and paint events even when workers keep
            # publishing progress faster than the result tables can refresh.
            for _ in range(32):
                event, value = self.events.get_nowait()
                if event == 'preextract_monitor':
                    self.finish_preextract_monitor(value)
                    break
                if event == 'live_results':
                    self.finish_live_results(value)
                    break
                if event in {'dataset_read_done','dataset_read_error'}:
                    self._finish_dataset_read(event,value)
                    continue
                if event=='dataset_read_idle_error':
                    self.connection.set('知识库读取失败：'+value)
                    self.rag_activity.set(self.connection.get())
                    continue
                if event in {'task_done', 'task_error'}:
                    token, *payload = value
                    if token is not self._active_task:
                        continue
                    self._active_task = None
                    if event == 'task_done':
                        event, value = 'done', tuple(payload)
                    else:
                        event, value = 'error', payload[0]
                if event == "progress":
                    self.show_script_progress(value)
                elif event == 'rag_progress':
                    self.rag_activity.set(value)
                elif event == "text":
                    self.connection.set(value)
                else:
                    task_kind=self.task_kind
                    self.busy = False
                    self.update_controls()
                    self.update_elapsed()
                    self.task_kind=None
                    self.task_started = None
                    if event == "done":
                        callback, result = value
                        callback(result)
                    else:
                        if task_kind=='processing' and not getattr(self,'_active_ai_retry',False):
                            self.show_progress({"stage": "脚本操作失败", "message": value, "level": "error"})
                        if self.current and task_kind=='processing':
                            self.current = self.store.load(self.current["run_id"])
                            self.render_results()
                            self.show_script_results(self.current)
                        self.refresh_source_upload_marks()
                        self.error(value)
                if time.monotonic()-started>=0.05:
                    break
        except queue.Empty:
            pass
        if self.live_results_active() or self.idle_delivery_refresh_needed():
            self.request_live_results()
        self.request_preextract_monitor()
        self.update_elapsed()
        self.root.after(150, self.poll)

    def error(self, message):
        self.state.set(str(message))
        messagebox.showerror("操作未完成", str(message), parent=self.root)

    def settings(self):
        write_json(self.store.root / "settings.json", {"input_dir": self.folder.get(), "ragflow_url": self.url.get()})
        from auto_config import save_key,save_config
        from auto_ui import read_ai_form
        save_key(self.store.root,self.key.get(),'ragflow')
        if hasattr(self,'ai_vars'):
            config,key=read_ai_form(self);save_config(self.store.root,config,key,self.ai_vars['vision_key'].get(),self.ai_vars['vision_fallback_key'].get())

    def choose(self):
        folder = filedialog.askdirectory(parent=self.root, title="选择论文输入目录")
        if folder:
            self.folder.set(folder)

    def scan(self):
        if self.busy:return
        try:
            limit = int(self.limit.get())
            folder = Path(self.folder.get())
            if not self.folder.get().strip():
                raise ValueError("请选择输入文件夹。")
            self.settings()
            self.state.set("正在扫描文件…")
            from upload_tracking import UploadIndex,mark_sources
            base_url=self.url.get()
            self.async_task(lambda: mark_sources(scan_sources(folder,limit,self.store.root),UploadIndex(self.store.history()),base_url),self.scanned)
        except Exception as exc:
            self.error(exc)

    def scanned(self, rows):
        if any('upload_mark' not in row for row in rows):
            from upload_tracking import UploadIndex,mark_sources
            rows=mark_sources(rows,UploadIndex(self.store.history()),self.url.get())
        self._script_manifest=None
        self.script_filter.set('全部结果')
        self.render_script_rows([dict(row, script_status='待处理', script_reason='', previous_error='') for row in rows], select_all=True)
        capped = len(rows) >= int(self.limit.get())
        self.state.set(f"扫描到 {len(rows)} 个文件。" + ('已达到扫描上限，如需全部处理请调大上限重新扫描。' if capped else '请选好本次任务设置后开始。'))

    def start(self):
        if self.busy:
            return
        if self._script_manifest is not None:
            self.state.set('本批请使用“继续跑未通过”；开始新批次请重新扫描文件。')
            return
        selected = [self.sources[int(i)] for i in self.source_tree.selection()]
        if not selected:
            return self.error("请扫描并选择文件。")
        self.start_automatic(selected)

    def start_automatic(self,selected):
        operation=getattr(self, '_pending_operation', 'process');self._pending_operation='process'
        try:
            from auto_ui import read_ai_form
            problem=self.task_setup_problem() if operation!='retry_ai' else ''
            if problem:
                raise ValueError(problem)
            config,key=read_ai_form(self)
            # Keep the tested selector, batch runner, controls and result pages.
            # Only new processing uses the complete one-request MD contract;
            # historical native checkpoints retain their original recovery path.
            config['extraction_mode']='paper-preextract/1.0'
            config.pop('preextract_delivery_mode',None)
            previous_mode=(self.current or {}).get('task_settings',{}).get('preextract_delivery_mode')
            if operation=='process' or (operation in {'retry_failed','retry_ai'} and previous_mode=='disk_queue_v1'):
                config['preextract_delivery_mode']='disk_queue_v1'
            if operation=='retry_ai' and any(
                    (row.get('prepared_checkpoint') or {}).get('format')!='paper-preextract-prepared/1'
                    for row in selected):
                config.pop('extraction_mode',None)
            if operation=='retry_ai':
                config['task_settings']=copy.deepcopy(self.current.get('task_settings',{}))
                config['task_settings'].setdefault('destinations', {'paper': {}, 'image': {}})
                config['task_settings'].update(text_model=config['text_model'],
                    vision_model=config['vision_model']['primary']['model'],
                    vision_fallback_model=config['vision_model']['fallback'].get('model','')
                        if config['vision_model']['fallback'].get('enabled') else '',
                    concurrency=copy.deepcopy(config['concurrency']))
            else:
                config['task_settings']=self.task_settings_snapshot(config)
            if config.get('preextract_delivery_mode')=='disk_queue_v1':
                config['task_settings']['preextract_delivery_mode']='disk_queue_v1'
            config['task_datasets']=config['task_settings']['destinations']
            config['auto_upload']=bool(config.get('auto_upload',True) and
                (config.get('extraction_mode')=='paper-preextract/1.0' or
                 (config['task_datasets'].get('paper') or {}).get('id')))
            if operation in {'retry_failed','retry_ai'}:
                config['retry_of']=self.current['run_id']
            if not confirm_serial_start(len(selected), config):
                self.state.set('已取消串行模式处理。启用并发后再开始可缩短预计耗时。')
                return
            self.settings()
            self.processor=AutoProcessor(self.store,config,key,self.ai_vars['vision_key'].get(),self.ai_vars['vision_fallback_key'].get())
            if config['auto_upload'] and self.key.get().strip():
                self.processor.rag_client=RagflowClient(self.url.get(),self.key.get())
        except Exception as exc: return self.error(str(exc))
        self._active_ai_retry=operation=='retry_ai'
        if not self._active_ai_retry:self.clear_progress()
        from billing import summarize
        from billing_ui import update_progress
        update_progress(self,{'billing':summarize([])})
        if self._active_ai_retry:
            self.ai_retry_hint.set(f'正在续跑 {len(selected)} 篇 AI 未通过项；复用脚本结果，采用当前 AI 设置。')
            self.state.set(self.ai_retry_hint.get())
        else:
            self.show_progress({"stage": "准备开始", "completed": 0, "total": len(selected)})
            self.state.set(f"正在启动处理，共 {len(selected)} 个源文件…")
        self.processor.stop.clear()
        self._script_progress_stamp=None
        self._live_results_enabled=True
        self._live_result=None
        self._live_requested_run=None
        self._live_waiting_run=True
        self._live_next_read=0
        if operation not in {'retry_failed','retry_ai'}:
            self._script_manifest=None
            self.render_script_rows([dict(row, script_status='待处理', script_reason='', previous_error='') for row in selected], select_all=True)
        self.async_task(lambda: self.processor.run(selected, lambda state: self.events.put(("progress", state.copy())), operation=operation), self.finished,kind='processing')

    def resume_latest(self):
        # Loading historical results is separate from starting paid processing.
        self.load_latest_script_results()

    def retry_failed(self):
        if self.busy or not self.current: return
        self.current=self.store.load(self.current['run_id'])
        if self.current.get('status')=='running':
            self.state.set('该批次仍在处理，请等待完成后再续跑。')
            return
        self.show_script_results(self.current)
        selected = retry_sources(self._script_manifest,scope='script')
        if not selected:
            self.state.set('本批没有可重试的脚本未通过文件。')
            return
        missing=sum(not Path(r['path']).is_file() for r in selected)
        if missing:
            self.error(f'有 {missing} 个待续跑源文件不存在，请恢复原路径后重试。已完成结果保留。')
            return
        self._pending_operation='retry_failed';self.start_automatic(selected)

    def stop(self):
        if not self.busy or self.task_kind!='processing':return
        self.processor.stop.set()
        self.update_controls()
        self.state.set("已请求停止，当前解析或网络请求返回后保存进度。")
        if getattr(self,'_active_ai_retry',False):
            self.ai_retry_hint.set('已请求停止 AI 续跑；当前请求结束后保存，之后可继续未完成项。')
        else:
            self.show_progress({"stage": "已请求停止", "message": "当前操作返回后保存，未完成来源记为不通过，可重新处理", "level": "warning"})

    def finished(self, manifest):
        self.current = manifest
        from billing_ui import update_progress
        update_progress(self,{**manifest,'billing':manifest.get('billing')})
        self.major.set("全部专业")
        self.render_results()
        self.show_script_results(manifest)
        if manifest.get('operation')=='retry_ai':
            self.ai_retry_hint.set('AI 续跑已结束；可筛选查看最新结果，仍未通过的可调整后继续。')
        else:
            self.progress_bar.configure(value=manifest.get('processed_sources',0), maximum=max(1,manifest.get('source_count',0)))
            self.progress_count.set(f"本次脚本完成 {manifest.get('processed_sources',0)}/{manifest.get('source_count',0)}")
        self.refresh_source_upload_marks()
        self.refresh_history()
        self.state.set(self.result_summary.get() if manifest.get('operation')=='retry_ai' else self.script_summary.get())
        self._active_ai_retry=False
        self.update_ai_retry_controls()
        view = manifest.get('human_view', {})
        if view.get('status') == 'failed':
            self.state.set(manifest['message'] + '；浏览目录未更新，可点击“打开批次目录”重试。')

    def _selected_target_dataset(self):
        """Return the currently selected RAGFlow dataset, if this connection is current."""
        labels = list(self.dataset_combo.cget('values')) if hasattr(self, 'dataset_combo') else []
        label = self.dataset.get().strip()
        index = labels.index(label) if label in labels else -1
        if getattr(self, 'connected_url', None) != self.url.get().strip().rstrip('/'):
            return None
        return self.datasets[index] if 0 <= index < len(self.datasets) else None

    def render_results(self):
        self.render_preprocess_results()
        self.render_rag_results()

    def render_preprocess_results(self):
        self.render_ai_results()

    @staticmethod
    def result_file_stamp(path):
        try:
            stat=Path(path).stat()
            return stat.st_mtime_ns,stat.st_size
        except OSError:
            return None

    def result_history_stamp(self):
        if self.live_results_active():
            return (getattr(self,'_live_result',None) or {}).get('history_stamp',())
        return tuple((path.parent.name,self.result_file_stamp(path))
                     for path in sorted(self.store.runs.glob('*/manifest.json')))

    def result_snapshot(self,manifest=None,*,refresh=True):
        """Reuse a read-only projection until its durable retry records change.

        Explicit render calls still honor unsaved edits in the supplied object.
        Notebook switches also reload the active manifest if its file changed.
        """
        if self.live_results_active():
            return self.live_result_snapshot()
        manifest=self.current if manifest is None else manifest
        if not manifest:return None
        from preextract_delivery_queue import queue_stamp,project_manifest
        delivery_stamp=queue_stamp(self.store.root)
        delivery_changed=delivery_stamp!=getattr(self,'_result_delivery_stamp',None)
        if delivery_changed:
            # Uploads write their own durable state, so the producer manifest
            # can remain unchanged while the original result page is open.
            project_manifest(self.store.root,manifest)
            self._result_delivery_stamp=delivery_stamp
        cached=getattr(self,'_result_snapshot_cache',None)
        current_stamp=self.result_file_stamp(self.store.manifest_path(manifest['run_id']))
        if not refresh and cached and cached[0] is self.store and cached[1] is manifest:
            unchanged=not delivery_changed and all(self.result_file_stamp(self.store.manifest_path(run_id))==stamp
                          for run_id,stamp in cached[2])
            if unchanged:return cached[3]
            if current_stamp!=cached[2][0][1]:
                latest=self.store.load(manifest['run_id'])
                if latest:
                    if manifest is self.current:self.current=latest
                    manifest=latest
        elif not refresh and manifest is self.current:
            # The first visit has no recorded file version to compare. Read
            # the current saved record before establishing the cache version.
            latest=self.store.load(manifest['run_id'])
            if latest:self.current=manifest=latest
        elif refresh:
            if cached and cached[0] is self.store and cached[1] is manifest:
                # A supplied object may predate a concurrent writer. Keep its
                # known read version; do not certify a newer file as loaded.
                current_stamp=cached[2][0][1]
            elif current_stamp is not None:
                saved=self.store.load(manifest['run_id'])
                if saved!=manifest:current_stamp=('not_loaded',)
        versions=[(manifest['run_id'],current_stamp)]
        workbench=self
        class Reader:
            def load(self,run_id):
                versions.append((run_id,workbench.result_file_stamp(workbench.store.manifest_path(run_id))))
                return workbench.store.load(run_id)
        snapshot=task_snapshot(Reader(),manifest)
        self._result_snapshot_cache=(self.store,manifest,tuple(versions),snapshot)
        return snapshot

    def selected_items(self):
        return self.selected_ai_items()

    def text_window(self, title, content):
        window = tk.Toplevel(self.root)
        window.title(title)
        window.geometry("950x680")
        box = tk.Text(window, wrap="word", font=("Microsoft YaHei UI", 11), padx=18, pady=15, background="#fafbfc", foreground="#243140")
        scroll = ttk.Scrollbar(window, command=box.yview)
        box.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        box.pack(fill="both", expand=True)
        box.insert("1.0", content)
        box.configure(state="disabled")

    def preview(self):
        self.preview_ai_result()

    def open_package(self):
        items=self.selected_items()
        if not items: return self.error('请选择一篇文档。')
        path = items[0].get('package_path') or str(self.store.manifest_path(self.current['run_id']).parent)
        try: os.startfile(path)
        except OSError: self.error('程序资料目录缺失或无法打开。')

    def review_evidence(self):
        items=self.selected_items()
        if not items: return self.error('请选择一篇文档。')
        if displayed_outcome(items[0], self.current)[0] != '通过':return self.error('此文件未生成通过的图表证据包，请查看不通过原因或程序资料。')
        if not items[0].get('package_path'): return self.error('旧版结果没有原生图表结构，请从原文件重新处理。')
        from evidence_ui import open_review
        open_review(self,items[0],read_only=True)

    def load_result_datasets(self, event=None):
        remote=self.tabs.select()==str(self.rag_result_tab)
        if hasattr(self,'progress_panel'):
            present=str(self.progress_panel) in tuple(str(p) for p in self.panes.panes())
            if remote and present:self.panes.forget(self.progress_panel)
            elif not remote and not present:self.panes.add(self.progress_panel,minsize=145,height=210,stretch='never')
        if remote:self.render_rag_results(refresh=False)
        elif self.tabs.select()==str(self.result_tab):self.render_ai_results(refresh=False)

    def can_read_datasets(self):
        return not getattr(self,'_dataset_read_task',None) and (not self.busy or self.task_kind=='processing')

    def _dataset_connection_identity(self):
        import hashlib
        return (self.url.get().strip().rstrip('/'),hashlib.sha256(self.key.get().strip().encode()).hexdigest())

    def _finish_dataset_read(self,event,value):
        token,identity,result=value
        if token is not self._dataset_read_task:return
        self._dataset_read_task=None
        if identity!=self._dataset_connection_identity():
            message='连接参数已变更，请重新读取知识库。'
            self.connection.set(message)
            self.rag_activity.set(message)
        elif event=='dataset_read_error':
            message='知识库读取失败：'+str(result)
            self.connection.set(message)
            self.rag_activity.set(message)
            if not self.busy:self.state.set(message)
        else:
            self.connected(result,identity[0])
            self.rag_activity.set(self.connection.get())
        self.update_controls()

    def connect(self):
        if not self.can_read_datasets():
            self.connection.set('正在读取知识库，请稍候。' if self._dataset_read_task else '请等待当前操作结束后读取知识库。')
            return
        query_token=None
        api_key=self.key.get().strip()
        try:
            client = RagflowClient(self.url.get(), self.key.get())
            self.url.set(client.base_url)
            self.settings()
            self.connection.set('正在读取 RAGFlow 知识库…')
            self.rag_activity.set(self.connection.get())
            if self.busy and self.task_kind=='processing':
                token=object()
                query_token=token
                identity=self._dataset_connection_identity()
                self._dataset_read_task=token
                self.update_controls()
                def read_datasets():
                    try:
                        datasets=client.datasets()
                        self.events.put(('dataset_read_done',(token,identity,datasets)))
                    except Exception as exc:
                        self.events.put(('dataset_read_error',(token,identity,str(exc).replace(client.api_key,'[密钥]'))))
                threading.Thread(target=read_datasets,daemon=True).start()
            else:
                self.state.set("正在连接 RAGFlow…")
                def read_idle_datasets():
                    try:return client.datasets()
                    except Exception as exc:
                        message=str(exc).replace(client.api_key,'[密钥]')
                        self.events.put(('dataset_read_idle_error',message))
                        raise RuntimeError(message) from None
                self.async_task(read_idle_datasets, lambda datasets:self.connected(datasets,client.base_url))
        except Exception as exc:
            if query_token is not None and self._dataset_read_task is query_token:
                self._dataset_read_task=None
                self.update_controls()
            message=str(exc).replace(api_key,'[密钥]') if api_key else str(exc)
            self.connection.set('知识库读取失败：'+message)
            self.rag_activity.set(self.connection.get())
            self.error(message)

    def connected(self, datasets, base_url=None):
        from collections import Counter
        from auto_config import normalize_dataset_map
        current_url = self.url.get().strip().rstrip('/')
        if base_url is not None and base_url != current_url:
            self.datasets=[]
            for combo in (self.dataset_combo,self.image_dataset_combo,self.task_dataset_combo,self.task_image_dataset_combo,
                          self.rag_dataset_combo,self.rag_image_dataset_combo):combo.configure(values=[])
            self.dataset.set('');self.image_dataset.set('')
            self.connection.set('服务地址已变更，请重新读取知识库。')
            self.rag_activity.set(self.connection.get())
            self.state.set(self.connection.get())
            return
        same_connection = getattr(self,'connected_url',None) == current_url
        index = self.dataset_combo.current()
        image_index = self.image_dataset_combo.current()
        previous_id = self.datasets[index]['id'] if same_connection and 0 <= index < len(self.datasets) else None
        previous_image_id = (self.datasets[image_index]['id'] if same_connection and
                             0 <= image_index < len(self.datasets) else None)
        try:
            route_map = normalize_dataset_map(getattr(self,'dataset_map',{}))
        except ValueError:
            route_map = {}
        route_major = getattr(self, 'route_major', None)
        route = route_map.get(self.major.get()) or (route_map.get(route_major.get(),{}) if route_major else {})
        preferred_id = previous_id if same_connection else route.get('paper','')
        preferred_image_id = previous_image_id if same_connection else route.get('image','')
        self.datasets = datasets
        self.connected_url = current_url
        names = Counter(d['name'] for d in datasets)
        labels = [d['name'] if names[d['name']] == 1 else f"{d['name']}  [{d['id']}]" for d in datasets]
        if len(set(labels)) != len(labels):
            labels = [f"{d['name']}  [{d['id']}]" for d in datasets]
        for combo in (self.dataset_combo,self.image_dataset_combo,self.task_dataset_combo,self.task_image_dataset_combo,
                      self.rag_dataset_combo,self.rag_image_dataset_combo):combo.configure(values=labels)
        selected = next((i for i,d in enumerate(datasets) if d['id'] == preferred_id),None)
        if selected is None and not preferred_id and not same_connection and labels:selected=0
        self.dataset.set(labels[selected] if selected is not None else '')
        image_selected = next((i for i,d in enumerate(datasets) if d['id'] == preferred_image_id),None)
        self.image_dataset.set(labels[image_selected] if image_selected is not None else '')
        self.connection.set(f"连接成功，共 {len(datasets)} 个知识库。分别选择 md 知识库与图片知识库。"+
            ('处理中任务设置已固定，此处选库用于后续导入。' if self.busy and self.task_kind=='processing' else ''))
        self.rag_activity.set(self.connection.get())
        if not (self.busy and self.task_kind=='processing'):
            self.state.set("RAGFlow 连接成功" if datasets else "连接成功，请先在 RAGFlow 创建专业知识库")

    def upload(self):
        if self.busy:
            return self.error("请等待当前操作结束。")
        items = self.selected_rag_items()
        if not items:
            return self.error("请选择要导入的文件。")
        labels = list(self.dataset_combo.cget('values'))
        index = labels.index(self.dataset.get()) if self.dataset.get() in labels else -1
        if index < 0 or not self.datasets:
            return self.error("请先选择 md 知识库；尚未读取时点击“刷新知识库”。")
        try:
            client = RagflowClient(self.url.get(), self.key.get())
            if client.base_url != self.connected_url:
                raise ValueError("服务地址已变更，请重新连接知识库。")
            majors = {item["major"] for item in items}
            if len(majors) != 1:
                raise ValueError("每次只导入一个专业，请先按专业筛选。")
            if any(displayed_outcome(i, self.current)[0] != '通过' for i in items): raise ValueError('仅导入全自动处理通过的文件；旧版或不通过结果请从源文件重新处理')
            major = next(iter(majors))
            image_labels = list(self.image_dataset_combo.cget('values'))
            image_index = image_labels.index(self.image_dataset.get()) if self.image_dataset.get() in image_labels else -1
            image_dataset = self.datasets[image_index] if 0 <= image_index < len(self.datasets) else None
            from auto_config import normalize_dataset_map
            configured = normalize_dataset_map(getattr(self,'dataset_map',{})).get(major,{})
            if image_dataset is None and configured.get('image'):
                image_dataset = next((d for d in self.datasets if d.get('id')==configured['image']),None)
            run_id = self.current["run_id"]
            routes={'paper':self.datasets[index],'image':image_dataset}
            from auto_config import load_config
            upload_config=load_config(self.store.root)
            concurrency=upload_config.get('concurrency',{})
            if upload_config.get('work_pool_enabled'):
                from auto_ragflow import handoff
                def queue_selected():
                    handoff(self.store,run_id,client,upload_config.get('dataset_map',{}),
                            progress=lambda stage,message:self.events.put(('text',stage+'：'+message)),
                            auto_parse=False,legacy_dataset_map_fallback=False,
                            target_datasets=routes,item_ids=[item['id'] for item in items],
                            upload_config=dict(upload_config,manual_reviewed=True,
                                               manual_request=True))
                    return len(items)
                self.async_task(queue_selected,lambda count:self.queued(run_id,count))
            else:
                self.async_task(lambda: upload_routed_items(self.store, run_id, [item["id"] for item in items], client, routes,
                                lambda text: self.events.put(("text", text)),concurrency=concurrency,upload_config=upload_config),
                                lambda count: self.uploaded(run_id, count))
        except Exception as exc:
            self.error(exc)

    def uploaded(self, run_id, count):
        self.current = self.store.load(run_id)
        self.render_results()
        self.refresh_source_upload_marks()
        self.state.set(f"本次新增导入 {count} 篇。请打开 RAGFlow 检查文件并启动已配置的 Pipeline。")

    def queued(self, run_id, count):
        self.current=self.store.load(run_id)
        self.render_results()
        self.render_rag_results()
        self.state.set(f'本次已有 {count} 篇进入上传队列；请在 RAGFlow 处理结果页查看确认状态。')

    def refresh_source_upload_marks(self):
        from upload_tracking import UploadIndex,mark_sources
        self.sources=mark_sources(self.sources,UploadIndex(self.store.history()),self.url.get())

    def refresh_upload_display(self):
        self.render_results()
        # During processing source controls are locked. Rehash their originals
        # only after completion, never on every URL edit in the connection form.
        if not self.live_results_active():self.refresh_source_upload_marks()
        self.update_controls()

    def open_ragflow(self):
        from urllib.parse import urlsplit
        url = self.url.get().strip().rstrip("/")
        if urlsplit(url).scheme not in {"http", "https"}:
            return self.error("请填写有效的 http/https 服务地址。")
        webbrowser.open(url + "/datasets")

    def open_run(self):
        if self.busy:
            return self.error('请等待当前操作结束。')
        if not self.current:
            return self.error('请先选择或完成一个批次。')
        run_id = self.current['run_id']
        self.state.set('正在整理中文浏览目录…')
        self.show_progress({'stage': '整理浏览目录', 'message': '按状态和专业整理，程序资料保留在后台'})
        self.async_task(lambda: self.store.refresh_view(run_id), lambda result: self.opened_view(run_id, result))

    def opened_view(self, run_id, result):
        if result.get('status') != 'ready':
            return self.error('浏览目录未更新：' + result.get('error', '未知原因'))
        if self.current and self.current['run_id'] == run_id:
            self.current = self.store.load(run_id)
            self.render_results()
        counts = result['counts']
        message = '已打开浏览目录：' + ' · '.join(f'{label} {count} 项' for label,count in counts.items())
        if result.get('failed_records'):
            message += f"（含 {result['failed_records']} 项解析失败记录）"
        if result.get('preserved'):
            message += f"；已保留 {len(result['preserved'])} 份手工修改，位置见处理报告。"
        self.state.set(message)
        self.show_progress({'stage': '浏览目录已更新', 'message': message})
        try:
            os.startfile(result['path'])
        except OSError as exc:
            self.error('目录已生成，但无法自动打开：' + str(exc))

    def report(self):
        if not self.current:
            return self.error("请先选择或完成一个批次。")
        m = self.current
        lines = [f"批次：{m['run_id']}", f"状态：{STATUS.get(m['status'], m['status'])}", m["message"], "", "逐篇结果"]
        from billing import summary_text
        lines.insert(3,'费用：'+summary_text(m.get('billing')))
        lines += [f"{i['id']} | {i['major']} | {displayed_outcome(i, m)[0]} | {displayed_outcome(i, m)[1]}" for i in result_items(m)]
        from upload_tracking import UploadIndex
        upload_index=UploadIndex(self.store.history())
        lines += ['', 'RAGFlow 处理结果（当前服务）']
        for item in result_items(m):
            state = self._remote_item_status(item, upload_index)
            lines.append(f"{item['id']} | {state['target']} | {state['upload']} | {state['process']} | {state['reason']}")
        lines += ['', '逐篇 Token 与人民币预估费用']
        lines += [i.get('title',i['id'])+' | '+summary_text(i.get('billing')) for i in m['items']]
        lines += ["", "失败、跳过与去重记录"]
        lines += [f"{e['status']} | {e.get('source', '')} | {e.get('reason', '')}" for e in m["events"]]
        view = m.get('human_view', {})
        lines += ['', '结果浏览目录', view.get('path', '点击“打开批次目录”生成'),
                  '通过仅表示全自动预处理合格，不代表学术等级。旧版未完成自动处理的记录归不通过，需从源文件重新处理。']
        if view.get('error'):
            lines.append('浏览目录更新失败：' + view['error'])
        lines += [f"{issue['id']} | {issue['reason']}" for issue in view.get('issues', [])]
        saved_edits = self.store.manifest_path(m['run_id']).parent / 'human_view_edits'
        if saved_edits.is_dir():
            lines += ['', '浏览版手工修改的保留副本（程序资料）', str(saved_edits)]
        self.text_window("处理报告", "\n".join(lines))

    def refresh_history(self):
        from auto_view import batch_summary
        if not hasattr(self, "history_tree"):
            return
        self.history_tree.delete(*self.history_tree.get_children())
        for m in self.store.history():
            self.history_tree.insert("", "end", iid=m["run_id"], values=(m["run_id"], m["started_at"], STATUS.get(m["status"], m["status"]), len(m["items"]), batch_summary(m)))

    def load_history(self):
        if self.busy:
            return self.error("请等待当前操作结束。")
        selection = self.history_tree.selection()
        if selection:
            self.current = self.store.load(selection[0])
            self.load_progress(self.current)
            self.major.set("全部专业")
            self.render_results()
            self.show_script_results(self.current)
            self.tabs.select(self.input_tab)

    def close(self):
        if self.busy and self.task_kind == 'model_settings':
            cancel = getattr(self, '_model_cancel', None)
            if cancel:
                cancel()
            else:
                self.cancel_model_task()
        if self.busy:
            messagebox.showinfo("任务正在进行", "当前任务仍在处理。可以最小化窗口，或点击“停止处理”，保存后再关闭。", parent=self.root)
            return
        self.settings()
        self.root.destroy()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--monitor', action='store_true', help='打开论文实时进度，不恢复或改写外部运行批次')
    parser.add_argument("--data-dir", default=str(Path(__file__).resolve().parent / "data"))
    parser.add_argument("--limit", type=int, default=None, help="本次扫描最多载入的源文件数")
    parser.add_argument("--smoke-test", action="store_true")
    args = parser.parse_args(argv)
    if args.limit is not None and not 1 <= args.limit <= MAX_SCAN_FILES:
        parser.error(f"--limit 必须为 1–{MAX_SCAN_FILES}")
    mutex = None
    if os.name == "nt" and not args.smoke_test:
        import ctypes
        import hashlib
        kernel = ctypes.windll.kernel32
        kernel.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
        kernel.CreateMutexW.restype = ctypes.c_void_p
        name = "Local\\PaperUpstream_" + hashlib.sha256(str(Path(args.data_dir).resolve()).lower().encode()).hexdigest()[:20]
        mutex = kernel.CreateMutexW(None, False, name)
        if not mutex:
            raise RuntimeError("无法创建程序运行锁。")
        if kernel.GetLastError() == 183:
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel.CloseHandle(mutex)
            ctypes.windll.user32.MessageBoxW(None, "程序已经运行，请从任务栏打开现有窗口。", "论文知识库预处理", 64)
            return
    # The new entrypoint is independent of the obsolete E:/uv/.venv interpreter.
    root = tk.Tk()
    if args.smoke_test:
        root.withdraw()
    app = Workbench(root, Path(args.data_dir), recover=not (args.smoke_test or args.monitor))
    if args.monitor and not args.smoke_test:
        app.show_preextract_monitor()
    if args.limit is not None:
        app.limit.set(str(args.limit))
    if args.smoke_test:
        root.update_idletasks()
        assert len(app.tabs.tabs()) == 6
        assert not app.busy and not app.processor.stop.is_set()
        root.destroy()
        print("Workbench UI smoke test passed: 6 tabs, idle, no processing started.")
    else:
        try:
            root.mainloop()
        finally:
            if mutex:
                kernel.CloseHandle.argtypes = [ctypes.c_void_p]
                kernel.CloseHandle(mutex)


if __name__ == "__main__":
    main()
