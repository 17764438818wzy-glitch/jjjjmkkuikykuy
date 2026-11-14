"""Visible, read-only RAGFlow status window for a live education batch."""
from __future__ import annotations

import argparse
import json
import tkinter as tk
import webbrowser
from datetime import datetime
from pathlib import Path
from tkinter import ttk

from education_stream_status import IMAGE_NAME, PAPER_NAME, monitor_snapshot

ROOT = Path(__file__).resolve().parent


class Monitor:
    def __init__(self, root: tk.Tk, run_id: str):
        self.root = root
        self.run_id = run_id
        self.batch_path = ROOT / 'data' / 'runs' / run_id / 'manifest.json'
        self.ledger_path = ROOT / 'data' / 'private' / 'education_full_ingest' / run_id / 'receipts.json'
        selection_path = ROOT / 'data' / 'private' / 'education_pilot' / 'selection.json'
        self.pilot_ids = {row['stable_paper_id'] for row in json.loads(
            selection_path.read_text(encoding='utf-8'))}
        settings = json.loads((ROOT / 'data' / 'settings.json').read_text(encoding='utf-8'))
        self.ragflow_url = settings.get('ragflow_url', '').rstrip('/')
        root.title('教育论文 RAGFlow 实时状态')
        root.geometry('1120x690+90+70')
        root.minsize(820, 480)
        outer = ttk.Frame(root, padding=14)
        outer.pack(fill='both', expand=True)
        ttk.Label(outer, text='教育论文导入实时状态', font=('Microsoft YaHei UI', 17, 'bold')).pack(anchor='w')
        ttk.Label(outer, text=f'正文：{PAPER_NAME}    图片：{IMAGE_NAME}',
                  foreground='#38536b').pack(anchor='w', pady=(5, 2))
        ttk.Label(outer, text='此窗口只读，不干扰正在运行的预处理；每 5 秒读取一次导入回执。',
                  foreground='#667788').pack(anchor='w')
        self.summary = tk.StringVar(value='正在读取批次与回执…')
        ttk.Label(outer, textvariable=self.summary, font=('Microsoft YaHei UI', 10, 'bold')).pack(
            anchor='w', pady=(12, 7))
        actions = ttk.Frame(outer)
        actions.pack(fill='x')
        ttk.Label(actions, text='搜索').pack(side='left')
        self.search = tk.StringVar()
        ttk.Entry(actions, textvariable=self.search, width=35).pack(side='left', padx=7)
        self.filter = tk.StringVar(value='全部')
        ttk.Combobox(actions, textvariable=self.filter, state='readonly', width=14,
                     values=('全部', '已上传', '待上传', '解析中', '已完成', '失败待核查')).pack(side='left')
        ttk.Button(actions, text='立即刷新', command=self.refresh).pack(side='right', padx=(6, 0))
        ttk.Button(actions, text='打开 RAGFlow', command=lambda: webbrowser.open(
            self.ragflow_url + '/datasets')).pack(side='right')
        box = ttk.Frame(outer)
        box.pack(fill='both', expand=True, pady=(9, 0))
        columns = ('title', 'major', 'upload', 'process', 'admission', 'images', 'paper_id')
        self.table = ttk.Treeview(box, columns=columns, show='headings')
        for column, title, width in [
            ('title', '题目', 360), ('major', '专业', 90), ('upload', '上传', 90),
            ('process', 'RAGFlow 解析', 105), ('admission', '准入', 65),
            ('images', '图片项', 60), ('paper_id', '稳定 paper_id', 250),
        ]:
            self.table.heading(column, text=title)
            self.table.column(column, width=width, minwidth=55)
        bar = ttk.Scrollbar(box, command=self.table.yview)
        self.table.configure(yscrollcommand=bar.set)
        self.table.pack(side='left', fill='both', expand=True)
        bar.pack(side='right', fill='y')
        self.search.trace_add('write', lambda *_: self.refresh())
        self.filter.trace_add('write', lambda *_: self.refresh())
        self.detail = tk.StringVar(value='')
        ttk.Label(outer, textvariable=self.detail, foreground='#667788').pack(anchor='w', pady=(6, 0))
        self.refresh()

    def refresh(self):
        try:
            batch = json.loads(self.batch_path.read_text(encoding='utf-8'))
            ledger = json.loads(self.ledger_path.read_text(encoding='utf-8'))
            if batch.get('run_id') != self.run_id or ledger.get('batch_id') != self.run_id:
                raise ValueError('批次编号不一致')
            rows = monitor_snapshot(batch, ledger, self.pilot_ids)
            primary = ledger.get('primary') or {}
            done = sum(row.get('status') == 'done' for row in primary.values())
            failures = sum(row.get('status') == 'fail' for row in primary.values())
            self.summary.set(
                f"预处理 {batch.get('processed_sources', 0)}/{batch.get('source_count', 0)}  ·  "
                f"教育类通过 {len(rows)}  ·  原有试跑 {len(self.pilot_ids)}  ·  "
                f"本轮新增上传 {len(primary)}  ·  解析完成 {done}  ·  失败待核查 {failures}  ·  "
                f"图片库新增 {len(ledger.get('images') or {})} 项")
            needle = self.search.get().strip().casefold()
            wanted = self.filter.get()
            visible = [row for row in rows if
                       (not needle or needle in (row['title'] + row['major'] + row['id'] + row['paper_id']).casefold())
                       and (wanted == '全部' or wanted in (row['upload'], row['process']))]
            selected = set(self.table.selection())
            self.table.delete(*self.table.get_children())
            for row in visible:
                self.table.insert('', 'end', iid=row['id'], values=(
                    row['title'], row['major'], row['upload'], row['process'],
                    row['admission'], row['images'], row['paper_id']))
            self.table.selection_set([key for key in selected if key in self.table.get_children()])
            self.detail.set(f"显示 {len(visible)}/{len(rows)} 篇；上次读取 {datetime.now():%H:%M:%S}。"
                            '“失败待核查”不代表论文质量不合格。')
        except (OSError, ValueError, KeyError) as exc:
            self.summary.set('回执暂不可读取：' + str(exc)[:160])
        self.root.after(5000, self.refresh)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_id')
    parser.add_argument('--smoke-test', action='store_true')
    args = parser.parse_args()
    root = tk.Tk()
    if args.smoke_test:
        root.withdraw()
    monitor = Monitor(root, args.run_id)
    if args.smoke_test:
        assert monitor.table.get_children()
        root.destroy()
        print('Education RAGFlow monitor smoke test passed')
    else:
        root.mainloop()


if __name__ == '__main__':
    main()
