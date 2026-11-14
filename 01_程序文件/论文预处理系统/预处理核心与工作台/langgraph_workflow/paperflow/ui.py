"""简单控制台；打开界面不自动开始处理。"""
from __future__ import annotations

import threading
import tkinter as tk
from tkinter import ttk

from .service import Service


class Workbench:
    def __init__(self, root, config):
        self.root=root
        self.service=Service(config)
        self.stop_event=threading.Event()
        self.thread=None
        self.message=tk.StringVar(value='尚未开始；点击开始后持续监听和处理。')
        root.title('论文持续处理工作流')
        root.geometry('1000x760')
        box=ttk.Frame(root,padding=18); box.pack(fill='both',expand=True)
        ttk.Label(box,text='三轮处理 · 自动修复 · 专业入库',font=('Microsoft YaHei UI',15)).pack(anchor='w')
        ttk.Label(box,textvariable=self.message,wraplength=810).pack(anchor='w',pady=12)
        self.alert=tk.Label(box,text='',fg='#b00020',anchor='w',justify='left',wraplength=940)
        self.alert.pack(fill='x')
        self.metrics_text=tk.StringVar(value='')
        ttk.Label(box,textvariable=self.metrics_text,wraplength=940).pack(anchor='w',pady=6)
        buttons=ttk.Frame(box); buttons.pack(fill='x')
        for title,fn in [('开始/继续',self.start),('暂停调度',self.pause),('停止并保存',self.stop),('查看代码',self.open_code)]:
            ttk.Button(buttons,text=title,command=fn).pack(side='left',padx=(0,10))
        self.table=ttk.Treeview(box,columns=('stage','count'),show='headings',height=6)
        self.table.heading('stage',text='阶段'); self.table.heading('count',text='文章数')
        self.table.pack(fill='both',expand=True,pady=14)
        self.timings=ttk.Treeview(box,columns=('stage','average','infra','content','controller'),show='headings',height=7)
        for key,title in [('stage','阶段'),('average','平均秒（近24h）'),('infra','网络/服务失败'),('content','交付失败'),('controller','程序错误')]:
            self.timings.heading(key,text=title);self.timings.column(key,width=160)
        self.timings.pack(fill='both',expand=True,pady=8)
        self.exceptions=tk.StringVar(value='')
        ttk.Label(box,textvariable=self.exceptions,wraplength=940).pack(anchor='w',pady=4)
        ttk.Label(box,text='没有100篇停止限制。技术检查不用LLM。关闭窗口前等待当前步骤保存。',wraplength=810).pack(anchor='w')
        self.root.protocol('WM_DELETE_WINDOW',self.close)
        self.root.after(700,self.refresh)

    def start(self):
        if self.thread and self.thread.is_alive():
            self.service.paused=False
            return
        self.stop_event.clear(); self.service.paused=False
        def run():
            try: self.service.run(self.stop_event)
            except Exception as exc:
                from .readiness import NotReady
                self.service.last_error=str(exc) if isinstance(exc,NotReady) else type(exc).__name__
        self.thread=threading.Thread(target=run,daemon=False,name='paperflow-controller')
        self.thread.start()

    def pause(self):
        self.service.paused=True

    def stop(self):
        self.stop_event.set()
        self.message.set('正在保存：等待当前步骤完成，不再派发新任务。')

    def open_code(self):
        import subprocess
        from pathlib import Path
        folder=Path(__file__).resolve().parent.parent
        subprocess.Popen([r'E:\Microsoft VS Code\Code.exe','--reuse-window',str(folder),
                          '--goto',str(folder/'paperflow/graph.py')+':30'])

    def refresh(self):
        labels={'round1':'第一轮筛选与理解','round2':'第二轮补正','round3':'第三轮整理','repair':'定点补交',
                'publish':'发布交付','validate':'技术检查','reconcile':'旧成果对账','ingest':'写入RAG',
                'verify':'读回核对','activate':'检索及版本切换','cleanup':'清理临时文件','complete':'完成',
                'held':'材料性质待定/排除','parked':'暂存问题稿','partial_complete':'部分完成'}
        labels.update(round3_assets='图表取舍',round3_redact='脱敏',round3_chunk='切片',
            publish_repaired='发布返修版',ingest_target='按专业写入/读回',join_targets='汇合专业回执',
            classify_error='错误分类',code_error='程序错误：已暂停该篇')
        status=self.service.status()
        for item in self.table.get_children(): self.table.delete(item)
        for stage,count in status['display_counts'].items(): self.table.insert('', 'end',values=(labels.get(stage,stage),count))
        m=status['metrics']
        self.metrics_text.set(f"近1小时成功 {m['success_1h']} 篇 · 近24小时 {m['success_24h']} 篇 · 部分完成 {len(m['partial'])} 篇 · 积压 {m['backlog']} 篇 · 问题暂存 {len(m['parked'])} 篇 · 程序暂停 {len(m['code_error'])} 篇")
        for item in self.timings.get_children():self.timings.delete(item)
        for stage,value in m['stages'].items():
            self.timings.insert('','end',values=(labels.get(stage,stage),value['average_seconds'],
                *(value['failures'][k] for k in ('infrastructure','content','controller'))))
        messages={'code_error':'存在程序错误，相关稿件已暂停。','no_success_1h_with_backlog':'积压已超过1小时，近1小时没有成功入库，请检查。'}
        self.alert.config(text=' '.join(messages[k] for k in status['alerts']))
        partial_ids={x['task_id'] for x in m['partial']}
        details=['部分完成 '+x['task_id']+'：写入已核对 '+','.join(x['written_targets'])+
                 '；可检索确认 '+(','.join(x['searchable_targets']) or '待确认')+
                 '；待处理目标 '+(','.join(x['pending_targets']) or '待完成检索/版本切换或收尾') for x in m['partial']]
        self.exceptions.set('程序暂停：'+', '.join(x['task_id'] for x in m['code_error'])+
            '\n问题暂存：'+', '.join(x['task_id'] for x in m['parked'] if x['task_id'] not in partial_ids)+
            ('\n'+'\n'.join(details) if details else ''))
        if self.stop_event.is_set() and self.thread and self.thread.is_alive():
            text='正在等待当前步骤保存后停止。'
        elif status['running']:
            text='已暂停调度，当前步骤保存后等待。' if status['paused'] else '持续运行中；队列为空时自动等待。'
            if status['backpressure']: text+=' 资源或入库积压限速中。'
        else:
            text='已停止，进度已保留。' if self.thread else '尚未开始；点击开始后持续监听和处理。'
        if status['last_error']: text+=' 最近控制器异常：'+status['last_error']
        if not status['acceptance']['ready']:text+=' '+status['acceptance']['reason']+'，持续运行暂不可启动。'
        self.message.set(text)
        self.root.after(1000,self.refresh)

    def close(self):
        if self.thread and self.thread.is_alive():
            self.stop(); self.root.after(500,self.close); return
        self.service.close(); self.root.destroy()


def main(config,smoke=False):
    root=tk.Tk()
    if smoke: root.withdraw()
    app=Workbench(root,config)
    if smoke:
        root.update_idletasks()
        assert not app.service.running and app.thread is None
        app.close()
        return dict(ui='ok',auto_start=False)
    root.mainloop()
