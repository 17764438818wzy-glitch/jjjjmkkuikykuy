"""A passive native view usable alongside an already loaded old application."""
import sys
sys.dont_write_bytecode=True
import json
import queue
import sqlite3
import threading
import time
from pathlib import Path

if __name__=='__main__':
    module=Path(__file__).resolve()
    bundle=next((p for p in module.parents if (p/'config/paths.json').is_file()),None)
    if bundle is None and (module.parent.parent/'baseline.json').is_file():
        bundle=Path(json.loads((module.parent.parent/'baseline.json').read_text(encoding='utf8'))['root'])
    if bundle:
        sys.path.extend([str(bundle/'desktop'),str(bundle/'core')])


def delivery_counts(root,run_id=None):
    path=Path(root)/'private/preextract_delivery/queue.sqlite3'
    if not path.is_file():return {}
    connection=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.25)
    try:
        if run_id:
            return dict(connection.execute('SELECT d.status,COUNT(DISTINCT d.id) FROM deliveries d '
                'JOIN references_to_deliveries r ON r.delivery_id=d.id WHERE r.run_id=? GROUP BY d.status',(run_id,)))
        return dict(connection.execute('SELECT status,COUNT(*) FROM deliveries GROUP BY status'))
    finally:connection.close()


def main():
    import argparse
    import tkinter as tk
    from tkinter import ttk
    from model_activity import ModelActivityReader
    from model_activity_ui import activity_text
    parser=argparse.ArgumentParser();parser.add_argument('--data-root',required=True)
    parser.add_argument('--run-id',required=True);parser.add_argument('--report')
    parser.add_argument('--close-after',type=int,default=0)
    args=parser.parse_args();reader=ModelActivityReader(args.data_root)
    root=tk.Tk();root.title('论文预处理 · 并发核查看板（只读）');root.geometry('1120x650')
    frame=ttk.Frame(root,padding=20);frame.pack(fill='both',expand=True)
    ttk.Label(frame,text='正文与 GPT 分类分开统计',font=('Microsoft YaHei UI',17,'bold')).pack(anchor='w')
    title=tk.StringVar(value='正在读取请求回执…')
    ttk.Label(frame,textvariable=title,font=('Microsoft YaHei UI',10,'bold'),foreground='#1e4c7a',wraplength=1060).pack(anchor='w',pady=(15,8))
    info=tk.StringVar();ttk.Label(frame,textvariable=info,wraplength=1060).pack(anchor='w',pady=(0,12))
    table=ttk.Treeview(frame,columns=('stage','quantity','meaning'),show='headings',height=8)
    for column,text,width in [('stage','阶段',155),('quantity','当前数量',180),('meaning','数量含义',680)]:
        table.heading(column,text=text);table.column(column,width=width,stretch=column=='meaning')
    table.pack(fill='x',expand=False)
    note=tk.StringVar()
    ttk.Label(frame,textvariable=note,wraplength=1050,foreground='#536377').pack(anchor='w',pady=(16,6))
    ttk.Label(frame,text='确认 50 篇可执行的新正文 → 8 路程序准备 → 正文模型最多 50 请求滚动补位；GPT 分类独立最多 10 请求。',wraplength=1060).pack(anchor='w')
    ttk.Label(frame,text='已确认附件复用用途回执，成员原件按现有合同关联交付；完整旧回答继续复用。全部来源耗尽不足 50 时处理末批。',wraplength=1060).pack(anchor='w',pady=6)
    health=tk.StringVar(value='正在同步…');ttk.Label(frame,textvariable=health,foreground='#536377').pack(anchor='w',pady=12)
    events=queue.SimpleQueue();reading=[False];closed=[False];last=[None]
    def read():
        try:
            snapshot=reader.read(args.run_id)
            try:
                snapshot['delivery_current']=delivery_counts(args.data_root,args.run_id)
                snapshot['delivery_all']=delivery_counts(args.data_root)
            except sqlite3.Error as exc:snapshot['delivery_error']=type(exc).__name__
            events.put((snapshot,None))
        except Exception as exc:events.put((None,type(exc).__name__))
    def render(snapshot):
        last[0]=snapshot;title.set(activity_text(snapshot))
        status={'stopped':'已停止','running':'正在运行','completed':'已完成','failed':'执行失败'}.get(snapshot.get('current_status'),'待确认')
        info.set(f"批次 {args.run_id} · {status} · 处理来源 {snapshot.get('processed_sources','?')}/{snapshot.get('source_count','?')} · 已复用回答不计新模型请求")
        c=snapshot.get('pipeline_counts',{});d=snapshot.get('delivery_current',{})
        certain=snapshot.get('journal_complete',True) and snapshot.get('current_complete',True)
        old=snapshot.get('legacy_shared_current')
        rows=[('GPT 用途分类',str(snapshot['purpose_current']) if certain else '待确认',
                  '本批用途判断实际在途请求；旧批次仍共享原总额度' if old else '独立上限 10；不占正文模型或 8 路准备名额'),
              ('已确认正文候选',str(c.get('body_candidates','旧版未单列')),'已确认用途、所属项目资料已齐；尚未进入准备'),
              ('正文程序准备',f"{c.get('preparation_active','?')}/{c.get('preparation_workers',8)}",'解析、文字与图片准备；不代表模型已发请求'),
              ('正文材料就绪',str(c.get('preparation_waiting','?')),'材料准备完成，等待正文模型名额'),
              ('正文模型请求',f"{snapshot['paper_current']}/{snapshot.get('paper_limit',50)}" if certain else '待确认','本批正文提取实际在途请求；按请求数统计'),
              ('程序后处理',f"{c.get('postprocess_active','?')} · 等待 {c.get('postprocess_waiting','?')}",'完整回答保存后的校验、脱敏、切片、封包'),
              ('本批 RAG 交付','待确认' if snapshot.get('delivery_error') else f"进行 {d.get('uploading',0)} · 排队 {d.get('queued',0)}",f"完成 {d.get('complete',0)} · 待核对 {d.get('needs_review',0)}；仅统计本批交付登记"),
              ('全部批次 RAG 交付',json.dumps(snapshot.get('delivery_all',{}),ensure_ascii=False),'全队列数量，与本批正文请求分开统计')]
        table.delete(*table.get_children())
        for row in rows:table.insert('',tk.END,values=row)
        note.set(('当前批次仍使用旧调度；新调度在原程序退出后、下次正常启动时载入。' if old else '本批已采用独立分类调度。')+
            (' 本批已停止，停止前队列计数不作当前活动量。' if status=='已停止' else '')+
            ' 本窗口只读统计；处理控制仍在原程序。')
        health.set('最近同步 '+time.strftime('%H:%M:%S')+' · '+('完整回执已读到最新' if snapshot.get('journal_complete',True) else '正在追赶完整回执，暂不显示模型数量'))
        if args.report:
            target=Path(args.report);target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(json.dumps(dict(snapshot=snapshot,header=title.get(),visible=bool(root.winfo_viewable()),
                ui_rows=rows,read_only=True,updated_at=time.time()),ensure_ascii=False,indent=2),encoding='utf8')
    def tick():
        if closed[0]:return
        try:value,error=events.get_nowait()
        except queue.Empty:pass
        else:
            reading[0]=False
            if error:health.set('统计暂不可读：'+error+'；当前数量待确认')
            else:render(value)
        if not reading[0]:
            reading[0]=True;threading.Thread(target=read,daemon=True,name='passive-pipeline-dashboard').start()
        root.after(2000,tick)
    def close():closed[0]=True;root.destroy()
    root.protocol('WM_DELETE_WINDOW',close);root.after(20,tick)
    if args.close_after:root.after(args.close_after*1000,close)
    root.mainloop()


if __name__=='__main__':main()
