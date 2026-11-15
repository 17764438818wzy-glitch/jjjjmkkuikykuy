"""Bound native table widgets to 50 rows; keep task data outside the view."""
import math
import tkinter as tk
from tkinter import ttk

PAGE_SIZE=50


def cell_values(values):
    """Long validation reports belong in details, not in table layout."""
    return tuple(value if not isinstance(value,str) or len(value)<=500 else
                 value[:480]+'…（详情中可查看完整内容）' for value in values)


class DisplayPage:
    def __init__(self,app,key,tree):
        self.app,self.key,self.tree=app,key,tree
        self.page=0;self.records=();self.stamp=None;self.context=None;self.render=None;self.before=None
        self.selection=set()
        bar=ttk.Frame(tree.master)
        bar.grid(row=2,column=0,sticky='ew',pady=(3,0))
        self.label=tk.StringVar()
        ttk.Label(bar,textvariable=self.label).pack(side='left')
        self.newer=ttk.Button(bar,text='上一页',command=lambda:self.change(-1))
        self.older=ttk.Button(bar,text='下一页',command=lambda:self.change(1))
        self.older.pack(side='right');self.newer.pack(side='right',padx=6)

    @property
    def total(self):return len(self.records)

    @property
    def pages(self):return max(1,math.ceil(self.total/PAGE_SIZE))

    def show(self,records,stamp,render,before=None,*,context=None,selected=None):
        context=stamp if context is None else context
        if self.context!=context:self.page=0
        self.context=context
        self.selection=set(self.tree.selection() if selected is None else selected)
        self.records,self.stamp,self.render,self.before=records,stamp,render,before
        self.page=min(self.page,self.pages-1)
        self.paint()

    def paint(self):
        # Page one is the last recorded group, without re-numbering identities.
        end=max(0,self.total-self.page*PAGE_SIZE)
        visible=self.records[max(0,end-PAGE_SIZE):end]
        self.render(visible,self.total)
        if self.key!='source':
            ids=set(self.tree.get_children())
            self.tree.selection_set([i for i in self.selection if i in ids])
        self.label.set(f'当前显示 {len(visible)}/{self.total} 条 · 第 {self.page+1}/{self.pages} 页 · 每页 50 条')
        self.newer.configure(state='normal' if self.page else 'disabled')
        self.older.configure(state='normal' if self.page+1<self.pages else 'disabled')

    def change(self,delta):
        target=max(0,min(self.pages-1,self.page+delta))
        if target==self.page:return
        if self.before:self.before()
        self.page=target
        self.selection=set()
        self.tree.selection_set(())
        self.paint()


def page_for(app,key,tree):
    pages=getattr(app,'_display_pages',None)
    if pages is None:pages=app._display_pages={}
    if key not in pages:pages[key]=DisplayPage(app,key,tree)
    return pages[key]


def history_upload_index(app,stamp):
    """Reuse receipt indexes; read large histories off Tk using a private queue."""
    from upload_tracking import UploadIndex
    cached=getattr(app,'_display_upload_index',None)
    if cached and cached[0] is app.store and cached[1]==stamp:return cached[2]
    size=sum(value[1] for _,value in stamp if isinstance(value,tuple) and len(value)==2 and isinstance(value[1],int))
    if size<5*1024*1024:
        index=UploadIndex(app.store.history())
        app._display_upload_index=(app.store,stamp,index)
        return index
    if getattr(app,'_display_index_job',None) is not None:return None
    import queue,threading
    store=app.store;results=queue.Queue(maxsize=1)
    job=app._display_index_job=(store,stamp,results)
    def read():
        try:results.put((UploadIndex(store.history()),None))
        except Exception as error:results.put((None,type(error).__name__))
    def finish():
        if getattr(app,'_display_index_job',None) is not job:return
        try:index,error=results.get_nowait()
        except queue.Empty:
            app.root.after(50,finish);return
        app._display_index_job=None
        if app.store is not store:return
        if error:
            app.rag_result_summary.set('上传记录暂未读取：'+error+'；已保存结果保留。')
            return
        app._display_upload_index=(store,stamp,index)
        app.render_rag_results(refresh=False)
    try:threading.Thread(target=read,daemon=True,name='display-upload-index').start()
    except Exception as error:
        app._display_index_job=None
        app.rag_result_summary.set('上传记录暂未读取：'+type(error).__name__)
        return None
    app.root.after(50,finish)
    return None


def read_history_rows(app,build,finish):
    """Keep large startup/manual history scans off the Tk thread too."""
    from pathlib import Path
    size=sum(p.stat().st_size for p in app.store.runs.glob('*/manifest.json') if p.is_file())
    if size<5*1024*1024:
        finish(build(app.store));return
    if getattr(app,'_display_history_job',None) is not None:return
    import queue,threading
    store=app.store;results=queue.Queue(maxsize=1)
    job=app._display_history_job=(store,results)
    def read():
        try:results.put((build(store),None))
        except Exception as error:results.put((None,type(error).__name__))
    def poll():
        if getattr(app,'_display_history_job',None) is not job:return
        try:rows,error=results.get_nowait()
        except queue.Empty:app.root.after(50,poll);return
        app._display_history_job=None
        if app.store is not store:return
        if error:
            app.state.set('历史列表暂未读取：'+error+'；处理任务不受影响。');return
        finish(rows)
    try:threading.Thread(target=read,daemon=True,name='display-history').start()
    except Exception as error:
        app._display_history_job=None
        app.state.set('历史列表暂未读取：'+type(error).__name__);return
    app.root.after(50,poll)


def load_latest_results(app):
    """Run the existing folder/ancestor lookup off Tk, preserving its scope."""
    from pathlib import Path
    import queue,threading
    pending=getattr(app,'_display_load_job',None)
    folder=app.folder.get().strip()
    if not folder:
        app.state.set('请先选择输入文件夹，或从批次记录载入结果。');return
    if pending is not None:
        pending['again']=pending['folder']!=folder;return
    store=app.store;results=queue.Queue(maxsize=1)
    job=app._display_load_job=dict(folder=folder,current=app.current,sources=app.sources,again=False)
    app.state.set('正在后台载入历史任务；完整结果保留，界面每页显示 50 条。')
    def read():
        try:
            from functools import lru_cache
            from script_task import task_snapshot,task_rows,effective_run
            from model_activity import run_owner_state
            from preextract_delivery_queue import queue_stamp
            def stamp(path):
                try:s=path.stat();return s.st_mtime_ns,s.st_size
                except OSError:return None
            before={p.parent.name:stamp(p) for p in store.runs.glob('*/manifest.json')}
            delivery=queue_stamp(store.root)
            history={m['run_id']:m for m in store.history() if m.get('pipeline')=='automatic'}
            base=Path(folder).resolve();belongs={}
            @lru_cache(maxsize=None)
            def in_path(path):return bool(path) and Path(path).resolve().is_relative_to(base)
            def in_folder(manifest,seen):
                rid=manifest['run_id']
                if rid in belongs:return belongs[rid]
                if rid in seen:return False
                paths=[row.get('input_source_path') or row.get('path') for row in manifest.get('inputs',[])]
                direct=bool(paths) and all(in_path(path) for path in paths)
                previous=history.get(manifest.get('retry_of'))
                belongs[rid]=direct or bool(previous and in_folder(previous,seen|{rid}))
                return belongs[rid]
            matches=[m for m in history.values() if in_folder(m,set())]
            if not matches:results.put((None,None));return
            manifest=effective_run(store.root,max(matches,key=lambda m:(m.get('started_at',''),m.get('run_id',''))))
            versions=[(manifest['run_id'],before.get(manifest['run_id']))]
            class Reader:
                root=store.root
                def load(self,run_id):
                    versions.append((run_id,before.get(run_id)))
                    return history.get(run_id) or store.load(run_id)
            snapshot=task_snapshot(Reader(),manifest)
            rows=task_rows(snapshot,for_display=True)
            owners=tuple((rid,run_owner_state(store.root,rid)) for rid,_ in versions)
            results.put(((manifest,snapshot,rows,tuple(versions),owners,delivery),None))
        except Exception as error:results.put((None,type(error).__name__))
    def finish():
        if getattr(app,'_display_load_job',None) is not job:return
        try:value,error=results.get_nowait()
        except queue.Empty:app.root.after(50,finish);return
        app._display_load_job=None
        stable=(app.store is store and app.current is job['current'] and app.sources is job['sources'])
        fresh=stable and app.folder.get().strip()==folder and not app.busy
        if fresh:
            if error:app.state.set('历史任务暂未载入：'+error+'；原始结果保留。')
            elif value is None:app.state.set('当前文件夹没有历史脚本批次。')
            else:
                manifest,snapshot,rows,versions,owners,delivery=value
                app.current=manifest;app._script_manifest=snapshot
                app._result_snapshot_cache=(store,manifest,versions,snapshot,owners)
                app._result_delivery_stamp=delivery
                app.render_script_rows(rows)
                app.load_progress(manifest)
                app.state.set('该批次仍在处理，请等待完成。' if manifest.get('status')=='running'
                              else '历史结果已载入；本地与 AI 未完成项在对应页显示，每页 50 条。')
                app.load_result_datasets()
        if job['again'] and stable and not app.busy and app.folder.get().strip()!=folder:
            app.load_latest_script_results()
    try:threading.Thread(target=read,daemon=True,name='display-folder-history').start()
    except Exception as error:
        app._display_load_job=None;app.state.set('历史任务暂未载入：'+type(error).__name__);return
    app.root.after(50,finish)
