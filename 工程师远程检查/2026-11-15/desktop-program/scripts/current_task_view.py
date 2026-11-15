"""Read-only native task window. Never follows retry history or starts work."""
import sys
sys.dont_write_bytecode=True
from pathlib import Path
import copy
import json
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'scripts'),str(ROOT/'desktop'),str(ROOT/'core')]


def projection(manifest):
    """A display copy; the durable retry link and original records stay intact."""
    from auto_view import result_items
    from archive_identity import record_key
    result=copy.copy(manifest)
    items=result_items(manifest)
    seen={item.get('id') for item in items}
    items+= [i for i in manifest.get('active_items',[]) if i.get('id') not in seen]
    result['items']=[dict(item,_result_run_id=manifest['run_id'],
                         _result_run_status=manifest['status']) for item in items]
    represented={record_key(item) for item in items}
    result['_pending_sources']=[record_key(row) for row in manifest.get('inputs',[])
                                if record_key(row) not in represented]
    return result


class CurrentReader:
    def __init__(self,store):self.store=store
    def read(self,run_id):
        from script_task import task_rows
        from upload_tracking import UploadIndex
        manifest=self.store.load(run_id)
        if not manifest:raise ValueError('当前批次记录缺失')
        snapshot=projection(manifest)
        try:settings=json.loads((self.store.root/'settings.json').read_text(encoding='utf8'))
        except (OSError,ValueError):settings={}
        return dict(manifest=manifest,snapshot=snapshot,rows=task_rows(snapshot),
            index=UploadIndex([manifest]),history_stamp=(),queue={},ledger={},
            queue_path=self.store.root/'private/preextract_delivery/queue.sqlite3',
            service_url=settings.get('ragflow_url',''),deployment={})


def main():
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-id',required=True)
    args=parser.parse_args()
    from source_update import activate_source_update
    activate_source_update(ROOT)
    from project_paths import DATA_DIR
    import tkinter as tk
    from tkinter import ttk
    from app import Workbench
    class Viewer(Workbench):
        def __init__(self,root):
            super().__init__(root,DATA_DIR,recover=False)
            self.current=self.store.load(args.run_id)
            self._live_reader=CurrentReader(self.store)
            self._live_results_enabled=True
            self._live_requested_run=args.run_id
            self._live_waiting_run=False
            self.busy=True;self.task_kind='processing'
            self._active_ai_retry=False
            self.root.title('论文预处理 · 本次任务 · 历史记录已隐藏')
            for tab in self.tabs.tabs():
                if tab not in {str(self.result_tab),str(self.rag_result_tab)}:self.tabs.hide(tab)
            self.tabs.select(self.result_tab)
            self.update_controls()
            def disable(widget):
                for child in widget.winfo_children():
                    if isinstance(child,(ttk.Button,ttk.Checkbutton)):child.state(['disabled'])
                    disable(child)
            disable(self.root)
            self.root.protocol('WM_DELETE_WINDOW',self.close)
            self.root.after(10,lambda:self.request_live_results(args.run_id))
        def start_automatic(self,*args):pass
        def async_task(self,*args,**kwargs):pass
        def stop(self):pass
        def close(self):
            for timer in self.root.tk.call('after','info'):self.root.after_cancel(timer)
            self.root.destroy()
        def render_ai_results(self,**kwargs):
            super().render_ai_results(**kwargs)
            self.result_summary.set(self.result_summary.get().replace('累计结果','本次结果'))
        def finish_live_results(self,value):
            super().finish_live_results(value)
            if not self._live_result:return
            manifest=self._live_result['manifest']
            done=manifest.get('processed_sources',0);total=manifest.get('source_count',0)
            self.progress_count.set(f'本次已完成 {done}/{total}')
            self.progress_bar.configure(value=done,maximum=max(1,total))
            self.progress_detail.set('仅显示本次任务；历史失败不计入本次统计')
            self.state.set('关闭本窗口不会停止任务；任务控制仍在原处理窗口。')
            report=dict(run_id=args.run_id,visible=bool(self.root.winfo_viewable()),
                        summary=self.result_summary.get(),processed=done,total=total,
                        history_included=False,read_only=True,updated_at=time.time())
            target=ROOT/'_verification/purpose-review-20261022/display/current-window.json'
            target.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
    root=tk.Tk();Viewer(root);root.mainloop()


if __name__=='__main__':main()
