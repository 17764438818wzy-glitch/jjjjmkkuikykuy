"""RAGFlow result display and selected recovery, separate from local processing."""
from collections import Counter,defaultdict
import copy
from pathlib import Path
import tkinter as tk
from tkinter import ttk

from rag_status import UPLOAD_STATES,PROCESS_STATES,describe_item,normalize_remote_doc,needs_recovery,processing_state
from script_task import task_snapshot,script_outcome
from upload_tracking import UploadIndex


class RAGResultsView:
    def build_rag_results(self):
        tab=self.rag_result_tab;tab.columnconfigure(0,weight=1);tab.rowconfigure(2,weight=1)
        top=ttk.Frame(tab);top.grid(row=0,column=0,sticky='ew')
        ttk.Label(top,text='搜索文件').pack(side='left')
        self.rag_search=tk.StringVar()
        ttk.Entry(top,textvariable=self.rag_search,width=24).pack(side='left',fill='x',expand=True,padx=6)
        self.rag_status_button=ttk.Button(top,text='刷新 RAGFlow 状态',command=self.refresh_rag_status,style='Task.TButton')
        self.rag_status_button.pack(side='right')
        ttk.Button(top,text='查看处理详情',command=self.preview_rag,style='Task.TButton').pack(side='right',padx=6)
        destinations=ttk.Frame(tab);destinations.grid(row=1,column=0,sticky='ew',pady=(6,0))
        destinations.columnconfigure(1,weight=1);destinations.columnconfigure(3,weight=1)
        ttk.Label(destinations,text='论文知识库').grid(row=0,column=0,sticky='w')
        self.rag_dataset_combo=ttk.Combobox(destinations,textvariable=self.dataset,state='readonly',width=20)
        self.rag_dataset_combo.grid(row=0,column=1,sticky='ew',padx=(6,12))
        ttk.Label(destinations,text='图片知识库').grid(row=0,column=2,sticky='w')
        self.rag_image_dataset_combo=ttk.Combobox(destinations,textvariable=self.image_dataset,state='readonly',width=20)
        self.rag_image_dataset_combo.grid(row=0,column=3,sticky='ew',padx=6)
        self.rag_connect_button=ttk.Button(destinations,text='读取知识库',command=self.connect,style='Task.TButton')
        self.rag_connect_button.grid(row=0,column=4)
        selection=ttk.Frame(destinations);selection.grid(row=1,column=0,columnspan=5,sticky='ew',pady=(6,0))
        self.rag_select_buttons=[]
        for text,mode in [('全选本页','all'),('取消本页','none'),('反选本页','invert')]:
            button=ttk.Button(selection,text=text,command=lambda mode=mode:self.select_files(self.rag_result_tree,mode),style='Task.TButton')
            button.pack(side='left',padx=(0,6));self.rag_select_buttons.append(button)
        self.rag_select_all_button=self.rag_select_buttons[0]
        self.rag_selection_count=tk.StringVar(value='已选 0 / 0 个文件')
        ttk.Label(selection,textvariable=self.rag_selection_count).pack(side='left',padx=5)
        self.rag_result_tree=self.tree(tab,['编号','题目','专业','预处理结果','目标知识库','上传状态','RAGFlow处理状态','原因','原文件名','附属资料'],
            [120,190,90,90,160,120,140,340,185,160],grid_row=2)
        self.rag_result_tree.configure(displaycolumns=['原文件名','题目','目标知识库','上传状态','RAGFlow处理状态','附属资料','原因'])
        self.rag_result_tree.bind('<Double-1>',lambda _:self.preview_rag())
        self.rag_result_tree.bind('<<TreeviewSelect>>',lambda _:self.update_rag_controls())
        filters=ttk.Frame(tab);filters.grid(row=3,column=0,sticky='ew')
        self.rag_upload_filter=tk.StringVar(value=UPLOAD_STATES[0]);self.rag_process_filter=tk.StringVar(value=PROCESS_STATES[0])
        ttk.Label(filters,text='上传状态').pack(side='left')
        self.rag_upload_filter_combo=ttk.Combobox(filters,textvariable=self.rag_upload_filter,values=['未完成',*UPLOAD_STATES],state='readonly',width=15)
        self.rag_upload_filter_combo.pack(side='left',padx=(6,16))
        ttk.Label(filters,text='RAGFlow 状态').pack(side='left')
        self.rag_process_filter_combo=ttk.Combobox(filters,textvariable=self.rag_process_filter,values=PROCESS_STATES,state='readonly',width=18)
        self.rag_process_filter_combo.pack(side='left',padx=6)
        self.rag_result_summary=tk.StringVar(value='尚无 RAGFlow 处理记录')
        summary=ttk.Label(tab,textvariable=self.rag_result_summary,wraplength=900,foreground='#617082')
        summary.grid(row=4,column=0,sticky='ew',pady=(4,0))
        actions=ttk.Frame(tab);actions.grid(row=5,column=0,sticky='ew',pady=(8,0))
        self.rag_settings_button=ttk.Button(actions,text='检查 RAGFlow 设置',command=lambda:self.tabs.select(self.rag_tab),style='Task.TButton')
        self.rag_settings_button.pack(side='left',padx=(0,6))
        self.rag_import_button=ttk.Button(actions,text='导入选中项',command=self.import_rag_selected,style='TaskAccent.TButton')
        self.rag_import_button.pack(side='left',padx=(0,6))
        self.rag_retry_button=ttk.Button(actions,text='重试选中项',command=self.retry_handoff,style='Task.TButton')
        self.rag_retry_button.pack(side='left')
        self.rag_activity=tk.StringVar(value='新批次按 MD 最终专业名单自动投递，逐专业读回及检索确认后才算完成；本地处理完成时，后台上传可能仍在继续。稍后导入可勾选文件使用原按钮；上方单库选择用于旧格式结果。')
        activity=ttk.Label(tab,textvariable=self.rag_activity,foreground='#617082',wraplength=900)
        activity.grid(row=6,column=0,sticky='ew',pady=4)
        tab.bind('<Configure>',lambda e:[w.configure(wraplength=max(200,e.width-30)) for w in (summary,activity)],add='+')
        self._rag_items={};self._rag_states={}
        for var in (self.rag_search,self.rag_upload_filter,self.rag_process_filter):var.trace_add('write',lambda *_:self.render_rag_results())

    def rag_batch_items(self,snapshot=None):
        if not self.current:return []
        if snapshot is None:
            from education_stream_status import load_external_ledger
            if load_external_ledger(self.store.root,self.current['run_id']):
                self.current=self.store.load(self.current['run_id']) or self.current
            snapshot=self.result_snapshot()
        return [i for i in snapshot['items'] if script_outcome(i,snapshot)[0]=='通过' or i.get('uploads')]

    def _remote_item_status(self,item,upload_index=None):
        live=getattr(self,'_live_result',None) if self.live_results_active() else None
        index=upload_index if upload_index is not None else (
            live['index'] if live else UploadIndex([]) if self.live_results_active() else UploadIndex(self.store.history()))
        manifest=self.current or {}
        origin=item.get('_result_run_id')
        if origin and origin!=manifest.get('run_id'):
            manifest=index.manifests.get(origin) or (None if self.live_results_active() else self.store.load(origin)) or manifest
        state=describe_item(item,manifest,self.url.get().strip().rstrip('/'),index,
                            self._selected_target_dataset())
        if item.get('queue_task_ids'):
            from ingest_queue import QueueStore
            from upload_tracking import queued_upload_status
            from auto_config import load_config
            snapshot=getattr(self,'_work_pool_snapshot',None)
            if snapshot is None and self.live_results_active():
                snapshot=(live or {}).get('queue',{})
            if snapshot is None:
                config=load_config(self.store.root)
                queue_path=config.get('work_pool_queue') or (
                    self.store.root/'private'/'universal_ingest'/'queue.sqlite3')
                snapshot=(QueueStore(queue_path).get_many(item['queue_task_ids'])
                          if Path(queue_path).is_file() else {})
            from upload_tracking import service_key
            current_service=service_key(self.url.get())
            visible={task_id:snapshot[task_id] for task_id in item['queue_task_ids']
                     if task_id in snapshot and snapshot[task_id]['service_key']==current_service}
            if visible:
                projection=queued_upload_status(item['queue_task_ids'],visible)
                primary=next((row for row in visible.values() if row['role']=='paper_primary'),{})
                state.update(upload=projection['upload'],process=projection['process'],
                             reason=projection['reason'] or state['reason'],
                             assets=f"已上传 {projection['assets_done']}/{projection['assets_total']} 项",
                             can_retry=(primary.get('status') in ('technical_review','outcome_unresolved','quality_hold','quality_reject')
                                        or primary.get('post_status')=='needs_review'
                                        or primary.get('source_metadata_status')=='blocked'))
                if primary.get('post_status')=='needs_review':
                    state['assets']+='；交接尚未完成'
                if projection.get('remote_id'):
                    state['record']={'document_id':projection['remote_id'],
                                     'dataset_id':projection['dataset_id']}
        context=getattr(self,'_stream_context',None)
        if context:
            from education_stream_status import overlay_status
            ledger,paper_dataset,image_dataset=context
            state=overlay_status(item,state,ledger,paper_dataset,image_dataset)
        return state

    def render_rag_results(self,*,refresh=True):
        from education_stream_status import load_external_ledger
        live=getattr(self,'_live_result',None) if self.live_results_active() else None
        try:
            ledger=(live or {}).get('ledger') if self.live_results_active() else (
                load_external_ledger(self.store.root,self.current['run_id']) if self.current else None)
            if refresh and ledger and not self.live_results_active():
                self.current=self.store.load(self.current['run_id']) or self.current
            snapshot=self.result_snapshot(refresh=refresh)
        except (ValueError,OSError) as exc:
            self.rag_result_tree.delete(*self.rag_result_tree.get_children())
            self._rag_items={};self._rag_states={}
            self._stream_context=None;self._work_pool_snapshot=None;self._rag_render_stamp=None
            self._rag_render_snapshot=None;self._rag_render_ledger=None
            self.rag_result_summary.set('无法载入批次：'+str(exc));self.update_rag_controls();return
        queue_path=Path(getattr(self,'_result_queue_path',self.store.root/'private'/'universal_ingest'/'queue.sqlite3'))
        stamp=(id(snapshot),self.result_history_stamp(),id(ledger),self.url.get(),self.dataset.get(),
               getattr(self,'connected_url',None),tuple(self.dataset_combo.cget('values')),
               self.rag_search.get(),self.rag_upload_filter.get(),self.rag_process_filter.get(),
               self.result_file_stamp(self.store.root/'ai_settings.json'),
               self.result_file_stamp(self.store.root/'settings.json'),
               self.result_file_stamp(self.store.root/'private'/'education_pilot'/'deployment.json'),
               self.result_file_stamp(queue_path),self.result_file_stamp(str(queue_path)+'-wal'))
        if not refresh and stamp==getattr(self,'_rag_render_stamp',None):
            self.update_rag_controls();return
        self._stream_context=None
        self._work_pool_snapshot=None
        if self.current:
            service=(live or {}).get('service_url','').strip().rstrip('/') if self.live_results_active() else self._stream_service_url()
            if ledger and self.url.get().strip().rstrip('/')==service:
                deployment=(live or {}).get('deployment') if self.live_results_active() else self._stream_deployment()
                if deployment:
                    self._stream_context=(ledger,deployment['paper_dataset_id'],deployment['image_dataset_id'])
        selected=set(self.rag_result_tree.selection())
        self.rag_result_tree.delete(*self.rag_result_tree.get_children());self._rag_items={};self._rag_states={}
        try:items=self.rag_batch_items(snapshot)
        except (ValueError,OSError) as exc:
            self.rag_result_summary.set('无法载入批次：'+str(exc));self.update_rag_controls();return
        task_ids=[task_id for item in items for task_id in item.get('queue_task_ids',[])]
        if self.live_results_active():
            self._work_pool_snapshot=(live or {}).get('queue',{})
            if live:self._result_queue_path=live['queue_path']
        elif task_ids:
            from auto_config import load_config
            from ingest_queue import QueueStore
            config=load_config(self.store.root)
            queue_path=config.get('work_pool_queue') or (
                self.store.root/'private'/'universal_ingest'/'queue.sqlite3')
            self._result_queue_path=queue_path
            self._work_pool_snapshot=(QueueStore(queue_path).get_many(task_ids)
                                      if Path(queue_path).is_file() else {})
        live=getattr(self,'_live_result',None) if self.live_results_active() else None
        if live:index=live['index']
        elif self.live_results_active():index=UploadIndex([])
        else:
            from display_page import history_upload_index
            index=history_upload_index(self,self.result_history_stamp())
            if index is None:
                self.rag_result_summary.set('正在后台读取上传记录；列表每页最多 50 条。')
                return
        states=[];records=[];seen={}
        chosen=self.rag_upload_filter.get()
        query=self.rag_search.get().strip().casefold()
        process_filter=self.rag_process_filter.get()
        for item in items:
            state=self._remote_item_status(item,index);states.append(state)
            iid=item['id'];number=seen.get(iid,0);seen[iid]=number+1
            iid=iid if not number else iid+'@'+str(number)
            delivery=item.get('preextract_delivery') or {}
            done=state['upload']=='已上传' and state['process']=='已完成' and not needs_recovery(state) and (not delivery or delivery.get('status')=='complete')
            if chosen=='未完成' and done:continue
            if chosen!='未完成' and chosen not in {UPLOAD_STATES[0],state['upload']}:continue
            if process_filter not in {PROCESS_STATES[0],state['process']}:continue
            filename=Path(item.get('source_path','')).name
            if query not in (filename+' '+item.get('title','')+' '+item['id']).casefold():continue
            records.append((iid,item,state,filename))
        uploads=Counter(s['upload'] for s in states);processes=Counter(s['process'] for s in states)
        from display_page import page_for,cell_values
        from live_results import sync_tree
        def paint(visible,total):
            self._rag_items={};self._rag_states={};rows=[]
            for iid,item,state,filename in visible:
                self._rag_items[iid]=item;self._rag_states[iid]=state
                rows.append((iid,cell_values((item['id'],item.get('title',''),item.get('major',''),'通过',state['target'],state['upload'],state['process'],state['reason'],filename,state['assets']))))
            sync_tree(self.rag_result_tree,rows)
            self.rag_result_summary.set(f'本批 {len(items)} 篇 · 显示 {len(visible)}/{total} 篇\n'+
                '上传：'+(' · '.join(f'{k} {v}' for k,v in uploads.items()) or '无记录')+'\n'+
                'RAGFlow：'+(' · '.join(f'{k} {v}' for k,v in processes.items()) or '无记录')+
                ('\n教育独立导入回执已同步显示；该页不会重复上传。' if self._stream_context else ''))
            self.update_rag_controls()
        context=(id(self.store),self.current['run_id'] if self.current else None,self.url.get(),
                 self.dataset.get(),self.rag_search.get(),self.rag_upload_filter.get(),self.rag_process_filter.get())
        page_for(self,'rag',self.rag_result_tree).show(records,stamp,paint,context=context,selected=selected)
        # A custom queue path is learned during the first render. Recompute its
        # file stamps on the next switch before deciding whether rows are stale.
        self._rag_render_stamp=stamp
        self._rag_render_snapshot=snapshot
        self._rag_render_ledger=ledger
        self.update_rag_controls()

    def _stream_service_url(self):
        import json
        try:
            settings=json.loads((self.store.root/'settings.json').read_text(encoding='utf-8'))
            return str(settings.get('ragflow_url') or '').strip().rstrip('/')
        except (OSError,ValueError):
            return ''

    def _stream_deployment(self):
        import json
        try:
            path=self.store.root/'private'/'education_pilot'/'deployment.json'
            value=json.loads(path.read_text(encoding='utf-8'))
            if value.get('paper_dataset_id') and value.get('image_dataset_id'):
                return value
        except (OSError,ValueError):
            pass
        return None

    def selected_rag_items(self):
        return [self._rag_items[iid] for iid in self.rag_result_tree.selection() if iid in self._rag_items]

    def update_rag_controls(self):
        if not hasattr(self,'rag_status_button'):return
        visible=len(self.rag_result_tree.get_children());selected=self.selected_rag_items()
        self.rag_selection_count.set(f'已选 {len(selected)} / {visible} 个文件')
        for button in self.rag_select_buttons:button.configure(state='normal' if visible and not self.busy else 'disabled')
        external=getattr(self,'_stream_context',None)
        self.rag_status_button.configure(state='normal' if self.current and (not self.busy or external) else 'disabled')
        self.rag_settings_button.configure(state='disabled' if self.busy else 'normal')
        self.rag_connect_button.configure(state='normal' if self.can_read_datasets() else 'disabled')
        for combo in (self.rag_dataset_combo,self.rag_image_dataset_combo):
            combo.configure(state='readonly' if not self.busy or self.task_kind=='processing' else 'disabled')
        importable=bool(selected) and all(item.get('status')=='pass' and item.get('automatic_approval') for item in selected)
        if external and any(self._rag_states.get(iid,{}).get('external_education')
                            for iid in self.rag_result_tree.selection()):
            importable=False
        self.rag_import_button.configure(state='normal' if importable and not self.busy and
            self.current and self.current.get('status')!='running' else 'disabled')
        retryable=any(needs_recovery(self._rag_states[iid]) for iid in self.rag_result_tree.selection() if iid in self._rag_states)
        self.rag_retry_button.configure(state='normal' if retryable and not self.busy else 'disabled')

    def preview_rag(self):
        items=self.selected_rag_items()
        if not items:return self.error('请选择一篇 RAGFlow 结果。')
        item=items[0];state=self._remote_item_status(item)
        lines=[item.get('title',''),'原文件：'+item.get('source_path',''),'论文知识库：'+state['target'],
               '图片知识库：'+state['image_target'],'上传状态：'+state['upload'],'RAGFlow 状态：'+state['process'],
               '结果说明：'+(state['reason'] or '无'),'附属资料：'+state['assets']]
        for record in state['records']:
            lines.extend(['','知识库：'+str(record.get('dataset_name') or record.get('dataset_id')),
                          '远端文档：'+str(record.get('document_id') or '待确认'),
                          '最近查询：'+str(record.get('last_checked_at') or '尚未查询')])
            for asset in record.get('asset_uploads',[]):
                lines.append(str(asset.get('path',''))+'：'+{'uploaded':'已上传','failed':'上传失败','unknown':'上传待确认','sending':'上传待确认'}.get(asset.get('status'),'待上传')+
                    '；远端状态：'+processing_state(asset)+('；'+asset['query_error'] if asset.get('query_error') else ''))
        self.text_window('RAGFlow 处理详情','\n'.join(lines))

    _normalize_remote_doc=staticmethod(normalize_remote_doc)

    def refresh_rag_status(self):
        if self.live_results_active():
            self.request_live_results()
            self.rag_activity.set('正在后台读取已保存的上传与解析状态…')
            return
        from education_stream_status import load_external_ledger
        if self.current and load_external_ledger(self.store.root,self.current['run_id']):
            self.current=self.store.load(self.current['run_id']) or self.current
            self.render_rag_results()
            self.rag_activity.set('已读取教育独立导入回执；远端状态由后台导入器持续核对。')
            return
        if self.busy:return
        if not self.current:return self.error('请先载入一个批次。')
        from ragflow_client import RagflowClient
        from rag_operations import refresh_documents
        try:
            client=RagflowClient(self.url.get(),self.key.get())
            index=UploadIndex(self.store.history());refs=[]
            for item in self.rag_batch_items():
                refs.append(self._remote_item_status(item,index).get('receipt_owner') or
                            (item.get('_result_run_id') or self.current['run_id'],item['id']))
        except (ValueError,OSError) as exc:return self.error(str(exc))
        self.rag_activity.set('正在查询远端状态…')
        self.async_task(lambda:refresh_documents(self.store,refs,client,lambda msg:self.events.put(('rag_progress',msg))),
            lambda count:self.rag_operation_finished(f'已查询 {count} 个远端文档；查询异常请查看原因。'))

    def rag_operation_finished(self,message):
        if self.current:self.current=self.store.load(self.current['run_id'])
        self.render_rag_results();self.rag_activity.set(message);self.state.set(message)

    def import_rag_selected(self):
        if self.busy:return
        if not self.current or self.current.get('status')=='running':
            return self.error('请等待批次处理完成后再导入。')
        items=self.selected_rag_items()
        if not items:return self.state.set('请先选中要导入的文件。')
        if any(item.get('status')!='pass' or not item.get('automatic_approval') for item in items):
            return self.error('只能导入 AI 处理通过的文件，请查看未通过项的原因。')
        from ragflow_client import RagflowClient
        from auto_ragflow import handoff
        from auto_config import load_config
        preextract_only=all(item.get('content_format')=='paper_preextract' for item in items)
        try:
            if not preextract_only and getattr(self,'connected_url',None)!=self.url.get().strip().rstrip('/'):
                raise ValueError('请先读取当前服务的知识库。')
            labels=list(self.dataset_combo.cget('values'));targets={}
            for kind,var,label in (('paper',self.dataset,'论文知识库'),('image',self.image_dataset,'图片知识库')):
                if preextract_only:continue
                if var.get() not in labels:raise ValueError('请先选择'+label+'。')
                dataset=self.datasets[labels.index(var.get())]
                targets[kind]={field:dataset[field] for field in ('id','name')}
            config=load_config(self.store.root);client=RagflowClient(self.url.get(),self.key.get())
            if preextract_only:
                self.processor.stop.clear()
                config['_stop_event']=self.processor.stop
        except (ValueError,OSError) as exc:return self.error(str(exc))
        groups=defaultdict(list)
        for item in items:groups[item.get('_result_run_id') or self.current['run_id']].append(item['id'])
        def work():
            for run_id,ids in groups.items():
                with self.store.lock:
                    manifest=self.store.load(run_id)
                    for item in manifest.get('items',[]):
                        if item.get('id') in ids and item.get('content_format')!='paper_preextract':
                            item['handoff_destinations']=copy.deepcopy(targets)
                            item.pop('handoff_reason',None)
                    self.store.save(manifest)
                handoff(self.store,run_id,client,config.get('dataset_map',{}),auto_parse=True,
                    progress=lambda stage,msg:self.events.put(('rag_progress',stage+'：'+msg)),
                    legacy_dataset_map_fallback=False,concurrency=config.get('concurrency'),item_ids=ids,
                    target_datasets=targets,upload_config=config)
        self.rag_activity.set(f'正在按各篇 MD 最终专业名单导入 {len(items)} 篇文件…' if preextract_only else
            f'正在将选中的 {len(items)} 篇文件导入：论文 → {targets["paper"]["name"]}，图片 → {targets["image"]["name"]}…')
        finished=lambda _:self.rag_operation_finished('所选文件导入操作已结束；请查看上传状态和 RAGFlow 处理状态。')
        if preextract_only:self.async_task(work,finished,kind='processing')
        else:self.async_task(work,finished)

    def retry_handoff(self):
        if self.busy:return
        items=self.selected_rag_items()
        if not items:return self.state.set('请先选中需要恢复的文件。')
        index=UploadIndex(self.store.history())
        items=[item for item in items if needs_recovery(self._remote_item_status(item,index))]
        if not items:return self.state.set('没有需要重试的交接；待选库文件请先选择知识库，再点击“导入选中项”。')
        from ragflow_client import RagflowClient
        from auto_ragflow import handoff
        from auto_config import load_config
        try:config=load_config(self.store.root);client=RagflowClient(self.url.get(),self.key.get())
        except Exception as exc:return self.error(str(exc))
        queued_items=[item for item in items if item.get('queue_task_ids')]
        if queued_items:
            from ingest_queue import QueueStore
            from ingest_workbench import request_queued_review
            from ingest_service import ensure_work_pool_worker,validate_worker_service
            queue_path=config.get('work_pool_queue') or self.store.root/'private'/'universal_ingest'/'queue.sqlite3'
            def review():
                validate_worker_service(self.store.root,client.base_url)
                queue=QueueStore(queue_path)
                count=request_queued_review(queue,[task_id for item in queued_items for task_id in item['queue_task_ids']],
                    lambda dataset,document:client.documents(dataset,document_id=document))
                ensure_work_pool_worker(self.store.root,Path(queue_path),client.base_url,
                                        parse_limit=int(config.get('work_pool_parse_limit',3)))
                return count
            if len(queued_items)!=len(items):
                return self.error('请分别选择队列任务和旧回执任务重试。')
            self.rag_activity.set('正在核对原文档并登记重试…')
            self.async_task(review,lambda count:self.rag_operation_finished(f'已登记 {count} 篇重试，沿用原文档。'))
            return
        groups=defaultdict(list)
        routes={}
        for item in items:
            state=self._remote_item_status(item,index)
            group=(item.get('_result_run_id') or self.current['run_id'],state['target_id'],state['image_target_id'])
            groups[group].append(item['id'])
            routes[group]={'paper':{'id':state['target_id'],'name':state['target']},
                           'image':{'id':state['image_target_id'] or '','name':state['image_target']}}
        def work():
            for group,ids in groups.items():
                run_id=group[0]
                handoff(self.store,run_id,client,config.get('dataset_map',{}),auto_parse=True,
                        progress=lambda stage,msg:self.events.put(('rag_progress',stage+'：'+msg)),
                        legacy_dataset_map_fallback=False,concurrency=config.get('concurrency'),item_ids=ids,
                        target_datasets=routes[group],upload_config=config)
        self.rag_activity.set(f'正在核对并恢复选中的 {len(items)} 篇文件…')
        self.async_task(work,lambda _:self.rag_operation_finished('所选文件恢复操作已结束；请查看最新状态和原因。'))
