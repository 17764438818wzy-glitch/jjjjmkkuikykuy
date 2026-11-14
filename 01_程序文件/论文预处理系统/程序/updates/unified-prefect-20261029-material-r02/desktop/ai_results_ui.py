"""AI outcomes, failure recovery and safe artifact previews for a logical batch."""
from collections import Counter
from pathlib import Path
import tkinter as tk
from tkinter import ttk

from auto_view import _published
from processing_outcomes import AI_LABELS, TASK_LABELS, ai_outcome, execution_outcome
from script_task import task_snapshot, task_rows, script_outcome, effective_run


class AIResultsView:
    def build_ai_results(self):
        tab=self.result_tab
        tab.columnconfigure(0,weight=1);tab.rowconfigure(3,weight=1)
        top=ttk.Frame(tab);top.grid(row=0,column=0,sticky='ew')
        self.ai_batch_label=tk.StringVar(value='尚无 AI 处理记录')
        ttk.Label(top,textvariable=self.ai_batch_label).pack(side='left')
        ttk.Button(top,text='返回选择与处理',command=lambda:self.tabs.select(self.input_tab),style='Task.TButton').pack(side='right')
        self.ai_report_button=ttk.Button(top,text='查看 AI 处理报告',command=self.ai_report,style='Task.TButton')
        self.ai_report_button.pack(side='right',padx=8)
        actions=ttk.Frame(tab);actions.grid(row=1,column=0,sticky='ew',pady=(8,0))
        self.ai_preview_button=ttk.Button(actions,text='查看 AI 结果',command=self.preview,style='Task.TButton')
        self.ai_preview_button.pack(side='left')
        ttk.Button(actions,text='查看图表',command=self.review_evidence,style='Task.TButton').pack(side='left',padx=6)
        ttk.Label(actions,text='搜索').pack(side='left',padx=(12,4))
        self.ai_result_search=tk.StringVar()
        ttk.Entry(actions,textvariable=self.ai_result_search,width=26).pack(side='left',fill='x',expand=True)
        self.ai_result_search.trace_add('write',lambda *_:self.render_preprocess_results())
        selection=ttk.Frame(tab);selection.grid(row=2,column=0,sticky='ew',pady=(6,0))
        self.ai_select_all_button=ttk.Button(selection,text='全选本页',
            command=lambda:self.select_files(self.result_tree,'all'),style='Task.TButton')
        self.ai_select_none_button=ttk.Button(selection,text='取消本页',
            command=lambda:self.select_files(self.result_tree,'none'),style='Task.TButton')
        self.ai_invert_button=ttk.Button(selection,text='反选本页',
            command=lambda:self.select_files(self.result_tree,'invert'),style='Task.TButton')
        for button in (self.ai_select_all_button,self.ai_select_none_button,self.ai_invert_button):
            button.pack(side='left',padx=(0,6))
        self.ai_selection_count=tk.StringVar(value='已选 0 / 0 个文件')
        ttk.Label(selection,textvariable=self.ai_selection_count).pack(side='left',padx=5)
        self.result_tree=self.tree(tab,['编号','题目','原文件名','AI 处理结果','处理环节','结果说明','本地处理','入库状态'],
                                   [145,190,190,95,125,280,90,100],grid_row=3)
        self.result_tree.configure(displaycolumns=['原文件名','题目','AI 处理结果','处理环节','本地处理','入库状态','结果说明'])
        self.result_tree.bind('<Double-1>',lambda _:self.preview())
        footer=ttk.Frame(tab);footer.grid(row=4,column=0,sticky='ew')
        ttk.Label(footer,text='显示').pack(side='left',padx=(0,6))
        self.ai_result_filter=ttk.Combobox(footer,textvariable=self.result_filter,state='readonly',width=12,
                                          values=['未完成','全部结果',*dict.fromkeys(AI_LABELS.values())])
        self.ai_result_filter.pack(side='left')
        self.result_filter.trace_add('write',lambda *_:self.render_preprocess_results())
        self.result_summary=tk.StringVar(value='尚无 AI 处理记录')
        self.ai_summary_label=ttk.Label(tab,textvariable=self.result_summary,foreground='#617082',wraplength=850)
        self.ai_summary_label.grid(row=5,column=0,sticky='ew',pady=(4,0))
        tab.bind('<Configure>',lambda event:self.ai_summary_label.configure(wraplength=max(200,event.width-35)),add='+')
        ttk.Label(footer,text='通过仅表示满足 AI 预处理要求，不评价论文质量。',foreground='#617082').pack(side='left',padx=12)
        self._ai_result_items={}
        retry=ttk.Frame(tab);retry.grid(row=6,column=0,sticky='ew',pady=(8,0))
        self.ai_retry_selected_button=ttk.Button(retry,text='继续选中项',
            command=self.retry_ai,style='Task.TButton')
        self.ai_settings_button=ttk.Button(retry,text='调整 AI 设置',command=self.open_ai_retry_settings,style='Task.TButton')
        self.ai_stop_button=ttk.Button(retry,text='停止 AI 续跑',command=self.stop,style='Task.TButton')
        for button in (self.ai_settings_button,self.ai_retry_selected_button,self.ai_stop_button):
            button.pack(side='left',padx=(0,6))
        self.ai_retry_hint=tk.StringVar(value='选中未完成项后继续；复用已保存的脚本和完整 AI 回答，仅补未完成分段与后处理。')
        self.ai_retry_hint_label=ttk.Label(tab,textvariable=self.ai_retry_hint,foreground='#617082',wraplength=850)
        self.ai_retry_hint_label.grid(row=7,column=0,sticky='ew',pady=(4,0))
        tab.bind('<Configure>',lambda event:self.ai_retry_hint_label.configure(wraplength=max(200,event.width-35)),add='+')
        self.result_tree.bind('<<TreeviewSelect>>',lambda _:self.update_ai_retry_controls())
        self.update_ai_retry_controls()

    def ai_batch_items(self,snapshot=None):
        if not self.current:return []
        snapshot=snapshot if snapshot is not None else self.result_snapshot()
        items=[]
        live=getattr(self,'_live_result',None) if self.live_results_active() else None
        rows=live['rows'] if live and snapshot is live['snapshot'] else task_rows(snapshot)
        for row in rows:
            item=dict(row.get('result_item') or {})
            if not item:
                item={'id':'WAIT_'+str(len(items)), 'source_path':row['path'], 'title':row['name'],
                      'status':'pending','ai_result':{'status':'not_started','reason':'尚未进入 AI 处理','stage':''},
                      '_result_run_id':self.current['run_id']}
            item['_ai_run_status']=item.get('_result_run_status',self.current.get('status'))
            items.append(item)
        return items

    def render_ai_results(self,*,refresh=True):
        try:snapshot=self.result_snapshot(refresh=refresh)
        except (ValueError,OSError) as exc:
            self.result_tree.delete(*self.result_tree.get_children())
            self._ai_result_items={};self._ai_render_stamp=None;self._ai_render_snapshot=None
            self.ai_batch_label.set('当前批次：'+str((self.current or {}).get('run_id','')))
            self.result_summary.set('无法载入本批 AI 记录：'+str(exc))
            self.update_ai_retry_controls();return
        stamp=(id(snapshot),self.result_filter.get(),self.ai_result_search.get())
        if not refresh and stamp==getattr(self,'_ai_render_stamp',None):
            self.update_ai_retry_controls();return
        selected=set(self.result_tree.selection())
        live_mode=self.live_results_active()
        if not live_mode:
            self.result_tree.delete(*self.result_tree.get_children())
            self.result_tree._live_values={}
        from display_page import page_for,cell_values
        from live_results import sync_tree
        if not self.current:
            sync_tree(self.result_tree,[]);self._ai_result_items={}
            self.result_summary.set('尚无 AI 处理记录');self.update_ai_retry_controls();return
        try:items=self.ai_batch_items(snapshot)
        except (ValueError,OSError) as exc:
            self.result_summary.set('无法载入本批 AI 记录：'+str(exc));return
        self.ai_batch_label.set('当前批次：'+self.current['run_id'])
        counts=Counter();records=[];seen={}
        query=self.ai_result_search.get().strip().casefold();chosen=self.result_filter.get()
        for item in items:
            label,reason,stage=ai_outcome(item,item.get('_ai_run_status'));counts[label]+=1
            local=execution_outcome(item,snapshot)[0]
            done=(local in {'通过','已保存','已删除'} and label in {'通过','未进入 AI','已删除'}) or item.get('reason_kind')=='duplicate'
            iid=item['id'];number=seen.get(iid,0);seen[iid]=number+1
            iid=iid if not number else iid+'@'+str(number)
            if chosen=='未完成' and done:continue
            if chosen!='未完成' and chosen not in {'全部结果',label}:continue
            filename=Path(item.get('source_path','')).name
            if query and query not in (filename+' '+item.get('title','')+' '+item['id']).casefold():continue
            records.append((iid,item,label,reason,stage,filename))
        summary=' · '.join(f'{label} {counts[label]}' for label in AI_LABELS.values() if counts[label])
        def paint(visible,total):
            from progress_summary import display_phase,delivery_label
            result=[];self._ai_result_items={}
            for iid,item,label,reason,stage,filename in visible:
                phase=display_phase(item)
                post=(item.get('script_result') or {}).get('status')=='running' and phase in {'后处理','等待后处理'}
                if phase and ((item.get('ai_result') or {}).get('status') in {'running','response_saved','not_started'} or post):stage=phase
                local_label=script_outcome(item,self.current)[0]
                if label=='通过' and local_label!='通过':reason+='；本地结果尚不可用：'+script_outcome(item,self.current)[1]
                result.append((iid,cell_values((item['id'],item.get('title',''),filename,label,stage,reason,local_label,delivery_label(item)))))
                self._ai_result_items[iid]=item
            sync_tree(self.result_tree,result)
            self.result_summary.set(f'累计结果 {len(items)} 份 · 当前显示 {len(visible)}/{total} 份'+(' · '+summary if summary else ''))
            self.update_ai_retry_controls()
        self._ai_render_snapshot=snapshot
        self._ai_snapshot_manifest=self.current
        context=(id(self.store),self.current['run_id'],self.result_filter.get(),self.ai_result_search.get())
        page_for(self,'ai',self.result_tree).show(records,stamp,paint,context=context,selected=selected)
        self._ai_render_snapshot=snapshot
        self._ai_render_stamp=stamp
        self.update_ai_retry_controls()

    def update_ai_retry_controls(self):
        if not hasattr(self,'ai_retry_selected_button'):return
        from ai_resume import retry_records
        snapshot=None
        if self.current and not self.busy:
            snapshot=(getattr(self,'_ai_render_snapshot',None) if getattr(self,'_ai_snapshot_manifest',None) is self.current
                      else self.result_snapshot(refresh=False))
        ready=(not self.busy and snapshot and snapshot.get('status')!='running'
               and not snapshot.get('_retry_blocked_runs'))
        visible=len(self.result_tree.get_children())
        self.ai_selection_count.set(f'已选 {len(self.result_tree.selection())} / {visible} 个文件')
        for button in (self.ai_select_all_button,self.ai_select_none_button,self.ai_invert_button):
            button.configure(state='normal' if visible and not self.busy else 'disabled')
        chosen=len(retry_records(self.selected_ai_items()))
        self.ai_retry_selected_button.configure(state='normal' if ready and chosen else 'disabled')
        self.ai_settings_button.configure(state='disabled' if self.busy else 'normal')
        active=self.busy and getattr(self,'_active_ai_retry',False)
        stopping=active and self.processor.stop.is_set()
        self.ai_stop_button.configure(state='normal' if active and not stopping else 'disabled',
            text='等待当前请求结束…' if stopping else '停止 AI 续跑')

    def open_ai_retry_settings(self):
        if self.busy:return
        self.tabs.select(self.ai_tab)
        self.state.set('调整模型、接口或超时设置后，回到“AI 处理结果”继续未通过项；续跑读取当前设置。')

    def retry_ai(self):
        if self.busy or not self.current:return
        selected_ids={item['id'] for item in self.selected_ai_items()}
        try:
            self.current=effective_run(self.store.root,self.store.load(self.current['run_id']))
            if self.current.get('status')=='running':
                return self.state.set('该批次仍在处理，请等待结束后再续跑 AI。')
            from ai_resume import retry_records
            snapshot=self.result_snapshot(self.current)
            if snapshot.get('_retry_blocked_runs'):
                return self.state.set('历史任务仍在运行或状态待确认，暂不重复派发 AI。')
            items=self.ai_batch_items()
            items=[item for item in items if item['id'] in selected_ids]
            selected=retry_records(items)
            if not selected:return self.state.set('没有可续跑的 AI 未通过项；已通过文件不会重跑。')
            missing=[row['name'] for row in selected if not row.get('prepared_checkpoint')]
            if missing:
                return self.error('以下记录缺少脚本中间结果，无法直接续跑 AI：\n'+'\n'.join(missing[:8])+
                    '\n旧批次需先在第一页重新生成一次脚本结果；本次没有启动处理。')
        except (ValueError,OSError) as exc:return self.error(str(exc))
        self._pending_operation='retry_ai'
        self.start_automatic(selected)

    def selected_ai_items(self):
        return [self._ai_result_items[iid] for iid in self.result_tree.selection() if iid in self._ai_result_items]

    def ai_detail(self,item):
        label,reason,stage=ai_outcome(item,item.get('_ai_run_status'))
        local,local_reason=execution_outcome(item,self.current or {})
        lines=[item.get('title') or Path(item.get('source_path','')).name,'记录编号：'+item.get('id',''),
               '原文件：'+item.get('source_path',''),'AI 处理结果：'+label,'处理环节：'+(stage or '未记录'),
               '结果说明：'+(reason or '无'),'脚本执行：'+local+'；'+local_reason,'','AI 处理记录：']
        statuses={'pass':'完成','reject':'不通过','error':'调用失败','validation_failed':'结果校验失败','running':'处理中'}
        if label=='结果校验失败':statuses['error']='结果校验失败'
        for step in item.get('ai_steps',[]):
            lines.append(' · '.join(filter(None,[step.get('stage') or TASK_LABELS.get(step.get('task'),''),
                         statuses.get(step.get('status'),step.get('status','')),
                         '复用已校验结果' if step.get('cache_hit') else '',step.get('reason','')])))
        if not item.get('ai_steps'):
            for metric in item.get('processing_metrics',[]):
                lines.append(TASK_LABELS.get(metric.get('task'),metric.get('task',''))+' · '+statuses.get(metric.get('outcome'),'未记录'))
            if not item.get('processing_metrics'):lines.append('此记录没有逐环节 AI 明细。')
        if label!='通过' or script_outcome(item,self.current or {})[0]!='通过':
            lines.extend(['','尚无通过发布检查的完整正文；这里保留已完成环节和失败说明。'])
        if item.get('previous_ai_reason'):
            lines.extend(['','上次 AI 未通过原因：'+item['previous_ai_reason']])
        if item.get('content_format')=='paper_preextract':
            from preextract_workbench import candidate_diagnostics
            diagnostics=candidate_diagnostics(item,self.store.root)
            if diagnostics:lines.extend(['','实际校验问题：',*diagnostics])
        from ai_resume import needs_ai_resume
        if needs_ai_resume(item):
            hint=('下一步：按实际校验问题复核；程序会核对并复用可用的已保存回答，失败或未决请求不会自动重发。'
                  if item.get('content_format')=='paper_preextract' and (item.get('ai_result',{}).get('status')=='incomplete' or label=='结果校验失败')
                  else '下一步：查看实际调用错误，按对应原因处理后再续跑。')
            lines.extend(['',hint,
                '续跑复用脚本结果；如果修改了原始论文，需要在第一页重新生成对应文件的脚本结果。'])
            if label=='通过':lines.append('AI 检查已通过，但保存结果未完成；可以在本页续跑收尾，未变更的 AI 结果将复用。')
            if not item.get('prepared_checkpoint'):
                lines.append('此记录缺少脚本中间结果，暂不能直接续跑 AI。')
        return '\n'.join(lines)

    def preview_ai_result(self):
        items=self.selected_items()
        if not items:return self.error('请选择一篇 AI 处理结果。')
        item=items[0]
        content=self.ai_detail(item)
        if item.get('content_format')=='paper_preextract':
            from preextract_workbench import candidate_text
            try:content+='\n\n完整 AI 提取结果（本地私有候选）：\n'+candidate_text(item,self.store.root)
            except (OSError,ValueError,KeyError):content+='\n\n候选文件暂不可读取；已保存的阶段记录保留。'
            return self.text_window('AI 处理结果',content)
        if script_outcome(item,self.current)[0]=='通过':
            try:
                run=self.store.manifest_path(item.get('_result_run_id') or self.current['run_id']).parent
                package=_published(item['package_path'],run)
                path=_published(item.get('display_path') or item['path'],package)
                content+='\n\n处理后的正文：\n'+path.read_text(encoding='utf-8')
            except (OSError,UnicodeError,ValueError,KeyError,TypeError):
                return self.error('正文文件缺失、不可读取或路径无效；AI 结论与本地文件异常需分别核对。')
        self.text_window('AI 处理结果',content)

    def ai_report(self):
        if not self.current:return self.error('请先载入或完成一个批次。')
        self.text_window('AI 处理报告','\n\n'.join(self.ai_detail(item) for item in self.ai_batch_items()))
