"""Local script task presentation; retries retain the original batch results."""
from collections import Counter
import copy
from pathlib import Path
import tkinter as tk
from tkinter import ttk

from auto_view import outcome, result_items
from archive_identity import source_key, record_key, member_fields


def effective_run(root, manifest):
    """Read-only lifecycle projection; preserve the durable run and its results."""
    if manifest and manifest.get('status')=='running':
        from model_activity import run_owner_state
        if run_owner_state(root,manifest.get('run_id'))=='dead':
            return dict(manifest,status='interrupted',message='上次处理进程已退出；已保存结果保留，等待手动继续')
    return manifest


def load_history_deletions(config_path=None):
    """Honor verified deletion receipts; never delete or hide existing files."""
    import hashlib,json
    from project_paths import ROOT
    config_path=Path(config_path) if config_path else ROOT/'config/task-history.json'
    base=config_path.parent.parent.resolve()
    result={}
    try:
        config=json.loads(config_path.read_text(encoding='utf8'))
        if config.get('schema')!='task-history-display/1':return result
        for ref in config.get('deletion_receipts',[]):
            path=(base/ref['file']).resolve()
            if not path.is_relative_to(base):continue
            content=path.read_bytes()
            if hashlib.sha256(content).hexdigest()!=ref.get('sha256'):continue
            receipt=json.loads(content)
            if receipt.get('status')!='complete':continue
            for row in receipt.get('files',[]):
                if row.get('status')=='deleted' and len(row.get('sha256',''))==64 and row.get('path'):
                    result[source_key(row['path'])]=row['sha256']
    except (OSError,ValueError,TypeError,KeyError):
        pass
    return result


def row_values(row):
    return (row.get('name') or Path(row['path']).name, row.get('script_status', '待处理'),
            row.get('script_reason', ''), row.get('previous_error', ''), row['path'])


def script_outcome(item, manifest):
    if '_script_label' in item:
        return item['_script_label'], item.get('_script_reason', '')
    label, reason = outcome(item, manifest.get('status'))
    if label == '通过':
        issues = [row['reason'] for row in manifest.get('human_view', {}).get('issues', [])
                  if isinstance(row, dict) and row.get('id') == item.get('id') and row.get('reason')]
        if issues:
            return '不通过', '技术失败：' + '；'.join(issues)
    return label, reason


def task_snapshot(store, manifest):
    """Follow durable retry links, retaining results that were not retried."""
    chain, seen = [], set()
    root=getattr(store,'root',None)
    current = effective_run(root,manifest) if root is not None else manifest
    manifest=current
    deletions=load_history_deletions()
    while current:
        run_id = current.get('run_id')
        if run_id in seen:
            raise ValueError('批次重试记录存在循环，无法安全载入。')
        seen.add(run_id)
        chain.append(current)
        previous = current.get('retry_of')
        current = store.load(previous) if previous else None
        if current and root is not None:current=effective_run(root,current)
        if previous and current is None:
            raise ValueError('原批次记录缺失，无法安全载入全部结果。')
    chain.reverse()
    # These three collections are replaced by the merged retry projection.
    # Copy only the final surviving records, rather than every superseded item
    # and another complete copy of the active manifest first.
    merged = copy.deepcopy({key:value for key,value in manifest.items()
                            if key not in {'inputs','items','events'}})
    merged['_retry_blocked_runs']=[run['run_id'] for run in chain if run.get('status')=='running']
    records, inputs, errors = {}, {}, {}
    for run in chain:
        completed_sources = {source_key(child.get('input_source', '')) for child in run.get('child_runs', [])
                             if child.get('status') == 'completed'}
        incoming = result_items(run)
        finalized={i.get('id') for i in incoming}
        incoming += [i for i in run.get('active_items',[]) if i.get('id') not in finalized]
        incoming += [
            {'id': 'SKIP_' + str(n), 'source_path': event.get('source', ''),
             'input_source_path': event.get('input_source_path', ''),
             'status': 'reject', 'reason_kind': 'duplicate', 'reason': '已有完成记录，脚本跳过'}
            for n, event in enumerate(run.get('events', [])) if event.get('status') == 'uploaded_skip'
        ]
        run_inputs = [*run.get('inputs', []), *run.get('discovered_inputs', [])]
        represented={record_key(i) for i in incoming}
        represented.update(source_key(i.get('input_source_path','')) for i in incoming)
        if run.get('status') in {'stopped','interrupted','failed'}:
            for entry in run_inputs:
                key=record_key(entry)
                if key and key not in represented and key not in records:
                    incoming.append({'id':'UNFINISHED_'+str(len(incoming)), 'source_path':entry['path'],
                                     **member_fields(entry),
                                     'status':'reject','reason_kind':'stopped',
                                     '_unstarted':True,'reason':'本批停止或中断，脚本未完成，可继续处理'})
        for entry in run_inputs:
            if entry.get('path'):
                inputs[record_key(entry)] = entry
        for item in incoming:
            path=item.get('source_path')
            if (path and source_key(path) in deletions and not Path(path).exists()
                    and (not item.get('source_hash') or item['source_hash']==deletions[source_key(path)])):
                item=dict(item,status='deleted',reason_kind='material_deleted',
                          reason='已按历史确认记录删除，不再续跑',script_result={'status':'deleted'})
            key = record_key(item)
            parent = source_key(item.get('input_source_path', ''))
            # A later successful extraction replaces an earlier archive-level
            # error with the actual member results (which may still fail AI).
            if ((run.get('status') == 'completed' or parent in completed_sources) and parent and parent != key
                    and Path(parent).suffix in {'.zip', '.rar', '.7z'}
                    and item.get('status') in {'pass', 'reject'}):
                previous_archive = records.get(parent)
                if previous_archive and previous_archive.get('reason_kind') in {'technical', 'stopped'}:
                    records.pop(parent)
            prior = records.get(key)
            if prior and prior.get('_script_label') == '不通过' and prior.get('reason_kind') != 'duplicate':
                errors[key] = prior.get('_script_reason', '')
            label, reason = script_outcome(item, run)
            records[key] = dict(item, _script_label=label, _script_reason=reason,
                                _result_run_id=run.get('run_id'),_result_run_status=run.get('status'))
    merged['inputs'] = copy.deepcopy(list(inputs.values()))
    merged['items'] = copy.deepcopy(list(records.values()))
    merged['events'] = []
    merged['_previous_errors'] = errors
    merged['_root_run_id'] = chain[0].get('run_id')
    # A queued retry retains the prior decision for history, but is not a
    # failure of this running attempt until it has produced a new result.
    represented = {record_key(i) for i in incoming}
    represented.update(source_key(i.get('input_source_path', '')) for i in incoming)
    merged['_pending_sources'] = sorted({record_key(row) for row in [*manifest.get('inputs', []), *manifest.get('discovered_inputs', [])]
                                         if row.get('path')} - represented) if manifest.get('status') == 'running' else []
    return merged


def task_rows(manifest, *, for_display=False):
    """Show one row per document, plus sources that have not produced a result."""
    rows = {}
    archive_inputs = {source_key(i['input_source_path']) for i in [*manifest.get('items', []), *manifest.get('inputs', [])]
                      if i.get('input_source_path') and
                      source_key(i['input_source_path']) != source_key(i.get('source_path', ''))}
    for record in manifest.get('inputs', []):
        path = record.get('path', '')
        if not path or source_key(path) in archive_inputs:
            continue
        rows[record_key(record)] = dict(record, name=record.get('name') or Path(path).name,
                                     script_status='待处理', script_reason='', previous_error='')
    for item in manifest.get('items', []):
        path = item.get('source_path') or item.get('id', '')
        key = record_key(item)
        from processing_outcomes import execution_outcome
        label, reason = execution_outcome(item, manifest)
        if for_display and item.get('_unstarted') and item.get('status')!='deleted':
            label,reason='待处理','上次中断前尚未处理，等待继续'
        if item.get('reason_kind') == 'duplicate':
            label = '重复跳过'
        previous_error = manifest.get('_previous_errors', {}).get(key, '')
        if key in manifest.get('_pending_sources', ()) and label not in {'通过', '重复跳过'}:
            previous_error = reason or previous_error
            label, reason = '待处理', '已加入本次续跑队列，等待处理'
        rows[key] = dict(rows.get(key, {}), path=path, name=Path(path).name,
                         script_status=label, script_reason=reason,
                         previous_error=previous_error,
                         result_item=item)
    if for_display:
        for row in rows.values():
            if (row['script_status'] not in {'通过','重复跳过','已保存','已删除'}
                    and row.get('path') and not Path(row['path']).is_file()):
                row.update(script_status='源文件缺失',
                           script_reason='原路径文件不存在，续跑时保留此项；恢复文件后重新载入可继续')
    return list(rows.values())


class ScriptTaskView:
    def build_task_settings(self):
        frame = self.processing_mode_row
        frame.configure(text='本次任务设置')
        for column in (1, 3):
            frame.columnconfigure(column, weight=1)
        self.task_text_model_combo = ttk.Combobox(frame, textvariable=self.ai_vars['text_model'],
                                                  state='readonly', width=23, postcommand=self.refresh_task_models)
        self.task_vision_model_combo = ttk.Combobox(frame, textvariable=self.ai_vars['vision_model'],
                                                    state='readonly', width=23, postcommand=self.refresh_task_models)
        self.task_dataset_combo = ttk.Combobox(frame, textvariable=self.dataset, state='readonly', width=23)
        self.task_image_dataset_combo = ttk.Combobox(frame, textvariable=self.image_dataset, state='readonly', width=23)
        for row, left, right, left_label, right_label in (
            (0, self.task_text_model_combo, self.task_vision_model_combo, '文本模型', '视觉模型'),
            (1, self.task_dataset_combo, self.task_image_dataset_combo, '论文知识库', '图片知识库'),
        ):
            ttk.Label(frame, text=left_label).grid(row=row, column=0, sticky='w', padx=(0, 8))
            left.grid(row=row, column=1, sticky='ew')
            ttk.Label(frame, text=right_label).grid(row=row, column=2, sticky='w', padx=(14, 8))
            right.grid(row=row, column=3, sticky='ew')
        self.task_model_settings_button = ttk.Button(frame, text='模型配置', command=lambda: self.tabs.select(self.ai_tab))
        self.task_model_settings_button.grid(row=0, column=4, padx=(8, 0), sticky='ew')
        library_actions = ttk.Frame(frame)
        library_actions.grid(row=1, column=4, padx=(8, 0), sticky='ew')
        self.task_connect_button = ttk.Button(library_actions, text='读取知识库', command=self.connect)
        self.task_connect_button.pack(side='left')
        self.task_defer_button = ttk.Button(library_actions, text='稍后入库', command=self.defer_task_upload, style='Task.TButton')
        self.task_defer_button.pack(side='left', padx=(4, 0))
        for control in (self.task_model_settings_button,self.task_connect_button,self.concurrency_settings_button):
            control.configure(style='Task.TButton')
        self.task_hint = tk.StringVar()
        self.task_hint_label = ttk.Label(self.input_tab, textvariable=self.task_hint, foreground='#617082')
        self.task_hint_label.grid(row=5, column=0, sticky='ew')
        self.input_tab.bind('<Configure>', lambda event: self.task_hint_label.configure(wraplength=max(200, event.width-30)), add='+')
        self.task_option_controls = [self.task_text_model_combo, self.task_vision_model_combo,
                                     self.task_dataset_combo, self.task_image_dataset_combo]
        for key in ('text_model', 'vision_model', 'vision_fallback_model', 'vision_fallback_enabled', 'key'):
            self.ai_vars[key].trace_add('write', lambda *_: self.refresh_task_models())
        for var in (self.dataset, self.image_dataset, self.key):
            var.trace_add('write', lambda *_: self.update_controls())
        self.refresh_task_models()

    def refresh_task_models(self):
        for field, combo in (('text_model', self.task_text_model_combo), ('vision_model', self.task_vision_model_combo)):
            values = list(self.ai_model_combos[field].cget('values'))
            current = self.ai_vars[field].get().strip()
            if current and current not in values:
                values.append(current)
            combo.configure(values=values)
        self.update_controls()

    def task_setup_problem(self):
        if not hasattr(self, 'ai_vars'):
            return '正在载入配置'
        missing = [label for field, label in (('text_model', '提取模型（支持图像）'),)
                   if not self.ai_vars[field].get().strip()]
        if not self.ai_vars['key'].get().strip():
            missing.append('模型接口密钥')
        if missing:
            return '开始前请配置：' + '、'.join(missing)
        if not self.ai_vars['auto_upload'].get():
            return ''
        if not self.key.get().strip():
            return '开始前请配置：RAGFlow 连接密钥；也可点击“稍后入库”先处理'
        from auto_config import validate_api_key
        from urllib.parse import urlsplit
        try:
            validate_api_key(self.key.get().strip(), 'RAGFlow API Key')
            target = urlsplit(self.url.get().strip())
            if target.scheme not in {'http', 'https'} or not target.netloc:
                return '开始前请填写有效的 RAGFlow 服务地址；也可点击“稍后入库”先处理'
        except ValueError as exc:
            return str(exc)
        # New processing uses the final MD specialties. Legacy single-library
        # choices remain checked by their original import/recovery entrypoints.
        return ''

    def defer_task_upload(self):
        if self.busy:
            return
        self.ai_vars['auto_upload'].set(False)
        self.dataset.set('')
        self.image_dataset.set('')

    def task_settings_snapshot(self, config):
        labels = list(self.dataset_combo.cget('values'))
        destinations = {}
        for kind, var in (('paper', self.dataset), ('image', self.image_dataset)):
            dataset = self.datasets[labels.index(var.get())] if var.get() in labels else {}
            destinations[kind] = {field: dataset[field] for field in ('id', 'name') if field in dataset}
        return {'text_model': config['text_model'],
                'vision_model': config['vision_model']['primary']['model'],
                'vision_fallback_model': config['vision_model']['fallback'].get('model', '')
                    if config['vision_model']['fallback'].get('enabled') else '',
                'destinations': destinations,
                'concurrency': copy.deepcopy(config['concurrency'])}

    def _script_row_state(self):
        from collections import deque
        from script_refresh import RowIndex
        selected=self.script_filter.get()
        if getattr(self,'_script_legacy_render_active',False):
            selected=self._script_legacy_render_filter
        state=getattr(self,'_script_row_index',None)
        if state is None or state.rows is not self.sources or len(state.paths)!=len(self.sources):
            state=self._script_row_index=RowIndex(self.sources,selected,source_key,row_values)
            self._script_drawn=set(range(len(self.sources)))
            self._script_shown={int(i) for i in self.source_tree.get_children() if i.isdigit()}
            self._script_shown_order=sorted(self._script_shown)
            self._script_draw_queue=deque()
            self._script_draw_queued=set()
        elif selected!=state.selected:
            changed=state.filter(selected)
            self._queue_script_rows(changed)
        return state

    def render_script_rows(self,rows,select_all=False):
        from collections import deque
        from script_refresh import RowIndex
        old=self._script_row_state()
        self.selected_source_rows()
        selected={old.keys[i] for i in getattr(self,'_source_selected_indices',()) if i<len(old.keys)}
        paths=tuple(row['path'] for row in rows)
        if paths==old.paths:
            old.replace(rows);self.sources=rows
            state=old
        else:
            self.sources=rows
            state=self._script_row_index=RowIndex(rows,old.selected,source_key,row_values)
            self._script_draw_queue=deque();self._script_draw_queued=set()
        self._source_selected_indices=(set(state.visible) if select_all else
            {i for i,key in enumerate(state.keys) if key in selected and i in state.visible_set})
        self._script_paint_preserves_selection=True
        self._queue_script_rows(())

    def _queue_script_rows(self,indices):
        # Index changes are already in RowIndex; never queue thousands of Tk rows.
        self._script_draw_pending=True
        if getattr(self,'_script_draw_after',None) is None:
            if len(self.sources)<=50 and not getattr(self,'_script_legacy_render_active',False):
                self._paint_script_rows()
            else:self._script_draw_after=self.root.after(1,self._paint_script_rows)

    def _paint_script_rows(self):
        from display_page import page_for,cell_values
        from live_results import sync_tree
        legacy=getattr(self,'_script_legacy_render_frame',None)
        if legacy is not None:
            import sys,threading
            frame=sys._current_frames().get(threading.main_thread().ident)
            while frame:
                if frame is legacy:
                    self._script_draw_after=self.root.after(10,self._paint_script_rows);return
                frame=frame.f_back
            del self._script_legacy_render_frame
        self._script_legacy_render_active=False;self._script_draw_after=None
        state=self._script_row_state()
        if not getattr(self,'_script_paint_preserves_selection',False):self.selected_source_rows()
        self._script_paint_preserves_selection=False
        self._source_selected_indices.intersection_update(state.visible_set)
        def paint(indices,total):
            desired={str(i) for i in indices}
            # Earlier releases kept detached rows in Tcl; remove those once too.
            prior=getattr(self,'_script_drawn',set())
            stale=[str(i) for i in prior if str(i) not in desired and self.source_tree.exists(str(i))]
            if stale:self.source_tree.delete(*stale)
            sync_tree(self.source_tree,[(str(i),cell_values(row_values(self.sources[i]))) for i in indices])
            self._script_drawn=set(indices);self._script_shown=set(indices);self._script_shown_order=list(indices)
            self.source_tree.selection_set([str(i) for i in indices if i in self._source_selected_indices])
            self.refresh_script_summary();self.update_controls()
        recent=getattr(self,'_source_recent_indices',{})
        records=([i for i in state.visible if i not in recent]+[i for i in recent if i in state.visible_set]
                 if recent else state.visible)
        page_for(self,'source',self.source_tree).show(records,(id(state),state.selected),paint,self.selected_source_rows)
        self._script_draw_pending=False

    def script_row_matches(self,row):
        from script_refresh import matches
        return matches(row,self.script_filter.get())

    def apply_script_filter(self):
        self.selected_source_rows()
        state=self._script_row_state()
        state.filter(self.script_filter.get())
        self._source_selected_indices.intersection_update(state.visible_set)
        self._script_paint_preserves_selection=True
        self._queue_script_rows(())

    def sync_script_filter_row(self,index):
        from collections import OrderedDict
        state=self._script_row_state()
        state.update(index)
        recent=getattr(self,'_source_recent_indices',None)
        if recent is None:recent=self._source_recent_indices=OrderedDict()
        recent.pop(index,None);recent[index]=None
        while len(recent)>50:recent.popitem(last=False)
        self._queue_script_rows((index,))

    def refresh_script_summary(self):
        state=getattr(self,'_script_row_index',None)
        if state is None or state.rows is not self.sources:
            state=self._script_row_state()
        counts=state.counts
        finished=sum(counts[label] for label in ('通过','重复跳过','已保存','已删除'))
        text=f"合并任务 {len(self.sources)} 份 · 未完成 {len(self.sources)-finished} · 已完成 {finished} · 显示 {len(self.source_tree.get_children())}/{len(state.visible)} · 不通过 {counts['不通过']}"
        if counts['源文件缺失']:text+=f" · 缺源 {counts['源文件缺失']}"
        self.script_summary.set(text)

    def queue_script_progress(self,payload):
        from script_refresh import ProgressBuffer
        buffer=getattr(self,'_script_progress_buffer',None)
        if buffer is None:
            buffer=self._script_progress_buffer=ProgressBuffer(source_key)
        if buffer.terminal(payload):
            self.show_script_progress(payload)
            return
        buffer.add(payload)
        if getattr(self,'_script_progress_after',None) is None:
            self._script_progress_after=self.root.after(250,self.flush_script_progress)

    def script_progress_barrier(self,event,value):
        buffer=getattr(self,'_script_progress_buffer',None)
        return bool(buffer and buffer.pending and
                    (event!='progress' or buffer.terminal(value)))

    def publish_script_progress_counts(self):
        buffer=getattr(self,'_script_progress_buffer',None)
        if not buffer or not buffer.pending:return
        latest=buffer.latest
        if 'processed_sources' not in latest and 'source_count' not in latest:return
        completed=latest.get('processed_sources',0)
        total=latest.get('source_count',0)
        if self.current and self.current.get('run_id')==buffer.run_id:
            total=max(total,self.current.get('source_count',0))
        stamp=(buffer.run_id,completed,total)
        if stamp!=getattr(self,'_script_queued_counts_stamp',None):
            self._script_queued_counts_stamp=stamp
            self.progress_bar.configure(value=completed,maximum=max(1,total))
            self.progress_count.set(f'本次脚本完成 {completed}/{total}')

    def flush_script_progress(self,force=False):
        from time import monotonic
        timer=getattr(self,'_script_progress_after',None)
        if timer is not None:
            self.root.after_cancel(timer)
        self._script_progress_after=None
        buffer=getattr(self,'_script_progress_buffer',None)
        if buffer is None:return
        deadline=monotonic()+.010
        while buffer.pending:
            self.show_script_progress(buffer.pop())
            if monotonic()>=deadline:break
        if buffer.pending:
            self._script_progress_after=self.root.after(1,self.flush_script_progress)

    def show_script_results(self, manifest,*,refresh=True):
        self._script_manifest = self.result_snapshot(manifest,refresh=refresh)
        self.render_script_rows(task_rows(self._script_manifest,for_display=True))

    def show_script_progress(self, payload):
        progress = dict(payload.get('progress') or {})
        if str(progress.get('stage', '')).startswith('RAGFlow'):
            return
        intake=payload.get('intake_progress')
        if intake:
            text=f"{intake['phase']} {intake['completed']}/{intake['total']} · 尚未进入 AI"
            self.progress_bar.configure(value=intake['completed'],maximum=max(1,intake['total']))
            self.progress_count.set(text)
            self.progress_detail.set(text+(' · '+progress['file'] if progress.get('file') else ''))
            self.state.set(text)
            return
        run_id = payload.get('run_id')
        completed, total = payload.get('processed_sources', 0), payload.get('source_count', 0)
        counters=payload.get('pipeline_concurrency')
        fresh_counts=counters is not None
        if counters is not None:
            self._pipeline_counts=counters
            self._pipeline_run=run_id
        else:counters=getattr(self,'_pipeline_counts',{}) if getattr(self,'_pipeline_run',None)==run_id else {}
        from progress_summary import pipeline_text
        limit=((self.current or {}).get('concurrency') or {}).get('global_llm')
        if not limit:limit=(getattr(self.processor,'config',{}).get('concurrency') or {}).get('global_llm')
        summary=pipeline_text(counters,limit)
        panel=getattr(self,'_model_activity_panel',None)
        if panel is not None and fresh_counts and counters:panel.observe_pipeline(run_id,counters,limit)
        live=self.live_results_active()
        if live:self.request_live_results(run_id)
        stamp = (run_id, completed,self.result_file_stamp(self.store.manifest_path(run_id)) if run_id and not live else None)
        if not live and run_id and (progress.get('stage')=='任务结束' or stamp != getattr(self, '_script_progress_stamp', None)):
            manifest = self.store.load(run_id)
            if manifest:
                self.current = manifest
                self.show_script_results(manifest,refresh=False)
                if self.tabs.select()==str(self.result_tab):self.render_preprocess_results()
            self._script_progress_stamp = stamp
        if (self.current and self.current.get('run_id')==run_id and self.current.get('operation')=='retry_ai'):
            self.ai_retry_hint.set(f'本轮已结束 {completed}/{total} · '+(summary or str(progress.get('stage') or '处理中')))
            self.progress_bar.configure(value=completed,maximum=max(1,total))
            self.progress_count.set(f'本轮已结束 {completed}/{total}')
            self.progress_detail.set(summary or str(progress.get('stage') or '处理中'))
            from billing_ui import update_progress
            update_progress(self,payload)
            self.state.set(self.ai_retry_hint.get())
            return
        if self.current and self.current.get('run_id')==run_id:
            # Prepared material is still in flight; it is not a finished paper.
            total=max(total,self.current.get('source_count',0))
        if payload.get('ai_item') or str(progress.get('stage','')).startswith('AI ') or progress.get('stage')=='续跑复用':
            self.progress_bar.configure(value=completed,maximum=max(1,total))
            self.progress_count.set(f'本轮已结束 {completed}/{total}')
            if summary:self.progress_detail.set(summary)
            return
        progress.update(completed=completed, total=total)
        row_index=self._script_row_state()
        matches=row_index.find(progress)
        if len(matches) == 1 and progress.get('stage') not in {'通过', '不通过', '跳过', '任务结束', '脚本处理结束','待解析','关联资料保存','已删除过程表单'}:
            index=matches[0]
            row = self.sources[index]
            if row.get('script_status') != '通过':
                if row.get('script_status') == '不通过':
                    row['previous_error'] = row.get('script_reason', '')
                row.update(script_status='处理中', script_reason=progress.get('stage', '处理中'))
                # Per-page/model events must not rebuild hundreds of table rows.
                self.sync_script_filter_row(index)
        if progress.get('stage') == '任务结束':
            progress['stage'] = '脚本处理结束'
            progress['message']=self.script_summary.get()
        if progress.get('stage'):
            self.show_progress(progress)
        else:
            self.progress_bar.configure(value=completed, maximum=max(1, total))
            self.progress_count.set(f'本次脚本完成 {completed}/{total}')
        from billing_ui import update_progress
        update_progress(self, payload)
        message=progress.get('message','') if progress.get('stage')=='脚本处理结束' else payload.get('message','')
        self.state.set(f"{message} · 本次脚本 {completed}/{total}")

    def load_latest_script_results(self):
        if self.busy:return
        from display_page import load_latest_results
        load_latest_results(self)

    def show_unified_tasks(self):
        self.load_latest_script_results()
        self.script_filter.set('未完成')
        self.result_filter.set('未完成')
        self.rag_upload_filter.set('未完成')
        self.tabs.select(self.input_tab)

    def preview_script_row(self, _event=None):
        selected = self.source_tree.selection()
        if not selected:
            return
        row = self.sources[int(selected[0])]
        item = row.get('result_item', {})
        text = '\n'.join((row.get('name', ''), '脚本状态：' + row.get('script_status', '待处理'),
                          '本次结果：' + row.get('script_reason', ''),
                          '上次失败：' + (row.get('previous_error') or '无'),
                          '来源：' + row['path']))
        path = item.get('display_path') or item.get('path')
        if path and Path(path).is_file():
            try:
                text += '\n\n' + Path(path).read_text(encoding='utf-8')
            except (OSError, UnicodeError):
                text += '\n\n结果文件暂不可读取。'
        associated=item.get('associated_material') or {}
        if associated:
            from associated_pending import describe_material
            facts=describe_material(item)
            text+='\n\n用途：'+facts['purpose']+'\n保存：'+facts['saved']+'\n解析：'+facts['parse']
            text+='\n上传：'+facts['upload']+'\n关联：'+facts['association']+'\n关联进度：'+facts['reason']
            text+='\n\n私有原件：'+associated.get('original_path','')
            text+='\n本地静态预览：'+(associated.get('preview_path') or '待解析')
            text+='\n关联依据：'+associated.get('association_basis','待确认')
            text+='\n处理限制：'+'；'.join(associated.get('limits',[]))
        self.text_window('脚本处理结果', text)

    def selected_source_rows(self):
        state=self._script_row_state()
        chosen=getattr(self,'_source_selected_indices',None)
        if chosen is None:chosen=self._source_selected_indices=set()
        if not getattr(self,'_script_paint_preserves_selection',False):
            page_ids={int(i) for i in self.source_tree.get_children() if i.isdigit()}
            chosen.difference_update(page_ids)
            chosen.update(int(i) for i in self.source_tree.selection() if i.isdigit())
        chosen.intersection_update(state.visible_set)
        return [self.sources[i] for i in sorted(chosen)]
