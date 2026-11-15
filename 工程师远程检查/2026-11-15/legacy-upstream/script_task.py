"""Local script task presentation; retries retain the original batch results."""
from collections import Counter
import copy
from pathlib import Path
import tkinter as tk
from tkinter import ttk

from auto_view import outcome, result_items
from archive_identity import source_key, record_key, member_fields


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
    current = manifest
    while current:
        run_id = current.get('run_id')
        if run_id in seen:
            raise ValueError('批次重试记录存在循环，无法安全载入。')
        seen.add(run_id)
        chain.append(current)
        previous = current.get('retry_of')
        current = store.load(previous) if previous else None
        if previous and current is None:
            raise ValueError('原批次记录缺失，无法安全载入全部结果。')
    chain.reverse()
    # These three collections are replaced by the merged retry projection.
    # Copy only the final surviving records, rather than every superseded item
    # and another complete copy of the active manifest first.
    merged = copy.deepcopy({key:value for key,value in manifest.items()
                            if key not in {'inputs','items','events'}})
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
                                     'reason':'本批停止或中断，脚本未完成，可继续处理'})
        for entry in run_inputs:
            if entry.get('path'):
                inputs[record_key(entry)] = entry
        for item in incoming:
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


def task_rows(manifest):
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

    def render_script_rows(self, rows, select_all=False):
        chosen = {source_key(self.sources[int(i)]['path']) for i in self.source_tree.selection()
                  if i.isdigit() and int(i) < len(self.sources)}
        old_keys=[source_key(row['path']) for row in self.sources]
        new_keys=[source_key(row['path']) for row in rows]
        same_order=(old_keys==new_keys and getattr(self,'_script_tree_keys',None)==new_keys)
        if same_order:
            old_rows=self.sources
            self.sources=rows
            for index,(old,new) in enumerate(zip(old_rows,rows)):
                if row_values(old)!=row_values(new):
                    self.source_tree.item(str(index),values=row_values(new))
                    self.sync_script_filter_row(index)
            if select_all:self.source_tree.selection_set(self.source_tree.get_children())
            self.refresh_script_summary()
            self.update_controls()
            return
        # get_children excludes detached (filtered) rows. Delete those too.
        existing=[str(i) for i in range(len(self.sources)) if self.source_tree.exists(str(i))]
        self.source_tree.delete(*existing)
        self.sources = rows
        for index, row in enumerate(rows):
            self.source_tree.insert('', 'end', iid=str(index), values=row_values(row))
        self._script_tree_keys=new_keys
        self.source_tree.selection_set([str(i) for i, row in enumerate(rows)
                                       if select_all or source_key(row['path']) in chosen])
        self.apply_script_filter()

    def script_row_matches(self,row):
        selected=self.script_filter.get()
        return selected=='全部结果' or row.get('script_status','待处理')==selected

    def apply_script_filter(self):
        visible=[str(i) for i,row in enumerate(self.sources) if self.script_row_matches(row)]
        visible_set=set(visible)
        hidden=[str(i) for i in range(len(self.sources)) if str(i) not in visible_set]
        self.source_tree.selection_remove(*hidden)
        self.source_tree.detach(*hidden)
        for iid in visible:
            self.source_tree.move(iid,'','end')
        self.source_tree.yview_moveto(0)
        self.refresh_script_summary()
        self.update_controls()

    def sync_script_filter_row(self,index):
        if self.script_filter.get()=='全部结果':
            return
        iid=str(index)
        if self.script_row_matches(self.sources[index]):
            position=sum(self.script_row_matches(row) for row in self.sources[:index])
            self.source_tree.move(iid,'',position)
        else:
            self.source_tree.selection_remove(iid)
            self.source_tree.detach(iid)
        self.update_controls()

    def refresh_script_summary(self):
        counts = Counter(row.get('script_status', '待处理') for row in self.sources)
        visible=len(self.source_tree.get_children())
        prefix=f'显示 {visible} / ' if self.script_filter.get()!='全部结果' else ''
        self.script_summary.set(prefix+f"本批 {len(self.sources)} 份 · 通过 {counts['通过']} · 不通过 {counts['不通过']} · 重复跳过 {counts['重复跳过']}"
                                + (f" · 已保存 {counts['已保存']}" if counts['已保存'] else '')
                                + (f" · 待解析 {counts['待解析']}" if counts['待解析'] else '')
                                + (f" · 已删除 {counts['已删除']}" if counts['已删除'] else '')
                                + (f" · 待处理 {counts['待处理']}" if counts['待处理'] else ''))

    def show_script_results(self, manifest,*,refresh=True):
        self._script_manifest = self.result_snapshot(manifest,refresh=refresh)
        self.render_script_rows(task_rows(self._script_manifest))

    def show_script_progress(self, payload):
        progress = dict(payload.get('progress') or {})
        if str(progress.get('stage', '')).startswith('RAGFlow'):
            return
        run_id = payload.get('run_id')
        completed, total = payload.get('processed_sources', 0), payload.get('source_count', 0)
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
            self.ai_retry_hint.set(f'本次 AI 续跑完成 {completed}/{total} · '+str(progress.get('stage') or '处理中'))
            from billing_ui import update_progress
            update_progress(self,payload)
            self.state.set(self.ai_retry_hint.get())
            return
        if self.current and self.current.get('run_id')==run_id:
            # The local preparation can finish while AI is still running.
            ready={i.get('input_source_path') or i.get('source_path') for i in self.current.get('active_items',[])
                   if i.get('script_result',{}).get('status')=='completed'}
            total=max(total,self.current.get('source_count',0))
            completed=min(total,max(completed,self.current.get('processed_sources',0)+len(ready)))
        if payload.get('ai_item') or str(progress.get('stage','')).startswith('AI ') or progress.get('stage')=='续跑复用':
            self.progress_bar.configure(value=completed,maximum=max(1,total))
            self.progress_count.set(f'本次脚本完成 {completed}/{total}')
            return
        progress.update(completed=completed, total=total)
        source = progress.get('source_path')
        matches = [row for row in self.sources if source_key(row['path']) == source_key(source)] if source else [
            row for row in self.sources if row.get('name') == progress.get('file')]
        if len(matches) == 1 and progress.get('stage') not in {'通过', '不通过', '跳过', '任务结束', '脚本处理结束','待解析','关联资料保存','已删除过程表单'}:
            row = matches[0]
            if row.get('script_status') != '通过':
                if row.get('script_status') == '不通过':
                    row['previous_error'] = row.get('script_reason', '')
                row.update(script_status='处理中', script_reason=progress.get('stage', '处理中'))
                # Per-page/model events must not rebuild hundreds of table rows.
                index=self.sources.index(row)
                self.source_tree.item(str(index), values=row_values(row))
                self.sync_script_filter_row(index)
                self.refresh_script_summary()
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
        if self.busy:
            return
        folder = self.folder.get().strip()
        if not folder:
            return self.state.set('请先选择输入文件夹，或从批次记录载入结果。')
        base = Path(folder).resolve()
        matches = [m for m in self.store.history() if m.get('pipeline') == 'automatic'
                   and m.get('inputs') and all(Path(r['path']).resolve().is_relative_to(base)
                                             for r in m['inputs'] if r.get('path'))]
        if not matches:
            return self.state.set('当前文件夹没有历史脚本批次。')
        self.current = max(matches, key=lambda m: (m.get('started_at', ''), m.get('run_id', '')))
        self.show_script_results(self.current)
        self.load_progress(self.current)
        self.state.set('该批次仍在处理，请等待完成。' if self.current.get('status')=='running'
                       else '已载入本批脚本结果，可查看失败原因后继续跑未通过。')

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
            text+='\n\n私有原件：'+associated.get('original_path','')
            text+='\n本地静态预览：'+(associated.get('preview_path') or '待解析')
            text+='\n关联依据：'+associated.get('association_basis','待确认')
            text+='\n处理限制：'+'；'.join(associated.get('limits',[]))
        self.text_window('脚本处理结果', text)
