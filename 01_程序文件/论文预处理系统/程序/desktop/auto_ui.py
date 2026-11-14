"""Agent model selection and probes, separate from RAGFlow destination settings."""
import tkinter as tk
import threading
from tkinter import ttk
from auto_config import load_config,load_key,save_config
from agent_client import AgentClient
from preextract_delivery_worker import DELIVERY_WORKERS

MODEL_REQUEST_DEADLINE_MS = 30000
ROUTING_LABELS = {'lan_first':'内网优先，满载后用官方',
                  'balanced':'两条通道按空闲比例分配',
                  'official_first':'官方优先，满载后用内网'}


def save_form(app):
    cfg,key=read_ai_form(app)
    save_config(app.store.root,cfg,key,app.ai_vars['vision_key'].get(),app.ai_vars['vision_fallback_key'].get())


def build_ai_tab(app):
    tab=ttk.Frame(app.tabs,padding=12);app.tabs.add(tab,text='  Agent 模型  ');app.ai_tab=tab
    cfg=load_config(app.store.root)
    app.ai_vars={k:(tk.BooleanVar(value=v) if type(v) is bool else tk.StringVar(value=str(v)))
                 for k,v in cfg.items() if k not in {'dataset_map','concurrency','resume','vision_model','vision_base_url','vision_model_legacy',
                    'work_pool_enabled','work_pool_auto_start','work_pool_queue','work_pool_parse_limit',
                    'category_dataset_map','dataset_pipeline_map','preprocess_routing',
                    'dataset_native_parser_map','source_receiver_command','preextract_postprocess','preextract_preparation'}}
    vision=cfg.get('vision_model') if isinstance(cfg.get('vision_model'),dict) else {}
    primary=vision.get('primary',{}) if isinstance(vision,dict) else {}
    fallback=vision.get('fallback',{}) if isinstance(vision,dict) else {}
    # Legacy variable names are shared aliases so existing controls/tests and
    # the new two-channel form always edit the same primary values.
    primary_url=tk.StringVar(value=str(primary.get('base_url','')))
    primary_model=tk.StringVar(value=str(primary.get('model','')))
    primary_key=tk.StringVar(value=load_key(app.store.root,'ai_vision_primary') or load_key(app.store.root,'ai_vision'))
    app.ai_vars.update({'vision_base_url':primary_url,'vision_model':primary_model,'vision_key':primary_key,
                        'vision_primary_base_url':primary_url,'vision_primary_model':primary_model,'vision_primary_key':primary_key,
                        'vision_fallback_enabled':tk.BooleanVar(value=bool(fallback.get('enabled',False))),
                        'vision_fallback_base_url':tk.StringVar(value=str(fallback.get('base_url',''))),
                        'vision_fallback_model':tk.StringVar(value=str(fallback.get('model',''))),
                        'vision_fallback_key':tk.StringVar(value=load_key(app.store.root,'ai_vision_fallback')),
                        'vision_retry_max_tries':tk.StringVar(value=str(vision.get('retry',{}).get('max_tries',3))),
                        'vision_retry_backoff_base_seconds':tk.StringVar(value=str(vision.get('retry',{}).get('backoff_base_seconds',2.0))),
                        'vision_circuit_failure_threshold':tk.StringVar(value=str(vision.get('circuit_breaker',{}).get('failure_threshold',3))),
                        'vision_circuit_cooldown_seconds':tk.StringVar(value=str(vision.get('circuit_breaker',{}).get('cooldown_seconds',300))),
                        'vision_circuit_cooldown_multiplier':tk.StringVar(value=str(vision.get('circuit_breaker',{}).get('cooldown_multiplier',2.0))),
                        'vision_circuit_max_cooldown_seconds':tk.StringVar(value=str(vision.get('circuit_breaker',{}).get('max_cooldown_seconds',3600)))})
    concurrency=cfg['concurrency'];resume=cfg['resume']
    routing=cfg['preprocess_routing']
    for key,value in {
        'concurrency_enabled':concurrency['enabled'],
        'concurrency_papers':concurrency['papers'],
        'concurrency_images_per_paper':concurrency['images_per_paper'],
        'concurrency_global_llm':concurrency['global_llm'],
        'concurrency_max_retries':concurrency['max_retries'],
        'concurrency_backoff_base_seconds':concurrency['backoff_base_seconds'],
        'concurrency_min_start_interval_seconds':concurrency['min_start_interval_seconds'],
        'resume_enabled':resume['enabled'],
        'resume_state_file':resume['state_file'],
        'preprocess_routing_enabled':routing['enabled'],
        'preprocess_lan_limit':routing['lan_limit'],
        'preprocess_official_limit':routing['official_limit'],
        'preprocess_routing_strategy':ROUTING_LABELS[routing['strategy']],
    }.items():
        app.ai_vars[key]=tk.BooleanVar(value=value) if type(value) is bool else tk.StringVar(value=str(value))
    app.ai_vars['key']=tk.StringVar(value=load_key(app.store.root,'ai'))
    app.dataset_map=dict(cfg['dataset_map']);app.ai_model_combos={}
    ttk.Label(tab,text='完整提取模型与运行参数',font=('Microsoft YaHei UI',13,'bold')).pack(anchor='w')
    toolbar=ttk.Frame(tab);toolbar.pack(fill='x',pady=7)
    app.ai_status=tk.StringVar(value='填写接口和密钥 → 读取可用模型 → 选择模型 → 连通测试')
    status_label=ttk.Label(tab,textvariable=app.ai_status,wraplength=850)
    status_label.pack(fill='x',pady=(0,6))
    inner=ttk.Notebook(tab);inner.pack(fill='both',expand=True);app.ai_model_tabs=inner
    statuses={False:'文字判断模型尚未测试',True:'图表主/备用通道尚未测试'}
    vision_channel_status={'primary':'主通道尚未测试','fallback':'备用通道尚未测试'}
    pending = None

    def clear_request(request):
        nonlocal pending
        if request is not pending:
            return False
        pending = None
        if request.get('timer'):
            app.root.after_cancel(request['timer'])
        app.ai_cancel_button.state(['disabled'])
        app._model_cancel = None
        return True

    def cancel_request(message='已停止等待，可重新读取或测试当前模型', expected=None):
        request = pending
        if request is None or (expected is not None and request is not expected):
            return
        request['stop'].set()
        clear_request(request)
        app.cancel_model_task()
        statuses[request['vision']] = message
        app.ai_status.set(message)
        app.state.set(message)
    app._model_cancel = None

    def scrolled_page(title):
        page=ttk.Frame(inner);inner.add(page,text=title)
        canvas=tk.Canvas(page,highlightthickness=0,background='#f3f5f7',height=1)
        bar=ttk.Scrollbar(page,command=canvas.yview);bar.pack(side='right',fill='y')
        canvas.configure(yscrollcommand=bar.set);canvas.pack(fill='both',expand=True)
        frame=ttk.Frame(canvas,padding=10);wid=canvas.create_window((0,0),window=frame,anchor='nw')
        frame.bind('<Configure>',lambda e:canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.bind('<Configure>',lambda e:canvas.itemconfigure(wid,width=e.width))
        frame.columnconfigure(1,weight=1)
        return frame

    def add_field(frame,row,key,label,model=False):
        ttk.Label(frame,text=label).grid(row=row,column=0,sticky='w',padx=(0,12),pady=5)
        if model:
            control=ttk.Combobox(frame,textvariable=app.ai_vars[key],state='normal',width=42)
            app.ai_model_combos[key]=control
        else: control=ttk.Entry(frame,textvariable=app.ai_vars[key],show='•' if 'key' in key else '',width=42)
        control.grid(row=row,column=1,sticky='ew',pady=5)

    for vision,title in [(False,'全文与图像提取模型'),(True,'旧格式图表通道')]:
        page=scrolled_page(title)
        add_field(page,0,'vision_base_url' if vision else 'base_url','接口地址'+('（空白复用文字接口）' if vision else '（含 /v1 等版本路径）'))
        add_field(page,1,'vision_key' if vision else 'key','API Key'+('（空白复用文字密钥）' if vision else '（Windows 加密保存）'))
        add_field(page,2,'vision_model' if vision else 'text_model','选择模型 / 手动输入',model=True)
        if vision:
            ttk.Label(page,text='旧格式图表主通道',font=('Microsoft YaHei UI',10,'bold')).grid(row=3,column=0,columnspan=2,sticky='w',pady=(10,2))
            ttk.Checkbutton(page,text='启用旧格式图表备用通道',variable=app.ai_vars['vision_fallback_enabled']).grid(row=4,column=0,columnspan=2,sticky='w',pady=4)
            for row,key,label,model in [(5,'vision_fallback_base_url','备用 Base URL',False),(6,'vision_fallback_key','备用 API Key',False),(7,'vision_fallback_model','备用模型名',True)]: add_field(page,row,key,label,model=model)
            for row,key,label in [(8,'vision_retry_max_tries','视觉单通道最多尝试（含首次）'),(9,'vision_retry_backoff_base_seconds','视觉退避起始秒'),(10,'vision_circuit_failure_threshold','熔断阈值'),(11,'vision_circuit_cooldown_seconds','熔断冷却秒'),(12,'vision_circuit_cooldown_multiplier','冷却倍增'),(13,'vision_circuit_max_cooldown_seconds','最大冷却秒')]: add_field(page,row,key,label)
            explanation='主通道与备用通道均为完整配置；400 多模态能力错误立即切换，认证/配置错误停止。探测请求同样计入 Token 账本。'
        else: explanation='新批次由此模型读取完整研究文本和实际图像，按现有 MD 提取完整学习内容、专业和脱敏计划。请选择支持图像的通道；旧格式图表通道仅用于历史结果恢复。'
        ttk.Label(page,text=explanation,wraplength=780).grid(row=14 if vision else 3,column=0,columnspan=2,sticky='w',pady=6)
    advanced=scrolled_page('运行参数')
    for row,key,label in [(0,'timeout','请求超时（秒，5–7200）'),(1,'attempts','旧格式最多尝试次数（1–5）'),(2,'max_pages','旧格式视觉核对页数上限'),(3,'max_tokens','完整提取输出长度上限')]: add_field(advanced,row,key,label)
    ttk.Checkbutton(advanced,text='启用并发预处理（先用小并发探测，遇到限流可关闭）',
                    variable=app.ai_vars['concurrency_enabled']).grid(row=4,column=0,columnspan=2,sticky='w',pady=5)
    for row,key,label in [(5,'concurrency_papers','同时提取的论文数（1–64）'),
                          (6,'concurrency_images_per_paper','旧格式单篇图片并发（1–16）'),
                          (7,'concurrency_global_llm','全局 LLM 并发上限（1–128）'),
                          (8,'concurrency_max_retries','旧格式并发最多尝试（含首次，1–8）'),
                          (9,'concurrency_backoff_base_seconds','退避起始秒数（0–60）')]: add_field(advanced,row,key,label)
    ttk.Checkbutton(advanced,text='启用图片级断点状态（每张图片完成立即落盘）',
                    variable=app.ai_vars['resume_enabled']).grid(row=10,column=0,columnspan=2,sticky='w',pady=5)
    add_field(advanced,11,'resume_state_file','断点状态文件名')
    add_field(advanced,12,'max_tokens_retry_limit','截断重试输出上限（0=按接口）')
    ttk.Label(advanced,text='新批次整篇可容纳时只发一次请求，超限时沿用程序自动分段。失败、截断或未决请求保留原记录，不自动重发；已有完整回答优先复用。完整回答保存后释放模型名额，按启动间隔递补，本地校验、脱敏和封包另行执行。上面的尝试、图片断点及截断重试设置用于旧格式恢复。',wraplength=780).grid(row=13,column=0,columnspan=2,sticky='w',pady=8)
    ttk.Checkbutton(advanced,text='启用内网和官方分流（文字和图片均可能使用官方额度）',
                    variable=app.ai_vars['preprocess_routing_enabled']).grid(row=14,column=0,columnspan=2,sticky='w',pady=5)
    add_field(advanced,15,'preprocess_lan_limit','内网同时处理的模型调用上限')
    add_field(advanced,16,'preprocess_official_limit','官方同时处理的模型调用上限')
    ttk.Label(advanced,text='通道分配方式').grid(row=17,column=0,sticky='w',pady=5)
    ttk.Combobox(advanced,textvariable=app.ai_vars['preprocess_routing_strategy'],
                 values=list(ROUTING_LABELS.values()),state='readonly').grid(row=17,column=1,sticky='ew',pady=5)
    ttk.Label(advanced,text='所有论文共享这两组上限，并受全局模型请求上限约束。官方通道使用“图表主/备用通道”中配置的备用地址、模型和密钥；关闭备用通道后不会自动分流。',wraplength=780).grid(row=18,column=0,columnspan=2,sticky='w',pady=8)
    add_field(advanced,19,'concurrency_min_start_interval_seconds','完整提取请求最小启动间隔（秒，0–3600）')
    ttk.Label(advanced,text='同一接口和密钥的整篇、分段及全文统合请求共用启动间隔；已有完整回答直接复用。16 秒对应每分钟最多约 4 次启动，已启动的论文仍同时处理。等待可停止，失败不自动重发。',wraplength=780).grid(row=20,column=0,columnspan=2,sticky='w',pady=8)

    def refresh_concurrency_status(*_):
        enabled=bool(app.ai_vars['concurrency_enabled'].get())
        papers=str(app.ai_vars['concurrency_papers'].get()).strip() or '—'
        global_llm=str(app.ai_vars['concurrency_global_llm'].get()).strip() or '—'
        interval=str(app.ai_vars['concurrency_min_start_interval_seconds'].get()).strip() or '0'
        if enabled:
            preparation=(cfg.get('preextract_preparation') or {}).get('workers',4)
            app.concurrency_status.set(f'前处理 {preparation} 路 · LibreOffice 4 / WPS 1 · 模型：论文 {papers} / 请求 {global_llm} · 间隔 ≥{interval} 秒 · 后处理 2 路 · 入库 {DELIVERY_WORKERS} 路')
        else:
            app.concurrency_status.set(f'模型串行处理 · 启动间隔 ≥{interval} 秒 · 预设论文 {papers} / 请求 {global_llm} · 入库 {DELIVERY_WORKERS} 路')

    app.concurrency_status=tk.StringVar()
    concurrency_row=ttk.Frame(app.processing_mode_row)
    concurrency_row.grid(row=2,column=0,columnspan=5,sticky='ew')
    app.concurrency_toggle=ttk.Checkbutton(concurrency_row,text='启用并发处理',
                                            variable=app.ai_vars['concurrency_enabled'],
                                            command=refresh_concurrency_status)
    app.concurrency_toggle.pack(side='left')
    app.concurrency_status_label=ttk.Label(concurrency_row,textvariable=app.concurrency_status,
                                           foreground='#617082')
    app.concurrency_status_label.pack(side='left',padx=(12,8),fill='x',expand=True)
    def open_concurrency_settings():
        app.tabs.select(app.ai_tab)
        app.ai_model_tabs.select(2)
        app.ai_status.set('已打开运行参数页，可调整并发数量、重试和退避参数')
    app.concurrency_settings_button=ttk.Button(concurrency_row,text='并发参数…',
                                                command=open_concurrency_settings)
    app.concurrency_settings_button.pack(side='right')
    app.processing_mode_controls=[app.concurrency_toggle,app.concurrency_settings_button]
    for key in ['concurrency_enabled','concurrency_papers','concurrency_images_per_paper','concurrency_global_llm','concurrency_min_start_interval_seconds']:
        app.ai_vars[key].trace_add('write',refresh_concurrency_status)
    refresh_concurrency_status()
    from billing_ui import build_settings
    build_settings(app,scrolled_page('Token 计费'))

    def active(): return inner.index(inner.select())==1
    def changed(*args):
        cancel_request('配置已更改，已取消旧请求等待；可重新读取或测试')
        for role in statuses: statuses[role]='配置已更改，请测试当前模型'
        app.ai_status.set(statuses[active()])
    for key in ['base_url','key','text_model','vision_base_url','vision_key','vision_model','vision_fallback_enabled','vision_fallback_base_url','vision_fallback_key','vision_fallback_model']:
        app.ai_vars[key].trace_add('write',changed)

    def operate(test=False):
        nonlocal pending
        if app.busy: return
        try:
            vision=active();config,key=read_ai_form(app);vk=app.ai_vars['vision_key'].get()
            request = {'stop': threading.Event(), 'vision': vision}
            # Interactive probes must not inherit multi-minute document retries.
            probe_config = dict(config, timeout=min(config['timeout'], 20), attempts=1)
            client=AgentClient(probe_config,key,stop=request['stop'],vision_key=vk,
                               fallback_key=app.ai_vars['vision_fallback_key'].get(),state_root=app.store.root)
            from billing import Ledger
            client.ledger=Ledger(app.store.root);client.billing_context={'scope':'model_test'}
            captured=(config,key,vk,app.ai_vars['vision_fallback_key'].get())
            pending = request
            app._model_cancel = lambda: cancel_request('程序关闭，已停止模型读取/测试', expected=request)
            message = ('正在测试' if test else '正在读取')+('图表识别模型…' if vision else '文字判断模型…')
            statuses[vision] = message
            app.ai_status.set(message)
            app.state.set(message)
            app.ai_cancel_button.state(['!disabled'])
            request['timer'] = app.root.after(MODEL_REQUEST_DEADLINE_MS, lambda: cancel_request(
                '请求等待超时，已停止等待；可手动填写模型名或重试', expected=request))
            def work():
                channels=[('primary','主通道')]
                if vision:
                    fallback=config.get('vision_model',{}).get('fallback',{})
                    # Read only the captured configuration in the worker;
                    # disabled channels must not be probed, even with saved fields.
                    if fallback.get('enabled'):
                        channels.append(('fallback','备用通道'))
                results=[]
                for channel,label in channels:
                    try:
                        if test:
                            data=client.probe(vision) if channel=='primary' else client.probe(vision,channel=channel)
                        else:
                            data=client.models(vision) if channel=='primary' else client.models(vision,channel=channel)
                        results.append((channel,label,data,''))
                    except Exception as exc:
                        results.append((channel,label,None,str(exc)))
                return results,None
            def done(result):
                if not clear_request(request):
                    return
                from billing_ui import refresh_total
                refresh_total(app)
                current,curr_key=read_ai_form(app)
                if (current,curr_key,app.ai_vars['vision_key'].get(),app.ai_vars['vision_fallback_key'].get())!=captured:
                    app.ai_status.set('配置在请求期间发生变化，请重新读取或测试');return
                data,error=result
                if error:
                    statuses[vision]=error
                elif vision:
                    messages=[]
                    for channel,label,value,channel_error in data:
                        if channel_error:
                            vision_channel_status[channel]=label+'：'+channel_error
                        elif test:
                            vision_channel_status[channel]=label+'：'+value
                        else:
                            field='vision_model' if channel=='primary' else 'vision_fallback_model'
                            app.ai_model_combos[field].configure(values=value)
                            vision_channel_status[channel]=f'{label}读取到 {len(value)} 个模型，请选择模型后再测试'
                        messages.append(vision_channel_status[channel])
                    statuses[vision]='；'.join(messages)
                elif data and data[0][3]:
                    statuses[vision]=data[0][3]
                elif test:
                    statuses[vision]=data[0][2] if data else '文字判断模型测试无结果'
                else:
                    values=data[0][2] if data else []
                    app.ai_model_combos['text_model'].configure(values=values)
                    statuses[vision]=f'读取到 {len(values)} 个模型，请在下方选择，再点击模型连通测试'
                app.ai_status.set(statuses[active()])
                app.state.set(statuses[vision])
            app.async_task(work,done,kind='model_settings')
        except Exception as exc:
            cancel_request()
            app.ai_status.set(str(exc))

    def save():
        try: save_form(app);app.ai_status.set('配置已保存；下次处理使用当前选中的模型')
        except Exception as exc: app.ai_status.set(str(exc))
    app.ai_read_button=ttk.Button(toolbar,text='读取可用模型',command=lambda:operate(False));app.ai_read_button.pack(side='left')
    app.ai_test_button=ttk.Button(toolbar,text='模型连通测试',style='Accent.TButton',command=lambda:operate(True));app.ai_test_button.pack(side='left',padx=8)
    app.ai_cancel_button=ttk.Button(toolbar,text='停止读取/测试',command=cancel_request,state='disabled')
    app.ai_cancel_button.pack(side='left')
    ttk.Button(toolbar,text='保存配置',command=save).pack(side='right')
    def select(_=None):
        advanced_selected=inner.index(inner.select())>=2
        for button in [app.ai_read_button,app.ai_test_button]: button.state(['disabled'] if advanced_selected else ['!disabled'])
        app.ai_status.set('参数修改后点击保存配置' if advanced_selected else statuses[active()])
    inner.bind('<<NotebookTabChanged>>',select)
    tab.bind('<Configure>',lambda e:status_label.configure(wraplength=max(200,e.width-25)))
    build_rag_options(app)


def build_rag_options(app):
    frame=ttk.LabelFrame(app.rag_content,text='处理完成后进入 RAGFlow',padding=10)
    frame.pack(fill='x',pady=12);app.rag_auto_frame=frame
    for key,label in [('auto_upload','封包通过后自动排队入库'),('auto_parse','旧格式导入后启动知识库解析（新批次直接写入现成切片）')]:
        ttk.Checkbutton(frame,text=label,variable=app.ai_vars[key]).pack(anchor='w',pady=3)
    from engine import load_legacy_taxonomy
    app.route_major=tk.StringVar(value='会计学')
    choices=[m for majors in load_legacy_taxonomy().values() for m in majors]
    row=ttk.Frame(frame);row.pack(fill='x',pady=8)
    ttk.Label(row,text='专业').pack(side='left')
    major_combo=ttk.Combobox(row,textvariable=app.route_major,values=choices,state='readonly',width=20)
    major_combo.pack(side='left',padx=8)
    def apply_route(*_):
        from auto_config import normalize_dataset_map
        entry=normalize_dataset_map(getattr(app,'dataset_map',{})).get(app.route_major.get(),{})
        labels=list(app.dataset_combo.cget('values'))
        for combo,var,key in [(app.dataset_combo,app.dataset,'paper'),
                              (app.image_dataset_combo,app.image_dataset,'image')]:
            target=entry.get(key,'')
            index=next((i for i,d in enumerate(app.datasets) if d.get('id')==target),None)
            var.set(labels[index] if index is not None and index<len(labels) else '')
    major_combo.bind('<<ComboboxSelected>>',apply_route)
    app.route_major.trace_add('write',apply_route)
    def bind():
        paper_index=app.dataset_combo.current()
        image_index=app.image_dataset_combo.current()
        if paper_index<0 or not app.datasets: return app.error('先连接并选择 md 知识库')
        app.dataset_map[app.route_major.get()]={
            'paper':app.datasets[paper_index]['id'],
            'image':app.datasets[image_index]['id'] if 0<=image_index<len(app.datasets) else '',
        }
        try: save_form(app)
        except Exception as exc: return app.error(str(exc))
        image_name=app.datasets[image_index]['name'] if 0<=image_index<len(app.datasets) else '（未选择，图片跳过）'
        app.connection.set('已绑定 '+app.route_major.get()+' → md：'+app.datasets[paper_index]['name']+'；图片：'+image_name)
    ttk.Button(row,text='绑定当前专业的 md/图片知识库',command=bind).pack(side='left')
    ttk.Label(frame,text='上方两个下拉框分别是 md 知识库和图片知识库；图片库留空时只导入论文侧文件并给出警告。未绑定时可按专业中文名精确匹配 md 库。',wraplength=780).pack(anchor='w')
    def save():
        try:
            # Selecting a destination and pressing “save” must persist that
            # selection.  Previously only the URL/key were saved; the visible
            # dropdown could point at a new library while dataset_map retained
            # a deleted ID, causing every automatic handoff to wait forever.
            paper_index=app.dataset_combo.current()
            image_index=app.image_dataset_combo.current()
            if 0<=paper_index<len(app.datasets):
                app.dataset_map[app.route_major.get()]={
                    'paper':app.datasets[paper_index]['id'],
                    'image':app.datasets[image_index]['id'] if 0<=image_index<len(app.datasets) else '',
                }
                message=('RAGFlow 配置已保存；已绑定 '+app.route_major.get()+
                         ' → md：'+app.datasets[paper_index]['name'])
            else:
                message='RAGFlow 连接设置已保存；当前专业尚未绑定 md 知识库'
            app.settings();app.connection.set(message)
        except Exception as exc: app.error(str(exc))
    ttk.Button(frame,text='保存 RAGFlow 设置',command=save).pack(anchor='w',pady=(8,0))


def read_ai_form(app):
    ignored={'key','vision_key','vision_primary_base_url','vision_primary_model','vision_primary_key',
             'preprocess_routing_enabled','preprocess_lan_limit','preprocess_official_limit','preprocess_routing_strategy',
             'vision_fallback_enabled','vision_fallback_base_url','vision_fallback_model','vision_fallback_key',
             'vision_retry_max_tries','vision_retry_backoff_base_seconds','vision_circuit_failure_threshold',
             'vision_circuit_cooldown_seconds','vision_circuit_cooldown_multiplier','vision_circuit_max_cooldown_seconds'}
    pool_keys={'work_pool_enabled','work_pool_auto_start','work_pool_queue','work_pool_parse_limit',
               'category_dataset_map','dataset_pipeline_map','preprocess_routing',
               'dataset_native_parser_map','source_receiver_command','preextract_limits','preextract_postprocess','preextract_preparation'}
    config={k:v.get() for k,v in app.ai_vars.items() if k not in ignored and k not in {'vision_model_legacy'} and k not in pool_keys}
    if getattr(app,'store',None):
        saved=load_config(app.store.root)
        config.update((key,saved[key]) for key in pool_keys)
    if 'preprocess_routing_enabled' in app.ai_vars:
        from auto_config import normalize_preprocess_routing
        config['preprocess_routing']=normalize_preprocess_routing({
            'enabled':app.ai_vars['preprocess_routing_enabled'].get(),
            'lan_limit':app.ai_vars['preprocess_lan_limit'].get(),
            'official_limit':app.ai_vars['preprocess_official_limit'].get(),
            'strategy':{label:key for key,label in ROUTING_LABELS.items()}.get(app.ai_vars['preprocess_routing_strategy'].get(),'')
                       if 'preprocess_routing_strategy' in app.ai_vars else config.get('preprocess_routing',{}).get('strategy','lan_first')})
    for key,lo,hi in [('timeout',5,7200),('attempts',1,5),('max_pages',1,500),('max_tokens',512,128000)]:
        try: config[key]=int(config[key])
        except ValueError: raise ValueError('请输入有效整数：'+key) from None
        if not lo<=config[key]<=hi: raise ValueError(f'{key} 必须在 {lo}–{hi} 范围内')
    try:config['max_tokens_retry_limit']=int(config.get('max_tokens_retry_limit',0))
    except ValueError:raise ValueError('截断重试输出上限必须是整数') from None
    limit=config['max_tokens_retry_limit']
    if limit!=0 and not config['max_tokens']<=limit<=65536:
        raise ValueError('截断重试输出上限须为 0，或介于模型输出长度上限和 65536 之间')
    for key,lo,hi in [('concurrency_papers',1,64),('concurrency_images_per_paper',1,16),
                      ('concurrency_global_llm',1,128),('concurrency_max_retries',1,8)]:
        try: config[key]=int(config[key])
        except ValueError: raise ValueError('请输入有效整数：'+key) from None
        if not lo<=config[key]<=hi: raise ValueError(f'{key} 必须在 {lo}–{hi} 范围内')
    try: config['concurrency_backoff_base_seconds']=float(config['concurrency_backoff_base_seconds'])
    except ValueError: raise ValueError('concurrency_backoff_base_seconds 必须是数字') from None
    if not 0<=config['concurrency_backoff_base_seconds']<=60: raise ValueError('concurrency_backoff_base_seconds 必须在 0–60 秒')
    from auto_config import normalize_concurrency
    interval=normalize_concurrency({'min_start_interval_seconds':config.pop('concurrency_min_start_interval_seconds')})['min_start_interval_seconds']
    config['concurrency']={
        'enabled':bool(config.pop('concurrency_enabled')),
        'papers':config.pop('concurrency_papers'),
        'images_per_paper':config.pop('concurrency_images_per_paper'),
        'global_llm':config.pop('concurrency_global_llm'),
        'max_retries':config.pop('concurrency_max_retries'),
        'backoff_base_seconds':config.pop('concurrency_backoff_base_seconds'),
        'min_start_interval_seconds':interval,
    }
    state_file=str(config.pop('resume_state_file')).strip()
    if not state_file: raise ValueError('resume_state_file 不能为空')
    config['resume']={'enabled':bool(config.pop('resume_enabled')),'state_file':state_file}
    try:
        vision_tries=int(app.ai_vars['vision_retry_max_tries'].get())
        if not 1<=vision_tries<=8: raise ValueError()
        vision_backoff=float(app.ai_vars['vision_retry_backoff_base_seconds'].get())
        if not 0<=vision_backoff<=60: raise ValueError()
        threshold=int(app.ai_vars['vision_circuit_failure_threshold'].get())
        cooldown=float(app.ai_vars['vision_circuit_cooldown_seconds'].get())
        multiplier=float(app.ai_vars['vision_circuit_cooldown_multiplier'].get())
        maximum=float(app.ai_vars['vision_circuit_max_cooldown_seconds'].get())
        if threshold<1 or cooldown<0 or multiplier<1 or maximum<cooldown: raise ValueError()
    except (ValueError,TypeError): raise ValueError('视觉重试与熔断参数无效，请检查数值范围') from None
    config['vision_model']={'enabled':True,
        'primary':{'base_url':app.ai_vars['vision_base_url'].get().strip(),'api_key':'','model':app.ai_vars['vision_model'].get().strip()},
        'fallback':{'enabled':bool(app.ai_vars['vision_fallback_enabled'].get()),'base_url':app.ai_vars['vision_fallback_base_url'].get().strip(),
                    'api_key':'','model':app.ai_vars['vision_fallback_model'].get().strip()},
        'retry':{'max_tries':vision_tries,'backoff_base_seconds':vision_backoff},
        'circuit_breaker':{'failure_threshold':threshold,'cooldown_seconds':cooldown,
                           'cooldown_multiplier':multiplier,'max_cooldown_seconds':maximum}}
    config['dataset_map']=dict(app.dataset_map)
    from billing import exchange_rate
    config['usd_cny']=str(exchange_rate(config['usd_cny']))
    return config,app.ai_vars['key'].get()
