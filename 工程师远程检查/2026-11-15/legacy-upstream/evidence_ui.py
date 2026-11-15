"""Inspect original media separately from table references and legacy full pages."""
import json
import os
from pathlib import Path
import tkinter as tk
from tkinter import ttk,messagebox


def resource_group(resource):
    kind=resource.get('kind')
    if kind=='image': return '原始图片'
    if kind in {'table_screenshot','table_source_page'}: return '表格核对'
    if kind=='source_page': return '历史整页截图'
    return '其他资源'


def open_review(app,item,read_only=False):
    from evidence import approve_resource
    package=Path(item['package_path'])
    window=tk.Toplevel(app.root);window.title('图表证据检查 · '+item['title'])
    window.geometry('1000x760');window.minsize(880,680)
    frame=ttk.Frame(window,padding=16);frame.pack(fill='both',expand=True)
    frame.columnconfigure(0,weight=1);frame.rowconfigure(2,weight=1);frame.rowconfigure(4,weight=2)
    intro='查看自动处理结果及原文位置，无需人工确认。历史未完成的资源保留其原状态。' if read_only else '原始图片直接从文档提取，仍需确认用途与脱敏。表格核对页、历史整页截图单独列出。'
    ttk.Label(frame,text=intro,wraplength=840).grid(row=0,column=0,sticky='w')
    toolbar=ttk.Frame(frame);toolbar.grid(row=1,column=0,sticky='ew',pady=8)
    group=tk.StringVar(value='原始图片')
    selector=ttk.Combobox(toolbar,textvariable=group,state='readonly',width=18,
                         values=['原始图片','表格核对','历史整页截图','其他资源'])
    selector.pack(side='left')
    counts=tk.StringVar();ttk.Label(toolbar,textvariable=counts).pack(side='left',padx=12)
    listing=ttk.Frame(frame);listing.grid(row=2,column=0,sticky='nsew')
    listing.columnconfigure(0,weight=1);listing.rowconfigure(0,weight=1)
    tree=ttk.Treeview(listing,columns=['类型','状态','资源'],show='headings',height=5,selectmode='browse')
    for c,w in [('类型',155),('状态',110),('资源',520)]: tree.heading(c,text=c);tree.column(c,width=w,minwidth=80)
    tree.grid(row=0,column=0,sticky='nsew')
    scroll=ttk.Scrollbar(listing,command=tree.yview);scroll.grid(row=0,column=1,sticky='ns');tree.configure(yscrollcommand=scroll.set)
    detail=tk.StringVar(value='请选择资源查看实际图片。')
    ttk.Label(frame,textvariable=detail,wraplength=840).grid(row=3,column=0,sticky='w',pady=6)
    preview=tk.Canvas(frame,height=220,background='#e6e8eb',highlightthickness=0)
    preview.grid(row=4,column=0,sticky='nsew')
    ttk.Label(frame,text='OCR 文字候选（下方是识别文字，上方才是图片预览）').grid(row=5,column=0,sticky='w',pady=(8,2))
    text=tk.Text(frame,height=5,wrap='word');text.grid(row=6,column=0,sticky='ew')
    verified=tk.BooleanVar(value=False)
    check=ttk.Checkbutton(frame,text='我已对照原始图片核对文字、数值、单位和图例，允许这些文字进入 ingest.md',variable=verified)
    check.grid(row=7,column=0,sticky='w',pady=6)
    state=tk.StringVar(value='图片检查不代表整篇论文或表格数值关系已核对。')
    ttk.Label(frame,textvariable=state,wraplength=840).grid(row=8,column=0,sticky='w')
    resources={};preview_image=[None]

    def selected():
        ids=tree.selection()
        if not ids: raise ValueError('请选择一个资源。')
        return resources[ids[0]]

    def resource_path(r):
        path=(package/r['local_path']).resolve()
        if not path.is_relative_to(package.resolve()): raise ValueError('资源路径越界')
        return path

    def draw(_=None):
        preview.delete('all')
        if preview_image[0] is None:
            preview.create_text(16,20,anchor='nw',text='没有可显示的图片。',fill='#555555');return
        from PIL import ImageTk
        im=preview_image[0].copy();im.thumbnail((max(50,preview.winfo_width()-16),max(50,preview.winfo_height()-16)))
        preview.photo=ImageTk.PhotoImage(im,master=window)
        preview.create_image(preview.winfo_width()/2,preview.winfo_height()/2,image=preview.photo)

    def load(_=None):
        preview_image[0]=None;verified.set(False);text.configure(state='normal');text.delete('1.0','end')
        confirm_button.state(['disabled']);check.state(['disabled'])
        try:
            r=selected();kind=r.get('kind');is_figure=kind=='image'
            labels={'image':'文档内嵌原始图片（用途待核对）','table_screenshot':'表格区域截图（对应关系待核对）',
                    'table_source_page':'表格候选整页（仅供核对，不是研究图片）','source_page':'旧版整页截图（不是提取的研究图片）'}
            if read_only:
                labels.update(image='文档内嵌图片 · '+('自动处理完成' if r.get('privacy_status')=='approved' else '历史处理未完成'),table_screenshot='表格区域参考截图')
            detail.set(labels.get(kind,'其他资源')+' · '+r['local_path'])
            if is_figure:
                text.insert('1.0',r.get('ocr',{}).get('text',''))
                if not read_only: check.state(['!disabled'])
            else:
                text.insert('1.0','此资源用于原文核对。其 OCR 不作为独立图片说明进入 ingest.md；表格数据来自原生单元格结构。')
                text.configure(state='disabled')
            from PIL import Image,ImageOps
            with Image.open(resource_path(r)) as im: preview_image[0]=ImageOps.exif_transpose(im).convert('RGB')
            if not read_only: confirm_button.state(['!disabled'])
        except ValueError: pass
        except Exception as exc: state.set('预览失败，可用外部程序打开查看：'+str(exc))
        draw()

    def refresh(_=None):
        if item.get('content_format')=='paper_preextract':
            rows=json.loads((package/'assets.json').read_text(encoding='utf8'))['items']
            meta={'resources':[dict(id=r['id'],kind='image',local_path=r['file'],privacy_status='approved',
                ocr={'text':r.get('description','')}) for r in rows if r.get('storage')=='image' and r.get('file')]}
        else:meta=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
        resources.clear();resources.update({r['id']:r for r in meta['resources']})
        tree.delete(*tree.get_children())
        counts.set(' / '.join(f'{name} {sum(resource_group(r)==name for r in resources.values())}' for name in ['原始图片','表格核对','历史整页截图']))
        labels={'image':'原始图片','table_screenshot':'表格区域截图','table_source_page':'表格候选整页','source_page':'历史整页截图'}
        for r in resources.values():
            if resource_group(r)!=group.get(): continue
            tree.insert('','end',iid=r['id'],values=(labels.get(r['kind'],'其他资源'),'已检查' if r['privacy_status']=='approved' else '待脱敏检查',r['local_path']))
        ids=tree.get_children()
        if ids: tree.selection_set(ids[0])
        else: detail.set('这一类没有资源。未解析的对象仍记录在 metadata.json，不能据此认定原文没有图。')
        load()

    def view():
        try: os.startfile(resource_path(selected()))
        except Exception as exc: messagebox.showerror('无法打开',str(exc),parent=window)

    def confirm():
        try:
            r=selected()
            if not messagebox.askokcancel('确认资源已脱敏','请确认选中资源中的姓名、学号、联系方式等已经检查并处理。\n原始图片只有勾选且核对过的文字才会进入 ingest.md。整页截图不作为研究图片入库。',parent=window): return
            approve_resource(package,r['id'],text.get('1.0','end-1c') if verified.get() and r['kind']=='image' else None)
            if app.current:
                latest=app.store.load(app.current['run_id'])
                for entry in latest['items']:
                    if entry['id']==item['id']: entry['reviewed_hash']='';entry['reviewed_package_hash']=''
                app.store.save(latest);app.current=latest;app.render_results()
                app.state.set('图表资源已更新；请重新确认文档后导入。')
            refresh();state.set('已保存资源检查结果；整体研究证据仍待核对。')
        except Exception as exc: messagebox.showerror('未保存',str(exc),parent=window)

    buttons=ttk.Frame(frame);buttons.grid(row=9,column=0,sticky='ew',pady=(8,0))
    ttk.Button(buttons,text='打开原尺寸资源',command=view).pack(side='left')
    ttk.Button(buttons,text='打开文档包',command=lambda:os.startfile(package)).pack(side='left',padx=8)
    confirm_button=ttk.Button(buttons,text='确认此资源已脱敏',command=confirm);confirm_button.pack(side='right')
    tree.bind('<<TreeviewSelect>>',load);selector.bind('<<ComboboxSelected>>',refresh);preview.bind('<Configure>',draw)
    refresh()
    if read_only:
        check.grid_remove();confirm_button.pack_forget()
        text.configure(state='disabled')
        state.set('自动处理记录，仅供查看；无需人工确认。')
    return window
