"""Chinese cost display; updates run on the Tk event loop."""
import tkinter as tk
from tkinter import ttk
from decimal import Decimal
from billing import Ledger,OFFICIAL,CHECKED_ON,price_for,summarize,summary_text


def build_settings(app,page):
    ttk.Label(page,text='按模型自动匹配已核对的官方标准价，再折算人民币。',wraplength=780).grid(row=0,column=0,columnspan=2,sticky='w',pady=5)
    ttk.Label(page,text='1 美元折合人民币（估算汇率）').grid(row=1,column=0,sticky='w',pady=5)
    ttk.Entry(page,textvariable=app.ai_vars['usd_cny'],width=20).grid(row=1,column=1,sticky='w')
    ttk.Label(page,text='默认 7 元是估算值，非实时汇率；修改后保存，对新请求生效。',wraplength=780).grid(row=2,column=0,columnspan=2,sticky='w')
    app.billing_prices=tk.StringVar()
    ttk.Label(page,textvariable=app.billing_prices,wraplength=780).grid(row=3,column=0,columnspan=2,sticky='w',pady=8)
    def refresh(*_):
        lines=[]
        model_fields=[('文字模型','text_model'),('图表主通道','vision_model')]
        if 'vision_fallback_model' in app.ai_vars:
            model_fields.append(('图表备用通道','vision_fallback_model'))
        for label,key in model_fields:
            model=app.ai_vars[key].get().strip()
            try:
                p=price_for(model,{'usd_cny':app.ai_vars['usd_cny'].get()})
                if p:
                    rate=Decimal(p['usd_cny'])
                    lines.append(f"{label} {model}\n每百万 Token：输入 ¥{Decimal(p['input'])*rate:g} / 缓存输入 ¥{Decimal(p['cached_input'])*rate:g} / 输出 ¥{Decimal(p['output'])*rate:g}")
                else: lines.append(f'{label} {model or "尚未选择"}：无价目表（未收录官方价）；保留 Token 用量，不套用其他模型价格')
            except ValueError as exc: lines.append(str(exc))
        app.billing_prices.set('\n\n'.join(lines))
    for key in ['text_model','vision_model','usd_cny']: app.ai_vars[key].trace_add('write',refresh)
    refresh()
    ttk.Label(page,text=f'价格核对日期：{CHECKED_ON}。支持 '+ '、'.join(OFFICIAL)+ ' 及 gpt-5.6 别名。\n大于 272K 的输入按长上下文倍率估算。官方价格可能调整，可查看明细中的来源。',wraplength=780).grid(row=4,column=0,columnspan=2,sticky='w',pady=6)
    ttk.Label(page,text='统计 Agent 请求（含图片输入、失败重试、连通测试）。缓存用量按接口返回值计算；未返回缓存细分时按普通输入估算。\n不含 RAGFlow 的 Embedding / 后续模型费用。代理服务实际扣费可能不同；旧请求无法追溯。',wraplength=780).grid(row=5,column=0,columnspan=2,sticky='w',pady=5)
    app.billing_total=tk.StringVar()
    ttk.Label(page,textvariable=app.billing_total,wraplength=780).grid(row=6,column=0,columnspan=2,sticky='w',pady=8)
    ttk.Button(page,text='查看费用明细',command=lambda:show_details(app)).grid(row=7,column=0,sticky='w')
    refresh_total(app)


def refresh_total(app):
    app.billing_total.set('累计（自启用计费起，含测试）：\n'+summary_text(Ledger(app.store.root).summary()))


def build_progress(app):
    app.billing_run_id=None
    app.billing_line=tk.StringVar(value='Token 及人民币费用将在模型返回后更新')
    row=ttk.Frame(app.progress_panel);row.pack(fill='x',pady=(0,5))
    app.billing_button=ttk.Button(row,text='费用明细',command=lambda:show_details(app))
    app.billing_button.pack(side='right',padx=(8,0))
    app.billing_label=ttk.Label(row,textvariable=app.billing_line,font=('Microsoft YaHei UI',9),wraplength=850)
    app.billing_label.pack(side='left',fill='x',expand=True)
    row.bind('<Configure>',lambda e:app.billing_label.configure(wraplength=max(160,e.width-125)))


def update_progress(app,value):
    if 'billing' not in value: return
    app.billing_run_id=value.get('run_id')
    summary=value['billing']
    message='本批 '+summary_text(summary)
    item=value.get('billing_item')
    if item is not None:
        cost=f"¥{Decimal(item['cost_cny']):.6f}"
        if item['unknown_usage'] or item['unpriced']:
            cost=cost+'（部分未知）' if item['priced_requests'] else '费用未知'
        message+=' | 本篇 '+cost
    app.billing_line.set(message)
    refresh_total(app)


def show_details(app):
    ledger=Ledger(app.store.root);rows=ledger.records();rid=app.billing_run_id
    lines=['Agent 费用估算（人民币）',
           '自启用此功能起统计，包含失败重试和连通测试；不含 RAGFlow 后续费用。',
           '官方标准价 × 每次请求保存的估算汇率，非代理服务实际账单。',
           '未返回缓存细分时按普通输入估算；请求中断/未返回 usage 的部分费用未知。','',
           '累计：'+summary_text(summarize(rows)),
           '连通测试：'+summary_text(summarize(r for r in rows if r['scope']=='model_test'))]
    if rid:
        manifest=app.store.load(rid)
        batch_ids={rid, *manifest.get('billing_run_ids', []),
                   *[child['run_id'] for child in manifest.get('child_runs', []) if child.get('run_id')]}
        selected=[r for r in rows if r['run_id'] in batch_ids]
        lines+=['','批次 '+rid,summary_text(summarize(selected)) if 'billing' in manifest or selected else summary_text(None),'','逐篇费用']
        items={i['id']:i for i in manifest.get('items',[])}
        ids=list(dict.fromkeys(list(items)+[r['item_id'] for r in selected if r['item_id']]))
        for item_id in ids:
            item=items.get(item_id,{})
            own=[r for r in selected if r['item_id']==item_id]
            summary=summarize(own) if own or 'billing' in item else None
            lines += [item.get('title',item_id)+' | '+summary_text(summary)]
    else: selected=rows
    lines+=['','所选范围按模型 / 阶段汇总']
    for model,task in sorted({(r['model'],r['task']) for r in selected}):
        group=[r for r in selected if (r['model'],r['task'])==(model,task)]
        lines.append(model+' / '+task+'：'+summary_text(summarize(group)))
    lines+=['','使用的价格快照（美元 / 百万 Token）']
    prices={str(r.get('pricing')):r.get('pricing') for r in selected if r.get('pricing')}
    for p in prices.values():
        lines += [f"{p['model']}：输入 ${p['input']}，缓存 ${p['cached_input']}，输出 ${p['output']}；1 美元≈{p['usd_cny']} 元；核对于 {p['checked_on']}；依据 {p.get('basis','unknown')}",p['source']]
    lines += ['','本地明细文件：'+str(ledger.path)]
    app.text_window('Token 与人民币费用明细','\n'.join(lines))
