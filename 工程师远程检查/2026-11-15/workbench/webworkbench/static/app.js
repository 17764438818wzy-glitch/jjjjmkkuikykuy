'use strict';
const $=s=>document.querySelector(s);
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const titles={work:'选择与处理',processes:'进程监控',history:'批次记录'};
const names={paused:'已暂停',pausing:'暂停中',running:'运行中',queued:'等待处理',processing:'处理中',complete:'已入库',held:'待处理问题',excluded:'已排除',pending_delivery:'等待投递',pending_reconcile:'待核对',exited:'已退出',retry:'等待重试',blocked_service:'服务等待',graph:'文章处理',delivery:'入库投递',cleanup:'材料清理',repair:'问题修复',round1:'第一轮 · 分类与画像',round2:'第二轮 · 复核与纠正',round3_assets:'第三轮 · 图表',round3_redact:'第三轮 · 脱敏',round3_chunk:'第三轮 · 切片',seal:'封包',handoff:'台账交接',handed_off:'已封包',attachment_handoff:'附件封包',attachment_ready:'附件已封包'};
let state={csrf:'',loggedIn:false,snapshot:null,inputs:[],selected:new Set(),busy:false,online:false,controlReachable:false,batch:null,revision:0,page:'work',request:null,inputBatch:null};
const duration=v=>v==null?'—':v<60?`${v.toFixed(1)} 秒`:`${Math.floor(v/60)} 分 ${(v%60).toFixed(0)} 秒`;
const date=v=>new Date(v*1000).toLocaleString('zh-CN');
const requestId=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)),v=>v.toString(16).padStart(2,'0')).join('');
const stageName=(action,fallback)=>names[action]||names[String(action||'').split(':')[0]]||names[fallback]||'等待处理';
const badge=v=>`<span class="status-badge" data-state="${esc(v)}">${esc(names[v]||v)}</span>`;
const factName=v=>({pending:'待生成',generated:'已生成',packaged:'已封包',available:'已入库',unreadable:'文件待核对',configuration_pending:'归档配置待补',not_checked:'未抽检',original_and_note_readback:'原件与说明已读回',complete:'已完成',drawing:'图纸',software:'程序'}[v]||names[v]||v||'—');
names.configuration_pending='归档配置待补';
Object.assign(names,{idle:'等待后续动作',unknown:'状态待核对',creating:'连接中',prepared_local:'准备连接'});
function showDetail(a){
 const f=a.artifacts,b=a.correction_budget,cleanup=a.detail.cleanup||{};
 const fields=[['当前阶段',stageName(a.current_action,a.stage)],['程序历时',duration(a.elapsed_seconds)],['文章处理工时',duration(a.graph_seconds)],['入库工时',duration(a.delivery_seconds)],['累计纠正预算',`${b.used} / ${b.limit} 次`],['执行尝试',`${a.attempts} 次（含服务重试）`]];
 let html=`<dl class="detail-facts">${fields.map(([k,v])=>`<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl>`;
 if(a.reason)html+=`<p class="error">${esc(a.reason)}</p>`;
 html+='<h3>自动分类与目标库</h3>'+(a.routes.length?`<ul>${a.routes.map(r=>`<li><strong>${esc(r.code)} ${esc(r.name)}</strong> · ${r.expected_complete?'当前期望版本已入库':r.pending_reconcile?'远端结果待核对':r.held?'存在待处理问题':'等待投递'}<small class="secondary">论文库 ${esc(r.paper_dataset_id||'未映射')} · 资料库 ${esc(r.materials_dataset_id||'未映射')}</small></li>`).join('')}</ul>`:`<p class="muted">${a.attachment?'独立附件使用既有归档映射，详见下方。':'分类尚未完成，链路确定专业后自动选库。'}</p>`);
 html+=`<h3>处理产物</h3><dl class="detail-facts">${[['画像',factName(f.profile)],['目录 Tree',`${factName(f.tree)} · ${f.tree_nodes} 节点`],['切片',`${factName(f.chunks)} · ${f.chunk_count} 项`],['保留图片',`${f.selected_images} 项 · ${factName(f.images)}`]].map(([k,v])=>`<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl>`;
 if(a.attachment){const at=a.attachment;html+=`<h3>附件原件与正文线索</h3><p>${esc(factName(at.kind))} · ${esc(factName(at.state))} · ${esc(factName(at.verification))}</p><p>${esc(at.note.description)}</p><p>相关论文：${esc(at.note.related_paper_title||'尚未确定')}；正文关联：${at.association.status==='pending'?'待后续关联':'已记录'}；说明检索：${esc(factName(at.retrieval))}</p><p class="muted">说明库 ${esc(at.target.dataset_id||'待配置')} · 原件文件 ${esc(at.remote?.raw?.file_id||at.remote?.file_id||'待保存')}</p>`;}
 html+=`<h3>收尾与保留</h3><p>收尾待办 ${cleanup.closeout_pending||0} · 保留期内 ${cleanup.delayed||0} · 待核对 ${cleanup.pending_reconcile||0} · 清理错误 ${cleanup.errors||0}</p><p class="muted">Graph、Wiki 后续再做。额外检索抽检单独记录，不计作必要入库确认。</p><details><summary>执行记录与台账详情</summary><pre>${esc(JSON.stringify({任务:a.task_id,执行记录:a.recent_attempts,投递事实:a.detail},null,2))}</pre></details>`;
 $('#detail-title').textContent=a.name;$('#detail-content').innerHTML=html;$('#detail-dialog').showModal();
}
function notice(text){$('#notice-text').textContent=text;$('#notice').hidden=false;}
async function api(path,options={}){
 const headers={'x-csrf-token':state.csrf,...options.headers};
 let body=options.body;if(body&&!(body instanceof Blob)){headers['Content-Type']='application/json';body=JSON.stringify(body);}
 const abort=new AbortController();const timer=setTimeout(()=>abort.abort(),body instanceof Blob?600000:5000);
 let response;try{response=await fetch(path,{credentials:'same-origin',...options,headers,body,signal:abort.signal});}finally{clearTimeout(timer);}
 let value;try{value=await response.json();}catch{throw new Error('管理进程未返回有效结果');}
 if(!response.ok){if(response.status===401)showLogin();throw new Error(value.detail||'操作未完成');}return value;
}
function showLogin(){state.loggedIn=false;state.online=false;state.controlReachable=false;$('#login-view').hidden=false;$('#app').hidden=true;}
function showPage(page){state.page=page;for(const key of Object.keys(titles))$('#'+(key==='work'?'work':key)+'-page').hidden=key!==page;document.querySelectorAll('[data-page]').forEach(b=>b.classList.toggle('active',b.dataset.page===page));$('#section-title').textContent=titles[page];}
function controls(){const m=state.snapshot?.manager;const historical=!!state.snapshot?.batch_id&&state.snapshot.batch_id!==state.snapshot.latest_batch_id;$('#start').disabled=!state.online||state.busy||historical||!state.selected.size||m?.state!=='paused';$('#continue').disabled=!state.online||state.busy||historical||m?.state!=='paused'||!state.snapshot?.batch_id||!state.snapshot.jobs.some(j=>['queued','retry','blocked_service'].includes(j.state));$('#pause').disabled=!state.controlReachable||!m?.epoch;$('#pause-global').disabled=$('#pause').disabled;$('#choose-files').disabled=state.busy;}
function renderInputs(){
 $('#input-body').innerHTML=state.inputs.map(row=>`<tr><td><input type="checkbox" data-input="${esc(row.id)}" aria-label="选择 ${esc(row.name)}" ${state.selected.has(row.id)?'checked':''}></td><td>${esc(row.name)}</td><td>${(row.bytes/1024/1024).toFixed(2)} MB</td><td>${esc(date(row.created))}</td></tr>`).join('');
 $('#input-empty').hidden=!!state.inputs.length;$('#select-all').checked=state.inputs.length>0&&state.inputs.every(r=>state.selected.has(r.id));
 $('#input-body').querySelectorAll('input').forEach(el=>el.onchange=()=>{el.checked?state.selected.add(el.dataset.input):state.selected.delete(el.dataset.input);state.request=null;controls();});controls();
}
function render(){
 const s=state.snapshot;if(!s)return;const m=s.manager;if(s.batch_id&&state.inputBatch!==s.batch_id){$('#input-section').open=false;$('#run-settings').open=false;state.inputBatch=s.batch_id;}
 $('#manager-badge').textContent=names[m.state]||m.state;$('#manager-badge').dataset.state=m.state;
 $('#manager-description').textContent=m.state==='pausing'?'新动作已禁止，正在等待在途任务收束':m.state==='paused'?'新动作已禁止；点击开始或继续后才执行':'后台调度器正在自动处理选定任务';
 $('#total-count').textContent=s.articles.length;$('#running-count').textContent=m.live_units;$('#complete-count').textContent=s.articles.filter(a=>a.complete).length;$('#pending-count').textContent=s.articles.filter(a=>!a.complete).length;
 const cfg=s.settings;$('#environment-tag').textContent=s.mode==='operator'?'已接入现有链路':'隔离验收';$('#settings-summary').innerHTML=`<span>模型 <strong>${esc(cfg.model)}</strong></span><span>LLM 并发 <strong>${cfg.llm_workers}</strong></span><span>入库并发 <strong>${cfg.rag_workers}</strong></span><span>目标库 <strong>按专业自动选择 · ${cfg.specialty_count} 个专业</strong></span>`;
 $('#settings-summary').innerHTML+=`<span>Harness 执行 <strong>${cfg.harness?.location==='server'?`服务器 ${esc(cfg.harness.host)} · ${esc(cfg.harness.version)}`:'本机'}</strong></span><span>读取、转换、台账与封包 <strong>本机服务</strong></span>`;
 const resources=s.resources;const reasons={memory:'电脑内存紧张',disk:'磁盘余量不足',target_backlog:'等待投递任务较多',package_bytes:'待投递封包占用较多'};
 $('#queue-summary').textContent=`等待投递 ${resources.pending_targets} · 待处理映射 ${resources.mapping_pending_targets} · ${resources.reasons.length?resources.reasons.map(r=>reasons[r]||r).join('、'):'资源允许按既定并发调度'}`;
 $('#resource-summary').innerHTML=`<span>电脑内存 <strong>${resources.memory_percent.toFixed(1)}% / 阈值 ${cfg.max_memory_percent}%</strong></span><span>电脑可用磁盘 <strong>${(resources.free_bytes/1024**3).toFixed(1)} GiB</strong></span><span>待投递封包 <strong>${(resources.pending_package_bytes/1024**2).toFixed(1)} MiB</strong></span><span>服务 <strong>${s.services.map(v=>`${v.kind==='llm'?'模型':v.kind==='rag'?'RAG':'本地'}：${v.state==='closed'?'可调度':v.state==='open'?'熔断等待':'试探恢复'}`).join(' · ')}</strong></span>`;
 $('#attachment-config').textContent=cfg.attachment_targets.length?`独立附件沿用现有归档映射：${cfg.attachment_targets.map(t=>`${factName(t.kind)} → ${t.name}资料库`).join('；')}。图片随正文按专业分流。`:'独立附件归档映射尚待配置；正文和图片仍按专业自动分流。';
 const historical=!!s.batch_id&&s.batch_id!==s.latest_batch_id;$('#current-batch').hidden=!historical;
 $('#batch-caption').textContent=s.batch_id?`${historical?'历史批次（只读）':'当前批次'} ${s.batch_id} · 历时从首个处理进程计起，包含中间等待；入库工时单列，额外抽检另记`:'尚未开始批次';
 const term=$('#search').value.trim().toLowerCase();const articles=s.articles.filter(a=>(a.name+' '+a.task_id).toLowerCase().includes(term));
 $('#article-body').innerHTML=articles.map(a=>`<tr><td>${esc(a.name)}<small class="secondary">${esc(a.routes.map(r=>r.name).join('、')||(a.attachment?'独立附件':'等待自动分类'))}</small></td><td>${esc(stageName(a.current_action,a.stage))}</td><td>${badge(a.state)}${a.reason?`<small class="secondary">${esc(a.reason)}</small>`:''}</td><td>${a.process_id||'—'}</td><td>${duration(a.elapsed_seconds)}</td><td>${duration(a.delivery_seconds)}</td><td><button data-detail="${esc(a.task_id)}" class="text-button">查看</button></td></tr>`).join('');$('#article-empty').hidden=!!articles.length;
 $('#article-body').querySelectorAll('[data-detail]').forEach(b=>b.onclick=()=>showDetail(s.articles.find(x=>x.task_id===b.dataset.detail)));
 const processRows=[];for(const p of s.processes){const a=s.articles.find(a=>a.task_id===p.task_id);processRows.push(`<tr><td>${esc(a?.name||p.task_id)}</td><td>${p.pid||'—'}</td><td>${esc(names[p.kind]||p.kind)}</td><td>${p.state==='running'?esc(stageName(p.action,p.kind==='graph'?a?.stage:p.kind)):'—'}</td><td>${p.state==='running'?duration(p.heartbeat_age_seconds):'—'}</td><td>${duration(p.elapsed_seconds)}</td><td>${badge(p.state)}${p.exit_confirmed?' <small>已确认退出</small>':''}</td></tr>`);for(const child of p.members||[]){if(child.pid&&child.pid!==p.pid)processRows.push(`<tr><td>${esc(a?.name||p.task_id)}</td><td>${child.pid}</td><td>组成员 / ${p.root_pid||p.pid}</td><td>所属工作进程树</td><td>—</td><td>—</td><td>${badge(child.state)}</td></tr>`);}}
 for(const p of s.remote_processes||[]){const a=s.articles.find(a=>a.task_id===p.task_id);processRows.push(`<tr><td>${esc(a?.name||p.task_id)}<small class="secondary">${esc(p.host)}</small></td><td>${p.pid||'—'}<small class="secondary">服务器 PID</small></td><td>服务器 Harness 容器<small class="secondary">${esc(p.container||p.job_id)}</small></td><td>模型执行</td><td>${p.exit_confirmed?'—':duration(p.heartbeat_age_seconds)}</td><td>${duration(p.elapsed_seconds)}</td><td>${badge(p.state)}${p.exit_confirmed?' <small>整棵进程树已退出</small>':''}</td></tr>`);}
 $('#process-body').innerHTML=processRows.join('');$('#process-empty').hidden=!!processRows.length;
 $('#history-body').innerHTML=s.batches.map(b=>`<tr><td>${esc(date(b.created))}</td><td>${b.count}</td><td>${esc(b.id)}</td><td><button data-batch="${esc(b.id)}">查看记录</button></td></tr>`).join('');$('#history-empty').hidden=!!s.batches.length;
 $('#history-body').querySelectorAll('[data-batch]').forEach(b=>b.onclick=()=>run(async()=>{state.batch=b.dataset.batch;await refresh();showPage('work');}));
 $('#log-text').textContent=[`管理代次 ${m.epoch} / ${m.generation}`,`准入 ${m.admission_open?'开放':'关闭'}；暂停已持久化 ${m.pause_persisted?'是':'否'}`,...m.errors,...s.jobs.map(j=>`${names[j.kind]||j.kind} · ${names[j.state]||j.state} · ${j.job_id}`)].join('\n');controls();
}
async function refresh(){const revision=state.revision;try{const s=await api('/api/status'+(state.batch?'?batch_id='+encodeURIComponent(state.batch):''));if(revision!==state.revision)return;state.snapshot=s;state.online=true;state.controlReachable=true;$('#service-state').textContent='管理进程已连接';$('#last-update').textContent='更新于 '+new Date().toLocaleTimeString('zh-CN');render();}catch(e){state.online=false;state.controlReachable=false;$('#service-state').textContent='状态未知 / 连接中断';$('#manager-badge').textContent='状态未知';$('#manager-description').textContent='不能确认后台状态，请恢复连接后查看';try{const control=await api('/api/control');if(revision===state.revision&&state.snapshot){state.snapshot.manager=control.manager;state.controlReachable=true;$('#running-count').textContent=control.manager.live_units;$('#manager-badge').dataset.state=control.manager.state;$('#manager-badge').textContent=names[control.manager.state]||control.manager.state;$('#manager-description').textContent='台账暂不可读；管理进程仍可接收暂停。文章进度为上次记录。';$('#service-state').textContent='控制可连接 / 台账暂不可读';}}catch{}controls();throw e;}}
async function initialize(){state.csrf=(await api('/api/session')).csrf;state.loggedIn=true;$('#login-view').hidden=true;$('#app').hidden=false;state.inputs=await api('/api/inputs');renderInputs();await refresh();}
async function run(fn){try{await fn();}catch(e){notice(e.message);}}
async function control(path,extra={}){const m=state.snapshot?.manager;if(!m)throw new Error('请先连接管理进程');state.revision++;const result=await api(path,{method:'POST',body:{epoch:m.epoch,generation:m.generation,...extra}});state.snapshot.manager=result.manager;if(result.batch_id)state.batch=result.batch_id;render();await refresh();}
$('#login-form').onsubmit=event=>{event.preventDefault();run(async()=>{const data=Object.fromEntries(new FormData(event.target));try{const result=await api('/api/login',{method:'POST',body:data});state.csrf=result.csrf;event.target.password.value='';await initialize();$('#login-error').textContent='';}catch(e){$('#login-error').textContent=e.message;}});};
$('#logout').onclick=()=>run(async()=>{await api('/api/logout',{method:'POST'});showLogin();});
document.querySelectorAll('[data-page]').forEach(b=>b.onclick=()=>showPage(b.dataset.page));
$('#choose-files').onclick=()=>$('#file-picker').click();
$('#file-picker').onchange=()=>run(async()=>{state.busy=true;controls();try{const files=Array.from($('#file-picker').files);let done=0;for(const file of files){$('#upload-state').textContent=`正在准备 ${++done}/${files.length}：${file.name}`;const row=await api('/api/inputs/upload?name='+encodeURIComponent(file.name),{method:'POST',body:file});state.selected.add(row.id);}state.inputs=await api('/api/inputs');state.request=null;renderInputs();$('#upload-state').textContent=`已准备 ${files.length} 个文件，点击开始后自动处理。`;}finally{state.busy=false;$('#file-picker').value='';controls();}});
$('#select-all').onchange=()=>{state.selected=new Set($('#select-all').checked?state.inputs.map(r=>r.id):[]);state.request=null;renderInputs();};
$('#start').onclick=()=>run(async()=>{state.busy=true;controls();try{state.request=state.request||requestId();await control('/api/start',{inputs:Array.from(state.selected),request_id:state.request});state.selected.clear();state.request=null;renderInputs();}finally{state.busy=false;controls();}});
$('#pause').onclick=()=>run(async()=>{await control('/api/pause');});
$('#pause-global').onclick=$('#pause').onclick;
$('#continue').onclick=()=>run(async()=>{state.busy=true;controls();try{await control('/api/continue',{batch_id:state.snapshot?.batch_id});}finally{state.busy=false;controls();}});
$('#current-batch').onclick=()=>run(async()=>{state.batch=null;await refresh();});
$('#refresh').onclick=()=>run(refresh);$('#search').oninput=render;$('#notice-close').onclick=()=>$('#notice').hidden=true;$('#close-detail').onclick=()=>$('#detail-dialog').close();
initialize().catch(()=>showLogin());
async function poll(){try{if(state.loggedIn)await refresh();}catch{}finally{setTimeout(poll,1500);}}setTimeout(poll,1500);
