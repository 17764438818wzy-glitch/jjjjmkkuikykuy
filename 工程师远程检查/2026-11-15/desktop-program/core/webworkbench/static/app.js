'use strict';
const $=s=>document.querySelector(s);
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const titles={work:'选择与处理',processes:'处理进度',completed:'处理完成',history:'批次记录'};
const names={paused:'已暂停',pausing:'暂停中',running:'运行中',queued:'等待处理',processing:'处理中',complete:'已入库',held:'待处理问题',excluded:'已排除',pending_delivery:'等待投递',pending_reconcile:'待核对',exited:'已退出',retry:'等待重试',blocked_service:'服务等待',graph:'文章处理',delivery:'入库投递',cleanup:'材料清理',repair:'问题修复',round1:'第一轮 · 分类与画像',round2:'第二轮 · 复核与纠正',round3_assets:'第三轮 · 图表',round3_redact:'第三轮 · 脱敏',round3_chunk:'第三轮 · 切片',seal:'封包',handoff:'台账交接',handed_off:'已封包',attachment_handoff:'附件封包',attachment_ready:'附件已封包'};
let state={csrf:'',loggedIn:false,snapshot:null,controlManager:null,refreshRequest:null,controlRequest:null,controlSequence:0,appliedControlSequence:0,connectionSequence:0,resourceSummary:null,detailsReadable:false,inputs:[],selected:new Set(),busy:false,online:false,controlReachable:false,batch:null,revision:0,page:'work',request:null,inputBatch:null,expanded:new Set(),technical:new Set(),draft:null,rawSnapshot:null,inputPage:0,progressPages:{article:0,process:0},progressFilter:'unfinished',progressSearch:'',serverInputs:[],inputListing:null};
const duration=v=>!Number.isFinite(v)?'—':v<60?`${v.toFixed(1)} 秒`:v<3600?`${Math.floor(v/60)} 分 ${Math.floor(v%60)} 秒`:v<86400?`${Math.floor(v/3600)} 小时 ${Math.floor(v%3600/60)} 分`:`${Math.floor(v/86400)} 天 ${Math.floor(v%86400/3600)} 小时 ${Math.floor(v%3600/60)} 分`;
const date=v=>new Date(v*1000).toLocaleString('zh-CN');
const requestId=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)),v=>v.toString(16).padStart(2,'0')).join('');
const stageName=(action,fallback)=>names[action]||names[String(action||'').split(':')[0]]||names[fallback]||'等待处理';
const badge=v=>`<span class="status-badge" data-state="${esc(v)}">${esc(names[v]||v)}</span>`;
const managerLabel=m=>({paused:'新任务已暂停',pausing:'正在暂停新任务',running:'程序运行中'}[m.state]||names[m.state]||m.state);
const memoryLabel=value=>Number.isFinite(value?.used_bytes)&&Number.isFinite(value?.total_bytes)&&value.total_bytes>0?`已用 ${(value.used_bytes/1024**3).toFixed(1)} GB / 总共 ${(value.total_bytes/1024**3).toFixed(1)} GB`:'暂不可读';
function renderResources(){
 const s=state.snapshot,summary=state.resourceSummary,resources=summary?summary.resources:s?.resources;
 const counter=PaperflowCounterHealth.describe((state.controlManager||s?.manager)?.embedding_counter);
 $('#counter-state').textContent=`精确分词：${counter.label} · ${counter.detail}`;
 $('#counter-impact').textContent=counter.impact;
 const amount=(value,scale,unit)=>Number.isFinite(value)?`${(value/scale).toFixed(1)} ${unit}`:'未知';
 const memory=Number.isFinite(resources?.memory_percent)?`已用 ${resources.memory_percent.toFixed(1)}%`:memoryLabel({used_bytes:resources?.memory_used_bytes,total_bytes:resources?.memory_total_bytes});
 $('#resource-summary').innerHTML=`<span>电脑内存 <strong>${memory}</strong></span><span>服务器内存 <strong>${memoryLabel(state.serverMemory)}</strong></span><span>电脑可用磁盘 <strong>${amount(resources?.free_bytes,1024**3,'GiB')}</strong></span><span>待投递封包 <strong>${amount(resources?.pending_package_bytes,1024**2,'MiB')}</strong></span>`;
 if(s?.services?.length)$('#resource-summary').innerHTML+=`<span>服务（详情采样） <strong>${s.services.map(v=>`${v.kind==='llm'?'模型':v.kind==='rag'?'RAG':'本地'}：${v.state==='closed'?'可调度':v.state==='open'?'熔断等待':'试探恢复'}`).join(' · ')}</strong></span>`;
 const reasons={memory:'电脑内存紧张',disk:'磁盘余量不足',target_backlog:'等待投递任务较多',package_bytes:'待投递封包占用较多'};
 $('#queue-summary').textContent=resources?`等待投递 ${resources.pending_targets??'未知'} · 待处理映射 ${resources.mapping_pending_targets??'未知'} · ${resources.reasons?.length?resources.reasons.map(r=>reasons[r]||r).join('、'):'采样时资源允许按既定并发调度'}`:'排队与资源尚未采样，当前数量未知';
 const sampled=summary?summary.resources_sampled_at:s?.observed_at;
 $('#resource-sample').textContent=Number.isFinite(sampled)?`资源采样于 ${date(sampled)} · 缓存 ${Math.max(0,Math.floor(Date.now()/1000-sampled))} 秒，反映该次采样${state.controlReachable?'':'；当前连接中断'}`:'资源尚未采样；未知不表示没有积压';
 $('#slot-summary').textContent=summary?.slots?Object.entries(summary.slots).map(([name,v])=>`${{article_slots:'文章占位',llm_permits:'模型许可',harness_actors:'执行单元'}[name]||name} ${v.current_used} / ${v.capacity}`).join(' · ')+(state.controlReachable?'':' · 连接中断，显示上次占位'):'当前占位尚未读取';
}
function renderDetailSample(){
 const sampled=state.snapshot?.observed_at;
 $('#detail-sample').textContent=Number.isFinite(sampled)?`${state.detailsReadable?'文章详情采样于':'详情未更新，保留上次采样于'} ${date(sampled)}；下方数量和列表对应这次采样。`:'文章数量与详情尚未读取，当前未知；点击“刷新详情”读取。';
 $('#detail-sample').dataset.state=state.detailsReadable?'ready':'unknown';
}
function batchCounts(s){
 const articles=s?.articles||[],expected=s?.batches?.find(b=>b.id===s.batch_id)?.count;
 const known=Number.isInteger(expected)&&expected>=0&&articles.length===expected&&new Set(articles.map(a=>a.task_id)).size===expected&&articles.every(a=>typeof a.task_id==='string'&&a.task_id&&typeof a.complete==='boolean');
 if(!known)return {complete:null,pending:null,excluded:null,problems:null};
 return {complete:articles.filter(a=>a.complete===true).length,pending:articles.filter(a=>a.complete===false&&a.state!=='excluded').length,excluded:articles.filter(a=>a.state==='excluded').length,problems:articles.filter(a=>PaperflowProgress.problem(a)).length};
}
function renderBatchCounts(){
 const s=state.snapshot,counts=batchCounts(s),number=v=>Number.isInteger(v)&&v>=0?v:'—';
 $('#total-count').textContent=state.draft?state.inputs.length:number(s?.intake?.total??s?.batches?.find(b=>b.id===s.batch_id)?.count);
 $('#prepared-count').textContent=number(s?.intake?.ready);
 $('#unprepared-count').textContent=number(s?.intake?.pending);
 $('#complete-count').textContent=number(counts.complete);$('#pending-count').textContent=number(counts.pending);
 $('#excluded-caption').textContent=counts.excluded==null?'任务记录待确认':`已排除 ${counts.excluded} 份另计`;
 $('#preparation-caption').textContent=s?.intake?`准备失败 ${number(s.intake.failed)} · 已跳过 ${number(s.intake.skipped)}；清单文件与任务按台账分别计数。`:'准备数量尚未提供；“—”表示未知。';
}
function renderProblemCount(){
 $('#problem-count').textContent=batchCounts(state.snapshot).problems??'—';
}
function renderBatchStatus(){
 const s=state.snapshot,articles=s?.articles||[],historical=!!s?.batch_id&&s.batch_id!==s.latest_batch_id;
 let text='尚未开始批次',detail='选择原稿并点击开始后，显示本批入库进度。',tone='queued';
 $('#batch-status-label').textContent=historical?'历史批次进度':'本批进度';
 if(!state.online){text='本批状态待确认';detail=state.snapshot?'详情未更新，下方保留上次采样；刷新后确认本批结果。':'详情尚未读取，读取成功后确认本批结果。';tone='unknown';}
 else if(state.draft){text='尚未开始本批';detail='本次清单已选好后，点击开始才会准备文件并处理。';}
 else if(s?.batch_id){
  const expected=s.batches.find(b=>b.id===s.batch_id)?.count,ids=new Set(articles.map(a=>a.task_id));
  const complete=articles.filter(a=>a.complete===true).length;
  if(!Number.isInteger(expected)||expected<1||articles.length!==expected||ids.size!==expected||articles.some(a=>typeof a.task_id!=='string'||!a.task_id||typeof a.complete!=='boolean')){
   text='本批状态待确认';detail='批次记录尚未完整，请刷新后确认。';tone='unknown';
  }else if(s.intake&&(s.intake.pending||s.intake.failed)){text='本批尚未完成';detail=`清单共 ${s.intake.total} 份；已准备 ${s.intake.ready}，待准备 ${s.intake.pending}，准备失败 ${s.intake.failed}，跳过 ${s.intake.skipped}。`;}
  else if(complete===expected){text='本批已完成';detail=`${expected} 篇 / 份材料均已确认入库。`;tone='complete';}
  else{text='本批尚未完成';detail=`已确认入库 ${complete} / ${expected} 篇 / 份材料。`;}
 }
 $('#batch-badge').textContent=text;$('#batch-badge').dataset.state=tone;$('#batch-description').textContent=detail;
 return tone==='complete';
}
const factName=v=>({pending:'待生成',generated:'已生成',packaged:'已封包',available:'已入库',unreadable:'文件待核对',configuration_pending:'归档配置待补',not_checked:'未抽检',original_and_note_readback:'原件与说明已读回',complete:'已完成',drawing:'图纸',software:'程序',image:'独立图片',spreadsheet:'配套表格',data:'数据附件'}[v]||names[v]||v||'—');
names.configuration_pending='归档配置待补';
Object.assign(names,{idle:'等待后续动作',unknown:'状态待核对',creating:'连接中',prepared_local:'准备连接'});
function progressKey(scope,a){return [scope,state.snapshot.batch_id||'',a.task_id].join(':');}
function stepsHtml(view){
 return `<ol class="article-steps">${view.steps.map(step=>`<li data-step="${esc(step.state)}"><span class="step-symbol" aria-hidden="true">${{done:'✓',current:'▶',pending:'○',skipped:'—'}[step.state]}</span><span><strong>${esc(step.label)}</strong><small>${esc(step.caption)}</small></span></li>`).join('')}</ol>`;
}
function technicalHtml(a){
 const s=state.snapshot;
 return `<pre>${esc(JSON.stringify({任务编号:a.task_id,本机进程及辅助进程:(s?.processes||[]).filter(p=>p.task_id===a.task_id),服务器进程:(s?.remote_processes||[]).filter(p=>p.task_id===a.task_id),执行记录:a.recent_attempts},null,2))}</pre>`;
}
function progressRows(articles,scope){
 return articles.map(a=>{
  const view=PaperflowProgress.describe(a,state.snapshot,state.online),key=progressKey(scope,a),open=state.expanded.has(key),id=scope+'-progress-'+a.task_id;
  return `<tr class="article-summary"><td>${esc(a.name)}<small class="secondary">${esc(a.routes.map(r=>r.name).join('、')||(a.attachment?'独立附件':'程序自动选库'))}</small></td><td class="activity-cell"><strong>${esc(view.title)}</strong><small class="activity-note">${esc(view.detail||view.location)}</small></td><td>${badge(view.tone)}</td><td>${duration(a.elapsed_seconds)}</td><td>${duration(a.delivery_seconds)}</td><td><button class="progress-toggle" data-progress="${esc(key)}" aria-controls="${esc(id)}" aria-expanded="${open}">${open?'收起进度 ▲':'展开进度 ▼'}</button></td></tr>
  <tr class="progress-row" id="${esc(id)}" ${open?'':'hidden'}><td colspan="6"><div class="progress-panel"><p class="progress-explanation">${esc(view.detail||view.title)}</p>${stepsHtml(view)}${a.selectedAttachments?.length?`<details><summary>同目录附件详情（${a.selectedAttachments.length} 份，关联待确认）</summary><ul>${a.selectedAttachments.map(r=>`<li>${esc(r.name)} · ${esc(r.state==='ready'?'已交给处理队列':r.state==='failed'?r.error:'等待准备')}</li>`).join('')}</ul></details>`:''}<div class="progress-bottom"><details class="technical-details" data-technical="${esc(key)}" ${state.technical.has(key)?'open':''}><summary>技术详情（进程编号与运行记录）</summary>${technicalHtml(a)}</details><button class="text-button" data-detail="${esc(a.task_id)}">查看产物与入库详情</button></div></div></td></tr>`;
 }).join('');
}
function groupProgress(articles){
 const byId=new Map(articles.map(a=>[a.task_id,a])),hidden=new Set(),extras=new Map();
 for(const row of (state.inputListing?state.inputs:PaperflowIntake.grouped(intakeQueue.entries))){
  if(row.task_id&&byId.has(row.task_id)&&row.attachments?.length){extras.set(row.task_id,row.attachments);for(const a of row.attachments)if(a.task_id&&a.task_id!==row.task_id)hidden.add(a.task_id);}
 }
 return articles.filter(a=>!hidden.has(a.task_id)).map(a=>({...a,selectedAttachments:extras.get(a.task_id)||[]}));
}
function matchesProgress(a,filter,s){
 const unfinished=a.complete!==true&&a.state!=='excluded',issue=!!PaperflowProgress.problem(a);
 if(filter==='all')return true;
 if(filter==='complete')return a.complete===true;
 if(filter==='excluded')return a.state==='excluded';
 if(filter==='held')return issue;
 if(filter==='running')return unfinished&&a.state==='processing';
 if(filter==='delivery')return unfinished&&a.state==='pending_delivery';
 if(filter==='reconcile')return unfinished&&a.state==='pending_reconcile';
 if(filter==='paused')return unfinished&&!issue&&a.state!=='pending_reconcile'&&s.manager.state==='paused';
 return unfinished;
}
function setProgressFilter(value){
 state.progressFilter=value;state.progressPages={article:0,process:0};
 for(const scope of ['article','process'])$('#'+scope+'-status-filter').value=value;
 renderProgressTables();
}
function renderProgressTables(){
 const s=state.snapshot;if(!s)return;
 const term=state.progressSearch.trim().toLowerCase(),list=s.articles.filter(a=>matchesProgress(a,state.progressFilter,s)&&(a.name+' '+a.task_id).toLowerCase().includes(term));
 for(const scope of ['article','process']){
  const grouped=state.progressFilter==='unfinished'||state.progressFilter==='all'?groupProgress(list):list,pages=Math.max(1,Math.ceil(grouped.length/20));state.progressPages[scope]=Math.min(state.progressPages[scope],pages-1);const offset=state.progressPages[scope]*20;
  $('#'+scope+'-page-caption').textContent=`匹配 ${list.length} 份任务${grouped.length!==list.length?`，合并附件后 ${grouped.length} 行`:''} · 第 ${state.progressPages[scope]+1}/${pages} 页，每页 20 行${state.detailsReadable?'':' · 上次采样'}`;$('#'+scope+'-prev').disabled=offset===0;$('#'+scope+'-next').disabled=offset+20>=grouped.length;
  const body=$('#'+scope+'-body');body.innerHTML=progressRows(grouped.slice(offset,offset+20),scope);const empty=$('#'+scope+'-empty');empty.hidden=!!list.length;
  empty.textContent=s.articles.length?'没有符合当前搜索和状态筛选的记录；可选择“全部状态”，或到“处理完成”查看跨批次结果。':'本批尚无已准备的任务记录。';
  body.querySelectorAll('[data-progress]').forEach(button=>button.onclick=()=>{
   const key=button.dataset.progress,open=!state.expanded.has(key);open?state.expanded.add(key):state.expanded.delete(key);
   button.setAttribute('aria-expanded',String(open));button.textContent=open?'收起进度 ▲':'展开进度 ▼';document.getElementById(button.getAttribute('aria-controls')).hidden=!open;
  });
  body.querySelectorAll('[data-technical]').forEach(details=>details.ontoggle=()=>{if(details.isConnected)details.open?state.technical.add(details.dataset.technical):state.technical.delete(details.dataset.technical);});
  body.querySelectorAll('[data-detail]').forEach(button=>button.onclick=()=>showDetail(s.articles.find(a=>a.task_id===button.dataset.detail)));
 }
 if($('#detail-dialog').open&&state.detailTask){
  const article=s.articles.find(a=>a.task_id===state.detailTask);
  $('#detail-activity').textContent=article?PaperflowProgress.describe(article,s,state.online).title:'当前批次已切换，请重新打开文章详情';
 }
}
function showDetail(a){
 if(!a.completed_details&&(a.complete||a.state==='excluded')){run(async()=>showDetail(await api('/api/completed/'+encodeURIComponent(a.task_id))));return;}
 state.detailTask=a.completed_details?null:a.task_id;
 const f=a.artifacts,b=a.correction_budget,cleanup=a.detail.cleanup||{};
 const issue=PaperflowProgress.problem(a),view=PaperflowProgress.describe(a,state.snapshot||{processes:[],remote_processes:[],manager:{},settings:{}},a.completed_details?true:state.online);
 const fields=[['程序历时',duration(a.elapsed_seconds)],['文章处理工时',duration(a.graph_seconds)],['入库工时',duration(a.delivery_seconds)],['累计纠正预算',`${b.used} / ${b.limit} 次`],['执行尝试',`${a.attempts} 次（含服务重试）`]];
 let html=`<section class="detail-overview${issue?' detail-problem':''}"><h3 id="detail-activity">${esc(view.title)}</h3>${issue?`<p>${esc(issue.detail)}</p><p><strong>下一步：</strong>${esc(issue.next)}</p>${issue.reason?`<details><summary>原始错误</summary><pre>${esc(issue.reason)}</pre></details>`:''}`:`<p>${esc(view.detail||view.location)}</p>`}</section>`;
 html+=`<details class="timing-details"><summary>耗时与尝试记录 · 程序历时 ${duration(a.elapsed_seconds)}</summary><dl class="detail-facts">${fields.map(([k,v])=>`<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl></details>`;
 html+='<h3>自动分类与目标库</h3>'+(a.routes.length?`<ul>${a.routes.map(r=>`<li><strong>${esc(r.code)} ${esc(r.name)}</strong> · ${r.expected_complete?'当前期望版本已入库':r.pending_reconcile?'远端结果待核对':r.held?'存在待处理问题':'等待投递'}<small class="secondary">论文库 ${esc(r.paper_dataset_id||'未映射')} · 资料库 ${esc(r.materials_dataset_id||'未映射')}</small></li>`).join('')}</ul>`:`<p class="muted">${a.attachment?'独立附件使用既有归档映射，详见下方。':'分类尚未完成，链路确定专业后自动选库。'}</p>`);
 if(a.completed_details&&a.attachment)html+='<p class="muted">独立附件按原件与说明归档，不生成论文目录和正文切片。</p>';
 else html+=`<h3>处理产物</h3><dl class="detail-facts">${[['画像',factName(f.profile)],['目录 Tree',`${factName(f.tree)} · ${f.tree_nodes} 节点`],['切片',`${factName(f.chunks)} · ${f.chunk_count} 项`],['图片与图表',a.completed_details?PaperflowCompleted.imageSummary(a.image_summary):`${f.selected_images} 项 · ${factName(f.images)}`]].map(([k,v])=>`<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl>`;
 if(a.completed_details)html+=PaperflowCompleted.details(a);
 else if(a.attachment){const at=a.attachment;html+=`<h3>附件原件与正文线索</h3><p>${esc(factName(at.kind))} · ${esc(factName(at.state))} · ${esc(factName(at.verification))}</p><p>${esc(at.note.description)}</p><p>相关论文：${esc(at.note.related_paper_title||'尚未确定')}；正文关联：${at.association.status==='pending'?'待后续关联':'已记录'}；说明检索：${esc(factName(at.retrieval))}</p><p class="muted">说明库 ${esc(at.target.dataset_id||'待配置')} · 原件文件 ${esc(at.remote?.raw?.file_id||at.remote?.file_id||'待保存')}</p>`;}
 html+=`<h3>收尾与保留</h3><p>收尾待办 ${cleanup.closeout_pending||0} · 保留期内 ${cleanup.delayed||0} · 待核对 ${cleanup.pending_reconcile||0} · 清理错误 ${cleanup.errors||0}</p><p class="muted">Graph、Wiki 后续再做。额外检索抽检单独记录，不计作必要入库确认。</p><details class="technical-details"><summary>技术详情（进程编号与运行记录）</summary>${technicalHtml(a)}<pre>${esc(JSON.stringify({投递事实:a.detail},null,2))}</pre></details>`;
 $('#detail-title').textContent=a.name;$('#detail-content').innerHTML=html;$('#detail-dialog').showModal();
}
function notice(text){$('#notice-text').textContent=text;$('#notice').hidden=false;}
async function api(path,options={}){
 const {timeoutMs,...fetchOptions}=options;
 const headers={'x-csrf-token':state.csrf,...options.headers};
 let body=options.body;if(body&&!(body instanceof Blob)){headers['Content-Type']='application/json';body=JSON.stringify(body);}
 const maximum=timeoutMs??(body instanceof Blob?600000:5000),abort=new AbortController(),timer=setTimeout(()=>abort.abort(),maximum);
 try{
  const response=await fetch(path,{credentials:'same-origin',...fetchOptions,headers,body,signal:abort.signal});
  let value;try{value=await response.json();}catch(error){if(abort.signal.aborted)throw error;throw Object.assign(new Error('管理进程未返回有效结果'),{retryable:response.status>=500});}
  if(!response.ok){if(response.status===401)showLogin();throw Object.assign(new Error(value.detail||'操作未完成'),{status:response.status,retryable:[408,429].includes(response.status)||response.status>=500});}return value;
 }catch(error){
  if(abort.signal.aborted)throw Object.assign(new Error(`请求等待超过 ${maximum/1000} 秒，${!options.method||options.method==='GET'?'本次读取未完成，请稍后刷新；任务结果需以台账核对':'尚未确认本次操作结果，请刷新后核对'}`),{path,code:'request_timeout',retryable:true});
  error.path=path;if(error.name==='TypeError')error.retryable=true;throw error;
 }finally{clearTimeout(timer);}
}
function showLogin(){state.revision++;state.loggedIn=false;state.online=false;state.controlReachable=false;state.controlManager=null;$('#login-view').hidden=false;$('#app').hidden=true;}
function showPage(page){state.page=page;for(const key of Object.keys(titles))$('#'+(key==='work'?'work':key)+'-page').hidden=key!==page;document.querySelectorAll('[data-page]').forEach(b=>b.classList.toggle('active',b.dataset.page===page));$('#section-title').textContent=titles[page];if(page==='completed')PaperflowCompleted.open();}
function controls(){
 const m=state.controlManager||state.snapshot?.manager,limit=state.snapshot?.settings.selected_limit,historical=!!state.snapshot?.batch_id&&state.snapshot.batch_id!==state.snapshot.latest_batch_id;
 const preparing=typeof intakeQueue!=='undefined'&&(intakeQueue.running||!!intakeQueue.pumping);
 const pending=!!(state.snapshot?.intake?.pending||state.snapshot?.intake?.failed);
 const preparationOnly=m?.state==='running'&&pending&&!preparing&&intakeQueue.token?.batch_id===state.snapshot?.batch_id&&intakeQueue.token?.epoch===m.epoch&&intakeQueue.token?.generation===m.generation&&intakeQueue.entries.some(r=>r.file||r.nas);
 $('#start').disabled=!state.online||state.busy||preparing||historical||!state.draft||!state.selected.size||(Number.isInteger(limit)&&state.selected.size>limit)||m?.state!=='paused';
 $('#new-batch').disabled=!state.online||state.busy||preparing;
 $('#continue').disabled=!state.online||state.busy||preparing||historical||!state.snapshot?.batch_id||!(preparationOnly||m?.state==='paused'&&(state.snapshot.jobs.some(j=>['queued','retry','blocked_service'].includes(j.state))||pending));
 $('#continue').textContent=preparationOnly?'继续准备文件':'继续当前批次';
 $('#continue').classList.toggle('primary',m?.state==='paused'&&!state.draft);
 $('#start').classList.toggle('primary',!!state.draft);
 $('#continue').title='接续当前批次中符合条件的任务；问题稿需按原因处理。';
 $('#pause').title='停止安排新动作，等待已发出的操作结束。';
 $('#pause').textContent=m?.state==='paused'?'已暂停':m?.state==='pausing'?'暂停中':'暂停新任务';
 $('#pause').disabled=!state.controlReachable||!m?.epoch;$('#pause-global').disabled=$('#pause').disabled;
 for(const id of ['choose-files','choose-folder','choose-nas'])$('#'+id).disabled=state.busy||preparing;
}
function inputDetails(row){
 const captions={pending:'等待准备',preparing:'正在准备副本',prepared:'副本已保存',ready:'已交给处理队列',skipped:'已跳过',failed:'准备失败'};
 return `<details><summary>详情${row.attachments?.length?' · 附件 '+row.attachments.length+' 份':''}</summary><p>位置：${esc(row.relative_path||row.name)}</p><p>${esc(captions[row.state]||'等待处理')}${row.error?'：'+esc(row.error):''}</p>${row.attachments?.length?`<p>同目录附件，关联待 LLM 确认；原件仍分别处理。</p><ul>${row.attachments.map(a=>`<li>${esc(a.name)} · ${esc(captions[a.state]||'等待准备')}${a.error?'：'+esc(a.error):''}</li>`).join('')}</ul>`:''}</details>`;
}
function renderInputs(){
 const inputs=state.inputs,page=state.inputListing?{rows:inputs,index:state.inputPage,pages:Math.max(1,Math.ceil(state.inputListing.main_total/20)),mainRows:state.inputListing.main_total,total:state.inputListing.summary.total}:PaperflowIntake.page(inputs,state.inputPage);state.inputPage=page.index;
 $('#input-body').innerHTML=page.rows.map(row=>`<tr><td><input type="checkbox" data-input="${esc(row.id)}" aria-label="选择 ${esc(row.name)}" ${state.selected.has(row.id)?'checked':''} ${row.state==='skipped'||!state.draft?'disabled':''}></td><td>${esc(row.name)}${inputDetails(row)}</td><td>${(row.bytes/1024/1024).toFixed(2)} MB</td><td>${esc(({pending:'待准备',preparing:'准备中',prepared:'副本已保存',ready:'已交给处理队列',failed:'准备失败',skipped:'跳过'})[row.state]||'待准备')}</td></tr>`).join('');
 selectionCaption();$('#input-empty').hidden=!!inputs.length;$('#select-all').disabled=!state.draft;
 $('#select-all').checked=inputs.some(r=>r.state!=='skipped')&&inputs.filter(r=>r.state!=='skipped').every(r=>state.selected.has(r.id));
 $('#input-page-caption').textContent=`共 ${page.total} 份文件（含附件），主列表 ${page.mainRows} 条；第 ${page.index+1}/${page.pages} 页，每页 20 条`;
 $('#input-prev').disabled=page.index===0;$('#input-next').disabled=page.index>=page.pages-1;
 $('#input-body').querySelectorAll('input').forEach(el=>el.onchange=()=>{const row=page.rows.find(r=>r.id===el.dataset.input);for(const r of [row,...(row.attachments||[])]){r.selected=el.checked;const original=inputs.find(x=>x.id===r.id);if(original)original.selected=el.checked;el.checked?state.selected.add(r.id):state.selected.delete(r.id);}state.request=null;renderInputs();});
 if(state.draft)$('#total-count').textContent=inputs.length;
 controls();
}
function selectionCaption(){
 const capacity=state.resourceSummary?.slots?.article_slots?.capacity;
 $('#selection-caption').textContent=`本次清单 ${state.inputListing?.summary.total??state.inputs.length} 份（含附件），已选 ${state.selected.size} 份；${Number.isInteger(capacity)?`当前文章占位上限 ${capacity}，`:'实际可处理数量见“排队与资源”，'}准备好后按现有调度递补。`;
}
function emptyBatch(s){return {...s,batch_id:null,articles:[],processes:[],remote_processes:[],jobs:[],intake:null};}
function beginDraft(){state.draft=true;state.batch=null;state.inputPage=0;state.serverInputs=[];state.inputListing=null;state.request=null;if(state.rawSnapshot)state.snapshot=emptyBatch(state.rawSnapshot);}
function renderControlState(complete=false){
 const m=state.controlManager||state.snapshot?.manager,known=!!m&&(state.online||state.controlReachable);
 $('#manager-badge').textContent=known?managerLabel(m):'状态未知';$('#manager-badge').dataset.state=known?m.state:'unknown';
 $('#running-count').textContent=known?m.live_units:'—';
 $('#manager-description').textContent=!state.online?(state.controlReachable?(state.snapshot?'文章进度未更新，显示上次记录；暂停仍可提交':'文章进度尚未读取，数量待确认；暂停仍可提交'):'连接中断，不能确认当前后台状态'):m.state==='pausing'?'等待已发出的操作结束，进度会保留':m.state==='paused'?(complete?'本批已完成；点“选择新一批”选择文件夹，再点击开始。':'点击开始或继续后才会处理；不会自动开始下一批'):'后台调度器正在自动处理选定任务';
 if(!state.snapshot){
  for(const id of ['total','complete','pending','prepared','unprepared'])$('#'+id+'-count').textContent='—';
  $('#batch-caption').textContent='批次信息尚未读取';
  for(const scope of ['article','process'])$('#'+scope+'-empty').textContent='文章进度尚未读取，恢复连接后显示。';
 }
}
function render(){
 renderDetailSample();
 const s=state.snapshot;if(!s){renderControlState();renderResources();controls();return;}const m=s.manager;
 const complete=renderBatchStatus(),inputKey=JSON.stringify([s.batch_id,complete]);
 if(s.batch_id&&state.inputBatch!==inputKey){$('#input-section').open=complete;$('#run-settings').open=false;state.inputBatch=inputKey;}
 renderControlState(complete);
 renderBatchCounts();
 renderProblemCount();
 const cfg=s.settings;$('#environment-tag').textContent=s.mode==='operator'?'已接入现有链路':'隔离验收';$('#settings-summary').innerHTML=`<span>模型 <strong>${esc(cfg.model)}</strong></span><span>LLM 并发 <strong>${cfg.llm_workers}</strong></span><span>入库并发 <strong>${cfg.rag_workers}</strong></span><span>目标库 <strong>按专业自动选择 · ${cfg.specialty_count} 个专业</strong></span>`;
 selectionCaption();
 $('#intake-policy').textContent=Number.isInteger(cfg.selected_limit)?`当前部署授权每批最多 ${cfg.selected_limit} 个文件。`:'可选择整个文件夹及子文件夹，选稿数量不受 10 个限制。';
 const accept=(cfg.supported_formats||[]).join(',');$('#file-picker').accept=accept;$('#folder-picker').accept=accept;
 $('#settings-summary').innerHTML+=`<span>Harness 执行 <strong>${cfg.harness?.location==='server'?`服务器 ${esc(cfg.harness.host)} · ${esc(cfg.harness.version)}`:'本机'}</strong></span><span>读取、转换、台账与封包 <strong>本机服务</strong></span>`;
 renderResources();
 $('#attachment-config').textContent=cfg.attachment_targets.length?`独立附件沿用现有归档映射：${cfg.attachment_targets.map(t=>`${factName(t.kind)} → ${t.name}资料库`).join('；')}。图片随正文按专业分流。`:'独立附件归档映射尚待配置；正文和图片仍按专业自动分流。';
 const historical=!!s.batch_id&&s.batch_id!==s.latest_batch_id;$('#current-batch').hidden=!(historical||(state.draft&&state.rawSnapshot?.batch_id&&(state.rawSnapshot.intake?.open||state.rawSnapshot.jobs.some(j=>['queued','retry','running','blocked_service'].includes(j.state)))));
 const batch=s.batches.find(b=>b.id===s.batch_id);
 $('#batch-caption').textContent=s.batch_id?`${historical?'历史批次（只读）':'当前批次'}${batch?.created?' · '+date(batch.created):''} · ${s.articles.length} 份已登记任务。历时含中间等待；入库工时单列。`:'尚未开始批次';
 $('#batch-caption').title=s.batch_id||'';
 renderProgressTables();
 $('#history-body').innerHTML=s.batches.map(b=>`<tr><td><strong>${esc(date(b.created))}</strong><small class="secondary">${b.id===s.latest_batch_id?'当前批次':'历史批次'}</small></td><td>${b.count} 份已登记任务</td><td><details><summary>查看批次编号</summary><code>${esc(b.id)}</code></details></td><td><button data-batch="${esc(b.id)}" aria-label="查看 ${esc(date(b.created))} 的批次记录">查看进度</button></td></tr>`).join('');$('#history-empty').hidden=!!s.batches.length;
 $('#history-body').querySelectorAll('[data-batch]').forEach(b=>b.onclick=()=>run(async()=>{state.draft=false;state.inputPage=0;state.batch=b.dataset.batch;await refresh();await loadBatchInputs();showPage('work');}));
 $('#log-text').textContent=[`管理代次 ${m.epoch} / ${m.generation}`,`准入 ${m.admission_open?'开放':'关闭'}；暂停已持久化 ${m.pause_persisted?'是':'否'}`,...m.errors,...s.jobs.map(j=>`${names[j.kind]||j.kind} · ${names[j.state]||j.state} · ${j.job_id}`)].join('\n');controls();
}
function refresh(){
 const revision=state.revision,batch=state.batch,current=state.refreshRequest;
 if(current&&current.revision===revision&&current.batch===batch)return current.promise;
 const request={revision,batch,sequence:++state.controlSequence};state.refreshRequest=request;
 request.promise=refreshStatus(request).finally(()=>{if(state.refreshRequest===request)state.refreshRequest=null;});
 return request.promise;
}
function acceptControl(manager,sequence){
 const previous=state.controlManager,sameEpoch=previous?.epoch===manager.epoch;
 if((sameEpoch&&manager.generation<previous.generation)||(sequence<state.appliedControlSequence&&!(sameEpoch&&manager.generation>previous.generation)))return false;
 state.appliedControlSequence=Math.max(state.appliedControlSequence,sequence);state.controlManager=manager;
 if(state.snapshot)state.snapshot.manager=manager;
 if(state.rawSnapshot)state.rawSnapshot.manager=manager;
 if(intakeQueue.running&&intakeQueue.token&&(!manager.admission_open||manager.epoch!==intakeQueue.token.epoch||manager.generation!==intakeQueue.token.generation))intakeQueue.pause();
 return true;
}
function observeConnection(reachable,sequence){
 // A higher generation can improve control facts without making an older
 // network observation newer than a failed connection check.
 if(sequence<state.connectionSequence)return false;
 state.connectionSequence=sequence;state.controlReachable=reachable;return true;
}
function refreshControl(){
 const revision=state.revision,current=state.controlRequest;
 if(current&&current.revision===revision)return current.promise;
 const request={revision,sequence:++state.controlSequence};state.controlRequest=request;
 request.promise=(async()=>{
  const current=()=>state.controlRequest===request&&state.revision===revision;
  try{
   const summary=await api('/api/control');
   if(!current())return;
   acceptControl(summary.manager,request.sequence);
   const connected=observeConnection(true,request.sequence);
   state.online=state.controlReachable&&!!state.snapshot&&state.detailsReadable;
   if(connected){
    state.resourceSummary=summary;
     $('#service-state').textContent=state.online?'程序控制在线 · 详情已读取':'程序控制在线 · 详情未更新';
    $('#last-update').textContent='控制更新于 '+new Date().toLocaleTimeString('zh-CN');
   }
  }catch(error){
   if(!current()||!observeConnection(false,request.sequence))return;
   state.appliedControlSequence=Math.max(state.appliedControlSequence,request.sequence);
   state.online=false;$('#service-state').textContent='状态未知 / 连接中断';
   throw error;
  }finally{
   if(current()){renderControlState();renderDetailSample();renderResources();controls();}
  }
 })().finally(()=>{if(state.controlRequest===request)state.controlRequest=null;});
 return request.promise;
}
async function refreshStatus(request){
 const current=()=>state.refreshRequest===request&&request.revision===state.revision&&request.batch===state.batch;
 try{
  const s=await api('/api/status'+(request.batch?'?batch_id='+encodeURIComponent(request.batch):''),{timeoutMs:15000});
  if(!current())return;
  acceptControl(s.manager,request.sequence);s.manager=state.controlManager||s.manager;
  state.rawSnapshot=s;
  if(state.draft===null)state.draft=!s.intake?.pending&&!s.intake?.failed&&!s.jobs.some(j=>['queued','retry','running','blocked_service'].includes(j.state))&&s.articles.every(a=>a.complete||a.state==='excluded');
  state.snapshot=state.draft?emptyBatch(s):s;
  state.detailsReadable=true;observeConnection(true,request.sequence);state.online=state.controlReachable;
   if(state.controlReachable)$('#service-state').textContent='程序控制在线 · 详情已读取';
  $('#last-update').textContent='详情更新于 '+new Date().toLocaleTimeString('zh-CN');render();
 }catch(e){
  if(!current())return;
  state.detailsReadable=false;state.online=false;
   // A failed ledger read does not invalidate the independent control sample.
   $('#service-state').textContent=state.controlReachable?'程序控制在线 · 详情未更新':'程序控制待确认 · 详情未更新';
  renderControlState();renderProblemCount();renderBatchStatus();renderProgressTables();renderDetailSample();controls();
  try{
   await refreshControl();
    if(current()&&state.controlReachable)$('#service-state').textContent='程序控制在线 · 台账暂不可读';
  }catch{}
  if(!current())return;
  renderControlState();controls();throw e;
 }
}
async function initialize(){
 state.csrf=(await api('/api/session')).csrf;state.loggedIn=true;$('#login-view').hidden=true;$('#app').hidden=false;state.inputs=[];renderInputs();
 try{await refresh();if(!state.draft)await loadBatchInputs();}catch(e){if(!state.loggedIn)throw e;notice(e.message);}
}
async function run(fn){try{await fn();}catch(e){notice(e.message);}}
async function control(path,extra={}){const m=state.controlManager||state.snapshot?.manager;if(!m)throw new Error('请先连接管理进程');const sequence=++state.controlSequence;state.revision++;const result=await api(path,{method:'POST',body:{epoch:m.epoch,generation:m.generation,...extra},timeoutMs:path==='/api/start'||path==='/api/continue'?600000:5000});state.revision++;acceptControl(result.manager,sequence);if(result.batch_id){state.batch=result.batch_id;state.draft=false;}render();await refresh();return result;}
async function prepareNewBatch(){
 if(state.busy||!state.online)return;
 state.busy=true;controls();
 try{
  state.revision++;intakeQueue.pause();intakeQueue.entries=[];intakeQueue.token=null;beginDraft();await refresh();
  state.inputs=[];state.selected.clear();state.request=null;showPage('work');renderInputs();
  $('#input-section').open=true;$('#upload-state').textContent='选择新一批文件或整个文件夹；准备好后再点击“开始选中的原稿”。';
  $('#input-section').scrollIntoView({behavior:'smooth',block:'start'});
 }finally{state.busy=false;controls();}
 $('#choose-folder').focus({preventScroll:true});
}
const intakeQueue=new PaperflowIntake.Queue({
 canReuse(row){return row.preparation_request===(intakeQueue.token?.request_id||state.request);},
 async prepare(row){
  $('#upload-state').textContent=`正在准备：${row.name}；已准备的文件继续由后台处理。`;
  const request_id=intakeQueue.token?.request_id||state.request;let result;
  if(row.nas)result=await api('/api/inputs/nas',{method:'POST',body:{...row.nas,request_id,item_id:row.id},timeoutMs:600000});
  else{
   if(!row.file)throw new Error('原稿尚未准备，请重新选择同一文件夹，再点击继续');
   result=await api('/api/inputs/upload?'+new URLSearchParams({name:row.name,relative_path:row.relative_path,request_id,item_id:row.id}),{method:'POST',body:row.file});
  }
  row.preparation_request=request_id;return result;
 },
 async start(row,manifest,first){state.request=state.request||requestId();return control('/api/start',{inputs:[row.id],request_id:state.request,manifest,first_item:first});},
 async append(items,token){return api('/api/intake/append',{method:'POST',body:{...token,items},timeoutMs:30000});},
 async capacity(token){const current=await api('/api/intake/'+encodeURIComponent(token.batch_id)+'?limit=1',{timeoutMs:15000});const s=current.summary;if(!s||s.epoch!==token.epoch||s.generation!==token.generation)throw Object.assign(new Error('本批准备授权已变化，请刷新后明确继续'),{retryable:false});if(s.deadline&&Date.now()/1000>=s.deadline)throw Object.assign(new Error('本批执行授权已到期，请明确继续'),{retryable:false});return s.unfinished_graphs<s.preparing_limit;},
 onWait(error,attempt){$('#upload-state').textContent=`准备连接暂时缓慢，正在第 ${attempt} 次重试；已准备的材料仍在后台处理。`;},
 onChange(){if(state.batch&&state.snapshot?.latest_batch_id!==state.batch)return;if(!intakeQueue.entries.length&&state.inputListing){renderInputs();return;}state.inputListing=null;state.inputs=intakeQueue.entries;state.selected=new Set(state.inputs.filter(r=>r.selected&&r.state!=='skipped').map(r=>r.id));renderInputs();}
});
async function loadBatchInputs(){
 const batch=state.snapshot?.batch_id;if(!batch)return;
 const result=await api('/api/intake/'+encodeURIComponent(batch)+'?offset='+state.inputPage*20+'&limit=20&grouped=true');
 if(result.summary){state.inputListing=result;state.inputs=result.items.map(r=>({...r,selected:true}));state.serverInputs=state.inputs;renderInputs();renderProgressTables();}
 else{state.inputs=[];state.selected.clear();renderInputs();}
}
async function matchPendingBatch(){
 const s=state.rawSnapshot;if(!s?.intake||(s.intake.pending===0&&s.intake.failed===0))return false;
 const all=[];for(let offset=0;offset<s.intake.total;offset+=200){const page=await api('/api/intake/'+encodeURIComponent(s.batch_id)+'?offset='+offset+'&limit=200');all.push(...page.items);}
 const remaining=new Map(all.map(r=>[JSON.stringify([r.name,r.relative_path,r.bytes]),r]));
 if(intakeQueue.entries.length!==all.length)throw new Error('当前批次尚未完成；请重新选择原来的完整文件夹后继续，不能替换清单');
 for(const row of intakeQueue.entries){const old=remaining.get(JSON.stringify([row.name,row.relative_path,row.bytes]));if(!old)throw new Error('所选文件夹与当前批次清单不一致');row.id=old.id;row.state=old.state;row.error=old.error;if(old.input_id){row.input={id:old.input_id};row.preparation_request=s.intake.request_id;}remaining.delete(JSON.stringify([row.name,row.relative_path,row.bytes]));}
 state.draft=false;state.batch=s.batch_id;state.snapshot=s;intakeQueue.token={batch_id:s.batch_id,epoch:s.intake.epoch,generation:s.intake.generation,request_id:s.intake.request_id};renderInputs();render();
 $('#upload-state').textContent='已匹配本批清单；点击“继续当前批次”才会准备剩余文件。';return true;
}
async function pumpInputs(){try{await intakeQueue.pump();await refresh();}catch(e){notice('文件准备暂时中断：'+e.message+'。后台已准备任务继续；可点击“继续准备文件”，暂停状态下点击“继续当前批次”。');}finally{controls();}}
$('#login-form').onsubmit=event=>{event.preventDefault();run(async()=>{const data=Object.fromEntries(new FormData(event.target));try{const result=await api('/api/login',{method:'POST',body:data});state.csrf=result.csrf;event.target.password.value='';await initialize();$('#login-error').textContent='';}catch(e){$('#login-error').textContent=e.message;}});};
$('#new-batch').onclick=()=>run(prepareNewBatch);
$('#logout').onclick=()=>run(async()=>{await api('/api/logout',{method:'POST'});showLogin();});
document.querySelectorAll('[data-page]').forEach(b=>b.onclick=()=>showPage(b.dataset.page));
$('#choose-files').onclick=()=>$('#file-picker').click();
$('#choose-folder').onclick=()=>$('#folder-picker').click();
async function prepareFiles(picker){
 if(state.busy||intakeQueue.running||intakeQueue.pumping)throw new Error('当前仍在准备，请先暂停');
 beginDraft();intakeQueue.chooseFiles(picker.files,state.snapshot?.settings.supported_formats);picker.value='';
 await matchPendingBatch();render();renderInputs();$('#input-section').open=true;
 if(state.draft)$('#upload-state').textContent=`已读取总清单 ${state.inputs.length} 份，尚未上传。点击开始后边准备边处理。`;
}
$('#file-picker').onchange=()=>run(()=>prepareFiles($('#file-picker')));
$('#folder-picker').onchange=()=>run(()=>prepareFiles($('#folder-picker')));
$('#select-all').onchange=()=>{for(const row of state.inputs)row.selected=row.state!=='skipped'&&$('#select-all').checked;state.selected=new Set(state.inputs.filter(r=>r.selected).map(r=>r.id));state.request=null;renderInputs();};
$('#input-prev').onclick=()=>run(async()=>{state.inputPage--;if(state.inputListing)await loadBatchInputs();else renderInputs();});$('#input-next').onclick=()=>run(async()=>{state.inputPage++;if(state.inputListing)await loadBatchInputs();else renderInputs();});
$('#start').onclick=()=>run(async()=>{state.busy=true;state.request=state.request||requestId();controls();try{await intakeQueue.start();state.request=null;}finally{state.busy=false;controls();}void pumpInputs();});
$('#pause').onclick=()=>run(async()=>{intakeQueue.pause();await control('/api/pause');});
$('#pause-global').onclick=$('#pause').onclick;
$('#continue').onclick=()=>run(async()=>{
 const localSources=intakeQueue.entries.some(r=>r.file||r.nas);
 state.busy=true;controls();try{
  await refresh();const manager=state.snapshot?.manager;let result;
  if(manager?.state==='running'){
   const listing=await api('/api/intake/'+encodeURIComponent(state.snapshot.batch_id)+'?limit=1',{timeoutMs:15000});
   if(!listing.summary||listing.summary.epoch!==manager.epoch||listing.summary.generation!==manager.generation)throw new Error('准备授权已变化，请暂停并刷新后再继续');
   result={manager,intake:listing.summary};
  }else result=await control('/api/continue',{batch_id:state.snapshot?.batch_id});
  if(state.snapshot?.manager?.state!=='running'){notice('当前仍处于暂停状态，任务未继续；请刷新后确认。');return;}
  if(localSources)intakeQueue.resume({...result,batch_id:state.snapshot?.batch_id});
  else notice((state.snapshot?.intake?.pending||state.snapshot?.intake?.failed)?'已准备的任务已继续。剩余原稿需由后台准备服务接续；通过浏览器补传时，请重新选择原文件夹。':'已继续当前批次，后台正在接续已准备的任务。');
 }finally{state.busy=false;controls();}if(localSources)void pumpInputs();
});
$('#current-batch').onclick=()=>run(async()=>{state.draft=false;state.batch=null;await refresh();await loadBatchInputs();});
for(const scope of ['article','process']){ $('#'+scope+'-prev').onclick=()=>{state.progressPages[scope]--;renderProgressTables();};$('#'+scope+'-next').onclick=()=>{state.progressPages[scope]++;renderProgressTables();};}
$('#refresh').onclick=()=>run(refresh);
for(const id of ['search','process-search']){
 const input=$('#'+id),peer=$('#'+(id==='search'?'process-search':'search'));
 let composing=false;
 const sync=()=>{state.progressSearch=input.value;if(peer.value!==input.value)peer.value=input.value;state.progressPages={article:0,process:0};renderProgressTables();};
 input.oncompositionstart=()=>{composing=true;};
 input.oncompositionend=()=>{composing=false;sync();};
 input.oninput=event=>{if(!composing&&!event?.isComposing)sync();};
}
for(const scope of ['article','process'])$('#'+scope+'-status-filter').onchange=()=>setProgressFilter($('#'+scope+'-status-filter').value);
document.querySelectorAll('[data-progress-filter]').forEach(button=>button.onclick=()=>{
 state.progressSearch='';$('#search').value='';$('#process-search').value='';
 setProgressFilter(button.dataset.progressFilter);showPage('processes');$('#processes-page').scrollIntoView({behavior:'smooth',block:'start'});
});
$('#preparation-overview').onclick=()=>{$('#input-section').open=true;$('#input-section').scrollIntoView({behavior:'smooth',block:'start'});};
$('#completed-overview').onclick=()=>showPage('completed');
$('#notice-close').onclick=()=>$('#notice').hidden=true;$('#close-detail').onclick=()=>$('#detail-dialog').close();
PaperflowCompleted.init({api,esc,duration,date,showDetail,notice});
PaperflowNas.init({api,esc,notice,
 onBusy(value){state.busy=value;controls();},
 async onSelected(rows){beginDraft();intakeQueue.chooseNas(rows,state.snapshot?.settings.supported_formats);await matchPendingBatch();render();renderInputs();$('#input-section').open=true;$('#upload-state').textContent='已读取 NAS 总清单，尚未复制；点击开始后边准备边处理。';},
 onPrepared(text){$('#upload-state').textContent=text;$('#input-section').open=true;}
});
initialize().catch(()=>showLogin());
async function poll(){try{if(state.loggedIn)await refreshControl();}catch{}finally{setTimeout(poll,1500);}}setTimeout(poll,1500);

async function pollServerMemory(){
 try{if(state.loggedIn)state.serverMemory=await api('/api/resources/server');}
 catch{state.serverMemory=null;}
 finally{if(state.loggedIn)renderResources();setTimeout(pollServerMemory,5000);}
}setTimeout(pollServerMemory,1000);
