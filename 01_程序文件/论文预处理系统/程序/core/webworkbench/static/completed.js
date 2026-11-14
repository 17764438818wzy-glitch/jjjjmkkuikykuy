'use strict';
// Display only: the existing authenticated GET API and article dialog are reused.
const PaperflowCompleted=(()=>{
 let ui,items=[],cursor=null,query='',serial=0,loaded=false,updatedAt=null;
 const el=id=>document.getElementById(id);
 function imageSummary(value){
  if(value.state==='not_applicable')return '独立附件';
  if(value.state==='unavailable')return '图片清单待核对';
  if(value.state==='unconfirmed')return `图片保存方式待核对 · 清单 ${value.inventory_items} 项`;
  if(value.state!=='checked')return '图片未检查';
  return value.retained||value.described||value.omitted?`原图 ${value.retained} · 文字图表 ${value.described} · 省略 ${value.omitted}`:'已检查，无图片或图表';
 }
 const attachmentSummary=value=>value.state==='registered'?`附件 ${value.count} 份`:'附件关联未检查';
 function render(){
  const {esc,date,duration}=ui;
  el('completed-body').innerHTML=items.map(a=>`<tr><td><strong>${esc(a.name)}</strong><small class="secondary">${a.attachment?'独立附件':a.state==='excluded'?'已按规则排除':'文章'}</small></td><td>${a.routes.length?a.routes.map(r=>`<span class="completed-route">${esc(r.code)} ${esc(r.name)}</span>`).join(''):a.attachment?'资料库 '+esc(a.attachment.target.dataset_id||'未记录'):'未分配专业'}</td><td><span class="status-badge">${a.complete?'已确认入库':'已排除'}</span><small class="secondary">${a.completed_at?esc(date(a.completed_at)):'完成时间未记录'}</small></td><td>${duration(a.elapsed_seconds)}<small class="secondary">入库 ${duration(a.delivery_seconds)}</small></td><td>${esc(imageSummary(a.image_summary))}<small class="secondary">${esc(attachmentSummary(a.attachment_summary))}</small></td><td><button data-completed-detail="${esc(a.task_id)}" aria-label="查看 ${esc(a.name)} 的专业、图片和附件">查看详情</button></td></tr>`).join('');
  el('completed-empty').hidden=!!items.length||!!cursor;
  el('completed-caption').textContent=`${query?'搜索“'+query+'” · ':''}跨批次已加载 ${items.length} 条完成记录${cursor?'，可继续加载':''}；更新于 ${date(updatedAt)}，以实际投递台账为准。`;
  el('completed-more').hidden=!cursor;
  el('completed-body').querySelectorAll('[data-completed-detail]').forEach(button=>button.onclick=async()=>{
   button.disabled=true;button.textContent='正在读取…';
   try{ui.showDetail(await ui.api('/api/completed/'+encodeURIComponent(button.dataset.completedDetail),{timeoutMs:15000}));}
   catch(error){ui.notice('这份稿子的详情暂时无法读取：'+error.message+'。请再次点击“查看详情”重试。');}
   finally{button.disabled=false;button.textContent='查看详情';}
  });
 }
 async function load(reset=true){
  const token=++serial,requestedQuery=reset?el('completed-search').value.trim():query;
  el('completed-state').textContent=loaded?'正在更新完成记录，暂时显示上次结果…':'正在读取完成记录…';
  el('completed-more').disabled=true;el('completed-refresh').disabled=true;
  const params=new URLSearchParams({query:requestedQuery,limit:'20'});if(!reset&&cursor)params.set('cursor',cursor);
  try{
   const value=await ui.api('/api/completed?'+params,{timeoutMs:15000});
   if(token!==serial)return;
   items=reset?value.items:[...items,...value.items.filter(a=>!items.some(old=>old.task_id===a.task_id))];
   cursor=value.next_cursor;query=requestedQuery;loaded=true;updatedAt=Date.now()/1000;render();el('completed-state').textContent='';
  }catch(error){if(token===serial)el('completed-state').textContent='完成记录列表暂时无法更新，'+(loaded?'保留上次列表（更新于 '+ui.date(updatedAt)+'）':'尚未读到结果')+'：'+error.message+'。请点击“刷新完成记录”重试。';}
  finally{if(token===serial){el('completed-more').disabled=false;el('completed-refresh').disabled=false;}}
 }
 function details(article){
  const {esc}=ui,details=article.completed_details;
  const field=(key,value)=>`<div><dt>${esc(key)}</dt><dd>${esc(value??'未记录')}</dd></div>`;
  let html='<h3>图片与图表详情</h3>';
  if(article.image_summary.state==='not_applicable')html+='<p class="muted">这是独立附件，原件信息见下方。</p>';
  else if(!details.image_versions.length)html+='<p class="muted">尚未检查图片清单，不能确认有无图片。</p>';
  for(const version of details.image_versions){
   const names=version.codes.map(c=>article.routes.find(r=>r.code===c)?.name||c).join('、');
   html+=`<p class="completed-version">对应专业：${esc(names)}</p>`;
   if(version.state!=='checked'){html+='<p class="muted">图片清单缺失、无法读取或与封包不一致，数量及详情待核对。</p>';continue;}
   if(!version.items.length){html+='<p class="muted">已读取最终图片清单：没有图片或图表。</p>';continue;}
   html+=version.items.map(a=>{
    const declared={image:'保留原图',description:'文字描述',omit:'省略'}[a.declared_storage]||'未识别方式';
    const mode={image:'保留原图',description:'以文字保存',omit:'已省略',unknown:`保存方式待核对（模型标记：${declared}）`}[a.storage];
    const where=a.destinations.map(d=>`${d.name} · ${d.dataset_id||'目标库未记录'}${d.document_id?' · 文档 '+d.document_id:''}${d.image_id?' · 图片 '+d.image_id:''}`).join('\n');
    return `<details class="completed-asset"><summary>${esc(a.caption||a.filename||a.id||'图表')} <span>${esc(mode)}</span></summary><dl class="detail-facts">${field('文件名',a.filename||(a.storage==='unknown'?'原图文件未确认':'没有单独原图文件'))}${field('处理方式',mode)}${field('说明',a.description)}${field('原文位置',a.source_location?JSON.stringify(a.source_location):null)}${field('章节 / 正文锚点',[a.section_id,a.body_anchor].filter(Boolean).join(' · ')||null)}${field(a.storage==='unknown'?'投递回执记录位置（不代表原图已保存）':'保存位置',where||(a.storage==='omit'?'已省略，未投递':'投递位置未记录'))}${a.sha256?field('封包原图 SHA256',a.sha256):''}</dl></details>`;
   }).join('');
  }
  html+='<h3>附件原件与正文关联</h3>';
  if(details.attachments.state==='not_checked'){
   html+='<p class="muted">附件关联未检查。当前按文件处理，未扫描同文件夹；没有关联记录不代表没有附件。独立处理的图纸、程序会作为各自的完成记录显示。</p>';
  }else for(const a of details.attachments.items){
   const at=a.association,confirmed=at.status==='confirmed'&&!!at.body_document_id;
   const raw=a.remote.raw||a.remote;
   html+=`<details class="completed-asset" open><summary>${esc(a.filename)} <span>${a.kind==='drawing'?'图纸':'程序'}</span></summary><dl class="detail-facts">${field('文件类型',a.filename.split('.').pop()?.toUpperCase())}${field('原件大小',a.bytes==null?null:a.bytes+' 字节')}${field('说明',a.description)}${field('原件完整读回',a.verification==='original_and_note_readback'?'已通过必要读回':'尚待确认')}${field('资料库',a.target.dataset_id)}${field('原件文件标识',raw.file_id)}${field('原件归档目录',a.target.parent_id)}${field('原件 SHA256',a.sha256)}${field('相关论文标题线索',at.related_paper_title)}${field('正文关联',confirmed?'已确认关联文档 '+at.body_document_id:'待确认；标题线索不代表已经关联')}${field('说明检索抽检',a.retrieval==='not_checked'?'未抽检':a.retrieval)}</dl></details>`;
  }
  return html;
 }
 return {
  init(options){ui=options;el('completed-refresh').onclick=()=>load();el('completed-more').onclick=()=>load(false);el('completed-filter').onsubmit=event=>{event.preventDefault();load();};},
  open(){load();},load,details,imageSummary
 };
})();
