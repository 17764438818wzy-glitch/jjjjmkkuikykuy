/* Read-only wording over existing workflow facts; never sends control requests. */
(function(root){
 'use strict';
 const modelStages={round1:'第一轮：LLM 正在分类、生成画像',round2:'第二轮：LLM 正在复核、纠正',round3_assets:'第三轮：LLM 正在整理图表',round3_redact:'第三轮：LLM 正在去除敏感信息',round3_chunk:'第三轮：LLM 正在把正文切成检索用的小段',repair:'LLM 正在修正之前发现的问题'};
 const indexes={round1:1,round2:2,round3:3,round3_assets:3,round3_redact:3,round3_chunk:3,seal:4,handoff:4,handed_off:5,attachment_handoff:4,attachment_ready:5,publish:4,validate:4,ingest:5,ingest_target:5};
 const operationText={prepare_cache:'正在整理本轮成果',cache_commit:'正在保存本轮成果，方便下次接着处理',handoff_commit:'正在把入库包交给投递队列',attachment_handoff:'正在把附件交给投递队列',seal:'正在打包正文、画像、目录和图片',seal_attachment:'正在打包附件原件和说明',correction_count:'正在核对本篇已用的纠正次数',correction_reserve:'正在登记这次纠正',verify_repair_input:'正在核对需要修复的材料',verify_checkpoint_outputs:'正在检查已有成果能否继续使用',load:'正在读取准备入库的材料',read_start:'正在核对已保存的内容',reconcile:'正在核对上次入库请求的结果',http:'正在入库并核对保存结果',attachment_http:'正在保存附件和说明，并核对结果',attachment_verify:'正在核对附件原件是否完整',switch:'正在确认这版内容可以使用',save:'正在保存入库进度',attachment_complete:'正在登记附件保存结果'};
 function problem(a){
  if(a.complete===true||a.state==='excluded')return null;
  const routeHeld=(a.routes||[]).some(r=>r.held===true);
  if(!['held','configuration_pending'].includes(a.state)&&!routeHeld)return null;
  const reason=typeof a.reason==='string'?a.reason.trim():'';
  if(a.state==='configuration_pending')return {title:'归档配置待补',detail:'当前材料尚未确认保存位置。',next:'查看目标库信息，补齐对应专业或附件的归档配置后再继续。',reason};
  if(/\bModelRequestError\b/.test(reason))return {title:'模型请求失败',detail:'模型调用未完成；这条错误未说明具体原因。',next:'查看执行记录中的服务返回或错误信息，确认原因后再处理该失败步骤。',reason};
  if(/\b(?:TimeoutError|ReadTimeout|ConnectTimeout)\b/.test(reason))return {title:'请求等待超时',detail:'该次请求未在规定时间内完成，原因尚需核对。',next:'查看失败步骤和服务返回；涉及入库时先核对保存结果。',reason};
  return {title:routeHeld&&a.state!=='held'?'部分专业投递遇到问题':'处理遇到问题',detail:reason||'台账尚未提供具体原因。',next:'展开详情查看失败步骤和执行记录，按已确认的原因处理。',reason};
 }
 function stepIndex(stage,action){
  if(['prepare:round1','cache_lookup:round1'].includes(action))return 0;
  if(action==='seal_attachment'||action==='seal')return 4;
  const parts=String(action||'').split(':');
  if(stage==='repair'||parts.includes('repair'))return null;
  if(!action&&stage==='round1')return 0;
  if(parts[1] in indexes)return indexes[parts[1]];
  return indexes[parts[0]]??indexes[stage]??null;
 }
 function describe(a,s,online=true){
  const local=(s.processes||[]).filter(p=>p.task_id===a.task_id);
  const active=local.find(p=>p.state==='running'&&!p.exit_confirmed);
  const remote=(s.remote_processes||[]).find(p=>p.task_id===a.task_id&&!p.exit_confirmed);
  const action=a.current_action||active?.action||'';
  const phase=String(action).split(':')[0];
  let index=stepIndex(a.stage,action),title,detail='',tone='queued',location='等待处理';
  const repairing=(a.recent_attempts||[]).some(attempt=>attempt.kind==='repair'&&attempt.finished==null);
  const explicitPhase=String(action).split(':').some(part=>Object.hasOwn(indexes,part))||action==='seal_attachment';
  if(repairing&&!explicitPhase)index=null;
  const isModel=phase in modelStages;
  if(active)location=active.kind==='delivery'?'电脑投递 → RAGFlow':isModel?(s.settings.harness?.location==='server'?'服务器调用 LLM':'电脑调用 LLM'):'电脑';
  if(!online){title='详情未更新，当前进度待确认';detail='下方保留上次采样的步骤；刷新详情后确认当前进度。';tone='unknown';}
  else if(a.complete===true){title=a.attachment?'附件原件和说明已保存并确认':'已入库，保存结果已确认';tone='complete';index=6;location='已完成';}
  else if(a.state==='excluded'){title='已按规则排除';detail='这份材料不继续进入正文入库流程。';tone='excluded';index=1;}
  else if(a.state==='pending_reconcile'){title='等待核对上次入库的结果';detail='核对清楚后才会继续，避免重复保存。'+(s.manager.state==='paused'?'新任务当前已暂停。':'');tone='pending_reconcile';}
  else if(problem(a)){const issue=problem(a);title=issue.title;detail=issue.detail+' '+issue.next;tone='held';}
  else if(s.manager.state==='pausing'){title='正在暂停，等待已发出的操作结束';detail='已停止安排新动作，正在确认相关进程退出。';tone='pausing';}
  else if(s.manager.state==='paused'){title='已暂停，等待你点击继续';detail=(a.state==='pending_delivery'?'已处理到入库阶段；':'')+'进度已保留，下方标出暂停位置。';tone='paused';}
  else if(remote?.state==='unknown'&&isModel){title='服务器响应待确认';detail='正在等待连接和任务状态的确认。';tone='unknown';}
  else if(active){
   tone='processing';
   if(phase==='prepare'){title=action==='prepare:round1'?'正在读取原稿，准备给 LLM 的材料':'正在准备这一轮需要的文件';}
   else if(phase==='cache_lookup')title='正在检查有没有可复用的处理结果';
   else if(phase==='harness_setup')title='正在准备 LLM 连接';
   else if(phase==='syntax_check'||phase==='validate')title='正在检查 LLM 返回的成果是否可用';
   else if(phase==='delivery_diagnostic')title='正在整理需要 LLM 修正的问题';
   else if(active.kind==='cleanup')title='正在整理临时文件和旧材料';
   else title=modelStages[phase]||operationText[phase]||(active.kind==='delivery'?'正在入库并核对保存结果':'程序正在衔接下一步');
  }else if(s.resources?.reasons?.includes('memory'))title='等待电脑腾出足够的内存';
  else if(s.resources?.reasons?.includes('disk'))title='等待电脑有足够的磁盘空间';
  else if((s.services||[]).some(v=>v.kind===(index===5?'rag':'llm')&&v.state==='open'))title=index===5?'等待 RAGFlow 服务恢复':'等待模型服务恢复';
  else if(a.state==='pending_delivery'||index===5){title='处理已完成，正在排队等待入库';detail='轮到这篇后，程序会自动投递。';}
  else title='正在排队，等待下一步处理';
  const attachment=!!a.attachment;
  const labels=attachment?['读取材料','识别附件、生成说明','正文复核（独立附件无需此步）','正文图表、脱敏、切片（独立附件无需此步）','整理附件入库包','保存原件和说明，并确认完整']:['读取原稿','第一轮：分类、生成画像','第二轮：复核、纠正','第三轮：整理图表 → 脱敏 → 切片','整理成入库包','上传到 RAGFlow，并确认保存成功'];
  const steps=labels.map((label,i)=>{
   let state=index===6||index!==null&&i<index?'done':i===index?'current':'pending';
   if(tone==='excluded'&&i<=1)state='done';
   if(attachment&&i>=2&&i<=3||tone==='excluded'&&i>1)state='skipped';
   let caption={done:'已完成',current:'当前步骤',pending:'等待前序步骤',skipped:'无需执行'}[state];
   if(state==='current')caption=!online?'上次记录':tone==='paused'?'暂停位置':tone==='pausing'?'正在收束':tone==='held'?'需处理问题':tone==='unknown'?'状态待确认':tone==='processing'?'正在进行':'等待继续';
   return {label,state,caption};
  });
  if(index===3&&tone==='processing')detail=title.includes('图表')?'第三轮：先整理图表，再脱敏，最后切片。':detail;
  if(index===null&&tone==='processing'&&(phase==='repair'||String(action).split(':').includes('repair')))detail='正在修复之前发现的问题；本次具体步骤待确认，不沿用旧版的完成标记。';
  return {title,detail,tone,location,index,steps,action,local,remote};
 }
 const api={describe,problem};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 else root.PaperflowProgress=api;
})(typeof globalThis!=='undefined'?globalThis:this);
