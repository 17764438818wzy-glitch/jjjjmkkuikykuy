(function(root){
 'use strict';
 const bodyFormats=new Set(['.pdf','.doc','.docx','.docm','.wps','.rtf','.odt','.txt','.md']);
 const extension=name=>'.'+name.split('.').pop().toLowerCase();
 const parent=row=>(row.relative_path||row.name).split('/').slice(0,-1).join('/');
 function grouped(entries){
  const folders=new Map();
  for(const r of entries){const p=parent(r);if(!folders.has(p))folders.set(p,[]);folders.get(p).push(r);}
  const hidden=new Set(),attachments=new Map();
  for(const rows of folders.values()){
   const bodies=rows.filter(r=>bodyFormats.has(extension(r.name))&&r.state!=='skipped');
   const extras=rows.filter(r=>!bodyFormats.has(extension(r.name))&&r.state!=='skipped');
   if(bodies.length===1&&extras.length){attachments.set(bodies[0].id,extras);extras.forEach(r=>hidden.add(r.id));}
  }
  return entries.filter(r=>!hidden.has(r.id)).map(r=>({...r,attachments:attachments.get(r.id)||[]}));
 }
 function page(entries,index=0){
  const all=grouped(entries),pages=Math.max(1,Math.ceil(all.length/20));
  index=Math.max(0,Math.min(index,pages-1));
  return {rows:all.slice(index*20,index*20+20),pages,index,total:entries.length,mainRows:all.length};
 }
 class Queue{
  constructor(host){this.host=host;this.entries=[];this.running=false;this.token=null;this.pumping=null;this.error=null;}
  changed(){this.host.onChange?.();}
  chooseFiles(files,formats){
   if(this.running||this.pumping)throw new Error('当前批次还在准备，请先暂停');
   this.entries=Array.from(files,(file,i)=>this.entry({id:'file-'+i,name:file.name,relative_path:file.webkitRelativePath||file.name,bytes:file.size,file},formats));
   this.token=null;this.changed();
  }
  chooseNas(rows,formats){
   if(this.running||this.pumping)throw new Error('当前批次还在准备，请先暂停');
   this.entries=rows.map((r,i)=>this.entry({id:'nas-'+i,name:r.name,relative_path:r.path,bytes:r.bytes,nas:{root:r.root,path:r.path,version:r.version}},formats));
   this.token=null;this.changed();
  }
  entry(row,formats){
   const error=formats&&!formats.includes(extension(row.name))?'格式暂不支持':!row.bytes?'空文件':row.bytes>2*1024**3?'超过单文件 2 GiB 上限':'';
   return {...row,state:error?'skipped':'pending',error,selected:!error,attachments:[]};
  }
  selected(){return this.entries.filter(r=>r.selected||r.state==='skipped');}
  manifest(){return this.selected().map(({id,name,relative_path,bytes})=>({id,name,relative_path,bytes}));}
  async prepare(row){
   if(row.input&&(!this.host.canReuse||this.host.canReuse(row)))return row.input;
   row.input=null;
   row.state='preparing';this.changed();
   try{row.input=await this.host.prepare(row);row.state='prepared';return row.input;}
   catch(e){row.state='failed';row.error=e.message;throw e;}
   finally{this.changed();}
  }
  async start(){
   const first=this.selected().find(r=>r.state!=='skipped');if(!first)throw new Error('请先选择可处理的原稿或附件');
   this.entries=this.selected();this.changed();
   this.running=true;
   try{
    const input=await this.prepare(first);
    if(!this.running)throw new Error('已暂停，首份副本已保留；尚未开始处理');
    const result=await this.host.start(input,this.manifest(),first.id);
    first.state='ready';first.task_id=result.task_id;this.resume(result);this.changed();return result;
   }catch(e){this.running=false;this.changed();throw e;}
  }
  pause(){this.running=false;this.changed();}
  resume(result){
   this.token={batch_id:result.batch_id||this.token?.batch_id,epoch:result.manager.epoch,generation:result.manager.generation,request_id:result.intake?.request_id||result.request_id||this.token?.request_id};
   const reopened=new Set(result.reopened_items||[]);
   this.entries.filter(r=>r.state==='skipped'&&reopened.has(r.id)).forEach(r=>{r.state='pending';r.error='';r.selected=true;});
   this.entries.filter(r=>r.state==='failed').forEach(r=>{r.state=r.input?'prepared':'pending';r.error='';});
   this.error=null;this.running=result.manager.state==='running';this.changed();
  }
  async pump(){
   if(this.pumping){await this.pumping;return this.running?this.pump():undefined;}
   const token=this.token;
   const current=()=>this.running&&token===this.token;
   const transient=error=>error.retryable===true||error.retryable!==false&&['AbortError','TimeoutError','TypeError'].includes(error.name);
   const wait=delay=>this.host.wait?.(delay)||new Promise(r=>setTimeout(r,delay));
   const retry=async operation=>{
    for(let attempt=0;current();attempt++){
     try{return await operation();}
     catch(error){
      if(!current())return;
      if(!transient(error)||attempt>=3)throw error;
      this.host.onWait?.(error,attempt+1);await wait(1000*2**attempt);
     }
    }
   };
   const work=async()=>{
    for(const row of this.selected()){
     if(!current())return;if(['ready','skipped','failed'].includes(row.state))continue;
     while(current()&&!await retry(()=>this.host.capacity(token)))if(current())await wait(1000);
     if(!current())return;
     let item;
     try{const input=await retry(()=>this.prepare(row));if(!current())return;item={item_id:row.id,input_id:input.id};}
     catch(e){if(transient(e))throw e;item={item_id:row.id,error:e.message};}
     if(!current())return;
     const result=await retry(()=>this.host.append([item],token));if(!current())return;
     row.task_id=result.items?.find(r=>r.item_id===row.id)?.task_id;row.state=item.error?'failed':'ready';this.changed();
    }
   };
   const pumping=work();this.pumping=pumping;
   try{await pumping;}catch(error){if(token===this.token)this.error=error;throw error;}
   finally{if(token===this.token)this.running=false;if(this.pumping===pumping)this.pumping=null;this.changed();}
  }
 }
 const api={Queue,page,grouped};if(typeof module==='object'&&module.exports)module.exports=api;else root.PaperflowIntake=api;
})(globalThis);
