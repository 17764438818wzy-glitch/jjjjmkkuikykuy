'use strict';
// NAS selection only prepares existing input rows. Starting remains an explicit separate action.
globalThis.PaperflowNas=(()=>{
 let host,model={roots:[],root:'',path:'',entries:[],next:null,selected:new Map(),loading:false,importing:false,scanning:false,cancelScan:false};
 const el=id=>document.getElementById(id);
 const key=(root,path)=>JSON.stringify([root,path]);
 const bytes=n=>n==null?'—':n<1024*1024?`${(n/1024).toFixed(1)} KB`:`${(n/1024/1024).toFixed(2)} MB`;
 function message(text,error=false){el('nas-state').textContent=text;el('nas-state').classList.toggle('error',error);}
 function render(){
  const esc=host.esc,busy=model.loading||model.importing||model.scanning;
  el('nas-root').disabled=busy;el('nas-up').disabled=busy||!model.path;el('nas-refresh').disabled=busy||!model.root;
  el('nas-more').hidden=model.next==null;el('nas-more').disabled=busy;
  el('nas-select-tree').disabled=busy||!model.root;el('nas-cancel-scan').hidden=!model.scanning;
  el('nas-import').disabled=busy||!model.selected.size;el('nas-import').textContent=`加入本次清单（${model.selected.size}）`;
  const root=model.roots.find(r=>r.id===model.root);
  el('nas-path').textContent=root?root.name+(model.path?' / '+model.path:''):'';
  el('nas-body').innerHTML=model.entries.map((row,index)=>`<tr><td>${row.kind==='file'?`<input type="checkbox" data-nas-file="${index}" aria-label="选择 ${esc(row.name)}" ${model.selected.has(key(model.root,row.path))?'checked':''} ${busy?'disabled':''}>`:''}</td><td>${row.kind==='directory'?`<button class="nas-folder" data-nas-folder="${index}" ${busy?'disabled':''}>打开 ${esc(row.name)}</button>`:esc(row.name)}</td><td>${row.kind==='directory'?'文件夹':'文件'}</td><td>${bytes(row.bytes)}</td></tr>`).join('');
  el('nas-empty').hidden=model.entries.length>0||model.loading;
  el('nas-selected').innerHTML=Array.from(model.selected.entries()).map(([id,row],index)=>`<li><span>${esc(row.name)}<small>${esc(row.rootName+' / '+row.path)}</small></span><button data-nas-remove="${index}" aria-label="取消选择 ${esc(row.name)}" ${busy?'disabled':''}>取消</button></li>`).join('');
  el('nas-selected-caption').textContent=`已勾选 ${model.selected.size} 个文件，可选择整个文件夹及子文件夹`;
  el('nas-body').querySelectorAll('[data-nas-folder]').forEach(button=>button.onclick=()=>load(model.entries[Number(button.dataset.nasFolder)].path));
  el('nas-body').querySelectorAll('[data-nas-file]').forEach(input=>input.onchange=()=>{
   const row=model.entries[Number(input.dataset.nasFile)],id=key(model.root,row.path);
   if(input.checked){
    model.selected.set(id,{...row,root:model.root,rootName:root.name});
   }else model.selected.delete(id);
   render();
  });
  el('nas-selected').querySelectorAll('[data-nas-remove]').forEach(button=>button.onclick=()=>{
   model.selected.delete(Array.from(model.selected.keys())[Number(button.dataset.nasRemove)]);render();
  });
 }
 async function load(path='',append=false){
  if(model.loading||model.importing||model.scanning||!model.root)return;
  model.loading=true;if(!append){model.path=path;model.entries=[];model.next=null;}
  message('正在读取 NAS 当前目录…');render();
  try{
   const query=new URLSearchParams({root:model.root,path,offset:String(append?model.next:0)});
   const result=await host.api('/api/nas/browse?'+query,{timeoutMs:20000});
   model.path=result.path;model.entries=append?model.entries.concat(result.entries):result.entries;model.next=result.next_offset;
   let changed=0;
   for(const row of result.entries){
    const id=key(model.root,row.path),selected=model.selected.get(id);
    if(selected&&selected.version!==row.version){model.selected.delete(id);changed++;}
   }
   message(changed?`发现 ${changed} 个文件已经改变，已取消勾选，请重新选择。`:'打开文件夹查看论文，勾选文件后加入待处理。',!!changed);
  }catch(error){message(error.message,true);}
  finally{model.loading=false;render();}
 }
 async function selectTree(){
  if(model.loading||model.importing||model.scanning||!model.root)return;
  model.scanning=true;model.cancelScan=false;host.onBusy(true);render();
  const root=model.root,rootName=model.roots.find(r=>r.id===root).name,pending=[model.path],seen=new Set();let scanned=0;
  try{
   while(pending.length&&!model.cancelScan){
    const path=pending.pop();if(seen.has(path))continue;seen.add(path);
    let offset=0;
    do{
     const result=await host.api('/api/nas/browse?'+new URLSearchParams({root,path,offset:String(offset)}),{timeoutMs:20000});
     if(model.cancelScan)break;
     for(const row of result.entries){
      if(row.kind==='directory')pending.push(row.path);
      else if(row.bytes>0)model.selected.set(key(root,row.path),{...row,root,rootName});
     }
     scanned++;offset=result.next_offset;
     message(`正在扫描文件夹（含子文件夹）；已选择 ${model.selected.size} 个文件，已读取 ${scanned} 页。不会启动处理。`);
    }while(offset!=null&&!model.cancelScan);
   }
   message(`${model.cancelScan?'已停止扫描，保留已选文件':'文件夹扫描完成'}：已选择 ${model.selected.size} 个文件；点击“加入本次清单”，开始后才会准备副本。`);
  }catch(error){message(`扫描未完成：${error.message}；已选文件保留，可重试。`,true);}
  finally{model.scanning=false;host.onBusy(false);render();}
 }
 async function open(){
  if(model.importing||model.scanning)return;
  el('nas-dialog').showModal();message('正在连接 NAS…');
  try{
   const result=await host.api('/api/nas/roots');model.roots=result.roots;
   el('nas-root').innerHTML=model.roots.map(row=>`<option value="${host.esc(row.id)}">${host.esc(row.name)}</option>`).join('');
   if(!model.roots.some(row=>row.id===model.root)){model.root=model.roots[0]?.id||'';model.path='';}
   el('nas-root').value=model.root;
   if(!model.root){message('暂未配置 NAS 论文目录。');render();return;}
   await load(model.path);
  }catch(error){message(error.message,true);}
 }
 async function prepare(){
  if(model.importing||model.scanning||model.loading||!model.selected.size)return;
  if(host.onSelected){model.importing=true;host.onBusy(true);render();try{await host.onSelected(Array.from(model.selected.values()));model.selected.clear();el('nas-dialog').close();}catch(error){message(error.message,true);host.notice(error.message);}finally{model.importing=false;host.onBusy(false);render();}return;}
  model.importing=true;host.onBusy(true);render();
  const chosen=Array.from(model.selected.entries());let completed=0,failure='';
  try{
   for(const [id,row] of chosen){
    const text=`正在准备 NAS 文件 ${completed+1}/${chosen.length}：${row.name}`;
    message(text);host.onPrepared(text);
    try{
     const input=await host.api('/api/inputs/nas',{method:'POST',body:{root:row.root,path:row.path,version:row.version},timeoutMs:600000});
     host.onImported(input);model.selected.delete(id);completed++;
    }catch(error){failure=error.message;break;}
   }
   // This also shows files accepted before a lost HTTP response. No automatic start.
   await host.onRefresh();
   const summary=failure?`已准备 ${completed}/${chosen.length} 个文件；${failure}。剩余文件保留勾选，可刷新后重试。`:`已准备 ${completed} 个 NAS 文件，点击“开始”才会处理。`;
   host.onPrepared(summary);message(summary,!!failure);host.notice(summary);
   if(!failure)el('nas-dialog').close();
  }catch(error){message(error.message,true);host.notice(error.message);}
  finally{model.importing=false;host.onBusy(false);render();}
 }
 function init(callbacks){
  host=callbacks;
  el('choose-nas').onclick=open;el('nas-close').onclick=()=>{model.cancelScan=true;el('nas-dialog').close();};
  el('nas-dialog').addEventListener('cancel',()=>{model.cancelScan=true;});
  el('nas-select-tree').onclick=selectTree;el('nas-cancel-scan').onclick=()=>{model.cancelScan=true;message('正在停止扫描，保留已选文件…');};
  el('nas-root').onchange=()=>{model.root=el('nas-root').value;load('');};
  el('nas-up').onclick=()=>load(model.path.includes('/')?model.path.slice(0,model.path.lastIndexOf('/')):'');
  el('nas-refresh').onclick=()=>load(model.path);
  el('nas-more').onclick=()=>load(model.path,true);el('nas-import').onclick=prepare;
 }
 return {init};
})();
