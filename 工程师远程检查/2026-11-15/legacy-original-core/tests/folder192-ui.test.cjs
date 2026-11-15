const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const staticRoot=path.join(__dirname,'../webworkbench/static');

function appFixture(){
 const elements=new Map();
 const el=id=>{if(!elements.has(id))elements.set(id,{value:'',innerHTML:'',textContent:'',hidden:false,dataset:{},classList:{toggle(){}},querySelectorAll:()=>[]});return elements.get(id);};
 const context=vm.createContext({document:{querySelector:el,querySelectorAll:()=>[]},PaperflowIntake:require('../webworkbench/static/intake.js'),Blob,AbortController,setTimeout,clearTimeout,URLSearchParams});
 const source=fs.readFileSync(path.join(staticRoot,'app.js'),'utf8');
 vm.runInContext(source.slice(0,source.indexOf("$('#login-form').onsubmit")),context);
 vm.runInContext(source.slice(source.indexOf('async function prepareFiles('),source.indexOf("$('#file-picker').onchange")),context);
 vm.runInContext("state.snapshot={manager:{state:'paused',epoch:'isolated'},settings:{selected_limit:null,article_workers:192,llm_workers:192,supported_formats:['.txt','.zip','.pdf']},jobs:[]};state.online=true;state.controlReachable=true;state.draft=true;render=()=>{};",context);
 return {context,elements,el};
}
function file(name,text,relative){return Object.assign(new Blob([text]),{name,webkitRelativePath:relative});}

test('whole local folder previews nested paths over 10 without uploading and shows only 20 rows',async()=>{
 const {context,el}=appFixture(),calls=[],rows=[],hashes=new Map();
 context.stub=async (url,options={})=>{
  calls.push({url,method:options.method||'GET'});
  if(url==='/api/inputs')return rows;
  const q=new URLSearchParams(url.split('?')[1]),content=await options.body.text();
  if(hashes.has(content))return hashes.get(content);
  const row={id:String(rows.length),name:q.get('name'),relative_path:q.get('relative_path'),bytes:options.body.size,created:1};
  hashes.set(content,row);rows.push(row);return row;
 };
 const files=Array.from({length:270},(_,n)=>file('正文.txt',String(n),`选稿/子目录${n}/正文.txt`));
 files.push(file('重复.txt','0','选稿/副本/重复.txt'),file('忽略.exe','skip','选稿/忽略.exe'));
 context.picker={files,value:'folder'};vm.runInContext('api=stub',context);
 await vm.runInContext('prepareFiles(picker)',context);
 assert.equal(vm.runInContext('state.selected.size',context),271);
 assert.equal(rows.length,0);assert.equal(el('#start').disabled,false);
 assert.equal((el('#input-body').innerHTML.match(/<tr>/g)||[]).length,20);
 vm.runInContext('state.inputPage=13;renderInputs()',context);
 assert.match(el('#input-body').innerHTML,/选稿\/子目录269\/正文.txt/);
 assert.match(el('#upload-state').textContent,/尚未上传/);
 assert.equal(calls.length,0);
 assert.equal(context.picker.value,'');
});

test('explicit legacy deployment limit still cannot accidentally grant a large batch',()=>{
 const {context,el}=appFixture();vm.runInContext('state.snapshot.settings.selected_limit=10;state.selected=new Set(Array.from({length:11},(_,n)=>String(n)));controls();',context);
 assert.equal(el('#start').disabled,true);
 assert.equal(el('#pause').disabled,false);
});

test('preview includes empty and unsupported files with explicit skip state',async()=>{
 const {context,el}=appFixture();let n=0;
 context.stub=async url=>{if(url==='/api/inputs')return [{id:'first',name:'a.txt',bytes:1,created:1}];if(n++)throw new Error('synthetic disconnect');return {id:'first'};};
 context.picker={files:[file('a.txt','a','folder/a.txt'),file('b.txt','','folder/b.txt'),file('c.zz','x','folder/c.zz')],value:'folder'};
 vm.runInContext('api=stub',context);
 await vm.runInContext('prepareFiles(picker)',context);
 assert.equal(vm.runInContext('state.selected.size',context),1);assert.match(el('#input-body').innerHTML,/a.txt/);
 assert.match(el('#input-body').innerHTML,/空文件/);assert.match(el('#input-body').innerHTML,/格式暂不支持/);assert.equal(vm.runInContext('state.busy',context),false);
});

test('restored manifest keeps attachment grouping when continuing without local file handles',()=>{
 const {context}=appFixture();
 vm.runInContext("state.inputListing={main_total:1,summary:{total:2}};state.inputs=[{id:'body',name:'paper.txt',relative_path:'f/paper.txt',bytes:1,state:'ready',task_id:'body',attachments:[{id:'drawing',name:'drawing.dwg',state:'ready',task_id:'drawing'}]}];intakeQueue.resume({batch_id:'batch',manager:{epoch:'e',generation:3,state:'running'}})",context);
 assert.equal(vm.runInContext('state.inputs.length',context),1);
 assert.equal(vm.runInContext("groupProgress([{task_id:'body'},{task_id:'drawing'}]).length",context),1);
});

function nasFixture(){
 const rows=new Map();
 const el=id=>{if(!rows.has(id))rows.set(id,{value:'',dataset:{},classList:{toggle(){}},querySelectorAll:()=>[],showModal(){},close(){},addEventListener(){}});return rows.get(id);};
 const context=vm.createContext({document:{getElementById:el},URLSearchParams});
 vm.runInContext(fs.readFileSync(path.join(staticRoot,'nas.js'),'utf8'),context);
 const imported=[],calls=[],tree={
  '':[{name:'管理学',path:'管理学',kind:'directory',bytes:null}],
  '管理学':Array.from({length:205},(_,n)=>({name:`正文${n}.txt`,path:`管理学/正文${n}.txt`,kind:'file',bytes:4,version:`v${n}`})).concat([{name:'附件',path:'管理学/附件',kind:'directory',bytes:null}]),
  '管理学/附件':[{name:'程序.zip',path:'管理学/附件/程序.zip',kind:'file',bytes:9,version:'vzip'}]
 };
 context.callbacks={esc:String,onBusy(){},onPrepared(){},onRefresh:async()=>{},onImported:r=>imported.push(r),notice(){},api:async(url,options={})=>{
  calls.push({url,method:options.method||'GET'});
  if(url==='/api/nas/roots')return {roots:[{id:'isolated',name:'隔离 NAS'}]};
  if(url==='/api/inputs/nas')return {id:options.body.path};
  const q=new URLSearchParams(url.split('?')[1]),p=q.get('path'),offset=Number(q.get('offset'));
  return {path:p,entries:tree[p].slice(offset,offset+100),next_offset:offset+100<tree[p].length?offset+100:null};
 }};
 vm.runInContext('PaperflowNas.init(callbacks)',context);
 return {context,el,calls,imported};
}

test('NAS whole folder follows all pages and subfolders, imports each file, never starts',async()=>{
 const {el,calls,imported}=nasFixture();await el('choose-nas').onclick();
 await el('nas-select-tree').onclick();
 assert.match(el('nas-selected-caption').textContent,/206 个文件/);
 assert.equal(imported.length,0);
 await el('nas-import').onclick();assert.equal(imported.length,206);
 assert.ok(imported.some(r=>r.id==='管理学/附件/程序.zip'));
 assert.ok(calls.every(c=>c.method==='GET'||c.url==='/api/inputs/nas'));
 assert.equal(el('nas-import').disabled,true);
});

test('NAS cancelled scan preserves selected files and does not continue unseen folders',async()=>{
 const {el,context,calls}=nasFixture();await el('choose-nas').onclick();
 const original=context.callbacks.api;
 context.callbacks.api=async(url,options)=>{const result=await original(url,options);if(url.includes('path=%E7%AE%A1%E7%90%86%E5%AD%A6'))el('nas-cancel-scan').onclick();return result;};
 await el('nas-select-tree').onclick();
 assert.match(el('nas-state').textContent,/已停止扫描/);
 assert.ok(calls.every(c=>!c.url.includes('path=%E7%AE%A1%E7%90%86%E5%AD%A6%2F')));
});

test('NAS new selection callback only builds this batch manifest without copying files',async()=>{
 const {el,context,calls,imported}=nasFixture();let chosen;
 context.callbacks.onSelected=async rows=>chosen=rows;
 await el('choose-nas').onclick();await el('nas-select-tree').onclick();await el('nas-import').onclick();
 assert.equal(chosen.length,206);assert.equal(imported.length,0);
 assert.ok(calls.every(c=>c.method==='GET'));
});
