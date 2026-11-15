const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const Progress=require('../webworkbench/static/progress.js');

function fixture(){
 const elements=new Map();
 const el=id=>{if(!elements.has(id))elements.set(id,{value:'',innerHTML:'',textContent:'',hidden:false,dataset:{},classList:{toggle(){}},querySelectorAll:()=>[],setAttribute(){},scrollIntoView(){},showModal(){this.open=true;}});return elements.get(id);};
 const context=vm.createContext({document:{querySelector:el,querySelectorAll:()=>[],getElementById:id=>el('#'+id)},PaperflowIntake:require('../webworkbench/static/intake.js'),PaperflowProgress:Progress,Blob,AbortController,setTimeout,clearTimeout,URLSearchParams});
 const source=fs.readFileSync(require.resolve('../webworkbench/static/app.js'),'utf8');
 vm.runInContext(source.slice(0,source.indexOf("$('#login-form').onsubmit")),context);
 const articles=[
  {task_id:'error',name:'模型失败稿',state:'held',complete:false,reason:'ModelRequestError',stage:'round1',routes:[]},
  {task_id:'waiting',name:'待入库稿',state:'pending_delivery',complete:false,stage:'handed_off',routes:[]},
  {task_id:'check',name:'待核对稿',state:'pending_reconcile',complete:false,stage:'handed_off',routes:[]},
  {task_id:'queued',name:'排队稿',state:'queued',complete:false,stage:'round1',routes:[]},
  {task_id:'done',name:'完成稿',state:'complete',complete:true,stage:'handed_off',routes:[]},
  {task_id:'excluded',name:'排除稿',state:'excluded',complete:false,stage:'round1',routes:[]}
 ];
 context.sample={batch_id:'batch',latest_batch_id:'batch',observed_at:1700000000,batches:[{id:'batch',count:6}],articles,processes:[],remote_processes:[],jobs:[],manager:{state:'paused',epoch:'e',generation:1,live_units:0},settings:{},intake:{total:100,ready:6,pending:94,failed:0,skipped:0}};
 vm.runInContext('state.snapshot=sample;state.online=true;state.detailsReadable=true;state.controlReachable=true;state.draft=false;',context);
 return {context,el};
}
test('both progress tables filter the same real states and preserve search',()=>{
 const {context,el}=fixture();
 vm.runInContext("setProgressFilter('held');",context);
 assert.match(el('#article-body').innerHTML,/模型失败稿/);
 assert.doesNotMatch(el('#article-body').innerHTML,/待入库稿/);
 assert.equal(el('#process-body').innerHTML.includes('模型失败稿'),true);
 assert.match(el('#article-page-caption').textContent,/1/);
 vm.runInContext("state.progressSearch='待';setProgressFilter('delivery');",context);
 assert.match(el('#process-body').innerHTML,/待入库稿/);
 assert.doesNotMatch(el('#process-body').innerHTML,/模型失败稿/);
 vm.runInContext("state.progressSearch='';setProgressFilter('complete');",context);
 assert.match(el('#article-body').innerHTML,/完成稿/);
 assert.doesNotMatch(el('#article-body').innerHTML,/排除稿/);
});
test('partial or ambiguous batch facts never become asserted completion counts',()=>{
 const {context,el}=fixture();
 assert.equal(vm.runInContext('batchCounts(state.snapshot).complete',context),1);
 assert.equal(vm.runInContext('batchCounts(state.snapshot).pending',context),4);
 assert.equal(vm.runInContext('batchCounts(state.snapshot).excluded',context),1);
 vm.runInContext('state.snapshot.articles.pop()',context);
 assert.equal(vm.runInContext('batchCounts(state.snapshot).complete',context),null);
 vm.runInContext('state.snapshot.batches[0].count=5;delete state.snapshot.articles[0].complete',context);
 assert.equal(vm.runInContext('batchCounts(state.snapshot).pending',context),null);
 vm.runInContext('renderBatchStatus()',context);
 assert.equal(el('#batch-badge').textContent,'本批状态待确认');
});
test('model error explanation is actionable without inventing timeout or credential causes',()=>{
 const {context}=fixture();
 const problem=Progress.problem(context.sample.articles[0]);
 assert.match(problem.title,/模型请求失败/);
 assert.match(problem.next,/查看.*(返回|错误|记录)/);
 assert.doesNotMatch(problem.title+problem.next,/密钥失效|已限流|一定|自动重试/);
 assert.match(Progress.describe(context.sample.articles[0],context.sample).detail,/具体原因/);
});
test('pending reconciliation stays visible when the manager is paused',()=>{
 const {context}=fixture();
 const view=Progress.describe(context.sample.articles[2],context.sample);
 assert.equal(view.tone,'pending_reconcile');
 assert.match(view.title,/核对/);
});
test('detail failure preserves the sample timestamp and does not drop reachable control',async()=>{
 const {context,el}=fixture();
 context.failure=Object.assign(new Error('台账暂不可读'),{status:503});
 vm.runInContext("api=async()=>{throw failure};refreshControl=async()=>{};state.refreshRequest={revision:state.revision,batch:state.batch,sequence:2};",context);
 await assert.rejects(vm.runInContext('refreshStatus(state.refreshRequest)',context),/台账暂不可读/);
 assert.equal(vm.runInContext('state.controlReachable',context),true);
 assert.equal(vm.runInContext('state.online',context),false);
 assert.equal(el('#continue').disabled,true);
 assert.match(el('#detail-sample').textContent,/上次|未更新/);
 assert.match(el('#detail-sample').textContent,/2023/);
 assert.match(el('#service-state').textContent,/控制.*(连接|在线)/);
});
test('long task durations use days and hours',()=>{
 const {context}=fixture();
 assert.equal(vm.runInContext('duration(90061)',context),'1 天 1 小时 1 分');
 assert.equal(vm.runInContext('duration(3661)',context),'1 小时 1 分');
 assert.equal(vm.runInContext('duration(null)',context),'—');
});
test('fresh completed detail opens even when the general ledger snapshot has never loaded',()=>{
 const {context,el}=fixture();
 context.completed={task_id:'done',name:'独立读取完成稿',state:'complete',complete:true,routes:[],artifacts:{profile:'available',tree:'available',tree_nodes:2,chunks:'available',chunk_count:3},correction_budget:{used:0,limit:2},detail:{cleanup:{}},attempts:1,completed_details:{image_versions:[],attachments:{items:[]}}};
 vm.runInContext("state.snapshot=null;state.online=false;PaperflowCompleted={imageSummary:()=>'',details:()=>''};showDetail(completed);",context);
 assert.equal(el('#detail-dialog').open,true);
 assert.match(el('#detail-content').innerHTML,/已入库，保存结果已确认/);
 assert.doesNotMatch(el('#detail-content').innerHTML,/当前进度待确认/);
});
test('Chinese composition is committed once and synchronization never rewrites the active search input',()=>{
 const {context,el}=fixture();
 const source=fs.readFileSync(require.resolve('../webworkbench/static/app.js'),'utf8');
 const bindingStart=source.indexOf("for(const id of ['search','process-search'])");
 vm.runInContext(source.slice(bindingStart,source.indexOf("for(const scope of ['article','process'])$('#'+scope+'-status-filter')",bindingStart)),context);
 const active=el('#search'),peer=el('#process-search');let draft='装配',writes=0;
 Object.defineProperty(active,'value',{get:()=>draft,set:value=>{writes++;draft=value;}});
 active.oninput({isComposing:true});
 assert.equal(writes,0);
 assert.equal(peer.value,'');
 draft='装配图';active.oncompositionend();
 assert.equal(peer.value,'装配图');
 assert.equal(vm.runInContext('state.progressSearch',context),'装配图');
 active.oninput({isComposing:false});
 assert.equal(writes,0);
 assert.equal(peer.value,'装配图');
});
