const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');

function fixture(){
 const elements=new Map(),calls=[];
 const el=id=>{if(!elements.has(id))elements.set(id,{value:'',textContent:'',innerHTML:'',hidden:true,disabled:false,querySelectorAll:()=>[]});return elements.get(id);};
 const context=vm.createContext({document:{getElementById:el},URLSearchParams,Date});
 vm.runInContext(fs.readFileSync(require.resolve('../webworkbench/static/completed.js'),'utf8'),context);
 const page=vm.runInContext('PaperflowCompleted',context);
 let response;
 page.init({esc:String,date:n=>'time-'+n,duration:String,showDetail(){},notice(){},
  api:async(path,options)=>{calls.push({path,options});if(response instanceof Error)throw response;return response;}});
 const row=id=>({task_id:id,name:id,routes:[],complete:true,state:'complete',image_summary:{state:'checked',retained:0,described:0,omitted:0},attachment_summary:{state:'not_checked'}});
 return {el,page,calls,row,respond(value){response=value;}};
}

test('completed list uses bounded pages and does not imply empty results on failure',async()=>{
 const f=fixture();
 f.respond(new Error('读取超时'));await f.page.load();
 assert.equal(new URLSearchParams(f.calls[0].path.split('?')[1]).get('limit'),'20');
 assert.equal(f.calls[0].options.timeoutMs,15000);
 assert.match(f.el('completed-state').textContent,/尚未读到结果.*刷新完成记录/);
 assert.equal(f.el('completed-empty').hidden,true);
 assert.equal(f.el('completed-refresh').disabled,false);
});

test('failed new search preserves the displayed query, cursor, rows and update timestamp',async()=>{
 const f=fixture();
 f.el('completed-search').value='old';f.respond({items:[f.row('first')],next_cursor:'page-two'});
 await f.page.load();
 const caption=f.el('completed-caption').textContent,body=f.el('completed-body').innerHTML;
 f.el('completed-search').value='new';f.respond(new Error('读取超时'));await f.page.load();
 assert.equal(f.el('completed-caption').textContent,caption);
 assert.equal(f.el('completed-body').innerHTML,body);
 assert.match(f.el('completed-state').textContent,/保留上次列表（更新于 time-/);
 f.respond({items:[f.row('second')],next_cursor:null});await f.page.load(false);
 const params=new URLSearchParams(f.calls[2].path.split('?')[1]);
 assert.equal(params.get('query'),'old');assert.equal(params.get('cursor'),'page-two');
 assert.match(f.el('completed-body').innerHTML,/first/);assert.match(f.el('completed-body').innerHTML,/second/);
});
