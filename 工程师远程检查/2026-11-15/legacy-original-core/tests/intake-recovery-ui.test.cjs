const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const {Queue}=require('../webworkbench/static/intake.js');

const files=n=>Array.from({length:n},(_,i)=>({name:`paper-${i}.txt`,size:1,webkitRelativePath:`folder/paper-${i}.txt`}));
const running={batch_id:'same-batch',manager:{epoch:'current',generation:7,state:'running'}};
const transient=()=>Object.assign(new Error('temporary connection timeout'),{name:'AbortError',retryable:true});
function host(extra={}){return {prepare:async r=>({id:r.id}),start:async()=>running,capacity:async()=>true,append:async()=>({}),wait:async()=>{},...extra};}

test('one capacity timeout after 33 ready items still refills the same batch',async()=>{
 let calls=0,starts=0;
 const q=new Queue(host({capacity:async()=>{if(++calls===33)throw transient();return true;},start:async()=>{starts++;return running;}}));
 q.chooseFiles(files(40),['.txt']);await q.start();await q.pump();
 assert.equal(q.entries.filter(r=>r.state==='ready').length,40);
 assert.equal(starts,1);assert.equal(q.token.batch_id,'same-batch');assert.equal(q.running,false);
});

test('persistent transport failure stops cleanly and preserves pending items for explicit continuation',async()=>{
 let calls=0;
 const q=new Queue(host({capacity:async()=>{calls++;throw transient();}}));
 q.chooseFiles(files(2),['.txt']);await q.start();await assert.rejects(q.pump(),/timeout/);
 assert.ok(calls>1&&calls<=5);assert.equal(q.running,false);assert.equal(q.pumping,null);
 assert.equal(q.entries[0].state,'ready');assert.equal(q.entries[1].state,'pending');
});

test('pause while waiting to retry never prepares or appends another file',async()=>{
 let prepared=0,appended=0;
 const q=new Queue(host({prepare:async r=>{prepared++;return{id:r.id};},capacity:async()=>{throw transient();},wait:async()=>q.pause(),append:async()=>{appended++;return{};}}));
 q.chooseFiles(files(3),['.txt']);await q.start();await q.pump();
 assert.equal(prepared,1);assert.equal(appended,0);assert.equal(q.running,false);
});

test('expired epoch is never retried or converted into a new admission grant',async()=>{
 let calls=0,appended=0;
 const q=new Queue(host({capacity:async()=>{calls++;throw Object.assign(new Error('epoch expired'),{status:409,retryable:false});},append:async()=>{appended++;return{};}}));
 q.chooseFiles(files(2),['.txt']);await q.start();await assert.rejects(q.pump(),/epoch expired/);
 assert.equal(calls,1);assert.equal(appended,0);assert.equal(q.running,false);
});

test('lost append response repeats only the same idempotent binding without reuploading',async()=>{
 const stored=new Set();let writes=0,prepared=0;
 const q=new Queue(host({prepare:async r=>{prepared++;return{id:r.id};},append:async(items,token)=>{
  assert.equal(token.batch_id,'same-batch');writes++;stored.add(JSON.stringify(items));if(writes===1)throw transient();return{};
 }}));
 q.chooseFiles(files(2),['.txt']);await q.start();await q.pump();
 assert.equal(prepared,2);assert.equal(writes,2);assert.equal(stored.size,1);assert.equal(q.entries[1].state,'ready');
});

test('old pump cleanup cannot disable an explicit newer generation resume',async()=>{
 let release,calls=0;const oldRead=new Promise(resolve=>release=resolve),generations=[];
 const q=new Queue(host({capacity:async()=>++calls===1?oldRead:true,append:async(items,token)=>{generations.push(token.generation);return{};}}));
 q.chooseFiles(files(3),['.txt']);await q.start();const oldPump=q.pump();await Promise.resolve();
 q.pause();q.resume({...running,manager:{...running.manager,generation:8}});const nextPump=q.pump();release(true);
 await Promise.all([oldPump,nextPump]);
 assert.equal(q.entries.filter(r=>r.state==='ready').length,3);assert.deepEqual(generations,[8,8]);
});

test('a transient copy failure retries the same file instead of permanently failing it',async()=>{
 let second=0,errors=0;
 const q=new Queue(host({prepare:async row=>{if(row.id==='file-1'&&++second===1)throw transient();return{id:row.id};},append:async items=>{errors+=Number(!!items[0].error);return{};}}));
 q.chooseFiles(files(3),['.txt']);await q.start();await q.pump();
 assert.equal(q.entries.filter(r=>r.state==='ready').length,3);assert.equal(errors,0);assert.equal(second,2);
});

test('explicit backend reopens formerly unsupported material items in the same browser queue',async()=>{
 const q=new Queue(host());q.chooseFiles([...files(1),{name:'figure.png',size:2,webkitRelativePath:'folder/figure.png'}],['.txt']);
 await q.start();assert.equal(q.entries[1].state,'skipped');
 q.pause();q.resume({...running,reopened_items:['file-1']});await q.pump();
 assert.equal(q.entries[1].state,'ready');assert.equal(q.entries.length,2);
});

function requestApi(fetch){
 const app=fs.readFileSync(require.resolve('../webworkbench/static/app.js'),'utf8');
 const source=app.slice(app.indexOf('async function api('),app.indexOf('function showLogin('));
 const context={state:{csrf:'test'},Blob,AbortController,fetch,setTimeout:(fn)=>setTimeout(fn,2),clearTimeout,showLogin(){}};
 vm.createContext(context);vm.runInContext(source,context);return context.api;
}
test('request timeout reports the failed operation with a useful message and retry classification',async()=>{
 const api=requestApi(async(path,{signal})=>new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(signal.reason),{once:true})));
 await assert.rejects(api('/api/intake/same-batch',{timeoutMs:15000}),error=>error.retryable===true&&error.path==='/api/intake/same-batch'&&/15.*秒/.test(error.message)&&!/without reason/.test(error.message));
});
test('request errors distinguish a transient service failure from stale control authorization',async()=>{
 for(const [status,want] of [[503,true],[409,false],[401,false]]){
  const api=requestApi(async()=>({ok:false,status,json:async()=>({detail:'test failure'})}));
  await assert.rejects(api('/api/intake/same-batch'),error=>error.status===status&&error.retryable===want);
 }
});
