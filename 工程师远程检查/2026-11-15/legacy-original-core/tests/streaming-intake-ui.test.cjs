const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const file=path.join(__dirname,'../webworkbench/static/intake.js');
const intake=fs.existsSync(file)?require(file):{};
function source(name,text,relative){return Object.assign(new Blob([text]),{name,webkitRelativePath:relative,lastModified:123});}

test('folder preview contains bodies and attachments without any upload and paginates by 20',()=>{
 assert.equal(typeof intake.Queue,'function');
 const calls=[],q=new intake.Queue({prepare:r=>calls.push(r)});
 q.chooseFiles(Array.from({length:41},(_,i)=>source('paper.txt',String(i),`folder/${i}/paper.txt`))
  .concat([source('drawing.dwg','dwg','folder/0/drawing.dwg'),source('program.zip','zip','folder/0/program.zip'),source('unknown.zz','xx','folder/unknown.zz')]),['.txt','.dwg','.zip']);
 assert.equal(calls.length,0);assert.equal(q.entries.length,44);
 assert.equal(q.entries.filter(r=>r.state==='skipped').length,1);
 const page=intake.page(q.entries,0);
 assert.equal(page.rows.length,20);assert.equal(page.pages,3);
 const paper=page.rows.find(r=>r.relative_path==='folder/0/paper.txt');
 assert.deepEqual(paper.attachments.map(r=>r.name),['drawing.dwg','program.zip']);
 assert.equal(intake.page(q.entries,2).rows.length,2);
});

test('one explicit start processes first file before the last file is prepared',async()=>{
 const calls=[];let release;const blocked=new Promise(r=>release=r);
 const q=new intake.Queue({
  prepare:async r=>{calls.push('prepare:'+r.name);if(r.name==='b.txt')await blocked;return {id:r.name};},
  start:async r=>{calls.push('start:'+r.id);return {batch_id:'batch',manager:{epoch:'e',generation:1,state:'running'}};},
  append:async items=>{calls.push('append:'+items[0].input_id);return {};},
  capacity:async()=>true,onChange(){}
 });
 q.chooseFiles([source('a.txt','a','f/a.txt'),source('b.txt','b','f/b.txt')],['.txt']);
 await q.start();assert.deepEqual(calls,['prepare:a.txt','start:a.txt']);
 const pumping=q.pump();await Promise.resolve();assert.ok(calls.includes('start:a.txt'));
 assert.ok(!calls.includes('append:b.txt'));release();await pumping;
 assert.equal(q.entries.filter(r=>r.state==='ready').length,2);
 assert.ok(calls.indexOf('start:a.txt')<calls.indexOf('prepare:b.txt'));
});

test('pause during preparation cannot append or start again until explicit resume',async()=>{
 const calls=[];let release;const blocked=new Promise(r=>release=r);
 const q=new intake.Queue({prepare:async r=>{if(r.name==='b.txt')await blocked;return{id:r.name};},
  start:async()=>({batch_id:'batch',manager:{epoch:'e',generation:1,state:'running'}}),
  append:async(items,token)=>{calls.push(token.generation);return{};},capacity:async()=>true,onChange(){}});
 q.chooseFiles([source('a.txt','a','f/a.txt'),source('b.txt','b','f/b.txt')],['.txt']);await q.start();
 const pumping=q.pump();await Promise.resolve();q.pause();release();await pumping;assert.equal(calls.length,0);
 q.resume({batch_id:'batch',manager:{epoch:'e',generation:3,state:'running'}});await q.pump();
 assert.deepEqual(calls,[3]);
});

test('readiness backpressure waits without preparing more files and honors pause',async()=>{
 let more=0;const q=new intake.Queue({prepare:async r=>{more++;return{id:r.id};},
  start:async()=>({batch_id:'batch',manager:{epoch:'e',generation:1,state:'running'}}),
  capacity:async()=>false,wait:async()=>q.pause(),onChange(){}});
 q.chooseFiles([source('a.txt','a','f/a.txt'),source('b.txt','b','f/b.txt')],['.txt']);await q.start();await q.pump();
 assert.equal(more,1);
});

test('new selection discards old UI rows only and same folder paths never get attached arbitrarily',()=>{
 const q=new intake.Queue({});q.chooseFiles([source('old.txt','o','old/old.txt')],['.txt']);
 q.chooseFiles([source('a.txt','a','f/a.txt'),source('b.txt','b','f/b.txt'),source('code.zip','z','f/code.zip')],['.txt','.zip']);
 assert.ok(q.entries.every(r=>r.name!=='old.txt'));
 const rows=intake.page(q.entries,0).rows;
 assert.equal(rows.length,3,'ambiguous attachment stays independent instead of being duplicated under both bodies');
 assert.equal(rows.filter(r=>r.attachments?.length).length,0);
});

test('changing the submission after a paused first copy rebinds its cached input',async()=>{
 let request='one',release,count=0;const copying=new Promise(r=>release=r);
 const q=new intake.Queue({canReuse:r=>r.request===request,
  prepare:async r=>{count++;if(count===1)await copying;r.request=request;return{id:'same-content'};},
  start:async()=>({batch_id:'batch',manager:{epoch:'e',generation:1,state:'running'}}),onChange(){}});
 q.chooseFiles([source('a.txt','a','f/a.txt')],['.txt']);const first=q.start();await Promise.resolve();q.pause();release();
 await assert.rejects(first,/已暂停/);request='two';await q.start();assert.equal(count,2);
});
