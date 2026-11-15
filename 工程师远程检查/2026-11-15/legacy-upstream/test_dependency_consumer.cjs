const assert=require('node:assert/strict');
const core=require('./profile_runtime/packages/api/dist/exemplars.cjs');
const p={units:[{id:'bad',assessments:[{verification:'verified',decision:'disabled',aspect:'execution_steps'}]},
{id:'downstream',assessments:[{verification:'verified',decision:'recommended',aspect:'result_interpretation'}]},
{id:'independent',assessments:[{verification:'verified',decision:'limited',aspect:'execution_steps'}]}],
relations:[{from:['bad'],to:['downstream'],assessment:{verification:'verified',decision:'recommended'}}]};
assert.equal(core.dependencyRestricted(p,'downstream','result_interpretation'),true);
assert.equal(core.dependencyRestricted(p,'downstream','section_function'),false);
assert.equal(core.dependencyRestricted(p,'independent','execution_steps'),false);
assert.equal(p.units[1].assessments[0].decision,'recommended');
console.log('dependency restrictions: downstream blocked; structure and independent uses retained');

// A short heading at the end of a native slice must not make a genuine quote unbindable.
const quote='1 优势\n\n企业转型需求明确，必须重新采集本次材料。';
const slices=[{id:'left',content:'前文\n1 优势\n\n'},{id:'right',content:'企业转型需求明确，必须重新采集本次材料。\n后文'}];
assert.deepEqual(core.evidenceChunkIds(quote,slices),['left','right']);
assert.equal(core.evidenceRead(quote,slices.map(c=>c.content)),true);
assert.deepEqual(core.evidenceChunkIds('1 优势\n\n伪造的研究过程',slices),[]);
assert.equal(core.evidenceRead('1 优势\n\n伪造的研究过程',slices.map(c=>c.content)),false);
assert.equal(core.evidenceRead(quote,[slices[0].content,slices[1].content.replace('需求明确','需求不明')]),false);
console.log('native slice boundary: exact quotation retained; fabricated and modified text rejected');
