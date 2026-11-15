import {createRequire} from 'node:module';
import {pathToFileURL,fileURLToPath} from 'node:url';
import path from 'node:path';
import fs from 'node:fs';
const root=path.join(path.dirname(fileURLToPath(import.meta.url)),'profile_runtime');
const require=createRequire(import.meta.url),core=require(path.join(root,'packages/api/dist/exemplars.cjs'));
const evaluation=await import(pathToFileURL(path.join(root,'integrations/research-exemplars/evaluation.mjs')));
const review=await import(pathToFileURL(path.join(root,'integrations/research-exemplars/review.mjs')));
const pack={id:'undergraduate-universal',version:'1.0.0',contract_version:'1.0.0',label:'跨专业本科研究范例',rag:{profile_field:'research_exemplar',annotation_field:'research_annotation',methods_field:'research_methods',source_hash_field:'research_source_version'},methods:[],agent:{task_patterns:[],default_needs:[]},calibration:[]};
const spansFor=original=>evaluation.evidenceCatalog(original).map((s,i)=>({...s,native_id:s.id,id:`s${i+1}`}));
const findSpan=(spans,id)=>spans.find(s=>s.id===id||s.native_id===id);
const extraSpans=(original,rows)=>{
 const catalog=spansFor(original),selected=new Map(),byKey=new Map();
 for(const e of rows){
  if(typeof e.key!=='string'||!e.key||byKey.has(e.key)||!Array.isArray(e.span_ids)||e.span_ids.length>64||new Set(e.span_ids).size!==e.span_ids.length)throw Error('extra_evidence_scope_invalid');
  const spans=e.span_ids.map(id=>{const s=findSpan(catalog,id);if(!s)throw Error('extra_evidence_span_missing');return s;}).filter(s=>s.text.trim().length>=2);
  byKey.set(e.key,spans);for(const s of spans)selected.set(s.native_id,s);
 }
 return {byKey,selected:[...selected.values()].sort((a,b)=>a.start-b.start)};
};
const extraRanges=(original,rows,sourceVersion)=>{
 const {selected,byKey}=extraSpans(original,rows),ranges=[];
 // Same adjacent-range rule as evaluation.importEvaluation, shared by assembly
 // and readback so a pre-existing wider core quote cannot replace a citation.
 for(const s of selected){
  const prior=ranges.at(-1);
  if(prior&&!original.slice(prior.end,s.start).trim()&&s.end-prior.start<=1800)prior.end=s.end;
  else ranges.push({start:s.start,end:s.end});
 }
 for(const r of ranges){r.quote=original.slice(r.start,r.end);r.id=`e:${core.stableHash([sourceVersion,r.start,r.quote]).slice(0,20)}`;}
 const mapping={};
 for(const [key,spans] of byKey)mapping[key]=[...new Set(spans.map(s=>ranges.find(r=>r.start<=s.start&&r.end>=s.end).id))];
 return {byKey,selected,ranges,mapping};
};
const validateExtraEvidence=(p,original,rows)=>{
 const {byKey,selected,mapping,ranges}=extraRanges(original,rows,p.source_version),evidence=new Map(p.evidence.map(e=>[e.id,e]));
 const expected=new Map(ranges.map(r=>[r.id,r]));
 for(const row of rows){
  const refs=row.evidence_ids,spans=byKey.get(row.key);
  if(!Array.isArray(refs)||new Set(refs).size!==refs.length||refs.some(id=>!evidence.has(id)))throw Error('extra_evidence_mapping_invalid');
  if(JSON.stringify(refs)!==JSON.stringify(mapping[row.key]))throw Error('extra_evidence_projection_changed');
  if(refs.some(id=>['start','end','quote'].some(k=>evidence.get(id)[k]!==expected.get(id)[k])))throw Error('extra_evidence_range_changed');
  const quotes=refs.map(id=>evidence.get(id));
  if(spans.some(s=>!quotes.some(q=>q.start<=s.start&&q.end>=s.end))||quotes.some(q=>!spans.some(s=>q.start<=s.start&&q.end>=s.end)))throw Error('extra_evidence_citation_not_covered');
 }
 return {targets:rows.length,source_spans:selected.length};
};
const normalizedReport=(report,spans)=>{
 const r=structuredClone(report);
 for(const e of r.evidence??[])if(e.span_ids)e.span_ids=e.span_ids.map(id=>{
  const span=findSpan(spans,id);if(!span)throw Error(`evidence_span_missing:${e.key}:${id}`);return span.native_id;
 });
 // An unrecognized rating has no accepted academic meaning. Downgrade to
 // unknown, never translate an admission label into a positive dimension.
 for(const a of [...(r.units??[]).flatMap(u=>u.assessments??[]),...(r.relations??[]).map(x=>x.assessment)])
  for(const v of Object.values(a?.grades??{}))if(v.grade!==null&&!['sufficient','defective','invalid'].includes(v.grade))v.grade=null;
 return r;
};
process.stdin.setEncoding('utf8');
let input='';for await(const c of process.stdin)input+=c;
try {
 const a=JSON.parse(input);let result;
 if(a.op==='prepare'){
  result=await evaluation.evaluationInput(a.doc,a.original,pack);
  const policy=a.processing_policy;
  if(!policy||!['undergraduate-profile-processing/2026-10-11.2','undergraduate-profile-processing/2026-10-11.3'].includes(policy.id))throw Error('profile_processing_policy_missing');
  result.instructions=result.instructions.replace('请选择2至4个原文真正实施的研究动作单元',`请选择原文真正实施且支撑研究结论的必要研究动作单元，最多${policy.max_core_units}个，不强制凑数`);
  result.payload.author_spans=spansFor(a.original).map(({id,text})=>({id,text}));
  result.payload.sections=a.sections;result.payload.assets=a.assets;result.payload.specialty_groups=a.groups;
  result.payload.processing_policy=policy;
  result.instructions+='\n必须输出有换行和缩进的JSON，不压成一行。每个unit的assessments数组结束后再关闭该unit；下一个unit属于units数组，不能嵌入assessments。evidence、units、applicable_levels、relations均为result的并列字段。';
  result.instructions+=`\n记录research_types数组，按实际实施方法，可多选，不按专业套模板。输出group_codes，仅依据研究对象和正文实质内容选择给定专业类代码，通用方法不能扩大专业归属。不用输出章节覆盖和资产作用，它们另有逐章任务。本次units总数最多${policy.max_core_units}个，relations最多${policy.max_relations}个，evidence最多${policy.max_core_evidence}条且每条最多${policy.max_spans_per_core_evidence}个真实span_ids；核心单元同时评execution_steps和result_interpretation。不按章节数量凑核心单元；只声称方法不算实施，但理论综合、文本解释、设计实现等实际研究动作均可构成核心。原文缺项如实unknown，grade未知用JSON null而不是字符串。如果其他必要核心动作超出本轮预算或未覆盖，记录core_route_coverage="partial"，只有实际主干全部覆盖才用"complete"；不得遗漏未描写步骤却自称完整。features为空数组。每条reason简明20至40字。所有结果必须在${policy.complete_json_output_budget??14000}输出token内完整结束JSON。只输出原格式与上述附加字段，不自授verified。`;
 } else if(a.op==='import') {
  const catalog=spansFor(a.original),report=normalizedReport(a.report,catalog);
  const profile=evaluation.importEvaluation(report,a.doc,a.original,[],pack,a.evaluator);
  // Batch background stays in the envelope; a source degree needs actual cover evidence.
  result={profile};
  result.key_evidence={};
  for(const e of report.evidence??[]){
   // Import each logical definition through the native range rule. Containment
   // alone could substitute an unrelated, wider quote from another definition.
   const exact=evaluation.importEvaluation({evidence:[e],units:[]},a.doc,a.original,[],pack,a.evaluator);
   const educationBase=new Set(exact.education?.evidence_ids??[]);
   result.key_evidence[e.key]=exact.evidence.filter(v=>!educationBase.has(v.id)).map(v=>v.id);
  }
 } else if(a.op==='add_evidence'){
  const p=core.validateProfile(a.profile,a.original),rows=a.evidence??[];
  const {ranges,mapping}=extraRanges(a.original,rows,p.source_version);
  // Reuse evaluation.importEvaluation's exact adjacent-range rule globally.
  // Only selected author spans with whitespace between them can be combined;
  // no citation or core claim is dropped, and each target keeps all its spans.
  for(const range of ranges){
   if(!p.evidence.some(e=>e.id===range.id))p.evidence.push({...range,section:'相邻正文引用',chunk_ids:[]});
  }
  if(p.evidence.length>256)throw Error(`extra_evidence_capacity_exceeded:${p.evidence.length}:limit=256`);
  validateExtraEvidence(p,a.original,rows.map(row=>({...row,evidence_ids:mapping[row.key]})));
  result={profile:core.validateProfile(p,a.original),mapping};
 } else if(a.op==='validate_extra_evidence')result=validateExtraEvidence(core.validateProfile(a.profile,a.original),a.original,a.evidence??[]);
 else if(a.op==='review_prepare')result=await review.prepareProfileReview(a.profile,a.original);
 else if(a.op==='review_apply'){
  result=core.applyProfileReview(a.profile,a.original,a.input_hash,a.result,a.reviewer,a.read_scopes??[a.read_scope]);
 } else if(a.op==='rebind'){
  const source=core.validateProfile(a.profile,a.original),next=structuredClone(source);
  const proof=source.semantic_review;if(!proof)throw Error('source_review_missing');
  next.dataset_id=a.dataset_id;next.document_id=a.document_id;next.document_name=a.document_name;
  next.evidence=next.evidence.map(e=>({...e,chunk_ids:core.evidenceChunkIds(e.quote,a.chunks)}));
  if(next.evidence.some(e=>!e.chunk_ids.length))throw Error('source_quote_chunk_binding_missing');
  const texts=new Map(a.chunks.map(c=>[c.id,c.content??c.content_with_weight??'']));
  if(next.evidence.some(e=>!core.evidenceRead(e.quote,e.chunk_ids.map(id=>texts.get(id)??''))))throw Error('source_quote_chunk_binding_incomplete');
  const before=core.profileReviewInput(source),after=core.profileReviewInput(next);
  const neutral=v=>{const n=structuredClone(v);delete n.input_hash;delete n.dataset_id;delete n.document_id;for(const e of n.evidence)delete e.chunk_ids;return n;};
  if(JSON.stringify(neutral(before))!==JSON.stringify(neutral(after)))throw Error('profile_projection_claim_changed');
  result={profile:core.applyProfileReview(next,a.original,after.input_hash,{checks:proof.checks},proof.reviewer,proof.read_scopes),
    binding:{mode:'projection_replay',source_input_hash:before.input_hash,target_input_hash:after.input_hash,
      source_reviewed_at:proof.created_at,source_reviewer:proof.reviewer,source_version:source.source_version,
      source_document_id:source.document_id,dataset_id:a.dataset_id,document_id:a.document_id,
      evidence_chunks:next.evidence.map(e=>({evidence_id:e.id,chunk_ids:e.chunk_ids}))}};
 } else if(a.op==='validate')result=core.validateProfile(a.profile,a.original);
 else if(a.op==='catalog')result={spans:evaluation.evidenceCatalog(a.original),source_version:core.stableHash(a.original)};
 else throw Error('unknown_bridge_operation');
 process.stdout.write(JSON.stringify({ok:true,result}));
} catch(e){process.stdout.write(JSON.stringify({ok:false,error:e.name,reason:String(e.message).slice(0,1600)}));process.exitCode=1;}
