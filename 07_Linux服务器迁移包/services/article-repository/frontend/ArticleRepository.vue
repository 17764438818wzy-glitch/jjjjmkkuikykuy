<script setup>
// Asset list, filters, tags, pagination and drawer adapted from dashboard.vue.
import {computed,onMounted,onBeforeUnmount,reactive,ref,watch} from 'vue';
import PaperProfile from './PaperProfile.vue';
import ReferenceTags from './ReferenceTags.vue';
import Icon from './Icon.vue';
import './warehouse.css';
const rows=ref([]),total=ref(0),majors=ref([]),years=ref([]),sources=ref([]),busy=ref(false),error=ref('');
const keyword=ref(''),major=ref(''),page=ref(1),level=ref(''),researchType=ref(''),year=ref(''),fileCondition=ref(''),attachmentExt=ref(''),pageSize=ref(30);
const statistics=ref(null),guide=ref(false),directory=ref(false),directoryKeyword=ref(''),majorError=ref(''),openedId=ref('');
const opened=ref(false),detailBusy=ref(false),detail=ref(null),detailError=ref(''),editing=ref(false),saving=ref(false),notice=ref('');
const form=reactive({title:'',majors:[],fields:{}});
let listController,detailController,majorController,yearController,statsController,sequence=0,detailSequence=0,debounce;
const yearError=ref('');
const labels={summary:'文章摘要',keywords:'主题关键词',audience:'适用人群',level:'参考层级标签',object:'研究对象',methods:'研究方法',path:'整体研究路线',learn:'可借鉴内容'};
const names=computed(()=>Object.fromEntries(majors.value.map(m=>[m.code,m.name])));
const options=computed(()=>majors.value.map(m=>({value:m.code,label:m.name+' · '+m.code})));
const availability={complete:'原文附件可下载',partial:'部分可下载',missing:'原件缺失',unknown:'原件待核实'};
const migration={complete:'已迁入新库',not_migrated:'未完成迁移',unverified:'迁移待核实'};
async function request(url,options={}){
  const response=await fetch(url,{...options,headers:{'Content-Type':'application/json',...(options.headers||{})}});
  const value=await response.json();
  if(!response.ok) throw new Error(value.detail||'读取未完成，请重试');
  return value;
}
async function load(){
  const current=++sequence;listController?.abort();listController=new AbortController();busy.value=true;
  try{
    const params=new URLSearchParams({keyword:keyword.value,major:major.value,page:String(page.value),page_size:String(pageSize.value),level:level.value,research_type:researchType.value,year:year.value,file_condition:fileCondition.value,attachment_ext:attachmentExt.value});
    const value=await request('/api/library/articles?'+params,{signal:listController.signal});
    if(current!==sequence)return;
    rows.value=value.rows;total.value=value.total;sources.value=value.sources;error.value='';
  }catch(e){if(e.name!=='AbortError'&&current===sequence)error.value=e.message}
  finally{if(current===sequence)busy.value=false}
}
async function open(row){
  const current=++detailSequence;detailController?.abort();detailController=new AbortController();
  openedId.value=row.id;opened.value=true;detail.value=null;detailError.value='';notice.value='';editing.value=false;detailBusy.value=true;
  try{
    const value=await request('/api/library/articles/'+row.id,{signal:detailController.signal});
    if(current===detailSequence){detail.value=value;const match=rows.value.find(r=>r.id===row.id);if(match){match.reference=value.reference;match.rating=value.rating;match.file_summary=value.file_summary;}}
  }catch(e){if(e.name!=='AbortError'&&current===detailSequence)detailError.value=e.message}
  finally{if(current===detailSequence)detailBusy.value=false}
}
function edit(){
  form.title=detail.value.title;form.majors=[...detail.value.majors];form.fields={...detail.value.fields};editing.value=true;notice.value='';
}
async function save(){
  const originId=detail.value.id,originSequence=detailSequence;
  saving.value=true;detailError.value='';
  try{
    const changes={};
    if(form.title!==detail.value.title)changes.title=form.title;
    if(JSON.stringify([...form.majors].sort())!==JSON.stringify([...detail.value.majors].sort()))changes.majors=form.majors;
    const fields=Object.fromEntries(Object.entries(form.fields).filter(([k,v])=>v!==detail.value.fields[k]));
    if(Object.keys(fields).length)changes.fields=fields;
    if(!Object.keys(changes).length){editing.value=false;return}
    const result=await request('/api/library/articles/'+detail.value.id,{method:'PATCH',body:JSON.stringify({revision:detail.value.revision,changes})});
    if(!opened.value||detail.value?.id!==originId||detailSequence!==originSequence)return;
    const status=result.sync?.status;
    await open(result);notice.value=status==='complete'?'修改已保存并同步新库':status==='saved_for_migration'?'修改已保存，将在后续迁移时使用':'修改已保存，新库同步未确认，可再次同步';
    await load();loadStats();
  }catch(e){if(opened.value&&detail.value?.id===originId&&detailSequence===originSequence)detailError.value=e.message}finally{saving.value=false}
}
async function synchronize(){
  const originId=detail.value.id,originSequence=detailSequence;
  saving.value=true;detailError.value='';
  try{
    const result=await request('/api/library/articles/'+detail.value.id+'/sync',{method:'POST'});
    if(!opened.value||detail.value?.id!==originId||detailSequence!==originSequence)return;
    await open(result);notice.value=result.sync?.status==='complete'?'已同步新库':result.sync?.status==='saved_for_migration'?'修改已保存，等待后续迁移':'新库同步未确认，可再次同步';
    await load();
  }catch(e){if(opened.value&&detail.value?.id===originId&&detailSequence===originSequence)detailError.value=e.message}finally{saving.value=false}
}

const levelOptions=[['','全部'],['correspondence','函授参考'],['bachelor','本科参考'],['key_bachelor','重点本科参考'],['unclassified','层级待明确']];
const researchTypeOptions=[['','全部'],['engineering_system','工程设计与系统实现'],['experimental_testing','实验研究与测试'],['survey_case_applied','调查、案例与应用分析'],['theoretical_modeling','理论推导与计算建模'],['textual_normative','文本分析与规范论证'],['creative_design','艺术创作与设计实践'],['unclassified','未分类']];
const fileOptions=[['','全部文件状态'],['original','原文可用'],['attachment','有可用附件'],['both','原文与附件均可用'],['unverified','文件状态待核实']];
const directoryMajors=computed(()=>majors.value.filter(m=>(m.name+' '+m.code).includes(directoryKeyword.value.trim())));
const activeFilters=computed(()=>[
  keyword.value&&{key:'keyword',label:'关键词：'+keyword.value},major.value&&{key:'major',label:names.value[major.value]||major.value},
  level.value&&{key:'level',label:levelOptions.find(o=>o[0]===level.value)?.[1]},
  researchType.value&&{key:'researchType',label:researchTypeOptions.find(o=>o[0]===researchType.value)?.[1]},
  year.value&&{key:'year',label:year.value==='unclassified'?'年份待明确':year.value+' 年'},
  fileCondition.value&&{key:'fileCondition',label:fileOptions.find(o=>o[0]===fileCondition.value)?.[1]},
  attachmentExt.value&&{key:'attachmentExt',label:'附件 '+attachmentExt.value.toUpperCase()},
].filter(Boolean));
const pageCount=computed(()=>Math.max(1,Math.ceil(total.value/pageSize.value)));
const pages=computed(()=>[...new Set([1,page.value-1,page.value,page.value+1,pageCount.value])].filter(n=>n>0&&n<=pageCount.value).sort((a,b)=>a-b));
const methodTags=row=>String(row.fields.methods||'').split(/[；;\n]/).map(s=>s.trim()).filter(Boolean).slice(0,2).map(s=>s.length>23?s.slice(0,23)+'…':s);
function clearFilter(key){({keyword,major,level,researchType,year,fileCondition,attachmentExt}[key]).value=''}
function reset(){keyword.value='';major.value='';level.value='';researchType.value='';year.value='';fileCondition.value='';attachmentExt.value='';}
async function loadStats(){statsController?.abort();statsController=new AbortController();try{statistics.value=await request('/api/library/statistics',{signal:statsController.signal})}catch(e){if(e.name!=='AbortError')statistics.value=null}}
async function loadMajors(){majorController?.abort();majorController=new AbortController();try{majors.value=await request('/api/library/majors',{signal:majorController.signal});majorError.value=''}catch(e){if(e.name!=='AbortError')majorError.value='专业目录暂不可读取，请刷新后重试'}}
async function loadYears(){yearController?.abort();yearController=new AbortController();try{years.value=await request('/api/library/years',{signal:yearController.signal});yearError.value=''}catch(e){if(e.name!=='AbortError')yearError.value='年份选项暂不可读取，请刷新后重试'}}
function refresh(){clearTimeout(debounce);load();loadStats();loadYears();if(majorError.value)loadMajors()}
watch([keyword,major,level,researchType,year,fileCondition,attachmentExt,pageSize,page],(next,previous)=>{
  clearTimeout(debounce);++sequence;listController?.abort();busy.value=true;
  if(next.slice(0,8).some((v,i)=>v!==previous[i])&&page.value!==1)page.value=1;
  debounce=setTimeout(load,180);
});
watch(opened,value=>{if(!value){++detailSequence;detailController?.abort();editing.value=false}});
onMounted(()=>{load();loadMajors();loadStats();loadYears()});
onBeforeUnmount(()=>{++sequence;++detailSequence;listController?.abort();detailController?.abort();majorController?.abort();yearController?.abort();statsController?.abort();clearTimeout(debounce)});
</script>
<template>
  <div class="app-shell">
    <aside class="sidebar"><a class="brand" href="/"><span class="brand-mark"><Icon name="book"/></span><div>Paperflow<small>论文知识仓库</small></div></a><div class="nav-label">我的知识库</div>
      <nav class="side-nav" aria-label="主导航"><button class="active" @click="reset"><Icon name="layers"/><span>文章仓库</span><span class="nav-count">{{statistics?.total?.toLocaleString()||'—'}}</span></button><button @click="directory=true"><Icon name="grid"/><span>专业目录</span><Icon name="chevron"/></button><button @click="reset();fileCondition='attachment'"><Icon name="folder"/><span>可用附件</span></button></nav>
      <div class="sidebar-divider"></div><div class="nav-label">使用帮助</div><nav class="side-nav"><button @click="guide=true"><Icon name="book"/><span>画像与筛选说明</span></button></nav>
      <div class="sidebar-bottom"><div class="preview-icon"><Icon name="book"/></div><strong>画像、原文与附件</strong><p>保留参考依据<br>查看借鉴内容与边界</p><span><span class="status-dot"></span>文章 · 画像 · 附件</span></div><div class="sidebar-foot">PAPERFLOW <span>LIBRARY</span></div>
    </aside>
    <div class="app-body"><header class="topbar"><div class="breadcrumb">我的知识库 <Icon name="chevron"/><span>文章仓库</span></div><div class="topbar-right"><span class="catalog-badge">论文知识仓库</span></div></header>
      <main><div class="page-heading"><div><div class="eyebrow">ARTICLE LIBRARY</div><h1>文章仓库<span v-if="statistics" class="heading-count">{{statistics.total.toLocaleString()}} 篇</span></h1><p>按专业、参考层级与附件条件，找到合适的论文参考。</p></div><div class="heading-actions"><button class="secondary-button guide-button" @click="guide=true"><Icon name="info"/>筛选说明</button><button class="secondary-button refresh-button" :disabled="busy" @click="refresh"><Icon name="reset"/>{{busy?'读取中…':'刷新'}}</button></div></div>
        <a-alert v-if="error||majorError||yearError" type="warning" show-icon :message="error||majorError||yearError" :description="error?'当前保留上次读取结果，可刷新后重试。':undefined" class="notice"/>
        <div class="overview-strip"><div class="overview-intro"><span class="overview-icon"><Icon name="layers"/></span><div><strong>从画像出发，挑选参考</strong><p>保留参考依据，也看得见使用边界。</p></div></div><div class="overview-numbers"><div><span>已登记文章</span><strong>{{statistics?.total?.toLocaleString()??'—'}}<small>篇</small></strong></div><div><span>可供本科参考</span><strong>{{statistics?.bachelor?.toLocaleString()??'—'}}<small>篇</small></strong></div><div><span>含可用附件</span><strong>{{statistics?.attachments?.toLocaleString()??'—'}}<small>篇</small></strong></div></div></div>
        <section class="filter-panel" aria-label="文章筛选"><div class="filter-top"><div class="filter-title"><Icon name="filter"/><h2>筛选文章</h2></div><button class="text-button reset-button" @click="reset"><Icon name="reset"/>重置条件</button></div>
          <div class="search-row"><label class="search-box"><Icon name="search"/><input v-model="keyword" type="search" maxlength="200" placeholder="搜索题目、关键词、研究方法…" aria-label="搜索文章"/><span class="search-hint">模糊搜索</span></label><div class="major-field"><span class="major-field-label">专业</span><a-select v-model:value="major" show-search :options="[{value:'',label:'全部专业'},...options]" option-filter-prop="label" aria-label="专业筛选" class="warehouse-major-select"/></div><label class="select-box file-select"><span class="field-label">文件条件</span><select v-model="fileCondition" aria-label="文件条件"><option v-for="o in fileOptions" :key="o[0]" :value="o[0]">{{o[1]}}</option></select><Icon name="down"/></label></div>
          <div class="level-row"><span class="filter-row-label">参考层级</span><div class="level-options"><button v-for="o in levelOptions" :key="o[0]" :class="{active:level===o[0]}" :aria-pressed="level===o[0]" @click="level=o[0]">{{o[1]}}<Icon v-if="level===o[0]&&o[0]" name="check"/></button></div><button class="filter-help" @click="guide=true" aria-label="了解参考层级的筛选规则"><Icon name="info"/></button></div>
          <div class="level-row research-type-row" role="group" aria-label="文章分类"><span class="filter-row-label">文章分类</span><div class="level-options"><button v-for="o in researchTypeOptions" :key="o[0]" :class="{active:researchType===o[0]}" :aria-pressed="researchType===o[0]" @click="researchType=o[0]">{{o[1]}}<Icon v-if="researchType===o[0]&&o[0]" name="check"/></button></div></div>
          <div class="level-row year-row" role="group" aria-label="稿件年份"><span class="filter-row-label">稿件年份</span><div class="level-options"><button :class="{active:year===''}" :aria-pressed="year===''" @click="year=''">全部</button><button v-for="y in years" :key="y" :class="{active:year===y}" :aria-pressed="year===y" @click="year=y">{{y}} 年<Icon v-if="year===y" name="check"/></button><button :class="{active:year==='unclassified'}" :aria-pressed="year==='unclassified'" @click="year='unclassified'">年份待明确</button></div></div>
          <div class="format-row"><span class="filter-row-label">附件格式</span><div class="format-options"><button v-for="ext in ['','pdf','docx','zip','dwg']" :key="ext" :class="{active:attachmentExt===ext}" :aria-pressed="attachmentExt===ext" @click="attachmentExt=ext">{{ext.toUpperCase()||'不限'}}<span v-if="ext" class="format-dot" :class="ext"></span></button></div><span class="format-note">仅匹配目录已核实可用的附件</span></div>
          <div v-if="activeFilters.length" class="active-filter-row"><span>已选</span><button v-for="f in activeFilters" :key="f.key" @click="clearFilter(f.key)">{{f.label}}<Icon name="close"/></button></div>
        </section>
        <section class="results-panel" aria-label="筛选结果" :aria-busy="busy"><div class="results-header"><div><h2>文章列表</h2><span class="results-count" role="status" aria-live="polite">{{busy?'正在筛选…':total.toLocaleString()+' 篇'}}<span v-if="!busy&&activeFilters.length">符合条件</span><span v-if="error">上次读取结果</span></span></div><span class="results-note"><span class="status-dot"></span>{{keyword.trim()?'按题目相关度排序':'原件待核实的文章仍保留在列表中'}}</span></div>
          <a-spin :spinning="busy"><div v-if="rows.length" class="table-wrap"><table class="paper-table"><colgroup><col style="width:38%"><col style="width:19%"><col style="width:24%"><col style="width:15%"><col style="width:4%"></colgroup><thead><tr><th scope="col">文章 / 专业</th><th scope="col">参考层级</th><th scope="col">研究方法与可借鉴点</th><th scope="col">原文与附件</th><th scope="col" aria-label="查看画像"></th></tr></thead><tbody>
            <tr v-for="row in rows" :key="row.id"><td><div class="table-title-cell"><span class="document-icon" :class="{violet:row.reference?.reference_only}"><Icon name="file"/></span><div><button class="paper-title" @click="open(row)">{{row.title}}</button><div class="major-tags"><span v-if="row.year">{{row.year}} 年</span><span v-for="code in row.majors.slice(0,2)" :key="code">{{names[code]||code}}</span><span v-if="row.majors.length>2" class="more-majors" :title="row.majors.slice(2).map(c=>names[c]||c).join('、')">+{{row.majors.length-2}}</span></div></div></div></td><td><ReferenceTags :reference="row.reference"/><div v-if="row.reference?.primary" class="table-primary">主评级：{{row.reference.primary}}</div></td><td><div class="method-tags"><span v-for="(m,i) in methodTags(row)" :key="i">{{m}}</span></div><p class="table-learn" :title="row.fields.learn">{{row.fields.learn||'展开画像查看研究内容'}}</p></td><td><div class="file-status"><span v-if="row.file_summary?.original_available" class="file-ok"><span class="status-dot"></span>原文可用</span><span v-else class="file-unknown"><span class="status-dot"></span>{{row.file_summary?.original?.missing&&!row.file_summary?.original?.unknown?'原文当前不可用':'原文待核实'}}</span><span v-if="row.file_summary?.attachment_available" class="attachment-note"><Icon name="clip"/>{{row.file_summary.attachment_available}} 个可用附件</span><span v-else class="file-subtle">{{row.file_summary?.attachment?.missing&&!row.file_summary?.attachment?.unknown?'附件当前不可用':row.file_summary?.inventory_complete&&!row.file_summary?.attachment?.registered?'未登记附件':'附件待核实'}}</span></div></td><td><button class="row-open" :aria-label="'查看画像：'+row.title" @click="open(row)"><Icon name="arrow"/></button></td></tr>
          </tbody></table></div><div v-else class="empty-results"><div class="empty-search"><Icon name="search"/></div><h3>{{busy?'正在查找文章':'暂时没有符合条件的文章'}}</h3><p v-if="!busy">可以减少一个筛选条件，或换个关键词再试。</p><button v-if="!busy" class="primary-button" @click="reset">清空筛选，查看全部</button></div></a-spin>
          <footer class="pagination"><span>共 {{total.toLocaleString()}} 篇<span v-if="rows.length"> · 显示 {{(page-1)*pageSize+1}}–{{Math.min(page*pageSize,total)}}</span></span><div><label class="page-size"><select v-model.number="pageSize" aria-label="每页篇数"><option :value="15">15 篇 / 页</option><option :value="30">30 篇 / 页</option><option :value="60">60 篇 / 页</option></select></label><button class="page-arrow" :disabled="busy||page<=1" @click="page--" aria-label="上一页"><Icon name="chevron"/></button><template v-for="(n,i) in pages" :key="n"><span v-if="i&&n-pages[i-1]>1" class="page-ellipsis">…</span><button class="page-number" :class="{active:page===n}" :disabled="busy" @click="page=n">{{n}}</button></template><button class="page-arrow next" :disabled="busy||page>=pageCount" @click="page++" aria-label="下一页"><Icon name="chevron"/></button></div></footer>
        </section><div class="page-footnote"><Icon name="shield"/>同一文件夹跨专业仅展示一条文章。参考层级依据画像标签，文件状态以目录最近核查为准。</div>
      </main>
    </div>
    <a-drawer v-model:open="opened" width="min(840px, 96vw)" :header-style="{display:'none'}" :body-style="{padding:0}" :mask-closable="!saving" :closable="false" :keyboard="!saving" destroy-on-close root-class-name="warehouse-drawer">
      <div v-if="detailBusy" class="detail-loading"><a-spin/><p>正在读取论文画像…</p></div><div v-if="detailError||notice||detail?.sync?.status==='failed'" class="detail-notices"><a-alert v-if="detailError" type="warning" show-icon :message="detailError"/><a-alert v-if="notice" type="info" show-icon :message="notice"/><a-alert v-if="detail?.sync?.status==='failed'" type="warning" show-icon message="修改已保存，新库同步未确认，可再次同步。"/></div><div v-if="!detailBusy&&!detail&&detailError" class="detail-empty"><button class="secondary-button" @click="open({id:openedId})">重新读取</button><button class="text-button" @click="opened=false">关闭</button></div>
      <template v-if="detail&&!detailBusy"><section v-if="editing" class="editing-panel"><div class="modal-header"><h2>修改文章资料</h2><button class="icon-button" :disabled="saving" @click="editing=false" aria-label="返回画像"><Icon name="close"/></button></div><a-form :model="form" layout="vertical" @finish="save"><a-form-item label="文章标题"><a-input v-model:value="form.title" :maxlength="1000" required/></a-form-item><a-form-item label="所属专业"><a-select v-model:value="form.majors" mode="multiple" show-search :options="options" option-filter-prop="label"/></a-form-item><a-form-item v-for="(label,key) in labels" :key="key" :label="label"><a-textarea v-model:value="form.fields[key]" :auto-size="{minRows:2,maxRows:9}"/></a-form-item><div class="edit-actions"><a-button type="primary" html-type="submit" :loading="saving">保存并同步</a-button><a-button :disabled="saving" @click="editing=false">取消</a-button></div><p class="subtle-note">尚未迁入新库的文章先保存修改，后续迁移时使用。</p></a-form></section><PaperProfile v-else :article="detail" :names="names" :saving="saving" @close="opened=false" @edit="edit" @sync="synchronize"/></template>
    </a-drawer>
    <a-modal v-model:open="guide" title="画像与筛选说明" :footer="null" width="590px"><p class="modal-intro">按已有画像与文件记录挑选参考。</p><div class="guide-item"><span>01</span><div><h3>专业：沿用已有分类</h3><p>同一篇论文可归入多个专业。选择专业后，再叠加层级和附件条件。</p></div></div><div class="guide-item"><span>02</span><div><h3>参考层级：提到可参考，就一起显示</h3><p>“重点本科（参照）”会出现在重点本科参考结果中，并保留现有主评级和参照说明。判断理由中的否定表述不作为参考标签；未单列主评级的论文不自动补评。</p></div></div><div class="guide-example"><span class="tag">主评级：普通本科</span><Icon name="arrow"/><span class="tag purple">重点本科参考 · 参照</span></div><div class="guide-item"><span>03</span><div><h3>文件条件：分别判断原文与附件</h3><p>“有可用附件”不代表原文已核实，也不代表附件一定是设计资料。附件格式仅匹配目录中已核实可用的附件文件。</p></div></div><div class="guide-item"><span>04</span><div><h3>层级待明确：保留原标签</h3><p>只有主题、使用人群等标签，无法映射到三档时，显示层级待明确。可在画像详情中查看原标签、判断依据和使用边界。</p></div></div></a-modal>
    <a-modal v-model:open="directory" title="专业目录" :footer="null" width="590px"><a-input v-model:value="directoryKeyword" placeholder="输入专业名称或代码" aria-label="搜索专业目录" allow-clear/><div class="major-directory"><button v-for="m in directoryMajors" :key="m.code" @click="major=m.code;directory=false"><span><strong>{{m.name}}</strong><small>{{m.code}}</small></span><Icon name="chevron"/></button></div></a-modal>
  </div>
</template>
