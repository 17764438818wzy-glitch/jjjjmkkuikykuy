<script setup>
import {computed,ref,watch} from 'vue';
import Icon from './Icon.vue';
import ReferenceTags from './ReferenceTags.vue';
import ArchiveCard from './ArchiveCard.vue';
const props=defineProps({article:Object,names:Object,saving:Boolean});
defineEmits(['close','edit','sync']);
const tab=ref('overview');
watch(()=>props.article.id,()=>tab.value='overview');
const profile=computed(()=>props.article.profile||{});
const fields=computed(()=>props.article.fields||{});
const reference=computed(()=>props.article.reference||{});
const keywords=computed(()=>String(fields.value.keywords||'').split(/[;；、\n]/).map(s=>s.trim()).filter(Boolean));
const pathParts=computed(()=>String(profile.value.path||'').split(/→|—>/).map(s=>s.trim()).filter(Boolean));
const units=computed(()=>profile.value.units||[]);
const gapType=type=>({source_not_stated:'原文未交代',extraction_missing:'提取缺失',uncertain:'待核实',conflict:'存在冲突',not_read:'尚未读取'}[type]||'使用限制');
const migration={complete:'已迁入新库',not_migrated:'未完成迁移',unverified:'迁移待核实'};
</script>
<template>
  <section class="paper-detail">
    <header class="detail-heading">
      <div class="detail-eyebrow"><span><Icon name="sparkle"/>论文画像</span><button class="icon-button" :disabled="saving" aria-label="关闭详情" @click="$emit('close')"><Icon name="close"/></button></div>
      <h2>{{article.title}}</h2>
      <div class="detail-major"><span v-if="article.year">稿件年份 {{article.year}}</span><span v-for="code in article.majors" :key="code">{{names[code]||code}}<small>{{code}}</small></span></div>
      <ReferenceTags :reference="reference"/>
      <div v-if="reference.primary" class="primary-rating">主评级：<strong>{{reference.primary}}</strong><span v-if="reference.reference_only"> · 重点本科仅作参照</span></div>
      <div class="detail-management"><span>{{migration[article.migration]}}</span><button :disabled="saving" @click="$emit('edit')">修改资料</button><button v-if="article.manually_edited&&article.migration==='complete'" :disabled="saving" @click="$emit('sync')">{{saving?'正在同步…':'同步新库'}}</button></div>
    </header>
    <nav class="detail-tabs" aria-label="画像内容"><button v-for="t in [['overview','画像概览'],['process','研究过程'],['reuse','可借鉴与限制'],['files','原文与附件']]" :key="t[0]" :class="{active:tab===t[0]}" :aria-current="tab===t[0]?'page':undefined" @click="tab=t[0]">{{t[1]}}<span v-if="t[0]==='files'&&article.files.length">{{article.files.length}}</span></button></nav>
    <div class="detail-content" :key="article.id+tab">
      <a-alert v-if="article.profile_notice" type="info" show-icon :message="article.profile_notice" class="notice"/>
      <template v-if="tab==='overview'">
        <section class="content-section"><h3>{{fields.summary?'画像摘要':'研究问题'}}</h3><p class="summary-copy">{{fields.summary||profile.question||'此画像尚未记录摘要或研究问题。'}}</p></section>
        <div v-if="keywords.length" class="keyword-list"><span v-for="k in keywords" :key="k"># {{k}}</span></div>
        <div v-if="fields.object||fields.audience||profile.audience" class="info-grid"><div v-if="fields.object"><h4>研究对象</h4><p>{{fields.object}}</p></div><div v-if="fields.audience||profile.audience"><h4>适用人群</h4><p>{{fields.audience||profile.audience}}</p></div></div>
        <section v-if="profile.learn" class="learn-highlight"><div class="section-title"><Icon name="book"/><h3>这篇可以学什么</h3></div><p>{{profile.learn}}</p></section>
        <section v-if="fields.methods" class="content-section"><h3>研究方法</h3><p class="summary-copy">{{fields.methods}}</p></section>
        <section class="content-section rating-section"><h3>参考层级依据 <span class="small-caption">来自现有画像</span></h3><p v-if="!reference.primary" class="subtle-note">当前画像未记录单独的整篇主评级；上方展示可参考档位。</p><p v-if="reference.manually_edited" class="subtle-note">参考标签经过人工修订，下方依据保留原画像内容。</p><div class="raw-labels"><span v-for="l in reference.labels" :key="l">{{l}}</span><span v-if="!reference.labels?.length">参考标签待明确</span></div><details v-if="reference.reason"><summary>展开画像中的完整判断依据 <Icon name="down"/></summary><p>{{reference.reason}}</p></details></section>
        <p class="source-folder">来源文件夹：{{article.folder_name}}</p>
        <div class="detail-footnote"><Icon name="info"/>参考层级描述借鉴范围，不代表原作者的学历或学习形式。</div>
      </template>
      <template v-else-if="tab==='process'">
        <section v-if="profile.path" class="content-section"><h3>整体研究路线</h3><ol class="path-list"><li v-for="(part,i) in pathParts" :key="i"><span>{{String(i+1).padStart(2,'0')}}</span><p>{{part}}</p></li></ol></section>
        <p v-if="profile.legacy_summary" class="subtle-note">当前为历史汇总画像，尚未载入逐步取材、操作与产出。</p>
        <section v-if="units.length" class="content-section"><div class="section-title between"><h3>研究学习单元</h3><span class="small-caption">{{units.length}} 个单元</span></div><details v-for="(unit,i) in units" :key="unit.id" class="unit" :open="i===0"><summary><span class="unit-number">{{String(i+1).padStart(2,'0')}}</span><strong>{{unit.title}}</strong><span v-if="unit.core" class="core-tag">核心</span><Icon name="down"/></summary><div class="unit-body"><div v-for="entry in [['研究问题',unit.question],['可学习内容',unit.learn],['方法与证据',unit.method||unit.evidence],['取材',unit.materials.join('；')],['实际操作',unit.steps.join('；')],['产出',unit.outputs.join('；')],['如何解释',unit.interpretation],['使用限制',unit.limits.join('；')]]" v-show="entry[1]" :key="entry[0]" class="unit-field"><h4>{{entry[0]}}</h4><p>{{entry[1]}}</p></div><div v-if="unit.location" class="evidence-location">出处：{{unit.location}}</div></div></details></section>
        <div v-if="!profile.path&&!units.length" class="detail-empty"><Icon name="layers"/><h3>研究过程尚未记录</h3><p>当前画像没有可展示的研究路线或学习单元。</p></div>
      </template>
      <template v-else-if="tab==='reuse'">
        <section v-if="profile.learn" class="learn-highlight"><div class="section-title"><Icon name="check"/><h3>可借鉴内容</h3></div><p>{{profile.learn}}</p></section>
        <section v-if="profile.needs?.length" class="content-section"><h3>换题后需要重新准备</h3><ul class="plain-list"><li v-for="(n,i) in profile.needs" :key="i">{{n}}</li></ul></section>
        <section v-if="profile.limits?.length" class="content-section"><h3>使用边界</h3><ul class="plain-list amber-list"><li v-for="(limit,i) in profile.limits" :key="i">{{limit}}</li></ul></section>
        <section v-if="profile.gaps?.length" class="content-section"><h3>证据缺口与不确定项</h3><div v-for="(g,i) in profile.gaps" :key="i" class="gap"><span class="gap-type">{{gapType(g.type)}}</span><p>{{g.text}}</p><p v-if="g.impact" class="gap-impact">{{g.impact}}</p></div></section>
        <div v-if="!profile.limits?.length&&!profile.needs?.length&&!profile.gaps?.length" class="detail-empty"><Icon name="shield"/><h3>使用边界尚未记录</h3><p>未记录不等于没有限制，使用时仍需结合原文核对。</p></div>
      </template>
      <ArchiveCard v-else-if="tab==='files'" :article="article"/>
    </div>
  </section>
</template>
