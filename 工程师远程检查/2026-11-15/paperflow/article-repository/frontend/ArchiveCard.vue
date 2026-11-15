<script setup>
import Icon from './Icon.vue';
defineProps({article:Object});
const availability={available:'目录核查可用',missing:'原件当前不可用',unknown:'待核实'};
const size=n=>n==null?'大小未记录':n<1048576?Math.ceil(n/1024)+' KB':(n/1048576).toFixed(1)+' MB';
const ext=file=>String(file.name||'').split('.').at(-1)?.toUpperCase()||'FILE';
</script>
<template>
  <section class="archive-card">
    <div class="file-section-header"><div><h3>原文与附件</h3><p>原文、附件分别展示，方便挑选。</p></div><a v-if="article.files?.some(f=>f.availability==='available')" :href="'/api/library/articles/'+article.id+'/bundle'" class="secondary-button"><Icon name="download"/>下载整包 ZIP</a></div>
    <p v-if="!article.inventory_complete||article.files?.some(f=>f.availability!=='available')" class="archive-note">资料尚未全部确认。可下载已确认的文件，整包会标注不完整。</p>
    <div v-for="file in article.files" :key="file.id" class="file-card"><div class="file-type" :class="ext(file).toLowerCase()">{{ext(file).slice(0,6)}}</div><div class="file-card-info"><span class="file-kind">{{file.kind}} · {{size(file.bytes)}}</span><h4>{{file.name}}</h4><span :class="file.availability==='available'?'file-ok':'file-unknown'"><span class="status-dot"></span>{{availability[file.availability]||'待核实'}}</span></div><a v-if="file.availability==='available'" class="icon-button download-button" :href="file.url" :download="file.name" :aria-label="'下载'+file.name"><Icon name="download"/></a></div>
    <div v-if="!article.files?.length" class="detail-empty"><Icon name="folder"/><h3>文件清单待核实</h3><p>当前未登记原文或附件位置，不能据此认定文件缺失。</p></div>
    <div class="detail-footnote"><Icon name="info"/>可用状态来自目录最近核查；附件格式不等于附件内容类型。下载时会再次校验原件。</div>
  </section>
</template>
