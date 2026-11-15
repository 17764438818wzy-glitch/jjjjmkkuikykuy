<script setup>
defineProps({reference:Object});
const names={correspondence:'函授参考',bachelor:'本科参考',key_bachelor:'重点本科参考'};
</script>
<template><div class="level-tags"><span v-for="level in reference?.levels||[]" :key="level" class="tag level-tag" :class="{purple:level==='key_bachelor',cyan:level==='correspondence'}">{{names[level]}}<span v-if="level==='key_bachelor'&&reference.reference_only" class="ref-note">参照</span></span><span v-if="!reference?.levels?.length" class="tag muted-tag">层级待明确</span></div></template>
