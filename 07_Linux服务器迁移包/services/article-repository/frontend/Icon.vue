<script setup>
defineProps({name:String});
const paths={
  book:'M4 4.5h6a3 3 0 0 1 3 3V21a4 4 0 0 0-3-1.5H4z M20 4.5h-4a3 3 0 0 0-3 3V21a4 4 0 0 1 3-1.5h4z',
  grid:'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
  folder:'M3 7a2 2 0 0 1 2-2h5l2 3h7a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z',
  file:'M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z M14 3v6h6 M8 13h8 M8 17h5',
  search:'M17 10.5a6.5 6.5 0 1 1-13 0 6.5 6.5 0 0 1 13 0 M16 16l4.5 4.5',
  chevron:'m9 5 7 7-7 7',down:'m6 9 6 6 6-6',arrow:'M4 12h15m-5-5 5 5-5 5',
  close:'m6 6 12 12M6 18 18 6',check:'m5 12 4 4L19 6',
  clip:'m8 13 7-7a3 3 0 0 1 4 4L9 20a5 5 0 0 1-7-7L12 3 M6 15l9-9',
  info:'M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0 M12 11v5m0-9v.1',
  filter:'M4 6h16 M7 12h10 M10 18h4',reset:'M3 10a9 9 0 1 1 2 8 M3 4v6h6',
  download:'M12 3v12m-5-5 5 5 5-5 M4 16v4h16v-4',
  layers:'m12 3 10 5-10 5L2 8z M2 12l10 5 10-5 M2 16l10 5 10-5',
  sparkle:'m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5z',
  shield:'m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6z M8 12l3 3 5-6',
};
</script>
<template><svg class="icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path :d="paths[name]||paths.file"/></svg></template>
