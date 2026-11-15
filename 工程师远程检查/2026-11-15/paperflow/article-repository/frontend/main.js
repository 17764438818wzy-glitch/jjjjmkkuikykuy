import { createApp } from 'vue';
import Antd from 'ant-design-vue';
import 'ant-design-vue/dist/reset.css';
import ArticleRepository from './ArticleRepository.vue';
createApp(ArticleRepository).use(Antd).mount('#app');
