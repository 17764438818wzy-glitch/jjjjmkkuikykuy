import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
export default defineConfig({
  plugins:[vue()],
  resolve:{alias:{'ant-design-vue':'F:/论文预处理系统/看板/frontend/apps/web-antd/node_modules/ant-design-vue'}},
  build:{outDir:'dist',emptyOutDir:true},
});
