import { build } from 'esbuild';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';

const root = process.cwd();
await build({ entryPoints: ['src/query-runtime.mjs'], bundle: true, minify: true, format: 'iife',
  platform: 'browser', define: { 'process.env.NODE_ENV': '"production"' }, outfile: 'vendor/query-runtime.js' });

const html = await readFile('index.html', 'utf8');
const urls = [...html.matchAll(/(?:src|href)="([^"]+)"/g)].map((match) => match[1])
  .filter((url) => !/^(?:https?:|\/\/|#)/.test(url));
const manifest = await Promise.all(['index.html', ...urls].map(async (url) => {
  const file = path.join(root, url.split('?')[0]);
  const revision = createHash('sha256').update(await readFile(file)).digest('hex');
  return { url, revision };
}));
await build({ entryPoints: ['src/service-worker.mjs'], bundle: true, minify: true, format: 'iife',
  platform: 'browser', define: { 'self.__PRECACHE_MANIFEST': JSON.stringify(manifest),
    'process.env.NODE_ENV': '"production"' }, outfile: 'service-worker.js' });
console.log(`Built Query bundle and Workbox service worker with ${manifest.length} static resources.`);
