import { precacheAndRoute } from 'workbox-precaching';
import { clientsClaim } from 'workbox-core';

precacheAndRoute(self.__PRECACHE_MANIFEST);
self.skipWaiting();
clientsClaim();
