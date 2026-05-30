// Service Worker for Matrix Watcher PWA
//
// Caching strategy (deliberately conservative — dynamic data is never cached):
//   - /api/* and /ws        -> not handled by the SW at all; always live from the network.
//   - navigations / shell '/' -> network-first; the cached shell is used only as an
//                                offline fallback, and only full 200 responses are cached.
//   - other static assets    -> cache-first, with the network filling the cache on a miss.
//
// Bump CACHE_NAME whenever the shell or assets change: old caches are deleted on activate,
// so every client drops stale content on its next launch.
const CACHE_NAME = 'matrix-watcher-v12';
const SHELL = ['/', '/static/manifest.json', '/static/icon-192.png', '/static/icon-512.png'];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(SHELL)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(names => Promise.all(names.map(n => (n !== CACHE_NAME ? caches.delete(n) : null))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const req = event.request;
  const url = new URL(req.url);

  // Only manage same-origin GETs. Anything else goes straight to the network.
  if (req.method !== 'GET' || url.origin !== self.location.origin) return;

  // Live data must NEVER be served from cache — leave it entirely to the network.
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws')) return;

  // App shell / navigations: network-first, so a fresh deploy shows immediately.
  // Fall back to the cached shell only when the network is unavailable.
  if (req.mode === 'navigate' || url.pathname === '/') {
    event.respondWith(
      fetch(req)
        .then(res => {
          if (res && res.status === 200) {
            const copy = res.clone();
            caches.open(CACHE_NAME).then(c => c.put('/', copy));
          }
          return res;
        })
        .catch(() => caches.match('/'))
    );
    return;
  }

  // Other static assets: cache-first, fill the cache from the network on a miss.
  event.respondWith(
    caches.match(req).then(hit =>
      hit ||
      fetch(req).then(res => {
        if (res && res.status === 200) {
          const copy = res.clone();
          caches.open(CACHE_NAME).then(c => c.put(req, copy));
        }
        return res;
      })
    )
  );
});
