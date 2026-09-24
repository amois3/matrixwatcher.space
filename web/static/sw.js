// Service Worker for Matrix Watcher PWA
//
// Caching strategy (deliberately conservative — dynamic data is never cached):
//   - /api/* and /ws        -> not handled by the SW at all; always live from the network.
//   - navigations / shell '/' -> network-first; the cached shell is used only as an
//                                offline fallback, and only full 200 responses are cached.
//   - other static assets    -> cache-first, with the network filling the cache on a miss.
//
// Bump CACHE_NAME and the /sw.js?v=... registration URL whenever the shell or assets
// change: old caches are deleted on activate and the new URL bypasses CDN copies.
const CACHE_NAME = 'matrix-watcher-v28';
const SHELL = ['/', '/static/manifest.json', '/static/icon-192.png', '/static/icon-512.png', '/static/notification-badge.png'];

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

self.addEventListener('push', event => {
  let message = {};
  try { message = event.data ? event.data.json() : {}; } catch (_) {}
  const title = String(message.title || 'Matrix Watcher update');
  const body = String(message.body || 'Open the site to review the latest observation.');
  const path = typeof message.url === 'string' && message.url.startsWith('/') &&
    !message.url.startsWith('//') ? message.url : '/';
  event.waitUntil(self.registration.showNotification(title, {
    body, icon: '/static/icon-192.png', badge: '/static/notification-badge.png',
    tag: String(message.tag || 'matrix-watcher-update'),
    data: { url: path }
  }));
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const path = event.notification.data?.url || '/';
  const url = new URL(path, self.location.origin).href;
  event.waitUntil((async () => {
    const windows = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const windowClient of windows) {
      if (windowClient.url.startsWith(self.location.origin) && 'focus' in windowClient) {
        await windowClient.navigate(url);
        return windowClient.focus();
      }
    }
    return clients.openWindow(url);
  })());
});

self.addEventListener('pushsubscriptionchange', event => {
  event.waitUntil((async () => {
    try {
      // A manual unsubscribe also changes the browser subscription. Rejoin
      // only when the old endpoint is still opted in on the server.
      if (!event.oldSubscription) return;
      const statusResponse = await fetch('/api/push/subscriptions/status', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ endpoint: event.oldSubscription.endpoint })
      });
      if (!statusResponse.ok) return;
      const status = await statusResponse.json();
      if (!status.subscribed || !Array.isArray(status.topics)) return;
      const response = await fetch('/api/push/config', { cache: 'no-store' });
      const config = await response.json();
      if (!config.public_key) return;
      const padded = config.public_key.replace(/-/g, '+').replace(/_/g, '/')
        .padEnd(Math.ceil(config.public_key.length / 4) * 4, '=');
      const key = Uint8Array.from(atob(padded), character => character.charCodeAt(0));
      const subscription = await self.registration.pushManager.subscribe({
        userVisibleOnly: true, applicationServerKey: key
      });
      const saved = await fetch('/api/push/subscriptions', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subscription: subscription.toJSON(), topics: status.topics })
      });
      if (saved.ok) {
        await fetch('/api/push/subscriptions', {
          method: 'DELETE', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ endpoint: event.oldSubscription.endpoint })
        });
      }
    } catch (_) {
      // The page retries registration when it is opened again.
    }
  })());
});
