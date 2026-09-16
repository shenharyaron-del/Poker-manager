// Minimal PWA service worker - only exists to satisfy installability criteria and give a
// basic offline fallback if the network briefly drops, NOT to serve live game data
// offline (there is none to serve without the server - everything is shared state).
//
// Network-first for everything: always try the network first and only fall back to the
// cache if that fails. Never serve a stale cached copy while online - this app deploys
// often, and index.html already sends Cache-Control: no-cache specifically so a phone
// picks up a new deploy on next reload instead of silently reusing old JS. A cache-first
// service worker would quietly defeat that same protection, so this deliberately doesn't
// do that.
const CACHE_NAME = "poker-manager-shell-v1";
const SHELL_URLS = ["/", "/static/manifest.json", "/static/icon-192.png", "/static/icon-512.png"];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((names) =>
      Promise.all(names.filter((n) => n !== CACHE_NAME).map((n) => caches.delete(n)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  // Never touch API calls (always need live data) or non-GET requests (saves etc.).
  if (event.request.method !== "GET" || url.pathname.startsWith("/api/")) return;
  event.respondWith(
    fetch(event.request)
      .then((res) => {
        const copy = res.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy)).catch(() => {});
        return res;
      })
      .catch(() => caches.match(event.request))
  );
});
