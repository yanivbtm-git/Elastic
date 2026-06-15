const CACHE_NAME = "ai-job-tracker-v1";
const CORE_ASSETS = ["./", "./index.html", "./manifest.webmanifest"];

function shouldUseNetworkFirst(request) {
  const url = new URL(request.url);
  if (request.mode === "navigate") return true;
  if (url.pathname.endsWith("/index.html")) return true;
  if (url.pathname.endsWith("/sw.js")) return true;
  return false;
}

async function fetchAndCache(request) {
  const response = await fetch(request);
  if (response && response.status === 200 && response.type === "basic") {
    const clone = response.clone();
    const cache = await caches.open(CACHE_NAME);
    await cache.put(request, clone);
  }
  return response;
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(CORE_ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key !== CACHE_NAME)
          .map((key) => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;
  event.respondWith(
    (async () => {
      if (shouldUseNetworkFirst(event.request)) {
        try {
          return await fetchAndCache(event.request);
        } catch {
          return (await caches.match(event.request)) || (await caches.match("./index.html"));
        }
      }

      const cached = await caches.match(event.request);
      if (cached) return cached;
      try {
        return await fetchAndCache(event.request);
      } catch {
        return caches.match("./index.html");
      }
    })()
  );
});
