/*
 * Service Worker for The Crawl Street Journal PWA.
 *
 * Caches the app shell (HTML, CSS, JS, icons) so the UI loads quickly
 * and displays an offline fallback when the Flask server is unreachable.
 * Crawl data requests are always network-first since they need live data.
 */

const CACHE_NAME = "csj-v2";

/* App-shell resources to pre-cache on install. */
const APP_SHELL = [
  "/",
  "/static/img/favicon.svg",
  "/static/img/icon-192.png",
  "/static/img/icon-512.png",
  "/static/manifest.json"
];

/* ── Install: pre-cache the app shell ────────────────────────────────── */
self.addEventListener("install", function (event) {
  event.waitUntil(
    caches.open(CACHE_NAME).then(function (cache) {
      return cache.addAll(APP_SHELL);
    })
  );
  /* Activate immediately rather than waiting for existing tabs to close. */
  self.skipWaiting();
});

/* ── Activate: clean up old caches ───────────────────────────────────── */
self.addEventListener("activate", function (event) {
  event.waitUntil(
    caches.keys().then(function (keys) {
      return Promise.all(
        keys
          .filter(function (key) { return key !== CACHE_NAME; })
          .map(function (key) { return caches.delete(key); })
      );
    })
  );
  self.clients.claim();
});

/* ── Fetch: network-first with cache fallback ────────────────────────── */
self.addEventListener("fetch", function (event) {
  /* Only handle GET requests; let POST/PUT/DELETE pass through. */
  if (event.request.method !== "GET") return;

  /* SSE streams and API endpoints should never be cached. */
  var url = new URL(event.request.url);
  if (url.pathname.startsWith("/api/")) return;

  /*
   * Never intercept loopback: the desktop GUI is always served from
   * localhost/127.0.0.1. Caching HTML there caused slow or stale in-app
   * navigation (especially on Windows / Edge WebView2 and browser fallback).
   */
  if (url.hostname === "localhost" || url.hostname === "127.0.0.1") return;

  event.respondWith(
    fetch(event.request)
      .then(function (response) {
        /* Cache a clone of successful responses for offline use. */
        if (response && response.status === 200) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then(function (cache) {
            cache.put(event.request, clone);
          });
        }
        return response;
      })
      .catch(function () {
        /* Network unavailable — try the cache. */
        return caches.match(event.request);
      })
  );
});
