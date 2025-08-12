const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer");
const robotsParser = require("robots-parser");

const START_DOMAIN = "https://www.kellereiladen.de";
const HOST = "www.kellereiladen.de";
const USER_AGENT = "KellereiladenSitemapBot/1.0 (+https://sitemap.kellereiladen.de)";

const MAX_DEPTH = 4;
const CONCURRENCY = 2;
const WAIT_AFTER_LOAD = 800;
const CRAWL_DELAY = 250;
const MAX_PAGES = 20000;
const CHUNK_SIZE = 45000;
const INDEX_FILE = "sitemap_index.xml";
const PRODUCTS_CACHE = "products_seen.json";
const RETAIN_DAYS = 14;

const RE_ISBN_END  = new RegExp(`^https?://${HOST}/[^?#]*-\\d{10,13}(?:[/?#]|$)`, "i");
const RE_ISBN_ONLY = new RegExp(`^https?://${HOST}/\\d{10,13}(?:[/?#]|$)`, "i");
const RE_ITEM_PATH = new RegExp(`^https?://${HOST}/shop/item/\\d{9,13}/`, "i");
const ALLOW_SEED = [/^https?:\/\/www\.kellereiladen\.de\/buecher/i];

const today = () => new Date().toISOString().slice(0, 10);

function parseSeedsFromCategoriesSitemap() {
  try {
    const xml = fs.readFileSync("sitemap_categories.xml", "utf-8");
    const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
    const seeds = locs.filter((u) => ALLOW_SEED.some((r) => r.test(u)));
    if (seeds.length) return Array.from(new Set(seeds));
  } catch (_) {}
  return [
    `${START_DOMAIN}/buecher-romane`,
    `${START_DOMAIN}/buecher-krimi-und-thriller`,
    `${START_DOMAIN}/buecher-historische-romane`,
    `${START_DOMAIN}/buecher-fantasy`,
  ];
}

function looksLikeProduct(url) {
  return RE_ISBN_END.test(url) || RE_ISBN_ONLY.test(url) || RE_ITEM_PATH.test(url);
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function buildUrlset(urls, lastmods) {
  const lm = (u) => (lastmods[u] ? lastmods[u] : today());
  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
    urls.map((u) => `  <url>\n    <loc>${u}</loc>\n    <lastmod>${lm(u)}</lastmod>\n  </url>`).join("\n") +
    `\n</urlset>\n`
  );
}

function ensureIndexIncludes(files) {
  const idxPath = path.resolve(INDEX_FILE);
  let content = "";
  try { content = fs.readFileSync(idxPath, "utf-8"); } catch (_) {}

  const existing = new Set([...content.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]));
  const lines = [
    `<?xml version="1.0" encoding="UTF-8"?>`,
    `<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`,
  ];

  if (existing.size) {
    for (const loc of existing) {
      lines.push(
        `  <sitemap>`,
        `    <loc>${loc}</loc>`,
        `    <lastmod>${today()}</lastmod>`,
        `  </sitemap>`
      );
    }
    // sicherstellen, dass Basis-Sitemaps enthalten sind
    for (const base of [
      "https://sitemap.kellereiladen.de/sitemap.xml",
      "https://sitemap.kellereiladen.de/sitemap_categories.xml",
      "https://sitemap.kellereiladen.de/sitemap_auto.xml",
    ]) {
      if (!existing.has(base)) {
        lines.push(
          `  <sitemap>`,
          `    <loc>${base}</loc>`,
          `    <lastmod>${today()}</lastmod>`,
          `  </sitemap>`
        );
      }
    }
  } else {
    // neuer Index
    for (const loc of [
      "https://sitemap.kellereiladen.de/sitemap.xml",
      "https://sitemap.kellereiladen.de/sitemap_categories.xml",
      "https://sitemap.kellereiladen.de/sitemap_auto.xml"
    ]) {
      lines.push(
        `  <sitemap>`,
        `    <loc>${loc}</loc>`,
        `    <lastmod>${today()}</lastmod>`,
        `  </sitemap>`
      );
    }
  }

  // Produkt-Sitemaps hinzufügen
  for (const f of files) {
    const loc = `https://sitemap.kellereiladen.de/${f}`;
    if (!existing.has(loc)) {
      lines.push(
        `  <sitemap>`,
        `    <loc>${loc}</loc>`,
        `    <lastmod>${today()}</lastmod>`,
        `  </sitemap>`
      );
    }
  }

  lines.push(`</sitemapindex>`);
  fs.writeFileSync(idxPath, lines.join("\n"), "utf-8");
}

async function fetchRobots() {
  try {
    const res = await fetch(`${START_DOMAIN}/robots.txt`, { headers: { "User-Agent": USER_AGENT } });
    const txt = await res.text();
    return robotsParser(`${START_DOMAIN}/robots.txt`, txt);
  } catch {
    return robotsParser(`${START_DOMAIN}/robots.txt`, "");
  }
}

async function expandAndScroll(page) {
  for (let i = 0; i < 4; i++) {
    const clicked = await page.$$eval("button", (btns) => {
      let did = false;
      for (const b of btns) {
        const t = (b.textContent || "").trim().toLowerCase();
        if (t.includes("mehr anzeigen")) { b.click(); did = true; }
      }
      return did;
    });
    if (!clicked) break;
    await page.waitForTimeout(600);
  }
  for (let i = 0; i < 12; i++) {
    await page.evaluate(() => window.scrollBy(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
  }
  await page.waitForTimeout(WAIT_AFTER_LOAD);
}

function isInternal(url) {
  try {
    const u = new URL(url);
    return u.hostname === HOST && (u.protocol === "https:" || u.protocol === "http:");
  } catch { return false; }
}

async function main() {
  const robots = await fetchRobots();
  const seeds = parseSeedsFromCategoriesSitemap();
  const queue = [];
  const seen = new Set();
  const products = new Set();

  for (const s of seeds) queue.push({ url: s, depth: 0 });

  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox", "--disable-setuid-sandbox", "--incognito"] });
  const context = await browser.createBrowserContext();

  let active = 0, processed = 0, stop = false;

  async function worker() {
    while (queue.length && !stop) {
      const { url, depth } = queue.shift();
      if (seen.has(url)) continue;
      seen.add(url);

      if (!robots.isAllowed(url, USER_AGENT)) continue;
      if (depth > MAX_DEPTH) continue;

      active++;
      try {
        const page = await context.newPage();
        await page.setUserAgent(USER_AGENT);
        await page.setRequestInterception(true);
        page.on("request", (req) => {
          const type = req.resourceType();
          if (["image","media","font"].includes(type)) return req.abort();
          req.continue();
        });

        await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });
        await expandAndScroll(page);

        const hrefs = await page.$$eval("a[href]", (as) => as.map((a) => a.href).filter(Boolean));
        const nexts = await page.$$eval('a[rel="next"]', (as) => as.map((a) => a.href).filter(Boolean));

        for (const h of hrefs) { if (isInternal(h) && looksLikeProduct(h)) products.add(h.split("#")[0].split("?")[0]); }
        for (const n of nexts)   { if (isInternal(n)) queue.push({ url: n, depth: depth + 1 }); }
        for (const h of hrefs)   { if (isInternal(h) && /^https?:\/\/www\.kellereiladen\.de\/buecher/i.test(h)) queue.push({ url: h.split("#")[0], depth: depth + 1 }); }

        await page.close();
      } catch (_) { /* ignore */ }
      finally {
        active--; processed++;
        if (processed >= MAX_PAGES) stop = true;
        await new Promise((r) => setTimeout(r, CRAWL_DELAY));
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  await context.close(); await browser.close();

  // Cache laden/aktualisieren
  let cache = {};
  try { cache = JSON.parse(fs.readFileSync(PRODUCTS_CACHE, "utf-8")); } catch {}
  const todayStr = today();
  for (const u of products) {
    cache[u] = cache[u] || {};
    cache[u].last_seen = todayStr;
    cache[u].lastmod = cache[u].lastmod || todayStr;
  }
  // Gnadenfrist
  const keep = {};
  for (const [u, meta] of Object.entries(cache)) {
    const ls = meta.last_seen || "1970-01-01";
    const age = (new Date(todayStr) - new Date(ls)) / (1000*60*60*24);
    if (age <= RETAIN_DAYS) keep[u] = meta;
  }
  cache = keep;

  const urls = Object.keys(cache).sort();
  const lastmods = Object.fromEntries(urls.map((u) => [u, cache[u].lastmod || cache[u].last_seen || todayStr]));

  const chunks = chunk(urls, CHUNK_SIZE);
  const files = [];
  chunks.forEach((arr, i) => {
    const name = i === 0 ? "sitemap_products.xml" : `sitemap_products_${i}.xml`;
    fs.writeFileSync(name, buildUrlset(arr, lastmods), "utf-8");
    files.push(name);
  });

  ensureIndexIncludes(files);
  fs.writeFileSync(PRODUCTS_CACHE, JSON.stringify(cache, null, 2), "utf-8");
  console.log(`Products: ${urls.length} | Files: ${files.join(", ")}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
