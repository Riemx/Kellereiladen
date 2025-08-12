const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer");
const robotsParser = require("robots-parser");

const START_DOMAIN = "https://www.kellereiladen.de";
const HOST = "www.kellereiladen.de";
const USER_AGENT = "KellereiladenSitemapBot/1.0 (+https://sitemap.kellereiladen.de)";

const MAX_DEPTH = 6;              // tiefer, um SPA-Routen zu erwischen
const CONCURRENCY = 2;
const WAIT_AFTER_LOAD = 900;
const CRAWL_DELAY = 250;
const MAX_PAGES = 20000;
const CHUNK_SIZE = 45000;
const INDEX_FILE = "sitemap_index.xml";
const PRODUCTS_CACHE = "products_seen.json";
const RETAIN_DAYS = 14;

// Produkt-URL-Muster & ISBN
const RE_ISBN_END  = new RegExp(`^https?://${HOST}/[^?#]*-\\d{13}(?:[/?#]|$)`, "i"); // /slug-978...
const RE_ISBN_ONLY = new RegExp(`^https?://${HOST}/\\d{13}(?:[/?#]|$)`, "i");         // /978...
const RE_ITEM_PATH = new RegExp(`^https?://${HOST}/shop/item/\\d{9,13}/`, "i");       // optional
const RE_ISBN13    = /\b97[89]\d{10}\b/g;                                             // 13-stellig, 978/979

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

function toProductUrlFromIsbn(isbn) {
  // sicher: Root-ISBN-Route existiert (z.B. /9783498007706)
  return `https://${HOST}/${isbn}`;
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
      lines.push(`  <sitemap>`,`    <loc>${loc}</loc>`,`    <lastmod>${today()}</lastmod>`,`  </sitemap>`);
    }
    for (const base of [
      "https://sitemap.kellereiladen.de/sitemap.xml",
      "https://sitemap.kellereiladen.de/sitemap_categories.xml",
      "https://sitemap.kellereiladen.de/sitemap_auto.xml",
    ]) {
      if (!existing.has(base)) lines.push(`  <sitemap>`,`    <loc>${base}</loc>`,`    <lastmod>${today()}</lastmod>`,`  </sitemap>`);
    }
  } else {
    for (const loc of [
      "https://sitemap.kellereiladen.de/sitemap.xml",
      "https://sitemap.kellereiladen.de/sitemap_categories.xml",
      "https://sitemap.kellereiladen.de/sitemap_auto.xml",
    ]) lines.push(`  <sitemap>`,`    <loc>${loc}</loc>`,`    <lastmod>${today()}</lastmod>`,`  </sitemap>`);
  }

  for (const f of files) {
    const loc = `https://sitemap.kellereiladen.de/${f}`;
    if (!existing.has(loc)) lines.push(`  <sitemap>`,`    <loc>${loc}</loc>`,`    <lastmod>${today()}</lastmod>`,`  </sitemap>`);
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

async function acceptCookies(page) {
  const texts = ["alle akzeptieren","akzeptieren","zustimmen","einverstanden","ich stimme zu"];
  try {
    await page.waitForTimeout(500);
    const clicked = await page.evaluate((texts) => {
      const candidates = [...document.querySelectorAll('button, [role="button"]')];
      for (const el of candidates) {
        const t = (el.textContent || "").toLowerCase();
        if (texts.some((x) => t.includes(x))) { el.click(); return true; }
      }
      const sel = ['#onetrust-accept-btn-handler', '.js-accept-cookies', '.ot-pc-refuse-all-handler'];
      for (const s of sel) { const e = document.querySelector(s); if (e) { e.click(); return true; } }
      return false;
    }, texts);
    if (clicked) await page.waitForTimeout(600);
  } catch {}
}

async function expandAndScroll(page) {
  for (let i = 0; i < 6; i++) {
    const clicked = await page.$$eval("button", (btns) => {
      let did = false;
      for (const b of btns) {
        const t = (b.textContent || "").trim().toLowerCase();
        if (t.includes("mehr anzeigen")) { b.click(); did = true; }
      }
      return did;
    });
    if (!clicked) break;
    await page.waitForTimeout(700);
  }
  for (let i = 0; i < 16; i++) {
    await page.evaluate(() => window.scrollBy(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
  }
  await page.waitForTimeout(WAIT_AFTER_LOAD);
}

function isInternal(url) {
  try { const u = new URL(url); return u.hostname === HOST && /^https?:$/.test(u.protocol); }
  catch { return false; }
}

function harvestIsbnsFromText(text) {
  const set = new Set();
  for (const m of text.matchAll(RE_ISBN13)) set.add(m[0]);
  return Array.from(set);
}

function harvestProductUrlsFromHtml(html) {
  const urls = new Set();
  // echte Links
  const urlRegex = new RegExp(`https?:\/\/${HOST}\/[^\\s"'<>]*?\\d{13}[^\\s"'<>]*`, "ig");
  for (const m of html.matchAll(urlRegex)) urls.add(m[0].split("#")[0].split("?")[0]);
  // nackte ISBN -> /978...
  for (const isbn of harvestIsbnsFromText(html)) urls.add(toProductUrlFromIsbn(isbn));
  return Array.from(urls);
}

async function main() {
  const robots = await fetchRobots();
  const seeds = parseSeedsFromCategoriesSitemap();
  const queue = [];
  const seen = new Set();
  const products = new Set();

  for (const s of seeds) queue.push({ url: s, depth: 0 });

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--incognito","--disable-blink-features=AutomationControlled"]
  });
  const context = await browser.createBrowserContext();

  let processed = 0, stop = false;

  async function worker() {
    while (queue.length && !stop) {
      const { url, depth } = queue.shift();
      if (seen.has(url)) continue;
      seen.add(url);
      if (!robots.isAllowed(url, USER_AGENT)) continue;
      if (depth > MAX_DEPTH) continue;

      try {
        const page = await context.newPage();
        await page.setUserAgent(USER_AGENT);
        await page.setRequestInterception(true);
        page.on("request", (req) => {
          const type = req.resourceType();
          if (["image","media","font"].includes(type)) return req.abort();
          req.continue();
        });

        // **XHR/JSON sniffen**: ISBNs & Produkt-URLs direkt aus Responses ziehen
        const sniffedUrls = new Set();
        const sniffedIsbns = new Set();
        page.on("response", async (res) => {
          try {
            const req = res.request();
            // nur XHR/Fetch/Document
            const type = req.resourceType();
            if (!["xhr","fetch","document"].includes(type)) return;
            const ct = res.headers()["content-type"] || "";
            if (!/json|html|text/i.test(ct)) return;

            const body = await res.text();
            // 1) URLs mit ISBN
            for (const u of harvestProductUrlsFromHtml(body)) sniffedUrls.add(u);
            // 2) nackte ISBNs -> /978...
            for (const isbn of harvestIsbnsFromText(body)) sniffedIsbns.add(isbn);
          } catch (_) {}
        });

        await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });
        await acceptCookies(page);
        await expandAndScroll(page);

        // DOM-Links + HTML-Quelle
        const hrefs = await page.$$eval("a[href]", (as) => as.map((a) => a.href).filter(Boolean));
        const html = await page.content();
        const htmlUrls = harvestProductUrlsFromHtml(html);

        // alles zusammenführen
        const allCandidates = new Set([
          ...hrefs.map((h) => h.split("#")[0].split("?")[0]),
          ...htmlUrls,
          ...Array.from(sniffedUrls),
          ...Array.from(sniffedIsbns).map(toProductUrlFromIsbn),
        ]);

        for (const h of allCandidates) {
          if (!isInternal(h)) continue;
          const clean = h.split("#")[0].split("?")[0];
          if (looksLikeProduct(clean)) products.add(clean);
        }

        // Pagination + weitere Kategoriepfade
        const nexts = await page.$$eval('a[rel="next"]', (as) => as.map((a) => a.href).filter(Boolean));
        for (const n of nexts) if (isInternal(n)) queue.push({ url: n, depth: depth + 1 });
        for (const h of hrefs) {
          if (isInternal(h) && /^https?:\/\/www\.kellereiladen\.de\/buecher/i.test(h)) {
            queue.push({ url: h.split("#")[0], depth: depth + 1 });
          }
        }

        await page.close();
      } catch (_) {
        // ignore
      } finally {
        processed++;
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

  // immer mindestens eine Datei schreiben
  const files = [];
  const chunks = chunk(urls.length ? urls : [START_DOMAIN], CHUNK_SIZE);
  chunks.forEach((arr, i) => {
    const name = i === 0 ? "sitemap_products.xml" : `sitemap_products_${i}.xml`;
    fs.writeFileSync(name, buildUrlset(urls.length ? arr : [] , lastmods), "utf-8");
    files.push(name);
  });

  ensureIndexIncludes(files);
  fs.writeFileSync(PRODUCTS_CACHE, JSON.stringify(cache, null, 2), "utf-8");
  console.log(`Products found: ${urls.length} | Files: ${files.join(", ")}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
