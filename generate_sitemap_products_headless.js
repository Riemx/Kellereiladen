const fs = require("fs");
const path = require("path");
const robotsParser = require("robots-parser");

const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(StealthPlugin());

// ======= Basiskonfiguration =======
const START_DOMAIN = "https://www.kellereiladen.de";
const HOST = "www.kellereiladen.de";
const UA =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// Crawl-Parameter
const MAX_DEPTH = 6;
const CONCURRENCY = 2;
const WAIT_AFTER_LOAD = 1500;
const CRAWL_DELAY = 250; // Basispause
const MAX_PAGES = 20000;

// Sitemap/Cache
const CHUNK_SIZE = 45000;
const PRODUCTS_CACHE = "products_seen.json";

// ======= Finetuning (mit ENV-Override) =======
const RETAIN_DAYS = Number(process.env.KL_RETAIN_DAYS || 21);
const REFRESH_AFTER_DAYS = Number(process.env.KL_REFRESH_AFTER_DAYS || 10);
const REFRESH_CONCURRENCY = Number(process.env.KL_REFRESH_CONCURRENCY || 6);
const REFRESH_TIMEOUT_MS = Number(process.env.KL_REFRESH_TIMEOUT_MS || 12000);
const RESPECT_ROBOTS = /^(1|true|yes)$/i.test(process.env.KL_RESPECT_ROBOTS || "false");

// Basis-URL für Index (aus Workflow-ENV)
const BASE = process.env.SITEMAP_BASE || "https://sitemap.kellereiladen.de";

// Seeds (Kategorien)
const ALLOW_SEED = [/^https?:\/\/www\.kellereiladen\.de\/buecher/i];

// Produktmuster/ISBN
const RE_ISBN13 = /\b97[89]\d{10}\b/g;
const RE_PROD_END = new RegExp(
  `^https?://${HOST}/[^?#]*-\\d{13}(?:[/?#]|$)`,
  "i"
);
const RE_PROD_ONLY = new RegExp(
  `^https?://${HOST}/\\d{13}(?:[/?#]|$)`,
  "i"
);
const RE_ITEM_PATH = new RegExp(
  `^https?://${HOST}/shop/item/\\d{9,13}/`,
  "i"
);

// ======= Utils =======
const today = () => new Date().toISOString().slice(0, 10);
const sleep = (ms) =>
  new Promise((r) => setTimeout(r, ms + Math.floor(Math.random() * 120))); // kleiner Jitter

function looksLikeProduct(url) {
  return (
    RE_PROD_END.test(url) || RE_PROD_ONLY.test(url) || RE_ITEM_PATH.test(url)
  );
}
function toProductUrlFromIsbn(isbn) {
  return `https://${HOST}/${isbn}`;
}

function parseSeedsFromCategoriesSitemap() {
  try {
    const xml = fs.readFileSync("sitemap_categories.xml", "utf-8");
    const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
    const seeds = locs.filter((u) => ALLOW_SEED.some((r) => r.test(u)));
    if (seeds.length) return Array.from(new Set(seeds));
  } catch {}
  return [
    `${START_DOMAIN}/buecher-romane`,
    `${START_DOMAIN}/buecher-krimi-und-thriller`,
    `${START_DOMAIN}/buecher-historische-romane`,
    `${START_DOMAIN}/buecher-fantasy`,
    `${START_DOMAIN}/buecher-liebesromane`,
  ];
}

function isInternal(url) {
  try {
    const u = new URL(url);
    return u.hostname === HOST && /^https?:$/.test(u.protocol);
  } catch {
    return false;
  }
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function buildUrlset(urls, lastmods) {
  const lm = (u) => (lastmods[u] ? lastmods[u] : today());
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${
    urls
      .map(
        (u) =>
          `  <url>\n    <loc>${u}</loc>\n    <lastmod>${lm(
            u
          )}</lastmod>\n  </url>`
      )
      .join("\n")
  }\n</urlset>\n`;
}

// Index IMMER frisch schreiben (Basis + alle vorhandenen product-XMLs)
function writeFreshIndex() {
  const todayStr = today();
  const productFiles = fs
    .readdirSync(".")
    .filter((f) => /^sitemap_products.*\.xml$/i.test(f))
    .sort((a, b) => a.localeCompare(b, "en"));
  const lines = [
    `<?xml version="1.0" encoding="UTF-8"?>`,
    `<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`,
    `  <sitemap><loc>${BASE}/sitemap.xml</loc><lastmod>${todayStr}</lastmod></sitemap>`,
    `  <sitemap><loc>${BASE}/sitemap_categories.xml</loc><lastmod>${todayStr}</lastmod></sitemap>`,
    `  <sitemap><loc>${BASE}/sitemap_auto.xml</loc><lastmod>${todayStr}</lastmod></sitemap>`,
    ...productFiles.map(
      (f) => `  <sitemap><loc>${BASE}/${f}</loc><lastmod>${todayStr}</lastmod></sitemap>`
    ),
    `</sitemapindex>`,
  ];
  fs.writeFileSync("sitemap_index.xml", lines.join("\n"), "utf-8");
}

async function fetchRobots() {
  try {
    const res = await fetch(`${START_DOMAIN}/robots.txt`, {
      headers: { "User-Agent": UA },
    });
    const txt = await res.text();
    return robotsParser(`${START_DOMAIN}/robots.txt`, txt);
  } catch {
    return robotsParser(`${START_DOMAIN}/robots.txt`, "");
  }
}

async function acceptCookies(page) {
  const texts = ["alle akzeptieren", "akzeptieren", "zustimmen", "einverstanden"];
  try {
    await page.waitForTimeout(600);
    const clicked = await page.evaluate((texts) => {
      const btns = [...document.querySelectorAll('button,[role="button"]')];
      for (const el of btns) {
        const t = (el.textContent || "").toLowerCase();
        if (texts.some((x) => t.includes(x))) {
          el.click();
          return true;
        }
      }
      const sel = [
        "#onetrust-accept-btn-handler",
        ".js-accept-cookies",
        ".ot-pc-refuse-all-handler",
      ];
      for (const s of sel) {
        const e = document.querySelector(s);
        if (e) {
          e.click();
          return true;
        }
      }
      return false;
    }, texts);
    if (clicked) await page.waitForTimeout(800);
  } catch {}
}

async function expandAndScroll(page) {
  for (let i = 0; i < 8; i++) {
    const did = await page.$$eval("button", (btns) => {
      let clicked = false;
      for (const b of btns) {
        const t = (b.textContent || "").toLowerCase().trim();
        if (t.includes("mehr laden") || t.includes("mehr anzeigen")) {
          b.click();
          clicked = true;
        }
      }
      return clicked;
    });
    await page.waitForTimeout(800);
    if (!did) break;
  }
  for (let i = 0; i < 20; i++) {
    await page.evaluate(() => window.scrollBy(0, document.body.scrollHeight));
    await page.waitForTimeout(400);
  }
  await page.waitForTimeout(WAIT_AFTER_LOAD);
}

function harvestIsbnsFromText(text) {
  const set = new Set();
  for (const m of text.matchAll(RE_ISBN13)) set.add(m[0]);
  return Array.from(set);
}

function harvestProductUrlsFromHtml(html) {
  const urls = new Set();
  const reUrl = new RegExp(
    `https?:\/\/${HOST}\/[^\\s"'<>]*?\\d{13}[^\\s"'<>]*`,
    "ig"
  );
  for (const m of html.matchAll(reUrl))
    urls.add(m[0].split("#")[0].split("?")[0]);
  for (const isbn of harvestIsbnsFromText(html))
    urls.add(toProductUrlFromIsbn(isbn));
  return Array.from(urls);
}

async function trySearchHarvest(page) {
  const queries = ["a", "e", "i", "o", "u", "der", "die", "das"];
  const found = new Set();
  for (const q of queries) {
    try {
      const ok = await page.evaluate(() => {
        const sel = [
          'input[type="search"]',
          'input[placeholder*="Suche" i]',
          'input[name*="search" i]',
          "#search",
        ];
        for (const s of sel) {
          const el = document.querySelector(s);
          if (el) {
            el.focus();
            return true;
          }
        }
        return false;
      });
      if (!ok) break;

      await page.keyboard.down("Control");
      await page.keyboard.press("KeyA");
      await page.keyboard.up("Control");
      await page.keyboard.type(q, { delay: 50 });
      await page.keyboard.press("Enter");
      await page.waitForNetworkIdle({ idleTime: 800, timeout: 30000 });
      await expandAndScroll(page);

      const hrefs = await page.$$eval("a[href]", (as) =>
        as.map((a) => a.href).filter(Boolean)
      );
      for (const h of hrefs) {
        const clean = h.split("#")[0].split("?")[0];
        if (clean.includes("/978") || clean.match(/-97[89]\d{10}\b/))
          found.add(clean);
      }
      const html = await page.content();
      for (const u of harvestProductUrlsFromHtml(html)) found.add(u);
    } catch {}
  }
  return Array.from(found);
}

// ======= Refresh (HEAD/GET) =======
function daysBetween(a, b) {
  return Math.round((new Date(b) - new Date(a)) / (1000 * 60 * 60 * 24));
}

async function headOrGet(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), REFRESH_TIMEOUT_MS);
  try {
    // HEAD
    let res = await fetch(url, {
      method: "HEAD",
      redirect: "follow",
      headers: { "User-Agent": UA, "Accept-Language": "de-DE,de;q=0.9,en;q=0.8" },
      signal: ctrl.signal,
    });
    if (res.ok)
      return {
        ok: true,
        status: res.status,
        lastModified: res.headers.get("last-modified") || "",
      };

    // Fallback GET (manche Server blocken HEAD)
    res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        "User-Agent": UA,
        Accept: "text/html,*/*;q=0.1",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
      },
      signal: ctrl.signal,
    });
    return {
      ok: res.ok,
      status: res.status,
      lastModified: res.headers.get("last-modified") || "",
    };
  } catch (e) {
    return { ok: false, status: 0, lastModified: "" };
  } finally {
    clearTimeout(t);
  }
}

async function refreshStaleProducts(cache, todayStr) {
  const entries = Object.entries(cache);
  const stale = entries
    .filter(([u, meta]) => {
      const ls = meta.last_seen || "1970-01-01";
      const age = daysBetween(ls, todayStr);
      return age >= REFRESH_AFTER_DAYS && age < RETAIN_DAYS;
    })
    .map(([u]) => u);

  if (!stale.length) return 0;

  let idx = 0,
    alive = 0;
  async function worker() {
    while (idx < stale.length) {
      const u = stale[idx++];
      const res = await headOrGet(u);
      if (res.ok) {
        cache[u].last_seen = todayStr;
        if (res.lastModified) {
          const d = new Date(res.lastModified);
          if (!isNaN(d)) cache[u].lastmod = d.toISOString().slice(0, 10);
        }
        alive++;
      }
      await sleep(100);
    }
  }
  const workers = Array.from({ length: REFRESH_CONCURRENCY }, () => worker());
  await Promise.all(workers);
  return alive;
}

// ======= Main =======
async function main() {
  const robots = await fetchRobots();
  const seeds = parseSeedsFromCategoriesSitemap();
  const queue = [];
  const seen = new Set();
  const products = new Set();
  for (const s of seeds) queue.push({ url: s, depth: 0 });

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--incognito",
      "--disable-blink-features=AutomationControlled",
    ],
  });
  const context = await browser.createBrowserContext();

  let processed = 0,
    stop = false;

  async function worker() {
    while (queue.length && !stop) {
      const { url, depth } = queue.shift();
      if (seen.has(url)) continue;
      seen.add(url);
      if (RESPECT_ROBOTS && !robots.isAllowed(url, UA)) continue;
      if (depth > MAX_DEPTH) continue;

      const page = await context.newPage();
      try {
        await page.setUserAgent(UA);
        await page.setViewport({ width: 1366, height: 900 });
        await page.setExtraHTTPHeaders({
          "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        });
        await page.setRequestInterception(true);
        page.on("request", (req) => {
          const t = req.resourceType();
          if (["image", "media", "font"].includes(t)) return req.abort();
          req.continue();
        });

        // XHR/HTML sniffen
        page.on("response", async (res) => {
          try {
            const req = res.request();
            const type = req.resourceType();
            if (!["xhr", "fetch", "document"].includes(type)) return;
            const ct = res.headers()["content-type"] || "";
            if (!/json|html|text/i.test(ct)) return;
            const body = await res.text();
            for (const u of harvestProductUrlsFromHtml(body)) {
              if (isInternal(u))
                products.add(u.split("#")[0].split("?")[0]);
            }
          } catch {}
        });

        await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });
        await acceptCookies(page);
        await expandAndScroll(page);

        // DOM-Links
        const hrefs = await page.$$eval("a[href]", (as) =>
          as.map((a) => a.href).filter(Boolean)
        );
        for (const h of hrefs) {
          const clean = h.split("#")[0].split("?")[0];
          if (isInternal(clean) && looksLikeProduct(clean)) products.add(clean);
          if (
            isInternal(clean) &&
            /^https?:\/\/www\.kellereiladen\.de\/buecher/i.test(clean)
          ) {
            queue.push({ url: clean, depth: depth + 1 });
          }
        }

        // Fallback: interne Suche
        if (products.size < 50 && depth <= 1) {
          const more = await trySearchHarvest(page);
          for (const u of more) {
            const clean = u.split("#")[0].split("?")[0];
            if (isInternal(clean) && looksLikeProduct(clean))
              products.add(clean);
          }
        }

        // Pagination
        const nexts = await page.$$eval('a[rel="next"]', (as) =>
          as.map((a) => a.href).filter(Boolean)
        );
        for (const n of nexts) if (isInternal(n)) queue.push({ url: n, depth: depth + 1 });
      } catch {} finally {
        await page.close();
        processed++;
        if (processed >= MAX_PAGES) stop = true;
        await sleep(CRAWL_DELAY);
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  await context.close();
  await browser.close();

  // Cache pflegen
  let cache = {};
  try {
    cache = JSON.parse(fs.readFileSync(PRODUCTS_CACHE, "utf-8"));
  } catch {}
  const todayStr = today();

  // Neue Produkte vom Crawl eintragen
  for (const u of products) {
    cache[u] = cache[u] || {};
    cache[u].last_seen = todayStr;
    cache[u].lastmod = cache[u].lastmod || todayStr;
  }

  // Refresh-Sweep für fast ablaufende Produkte (HEAD/GET)
  const refreshed = await refreshStaleProducts(cache, todayStr);
  console.log(`Refreshed near-stale products: ${refreshed}`);

  // Prune nach RETAIN_DAYS
  const keep = {};
  for (const [u, meta] of Object.entries(cache)) {
    const ls = meta.last_seen || "1970-01-01";
    const age = (new Date(todayStr) - new Date(ls)) / (1000 * 60 * 60 * 24);
    if (age <= RETAIN_DAYS) keep[u] = meta;
  }
  cache = keep;

  const urls = Object.keys(cache).sort();
  const lastmods = Object.fromEntries(
    urls.map((u) => [u, cache[u].lastmod || cache[u].last_seen || todayStr])
  );

  // Sitemaps schreiben (mind. eine Datei)
  const files = [];
  const chunks = chunk(urls.length ? urls : [START_DOMAIN], CHUNK_SIZE);
  chunks.forEach((arr, i) => {
    const name = i === 0 ? "sitemap_products.xml" : `sitemap_products_${i}.xml`;
    fs.writeFileSync(name, buildUrlset(urls.length ? arr : [], lastmods), "utf-8");
    files.push(name);
  });

  // Index neu schreiben
  writeFreshIndex();

  // Cache speichern
  fs.writeFileSync(PRODUCTS_CACHE, JSON.stringify(cache, null, 2), "utf-8");

  console.log(`Products found: ${urls.length} | Files: ${files.join(", ")}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
