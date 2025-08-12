#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawlt www.kellereiladen.de, erzeugt sitemap_products*.xml.
- Respektiert robots.txt
- Nur interne Links
- Produkt-Erkennung per URL-Muster & Inhalt (ISBN/Titelnr)
- Splits à 45.000 URLs
- NEU: Cache + Gnadenfrist (RETAIN_DAYS), damit veraltete Seiten sauber auslaufen
"""

import datetime
import http.client
import ssl
import urllib.parse
import urllib.robotparser
from collections import deque
import re, time, json, os
from lxml import html
from pathlib import Path
from typing import Set, List, Dict, Tuple

START_URL    = "https://www.kellereiladen.de/"
ALLOWED_HOST = "www.kellereiladen.de"
USER_AGENT   = "KellereiladenSitemapBot/1.0 (+https://sitemap.kellereiladen.de)"
TIMEOUT      = 15
CRAWL_DELAY  = 0.20
MAX_PAGES    = 200000
MAX_PRODUCTS = 999999
RETAIN_DAYS  = 7                     # Gnadenfrist
CACHE_PATH   = Path("products_seen.json")

RE_ITEM_PATH     = re.compile(r"^/shop/item/\d{9,13}/", re.I)
RE_ISBN_TAIL     = re.compile(r"/[a-z0-9-]+-\d{10,13}$", re.I)
RE_CAT_ISBN_TAIL = re.compile(r"^/[^/]+/[a-z0-9-]+-\d{10,13}$", re.I)
RE_CANONICAL_TAG = re.compile(rb'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', re.I)

def fetch(url) -> Tuple[int, str, bytes, Dict[str,str]]:
    p = urllib.parse.urlsplit(url)
    if p.scheme not in ("http", "https"):
        return 0, "", b"", {}
    conn_cls = http.client.HTTPSConnection if p.scheme == "https" else http.client.HTTPConnection
    ctx = ssl.create_default_context() if p.scheme == "https" else None
    port = p.port or (443 if p.scheme == "https" else 80)
    conn = conn_cls(p.hostname, port, timeout=TIMEOUT, context=ctx) if p.scheme == "https" else conn_cls(p.hostname, port, timeout=TIMEOUT)
    path = p.path or "/"
    if p.query:
        path += "?" + p.query
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"}
    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
        status = resp.status
        ctype = resp.getheader("Content-Type","") or ""
        hdrs  = {k.lower():v for k,v in resp.getheaders()}
        conn.close()
        return status, ctype, body, hdrs
    except Exception:
        try: conn.close()
        except Exception: pass
        return 0, "", b"", {}

def clean_url(u: str) -> str:
    u = urllib.parse.urljoin(START_URL, u)
    u = urllib.parse.urldefrag(u)[0]
    p = urllib.parse.urlsplit(u)
    if p.netloc.lower() != ALLOWED_HOST:
        return ""
    p = p._replace(query="")
    if p.scheme != "https":
        p = p._replace(scheme="https")
    return urllib.parse.urlunsplit(p)

def extract_links(content: bytes) -> Set[str]:
    try:
        doc = html.fromstring(content)
        return {el.get("href") for el in doc.xpath("//a[@href]") if el.get("href")}
    except Exception:
        return set()

def looks_like_product(url_path: str, content: bytes) -> bool:
    if RE_ITEM_PATH.search(url_path) or RE_CAT_ISBN_TAIL.search(url_path) or RE_ISBN_TAIL.search(url_path):
        return True
    try:
        text = content.decode("utf-8","ignore")
    except Exception:
        text = ""
    isbn_hits = text.count("ISBN")
    if isbn_hits and isbn_hits <= 4:
        return True
    if "Titelnr" in text or "DETAILS" in text:
        return True
    return False

def resolve_canonical(url: str, content: bytes) -> str:
    m = RE_CANONICAL_TAG.search(content)
    if not m:
        return url
    href = m.group(1).decode("utf-8","ignore")
    cu = clean_url(href)
    return cu or url

_rp = None
def robots_ok(u: str) -> bool:
    global _rp
    if _rp is None:
        _rp = urllib.robotparser.RobotFileParser()
        _rp.set_url("https://www.kellereiladen.de/robots.txt")
        try: _rp.read()
        except Exception: pass
    return _rp.can_fetch(USER_AGENT, u)

def load_cache() -> Dict[str, Dict]:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: Dict[str, Dict]):
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def iso_today() -> str:
    return datetime.date.today().isoformat()

def parse_last_modified(headers: Dict[str,str]) -> str:
    lm = headers.get("last-modified")
    if not lm:
        return ""
    try:
        # http date -> iso date
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(lm)
        return dt.date().isoformat()
    except Exception:
        return ""

def crawl(start: str):
    q = deque([start])
    seen = {start}
    found = set()
    pages = 0
    while q and pages < MAX_PAGES and len(found) < MAX_PRODUCTS:
        url = q.popleft()
        if not robots_ok(url):
            continue
        status, ctype, body, hdrs = fetch(url)
        if status != 200:
            continue
        pages += 1

        if "text/html" in ctype:
            path = urllib.parse.urlsplit(url).path or "/"
            if looks_like_product(path, body):
                found.add(resolve_canonical(url, body))
            for h in extract_links(body):
                cu = clean_url(h)
                if cu and cu not in seen:
                    seen.add(cu)
                    q.append(cu)
        time.sleep(CRAWL_DELAY)
    return sorted(found)

def age_days(date_iso: str) -> int:
    try:
        d = datetime.date.fromisoformat(date_iso)
    except Exception:
        return 9999
    return (datetime.date.today() - d).days

def write_sitemaps(urls: List[str], lastmods: Dict[str,str], base_name="sitemap_products", max_urls=45000):
    today = iso_today()
    def write_chunk(chunk_urls, idx=None):
        name = f"{base_name}.xml" if idx is None else f"{base_name}_{idx}.xml"
        with open(name, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
            for u in chunk_urls:
                lm = lastmods.get(u) or today
                f.write("  <url>\n")
                f.write(f"    <loc>{u}</loc>\n")
                f.write(f"    <lastmod>{lm}</lastmod>\n")
                f.write("  </url>\n")
            f.write("</urlset>\n")
        return name

    files = []
    if len(urls) <= max_urls:
        files.append(write_chunk(urls))
    else:
        idx = 1
        for i in range(0, len(urls), max_urls):
            files.append(write_chunk(urls[i:i+max_urls], idx))
            idx += 1
    return files

def ensure_index_has_products(index_path="sitemap_index.xml", product_files=None):
    product_files = product_files or ["sitemap_products.xml"]
    today = iso_today()
    try:
        content = open(index_path, "r", encoding="utf-8").read()
    except FileNotFoundError:
        content = ""

    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']

    if content.strip():
        import re
        existing_locs = re.findall(r"<loc>([^<]+)</loc>", content)
        for loc in existing_locs:
            lines += ["  <sitemap>", f"    <loc>{loc}</loc>", f"    <lastmod>{today}</lastmod>", "  </sitemap>"]
    else:
        for loc in [
            "https://sitemap.kellereiladen.de/sitemap.xml",
            "https://sitemap.kellereiladen.de/sitemap_categories.xml",
        ]:
            lines += ["  <sitemap>", f"    <loc>{loc}</loc>", f"    <lastmod>{today}</lastmod>", "  </sitemap>"]

    for pf in product_files:
        full = f"https://sitemap.kellereiladen.de/{pf}"
        if full not in "\n".join(lines):
            lines += ["  <sitemap>", f"    <loc>{full}</loc>", f"    <lastmod>{today}</lastmod>", "  </sitemap>"]

    lines.append("</sitemapindex>")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    # 1) Vorherige Daten laden
    cache = load_cache()  # {url: {"last_seen": "YYYY-MM-DD", "lastmod": "YYYY-MM-DD"}}

    # 2) Crawlen & aktuelle Treffer
    current = crawl(START_URL)
    today = iso_today()

    # 3) Cache aktualisieren
    for u in current:
        # Optional Last-Modified Header holen (HEAD-Request sparen wir uns; wir haben ihn aus GET)
        # Für Einfachheit: lastmod = last_seen; wer will, kann fetch-Header zwischenspeichern.
        entry = cache.get(u, {})
        entry["last_seen"] = today
        # Wenn du echte Last-Modified-Daten willst, musst du sie in crawl()/fetch() sammeln und hier setzen.
        entry.setdefault("lastmod", today)
        cache[u] = entry

    # 4) Veraltete entfernen (älter als RETAIN_DAYS nicht mehr gesehen)
    keep: Dict[str, Dict] = {}
    for u, meta in cache.items():
        ls = meta.get("last_seen", "")
        if age_days(ls) <= RETAIN_DAYS:
            keep[u] = meta
    cache = keep

    # 5) Ausgabemenge vorbereiten
    urls = sorted(cache.keys())
    lastmods = {u: cache[u].get("lastmod") or cache[u].get("last_seen", today) for u in urls}

    # 6) Schreiben & Index aktualisieren
    files = write_sitemaps(urls, lastmods, base_name="sitemap_products", max_urls=45000)
    ensure_index_has_products(product_files=[Path(p).name for p in files])

    # 7) Cache speichern
    save_cache(cache)

    print(f"Products in cache: {len(urls)}; Files: {', '.join(files)}")
