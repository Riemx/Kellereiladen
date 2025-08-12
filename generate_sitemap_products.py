#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawlt www.kellereiladen.de, extrahiert Produktseiten (Bücher) und schreibt sitemap_products*.xml.
- Respektiert robots.txt
- Folgt nur internen Links (Host-Filter)
- Heuristiken: /shop/item/..., Slugs mit -978..., sowie Fallback über Seiteninhalt (ISBN/Titelnr)
- Aufgeteilt in Chunks à 45.000 URLs (unter 50.000/50 MB Limit)
"""

import datetime
import http.client
import ssl
import socket
import urllib.parse
import urllib.robotparser
from collections import deque
import re
from lxml import html
import time
from typing import Set, List
from pathlib import Path

START_URL   = "https://www.kellereiladen.de/"
ALLOWED_HOST = "www.kellereiladen.de"
USER_AGENT  = "KellereiladenSitemapBot/1.0 (+https://sitemap.kellereiladen.de)"
TIMEOUT     = 15
CRAWL_DELAY = 0.15     # Sekunden, bei Bedarf senken (robots.txt beachten!)
MAX_PAGES   = 200000   # vorher 20000

def fetch(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("http", "https"):
        return 0, "", b""
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    ctx = ssl.create_default_context() if parsed.scheme == "https" else None
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    conn = conn_cls(parsed.hostname, port, timeout=TIMEOUT, context=ctx) if parsed.scheme == "https" else conn_cls(parsed.hostname, port, timeout=TIMEOUT)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"}
    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
        status = resp.status
        ctype = resp.getheader("Content-Type", "") or ""
        conn.close()
        return status, ctype, body
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return 0, "", b""

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

RE_ITEM_PATH      = re.compile(r"^/shop/item/\d{9,13}/", re.I)
RE_ISBN_TAIL      = re.compile(r"/[a-z0-9-]+-\d{10,13}$", re.I)          # slug-978...
RE_CAT_ISBN_TAIL  = re.compile(r"^/[^/]+/[a-z0-9-]+-\d{10,13}$", re.I)   # /buecher-.../slug-978...
RE_CANONICAL_TAG  = re.compile(rb'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', re.I)

def looks_like_product(url_path: str, content: bytes) -> bool:
    if RE_ITEM_PATH.search(url_path) or RE_CAT_ISBN_TAIL.search(url_path) or RE_ISBN_TAIL.search(url_path):
        return True
    try:
        text = content.decode("utf-8", "ignore")
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
    href = m.group(1).decode("utf-8", "ignore")
    cu = clean_url(href)
    return cu or url

_rp = None
def robots_ok(u: str) -> bool:
    global _rp
    if _rp is None:
        _rp = urllib.robotparser.RobotFileParser()
        _rp.set_url("https://www.kellereiladen.de/robots.txt")
        try:
            _rp.read()
        except Exception:
            pass
    return _rp.can_fetch(USER_AGENT, u)

def crawl(start: str):
    q = deque([start])
    seen = {start}
    products = set()
    pages = 0
    while q and pages < MAX_PAGES:
        url = q.popleft()
        if not robots_ok(url):
            continue
        status, ctype, body = fetch(url)
        if status != 200:
            continue
        pages += 1

        if "text/html" in ctype:
            path = urllib.parse.urlsplit(url).path or "/"
            if looks_like_product(path, body):
                products.add(resolve_canonical(url, body))
            for h in extract_links(body):
                cu = clean_url(h)
                if cu and cu not in seen:
                    seen.add(cu)
                    q.append(cu)
        time.sleep(CRAWL_DELAY)
    return sorted(products)

def write_sitemaps(urls: List[str], base_name="sitemap_products", max_urls=45000):  # vorher 50000
    today = datetime.date.today().isoformat()
    def write_chunk(chunk_urls, idx=None):
        name = f"{base_name}.xml" if idx is None else f"{base_name}_{idx}.xml"
        with open(name, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
            for u in chunk_urls:
                f.write("  <url>\n")
                f.write(f"    <loc>{u}</loc>\n")
                f.write(f"    <lastmod>{today}</lastmod>\n")
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
    today = datetime.date.today().isoformat()
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
    urls = crawl(START_URL)
    urls = sorted(set(urls))
    files = write_sitemaps(urls, base_name="sitemap_products", max_urls=45000)  # Chunks à 45k
    ensure_index_has_products(product_files=[Path(p).name for p in files])
    print(f"Products discovered: {len(urls)}; Files: {', '.join(files)}")
