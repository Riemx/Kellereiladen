import re, time, urllib.parse, urllib.robotparser, datetime, sys
from collections import deque
from lxml import html
import http.client
import ssl
import socket

START_URL = "https://www.kellereiladen.de/"
ALLOWED_HOST = "www.kellereiladen.de"
MAX_PAGES = 5000
TIMEOUT = 15
HEADERS = {"User-Agent": "SitemapBot/1.0 (+https://sitemap.kellereiladen.de)"}

def fetch(url):
    # einfacher GET ohne externe Libs (Actions-Umgebung ist schlank)
    parsed = urllib.parse.urlsplit(url)
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    ctx = ssl.create_default_context() if parsed.scheme == "https" else None
    conn = conn_cls(parsed.hostname, parsed.port or (443 if parsed.scheme=="https" else 80), timeout=TIMEOUT, context=ctx) if parsed.scheme=="https" else conn_cls(parsed.hostname, parsed.port or 80, timeout=TIMEOUT)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    conn.request("GET", path, headers=HEADERS)
    resp = conn.getresponse()
    body = resp.read()
    conn.close()
    return resp.status, resp.getheader("Content-Type",""), body

def clean(u):
    # absolut machen, Query+Fragment kappen, nur gleiche Host, https erzwingen
    u = urllib.parse.urljoin(START_URL, u)
    u = urllib.parse.urldefrag(u)[0]
    parts = urllib.parse.urlsplit(u)
    if parts.netloc.lower() != ALLOWED_HOST:
        return None
    # Querys weg
    parts = parts._replace(query="")
    # http->https
    if parts.scheme != "https":
        parts = parts._replace(scheme="https")
    # Trailing slash für Verzeichnisse: egal für Sitemap; so belassen
    return urllib.parse.urlunsplit(parts)

def extract_links(content_bytes):
    try:
        doc = html.fromstring(content_bytes)
        doc.make_links_absolute(START_URL)
        hrefs = set()
        for el in doc.xpath("//a[@href]"):
            hrefs.add(el.get("href"))
        return hrefs
    except Exception:
        return set()

def allowed_by_robots(url):
    rp = getattr(allowed_by_robots, "_rp", None)
    if rp is None:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url("https://www.kellereiladen.de/robots.txt")
        try:
            rp.read()
        except Exception:
            pass
        allowed_by_robots._rp = rp
    return rp.can_fetch(HEADERS["User-Agent"], url)

def crawl(start):
    q = deque([start])
    seen = set([start])
    ok = []
    while q and len(ok) < MAX_PAGES:
        url = q.popleft()
        if not allowed_by_robots(url):
            continue
        try:
            status, ctype, body = fetch(url)
        except (socket.timeout, ssl.SSLError, ConnectionError, OSError):
            continue
        if status != 200:
            continue
        ok.append(url)
        # nur HTML weiterverfolgen
        if "text/html" in ctype:
            for h in extract_links(body):
                cu = clean(h)
                if not cu:
                    continue
                if cu not in seen:
                    seen.add(cu)
                    q.append(cu)
        # sanft
        time.sleep(0.2)
    return sorted(ok)

def write_urlset(urls, path):
    today = datetime.date.today().isoformat()
    with open(path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for u in urls:
            f.write("  <url>\n")
            f.write(f"    <loc>{u}</loc>\n")
            f.write(f"    <lastmod>{today}</lastmod>\n")
            f.write("  </url>\n")
        f.write("</urlset>\n")

def ensure_index():
    # Falls du den Index manuell pflegst, lass das so.
    # Hier erweitern wir ihn nur um sitemap_auto.xml, wenn nicht enthalten.
    try:
        with open("sitemap_index.xml", "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        content = ""
    if "sitemap_auto.xml" in content:
        return
    today = datetime.date.today().isoformat()
    # Minimaler Index mit allen drei Dateien
    index = f"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://sitemap.kellereiladen.de/sitemap.xml</loc>
    <lastmod>{today}</lastmod>
  </sitemap>
  <sitemap>
    <loc>https://sitemap.kellereiladen.de/sitemap_categories.xml</loc>
    <lastmod>{today}</lastmod>
  </sitemap>
  <sitemap>
    <loc>https://sitemap.kellereiladen.de/sitemap_auto.xml</loc>
    <lastmod>{today}</lastmod>
  </sitemap>
</sitemapindex>
"""
    with open("sitemap_index.xml", "w", encoding="utf-8") as f:
        f.write(index)

if __name__ == "__main__":
    urls = crawl(START_URL)
    # Optional: Pflichtseiten sicherstellen
    must = [
        "https://www.kellereiladen.de/",
        "https://www.kellereiladen.de/UeberUns",
        "https://www.kellereiladen.de/privacyPolicy",
        "https://www.kellereiladen.de/impressum",
    ]
    for m in must:
        if m not in urls:
            urls.append(m)
    urls = sorted(set(urls))
    write_urlset(urls, "sitemap_auto.xml")
    ensure_index()
    print(f"Collected {len(urls)} URLs")
