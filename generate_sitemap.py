#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime, http.client, ssl, urllib.parse, urllib.robotparser, time
from collections import deque
from lxml import html

START_URL = "https://www.kellereiladen.de/"
ALLOWED_HOST = "www.kellereiladen.de"
UA = "KellereiladenAutoSitemap/1.0 (+https://sitemap.kellereiladen.de)"
TIMEOUT = 15
CRAWL_DELAY = 0.2
MAX_PAGES = 10000

def fetch(u):
    p = urllib.parse.urlsplit(u)
    conn_cls = http.client.HTTPSConnection if p.scheme=="https" else http.client.HTTPConnection
    ctx = ssl.create_default_context() if p.scheme=="https" else None
    port = p.port or (443 if p.scheme=="https" else 80)
    conn = conn_cls(p.hostname, port, timeout=TIMEOUT, context=ctx) if p.scheme=="https" else conn_cls(p.hostname, port, timeout=TIMEOUT)
    path = p.path or "/"
    if p.query: path += "?"+p.query
    conn.request("GET", path, headers={"User-Agent": UA})
    r = conn.getresponse(); body = r.read()
    st, ct = r.status, r.getheader("Content-Type","")
    conn.close()
    return st, ct, body

def clean(u):
    u = urllib.parse.urljoin(START_URL, u)
    u = urllib.parse.urldefrag(u)[0]
    p = urllib.parse.urlsplit(u)
    if p.netloc.lower()!=ALLOWED_HOST: return ""
    p = p._replace(query="")
    if p.scheme!="https": p = p._replace(scheme="https")
    return urllib.parse.urlunsplit(p)

def crawl(start):
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url("https://www.kellereiladen.de/robots.txt")
    try: rp.read()
    except: pass
    q = deque([start]); seen={start}; ok=[]
    while q and len(ok)<MAX_PAGES:
        u = q.popleft()
        if not rp.can_fetch(UA, u): continue
        try: st, ct, body = fetch(u)
        except: continue
        if st!=200: continue
        ok.append(u)
        if "text/html" in ct:
            try:
                doc = html.fromstring(body)
                for a in doc.xpath("//a[@href]"):
                    cu = clean(a.get("href") or "")
                    if cu and cu not in seen:
                        seen.add(cu); q.append(cu)
            except: pass
        time.sleep(CRAWL_DELAY)
    return sorted(set(ok))

def write_urlset(urls, path):
    today = datetime.date.today().isoformat()
    with open(path,"w",encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for u in urls:
            f.write("  <url>\n")
            f.write(f"    <loc>{u}</loc>\n")
            f.write(f"    <lastmod>{today}</lastmod>\n")
            f.write("  </url>\n")
        f.write("</urlset>\n")

if __name__=="__main__":
    urls = crawl(START_URL)
    # Sicherstellen, dass Pflichtseiten drin sind
    must = [
        "https://www.kellereiladen.de/",
        "https://www.kellereiladen.de/UeberUns",
        "https://www.kellereiladen.de/impressum",
        "https://www.kellereiladen.de/privacyPolicy",
    ]
    for m in must:
        if m not in urls: urls.append(m)
    write_urlset(sorted(urls), "sitemap_auto.xml")
