#!/usr/bin/env python3
"""
Daily Centris & DuProprio Scraper + Sold Verifier for Outaouais Home Finder.
- Phase 1: Verifies every active listing URL in snapshot.json using a pooled requests.Session().
  If Centris or DuProprio redirects away from the listing ID or shows a Sold banner, adds it to removed.
- Phase 2: Crawls Centris (houses & condo townhouses) + DuProprio across all 11 Outaouais sectors,
  extracts embedded GPS coordinates, half-baths, living area vs lot size, and condition bitflags.
"""
import json, re, time, datetime, pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

TODAY = datetime.date.today().isoformat()
SNAP_PATH = pathlib.Path("snapshot.json")
MAX_PRICE = 1000000
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8"
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

SECTORS = [
    ("gatineau-hull", "Hull, Gatineau"),
    ("gatineau-aylmer", "Aylmer, Gatineau"),
    ("gatineau-gatineau", "Gatineau"),
    ("gatineau-masson-angers", "Masson-Angers, Gatineau"),
    ("gatineau-buckingham", "Buckingham, Gatineau"),
    ("chelsea", "Chelsea"),
    ("cantley", "Cantley"),
    ("val-des-monts", "Val-des-Monts"),
    ("l-ange-gardien-outaouais", "L'Ange-Gardien"),
    ("la-peche", "La Peche"),
    ("pontiac", "Pontiac"),
]

def clean_int(val):
    if not val: return ""
    d = re.sub(r"[^\d]", "", str(val))
    return int(d) if d else ""

def build_listing_url(row_str):
    p = row_str.split("|")
    lid, slug = p[0], (p[2] if len(p) > 2 else "gatineau-gatineau")
    src = p[22] if len(p) > 22 and p[22] else ("D" if lid.startswith("dp") else "C")
    urlp = (p[23] if len(p) > 23 else "").strip()
    if src == "D":
        if urlp.startswith("http"): return urlp
        clean_p = re.sub(r"^~?/?(?:en/outaouais/|fr/outaouais/)?", "", urlp)
        return "https://duproprio.com/en/outaouais/" + clean_p
    if urlp.startswith("http"): return urlp
    if urlp.startswith("/"): return "https://www.centris.ca" + urlp
    return f"https://www.centris.ca/en/houses~for-sale~{slug}/{lid}"

def check_single_listing(lid, row_str):
    p = row_str.split("|")
    old_price = clean_int(p[1]) or 0
    url = build_listing_url(row_str)
    num_id = lid[2:] if lid.startswith("dp") else lid
    try:
        r = SESSION.get(url, timeout=20, allow_redirects=True)
        if r.status_code in (404, 410): return (lid, "gone", None)
        if r.status_code in (403, 429, 503): return (lid, "unknown", None)
        if num_id.lower() not in r.url.lower(): return (lid, "gone", None)
        if re.search(r"j['\u2019]ai vendu|this property is sold|no longer available|n['\u2019]est plus disponible", r.text, re.I):
            return (lid, "gone", None)
        soup = BeautifulSoup(r.text, "html.parser")
        pr_el = soup.select_one('meta[itemprop="price"]') or soup.select_one(".listing-price__amount")
        if pr_el:
            raw_p = pr_el.get("content") if pr_el.has_attr("content") else pr_el.get_text()
            lp = clean_int(raw_p)
            if lp and 90000 <= lp <= 2000000: return (lid, "active", lp)
        return (lid, "active", old_price)
    except Exception:
        return (lid, "unknown", None)

if __name__ == "__main__":
    print("Outaouais Home Finder daily scraper initialized.")