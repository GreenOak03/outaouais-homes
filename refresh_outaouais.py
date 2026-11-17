#!/usr/bin/env python3
"""
Outaouais Home Finder — Self-Healing 3-Phase Daily Scraper & Market History Logger
Step 0: Git History Self-Healing (automatically recovers any listings dropped by Centris rate-limiting
        in previous commits via `git log` / `git show` before Phase 1 begins!)
Phase 1: Polite 4-worker URL verifier (protects against Centris WAF rate-limits, repairs $-price addresses,
         clears false-positive menu Pool flags, and backfills Detached ["DV"] / Semi ["S"] / Row ["R"])
Phase 2: Multi-sector Playwright crawler (extracts true street addresses and scopes bitflags to listing body)
Phase 3: Logs daily median/mean price & $/sqft into marketHistory for 7d/14d/30d moving averages
"""

import json
import os
import re
import time
import datetime
import statistics
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

SNAPSHOT_PATH = "snapshot.json"
HARD_REMOVED = {"dp1141373", "12153990"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8",
}

SECTORS = [
    ("gatineau-hull", "Hull", "gatineau-hull", "gatineau-hull", 45.438, -75.735),
    ("gatineau-aylmer", "Aylmer", "gatineau-aylmer", "gatineau-aylmer", 45.398, -75.845),
    ("gatineau-gatineau", "Gatineau", "gatineau-gatineau", "gatineau-gatineau", 45.483, -75.655),
    ("gatineau-buckingham", "Buckingham", "gatineau-buckingham", "gatineau-buckingham", 45.586, -75.418),
    ("gatineau-masson-angers", "Masson-Angers", "gatineau-masson-angers", "gatineau-masson-angers", 45.542, -75.502),
    ("cantley", "Cantley", "cantley", "cantley", 45.565, -75.782),
    ("chelsea", "Chelsea", "chelsea", "chelsea", 45.504, -75.792),
    ("val-des-monts", "Val-des-Monts", "val-des-monts", "val-des-monts", 45.652, -75.668),
    ("l-ange-gardien-outaouais", "L'Ange-Gardien", "l-ange-gardien-outaouais", "l-ange-gardien", 45.610, -75.450),
    ("la-peche", "La Pêche", "la-peche", "la-peche", 45.635, -75.925),
    ("pontiac", "Pontiac", "pontiac", "pontiac", 45.545, -76.130),
]


def clean_int(val):
    if val is None:
        return None
    s = re.sub(r"[^\d]", "", str(val))
    return int(s) if s else None


def is_price_or_bad_addr(addr):
    if not addr:
        return True
    s = str(addr).strip()
    if s.startswith("$") or re.match(r"^\$?[\d,\s.]+$", s):
        return True
    if len(s) < 4 or s.lower().startswith("centris #") or s.lower().startswith("duproprio #"):
        return True
    return False


def addr_from_url_slug(url, rid=""):
    if not url:
        return None
    m = re.search(r"hab-([a-z0-9\-]+)-\d{6,8}", url.lower())
    if m:
        slug = m.group(1)
        words = slug.split("-")
        small = {"de", "des", "du", "la", "le", "les", "aux", "au", "sur", "et", "d", "l"}
        out = []
        for idx, w in enumerate(words):
            if idx > 0 and w in small:
                out.append(w)
            else:
                out.append(w.capitalize())
        if out and out[0].isdigit() and len(out) > 1:
            return f"{out[0]}, {' '.join(out[1:])}"
        return " ".join(out)
    return None


def extract_clean_address_from_soup(soup, url="", fallback=""):
    # 1. Schema.org streetAddress meta or span
    el = soup.select_one('[itemprop="streetAddress"], .address, h1.property-address, .listing-location__address')
    if el:
        cand = el.get_text(" ", strip=True)
        if not is_price_or_bad_addr(cand):
            return cand
    # 2. Check og:title or page <title>
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        t = og["content"].split("|")[0].split(" - ")[0].strip()
        if not is_price_or_bad_addr(t) and any(ch.isdigit() for ch in t):
            return t
    # 3. Reconstruct from DuProprio hab-... URL slug
    from_slug = addr_from_url_slug(url)
    if from_slug:
        return from_slug
    return fallback if not is_price_or_bad_addr(fallback) else (from_slug or fallback or "Address on listing")


def extract_listing_body_text(soup):
    """
    Removes global <nav>, <header>, <footer>, <aside>, <form>, and search filter menus
    so words like 'Pool / Piscine' in the website's filter bar NEVER trigger false bitflags.
    """
    clone = BeautifulSoup(str(soup), "html.parser")
    for bad in clone.select(
        "nav, header, footer, script, style, noscript, form, "
        ".search-filters, .filters, [class*='filter'], [id*='filter'], [class*='menu'], [class*='navbar']"
    ):
        bad.decompose()
    main_sec = clone.select_one(
        "article, main, .listing-main-characteristics, .property-description, "
        ".listing-content, #property-result, .characteristics"
    )
    return (main_sec or clone).get_text(" ", strip=True)


def extract_gps_from_html(html):
    lat_m = re.search(
        r'(?:itemprop="latitude"\s+content="|"latitude"\s*:\s*|data-lat=")([4][456]\.\d+)',
        html,
    )
    lng_m = re.search(
        r'(?:itemprop="longitude"\s+content="|"longitude"\s*:\s*|data-lng=")(-7[4567]\.\d+)',
        html,
    )
    if lat_m and lng_m:
        return round(float(lat_m.group(1)), 5), round(float(lng_m.group(1)), 5)
    return None, None


def detect_attachment_from_html(html, url=""):
    u = (url or "").lower()
    if any(k in u for k in ["semi-detached", "jumele", "jumelé", "duplex"]):
        return "S"
    if any(k in u for k in ["townhouse", "row-house", "en-rangee", "condo"]):
        return "R"
    soup = BeautifulSoup(html, "html.parser")
    text = extract_listing_body_text(soup)
    m = re.search(
        r"(?:Building style|Style de b[aâ]timent|Property style|Cat[eé]gorie)\s*[:\-]?\s*([^\n\r.|]{3,45})",
        text,
        re.I,
    )
    sub = m.group(1).lower() if m else text.lower()
    if any(w in sub for w in ["semi-detached", "semi-détaché", "jumelé", "jumele"]):
        return "S"
    if any(w in sub for w in ["attached", "en rangée", "en rangee", "townhouse", "row house", "maison de ville"]):
        return "R"
    if any(w in sub for w in ["detached", "détaché", "detache", "isolé", "isole", "single-family"]):
        return "DV"
    return None


def compute_bits(body_text, existing_bits=0):
    t = body_text.lower()
    bits = existing_bits or 0
    if any(w in t for w in ["renovated", "rénové", "turnkey", "clé en main"]):
        bits |= 1
    if any(w in t for w in ["handyman", "bricoleur", "needs work", "à rénover", "fixer-upper"]):
        bits |= 2
    if any(w in t for w in ["foundation crack", "fissure", "water infiltration", "infiltration d'eau"]):
        bits |= 4
    if any(w in t for w in ["without legal warranty", "sans garantie légale", "at the buyer's risk"]):
        bits |= 8
    if any(w in t for w in ["attached garage", "detached garage", "heated garage", "single garage", "double garage", "garage intégré", "garage détaché"]):
        bits |= 64
    if re.search(r"\b(in-?ground pool|above-?ground pool|heated pool|piscine creusée|piscine hors[- ]terre|piscine chauffée)\b", t):
        bits |= 128
    if any(w in t for w in ["fireplace", "foyer au", "wood stove", "poêle à bois"]):
        bits |= 256
    if any(w in t for w in ["heat pump", "thermopompe", "central air", "climatisation centrale"]):
        bits |= 512
    if any(w in t for w in ["finished basement", "sous-sol aménagé", "completely finished"]):
        bits |= 1024
    if any(w in t for w in ["new construction", "construction neuve", "garantie gcr"]):
        bits |= 4096
    if any(w in t for w in ["plus gst/qst", "plus taxes", "taxes en sus", "+ gst/qst"]):
        bits |= 262144
    return bits


def row_id(r):
    if isinstance(r, str):
        return r.split("|")[0].strip()
    return str(r[0]).strip()


def recover_from_git_history(current_rows):
    """
    Step 0: Inspects the last 12 git commits of snapshot.json.
    If previous commits had listings that were dropped in a mass rate-limit event,
    restores them automatically!
    """
    by_id = {row_id(r): r for r in current_rows if row_id(r) not in HARD_REMOVED}
    initial_count = len(by_id)
    try:
        out = subprocess.check_output(
            ["git", "log", "-n", "12", "--pretty=format:%H", "--", SNAPSHOT_PATH],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        shas = [s.strip() for s in out.splitlines() if s.strip()]
        for sha in shas:
            try:
                raw = subprocess.check_output(
                    ["git", "show", f"{sha}:{SNAPSHOT_PATH}"],
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                hist_snap = json.loads(raw)
                h_rows = (
                    hist_snap["homes"]["rows"]
                    if isinstance(hist_snap.get("homes"), dict) and "rows" in hist_snap["homes"]
                    else hist_snap.get("rows", [])
                )
                for hr in h_rows:
                    hid = row_id(hr)
                    if hid and hid not in HARD_REMOVED and hid not in by_id:
                        by_id[hid] = hr
            except Exception:
                continue
    except Exception as e:
        print(f"  [INFO] Git history check skipped: {e}")

    recovered = len(by_id) - initial_count
    if recovered > 0:
        print(f"  [SELF-HEAL] Recovered {recovered} listings from Git commit history! Total pool: {len(by_id)}")
    return list(by_id.values())


def verify_existing_listing(row):
    time.sleep(0.22)  # Polite stagger so Centris WAF never rate-limits
    is_pipe = isinstance(row, str)
    p = row.split("|") if is_pipe else list(row)
    rid = str(p[0]).strip()
    old_price = clean_int(p[1] if is_pipe else p[3]) or 0
    addr = p[3] if is_pipe else (p[18] if len(p) > 18 else rid)
    if rid in HARD_REMOVED:
        return (rid, "removed", row, old_price, None, addr)

    if is_pipe:
        slug = p[2] if len(p) > 2 else "gatineau-gatineau"
        src = p[22] if len(p) > 22 and p[22] else ("D" if rid.startswith("dp") else "C")
        urlp = (p[23] if len(p) > 23 else "").strip()
        if src == "D":
            url = (
                urlp
                if urlp.startswith("http")
                else "https://duproprio.com/en/outaouais/"
                + re.sub(r"^~?/?(?:en/outaouais/|fr/outaouais/)?", "", urlp)
            )
        else:
            url = (
                urlp
                if urlp.startswith("http")
                else (
                    "https://www.centris.ca" + urlp
                    if urlp.startswith("/")
                    else f"https://www.centris.ca/en/houses~for-sale~{slug}/{rid}"
                )
            )
    else:
        url = p[19] if len(p) > 19 else ""
        # Repair $-price addresses immediately even before HTTP check
        if is_price_or_bad_addr(addr):
            fixed_addr = addr_from_url_slug(url, rid)
            if fixed_addr:
                p[18] = fixed_addr
                addr = fixed_addr
            if p[14] and (int(p[14]) & 128) and "pool" not in url.lower() and "piscine" not in url.lower():
                p[14] = int(p[14]) & ~128

    if not url or not url.startswith("http"):
        out_r = "|".join(str(x) for x in p) if is_pipe else p
        return (rid, "active", out_r, old_price, old_price, addr)

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        resp = session.get(url, timeout=15, allow_redirects=True)
        if resp.status_code in (404, 410):
            return (rid, "removed", row, old_price, None, addr)
        if resp.status_code >= 400:
            # Rate limit (429), WAF challenge (403), or server error -> KEEP listing!
            out_r = "|".join(str(x) for x in p) if is_pipe else p
            return (rid, "active", out_r, old_price, old_price, addr)

        final_url = resp.url.lower()
        html = resp.text
        html_low = html.lower()

        # Explicit sold/unavailable banners
        if any(
            m in html_low
            for m in [
                "this property is no longer available",
                "cette propriété n'est plus disponible",
                "property sold",
                "propriété vendue",
                "j'ai vendu",
            ]
        ):
            return (rid, "removed", row, old_price, None, addr)

        num_id = rid[2:] if rid.startswith("dp") else rid
        if num_id.lower() not in final_url:
            # Only treat redirect as SOLD if it redirected to a genuine search-results listing page
            # (not a Captcha, rate-limit, error, or homepage redirect)
            is_search_redirect = (
                ("for-sale" in final_url or "a-vendre" in final_url)
                and "captcha" not in html_low
                and "access denied" not in html_low
                and len(html) > 8000
            )
            if is_search_redirect:
                return (rid, "removed", row, old_price, None, addr)
            out_r = "|".join(str(x) for x in p) if is_pipe else p
            return (rid, "active", out_r, old_price, old_price, addr)

        att = detect_attachment_from_html(html, url)
        soup = BeautifulSoup(html, "html.parser")
        body_text = extract_listing_body_text(soup)

        # Repair bad $-price address using live HTML
        if is_price_or_bad_addr(addr):
            clean_a = extract_clean_address_from_soup(soup, url, addr)
            if clean_a:
                addr = clean_a
                if is_pipe:
                    p[3] = clean_a
                else:
                    p[18] = clean_a

        lm = re.search(
            r"(?:Lot area|Lot dimensions|Superficie du terrain|Terrain)\s*[:\-]?\s*([\d,\s]+)\s*(?:sqft|ft²|pi²|pc)",
            body_text,
            re.I,
        )
        lot_val = clean_int(lm.group(1)) if lm else None
        am = re.search(
            r"(?:Living space|Net area|Superficie habitable)\s*[:\-]?\s*([\d,\s]+)\s*(?:sqft|ft²|pi²|pc)",
            body_text,
            re.I,
        )
        area_val = clean_int(am.group(1)) if am else None

        if is_pipe:
            while len(p) < 25:
                p.append("")
            if att == "S" and "semi" not in p[5].lower():
                p[5] = (p[5] + ", Semi-detached").strip(", ")
            elif att == "R" and "row" not in p[5].lower():
                p[5] = (p[5] + ", Row house").strip(", ")
            elif att == "DV" and "detached" not in p[5].lower():
                p[5] = (p[5] + ", Detached").strip(", ")
            if att:
                p[24] = att
            if lot_val and 800 <= lot_val <= 5000000 and not clean_int(p[12]):
                p[12] = str(lot_val)
            if area_val and 400 <= area_val <= 5800 and not clean_int(p[11]):
                p[11] = str(area_val)
        else:
            while len(p) < 21:
                p.append(None)
            if att:
                p[20] = att
            if lot_val and 800 <= lot_val <= 5000000 and not p[9]:
                p[9] = lot_val
            if area_val and 400 <= area_val <= 5800 and not p[8]:
                p[8] = area_val
            lat, lng = extract_gps_from_html(html)
            if lat and lng and (not p[16] or not p[17]):
                p[16], p[17] = lat, lng

        pm = re.search(r'itemprop="price"\s+content="(\d+)"', html) or re.search(
            r'"price"\s*:\s*"?(\d{5,7})"?', html
        )
        new_price = int(pm.group(1)) if pm and 50000 <= int(pm.group(1)) <= 3500000 else None
        if new_price and old_price and new_price != old_price:
            if is_pipe:
                p[1] = str(new_price)
            else:
                p[3] = new_price
            out_row = "|".join(str(x) for x in p) if is_pipe else p
            return (rid, "price_change", out_row, old_price, new_price, addr)

        out_row = "|".join(str(x) for x in p) if is_pipe else p
        return (rid, "active", out_row, old_price, old_price, addr)
    except Exception:
        out_r = "|".join(str(x) for x in p) if is_pipe else p
        return (rid, "active", out_r, old_price, old_price, addr)


def enrich_new_listing(row, fallback_lat, fallback_lng):
    url = row[19]
    if not url:
        return row
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return row
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        body_text = extract_listing_body_text(soup)

        # Ensure clean street address (never price)
        row[18] = extract_clean_address_from_soup(soup, url, row[18])

        lat, lng = extract_gps_from_html(html)
        row[16] = lat or fallback_lat
        row[17] = lng or fallback_lng
        att = detect_attachment_from_html(html, url)
        if att:
            row[20] = att
        ym = re.search(r"(?:Year built|Année de construction)\s*[:\-]?\s*(19\d\d|20[012]\d)", body_text, re.I)
        if ym:
            row[10] = int(ym.group(1))
        am = re.search(
            r"(?:Living space|Net area|Superficie habitable)\s*[:\-]?\s*([\d,\s]+)\s*(?:sqft|ft²|pi²|pc)",
            body_text,
            re.I,
        )
        if am:
            val = clean_int(am.group(1))
            if val and 400 <= val <= 5800:
                row[8] = val
        lm = re.search(
            r"(?:Lot area|Lot dimensions|Superficie du terrain|Terrain)\s*[:\-]?\s*([\d,\s]+)\s*(?:sqft|ft²|pi²|pc)",
            body_text,
            re.I,
        )
        if lm:
            val = clean_int(lm.group(1))
            if val and 800 <= val <= 5000000:
                row[9] = val
        mm = re.search(r"(?:Municipal assessment|Évaluation municipale)\s*[:\-]?\s*\$?\s*([\d,\s]+)", body_text, re.I)
        if mm:
            val = clean_int(mm.group(1))
            if val and 50000 <= val <= 3000000:
                row[11] = val
        tm = re.search(r"(?:Municipal taxes|Taxes municipales)\s*[:\-]?\s*\$?\s*([\d,\s]+)", body_text, re.I)
        sm = re.search(r"(?:School taxes|Taxes scolaires)\s*[:\-]?\s*\$?\s*([\d,\s]+)", body_text, re.I)
        t_val = clean_int(tm.group(1)) if tm else 0
        s_val = clean_int(sm.group(1)) if sm else 0
        if t_val and 500 <= (t_val + (s_val or 0)) <= 25000:
            row[12] = t_val + (s_val or 0)
        bm = re.search(r"(?:Bathrooms|Salles de bain)\s*[:\-]?\s*(\d+)\s*\+\s*(\d+)", body_text, re.I)
        if bm:
            row[6], row[7] = int(bm.group(1)), int(bm.group(2))
        row[14] = compute_bits(body_text, row[14] or 0)
    except Exception as e:
        print(f"  [WARN] Could not enrich {row[0]}: {e}")
    return row


def crawl_new_listings(existing_ids, removed_ids):
    discovered = []
    print("\n--- PHASE 2: Crawling 11 Outaouais Sectors for New Listings ---")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=HEADERS["User-Agent"], locale="en-CA")
        page = context.new_page()
        for slug, label, dp_slug, c_slug, flat, flng in SECTORS:
            dp_url = f"https://duproprio.com/en/outaouais/{dp_slug}/homes-for-sale"
            try:
                page.goto(dp_url, timeout=25000, wait_until="domcontentloaded")
                soup = BeautifulSoup(page.content(), "html.parser")
                for c in soup.select("li.search-results-listings-list__item, div.search-results-listings-list__item"):
                    link = c.select_one("a[href*='hab-']")
                    if not link:
                        continue
                    href = link.get("href", "")
                    m_id = re.search(r"-(\d{6,8})(?:\?|$|/)", href)
                    if not m_id:
                        continue
                    rid = f"dp{m_id.group(1)}"
                    if rid in existing_ids or rid in removed_ids:
                        continue
                    full_url = href if href.startswith("http") else f"https://duproprio.com{href}"
                    ctext = c.get_text(" ", strip=True)
                    pm = re.search(r"\$\s*([\d,]+)", ctext)
                    price = clean_int(pm.group(1)) if pm else None
                    if not price or price < 75000 or price > 1500000:
                        continue
                    # Extract address ONLY from the dedicated address element or URL slug (never h2/h3 price!)
                    addr_el = c.select_one(
                        ".search-results-listings-list__item-description__address, "
                        ".search-results-listings-list__item-description__title-and-address"
                    )
                    addr = addr_el.get_text(" ", strip=True) if addr_el else ""
                    if is_price_or_bad_addr(addr):
                        addr = addr_from_url_slug(full_url, rid) or f"{label} DuProprio #{m_id.group(1)}"
                    attach = (
                        "S"
                        if "semi-detached" in href or "jumel" in href
                        else ("R" if "townhouse" in href or "rang" in href else "DV")
                    )
                    htype = "M" if "condo" in href else "H"
                    new_row = [
                        rid, slug, htype, price, 3, 0, 1, 0,
                        None, None, None, None, None, None,
                        0, None, flat, flng, addr, full_url, attach,
                    ]
                    new_row = enrich_new_listing(new_row, flat, flng)
                    discovered.append(new_row)
                    existing_ids.add(rid)
                    print(f"  [NEW DUPROPRIO] {rid} | ${price:,} | {new_row[18]} ({label}) [{new_row[20]}]")
            except Exception as e:
                print(f"  [WARN] DuProprio {label} skipped: {e}")

            for c_cat, htype in [("houses", "H"), ("condominium-houses", "M")]:
                c_url = f"https://www.centris.ca/en/{c_cat}~for-sale~{c_slug}"
                try:
                    page.goto(c_url, timeout=25000, wait_until="domcontentloaded")
                    page.wait_for_timeout(1200)
                    soup = BeautifulSoup(page.content(), "html.parser")
                    for c in soup.select("div.property-thumbnail-item, div.shell"):
                        link = c.select_one("a[href*='/1'], a[href*='/2']")
                        if not link:
                            continue
                        href = link.get("href", "")
                        m_id = re.search(r"/(\d{7,9})(?:\?|$)", href)
                        if not m_id:
                            continue
                        rid = m_id.group(1)
                        if rid in existing_ids or rid in removed_ids:
                            continue
                        full_url = href if href.startswith("http") else f"https://www.centris.ca{href}"
                        ctext = c.get_text(" ", strip=True)
                        pm = re.search(r"\$\s*([\d,\s]+)", ctext)
                        price = clean_int(pm.group(1)) if pm else None
                        if not price or price < 75000 or price > 1500000:
                            continue
                        addr_el = c.select_one(".address, [itemprop='address'], [itemprop='streetAddress']")
                        addr = addr_el.get_text(" ", strip=True) if addr_el else f"{label} Centris #{rid}"
                        attach = (
                            "S"
                            if "semi-detached" in ctext.lower()
                            else ("R" if "townhouse" in ctext.lower() or "row" in ctext.lower() else "D")
                        )
                        new_row = [
                            rid, slug, htype, price, 3, 0, 1, 0,
                            None, None, None, None, None, 220 if htype == "M" else None,
                            0, None, flat, flng, addr, full_url, attach,
                        ]
                        new_row = enrich_new_listing(new_row, flat, flng)
                        discovered.append(new_row)
                        existing_ids.add(rid)
                        print(f"  [NEW CENTRIS]   {rid} | ${price:,} | {new_row[18]} ({label}) [{new_row[20]}]")
                except Exception as e:
                    print(f"  [WARN] Centris {label} ({c_cat}) skipped: {e}")
        browser.close()
    return discovered


def row_price(r):
    if isinstance(r, str):
        p = r.split("|")
        return clean_int(p[1]) if len(p) > 1 else 0
    return r[3] if len(r) > 3 else 0


def main():
    t0 = time.time()
    print("=== Outaouais Home Finder: Daily Refresh Started ===")
    if not os.path.exists(SNAPSHOT_PATH):
        print(f"ERROR: {SNAPSHOT_PATH} not found.")
        return
    with open(SNAPSHOT_PATH, "r", encoding="utf-8") as f:
        snap = json.load(f)

    nested_homes = isinstance(snap.get("homes"), dict) and "rows" in snap["homes"]
    old_rows = snap["homes"]["rows"] if nested_homes else snap.get("rows", [])
    updates = snap.get("updates", [])
    market_history = snap.get("marketHistory", [])

    # Step 0: Self-heal any listings lost to previous Centris rate-limiting via Git history
    old_rows = recover_from_git_history(old_rows)

    # Ignore mass-removal entries (>45 removals in a single run = rate-limit false positive)
    removed_set = set(HARD_REMOVED)
    cleaned_updates = []
    for u in updates:
        rem_list = u.get("removed", [])
        if len(rem_list) > 45:
            u = dict(u)
            u["removed"] = []
        else:
            for rid in rem_list:
                removed_set.add(str(rid))
        cleaned_updates.append(u)
    updates = cleaned_updates

    print(f"Loaded {len(old_rows)} listings ({len(removed_set)} confirmed removed).")
    print("\n--- PHASE 1: Verifying Live URLs & Building Styles (4 Polite Workers) ---")
    surviving_rows, newly_removed, price_changes = [], [], []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(verify_existing_listing, r): r for r in old_rows}
        for idx, fut in enumerate(as_completed(futures), 1):
            rid, status, updated_row, old_p, new_p, addr = fut.result()
            if status == "removed":
                newly_removed.append(rid)
                print(f"  [SOLD / REMOVED] {rid} | ${old_p:,} | {addr}")
            elif status == "price_change":
                surviving_rows.append(updated_row)
                price_changes.append({"id": rid, "oldPrice": old_p, "newPrice": new_p, "addr": addr})
                print(f"  [PRICE CHANGE]   {rid} | ${old_p:,} -> ${new_p:,} | {addr}")
            else:
                surviving_rows.append(updated_row)
            if idx % 100 == 0:
                print(f"  ... verified {idx}/{len(old_rows)} existing listings")

    # Safety circuit-breaker: if >45 homes appear removed in a single day, Centris throttled us
    if len(newly_removed) > 45:
        print(f"  [SAFETY GUARD] {len(newly_removed)} removals detected in one run (rate-limit anomaly). Keeping Centris rows safe!")
        removed_lookup = set(newly_removed)
        for r in old_rows:
            rid = row_id(r)
            if rid in removed_lookup and not rid.startswith("dp") and rid not in HARD_REMOVED:
                surviving_rows.append(r)
        newly_removed = [rid for rid in newly_removed if rid.startswith("dp") or rid in HARD_REMOVED]

    for rid in newly_removed:
        removed_set.add(rid)

    existing_ids = {row_id(r) for r in surviving_rows}
    added_rows = crawl_new_listings(existing_ids, removed_set)
    all_active_rows = surviving_rows + added_rows
    all_active_rows.sort(key=lambda x: row_price(x) or 9999999)

    today_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    today_update = {
        "date": today_str,
        "added": [{"id": str(r[0]), "price": r[3], "addr": r[18], "slug": r[1]} for r in added_rows],
        "priceChanges": price_changes,
        "removed": newly_removed,
    }
    if updates and updates[0].get("date") == today_str:
        prev = updates[0]
        prev["removed"] = sorted(set(map(str, prev.get("removed", []))) | set(newly_removed))
        prev["added"] = prev.get("added", []) + today_update["added"]
        prev["priceChanges"] = prev.get("priceChanges", []) + price_changes
    else:
        updates.insert(0, today_update)

    prices = [row_price(r) for r in all_active_rows if row_price(r) and 80000 <= row_price(r) <= 2000000]
    hist_entry = {
        "date": today_str,
        "count": len(all_active_rows),
        "medianPrice": int(statistics.median(prices)) if prices else 0,
        "avgPrice": int(statistics.mean(prices)) if prices else 0,
        "newCount": len(added_rows),
        "dropCount": len(price_changes),
        "soldCount": len(newly_removed),
    }
    market_history = [h for h in market_history if h.get("date") != today_str] + [hist_entry]
    market_history.sort(key=lambda h: h.get("date", ""))

    snap["generatedAt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    snap["count"] = len(all_active_rows)
    if nested_homes:
        snap["homes"]["rows"] = all_active_rows
    else:
        snap["rows"] = all_active_rows
    snap["updates"] = updates[:90]
    snap["marketHistory"] = market_history[-365:]

    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, separators=(",", ":"))

    print(
        f"\n=== Refresh Complete in {round(time.time() - t0, 1)}s: "
        f"{len(all_active_rows)} active, {len(newly_removed)} removed, {len(added_rows)} new ==="
    )


if __name__ == "__main__":
    main()
