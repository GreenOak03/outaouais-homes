#!/usr/bin/env python3
"""
Full daily Centris & DuProprio scraper + sold-listing verifier for Outaouais Home Finder.
1. Verifies every active listing URL in snapshot.json: if Centris or DuProprio redirects
   away from the listing ID (or shows a Sold banner / 404), marks the listing as removed.
   Also detects any price drops directly from the live listing page.
2. Crawls Centris & DuProprio across all 11 Outaouais sectors for newly added 3+ bedroom
   homes up to $1M and geocodes them via OpenStreetMap Nominatim.
"""
import json, re, time, datetime, pathlib, urllib.request, urllib.parse
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

def geocode(addr, town):
    q = urllib.parse.quote(f"{addr}, {town}, QC, Canada")
    url = f"https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1"
    req = urllib.request.Request(url, headers={"User-Agent": "OutaouaisHomeFinder/1.0 (github-actions)"})
    try:
        time.sleep(1.05)
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
            if data:
                return [round(float(data[0]["lat"]), 5), round(float(data[0]["lon"]), 5)]
    except Exception:
        pass
    return None

def clean_int(val):
    if not val:
        return ""
    digits = re.sub(r"[^\d]", "", str(val))
    return int(digits) if digits else ""

def build_listing_url(row_str):
    parts = row_str.split("|")
    lid = parts[0]
    slug = parts[2] if len(parts) > 2 else "gatineau-gatineau"
    src = parts[22] if len(parts) > 22 and parts[22] else ("D" if lid.startswith("dp") else "C")
    urlp = (parts[23] if len(parts) > 23 else "").strip()
    if src == "D":
        if urlp.startswith("http"):
            return urlp
        if urlp.startswith("/en/") or urlp.startswith("/fr/"):
            return "https://duproprio.com" + urlp
        clean_p = re.sub(r"^~?/?(?:en/outaouais/|fr/outaouais/)?", "", urlp)
        return "https://duproprio.com/en/outaouais/" + clean_p
    else:
        if urlp.startswith("http"):
            return urlp
        if urlp.startswith("/"):
            return "https://www.centris.ca" + urlp
        return f"https://www.centris.ca/en/houses~for-sale~{slug}/{lid}"

def check_single_listing(lid, row_str):
    """Returns (lid, status, live_price) where status is 'active', 'gone', or 'unknown'."""
    parts = row_str.split("|")
    old_price = clean_int(parts[1]) or 0
    src = parts[22] if len(parts) > 22 and parts[22] else ("D" if lid.startswith("dp") else "C")
    url = build_listing_url(row_str)
    num_id = lid[2:] if lid.startswith("dp") else lid

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 404 or resp.status_code == 410:
            return (lid, "gone", None)
        if resp.status_code in (403, 429, 503):
            return (lid, "unknown", None)

        final_url = resp.url.lower()
        html = resp.text

        # Both Centris and DuProprio redirect sold/removed listings to the generic city/category page,
        # stripping the numeric listing ID from the final URL.
        if num_id.lower() not in final_url:
            return (lid, "gone", None)

        if src == "D":
            if re.search(r"j['’]ai vendu|cette propri[eé]t[eé] est vendue|this property is sold|property sold", html, re.I):
                return (lid, "gone", None)
            soup = BeautifulSoup(html, "html.parser")
            pr_el = soup.select_one(".listing-price__amount") or soup.select_one('meta[property="product:price:amount"]')
            if pr_el:
                raw_p = pr_el.get("content") if pr_el.has_attr("content") else pr_el.get_text()
                lp = clean_int(raw_p)
                if lp and 90000 <= lp <= 2000000:
                    return (lid, "active", lp)
        else:
            if re.search(r"property is no longer available|propri[eé]t[eé] n['’]est plus disponible", html, re.I):
                return (lid, "gone", None)
            soup = BeautifulSoup(html, "html.parser")
            pr_meta = soup.select_one('meta[itemprop="price"]')
            if pr_meta and pr_meta.get("content"):
                lp = clean_int(pr_meta["content"])
                if lp and 90000 <= lp <= 2000000:
                    return (lid, "active", lp)

        return (lid, "active", old_price)
    except Exception:
        return (lid, "unknown", None)

def make_row(lid, price, slug, addr, htype, style, beds, baths, area="", lot="", yr="", first_seen=TODAY, src="C", urlp=""):
    fields = [
        str(lid), str(price), slug, addr, htype, style,
        "", str(beds), "0", str(baths), "0",
        str(area), str(lot), str(yr), "", "", "", "", "2", "0", "",
        first_seen, src, urlp
    ]
    return "|".join(fields)

def scrape_duproprio(page, slug):
    found = {}
    for pnum in range(1, 8):
        url = f"https://duproprio.com/en/outaouais/{slug}/home-for-sale?pageNumber={pnum}"
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(1200)
            soup = BeautifulSoup(page.content(), "html.parser")
            items = soup.select("li.search-results-listings-list__item")
            if not items:
                break
            for item in items:
                if re.search(r"sold|vendu", item.get_text(" ", strip=True), re.I):
                    continue
                link_el = item.select_one("a.search-results-listings-list__item-image-link")
                if not link_el or not link_el.get("href"):
                    continue
                href = link_el["href"]
                m_id = re.search(r"-(\d+)\s*$", href)
                if not m_id:
                    continue
                lid = "dp" + m_id.group(1)
                price_el = item.select_one(".search-results-listings-list__item-description__price")
                price = clean_int(price_el.get_text() if price_el else "")
                if not price or price > MAX_PRICE or price < 90000:
                    continue
                addr_el = item.select_one(".search-results-listings-list__item-description__address")
                addr = addr_el.get_text(strip=True) if addr_el else "Address unlisted"
                text_blob = item.get_text(" ", strip=True)
                m_beds = re.search(r"(\d+)\s*bedroom", text_blob, re.I)
                beds = int(m_beds.group(1)) if m_beds else 3
                if beds < 3:
                    continue
                m_baths = re.search(r"(\d+)\s*bathroom", text_blob, re.I)
                baths = int(m_baths.group(1)) if m_baths else 1
                m_area = re.search(r"living\s*space[^\d]*([\d,]+)\s*ft", text_blob, re.I)
                area = clean_int(m_area.group(1)) if m_area else ""
                if area and area > 6000:
                    area = ""
                m_lot = re.search(r"lot[^\d]*([\d,]+)\s*ft", text_blob, re.I)
                lot = clean_int(m_lot.group(1)) if m_lot else ""
                style = "Bungalow, Detached" if "bungalow" in href.lower() else ("Two or more storey, Semi-detached" if "semi-detached" in href.lower() else "Two or more storey, Detached")
                htype = "M" if "townhouse" in href.lower() else "H"
                urlp = "~" + href.split("/outaouais/")[-1] if "/outaouais/" in href else href
                found[lid] = (price, make_row(lid, price, slug, addr, htype, style, beds, baths, area=area, lot=lot, src="D", urlp=urlp), addr)
        except Exception as e:
            print(f"  [DuProprio {slug} p{pnum}] {e}")
            break
    return found

def scrape_centris(page, slug):
    found = {}
    url = f"https://www.centris.ca/en/houses~for-sale~{slug}"
    try:
        page.goto(url, timeout=35000, wait_until="domcontentloaded")
        page.wait_for_timeout(1800)
        for _ in range(12):
            soup = BeautifulSoup(page.content(), "html.parser")
            cards = soup.select("div.property-thumbnail-item")
            for card in cards:
                sku = card.select_one('meta[itemprop="sku"]')
                pr_meta = card.select_one('meta[itemprop="price"]')
                if not sku or not pr_meta:
                    continue
                lid = sku.get("content", "").strip()
                price = clean_int(pr_meta.get("content", ""))
                if not lid or not price or price > MAX_PRICE or price < 90000:
                    continue
                beds_el = card.select_one(".cac")
                beds = clean_int(beds_el.get_text() if beds_el else "3") or 3
                if beds < 3:
                    continue
                baths_el = card.select_one(".sdb")
                baths = clean_int(baths_el.get_text() if baths_el else "1") or 1
                addr_div = card.select_one(".address")
                addr_lines = [d.get_text(strip=True) for d in addr_div.select("div")] if addr_div else []
                addr = addr_lines[0] if addr_lines else "Address on Centris"
                a_el = card.select_one("a.property-thumbnail-summary-link")
                urlp = a_el["href"] if a_el and a_el.get("href") else f"/en/houses~for-sale~{slug}/{lid}"
                cat_el = card.select_one(".category")
                cat_txt = cat_el.get_text(" ", strip=True) if cat_el else ""
                style = "Bungalow, Detached" if "bungalow" in cat_txt.lower() else ("Two or more storey, Semi-detached" if "semi-detached" in cat_txt.lower() else "Two or more storey, Detached")
                found[lid] = (price, make_row(lid, price, slug, addr, "H", style, beds, baths, src="C", urlp=urlp), addr)
            next_btn = page.query_selector("li.next:not(.inactive) a")
            if not next_btn:
                break
            next_btn.click()
            page.wait_for_timeout(1400)
    except Exception as e:
        print(f"  [Centris {slug}] {e}")
    return found

def main():
    old = json.loads(SNAP_PATH.read_text("utf-8")) if SNAP_PATH.exists() else {
        "asOf": TODAY, "homes": {"asOf": TODAY, "rows": []}, "geo": {"coords": {}}, "updates": [], "marks": {}
    }
    old_rows = {}
    for r in old.get("homes", {}).get("rows", []):
        parts = r.split("|")
        if parts and parts[0]:
            old_rows[parts[0]] = r
    coords = old.get("geo", {}).get("coords", {})
    updates = old.get("updates", [])

    already_removed = set()
    for u in updates:
        for rid in u.get("removed", []):
            already_removed.add(str(rid))

    active_to_check = {lid: r for lid, r in old_rows.items() if lid not in already_removed}
    print(f"Phase 1: Checking {len(active_to_check)} active listing URLs for sold/off-market status & price drops...")

    fresh_rows = dict(old_rows)
    added, price_changes, newly_removed = [], [], []

    with ThreadPoolExecutor(max_workers=10) as pool:
        futs = {pool.submit(check_single_listing, lid, r): lid for lid, r in active_to_check.items()}
        for fut in as_completed(futs):
            lid, status, live_price = fut.result()
            if status == "gone":
                newly_removed.append(lid)
                already_removed.add(lid)
                print(f"  [SOLD / REMOVED] {lid}")
            elif status == "active" and live_price:
                parts = fresh_rows[lid].split("|")
                old_p = clean_int(parts[1]) or live_price
                if old_p != live_price:
                    price_changes.append([lid, old_p, live_price])
                    parts[1] = str(live_price)
                    fresh_rows[lid] = "|".join(parts)
                    print(f"  [PRICE CHANGE] {lid}: {old_p} -> {live_price}")

    print(f"Phase 1 complete: {len(newly_removed)} sold/removed, {len(price_changes)} price changes.")
    print("Phase 2: Crawling Centris & DuProprio sectors for new listings...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = context.new_page()
        for slug, town in SECTORS:
            print(f"Scanning {slug} ({town})...")
            found = {}
            found.update(scrape_duproprio(page, slug))
            found.update(scrape_centris(page, slug))
            for lid, (new_price, row_str, addr) in found.items():
                if lid in fresh_rows:
                    parts = fresh_rows[lid].split("|")
                    old_price = clean_int(parts[1]) or new_price
                    if old_price != new_price and not any(pc[0] == lid for pc in price_changes):
                        price_changes.append([lid, old_price, new_price])
                        parts[1] = str(new_price)
                        fresh_rows[lid] = "|".join(parts)
                else:
                    fresh_rows[lid] = row_str
                    added.append(row_str)
                    if lid not in coords:
                        ll = geocode(addr, town)
                        if ll:
                            coords[lid] = ll
                    print("  [NEW] " + str(lid) + ": " + str(addr) + " ($" + str(new_price) + ")")
        browser.close()

    old["asOf"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    old["homes"]["asOf"] = TODAY
    old["homes"]["rows"] = list(fresh_rows.values())
    old["geo"]["coords"] = coords
    if added or price_changes or newly_removed:
        updates.append({"date": TODAY, "added": added, "price": price_changes, "removed": newly_removed})
    old["updates"] = updates
    SNAP_PATH.write_text(json.dumps(old, ensure_ascii=False, indent=2), "utf-8")
    print(f"Done! Saved snapshot.json ({len(fresh_rows) - len(already_removed)} active homes, -{len(newly_removed)} sold/removed, +{len(added)} new, {len(price_changes)} price changes).")

if __name__ == "__main__":
    main()
