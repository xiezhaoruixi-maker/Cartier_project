# scrape_cartier_2026.py
# Goal: generate current_2026_raw.csv from Cartier FR site (Algolia endpoint seen in DevTools)
# Output columns (safe for later joins / feature engineering):
#   reference_code, local_reference, title, price, currency, url, collection, objectID
#
# Why this version works:
# - Uses the EXACT request URL shape from your DevTools (prod_cartier_europe_fr_fr_products)
# - Uses JSON payload (not the "params" string) like DevTools "Request Payload"
# - Paginates using nbPages / page
# - Extracts reference_code from globalReference first (fallbacks included)
# - Avoids PermissionError by writing a timestamped file into data/raw/

import os
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

import requests
import pandas as pd


# ========== 1) Configure from DevTools ==========
ALGOLIA_APP_ID = "96TW5XP97E"
ALGOLIA_API_KEY = "c4abddc2d213f6c80ebdb1a1327c0303"  # from DevTools request headers

# IMPORTANT: this index name must match DevTools "Request URL"
ALGOLIA_INDEX = "prod_cartier_europe_fr_fr_products"

URL = f"https://{ALGOLIA_APP_ID.lower()}.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

HEADERS = {
    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
    "X-Algolia-API-Key": ALGOLIA_API_KEY,
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": "https://www.cartier.com",
    "Referer": "https://www.cartier.com/",
}

# ========== 2) Payload template (matches DevTools Request Payload) ==========
def build_payload(page: int, hits_per_page: int = 1000) -> Dict[str, Any]:
    # Mirrors what you showed in DevTools:
    # hitsPerPage, page, attributesToRetrieve ["*"], facets ["*"], filters 'categoryId:WATCH'
    return {
        "attributesToHighlight": ["productName", "shortDescription", "description"],
        "attributesToRetrieve": ["*"],
        "clickAnalytics": True,
        "facets": ["*"],
        "filters": 'categoryId:WATCH',
        "highlightPostTag": "</em>",
        "highlightPreTag": "<em>",
        "hitsPerPage": hits_per_page,
        "maxValuesPerFacet": 100,
        "page": page,
    }


# ========== 3) HTTP fetch ==========
def fetch_page(page: int, hits_per_page: int = 1000, timeout: int = 30) -> Dict[str, Any]:
    payload = build_payload(page=page, hits_per_page=hits_per_page)

    r = requests.post(URL, headers=HEADERS, json=payload, timeout=timeout)
    if r.status_code != 200:
        # Print enough context to debug quickly
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:800]}")

    data = r.json()
    # Basic sanity check
    if "hits" not in data:
        raise RuntimeError(f"Unexpected response (no 'hits'): keys={list(data.keys())} | head={str(data)[:800]}")
    return data


# ========== 4) Field extraction ==========
def pick_reference_code(hit: Dict[str, Any]) -> str:
    """
    Prefer globalReference (e.g., 'CRWSPN0013') because it is the most stable for cross-year joins.
    Fallbacks included to avoid empty values.
    """
    for k in ["globalReference", "shortGlobalReference", "localReference", "objectID"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def pick_local_reference(hit: Dict[str, Any]) -> str:
    v = hit.get("localReference") or hit.get("shortGlobalReference") or ""
    return v.strip() if isinstance(v, str) else ""


def pick_title(hit: Dict[str, Any]) -> str:
    # Your sample has productName (FR), englishProductName (EN)
    for k in ["productName", "englishProductName", "title", "productModel"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def pick_price(hit: Dict[str, Any]) -> Optional[float]:
    # Prefer numeric priceValue when available
    pv = hit.get("priceValue")
    if isinstance(pv, (int, float)):
        return float(pv)

    # Fallback: parse "5,000€" -> 5000
    p = hit.get("price")
    if isinstance(p, str) and p.strip():
        s = p.strip()
        s = s.replace("€", "").replace("\u202f", "").replace("\xa0", "")
        s = s.replace(" ", "")
        s = s.replace(".", "")  # just in case thousands separators appear
        s = s.replace(",", ".")  # FR decimal
        try:
            return float(s)
        except Exception:
            return None
    return None


def pick_currency(hit: Dict[str, Any]) -> str:
    for k in ["priceCurrency", "currency"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return "EUR"


def pick_url(hit: Dict[str, Any]) -> str:
    # sample has newPdpLink / pdpLink / oldPdpLink
    for k in ["newPdpLink", "pdpLink", "oldPdpLink"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            # Make absolute
            if v.startswith("http"):
                return v
            return "https://www.cartier.com" + v
    return ""


def pick_collection(hit: Dict[str, Any]) -> str:
    for k in ["collectionProductLine", "englishCollectionProductLine", "englishCollectionName", "collectionText"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # Sometimes "_collections" is a list; keep first meaningful
    cols = hit.get("_collections")
    if isinstance(cols, list) and cols:
        return str(cols[0])
    return ""


def hit_to_row(hit: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "reference_code": pick_reference_code(hit),
        "local_reference": pick_local_reference(hit),
        "title": pick_title(hit),
        "price": pick_price(hit),
        "currency": pick_currency(hit),
        "url": pick_url(hit),
        "collection": pick_collection(hit),
        "objectID": hit.get("objectID", ""),
    }


# ========== 5) Main scrape ==========
def scrape_all(hits_per_page: int = 1000, sleep_sec: float = 0.2, max_pages: Optional[int] = None) -> pd.DataFrame:
    all_rows: List[Dict[str, Any]] = []

    first = fetch_page(page=0, hits_per_page=hits_per_page)
    nb_pages = int(first.get("nbPages", 1))
    nb_hits = int(first.get("nbHits", 0))

    print(f"Detected nbHits={nb_hits}, nbPages={nb_pages}, hitsPerPage={hits_per_page}")

    pages_to_fetch = nb_pages
    if isinstance(max_pages, int) and max_pages > 0:
        pages_to_fetch = min(pages_to_fetch, max_pages)

    # page 0
    hits0 = first.get("hits", [])
    for h in hits0:
        all_rows.append(hit_to_row(h))
    print(f"Fetched page 1/{pages_to_fetch} | total_rows={len(all_rows)}")

    # remaining pages
    for page in range(1, pages_to_fetch):
        time.sleep(sleep_sec)
        data = fetch_page(page=page, hits_per_page=hits_per_page)
        hits = data.get("hits", [])
        for h in hits:
            all_rows.append(hit_to_row(h))
        print(f"Fetched page {page+1}/{pages_to_fetch} | total_rows={len(all_rows)}")

    df = pd.DataFrame(all_rows)

    # Basic cleanup
    if "reference_code" in df.columns:
        df["reference_code"] = df["reference_code"].astype(str).str.strip()

    # Drop empty reference_code rows (still keep if you want; but A-role needs stable ids)
    before = len(df)
    df = df[df["reference_code"].astype(str).str.len() > 0].copy()
    print(f"Drop empty reference_code: {before} -> {len(df)}")

    # Deduplicate by reference_code (keep first)
    before = len(df)
    df = df.drop_duplicates(subset=["reference_code"], keep="first")
    print(f"Dedup by reference_code: {before} -> {len(df)}")

    return df


def save_df(df: pd.DataFrame) -> str:
    # Avoid PermissionError by writing timestamped file
    out_dir = os.path.join("data", "raw")
    os.makedirs(out_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"current_2026_raw_{ts}.csv")

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Saved -> {out_path} | rows={len(df)} | cols={len(df.columns)}")
    return out_path


def main():
    # You can tune these if needed
    HITS_PER_PAGE = 1000
    SLEEP_SEC = 0.2
    MAX_PAGES = None  # set to 1/2 for quick test

    df = scrape_all(hits_per_page=HITS_PER_PAGE, sleep_sec=SLEEP_SEC, max_pages=MAX_PAGES)

    # Quick QA
    missing_ref = df["reference_code"].isna().mean() if "reference_code" in df.columns else None
    print("Missing reference_code ratio:", missing_ref)

    save_df(df)


if __name__ == "__main__":
    main()
