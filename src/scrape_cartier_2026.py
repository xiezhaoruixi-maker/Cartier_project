"""
Scrape Cartier watches from Algolia index and save to data/raw/current_2026_raw_YYYYMMDD_HHMMSS.csv

Docker-ready:
- No Windows absolute paths
- Secrets via env (.env + docker-compose) preferred
- Configurable via CLI args
"""

import os
import time
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional

import requests
import pandas as pd

def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip()

def build_algolia_config() -> Dict[str, str]:
    """
    Prefer ENV vars so you don't hardcode secrets in repo.
    Put these in .env:
      ALGOLIA_APP_ID=...
      ALGOLIA_API_KEY=...
      ALGOLIA_INDEX=...
    """
    app_id = getenv("ALGOLIA_APP_ID", "96TW5XP97E")
    api_key = getenv("ALGOLIA_API_KEY")  # no default: force you to set for safety
    index = getenv("ALGOLIA_INDEX", "prod_cartier_europe_fr_fr_products")

    if not api_key:
        raise RuntimeError(
            "Missing ALGOLIA_API_KEY. Create a .env file in repo root and set:\n"
            "ALGOLIA_API_KEY=xxxxxxxx\n"
            "Then run via: docker compose run --rm cartier python src/scrape_cartier_2026.py"
        )

    url = f"https://{app_id.lower()}.algolia.net/1/indexes/{index}/query"

    headers = {
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://www.cartier.com",
        "Referer": "https://www.cartier.com/",
    }
    return {"app_id": app_id, "api_key": api_key, "index": index, "url": url, "headers": headers}


def build_payload(page: int, hits_per_page: int, category_filter: str) -> Dict[str, Any]:
    return {
        "attributesToHighlight": ["productName", "shortDescription", "description"],
        "attributesToRetrieve": ["*"],
        "clickAnalytics": True,
        "facets": ["*"],
        "filters": f"categoryId:{category_filter}",
        "highlightPostTag": "</em>",
        "highlightPreTag": "<em>",
        "hitsPerPage": hits_per_page,
        "maxValuesPerFacet": 100,
        "page": page,
    }


def fetch_page(url: str, headers: Dict[str, str], page: int, hits_per_page: int, category_filter: str, timeout: int = 30) -> Dict[str, Any]:
    payload = build_payload(page=page, hits_per_page=hits_per_page, category_filter=category_filter)
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:800]}")
    data = r.json()
    if "hits" not in data:
        raise RuntimeError(f"Unexpected response (no 'hits'): keys={list(data.keys())} | head={str(data)[:800]}")
    return data

def pick_reference_code(hit: Dict[str, Any]) -> str:
    for k in ["globalReference", "shortGlobalReference", "localReference", "objectID"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return ""


def pick_local_reference(hit: Dict[str, Any]) -> str:
    v = hit.get("localReference") or hit.get("shortGlobalReference") or ""
    return v.strip().upper() if isinstance(v, str) else ""


def pick_title(hit: Dict[str, Any]) -> str:
    for k in ["productName", "englishProductName", "title", "productModel"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def pick_price(hit: Dict[str, Any]) -> Optional[float]:
    pv = hit.get("priceValue")
    if isinstance(pv, (int, float)):
        return float(pv)

    p = hit.get("price")
    if isinstance(p, str) and p.strip():
        s = p.strip()
        s = s.replace("€", "").replace("\u202f", "").replace("\xa0", "")
        s = s.replace(" ", "")
        s = s.replace(".", "")      # thousands sep (rare)
        s = s.replace(",", ".")     # FR decimal
        try:
            return float(s)
        except Exception:
            return None
    return None


def pick_currency(hit: Dict[str, Any]) -> str:
    for k in ["priceCurrency", "currency"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return "EUR"


def pick_url(hit: Dict[str, Any]) -> str:
    for k in ["newPdpLink", "pdpLink", "oldPdpLink"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            if v.startswith("http"):
                return v
            return "https://www.cartier.com" + v
    return ""


def pick_collection(hit: Dict[str, Any]) -> str:
    for k in ["collectionProductLine", "englishCollectionProductLine", "englishCollectionName", "collectionText"]:
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
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
        "objectID": str(hit.get("objectID", "")).strip(),
    }

def scrape_all(url: str, headers: Dict[str, str], hits_per_page: int, sleep_sec: float, category_filter: str, max_pages: Optional[int]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    first = fetch_page(url, headers, page=0, hits_per_page=hits_per_page, category_filter=category_filter)
    nb_pages = int(first.get("nbPages", 1))
    nb_hits = int(first.get("nbHits", 0))
    print(f"[INFO] nbHits={nb_hits}, nbPages={nb_pages}, hitsPerPage={hits_per_page}, filter=categoryId:{category_filter}")

    pages_to_fetch = nb_pages
    if isinstance(max_pages, int) and max_pages > 0:
        pages_to_fetch = min(pages_to_fetch, max_pages)

    for h in first.get("hits", []):
        rows.append(hit_to_row(h))
    print(f"[INFO] fetched page 1/{pages_to_fetch} | total_rows={len(rows)}")

    for page in range(1, pages_to_fetch):
        time.sleep(sleep_sec)
        data = fetch_page(url, headers, page=page, hits_per_page=hits_per_page, category_filter=category_filter)
        for h in data.get("hits", []):
            rows.append(hit_to_row(h))
        print(f"[INFO] fetched page {page+1}/{pages_to_fetch} | total_rows={len(rows)}")

    df = pd.DataFrame(rows)

    before = len(df)
    df["reference_code"] = df["reference_code"].fillna("").astype(str).str.strip().str.upper()
    df = df[df["reference_code"] != ""].copy()
    print(f"[QA] drop empty reference_code: {before} -> {len(df)}")

    before = len(df)
    df = df.drop_duplicates(subset=["reference_code"], keep="first")
    print(f"[QA] dedup by reference_code: {before} -> {len(df)}")

    return df


def save_df(df: pd.DataFrame, out_dir: str, base_name: str) -> str:
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"{base_name}_{now_ts()}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] saved: {out_path} | rows={len(df)} cols={len(df.columns)}")
    return out_path


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hits-per-page", type=int, default=1000)
    ap.add_argument("--sleep-sec", type=float, default=0.2)
    ap.add_argument("--max-pages", type=int, default=0, help="0 means fetch all pages; set 1/2 for quick test")
    ap.add_argument("--category-filter", type=str, default="WATCH")
    ap.add_argument("--out-dir", type=str, default=os.path.join("data", "raw"))
    ap.add_argument("--out-name", type=str, default="current_2026_raw")
    return ap.parse_args()


def main():
    args = parse_args()
    cfg = build_algolia_config()

    max_pages = None if args.max_pages <= 0 else args.max_pages
    df = scrape_all(
        url=cfg["url"],
        headers=cfg["headers"],
        hits_per_page=args.hits_per_page,
        sleep_sec=args.sleep_sec,
        category_filter=args.category_filter,
        max_pages=max_pages,
    )

    print("[QA] currency top:", df["currency"].value_counts().head(10).to_dict())
    save_df(df, out_dir=args.out_dir, base_name=args.out_name)


if __name__ == "__main__":
    main()
