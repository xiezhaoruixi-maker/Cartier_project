# src/feature_engineering.py
# -*- coding: utf-8 -*-
"""
Cartier 2026 Feature Engineering
Outputs:
  1) baseline_2026_fe_YYYYMMDD_HHMMSS.csv  (full engineered table)
  2) current_2026_labeled.csv              (row-level labeled table required by teacher)
"""

import os

# Avoid pandas importing pyarrow backend (common env issue)
os.environ["PANDAS_ARROW_DISABLED"] = "1"

import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


# ========= 0) Paths (repo-relative) =========
REPO_ROOT = Path(__file__).resolve().parents[1]  # .../Cartier_project
DATA_RAW = REPO_ROOT / "data" / "raw"
DATA_PROCESSED = REPO_ROOT / "data" / "processed"

# Preferred: set an explicit raw file name here if you want.
# If file not found, script will auto-pick the latest current_2026_raw_*.csv in data/raw.
RAW_2026_PATH = DATA_RAW / "current_2026_raw_20260211_140133.csv"

# Output directory
OUT_DIR = DATA_PROCESSED

# Canonical collections (teacher hint: str.contains on Title works well)
COLLECTION_PATTERNS = [
    ("Tank", r"\btank\b"),
    ("Santos", r"\bsantos\b"),
    ("Panthère", r"panth[èe]re|panthere"),
    ("Ballon Bleu", r"ballon\s*bleu"),
    ("Trinity", r"\btrinity\b"),
]


# ========= 1) Utils =========
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def pick_latest_raw_2026(data_raw_dir: Path) -> Path:
    """Pick latest current_2026_raw_*.csv if RAW_2026_PATH doesn't exist."""
    cands = sorted(data_raw_dir.glob("current_2026_raw_*.csv"))
    if not cands:
        raise FileNotFoundError(
            f"No raw 2026 file found under: {data_raw_dir}. "
            f"Expected something like current_2026_raw_YYYYMMDD_HHMMSS.csv"
        )
    return cands[-1]


def parse_eur_price_to_float(x) -> float:
    """
    Input examples:
      "5,000€" / "5 000€" / "5000 €" / 5000.0 / None
    Output:
      5000.0 or NaN
    """
    if pd.isna(x):
        return float("nan")
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)

    s = str(x).strip()
    if s == "":
        return float("nan")

    # Remove euro sign, NBSP, and normalize spaces
    s = s.replace("€", "").replace("\u00a0", " ").strip()
    s = s.replace(" ", "")

    # Remove thousands separators (common integer EUR displays)
    s = s.replace(",", "").replace(".", "")

    m = re.search(r"(\d+)", s)
    if not m:
        return float("nan")
    return float(m.group(1))


def normalize_cartier_url(u: str, country_path: str = "/fr-fr") -> str:
    """
    When raw url is a relative path like /fr-fr/product/CR..., fill to https://www.cartier.com/fr-fr/...
    """
    if pd.isna(u):
        return ""
    s = str(u).strip()
    if s == "":
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s

    if not s.startswith(country_path):
        if s.startswith("/"):
            s = country_path + s
        else:
            s = country_path + "/" + s

    return "https://www.cartier.com" + s


def extract_market_from_url(url_full: str) -> str:
    """
    Extract market/locale from URL like https://www.cartier.com/fr-fr/...
    Fallback to 'fr-fr' when not found.
    """
    if not isinstance(url_full, str) or url_full.strip() == "":
        return "fr-fr"
    m = re.search(r"cartier\.com/([a-z]{2}-[a-z]{2})(?:/|$)", url_full.lower())
    return m.group(1) if m else "fr-fr"


def canonicalize_collection(collection: str, title: str, url_full: str) -> str:
    """
    Teacher hint: match directly in Title with str.contains is near-perfect.
    We'll match over (collection + title + url_full) to be robust.
    """
    text = f"{collection or ''} {title or ''} {url_full or ''}".lower()
    for name, pat in COLLECTION_PATTERNS:
        if re.search(pat, text, flags=re.IGNORECASE):
            return name
    return "Other"


def safe_write_csv(df: pd.DataFrame, out_dir: Path, base_name: str) -> Path:
    """
    Avoid PermissionError when file is open:
    always write a timestamped new filename.
    """
    ensure_dir(out_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{base_name}_{ts}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def try_write_fixed_csv(df: pd.DataFrame, out_path: Path) -> Path:
    """
    Write a fixed-name CSV (teacher-required). If blocked by PermissionError, fallback to timestamp.
    """
    ensure_dir(out_path.parent)
    try:
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path
    except PermissionError:
        # fallback
        return safe_write_csv(df, out_path.parent, out_path.stem)


# ========= 2) Main pipeline =========
def build_baseline_2026_fe(raw_path: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_path)

    # Required columns from scraper output
    expected = {
        "reference_code",
        "local_reference",
        "title",
        "price",
        "currency",
        "url",
        "collection",
        "objectID",
    }
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"RAW_2026 missing columns: {missing} | existing: {df.columns.tolist()}")

    df = df.copy()

    # Year
    df["year"] = 2026

    # Currency normalize
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()

    # Price EUR numeric
    df["price_eur"] = df["price"].apply(parse_eur_price_to_float)

    # URL full
    df["url_full"] = df["url"].apply(lambda x: normalize_cartier_url(x, country_path="/fr-fr"))

    # Clean strings
    df["collection"] = df["collection"].fillna("").astype(str).str.strip()
    df["title"] = df["title"].fillna("").astype(str).str.strip()
    df["url"] = df["url"].fillna("").astype(str).str.strip()
    df["url_full"] = df["url_full"].fillna("").astype(str).str.strip()

    # Reference codes normalize
    for c in ["reference_code", "local_reference", "objectID"]:
        df[c] = df[c].fillna("").astype(str).str.strip().str.upper()

    # Drop empty reference_code + dedup
    before = len(df)
    df = df[df["reference_code"] != ""].copy()
    df = df.drop_duplicates(subset=["reference_code"], keep="first")
    print(f"[QA] drop empty + dedup by reference_code: {before} -> {len(df)}")

    # Canonical collection + market
    df["collection_canonical"] = df.apply(
        lambda r: canonicalize_collection(r.get("collection", ""), r.get("title", ""), r.get("url_full", "")),
        axis=1,
    )
    df["market"] = df["url_full"].apply(extract_market_from_url)

    # Final engineered table (stable columns)
    out = df[
        [
            "year",
            "reference_code",
            "local_reference",
            "objectID",
            "title",
            "collection",
            "collection_canonical",
            "currency",
            "price",
            "price_eur",
            "market",
            "url",
            "url_full",
        ]
    ].copy()

    # QA
    print("[QA] rows:", len(out))
    print("[QA] missing price_eur ratio:", out["price_eur"].isna().mean())
    print("[QA] unique reference_code:", out["reference_code"].nunique())
    print("[QA] collections:", out["collection_canonical"].value_counts(dropna=False).head(10).to_dict())

    return out


def build_current_2026_labeled(df_2026_fe: pd.DataFrame) -> pd.DataFrame:
    """
    Teacher-required row-level labeled table (not summary):
      reference_code
      collection_canonical
      price
      currency
      market/locale
      url or title
    """
    labeled = df_2026_fe[
        [
            "year",
            "reference_code",
            "collection_canonical",
            "price",
            "price_eur",
            "currency",
            "market",
            "title",
            "url_full",
        ]
    ].copy()

    # Rename for clarity (optional): keep url_full as url for downstream users
    labeled = labeled.rename(columns={"url_full": "url"})

    # Ensure types are stable
    labeled["year"] = labeled["year"].astype(int)
    labeled["reference_code"] = labeled["reference_code"].astype(str)
    labeled["collection_canonical"] = labeled["collection_canonical"].astype(str)
    labeled["currency"] = labeled["currency"].astype(str)
    labeled["market"] = labeled["market"].astype(str)

    return labeled


def main():
    raw_path = RAW_2026_PATH
    if not raw_path.exists():
        raw_path = pick_latest_raw_2026(DATA_RAW)
        print(f"[INFO] RAW_2026_PATH not found, auto-picked latest: {raw_path}")

    # 1) Build engineered baseline
    df_2026_fe = build_baseline_2026_fe(raw_path)

    # 2) Save baseline_2026_fe (timestamped)
    ensure_dir(OUT_DIR)
    fe_path = safe_write_csv(df_2026_fe, OUT_DIR, base_name="baseline_2026_fe")
    print(f"[OK] baseline_2026_fe saved -> {fe_path} | rows={len(df_2026_fe)} cols={len(df_2026_fe.columns)}")

    # 3) Build + save current_2026_labeled (fixed name required)
    df_labeled = build_current_2026_labeled(df_2026_fe)
    labeled_path = OUT_DIR / "current_2026_labeled.csv"
    final_labeled_path = try_write_fixed_csv(df_labeled, labeled_path)

    print(f"[OK] current_2026_labeled saved -> {final_labeled_path} | rows={len(df_labeled)} cols={len(df_labeled.columns)}")
    print(df_labeled.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
