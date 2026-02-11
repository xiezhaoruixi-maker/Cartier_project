import re
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_price_to_float(x) -> float:
    """
    Robust parsing for prices like:
      "5,000€" / "5 000 €" / "5000" / 5000.0 / None
    Returns float or NaN.
    """
    if pd.isna(x):
        return float("nan")
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if not s:
        return float("nan")

    s = s.replace("\u00a0", " ").replace("€", "").strip()
    s = s.replace(" ", "")
    s = s.replace(",", "").replace(".", "")

    m = re.search(r"(\d+)", s)
    return float(m.group(1)) if m else float("nan")


def normalize_cartier_url(u: str, country_path="/fr-fr") -> str:
    if pd.isna(u):
        return ""
    s = str(u).strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if not s.startswith(country_path):
        if s.startswith("/"):
            s = country_path + s
        else:
            s = country_path + "/" + s
    return "https://www.cartier.com" + s


def infer_market_from_url(url_full: str) -> str:
    """
    Example: https://www.cartier.com/fr-fr/... -> fr-fr
    """
    if not url_full:
        return ""
    m = re.search(r"cartier\.com/([a-z]{2}-[a-z]{2})/", url_full)
    return m.group(1) if m else ""


def canonicalize_collection_from_text(text: str) -> str:
    """
    Role A requirement: use string match (str.contains) in title/url to infer collection.
    Extend keywords as needed.
    """
    t = (text or "").lower()
    rules = [
        ("Tank", [" tank " , "tank"]),
        ("Santos", ["santos"]),
        ("Panthère", ["panth", "panthere", "panthère"]),
        ("Ballon Bleu", ["ballon bleu", "ballon-bleu", "ballonbleu"]),
        ("Trinity", ["trinity"]),
    ]
    for name, kws in rules:
        if any(k in t for k in kws):
            return name
    return "Other"

def build_baseline_2026_fe(raw_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_csv)

    expected = {"reference_code", "local_reference", "title", "price", "currency", "url"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"RAW_2026 missing columns: {missing} | got={df.columns.tolist()}")

    df = df.copy()
    df["year"] = 2026

    df["currency"] = df["currency"].astype(str).str.strip().str.upper()
    df["price_eur"] = df["price"].apply(parse_price_to_float)

    df["title"] = df["title"].fillna("").astype(str).str.strip()
    df["url"] = df["url"].fillna("").astype(str).str.strip()
    df["url_full"] = df["url"].apply(lambda x: normalize_cartier_url(x, country_path="/fr-fr"))
    df["market"] = df["url_full"].apply(infer_market_from_url)

    for c in ["reference_code", "local_reference"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip().str.upper()

    before = len(df)
    df = df[df["reference_code"] != ""].copy()
    df = df.drop_duplicates(subset=["reference_code"], keep="first")
    print(f"[QA] 2026 dedup by reference_code: {before} -> {len(df)}")

    out = df[
        [
            "year",
            "reference_code",
            "local_reference",
            "title",
            "currency",
            "price",
            "price_eur",
            "url",
            "url_full",
            "market",
        ]
    ].copy()

    print("[QA] 2026 rows:", len(out))
    print("[QA] 2026 missing price_eur ratio:", out["price_eur"].isna().mean())
    print("[QA] 2026 unique reference_code:", out["reference_code"].nunique())
    return out


def build_current_2026_labeled(df_2026_fe: pd.DataFrame) -> pd.DataFrame:
    """
    Teacher-required row-level labeled table (NOT summary):
      - reference_code
      - collection_canonical
      - price (original)
      - currency
      - market/locale
      - url or title
    """
    df = df_2026_fe.copy()
    df["collection_canonical"] = (df["title"].fillna("") + " " + df["url_full"].fillna("")).apply(
        canonicalize_collection_from_text
    )

    labeled = df[
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

    return labeled

def main():
    parser = argparse.ArgumentParser(description="Cartier 2026 feature engineering (Docker-ready).")
    parser.add_argument("--raw", type=str, default="data/raw", help="Raw data directory (default: data/raw)")
    parser.add_argument("--processed", type=str, default="data/processed", help="Processed output dir (default: data/processed)")
    parser.add_argument("--raw-2026-file", type=str, default="", help="Optional exact raw 2026 csv filename under --raw")
    args = parser.parse_args()

    raw_dir = Path(args.raw)
    processed_dir = Path(args.processed)
    processed_dir.mkdir(parents=True, exist_ok=True)

    if args.raw_2026_file:
        raw_2026 = raw_dir / args.raw_2026_file
    else:
        candidates = sorted(raw_dir.glob("current_2026_raw*.csv"))
        if not candidates:
            raise FileNotFoundError(f"No current_2026_raw*.csv found under {raw_dir.resolve()}")
        raw_2026 = candidates[-1]

    df_2026_fe = build_baseline_2026_fe(raw_2026)
    df_2026_lab = build_current_2026_labeled(df_2026_fe)

    fe_path = processed_dir / f"baseline_2026_fe_{ts()}.csv"
    lab_path = processed_dir / f"current_2026_labeled_{ts()}.csv"

    df_2026_fe.to_csv(fe_path, index=False, encoding="utf-8-sig")
    df_2026_lab.to_csv(lab_path, index=False, encoding="utf-8-sig")

    print(f"[OK] Saved: {fe_path}")
    print(f"[OK] Saved: {lab_path}")
    print(df_2026_lab.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
