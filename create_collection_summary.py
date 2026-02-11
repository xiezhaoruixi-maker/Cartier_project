# create_collection_summary.py
# Purpose: create data/processed/collection_summary.csv for Portfolio Analysis (2022 vs 2026)

import os
os.environ["PANDAS_ARROW_DISABLED"] = "1"  # avoid pyarrow import issues

import re
import pandas as pd
import numpy as np
from pathlib import Path

# ============ PATHS (repo-relative) ============
ROOT = Path(__file__).resolve().parent  # repo root if this file is in repo root
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"

FILE_2022 = DATA_RAW / "baseline_2022_fe.csv"  # <-- note: your file name in data/raw
FILE_2026 = DATA_RAW / "baseline_2026_fe_20260211_142006.csv"  # <-- adjust if different
OUT_PATH = DATA_PROCESSED / "collection_summary.csv"

CANONICAL_COLLECTIONS = ["Tank", "Santos", "Panthère", "Ballon Bleu", "Trinity"]

def _coalesce_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def canonicalize_collection(text: str) -> str:
    t = (text or "").lower()
    patterns = [
        ("Tank", r"\btank\b"),
        ("Santos", r"\bsantos\b"),
        ("Panthère", r"panth[èe]re|panthere"),
        ("Ballon Bleu", r"ballon\s*bleu"),
        ("Trinity", r"\btrinity\b"),
    ]
    for name, pat in patterns:
        if re.search(pat, t, flags=re.IGNORECASE):
            return name
    return "Other"

def ensure_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

# ============ LOAD ============
if not FILE_2022.exists():
    raise FileNotFoundError(f"2022 file not found: {FILE_2022}")
if not FILE_2026.exists():
    raise FileNotFoundError(f"2026 file not found: {FILE_2026}")

df22 = pd.read_csv(FILE_2022)
df26 = pd.read_csv(FILE_2026)

# ============ STANDARDIZE COLUMNS ============
col22_ref = _coalesce_col(df22, ["reference_code", "ref", "id"])
col22_collection = _coalesce_col(df22, ["collection"])
col22_title = _coalesce_col(df22, ["title", "name"])
col22_price = _coalesce_col(df22, ["price", "price_local"])
col22_currency = _coalesce_col(df22, ["currency"])

# Your 2022 file may not contain 'year' or 'price_eur'
df22["year"] = 2022

col26_ref = _coalesce_col(df26, ["reference_code", "ref", "id"])
col26_collection = _coalesce_col(df26, ["collection"])
col26_title = _coalesce_col(df26, ["title", "name"])
col26_price_eur = _coalesce_col(df26, ["price_eur"])
col26_price = _coalesce_col(df26, ["price", "price_local"])
col26_currency = _coalesce_col(df26, ["currency"])
col26_year = _coalesce_col(df26, ["year"])

need22 = [col22_ref, col22_collection, col22_price, col22_currency]
need26 = [col26_ref, col26_collection, col26_currency, col26_year]
if any(x is None for x in need22):
    raise ValueError(f"2022 missing required cols; got: {list(df22.columns)}")
if any(x is None for x in need26):
    raise ValueError(f"2026 missing required cols; got: {list(df26.columns)}")

# numeric
df22[col22_price] = ensure_numeric(df22[col22_price])
if col26_price_eur:
    df26[col26_price_eur] = ensure_numeric(df26[col26_price_eur])
if col26_price:
    df26[col26_price] = ensure_numeric(df26[col26_price])

# ============ COLLECTION CANONICAL ============
# Prefer 'collection'; fallback to 'title' if collection empty
df22["_text"] = df22[col22_collection].fillna("").astype(str) + " " + (df22[col22_title].fillna("").astype(str) if col22_title else "")
df26["_text"] = df26[col26_collection].fillna("").astype(str) + " " + (df26[col26_title].fillna("").astype(str) if col26_title else "")

df22["collection_canonical"] = df22["_text"].apply(canonicalize_collection)
df26["collection_canonical"] = df26["_text"].apply(canonicalize_collection)

# ============ EUR PRICE ============
# 2022: only trust EUR prices if currency==EUR; otherwise NaN (avoid FX mixing)
df22["price_eur"] = np.where(
    df22[col22_currency].astype(str).str.upper().eq("EUR"),
    df22[col22_price],
    np.nan
)

# 2026: use price_eur if exists, else fallback when currency==EUR
if col26_price_eur:
    df26["price_eur"] = df26[col26_price_eur]
else:
    df26["price_eur"] = np.where(
        df26[col26_currency].astype(str).str.upper().eq("EUR"),
        df26[col26_price],
        np.nan
    )

df26["year"] = df26[col26_year].astype(int)

# ============ FACT ============
fact = pd.concat([
    df22[["year", "collection_canonical", col22_ref, "price_eur"]].rename(columns={col22_ref: "reference_code"}),
    df26[["year", "collection_canonical", col26_ref, "price_eur"]].rename(columns={col26_ref: "reference_code"}),
], ignore_index=True)

# Optional: keep only key collections (uncomment if teacher要求只看这些系列)
# fact = fact[fact["collection_canonical"].isin(CANONICAL_COLLECTIONS)]

# ============ AGG ============
summary = (
    fact.groupby(["year", "collection_canonical"], as_index=False)
    .agg(
        n_total=("reference_code", "nunique"),
        n_price_eur=("price_eur", lambda s: int(np.isfinite(s).sum())),
        avg_price_eur=("price_eur", "mean"),
        median_price_eur=("price_eur", "median"),
    )
)

summary["share_in_year"] = summary["n_total"] / summary.groupby("year")["n_total"].transform("sum")
summary[["avg_price_eur", "median_price_eur"]] = summary[["avg_price_eur", "median_price_eur"]].round(2)
summary["share_in_year"] = summary["share_in_year"].round(6)
summary = summary.sort_values(["year", "n_total"], ascending=[True, False])

# ============ SAVE ============
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
summary.to_csv(OUT_PATH, index=False, encoding="utf-8")

print(f"[OK] Saved: {OUT_PATH}")
print(summary.head(30).to_string(index=False))
