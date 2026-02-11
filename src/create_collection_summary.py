import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def pick_latest(dir_: Path, pattern: str) -> Path:
    files = sorted(dir_.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern} under {dir_.resolve()}")
    return files[-1]


def ensure_cols(df: pd.DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing columns: {missing} | got={df.columns.tolist()}")


def summarize_by_collection(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Expects at least:
      collection_canonical, price_eur
    """
    df = df.copy()
    df["year"] = year

    df["price_eur"] = pd.to_numeric(df["price_eur"], errors="coerce")
    df_valid = df[df["price_eur"].notna()].copy()

    if len(df_valid) == 0:
        raise ValueError(f"No valid price_eur rows for year={year}")

    total = df.groupby("collection_canonical").size().rename("n_total").reset_index()
    eur_n = df_valid.groupby("collection_canonical").size().rename("n_price_eur").reset_index()

    stats = (
        df_valid.groupby("collection_canonical")["price_eur"]
        .agg(avg_price_eur="mean", median_price_eur="median")
        .reset_index()
    )

    out = total.merge(eur_n, on="collection_canonical", how="left").merge(stats, on="collection_canonical", how="left")
    out["year"] = year

    out["share_in_year"] = out["n_total"] / out["n_total"].sum()

    out = out[["year", "collection_canonical", "n_total", "n_price_eur", "avg_price_eur", "median_price_eur", "share_in_year"]]
    out = out.sort_values(["year", "n_total"], ascending=[True, False])
    return out


def main():
    parser = argparse.ArgumentParser(description="Create collection_summary.csv (Docker-ready).")
    parser.add_argument("--processed", type=str, default="data/processed", help="Processed dir (default: data/processed)")
    parser.add_argument("--in-2022", type=str, default="", help="Optional: exact 2022 labeled file under processed")
    parser.add_argument("--in-2026", type=str, default="", help="Optional: exact 2026 labeled file under processed")
    parser.add_argument("--out", type=str, default="", help="Optional output path; default data/processed/collection_summary.csv")
    args = parser.parse_args()

    processed = Path(args.processed)
    processed.mkdir(parents=True, exist_ok=True)

    f2022 = processed / args.in_2022 if args.in_2022 else pick_latest(processed, "baseline_2022_labeled*.csv")
    f2026 = processed / args.in_2026 if args.in_2026 else pick_latest(processed, "current_2026_labeled*.csv")

    df22 = pd.read_csv(f2022)
    df26 = pd.read_csv(f2026)

    ensure_cols(df22, ["collection_canonical", "price_eur"], "2022 labeled")
    ensure_cols(df26, ["collection_canonical", "price_eur"], "2026 labeled")

    s22 = summarize_by_collection(df22, 2022)
    s26 = summarize_by_collection(df26, 2026)
    summary = pd.concat([s22, s26], ignore_index=True)

    out_path = Path(args.out) if args.out else (processed / "collection_summary.csv")

    try:
        summary.to_csv(out_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        out_path = processed / f"collection_summary_{ts()}.csv"
        summary.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[OK] Saved: {out_path}")
    print(summary.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
