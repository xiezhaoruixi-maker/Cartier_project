import re
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

GOLD_PAT = re.compile(r"\b(pink gold|rose gold|yellow gold|white gold|gold)\b", re.I)
STEEL_PAT = re.compile(r"\bsteel\b", re.I)

SMALL_PAT = re.compile(r"\b(small|mini|sm)\b", re.I)
LARGE_PAT = re.compile(r"\b(large|xl)\b", re.I)

def normalize_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()

def extract_material(text: str) -> str:
    t = normalize_text(text).lower()
    if not t:
        return "Unknown"
    if GOLD_PAT.search(t):
        return "Gold"
    # teacher rule: "if not contain Gold and contain Steel -> Steel"
    if STEEL_PAT.search(t) and not GOLD_PAT.search(t):
        return "Steel"
    return "Other"

def extract_size(text: str) -> str:
    t = normalize_text(text).lower()
    if not t:
        return "Unknown"
    if SMALL_PAT.search(t):
        return "Small"
    if LARGE_PAT.search(t):
        return "Large"
    return "Medium"

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expect df contains at least one of: description, title, url
    We create a 'text_for_features' by concatenating available fields.
    """
    for col in ["description", "title", "url"]:
        if col not in df.columns:
            df[col] = ""

    df["text_for_features"] = (
        df["description"].astype(str).fillna("") + " " +
        df["title"].astype(str).fillna("") + " " +
        df["url"].astype(str).fillna("")
    ).str.strip()

    df["material"] = df["text_for_features"].apply(extract_material)
    df["size"] = df["text_for_features"].apply(extract_size)
    return df

def summarize(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Create a summary table for Power BI:
    - material share
    - size share
    """
    total = len(df)
    mat = df["material"].value_counts(dropna=False).rename_axis("material").reset_index(name="n")
    mat["share"] = mat["n"] / total
    mat["year"] = year
    mat["metric"] = "material"

    siz = df["size"].value_counts(dropna=False).rename_axis("size").reset_index(name="n")
    siz["share"] = siz["n"] / total
    siz["year"] = year
    siz["metric"] = "size"
    siz = siz.rename(columns={"size": "label"})

    mat = mat.rename(columns={"material": "label"})
    return pd.concat([mat, siz], ignore_index=True)

def load_csv_safely(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing file: {path}\n"
            f"Put your raw CSV into: {RAW_DIR}\n"
            f"Expected names: baseline_2022.csv and current_2026_raw.csv"
        )
    return pd.read_csv(path)

def main():
    # Expected input file names (from team B and C)
    path_2022 = RAW_DIR / "baseline_2022.csv"
    path_2026 = RAW_DIR / "current_2026_raw.csv"

    df_2022 = load_csv_safely(path_2022)
    df_2026 = load_csv_safely(path_2026)

    df_2022["year"] = 2022
    df_2026["year"] = 2026

    df_2022_feat = build_features(df_2022.copy())
    df_2026_feat = build_features(df_2026.copy())

    out_2022 = OUT_DIR / "catalogue_features_2022.csv"
    out_2026 = OUT_DIR / "catalogue_features_2026.csv"
    df_2022_feat.to_csv(out_2022, index=False)
    df_2026_feat.to_csv(out_2026, index=False)

    sum_2022 = summarize(df_2022_feat, 2022)
    sum_2026 = summarize(df_2026_feat, 2026)
    summary = pd.concat([sum_2022, sum_2026], ignore_index=True)

    out_summary = OUT_DIR / "catalogue_summary_2022_vs_2026.csv"
    summary.to_csv(out_summary, index=False)

    print("Done.")
    print(f"- {out_2022}")
    print(f"- {out_2026}")
    print(f"- {out_summary}")

if __name__ == "__main__":
    main()
