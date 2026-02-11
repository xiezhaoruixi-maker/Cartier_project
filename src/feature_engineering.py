import re
import argparse
from pathlib import Path
import pandas as pd

# =========================
# Paths
# =========================
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_INPUT = RAW_DIR / "baseline_2022_fe.csv"
DEFAULT_OUTPUT = OUT_DIR / "baseline_2022_labeled.csv"

# =========================
# Regex patterns (teacher rules)
# =========================
# Material:
# - If contains Pink/Yellow/Rose/White Gold OR Gold -> Gold
# - If NOT contain Gold AND contains Steel -> Steel
# - Else -> Other
GOLD_PAT = re.compile(r"\b(pink gold|rose gold|yellow gold|white gold|gold)\b", re.I)
STEEL_PAT = re.compile(r"\bsteel\b", re.I)

# Size:
# - Small/Mini/SM -> Small
# - Large/XL -> Large
# - Else -> Medium (needed for the Size Matrix insight)
SMALL_PAT = re.compile(r"\b(small|mini|sm)\b", re.I)
LARGE_PAT = re.compile(r"\b(large|xl)\b", re.I)

# =========================
# Helpers
# =========================
TEXT_COL_CANDIDATES = [
    "Description", "description",
    "Title", "title",
    "Name", "name",
    "ProductName", "product_name",
    "url", "URL", "link", "Link"
]

def normalize_text(x) -> str:
    if x is None:
        return ""
    # Handle NaN
    if isinstance(x, float) and pd.isna(x):
        return ""
    if pd.isna(x):
        return ""
    return str(x).strip()

def pick_text_column(df: pd.DataFrame) -> str:
    # Prefer richer text first (description/title/name), then fall back to url
    for c in TEXT_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise ValueError(
        "No usable text column found. "
        f"Looked for: {TEXT_COL_CANDIDATES}. "
        f"Available columns: {list(df.columns)}"
    )

def extract_material(text: str) -> str:
    t = normalize_text(text)
    if not t:
        return "Other"
    if GOLD_PAT.search(t):
        return "Gold"
    if STEEL_PAT.search(t) and not GOLD_PAT.search(t):
        return "Steel"
    return "Other"

def extract_size(text: str) -> str:
    t = normalize_text(text)
    if not t:
        return "Medium"
    if SMALL_PAT.search(t):
        return "Small"
    if LARGE_PAT.search(t):
        return "Large"
    return "Medium"

def ensure_reference_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional: if reference_code missing but url contains 'CRWT100015' patterns,
    try to parse it. If reference_code exists, do nothing.
    """
    if "reference_code" in df.columns:
        return df

    if "Reference Code" in df.columns:
        df = df.rename(columns={"Reference Code": "reference_code"})
        return df

    # Try parse from url
    url_col = None
    for c in ["url", "URL", "link", "Link"]:
        if c in df.columns:
            url_col = c
            break

    if url_col is None:
        return df

    # Typical Cartier ref patterns: CRWT100015, WSTA..., etc.
    # We’ll capture sequences of letters+digits >= 6
    pat = re.compile(r"\b([A-Z]{2,6}\d{4,10})\b")
    df["reference_code"] = df[url_col].astype(str).str.extract(pat, expand=False)
    return df

def main(input_path: Path, output_path: Path) -> None:
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}\n"
            f"Put your baseline CSV into: {RAW_DIR}\n"
            f"Recommended name: baseline_2022_fe.csv"
        )

    df = pd.read_csv(input_path)
    df = ensure_reference_code(df)

    text_col = pick_text_column(df)

    # Create labels
    df["material_label"] = df[text_col].apply(extract_material)
    df["size_label"] = df[text_col].apply(extract_size)

    # Output labeled file
    df.to_csv(output_path, index=False)

    # Summaries for Power BI
    mat_summary = df.groupby("material_label").size().reset_index(name="n")
    mat_summary["share"] = mat_summary["n"] / mat_summary["n"].sum()
    mat_summary = mat_summary.sort_values("n", ascending=False)
    mat_summary.to_csv(OUT_DIR / "material_summary_2022.csv", index=False)

    size_summary = df.groupby("size_label").size().reset_index(name="n")
    size_summary["share"] = size_summary["n"] / size_summary["n"].sum()
    size_summary = size_summary.sort_values("n", ascending=False)
    size_summary.to_csv(OUT_DIR / "size_summary_2022.csv", index=False)

    # Print quick checks
    print("✅ Done.")
    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print(f"Text column used: {text_col}")
    print("\nMaterial distribution:")
    print(mat_summary.to_string(index=False))
    print("\nSize distribution:")
    print(size_summary.to_string(index=False))
    print(f"\nSaved summaries to:\n- {OUT_DIR / 'material_summary_2022.csv'}\n- {OUT_DIR / 'size_summary_2022.csv'}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cartier catalogue feature engineering (material & size labels).")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT), help="Path to input baseline CSV (default: data/raw/baseline_2022_fe.csv)")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT), help="Path to output labeled CSV (default: data/processed/baseline_2022_labeled.csv)")
    args = parser.parse_args()

    main(Path(args.input), Path(args.output))
