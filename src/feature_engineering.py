# feature_engineering_2026.py
# -*- coding: utf-8 -*-

import re
import os
from datetime import datetime

import pandas as pd


# ========= 0) 路径配置（按你项目结构来） =========
RAW_2026_PATH = r"data/raw/current_2026_raw_20260211_140133.csv"  # 改成你真实文件名
OUT_DIR = r"data/raw"  # 先存 raw 目录也可以；你也可以换成 data/processed


# ========= 1) 工具函数 =========
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_eur_price_to_float(x) -> float:
    """
    输入示例：
      "5,000€" / "5 000€" / "5000 €" / 5000.0 / None
    输出：
      5000.0 或 NaN
    """
    if pd.isna(x):
        return float("nan")
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if s == "":
        return float("nan")

    # 去掉货币符号、空格、NBSP等
    s = s.replace("€", "").replace("\u00a0", " ").strip()

    # 常见格式：5,000 / 5 000 / 5.000
    # 这里优先把空格删掉，再把逗号/点当作千分位处理（针对欧元页面常见写法）
    s = s.replace(" ", "")
    # 如果同时出现逗号和点：保守处理，先移除千分位分隔符
    # 简化：把所有逗号都删掉，把所有点都删掉（因为你这里基本都是整数欧元）
    s = s.replace(",", "").replace(".", "")

    # 只保留数字
    m = re.search(r"(\d+)", s)
    if not m:
        return float("nan")
    return float(m.group(1))


def normalize_cartier_url(u: str, country_path="/fr-fr") -> str:
    """
    你的 newPdpLink/pdpLink 是相对路径时，补全为 https://www.cartier.com/fr-fr/...
    """
    if pd.isna(u):
        return ""
    s = str(u).strip()
    if s == "":
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    # 统一以 /fr-fr 开头
    if not s.startswith(country_path):
        if s.startswith("/"):
            s = country_path + s
        else:
            s = country_path + "/" + s
    return "https://www.cartier.com" + s


def safe_write_csv(df: pd.DataFrame, out_dir: str, base_name: str) -> str:
    """
    避免 PermissionError（文件正被 VSCode 打开时很常见）：
    永远写一个带时间戳的新文件名。
    """
    ensure_dir(out_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"{base_name}_{ts}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


# ========= 2) 主流程 =========
def build_baseline_2026_fe(raw_path: str) -> pd.DataFrame:
    df = pd.read_csv(raw_path)

    # 基础检查
    expected = {"reference_code", "local_reference", "title", "price", "currency", "url", "collection", "objectID"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"RAW_2026 缺列: {missing} | 现有列: {df.columns.tolist()}")

    # 规范列名（如果你想严格一致）
    df = df.copy()
    df["year"] = 2026

    # 价格数值化（欧元）
    df["price_eur"] = df["price"].apply(parse_eur_price_to_float)

    # currency 标准化
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()

    # URL 补全（你抓到的是 /fr-fr/product/CR... 这种）
    df["url_full"] = df["url"].apply(lambda x: normalize_cartier_url(x, country_path="/fr-fr"))

    # collection/title 清洗（去掉多余空白）
    df["collection"] = df["collection"].fillna("").astype(str).str.strip()
    df["title"] = df["title"].fillna("").astype(str).str.strip()

    # 参考码清洗（保证大写无空格）
    for c in ["reference_code", "local_reference", "objectID"]:
        df[c] = df[c].fillna("").astype(str).str.strip().str.upper()

    # 去重逻辑：以 reference_code 为主（你们 later join 最常用它）
    before = len(df)
    df = df[df["reference_code"] != ""].copy()
    df = df.drop_duplicates(subset=["reference_code"], keep="first")
    print(f"[QA] drop empty + dedup by reference_code: {before} -> {len(df)}")

    # 最终输出列（这一版是“ seeable & stable ”的 baseline 2026）
    out = df[
        [
            "year",
            "reference_code",
            "local_reference",
            "objectID",
            "title",
            "collection",
            "currency",
            "price",
            "price_eur",
            "url",
            "url_full",
        ]
    ].copy()

    # 质量检查
    print("[QA] rows:", len(out))
    print("[QA] missing price_eur ratio:", out["price_eur"].isna().mean())
    print("[QA] unique reference_code:", out["reference_code"].nunique())

    return out


def main():
    df_2026_fe = build_baseline_2026_fe(RAW_2026_PATH)

    # 输出：永远写一个带时间戳的新文件，避免 PermissionError
    out_path = safe_write_csv(df_2026_fe, OUT_DIR, base_name="baseline_2026_fe")
    print(f"[OK] Saved -> {out_path} | rows={len(df_2026_fe)} cols={len(df_2026_fe.columns)}")


if __name__ == "__main__":
    main()
