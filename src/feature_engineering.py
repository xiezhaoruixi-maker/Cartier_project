import pandas as pd

def extract_material(text):
    if pd.isna(text):
        return "Unknown"

    t = str(text).lower()

    if "gold" in t:
        return "Gold"
    if "steel" in t:
        return "Steel"

    return "Other"


def extract_size(text):
    if pd.isna(text):
        return "Unknown"

    t = str(text).lower()

    if "small" in t or "mini" in t:
        return "Small"
    if "large" in t or "xl" in t:
        return "Large"

    return "Medium"


def main():
    print("Feature engineering pipeline ready")


if __name__ == "__main__":
    main()

