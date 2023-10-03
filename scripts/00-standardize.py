import re
from pathlib import Path

import pandas as pd

abbrs = (
    pd.read_csv("data/manual/column-renames.csv")
    .set_index("original")["rename"]
    .to_dict()
)


def rename_column(c: str) -> str:
    # Remove "Custom field" cruft
    if "Custom field" in c:
        c = re.sub(r"^Custom field \(|\)$", r"", c)

    # Make manual abbreviations
    c = abbrs.get(c, c)

    # Normalize measures
    c = re.sub(r"(\d[a-c]?)\..*$", r"m_\1", c)

    # Standardize everything else
    c = c.lower().replace(" ", "_")
    return c


def get_sort_order(c: str) -> tuple[float, str]:
    if re.search(r"^ccn", c):
        return (0.0, c)
    if re.search(r"^hospital_name", c):
        return (1.0, c)
    if re.search(r"^hospital_phone", c):
        return (1.9, c)
    if re.search(r"^hospital", c):
        return (1.1, c)
    if re.search(r"created|status", c):
        return (1.2, c)
    if re.search(r"_date$", c):
        return (2.0, c)
    if re.search(r"^m_|^measure|^resp_", c):
        return (2.1, c)
    return (5.0, c)


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if "_date" in c:
            df[c] = pd.to_datetime(df[c], format="%b/%d/%Y 12:00 AM")
        elif c == "created":
            df[c] = pd.to_datetime(df[c], format="%b/%d/%Y %I:%M %p")
    return df


def standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(lambda df: df.rename(columns={c: rename_column(c) for c in df.columns}))
        .pipe(lambda df: df[sorted(df.columns, key=get_sort_order)])
        .loc[lambda df: df["ccn"].notnull()]
        .pipe(normalize_dates)
    )


def main() -> None:
    src_paths = sorted(Path("data/raw/").glob("*.csv"))

    for src_path in src_paths:
        dest = re.sub(
            r"FOIA - Tier (\d) (Measure|Waiver)s?.csv",
            r"tier-\1-\2s.csv",
            src_path.name,
        ).lower()
        df = pd.read_csv(src_path, dtype=str).pipe(standardize_df)
        df.to_csv(f"data/standardized/{dest}", index=False)


if __name__ == "__main__":
    main()
