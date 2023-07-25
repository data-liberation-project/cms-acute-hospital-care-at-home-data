import re

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
    if re.search(r"_date$", c):
        return (2.0, c)
    if re.search(r"^m_|^measure|^resp_", c):
        return (2.1, c)
    if re.search(r"^poc_", c):
        return (9.0, c)
    if re.search(r"_issues", c):
        return (10.0, c)
    return (5.0, c)


def strlist_nonnulls(series: pd.Series) -> str:
    return " • ".join(sorted(series.dropna()))


def drop_cols(df: pd.DataFrame, pat: re.Pattern[str]) -> pd.DataFrame:
    return df[[c for c in df.columns if not re.search(pat, c)]]


def cols_to_strlist(df: pd.DataFrame, pat: re.Pattern[str]) -> pd.Series:
    cols = [c for c in df.columns if re.search(pat, c)]
    if len(cols):
        return df[cols].apply(strlist_nonnulls, axis=1)
    else:
        return pd.Series(None)


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if "_date" in c:
            df[c] = pd.to_datetime(df[c], format="%b/%d/%Y 12:00 AM")
    return df


def standard_df(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(
            outward_issues=lambda df: df.pipe(cols_to_strlist, r"Outward issue"),
            inward_issues=lambda df: df.pipe(cols_to_strlist, r"Inward issue"),
        )
        .pipe(drop_cols, r"Outward issue")
        .pipe(drop_cols, r"Inward issue")
        .pipe(drop_cols, r"^Unnamed")
        .pipe(lambda df: df.rename(columns={c: rename_column(c) for c in df.columns}))
        .pipe(lambda df: df[sorted(df.columns, key=get_sort_order)])
        .loc[lambda df: ~df["summary"].str.contains(r"^Testing", na=False)]
        .loc[lambda df: df["ccn"].notnull()]
        .pipe(normalize_dates)
    )


def read_and_standardize(path: str) -> pd.DataFrame:
    if "Tier 1 Measures" in path:
        return pd.concat(
            [
                pd.read_csv(path, skipfooter=263, engine="python", dtype=str).pipe(
                    standard_df
                ),
                pd.read_csv(path, header=1001, dtype=str).pipe(standard_df),
            ]
        )
    else:
        return pd.read_csv(path, dtype=str).pipe(standard_df)


def main() -> None:
    src_dests = [
        (
            "FOIA - Tier 1 Waiver (QualityNet JIRA) 2023-04-19T13_50_58-0400.csv",
            "tier-1-waivers.csv",
        ),
        (
            "FOIA - Tier 2 Waiver (QualityNet JIRA) 2023-04-19.csv",
            "tier-2-waivers.csv",
        ),
        (
            "FOIA - Tier 1 Measures (QualityNet JIRA) 2023-04-19.csv",
            "tier-1-measures.csv",
        ),
        (
            "FOIA - Tier 2 Measures (QualityNet JIRA) 2023-04-19.csv",
            "tier-2-measures.csv",
        ),
    ]

    for src, dest in src_dests:
        df = read_and_standardize(f"data/redacted/{src}")
        df.to_csv(f"data/standardized/{dest}", index=False)


if __name__ == "__main__":
    main()
