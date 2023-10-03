import pandas as pd


def drop_issues(df):
    return df.drop(columns=[c for c in df.columns if "_issues" in c])


def clean_hospital_name(df):
    return df.assign(
        hospital_name=lambda df: df["hospital_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


def clean_waivers(df):
    return (
        df.pipe(drop_issues)
        .pipe(clean_hospital_name)
        .loc[
            lambda df: ~df["ccn"].isin(
                [
                    380051,  # Apparent error for Salem Medical Center
                    330195,  # Apparent error for North Shore University Hospital
                ]
            )
        ]
    )


def clean_measures(df):
    return (
        df.pipe(drop_issues)
        .pipe(clean_hospital_name)
        .replace(
            {
                "hospital_name": {
                    "Huntsman Cancer Hospital": (
                        "University of Utah Health and Huntsman Cancer Institute"
                    ),
                    "Medical City Las Clinas": "Medical City Las Colinas",
                }
            }
        )
        .loc[lambda df: df["status"] != "Duplicate"]
        .drop_duplicates(subset=["ccn", "hospital_name", "measure_from_date"])
        .drop(
            columns=[
                "issue_type",
                "priority",
                "reporter",
                "summary",
            ]
        )
    )


for kind in ["measures", "waivers"]:
    cleaner = eval(f"clean_{kind}")
    for num in [1, 2]:
        filename = f"tier-{num}-{kind}.csv"
        raw = pd.read_csv(f"data/standardized/{filename}")
        raw.pipe(cleaner).to_csv(f"data/cleaned/{filename}", index=False)
