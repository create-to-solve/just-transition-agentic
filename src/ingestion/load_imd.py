import pandas as pd

def load_imd(path):
    """
    Load IMD2019 LSOA-level data and aggregate to LAD-level mean IMD rank.
    """

    df = pd.read_excel(path, sheet_name="IMD2019")

    df = df.rename(columns={
        "Local Authority District code (2019)": "lad_code",
        "Local Authority District name (2019)": "lad_name",
        "Index of Multiple Deprivation (IMD) Rank": "imd_rank",
    })

    # Keep only needed columns
    df = df[["lad_code", "lad_name", "imd_rank"]]

    # Aggregate: LAD-level mean rank
    df_out = (
        df.groupby(["lad_code", "lad_name"], as_index=False)["imd_rank"]
        .mean()
        .rename(columns={"imd_rank": "imd_mean_rank"})
    )

    return df_out

