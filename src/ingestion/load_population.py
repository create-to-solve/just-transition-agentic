import pandas as pd

def load_population(path):
    """
    Load ONS MYEB1 population dataset and aggregate to LAD-year totals.

    Steps:
    - Load sheet MYEB1 with skiprows=1
    - Columns population_2011..population_2024 -> wide years
    - Melt to long format
    - Strip year prefix
    - Group by LAD/year (sum across age, sex)
    """

    df = pd.read_excel(path, sheet_name="MYEB1", skiprows=1)

    # Identify population year columns
    year_cols = [c for c in df.columns if c.startswith("population_")]

    # Melt wide → long
    df_long = df.melt(
        id_vars=["ladcode23", "laname23", "sex", "age"],
        value_vars=year_cols,
        var_name="year",
        value_name="population"
    )

    # Extract numeric year
    df_long["year"] = df_long["year"].str.replace("population_", "").astype(int)

    # Aggregate to LAD-year totals
    df_out = (
        df_long
        .groupby(["ladcode23", "laname23", "year"], as_index=False)["population"]
        .sum()
    )

    # Harmonise names
    df_out = df_out.rename(
        columns={
            "ladcode23": "lad_code",
            "laname23": "lad_name"
        }
    )

    return df_out

