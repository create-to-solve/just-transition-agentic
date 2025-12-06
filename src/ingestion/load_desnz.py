import pandas as pd
from pathlib import Path

def load_desnz_ghg(path: str | Path) -> pd.DataFrame:
    """
    Load DESNZ LA greenhouse gas emissions (raw CSV) and return a clean,
    aggregated LAD-year table.

    Returns columns:
        lad_code
        lad_name
        year
        total_emissions_ktco2e
        population

    Assumptions:
    - Raw file contains multiple gases per LAD-year.
    - Population is in thousands and must be multiplied by 1000.
    """

    df = pd.read_csv(path)

    # Standardise column names for convenience
    df = df.rename(
        columns={
            "Local Authority Code": "lad_code",
            "Local Authority": "lad_name",
            "Calendar Year": "year",
            "Territorial emissions (kt CO2e)": "emissions_ktco2e",
            "Mid-year Population (thousands)": "population_k",
        }
    )

    # Convert population from thousands to actual count
    df["population"] = df["population_k"] * 1000

    # Aggregate: sum across all gases for each LAD-year
    grouped = (
        df.groupby(["lad_code", "lad_name", "year"], as_index=False)
        .agg(total_emissions_ktco2e=("emissions_ktco2e", "sum"),
             population=("population", "mean"))  # mean because same value repeats
    )

    return grouped
