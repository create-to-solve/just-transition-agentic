from __future__ import annotations

import pandas as pd


def harmonise_all(
    df_emissions: pd.DataFrame,
    df_fuel: pd.DataFrame,
    df_population: pd.DataFrame,
    df_imd: pd.DataFrame,
    year_min: int = 2011,
    year_max: int = 2023,
) -> pd.DataFrame:
    """
    Harmonise four ingested datasets into a single LAD-year table.

    Inputs (expected columns)
    -------------------------
    df_emissions:
        lad_code, lad_name, year, total_emissions_ktco2e, population (DESNZ)
    df_fuel:
        lad_code, year, fuel_ktoe
    df_population:
        lad_code, lad_name, year, population (ONS MYEB1)
    df_imd:
        lad_code, lad_name, imd_mean_rank

    Output
    ------
    DataFrame with columns:
        lad_code, lad_name, year,
        total_emissions_ktco2e,
        population,
        fuel_ktoe,
        imd_mean_rank
    """

    # --- 1. Restrict to the common year window ---
    emissions = df_emissions[
        (df_emissions["year"] >= year_min) & (df_emissions["year"] <= year_max)
    ].copy()

    fuel = df_fuel[
        (df_fuel["year"] >= year_min) & (df_fuel["year"] <= year_max)
    ].copy()

    population = df_population[
        (df_population["year"] >= year_min) & (df_population["year"] <= year_max)
    ].copy()

    # --- 2. Base table from emissions ---
    base = emissions[["lad_code", "lad_name", "year", "total_emissions_ktco2e"]].copy()

    # --- 3. Join population (ONS replaces DESNZ population) ---
    pop_cols = ["lad_code", "year", "population"]
    base = base.merge(
        population[pop_cols],
        on=["lad_code", "year"],
        how="inner",
        suffixes=("", "_pop"),
    )

    # --- 4. Join fuel (DfT ktoe) ---
    fuel_cols = ["lad_code", "year", "fuel_ktoe"]
    base = base.merge(
        fuel[fuel_cols],
        on=["lad_code", "year"],
        how="inner",
    )

    # --- 5. Join IMD (static per LAD) ---
    imd_cols = ["lad_code", "imd_mean_rank"]
    base = base.merge(
        df_imd[imd_cols],
        on="lad_code",
        how="left",
    )

    # Optionally sort for readability
    base = base.sort_values(["lad_code", "year"]).reset_index(drop=True)

    return base
