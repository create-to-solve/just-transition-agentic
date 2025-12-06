from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT / "data" / "processed"
CANONICAL_DIR = PROCESSED_DIR / "canonical"
CANONICAL_DIR.mkdir(parents=True, exist_ok=True)


RAW_PROCESSED_FILE = PROCESSED_DIR / "desnz_ghg_emissions_processed.csv"
CANONICAL_OUT_FILE = CANONICAL_DIR / "desnz_la_year.csv"


def load_desnz_processed() -> pd.DataFrame:
    """
    Load the minimally processed DESNZ GHG emissions table.

    We assume this is the Phase 1 ingestion output:
      data/processed/desnz_ghg_emissions_processed.csv
    """
    if not RAW_PROCESSED_FILE.exists():
        raise FileNotFoundError(
            f"Processed DESNZ file not found at {RAW_PROCESSED_FILE}. "
            "Run src/ingestion/desnz_ingest.py first."
        )

    print(f"[DESNZ_CANONICAL] Loading processed DESNZ from: {RAW_PROCESSED_FILE}")
    df = pd.read_csv(RAW_PROCESSED_FILE)

    # Normalise column names (strip whitespace) just to be safe
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]

    required_cols = [
        "Country",
        "Country Code",
        "Region",
        "Region Code",
        "Local Authority",
        "Local Authority Code",
        "Calendar Year",
        "Territorial emissions (kt CO2e)",
        "Emissions within the scope of influence of LAs (kt CO2)",
        "Mid-year Population (thousands)",
        "Area (km2)",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            "DESNZ processed table is missing required columns: "
            + ", ".join(missing)
        )

    print(f"[DESNZ_CANONICAL] Loaded shape: {df.shape[0]} rows x {df.shape[1]} columns")
    return df


def build_la_year_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a canonical LA–year table from DESNZ sector/gas rows.

    We aggregate:
      * Emissions within the scope of influence of LAs (kt CO2)
      * Territorial emissions (kt CO2e)

    to Local Authority Code + Local Authority + Calendar Year.

    For population and area, DESNZ repeats the same value across
    sector/gas rows, so we take the first occurrence per group.
    """
    print("[DESNZ_CANONICAL] Building LA–year canonical table...")

    group_keys = [
        "Country",
        "Country Code",
        "Region",
        "Region Code",
        "Local Authority",
        "Local Authority Code",
        "Calendar Year",
    ]

    agg_df = (
        df.groupby(group_keys, as_index=False)
        .agg(
            {
                "Emissions within the scope of influence of LAs (kt CO2)": "sum",
                "Territorial emissions (kt CO2e)": "sum",
                "Mid-year Population (thousands)": "first",
                "Area (km2)": "first",
            }
        )
    )

    # Rename to a canonical JTIS-friendly schema
    agg_df = agg_df.rename(
        columns={
            "Country": "country",
            "Country Code": "country_code",
            "Region": "region",
            "Region Code": "region_code",
            "Local Authority": "lad_name",
            "Local Authority Code": "lad_code",
            "Calendar Year": "year",
            "Emissions within the scope of influence of LAs (kt CO2)": "total_emissions_scope_ktco2",
            "Territorial emissions (kt CO2e)": "territorial_emissions_ktco2e",
            "Mid-year Population (thousands)": "mid_year_population_thousands",
            "Area (km2)": "area_km2",
        }
    )

    # Ensure sensible ordering of columns
    ordered_cols = [
        "lad_code",
        "lad_name",
        "year",
        "country",
        "country_code",
        "region",
        "region_code",
        "total_emissions_scope_ktco2",
        "territorial_emissions_ktco2e",
        "mid_year_population_thousands",
        "area_km2",
    ]

    # Keep only the columns we know we need for now
    agg_df = agg_df[ordered_cols]

    # Sort for reproducibility
    agg_df = agg_df.sort_values(["lad_code", "year"]).reset_index(drop=True)

    print(
        "[DESNZ_CANONICAL] Canonical LA–year table built. "
        f"Shape: {agg_df.shape[0]} rows x {agg_df.shape[1]} columns"
    )
    return agg_df


def write_canonical_table(df: pd.DataFrame) -> None:
    print(f"[DESNZ_CANONICAL] Writing canonical LA–year table to: {CANONICAL_OUT_FILE}")
    df.to_csv(CANONICAL_OUT_FILE, index=False)
    print("[DESNZ_CANONICAL] Write complete.")


def main() -> int:
    print("[DESNZ_CANONICAL] Phase 2 harmonisation (DESNZ LA–year) starting...")
    df = load_desnz_processed()
    canonical_df = build_la_year_canonical(df)
    write_canonical_table(canonical_df)
    print("[DESNZ_CANONICAL] Phase 2 harmonisation (DESNZ LA–year) finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
 
