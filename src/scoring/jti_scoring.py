from __future__ import annotations

from pathlib import Path
import json

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
CANONICAL_DIR = ROOT / "data" / "processed" / "canonical"

BASE_FILE = CANONICAL_DIR / "jtis_base_la_year.csv"
OUT_FILE = CANONICAL_DIR / "jtis_scored_la_year.csv"
DIAG_FILE = ROOT / "outputs" / "diagnostics" / "scoring_report.json"


def min_max_normalise(series: pd.Series) -> pd.Series:
    """0–1 min-max normalisation; returns 0.5 if constant."""
    s = series.astype(float)
    s_min = s.min(skipna=True)
    s_max = s.max(skipna=True)
    if pd.isna(s_min) or pd.isna(s_max) or s_max == s_min:
        return pd.Series(0.5, index=s.index)
    return (s - s_min) / (s_max - s_min)


def compute_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-capita, ratios, densities, and YoY changes.
    Assumes df has:
      - total_emissions_scope_ktco2
      - total_fuel_ktoe
      - personal_transport_ktoe
      - freight_transport_ktoe
      - bioenergy_ktoe
      - area_km2
      - population (ONS)
    """
    df = df.copy()

    # Ensure numeric
    numeric_cols = [
        "total_emissions_scope_ktco2",
        "territorial_emissions_ktco2e",
        "total_fuel_ktoe",
        "personal_transport_ktoe",
        "freight_transport_ktoe",
        "bioenergy_ktoe",
        "area_km2",
        "population",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ---- Per-capita metrics ----
    pop = df["population"].replace({0: np.nan})

    df["emissions_pc_tco2"] = df["total_emissions_scope_ktco2"] * 1000.0 / pop
    df["fuel_pc_ktoe_per_1000"] = df["total_fuel_ktoe"] * 1000.0 / pop
    df["personal_pc_ktoe_per_1000"] = df["personal_transport_ktoe"] * 1000.0 / pop
    df["freight_pc_ktoe_per_1000"] = df["freight_transport_ktoe"] * 1000.0 / pop

    # ---- Transport mix ratios ----
    fuel = df["total_fuel_ktoe"].replace({0: np.nan})
    df["freight_share"] = df["freight_transport_ktoe"] / fuel
    df["personal_share"] = df["personal_transport_ktoe"] / fuel
    df["bioenergy_share"] = df["bioenergy_ktoe"] / fuel

    # ---- Spatial intensity ----
    area = df["area_km2"].replace({0: np.nan})
    df["emissions_density_tco2_per_km2"] = (
        df["total_emissions_scope_ktco2"] * 1000.0 / area
    )

    # ---- Year-on-year changes per LAD ----
    df = df.sort_values(["lad_code", "year"]).reset_index(drop=True)

    df["emissions_yoy_pct"] = (
        df.groupby("lad_code")["total_emissions_scope_ktco2"].pct_change()
    )
    df["fuel_yoy_pct"] = (
        df.groupby("lad_code")["total_fuel_ktoe"].pct_change()
    )
    df["population_yoy_pct"] = (
        df.groupby("lad_code")["population"].pct_change()
    )

    return df


def compute_scores(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Compute normalised metrics and composite JTIS scores.
    Returns updated df and a diagnostics dict.
    """
    df = df.copy()

    # ---- Normalised emissions metrics ----
    df["norm_emissions_pc"] = min_max_normalise(df["emissions_pc_tco2"])
    df["norm_emissions_density"] = min_max_normalise(
        df["emissions_density_tco2_per_km2"]
    )
    df["norm_emissions_yoy"] = min_max_normalise(df["emissions_yoy_pct"])

    # ---- Normalised transport metrics ----
    df["norm_fuel_pc"] = min_max_normalise(df["fuel_pc_ktoe_per_1000"])
    df["norm_freight_share"] = min_max_normalise(df["freight_share"])
    df["norm_bioenergy_share"] = min_max_normalise(df["bioenergy_share"])

    # ---- Normalised structural metric ----
    df["population_yoy_abs"] = df["population_yoy_pct"].abs()
    df["norm_population_yoy_abs"] = min_max_normalise(df["population_yoy_abs"])

    # ---- Component scores ----
    df["emissions_score"] = (
        df["norm_emissions_pc"]
        + df["norm_emissions_density"]
        + df["norm_emissions_yoy"]
    ) / 3.0

    df["transport_score"] = (
        df["norm_fuel_pc"]
        + df["norm_freight_share"]
        + (1.0 - df["norm_bioenergy_share"])
    ) / 3.0

    df["structural_score"] = df["norm_population_yoy_abs"]

    # ---- Composite JTI score ----
    df["jti_score"] = (
        0.5 * df["emissions_score"]
        + 0.4 * df["transport_score"]
        + 0.1 * df["structural_score"]
    )

    diagnostics = {
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "years": {
            "min": int(df["year"].min()),
            "max": int(df["year"].max()),
        },
        "lads": int(df["lad_code"].nunique()),
    }

    return df, diagnostics


def main() -> int:
    if not BASE_FILE.exists():
        raise FileNotFoundError(
            f"Base JTIS table not found: {BASE_FILE}. "
            "Run src/agents/composer_agent.py first."
        )

    print(f"[JTI_SCORING] Loading base table from: {BASE_FILE}")
    df = pd.read_csv(BASE_FILE)

    print("[JTI_SCORING] Computing derived metrics...")
    df = compute_derived_metrics(df)

    print("[JTI_SCORING] Computing scores...")
    scored_df, diagnostics = compute_scores(df)

    print(f"[JTI_SCORING] Writing scored table to: {OUT_FILE}")
    scored_df.to_csv(OUT_FILE, index=False)

    DIAG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DIAG_FILE, "w") as f:
        json.dump(diagnostics, f, indent=2)

    print("[JTI_SCORING] Done.")
    print("[JTI_SCORING] Diagnostics:", diagnostics)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
 
