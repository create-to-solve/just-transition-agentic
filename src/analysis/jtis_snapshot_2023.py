from __future__ import annotations

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCORED = ROOT / "data" / "processed" / "canonical" / "jtis_scored_la_year.csv"
OUT = ROOT / "outputs" / "jtis_2023_ranked.csv"


def main():
    print("[JTIS_2023] Loading scored LA-year dataset...")
    df = pd.read_csv(SCORED)

    # Filter to 2023
    df2023 = df[df["year"] == 2023].copy()
    print(f"[JTIS_2023] LADs in 2023: {df2023['lad_code'].nunique()}")

    # Rank LADs by JTI score (descending = more transition pressure)
    df2023 = df2023.sort_values("jti_score", ascending=False).reset_index(drop=True)
    df2023["rank"] = df2023.index + 1

    # Select clean output columns
    cols = [
        "rank",
        "lad_code",
        "lad_name",
        "region",
        "jti_score",
        "emissions_score",
        "transport_score",
        "structural_score",
        "emissions_pc_tco2",
        "fuel_pc_ktoe_per_1000",
        "freight_share",
        "bioenergy_share",
        "population",
        "area_km2",
    ]
    existing_cols = [c for c in cols if c in df2023.columns]

    df2023_out = df2023[existing_cols]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df2023_out.to_csv(OUT, index=False)

    print(f"[JTIS_2023] Snapshot written to: {OUT}")
    print("[JTIS_2023] Top 5 LADs:")
    print(df2023_out.head())
    print("[JTIS_2023] Bottom 5 LADs:")
    print(df2023_out.tail())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
