from __future__ import annotations
import pandas as pd
from pathlib import Path


def main():
    ROOT = Path(__file__).resolve().parents[2]
    RAW = ROOT / "data" / "raw" / "imd_2019.xlsx"
    OUT = ROOT / "data" / "processed" / "canonical" / "imd_la.csv"

    print("[IMD_CANONICAL] Phase 2 harmonisation (IMD 2019 England LSOA → LAD) starting...")
    print(f"[IMD_CANONICAL] Loading raw IMD from: {RAW}")

    # Load IMD LSOA-level
    df = pd.read_excel(RAW, sheet_name="IMD2019")

    # Required columns
    lad_code_col = "Local Authority District code (2019)"
    lad_name_col = "Local Authority District name (2019)"
    imd_rank_col = "Index of Multiple Deprivation (IMD) Rank"

    for col in [lad_code_col, lad_name_col, imd_rank_col]:
        if col not in df.columns:
            raise ValueError(f"[IMD_CANONICAL] Missing IMD column: {col}")

    # England-only (LSOA codes beginning with E)
    df = df[df["LSOA code (2011)"].str.startswith("E")].copy()

    # Group to LAD level (mean IMD rank across LSOAs)
    imd = (
        df.groupby([lad_code_col, lad_name_col])[imd_rank_col]
        .mean()
        .reset_index()
        .rename(columns={
            lad_code_col: "lad_code",
            lad_name_col: "lad_name",
            imd_rank_col: "imd_rank_avg"
        })
    )

    print(f"[IMD_CANONICAL] LAD-level IMD shape: {imd.shape}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    imd.to_csv(OUT, index=False)

    print(f"[IMD_CANONICAL] Wrote canonical IMD table → {OUT}")
    print("[IMD_CANONICAL] Done.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
