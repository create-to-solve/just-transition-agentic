from __future__ import annotations

from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT / "data" / "processed"
RAW_DIR = ROOT / "data" / "raw"
CANONICAL_DIR = PROCESSED_DIR / "canonical"
CANONICAL_DIR.mkdir(parents=True, exist_ok=True)

RAW_FILE = RAW_DIR / "ons_population.xlsx"
CANONICAL_OUT_FILE = CANONICAL_DIR / "ons_la_year.csv"


def load_ons_raw() -> pd.DataFrame:
    """
    Load the raw ONS MYE sheet MYEB1 directly.
    """
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"ONS population file not found: {RAW_FILE}")

    print(f"[ONS_CANONICAL] Loading ONS MYE from: {RAW_FILE}")

    df = pd.read_excel(RAW_FILE, sheet_name="MYEB1", skiprows=1)

    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    return df


def build_la_year_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert age-sex detailed MYE table into LA-year totals.
    """
    print("[ONS_CANONICAL] Building LA–year canonical table...")

    # Identify columns
    lad_code_col = "ladcode23"
    lad_name_col = "laname23"

    # Identify population year columns
    pop_year_cols = [c for c in df.columns if c.startswith("population_")]

    if not pop_year_cols:
        raise ValueError("[ONS_CANONICAL] No population_<year> columns found.")

    # Melt wide → long
    long_df = df.melt(
        id_vars=[lad_code_col, lad_name_col],
        value_vars=pop_year_cols,
        var_name="year_col",
        value_name="population",
    )

    # Extract year from population_YYYY
    long_df["year"] = long_df["year_col"].str.replace("population_", "", regex=False).astype(int)

    # Sum across sex and age
    agg = (
        long_df.groupby([lad_code_col, lad_name_col, "year"], as_index=False)
        .agg({"population": "sum"})
    )

    agg = agg.rename(
        columns={
            lad_code_col: "lad_code",
            lad_name_col: "lad_name",
        }
    )

    agg = agg.sort_values(["lad_code", "year"]).reset_index(drop=True)

    print(
        f"[ONS_CANONICAL] Canonical LA–year table built. "
        f"Shape: {agg.shape[0]} rows x {agg.shape[1]} columns"
    )
    return agg


def write_canonical(df: pd.DataFrame) -> None:
    print(f"[ONS_CANONICAL] Writing canonical table to: {CANONICAL_OUT_FILE}")
    df.to_csv(CANONICAL_OUT_FILE, index=False)
    print("[ONS_CANONICAL] Write complete.")


def main() -> int:
    print("[ONS_CANONICAL] Phase 2 harmonisation (ONS LA–year) starting...")
    df = load_ons_raw()
    canonical = build_la_year_canonical(df)
    write_canonical(canonical)
    print("[ONS_CANONICAL] Phase 2 harmonisation (ONS LA–year) finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
 
