from __future__ import annotations

from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT / "data" / "processed"
CANONICAL_DIR = PROCESSED_DIR / "canonical"
CANONICAL_DIR.mkdir(parents=True, exist_ok=True)

RAW_PROCESSED_FILE = PROCESSED_DIR / "dft_fuel_consumption_processed.csv"
CANONICAL_OUT_FILE = CANONICAL_DIR / "dft_la_year.csv"


def load_dft_processed() -> pd.DataFrame:
    """
    Load the Phase 1 processed DfT dataset.
    """
    if not RAW_PROCESSED_FILE.exists():
        raise FileNotFoundError(
            f"Processed DfT file not found: {RAW_PROCESSED_FILE}. "
            "Run src/ingestion/dft_ingest.py first."
        )

    print(f"[DFT_CANONICAL] Loading processed DfT from: {RAW_PROCESSED_FILE}")
    df = pd.read_csv(RAW_PROCESSED_FILE)
    df.columns = [c.strip() for c in df.columns]
    return df


def find_column(df: pd.DataFrame, keyword: str) -> str:
    """
    Find a column containing the keyword, case-insensitive.
    Raise an error if none or multiple are found.
    """
    matches = [c for c in df.columns if keyword.lower() in c.lower()]
    if len(matches) == 0:
        raise ValueError(f"[DFT_CANONICAL] Missing required column with keyword: '{keyword}'")
    if len(matches) > 1:
        # Choose the most specific, longest match
        matches = sorted(matches, key=len, reverse=True)
    return matches[0]


def build_la_year_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a canonical LA–year table from the DfT fuel consumption dataset.
    Uses robust fuzzy matching for column names.
    """
    print("[DFT_CANONICAL] Building LA–year canonical table...")

    # --- Robust lookup of required columns ---
    lad_code_col = find_column(df, "Local Authority Code")
    lad_name_col = find_column(df, "Local Authority")
    year_col = find_column(df, "__source_sheet__")

    # Fuel-related required fields (fuzzy matching)
    total_fuel_col = find_column(df, "Fuel consumption by all vehicles")
    personal_col = find_column(df, "Personal transport")
    freight_col = find_column(df, "Freight transport")
    bioenergy_col = find_column(df, "bioenergy")

    print("[DFT_CANONICAL] Resolved columns:")
    print("  lad_code_col:", lad_code_col)
    print("  lad_name_col:", lad_name_col)
    print("  year_col:", year_col)
    print("  total_fuel_col:", total_fuel_col)
    print("  personal_col:", personal_col)
    print("  freight_col:", freight_col)
    print("  bioenergy_col:", bioenergy_col)

    df = df.rename(
        columns={
            lad_code_col: "lad_code",
            lad_name_col: "lad_name",
            year_col: "year",
            total_fuel_col: "total_fuel_ktoe",
            personal_col: "personal_transport_ktoe",
            freight_col: "freight_transport_ktoe",
            bioenergy_col: "bioenergy_ktoe",
        }
    )

    df["year"] = df["year"].astype(int)

    canonical_cols = [
        "lad_code",
        "lad_name",
        "year",
        "total_fuel_ktoe",
        "personal_transport_ktoe",
        "freight_transport_ktoe",
        "bioenergy_ktoe",
    ]

    other_cols = [c for c in df.columns if c not in canonical_cols]

    out_df = df[canonical_cols + other_cols]
    out_df = out_df.sort_values(["lad_code", "year"]).reset_index(drop=True)

    print(
        f"[DFT_CANONICAL] Canonical LA–year table built. "
        f"Shape: {out_df.shape[0]} rows x {out_df.shape[1]} columns"
    )
    return out_df


def write_canonical_table(df: pd.DataFrame) -> None:
    print(f"[DFT_CANONICAL] Writing canonical table to: {CANONICAL_OUT_FILE}")
    df.to_csv(CANONICAL_OUT_FILE, index=False)
    print("[DFT_CANONICAL] Write complete.")


def main() -> int:
    print("[DFT_CANONICAL] Phase 2 harmonisation (DfT LA–year) starting...")
    df = load_dft_processed()
    canonical = build_la_year_canonical(df)
    write_canonical_table(canonical)
    print("[DFT_CANONICAL] Phase 2 harmonisation (DfT LA–year) finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

