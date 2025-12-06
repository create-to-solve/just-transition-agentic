from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ROOT = Path(__file__).resolve().parents[2]
CANONICAL_DIR = ROOT / "data" / "processed" / "canonical"

DESNZ_FILE = CANONICAL_DIR / "desnz_la_year.csv"
DFT_FILE = CANONICAL_DIR / "dft_la_year.csv"
ONS_FILE = CANONICAL_DIR / "ons_la_year.csv"

OUT_FILE = CANONICAL_DIR / "jtis_base_la_year.csv"
DIAG_FILE = ROOT / "outputs" / "diagnostics" / "composer_report.json"


def load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    logging.info(f"Loading: {path.name}")
    return pd.read_csv(path)


def filter_england_wales(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure lad_code is string, drop nulls, keep only E/W LADs."""
    df = df.copy()

    # Fix dtype
    df["lad_code"] = df["lad_code"].astype(str)

    # Drop rows where lad_code is null or "nan"
    df = df[df["lad_code"].notna()]
    df = df[~df["lad_code"].isin(["nan", "NaN", "None"])]

    # Now apply filter
    df = df[df["lad_code"].str.startswith(("E", "W"))]

    return df



def check_missing_combinations(df_list: list[pd.DataFrame]) -> dict:
    """
    Identify any LAD-year combinations missing from any dataset.
    """
    lad_year_sets = [set(zip(df["lad_code"], df["year"])) for df in df_list]
    all_keys = set.union(*lad_year_sets)

    reports = {}
    labels = ["desnz", "dft", "ons"]

    for name, df, keyset in zip(labels, df_list, lad_year_sets):
        missing = sorted(all_keys - keyset)
        reports[name] = {
            "missing_count": len(missing),
            "missing_examples": missing[:20],
        }

    return reports


def compose():
    logging.info("=== ComposerAgent: start composition ===")

    desnz = filter_england_wales(load_dataset(DESNZ_FILE))
    dft = filter_england_wales(load_dataset(DFT_FILE))
    ons = filter_england_wales(load_dataset(ONS_FILE))

    # Ensure correct dtypes
    for df in [desnz, dft, ons]:
        df["year"] = df["year"].astype(int)

    # Diagnostics
    diagnostics = check_missing_combinations([desnz, dft, ons])
    logging.info(f"Diagnostics: {diagnostics}")

    # Merge DESNZ + DfT
    logging.info("Merging DESNZ + DfT...")
    merged = pd.merge(
        desnz,
        dft,
        on=["lad_code", "year"],
        how="inner",
        suffixes=("", "_dft"),
    )

    # Merge ONS (population)
    logging.info("Merging with ONS population...")
    merged = pd.merge(
        merged,
        ons[["lad_code", "year", "population"]],
        on=["lad_code", "year"],
        how="inner",
    )

    # Validate LAD-name consistency
    name_cols = [c for c in merged.columns if "lad_name" in c]
    if len(name_cols) > 1:
        # Ensure DESNZ lad_name wins
        merged = merged.rename(columns={name_cols[0]: "lad_name"})
        merged = merged.drop(columns=[c for c in name_cols[1:]], errors="ignore")

    merged = merged.sort_values(["lad_code", "year"]).reset_index(drop=True)

    logging.info(f"Final JTIS base table shape: {merged.shape}")
    logging.info(f"Writing JTIS base table → {OUT_FILE}")

    merged.to_csv(OUT_FILE, index=False)

    # Write diagnostics
    import json
    DIAG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DIAG_FILE, "w") as f:
        json.dump(diagnostics, f, indent=2)

    logging.info("=== ComposerAgent finished successfully ===")
    return merged


def main():
    compose()


if __name__ == "__main__":
    main()
 
