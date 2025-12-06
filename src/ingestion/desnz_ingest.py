from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml


# Paths
ROOT = Path(__file__).resolve().parents[2]
DATASETS_CONFIG = ROOT / "config" / "datasets.yaml"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def load_datasets_config() -> dict:
    """Load the datasets registry from YAML."""
    with DATASETS_CONFIG.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    # either {datasets: {...}} or direct mapping
    return doc.get("datasets", doc)


def get_desnz_config(datasets_cfg: dict) -> dict:
    """Return the config block for desnz_ghg_emissions."""
    try:
        return datasets_cfg["desnz_ghg_emissions"]
    except KeyError:
        raise KeyError(
            "Dataset 'desnz_ghg_emissions' not found in config/datasets.yaml"
        )


def read_desnz_raw(desnz_cfg: dict) -> pd.DataFrame:
    """
    Read the DESNZ raw CSV using the loader information.

    Phase 1 ingestion: we do minimal, safe cleaning:
      - load via pandas
      - strip whitespace from column names
    """
    loader = desnz_cfg.get("loader", "csv")
    path_str = desnz_cfg.get("path")
    if not path_str:
        raise ValueError("desnz_ghg_emissions config must contain a 'path' field")

    raw_path = ROOT / path_str
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw DESNZ file not found at {raw_path}")

    if loader != "csv":
        raise ValueError(
            f"Expected loader='csv' for desnz_ghg_emissions, found loader={loader!r}"
        )

    print(f"[DESNZ] Reading raw CSV from: {raw_path}")
    df = pd.read_csv(raw_path)

    # Minimal cleaning
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]

    print(f"[DESNZ] Loaded shape: {df.shape[0]} rows x {df.shape[1]} columns")
    return df


def write_desnz_processed(df: pd.DataFrame) -> Path:
    """
    Write the minimally cleaned DESNZ table to data/processed/.

    Phase 1, we simply write as CSV with a consistent filename.
    """
    out_path = PROCESSED_DIR / "desnz_ghg_emissions_processed.csv"
    print(f"[DESNZ] Writing processed CSV to: {out_path}")
    df.to_csv(out_path, index=False)
    print("[DESNZ] Write complete.")
    return out_path


def main() -> int:
    print("[DESNZ] Phase 1 ingestion starting...")
    datasets_cfg = load_datasets_config()
    desnz_cfg = get_desnz_config(datasets_cfg)
    df = read_desnz_raw(desnz_cfg)
    write_desnz_processed(df)
    print("[DESNZ] Phase 1 ingestion finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
