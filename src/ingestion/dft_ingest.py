from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[2]
DATASETS_CONFIG = ROOT / "config" / "datasets.yaml"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def load_datasets_config() -> dict:
    with DATASETS_CONFIG.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    return doc.get("datasets", doc)


def get_dft_config(datasets_cfg: dict) -> dict:
    try:
        return datasets_cfg["dft_fuel_consumption"]
    except KeyError:
        raise KeyError(
            "Dataset 'dft_fuel_consumption' not found in config/datasets.yaml"
        )


def read_dft_raw(cfg: dict) -> pd.DataFrame:
    loader = cfg.get("loader", "excel")
    path_str = cfg.get("path")
    sheets = cfg.get("sheets")
    header_rows_to_skip = cfg.get("header_rows_to_skip", 0)

    if not path_str:
        raise ValueError("dft_fuel_consumption config must contain a 'path' field")

    raw_path = ROOT / path_str
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw DfT file not found at {raw_path}")

    if loader != "excel":
        raise ValueError(
            f"Expected loader='excel' for dft_fuel_consumption, found {loader!r}"
        )

    print(f"[DfT] Reading raw Excel from: {raw_path}")

    # Phase 1: simple concatenation of all specified sheets (if list),
    #          with consistent header skipping
    if isinstance(sheets, list):
        frames = []
        for sheet in sheets:
            print(f"[DfT]  - Reading sheet: {sheet}")
            df_sheet = pd.read_excel(
                raw_path, sheet_name=sheet, skiprows=header_rows_to_skip
            )
            df_sheet.columns = [c.strip() if isinstance(c, str) else c for c in df_sheet.columns]
            df_sheet["__source_sheet__"] = sheet
            frames.append(df_sheet)
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.read_excel(
            raw_path,
            sheet_name=sheets if sheets is not None else 0,
            skiprows=header_rows_to_skip,
        )
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]

    print(f"[DfT] Loaded shape: {df.shape[0]} rows x {df.shape[1]} columns")
    return df


def write_dft_processed(df: pd.DataFrame) -> Path:
    out_path = PROCESSED_DIR / "dft_fuel_consumption_processed.csv"
    print(f"[DfT] Writing processed CSV to: {out_path}")
    df.to_csv(out_path, index=False)
    print("[DfT] Write complete.")
    return out_path


def main() -> int:
    print("[DfT] Phase 1 ingestion starting...")
    datasets_cfg = load_datasets_config()
    cfg = get_dft_config(datasets_cfg)
    df = read_dft_raw(cfg)
    write_dft_processed(df)
    print("[DfT] Phase 1 ingestion finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
