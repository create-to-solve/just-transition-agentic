"""
JTIS v3 – ScoutAgent
Pre-ingestion diagnostics with schema-aware loading.

This agent:
- Reads dataset registry from config/datasets.yaml
- Loads each dataset according to loader/sheet/skiprows settings
- Applies validation schemas for required columns, numeric columns, year ranges, etc.
- Guesses LAD/year columns heuristically
- Produces a diagnostics JSON report in outputs/diagnostics
- Prints a summary to stdout

Safe to run anytime. Does not modify data.
"""

from __future__ import annotations

import json
import datetime as dt
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import yaml

# -------------------------------------------------------
# Paths
# -------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config"
REGISTRY_PATH = CONFIG / "datasets.yaml"
SCHEMAS_DIR = CONFIG / "validation_schemas"
DIAG_DIR = ROOT / "outputs" / "diagnostics"


# -------------------------------------------------------
# Dataclasses
# -------------------------------------------------------

@dataclass
class DatasetCheck:
    dataset_key: str
    name: str
    path: str
    exists: bool
    readable: bool
    n_rows: Optional[int]
    columns: List[str]
    schema_checked: bool
    schema_ok: Optional[bool]
    missing_columns: List[str]
    extra_columns: List[str]
    lad_guess: Optional[str]
    year_guess: Optional[str]
    errors: List[str]


@dataclass
class ScoutReport:
    timestamp_utc: str
    repo_root: str
    datasets_registry_path: str
    all_ok: bool
    datasets: List[DatasetCheck]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp_utc": self.timestamp_utc,
            "repo_root": self.repo_root,
            "datasets_registry_path": self.datasets_registry_path,
            "all_ok": self.all_ok,
            "datasets": [asdict(d) for d in self.datasets]
        }


# -------------------------------------------------------
# Utility functions
# -------------------------------------------------------

def load_registry(path: Path) -> Dict[str, Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    if "datasets" not in doc:
        raise ValueError("datasets.yaml missing 'datasets:' block")
    return doc["datasets"]


def load_schema(key: str) -> Optional[Dict[str, Any]]:
    schema_path = SCHEMAS_DIR / f"{key}.yaml"
    if not schema_path.exists():
        return None
    with schema_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def guess_lad(cols: List[Any]) -> Optional[str]:
    lower_cols = [str(c).lower() for c in cols]
    hints = ["lad", "local authority", "lad19", "la code"]
    for orig, low in zip(cols, lower_cols):
        if any(h in low for h in hints):
            return str(orig)
    return None


def guess_year(cols: List[Any]) -> Optional[str]:
    lower_cols = [str(c).lower() for c in cols]
    for orig, low in zip(cols, lower_cols):
        if "year" in low:
            return str(orig)
    return None


# Validation helpers
def validate_required(df: pd.DataFrame, schema: Dict[str, Any]):
    missing = []
    required = schema.get("required_columns", {})
    for logical_name, rule in required.items():
        allowed = rule.get("any_of", [])
        if not any(col in df.columns for col in allowed):
            missing.append(f"{logical_name}: {allowed}")
    return len(missing) == 0, missing


def validate_wide_years(df: pd.DataFrame, schema: Dict[str, Any]):
    wy = schema.get("wide_years")
    if not wy:
        return True, []
    prefix = wy["prefix"]
    start = wy["allowed_year_range"]["start"]
    end = wy["allowed_year_range"]["end"]
    missing = []
    for year in range(start, end + 1):
        col = f"{prefix}{year}"
        if col not in df.columns:
            missing.append(col)
    return len(missing) == 0, missing


def validate_numeric(df: pd.DataFrame, schema: Dict[str, Any]):
    numeric = schema.get("column_rules", {}).get("numeric_columns", [])
    non_numeric = []
    for col in numeric:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            non_numeric.append(col)
    return len(non_numeric) == 0, non_numeric


def validate_row_count(df: pd.DataFrame, schema: Dict[str, Any]):
    expected = schema.get("row_rules", {}).get("min_rows")
    if not expected:
        return True, None
    return len(df) >= expected, expected


# -------------------------------------------------------
# ScoutAgent
# -------------------------------------------------------

class ScoutAgent:
    def __init__(self, registry_path: Path | None = None):
        self.registry_path = registry_path or REGISTRY_PATH

    def run(self) -> ScoutReport:
        registry = load_registry(self.registry_path)
        results = []

        for key, meta in registry.items():
            result = self._check_dataset(key, meta)
            results.append(result)

        all_ok = all(r.exists and r.readable and (r.schema_ok is not False) for r in results)

        return ScoutReport(
            timestamp_utc=dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            repo_root=str(ROOT),
            datasets_registry_path=str(self.registry_path),
            all_ok=all_ok,
            datasets=results,
        )

    def save(self, report: ScoutReport) -> None:
        DIAG_DIR.mkdir(parents=True, exist_ok=True)
        out_path = DIAG_DIR / "scout_report.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)
        print(f"[ScoutAgent] Report written to {out_path}")

    # ----------------------------
    # Internal: dataset check
    # ----------------------------
    def _check_dataset(self, key: str, meta: Dict[str, Any]) -> DatasetCheck:
        errors = []
        name = meta.get("description", key)

        raw_path = meta.get("path")
        path = ROOT / raw_path
        exists = path.exists()

        if not exists:
            errors.append(f"File does not exist: {path}")
            return DatasetCheck(
                dataset_key=key, name=name, path=str(path),
                exists=False, readable=False, n_rows=None, columns=[],
                schema_checked=False, schema_ok=None,
                missing_columns=[], extra_columns=[], lad_guess=None,
                year_guess=None, errors=errors,
            )

        loader = meta.get("loader", "csv")
        sheet = meta.get("sheet")
        sheets = meta.get("sheets")
        skip = meta.get("header_rows_to_skip")

        read_kwargs = {}
        if skip is not None:
            read_kwargs["skiprows"] = skip

        if loader == "excel":
            if sheet:
                read_kwargs["sheet_name"] = sheet
            elif sheets:
                read_kwargs["sheet_name"] = sheets[0]

        # Try reading
        try:
            if loader == "excel":
                df = pd.read_excel(path, **read_kwargs)
            else:
                df = pd.read_csv(path, **read_kwargs)
            readable = True
        except Exception as exc:
            errors.append(f"Failed to read file: {exc}")
            return DatasetCheck(
                dataset_key=key, name=name, path=str(path),
                exists=True, readable=False, n_rows=None, columns=[],
                schema_checked=True, schema_ok=False,
                missing_columns=["<unreadable>"], extra_columns=[],
                lad_guess=None, year_guess=None, errors=errors,
            )

        # Schema validation
        schema = load_schema(key)
        schema_checked = schema is not None
        missing_total = []
        schema_ok = None

        if schema:
            ok_req, miss_req = validate_required(df, schema)
            ok_wy, miss_wy = validate_wide_years(df, schema)
            ok_num, miss_num = validate_numeric(df, schema)
            ok_rows, expected_rows = validate_row_count(df, schema)

            missing_total.extend(miss_req)
            missing_total.extend(miss_wy)
            if miss_num:
                missing_total.append(f"Non-numeric: {miss_num}")
            if not ok_rows and expected_rows is not None:
                missing_total.append(f"Row count < {expected_rows}")

            schema_ok = ok_req and ok_wy and ok_num and ok_rows

        return DatasetCheck(
            dataset_key=key,
            name=name,
            path=str(path),
            exists=True,
            readable=True,
            n_rows=len(df),
            columns=list(df.columns),
            schema_checked=schema_checked,
            schema_ok=schema_ok,
            missing_columns=missing_total,
            extra_columns=[],
            lad_guess=guess_lad(df.columns),
            year_guess=guess_year(df.columns),
            errors=errors,
        )


# -------------------------------------------------------
# CLI entrypoint
# -------------------------------------------------------

def main() -> None:
    agent = ScoutAgent()
    report = agent.run()
    agent.save(report)
    print("=== ScoutAgent Summary ===")
    print(f"All OK: {report.all_ok}")
    for d in report.datasets:
        print(f"[{d.dataset_key}] {d.name} | Exists={d.exists} | Readable={d.readable} | SchemaOK={d.schema_ok}")


if __name__ == "__main__":
    main()
