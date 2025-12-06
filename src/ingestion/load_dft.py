import pandas as pd
from pathlib import Path

def load_dft_fuel(path: str | Path) -> pd.DataFrame:
    """
    Load DfT subnational road fuel consumption dataset (Excel with multiple yearly sheets).
    
    Each sheet corresponds to one calendar year, e.g. "2020".
    Sheets contain:
        - metadata rows (0 and 1)
        - true header row at row index 2
        - LAD-level fuel consumption columns (ktoe)
    
    Returns a tidy table with columns:
        lad_code, year, fuel_ktoe
    """

    path = Path(path)
    xls = pd.ExcelFile(path)

    # Identify which sheet names are valid years (e.g. "2005" → "2023")
    year_sheets = [s for s in xls.sheet_names if s.isdigit()]

    all_years = []

    for year in year_sheets:

        # Load sheet, skip metadata rows
        df = pd.read_excel(path, sheet_name=year, skiprows=3)

        # Identify LAD code column
        # Sometimes it's exactly "Local Authority Code"
        lad_code_col = None
        for col in df.columns:
            if isinstance(col, str) and col.strip().lower() == "local authority code":
                lad_code_col = col
                break

        if lad_code_col is None:
            raise ValueError(f"Could not find LAD code column in sheet {year}")

        # Select numeric fuel columns only
        numeric_cols = df.select_dtypes(include=["number"]).columns

        # Sum all numeric columns to get total fuel consumption
        # (cleanest, avoids per-category processing)
        df["fuel_ktoe"] = df[numeric_cols].sum(axis=1)

        # Keep only LAD code + year + fuel_ktoe
        cleaned = df[[lad_code_col, "fuel_ktoe"]].copy()
        cleaned = cleaned.rename(columns={lad_code_col: "lad_code"})

        cleaned["year"] = int(year)

        # Drop rows where LAD code is missing (empty metadata/footers)
        cleaned = cleaned.dropna(subset=["lad_code"])

        all_years.append(cleaned)

    # Combine all sheets
    out = pd.concat(all_years, ignore_index=True)

    return out
 
