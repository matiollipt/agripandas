"""Agripandas — Simplified Production Pipeline

This script runs a deterministic pipeline on an Excel workbook:
1. Loads the workbook into the registry.
2. Generates schemas and column lists for all tables.
3. Computes basic statistics for numeric columns.
4. Saves all artifacts (JSON metadata + cleaned CSVs) to disk.
"""

from pathlib import Path
import json
from typing import Any

from agripandas import (
    DataFrameRegistry,
    load_excel,
    describe_dataframe,
    get_columns,
    compute_stat,
)
from agripandas.export import export_to_csv

# --- Configuration ---
XLSX_PATH = Path("data/1_ExemploHarvista.xlsx")
OUT_DIR = Path("./_artifacts")

def save_json(path: Path, obj: Any) -> None:
    """Helper to save JSON data to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved artifact: {path}")

def run_pipeline():
    print(f"--- Starting Pipeline for: {XLSX_PATH} ---")

    # 1. Initialize Registry
    registry = DataFrameRegistry()

    # 2. Load Workbook
    if not XLSX_PATH.exists():
        print(f"Error: Input file not found: {XLSX_PATH}")
        return

    try:
        tables = load_excel(registry, XLSX_PATH)
    except Exception as e:
        print(f"Failed to load Excel file: {e}")
        return

    if not tables:
        print("No tables found in workbook.")
        return

    print(f"Successfully registered {len(tables)} tables: {tables}")
    save_json(OUT_DIR / "tables.json", tables)

    # 3. Process Each Table
    for table in tables:
        print(f"\nProcessing table: {table}")
        
        # A. Schema & Columns
        schema = describe_dataframe(registry, table)
        save_json(OUT_DIR / f"{table}__schema.json", schema)

        cols = get_columns(registry, table)
        save_json(OUT_DIR / f"{table}__columns.json", cols)

        # B. Basic Statistics (for numeric columns)
        # We access the dataframe directly to check dtypes for the generic pipeline
        df = registry.get(table)
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        
        if numeric_cols:
            stats = {}
            for col in numeric_cols:
                stats[col] = {
                    "mean": compute_stat(registry, table, column=col, metric="mean"),
                    "min": compute_stat(registry, table, column=col, metric="min"),
                    "max": compute_stat(registry, table, column=col, metric="max"),
                }
            save_json(OUT_DIR / f"{table}__stats.json", stats)
        else:
            print(f"  (No numeric columns found for stats in {table})")

    # 4. Export Clean Data
    export_to_csv(registry, OUT_DIR, tables)

    print(f"\n--- Pipeline Finished. Artifacts in {OUT_DIR.resolve()} ---")

if __name__ == "__main__":
    run_pipeline()
