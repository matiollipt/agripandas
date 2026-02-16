"""
Export module for agripandas.
Handles the persistence of registered dataframes to disk (e.g., as CSVs).
"""

from pathlib import Path
from typing import Any, List, Optional


def export_to_csv(
    registry: Any, output_dir: Path, tables: Optional[List[str]] = None
) -> None:
    """
    Exports registered dataframes to clean CSV files.

    The output filename is derived from the table name (e.g., 'File__Sheet.csv').

    Args:
        registry: The DataFrameRegistry containing the data.
        output_dir: The directory where CSVs will be saved.
        tables: Optional list of table names to export. If None, attempts to export all.
    """
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    # If tables not provided, try to list them from registry
    if tables is None:
        if hasattr(registry, "list_tables"):
            tables = registry.list_tables()
        elif hasattr(registry, "_frames"):
            tables = list(registry._frames.keys())
        else:
            print("Error: No tables specified and cannot list from registry.")
            return

    print(f"--- Exporting {len(tables)} tables to {output_dir} ---")
    for table in tables:
        df = registry.get(table)
        if df is not None:
            # Use the table name as the filename.
            # Table names typically include the sheet name (e.g. "File__Sheet").
            filename = f"{table}.csv"
            file_path = output_dir / filename

            df.to_csv(file_path, index=False)
            print(f"  Saved: {file_path}")
        else:
            print(f"  Warning: Table '{table}' not found.")
