"""
Loaders for external tabular data.

This module provides functions to ingest Excel workbooks or entire
folders of workbooks into a :class:`~agripandas.registry.DataFrameRegistry`.
Functions in this module do not perform any schema inference beyond
normalising column names; they simply read sheets into dataframes and
store them under deterministic names derived from file paths.

All loaders return the names of the registered dataframes for further
processing.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

from .registry import DataFrameRegistry


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with normalised column names.

    Column names are stripped, converted to lowercase and whitespace is
    replaced with underscores.  Duplicate names are deduplicated by
    appending a numeric suffix.
    """
    df = df.copy()
    new_columns = []
    seen = {}
    for col in df.columns:
        base = str(col).strip().lower().replace(" ", "_")
        if base not in seen:
            seen[base] = 0
            new_columns.append(base)
        else:
            seen[base] += 1
            new_columns.append(f"{base}_{seen[base]}")
    df.columns = new_columns
    return df


def load_excel(
    registry: DataFrameRegistry, path: str, sheet: Optional[str] = None
) -> List[str]:
    """Load an Excel workbook into a registry with robust multi-row header detection.

    This function reads a single Excel file from ``path``. It attempts to detect
    the real header row and handles merged cells or multi-row headers by
    combining them.
    """
    file_path = Path(path)
    registered: List[str] = []
    
    anchors = {"tratamento", "parcela", "ramo", "bloco", "nó", "data"}

    with pd.ExcelFile(file_path) as xls:
        sheet_names: Iterable[str] = [sheet] if sheet is not None else xls.sheet_names
        for sheet_name in sheet_names:
            # 1. Peek at the first 20 rows to find the header start
            df_peek = xls.parse(sheet_name, header=None, nrows=20)
            
            start_idx = 0
            for idx, row in df_peek.iterrows():
                row_vals = [str(v).lower() for v in row if pd.notnull(v)]
                if any(anchor in row_vals for anchor in anchors):
                    start_idx = idx
                    break
                # Fallback: first row with significant non-nulls
                if row.count() > df_peek.shape[1] * 0.5:
                    start_idx = idx
                    break

            # 2. Heuristic: Check if the next 1-2 rows should be part of the header
            # We look for rows that contain strings and are not mostly numeric
            header_rows = [start_idx]
            for next_idx in range(start_idx + 1, min(start_idx + 3, 20)):
                row = df_peek.iloc[next_idx]
                # If row has many strings and few numbers, it's likely a sub-header
                str_count = sum(1 for v in row if isinstance(v, str))
                num_count = sum(1 for v in row if isinstance(v, (int, float)) and pd.notnull(v))
                if str_count > num_count and row.count() > 0:
                    header_rows.append(next_idx)
                else:
                    break

            # 3. Construct the merged header
            header_df = df_peek.iloc[header_rows].ffill(axis=1).fillna("")
            new_columns = []
            for col in range(header_df.shape[1]):
                parts = []
                for r in range(header_df.shape[0]):
                    val = str(header_df.iloc[r, col]).strip()
                    if val and val.lower() not in [p.lower() for p in parts]:
                        parts.append(val)
                new_columns.append("_".join(parts) if parts else f"unnamed_{col}")

            # 4. Re-read the full data skipping the header rows
            df = xls.parse(sheet_name, skiprows=max(header_rows) + 1, header=None)
            df.columns = new_columns
            
            # 5. Cleaning
            df = df.dropna(how="all").dropna(axis=1, how="all")
            df = _normalize_column_names(df)
            
            table_name = f"{file_path.stem}__{sheet_name.strip().lower()}"
            registry.register(
                table_name,
                df,
                {
                    "file_path": os.fspath(file_path),
                    "sheet": sheet_name,
                    "header_rows": header_rows,
                },
            )
            registered.append(table_name)
    return registered


def load_csv(registry: DataFrameRegistry, path: str) -> List[str]:


    """Load a CSV file into a registry.





    Each file is registered under a name derived from the file stem.





    Parameters


    ----------


    registry:


        The registry into which the dataframe will be stored.


    path:


        Path to the ``.csv`` file.





    Returns


    -------


    list of str


        The name of the registered table.


    """


    file_path = Path(path)


    df = pd.read_csv(file_path)


    df = _normalize_column_names(df)


    table_name = file_path.stem.strip().lower()


    registry.register(


        table_name,


        df,


        {


            "file_path": os.fspath(file_path),


        },


    )


    return [table_name]








def load_file(registry: DataFrameRegistry, path: str) -> List[str]:


    """Load a file into a registry, detecting format by extension.





    Supports ``.xlsx`` and ``.csv``.





    Parameters


    ----------


    registry:


        The registry into which the dataframes will be stored.


    path:


        Path to the file.





    Returns


    -------


    list of str


        The names of the registered tables.


    """


    file_path = Path(path)


    suffix = file_path.suffix.lower()


    if suffix == ".xlsx":


        return load_excel(registry, path)


    elif suffix == ".csv":


        return load_csv(registry, path)


    else:


        raise ValueError(f"Unsupported file extension: {suffix}")








def load_folder(


    registry: DataFrameRegistry, path: str, pattern: Optional[str] = None


) -> List[str]:


    """Load all matching files in a folder.





    The function walks the directory specified by ``path`` and loads every


    file matching ``pattern``. If ``pattern`` is ``None``, it tries to load


    all ``.xlsx`` and ``.csv`` files.





    Parameters


    ----------


    registry:


        The registry into which the dataframes will be stored.


    path:


        Path to a directory containing data files.


    pattern:


        Optional glob pattern for filenames to load.





    Returns


    -------


    list of str


        The names of all registered tables.


    """


    base = Path(path)


    names: List[str] = []


    


    if pattern:


        for file in base.glob(pattern):


            names.extend(load_file(registry, str(file)))


    else:


        # Default to both xlsx and csv


        for file in base.glob("*.xlsx"):


            names.extend(load_excel(registry, str(file)))


        for file in base.glob("*.csv"):


            names.extend(load_csv(registry, str(file)))


            


    return names

