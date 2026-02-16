"""
Schema inspection utilities.

This module defines lightweight Pydantic models that describe the shape
of a table and functions to compute these models from a
``pandas.DataFrame``.  The models are designed to be JSON serialisable
and useful for validation or reporting.
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, Field


class ColumnInfo(BaseModel):
    """Metadata about a single column in a dataframe."""

    name: str = Field(..., description="Normalised column name")
    dtype: str = Field(..., description="String representation of the pandas dtype")
    nulls: int = Field(..., description="Number of null values in the column")
    unique: Optional[int] = Field(
        None,
        description=(
            "Number of distinct non-null values in the column.  "
            "None if the number of unique values exceeds a threshold."
        ),
    )

    model_config = {"from_attributes": True}


class TableSchema(BaseModel):
    """Summary information about a dataframe."""

    name: str = Field(..., description="Name of the table")
    rows: int = Field(..., description="Number of rows in the dataframe")
    columns: List[ColumnInfo] = Field(
        ..., description="List of columns with metadata"
    )

    model_config = {"from_attributes": True}


def inspect_schema(name: str, df: pd.DataFrame, max_unique: int = 50) -> TableSchema:
    """Compute a :class:`TableSchema` for ``df``.

    Parameters
    ----------
    name:
        Name of the table.
    df:
        The dataframe to inspect.
    max_unique:
        Maximum number of distinct values to count when computing the
        ``unique`` field for each column.  If the number of unique
        values exceeds this threshold, ``unique`` will be ``None``.

    Returns
    -------
    TableSchema
        A serialisable summary of the dataframe.
    """
    cols: List[ColumnInfo] = []
    for col in df.columns:
        series = df[col]
        nulls = int(series.isna().sum())
        # Count unique values but avoid large cardinalities
        nunique = series.nunique(dropna=True)
        unique = int(nunique) if nunique <= max_unique else None
        cols.append(
            ColumnInfo(
                name=str(col),
                dtype=str(series.dtype),
                nulls=nulls,
                unique=unique,
            )
        )
    return TableSchema(name=name, rows=int(len(df)), columns=cols)