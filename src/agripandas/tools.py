"""
Structured tool definitions.

This module wraps deterministic functions from the core engine into
callables that accept and return JSON-serialisable data.  These
functions are designed to be safe entry points for a LangChain agent
and can also be used directly.  Each tool accepts an instance of
:class:`~agripandas.registry.DataFrameRegistry` to operate on.

The functions here intentionally avoid returning raw DataFrame objects
and instead produce dictionaries or simple lists.  For example,
selection and aggregation operations return plain Python structures
instead of pandas objects.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional
import pandas as pd
from pydantic import BaseModel, Field, field_validator

from . import DataFrameRegistry, inspect_schema, TableSchema


# Input schemas for structured tools


class ListInput(BaseModel):
    """Input schema for listing dataframes."""

    pass


class DescribeInput(BaseModel):
    """Input schema for describing a dataframe."""

    name: str = Field(..., description="Name of the dataframe to describe")


class ColumnsInput(BaseModel):
    """Input schema for retrieving column names."""

    name: str = Field(..., description="Name of the dataframe")


class SubsetInput(BaseModel):
    """Input schema for extracting a subset of a dataframe."""

    name: str = Field(..., description="Name of the dataframe")
    filters: Optional[List[Dict[str, Any]]] = Field(
        None,
        description=(
            "Optional list of filtering conditions.  Each filter should be a "
            "mapping with keys 'column', 'op' and 'value'.  Supported "
            "operators: '==', '!=', '<', '<=', '>', '>='."
        ),
    )
    columns: Optional[List[str]] = Field(
        None, description="Optional list of column names to select"
    )

    @field_validator("filters")
    @classmethod
    def validate_filters(cls, v: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        """Validate filter specification."""
        if v is None:
            return v
        for item in v:
            if "column" not in item or "op" not in item or "value" not in item:
                raise ValueError("filters entries require 'column', 'op' and 'value'")
            if item["op"] not in ("==", "!=", "<", "<=", ">", ">="):
                raise ValueError(f"unsupported operator {item['op']}")
        return v


class GroupByInput(BaseModel):
    """Input schema for groupby-aggregate."""

    name: str = Field(..., description="Name of the dataframe")
    group_cols: List[str] = Field(..., description="Columns to group by")
    agg_spec: Dict[str, str] = Field(
        ...,
        description=(
            "Mapping of column name to aggregation function name "
            "(e.g., {'sales': 'sum', 'count': 'count'}).  Supported "
            "functions mirror pandas: 'sum', 'mean', 'min', 'max', 'count'."
        ),
    )


class StatInput(BaseModel):
    """Input schema for computing a simple statistic on a single column."""

    name: str = Field(..., description="Name of the dataframe")
    column: str = Field(..., description="Column name")
    metric: str = Field(
        ...,
        description=(
            "Statistical metric: one of 'mean', 'median', 'min', 'max', 'std', 'var'"
        ),
    )

    @field_validator("metric")
    @classmethod
    def validate_metric(cls, v: str) -> str:
        allowed = {"mean", "median", "min", "max", "std", "var"}
        if v not in allowed:
            raise ValueError(f"unsupported metric '{v}'")
        return v


# Tool functions


def list_dataframes(registry: DataFrameRegistry) -> List[str]:
    """Return a list of registered dataframe names.

    Parameters
    ----------
    registry:
        The registry from which to list tables.
    """
    return registry.list()


def describe_dataframe(registry: DataFrameRegistry, name: str) -> Dict[str, Any]:
    """Return a schema description of a registered dataframe.

    Parameters
    ----------
    registry:
        The registry containing the dataframe.
    name:
        Name of the dataframe to describe.

    Returns
    -------
    dict
        A dictionary representation of the table schema.
    """
    df = registry.get(name)
    schema: TableSchema = inspect_schema(name, df)
    return schema.model_dump()


def get_columns(registry: DataFrameRegistry, name: str) -> List[str]:
    """Return the list of column names for a registered dataframe."""
    df = registry.get(name)
    return list(map(str, df.columns))


def extract_subset(
    registry: DataFrameRegistry,
    name: str,
    filters: Optional[List[Dict[str, Any]]] = None,
    columns: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return a JSON-friendly subset of a dataframe.

    Filters and column selection are applied in order.  The result is a
    list of row dictionaries with primitive values.

    Parameters
    ----------
    registry:
        The registry containing the dataframe.
    name:
        Name of the dataframe.
    filters:
        Optional list of filter specifications.  See :class:`SubsetInput`.
    columns:
        Optional list of columns to include.
    """
    df = registry.get(name)
    # Apply filters
    if filters:
        for flt in filters:
            col, op, val = flt["column"], flt["op"], flt["value"]
            if col not in df.columns:
                continue  # Or raise error? Let's be safe and skip for now
            if op == "==":
                df = df[df[col] == val]
            elif op == "!=":
                df = df[df[col] != val]
            elif op == "<":
                df = df[df[col] < val]
            elif op == "<=":
                df = df[df[col] <= val]
            elif op == ">":
                df = df[df[col] > val]
            elif op == ">=":
                df = df[df[col] >= val]
    # Select columns
    if columns:
        valid_cols = [c for c in columns if c in df.columns]
        df = df[valid_cols]
    # Convert to list of dicts with native Python types
    result: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        row_dict = {
            str(k): (None if pd.isna(v) else v.item() if hasattr(v, "item") else v)
            for k, v in row.items()
        }
        result.append(row_dict)
    return result


def groupby_aggregate(
    registry: DataFrameRegistry,
    name: str,
    group_cols: List[str],
    agg_spec: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Perform a group-by aggregation and return the result as a list of dicts.

    Parameters
    ----------
    registry:
        The registry containing the dataframe.
    name:
        Name of the dataframe.
    group_cols:
        Columns to group by.
    agg_spec:
        Mapping of target column to aggregation function name.

    Returns
    -------
    list of dict
        Each row of the aggregated result as a dictionary.
    """
    df = registry.get(name)
    # Validate columns
    for col in group_cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in table '{name}'")
    for col in agg_spec.keys():
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in table '{name}'")

    grouped = df.groupby(group_cols).agg(agg_spec).reset_index()
    # Convert to JSON-friendly output
    records: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        row_dict = {
            str(k): (None if pd.isna(v) else v.item() if hasattr(v, "item") else v)
            for k, v in row.items()
        }
        records.append(row_dict)
    return records


def compute_stat(
    registry: DataFrameRegistry, name: str, column: str, metric: str
) -> Dict[str, Any]:
    """Compute a simple statistic on a column.

    Parameters
    ----------
    registry:
        The registry containing the dataframe.
    name:
        Name of the dataframe.
    column:
        Column name.
    metric:
        The statistic to compute (see :class:`StatInput`).

    Returns
    -------
    dict
        A dictionary with keys ``column``, ``metric`` and ``value``.
    """
    df = registry.get(name)
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in table '{name}'")
    series = df[column]
    if metric == "mean":
        val = series.mean()
    elif metric == "median":
        val = series.median()
    elif metric == "min":
        val = series.min()
    elif metric == "max":
        val = series.max()
    elif metric == "std":
        val = series.std()
    elif metric == "var":
        val = series.var()
    else:
        raise ValueError(f"unsupported metric '{metric}'")
    
    # Handle NaN for JSON compatibility
    value = float(val) if pd.notna(val) else None
    return {"column": column, "metric": metric, "value": value}
