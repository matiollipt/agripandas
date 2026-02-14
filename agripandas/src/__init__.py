# Copyright (c) 2026, agripandas development team
"""
agripandas
==========

A lightweight, deterministic tabular analytics engine built on top of
``pandas``.  This package provides a small set of modules that can be
combined to ingest Excel workbooks, register `pandas.DataFrame` objects
under stable names, inspect table schemas and perform common
transformations.  All functions avoid dynamic code execution and are
designed to be used both directly and as part of a LangChain agent
workflow.

The public API exports the most commonly used classes and functions for
convenience.
"""

from .registry import DataFrameRegistry
from .loaders import load_excel, load_folder, load_csv, load_file
from .schema import ColumnInfo, TableSchema, inspect_schema
from .tools import (
    list_dataframes,
    describe_dataframe,
    get_columns,
    extract_subset,
    groupby_aggregate,
    compute_stat,
)

__all__ = [
    "DataFrameRegistry",
    "load_excel",
    "load_csv",
    "load_file",
    "load_folder",
    "ColumnInfo",
    "TableSchema",
    "inspect_schema",
    "list_dataframes",
    "describe_dataframe",
    "get_columns",
    "extract_subset",
    "groupby_aggregate",
    "compute_stat",
]