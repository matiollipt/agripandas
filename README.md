agripandas
==========

This repository contains a minimal, working implementation of an
experimental tabular analytics engine designed to serve as a safe
backend for LangChain agents.  The modules under ``agripandas`` are
organised according to a clear layer separation:

* **registry.py** – Defines a :class:`DataFrameRegistry` for storing
  named :class:`pandas.DataFrame` objects alongside provenance metadata.
* **loaders.py** – Functions for ingesting Excel workbooks or folders
  into a registry, normalising column names and returning the registered
  table names.
* **schema.py** – Pydantic models (:class:`ColumnInfo` and
  :class:`TableSchema`) and a helper to inspect dataframe schemas.
* **tools.py** – Structured functions that wrap core operations
  (listing tables, describing schemas, extracting subsets, grouping and
  aggregating, computing simple statistics).  All outputs are
  JSON-serialisable and avoid returning raw DataFrames.
* **agent.py** – An illustrative “ReAct”-style agent scaffold.  This
  agent uses simple heuristics to select which tool to call based on a
  natural language question.  It serves as a template for integrating
  more sophisticated language models.

The code emphasises deterministic behaviour, reproducibility and a
small, explicit API surface.  It avoids executing any arbitrary
Python code and is suitable as a starting point for building safe
agent-driven analytics pipelines.
