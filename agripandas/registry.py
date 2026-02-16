"""
Registry module.

This module defines :class:`DataFrameRegistry`, a simple container for
registering and retrieving :class:`pandas.DataFrame` objects by name.
A registry instance maintains a mapping from string identifiers to
dataframes along with optional metadata about the origin of each table.

Example:

    >>> from agripandas.registry import DataFrameRegistry
    >>> import pandas as pd
    >>> reg = DataFrameRegistry()
    >>> reg.register("foo", pd.DataFrame({"a": [1, 2]}), {"source": "demo"})
    >>> reg.list()
    ['foo']
    >>> df = reg.get("foo")

All registry operations are deterministic and do not perform any I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Any
import pandas as pd


@dataclass
class DataFrameEntry:
    """Container for a dataframe and its associated metadata."""

    dataframe: pd.DataFrame
    metadata: Mapping[str, Any] = field(default_factory=dict)


class DataFrameRegistry:
    """A simple registry for storing named DataFrames.

    The registry maintains a dictionary mapping names to ``DataFrameEntry``
    instances.  It exposes methods to register new tables, retrieve tables,
    and list all registered names.  Names must be unique; registering a
    name that already exists will overwrite the previous entry.

    This class does not enforce any global or singleton behaviour.  Create
    separate instances as needed.
    
    Example:
    >>> from agripandas.registry import DataFrameRegistry
    >>> import pandas as pd
    >>> reg = DataFrameRegistry()
    >>> reg.register("foo", pd.DataFrame({"a": [1, 2]}), {"source": "demo"})
    >>> reg.list()
    ['foo']
    >>> df = reg.get("foo")
    >>> df
       a
    0  1
    1  2
    """

    def __init__(self) -> None:
        self._tables: Dict[str, DataFrameEntry] = {}

    def register(
        self,
        name: str,
        dataframe: pd.DataFrame,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Register a new dataframe under a given name.

        Parameters
        ----------
        name:
            The unique identifier for the table.  If the name already
            exists, the previous entry will be replaced.
        dataframe:
            The ``pandas.DataFrame`` instance to store.
        metadata:
            Optional arbitrary key/value pairs describing the table,
            such as file path, sheet name, timestamp or domain-specific
            provenance.
        """
        self._tables[name] = DataFrameEntry(
            dataframe=dataframe, metadata=dict(metadata or {})
        )

    def get(self, name: str) -> pd.DataFrame:
        """Retrieve a registered dataframe by name.

        Raises
        ------
        KeyError
            If no table with the given name exists.
        """
        return self._tables[name].dataframe

    def get_metadata(self, name: str) -> Mapping[str, Any]:
        """Return the metadata associated with a registered table.

        Raises
        ------
        KeyError
            If no table with the given name exists.
        """
        return self._tables[name].metadata

    def list(self) -> List[str]:
        """Return a list of all registered table names."""
        return list(self._tables.keys())

    def unregister(self, name: str) -> None:
        """Remove a table from the registry.

        Raises
        ------
        KeyError
            If the name is not present.
        """
        del self._tables[name]