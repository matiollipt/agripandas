"""
Minimal agent scaffold.

This module provides a simple foundation for building ReAct-style
agents that interact with :class:`~agripandas.registry.DataFrameRegistry`
and the structured tools defined in :mod:`agripandas.tools`.  The
implementation here is deliberately minimal: it does not integrate with
any specific LLM provider and avoids executing arbitrary code.

The :class:`SimpleAgent` accepts a registry and a mapping of tool
functions.  When run with a textual question, the agent proceeds
through a fixed number of reasoning steps, each time selecting a tool
based on a heuristic and combining its outputs.  The goal is to
illustrate how one might orchestrate deterministic tools without
coupling to any particular model.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple

from .registry import DataFrameRegistry
from .tools import (
    list_dataframes,
    describe_dataframe,
    get_columns,
    extract_subset,
    groupby_aggregate,
    compute_stat,
)


ToolFunc = Callable[..., Any]


class SimpleAgent:
    """A trivial agent that chooses tools via a heuristic.

    This agent is intended for demonstration purposes only.  It parses
    very simple questions to decide which tool to invoke.  Real
    implementations should replace the naive heuristics here with
    language model reasoning or rule-based planners.
    """

    def __init__(self, registry: DataFrameRegistry, tools: Dict[str, ToolFunc]) -> None:
        self.registry = registry
        self.tools = tools

    def run(self, question: str, max_steps: int = 3) -> Dict[str, Any]:
        """Run the agent on a question and return a final answer.

        The agent loops up to ``max_steps`` times.  At each iteration it
        chooses a tool based on keywords in the question, calls the tool
        with appropriate arguments and incorporates the result into a
        running state.  This simplified flow is purely illustrative.

        Parameters
        ----------
        question:
            The user question in plain language.
        max_steps:
            Maximum number of tool invocations.

        Returns
        -------
        dict
            The final answer.  The format depends on the tools used.
        """
        history: List[Tuple[str, Any]] = []
        for _ in range(max_steps):
            # Naive keyword-based dispatch
            q = question.lower()
            if "list" in q and not any(h[0] == "list" for h in history):
                result = self.tools["list"]()
                history.append(("list", result))
            elif ("describe" in q or "schema" in q) and not any(h[0] == "describe" for h in history):
                # Assume the user mentions the name after the word "describe"
                parts = q.split()
                name = None
                try:
                    if "describe" in parts:
                        name_index = parts.index("describe") + 1
                        if name_index < len(parts):
                            name = parts[name_index]
                except (ValueError, IndexError):
                    pass
                
                if not name:
                    tables = self.registry.list()
                    if tables:
                        name = tables[0]
                
                if name:
                    try:
                        result = self.tools["describe"](name)
                        history.append(("describe", result))
                    except KeyError:
                        history.append(("error", f"Table '{name}' not found"))
            elif "columns" in q and not any(h[0] == "columns" for h in history):
                # Extract the table name heuristically
                tables = self.registry.list()
                if tables:
                    name = tables[0]
                    result = self.tools["columns"](name)
                    history.append(("columns", result))
            
            # If we've already done something, or if no keywords match, we might stop
            if not history:
                # As a default, return available tables if nothing else matched
                result = self.tools["list"]()
                history.append(("list", result))
                break
            
            # For this simple agent, if we have history, we might just break anyway 
            # unless we want to allow it to continue. Let's allow it to continue 
            # if there are multiple keywords.
            if len(history) >= max_steps:
                break
        
        return {"question": question, "history": history}


def default_tools(registry: DataFrameRegistry) -> Dict[str, ToolFunc]:
    """Construct the default tool mapping for :class:`SimpleAgent`.

    Each entry in the returned dictionary is a zero- or single-argument
    function bound to the provided registry.  They can be used directly
    by an agent to operate on the current dataframes.
    """
    return {
        "list": lambda: list_dataframes(registry),
        "describe": lambda name: describe_dataframe(registry, name),
        "columns": lambda name: get_columns(registry, name),
        "subset": lambda name, filters=None, columns=None: extract_subset(
            registry, name, filters=filters, columns=columns
        ),
        "groupby": lambda name, group_cols, agg_spec: groupby_aggregate(
            registry, name, group_cols, agg_spec
        ),
        "stat": lambda name, column, metric: compute_stat(registry, name, column, metric),
    }