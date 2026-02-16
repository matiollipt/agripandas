"""
LangChain-based agent implementation.

This module provides an agent that loads data artifacts (CSVs) from a directory
into a DataFrameRegistry and uses an LLM (via Ollama) to answer questions
using the structured tools defined in `agripandas.tools`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List

from langchain_community.chat_models import ChatOllama
try:
    from langchain.agents import create_tool_calling_agent
except ImportError:
    create_tool_calling_agent = None
    from langchain.agents import initialize_agent, AgentType

try:
    from langchain.agents import AgentExecutor
except ImportError:
    from langchain.agents.agent import AgentExecutor
from langchain_core.prompts import ChatPromptTemplate
from langchain.tools import StructuredTool

from . import (
    DataFrameRegistry,
    load_folder,
    list_dataframes,
    describe_dataframe,
    get_columns,
    extract_subset,
    groupby_aggregate,
    compute_stat,
    DescribeInput,
    ColumnsInput,
    SubsetInput,
    GroupByInput,
    StatInput,
)


class AgriPandasAgent:
    """
    An agent that connects a DataFrameRegistry (populated from artifacts)
    to an Ollama-powered LLM using LangChain.
    """

    def __init__(
        self,
        artifacts_dir: str | Path = "./_artifacts",
        model: str = "llama3",
        base_url: str = "http://localhost:11434",
    ) -> None:
        self.registry = DataFrameRegistry()
        self.artifacts_dir = Path(artifacts_dir)

        # 1. Load artifacts into registry
        if self.artifacts_dir.exists():
            # We assume artifacts are CSVs exported by the pipeline
            load_folder(self.registry, str(self.artifacts_dir), pattern="*.csv")
        else:
            print(f"Warning: Artifacts directory '{self.artifacts_dir}' not found.")

        # 2. Setup LLM
        self.llm = ChatOllama(
            model=model,
            base_url=base_url,
            temperature=0,
        )

        # 3. Setup Tools
        self.tools = self._build_tools()

        # 4. Setup Agent
        if create_tool_calling_agent:
            prompt = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        "You are a data analysis assistant. You have access to a set of "
                        "dataframes loaded from CSV artifacts. Use the provided tools to "
                        "inspect the data (list tables, describe schema) and answer "
                        "questions. Always verify column names before running queries.",
                    ),
                    ("human", "{input}"),
                    ("placeholder", "{agent_scratchpad}"),
                ]
            )

            agent = create_tool_calling_agent(self.llm, self.tools, prompt)
            self.agent_executor = AgentExecutor(
                agent=agent, tools=self.tools, verbose=True, handle_parsing_errors=True
            )
        else:
            # Fallback for older LangChain versions
            self.agent_executor = initialize_agent(
                tools=self.tools,
                llm=self.llm,
                agent=AgentType.STRUCTURED_CHAT_ZERO_SHOT_REACT_DESCRIPTION,
                verbose=True,
                handle_parsing_errors=True,
            )

    def _build_tools(self) -> List[StructuredTool]:
        """Create LangChain tools bound to the current registry."""
        return [
            StructuredTool.from_function(
                func=lambda: list_dataframes(self.registry),
                name="list_dataframes",
                description="List the names of all available dataframes.",
            ),
            StructuredTool.from_function(
                func=lambda name: describe_dataframe(self.registry, name),
                name="describe_dataframe",
                description="Get the schema (columns, dtypes) of a dataframe.",
                args_schema=DescribeInput,
            ),
            StructuredTool.from_function(
                func=lambda name: get_columns(self.registry, name),
                name="get_columns",
                description="Get a list of column names for a dataframe.",
                args_schema=ColumnsInput,
            ),
            StructuredTool.from_function(
                func=lambda name, filters=None, columns=None: extract_subset(
                    self.registry, name, filters=filters, columns=columns
                ),
                name="extract_subset",
                description="Extract a subset of rows based on filters.",
                args_schema=SubsetInput,
            ),
            StructuredTool.from_function(
                func=lambda name, group_cols, agg_spec: groupby_aggregate(
                    self.registry, name, group_cols, agg_spec
                ),
                name="groupby_aggregate",
                description="Group by columns and calculate aggregations.",
                args_schema=GroupByInput,
            ),
            StructuredTool.from_function(
                func=lambda name, column, metric: compute_stat(
                    self.registry, name, column, metric
                ),
                name="compute_stat",
                description="Compute a statistic (mean, max, etc.) for a column.",
                args_schema=StatInput,
            ),
        ]

    def run(self, question: str) -> Any:
        """Run the agent on a question."""
        result = self.agent_executor.invoke({"input": question})
        return result["output"]

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        print(AgriPandasAgent().run(sys.argv[1]))
    else:
        print("Usage: python -m agripandas.agent 'Your question here'")