"""
Production Pipeline for agripandas.

This script implements a pipeline to extract, inspect, and process data from 
Excel workbooks using the agripandas framework.
"""

from agripandas import (
    DataFrameRegistry,
    load_excel
)

def run_production_pipeline(input_file: str):
    print(f"Starting robust production pipeline for: {input_file}")
    
    registry = DataFrameRegistry()

    try:
        # load_excel now uses robust header detection and merging
        registered_tables = load_excel(registry, input_file)
    except Exception as e:
        print(f"Failed to load Excel file: {e}")
        return

    if not registered_tables:
        print("No data found in the specified file.")
        return

    print(f"Successfully registered {len(registered_tables)} tables.")

    summary = []
    for table_name in registered_tables:
        df = registry.get(table_name)
        row_count = len(df)
        col_count = len(df.columns)
        
        # Identify columns that are likely data (numeric)
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        print(f"\n--- Table: {table_name} ---")
        print(f"Shape: {row_count} rows x {col_count} columns")
        print(f"Detected Headers: {', '.join(df.columns[:5])} ...")
        
        summary.append({
            "table": table_name,
            "rows": row_count,
            "cols": col_count,
            "numeric_cols": len(numeric_cols)
        })

    print("\n--- Pipeline Summary ---")
    for item in summary:
        print(f"{item['table']}: {item['rows']} rows, {item['numeric_cols']} numeric columns")

    print("\nPipeline execution finished successfully.")

if __name__ == "__main__":
    target_file = "data/1_ExemploHarvista.xlsx"
    run_production_pipeline(target_file)
