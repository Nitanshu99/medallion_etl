"""Handles formatting and printing of query results."""
import pandas as pd

def print_welcome_banner(table_names: list):
    """Prints the REPL welcome and help text."""
    print("\n---")
    print("Type your SQL query and press Enter.")
    if table_names:
        print(f"e.g., 'SELECT * FROM {table_names[0]} LIMIT 5;'")
    print("Type 'q' or 'exit' to quit.")
    print("Type '.vertical' or '.horizontal' to change display (Default: VERTICAL).")
    print("---\n")

def print_results(df: pd.DataFrame, vertical_mode: bool):
    """Prints a DataFrame in either vertical or horizontal mode."""
    if df.empty:
        print("(No results)")
        return

    if vertical_mode:
        # --- VERTICAL MODE ---
        # Print each row one by one, like a form
        total_rows = len(df)
        for row_num, (_, row) in enumerate(df.iterrows(), 1):
            print(f"---[ Row {row_num} / {total_rows} ]---")
            # A pandas Series.to_string() is always readable
            print(row.to_string())
        print(f"---[ End of {total_rows} rows ]---")
    else:
        # --- HORIZONTAL MODE ---
        # The original method, for tables you know are small
        print(df.to_string(index=False))
