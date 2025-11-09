"""Contains the main Read-Eval-Print Loop (REPL) for the query tool."""
import duckdb
from .database import connect_and_create_views
from .display import print_welcome_banner, print_results

def start_query_repl(layer_name: str, data_path: str):
    """
    Starts an interactive Read-Eval-Print Loop (REPL) for querying.
    """

    # Setup the database connection and views
    try:
        con, table_names = connect_and_create_views(layer_name, data_path)
    except SystemExit:
        return # Exit gracefully if db setup failed

    # Print the welcome message
    print_welcome_banner(table_names)

    # Start the query loop
    vertical_mode = True # Default to readable vertical mode

    while True:
        try:
            query = input("sql> ").strip()

            if query.lower() in ['q', 'exit', '.exit']:
                break

            if query.lower() == '.vertical':
                vertical_mode = True
                print("Display mode set to: VERTICAL")
                continue

            if query.lower() == '.horizontal':
                vertical_mode = False
                print("Display mode set to: HORIZONTAL")
                continue

            if not query:
                continue

            # Execute the query
            result = con.sql(query)

            # If it's a SELECT statement, print the result
            if result:
                df = result.df()
                print_results(df, vertical_mode)

        except duckdb.Error as e:
            print(f"DuckDB Error: {e}")
        except Exception as e: # pylint: disable=broad-except
            print(f"An error occurred: {e}")

    print("\nClosing connection. Goodbye!")
    con.close()
