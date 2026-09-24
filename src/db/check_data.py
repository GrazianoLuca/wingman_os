from pathlib import Path 
import duckdb


# src/db/database.py -> parent(db) -> parent(src) -> parent(project root)
DB_PATH = Path(__file__).resolve().parent / "test.db"


ALLOWED_TABLES = ['chat', 'gift', 'social', 'subscribe']


def get_db_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open (and create if needed) the DuckDB database file."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)

def do_query(db_con: duckdb.DuckDBPyConnection, table: str, allowed_tables: set[str]):
    """
    Executes a grouped count query on an allowed DuckDB table.
    """
    if table in allowed_tables:
        query = f"""
            SELECT user_id, COUNT(comment) AS comment_count
            FROM {table}
            GROUP BY user_id
        """
        result = db_con.execute(query).df()  # Returns a pandas DataFrame
        print(result)
        return result
    
    raise ValueError(f"Table '{table}' is not in the allowed tables list.")


def main() -> None:

    TABLE = 'chat'
    with get_db_connection(read_only=True) as db_con:
        do_query(
            db_con=db_con,
            table=TABLE,
            allowed_tables=ALLOWED_TABLES,
        )


if __name__ == "__main__":
    main()

