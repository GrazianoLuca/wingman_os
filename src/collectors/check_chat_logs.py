from pathlib import Path

import duckdb

# Same location logic as tiktok_logger.py: database/test.db in the parent
# of this script's folder.
DB_PATH = Path(__file__).resolve().parent.parent / "database" / "test.db"

table_name = 'chat'

def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"No database found at {DB_PATH}")

    db_con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        rows = db_con.execute(
            f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT 10"
        ).fetchall()
        columns = [desc[0] for desc in db_con.description]

        if not rows:
            print(f"No rows found in {table_name} table yet.")
            return

        print(f"Latest {len(rows)} row(s) from {table_name}:\n")
        print(" | ".join(columns))
        print("-" * 80)
        for row in rows:
            print(" | ".join(str(value) for value in row))
    finally:
        db_con.close()


if __name__ == "__main__":
    main()