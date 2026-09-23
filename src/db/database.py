"""Table creation and batch insertion for the TikTok live events DuckDB store."""

from __future__ import annotations
 
from pathlib import Path
from typing import Sequence
 
import duckdb
 
# src/db/database.py -> parent(db) -> parent(src) -> parent(project root)
DB_PATH = Path(__file__).resolve().parent / "test.db"
 
# Column order per table. Used both to build INSERT statements and by the
# loader to know how many placeholders a row needs.
TABLE_COLUMNS: dict[str, list[str]] = {
    "chat": ["timestamp", "streamer", "user_id", "nickname", "comment"],
    "gift": [
        "timestamp", "streamer", "user_id", "nickname", "gift_name",
        "repeat_count", "coin_value", "total_coins",
    ],
    "social": ["timestamp", "streamer", "user_id", "nickname", "action"],
    "subscribe": ["timestamp", "streamer", "user_id", "nickname"],
}
 
 
def get_db_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open (and create if needed) the DuckDB database file."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)
 
 
def create_tables(db_con: duckdb.DuckDBPyConnection) -> None:
    """Create the event tables if they don't already exist."""
    db_con.execute("""
        CREATE TABLE IF NOT EXISTS chat (
            timestamp TIMESTAMP,
            streamer VARCHAR,
            user_id VARCHAR,
            nickname VARCHAR,
            comment VARCHAR
        )
    """)
 
    db_con.execute("""
        CREATE TABLE IF NOT EXISTS gift (
            timestamp TIMESTAMP,
            streamer VARCHAR,
            user_id VARCHAR,
            nickname VARCHAR,
            gift_name VARCHAR,
            repeat_count INTEGER,
            coin_value INTEGER,
            total_coins INTEGER
        )
    """)
 
    db_con.execute("""
        CREATE TABLE IF NOT EXISTS social (
            timestamp TIMESTAMP,
            streamer VARCHAR,
            user_id VARCHAR,
            nickname VARCHAR,
            action VARCHAR
        )
    """)
 
    db_con.execute("""
        CREATE TABLE IF NOT EXISTS subscribe (
            timestamp TIMESTAMP,
            streamer VARCHAR,
            user_id VARCHAR,
            nickname VARCHAR
        )
    """)
 
 
def batch_insert(db_con: duckdb.DuckDBPyConnection, table: str, rows: Sequence[Sequence]) -> int:
    """Insert many rows into `table` in a single executemany call.
 
    Returns the number of rows inserted (0 if `rows` is empty — this is a
    no-op, not an error, since the loader calls this per-table per-batch and
    not every batch will contain every event type).
    """
    if not rows:
        return 0
    if table not in TABLE_COLUMNS:
        raise ValueError(f"Unknown table: {table!r}")
 
    placeholders = ", ".join(["?"] * len(TABLE_COLUMNS[table]))
    db_con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
    return len(rows)
 
 
def batch_insert_events(
    db_con: duckdb.DuckDBPyConnection,
    rows_by_table: dict[str, list[Sequence]],
) -> dict[str, int]:
    """Batch-insert rows for several tables inside a single transaction.
 
    `rows_by_table` looks like {"chat": [row, row, ...], "gift": [...]}.
    All-or-nothing: if any table's insert fails, the whole batch rolls back
    so a message the loader later retries doesn't get double-counted.
    """
    inserted: dict[str, int] = {}
    db_con.execute("BEGIN TRANSACTION")
    try:
        for table, rows in rows_by_table.items():
            inserted[table] = batch_insert(db_con, table, rows)
        db_con.execute("COMMIT")
    except Exception:
        db_con.execute("ROLLBACK")
        raise
    return inserted
 