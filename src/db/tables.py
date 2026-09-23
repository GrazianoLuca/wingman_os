import duckdb
import Path
import asyncio
from datetime import datetime

DB_PATH = Path(__file__).resolve().parent / "test.db"

def get_db_connection() -> duckdb.DuckDBPyConnection:
    """Open (and create if needed) the DuckDB database file."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


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



async def save_chat(db_con, streamer, user_id, nickname, comment) -> None:
    await asyncio.to_thread(
        db_con.execute,
        "INSERT INTO chat VALUES (?, ?, ?, ?, ?)",
        [datetime.now(), streamer, user_id, nickname, comment],
    )


async def save_gift(db_con, streamer, user_id, nickname, gift_name, repeat_count, coin_value, total_coins) -> None:
    await asyncio.to_thread(
        db_con.execute,
        "INSERT INTO gift VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [datetime.now(), streamer, user_id, nickname, gift_name, repeat_count, coin_value, total_coins],
    )


async def save_social(db_con, streamer, user_id, nickname, action) -> None:
    await asyncio.to_thread(
        db_con.execute,
        "INSERT INTO social VALUES (?, ?, ?, ?, ?)",
        [datetime.now(), streamer, user_id, nickname, action],
    )


async def save_subscribe(db_con, streamer, user_id, nickname) -> None:
    await asyncio.to_thread(
        db_con.execute,
        "INSERT INTO subscribe VALUES (?, ?, ?, ?)",
        [datetime.now(), streamer, user_id, nickname],
    )