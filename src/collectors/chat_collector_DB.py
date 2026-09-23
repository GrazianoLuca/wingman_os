import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

import duckdb
import websockets
from dotenv import load_dotenv

# database/test.db lives in the parent of this file's folder
DB_PATH = Path(__file__).resolve().parent.parent / "database" / "test.db"


# ---------------------------------------------------------------------------
# Formatted terminal output — every event that arrives gets printed, whether
# or not we have a table/handler for it. This is what lets you visually
# confirm the connection is actually receiving data.
# ---------------------------------------------------------------------------

def timestamp_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def print_event(event_type: str, emoji: str, headline: str, raw_event: dict | None = None) -> None:
    print(f"[{timestamp_str()}] {emoji} {event_type.upper():<10} | {headline}")
    if raw_event is not None:
        # Unrecognized event type — dump the raw payload so you can see its shape
        print(f"           raw: {json.dumps(raw_event, default=str)[:300]}")


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


# ---------------------------------------------------------------------------
# Persistence — these run synchronously (awaited directly, no fire-and-forget
# create_task) so that any insert error surfaces immediately instead of
# failing silently in a background task.
# ---------------------------------------------------------------------------

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


async def run_save(coro) -> None:
    """Await a save_* coroutine and surface any failure instead of swallowing it."""
    try:
        await coro
    except Exception as exc:  # noqa: BLE001 - we want to see any DB error, not hide it
        print(f"           ❌ DB WRITE FAILED: {exc!r}")


# ---------------------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------------------

async def handle_chat_event(db_con, streamer, event) -> None:
    user_id = event["data"]["user"]["uniqueId"]
    nickname = event["data"]["user"]["nickname"]
    comment = event["data"]["comment"]

    print_event("chat", "💬", f"@{user_id} ({nickname}): {comment}")
    await run_save(save_chat(db_con, streamer, user_id, nickname, comment))


async def handle_gift_event(db_con, streamer, event) -> None:
    user_id = event["data"]["user"]["uniqueId"]
    nickname = event["data"]["user"]["nickname"]
    gift_name = event["data"].get("giftName", "Unknown Gift")
    repeat_count = event["data"].get("repeatCount", 1)
    coin_value = event["data"].get("diamondCount", 0)
    total_coins = repeat_count * coin_value

    print_event(
        "gift", "🎁",
        f"@{user_id} ({nickname}) sent {repeat_count}x {gift_name} (Total: {total_coins} coins)",
    )
    await run_save(
        save_gift(db_con, streamer, user_id, nickname, gift_name, repeat_count, coin_value, total_coins)
    )


async def handle_social_event(db_con, streamer, event) -> None:
    user_id = event["data"]["user"]["uniqueId"]
    nickname = event["data"]["user"]["nickname"]
    action = event["data"].get("action", "interaction")

    if "follow" in str(action).lower():
        print_event("social", "➕", f"@{user_id} ({nickname}) followed the stream!")
    else:
        print_event("social", "🔗", f"@{user_id} ({nickname}) did: {action}")

    await run_save(save_social(db_con, streamer, user_id, nickname, action))


async def handle_subscribe_event(db_con, streamer, event) -> None:
    user_id = event["data"]["user"]["uniqueId"]
    nickname = event["data"]["user"]["nickname"]

    print_event("subscribe", "⭐", f"@{user_id} ({nickname}) subscribed!")
    await run_save(save_subscribe(db_con, streamer, user_id, nickname))


async def handle_unrecognized_event(db_con, streamer, event) -> None:
    """Catch-all so nothing arriving over the socket goes unseen, even if we
    don't have a table/handler for it yet (e.g. 'member', 'like', 'roomUser')."""
    event_type = event.get("event", "unknown")


EVENT_HANDLERS = {
    "chat": handle_chat_event,
    "gift": handle_gift_event,
    "social": handle_social_event,
    "subscribe": handle_subscribe_event,
}


async def listen(db_con, streamer: str, api_key: str) -> None:
    """Connect to the tik.tools websocket and dispatch incoming events."""
    url = f"wss://api.tik.tools?uniqueId={streamer}&apiKey={api_key}"

    async with websockets.connect(url) as ws:
        print(f"Connected. Listening for events on @{streamer}...\n")
        async for message in ws:
            try:
                event = json.loads(message)
            except json.JSONDecodeError:
                print(f"[{timestamp_str()}] ⚠️  Non-JSON message: {message!r}")
                continue

            event_type = event.get("event")
            handler = EVENT_HANDLERS.get(event_type, handle_unrecognized_event)
            await handler(db_con, streamer, event)


def main() -> None:
    load_dotenv()
    api_key = os.getenv("TIKTOOL_API_KEY")
    streamer = os.getenv("STREAMER_NAME")

    if not api_key or not streamer:
        raise RuntimeError("TIKTOOL_API_KEY and STREAMER_NAME must be set in your .env file")

    db_con = get_db_connection()
    create_tables(db_con)
    print(f"Using database: {DB_PATH}")

    try:
        asyncio.run(listen(db_con, streamer, api_key))
    finally:
        db_con.close()


if __name__ == "__main__":
    main()