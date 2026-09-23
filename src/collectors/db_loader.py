"""Pull-consumes events from NATS JetStream in batches and writes them to
DuckDB via src/db/database.py.

Run this alongside src/collectors/chat_collector.py. The two processes only
talk to each other through NATS, so either can be restarted independently
without losing events — JetStream holds messages until this loader acks
them, and a durable consumer means restarting the loader resumes where it
left off rather than re-reading the whole stream.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime

from nats.js.api import ConsumerConfig, DeliverPolicy

from src.db.database import batch_insert_events, create_tables, get_db_connection
from src.collectors.nats_client import STREAM_NAME, SUBJECT_PREFIX, connect, ensure_stream

DURABLE_NAME = os.getenv("NATS_DURABLE_NAME", "db-loader")
BATCH_SIZE = int(os.getenv("LOADER_BATCH_SIZE", "100"))
BATCH_TIMEOUT_S = float(os.getenv("LOADER_BATCH_TIMEOUT_S", "5"))


# event_type -> (table name, function turning the published payload into the
# row tuple that database.TABLE_COLUMNS[table] expects, in order).
def _chat_row(p: dict) -> tuple:
    d = p["data"]
    u = d["user"]
    return (p["collected_at"], p["streamer"], u["uniqueId"], u["nickname"], d["comment"])


def _gift_row(p: dict) -> tuple:
    d = p["data"]
    u = d["user"]
    repeat_count = d.get("repeatCount", 1)
    coin_value = d.get("diamondCount", 0)
    return (
        p["collected_at"], p["streamer"], u["uniqueId"], u["nickname"],
        d.get("giftName", "Unknown Gift"), repeat_count, coin_value, repeat_count * coin_value,
    )


def _social_row(p: dict) -> tuple:
    d = p["data"]
    u = d["user"]
    return (p["collected_at"], p["streamer"], u["uniqueId"], u["nickname"], d.get("action", "interaction"))


def _subscribe_row(p: dict) -> tuple:
    d = p["data"]
    u = d["user"]
    return (p["collected_at"], p["streamer"], u["uniqueId"], u["nickname"])


ROW_BUILDERS = {
    "chat": ("chat", _chat_row),
    "gift": ("gift", _gift_row),
    "social": ("social", _social_row),
    "subscribe": ("subscribe", _subscribe_row),
}


def timestamp_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


async def fetch_batch(sub) -> list:
    """Pull up to BATCH_SIZE messages, waiting at most BATCH_TIMEOUT_S. If no
    messages arrive in that window, returns an empty list rather than
    blocking forever — that's what lets the loop stay responsive."""
    try:
        return await sub.fetch(BATCH_SIZE, timeout=BATCH_TIMEOUT_S)
    except asyncio.TimeoutError:
        return []


async def process_batch(db_con, msgs) -> None:
    rows_by_table: dict[str, list] = {}
    for msg in msgs:
        try:
            payload = json.loads(msg.data.decode())
            table, build_row = ROW_BUILDERS[payload["event_type"]]
            rows_by_table.setdefault(table, []).append(build_row(payload))
        except Exception as exc:  # noqa: BLE001 - one bad message shouldn't sink the whole batch
            print(f"[{timestamp_str()}] ⚠️  Skipping malformed message: {exc!r}")

    if rows_by_table:
        inserted = batch_insert_events(db_con, rows_by_table)
        print(f"[{timestamp_str()}] 💾 Inserted {sum(inserted.values())} rows: {inserted}")

    # Ack only after the DB write succeeds, so a crash mid-batch leaves the
    # messages redelivered rather than silently lost.
    for msg in msgs:
        await msg.ack()


async def run() -> None:
    db_con = get_db_connection()
    create_tables(db_con)

    nc, js = await connect()
    await ensure_stream(js)

    sub = await js.pull_subscribe(
        f"{SUBJECT_PREFIX}.>",
        durable=DURABLE_NAME,
        stream=STREAM_NAME,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL),
    )

    print(f"Consuming '{SUBJECT_PREFIX}.>' from stream '{STREAM_NAME}' as durable '{DURABLE_NAME}'...")
    print(f"Batch size={BATCH_SIZE}, batch timeout={BATCH_TIMEOUT_S}s")

    try:
        while True:
            msgs = await fetch_batch(sub)
            if msgs:
                await process_batch(db_con, msgs)
    finally:
        db_con.close()
        await nc.drain()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()