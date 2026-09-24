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
from dotenv import load_dotenv

from nats.js.api import ConsumerConfig, DeliverPolicy

from src.db.database import batch_insert_events, create_tables, get_db_connection
from src.collectors.nats_client import STREAM_NAME, SUBJECT_PREFIX, connect, ensure_stream




# event_type -> (table name, function turning the published payload into the
# row tuple that database.TABLE_COLUMNS[table] expects, in order).
def _chat_row(event: dict) -> tuple:
    data = event.get("data", {})
    collected_at = event.get("collected_at")


    user = data.get("user", {})
    comment = data.get("comment", "")
    uid = user.get("uniqueId")
    nick = user.get("nickname")


    return (collected_at, nick, uid, comment)


def _gift_row(event: dict) -> tuple:
    data = event.get("data", {})
    collected_at = event.get("collected_at")
    user = data.get("user", {})

    gift_name = data.get("giftName", "Unknown Gift")
    repeat_count = data.get("repeatCount", 1)
    coin_value = data.get("diamondCount", 0)

    uid = user.get("uniqueId")
    nick = user.get("nickname")

    return (
        collected_at, uid, nick,
        gift_name, repeat_count, coin_value, repeat_count * coin_value,
    )


def _social_row(event: dict) -> tuple:
    data = event.get("data", {})
    collected_at = event.get("collected_at")
    user = data.get("user", {})


    uid = user.get("uniqueId")
    nick = user.get("nickname")

    action = data.get("action", "interaction")



    return (collected_at, uid, nick, action)


def _subscribe_row(event: dict) -> tuple:
    data = event.get("data", {})
    collected_at = event.get("collected_at")
    user = data.get("user", {})


    uid = user.get("uniqueId")
    nick = user.get("nickname")



    return (collected_at, uid, nick)

ROW_BUILDERS = {
    "chat": ("chat", _chat_row),
    "gift": ("gift", _gift_row),
    "social": ("social", _social_row),
    "subscribe": ("subscribe", _subscribe_row),
}


def timestamp_str() -> str:
    return datetime.now().strftime("%H:%M:%S")

async def fetch_batch_accumulate(sub, target_batch_size: int, timeout_s: float) -> list:
    """Accumulates messages until target_batch_size is reached OR timeout_s expires."""
    msgs = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s

    while len(msgs) < target_batch_size:
        time_left = deadline - loop.time()
        if time_left <= 0:
            break

        fetch_count = target_batch_size - len(msgs)
        try:
            # Fetch remaining items with time_left as timeout
            new_msgs = await sub.fetch(fetch_count, timeout=time_left)
            msgs.extend(new_msgs)
        except asyncio.TimeoutError:
            break
        except Exception as exc: # e.g. TimeoutError variant in nats client
            if "timeout" in str(exc).lower():
                break
            raise

    return msgs

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
    load_dotenv()
    NATS_DURABLE_NAME = os.getenv("NATS_DURABLE_NAME", "db-loader")
    LOADER_BATCH_SIZE = int(os.getenv("LOADER_BATCH_SIZE", "100"))
    LOADER_BATCH_TIMEOUT_S = float(os.getenv("LOADER_BATCH_TIMEOUT_S", "60"))


    db_con = get_db_connection()
    create_tables(db_con)

    nc, js = await connect()
    await ensure_stream(js)

    sub = await js.pull_subscribe(
        f"{SUBJECT_PREFIX}.>",
        durable=NATS_DURABLE_NAME,
        stream=STREAM_NAME,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL),
    )

    print(f"Consuming '{SUBJECT_PREFIX}.>' from stream '{STREAM_NAME}' as durable '{NATS_DURABLE_NAME}'...")
    print(f"Batch size={LOADER_BATCH_SIZE}, batch timeout={LOADER_BATCH_TIMEOUT_S}s")

    try:
        while True:
            msgs = await fetch_batch_accumulate(sub, LOADER_BATCH_SIZE, LOADER_BATCH_TIMEOUT_S)
            if msgs:
                await process_batch(db_con, msgs)
    finally:
        db_con.close()
        await nc.drain()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()