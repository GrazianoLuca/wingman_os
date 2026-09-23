"""Connects to the tik.tools websocket for one streamer and publishes every
recognized event onto NATS JetStream.

This script has no DuckDB dependency — it is a pure producer. Run
src/loaders/db_loader.py as a separate process to actually persist events;
the two only talk to each other through NATS, so either can be restarted or
scaled independently.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import websockets
from dotenv import load_dotenv

from src.collectors.nats_client import connect, ensure_stream, subject_for

RECOGNIZED_EVENTS = {"chat", "gift", "social", "subscribe"}
EVENT_EMOJI = {"chat": "💬", "gift": "🎁", "social": "🔗", "subscribe": "⭐"}


def timestamp_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def describe(event_type: str, event: dict) -> str:
    """Short human-readable summary for terminal logging."""
    data = event.get("data", {})
    user = data.get("user", {})
    uid = user.get("uniqueId", "?")
    nick = user.get("nickname", "?")

    if event_type == "chat":
        return f"@{uid} ({nick}): {data.get('comment', '')}"
    if event_type == "gift":
        repeat = data.get("repeatCount", 1)
        coins = data.get("diamondCount", 0)
        gift_name = data.get("giftName", "Unknown Gift")
        return f"@{uid} ({nick}) sent {repeat}x {gift_name} (Total: {repeat * coins} coins)"
    if event_type == "social":
        return f"@{uid} ({nick}) did: {data.get('action', 'interaction')}"
    if event_type == "subscribe":
        return f"@{uid} ({nick}) subscribed!"
    return json.dumps(event, default=str)[:200]


async def publish_event(js, streamer: str, event_type: str, event: dict) -> None:
    """Wrap the raw event with metadata the loader needs and publish it.

    `collected_at` is stamped here (at collection time) rather than in the
    loader, so the DB timestamp reflects when the event actually happened,
    not whenever the loader gets around to consuming it.
    """
    payload = {
        "streamer": streamer,
        "event_type": event_type,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "data": event.get("data", {}),
    }
    await js.publish(subject_for(event_type), json.dumps(payload, default=str).encode())


async def listen(js, streamer: str, api_key: str) -> None:
    """Connect to the tik.tools websocket and publish incoming events to NATS."""
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
            if event_type not in RECOGNIZED_EVENTS:
                # Still visible in the terminal so nothing goes unseen, but
                # not published — the loader only knows how to persist the
                # four recognized event types.
                
                # print(f"[{timestamp_str()}] ❔ UNRECOGNIZED | {json.dumps(event, default=str)[:200]}")
                continue

            print(f"[{timestamp_str()}] {EVENT_EMOJI[event_type]} {event_type.upper():<10} | "
                  f"{describe(event_type, event)}")

            try:
                await publish_event(js, streamer, event_type, event)
            except Exception as exc:  # noqa: BLE001 - surface publish failures, don't swallow
                print(f"           ❌ NATS PUBLISH FAILED: {exc!r}")


async def main_async() -> None:
    load_dotenv()
    api_key = os.getenv("TIKTOOL_API_KEY")
    streamer = os.getenv("STREAMER_NAME")

    if not api_key or not streamer:
        raise RuntimeError("TIKTOOL_API_KEY and STREAMER_NAME must be set in your .env file")

    nc, js = await connect()
    await ensure_stream(js)
    try:
        await listen(js, streamer, api_key)
    finally:
        await nc.drain()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()