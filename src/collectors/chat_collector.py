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
from websockets.exceptions import InvalidStatusCode, ConnectionClosedError, ConnectionClosedOK
from dotenv import load_dotenv

from src.collectors.nats_client import connect, ensure_stream, subject_for, ensure_nats_server

RECOGNIZED_EVENTS = {"chat", "gift", "social", "subscribe", 'stream_end'}


def timestamp_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def extract_levels_from_user(user: dict) -> tuple[int, int]:
    """
    Extracts (gifter_level, team_level) safely across tik.tools JSON structures.
    """
    gifter_level = user.get("gifterLevel") or user.get("badgeLevel") or 0
    team_level = user.get("teamLevel") or user.get("fanLevel") or 0

    # Inspect 'badges' or 'badgeList' if top-level fields are missing/zero
    badges = user.get("badges") or user.get("badgeList") or []
    if isinstance(badges, list):
        for badge in badges:
            if not isinstance(badge, dict):
                continue

            b_type = str(badge.get("type", "")).lower()
            b_scene = str(badge.get("badgeSceneType", "")).lower()
            b_name = str(badge.get("name", "")).lower()
            level = badge.get("level") or badge.get("badgeLevel") or 0

            # 1. Platform-Wide Gifter Level
            if gifter_level == 0:
                if "gifter" in b_type or "gifter" in b_scene or "gifter" in b_name:
                    gifter_level = level

            # 2. Channel-Specific Team / Fan / Grade Level
            if team_level == 0:
                if any(k in b_type or k in b_scene or k in b_name for k in ("fan", "team", "grade", "sub")):
                    team_level = level

    return int(gifter_level), int(team_level)


def describe(event_type: str, event: dict) -> str:
    """Short human-readable summary for terminal logging."""
    data = event.get("data", {})
    user = data.get("user", {})
    uid = user.get("uniqueId", "?")
    nick = user.get("nickname", "?")
    
    g_level, t_level = extract_levels_from_user(user)
    
    # Format badge indicator: e.g., [Gifter Lvl 25 | Team Lvl 5]
    levels = []
    if g_level > 0:
        levels.append(f"Gifter Lvl {g_level}")
    if t_level > 0:
        levels.append(f"Stream Lvl {t_level}")
    level_str = f" [{' | '.join(levels)}]" if levels else ""

    if event_type == "chat":
        lang = data.get("language")
        lang_str = f" ({lang})" if lang and lang != "unknown" else ""
        return f"{lang_str} | Level {g_level} | {nick}: {data.get('comment', '')}"
    
    if event_type == "gift":
        repeat = data.get("repeatCount", 1)
        coins = data.get("diamondCount", 0)
        gift_name = data.get("giftName", "Unknown Gift")
        return f"Level {g_level} | {nick} sent {repeat}x {gift_name} (Total: {repeat * coins} coins)"

    if event_type == "social":
        action = data.get('action', 'interaction')
        return f"nick did: {action}"
    
    if event_type == "subscribe":
        return f"{nick} subscribed!"
    
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
    """Connect to tik.tools websocket with exponential backoff and publish events to NATS."""
    url = f"wss://api.tik.tools?uniqueId={streamer}&apiKey={api_key}"
    backoff = 2  # Start with a safer initial delay

    while True:
        try:
            async with websockets.connect(
                url, 
                ping_interval=20, 
                ping_timeout=20,
                close_timeout=10,
                max_size=None
            ) as ws:
                print(f"[{timestamp_str()}] Connected to @{streamer}")
                backoff = 2  # Reset backoff ONLY after a successful connection

                async for message in ws:
                    try:
                        event = json.loads(message)
                    except json.JSONDecodeError:
                        print(f"[{timestamp_str()}] ⚠️ Non-JSON message: {message!r}")
                        continue

                    # 1. Handle explicit stream status/error payloads
                    status = event.get("status") or event.get("data", {}).get("status")
                    event_type = event.get("event")

                    if status == "offline" or event_type in ("stream_end", "live_end"):
                        print(f"[{timestamp_str()}] 🛑 Streamer @{streamer} OFFLINE.")
                        # Set long poll interval for offline streams and exit WS loop
                        backoff = 60
                        break

                    if event_type not in RECOGNIZED_EVENTS:
                        continue

                    print(f"{timestamp_str()} | {event_type.upper()} | {describe(event_type, event)}")

                    try:
                        await publish_event(js, streamer, event_type, event)
                    except Exception as exc:  # noqa: BLE001
                        print(f"❌ NATS PUBLISH FAILED: {exc!r}")

        except InvalidStatusCode as exc:
            # Captures HTTP 429 (Rate Limit) & HTTP 403 (Forbidden / API Key Block)
            status_code = exc.status_code
            print(f"[{timestamp_str()}] 🚫 Handshake HTTP {status_code} on @{streamer}.")
            
            if status_code == 429:
                backoff = min(max(backoff * 2, 60), 300)  # Cool down 1 to 5 mins
            elif status_code == 403:
                print(f"[{timestamp_str()}] ❌ Invalid API Key or IP banned. Stopping loop.")
                return  # Terminate task on auth failure to conserve resources
            else:
                backoff = min(backoff * 2, 60)

        except (ConnectionClosedError, ConnectionClosedOK) as exc:
            print(f"[{timestamp_str()}] ⚠️ WS connection dropped ({exc!r}). Reconnecting...")
            backoff = min(backoff * 2, 60)

        except Exception as exc:
            print(f"[{timestamp_str()}] ❌ Unexpected error ({exc!r}). Reconnecting...")
            backoff = min(backoff * 2, 60)

        await asyncio.sleep(backoff)


async def main_async() -> None:
    load_dotenv()
    api_key = os.getenv("TIKTOOL_API_KEY")
    streamer = os.getenv("STREAMER_NAME")

    if not api_key or not streamer:
        raise RuntimeError("TIKTOOL_API_KEY and STREAMER_NAME must be set in your .env file")

    ensure_nats_server()
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