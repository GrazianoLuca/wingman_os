"""Shared NATS JetStream plumbing used by both the collector (publisher) and
the DB loader (consumer), so subject names and stream config live in exactly
one place.
"""
from __future__ import annotations

import os

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js.api import RetentionPolicy, StreamConfig

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
STREAM_NAME = os.getenv("NATS_STREAM_NAME", "TIKTOK_EVENTS")
SUBJECT_PREFIX = "tiktok.events"


def subject_for(event_type: str) -> str:
    return f"{SUBJECT_PREFIX}.{event_type}"


async def connect() -> tuple[NATSClient, JetStreamContext]:
    """Connect to NATS and return (raw client, JetStream context)."""
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    return nc, js


async def ensure_stream(js: JetStreamContext) -> None:
    """Create the JetStream stream if it doesn't exist yet. Safe to call from
    both the publisher and the consumer on every startup."""
    try:
        await js.stream_info(STREAM_NAME)
    except Exception:
        await js.add_stream(
            StreamConfig(
                name=STREAM_NAME,
                subjects=[f"{SUBJECT_PREFIX}.>"],
                retention=RetentionPolicy.LIMITS,
                max_age=7 * 24 * 60 * 60,  # keep 7 days of events, in seconds
            )
        )