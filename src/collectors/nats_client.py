"""Shared NATS JetStream plumbing used by both the collector (publisher) and
the DB loader (consumer), so subject names and stream config live in exactly
one place.
"""
from __future__ import annotations

import os
import sys
import subprocess
import time

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js.api import RetentionPolicy, StreamConfig

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
STREAM_NAME = os.getenv("NATS_STREAM_NAME", "TIKTOK_EVENTS")
SUBJECT_PREFIX = "tiktok.events"


def subject_for(event_type: str) -> str:
    return f"{SUBJECT_PREFIX}.{event_type}"

def ensure_nats_server(name: str = "nats-server") -> None:
    """Ensure NATS JetStream container is running."""
    res = subprocess.run(f"docker inspect -f '{{{{.State.Running}}}}' {name}", shell=True, capture_output=True, text=True)
    
    if res.returncode != 0 and "Cannot connect to the Docker daemon" in res.stderr:
        sys.exit("Error: Docker daemon is not running. Please start Docker/OrbStack and try again.")
        
    if "true" not in res.stdout:
        print('Starting NATS SERVER')
        cmd = f"docker start {name}" if res.returncode == 0 else f"docker run -d --name {name} -p 4222:4222 nats -js"
        subprocess.run(cmd, shell=True, check=True)
        time.sleep(1)

        



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