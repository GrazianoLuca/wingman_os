"""Shared NATS JetStream plumbing used by both the collector (publisher) and
the DB loader (consumer), so subject names and stream config live in exactly
one place.
"""
from __future__ import annotations

import os
import subprocess
import time
import docker
from docker.errors import APIError, DockerException, NotFound

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js.api import RetentionPolicy, StreamConfig

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
STREAM_NAME = os.getenv("NATS_STREAM_NAME", "TIKTOK_EVENTS")
SUBJECT_PREFIX = "tiktok.events"


def subject_for(event_type: str) -> str:
    return f"{SUBJECT_PREFIX}.{event_type}"


def start_docker_daemon(timeout_seconds: int = 30) -> None:
    """Attempts to launch OrbStack or Docker Desktop on macOS and waits for daemon."""
    print("Docker daemon not running. Attempting to start...")
    
    # Try OrbStack first, fall back to Docker Desktop
    try:
        subprocess.run(["open", "-a", "OrbStack"], check=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        try:
            subprocess.run(["open", "-a", "Docker"], check=True, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            raise RuntimeError("Neither OrbStack nor Docker Desktop is installed.")

    # Poll until socket responds
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        res = subprocess.run(["docker", "info"], capture_output=True)
        if res.returncode == 0:
            print("Docker daemon started successfully.")
            return
        time.sleep(1)

    raise TimeoutError(f"Docker daemon failed to start within {timeout_seconds} seconds.")

def ensure_nats_server(name: str = "nats-server") -> None:
    """Ensure NATS JetStream container is running using Docker Python SDK."""
    try:
        client = docker.from_env()
        # Ping daemon to fail fast if down
        client.ping()
    except:
        start_docker_daemon(5)
        client = docker.from_env()

    # Step 2: Ensure container is running
    try:
        container = client.containers.get(name)
        if container.status != "running":
            print(f"Starting stopped container '{name}'...")
            container.start()
            time.sleep(1)
    except NotFound:
        print(f"Creating and starting container '{name}'...")
        client.containers.run(
            "nats",
            command="-js",
            name=name,
            ports={"4222/tcp": 4222},
            detach=True
        )
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