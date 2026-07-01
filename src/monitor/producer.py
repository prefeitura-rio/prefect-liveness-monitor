from collections.abc import Iterable
from datetime import UTC, datetime
from queue import Queue

from httpx import Client
from loguru import logger
from stamina import retry_context
from stamina.instrumentation import RetryDetails, set_on_retry_hooks

from .config import Config
from .http import read_token

CONTAINER = "prefect-background-services"
BACKOFF_CAP: int = 30
MAX_RECONNECT_ATTEMPTS: int = 10


class StreamClosed(Exception):
    """Raised after a clean stream end to signal stamina to reconnect."""


def log_retry(details: RetryDetails) -> None:
    """Log a warning via loguru whenever stamina schedules a retry."""
    logger.warning(
        "stream disconnected (attempt {}/{}), reconnecting in {:.1f}s",
        details.retry_num,
        MAX_RECONNECT_ATTEMPTS,
        details.wait_for,
    )


def drain_lines(
    lines: Iterable[str],
    seen: set[int],
    out: Queue[str | None],
) -> datetime | None:
    """Filter, deduplicate and emit log lines; return timestamp of last new line."""
    new_lines = [
        line
        for raw in lines
        # set.add() returns None; not None is True — side-effect: records h in seen
        if (line := raw.strip()) and (h := hash(line)) not in seen and not seen.add(h)
    ]
    for line in new_lines:
        out.put(line)
    return datetime.now(UTC) if new_lines else None


def connect_and_stream(
    session: Client,
    config: Config,
    seen: set[int],
    out: Queue[str | None],
    last_seen: datetime,
) -> datetime:
    """Open one log stream connection, drain lines, and return updated last_seen."""
    params = {
        "container": CONTAINER,
        "follow": "true",
        "sinceTime": last_seen.strftime("%Y-%m-%dT%H:%M:%SZ"),  # last_seen is always UTC
    }
    with session.stream(
        "GET",
        config.log_url,
        headers={"Authorization": f"Bearer {read_token()}"},
        params=params,
    ) as resp:
        _ = resp.raise_for_status()
        return drain_lines(resp.iter_lines(), seen, out) or last_seen


def stream_producer(
    session: Client,
    config: Config,
    out: Queue[str | None],
) -> None:
    """Push log lines from the K8s streaming log API into *out*.

    Opens a follow=true connection and forwards each non-blank line.
    A persistent hash set deduplicates lines replayed across the sinceTime
    reconnect boundary. Puts None as a terminal sentinel after exhausting
    MAX_RECONNECT_ATTEMPTS consecutive failures.
    """
    set_on_retry_hooks([log_retry])
    seen_hashes: set[int] = set()
    last_seen: datetime = datetime.now(UTC)

    try:
        for attempt in retry_context(
            on=Exception,
            attempts=MAX_RECONNECT_ATTEMPTS,
            wait_max=BACKOFF_CAP,
        ):
            with attempt:
                last_seen = connect_and_stream(session, config, seen_hashes, out, last_seen)
                raise StreamClosed()

    except Exception as exc:
        logger.warning("last error before giving up: {}", exc)

    logger.error("max reconnect attempts reached, giving up")
    out.put(None)
