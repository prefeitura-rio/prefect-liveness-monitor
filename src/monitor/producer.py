import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

from httpx import AsyncClient
from loguru import logger

from .config import Config
from .http import read_token

CONTAINER = "prefect-background-services"
BACKOFF_CAP: int = 30
MAX_RECONNECT_ATTEMPTS: int = 10


async def stream_logs(session: AsyncClient, config: Config) -> AsyncGenerator[str]:
    """Yield deduplicated log lines from the K8s log stream, reconnecting on failure."""
    seen: set[int] = set()
    last_seen = datetime.now(UTC)

    for attempt in range(MAX_RECONNECT_ATTEMPTS):
        try:
            params = {
                "container": CONTAINER,
                "follow": "true",
                "sinceTime": last_seen.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            async with session.stream(
                "GET",
                config.log_url,
                headers={"Authorization": f"Bearer {read_token()}"},
                params=params,
            ) as resp:
                _ = resp.raise_for_status()
                async for raw in resp.aiter_lines():
                    line = raw.strip()
                    line_hash = hash(line)
                    if not line or line_hash in seen:
                        continue
                    seen.add(line_hash)
                    last_seen = datetime.now(UTC)
                    yield line
        except Exception as exc:
            if attempt == MAX_RECONNECT_ATTEMPTS - 1:
                logger.error("max reconnect attempts reached, giving up: {}", exc)
                return
            backoff = min(float(BACKOFF_CAP), 0.1 * (2.0**attempt))
            logger.warning(
                "stream disconnected (attempt {}/{}), reconnecting in {:.1f}s",
                attempt + 1,
                MAX_RECONNECT_ATTEMPTS,
                backoff,
            )
            await asyncio.sleep(backoff)
