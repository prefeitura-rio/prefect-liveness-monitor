import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import UTC, datetime

from httpx import AsyncClient
from loguru import logger

from .config import Config
from .http import read_token

CONTAINER = "prefect-background-services"
BACKOFF_CAP: int = 30
MAX_RECONNECT_ATTEMPTS: int = 10


@dataclass
class LogStream:
    """Stream log lines from the Kubernetes log API, reconnecting on failure."""

    session: AsyncClient
    config: Config
    seen: set[int] = field(default_factory=set, init=False)
    last_seen: datetime = field(default_factory=lambda: datetime.now(UTC), init=False)

    def build_params(self) -> dict[str, str]:
        """Build K8s log API query parameters for the current stream position."""
        return {
            "container": CONTAINER,
            "follow": "true",
            "sinceTime": self.last_seen.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    def next_line(self, raw: str) -> str | None:
        """Strip and dedup a raw line; return None if blank or already seen."""
        line = raw.strip()
        line_hash = hash(line)

        if not line or line_hash in self.seen:
            return None

        self.seen.add(line_hash)
        self.last_seen = datetime.now(UTC)
        return line

    async def connect(self) -> AsyncGenerator[str]:
        """Open one HTTP stream and yield new deduplicated lines."""
        async with self.session.stream(
            "GET",
            self.config.log_url,
            headers={"Authorization": f"Bearer {read_token()}"},
            params=self.build_params(),
        ) as resp:
            _ = resp.raise_for_status()
            async for raw in resp.aiter_lines():
                line = self.next_line(raw)

                if line is not None:
                    yield line

    async def stream(self) -> AsyncGenerator[str]:
        """Yield lines, reconnecting on failure with exponential backoff."""
        for attempt in range(MAX_RECONNECT_ATTEMPTS):
            try:
                async for line in self.connect():
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
